// A flow's verdict (flow.completed / flow.failed): when it is recorded, that it is recorded
// once, and how it is delivered to webhooks. Verdicts are delivered from the database, so
// these tests call deliverFlowOutcomes() directly with a fake fetch; the last one runs the
// dispatcher on fake timers to check it polls and wakes up by itself.
import { afterEach, describe, expect, it, vi } from "vitest";
import { Broker } from "../../src/broker/broker.js";
import { Conveyor } from "../../src/core/conveyor.js";
import type { EventName } from "../../src/core/events.js";
import type { FlowJobInput, FlowOutcome, Job } from "../../src/core/types.js";
import { WebhookDispatcher, type FetchLike } from "../../src/webhooks/dispatcher.js";
import { manualClock, setup } from "../helpers.js";

afterEach(() => {
  vi.useRealTimers();
});

const node = (key: string, dependsOn?: FlowJobInput["dependsOn"], extra: Partial<FlowJobInput> = {}): FlowJobInput => ({
  key,
  queue: "q",
  name: "x",
  ...(dependsOn ? { dependsOn } : {}),
  ...extra,
});

async function ctx() {
  const clock = manualClock();
  const s = await setup({ clock });
  const outcomes: [EventName, FlowOutcome][] = [];
  s.conveyor.events.on("flow.completed", (o) => outcomes.push(["flow.completed", o]));
  s.conveyor.events.on("flow.failed", (o) => outcomes.push(["flow.failed", o]));
  const calls: { event: string; body: Record<string, any> }[] = [];
  const fetchImpl: FetchLike = async (_url, init) => {
    const body = JSON.parse(String(init.body));
    calls.push({ event: body.event, body });
    return new Response("ok");
  };
  const d = new WebhookDispatcher(s.conveyor, fetchImpl);
  /** Claim the next waiting job in "q" and finish it. */
  const finish = async (how: "complete" | "die" = "complete", queue = "q") => {
    const lease = (await s.broker.claim([queue], "w"))!;
    if (how === "complete") await s.broker.complete(lease.job.id, lease.token, { by: lease.job.flowKey });
    else await s.broker.fail(lease.job.id, lease.token, "boom", { unrecoverable: true });
    return lease.job;
  };
  return { ...s, clock, outcomes, calls, d, finish };
}

describe("flow verdicts", () => {
  it("flow.completed is recorded by the transition that completes the last job, once", async () => {
    const { conveyor, outcomes, finish, clock } = await ctx();
    const flow = await conveyor.enqueueFlow({ jobs: [node("a"), node("b", ["a"]), node("c")] });
    await finish();
    await finish();
    expect(outcomes).toEqual([]);
    expect((await conveyor.getFlow(flow.id)).outcome).toBeNull();
    clock.advance(10);
    await finish();
    const expected = { flowId: flow.id, event: "flow.completed", reason: null, jobId: null, at: clock.now(), dispatchedAt: null };
    expect(outcomes).toEqual([["flow.completed", expected]]);
    expect((await conveyor.getFlow(flow.id)).outcome).toEqual(expected);
  });

  it("flow.failed is recorded as soon as a job dies, even while others still run, and only once", async () => {
    const { conveyor, outcomes, finish } = await ctx();
    const flow = await conveyor.enqueueFlow({
      jobs: [node("a"), node("b"), node("after", [{ key: "a", onFailure: "continue" }, { key: "b", onFailure: "continue" }])],
    });
    const a = await finish("die");
    expect(outcomes).toEqual([["flow.failed", expect.objectContaining({ flowId: flow.id, reason: "dead", jobId: a.id })]]);
    expect((await conveyor.getFlow(flow.id)).status).toBe("running");
    await finish("die"); // b
    await finish(); // after
    expect(outcomes).toHaveLength(1);
  });

  it("the first verdict is final: retrying the dead job and completing the flow records nothing more", async () => {
    const { conveyor, outcomes, finish } = await ctx();
    const flow = await conveyor.enqueueFlow({ jobs: [node("a"), node("b", ["a"])] });
    const a = await finish("die");
    await conveyor.retry(a.id);
    await finish();
    await finish();
    expect((await conveyor.getFlow(flow.id)).status).toBe("completed");
    expect(outcomes.map(([e]) => e)).toEqual(["flow.failed"]);
    expect((await conveyor.getFlow(flow.id)).outcome).toMatchObject({ event: "flow.failed", reason: "dead" });
  });

  it("flow.failed with reason cancelled for cancelFlow, for a cancelled job, and for a job cancelled by a dependency in another flow", async () => {
    const { conveyor, outcomes, finish } = await ctx();
    const viaCancelFlow = await conveyor.enqueueFlow({ jobs: [node("a"), node("b", ["a"])] });
    await conveyor.cancelFlow(viaCancelFlow.id);
    expect(outcomes.at(-1)).toEqual(["flow.failed", expect.objectContaining({ flowId: viaCancelFlow.id, reason: "cancelled", jobId: null })]);

    const oneCancelled = await conveyor.enqueueFlow({ jobs: [node("a", undefined, { queue: "other" }), node("b", undefined, { queue: "other" })] });
    await conveyor.cancel(oneCancelled.jobs[0]!.id);
    expect(outcomes.at(-1)).toEqual(["flow.failed", expect.objectContaining({ flowId: oneCancelled.id, reason: "cancelled", jobId: oneCancelled.jobs[0]!.id })]);

    const upstream = await conveyor.enqueueFlow({ jobs: [node("up")] });
    const downstream = await conveyor.enqueueFlow({ jobs: [node("down", [{ jobId: upstream.jobs[0]!.id }], { queue: "other" })] });
    outcomes.length = 0;
    await finish("die"); // up
    expect(outcomes).toEqual([
      ["flow.failed", expect.objectContaining({ flowId: upstream.id, reason: "dead" })],
      ["flow.failed", expect.objectContaining({ flowId: downstream.id, reason: "cancelled", jobId: downstream.jobs[0]!.id })],
    ]);
  });

  it("cancelFlow of a flow that already failed records nothing more", async () => {
    const { conveyor, outcomes, finish } = await ctx();
    const flow = await conveyor.enqueueFlow({ jobs: [node("a"), node("b")] });
    await finish("die");
    await conveyor.cancelFlow(flow.id);
    expect(outcomes.map(([e, o]) => [e, o.reason])).toEqual([["flow.failed", "dead"]]);
  });

  it("a job that is not in a flow records nothing", async () => {
    const { conveyor, outcomes, finish } = await ctx();
    await conveyor.enqueue({ queue: "q", name: "x" });
    await finish();
    expect(outcomes).toEqual([]);
  });
});

describe("delivering verdicts to webhooks", () => {
  it("delivers each verdict once, to the webhooks subscribed to it, with the flow in the body", async () => {
    const { conveyor, d, calls, finish, clock } = await ctx();
    const hooks = await Promise.all([
      d.create({ url: "https://example.test/all", events: ["*"] }),
      d.create({ url: "https://example.test/completed", events: ["flow.completed"] }),
      d.create({ url: "https://example.test/failed", events: ["flow.failed"] }),
      d.create({ url: "https://example.test/jobs-only", events: ["job.completed"] }),
      d.create({ url: "https://example.test/q2", events: ["*"], queues: ["q2"] }),
      d.create({ url: "https://example.test/elsewhere", events: ["*"], queues: ["elsewhere"] }),
    ]);
    const flow = await conveyor.enqueueFlow({ jobs: [node("a"), node("b", ["a"], { queue: "q2" })] });
    await finish();
    clock.advance(5);
    await finish("complete", "q2");
    clock.advance(5);

    expect(await d.deliverFlowOutcomes()).toBe(1);
    expect(await d.deliverFlowOutcomes()).toBe(0); // claimed: never delivered again
    // "*", flow.completed, and the hook for q2 (one of the flow's queues).
    expect(calls).toHaveLength(3);
    const [first] = calls;
    const jobs = (await conveyor.getFlow(flow.id)).jobs;
    expect(first!.body).toEqual({
      id: expect.stringMatching(/^[0-9a-f-]{36}$/),
      event: "flow.completed",
      createdAt: new Date(clock.now()).toISOString(),
      data: {
        id: flow.id,
        reason: null,
        jobId: null,
        at: new Date(clock.now() - 5).toISOString(),
        status: "completed",
        counts: expect.objectContaining({ completed: 2 }),
        jobs: jobs.map((j) => ({ id: j.id, key: j.flowKey, queue: j.queue, name: j.name, state: "completed" })),
      },
    });
    const byHook = new Map<string, number>();
    for (const h of hooks) byHook.set(h.url, (await d.deliveries(h.id)).length);
    expect(Object.fromEntries(byHook)).toEqual({
      "https://example.test/all": 1,
      "https://example.test/completed": 1,
      "https://example.test/failed": 0,
      "https://example.test/jobs-only": 0,
      "https://example.test/q2": 1,
      "https://example.test/elsewhere": 0,
    });
    expect((await d.deliveries(hooks[0]!.id))[0]).toMatchObject({ event: "flow.completed", status: "succeeded", attempts: 1 });
    expect((await conveyor.getFlow(flow.id)).outcome?.dispatchedAt).toBe(clock.now());
  });

  it("claims a verdict even when no webhook wants it, so it isn't looked at again", async () => {
    const { conveyor, d, calls, finish } = await ctx();
    const flow = await conveyor.enqueueFlow({ jobs: [node("a")] });
    await finish("die");
    expect(await d.deliverFlowOutcomes()).toBe(1);
    expect(calls).toEqual([]);
    expect(await conveyor.store.listPendingFlowOutcomes(10)).toEqual([]);
    expect((await conveyor.getFlow(flow.id)).outcome).toMatchObject({ event: "flow.failed", dispatchedAt: expect.any(Number) });
  });

  it("a second dispatcher on the same database doesn't deliver a verdict the first claimed", async () => {
    const { conveyor, d, calls, finish } = await ctx();
    await d.create({ url: "https://example.test/all", events: ["*"] });
    await conveyor.enqueueFlow({ jobs: [node("a")] });
    await finish();
    const otherCalls: unknown[] = [];
    const other = new WebhookDispatcher(conveyor, async () => (otherCalls.push(1), new Response("ok")));
    const results = await Promise.all([d.deliverFlowOutcomes(), other.deliverFlowOutcomes()]);
    expect(results.sort()).toEqual([0, 1]);
    expect(calls.length + otherCalls.length).toBe(1);
  });

  it("retries a failed delivery like any other", async () => {
    vi.useFakeTimers();
    const { conveyor, d, finish } = await ctx();
    const hook = await d.create({ url: "https://example.test/all", events: ["flow.completed"] });
    const flaky = new WebhookDispatcher(conveyor, async () => new Response("down", { status: 503 }));
    await conveyor.enqueueFlow({ jobs: [node("a")] });
    await finish();
    expect(await flaky.deliverFlowOutcomes()).toBe(1);
    expect((await d.deliveries(hook.id))[0]).toMatchObject({ status: "pending", attempts: 1, lastStatusCode: 503 });
    await flaky.stop();
  });

  it("once started, delivers verdicts recorded here at once, and others at the next poll", async () => {
    vi.useFakeTimers();
    const { conveyor, d, calls, finish } = await ctx();
    await d.create({ url: "https://example.test/all", events: ["flow.completed", "flow.failed"] });
    d.start();
    await vi.advanceTimersByTimeAsync(0);
    await conveyor.enqueueFlow({ jobs: [node("a")] });
    await finish(); // recorded in this process: the event wakes the dispatcher
    await vi.advanceTimersByTimeAsync(0);
    expect(calls.map((c) => c.event)).toEqual(["flow.completed"]);

    // Recorded by "another process": a Conveyor with its own event bus on the same store.
    const elsewhere = new Conveyor({ store: conveyor.store, clock: conveyor.clock });
    const flow = await elsewhere.enqueueFlow({ jobs: [node("b")] });
    const lease = (await new Broker(elsewhere).claim(["q"], "w"))!;
    await new Broker(elsewhere).fail(lease.job.id, lease.token, "boom", { unrecoverable: true });
    await vi.advanceTimersByTimeAsync(0);
    expect(calls).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(calls.map((c) => [c.event, c.body.data.id])).toEqual([
      ["flow.completed", expect.any(String)],
      ["flow.failed", flow.id],
    ]);
    await d.stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a verdict recorded while a delivery is in flight is delivered right after it", async () => {
    vi.useFakeTimers();
    const { conveyor, broker } = await ctx();
    const calls: string[] = [];
    let release!: () => void;
    const slow = new Promise<void>((r) => (release = r));
    const d = new WebhookDispatcher(conveyor, async (_url, init) => {
      calls.push(JSON.parse(String(init.body)).data.id);
      if (calls.length === 1) await slow;
      return new Response("ok");
    });
    await d.create({ url: "https://example.test/flows", events: ["flow.completed", "flow.failed"] });
    d.start();
    const first = await conveyor.enqueueFlow({ jobs: [node("a")] });
    const second = await conveyor.enqueueFlow({ jobs: [node("b", undefined, { queue: "q2" })] });
    const finishIn = async (queue: string) => {
      const lease = (await broker.claim([queue], "w"))!;
      await broker.complete(lease.job.id, lease.token);
    };
    await finishIn("q");
    await vi.advanceTimersByTimeAsync(0);
    expect(calls).toEqual([first.id]); // and still waiting for the response
    await finishIn("q2"); // wakes the dispatcher while it is busy
    await vi.advanceTimersByTimeAsync(0);
    expect(calls).toEqual([first.id]);
    release();
    await vi.advanceTimersByTimeAsync(0);
    expect(calls).toEqual([first.id, second.id]); // without waiting for the next poll
    await d.stop();
  });
});
