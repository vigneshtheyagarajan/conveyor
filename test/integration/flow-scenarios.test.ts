// Whole-flow scenarios, stepped by hand through the broker (ManualClock, no workers):
// cancelling part of a running flow, and retrying a dead job in the middle of one.
import { describe, expect, it } from "vitest";
import { LeaseLostError } from "../../src/core/errors.js";
import type { EventName } from "../../src/core/events.js";
import type { Flow, FlowJobInput, Job } from "../../src/core/types.js";
import { manualClock, setup } from "../helpers.js";

const node = (key: string, dependsOn?: FlowJobInput["dependsOn"], extra: Partial<FlowJobInput> = {}): FlowJobInput => ({
  key,
  queue: "q",
  name: key,
  ...(dependsOn ? { dependsOn } : {}),
  ...extra,
});

async function ctx() {
  const clock = manualClock();
  const s = await setup({ clock });
  const events: [EventName, string][] = [];
  s.conveyor.events.onAny((event, first) => events.push([event, (first as Job).id ?? (first as { flowId: string }).flowId]));
  const leases = new Map<string, string>();
  /** Claim the job with this key (it must be the next one in its queue), leaving it running. */
  const start = async (flow: Flow, key: string, queue = "q") => {
    const lease = (await s.broker.claim([queue], "w"))!;
    expect(lease?.job.flowKey ?? lease?.job.name).toBe(key);
    leases.set(key, lease.token);
    return lease.job;
  };
  const byKey = (flow: Flow, key: string) => flow.jobs.find((j) => j.flowKey === key)!;
  const finish = async (flow: Flow, key: string, how: "complete" | "die" = "complete") => {
    const id = byKey(flow, key).id;
    if (how === "complete") await s.broker.complete(id, leases.get(key)!, { key });
    else await s.broker.fail(id, leases.get(key)!, "boom", { unrecoverable: true });
  };
  /** Run every job that is ready in these queues, until none is. */
  const drain = async (queues = ["q"]) => {
    const ran: string[] = [];
    for (let l = await s.broker.claim(queues, "w"); l; l = await s.broker.claim(queues, "w")) {
      ran.push(l.job.flowKey ?? l.job.name);
      await s.broker.complete(l.job.id, l.token, { key: l.job.flowKey });
    }
    return ran;
  };
  const states = async (flow: Flow) => Object.fromEntries((await s.conveyor.getFlow(flow.id)).jobs.map((j) => [j.flowKey, j.state]));
  return { ...s, clock, events, start, finish, drain, states, byKey };
}

describe("cancelling in the middle of a running flow", () => {
  // fetch ─┬─ parseA ─┬─ indexA ── reportA ─┐
  //        │          ├─ auditA (ignore)     ├─ final (continue on indexB)
  //        │          └─ notesA (continue)   │
  //        └─ parseB ─── indexB ─────────────┘
  const FLOW = {
    jobs: [
      node("fetch"),
      node("parseA", ["fetch"]),
      node("parseB", ["fetch"]),
      node("indexA", ["parseA"]),
      node("auditA", [{ key: "parseA", onFailure: "ignore" }]),
      node("notesA", [{ key: "parseA", onFailure: "continue" }]),
      node("reportA", ["indexA"]),
      node("indexB", ["parseB"]),
      node("final", ["reportA", { key: "indexB", onFailure: "continue" }]),
    ],
  };

  it("cancelling one running job cancels exactly the jobs that can no longer run", async () => {
    const { conveyor, events, start, finish, drain, states, byKey } = await ctx();
    const flow = await conveyor.enqueueFlow(FLOW);
    await start(flow, "fetch");
    await finish(flow, "fetch");
    const parseA = await start(flow, "parseA"); // running when it is cancelled
    await start(flow, "parseB");
    await finish(flow, "parseB");

    // Jobs outside the flow that depend on parts of it.
    const onParseAFail = await conveyor.enqueue({ queue: "out", name: "x", dependsOn: [parseA.id] });
    const onParseAIgnore = await conveyor.enqueue({ queue: "out", name: "x", dependsOn: [{ jobId: parseA.id, onFailure: "ignore" }] });
    const onReportAContinue = await conveyor.enqueue({ queue: "out", name: "x", dependsOn: [{ jobId: byKey(flow, "reportA").id, onFailure: "continue" }] });
    const onParseB = await conveyor.enqueue({ queue: "out", name: "x", dependsOn: [byKey(flow, "parseB").id] });
    events.length = 0;

    await conveyor.cancel(parseA.id);
    expect(await states(flow)).toEqual({
      fetch: "completed",
      parseA: "cancelled",
      parseB: "completed",
      indexA: "cancelled", // fail on parseA
      auditA: "waiting", // ignore: runs
      notesA: "waiting", // continue: runs
      reportA: "cancelled", // fail on indexA
      indexB: "waiting", // not downstream of parseA
      final: "cancelled", // fail on reportA
    });
    for (const [key, by] of [
      ["indexA", "parseA"],
      ["reportA", "indexA"],
      ["final", "reportA"],
    ]) {
      expect(byKey(await conveyor.getFlow(flow.id), key!).terminatedBy).toBe(byKey(flow, by!).id);
    }
    expect((await conveyor.getJob(onParseAFail.id)).state).toBe("cancelled");
    expect((await conveyor.getJob(onParseAIgnore.id)).state).toBe("waiting");
    expect((await conveyor.getJob(onReportAContinue.id)).state).toBe("waiting");
    expect((await conveyor.getJob(onParseB.id)).state).toBe("waiting"); // already released, untouched
    // One verdict, at once.
    expect(events.filter(([e]) => e.startsWith("flow."))).toEqual([["flow.failed", flow.id]]);
    expect((await conveyor.getFlow(flow.id)).outcome).toMatchObject({ reason: "cancelled", jobId: parseA.id });

    // The worker that was running parseA finds out when it reports back.
    await expect(finish(flow, "parseA")).rejects.toThrow(LeaseLostError);
    // Everything that can still run does, and nothing else.
    expect((await drain()).sort()).toEqual(["auditA", "indexB", "notesA"]);
    expect(await drain(["out"])).toHaveLength(3);
    const final = await conveyor.getFlow(flow.id);
    expect(final.status).toBe("cancelled");
    expect(final.outcome).toMatchObject({ event: "flow.failed", reason: "cancelled" });
    expect(events.filter(([e]) => e.startsWith("flow."))).toHaveLength(1);
    // notesA saw parseA as cancelled; auditA didn't see it at all.
    const seen = async (key: string) => (await conveyor.dependencyResults(byKey(flow, key))).map((d) => [d.key, d.state]);
    expect(await seen("notesA")).toEqual([["parseA", "cancelled"]]);
    expect((await conveyor.dependencyResultsPage(byKey(flow, "auditA").id)).items).toEqual([]);
  });

  it("cancelling the whole flow while jobs run: jobs outside it follow their own policy", async () => {
    const { conveyor, start, finish, drain, byKey } = await ctx();
    const flow = await conveyor.enqueueFlow(FLOW);
    await start(flow, "fetch");
    await finish(flow, "fetch");
    await start(flow, "parseA");
    await start(flow, "parseB");
    const target = byKey(flow, "indexB").id;
    const outside = {
      fail: await conveyor.enqueue({ queue: "out", name: "fail", dependsOn: [target] }),
      ignore: await conveyor.enqueue({ queue: "out", name: "ignore", dependsOn: [{ jobId: target, onFailure: "ignore" }] }),
      continue: await conveyor.enqueue({ queue: "out", name: "continue", dependsOn: [{ jobId: target, onFailure: "continue" }] }),
      onCompleted: await conveyor.enqueue({ queue: "out", name: "done", dependsOn: [byKey(flow, "fetch").id] }),
    };
    const cancelled = await conveyor.cancelFlow(flow.id);
    expect(cancelled.jobs.filter((j) => j.state !== "cancelled").map((j) => j.flowKey)).toEqual(["fetch"]);
    // Cancelled directly, each of them: no job of the flow names another as the reason.
    expect(cancelled.jobs.every((j) => j.terminatedBy === null)).toBe(true);
    const state = async (j: Job) => (await conveyor.getJob(j.id)).state;
    expect({
      fail: await state(outside.fail),
      ignore: await state(outside.ignore),
      continue: await state(outside.continue),
      onCompleted: await state(outside.onCompleted),
    }).toEqual({ fail: "cancelled", ignore: "waiting", continue: "waiting", onCompleted: "waiting" });
    expect((await drain(["out"])).sort()).toEqual(["continue", "done", "ignore"]);
    expect(await drain()).toEqual([]);
  });
});

describe("retrying a dead job in the middle of a flow", () => {
  it("brings its cancelled descendants back, runs them, and keeps the verdict already recorded", async () => {
    const { conveyor, events, start, finish, drain, states, byKey, clock } = await ctx();
    //  a ── b ── c ── d
    //       └─ e (continue)
    //  f
    const flow = await conveyor.enqueueFlow({
      jobs: [node("a"), node("b", ["a"], { maxAttempts: 1 }), node("c", ["b"]), node("d", ["c"]), node("e", [{ key: "b", onFailure: "continue" }]), node("f")],
    });
    await start(flow, "a");
    await finish(flow, "a");
    clock.advance(1000);
    await start(flow, "b"); // created before f (dependency order), so claimed first
    await finish(flow, "b", "die");
    await start(flow, "f");
    await finish(flow, "f");
    expect(await states(flow)).toEqual({ a: "completed", b: "dead", c: "cancelled", d: "cancelled", e: "waiting", f: "completed" });
    const verdict = (await conveyor.getFlow(flow.id)).outcome;
    expect(verdict).toMatchObject({ event: "flow.failed", reason: "dead", jobId: byKey(flow, "b").id, at: clock.now() });

    clock.advance(5000);
    events.length = 0;
    await conveyor.retry(byKey(flow, "b").id);
    expect(await states(flow)).toEqual({ a: "completed", b: "waiting", c: "blocked", d: "blocked", e: "waiting", f: "completed" });
    for (const key of ["c", "d"]) expect(byKey(await conveyor.getFlow(flow.id), key)).toMatchObject({ terminatedBy: null, lastError: null });
    expect(events).toEqual([
      ["job.enqueued", byKey(flow, "b").id],
      ["job.enqueued", byKey(flow, "c").id],
      ["job.enqueued", byKey(flow, "d").id],
    ]);

    // e was released when b died, before b's retry, so it runs first; then b, c, d in order.
    expect(await drain()).toEqual(["e", "b", "c", "d"]);
    const done = await conveyor.getFlow(flow.id);
    expect(done.status).toBe("completed");
    // The verdict was recorded when b died and stays: no second one, no flow event.
    expect(done.outcome).toEqual(verdict);
    expect(events.filter(([e]) => e.startsWith("flow."))).toEqual([]);
  });
});
