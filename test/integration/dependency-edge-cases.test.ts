// Edge cases of dependencies, from the final review: dependencies across queues, on completed,
// delayed and retrying jobs, and on jobs in paused queues; and jobs without dependencies,
// which must behave exactly as before dependencies existed.
import { describe, expect, it } from "vitest";
import { Maintenance } from "../../src/broker/maintenance.js";
import type { EventName } from "../../src/core/events.js";
import type { Job } from "../../src/core/types.js";
import type { SqliteStore } from "../../src/storage/sqlite-store.js";
import { manualClock, setup } from "../helpers.js";

async function ctx() {
  const clock = manualClock();
  const s = await setup({ clock });
  const events: [EventName, string][] = [];
  s.conveyor.events.onAny((event, first) => events.push([event, (first as Job).id ?? (first as { flowId: string }).flowId]));
  const m = new Maintenance(s.conveyor);
  /** Claim the next job in `queue` and complete it (or fail it, to retry). */
  const run = async (queue: string, how: "complete" | "fail" = "complete") => {
    const lease = (await s.broker.claim([queue], "w"))!;
    expect(lease, `a job to claim in ${queue}`).toBeDefined();
    if (how === "complete") await s.broker.complete(lease.job.id, lease.token, { from: queue });
    else await s.broker.fail(lease.job.id, lease.token, "transient");
    return lease.job;
  };
  const state = async (j: Job) => (await s.conveyor.getJob(j.id)).state;
  return { ...s, clock, events, m, run, state };
}

describe("a dependency on a job in another queue", () => {
  it("is released when that job completes, and runs in its own queue with that queue's settings", async () => {
    const { conveyor, broker, run, state } = await ctx();
    await conveyor.updateQueue("kids", { concurrency: 1 });
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    const child = await conveyor.enqueue({ queue: "kids", name: "y", dependsOn: [parent.id] });
    const other = await conveyor.enqueue({ queue: "kids", name: "z" });
    expect(await broker.claim(["kids"], "w")).toMatchObject({ job: { id: other.id } });
    await run("parents");
    expect(await state(child)).toBe("waiting");
    // "kids" allows one job at a time, and `other` is still running.
    expect(await broker.claim(["kids"], "w")).toBeUndefined();
  });
});

describe("a dependency on a job that has already completed", () => {
  it("is released at once and queued like a plain job enqueued at the same moment", async () => {
    const { conveyor, broker, run, events, clock } = await ctx();
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    await run("parents");
    clock.advance(100);
    events.length = 0;
    const child = await conveyor.enqueue({ queue: "q", name: "y", dependsOn: [parent.id] });
    const plain = await conveyor.enqueue({ queue: "q", name: "y" });
    expect(child).toMatchObject({ state: "waiting", runAt: clock.now() });
    expect(events).toEqual([
      ["job.enqueued", child.id],
      ["job.unblocked", child.id],
      ["job.enqueued", plain.id],
    ]);
    expect((await broker.claim(["q"], "w"))!.job.id).toBe(child.id);
    expect((await broker.claim(["q"], "w"))!.job.id).toBe(plain.id);
  });
});

describe("a dependency on a job that is still delayed", () => {
  it("keeps the dependent blocked until the delayed job runs and completes", async () => {
    const { conveyor, run, state, m, clock } = await ctx();
    const parent = await conveyor.enqueue({ queue: "parents", name: "x", delayMs: 10_000 });
    const child = await conveyor.enqueue({ queue: "q", name: "y", dependsOn: [parent.id] });
    expect([parent.state, child.state]).toEqual(["delayed", "blocked"]);
    clock.advance(10_000);
    expect(await m.promote()).toBe(1); // only the parent: a blocked job is never promoted
    expect(await state(child)).toBe("blocked");
    clock.advance(500);
    await run("parents");
    expect(await conveyor.getJob(child.id)).toMatchObject({ state: "waiting", runAt: clock.now() });
  });

  it("a dependent with its own, later delay is released into delayed and promoted then", async () => {
    const { conveyor, run, state, m, clock } = await ctx();
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    const child = await conveyor.enqueue({ queue: "q", name: "y", dependsOn: [parent.id], delayMs: 60_000 });
    await run("parents");
    expect(await conveyor.getJob(child.id)).toMatchObject({ state: "delayed", runAt: child.runAt });
    clock.advance(60_000);
    expect(await m.promote()).toBe(1);
    expect(await state(child)).toBe("waiting");
  });

  it("keeps the dependent blocked while the dependency waits for a retry (delayed by backoff)", async () => {
    const { conveyor, run, state, m, clock } = await ctx();
    const parent = await conveyor.enqueue({ queue: "parents", name: "x", maxAttempts: 2, backoff: { type: "fixed", delayMs: 1000 } });
    const child = await conveyor.enqueue({ queue: "q", name: "y", dependsOn: [parent.id] });
    await run("parents", "fail");
    expect([await state(parent), await state(child)]).toEqual(["delayed", "blocked"]);
    clock.advance(1000);
    await m.promote();
    await run("parents");
    expect(await state(child)).toBe("waiting");
  });
});

describe("a dependency in a paused queue", () => {
  it("keeps the dependent blocked while paused; nothing is stuck once the queue resumes", async () => {
    const { conveyor, broker, run, state, m } = await ctx();
    await conveyor.pause("parents");
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    const child = await conveyor.enqueue({ queue: "q", name: "y", dependsOn: [parent.id] });
    expect(await broker.claim(["parents", "q"], "w")).toBeUndefined();
    expect(await m.reconcile()).toBe(0); // waiting on a job that can still run: not stuck
    expect(await state(child)).toBe("blocked");
    await conveyor.resume("parents");
    await run("parents");
    expect(await state(child)).toBe("waiting");
  });

  it("a dependency that was running when its queue was paused still releases the dependent", async () => {
    const { conveyor, broker, state } = await ctx();
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    const child = await conveyor.enqueue({ queue: "q", name: "y", dependsOn: [parent.id] });
    const lease = (await broker.claim(["parents"], "w"))!;
    await conveyor.pause("parents");
    await broker.complete(parent.id, lease.token);
    expect(await state(child)).toBe("waiting");
  });

  it("a dependency cancelled while its queue is paused applies the dependents' policies", async () => {
    const { conveyor, state } = await ctx();
    await conveyor.pause("parents");
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    const failing = await conveyor.enqueue({ queue: "q", name: "y", dependsOn: [parent.id] });
    const ignoring = await conveyor.enqueue({ queue: "q", name: "y", dependsOn: [{ jobId: parent.id, onFailure: "ignore" }] });
    await conveyor.cancel(parent.id);
    expect([await state(failing), await state(ignoring)]).toEqual(["cancelled", "waiting"]);
  });

  it("a dependent in a paused queue is released to waiting, and claimed once its queue resumes", async () => {
    const { conveyor, broker, run, state } = await ctx();
    await conveyor.pause("q");
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    const child = await conveyor.enqueue({ queue: "q", name: "y", dependsOn: [parent.id] });
    await run("parents");
    expect(await state(child)).toBe("waiting");
    expect(await broker.claim(["q"], "w")).toBeUndefined();
    await conveyor.resume("q");
    expect((await broker.claim(["q"], "w"))!.job.id).toBe(child.id);
  });
});

describe("jobs without dependsOn", () => {
  it("go through their whole life exactly as before: same states, history, events and rows", async () => {
    const { conveyor, broker, events, m, clock, store } = await ctx();
    const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 2, backoff: { type: "fixed", delayMs: 1000 } });
    expect(job).toMatchObject({ state: "waiting", runAt: clock.now(), flowId: null, flowKey: null, terminatedBy: null });
    let lease = (await broker.claim(["q"], "w"))!;
    await broker.fail(job.id, lease.token, "once");
    clock.advance(1000);
    await m.promote();
    lease = (await broker.claim(["q"], "w"))!;
    await broker.complete(job.id, lease.token, { ok: true });
    expect((await conveyor.jobEvents(job.id)).map((e) => [e.type, e.data])).toEqual([
      ["enqueued", { state: "waiting" }],
      ["claimed", { workerId: "w", attempt: 1 }],
      ["retry_scheduled", { error: "once", delayMs: 1000 }],
      ["claimed", { workerId: "w", attempt: 2 }],
      ["completed", null],
    ]);
    expect(events).toEqual([
      ["job.enqueued", job.id],
      ["job.claimed", job.id],
      ["job.failed", job.id],
      ["job.retrying", job.id],
      ["job.claimed", job.id],
      ["job.completed", job.id],
    ]);
    const db = (store as SqliteStore).db;
    expect(db.prepare("SELECT COUNT(*) AS n FROM job_dependencies").get()).toEqual({ n: 0 });
    expect(db.prepare("SELECT COUNT(*) AS n FROM flow_events").get()).toEqual({ n: 0 });
    expect(await m.reconcile()).toBe(0);
  });

  it("cancel, retry and the reaper keep their old history entries and events", async () => {
    const { conveyor, broker, events, m, clock } = await ctx();
    const a = await conveyor.enqueue({ queue: "q", name: "x", delayMs: 500 });
    await conveyor.cancel(a.id);
    const retried = await conveyor.retry(a.id);
    expect(retried).toMatchObject({ state: "waiting", attempts: 0, runAt: clock.now() });
    const lease = (await broker.claim(["q"], "crashy"))!;
    clock.advance(5000);
    expect(await m.reap()).toBe(1);
    await expect(broker.complete(a.id, lease.token)).rejects.toThrow("no longer held");
    expect((await conveyor.jobEvents(a.id)).map((e) => [e.type, e.data])).toEqual([
      ["enqueued", { state: "delayed" }],
      ["cancelled", { previousState: "delayed" }],
      ["retried", { previousState: "cancelled", state: "waiting" }],
      ["claimed", { workerId: "crashy", attempt: 1 }],
      ["lease_expired", { workerId: "crashy" }],
    ]);
    expect(events.map(([e]) => e)).toEqual(["job.enqueued", "job.cancelled", "job.enqueued", "job.claimed", "job.reaped"]);
  });
});
