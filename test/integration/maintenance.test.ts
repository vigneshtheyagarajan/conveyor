import { afterEach, describe, expect, it, vi } from "vitest";
import { Maintenance } from "../../src/broker/maintenance.js";
import { ConflictError } from "../../src/core/errors.js";
import type { Job } from "../../src/core/types.js";
import { manualClock, memoryLogger, setup } from "../helpers.js";

const DAY = 24 * 60 * 60 * 1000;

describe("reaper", () => {
  it("requeues jobs whose lease expired, keeping the attempt they used", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock }); // leaseMs 1000
    const reaped: Job[] = [];
    conveyor.events.on("job.reaped", (j) => reaped.push(j));
    const m = new Maintenance(conveyor);
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "crashed-worker");

    clock.advance(1000); // expires exactly now: not yet
    expect(await m.reap()).toBe(0);
    clock.advance(1);
    expect(await m.reap()).toBe(1);

    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "waiting", attempts: 1, workerId: null, leaseToken: null, runAt: clock.now() });
    expect((await conveyor.jobEvents(job.id)).at(-1)).toMatchObject({ type: "lease_expired", data: { workerId: "crashed-worker" } });
    expect(reaped.map((j) => j.id)).toEqual([job.id]);
    await expect(broker.complete(job.id, lease!.token)).rejects.toThrow("no longer held");
    expect((await broker.claim(["q"], "w2"))?.job.attempts).toBe(2);
  });

  it("leaves jobs with a live lease alone, including ones kept alive by heartbeats", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const m = new Maintenance(conveyor);
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    for (let i = 0; i < 5; i++) {
      clock.advance(800);
      await broker.heartbeat(job.id, lease!.token);
      expect(await m.reap()).toBe(0);
    }
    expect((await conveyor.getJob(job.id)).state).toBe("active");
  });

  it("reaps at most batchSize jobs per pass", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const m = new Maintenance(conveyor, { batchSize: 2 });
    for (let i = 0; i < 3; i++) {
      await conveyor.enqueue({ queue: "q", name: "x" });
      await broker.claim(["q"], "w");
    }
    clock.advance(5000);
    expect(await m.reap()).toBe(2);
    expect(await m.reap()).toBe(1);
    expect(await m.reap()).toBe(0);
  });

  it("logs each requeue as a warning", async () => {
    const clock = manualClock();
    const { logger, records } = memoryLogger();
    const { conveyor, broker } = await setup({ clock, logger });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    await broker.claim(["q"], "w9");
    clock.advance(5000);
    await new Maintenance(conveyor).reap();
    expect(records).toContainEqual(
      expect.objectContaining({ level: "warn", msg: "lease expired, job requeued", jobId: job.id, workerId: "w9", component: "maintenance" }),
    );
  });

  // BUG(KB-1): the reaper requeues every expired lease without looking at maxAttempts, so a job
  // that crashes its worker every time is retried forever. It should go to dead.
  it.fails("moves a job with no attempts left to dead instead of requeueing it [KB-1]", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const m = new Maintenance(conveyor);
    const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 2 });
    for (let i = 0; i < 2; i++) {
      expect(await broker.claim(["q"], "w")).toBeDefined();
      clock.advance(5000);
      await m.reap();
    }
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "dead", attempts: 2 });
    expect(await broker.claim(["q"], "w")).toBeUndefined();
  });
});

describe("promoter", () => {
  it("moves due delayed jobs to waiting", async () => {
    const clock = manualClock();
    const { conveyor } = await setup({ clock });
    const m = new Maintenance(conveyor);
    const soon = await conveyor.enqueue({ queue: "q", name: "x", delayMs: 100 });
    const later = await conveyor.enqueue({ queue: "q", name: "x", delayMs: 10_000 });
    clock.advance(100);
    expect(await m.promote()).toBe(1);
    expect((await conveyor.getJob(soon.id)).state).toBe("waiting");
    expect((await conveyor.getJob(later.id)).state).toBe("delayed");
  });
});

describe("janitor (retention)", () => {
  async function finishedJobs(retentionMs: number) {
    const clock = manualClock();
    const { logger, records } = memoryLogger();
    const { conveyor, broker } = await setup({ clock, logger, config: { retentionMs } });
    const make = async (finish: (id: string, token: string) => Promise<unknown>) => {
      const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
      const lease = await broker.claim(["q"], "w");
      await finish(job.id, lease!.token);
      return job.id;
    };
    const completed = await make((id, t) => broker.complete(id, t));
    const dead = await make((id, t) => broker.fail(id, t, "boom"));
    const cancelled = await make((id) => conveyor.cancel(id));
    return { clock, conveyor, records, completed, dead, cancelled, m: new Maintenance(conveyor) };
  }

  it("deletes completed and cancelled jobs past the retention period and keeps dead ones", async () => {
    const { clock, conveyor, records, completed, dead, cancelled, m } = await finishedJobs(7 * DAY);
    clock.advance(7 * DAY);
    expect(await m.cleanup()).toBe(0); // exactly at the cutoff: kept
    clock.advance(1);
    expect(await m.cleanup()).toBe(2);
    await expect(conveyor.getJob(completed)).rejects.toThrow("not found");
    await expect(conveyor.getJob(cancelled)).rejects.toThrow("not found");
    expect((await conveyor.getJob(dead)).state).toBe("dead");
    expect(records).toContainEqual(expect.objectContaining({ level: "info", msg: "deleted finished jobs", deleted: 2 }));
  });

  // A dependent reads its dependencies' results when it runs, so a finished job must stay
  // while anything that depends on it is unfinished. After that, normal retention applies.
  it("keeps a finished job while a job that depends on it is unfinished", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock, config: { retentionMs: DAY } });
    const m = new Maintenance(conveyor);
    const finish = async (queue: string) => {
      const lease = (await broker.claim([queue], "w"))!;
      await broker.complete(lease.job.id, lease.token, { from: lease.job.id });
      return lease.job.id;
    };
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    const slow = await conveyor.enqueue({ queue: "slow", name: "x" });
    const child = await conveyor.enqueue({ queue: "kids", name: "x", dependsOn: [parent.id, slow.id] });
    await finish("parents");

    clock.advance(3 * DAY); // the parent is well past retention, but the child is still blocked on `slow`
    expect(await m.cleanup()).toBe(0);
    expect((await conveyor.getJob(parent.id)).state).toBe("completed");

    await finish("slow"); // the child is released; `slow` is fresh, the parent still protected
    expect((await conveyor.getJob(child.id)).state).toBe("waiting");
    expect(await m.cleanup()).toBe(0);
    const running = (await broker.claim(["kids"], "w"))!;
    clock.advance(DAY);
    expect(await m.cleanup()).toBe(0); // still protected while the child runs

    await broker.complete(running.job.id, running.token);
    // Every dependent has finished: normal retention applies. The parent finished long ago and
    // goes now; `slow` (finished a day ago) goes once it is past retention, and the child after
    // its own retention period.
    expect(await m.cleanup()).toBe(1);
    await expect(conveyor.getJob(parent.id)).rejects.toThrow("not found");
    clock.advance(1);
    expect(await m.cleanup()).toBe(1);
    await expect(conveyor.getJob(slow.id)).rejects.toThrow("not found");
    expect((await conveyor.getJob(child.id)).state).toBe("completed");
    clock.advance(DAY);
    expect(await m.cleanup()).toBe(1);
    await expect(conveyor.getJob(child.id)).rejects.toThrow("not found");
  });

  it("keeps a cancelled job while a job that depends on it with ignore or continue is unfinished", async () => {
    const clock = manualClock();
    const { conveyor } = await setup({ clock, config: { retentionMs: DAY } });
    const parent = await conveyor.enqueue({ queue: "q", name: "x" });
    const other = await conveyor.enqueue({ queue: "q", name: "x" });
    await conveyor.enqueue({ queue: "kids", name: "x", dependsOn: [{ jobId: parent.id, onFailure: "continue" }, other.id] });
    await conveyor.cancel(parent.id);
    clock.advance(2 * DAY);
    expect(await new Maintenance(conveyor).cleanup()).toBe(0);
    expect((await conveyor.getJob(parent.id)).state).toBe("cancelled");
  });

  // A finished job keeps nothing alive, so retention may delete its dependencies. A retry must
  // then refuse, naming the missing job, rather than run without it.
  it("refuses to retry a dead job whose dependency retention deleted", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock, config: { retentionMs: DAY } });
    const kept = await conveyor.enqueue({ queue: "parents", name: "x" });
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    const child = await conveyor.enqueue({ queue: "kids", name: "x", dependsOn: [kept.id, parent.id], maxAttempts: 1 });
    for (const _ of [kept, parent]) {
      const p = (await broker.claim(["parents"], "w"))!;
      await broker.complete(p.job.id, p.token, "result");
    }
    const c = (await broker.claim(["kids"], "w"))!;
    await broker.fail(child.id, c.token, "boom");
    // `kept` is also needed by a job that is still blocked.
    await conveyor.enqueue({ queue: "other", name: "x", dependsOn: [kept.id, (await conveyor.enqueue({ queue: "other", name: "x" })).id] });

    clock.advance(2 * DAY);
    expect(await new Maintenance(conveyor).cleanup()).toBe(1);
    await expect(conveyor.getJob(parent.id)).rejects.toThrow("not found");

    const before = await conveyor.getJob(child.id);
    await expect(conveyor.retry(child.id)).rejects.toThrow(
      new ConflictError(`job ${child.id} can't be retried: its dependency ${parent.id} no longer exists`),
    );
    expect(await conveyor.getJob(child.id)).toEqual(before);
  });

  it("refuses to retry a job cancelled by its dependency once retention deleted that dependency", async () => {
    const clock = manualClock();
    const { conveyor } = await setup({ clock, config: { retentionMs: DAY } });
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    const child = await conveyor.enqueue({ queue: "kids", name: "x", dependsOn: [parent.id] });
    await conveyor.cancel(parent.id); // cancels the child too
    clock.advance(2 * DAY);
    // Both have finished; delete just the parent, as a cleanup that stops at its batch size would.
    expect(await new Maintenance(conveyor, { batchSize: 1 }).cleanup()).toBe(1);
    await expect(conveyor.getJob(parent.id)).rejects.toThrow("not found");
    await expect(conveyor.retry(child.id)).rejects.toThrow(`its dependency ${parent.id} no longer exists`);
  });

  it("retryDead skips, with the reason, a dead job whose dependency retention deleted, and retries the rest", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock, config: { retentionMs: DAY } });
    const parent = await conveyor.enqueue({ queue: "parents", name: "x" });
    const child = await conveyor.enqueue({ queue: "kids", name: "x", dependsOn: [parent.id], maxAttempts: 1 });
    const plain = await conveyor.enqueue({ queue: "kids", name: "y", maxAttempts: 1 });
    const p = (await broker.claim(["parents"], "w"))!;
    await broker.complete(parent.id, p.token);
    for (let i = 0; i < 2; i++) {
      const l = (await broker.claim(["kids"], "w"))!;
      await broker.fail(l.job.id, l.token, "boom");
    }
    clock.advance(2 * DAY);
    await new Maintenance(conveyor).cleanup();
    expect(await conveyor.retryDead("kids")).toEqual({
      retried: 1,
      skipped: [{ jobId: child.id, error: `job ${child.id} can't be retried: its dependency ${parent.id} no longer exists` }],
    });
    expect((await conveyor.getJob(plain.id)).state).toBe("waiting");
    expect((await conveyor.getJob(child.id)).state).toBe("dead");
    expect(await conveyor.retryAllDead("kids")).toBe(0);
  });

  it("keeps unfinished jobs however old they are", async () => {
    const clock = manualClock();
    const { conveyor } = await setup({ clock, config: { retentionMs: DAY } });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    clock.advance(30 * DAY);
    expect(await new Maintenance(conveyor).cleanup()).toBe(0);
    expect((await conveyor.getJob(job.id)).state).toBe("waiting");
  });
});

// start()/stop() use setInterval, so these use vitest's fake timers for the intervals; job
// times still come from the ManualClock.
describe("Maintenance.start / stop", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("runs the reaper, promoter and janitor on their intervals until stopped", async () => {
    vi.useFakeTimers();
    const { conveyor } = await setup({ clock: manualClock(), config: { reaperIntervalMs: 300, retentionMs: DAY } });
    const m = new Maintenance(conveyor, { janitorIntervalMs: 1000 });
    const reap = vi.spyOn(m, "reap");
    const promote = vi.spyOn(m, "promote");
    const cleanup = vi.spyOn(m, "cleanup");
    m.start();
    m.start(); // idempotent
    await vi.advanceTimersByTimeAsync(1000);
    expect(reap).toHaveBeenCalledTimes(3);
    expect(promote).toHaveBeenCalledTimes(2); // every 500ms by default
    expect(cleanup).toHaveBeenCalledTimes(1);

    m.stop();
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(reap).toHaveBeenCalledTimes(3);
  });

  it("does not run the janitor when retention is disabled", async () => {
    vi.useFakeTimers();
    const { conveyor } = await setup({ clock: manualClock(), config: { retentionMs: 0 } });
    const m = new Maintenance(conveyor, { janitorIntervalMs: 10 });
    const cleanup = vi.spyOn(m, "cleanup");
    m.start();
    await vi.advanceTimersByTimeAsync(1000);
    expect(cleanup).not.toHaveBeenCalled();
    m.stop();
  });

  it("promotes and reaps in the background", async () => {
    vi.useFakeTimers();
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const m = new Maintenance(conveyor, { reaperIntervalMs: 100, promoteIntervalMs: 100 });
    const delayed = await conveyor.enqueue({ queue: "q", name: "x", delayMs: 50 });
    await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    m.start();
    clock.advance(5000);
    await vi.advanceTimersByTimeAsync(100);
    m.stop();
    expect((await conveyor.getJob(delayed.id)).state).toBe("waiting");
    expect((await conveyor.getJob(lease!.job.id)).state).toBe("waiting");
  });

  it("logs a failing task and keeps running", async () => {
    vi.useFakeTimers();
    const { logger, records } = memoryLogger();
    const { conveyor } = await setup({ clock: manualClock(), logger, config: { reaperIntervalMs: 100 } });
    const m = new Maintenance(conveyor, { promoteIntervalMs: 10_000 });
    const find = vi.spyOn(conveyor.store, "findExpiredLeases").mockRejectedValueOnce(new Error("db busy"));
    m.start();
    await vi.advanceTimersByTimeAsync(200);
    m.stop();
    expect(find).toHaveBeenCalledTimes(2);
    expect(records).toContainEqual(expect.objectContaining({ level: "error", msg: "maintenance task failed", err: expect.objectContaining({ message: "db busy" }) }));
  });
});
