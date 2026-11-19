import { afterEach, describe, expect, it, vi } from "vitest";
import { Maintenance } from "../../src/broker/maintenance.js";
import type { Job } from "../../src/core/types.js";
import { manualClock, memoryLogger, setup } from "../helpers.js";

const DAY = 24 * 60 * 60 * 1000;

describe("reaper", () => {
  // A lease expiry is a failed attempt: the job waits out its normal retry backoff, as if the
  // handler had thrown, instead of going straight back to the queue.
  it("counts an expired lease as a failed attempt and retries it after the backoff", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock }); // leaseMs 1000
    const seen: string[] = [];
    conveyor.events.on("job.reaped", (j) => seen.push(`reaped ${j.state}`));
    conveyor.events.on("job.failed", (j, err) => seen.push(`failed ${j.state}: ${err}`));
    conveyor.events.on("job.retrying", (j, _err, delay) => seen.push(`retrying in ${delay}`));
    const m = new Maintenance(conveyor, { random: () => 0.5 });
    const job = await conveyor.enqueue({ queue: "q", name: "x" }); // default backoff: exponential from 1s, jitter
    const lease = await broker.claim(["q"], "crashed-worker");

    clock.advance(1000); // expires exactly now: not yet
    expect(await m.reap()).toBe(0);
    clock.advance(1);
    expect(await m.reap()).toBe(1);

    const error = "lease expired; worker crashed-worker may have crashed or stalled";
    const delay = 750; // attempt 1: 1000ms, jittered with random 0.5
    expect(await conveyor.getJob(job.id)).toMatchObject({
      state: "delayed",
      attempts: 1,
      runAt: clock.now() + delay,
      lastError: error,
      workerId: null,
      leaseToken: null,
      leaseExpiresAt: null,
    });
    expect((await conveyor.jobEvents(job.id)).at(-1)).toMatchObject({
      type: "lease_expired",
      data: { workerId: "crashed-worker", attempts: 1, outcome: "retry", delayMs: delay },
    });
    expect(seen).toEqual(["reaped delayed", `failed delayed: ${error}`, `retrying in ${delay}`]);
    await expect(broker.complete(job.id, lease!.token)).rejects.toThrow("no longer held");

    expect(await broker.claim(["q"], "w2")).toBeUndefined(); // not before the backoff
    clock.advance(delay);
    expect(await m.promote()).toBe(1);
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

  it("logs each expired lease: a warning when it is retried, an error when the job is dead", async () => {
    const clock = manualClock();
    const { logger, records } = memoryLogger();
    const { conveyor, broker } = await setup({ clock, logger });
    const m = new Maintenance(conveyor, { random: () => 0.5 });
    const retried = await conveyor.enqueue({ queue: "q", name: "x", backoff: { type: "fixed", delayMs: 100 } });
    const dead = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
    await broker.claim(["q"], "w9");
    await broker.claim(["q"], "w8");
    clock.advance(5000);
    expect(await m.reap()).toBe(2);
    expect(records).toContainEqual(
      expect.objectContaining({ level: "warn", msg: "lease expired, retry scheduled", jobId: retried.id, workerId: "w9", retryInMs: 100, component: "maintenance" }),
    );
    expect(records).toContainEqual(
      expect.objectContaining({ level: "error", msg: "lease expired, job is dead", jobId: dead.id, workerId: "w8", component: "maintenance" }),
    );
  });

  // Regression (was KB-1): the reaper requeued every expired lease without looking at
  // maxAttempts, so a job that crashed its worker every time was retried forever.
  it("moves a job to dead when the lease of its last attempt expires", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const seen: string[] = [];
    conveyor.events.on("job.failed", (j) => seen.push(`failed ${j.state}`));
    conveyor.events.on("job.dead", (j, err) => seen.push(`dead: ${err}`));
    const m = new Maintenance(conveyor, { random: () => 0.5 });
    const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 2, backoff: { type: "fixed", delayMs: 5000 } });
    for (let i = 0; i < 2; i++) {
      expect(await broker.claim(["q"], "w")).toBeDefined();
      clock.advance(5000); // the lease (1000ms) expires
      expect(await m.reap()).toBe(1);
      clock.advance(5000); // the retry backoff
      await m.promote();
    }
    const error = "lease expired 2 times; worker may be crashing";
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "dead", attempts: 2, lastError: error, leaseToken: null, finishedAt: clock.now() - 5000 });
    expect(await broker.claim(["q"], "w")).toBeUndefined();
    expect(seen).toEqual(["failed delayed", "failed dead", `dead: ${error}`]);
    expect((await conveyor.jobEvents(job.id)).at(-1)).toMatchObject({ type: "lease_expired", data: { attempts: 2, outcome: "dead" } });
  });

  it("says 'once' when a single expired lease used up the job's only attempt", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
    await broker.claim(["q"], "w");
    clock.advance(5000);
    await new Maintenance(conveyor).reap();
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "dead", lastError: "lease expired once; worker may be crashing" });
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
    // Reaped: a failed attempt, retried after its backoff.
    expect(await conveyor.getJob(lease!.job.id)).toMatchObject({ state: "delayed", lastError: expect.stringContaining("lease expired") });
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
