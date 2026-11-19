// A worker whose lease was taken away while one of its calls was already under way.
//
// Every lease call (complete, fail, release, heartbeat, progress) checks the lease and then
// writes. Another process can change the job in between: the reaper requeues the expired lease
// and a second worker claims the job. The stale call must then fail with LeaseLostError and
// leave the new holder's lease alone.
//
// The interleaving is forced with a store wrapper that runs a hook right after the stale
// broker's first store call returns, before the broker sees the result. If an implementation
// writes with a single conditional statement, the hook runs after that write; the tests still
// hold, because the hook then finds nothing to reap and no second worker gets the job.
import { beforeEach, describe, expect, it } from "vitest";
import { Broker, type Lease } from "../../src/broker/broker.js";
import { Maintenance } from "../../src/broker/maintenance.js";
import { Conveyor } from "../../src/core/conveyor.js";
import { LeaseLostError } from "../../src/core/errors.js";
import type { Job } from "../../src/core/types.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import type { ManualClock } from "../../src/util/clock.js";
import { interruptible, manualClock } from "../helpers.js";

const LEASE_MS = 1000;

let clock: ManualClock;
let conveyor: Conveyor;
let broker: Broker;
let stale: Broker;
let interrupt: ReturnType<typeof interruptible>;

beforeEach(async () => {
  clock = manualClock();
  const store = new SqliteStore(":memory:");
  const config = { leaseMs: LEASE_MS, heartbeatMs: 200 };
  conveyor = new Conveyor({ store, clock, config });
  await conveyor.init();
  broker = new Broker(conveyor, { random: () => 0.5 });
  // The stale worker talks to the same database through the interruptible wrapper.
  interrupt = interruptible(store);
  stale = new Broker(new Conveyor({ store: interrupt.store, clock, config }), { random: () => 0.5 });
});

/**
 * w1 claims a job. Its next call is interrupted after the first store call: w1's lease expires,
 * the reaper requeues the job and w2 claims it. Returns w1's lease and a getter for w2's.
 */
async function staleLeaseScenario(): Promise<{ job: Job; w1: Lease; w2: () => Lease }> {
  // No retry backoff, so the reaped job can be claimed again at once.
  const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 3, backoff: { type: "fixed", delayMs: 0 } });
  const w1 = (await broker.claim(["q"], "w1"))!;
  let w2: Lease | undefined;
  interrupt.afterNextCall(async () => {
    clock.advance(LEASE_MS + 1);
    const reaper = new Maintenance(conveyor);
    await reaper.reap();
    await reaper.promote();
    w2 = await broker.claim(["q"], "w2");
    // Let time pass, so a stale heartbeat would visibly move w2's expiry.
    clock.advance(10);
  });
  return {
    job,
    w1,
    w2: () => {
      expect(interrupt.fired).toBe(1);
      expect(w2).toBeDefined();
      return w2!;
    },
  };
}

type Op = (b: Broker, jobId: string, token: string) => Promise<unknown>;

const ops: [string, Op][] = [
  ["complete", (b, id, t) => b.complete(id, t, { from: "w1" })],
  ["fail (retry)", (b, id, t) => b.fail(id, t, "w1 failed")],
  ["fail (unrecoverable)", (b, id, t) => b.fail(id, t, "w1 gave up", { unrecoverable: true })],
  ["release", (b, id, t) => b.release(id, t)],
  ["heartbeat", (b, id, t) => b.heartbeat(id, t)],
  ["progress", (b, id, t) => b.progress(id, t, 0.5)],
];

describe("a worker whose lease was reaped and re-claimed mid-call", () => {
  it.each(ops)("%s fails with LeaseLostError and leaves the new lease alone", async (_name, op) => {
    const { job, w1, w2 } = await staleLeaseScenario();
    await expect(op(stale, job.id, w1.token)).rejects.toThrow(LeaseLostError);
    const lease = w2();
    expect(await conveyor.getJob(job.id)).toMatchObject({
      state: "active",
      workerId: "w2",
      leaseToken: lease.token,
      leaseExpiresAt: lease.expiresAt,
      attempts: 2,
      progress: 0,
      result: null,
      // The reaper's record of w1's lost lease, not anything w1 wrote.
      lastError: "lease expired; worker w1 may have crashed or stalled",
    });
    // w2 still owns the job and can finish it.
    expect((await broker.complete(job.id, lease.token, { from: "w2" })).result).toEqual({ from: "w2" });
  });
});

describe("a heartbeat that loses to the reaper", () => {
  // Case 6: the heartbeat's write found the job already requeued, changed nothing, and still
  // reported a new expiry, so the worker kept running a job that was back in the queue.
  it("fails with LeaseLostError instead of reporting a new expiry", async () => {
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const w1 = (await broker.claim(["q"], "w1"))!;
    interrupt.afterNextCall(async () => {
      clock.advance(LEASE_MS + 1);
      expect(await new Maintenance(conveyor).reap()).toBe(1);
    });
    await expect(stale.heartbeat(job.id, w1.token)).rejects.toThrow(LeaseLostError);
    expect(interrupt.fired).toBe(1);
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "delayed", leaseToken: null, leaseExpiresAt: null });
  });
});
