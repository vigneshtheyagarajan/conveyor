// Races between the reaper, cancel, retry, enqueue, the scheduler and workers.
//
// Each test forces one interleaving with `interruptible` (test/helpers.ts): the operation under
// test runs against a wrapped store, and right after its first store call returns, something
// else changes the job. The assertions state what must hold whichever way the two land:
// exactly one of them wins, and the loser changes nothing.
import { beforeEach, describe, expect, it } from "vitest";
import { Broker, type Lease } from "../../src/broker/broker.js";
import { Maintenance } from "../../src/broker/maintenance.js";
import { Conveyor } from "../../src/core/conveyor.js";
import { ConflictError, LeaseLostError } from "../../src/core/errors.js";
import type { Job } from "../../src/core/types.js";
import { Scheduler } from "../../src/scheduler/scheduler.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import type { ManualClock } from "../../src/util/clock.js";
import { interruptible, manualClock, memoryLogger } from "../helpers.js";

const LEASE_MS = 1000;
const config = { leaseMs: LEASE_MS, heartbeatMs: 200 };
const random = () => 0.5;

let clock: ManualClock;
let sqlite: SqliteStore;
let conveyor: Conveyor;
let broker: Broker;
let reaper: Maintenance;
let interrupt: ReturnType<typeof interruptible>;
/** Same database, through the interruptible store. */
let stale: { conveyor: Conveyor; reaper: Maintenance; records: Record<string, unknown>[]; reaped: Job[] };

beforeEach(async () => {
  clock = manualClock();
  const store = (sqlite = new SqliteStore(":memory:"));
  conveyor = new Conveyor({ store, clock, config });
  await conveyor.init();
  broker = new Broker(conveyor, { random });
  reaper = new Maintenance(conveyor, { random });
  interrupt = interruptible(store);
  const { logger, records } = memoryLogger();
  const staleConveyor = new Conveyor({ store: interrupt.store, clock, config, logger });
  const reaped: Job[] = [];
  staleConveyor.events.on("job.reaped", (j) => reaped.push(j));
  stale = { conveyor: staleConveyor, reaper: new Maintenance(staleConveyor, { random }), records, reaped };
});

const events = async (id: string, type: string) => (await conveyor.jobEvents(id)).filter((e) => e.type === type);

describe("a reaper whose expired lease changed before it wrote", () => {
  /** A job whose lease has just expired. Its retries have no backoff, so they can be claimed at once. */
  async function expiredLease(): Promise<{ job: Job; lease: Lease }> {
    const job = await conveyor.enqueue({ queue: "q", name: "x", backoff: { type: "fixed", delayMs: 0 } });
    const lease = (await broker.claim(["q"], "w1"))!;
    clock.advance(LEASE_MS + 1);
    return { job, lease };
  }

  /** The stale reaper ran, read the lease, and changed, logged and announced nothing. */
  async function staleReaperDidNothing(reaped: number, jobId: string) {
    expect(interrupt.fired).toBe(1);
    expect(reaped).toBe(0);
    expect(stale.reaped).toEqual([]);
    expect(stale.records.filter((r) => r.level === "warn" || r.level === "error")).toEqual([]);
    expect(await events(jobId, "lease_expired")).toEqual([]);
  }

  it("leaves a lease that a heartbeat renewed", async () => {
    const { job, lease } = await expiredLease();
    interrupt.afterNextCall(async () => {
      await broker.heartbeat(job.id, lease.token);
    });
    await staleReaperDidNothing(await stale.reaper.reap(), job.id);
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "active", leaseToken: lease.token, leaseExpiresAt: clock.now() + LEASE_MS });
    await broker.complete(job.id, lease.token); // w1 still holds it
  });

  it("leaves a job that was completed", async () => {
    const { job, lease } = await expiredLease();
    interrupt.afterNextCall(async () => {
      await broker.complete(job.id, lease.token, { ok: 1 });
    });
    await staleReaperDidNothing(await stale.reaper.reap(), job.id);
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "completed", result: { ok: 1 } });
    expect(await broker.claim(["q"], "w2")).toBeUndefined();
  });

  it("leaves a job whose failure scheduled a retry", async () => {
    const { job, lease } = await expiredLease();
    interrupt.afterNextCall(async () => {
      await broker.fail(job.id, lease.token, "boom", { retryInMs: 60_000 });
    });
    await staleReaperDidNothing(await stale.reaper.reap(), job.id);
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "delayed", runAt: clock.now() + 60_000, lastError: "boom" });
  });

  it("leaves a job that was cancelled", async () => {
    const { job } = await expiredLease();
    interrupt.afterNextCall(async () => {
      await conveyor.cancel(job.id);
    });
    await staleReaperDidNothing(await stale.reaper.reap(), job.id);
    expect((await conveyor.getJob(job.id)).state).toBe("cancelled");
  });

  it("leaves a job that another reaper already requeued and a new worker claimed", async () => {
    const { job } = await expiredLease();
    let w2: Lease | undefined;
    interrupt.afterNextCall(async () => {
      expect(await reaper.reap()).toBe(1);
      await reaper.promote();
      w2 = await broker.claim(["q"], "w2");
    });
    expect(await stale.reaper.reap()).toBe(0);
    expect(interrupt.fired).toBe(1);
    expect(w2).toBeDefined();
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "active", workerId: "w2", leaseToken: w2!.token, attempts: 2 });
    expect(await events(job.id, "lease_expired")).toHaveLength(1);
    expect(stale.reaped).toEqual([]);
    expect(await broker.claim(["q"], "w3")).toBeUndefined();
    await broker.complete(job.id, w2!.token);
  });

  it("ends an expired lease once when several reapers run at the same moment", async () => {
    const { job } = await expiredLease();
    const reapers = [1, 2, 3].map(() => new Maintenance(conveyor, { random }));
    const counts = await Promise.all(reapers.map((m) => m.reap()));
    expect(counts.reduce((a, b) => a + b, 0)).toBe(1);
    expect(await events(job.id, "lease_expired")).toHaveLength(1);
  });
});

describe("cancel", () => {
  // Regression (was KB-13, and case 9): cancel checked the state, then wrote by id. A job that
  // finished in between was overwritten: it ended up cancelled although its handler had
  // completed, or although it had died.
  it.each([
    ["completes", "completed", (id: string, token: string) => broker.complete(id, token, { ok: 1 })],
    ["dies", "dead", (id: string, token: string) => broker.fail(id, token, "boom")],
  ] as const)("and a job that %s in between: exactly one of them wins", async (_name, finishedState, finish) => {
    const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
    const lease = (await broker.claim(["q"], "w1"))!;
    let finished: PromiseSettledResult<Job> | undefined;
    interrupt.afterNextCall(async () => {
      [finished] = await Promise.allSettled([finish(job.id, lease.token)]);
    });
    const [cancel] = await Promise.allSettled([stale.conveyor.cancel(job.id)]);
    expect(interrupt.fired).toBe(1);
    const final = await conveyor.getJob(job.id);
    if (finished!.status === "fulfilled") {
      // The job finished first: the cancel must be refused and change nothing.
      expect(cancel!.status).toBe("rejected");
      expect((cancel as PromiseRejectedResult).reason).toBeInstanceOf(ConflictError);
      expect((cancel as PromiseRejectedResult).reason.message).toBe(`job ${job.id} is already ${finishedState}`);
      expect(final.state).toBe(finishedState);
      expect(await events(job.id, "cancelled")).toEqual([]);
    } else {
      // The cancel landed first: finishing must be refused.
      expect((finished as PromiseRejectedResult).reason).toBeInstanceOf(LeaseLostError);
      expect(cancel!.status).toBe("fulfilled");
      expect(final).toMatchObject({ state: "cancelled", result: null, lastError: null });
    }
  });

  it("twice at the same moment: one cancels, the other gets a ConflictError", async () => {
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const results = await Promise.allSettled([conveyor.cancel(job.id), conveyor.cancel(job.id)]);
    expect(results.map((r) => r.status).toSorted()).toEqual(["fulfilled", "rejected"]);
    const refused = results.find((r) => r.status === "rejected") as PromiseRejectedResult;
    expect(refused.reason).toBeInstanceOf(ConflictError);
    expect(refused.reason.message).toBe(`job ${job.id} is already cancelled`);
    expect(await events(job.id, "cancelled")).toHaveLength(1);
  });

  // Case 2 (fixed by the atomic claim): a cancel landing between a claim's read and its write
  // used to be undone by the claim, and cancel() returned a job that was active.
  it.each(["claim first", "cancel first"])("racing a claim (%s) leaves the job cancelled", async (order) => {
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const claim = () => broker.claim(["q"], "w1");
    const cancel = () => conveyor.cancel(job.id);
    const [a, b] = order === "claim first" ? await Promise.all([claim(), cancel()]) : (await Promise.all([cancel(), claim()])).reverse();
    const lease = a as Lease | undefined;
    const cancelled = b as Job;
    expect(cancelled.state).toBe("cancelled");
    expect((await conveyor.getJob(job.id)).state).toBe("cancelled");
    if (lease) await expect(broker.complete(job.id, lease.token)).rejects.toThrow(LeaseLostError);
    expect(await broker.claim(["q"], "w2")).toBeUndefined();
  });
});

describe("retry", () => {
  async function deadJob(): Promise<Job> {
    const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
    const lease = (await broker.claim(["q"], "w1"))!;
    await broker.fail(job.id, lease.token, "boom");
    return job;
  }

  // Case 10: both retries passed the "dead or cancelled" check and both requeued the job.
  it("twice at the same moment: one requeue, the other gets a ConflictError", async () => {
    const job = await deadJob();
    const results = await Promise.allSettled([conveyor.retry(job.id), conveyor.retry(job.id)]);
    expect(results.map((r) => r.status).toSorted()).toEqual(["fulfilled", "rejected"]);
    const refused = results.find((r) => r.status === "rejected") as PromiseRejectedResult;
    expect(refused.reason).toBeInstanceOf(ConflictError);
    expect(refused.reason.message).toBe("only dead or cancelled jobs can be retried (job is waiting)");
    expect(await events(job.id, "retried")).toHaveLength(1);
  });

  it("landing after another retry and a new claim does not requeue the running job", async () => {
    const job = await deadJob();
    let other: PromiseSettledResult<Job> | undefined;
    let w2: Lease | undefined;
    interrupt.afterNextCall(async () => {
      [other] = await Promise.allSettled([conveyor.retry(job.id)]);
      w2 = await broker.claim(["q"], "w2");
    });
    const [mine] = await Promise.allSettled([stale.conveyor.retry(job.id)]);
    expect(interrupt.fired).toBe(1);
    // Exactly one of the two retries requeued the job, and w2 claimed it.
    expect([mine!.status, other!.status].toSorted()).toEqual(["fulfilled", "rejected"]);
    expect(await events(job.id, "retried")).toHaveLength(1);
    expect(w2).toBeDefined();
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "active", workerId: "w2", leaseToken: w2!.token, attempts: 1 });
    expect(await broker.claim(["q"], "w3")).toBeUndefined();
  });

  it("retryAllDead run twice at once retries each dead job once", async () => {
    const jobs = [await deadJob(), await deadJob(), await deadJob()];
    const counts = await Promise.all([conveyor.retryAllDead("q"), conveyor.retryAllDead("q")]);
    expect(counts[0]! + counts[1]!).toBe(3);
    for (const j of jobs) expect(await events(j.id, "retried")).toHaveLength(1);
  });
});

const UNFINISHED = ["delayed", "waiting", "active"];
const unfinishedWithKey = async (key: string) =>
  (await conveyor.listJobs({ queue: "q", limit: 500 })).items.filter((j) => j.uniqueKey === key && UNFINISHED.includes(j.state)).map((j) => j.id);

describe("unique keys", () => {
  // Regression (was KB-14): enqueue looked for an unfinished job with the key in one step and
  // inserted in another, so two enqueues at the same moment both inserted.
  it("two enqueues with the same key at the same moment give one job; the second gets it back", async () => {
    const announced: string[] = [];
    conveyor.events.on("job.enqueued", (j) => announced.push(j.id));
    const [a, b] = await Promise.all([
      conveyor.enqueue({ queue: "q", name: "x", uniqueKey: "k", payload: 1 }),
      conveyor.enqueue({ queue: "q", name: "x", uniqueKey: "k", payload: 2 }),
    ]);
    expect(b).toEqual(a);
    expect(await unfinishedWithKey("k")).toEqual([a.id]);
    expect(announced).toEqual([a.id]);
    expect(await events(a.id, "enqueued")).toHaveLength(1);
  });

  // A single enqueue of "b" is injected after each store call the bulk enqueue makes in turn.
  // Wherever it lands, there is one job per key and both callers get the same job for "b".
  it("a bulk enqueue and an enqueue sharing a key give one job per key, wherever they interleave", async () => {
    for (let k = 0; k < 8; k++) {
      const keys = [`a${k}`, `b${k}`, `c${k}`];
      let single: Job | undefined;
      const enqueueB = async () => {
        single = await conveyor.enqueue({ queue: "q", name: "x", uniqueKey: keys[1] });
      };
      const before = interrupt.fired;
      interrupt.afterNextCall(enqueueB, k);
      const bulk = await stale.conveyor.enqueueBulk(keys.map((key) => ({ queue: "q", name: "x", uniqueKey: key })));
      if (interrupt.fired === before) {
        interrupt.afterNextCall(async () => {}); // disarm
        await enqueueB();
      }
      expect({ k, same: bulk[1]!.id === single!.id }).toEqual({ k, same: true });
      for (const [i, key] of keys.entries()) expect({ k, key, ids: await unfinishedWithKey(key) }).toEqual({ k, key, ids: [bulk[i]!.id] });
    }
  });

  it("retry refuses to revive a job whose key a newer unfinished job holds", async () => {
    const old = await conveyor.enqueue({ queue: "q", name: "x", uniqueKey: "k", maxAttempts: 1 });
    const lease = (await broker.claim(["q"], "w1"))!;
    await broker.fail(old.id, lease.token, "boom");
    const newer = await conveyor.enqueue({ queue: "q", name: "x", uniqueKey: "k" });
    expect(newer.id).not.toBe(old.id);

    await expect(conveyor.retry(old.id)).rejects.toThrow(
      new ConflictError(`job ${old.id} cannot be retried: job ${newer.id} with the same unique key "k" is unfinished`),
    );
    expect(await conveyor.retryAllDead("q")).toBe(0);
    expect((await conveyor.getJob(old.id)).state).toBe("dead");
    expect(await unfinishedWithKey("k")).toEqual([newer.id]);
    expect(await events(old.id, "retried")).toEqual([]);

    // Once the newer job has finished, the old one can be retried.
    await conveyor.cancel(newer.id);
    expect((await conveyor.retry(old.id)).state).toBe("waiting");
    expect(await unfinishedWithKey("k")).toEqual([old.id]);
  });

  // Fixed by step 2 (the reaper checks the lease it read); kept as a regression test. The old
  // reaper requeued a job that completed after its read, next to a newer job with the same key.
  it("the reaper does not revive a finished job whose key a newer job now holds", async () => {
    const job = await conveyor.enqueue({ queue: "q", name: "x", uniqueKey: "k" });
    const lease = (await broker.claim(["q"], "w1"))!;
    clock.advance(LEASE_MS + 1);
    let newer: Job | undefined;
    interrupt.afterNextCall(async () => {
      await broker.complete(job.id, lease.token);
      newer = await conveyor.enqueue({ queue: "q", name: "x", uniqueKey: "k" });
    });
    await stale.reaper.reap();
    expect(interrupt.fired).toBe(1);
    expect((await conveyor.getJob(job.id)).state).toBe("completed");
    expect(await unfinishedWithKey("k")).toEqual([newer!.id]);
  });
});

describe("scheduler", () => {
  let scheduler: Scheduler;
  let staleScheduler: Scheduler;

  beforeEach(() => {
    scheduler = new Scheduler(conveyor);
    staleScheduler = new Scheduler(stale.conveyor);
  });

  /** A schedule whose slot is due now (it fires every 5 minutes, so no other slot comes due). */
  async function dueSchedule(name: string, overlap: "allow" | "skip" = "allow") {
    const s = await scheduler.create({ name, queue: "q", jobName: "x", cron: "*/5 * * * *", overlap, payload: { v: 0 } });
    const slot = clock.now() - 1;
    sqlite.db.prepare("UPDATE schedules SET next_run_at = ? WHERE id = ?").run(slot, s.id);
    return { id: s.id, slot };
  }

  const jobsOf = async (scheduleId: string) => (await conveyor.listJobs({ scheduleId })).items;

  it("two schedulers ticking at the same moment fire a slot once", async () => {
    const { id, slot } = await dueSchedule("twice");
    const [a, b] = await Promise.all([scheduler.tick(), new Scheduler(conveyor).tick()]);
    expect([...a, ...b]).toHaveLength(1);
    expect(await jobsOf(id)).toHaveLength(1);
    expect(await scheduler.get(id)).toMatchObject({ lastRunAt: slot, lastJobId: [...a, ...b][0] });
  });

  // The tick read the schedule, enqueued, and wrote the whole row back, so an edit made in
  // between was lost (a schedule disabled meanwhile came back enabled). The edit is injected
  // after each store call the tick makes in turn.
  it("keeps an edit made at any point during a tick", async () => {
    for (let k = 0; k < 8; k++) {
      const { id } = await dueSchedule(`edit-${k}`);
      const edit = () => scheduler.update(id, { enabled: false, payload: { v: k } }).then(() => undefined);
      const before = interrupt.fired;
      interrupt.afterNextCall(edit, k);
      await staleScheduler.tick();
      if (interrupt.fired === before) {
        interrupt.afterNextCall(async () => {}); // disarm
        await edit();
      }
      expect({ k, ...(await scheduler.get(id)) }).toMatchObject({ k, enabled: false, payload: { v: k } });
    }
  });

  // The other direction: an edit read the schedule, a tick fired the slot, then the edit wrote
  // the whole row back, putting next_run_at back on the slot that had just fired.
  it("an edit that overlaps a tick does not make the slot fire again", async () => {
    const { id } = await dueSchedule("overlapping-edit");
    interrupt.afterNextCall(async () => {
      expect(await scheduler.tick()).toHaveLength(1);
    });
    await staleScheduler.update(id, { payload: { v: 1 } });
    expect(interrupt.fired).toBe(1);
    expect(await scheduler.tick()).toEqual([]);
    expect(await jobsOf(id)).toHaveLength(1);
    expect(await scheduler.get(id)).toMatchObject({ payload: { v: 1 }, nextRunAt: expect.any(Number) });
    expect((await scheduler.get(id)).nextRunAt).toBeGreaterThan(clock.now());
  });

  // overlap "skip": the tick checked for an unfinished run, then enqueued in another step. A
  // run created in between (here by trigger()) was not seen, and the tick enqueued next to it.
  // The trigger is injected after each store call the tick makes in turn; whenever the tick
  // enqueues, it must have done so before the triggered run existed.
  it("overlap skip: the tick never enqueues next to a run that already exists", async () => {
    const insertOrder = async (jobId: string) => (await sqlite.listJobEvents(jobId)).find((e) => e.type === "enqueued")!.id;
    for (let k = 0; k < 8; k++) {
      const { id } = await dueSchedule(`skip-${k}`, "skip");
      let triggered: string | undefined;
      const trigger = async () => {
        triggered = await scheduler.trigger(id);
      };
      const before = interrupt.fired;
      interrupt.afterNextCall(trigger, k);
      const fired = await staleScheduler.tick();
      if (interrupt.fired === before) {
        interrupt.afterNextCall(async () => {}); // disarm
        await trigger();
      }
      if (fired.length > 0) {
        expect({ k, firedBeforeTrigger: (await insertOrder(fired[0]!)) < (await insertOrder(triggered!)) }).toEqual({ k, firedBeforeTrigger: true });
      } else {
        expect({ k, jobs: (await jobsOf(id)).map((j) => j.id) }).toEqual({ k, jobs: [triggered] });
      }
    }
  });
});
