import { describe, expect, it } from "vitest";
import type { Broker } from "../../src/broker/broker.js";
import type { Conveyor } from "../../src/core/conveyor.js";
import { LeaseLostError, NotFoundError, UnrecoverableError, ValidationError } from "../../src/core/errors.js";
import type { EventName } from "../../src/core/events.js";
import type { SqliteStore } from "../../src/storage/sqlite-store.js";
import { legacyJobId, manualClock, setup } from "../helpers.js";

/** Record every event as [name, jobId-or-arg, ...rest]. */
function recordEvents(conveyor: Conveyor) {
  const events: [EventName, ...unknown[]][] = [];
  conveyor.events.onAny((event, ...args) => {
    const [first, ...rest] = args;
    events.push([event, (first as { id?: string })?.id ?? first, ...rest]);
  });
  return events;
}

async function claimAll(broker: Broker, queues: string[]) {
  const ids: string[] = [];
  for (;;) {
    const lease = await broker.claim(queues, "w");
    if (!lease) return ids;
    ids.push(lease.job.id);
    await broker.complete(lease.job.id, lease.token);
  }
}

describe("claim order", () => {
  it("runs higher priority first, then first-in-first-out, for jobs enqueued in the same millisecond", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const enqueued: { id: string; priority: number }[] = [];
    for (let i = 0; i < 40; i++) {
      const priority = [0, 5, -5, 0][i % 4]!;
      enqueued.push({ id: (await conveyor.enqueue({ queue: "q", name: "x", priority, payload: i })).id, priority });
    }
    const expected = [5, 0, -5].flatMap((p) => enqueued.filter((j) => j.priority === p).map((j) => j.id));
    expect(await claimAll(broker, ["q"])).toEqual(expected);
  });

  it("is first-in-first-out across milliseconds", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const ids: string[] = [];
    for (let i = 0; i < 10; i++) {
      ids.push((await conveyor.enqueue({ queue: "q", name: "x" })).id);
      if (i % 3 === 0) clock.advance(1);
    }
    expect(await claimAll(broker, ["q"])).toEqual(ids);
  });

  it("keeps the order of a bulk enqueue", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const jobs = await conveyor.enqueueBulk(Array.from({ length: 25 }, (_, i) => ({ queue: "q", name: "x", payload: i })));
    expect(await claimAll(broker, ["q"])).toEqual(jobs.map((j) => j.id));
  });

  it("orders a promoted delayed job by when it became due", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const delayed = await conveyor.enqueue({ queue: "q", name: "x", delayMs: 100 });
    clock.advance(200);
    const later = await conveyor.enqueue({ queue: "q", name: "x" });
    await conveyor.store.promoteDelayed(clock.now(), 10);
    expect(await claimAll(broker, ["q"])).toEqual([delayed.id, later.id]);
  });

  it("takes the best job across several queues", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const a = await conveyor.enqueue({ queue: "a", name: "x" });
    const b = await conveyor.enqueue({ queue: "b", name: "x", priority: 1 });
    expect(await claimAll(broker, ["a", "b"])).toEqual([b.id, a.id]);
  });

  it("only claims from the queues asked for", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    await conveyor.enqueue({ queue: "other", name: "x" });
    expect(await broker.claim(["q"], "w")).toBeUndefined();
  });
});

describe("a database with job ids from before the FIFO change", () => {
  it("claims old and new jobs in order, and pages through them exactly once", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    // Three jobs enqueued by the old version in one millisecond, with ids that sort in reverse.
    const oldAt = clock.now() - 60_000;
    const oldIds = [legacyJobId(oldAt, Buffer.alloc(10, 31)), legacyJobId(oldAt, Buffer.alloc(10, 15)), legacyJobId(oldAt, Buffer.alloc(10, 0))];
    for (const id of oldIds) {
      const job = await conveyor.enqueue({ queue: "q", name: "x" });
      const row = { ...job, id, runAt: oldAt, createdAt: oldAt, updatedAt: oldAt };
      (conveyor.store as SqliteStore).db.prepare("DELETE FROM job_events WHERE job_id = ?").run(job.id);
      (conveyor.store as SqliteStore).db.prepare("DELETE FROM jobs WHERE id = ?").run(job.id);
      await conveyor.store.insertJob(row);
    }
    const newIds: string[] = [];
    for (let i = 0; i < 3; i++) newIds.push((await conveyor.enqueue({ queue: "q", name: "x" })).id);

    const seen: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await conveyor.listJobs({ queue: "q", limit: 2, cursor });
      seen.push(...page.items.map((j) => j.id));
      cursor = page.nextCursor ?? undefined;
    } while (cursor);
    expect(seen.sort()).toEqual([...oldIds, ...newIds].sort());

    // Old jobs first (earlier run_at), in id order, which for them is arbitrary; then new
    // jobs strictly first-in-first-out.
    const claimed = await claimAll(broker, ["q"]);
    expect(claimed.slice(0, 3)).toEqual([...oldIds].sort());
    expect(claimed.slice(3)).toEqual(newIds);
  });
});

describe("claiming", () => {
  it("leases the job to the worker for leaseMs and records the attempt", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const events = recordEvents(conveyor);
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "worker-1");
    expect(lease).toMatchObject({ expiresAt: clock.now() + 1000, job: { id: job.id, state: "active", attempts: 1, workerId: "worker-1" } });
    expect(lease!.token).toBe(lease!.job.leaseToken);
    expect(events).toContainEqual(["job.claimed", job.id, "worker-1"]);
    expect((await conveyor.jobEvents(job.id)).at(-1)).toMatchObject({ type: "claimed", data: { workerId: "worker-1", attempt: 1 } });
  });

  it("hands out different lease tokens for each claim", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const first = await broker.claim(["q"], "w");
    await broker.release(job.id, first!.token);
    const second = await broker.claim(["q"], "w");
    expect(second!.token).not.toBe(first!.token);
  });

  it("uses the broker's leaseMs override", async () => {
    const clock = manualClock();
    const { conveyor } = await setup({ clock });
    const { Broker } = await import("../../src/broker/broker.js");
    const broker = new Broker(conveyor, { leaseMs: 5000 });
    await conveyor.enqueue({ queue: "q", name: "x" });
    expect((await broker.claim(["q"], "w"))!.expiresAt).toBe(clock.now() + 5000);
  });
});

describe("delayed jobs", () => {
  it("are not claimable until they are due and promoted", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const job = await conveyor.enqueue({ queue: "q", name: "x", delayMs: 5000 });
    expect(job).toMatchObject({ state: "delayed", runAt: clock.now() + 5000 });

    clock.advance(4999);
    expect(await conveyor.store.promoteDelayed(clock.now(), 10)).toBe(0);
    expect(await broker.claim(["q"], "w")).toBeUndefined();

    clock.advance(1);
    expect(await conveyor.store.promoteDelayed(clock.now(), 10)).toBe(1);
    expect((await broker.claim(["q"], "w"))?.job.id).toBe(job.id);
  });

  it("use runAt over delayMs, and run immediately if runAt is not in the future", async () => {
    const clock = manualClock();
    const { conveyor } = await setup({ clock });
    expect(await conveyor.enqueue({ queue: "q", name: "x", runAt: clock.now() + 10, delayMs: 99_999 })).toMatchObject({
      state: "delayed",
      runAt: clock.now() + 10,
    });
    expect(await conveyor.enqueue({ queue: "q", name: "x", runAt: clock.now() })).toMatchObject({ state: "waiting" });
    expect(await conveyor.enqueue({ queue: "q", name: "x", runAt: clock.now() - 60_000 })).toMatchObject({ state: "waiting" });
    expect(await conveyor.enqueue({ queue: "q", name: "x", delayMs: 0 })).toMatchObject({ state: "waiting", runAt: clock.now() });
  });
});

describe("retries with backoff", () => {
  it("retries with each attempt's backoff delay, then moves to dead after maxAttempts", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const events = recordEvents(conveyor);
    const job = await conveyor.enqueue({
      queue: "q",
      name: "x",
      maxAttempts: 4,
      backoff: { type: "exponential", baseMs: 100, maxMs: 1000, jitter: false },
    });

    for (const [attempt, delay] of [[1, 100], [2, 200], [3, 400]] as const) {
      const lease = await broker.claim(["q"], "w");
      expect(lease!.job.attempts).toBe(attempt);
      const failed = await broker.fail(job.id, lease!.token, new Error(`fail ${attempt}`));
      expect(failed).toMatchObject({ state: "delayed", runAt: clock.now() + delay, lastError: `fail ${attempt}` });
      expect(events).toContainEqual(["job.retrying", job.id, `fail ${attempt}`, delay]);

      clock.advance(delay - 1);
      await conveyor.store.promoteDelayed(clock.now(), 10);
      expect(await broker.claim(["q"], "w")).toBeUndefined();
      clock.advance(1);
      await conveyor.store.promoteDelayed(clock.now(), 10);
    }

    const last = await broker.claim(["q"], "w");
    const dead = await broker.fail(job.id, last!.token, new Error("fail 4"));
    expect(dead).toMatchObject({ state: "dead", attempts: 4, lastError: "fail 4", finishedAt: clock.now(), leaseToken: null });
    expect(events.filter((e) => e[0] === "job.failed")).toHaveLength(4);
    expect(events.filter((e) => e[0] === "job.dead")).toEqual([["job.dead", job.id, "fail 4"]]);
    expect((await conveyor.jobEvents(job.id)).map((e) => e.type)).toEqual([
      "enqueued",
      "claimed",
      "retry_scheduled",
      "claimed",
      "retry_scheduled",
      "claimed",
      "retry_scheduled",
      "claimed",
      "dead",
    ]);
  });

  it.each([
    [{ type: "fixed", delayMs: 750 } as const, [750, 750]],
    [{ type: "linear", stepMs: 300, maxMs: 500 } as const, [300, 500]],
  ])("applies %j", async (backoff, delays) => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 3, backoff });
    for (const delay of delays) {
      const lease = await broker.claim(["q"], "w");
      expect((await broker.fail(job.id, lease!.token, "e")).runAt).toBe(clock.now() + delay);
      clock.advance(delay);
      await conveyor.store.promoteDelayed(clock.now(), 10);
    }
  });

  it("uses the default exponential backoff with jitter when none is given", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock }); // broker random() is 0.5
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    expect(job.backoff).toEqual({ type: "exponential", baseMs: 1000, maxMs: 600_000, jitter: true });
    const lease = await broker.claim(["q"], "w");
    // 1000ms, jittered to between 500 and 1000: 500 + 0.5 * 500.
    expect((await broker.fail(job.id, lease!.token, "e")).runAt).toBe(clock.now() + 750);
  });

  it("falls back to the default backoff for a job stored without one", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    (conveyor.store as SqliteStore).db.prepare("UPDATE jobs SET backoff = NULL WHERE id = ?").run(job.id);
    const lease = await broker.claim(["q"], "w");
    expect(lease!.job.backoff).toBeNull();
    expect((await broker.fail(job.id, lease!.token, "e")).runAt).toBe(clock.now() + 750);
  });

  // Previously KB-25's twin: retryInMs wrote an unrepresentable run_at.
  it("rejects a retryInMs over 100 years before changing anything", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    await expect(broker.fail(job.id, lease!.token, "e", { retryInMs: 1e300 })).rejects.toThrow(
      new ValidationError("retryInMs must be at most 3155760000000 (100 years)"),
    );
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "active", attempts: 1, lastError: null, leaseToken: lease!.token });
  });

  it("honours retryInMs over the backoff", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const job = await conveyor.enqueue({ queue: "q", name: "x", backoff: { type: "fixed", delayMs: 60_000 } });
    const lease = await broker.claim(["q"], "w");
    expect((await broker.fail(job.id, lease!.token, "e", { retryInMs: 5 })).runAt).toBe(clock.now() + 5);
  });

  it("moves straight to dead for UnrecoverableError or the unrecoverable option", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 5 });
    await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 5 });
    let lease = await broker.claim(["q"], "w");
    expect(await broker.fail(lease!.job.id, lease!.token, new UnrecoverableError("bad input"))).toMatchObject({ state: "dead", attempts: 1 });
    lease = await broker.claim(["q"], "w");
    expect(await broker.fail(lease!.job.id, lease!.token, "nope", { unrecoverable: true })).toMatchObject({ state: "dead", attempts: 1 });
  });

  it("takes maxAttempts from the queue's default, or 3", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    expect((await conveyor.enqueue({ queue: "q", name: "x" })).maxAttempts).toBe(3);
    await conveyor.updateQueue("q", { defaultMaxAttempts: 8 });
    expect((await conveyor.enqueue({ queue: "q", name: "x" })).maxAttempts).toBe(8);
    expect((await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 2 })).maxAttempts).toBe(2);
  });
});

describe("complete", () => {
  it("stores the result and finishes the job", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const events = recordEvents(conveyor);
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    clock.advance(250);
    const done = await broker.complete(job.id, lease!.token, { sum: 5 });
    expect(done).toMatchObject({ state: "completed", result: { sum: 5 }, progress: 1, finishedAt: clock.now(), leaseToken: null });
    expect(events).toContainEqual(["job.completed", job.id]);
    expect((await conveyor.jobEvents(job.id)).at(-1)?.type).toBe("completed");
  });

  it("stores no result as null", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    expect((await broker.complete(job.id, lease!.token)).result).toBeNull();
  });
});

describe("progress", () => {
  it("stores progress clamped to 0..1 and emits it", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const events = recordEvents(conveyor);
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    await broker.progress(job.id, lease!.token, 0.4);
    expect((await conveyor.getJob(job.id)).progress).toBe(0.4);
    expect(events).toContainEqual(["job.progress", job.id, 0.4]);
    await broker.progress(job.id, lease!.token, 7);
    expect((await conveyor.getJob(job.id)).progress).toBe(1);
    await broker.progress(job.id, lease!.token, -2);
    expect((await conveyor.getJob(job.id)).progress).toBe(0);
  });

  it("resets when the job is claimed again after a failure", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x", backoff: { type: "fixed", delayMs: 0 } });
    let lease = await broker.claim(["q"], "w");
    await broker.progress(job.id, lease!.token, 0.9);
    await broker.fail(job.id, lease!.token, "e");
    await conveyor.store.promoteDelayed(conveyor.clock.now(), 10);
    lease = await broker.claim(["q"], "w");
    expect(lease!.job.progress).toBe(0);
  });

  // Previously NaN reached SQLite and failed with "NOT NULL constraint failed: jobs.progress".
  it("rejects a progress value that is not a finite number", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    await expect(broker.progress(job.id, lease!.token, Number.NaN)).rejects.toThrow(ValidationError);
    await expect(broker.progress(job.id, lease!.token, Number.POSITIVE_INFINITY)).rejects.toThrow(ValidationError);
    expect((await conveyor.getJob(job.id)).progress).toBe(0);
  });
});

describe("heartbeat", () => {
  it("extends the lease by leaseMs from now", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    clock.advance(800);
    expect(await broker.heartbeat(job.id, lease!.token)).toBe(clock.now() + 1000);
    expect((await conveyor.getJob(job.id)).leaseExpiresAt).toBe(clock.now() + 1000);
  });
});

describe("lease checks", () => {
  it("reject every lease operation with a wrong or stale token", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    for (const op of [
      () => broker.heartbeat(job.id, "wrong"),
      () => broker.progress(job.id, "wrong", 0.5),
      () => broker.complete(job.id, "wrong"),
      () => broker.fail(job.id, "wrong", "e"),
      () => broker.release(job.id, "wrong"),
    ]) {
      await expect(op()).rejects.toThrow(LeaseLostError);
    }
    await broker.complete(job.id, lease!.token);
    await expect(broker.complete(job.id, lease!.token)).rejects.toThrow(LeaseLostError);
    await expect(broker.heartbeat(job.id, lease!.token)).rejects.toThrow(LeaseLostError);
    expect((await conveyor.getJob(job.id)).state).toBe("completed");
  });

  it("report an unknown job as not found", async () => {
    const { broker } = await setup({ clock: manualClock() });
    await expect(broker.heartbeat("nope", "t")).rejects.toThrow(NotFoundError);
  });
});

describe("cancel", () => {
  it("cancels waiting, delayed and active jobs", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const events = recordEvents(conveyor);
    const waiting = await conveyor.enqueue({ queue: "q", name: "x" });
    const delayed = await conveyor.enqueue({ queue: "q", name: "x", delayMs: 1000 });
    const active = await conveyor.enqueue({ queue: "q", name: "x" });
    await broker.claim(["q"], "w"); // waiting job
    await conveyor.cancel(waiting.id);
    await broker.claim(["q"], "w"); // `active` job

    for (const [job, previousState] of [
      [delayed, "delayed"],
      [active, "active"],
    ] as const) {
      const cancelled = await conveyor.cancel(job.id);
      expect(cancelled).toMatchObject({ state: "cancelled", finishedAt: clock.now(), leaseToken: null });
      expect((await conveyor.jobEvents(job.id)).at(-1)).toMatchObject({ type: "cancelled", data: { previousState } });
      expect(events).toContainEqual(["job.cancelled", job.id]);
    }
    expect(await broker.claim(["q"], "w")).toBeUndefined();
  });

  it("takes the lease away from the worker running a cancelled job", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    await conveyor.cancel(job.id);
    await expect(broker.heartbeat(job.id, lease!.token)).rejects.toThrow(LeaseLostError);
    await expect(broker.complete(job.id, lease!.token, { late: true })).rejects.toThrow(LeaseLostError);
    await expect(broker.fail(job.id, lease!.token, "late")).rejects.toThrow(LeaseLostError);
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "cancelled", result: null });
  });

  it("refuses jobs that have already finished, and unknown jobs", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    await broker.complete(job.id, lease!.token);
    await expect(conveyor.cancel(job.id)).rejects.toThrow("is already completed");
    await expect(conveyor.cancel("nope")).rejects.toThrow(NotFoundError);
  });
});

describe("queue controls", () => {
  it("does not claim from a paused queue until it is resumed", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const events = recordEvents(conveyor);
    const a = await conveyor.enqueue({ queue: "a", name: "x", priority: 10 });
    const b = await conveyor.enqueue({ queue: "b", name: "x" });
    await conveyor.pause("a");
    await conveyor.pause("a");
    expect((await broker.claim(["a", "b"], "w"))?.job.id).toBe(b.id);
    expect(await broker.claim(["a"], "w")).toBeUndefined();
    await conveyor.resume("a");
    expect((await broker.claim(["a"], "w"))?.job.id).toBe(a.id);
    expect(events.filter(([e]) => e.startsWith("queue."))).toEqual([
      ["queue.paused", "a"],
      ["queue.resumed", "a"],
    ]);
  });

  it("respects a queue's concurrency limit", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    await conveyor.updateQueue("q", { concurrency: 2 });
    for (let i = 0; i < 4; i++) await conveyor.enqueue({ queue: "q", name: "x" });
    const first = await broker.claim(["q"], "w");
    expect(await broker.claim(["q"], "w")).toBeDefined();
    expect(await broker.claim(["q"], "w")).toBeUndefined();
    await broker.complete(first!.job.id, first!.token);
    expect(await broker.claim(["q"], "w")).toBeDefined();
  });

  // Regression (was KB-12): claim() counted active jobs and activated in separate steps, so
  // parallel claims all saw room under the limit. Across processes: claim-race.test.ts.
  it("respects the concurrency limit under parallel claims", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    await conveyor.updateQueue("q", { concurrency: 1 });
    for (let i = 0; i < 5; i++) await conveyor.enqueue({ queue: "q", name: "x" });
    const leases = await Promise.all([1, 2, 3, 4, 5].map((i) => broker.claim(["q"], `w${i}`)));
    expect(leases.filter(Boolean)).toHaveLength(1);
    expect(await conveyor.store.countActive("q")).toBe(1);
  });

  it("respects a queue's rate limit, refilling over time", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    await conveyor.updateQueue("q", { rateLimitMax: 2, rateLimitIntervalMs: 1000 });
    for (let i = 0; i < 5; i++) await conveyor.enqueue({ queue: "q", name: "x" });
    expect(await broker.claim(["q"], "w")).toBeDefined();
    expect(await broker.claim(["q"], "w")).toBeDefined();
    expect(await broker.claim(["q"], "w")).toBeUndefined();
    clock.advance(500);
    expect(await broker.claim(["q"], "w")).toBeDefined();
    expect(await broker.claim(["q"], "w")).toBeUndefined();
  });

  // Regression (was KB-17): claim() picked the best job across all queues first and only then
  // checked that queue's rate limit. If that queue was out of tokens the claim returned nothing,
  // even when another queue in the same claim had jobs ready.
  it("does not let a rate-limited queue block the other queues in the same claim", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    await conveyor.updateQueue("a", { rateLimitMax: 1, rateLimitIntervalMs: 60_000 });
    await conveyor.enqueue({ queue: "a", name: "x", priority: 10 });
    await conveyor.enqueue({ queue: "a", name: "x", priority: 10 });
    const b = await conveyor.enqueue({ queue: "b", name: "x" });
    expect((await broker.claim(["a", "b"], "w"))?.job.queue).toBe("a");
    expect((await broker.claim(["a", "b"], "w"))?.job.id).toBe(b.id);
  });

  // claim() reserves a token in each rate-limited queue before it claims; it must give back
  // every token that did not pay for the job it returns.
  it("gives the token back when a rate-limited queue supplied no job", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    await conveyor.updateQueue("a", { rateLimitMax: 1, rateLimitIntervalMs: 60_000 });
    expect(await broker.claim(["a"], "w")).toBeUndefined(); // empty queue
    const b = await conveyor.enqueue({ queue: "b", name: "x", priority: 10 });
    const a = await conveyor.enqueue({ queue: "a", name: "x" });
    expect((await broker.claim(["a", "b"], "w"))?.job.id).toBe(b.id); // b wins on priority
    await conveyor.pause("a");
    expect(await broker.claim(["a"], "w")).toBeUndefined(); // paused
    await conveyor.resume("a");
    // All three claims above left a's single token in place.
    expect((await broker.claim(["a"], "w"))?.job.id).toBe(a.id);
    await conveyor.enqueue({ queue: "a", name: "x" });
    expect(await broker.claim(["a"], "w")).toBeUndefined();
  });

  it("keeps a rate limit under parallel claims in one process", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    await conveyor.updateQueue("q", { rateLimitMax: 2, rateLimitIntervalMs: 60_000 });
    for (let i = 0; i < 5; i++) await conveyor.enqueue({ queue: "q", name: "x" });
    const leases = await Promise.all([1, 2, 3, 4, 5].map((i) => broker.claim(["q"], `w${i}`)));
    expect(leases.filter(Boolean)).toHaveLength(2);
  });

  it("gives reserved tokens back when the claim fails", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    await conveyor.updateQueue("q", { rateLimitMax: 1, rateLimitIntervalMs: 60_000 });
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    const claimNext = conveyor.store.claimNext.bind(conveyor.store);
    conveyor.store.claimNext = () => Promise.reject(new Error("database is locked"));
    await expect(broker.claim(["q"], "w")).rejects.toThrow("database is locked");
    conveyor.store.claimNext = claimNext;
    expect((await broker.claim(["q"], "w"))?.job.id).toBe(j.id);
  });
});
