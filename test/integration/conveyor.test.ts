import { describe, expect, it } from "vitest";
import type { Conveyor } from "../../src/core/conveyor.js";
import { ConflictError, NotFoundError, QueuePausedError, ValidationError } from "../../src/core/errors.js";
import type { EnqueueRequest, Job } from "../../src/core/types.js";
import type { SqliteStore } from "../../src/storage/sqlite-store.js";
import { manualClock, setup } from "../helpers.js";

/** Make the database reject inserting any job called `name`, the way a failing disk or constraint would. */
function failInsertsNamed(conveyor: Conveyor, name: string) {
  (conveyor.store as SqliteStore).db.exec(
    `CREATE TRIGGER fail_insert BEFORE INSERT ON jobs WHEN NEW.name = '${name}' BEGIN SELECT RAISE(ABORT, 'simulated insert failure'); END;`,
  );
}

async function allJobs(conveyor: Conveyor): Promise<Job[]> {
  return (await conveyor.listJobs({ limit: 500 })).items;
}

describe("enqueue", () => {
  it("creates a waiting job with defaults and records the enqueue", async () => {
    const clock = manualClock();
    const { conveyor } = await setup({ clock });
    const seen: Job[] = [];
    conveyor.events.on("job.enqueued", (j) => seen.push(j));
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    expect(job).toMatchObject({
      queue: "q",
      name: "x",
      payload: null,
      state: "waiting",
      priority: 0,
      attempts: 0,
      maxAttempts: 3,
      timeoutMs: 0,
      runAt: clock.now(),
      createdAt: clock.now(),
      uniqueKey: null,
      scheduleId: null,
    });
    expect(await conveyor.getJob(job.id)).toEqual(job);
    expect(seen).toEqual([job]);
    expect((await conveyor.jobEvents(job.id)).map(({ type, data }) => ({ type, data }))).toEqual([{ type: "enqueued", data: { state: "waiting" } }]);
  });

  it.each<[string, Partial<EnqueueRequest>, RegExp]>([
    ["an invalid queue name", { queue: "bad queue" }, /invalid queue name/],
    ["an empty queue name", { queue: "" }, /invalid queue name/],
    ["an invalid job name", { name: "-x" }, /invalid job name/],
    ["a fractional priority", { priority: 1.5 }, /priority must be an integer between -1000 and 1000/],
    ["a priority out of range", { priority: 1001 }, /priority/],
    ["a priority below range", { priority: -1001 }, /priority/],
    ["zero maxAttempts", { maxAttempts: 0 }, /maxAttempts must be a positive integer/],
    ["fractional maxAttempts", { maxAttempts: 2.5 }, /maxAttempts/],
    ["a negative delay", { delayMs: -1 }, /delayMs must be a non-negative number/],
    ["a NaN delay", { delayMs: Number.NaN }, /delayMs/],
    ["a negative timeout", { timeoutMs: -1 }, /timeoutMs must be a non-negative number/],
    ["an infinite timeout", { timeoutMs: Number.POSITIVE_INFINITY }, /timeoutMs/],
    ["an empty unique key", { uniqueKey: "" }, /uniqueKey must be 1-256 characters/],
    ["a long unique key", { uniqueKey: "k".repeat(257) }, /uniqueKey/],
    // Previously these reached SQLite and failed with "NOT NULL constraint failed: jobs.run_at".
    ["a NaN runAt", { runAt: Number.NaN }, /runAt must be a finite timestamp/],
    ["an infinite runAt", { runAt: Number.POSITIVE_INFINITY }, /runAt must be a finite timestamp/],
    // Limits (previously KB-25: 1e300 was stored and then could not be rendered as a date).
    ["a delay of 1e300", { delayMs: 1e300 }, /^delayMs must be at most 3155760000000 \(100 years\)$/],
    ["a delay just over 100 years", { delayMs: 3_155_760_000_001 }, /delayMs must be at most 3155760000000/],
    ["a runAt after 9999", { runAt: Date.UTC(10000, 0, 1) }, /^runAt must be between 1970-01-01T00:00:00.000Z and 9999-12-31T23:59:59.999Z$/],
    ["a runAt of 1e300", { runAt: 1e300 }, /runAt must be between/],
    ["a runAt before 1970", { runAt: -1 }, /runAt must be between/],
    ["a fixed backoff over 100 years", { backoff: { type: "fixed", delayMs: 1e300 } }, /^backoff.delayMs must be at most 3155760000000 \(100 years\)$/],
    ["a linear backoff step over 100 years", { backoff: { type: "linear", stepMs: 1e20, maxMs: 1000 } }, /backoff.stepMs must be at most/],
    ["an exponential backoff cap over 100 years", { backoff: { type: "exponential", baseMs: 1000, maxMs: 1e300 } }, /backoff.maxMs must be at most/],
    ["a NaN backoff delay", { backoff: { type: "fixed", delayMs: Number.NaN } }, /backoff.delayMs must be a non-negative number/],
  ])("rejects %s", async (_label, patch, message) => {
    const { conveyor } = await setup({ clock: manualClock() });
    await expect(conveyor.enqueue({ queue: "q", name: "x", ...patch })).rejects.toThrow(ValidationError);
    await expect(conveyor.enqueue({ queue: "q", name: "x", ...patch })).rejects.toThrow(message);
    expect(await allJobs(conveyor)).toEqual([]);
  });

  it("accepts delays and run times right at the limits", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    const latest = Date.UTC(9999, 11, 31, 23, 59, 59, 999);
    expect((await conveyor.enqueue({ queue: "q", name: "x", runAt: latest })).runAt).toBe(latest);
    expect((await conveyor.enqueue({ queue: "q", name: "x", runAt: 0 })).state).toBe("waiting");
    const hundredYears = 100 * 365.25 * 24 * 60 * 60 * 1000;
    expect((await conveyor.enqueue({ queue: "q", name: "x", delayMs: hundredYears })).runAt).toBe(conveyor.clock.now() + hundredYears);
    await conveyor.enqueue({ queue: "q", name: "x", backoff: { type: "exponential", baseMs: hundredYears, maxMs: hundredYears } });
  });

  it("checks every job in a bulk enqueue against the limits before writing any", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    await expect(conveyor.enqueueBulk([{ queue: "q", name: "x" }, { queue: "q", name: "x", delayMs: 1e300 }])).rejects.toThrow(/delayMs must be at most/);
    expect(await allJobs(conveyor)).toEqual([]);
  });

  it("enforces the payload size limit in bytes", async () => {
    const { conveyor } = await setup({ clock: manualClock(), config: { maxPayloadBytes: 20 } });
    await expect(conveyor.enqueue({ queue: "q", name: "x", payload: "é".repeat(8) })).resolves.toBeDefined(); // 18 bytes
    await expect(conveyor.enqueue({ queue: "q", name: "x", payload: "é".repeat(10) })).rejects.toThrow("payload is 22 bytes; limit is 20");
  });

  it("accepts jobs for a paused queue (pausing stops claims, not producers)", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    await conveyor.pause("q");
    await expect(conveyor.enqueue({ queue: "q", name: "x" })).resolves.toMatchObject({ state: "waiting" });
    await expect(conveyor.assertAccepting("q")).rejects.toThrow(QueuePausedError);
    await expect(conveyor.assertAccepting("other")).resolves.toBeUndefined();
  });
});

describe("unique keys", () => {
  it("returns the existing job while one with the key is unfinished", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const a = await conveyor.enqueue({ queue: "q", name: "sync", uniqueKey: "user:1" });
    const b = await conveyor.enqueue({ queue: "q", name: "sync", uniqueKey: "user:1", payload: { different: true } });
    expect(b).toEqual(a);
    const lease = await broker.claim(["q"], "w");
    expect((await conveyor.enqueue({ queue: "q", name: "sync", uniqueKey: "user:1" })).id).toBe(a.id);
    await broker.complete(a.id, lease!.token);
    const c = await conveyor.enqueue({ queue: "q", name: "sync", uniqueKey: "user:1" });
    expect(c.id).not.toBe(a.id);
  });

  it("is scoped to the queue", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    const a = await conveyor.enqueue({ queue: "a", name: "x", uniqueKey: "k" });
    const b = await conveyor.enqueue({ queue: "b", name: "x", uniqueKey: "k" });
    expect(b.id).not.toBe(a.id);
  });
});

describe("retry", () => {
  it("re-runs a dead or cancelled job from scratch", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const enqueued: string[] = [];
    conveyor.events.on("job.enqueued", (j) => enqueued.push(j.id));
    const dead = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
    const lease = await broker.claim(["q"], "w");
    await broker.fail(dead.id, lease!.token, "boom");
    const cancelled = await conveyor.enqueue({ queue: "q", name: "x" });
    await conveyor.cancel(cancelled.id);
    clock.advance(1000);

    for (const [job, previousState] of [
      [dead, "dead"],
      [cancelled, "cancelled"],
    ] as const) {
      const retried = await conveyor.retry(job.id);
      expect(retried).toMatchObject({ state: "waiting", attempts: 0, runAt: clock.now(), finishedAt: null });
      expect((await conveyor.jobEvents(job.id)).at(-1)).toMatchObject({ type: "retried", data: { previousState } });
    }
    expect(enqueued.slice(-2)).toEqual([dead.id, cancelled.id]);
    expect((await broker.claim(["q"], "w"))?.job.id).toBe(dead.id);
  });

  it("refuses jobs that are not dead or cancelled", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    await expect(conveyor.retry(job.id)).rejects.toThrow(ConflictError);
    await expect(conveyor.retry("nope")).rejects.toThrow(NotFoundError);
  });

  it("retryAllDead retries every dead job in the queue, up to the limit", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    for (let i = 0; i < 3; i++) await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
    await conveyor.enqueue({ queue: "other", name: "x", maxAttempts: 1 });
    for (let i = 0; i < 4; i++) {
      const lease = await broker.claim(["q", "other"], "w");
      await broker.fail(lease!.job.id, lease!.token, "e");
    }
    expect(await conveyor.retryAllDead("q", 2)).toBe(2);
    expect(await conveyor.retryAllDead("q")).toBe(1);
    expect(await conveyor.retryAllDead("q")).toBe(0);
    expect((await conveyor.queueStats("other")).counts.dead).toBe(1);
  });

  // It skips jobs someone else retried meanwhile (a ConflictError), but a real failure must
  // not be swallowed.
  it("retryAllDead passes on errors other than a job being retried meanwhile", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
    const lease = await broker.claim(["q"], "w");
    await broker.fail(lease!.job.id, lease!.token, "e");
    conveyor.store.retryJob = () => Promise.reject(new Error("database is locked"));
    await expect(conveyor.retryAllDead("q")).rejects.toThrow("database is locked");
  });
});

describe("queue settings and stats", () => {
  it("uses defaults for a queue that has never been configured", async () => {
    const clock = manualClock();
    const { conveyor } = await setup({ clock });
    expect(await conveyor.queueSettings("new")).toEqual({
      name: "new",
      paused: false,
      concurrency: 0,
      rateLimitMax: 0,
      rateLimitIntervalMs: 0,
      defaultMaxAttempts: 3,
      createdAt: clock.now(),
      updatedAt: clock.now(),
    });
  });

  it("updates settings and keeps unspecified ones", async () => {
    const clock = manualClock();
    const { conveyor } = await setup({ clock });
    await conveyor.updateQueue("q", { concurrency: 3 });
    clock.advance(10);
    const updated = await conveyor.updateQueue("q", { rateLimitMax: 5, rateLimitIntervalMs: 1000 });
    expect(updated).toMatchObject({ concurrency: 3, rateLimitMax: 5, rateLimitIntervalMs: 1000, updatedAt: clock.now() });
    expect(await conveyor.queueSettings("q")).toEqual(updated);
  });

  it.each<[string, Record<string, number>, RegExp]>([
    ["negative concurrency", { concurrency: -1 }, /queue limits must be non-negative/],
    ["a negative rate limit", { rateLimitMax: -1 }, /non-negative/],
    ["a rate limit without an interval", { rateLimitMax: 5 }, /rateLimitIntervalMs is required when rateLimitMax is set/],
    ["zero default attempts", { defaultMaxAttempts: 0 }, /defaultMaxAttempts must be >= 1/],
  ])("rejects %s", async (_label, patch, message) => {
    const { conveyor } = await setup({ clock: manualClock() });
    await expect(conveyor.updateQueue("q", patch)).rejects.toThrow(message);
    expect(await conveyor.store.getQueue("q")).toBeUndefined();
  });

  it("rejects an invalid queue name", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    await expect(conveyor.updateQueue("bad name", { concurrency: 1 })).rejects.toThrow("invalid queue name");
  });

  it("reports counts and the oldest waiting job per queue", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const first = clock.now();
    await conveyor.enqueue({ queue: "q", name: "x" });
    clock.advance(5);
    await conveyor.enqueue({ queue: "q", name: "x" });
    await conveyor.enqueue({ queue: "q", name: "x", delayMs: 100 });
    await conveyor.enqueue({ queue: "r", name: "x" });
    const lease = await broker.claim(["q"], "w");
    expect(lease!.job.runAt).toBe(first);
    const stats = await conveyor.queueStats("q");
    expect(stats.counts).toMatchObject({ waiting: 1, active: 1, delayed: 1, completed: 0 });
    expect(stats.oldestWaitingAt).toBe(first + 5);
    expect((await conveyor.listQueues()).map((q) => [q.name, q.counts.waiting])).toEqual([
      ["q", 1],
      ["r", 1],
    ]);
  });
});

describe("enqueueBulk", () => {
  it("enqueues every job and returns them in order", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    const seen: string[] = [];
    conveyor.events.on("job.enqueued", (j) => seen.push(j.id));
    const jobs = await conveyor.enqueueBulk([
      { queue: "a", name: "x", payload: 1 },
      { queue: "b", name: "y", payload: 2, delayMs: 100 },
    ]);
    expect(jobs.map((j) => [j.queue, j.payload, j.state])).toEqual([
      ["a", 1, "waiting"],
      ["b", 2, "delayed"],
    ]);
    expect(seen).toEqual(jobs.map((j) => j.id));
    for (const j of jobs) {
      expect(await conveyor.getJob(j.id)).toEqual(j);
      expect((await conveyor.jobEvents(j.id)).map((e) => e.type)).toEqual(["enqueued"]);
    }
  });

  it("inserts nothing if any job in the batch is invalid", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    await expect(
      conveyor.enqueueBulk([
        { queue: "q", name: "ok" },
        { queue: "q", name: "ok" },
        { queue: "q", name: "bad name" },
      ]),
    ).rejects.toThrow(ValidationError);
    expect(await allJobs(conveyor)).toEqual([]);
  });

  it("inserts nothing, and announces nothing, if the database fails part-way", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    failInsertsNamed(conveyor, "explode");
    const seen: string[] = [];
    conveyor.events.on("job.enqueued", (j) => seen.push(j.id));
    await expect(
      conveyor.enqueueBulk([
        { queue: "q", name: "ok" },
        { queue: "q", name: "ok" },
        { queue: "q", name: "explode" },
        { queue: "q", name: "ok" },
      ]),
    ).rejects.toThrow("simulated insert failure");
    expect(await allJobs(conveyor)).toEqual([]);
    expect((conveyor.store as SqliteStore).db.prepare("SELECT COUNT(*) AS n FROM job_events").get()).toEqual({ n: 0 });
    expect(seen).toEqual([]);
  });

  it("still works normally after a failed batch", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    failInsertsNamed(conveyor, "explode");
    await expect(conveyor.enqueueBulk([{ queue: "q", name: "ok" }, { queue: "q", name: "explode" }])).rejects.toThrow();
    const jobs = await conveyor.enqueueBulk([{ queue: "q", name: "ok" }, { queue: "q", name: "ok" }]);
    expect((await allJobs(conveyor)).map((j) => j.id).sort()).toEqual(jobs.map((j) => j.id).sort());
  });

  it("de-duplicates unique keys within the batch and against existing jobs", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    const existing = await conveyor.enqueue({ queue: "q", name: "x", uniqueKey: "old" });
    const jobs = await conveyor.enqueueBulk([
      { queue: "q", name: "x", uniqueKey: "new" },
      { queue: "q", name: "x", uniqueKey: "new" },
      { queue: "q", name: "x", uniqueKey: "old" },
      { queue: "other", name: "x", uniqueKey: "new" },
    ]);
    expect(jobs[1]!.id).toBe(jobs[0]!.id);
    expect(jobs[2]!.id).toBe(existing.id);
    expect(jobs[3]!.id).not.toBe(jobs[0]!.id);
    expect(await allJobs(conveyor)).toHaveLength(3);
  });

  it("rejects more than 1000 jobs", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    await expect(conveyor.enqueueBulk(Array.from({ length: 1001 }, () => ({ queue: "q", name: "x" })))).rejects.toThrow(
      "at most 1000 jobs per bulk request",
    );
  });

  it("accepts an empty batch", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    expect(await conveyor.enqueueBulk([])).toEqual([]);
  });
});

describe("single enqueue atomicity", () => {
  it("leaves no job behind when the insert fails", async () => {
    const { conveyor } = await setup({ clock: manualClock() });
    failInsertsNamed(conveyor, "explode");
    await expect(conveyor.enqueue({ queue: "q", name: "explode" })).rejects.toThrow("simulated insert failure");
    expect(await allJobs(conveyor)).toEqual([]);
  });
});
