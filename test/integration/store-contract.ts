// Behaviour every Store implementation must have. Run it with describeStoreContract() from a
// *.test.ts file; see sqlite-store.test.ts.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { ApiToken, Job, JobState, QueueSettings, Schedule, Webhook, WebhookDelivery } from "../../src/core/types.js";
import { JOB_STATES } from "../../src/core/types.js";
import { MIGRATIONS } from "../../src/storage/migrations.js";
import type { Store } from "../../src/storage/store.js";

export const T = Date.UTC(2026, 0, 5, 12, 0, 0);

let seq = 0;
/** A complete job row. Ids increase with each call, so they sort in creation order. */
export function makeJob(over: Partial<Job> = {}): Job {
  seq++;
  return {
    id: `job-${String(seq).padStart(8, "0")}`,
    queue: "q",
    name: "n",
    payload: null,
    state: "waiting",
    priority: 0,
    attempts: 0,
    maxAttempts: 3,
    backoff: null,
    timeoutMs: 0,
    runAt: T,
    createdAt: T,
    updatedAt: T,
    startedAt: null,
    finishedAt: null,
    workerId: null,
    leaseToken: null,
    leaseExpiresAt: null,
    progress: 0,
    result: null,
    lastError: null,
    uniqueKey: null,
    scheduleId: null,
    ...over,
  };
}

export function makeQueue(over: Partial<QueueSettings> = {}): QueueSettings {
  return {
    name: "q",
    paused: false,
    concurrency: 0,
    rateLimitMax: 0,
    rateLimitIntervalMs: 0,
    defaultMaxAttempts: 3,
    createdAt: T,
    updatedAt: T,
    ...over,
  };
}

export function makeSchedule(over: Partial<Schedule> = {}): Schedule {
  seq++;
  return {
    id: `sched-${seq}`,
    name: `schedule-${seq}`,
    queue: "q",
    jobName: "n",
    payload: null,
    cron: "* * * * *",
    timezone: "UTC",
    enabled: true,
    overlap: "allow",
    nextRunAt: T,
    lastRunAt: null,
    lastJobId: null,
    createdAt: T,
    updatedAt: T,
    ...over,
  };
}

export function makeWebhook(over: Partial<Webhook> = {}): Webhook {
  seq++;
  return {
    id: `hook-${seq}`,
    url: "https://example.test/hook",
    secret: "whsec_x",
    events: ["*"],
    queues: [],
    enabled: true,
    createdAt: T,
    ...over,
  };
}

export function makeDelivery(webhookId: string, over: Partial<WebhookDelivery> = {}): WebhookDelivery {
  seq++;
  return {
    id: `delivery-${seq}`,
    webhookId,
    event: "job.completed",
    body: '{"a":1}',
    status: "pending",
    attempts: 0,
    lastStatusCode: null,
    lastError: null,
    nextAttemptAt: T,
    createdAt: T,
    updatedAt: T,
    ...over,
  };
}

export function makeToken(over: Partial<ApiToken> = {}): ApiToken {
  seq++;
  return {
    id: `token-${seq}`,
    name: `t${seq}`,
    hash: `hash-${seq}`,
    scopes: ["jobs:read"],
    createdAt: T,
    lastUsedAt: null,
    revokedAt: null,
    ...over,
  };
}

export interface StoreHarness {
  store: Store;
  /** Close and remove everything the harness created. */
  cleanup(): Promise<void>;
}

export function describeStoreContract(name: string, create: () => Promise<StoreHarness>): void {
  describe(`Store contract: ${name}`, () => {
    let h: StoreHarness;
    let store: Store;

    beforeEach(async () => {
      h = await create();
      store = h.store;
      await store.migrate();
    });

    afterEach(async () => {
      await h.cleanup();
    });

    /** Insert jobs and return them. */
    async function insert(...jobs: Job[]): Promise<Job[]> {
      for (const j of jobs) await store.insertJob(j);
      return jobs;
    }

    async function get(id: string): Promise<Job> {
      const j = await store.getJob(id);
      expect(j, `job ${id}`).toBeDefined();
      return j!;
    }

    describe("migrate", () => {
      it("returns the schema version and is idempotent", async () => {
        const [job] = await insert(makeJob());
        expect(await store.migrate()).toBe(MIGRATIONS.length);
        expect(await store.migrate()).toBe(MIGRATIONS.length);
        expect(await store.getJob(job!.id)).toEqual(job);
      });
    });

    describe("insertJob / getJob", () => {
      it("round-trips every field", async () => {
        const job = makeJob({
          queue: "emails",
          name: "send",
          payload: { to: "ä@example.test", n: [1, 2.5, -3], nested: { ok: true, none: null }, emoji: "📨" },
          state: "active",
          priority: -1000,
          attempts: 2,
          maxAttempts: 7,
          backoff: { type: "exponential", baseMs: 100, maxMs: 1000, jitter: false },
          timeoutMs: 1500,
          runAt: T + 1,
          createdAt: T - 2,
          updatedAt: T + 3,
          startedAt: T + 4,
          finishedAt: T + 5,
          workerId: "host:1:abc",
          leaseToken: "lease-1",
          leaseExpiresAt: T + 30_000,
          progress: 0.25,
          result: { ok: [1, "two"] },
          lastError: "previous failure",
          uniqueKey: "user:1",
          scheduleId: "sched-1",
        });
        await store.insertJob(job);
        expect(await store.getJob(job.id)).toEqual(job);
      });

      it("round-trips scalar payloads and results", async () => {
        for (const payload of ["text", 0, false, [], null]) {
          const job = makeJob({ payload, result: payload === null ? null : payload });
          await store.insertJob(job);
          expect(await get(job.id)).toMatchObject({ payload, result: payload });
        }
      });

      it("stores an undefined payload as null", async () => {
        const job = makeJob({ payload: undefined as never });
        await store.insertJob(job);
        expect((await get(job.id)).payload).toBeNull();
      });

      it("returns undefined for an unknown id", async () => {
        expect(await store.getJob("nope")).toBeUndefined();
      });

      it("rejects a duplicate id", async () => {
        const job = makeJob();
        await store.insertJob(job);
        await expect(store.insertJob({ ...job })).rejects.toThrow();
      });
    });

    describe("insertJobs", () => {
      it("inserts all the jobs and events together", async () => {
        const jobs = [makeJob(), makeJob({ queue: "other" })];
        await store.insertJobs(jobs, [
          { jobId: jobs[0]!.id, type: "enqueued", data: { state: "waiting" }, at: T },
          { jobId: jobs[1]!.id, type: "enqueued", data: null, at: T + 1 },
        ]);
        for (const j of jobs) expect(await store.getJob(j.id)).toEqual(j);
        expect((await store.listJobEvents(jobs[1]!.id)).map((e) => [e.type, e.data, e.at])).toEqual([["enqueued", null, T + 1]]);
      });

      it("accepts an empty batch", async () => {
        await store.insertJobs([], []);
        expect((await store.listJobs({})).items).toEqual([]);
      });

      it("stores nothing if a job fails to insert part-way", async () => {
        const [existing] = await insert(makeJob());
        const batch = [makeJob(), makeJob(), { ...makeJob(), id: existing!.id }, makeJob()];
        await expect(store.insertJobs(batch, [])).rejects.toThrow();
        expect((await store.listJobs({})).items.map((j) => j.id)).toEqual([existing!.id]);
      });

      it("stores nothing if an event fails to insert", async () => {
        const jobs = [makeJob(), makeJob()];
        await expect(
          store.insertJobs(jobs, [
            { jobId: jobs[0]!.id, type: "enqueued", data: null, at: T },
            { jobId: "not-a-job", type: "enqueued", data: null, at: T },
          ]),
        ).rejects.toThrow();
        expect((await store.listJobs({})).items).toEqual([]);
        expect(await store.listJobEvents(jobs[0]!.id)).toEqual([]);
      });

      it("keeps working normally after a failed batch", async () => {
        const dup = makeJob();
        await expect(store.insertJobs([dup, { ...dup }], [])).rejects.toThrow();
        const ok = makeJob();
        await store.insertJobs([ok], []);
        await store.insertJob(makeJob());
        expect((await store.listJobs({})).items).toHaveLength(2);
      });
    });

    describe("findActiveByUniqueKey", () => {
      it("finds an unfinished job with the key in the same queue", async () => {
        for (const state of ["delayed", "waiting", "active"] as JobState[]) {
          const [j] = await insert(makeJob({ uniqueKey: `k-${state}`, state }));
          expect((await store.findActiveByUniqueKey("q", `k-${state}`))?.id).toBe(j!.id);
        }
      });

      it("ignores finished jobs, other queues and other keys", async () => {
        await insert(
          makeJob({ uniqueKey: "k", state: "completed" }),
          makeJob({ uniqueKey: "k", state: "dead" }),
          makeJob({ uniqueKey: "k", state: "cancelled" }),
          makeJob({ uniqueKey: "k", queue: "other" }),
          makeJob({ uniqueKey: "k2" }),
        );
        expect(await store.findActiveByUniqueKey("q", "k")).toBeUndefined();
      });

      it("returns the oldest match if there are several", async () => {
        const [a] = await insert(makeJob({ uniqueKey: "k" }), makeJob({ uniqueKey: "k", state: "delayed" }));
        expect((await store.findActiveByUniqueKey("q", "k"))?.id).toBe(a!.id);
      });
    });

    describe("listJobs", () => {
      it("lists newest first (by id), 50 by default", async () => {
        const jobs = await insert(...Array.from({ length: 60 }, () => makeJob()));
        const page = await store.listJobs({});
        expect(page.items).toHaveLength(50);
        expect(page.items[0]!.id).toBe(jobs[59]!.id);
        expect(page.items.map((j) => j.id)).toEqual(jobs.slice(10).reverse().map((j) => j.id));
      });

      it("filters by queue, state, name and schedule, combined with AND", async () => {
        const [match] = await insert(
          makeJob({ queue: "a", state: "dead", name: "x", scheduleId: "s" }),
          makeJob({ queue: "b", state: "dead", name: "x", scheduleId: "s" }),
          makeJob({ queue: "a", state: "waiting", name: "x", scheduleId: "s" }),
          makeJob({ queue: "a", state: "dead", name: "y", scheduleId: "s" }),
          makeJob({ queue: "a", state: "dead", name: "x", scheduleId: "t" }),
        );
        const page = await store.listJobs({ queue: "a", state: "dead", name: "x", scheduleId: "s" });
        expect(page.items.map((j) => j.id)).toEqual([match!.id]);
        expect((await store.listJobs({ queue: "a" })).items).toHaveLength(4);
        expect((await store.listJobs({ state: "dead" })).items).toHaveLength(4);
      });

      it("pages through everything exactly once with nextCursor", async () => {
        const jobs = await insert(...Array.from({ length: 23 }, () => makeJob()));
        const seen: string[] = [];
        let cursor: string | undefined;
        for (let i = 0; i < 10; i++) {
          const page = await store.listJobs({ limit: 5, cursor });
          seen.push(...page.items.map((j) => j.id));
          if (!page.nextCursor) break;
          expect(page.nextCursor).toBe(page.items[page.items.length - 1]!.id);
          cursor = page.nextCursor;
        }
        expect(seen).toEqual(jobs.map((j) => j.id).reverse());
      });

      it("has no next page when the last page is exactly full", async () => {
        await insert(...Array.from({ length: 10 }, () => makeJob()));
        const first = await store.listJobs({ limit: 5 });
        const second = await store.listJobs({ limit: 5, cursor: first.nextCursor! });
        expect(second.items).toHaveLength(5);
        expect(second.nextCursor).toBeNull();
      });

      it("clamps the limit to 1..500", async () => {
        await insert(...Array.from({ length: 505 }, () => makeJob()));
        expect((await store.listJobs({ limit: 0 })).items).toHaveLength(1);
        expect((await store.listJobs({ limit: -5 })).items).toHaveLength(1);
        expect((await store.listJobs({ limit: 10_000 })).items).toHaveLength(500);
      });

      it("returns an empty page when nothing matches", async () => {
        expect(await store.listJobs({ queue: "none" })).toEqual({ items: [], nextCursor: null });
      });
    });

    describe("countJobs / countActive / oldestWaiting", () => {
      it("counts by state, per queue or overall, with zeros for empty states", async () => {
        await insert(
          makeJob({ queue: "a", state: "waiting" }),
          makeJob({ queue: "a", state: "waiting" }),
          makeJob({ queue: "a", state: "active" }),
          makeJob({ queue: "b", state: "dead" }),
        );
        const zero = Object.fromEntries(JOB_STATES.map((s) => [s, 0]));
        expect(await store.countJobs("a")).toEqual({ ...zero, waiting: 2, active: 1 });
        expect(await store.countJobs()).toEqual({ ...zero, waiting: 2, active: 1, dead: 1 });
        expect(await store.countJobs("none")).toEqual(zero);
      });

      it("counts active jobs in one queue", async () => {
        await insert(makeJob({ state: "active" }), makeJob({ state: "active" }), makeJob({ state: "waiting" }), makeJob({ queue: "b", state: "active" }));
        expect(await store.countActive("q")).toBe(2);
        expect(await store.countActive("none")).toBe(0);
      });

      it("reports the earliest run_at among waiting jobs only", async () => {
        expect(await store.oldestWaiting("q")).toBeNull();
        await insert(
          makeJob({ state: "waiting", runAt: T + 50 }),
          makeJob({ state: "waiting", runAt: T + 10 }),
          makeJob({ state: "delayed", runAt: T - 100 }),
          makeJob({ state: "active", runAt: T - 100 }),
          makeJob({ queue: "b", state: "waiting", runAt: T - 100 }),
        );
        expect(await store.oldestWaiting("q")).toBe(T + 10);
      });
    });

    describe("findClaimCandidate", () => {
      it("only considers waiting jobs that are due, in the given queues", async () => {
        await insert(
          makeJob({ state: "delayed", runAt: T - 1 }),
          makeJob({ state: "active", runAt: T - 1 }),
          makeJob({ state: "completed", runAt: T - 1 }),
          makeJob({ state: "waiting", runAt: T + 1 }),
          makeJob({ state: "waiting", queue: "other", runAt: T - 1 }),
        );
        expect(await store.findClaimCandidate({ queues: ["q"], now: T })).toBeUndefined();
        const [due] = await insert(makeJob({ runAt: T }));
        expect((await store.findClaimCandidate({ queues: ["q"], now: T }))?.id).toBe(due!.id);
      });

      it("orders by priority (high first), then run_at, then id", async () => {
        const [low, , , high, earlyTie, lateTie] = await insert(
          makeJob({ priority: -5, runAt: T - 100 }),
          makeJob({ priority: 0, runAt: T - 3 }),
          makeJob({ priority: 0, runAt: T - 2 }),
          makeJob({ priority: 10, runAt: T }),
          makeJob({ priority: 5, runAt: T - 1 }),
          makeJob({ priority: 5, runAt: T - 1 }),
        );
        const order: string[] = [];
        for (;;) {
          const c = await store.findClaimCandidate({ queues: ["q"], now: T });
          if (!c) break;
          order.push(c.id);
          await store.completeJob(c.id, null, T);
        }
        expect(order[0]).toBe(high!.id);
        expect(order.slice(1, 3)).toEqual([earlyTie!.id, lateTie!.id]);
        expect(order[order.length - 1]).toBe(low!.id);
        expect(order).toHaveLength(6);
      });

      it("searches across several queues and honours exclude", async () => {
        const [a, b] = await insert(makeJob({ queue: "a", priority: 1 }), makeJob({ queue: "b" }));
        expect((await store.findClaimCandidate({ queues: ["a", "b"], now: T }))?.id).toBe(a!.id);
        expect((await store.findClaimCandidate({ queues: ["a", "b"], now: T, exclude: ["a"] }))?.id).toBe(b!.id);
        expect(await store.findClaimCandidate({ queues: ["a", "b"], now: T, exclude: ["a", "b"] })).toBeUndefined();
        expect(await store.findClaimCandidate({ queues: [], now: T })).toBeUndefined();
      });
    });

    describe("lease lifecycle", () => {
      it("activateJob leases the job and counts the attempt", async () => {
        const [j] = await insert(makeJob({ attempts: 1, progress: 0.7 }));
        await store.activateJob(j!.id, { workerId: "w1", leaseToken: "tok", leaseExpiresAt: T + 1000, now: T + 5 });
        expect(await get(j!.id)).toMatchObject({
          state: "active",
          attempts: 2,
          workerId: "w1",
          leaseToken: "tok",
          leaseExpiresAt: T + 1000,
          startedAt: T + 5,
          updatedAt: T + 5,
          progress: 0,
        });
      });

      // BUG(KB-11): activateJob has no `state = 'waiting'` guard, so a second claim of the same
      // candidate re-leases a job that another worker already holds.
      it.fails("activateJob does not re-lease a job that is already active [KB-11]", async () => {
        const [j] = await insert(makeJob());
        await store.activateJob(j!.id, { workerId: "w1", leaseToken: "first", leaseExpiresAt: T + 1000, now: T });
        await store.activateJob(j!.id, { workerId: "w2", leaseToken: "second", leaseExpiresAt: T + 1000, now: T });
        expect(await get(j!.id)).toMatchObject({ workerId: "w1", leaseToken: "first", attempts: 1 });
      });

      it("extendLease moves the expiry of an active job only", async () => {
        const [active, waiting] = await insert(makeJob({ state: "active", leaseExpiresAt: T }), makeJob({ state: "waiting" }));
        await store.extendLease(active!.id, T + 5000, T + 1);
        await store.extendLease(waiting!.id, T + 5000, T + 1);
        expect(await get(active!.id)).toMatchObject({ leaseExpiresAt: T + 5000, updatedAt: T + 1 });
        expect(await get(waiting!.id)).toEqual(waiting);
      });

      it("updateProgress stores the progress", async () => {
        const [j] = await insert(makeJob({ state: "active" }));
        await store.updateProgress(j!.id, 0.42, T + 9);
        expect(await get(j!.id)).toMatchObject({ progress: 0.42, updatedAt: T + 9 });
      });

      it("completeJob records the result and clears the lease and error", async () => {
        const [j] = await insert(makeJob({ state: "active", leaseToken: "tok", leaseExpiresAt: T + 1, lastError: "earlier", workerId: "w1" }));
        await store.completeJob(j!.id, { sum: 5 }, T + 10);
        expect(await get(j!.id)).toMatchObject({
          state: "completed",
          result: { sum: 5 },
          finishedAt: T + 10,
          updatedAt: T + 10,
          progress: 1,
          leaseToken: null,
          leaseExpiresAt: null,
          lastError: null,
          workerId: "w1",
        });
      });

      it("completeJob stores a missing result as null", async () => {
        const [a, b] = await insert(makeJob({ state: "active" }), makeJob({ state: "active" }));
        await store.completeJob(a!.id, null, T);
        await store.completeJob(b!.id, undefined as never, T);
        expect((await get(a!.id)).result).toBeNull();
        expect((await get(b!.id)).result).toBeNull();
      });

      it("scheduleRetry delays the job and releases the lease", async () => {
        const [j] = await insert(makeJob({ state: "active", attempts: 1, workerId: "w1", leaseToken: "tok", leaseExpiresAt: T + 1 }));
        await store.scheduleRetry(j!.id, "boom", T + 2000, T);
        expect(await get(j!.id)).toMatchObject({
          state: "delayed",
          runAt: T + 2000,
          lastError: "boom",
          updatedAt: T,
          attempts: 1,
          workerId: null,
          leaseToken: null,
          leaseExpiresAt: null,
        });
      });

      it("markDead finishes the job with the error", async () => {
        const [j] = await insert(makeJob({ state: "active", leaseToken: "tok", leaseExpiresAt: T + 1 }));
        await store.markDead(j!.id, "gave up", T + 3);
        expect(await get(j!.id)).toMatchObject({ state: "dead", lastError: "gave up", finishedAt: T + 3, leaseToken: null, leaseExpiresAt: null });
      });

      it("cancelJob finishes the job and clears the lease", async () => {
        const [j] = await insert(makeJob({ state: "active", leaseToken: "tok", leaseExpiresAt: T + 1 }));
        await store.cancelJob(j!.id, T + 4);
        expect(await get(j!.id)).toMatchObject({ state: "cancelled", finishedAt: T + 4, updatedAt: T + 4, leaseToken: null, leaseExpiresAt: null });
      });

      // BUG(KB-13): cancelJob (like the reaper's requeueJob) updates by id only, so a cancel that
      // races a completion turns a completed job into a cancelled one.
      it.fails("cancelJob leaves a job that already finished alone [KB-13]", async () => {
        const [j] = await insert(makeJob({ state: "completed", result: { ok: 1 }, finishedAt: T }));
        await store.cancelJob(j!.id, T + 1);
        expect(await get(j!.id)).toMatchObject({ state: "completed", finishedAt: T });
      });

      it("requeueJob makes the job waiting now, optionally resetting attempts", async () => {
        const [dead, active] = await insert(
          makeJob({ state: "dead", attempts: 3, finishedAt: T, runAt: T - 500 }),
          makeJob({ state: "active", attempts: 2, workerId: "w1", leaseToken: "tok", leaseExpiresAt: T }),
        );
        await store.requeueJob(dead!.id, T + 7, true);
        await store.requeueJob(active!.id, T + 7, false);
        expect(await get(dead!.id)).toMatchObject({ state: "waiting", attempts: 0, runAt: T + 7, updatedAt: T + 7, finishedAt: null });
        expect(await get(active!.id)).toMatchObject({
          state: "waiting",
          attempts: 2,
          runAt: T + 7,
          workerId: null,
          leaseToken: null,
          leaseExpiresAt: null,
        });
      });

      it("findExpiredLeases returns active jobs whose lease ended before now, oldest first", async () => {
        const [late, early] = await insert(
          makeJob({ state: "active", leaseExpiresAt: T - 10 }),
          makeJob({ state: "active", leaseExpiresAt: T - 20 }),
          makeJob({ state: "active", leaseExpiresAt: T }), // expires exactly now: not yet
          makeJob({ state: "active", leaseExpiresAt: T + 10 }),
          makeJob({ state: "waiting", leaseExpiresAt: T - 50 }),
        );
        expect((await store.findExpiredLeases(T, 10)).map((j) => j.id)).toEqual([early!.id, late!.id]);
        expect((await store.findExpiredLeases(T, 1)).map((j) => j.id)).toEqual([early!.id]);
      });

      // Releasing is a clean hand-back, not a failure, so the attempt it used is given back.
      it("releaseLease makes the job waiting again and gives back the attempt", async () => {
        const [j] = await insert(makeJob({ runAt: T - 100 }));
        await store.activateJob(j!.id, { workerId: "w1", leaseToken: "tok", leaseExpiresAt: T + 1000, now: T });
        await store.releaseLease(j!.id, T + 1);
        expect(await get(j!.id)).toMatchObject({
          state: "waiting",
          attempts: 0,
          updatedAt: T + 1,
          workerId: null,
          leaseToken: null,
          leaseExpiresAt: null,
          runAt: T - 100, // keeps its place in the queue
        });
      });

      it("releaseLease never takes attempts below zero", async () => {
        const [j] = await insert(makeJob({ state: "active", attempts: 0 }));
        await store.releaseLease(j!.id, T);
        expect((await get(j!.id)).attempts).toBe(0);
      });
    });

    describe("promoteDelayed", () => {
      it("moves due delayed jobs to waiting and returns how many", async () => {
        const [due, exactlyDue, future, waiting] = await insert(
          makeJob({ state: "delayed", runAt: T - 1 }),
          makeJob({ state: "delayed", runAt: T }),
          makeJob({ state: "delayed", runAt: T + 1 }),
          makeJob({ state: "waiting", runAt: T - 1 }),
        );
        expect(await store.promoteDelayed(T, 100)).toBe(2);
        expect(await get(due!.id)).toMatchObject({ state: "waiting", updatedAt: T, runAt: T - 1 });
        expect((await get(exactlyDue!.id)).state).toBe("waiting");
        expect((await get(future!.id)).state).toBe("delayed");
        expect(await get(waiting!.id)).toEqual(waiting);
      });

      it("promotes at most `limit` jobs per call", async () => {
        await insert(...Array.from({ length: 5 }, () => makeJob({ state: "delayed", runAt: T - 1 })));
        expect(await store.promoteDelayed(T, 3)).toBe(3);
        expect(await store.promoteDelayed(T, 3)).toBe(2);
        expect(await store.promoteDelayed(T, 3)).toBe(0);
      });
    });

    describe("deleteFinishedBefore", () => {
      // The README promises dead jobs are kept for inspection, so retention only removes
      // completed and cancelled jobs.
      it("deletes completed and cancelled jobs that finished before the cutoff, and keeps dead ones", async () => {
        const [completed, cancelled, dead] = await insert(
          makeJob({ state: "completed", finishedAt: T - 10 }),
          makeJob({ state: "cancelled", finishedAt: T - 10 }),
          makeJob({ state: "dead", finishedAt: T - 10 }),
        );
        expect(await store.deleteFinishedBefore(T, 100)).toBe(2);
        expect(await store.getJob(completed!.id)).toBeUndefined();
        expect(await store.getJob(cancelled!.id)).toBeUndefined();
        expect(await store.getJob(dead!.id)).toEqual(dead);
      });

      it("keeps recently finished and unfinished jobs", async () => {
        const kept = await insert(
          makeJob({ state: "completed", finishedAt: T }), // exactly at the cutoff
          makeJob({ state: "completed", finishedAt: T + 1 }),
          makeJob({ state: "waiting" }),
          makeJob({ state: "delayed" }),
          makeJob({ state: "active" }),
        );
        expect(await store.deleteFinishedBefore(T, 100)).toBe(0);
        for (const j of kept) expect(await store.getJob(j.id)).toBeDefined();
      });

      it("deletes at most `limit` jobs per call", async () => {
        await insert(...Array.from({ length: 5 }, () => makeJob({ state: "completed", finishedAt: T - 1 })));
        expect(await store.deleteFinishedBefore(T, 2)).toBe(2);
        expect(await store.deleteFinishedBefore(T, 10)).toBe(3);
      });

      it("deletes the jobs' event history with them", async () => {
        const [j] = await insert(makeJob({ state: "completed", finishedAt: T - 1 }));
        await store.appendJobEvent(j!.id, "completed", null, T - 1);
        await store.deleteFinishedBefore(T, 10);
        expect(await store.listJobEvents(j!.id)).toEqual([]);
      });
    });

    describe("hasUnfinishedForSchedule", () => {
      it("is true while a job from the schedule is delayed, waiting or active", async () => {
        expect(await store.hasUnfinishedForSchedule("s")).toBe(false);
        await insert(makeJob({ scheduleId: "s", state: "completed" }), makeJob({ scheduleId: "s", state: "dead" }), makeJob({ scheduleId: "other" }));
        expect(await store.hasUnfinishedForSchedule("s")).toBe(false);
        for (const state of ["delayed", "waiting", "active"] as JobState[]) {
          const [j] = await insert(makeJob({ scheduleId: `s-${state}`, state }));
          expect(await store.hasUnfinishedForSchedule(`s-${state}`)).toBe(true);
          await store.completeJob(j!.id, null, T);
          expect(await store.hasUnfinishedForSchedule(`s-${state}`)).toBe(false);
        }
      });
    });

    // BUG(KB-14): nothing in the store stops two unfinished jobs with the same unique key, so
    // two concurrent enqueues that both pass Conveyor's check both get inserted.
    it.fails("rejects a second unfinished job with the same queue and unique key [KB-14]", async () => {
      await insert(makeJob({ uniqueKey: "user:1" }));
      await expect(store.insertJob(makeJob({ uniqueKey: "user:1" }))).rejects.toThrow();
    });

    describe("job events", () => {
      it("appends and lists events in order, with optional data", async () => {
        const [j, other] = await insert(makeJob(), makeJob());
        await store.appendJobEvent(j!.id, "enqueued", { state: "waiting" }, T);
        await store.appendJobEvent(j!.id, "claimed", null, T + 1);
        await store.appendJobEvent(other!.id, "enqueued", null, T);
        const events = await store.listJobEvents(j!.id);
        expect(events.map(({ jobId, type, data, at }) => ({ jobId, type, data, at }))).toEqual([
          { jobId: j!.id, type: "enqueued", data: { state: "waiting" }, at: T },
          { jobId: j!.id, type: "claimed", data: null, at: T + 1 },
        ]);
        expect(events[0]!.id).toBeLessThan(events[1]!.id);
      });

      it("lists nothing for an unknown job", async () => {
        expect(await store.listJobEvents("nope")).toEqual([]);
      });

      it("refuses events for a job that does not exist", async () => {
        await expect(store.appendJobEvent("nope", "enqueued", null, T)).rejects.toThrow();
      });
    });

    describe("queues", () => {
      it("getQueue returns undefined for a queue without settings", async () => {
        expect(await store.getQueue("q")).toBeUndefined();
      });

      it("upsertQueue inserts, then updates everything except createdAt", async () => {
        await store.upsertQueue(makeQueue({ name: "q", createdAt: T, updatedAt: T }));
        expect(await store.getQueue("q")).toEqual(makeQueue({ name: "q" }));
        const changed = makeQueue({
          name: "q",
          paused: true,
          concurrency: 4,
          rateLimitMax: 10,
          rateLimitIntervalMs: 1000,
          defaultMaxAttempts: 9,
          createdAt: T + 999,
          updatedAt: T + 5,
        });
        await store.upsertQueue(changed);
        expect(await store.getQueue("q")).toEqual({ ...changed, createdAt: T });
      });

      it("listQueues includes queues that only exist through their jobs, sorted by name", async () => {
        await store.upsertQueue(makeQueue({ name: "m", concurrency: 2 }));
        await insert(
          makeJob({ queue: "z", createdAt: T + 5 }),
          makeJob({ queue: "z", createdAt: T + 2 }),
          makeJob({ queue: "a", createdAt: T + 1 }),
          makeJob({ queue: "m" }),
        );
        const queues = await store.listQueues();
        expect(queues.map((q) => q.name)).toEqual(["a", "m", "z"]);
        expect(queues[1]).toMatchObject({ name: "m", concurrency: 2 });
        expect(queues[2]).toEqual({
          name: "z",
          paused: false,
          concurrency: 0,
          rateLimitMax: 0,
          rateLimitIntervalMs: 0,
          defaultMaxAttempts: 3,
          createdAt: T + 2,
          updatedAt: T + 2,
        });
      });
    });

    describe("schedules", () => {
      it("inserts and reads by id and by name", async () => {
        const s = makeSchedule({ payload: { n: 1 }, timezone: "Europe/London", overlap: "skip", enabled: false, lastRunAt: T - 60_000, lastJobId: "j1" });
        await store.insertSchedule(s);
        expect(await store.getSchedule(s.id)).toEqual(s);
        expect(await store.getScheduleByName(s.name)).toEqual(s);
        expect(await store.getSchedule("nope")).toBeUndefined();
        expect(await store.getScheduleByName("nope")).toBeUndefined();
      });

      it("rejects a duplicate name", async () => {
        await store.insertSchedule(makeSchedule({ name: "nightly" }));
        await expect(store.insertSchedule(makeSchedule({ name: "nightly" }))).rejects.toThrow();
      });

      it("updates every field except id and createdAt", async () => {
        const s = makeSchedule();
        await store.insertSchedule(s);
        const changed: Schedule = {
          ...s,
          name: "renamed",
          queue: "q2",
          jobName: "j2",
          payload: [1],
          cron: "0 0 * * *",
          timezone: "Asia/Tokyo",
          enabled: false,
          overlap: "skip",
          nextRunAt: T + 1,
          lastRunAt: T,
          lastJobId: "j9",
          createdAt: T + 12345,
          updatedAt: T + 2,
        };
        await store.updateSchedule(changed);
        expect(await store.getSchedule(s.id)).toEqual({ ...changed, createdAt: T });
      });

      it("lists by name and deletes", async () => {
        const b = makeSchedule({ name: "b" });
        const a = makeSchedule({ name: "a" });
        await store.insertSchedule(b);
        await store.insertSchedule(a);
        expect((await store.listSchedules()).map((s) => s.name)).toEqual(["a", "b"]);
        expect(await store.deleteSchedule(a.id)).toBe(true);
        expect(await store.deleteSchedule(a.id)).toBe(false);
        expect((await store.listSchedules()).map((s) => s.name)).toEqual(["b"]);
      });

      it("finds enabled schedules that are due, soonest first, up to a limit", async () => {
        const later = makeSchedule({ nextRunAt: T - 1 });
        const sooner = makeSchedule({ nextRunAt: T - 5 });
        const exactly = makeSchedule({ nextRunAt: T });
        await store.insertSchedule(later);
        await store.insertSchedule(sooner);
        await store.insertSchedule(exactly);
        await store.insertSchedule(makeSchedule({ nextRunAt: T + 1 }));
        await store.insertSchedule(makeSchedule({ nextRunAt: T - 100, enabled: false }));
        expect((await store.findDueSchedules(T, 10)).map((s) => s.id)).toEqual([sooner.id, later.id, exactly.id]);
        expect((await store.findDueSchedules(T, 1)).map((s) => s.id)).toEqual([sooner.id]);
      });
    });

    describe("webhooks and deliveries", () => {
      it("inserts, lists in creation order, gets and deletes webhooks", async () => {
        const w1 = makeWebhook({ events: ["job.completed", "job.dead"], queues: ["a", "b"], enabled: false, createdAt: T + 1 });
        const w0 = makeWebhook({ createdAt: T });
        await store.insertWebhook(w1);
        await store.insertWebhook(w0);
        expect(await store.listWebhooks()).toEqual([w0, w1]);
        expect(await store.getWebhook(w1.id)).toEqual(w1);
        expect(await store.getWebhook("nope")).toBeUndefined();
        expect(await store.deleteWebhook(w1.id)).toBe(true);
        expect(await store.deleteWebhook(w1.id)).toBe(false);
        expect(await store.listWebhooks()).toEqual([w0]);
      });

      it("stores, updates and lists deliveries newest first, per webhook, up to a limit", async () => {
        const w = makeWebhook();
        const other = makeWebhook();
        await store.insertWebhook(w);
        await store.insertWebhook(other);
        const d1 = makeDelivery(w.id, { createdAt: T });
        const d2 = makeDelivery(w.id, { createdAt: T + 1 });
        await store.insertDelivery(d1);
        await store.insertDelivery(d2);
        await store.insertDelivery(makeDelivery(other.id));
        const updated: WebhookDelivery = {
          ...d1,
          status: "failed",
          attempts: 5,
          lastStatusCode: 503,
          lastError: "HTTP 503",
          nextAttemptAt: null,
          updatedAt: T + 60_000,
        };
        await store.updateDelivery(updated);
        expect(await store.listDeliveries(w.id, 10)).toEqual([d2, updated]);
        expect(await store.listDeliveries(w.id, 1)).toEqual([d2]);
      });

      it("finds pending deliveries across webhooks, soonest next attempt first, up to a limit", async () => {
        const a = makeWebhook();
        const b = makeWebhook();
        await store.insertWebhook(a);
        await store.insertWebhook(b);
        const later = makeDelivery(a.id, { nextAttemptAt: T + 500, attempts: 1 });
        const sooner = makeDelivery(b.id, { nextAttemptAt: T - 500, attempts: 2 });
        const never = makeDelivery(a.id, { nextAttemptAt: T, attempts: 0 });
        await store.insertDelivery(later);
        await store.insertDelivery(sooner);
        await store.insertDelivery(never);
        await store.insertDelivery(makeDelivery(a.id, { status: "succeeded", nextAttemptAt: null }));
        await store.insertDelivery(makeDelivery(b.id, { status: "failed", nextAttemptAt: null }));
        expect(await store.findPendingDeliveries(10)).toEqual([sooner, never, later]);
        expect(await store.findPendingDeliveries(1)).toEqual([sooner]);
        await store.updateDelivery({ ...sooner, status: "succeeded", nextAttemptAt: null });
        expect(await store.findPendingDeliveries(10)).toEqual([never, later]);
      });

      it("finds no pending deliveries when there are none", async () => {
        expect(await store.findPendingDeliveries(10)).toEqual([]);
      });

      it("deletes a webhook's deliveries with it", async () => {
        const w = makeWebhook();
        await store.insertWebhook(w);
        await store.insertDelivery(makeDelivery(w.id));
        await store.deleteWebhook(w.id);
        expect(await store.listDeliveries(w.id, 10)).toEqual([]);
      });

      it("refuses deliveries for a webhook that does not exist", async () => {
        await expect(store.insertDelivery(makeDelivery("nope"))).rejects.toThrow();
      });
    });

    describe("api tokens", () => {
      it("inserts, finds by hash and lists in creation order", async () => {
        const t1 = makeToken({ scopes: ["admin", "workers"], createdAt: T + 1 });
        const t0 = makeToken({ createdAt: T });
        await store.insertToken(t1);
        await store.insertToken(t0);
        expect(await store.findTokenByHash(t1.hash)).toEqual(t1);
        expect(await store.findTokenByHash("nope")).toBeUndefined();
        expect(await store.listTokens()).toEqual([t0, t1]);
      });

      it("rejects a duplicate hash", async () => {
        await store.insertToken(makeToken({ hash: "same" }));
        await expect(store.insertToken(makeToken({ hash: "same" }))).rejects.toThrow();
      });

      it("revokes once", async () => {
        const t = makeToken();
        await store.insertToken(t);
        expect(await store.revokeToken(t.id, T + 1)).toBe(true);
        expect(await store.revokeToken(t.id, T + 2)).toBe(false);
        expect(await store.revokeToken("nope", T)).toBe(false);
        expect((await store.findTokenByHash(t.hash))?.revokedAt).toBe(T + 1);
      });

      it("records when a token was last used", async () => {
        const t = makeToken();
        await store.insertToken(t);
        await store.touchToken(t.id, T + 99);
        expect((await store.findTokenByHash(t.hash))?.lastUsedAt).toBe(T + 99);
      });
    });
  });
}
