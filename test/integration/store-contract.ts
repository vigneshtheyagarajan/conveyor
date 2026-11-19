// Behaviour every Store implementation must have. Run it with describeStoreContract() from a
// *.test.ts file; see sqlite-store.test.ts.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ConflictError, ValidationError } from "../../src/core/errors.js";
import type { ApiToken, FailurePolicy, Job, JobState, QueueSettings, Schedule, Webhook, WebhookDelivery } from "../../src/core/types.js";
import { isVisibleToDependent } from "../../src/core/dependencies.js";
import { FAILURE_POLICIES, JOB_STATES } from "../../src/core/types.js";
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
    flowId: null,
    flowKey: null,
    terminatedBy: null,
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
          flowId: "flow-1",
          flowKey: "resize",
          terminatedBy: "job-parent",
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
        for (const state of ["blocked", "delayed", "waiting", "active"] as JobState[]) {
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
          await store.cancelJob(c.id, T);
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
        const res = await store.completeJob(j!.id, "tok", { sum: 5 }, T + 10);
        expect(res).toEqual({ job: await get(j!.id), from: "active", dependents: [], flowOutcomes: [] });
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
        const [a, b] = await insert(makeJob({ state: "active", leaseToken: "a" }), makeJob({ state: "active", leaseToken: "b" }));
        await store.completeJob(a!.id, "a", null, T);
        await store.completeJob(b!.id, "b", undefined as never, T);
        expect((await get(a!.id)).result).toBeNull();
        expect((await get(b!.id)).result).toBeNull();
      });

      it("scheduleRetry delays the job and releases the lease", async () => {
        const [j] = await insert(makeJob({ state: "active", attempts: 1, workerId: "w1", leaseToken: "tok", leaseExpiresAt: T + 1 }));
        expect((await store.scheduleRetry(j!.id, "tok", "boom", T + 2000, T))?.job.state).toBe("delayed");
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
        expect((await store.markDead(j!.id, "tok", "gave up", T + 3))?.from).toBe("active");
        expect(await get(j!.id)).toMatchObject({ state: "dead", lastError: "gave up", finishedAt: T + 3, leaseToken: null, leaseExpiresAt: null });
      });

      it("cancelJob finishes the job and clears the lease", async () => {
        const [j] = await insert(makeJob({ state: "active", leaseToken: "tok", leaseExpiresAt: T + 1 }));
        expect((await store.cancelJob(j!.id, T + 4))?.from).toBe("active");
        expect(await get(j!.id)).toMatchObject({ state: "cancelled", finishedAt: T + 4, updatedAt: T + 4, leaseToken: null, leaseExpiresAt: null });
      });

      // Regression (was KB-13): cancelJob (like the reaper's requeueJob) updated by id only, so
      // a cancel that raced a completion turned a completed job into a cancelled one.
      it("cancelJob leaves a job that already finished alone [KB-13]", async () => {
        const [j] = await insert(makeJob({ state: "completed", result: { ok: 1 }, finishedAt: T }));
        expect(await store.cancelJob(j!.id, T + 1)).toBeUndefined();
        expect(await get(j!.id)).toMatchObject({ state: "completed", finishedAt: T });
        expect(await store.listJobEvents(j!.id)).toEqual([]);
      });

      it("retryJob makes a dead or cancelled job waiting now with its attempts reset", async () => {
        const [dead, cancelled] = await insert(
          makeJob({ state: "dead", attempts: 3, finishedAt: T, runAt: T - 500, workerId: "w1" }),
          makeJob({ state: "cancelled", attempts: 1, finishedAt: T }),
        );
        expect((await store.retryJob(dead!.id, T + 7))?.from).toBe("dead");
        expect((await store.retryJob(cancelled!.id, T + 7))?.from).toBe("cancelled");
        for (const j of [dead!, cancelled!]) {
          expect(await get(j.id)).toMatchObject({ state: "waiting", attempts: 0, runAt: T + 7, updatedAt: T + 7, finishedAt: null, workerId: null });
        }
        expect((await store.listJobEvents(dead!.id)).map((e) => [e.type, e.data])).toEqual([["retried", { previousState: "dead", state: "waiting" }]]);
      });

      it("requeueExpired makes an expired active job waiting now and keeps its attempts", async () => {
        const [j] = await insert(makeJob({ state: "active", attempts: 2, workerId: "w1", leaseToken: "tok", leaseExpiresAt: T }));
        expect((await store.requeueExpired(j!.id, "tok", T + 7))?.from).toBe("active");
        expect(await get(j!.id)).toMatchObject({
          state: "waiting",
          attempts: 2,
          runAt: T + 7,
          workerId: null,
          leaseToken: null,
          leaseExpiresAt: null,
        });
        expect((await store.listJobEvents(j!.id)).map((e) => [e.type, e.data])).toEqual([["lease_expired", { workerId: "w1" }]]);
      });

      // Regression (was KB-13): the reaper's requeue updated by id only, so a worker that
      // completed (or heartbeated) between findExpiredLeases and the requeue was overwritten.
      it("requeueExpired leaves a job alone once it finished, was re-leased or its lease was extended", async () => {
        const [done, released, extended, otherLease] = await insert(
          makeJob({ state: "completed", leaseToken: null, leaseExpiresAt: null }),
          makeJob({ state: "waiting", leaseToken: null, leaseExpiresAt: null }),
          makeJob({ state: "active", leaseToken: "tok", leaseExpiresAt: T + 10 }),
          makeJob({ state: "active", leaseToken: "new", leaseExpiresAt: T - 10 }),
        );
        for (const j of [done!, released!, extended!]) expect(await store.requeueExpired(j.id, "tok", T)).toBeUndefined();
        expect(await store.requeueExpired(otherLease!.id, "tok", T)).toBeUndefined();
        for (const j of [done!, released!, extended!, otherLease!]) expect(await get(j.id)).toEqual(j);
      });

      it("requeueExpired reaps an expired active job that has no lease token", async () => {
        const [j] = await insert(makeJob({ state: "active", leaseToken: null, leaseExpiresAt: T - 1 }));
        expect((await store.requeueExpired(j!.id, null, T))?.job.state).toBe("waiting");
      });

      it("every leased transition needs the job to be active with the same lease token", async () => {
        const ops = [
          (id: string, tok: string) => store.completeJob(id, tok, { r: 1 }, T + 1),
          (id: string, tok: string) => store.scheduleRetry(id, tok, "e", T + 100, T + 1),
          (id: string, tok: string) => store.markDead(id, tok, "e", T + 1),
          (id: string, tok: string) => store.releaseLease(id, tok, T + 1),
          (id: string, tok: string) => store.requeueExpired(id, tok, T + 1),
        ];
        for (const op of ops) {
          const [active, cancelled] = await insert(
            makeJob({ state: "active", leaseToken: "tok", leaseExpiresAt: T }),
            makeJob({ state: "cancelled", leaseToken: "tok", finishedAt: T }),
          );
          expect(await op(active!.id, "wrong")).toBeUndefined();
          expect(await op(cancelled!.id, "tok")).toBeUndefined();
          expect(await op("nope", "tok")).toBeUndefined();
          expect(await get(active!.id)).toEqual(active);
          expect(await get(cancelled!.id)).toEqual(cancelled);
          expect(await op(active!.id, "tok")).toBeDefined();
        }
      });

      it("cancelJob and retryJob change nothing for a job in the wrong state", async () => {
        const [waiting, dead] = await insert(makeJob({ state: "waiting" }), makeJob({ state: "dead", finishedAt: T }));
        expect(await store.retryJob(waiting!.id, T)).toBeUndefined();
        expect(await store.retryJob("nope", T)).toBeUndefined();
        expect(await store.cancelJob(dead!.id, T)).toBeUndefined();
        expect(await store.cancelJob("nope", T)).toBeUndefined();
        expect(await get(waiting!.id)).toEqual(waiting);
        expect(await get(dead!.id)).toEqual(dead);
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
        await store.releaseLease(j!.id, "tok", T + 1);
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
        const [j] = await insert(makeJob({ state: "active", attempts: 0, leaseToken: "tok" }));
        await store.releaseLease(j!.id, "tok", T);
        expect((await get(j!.id)).attempts).toBe(0);
      });
    });

    describe("dependencies", () => {
      /** A leased, running job. */
      const running = (over: Partial<Job> = {}) => makeJob({ state: "active", leaseToken: "tok", leaseExpiresAt: T + 1000, attempts: 1, ...over });

      type Dep = string | [string, FailurePolicy];
      /** Insert a blocked job that depends on `deps` (default policy fail). */
      async function child(deps: Dep[], over: Partial<Job> = {}) {
        const job = makeJob({ state: "blocked", ...over });
        const settled = await store.insertJobs(
          [job],
          [],
          deps.map((d) => (typeof d === "string" ? { childId: job.id, parentId: d, onFailure: "fail" } : { childId: job.id, parentId: d[0], onFailure: d[1] })),
        );
        return { id: job.id, settled };
      }

      const events = async (id: string) => (await store.listJobEvents(id)).map((e) => [e.type, e.data]);
      const states = async (...ids: string[]) => Promise.all(ids.map(async (id) => (await get(id)).state));

      describe("insertJobs with dependencies", () => {
        it("stores the edges and leaves the job blocked while a dependency is unfinished", async () => {
          const [a, b] = await insert(running(), makeJob({ state: "delayed" }));
          const c = await child([a!.id, [b!.id, "continue"]]);
          expect(c.settled).toEqual([]);
          expect((await get(c.id)).state).toBe("blocked");
          expect(await store.listDependencies(c.id)).toEqual(
            [
              { jobId: a!.id, onFailure: "fail" },
              { jobId: b!.id, onFailure: "continue" },
            ].sort((x, y) => x.jobId.localeCompare(y.jobId)),
          );
          expect(await store.listDependencies(a!.id)).toEqual([]);
        });

        it("releases the job at once when every dependency has already completed", async () => {
          const [a] = await insert(makeJob({ state: "completed", finishedAt: T - 5 }));
          const c = await child([a!.id], { createdAt: T + 3, updatedAt: T + 3, runAt: T + 3 });
          expect(c.settled).toEqual([{ job: await get(c.id), from: "blocked", cause: null, blockedMs: 0 }]);
          expect(await get(c.id)).toMatchObject({ state: "waiting", runAt: T + 3 });
          expect(await events(c.id)).toEqual([["unblocked", { state: "waiting" }]]);
        });

        it("accepts an ignore or continue dependency that already failed, as settled", async () => {
          const [dead, cancelled] = await insert(makeJob({ state: "dead" }), makeJob({ state: "cancelled" }));
          const c = await child([
            [dead!.id, "ignore"],
            [cancelled!.id, "continue"],
          ]);
          expect(c.settled.map((t) => t.job.state)).toEqual(["waiting"]);
        });

        it.each(["dead", "cancelled"] as JobState[])("rejects a fail dependency that is already %s, storing nothing", async (state) => {
          const [p] = await insert(makeJob({ state }));
          const job = makeJob({ state: "blocked" });
          await expect(store.insertJobs([job], [{ jobId: job.id, type: "enqueued", data: null, at: T }], [{ childId: job.id, parentId: p!.id, onFailure: "fail" }])).rejects.toThrow(
            new ConflictError(`dependency ${p!.id} is already ${state}`),
          );
          expect(await store.getJob(job.id)).toBeUndefined();
          expect(await store.listJobEvents(job.id)).toEqual([]);
        });

        it("rejects a dependency that doesn't exist, storing nothing", async () => {
          const [ok] = await insert(makeJob());
          const job = makeJob({ state: "blocked" });
          const deps = [ok!.id, "nope"].map((parentId) => ({ childId: job.id, parentId, onFailure: "fail" as const }));
          await expect(store.insertJobs([job], [], deps)).rejects.toThrow(new ValidationError("dependency nope not found"));
          expect(await store.getJob(job.id)).toBeUndefined();
          expect(await store.listDependencies(job.id)).toEqual([]);
        });

        it("rejects an edge from a job to itself, a repeated edge and an unknown policy", async () => {
          const [p] = await insert(makeJob());
          for (const deps of [
            (id: string) => [{ childId: id, parentId: id, onFailure: "fail" as const }],
            (id: string) => [
              { childId: id, parentId: p!.id, onFailure: "fail" as const },
              { childId: id, parentId: p!.id, onFailure: "ignore" as const },
            ],
            (id: string) => [{ childId: id, parentId: p!.id, onFailure: "sometimes" as FailurePolicy }],
          ]) {
            const job = makeJob({ state: "blocked" });
            await expect(store.insertJobs([job], [], deps(job.id))).rejects.toThrow();
            expect(await store.getJob(job.id)).toBeUndefined();
          }
        });

        it("lets jobs in the same batch depend on each other", async () => {
          const parent = makeJob();
          const kid = makeJob({ state: "blocked" });
          expect(await store.insertJobs([parent, kid], [], [{ childId: kid.id, parentId: parent.id, onFailure: "fail" }])).toEqual([]);
          expect((await get(kid.id)).state).toBe("blocked");
        });
      });

      describe("completion", () => {
        it("releases a job when its last dependency completes, in the same call, and not before", async () => {
          const [a, b] = await insert(running({ leaseToken: "a" }), running({ leaseToken: "b" }));
          const c = await child([a!.id, b!.id], { runAt: T - 100 });
          expect((await store.completeJob(a!.id, "a", null, T + 10))!.dependents).toEqual([]);
          expect((await get(c.id)).state).toBe("blocked");
          const res = await store.completeJob(b!.id, "b", null, T + 20);
          // Blocked from its insert (updatedAt T) until now.
          expect(res!.dependents).toEqual([{ job: await get(c.id), from: "blocked", cause: b!.id, blockedMs: 20 }]);
          // run_at moves to the release time: time spent blocked isn't time spent in the queue
          expect(await get(c.id)).toMatchObject({ state: "waiting", runAt: T + 20, updatedAt: T + 20 });
          expect(await events(c.id)).toEqual([["unblocked", { state: "waiting" }]]);
        });

        it("releases a job with a later run time as delayed, keeping that time", async () => {
          const [a] = await insert(running());
          const c = await child([a!.id], { runAt: T + 5000 });
          await store.completeJob(a!.id, "tok", null, T + 10);
          expect(await get(c.id)).toMatchObject({ state: "delayed", runAt: T + 5000 });
          expect(await events(c.id)).toEqual([["unblocked", { state: "delayed" }]]);
        });

        it("releases the join of a diamond exactly once", async () => {
          const [root] = await insert(running({ leaseToken: "root" }));
          const left = await child([root!.id]);
          const right = await child([root!.id]);
          const join = await child([left.id, right.id]);
          const res = await store.completeJob(root!.id, "root", null, T + 1);
          expect(res!.dependents.map((t) => t.job.id)).toEqual([left.id, right.id].sort());
          for (const side of [left.id, right.id]) {
            await store.activateJob(side, { workerId: "w", leaseToken: side, leaseExpiresAt: T + 1000, now: T + 2 });
            await store.completeJob(side, side, null, T + 3);
          }
          expect((await get(join.id)).state).toBe("waiting");
          expect((await events(join.id)).filter(([type]) => type === "unblocked")).toHaveLength(1);
        });

        it("leaves a dependent that was cancelled in the meantime alone", async () => {
          const [a] = await insert(running());
          const c = await child([a!.id]);
          await store.cancelJob(c.id, T + 1);
          expect((await store.completeJob(a!.id, "tok", null, T + 2))!.dependents).toEqual([]);
          expect((await get(c.id)).state).toBe("cancelled");
        });
      });

      // markDead is the one method every path to dead goes through (workers failing a job, and
      // the reaper once it dead-letters), so propagation is tested on it directly.
      describe("markDead propagates to dependents", () => {
        it("cancels fail dependents transitively, and releases ignore and continue ones", async () => {
          const [p, other] = await insert(running({ lastError: "old" }), running({ leaseToken: "other" }));
          const a = await child([p!.id]);
          const a2 = await child([a.id]);
          const a3 = await child([[a2.id, "fail"], [other!.id, "continue"]]);
          const ignoring = await child([[p!.id, "ignore"]]);
          const continuing = await child([[p!.id, "continue"]]);
          const waitsForOther = await child([[p!.id, "continue"], other!.id]);
          const failsDespiteOther = await child([p!.id, other!.id]);

          const res = await store.markDead(p!.id, "tok", "boom", T + 7);
          expect(res!.job).toMatchObject({ state: "dead", lastError: "boom", finishedAt: T + 7 });
          expect(await events(p!.id)).toEqual([["dead", { error: "boom", attempts: 1 }]]);

          expect(await get(a.id)).toMatchObject({ state: "cancelled", terminatedBy: p!.id, lastError: `dependency ${p!.id} is dead: boom`, finishedAt: T + 7 });
          expect(await get(a2.id)).toMatchObject({ state: "cancelled", terminatedBy: a.id, lastError: `dependency ${a.id} was cancelled` });
          expect(await get(a3.id)).toMatchObject({ state: "cancelled", terminatedBy: a2.id });
          expect(await events(a.id)).toEqual([["cancelled", { previousState: "blocked", dependencyId: p!.id, dependencyState: "dead" }]]);
          expect(await states(ignoring.id, continuing.id, waitsForOther.id, failsDespiteOther.id)).toEqual(["waiting", "waiting", "blocked", "cancelled"]);

          // Breadth-first: direct dependents in id order, then theirs.
          expect(res!.dependents.map((t) => [t.job.id, t.from, t.job.state, t.cause])).toEqual([
            [a.id, "blocked", "cancelled", p!.id],
            [ignoring.id, "blocked", "waiting", p!.id],
            [continuing.id, "blocked", "waiting", p!.id],
            [failsDespiteOther.id, "blocked", "cancelled", p!.id],
            [a2.id, "blocked", "cancelled", a.id],
            [a3.id, "blocked", "cancelled", a2.id],
          ]);
          // The job that was still blocked on `other` is untouched by what happened to p.
          expect(await events(waitsForOther.id)).toEqual([]);
        });

        it("accepts the token of a lease that has already expired, as the reaper would use it", async () => {
          const [p] = await insert(running({ leaseToken: "expired", leaseExpiresAt: T - 1000 }));
          const c = await child([p!.id]);
          const res = await store.markDead(p!.id, "expired", "lease expired too often", T);
          expect(res!.dependents.map((t) => [t.job.id, t.job.state])).toEqual([[c.id, "cancelled"]]);
        });

        it("propagates nothing when the guard fails", async () => {
          const [p] = await insert(running());
          const c = await child([p!.id]);
          expect(await store.markDead(p!.id, "wrong", "e", T)).toBeUndefined();
          expect(await states(p!.id, c.id)).toEqual(["active", "blocked"]);
        });

        it("scheduling a retry is not a failure: dependents stay blocked", async () => {
          const [p] = await insert(running());
          const c = await child([p!.id]);
          expect((await store.scheduleRetry(p!.id, "tok", "e", T + 100, T))!.dependents).toEqual([]);
          expect((await get(c.id)).state).toBe("blocked");
        });
      });

      describe("cancellation propagates to dependents", () => {
        it.each(["blocked", "delayed", "waiting", "active"] as JobState[])("cancelling a %s job applies its dependents' policies", async (state) => {
          const [p] = await insert(makeJob({ state }));
          const failing = await child([p!.id]);
          const continuing = await child([[p!.id, "continue"]]);
          const res = await store.cancelJob(p!.id, T + 3);
          expect(res!.dependents.map((t) => [t.job.id, t.job.state])).toEqual([
            [failing.id, "cancelled"],
            [continuing.id, "waiting"],
          ]);
          expect(await get(failing.id)).toMatchObject({ terminatedBy: p!.id, lastError: `dependency ${p!.id} was cancelled` });
        });

        it("cancelling a blocked job leaves the jobs it waits for alone", async () => {
          const [p] = await insert(running());
          const c = await child([p!.id]);
          const grandchild = await child([c.id]);
          await store.cancelJob(c.id, T);
          expect(await states(p!.id, c.id, grandchild.id)).toEqual(["active", "cancelled", "cancelled"]);
          expect((await get(c.id)).terminatedBy).toBeNull(); // cancelled directly, not by a dependency
        });
      });

      describe("retryJob", () => {
        it("brings back, as blocked, the dependents a failure cancelled, transitively", async () => {
          const [p] = await insert(running());
          const a = await child([p!.id]);
          const a2 = await child([a.id]);
          const continuing = await child([[p!.id, "continue"]]);
          await store.markDead(p!.id, "tok", "boom", T);

          const res = await store.retryJob(p!.id, T + 50);
          expect(res!.job).toMatchObject({ state: "waiting", attempts: 0, runAt: T + 50 });
          expect(res!.dependents.map((t) => [t.job.id, t.from, t.job.state, t.cause])).toEqual([
            [a.id, "cancelled", "blocked", p!.id],
            [a2.id, "cancelled", "blocked", a.id],
          ]);
          for (const id of [a.id, a2.id]) {
            expect(await get(id)).toMatchObject({ state: "blocked", terminatedBy: null, lastError: null, finishedAt: null, attempts: 0 });
          }
          expect((await events(a2.id)).at(-1)).toEqual(["retried", { previousState: "cancelled", state: "blocked", dependencyId: a.id }]);
          // A dependent that already ran past the failure is not touched.
          expect((await get(continuing.id)).state).toBe("waiting");
        });

        it("cancels a revived dependent again if another fail dependency is still dead or cancelled", async () => {
          const [p, q] = await insert(running({ leaseToken: "p" }), makeJob({ state: "waiting" }));
          const c = await child([p!.id, q!.id]);
          await store.markDead(p!.id, "p", "boom", T);
          await store.cancelJob(q!.id, T + 1);
          const res = await store.retryJob(p!.id, T + 2);
          expect(res!.dependents.map((t) => [t.job.id, t.from, t.job.state])).toEqual([
            [c.id, "cancelled", "blocked"],
            [c.id, "blocked", "cancelled"],
          ]);
          expect(await get(c.id)).toMatchObject({ state: "cancelled", terminatedBy: q!.id });
        });

        it("refuses a job whose fail dependency is still dead or cancelled, and changes nothing", async () => {
          const [p] = await insert(running());
          const c = await child([p!.id]);
          await store.markDead(p!.id, "tok", "boom", T);
          const before = await get(c.id);
          await expect(store.retryJob(c.id, T + 1)).rejects.toThrow(new ConflictError(`dependency ${p!.id} is dead; retry it first`));
          expect(await get(c.id)).toEqual(before);
        });

        it("makes a job with dependencies blocked while one is unfinished, and waiting once all have completed", async () => {
          const [done, pending] = await insert(makeJob({ state: "completed" }), makeJob({ state: "waiting" }));
          const ran = await child([done!.id]); // released at insert
          await store.activateJob(ran.id, { workerId: "w", leaseToken: "r", leaseExpiresAt: T + 1000, now: T });
          await store.markDead(ran.id, "r", "boom", T);
          const stillBlocked = await child([pending!.id]);
          await store.cancelJob(stillBlocked.id, T);
          expect((await store.retryJob(ran.id, T + 1))!.job.state).toBe("waiting");
          expect((await store.retryJob(stillBlocked.id, T + 1))!.job.state).toBe("blocked");
          expect((await events(stillBlocked.id)).at(-1)).toEqual(["retried", { previousState: "cancelled", state: "blocked" }]);
        });
      });

      it("reconcileBlocked finds nothing when every transition settled its dependents", async () => {
        const [p, q] = await insert(running({ leaseToken: "p" }), running({ leaseToken: "q" }));
        await child([p!.id]);
        await child([q!.id]);
        await child([[p!.id, "continue"], q!.id]);
        await store.completeJob(p!.id, "p", null, T);
        expect(await store.reconcileBlocked(T, 100)).toEqual({ jobs: [], flowOutcomes: [] });
        await store.markDead(q!.id, "q", "e", T);
        expect(await store.reconcileBlocked(T, 100)).toEqual({ jobs: [], flowOutcomes: [] });
      });

      it.each(["blocked", "delayed", "waiting", "active"] as JobState[])(
        "retention keeps a finished job while a dependent is %s, and deletes it once the dependent has finished",
        async (state) => {
          const [parent, other] = await insert(makeJob({ state: "completed", finishedAt: T - 10 }), makeJob({ state: "waiting" }));
          // Inserted in `state` directly; only a blocked job is settled at insert, and it stays
          // blocked on `other`.
          const kid = makeJob({ state, runAt: T + 1000, leaseToken: state === "active" ? "k" : null });
          await store.insertJobs([kid], [], [parent!, other!].map((p) => ({ childId: kid.id, parentId: p.id, onFailure: "fail" as const })));
          expect((await get(kid.id)).state).toBe(state);
          expect(await store.deleteFinishedBefore(T, 10)).toBe(0);
          expect(await get(parent!.id)).toEqual(parent);

          await store.cancelJob(kid.id, T + 5);
          expect(await store.deleteFinishedBefore(T, 10)).toBe(1); // the parent; the kid finished after the cutoff
          expect(await store.getJob(parent!.id)).toBeUndefined();
          // The edge outlives the parent, so the kid can never be retried without it.
          expect((await store.listDependencies(kid.id)).map((d) => d.jobId).sort()).toEqual([parent!.id, other!.id].sort());
          expect((await store.listDependencyJobs(kid.id)).map((l) => l.job.id)).toEqual([other!.id]);
          await expect(store.retryJob(kid.id, T + 6)).rejects.toThrow(new ConflictError(`job ${kid.id} can't be retried: its dependency ${parent!.id} no longer exists`));
          expect((await get(kid.id)).state).toBe("cancelled");
        },
      );

      it("retention doesn't keep a job for a dependent that is dead", async () => {
        const [parent] = await insert(makeJob({ state: "completed", finishedAt: T - 10 }));
        const kid = makeJob({ state: "dead", finishedAt: T - 10 });
        await store.insertJobs([kid], [], [{ childId: kid.id, parentId: parent!.id, onFailure: "fail" }]);
        expect(await store.deleteFinishedBefore(T, 10)).toBe(1);
        expect(await get(kid.id)).toEqual(kid);
      });
    });

    describe("flows", () => {
      /** Flow jobs for `flow`, in the order given, keyed k0, k1, ... */
      const flowJobs = (flow: string, ...over: Partial<Job>[]) => over.map((o, i) => makeJob({ flowId: flow, flowKey: `k${i}`, ...o }));
      const edge = (child: Job, parent: Job | string, onFailure: FailurePolicy = "fail") => ({
        childId: child.id,
        parentId: typeof parent === "string" ? parent : parent.id,
        onFailure,
      });
      const everything = async () => ({
        jobs: (await store.listJobs({ limit: 500 })).items,
        events: (await Promise.all((await store.listJobs({ limit: 500 })).items.map((j) => store.listJobEvents(j.id)))).flat(),
      });

      it("inserts the jobs, edges and history, and settles jobs whose outside dependencies completed", async () => {
        const [done] = await insert(makeJob({ state: "completed" }));
        const [a, b, c] = flowJobs("f1", {}, { state: "blocked" }, { state: "blocked" });
        const settled = await store.insertFlow({
          id: "f1",
          jobs: [a!, b!, c!],
          dependencies: [edge(b!, a!), edge(b!, done!), edge(c!, done!, "continue")],
          events: [a!, b!, c!].map((j) => ({ jobId: j.id, type: "enqueued", data: { state: j.state }, at: T })),
        });
        expect(settled.map((t) => [t.job.id, t.job.state])).toEqual([[c!.id, "waiting"]]);
        expect((await store.listFlowJobs("f1")).map((j) => [j.flowKey, j.state])).toEqual([
          ["k0", "waiting"],
          ["k1", "blocked"],
          ["k2", "waiting"],
        ]);
        expect(await store.listFlowDependencies("f1")).toEqual(
          [edge(b!, a!), edge(b!, done!), edge(c!, done!, "continue")].sort((x, y) => `${x.childId}${x.parentId}`.localeCompare(`${y.childId}${y.parentId}`)),
        );
        expect((await store.listJobEvents(b!.id)).map((e) => e.type)).toEqual(["enqueued"]);
        expect(await store.listFlowJobs("other")).toEqual([]);
        expect(await store.listFlowDependencies("other")).toEqual([]);
      });

      it.each<[string, (jobs: Job[], existing: Job[]) => { jobs: Job[]; dependencies: ReturnType<typeof edge>[] }, RegExp]>([
        ["jobs out of dependency order", ([a, b]) => ({ jobs: [a!, b!], dependencies: [edge(a!, b!)] }), /not in dependency order/],
        ["a cycle", ([a, b]) => ({ jobs: [a!, b!], dependencies: [edge(b!, a!), edge(a!, b!)] }), /not in dependency order/],
        ["a self-dependency", ([a]) => ({ jobs: [a!], dependencies: [edge(a!, a!)] }), /not in dependency order/],
        ["an edge from a job outside the flow", ([a], [x]) => ({ jobs: [a!], dependencies: [edge(x!, a!)] }), /not in flow/],
        ["a job of another flow", ([a, b]) => ({ jobs: [a!, { ...b!, flowId: "other" }], dependencies: [] }), /does not belong to flow/],
        ["a repeated key", ([a, b]) => ({ jobs: [a!, { ...b!, flowKey: "k0" }], dependencies: [] }), /keys must be present and unique/],
        ["a missing key", ([a]) => ({ jobs: [{ ...a!, flowKey: null }], dependencies: [] }), /keys must be present and unique/],
        ["an unknown outside dependency", ([a]) => ({ jobs: [a!], dependencies: [edge(a!, "nope")] }), /dependency nope not found/],
        ["a fail dependency that is already dead", ([a], [, dead]) => ({ jobs: [a!], dependencies: [edge(a!, dead!)] }), /is already dead/],
        ["a repeated edge", ([a, b]) => ({ jobs: [a!, b!], dependencies: [edge(b!, a!), edge(b!, a!, "ignore")] }), /./],
      ])("stores nothing for %s", async (_label, build, message) => {
        const existing = await insert(makeJob({ state: "active", leaseToken: "t" }), makeJob({ state: "dead" }));
        const before = await everything();
        const jobs = flowJobs("f", { state: "blocked" }, { state: "blocked" });
        const { jobs: flowJobsToInsert, dependencies } = build(jobs, existing);
        await expect(
          store.insertFlow({ id: "f", jobs: flowJobsToInsert, dependencies, events: flowJobsToInsert.map((j) => ({ jobId: j.id, type: "enqueued", data: null, at: T })) }),
        ).rejects.toThrow(message);
        expect(await everything()).toEqual(before);
        expect(await store.listFlowJobs("f")).toEqual([]);
      });

      it("the ConflictError for a dead fail dependency is the conflict, not a validation error", async () => {
        const [dead] = await insert(makeJob({ state: "cancelled" }));
        const [a] = flowJobs("f", { state: "blocked" });
        await expect(store.insertFlow({ id: "f", jobs: [a!], dependencies: [edge(a!, dead!)], events: [] })).rejects.toThrow(ConflictError);
      });

      it("cancelFlow cancels every unfinished job directly, and settles dependents outside the flow", async () => {
        const [a, b, c, d] = flowJobs("f", { state: "completed" }, { state: "active", leaseToken: "t" }, { state: "blocked" }, { state: "waiting" });
        await store.insertFlow({ id: "f", jobs: [a!, b!, c!, d!], dependencies: [edge(c!, b!)], events: [] });
        const outsideFail = makeJob({ state: "blocked" });
        const outsideContinue = makeJob({ state: "blocked" });
        await store.insertJobs([outsideFail, outsideContinue], [], [edge(outsideFail, c!), edge(outsideContinue, d!, "continue")]);

        const res = await store.cancelFlow("f", T + 9);
        expect(res!.jobs.map((t) => [t.job.flowKey, t.from, t.job.state, t.cause])).toEqual([
          ["k1", "active", "cancelled", null],
          ["k2", "blocked", "cancelled", null],
          ["k3", "waiting", "cancelled", null],
        ]);
        // c was cancelled directly, not by b's cancellation.
        expect(await get(c!.id)).toMatchObject({ terminatedBy: null, leaseToken: null, finishedAt: T + 9 });
        expect((await store.listJobEvents(c!.id)).at(-1)!.data).toEqual({ previousState: "blocked", flowId: "f" });
        expect((await get(a!.id)).state).toBe("completed");
        expect(res!.dependents.map((t) => [t.job.id, t.job.state])).toEqual([
          [outsideFail.id, "cancelled"],
          [outsideContinue.id, "waiting"],
        ]);
        expect(res!.flowOutcomes).toEqual([{ flowId: "f", event: "flow.failed", reason: "cancelled", jobId: null, at: T + 9, dispatchedAt: null }]);
        expect(await store.cancelFlow("f", T + 10)).toEqual({ jobs: [], dependents: [], flowOutcomes: [] });
        expect(await store.cancelFlow("nope", T)).toBeUndefined();
      });

      it("pages a job's dependencies by id with after and limit", async () => {
        const parents = await insert(...Array.from({ length: 5 }, () => makeJob()));
        const kid = makeJob({ state: "blocked" });
        await store.insertJobs([kid], [], parents.map((p) => edge(kid, p)));
        const ids = parents.map((p) => p.id);
        expect((await store.listDependencyJobs(kid.id, { limit: 2 })).map((l) => l.job.id)).toEqual(ids.slice(0, 2));
        expect((await store.listDependencyJobs(kid.id, { after: ids[1], limit: 2 })).map((l) => l.job.id)).toEqual(ids.slice(2, 4));
        expect((await store.listDependencyJobs(kid.id, { after: ids[3] })).map((l) => l.job.id)).toEqual(ids.slice(4));
        expect(await store.listDependencyJobs(kid.id, { after: ids[4] })).toEqual([]);
      });

      // The database and isVisibleToDependent must agree, so that an HTTP worker (which pages
      // through the database) sees the same dependencies as an in-process handler (which
      // filters in JavaScript).
      it("visibleOnly leaves out exactly what isVisibleToDependent does, for every policy and state", async () => {
        const kid = makeJob({ state: "active", leaseToken: "k" });
        const links: { parent: Job; onFailure: FailurePolicy; end?: JobState }[] = [];
        for (const onFailure of FAILURE_POLICIES) {
          for (const state of JOB_STATES) {
            // A fail edge to a job that has already failed is refused at insert, so those
            // parents start out running and fail after the edge exists. (The dependent is
            // active, so settling leaves it alone.)
            const failsLater = state === "dead" || state === "cancelled";
            links.push({ parent: makeJob(failsLater ? { state: "active", leaseToken: "p" } : { state }), onFailure, end: failsLater ? state : undefined });
          }
        }
        await insert(...links.map((l) => l.parent));
        await store.insertJobs([kid], [], links.map((l) => edge(kid, l.parent, l.onFailure)));
        for (const l of links) {
          if (l.end === "dead") await store.markDead(l.parent.id, "p", "boom", T);
          if (l.end === "cancelled") await store.cancelJob(l.parent.id, T);
        }
        expect((await get(kid.id)).state).toBe("active");
        const all = await store.listDependencyJobs(kid.id);
        expect(all).toHaveLength(FAILURE_POLICIES.length * JOB_STATES.length);
        const visible = (await store.listDependencyJobs(kid.id, { visibleOnly: true })).map((l) => l.job.id);
        expect(visible).toEqual(all.filter((l) => isVisibleToDependent({ onFailure: l.onFailure, state: l.job.state })).map((l) => l.job.id));
        expect(visible).toHaveLength(all.length - 2); // ignore + dead, ignore + cancelled
      });

      describe("verdicts", () => {
        async function flowOf(...over: Partial<Job>[]) {
          const jobs = flowJobs("fv", ...over.map((o) => ({ state: "active" as const, leaseToken: "t", ...o })));
          await store.insertFlow({ id: "fv", jobs, dependencies: [], events: [] });
          return jobs;
        }

        it("records flow.completed when the last job completes, once", async () => {
          const [a, b] = await flowOf({}, {});
          expect((await store.completeJob(a!.id, "t", null, T + 1))!.flowOutcomes).toEqual([]);
          const res = await store.completeJob(b!.id, "t", null, T + 2);
          const outcome = { flowId: "fv", event: "flow.completed", reason: null, jobId: null, at: T + 2, dispatchedAt: null };
          expect(res!.flowOutcomes).toEqual([outcome]);
          expect(await store.getFlowOutcome("fv")).toEqual(outcome);
          expect(await store.getFlowOutcome("other")).toBeUndefined();
        });

        it("records flow.failed when a job dies, and nothing more after that", async () => {
          const [a, b, c] = await flowOf({}, {}, {});
          expect((await store.markDead(a!.id, "t", "boom", T + 1))!.flowOutcomes).toEqual([
            { flowId: "fv", event: "flow.failed", reason: "dead", jobId: a!.id, at: T + 1, dispatchedAt: null },
          ]);
          expect((await store.cancelJob(b!.id, T + 2))!.flowOutcomes).toEqual([]);
          expect((await store.completeJob(c!.id, "t", null, T + 3))!.flowOutcomes).toEqual([]);
          expect((await store.getFlowOutcome("fv"))!.reason).toBe("dead");
        });

        it("claims a verdict once, storing its deliveries in the same transaction", async () => {
          const [a] = await flowOf({});
          await store.completeJob(a!.id, "t", null, T);
          const hook = makeWebhook();
          await store.insertWebhook(hook);
          expect(await store.listPendingFlowOutcomes(10)).toEqual([expect.objectContaining({ flowId: "fv" })]);
          expect(await store.claimFlowOutcome("fv", T + 5, [makeDelivery(hook.id)])).toBe(true);
          expect(await store.claimFlowOutcome("fv", T + 6, [makeDelivery(hook.id)])).toBe(false);
          expect(await store.listDeliveries(hook.id, 10)).toHaveLength(1);
          expect(await store.listPendingFlowOutcomes(10)).toEqual([]);
          expect((await store.getFlowOutcome("fv"))!.dispatchedAt).toBe(T + 5);
          expect(await store.claimFlowOutcome("nope", T, [])).toBe(false);
        });

        it("claims nothing if a delivery can't be stored", async () => {
          const [a] = await flowOf({});
          await store.completeJob(a!.id, "t", null, T);
          await expect(store.claimFlowOutcome("fv", T, [makeDelivery("no-such-webhook")])).rejects.toThrow();
          expect((await store.getFlowOutcome("fv"))!.dispatchedAt).toBeNull();
        });

        it("lists pending verdicts oldest first, up to the limit", async () => {
          for (const [i, id] of ["f1", "f2", "f3"].entries()) {
            const j = makeJob({ flowId: id, flowKey: "k", state: "active", leaseToken: "t" });
            await store.insertFlow({ id, jobs: [j], dependencies: [], events: [] });
            await store.completeJob(j.id, "t", null, T + 10 - i);
          }
          expect((await store.listPendingFlowOutcomes(2)).map((o) => o.flowId)).toEqual(["f3", "f2"]);
        });

        it("retention deletes a verdict once its flow has no jobs left, and not before", async () => {
          const [done, dead] = await flowOf({}, {});
          await store.completeJob(done!.id, "t", null, T - 100);
          await store.markDead(dead!.id, "t", "boom", T - 100);
          // The verdict is old enough, but the dead job is kept, so the verdict must stay.
          expect(await store.deleteFinishedBefore(T, 10)).toBe(1);
          expect(await store.getJob(done!.id)).toBeUndefined();
          expect(await store.getFlowOutcome("fv")).toBeDefined();

          const [other] = flowJobs("gone", { state: "active", leaseToken: "t" });
          await store.insertFlow({ id: "gone", jobs: [other!], dependencies: [], events: [] });
          await store.completeJob(other!.id, "t", null, T - 100);
          expect(await store.deleteFinishedBefore(T - 200, 10)).toBe(0); // too recent
          expect(await store.getFlowOutcome("gone")).toBeDefined();
          expect(await store.deleteFinishedBefore(T, 10)).toBe(1);
          expect(await store.getFlowOutcome("gone")).toBeUndefined();
        });
      });

      it("lists a job's dependencies and dependents with their jobs and policies", async () => {
        const [p1, p2] = await insert(makeJob(), makeJob({ state: "completed" }));
        const kids = Array.from({ length: 5 }, () => makeJob({ state: "blocked" }));
        await store.insertJobs(kids, [], [...kids.map((k) => edge(k, p1!, "continue")), edge(kids[0]!, p2!)]);

        expect(await store.listDependencyJobs(kids[0]!.id)).toEqual([
          { onFailure: "continue", job: await get(p1!.id) },
          { onFailure: "fail", job: await get(p2!.id) },
        ]);
        expect(await store.listDependencyJobs(p1!.id)).toEqual([]);

        const first = await store.listDependentJobs(p1!.id, { limit: 2 });
        expect(first.items.map((l) => [l.job.id, l.onFailure])).toEqual([
          [kids[4]!.id, "continue"],
          [kids[3]!.id, "continue"],
        ]);
        expect(first.nextCursor).toBe(kids[3]!.id);
        const rest = await store.listDependentJobs(p1!.id, { limit: 10, cursor: first.nextCursor! });
        expect(rest.items.map((l) => l.job.id)).toEqual([kids[2]!.id, kids[1]!.id, kids[0]!.id]);
        expect(rest.nextCursor).toBeNull();
        expect((await store.listDependentJobs(p2!.id, {})).items.map((l) => l.job.id)).toEqual([kids[0]!.id]);
        expect(await store.listDependentJobs("nope", {})).toEqual({ items: [], nextCursor: null });
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
      it("is true while a job from the schedule is unfinished", async () => {
        expect(await store.hasUnfinishedForSchedule("s")).toBe(false);
        await insert(makeJob({ scheduleId: "s", state: "completed" }), makeJob({ scheduleId: "s", state: "dead" }), makeJob({ scheduleId: "other" }));
        expect(await store.hasUnfinishedForSchedule("s")).toBe(false);
        for (const state of ["blocked", "delayed", "waiting", "active"] as JobState[]) {
          const [j] = await insert(makeJob({ scheduleId: `s-${state}`, state }));
          expect(await store.hasUnfinishedForSchedule(`s-${state}`)).toBe(true);
          await store.cancelJob(j!.id, T);
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
