// Job dependencies through the producer and worker APIs: enqueue with dependsOn, and how
// completing, failing, cancelling and retrying a job settles the jobs that depend on it.
// Store-level behaviour (policies, cascades, revival) is covered by the store contract.
import { afterEach, describe, expect, it, vi } from "vitest";
import { Maintenance } from "../../src/broker/maintenance.js";
import { conveyorMetrics } from "../../src/metrics/conveyor-metrics.js";
import { isVisibleToDependent } from "../../src/core/dependencies.js";
import { MAX_DEPENDENCIES } from "../../src/core/conveyor.js";
import type { Conveyor } from "../../src/core/conveyor.js";
import { ConflictError, ValidationError } from "../../src/core/errors.js";
import type { EventName } from "../../src/core/events.js";
import type { DependencyInput, EnqueueRequest, Job } from "../../src/core/types.js";
import type { SqliteStore } from "../../src/storage/sqlite-store.js";
import { manualClock, memoryLogger, setup } from "../helpers.js";

function recordEvents(conveyor: Conveyor) {
  const events: [EventName, string][] = [];
  conveyor.events.onAny((event, first) => events.push([event, (first as Job)?.id ?? String(first)]));
  return events;
}

async function ctx() {
  const clock = manualClock();
  const s = await setup({ clock });
  const events = recordEvents(s.conveyor);
  /** Enqueue in queue "q" and claim it, so it can be completed or failed. */
  const running = async (req: Partial<EnqueueRequest> = {}) => {
    const job = await s.conveyor.enqueue({ queue: "q", name: "parent", ...req });
    const lease = await s.broker.claim(["q"], "w");
    expect(lease?.job.id).toBe(job.id);
    return { id: job.id, token: lease!.token };
  };
  /** Enqueue a dependent in queue "kids", which nothing claims unless asked. */
  const dependent = (dependsOn: DependencyInput[], req: Partial<EnqueueRequest> = {}) =>
    s.conveyor.enqueue({ queue: "kids", name: "child", dependsOn, ...req });
  const state = async (id: string) => (await s.conveyor.getJob(id)).state;
  return { ...s, clock, events, running, dependent, state };
}

describe("enqueue with dependsOn", () => {
  it("creates a blocked job that can't be claimed until its dependency completes", async () => {
    const { conveyor, broker, clock, events, running, dependent } = await ctx();
    const p = await running();
    const child = await dependent([p.id]);
    expect(child).toMatchObject({ state: "blocked", terminatedBy: null });
    expect(await conveyor.getJob(child.id)).toEqual(child);
    expect(await conveyor.store.listDependencies(child.id)).toEqual([{ jobId: p.id, onFailure: "fail" }]);
    expect((await conveyor.jobEvents(child.id)).map((e) => [e.type, e.data])).toEqual([["enqueued", { state: "blocked" }]]);
    expect(await broker.claim(["kids"], "w")).toBeUndefined();

    clock.advance(30);
    await broker.complete(p.id, p.token);
    expect(events.slice(-2)).toEqual([
      ["job.completed", p.id],
      ["job.unblocked", child.id],
    ]);
    expect((await broker.claim(["kids"], "w"))?.job).toMatchObject({ id: child.id, runAt: clock.now() });
  });

  it("accepts ids and {jobId, onFailure} objects, and several dependencies", async () => {
    const { conveyor, running, dependent } = await ctx();
    const a = await running();
    const b = await running();
    const child = await dependent([a.id, { jobId: b.id, onFailure: "continue" }]);
    expect((await conveyor.store.listDependencies(child.id)).map((d) => d.onFailure).sort()).toEqual(["continue", "fail"]);
  });

  it("returns the job already released when every dependency has completed", async () => {
    const { broker, events, running, dependent } = await ctx();
    const p = await running();
    await broker.complete(p.id, p.token);
    const child = await dependent([p.id], { delayMs: 500 });
    expect(child).toMatchObject({ state: "delayed" });
    expect(events.slice(-2)).toEqual([
      ["job.enqueued", child.id],
      ["job.unblocked", child.id],
    ]);
  });

  it("rejects a fail dependency that is already dead or cancelled, and accepts ignore and continue", async () => {
    const { conveyor, broker, running, dependent } = await ctx();
    const dead = await running({ maxAttempts: 1 });
    await broker.fail(dead.id, dead.token, "boom");
    const cancelled = await conveyor.enqueue({ queue: "q", name: "x" });
    await conveyor.cancel(cancelled.id);
    for (const id of [dead.id, cancelled.id]) {
      await expect(dependent([id])).rejects.toThrow(ConflictError);
      await expect(dependent([{ jobId: id, onFailure: "fail" }])).rejects.toThrow(/is already (dead|cancelled)/);
    }
    expect((await dependent([{ jobId: dead.id, onFailure: "ignore" }])).state).toBe("waiting");
    expect((await dependent([{ jobId: cancelled.id, onFailure: "continue" }])).state).toBe("waiting");
    expect((await conveyor.listJobs({ queue: "kids" })).items).toHaveLength(2);
  });

  it.each<[string, unknown, RegExp]>([
    ["not an array", "abc", /dependsOn must be an array/],
    ["an empty id", [""], /each dependency must be a job id or \{jobId, onFailure\}/],
    ["a number", [42], /each dependency/],
    ["null", [null], /each dependency/],
    ["an object without jobId", [{ onFailure: "fail" }], /each dependency/],
    ["an unknown policy", [{ jobId: "x", onFailure: "retry" }], /onFailure must be one of fail, ignore, continue/],
    ["a repeated id", ["x", { jobId: "x" }], /dependency x is listed twice/],
    ["too many", Array.from({ length: MAX_DEPENDENCIES + 1 }, (_, i) => `j${i}`), /at most 1000 dependencies/],
  ])("rejects %s", async (_label, dependsOn, message) => {
    const { conveyor } = await ctx();
    await expect(conveyor.enqueue({ queue: "q", name: "x", dependsOn: dependsOn as DependencyInput[] })).rejects.toThrow(message);
    await expect(conveyor.enqueue({ queue: "q", name: "x", dependsOn: dependsOn as DependencyInput[] })).rejects.toThrow(ValidationError);
    expect((await conveyor.listJobs({})).items).toEqual([]);
  });

  it("rejects an unknown dependency and, in a bulk enqueue, stores none of the jobs", async () => {
    const { conveyor, events } = await ctx();
    const ok = await conveyor.enqueue({ queue: "q", name: "x" });
    await expect(
      conveyor.enqueueBulk([
        { queue: "q", name: "a", dependsOn: [ok.id] },
        { queue: "q", name: "b", dependsOn: ["missing"] },
      ]),
    ).rejects.toThrow(new ValidationError("dependency missing not found"));
    expect((await conveyor.listJobs({})).items.map((j) => j.id)).toEqual([ok.id]);
    expect(events.filter(([e]) => e === "job.enqueued")).toHaveLength(1);
  });

  it("counts a blocked job as unfinished for unique keys and schedule overlap", async () => {
    const { conveyor, running, dependent } = await ctx();
    const p = await running();
    const child = await dependent([p.id], { uniqueKey: "k" });
    expect((await dependent([p.id], { uniqueKey: "k" })).id).toBe(child.id);
    expect(await conveyor.store.findActiveByUniqueKey("kids", "k")).toMatchObject({ id: child.id });
  });

  it("shows blocked jobs in queue stats, and not in oldestWaitingAt until released", async () => {
    const { conveyor, broker, clock, running, dependent } = await ctx();
    const p = await running();
    await dependent([p.id]);
    clock.advance(60_000);
    const stats = await conveyor.queueStats("kids");
    expect(stats.counts).toMatchObject({ blocked: 1, waiting: 0 });
    expect(stats.oldestWaitingAt).toBeNull();
    await broker.complete(p.id, p.token);
    // Released now, so the queue doesn't look a minute behind.
    expect((await conveyor.queueStats("kids")).oldestWaitingAt).toBe(clock.now());
  });
});

describe("failure and cancellation propagate by policy", () => {
  it("a dependency that dies cancels fail dependents (job.cancelled, not job.failed or job.dead) and releases the others", async () => {
    const { broker, events, running, dependent, state } = await ctx();
    const p = await running({ maxAttempts: 1 });
    const failing = await dependent([p.id]);
    const grandchild = await dependent([failing.id]);
    const ignoring = await dependent([{ jobId: p.id, onFailure: "ignore" }]);
    const continuing = await dependent([{ jobId: p.id, onFailure: "continue" }]);

    await broker.fail(p.id, p.token, new Error("boom"));
    expect([await state(failing.id), await state(grandchild.id), await state(ignoring.id), await state(continuing.id)]).toEqual([
      "cancelled",
      "cancelled",
      "waiting",
      "waiting",
    ]);
    expect(events.slice(-6)).toEqual([
      ["job.failed", p.id],
      ["job.dead", p.id],
      ["job.cancelled", failing.id],
      ["job.unblocked", ignoring.id],
      ["job.unblocked", continuing.id],
      ["job.cancelled", grandchild.id],
    ]);
  });

  it("a retry of the dependency is not a failure: dependents stay blocked", async () => {
    const { broker, running, dependent, state } = await ctx();
    const p = await running({ maxAttempts: 2 });
    const child = await dependent([p.id]);
    expect((await broker.fail(p.id, p.token, "transient")).state).toBe("delayed");
    expect(await state(child.id)).toBe("blocked");
  });

  it("cancelling a job cascades to its fail dependents and is logged for each", async () => {
    const { logger, records } = memoryLogger();
    const s = await setup({ clock: manualClock(), logger });
    const events = recordEvents(s.conveyor);
    const p = await s.conveyor.enqueue({ queue: "q", name: "x" });
    const child = await s.conveyor.enqueue({ queue: "q", name: "y", dependsOn: [p.id] });
    const cancelled = await s.conveyor.cancel(p.id);
    expect(cancelled.state).toBe("cancelled");
    expect(events.slice(-2)).toEqual([
      ["job.cancelled", p.id],
      ["job.cancelled", child.id],
    ]);
    expect(await s.conveyor.getJob(child.id)).toMatchObject({ state: "cancelled", terminatedBy: p.id, lastError: `dependency ${p.id} was cancelled` });
    expect(records).toContainEqual(expect.objectContaining({ level: "info", msg: "job cancelled by its dependency", jobId: child.id, dependencyId: p.id }));
  });

  it("cancelling a job whose dependents already finished is still just a conflict", async () => {
    const { conveyor, broker, running, dependent } = await ctx();
    const p = await running();
    await dependent([p.id]);
    await broker.complete(p.id, p.token);
    await expect(conveyor.cancel(p.id)).rejects.toThrow(`job ${p.id} is already completed`);
  });
});

describe("retry brings back what a failure cancelled", () => {
  it("retrying the failed dependency revives its cancelled dependents as blocked, and they run after it", async () => {
    const { conveyor, broker, events, running, dependent, state } = await ctx();
    const p = await running({ maxAttempts: 1 });
    const child = await dependent([p.id]);
    const grandchild = await dependent([child.id]);
    await broker.fail(p.id, p.token, "boom");

    const retried = await conveyor.retry(p.id);
    expect(retried.state).toBe("waiting");
    expect(events.slice(-3)).toEqual([
      ["job.enqueued", p.id],
      ["job.enqueued", child.id],
      ["job.enqueued", grandchild.id],
    ]);
    expect([await state(child.id), await state(grandchild.id)]).toEqual(["blocked", "blocked"]);

    const lease = await broker.claim(["q"], "w");
    await broker.complete(p.id, lease!.token);
    expect(await state(child.id)).toBe("waiting");
  });

  it("refuses to retry a dependent directly while its fail dependency is still dead", async () => {
    const { conveyor, broker, running, dependent } = await ctx();
    const p = await running({ maxAttempts: 1 });
    const child = await dependent([p.id]);
    await broker.fail(p.id, p.token, "boom");
    await expect(conveyor.retry(child.id)).rejects.toThrow(new ConflictError(`dependency ${p.id} is dead; retry it first`));
  });

  it("retryAllDead retries dead jobs, which bring back their dependents", async () => {
    const { conveyor, broker, running, dependent, state } = await ctx();
    const p = await running({ maxAttempts: 1 });
    const child = await dependent([p.id]);
    await broker.fail(p.id, p.token, "boom");
    expect(await conveyor.retryAllDead("q")).toBe(1);
    expect(await state(child.id)).toBe("blocked");
  });
});

describe("time spent blocked", () => {
  it("is reported with job.unblocked: from insert, or from being brought back by a retry, until release", async () => {
    const { conveyor, broker, clock, running, dependent } = await ctx();
    const blockedFor: [string, number][] = [];
    conveyor.events.on("job.unblocked", (j, ms) => blockedFor.push([j.id, ms]));
    const metrics = conveyorMetrics(conveyor);

    const p = await running({ maxAttempts: 1 });
    const child = await dependent([p.id]);
    clock.advance(5000);
    await broker.fail(p.id, p.token, "boom"); // cancels the child
    clock.advance(60_000);
    await conveyor.retry(p.id); // brings the child back, blocked from now
    clock.advance(2000);
    const lease = (await broker.claim(["q"], "w"))!;
    await broker.complete(lease.job.id, lease.token);
    expect(blockedFor).toEqual([[child.id, 2000]]);
    expect(await metrics.render()).toContain('conveyor_job_blocked_seconds_sum{queue="kids"} 2');
    expect(await metrics.render()).toContain('conveyor_jobs_cancelled_by_dependency_total{queue="kids"} 1');
  });
});

describe("retryDead", () => {
  it("stops at an unexpected error instead of skipping the job", async () => {
    const { conveyor, broker, running } = await ctx();
    const p = await running({ maxAttempts: 1 });
    await broker.fail(p.id, p.token, "boom");
    vi.spyOn(conveyor.store, "retryJob").mockRejectedValueOnce(new Error("disk I/O error"));
    await expect(conveyor.retryDead("q")).rejects.toThrow("disk I/O error");
  });
});

describe("announce", () => {
  it("reports a release without a blocked time (from another store) as 0", async () => {
    const { conveyor } = await ctx();
    const seen: number[] = [];
    conveyor.events.on("job.unblocked", (_j, ms) => seen.push(ms));
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    conveyor.announce([{ job, from: "blocked", cause: null }]);
    expect(seen).toEqual([0]);
  });
});

describe("what a released dependent sees of a dependency retried later", () => {
  // Documented limitation: it sees the dependency's current state, not the one it was released on.
  it("lists a failed ignore dependency again, in its current state, once it is retried", async () => {
    const { conveyor, broker, running, dependent } = await ctx();
    const p = await running({ maxAttempts: 1 });
    const child = await dependent([{ jobId: p.id, onFailure: "ignore" }]);
    await broker.fail(p.id, p.token, "boom");
    const view = async () => (await conveyor.dependencyResults(await conveyor.getJob(child.id))).filter(isVisibleToDependent);
    expect(await view()).toEqual([]);
    expect((await conveyor.getJob(child.id)).state).toBe("waiting");
    await conveyor.retry(p.id);
    expect((await view()).map((d) => [d.id, d.state, d.error])).toEqual([[p.id, "waiting", "boom"]]);
  });
});

describe("Maintenance.reconcile (safety net)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("does nothing when every transition settled its dependents", async () => {
    const { conveyor, broker, running, dependent } = await ctx();
    const p = await running();
    await dependent([p.id]);
    await broker.complete(p.id, p.token);
    expect(await new Maintenance(conveyor).reconcile()).toBe(0);
  });

  it("settles blocked jobs a transition missed, warns, and announces them", async () => {
    const { logger, records } = memoryLogger();
    const s = await setup({ clock: manualClock(), logger });
    const events = recordEvents(s.conveyor);
    const p = await s.conveyor.enqueue({ queue: "q", name: "x" });
    const child = await s.conveyor.enqueue({ queue: "q", name: "y", dependsOn: [p.id] });
    // A change made behind the store's back, as a bug or a manual edit would.
    (s.store as SqliteStore).db.prepare("UPDATE jobs SET state = 'completed' WHERE id = ?").run(p.id);

    expect(await new Maintenance(s.conveyor).reconcile()).toBe(1);
    expect((await s.conveyor.getJob(child.id)).state).toBe("waiting");
    expect(events.at(-1)).toEqual(["job.unblocked", child.id]);
    expect(records).toContainEqual(expect.objectContaining({ level: "warn", msg: "settled blocked jobs that were missed", jobIds: [child.id] }));
  });

  it("runs on its own interval once started", async () => {
    vi.useFakeTimers();
    const { conveyor } = await setup({ clock: manualClock() });
    const m = new Maintenance(conveyor, { reconcileIntervalMs: 100 });
    const reconcile = vi.spyOn(m, "reconcile");
    m.start();
    await vi.advanceTimersByTimeAsync(1000);
    m.stop();
    expect(reconcile).toHaveBeenCalledTimes(10);
  });
});
