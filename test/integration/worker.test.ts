// The worker uses global timers (polling, heartbeats, timeouts, shutdown), so these tests run
// with vitest's fake timers; the broker's times come from a ManualClock advanced in step.
// No real sleeps.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Maintenance } from "../../src/broker/maintenance.js";
import { LeaseLostError, UnrecoverableError } from "../../src/core/errors.js";
import type { ManualClock } from "../../src/util/clock.js";
import { HandlerRegistry, type JobContext, type JobHandler } from "../../src/worker/handlers.js";
import { JobTimeoutError, Worker, WorkerShutdownError, type WorkerOptions } from "../../src/worker/worker.js";
import { manualClock, memoryLogger, setup } from "../helpers.js";

function deferred<T = void>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** A handler that only finishes when its signal aborts, rejecting with the abort reason. */
const untilAborted = ({ signal }: JobContext): Promise<never> =>
  new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true }));

let clock: ManualClock;

/** Advance fake timers and the ManualClock together, in small steps. */
async function advance(ms: number, step = 5) {
  for (let t = 0; t < ms; t += step) {
    clock.advance(step);
    await vi.advanceTimersByTimeAsync(step);
  }
}

/** Advance time until the condition holds (fails after maxMs of fake time). */
async function until(cond: () => boolean | Promise<boolean>, maxMs = 10_000) {
  for (let t = 0; t <= maxMs; t += 5) {
    if (await cond()) return;
    await advance(5);
  }
  throw new Error(`condition not met within ${maxMs}ms of fake time`);
}

/** Stop the worker, advancing fake time while it shuts down. */
async function stopWorker(worker: Worker) {
  let done = false;
  const stopping = worker.stop().then(() => (done = true));
  await until(() => done, 60_000);
  await stopping;
}

async function start(handlers: Record<string, JobHandler>, opts: Partial<WorkerOptions> = {}, config = {}) {
  const { logger, records } = memoryLogger();
  const s = await setup({ clock, logger, config }); // leaseMs 1000, heartbeatMs 200
  const registry = new HandlerRegistry();
  for (const [name, fn] of Object.entries(handlers)) registry.register(name, fn);
  const worker = new Worker(s.broker, { queues: ["q"], handlers: registry, pollMs: 10, maxPollMs: 10, id: "w1", ...opts });
  worker.start();
  const job = (id: string) => s.conveyor.getJob(id);
  return { ...s, worker, records, job };
}

beforeEach(() => {
  vi.useFakeTimers();
  clock = manualClock();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("running jobs", () => {
  it("runs a job and stores its result", async () => {
    const { conveyor, worker, job } = await start({ add: ({ job }) => ({ sum: (job.payload as { a: number; b: number }).a + (job.payload as { a: number; b: number }).b }) });
    const j = await conveyor.enqueue({ queue: "q", name: "add", payload: { a: 2, b: 3 } });
    await until(async () => (await job(j.id)).state === "completed");
    expect(await job(j.id)).toMatchObject({ result: { sum: 5 }, workerId: "w1", attempts: 1 });
    expect(worker.status).toMatchObject({ id: "w1", state: "running", processed: 1, failed: 0, running: [] });
    await stopWorker(worker);
  });

  it("gives the handler the job, a logger, progress and heartbeat", async () => {
    const seen = deferred<Record<string, unknown>>();
    const { conveyor, worker, job } = await start({
      x: async (ctx) => {
        await ctx.progress(0.5);
        await ctx.heartbeat();
        ctx.logger.info("from handler");
        seen.resolve({ payload: ctx.job.payload, aborted: ctx.signal.aborted, progress: (await conveyor.getJob(ctx.job.id)).progress });
      },
    });
    const j = await conveyor.enqueue({ queue: "q", name: "x", payload: { hi: 1 } });
    await until(async () => (await job(j.id)).state === "completed");
    expect(await seen.promise).toEqual({ payload: { hi: 1 }, aborted: false, progress: 0.5 });
    await stopWorker(worker);
  });

  it("retries a failing job with backoff and counts the failure", async () => {
    const { conveyor, worker, job } = await start({
      x: () => {
        throw new Error("flaky");
      },
    });
    const j = await conveyor.enqueue({ queue: "q", name: "x", backoff: { type: "fixed", delayMs: 60_000 } });
    await until(async () => (await job(j.id)).state === "delayed");
    expect(await job(j.id)).toMatchObject({ attempts: 1, lastError: "flaky" });
    expect(worker.status.failed).toBe(1);
    await stopWorker(worker);
  });

  it("records `throw undefined` as a failed attempt (was KB-3)", async () => {
    const { conveyor, worker, job } = await start({
      x: () => {
        throw undefined;
      },
    });
    const j = await conveyor.enqueue({ queue: "q", name: "x", backoff: { type: "fixed", delayMs: 60_000 } });
    await until(async () => (await job(j.id)).state === "delayed");
    expect(await job(j.id)).toMatchObject({ attempts: 1, lastError: "undefined" });
    await stopWorker(worker);
  });

  it("moves a job straight to dead for UnrecoverableError", async () => {
    const { conveyor, worker, job } = await start({
      x: () => {
        throw new UnrecoverableError("bad input");
      },
    });
    const j = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 5 });
    await until(async () => (await job(j.id)).state === "dead");
    expect(await job(j.id)).toMatchObject({ attempts: 1, lastError: "bad input" });
    await stopWorker(worker);
  });

  it("moves a job with no registered handler to dead", async () => {
    const { conveyor, worker, job } = await start({});
    const j = await conveyor.enqueue({ queue: "q", name: "unknown", maxAttempts: 5 });
    await until(async () => (await job(j.id)).state === "dead");
    expect((await job(j.id)).lastError).toBe('no handler registered for "unknown"');
    await stopWorker(worker);
  });

  it("runs up to `concurrency` jobs at once", async () => {
    const gate = deferred();
    let running = 0;
    let peak = 0;
    const { conveyor, worker, job } = await start(
      {
        x: async () => {
          peak = Math.max(peak, ++running);
          await gate.promise;
          running--;
        },
      },
      { concurrency: 3 },
    );
    const jobs = await conveyor.enqueueBulk(Array.from({ length: 5 }, () => ({ queue: "q", name: "x" })));
    await until(() => running === 3);
    await advance(100);
    expect(peak).toBe(3);
    expect(worker.status.running).toHaveLength(3);
    gate.resolve();
    await until(async () => (await Promise.all(jobs.map((j) => job(j.id)))).every((x) => x.state === "completed"));
    expect(peak).toBe(3);
    await stopWorker(worker);
  });

  it("wakes up as soon as a job is enqueued instead of waiting out its poll backoff", async () => {
    const { conveyor, worker, job } = await start({ x: () => "done" }, { pollMs: 10, maxPollMs: 60_000 });
    await advance(200_000, 1000); // idle long enough to back off to the maximum poll interval
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    await advance(20);
    expect((await job(j.id)).state).toBe("completed");
    await stopWorker(worker);
  });

  it("logs a failed claim and keeps polling", async () => {
    const { conveyor, broker, worker, records, job } = await start({ x: () => "ok" });
    vi.spyOn(broker, "claim").mockRejectedValueOnce(new Error("database is locked"));
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    await until(async () => (await job(j.id)).state === "completed");
    expect(records).toContainEqual(expect.objectContaining({ level: "error", msg: "claim failed", err: expect.objectContaining({ message: "database is locked" }) }));
    await stopWorker(worker);
  });

  it("keeps running when a heartbeat fails for a reason other than a lost lease", async () => {
    const gate = deferred();
    const { conveyor, broker, worker, records, job } = await start({ x: () => gate.promise.then(() => "ok") });
    vi.spyOn(broker, "heartbeat").mockRejectedValueOnce(new Error("database is locked"));
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    await until(async () => (await job(j.id)).state === "active");
    await advance(250);
    expect(records).toContainEqual(expect.objectContaining({ level: "error", msg: "heartbeat failed", err: expect.objectContaining({ message: "database is locked" }) }));
    gate.resolve();
    await until(async () => (await job(j.id)).state === "completed");
    await stopWorker(worker);
  });

  it("logs a failure that cannot be recorded", async () => {
    const { conveyor, broker, worker, records } = await start({
      x: () => {
        throw new Error("handler broke");
      },
    });
    const fail = vi.spyOn(broker, "fail").mockRejectedValue(new Error("database is locked"));
    await conveyor.enqueue({ queue: "q", name: "x" });
    await until(() => fail.mock.calls.length > 0);
    await advance(10);
    expect(records).toContainEqual(expect.objectContaining({ level: "error", msg: "could not record job failure" }));
    await stopWorker(worker);
  });

  it("refuses to start twice and needs at least one queue", async () => {
    const { broker, worker } = await start({});
    expect(() => worker.start()).toThrow("worker is running");
    expect(() => new Worker(broker, { queues: [], handlers: new HandlerRegistry() })).toThrow("worker needs at least one queue");
    await stopWorker(worker);
  });
});

describe("timeouts", () => {
  it("aborts the handler's signal with JobTimeoutError and fails the attempt", async () => {
    let reason: unknown;
    const { conveyor, worker, job } = await start({
      x: (ctx) => {
        ctx.signal.addEventListener("abort", () => (reason = ctx.signal.reason));
        return untilAborted(ctx);
      },
    });
    const j = await conveyor.enqueue({ queue: "q", name: "x", timeoutMs: 500, backoff: { type: "fixed", delayMs: 60_000 } });
    await until(async () => (await job(j.id)).state === "active");
    await advance(480);
    expect((await job(j.id)).state).toBe("active");
    await until(async () => (await job(j.id)).state === "delayed", 100);
    expect(reason).toBeInstanceOf(JobTimeoutError);
    expect((await job(j.id)).lastError).toBe("job timed out after 500ms");
    await stopWorker(worker);
  });

  it("moves on to the next job even if the handler ignores the timeout", async () => {
    const { conveyor, worker, job } = await start({ stuck: () => new Promise(() => {}), quick: () => "ok" });
    const stuck = await conveyor.enqueue({ queue: "q", name: "stuck", timeoutMs: 200, maxAttempts: 1 });
    const quick = await conveyor.enqueue({ queue: "q", name: "quick" });
    await until(async () => (await job(quick.id)).state === "completed");
    expect((await job(stuck.id)).state).toBe("dead");
    await stopWorker(worker);
  });

  it("has no time limit when timeoutMs is 0", async () => {
    const gate = deferred();
    const { conveyor, worker, job } = await start({ x: () => gate.promise });
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    await advance(60_000, 100);
    expect((await job(j.id)).state).toBe("active");
    gate.resolve();
    await until(async () => (await job(j.id)).state === "completed");
    await stopWorker(worker);
  });
});

describe("leases while running", () => {
  it("heartbeats keep a long job's lease alive, so the reaper leaves it alone", async () => {
    const gate = deferred();
    const { conveyor, worker, job } = await start({ x: () => gate.promise }); // lease 1000ms, heartbeat 200ms
    const m = new Maintenance(conveyor);
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    await until(async () => (await job(j.id)).state === "active");
    for (let i = 0; i < 10; i++) {
      await advance(500);
      expect(await m.reap()).toBe(0);
      expect((await job(j.id)).leaseExpiresAt).toBeGreaterThan(clock.now());
    }
    gate.resolve();
    await until(async () => (await job(j.id)).state === "completed");
    await stopWorker(worker);
  });

  it("aborts a job that is cancelled while running and discards its result", async () => {
    let reason: unknown;
    const { conveyor, worker, records, job } = await start({
      x: async (ctx) => {
        ctx.signal.addEventListener("abort", () => (reason = ctx.signal.reason));
        await untilAborted(ctx).catch(() => {});
        return { late: true };
      },
    });
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    await until(async () => (await job(j.id)).state === "active");
    await conveyor.cancel(j.id);
    await until(() => reason !== undefined, 500); // noticed at the next heartbeat
    expect(reason).toBeInstanceOf(LeaseLostError);
    await advance(50);
    expect(await job(j.id)).toMatchObject({ state: "cancelled", result: null });
    expect(records).toContainEqual(expect.objectContaining({ level: "warn", msg: "lease lost during execution", jobId: j.id }));
    await stopWorker(worker);
  });

  it("discards the result of a job whose lease expired and was reaped while it ran", async () => {
    const gate = deferred();
    const { conveyor, worker, records, job } = await start(
      { x: ({ job }) => (job.attempts === 1 ? gate.promise.then(() => "stale") : "fresh") },
      { heartbeatMs: 60_000 },
    );
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    await until(async () => (await job(j.id)).state === "active");
    clock.advance(5000); // no heartbeat within the lease
    await new Maintenance(conveyor).reap();
    gate.resolve(); // the first run finishes late; the requeued job then runs again
    await until(async () => (await job(j.id)).state === "completed");
    expect(await job(j.id)).toMatchObject({ result: "fresh", attempts: 2 });
    expect(records).toContainEqual(expect.objectContaining({ msg: "job finished after its lease was lost; result discarded" }));
    await stopWorker(worker);
  });
});

describe("shutdown", () => {
  it("waits for jobs in flight to finish and takes no new ones", async () => {
    const gate = deferred();
    const { conveyor, worker, job } = await start({ x: () => gate.promise.then(() => "finished") }, { shutdownTimeoutMs: 30_000 });
    const inFlight = await conveyor.enqueue({ queue: "q", name: "x" });
    await until(async () => (await job(inFlight.id)).state === "active");

    let stopped = false;
    const stopping = worker.stop().then(() => (stopped = true));
    expect(worker.status.state).toBe("stopping");
    const queued = await conveyor.enqueue({ queue: "q", name: "x" });
    await advance(1000);
    expect(stopped).toBe(false);
    expect((await job(queued.id)).state).toBe("waiting");

    gate.resolve();
    await until(() => stopped);
    await stopping;
    expect(await job(inFlight.id)).toMatchObject({ state: "completed", result: "finished" });
    expect((await job(queued.id)).state).toBe("waiting");
    expect(worker.status.state).toBe("stopped");
  });

  it("leaves no timers behind once stopped", async () => {
    const { conveyor, worker, job } = await start({ x: () => "ok" }, { shutdownTimeoutMs: 30_000 });
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    await until(async () => (await job(j.id)).state === "completed");
    await stopWorker(worker);
    // Previously the 30s shutdown timer stayed armed, keeping a CLI worker's process alive.
    expect(vi.getTimerCount()).toBe(0);
  });

  it("after the shutdown timeout, aborts running jobs and hands them back without using an attempt", async () => {
    let reason: unknown;
    const { conveyor, worker, job } = await start(
      {
        x: (ctx) => {
          ctx.signal.addEventListener("abort", () => (reason = ctx.signal.reason));
          return untilAborted(ctx);
        },
      },
      { shutdownTimeoutMs: 1000 },
    );
    const j = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
    await until(async () => (await job(j.id)).state === "active");

    let stopped = false;
    const stopping = worker.stop().then(() => (stopped = true));
    await advance(990);
    expect(stopped).toBe(false);
    expect(reason).toBeUndefined();
    await advance(20);
    await stopping;

    expect(reason).toBeInstanceOf(WorkerShutdownError);
    // Previously the job was failed, and with maxAttempts 1 it went straight to dead.
    expect(await job(j.id)).toMatchObject({ state: "waiting", attempts: 0, lastError: null, leaseToken: null });
    expect((await conveyor.jobEvents(j.id)).at(-1)?.type).toBe("released");
    expect(worker.status.failed).toBe(0);
  });

  it("logs a job that cannot be released on shutdown", async () => {
    const { conveyor, broker, worker, records, job } = await start({ x: untilAborted }, { shutdownTimeoutMs: 100 });
    vi.spyOn(broker, "release").mockRejectedValueOnce(new Error("database is locked"));
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    await until(async () => (await job(j.id)).state === "active");
    await stopWorker(worker);
    expect(records).toContainEqual(expect.objectContaining({ level: "error", msg: "could not release job on shutdown" }));
  });

  // Regression (was KB-20): stop() waited forever for a handler that ignored its signal.
  it("finishes within the grace period even if a handler ignores its signal, and hands the job back", async () => {
    const { conveyor, worker, job } = await start({ x: () => new Promise(() => {}) }, { shutdownTimeoutMs: 1000 });
    const j = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
    await until(async () => (await job(j.id)).state === "active");
    let stopped = false;
    const stopping = worker.stop().then(() => (stopped = true));
    await advance(990);
    expect(stopped).toBe(false);
    await advance(20);
    expect(stopped).toBe(true);
    await stopping;
    expect(await job(j.id)).toMatchObject({ state: "waiting", attempts: 0, leaseToken: null });
    expect((await conveyor.jobEvents(j.id)).at(-1)?.type).toBe("released");
    expect(vi.getTimerCount()).toBe(0);
  });

  // Regression (was KB-21): a claim already under way when stop() was called still ran its job,
  // and stop() waited for it with no time limit.
  it("hands back, without running, a job claimed after stop() was called", async () => {
    const ran = vi.fn();
    const { conveyor, broker, worker, job } = await start({ x: ran }, { shutdownTimeoutMs: 1000 });
    const gate = deferred();
    const claim = broker.claim.bind(broker);
    vi.spyOn(broker, "claim").mockImplementation(async (...args) => {
      await gate.promise;
      return claim(...args);
    });
    const j = await conveyor.enqueue({ queue: "q", name: "x" });
    await advance(50); // the loop is now waiting inside claim()
    let stopped = false;
    const stopping = worker.stop().then(() => (stopped = true));
    gate.resolve();
    await until(() => stopped, 5000);
    await stopping;
    expect(ran).not.toHaveBeenCalled();
    expect(await job(j.id)).toMatchObject({ state: "waiting", attempts: 0 });
    expect((await conveyor.jobEvents(j.id)).map((e) => e.type)).toEqual(["enqueued", "claimed", "released"]);
  });

  // Regression (was KB-22): stop() during a claim waited out a whole poll interval, because
  // the idle wait's abort listener was added after the abort had fired.
  it("stops promptly even when stop() is called while the worker is polling", async () => {
    const { worker } = await start({}, { pollMs: 5000, maxPollMs: 5000 });
    let stopped = false;
    const stopping = worker.stop().then(() => (stopped = true)); // the loop is inside its first claim()
    await vi.advanceTimersByTimeAsync(0);
    expect(stopped).toBe(true);
    await stopping;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("stops promptly while idle between polls", async () => {
    const { worker } = await start({}, { pollMs: 5000, maxPollMs: 5000 });
    await advance(100); // now waiting in the idle timer
    let stopped = false;
    const stopping = worker.stop().then(() => (stopped = true));
    await vi.advanceTimersByTimeAsync(0);
    expect(stopped).toBe(true);
    await stopping;
  });

  it("is safe to call stop() more than once", async () => {
    const { worker } = await start({});
    let done = false;
    const both = Promise.all([worker.stop(), worker.stop()]).then(() => (done = true));
    await until(() => done);
    await both;
    await stopWorker(worker);
    expect(worker.status.state).toBe("stopped");
  });
});
