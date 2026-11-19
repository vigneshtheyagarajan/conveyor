import { randomBytes } from "node:crypto";
import { hostname } from "node:os";
import type { Broker, Lease } from "../broker/broker.js";
import { LeaseLostError, UnrecoverableError } from "../core/errors.js";
import type { Logger } from "../core/logger.js";
import type { Json } from "../util/json.js";
import type { HandlerRegistry, JobContext } from "./handlers.js";

export interface WorkerOptions {
  queues: string[];
  handlers: HandlerRegistry;
  concurrency?: number;
  /** Poll interval when queues are empty. Grows to maxPollMs while idle. */
  pollMs?: number;
  maxPollMs?: number;
  heartbeatMs?: number;
  /**
   * Grace period of stop(): how long it waits for running jobs to finish before aborting them
   * and handing them back.
   */
  shutdownTimeoutMs?: number;
  id?: string;
}

export type WorkerState = "idle" | "running" | "stopping" | "stopped";

export class JobTimeoutError extends Error {
  constructor(ms: number) {
    super(`job timed out after ${ms}ms`);
    this.name = "JobTimeoutError";
  }
}

/** The abort reason handlers see when stop() gives up waiting for them. */
export class WorkerShutdownError extends Error {
  constructor() {
    super("worker shutting down");
    this.name = "WorkerShutdownError";
  }
}

interface Running {
  lease: Lease;
  controller: AbortController;
  done: Promise<void>;
  /** Called by stop() when its grace period is over: stop waiting for this run. */
  giveUp: () => void;
}

// In-process worker
export class Worker {
  readonly id: string;
  private state: WorkerState = "idle";
  private loops: Promise<void>[] = [];
  /**
   * Runs in progress, by lease token. Not by job id: a job can be running twice in this worker
   * (its lease expired, the job was requeued and another loop claimed it), and each run must be
   * tracked, and aborted on shutdown, on its own.
   */
  private running = new Map<string, Running>();
  private stopController = new AbortController();
  private stopping?: Promise<void>;
  /** Wake-ups of the loops that are idle right now. */
  private idleLoops = new Set<() => void>();
  private logger: Logger;
  private unsubscribe: (() => void)[] = [];
  private readonly opts: Required<Omit<WorkerOptions, "id">>;
  private processedCount = 0;
  private failedCount = 0;

  constructor(
    private broker: Broker,
    options: WorkerOptions,
  ) {
    if (options.queues.length === 0) throw new Error("worker needs at least one queue");
    const cfg = broker.conveyor.config;
    this.opts = {
      queues: options.queues,
      handlers: options.handlers,
      concurrency: options.concurrency ?? 1,
      pollMs: options.pollMs ?? 250,
      maxPollMs: options.maxPollMs ?? 5000,
      heartbeatMs: options.heartbeatMs ?? cfg.heartbeatMs,
      shutdownTimeoutMs: options.shutdownTimeoutMs ?? 30_000,
    };
    this.id = options.id ?? `${hostname()}:${process.pid}:${randomBytes(3).toString("hex")}`;
    this.logger = broker.conveyor.logger.child({ component: "worker", workerId: this.id });
  }

  get status() {
    return {
      id: this.id,
      state: this.state,
      queues: this.opts.queues,
      concurrency: this.opts.concurrency,
      running: [...this.running.values()].map((r) => r.lease.job.id),
      processed: this.processedCount,
      failed: this.failedCount,
    };
  }

  start(): void {
    if (this.state !== "idle") throw new Error(`worker is ${this.state}`);
    this.state = "running";
    // Wake every idle loop when there may be new work: a job enqueued into, or a resume of, one
    // of our queues. Each loop tries to claim; those that find nothing go back to sleep. (These
    // are in-process events: work appearing through another process is found by polling.)
    const events = this.broker.conveyor.events;
    const wakeFor = (queue: string) => {
      if (this.opts.queues.includes(queue)) for (const wake of [...this.idleLoops]) wake();
    };
    this.unsubscribe = [events.on("job.enqueued", (job) => wakeFor(job.queue)), events.on("queue.resumed", wakeFor)];
    for (let i = 0; i < this.opts.concurrency; i++) {
      this.loops.push(this.loop(i));
    }
    this.logger.info("worker started", { queues: this.opts.queues, concurrency: this.opts.concurrency });
  }

  /**
   * Stop taking jobs and shut down. Running jobs get `shutdownTimeoutMs` to finish; after that
   * their signals are aborted and they are handed back (released, without using an attempt;
   * a job that had already timed out is failed instead). Jobs claimed while stopping are handed
   * back unrun. Every call returns the same promise, which resolves once all of that is done.
   *
   * A handler that ignores its abort signal keeps running after stop() resolves, while its job
   * is already back in the queue: exit the process after stop() (`conveyor worker` does).
   */
  stop(): Promise<void> {
    this.stopping ??= this.shutdown();
    return this.stopping;
  }

  private async shutdown(): Promise<void> {
    this.state = "stopping";
    this.stopController.abort(); // wakes every idle loop
    for (const off of this.unsubscribe) off();

    const inFlight = [...this.running.values()].map((r) => r.done);
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => resolve("timeout"), this.opts.shutdownTimeoutMs);
    });
    const result = await Promise.race([Promise.all(inFlight).then(() => "done" as const), timeout]);
    // Don't leave the timer armed: it would keep the process alive after stop().
    clearTimeout(timer);
    if (result === "timeout") {
      this.logger.warn("shutdown timeout reached, aborting running jobs", { running: this.running.size });
      for (const r of this.running.values()) {
        r.controller.abort(new WorkerShutdownError());
        r.giveUp();
      }
    }
    await Promise.all(this.loops);
    this.state = "stopped";
    this.logger.info("worker stopped", { processed: this.processedCount });
  }

  private async loop(index: number): Promise<void> {
    let idleMs = this.opts.pollMs;
    while (this.state === "running") {
      let lease: Lease | undefined;
      try {
        lease = await this.broker.claim(this.opts.queues, this.id);
      } catch (err) {
        this.logger.error("claim failed", { err, loop: index });
      }
      if (!lease) {
        await this.idle(idleMs);
        idleMs = Math.min(idleMs * 2, this.opts.maxPollMs);
        continue;
      }
      if (this.state !== "running") {
        // stop() was called while this claim was in progress: hand the job back unrun.
        await this.broker.release(lease.job.id, lease.token).catch((err) => {
          this.logger.error("could not release job claimed during shutdown", { err, jobId: lease!.job.id });
        });
        break;
      }
      idleMs = this.opts.pollMs;
      await this.execute(lease);
    }
  }

  private idle(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const stop = this.stopController.signal;
      // stop() may already have fired while this loop was claiming; its abort event is gone.
      if (stop.aborted) return resolve();
      const t = setTimeout(done, ms);
      const idleLoops = this.idleLoops;
      function done() {
        clearTimeout(t);
        stop.removeEventListener("abort", done);
        idleLoops.delete(done);
        resolve();
      }
      idleLoops.add(done);
      stop.addEventListener("abort", done, { once: true });
    });
  }

  private async execute(lease: Lease): Promise<void> {
    const { job, token } = lease;
    const log = this.logger.child({ jobId: job.id, queue: job.queue, name: job.name, attempt: job.attempts });
    const controller = new AbortController();
    let finish!: () => void;
    const done = new Promise<void>((r) => (finish = r));
    let giveUp!: () => void;
    const givenUp = new Promise<void>((r) => (giveUp = r));
    this.running.set(token, { lease, controller, done, giveUp });

    const heartbeat = setInterval(() => {
      this.broker.heartbeat(job.id, token).catch((err) => {
        if (err instanceof LeaseLostError) {
          // The lease is gone for good (reaped, cancelled, claimed by another worker).
          clearInterval(heartbeat);
          if (!controller.signal.aborted) log.warn("lease lost during execution");
          else log.warn("lease lost while waiting for the handler to stop");
          controller.abort(err);
        } else {
          log.error("heartbeat failed", { err });
        }
      });
    }, this.opts.heartbeatMs);
    heartbeat.unref();

    let timeout: NodeJS.Timeout | undefined;
    let handlerStopped: Promise<void> | undefined;
    const started = Date.now();
    try {
      const handler = this.opts.handlers.get(job.name);
      if (!handler) throw new UnrecoverableError(`no handler registered for "${job.name}"`);

      const ctx: JobContext = {
        job,
        signal: controller.signal,
        logger: log,
        progress: (value) => this.broker.progress(job.id, token, value),
        heartbeat: async () => {
          await this.broker.heartbeat(job.id, token);
        },
      };

      const run = Promise.resolve().then(() => handler(ctx));
      handlerStopped = run.then(
        () => undefined,
        () => undefined,
      );
      // Stop waiting for the result as soon as the job is aborted (timeout, lost lease, forced
      // shutdown), even if the handler ignores its signal. Its eventual result is discarded.
      const aborted = new Promise<never>((_, reject) => {
        controller.signal.addEventListener("abort", () => reject(controller.signal.reason), { once: true });
      });
      if (job.timeoutMs > 0) {
        timeout = setTimeout(() => controller.abort(new JobTimeoutError(job.timeoutMs)), job.timeoutMs);
      }
      const result: Json | void = await Promise.race([run, aborted]);

      await this.broker.complete(job.id, token, result ?? null);
      this.processedCount++;
      log.info("job completed", { durationMs: Date.now() - started });
    } catch (err) {
      if (controller.signal.reason instanceof WorkerShutdownError) {
        // Shutting down is not the job's fault: hand it back without using up an attempt.
        try {
          await this.broker.release(job.id, token);
          log.info("job released on shutdown");
        } catch (releaseErr) {
          log.error("could not release job on shutdown", { err: releaseErr });
        }
        return;
      }
      if (controller.signal.reason instanceof JobTimeoutError && handlerStopped) {
        // Timed out. Keep the lease (heartbeats go on) until the handler has actually stopped:
        // recording the failure now would let the retry start while this run is still going.
        // A handler that ignores its signal holds the job, and this loop, until it returns, or
        // until stop() gives up on it.
        log.warn("job timed out; waiting for its handler to stop before recording the failure", { timeoutMs: job.timeoutMs });
        const outcome = await Promise.race([handlerStopped.then(() => "stopped" as const), givenUp.then(() => "shutdown" as const)]);
        if (outcome === "stopped") log.info("timed-out handler stopped", { durationMs: Date.now() - started });
        else log.warn("timed-out handler still running at shutdown; recording the failure anyway");
      }
      this.failedCount++;
      if (err instanceof LeaseLostError) {
        log.warn("job finished after its lease was lost; result discarded");
      } else {
        try {
          await this.broker.fail(job.id, token, err);
          log.warn("job failed", { err, durationMs: Date.now() - started });
        } catch (failErr) {
          if (failErr instanceof LeaseLostError) {
            // Reaped, cancelled or claimed by another worker meanwhile: the job is theirs now.
            log.warn("job failed after its lease was lost; failure not recorded", { err });
          } else {
            log.error("could not record job failure", { err: failErr });
          }
        }
      }
    } finally {
      clearInterval(heartbeat);
      if (timeout) clearTimeout(timeout);
      this.running.delete(token);
      finish();
    }
  }
}
