import { randomBytes } from "node:crypto";
import { hostname } from "node:os";
import type { Broker, Lease } from "../broker/broker.js";
import { LeaseLostError, UnrecoverableError } from "../core/errors.js";
import type { Logger } from "../core/logger.js";
import { sleep } from "../util/clock.js";
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
  /** How long stop() waits for running jobs before aborting them. */
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

interface Running {
  lease: Lease;
  controller: AbortController;
  done: Promise<void>;
}

// In-process worker
export class Worker {
  readonly id: string;
  private state: WorkerState = "idle";
  private loops: Promise<void>[] = [];
  private running = new Map<string, Running>();
  private stopController = new AbortController();
  private wake?: () => void;
  private logger: Logger;
  private unsubscribe?: () => void;
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
      running: [...this.running.keys()],
      processed: this.processedCount,
      failed: this.failedCount,
    };
  }

  start(): void {
    if (this.state !== "idle") throw new Error(`worker is ${this.state}`);
    this.state = "running";
    this.unsubscribe = this.broker.conveyor.events.on("job.enqueued", (job) => {
      if (this.opts.queues.includes(job.queue)) this.wake?.();
    });
    for (let i = 0; i < this.opts.concurrency; i++) {
      this.loops.push(this.loop(i));
    }
    this.logger.info("worker started", { queues: this.opts.queues, concurrency: this.opts.concurrency });
  }

  async stop(): Promise<void> {
    if (this.state === "stopped" || this.state === "stopping") return;
    this.state = "stopping";
    this.stopController.abort();
    this.wake?.();
    this.unsubscribe?.();

    const inFlight = [...this.running.values()].map((r) => r.done);
    const timeout = sleep(this.opts.shutdownTimeoutMs).then(() => "timeout" as const);
    const result = await Promise.race([Promise.all(inFlight).then(() => "done" as const), timeout]);
    if (result === "timeout") {
      this.logger.warn("shutdown timeout reached, aborting running jobs", { running: this.running.size });
      for (const r of this.running.values()) r.controller.abort(new Error("worker shutting down"));
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
      idleMs = this.opts.pollMs;
      await this.execute(lease);
    }
  }

  private idle(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const t = setTimeout(done, ms);
      const stop = this.stopController.signal;
      function done() {
        clearTimeout(t);
        stop.removeEventListener("abort", done);
        resolve();
      }
      this.wake = done;
      stop.addEventListener("abort", done, { once: true });
    });
  }

  private async execute(lease: Lease): Promise<void> {
    const { job, token } = lease;
    const log = this.logger.child({ jobId: job.id, queue: job.queue, name: job.name, attempt: job.attempts });
    const controller = new AbortController();
    let finish!: () => void;
    const done = new Promise<void>((r) => (finish = r));
    this.running.set(job.id, { lease, controller, done });

    const heartbeat = setInterval(() => {
      this.broker.heartbeat(job.id, token).catch((err) => {
        if (err instanceof LeaseLostError) {
          log.warn("lease lost during execution");
          controller.abort(err);
        } else {
          log.error("heartbeat failed", { err });
        }
      });
    }, this.opts.heartbeatMs);
    heartbeat.unref();

    let timeout: NodeJS.Timeout | undefined;
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
      let result: Json | void;
      if (job.timeoutMs > 0) {
        const timedOut = new Promise<never>((_, reject) => {
          timeout = setTimeout(() => {
            const err = new JobTimeoutError(job.timeoutMs);
            controller.abort(err);
            reject(err);
          }, job.timeoutMs);
        });
        result = await Promise.race([run, timedOut]);
      } else {
        result = await run;
      }

      await this.broker.complete(job.id, token, result ?? null);
      this.processedCount++;
      log.info("job completed", { durationMs: Date.now() - started });
    } catch (err) {
      this.failedCount++;
      if (err instanceof LeaseLostError) {
        log.warn("job finished after its lease was lost; result discarded");
      } else {
        try {
          await this.broker.fail(job.id, token, err);
          log.warn("job failed", { err, durationMs: Date.now() - started });
        } catch (failErr) {
          log.error("could not record job failure", { err: failErr });
        }
      }
    } finally {
      clearInterval(heartbeat);
      if (timeout) clearTimeout(timeout);
      this.running.delete(job.id);
      finish();
    }
  }
}
