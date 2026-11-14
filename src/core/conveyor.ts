import { ConflictError, NotFoundError, QueuePausedError, ValidationError } from "./errors.js";
import { EventBus } from "./events.js";
import { silentLogger, type Logger } from "./logger.js";
import { DEFAULT_CONFIG, type ConveyorConfig } from "./config.js";
import type {
  EnqueueRequest,
  Job,
  JobFilter,
  Page,
  QueueSettings,
  QueueStats,
} from "./types.js";
import { TERMINAL_STATES } from "./types.js";
import type { Store } from "../storage/store.js";
import { systemClock, type Clock } from "../util/clock.js";
import { jobId } from "../util/ids.js";
import { DEFAULT_BACKOFF } from "../util/backoff.js";
import { NAME_PATTERN } from "../util/validate.js";

export interface ConveyorOptions {
  store: Store;
  config?: Partial<ConveyorConfig>;
  clock?: Clock;
  logger?: Logger;
  events?: EventBus;
}

const MAX_PRIORITY = 1000;

// Producer-side API
export class Conveyor {
  readonly store: Store;
  readonly config: ConveyorConfig;
  readonly clock: Clock;
  readonly logger: Logger;
  readonly events: EventBus;

  constructor(opts: ConveyorOptions) {
    this.store = opts.store;
    this.config = { ...DEFAULT_CONFIG, ...opts.config };
    this.clock = opts.clock ?? systemClock;
    this.logger = opts.logger ?? silentLogger;
    this.events = opts.events ?? new EventBus();
  }

  async init(): Promise<void> {
    await this.store.migrate();
  }

  async enqueue(req: EnqueueRequest): Promise<Job> {
    this.validate(req);
    const queue = await this.queueSettings(req.queue);
    if (req.uniqueKey) {
      const existing = await this.store.findActiveByUniqueKey(req.queue, req.uniqueKey);
      if (existing) return existing;
    }
    const now = this.clock.now();
    const runAt = req.runAt ?? now + (req.delayMs ?? 0);
    const job: Job = {
      id: jobId(now),
      queue: req.queue,
      name: req.name,
      payload: req.payload ?? null,
      state: runAt > now ? "delayed" : "waiting",
      priority: req.priority ?? 0,
      attempts: 0,
      maxAttempts: req.maxAttempts ?? queue.defaultMaxAttempts,
      backoff: req.backoff ?? DEFAULT_BACKOFF,
      timeoutMs: req.timeoutMs ?? 0,
      runAt,
      createdAt: now,
      updatedAt: now,
      startedAt: null,
      finishedAt: null,
      workerId: null,
      leaseToken: null,
      leaseExpiresAt: null,
      progress: 0,
      result: null,
      lastError: null,
      uniqueKey: req.uniqueKey ?? null,
      scheduleId: req.scheduleId ?? null,
    };
    await this.store.insertJob(job);
    await this.store.appendJobEvent(job.id, "enqueued", { state: job.state }, now);
    this.events.emit("job.enqueued", job);
    this.logger.debug("job enqueued", { jobId: job.id, queue: job.queue, name: job.name });
    return job;
  }

  async enqueueBulk(reqs: EnqueueRequest[]): Promise<Job[]> {
    if (reqs.length > 1000) throw new ValidationError("at most 1000 jobs per bulk request");
    reqs.forEach((r) => this.validate(r));
    const jobs: Job[] = [];
    for (const req of reqs) jobs.push(await this.enqueue(req));
    return jobs;
  }

  async getJob(id: string): Promise<Job> {
    const job = await this.store.getJob(id);
    if (!job) throw new NotFoundError(`job ${id} not found`);
    return job;
  }

  listJobs(filter: JobFilter): Promise<Page<Job>> {
    return this.store.listJobs(filter);
  }

  async cancel(id: string): Promise<Job> {
    const job = await this.getJob(id);
    if (TERMINAL_STATES.includes(job.state)) {
      throw new ConflictError(`job ${id} is already ${job.state}`);
    }
    const now = this.clock.now();
    await this.store.cancelJob(id, now);
    await this.store.appendJobEvent(id, "cancelled", { previousState: job.state }, now);
    const updated = await this.getJob(id);
    this.events.emit("job.cancelled", updated);
    return updated;
  }

  /** Re-run a dead or cancelled job from scratch. */
  async retry(id: string): Promise<Job> {
    const job = await this.getJob(id);
    if (job.state !== "dead" && job.state !== "cancelled") {
      throw new ConflictError(`only dead or cancelled jobs can be retried (job is ${job.state})`);
    }
    const now = this.clock.now();
    await this.store.requeueJob(id, now, true);
    await this.store.appendJobEvent(id, "retried", { previousState: job.state }, now);
    const updated = await this.getJob(id);
    this.events.emit("job.enqueued", updated);
    return updated;
  }

  async retryAllDead(queue: string, limit = 1000): Promise<number> {
    const page = await this.store.listJobs({ queue, state: "dead", limit });
    for (const job of page.items) await this.retry(job.id);
    return page.items.length;
  }

  async jobEvents(id: string) {
    await this.getJob(id);
    return this.store.listJobEvents(id);
  }

  async queueSettings(name: string): Promise<QueueSettings> {
    const existing = await this.store.getQueue(name);
    if (existing) return existing;
    const now = this.clock.now();
    return {
      name,
      paused: false,
      concurrency: 0,
      rateLimitMax: 0,
      rateLimitIntervalMs: 0,
      defaultMaxAttempts: 3,
      createdAt: now,
      updatedAt: now,
    };
  }

  async updateQueue(
    name: string,
    patch: Partial<Pick<QueueSettings, "concurrency" | "rateLimitMax" | "rateLimitIntervalMs" | "defaultMaxAttempts" | "paused">>,
  ): Promise<QueueSettings> {
    if (!NAME_PATTERN.test(name)) throw new ValidationError("invalid queue name");
    const current = await this.queueSettings(name);
    const next: QueueSettings = { ...current, ...patch, updatedAt: this.clock.now() };
    if (next.concurrency < 0 || next.rateLimitMax < 0 || next.rateLimitIntervalMs < 0) {
      throw new ValidationError("queue limits must be non-negative");
    }
    if (next.rateLimitMax > 0 && next.rateLimitIntervalMs === 0) {
      throw new ValidationError("rateLimitIntervalMs is required when rateLimitMax is set");
    }
    if (next.defaultMaxAttempts < 1) throw new ValidationError("defaultMaxAttempts must be >= 1");
    await this.store.upsertQueue(next);
    if (patch.paused !== undefined && patch.paused !== current.paused) {
      this.events.emit(patch.paused ? "queue.paused" : "queue.resumed", name);
    }
    return next;
  }

  pause(name: string) {
    return this.updateQueue(name, { paused: true });
  }

  resume(name: string) {
    return this.updateQueue(name, { paused: false });
  }

  async queueStats(name: string): Promise<QueueStats> {
    const settings = await this.queueSettings(name);
    return {
      ...settings,
      counts: await this.store.countJobs(name),
      oldestWaitingAt: await this.store.oldestWaiting(name),
    };
  }

  async listQueues(): Promise<QueueStats[]> {
    const queues = await this.store.listQueues();
    return Promise.all(queues.map((q) => this.queueStats(q.name)));
  }

  /** Fails fast if a queue is paused; used by API producers that opt in. */
  async assertAccepting(queue: string): Promise<void> {
    const q = await this.queueSettings(queue);
    if (q.paused) throw new QueuePausedError(queue);
  }

  private validate(req: EnqueueRequest): void {
    if (!req.queue || !NAME_PATTERN.test(req.queue)) throw new ValidationError("invalid queue name");
    if (!req.name || !NAME_PATTERN.test(req.name)) throw new ValidationError("invalid job name");
    if (req.priority !== undefined && (!Number.isInteger(req.priority) || Math.abs(req.priority) > MAX_PRIORITY)) {
      throw new ValidationError(`priority must be an integer between -${MAX_PRIORITY} and ${MAX_PRIORITY}`);
    }
    if (req.maxAttempts !== undefined && (!Number.isInteger(req.maxAttempts) || req.maxAttempts < 1)) {
      throw new ValidationError("maxAttempts must be a positive integer");
    }
    if (req.delayMs !== undefined && (!Number.isFinite(req.delayMs) || req.delayMs < 0)) {
      throw new ValidationError("delayMs must be a non-negative number");
    }
    if (req.timeoutMs !== undefined && (!Number.isFinite(req.timeoutMs) || req.timeoutMs < 0)) {
      throw new ValidationError("timeoutMs must be a non-negative number");
    }
    if (req.uniqueKey !== undefined && (req.uniqueKey.length === 0 || req.uniqueKey.length > 256)) {
      throw new ValidationError("uniqueKey must be 1-256 characters");
    }
    const size = Buffer.byteLength(JSON.stringify(req.payload ?? null));
    if (size > this.config.maxPayloadBytes) {
      throw new ValidationError(`payload is ${size} bytes; limit is ${this.config.maxPayloadBytes}`);
    }
  }
}
