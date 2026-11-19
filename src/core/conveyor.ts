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

/**
 * Latest run time a job can have: the last instant whose ISO-8601 form has a four-digit year
 * (the API renders times that way). Earliest is the Unix epoch.
 */
export const MAX_RUN_AT = Date.UTC(9999, 11, 31, 23, 59, 59, 999);

/**
 * Longest delay accepted anywhere a delay is given (delayMs, retryInMs, backoff settings):
 * 100 years. That keeps every computed run time far inside MAX_RUN_AT, and a longer delay is
 * almost certainly a unit mistake.
 */
export const MAX_DELAY_MS = 100 * 365.25 * 24 * 60 * 60 * 1000;

const RUN_AT_RANGE = `between ${new Date(0).toISOString()} and ${new Date(MAX_RUN_AT).toISOString()}`;

/** Throws a ValidationError unless `value` is a delay between 0 and MAX_DELAY_MS. */
export function assertDelay(value: unknown, name: string): void {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new ValidationError(`${name} must be a non-negative number`);
  }
  if (value > MAX_DELAY_MS) throw new ValidationError(`${name} must be at most ${MAX_DELAY_MS} (100 years)`);
}

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
    const [job] = await this.enqueueMany([req]);
    return job!;
  }

  /**
   * Enqueue several jobs atomically: if any request is invalid or the insert fails, no job is
   * stored. Unique keys are de-duplicated within the batch as well as against existing jobs.
   */
  async enqueueBulk(reqs: EnqueueRequest[]): Promise<Job[]> {
    if (reqs.length > 1000) throw new ValidationError("at most 1000 jobs per bulk request");
    return this.enqueueMany(reqs);
  }

  private async enqueueMany(reqs: EnqueueRequest[]): Promise<Job[]> {
    reqs.forEach((r) => this.validate(r));
    const now = this.clock.now();
    const queues = new Map<string, QueueSettings>();
    const jobs: Job[] = [];
    for (const req of reqs) {
      let queue = queues.get(req.queue);
      if (!queue) queues.set(req.queue, (queue = await this.queueSettings(req.queue)));
      jobs.push(this.newJob(req, queue, now));
    }
    // The store checks unique keys in the same transaction as the insert: a job whose key an
    // unfinished job already holds (or one earlier in this batch) is not inserted, and that
    // job is returned in its place.
    const stored = await this.store.insertJobs(
      jobs,
      jobs.map((job) => ({ jobId: job.id, type: "enqueued", data: { state: job.state }, at: now })),
    );
    stored.forEach((job, i) => {
      if (job.id === jobs[i]!.id) this.announceEnqueued(job);
    });
    return stored;
  }

  /**
   * Validate a request and build the job it would create, without storing it. For callers that
   * store the job themselves as part of a larger atomic step (the scheduler), and then call
   * announceEnqueued().
   */
  async buildJob(req: EnqueueRequest): Promise<Job> {
    this.validate(req);
    return this.newJob(req, await this.queueSettings(req.queue), this.clock.now());
  }

  /** Emit job.enqueued for a job that has been stored. */
  announceEnqueued(job: Job): void {
    this.events.emit("job.enqueued", job);
    this.logger.debug("job enqueued", { jobId: job.id, queue: job.queue, name: job.name });
  }

  private newJob(req: EnqueueRequest, queue: QueueSettings, now: number): Job {
    const runAt = req.runAt ?? now + (req.delayMs ?? 0);
    return {
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
  }

  async getJob(id: string): Promise<Job> {
    const job = await this.store.getJob(id);
    if (!job) throw new NotFoundError(`job ${id} not found`);
    return job;
  }

  listJobs(filter: JobFilter): Promise<Page<Job>> {
    return this.store.listJobs(filter);
  }

  /**
   * Cancel a delayed, waiting or active job. The store checks the state and cancels in one
   * step, so a job that completes or dies at the same moment is never overwritten: whichever
   * lands first wins, and a cancel that lost gets a ConflictError. A worker running the job
   * finds out at its next heartbeat (LeaseLostError), and its result is discarded.
   */
  async cancel(id: string): Promise<Job> {
    const change = await this.store.cancelJob(id, this.clock.now());
    if (!change) throw new NotFoundError(`job ${id} not found`);
    if (!change.changed) throw new ConflictError(`job ${id} is already ${change.from}`);
    const updated = await this.getJob(id);
    this.events.emit("job.cancelled", updated);
    return updated;
  }

  /**
   * Re-run a dead or cancelled job from scratch. Checked and applied in one step: of two
   * concurrent retries, one requeues the job and the other gets a ConflictError. A job whose
   * unique key a newer unfinished job holds is refused (ConflictError) until that job finishes.
   */
  async retry(id: string): Promise<Job> {
    const change = await this.store.retryJob(id, this.clock.now());
    if (!change) throw new NotFoundError(`job ${id} not found`);
    if (change.heldBy) {
      const { jobId, uniqueKey } = change.heldBy;
      throw new ConflictError(`job ${id} cannot be retried: job ${jobId} with the same unique key "${uniqueKey}" is unfinished`);
    }
    if (!change.changed) throw new ConflictError(`only dead or cancelled jobs can be retried (job is ${change.from})`);
    const updated = await this.getJob(id);
    this.events.emit("job.enqueued", updated);
    return updated;
  }

  /** Retry the queue's dead jobs (up to `limit`). Returns how many this call retried. */
  async retryAllDead(queue: string, limit = 1000): Promise<number> {
    const page = await this.store.listJobs({ queue, state: "dead", limit });
    let retried = 0;
    for (const job of page.items) {
      try {
        await this.retry(job.id);
        retried++;
      } catch (err) {
        // Retried (or removed) by someone else since the list was read: not ours to count.
        if (!(err instanceof ConflictError || err instanceof NotFoundError)) throw err;
      }
    }
    return retried;
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
    if (req.runAt !== undefined) {
      if (!Number.isFinite(req.runAt)) throw new ValidationError("runAt must be a finite timestamp (ms since epoch)");
      if (req.runAt < 0 || req.runAt > MAX_RUN_AT) throw new ValidationError(`runAt must be ${RUN_AT_RANGE}`);
    }
    if (req.delayMs !== undefined) assertDelay(req.delayMs, "delayMs");
    if (req.backoff) {
      const b = req.backoff as unknown as Record<string, unknown>;
      for (const key of ["delayMs", "stepMs", "baseMs", "maxMs"]) {
        if (key in b) assertDelay(b[key], `backoff.${key}`);
      }
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
