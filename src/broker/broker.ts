import { assertDelay, type Conveyor } from "../core/conveyor.js";
import { LeaseLostError, NotFoundError, UnrecoverableError, ValidationError } from "../core/errors.js";
import type { Job, QueueSettings } from "../core/types.js";
import { LocalRateLimiter, type RateLimiter } from "../ratelimit/limiter.js";
import { backoffDelay, DEFAULT_BACKOFF } from "../util/backoff.js";
import { leaseToken } from "../util/ids.js";
import type { Json } from "../util/json.js";

export interface Lease {
  job: Job;
  token: string;
  expiresAt: number;
}

export interface FailOptions {
  /** Skip retries and move straight to dead. */
  unrecoverable?: boolean;
  /** Override the backoff delay for this retry. */
  retryInMs?: number;
}

export interface BrokerOptions {
  rateLimiter?: RateLimiter;
  leaseMs?: number;
  random?: () => number;
}

// Lease protocol between the store and workers
export class Broker {
  readonly rateLimiter: RateLimiter;
  readonly leaseMs: number;
  private random: () => number;

  constructor(
    readonly conveyor: Conveyor,
    opts: BrokerOptions = {},
  ) {
    this.rateLimiter = opts.rateLimiter ?? new LocalRateLimiter(conveyor.clock);
    this.leaseMs = opts.leaseMs ?? conveyor.config.leaseMs;
    this.random = opts.random ?? Math.random;
  }

  private get store() {
    return this.conveyor.store;
  }

  private get clock() {
    return this.conveyor.clock;
  }

  async claim(queues: string[], workerId: string): Promise<Lease | undefined> {
    const now = this.clock.now();
    const settings = new Map<string, QueueSettings>();
    const exclude: string[] = [];
    for (const name of queues) {
      const q = await this.conveyor.queueSettings(name);
      settings.set(name, q);
      if (q.paused) {
        exclude.push(name);
        continue;
      }
      if (q.concurrency > 0 && (await this.store.countActive(name)) >= q.concurrency) {
        exclude.push(name);
      }
    }

    const candidate = await this.store.findClaimCandidate({ queues, now, exclude });
    if (!candidate) return undefined;

    const q = settings.get(candidate.queue)!;
    if (!(await this.rateLimiter.acquire(q))) return undefined;

    const token = leaseToken();
    const expiresAt = now + this.leaseMs;
    await this.store.activateJob(candidate.id, { workerId, leaseToken: token, leaseExpiresAt: expiresAt, now });
    await this.store.appendJobEvent(candidate.id, "claimed", { workerId, attempt: candidate.attempts + 1 }, now);

    const job = (await this.store.getJob(candidate.id))!;
    this.conveyor.events.emit("job.claimed", job, workerId);
    return { job, token, expiresAt };
  }

  private async owned(jobId: string, token: string): Promise<Job> {
    const job = await this.store.getJob(jobId);
    if (!job) throw new NotFoundError(`job ${jobId} not found`);
    if (job.state !== "active" || job.leaseToken !== token) throw new LeaseLostError(jobId);
    return job;
  }

  /** Extend the lease. Returns the new expiry. */
  async heartbeat(jobId: string, token: string): Promise<number> {
    await this.owned(jobId, token);
    const now = this.clock.now();
    const expiresAt = now + this.leaseMs;
    await this.store.extendLease(jobId, expiresAt, now);
    return expiresAt;
  }

  async progress(jobId: string, token: string, progress: number): Promise<void> {
    if (typeof progress !== "number" || !Number.isFinite(progress)) {
      throw new ValidationError("progress must be a finite number");
    }
    const job = await this.owned(jobId, token);
    const clamped = Math.max(0, Math.min(1, progress));
    await this.store.updateProgress(jobId, clamped, this.clock.now());
    this.conveyor.events.emit("job.progress", { ...job, progress: clamped }, clamped);
  }

  async complete(jobId: string, token: string, result: Json | null = null): Promise<Job> {
    await this.owned(jobId, token);
    const now = this.clock.now();
    await this.store.completeJob(jobId, result, now);
    await this.store.appendJobEvent(jobId, "completed", null, now);
    const job = (await this.store.getJob(jobId))!;
    this.conveyor.events.emit("job.completed", job);
    return job;
  }

  async fail(jobId: string, token: string, error: unknown, opts: FailOptions = {}): Promise<Job> {
    if (opts.retryInMs !== undefined) assertDelay(opts.retryInMs, "retryInMs");
    const job = await this.owned(jobId, token);
    const message = errorMessage(error);
    const now = this.clock.now();
    const unrecoverable = opts.unrecoverable || error instanceof UnrecoverableError;

    if (unrecoverable || job.attempts >= job.maxAttempts) {
      await this.store.markDead(jobId, message, now);
      await this.store.appendJobEvent(jobId, "dead", { error: message, attempts: job.attempts }, now);
      const dead = (await this.store.getJob(jobId))!;
      this.conveyor.events.emit("job.failed", dead, message);
      this.conveyor.events.emit("job.dead", dead, message);
      return dead;
    }

    const delay = opts.retryInMs ?? backoffDelay(job.backoff ?? DEFAULT_BACKOFF, job.attempts, this.random);
    await this.store.scheduleRetry(jobId, message, now + delay, now);
    await this.store.appendJobEvent(jobId, "retry_scheduled", { error: message, delayMs: delay }, now);
    const retrying = (await this.store.getJob(jobId))!;
    this.conveyor.events.emit("job.failed", retrying, message);
    this.conveyor.events.emit("job.retrying", retrying, message, delay);
    return retrying;
  }

  /** Give a job back without counting it as a failure (e.g. on shutdown). */
  async release(jobId: string, token: string): Promise<void> {
    await this.owned(jobId, token);
    const now = this.clock.now();
    await this.store.releaseLease(jobId, now);
    await this.store.appendJobEvent(jobId, "released", null, now);
  }
}

/** A string describing any thrown value, so every failure can be recorded. */
export function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message || error.name;
  if (typeof error === "string") return error;
  if (typeof error === "function") return error.name ? `[function ${error.name}]` : "[function]";
  try {
    // undefined for undefined and symbols; throws for BigInt and circular values
    const json = JSON.stringify(error);
    if (json !== undefined) return json;
  } catch {
    // fall through
  }
  return String(error);
}
