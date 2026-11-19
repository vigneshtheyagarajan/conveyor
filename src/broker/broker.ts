import { assertDelay, type Conveyor } from "../core/conveyor.js";
import { LeaseLostError, NotFoundError, UnrecoverableError, ValidationError } from "../core/errors.js";
import type { Job } from "../core/types.js";
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

  /**
   * Lease the next job from `queues` to a worker. Which job, and whether a queue is paused or
   * at its concurrency limit, is decided by the store in one atomic step, so this is safe with
   * any number of concurrent claims in this process and in other processes.
   *
   * Rate limits are per process (see LocalRateLimiter). A token is reserved in every rate-limited
   * queue before the claim and queues without one are left out, so a queue that is out of tokens
   * doesn't block the others. Tokens of queues that did not supply the job are given back.
   */
  async claim(queues: string[], workerId: string): Promise<Lease | undefined> {
    const now = this.clock.now();
    const names = [...new Set(queues)];
    const token = leaseToken();
    const expiresAt = now + this.leaseMs;
    const reserved: string[] = [];
    const exclude: string[] = [];
    let job: Job | undefined;
    try {
      for (const name of names) {
        // Take the token as soon as the settings are read, so parallel claims in this process
        // can't both count on the last one.
        if (await this.rateLimiter.acquire(await this.conveyor.queueSettings(name))) reserved.push(name);
        else exclude.push(name);
      }
      if (exclude.length === names.length) return undefined;
      job = await this.store.claimNext({ queues: names, workerId, leaseToken: token, leaseExpiresAt: expiresAt, now, exclude });
    } finally {
      for (const name of reserved) if (name !== job?.queue) await this.rateLimiter.release(name);
    }
    if (!job) return undefined;
    this.conveyor.events.emit("job.claimed", job, workerId);
    return { job, token, expiresAt };
  }

  /**
   * Read the job and check the caller holds its lease. This gives a NotFoundError for an unknown
   * job and the job's current data, but it is not what protects the job: another process can
   * take the lease right after this read. Every write that follows is conditional on the lease
   * token, and `held()` turns a write that changed nothing into a LeaseLostError.
   */
  private async owned(jobId: string, token: string): Promise<Job> {
    const job = await this.store.getJob(jobId);
    if (!job) throw new NotFoundError(`job ${jobId} not found`);
    if (job.state !== "active" || job.leaseToken !== token) throw new LeaseLostError(jobId);
    return job;
  }

  private held(jobId: string, written: boolean): void {
    if (!written) throw new LeaseLostError(jobId);
  }

  /** Extend the lease. Returns the new expiry. */
  async heartbeat(jobId: string, token: string): Promise<number> {
    await this.owned(jobId, token);
    const now = this.clock.now();
    const expiresAt = now + this.leaseMs;
    this.held(jobId, await this.store.extendLease(jobId, token, expiresAt, now));
    return expiresAt;
  }

  async progress(jobId: string, token: string, progress: number): Promise<void> {
    if (typeof progress !== "number" || !Number.isFinite(progress)) {
      throw new ValidationError("progress must be a finite number");
    }
    const job = await this.owned(jobId, token);
    const clamped = Math.max(0, Math.min(1, progress));
    this.held(jobId, await this.store.updateProgress(jobId, token, clamped, this.clock.now()));
    this.conveyor.events.emit("job.progress", { ...job, progress: clamped }, clamped);
  }

  async complete(jobId: string, token: string, result: Json | null = null): Promise<Job> {
    await this.owned(jobId, token);
    const now = this.clock.now();
    this.held(jobId, await this.store.completeJob(jobId, token, result, now));
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
      this.held(jobId, await this.store.markDead(jobId, token, message, now));
      await this.store.appendJobEvent(jobId, "dead", { error: message, attempts: job.attempts }, now);
      const dead = (await this.store.getJob(jobId))!;
      this.conveyor.events.emit("job.failed", dead, message);
      this.conveyor.events.emit("job.dead", dead, message);
      return dead;
    }

    const delay = opts.retryInMs ?? backoffDelay(job.backoff ?? DEFAULT_BACKOFF, job.attempts, this.random);
    this.held(jobId, await this.store.scheduleRetry(jobId, token, message, now + delay, now));
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
    this.held(jobId, await this.store.releaseLease(jobId, token, now));
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
