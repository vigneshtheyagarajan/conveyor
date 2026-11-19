import type { Conveyor } from "../core/conveyor.js";
import type { Logger } from "../core/logger.js";
import type { Job } from "../core/types.js";
import { backoffDelay, DEFAULT_BACKOFF } from "../util/backoff.js";

export interface MaintenanceOptions {
  reaperIntervalMs?: number;
  promoteIntervalMs?: number;
  janitorIntervalMs?: number;
  batchSize?: number;
  /** Jitter source for retry backoff (tests pass a fixed one). */
  random?: () => number;
}

// reaper: expired leases -> delayed (retry with backoff) or dead, promoter: delayed -> waiting,
// janitor: retention for completed and cancelled jobs (dead jobs are kept)
export class Maintenance {
  private timers: NodeJS.Timeout[] = [];
  private running = false;
  private logger: Logger;

  constructor(
    private conveyor: Conveyor,
    private opts: MaintenanceOptions = {},
  ) {
    this.logger = conveyor.logger.child({ component: "maintenance" });
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    const cfg = this.conveyor.config;
    this.every(this.opts.reaperIntervalMs ?? cfg.reaperIntervalMs, () => this.reap());
    this.every(this.opts.promoteIntervalMs ?? 500, () => this.promote());
    if (cfg.retentionMs > 0) {
      this.every(this.opts.janitorIntervalMs ?? 60_000, () => this.cleanup());
    }
  }

  stop(): void {
    this.running = false;
    this.timers.forEach(clearInterval);
    this.timers = [];
  }

  private every(ms: number, fn: () => Promise<unknown>): void {
    const t = setInterval(() => {
      fn().catch((err) => this.logger.error("maintenance task failed", { err }));
    }, ms);
    t.unref();
    this.timers.push(t);
  }

  /**
   * End expired leases. An expired lease counts as a failed attempt: the job is retried after
   * its normal backoff, or goes to dead once it has used all its attempts.
   *
   * Any number of reapers (one per worker process, plus `serve`) can run at once. Each one only
   * ends a lease that is still exactly as it read it, so a heartbeat or completion that lands
   * after the read wins, and a lease another reaper already ended is left alone. Returns the
   * number of leases this call ended; only those are logged and announced.
   */
  async reap(): Promise<number> {
    const { store, clock } = this.conveyor;
    const now = clock.now();
    const expired = await store.findExpiredLeases(now, this.opts.batchSize ?? 100);
    let ended = 0;
    for (const job of expired) if (await this.expire(job, now)) ended++;
    return ended;
  }

  private async expire(job: Job, now: number): Promise<boolean> {
    const { store, events } = this.conveyor;
    const dead = job.attempts >= job.maxAttempts;
    let error: string;
    let retryAt: number | null = null;
    let delay = 0;
    if (dead) {
      const earlier = (await store.listJobEvents(job.id)).filter((e) => e.type === "lease_expired").length;
      const times = earlier + 1;
      error = `lease expired ${times === 1 ? "once" : `${times} times`}; worker may be crashing`;
    } else {
      delay = backoffDelay(job.backoff ?? DEFAULT_BACKOFF, job.attempts, this.opts.random);
      retryAt = now + delay;
      error = `lease expired; worker ${job.workerId} may have crashed or stalled`;
    }
    const ended = await store.expireLease({
      id: job.id,
      leaseToken: job.leaseToken,
      leaseExpiresAt: job.leaseExpiresAt!,
      now,
      error,
      retryAt,
      event: { workerId: job.workerId, attempts: job.attempts, ...(dead ? { outcome: "dead" } : { outcome: "retry", delayMs: delay }) },
    });
    if (!ended) return false;

    const updated = (await store.getJob(job.id))!;
    events.emit("job.reaped", updated);
    events.emit("job.failed", updated, error);
    if (dead) {
      this.logger.error("lease expired, job is dead", { jobId: job.id, workerId: job.workerId, attempts: job.attempts });
      events.emit("job.dead", updated, error);
    } else {
      this.logger.warn("lease expired, retry scheduled", { jobId: job.id, workerId: job.workerId, retryInMs: delay });
      events.emit("job.retrying", updated, error, delay);
    }
    return true;
  }

  async promote(): Promise<number> {
    const { store, clock } = this.conveyor;
    return store.promoteDelayed(clock.now(), this.opts.batchSize ?? 500);
  }

  async cleanup(): Promise<number> {
    const { store, clock, config } = this.conveyor;
    const cutoff = clock.now() - config.retentionMs;
    const deleted = await store.deleteFinishedBefore(cutoff, this.opts.batchSize ?? 1000);
    if (deleted) this.logger.info("deleted finished jobs", { deleted });
    return deleted;
  }
}
