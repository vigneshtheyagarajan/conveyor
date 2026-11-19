import type { Conveyor } from "../core/conveyor.js";
import type { Logger } from "../core/logger.js";

export interface MaintenanceOptions {
  reaperIntervalMs?: number;
  promoteIntervalMs?: number;
  janitorIntervalMs?: number;
  /** How often the dependency safety net runs. */
  reconcileIntervalMs?: number;
  batchSize?: number;
}

// reaper: expired leases -> waiting, promoter: delayed -> waiting,
// janitor: retention for completed and cancelled jobs (dead jobs are kept),
// reconciler: safety net for blocked jobs whose dependencies have settled
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
    this.every(this.opts.reconcileIntervalMs ?? 5_000, () => this.reconcile());
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

  /** Returns the number of jobs requeued. */
  async reap(): Promise<number> {
    const { store, clock, events } = this.conveyor;
    const now = clock.now();
    const expired = await store.findExpiredLeases(now, this.opts.batchSize ?? 100);
    let requeued = 0;
    for (const job of expired) {
      // BUG(KB-1): a job with no attempts left should go to dead, through store.markDead (with
      // the expired lease's token) so that its dependents are settled. It is requeued instead.
      // Changes nothing if the worker finished or heartbeated after findExpiredLeases.
      const res = await store.requeueExpired(job.id, job.leaseToken, now);
      if (!res) continue;
      requeued++;
      this.logger.warn("lease expired, job requeued", { jobId: job.id, workerId: job.workerId });
      events.emit("job.reaped", job);
    }
    return requeued;
  }

  async promote(): Promise<number> {
    const { store, clock } = this.conveyor;
    return store.promoteDelayed(clock.now(), this.opts.batchSize ?? 500);
  }

  /**
   * Settle blocked jobs whose dependencies no longer keep them blocked. Every transition
   * already does this in its own transaction, so anything found here means some path missed
   * it: it is logged as a warning. Returns the number of jobs changed.
   */
  async reconcile(): Promise<number> {
    const { store, clock } = this.conveyor;
    const { jobs, flowOutcomes } = await store.reconcileBlocked(clock.now(), this.opts.batchSize ?? 500);
    if (jobs.length) {
      this.logger.warn("settled blocked jobs that were missed", { jobIds: jobs.map((t) => t.job.id) });
      this.conveyor.announce(jobs);
      this.conveyor.announceFlows(flowOutcomes);
    }
    return jobs.length;
  }

  async cleanup(): Promise<number> {
    const { store, clock, config } = this.conveyor;
    const cutoff = clock.now() - config.retentionMs;
    const deleted = await store.deleteFinishedBefore(cutoff, this.opts.batchSize ?? 1000);
    if (deleted) this.logger.info("deleted finished jobs", { deleted });
    return deleted;
  }
}
