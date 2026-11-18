import type { Conveyor } from "../core/conveyor.js";
import type { Logger } from "../core/logger.js";

export interface MaintenanceOptions {
  reaperIntervalMs?: number;
  promoteIntervalMs?: number;
  janitorIntervalMs?: number;
  batchSize?: number;
}

// reaper: expired leases -> waiting, promoter: delayed -> waiting,
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

  /** Returns the number of jobs requeued. */
  async reap(): Promise<number> {
    const { store, clock, events } = this.conveyor;
    const now = clock.now();
    const expired = await store.findExpiredLeases(now, this.opts.batchSize ?? 100);
    for (const job of expired) {
      await store.requeueJob(job.id, now, false);
      await store.appendJobEvent(job.id, "lease_expired", { workerId: job.workerId }, now);
      this.logger.warn("lease expired, job requeued", { jobId: job.id, workerId: job.workerId });
      events.emit("job.reaped", job);
    }
    return expired.length;
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
