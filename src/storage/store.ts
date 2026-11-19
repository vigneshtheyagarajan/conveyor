import type {
  ApiToken,
  Job,
  JobEvent,
  JobFilter,
  JobState,
  Page,
  QueueCounts,
  QueueSettings,
  Schedule,
  Webhook,
  WebhookDelivery,
} from "../core/types.js";
import type { Json } from "../util/json.js";

export interface ClaimParams {
  queues: string[];
  workerId: string;
  leaseToken: string;
  leaseExpiresAt: number;
  now: number;
  /** Queues to leave out of this claim (e.g. out of rate-limit tokens in this process). */
  exclude?: string[];
}

/**
 * Outcome of a guarded state change. `from` is the state the job was in when the change was
 * tried: the state it left if `changed`, otherwise the state that refused the change.
 */
export interface StateChange {
  changed: boolean;
  from: JobState;
  /** retryJob only: the unfinished job that holds this job's unique key, which refused the retry. */
  heldBy?: { jobId: string; uniqueKey: string };
}

/** Columns of a schedule to change; everything else is left as it is. */
export type SchedulePatch = Partial<Omit<Schedule, "id" | "createdAt">>;

export interface FireScheduleParams {
  scheduleId: string;
  /** The slot to fire, and the cron and timezone it came from, as read. */
  slot: number;
  cron: string;
  timezone: string;
  /** The slot after this one. */
  nextRunAt: number;
  now: number;
  /** The job to enqueue for the slot. */
  job: Job;
}

/**
 * fired: the job was enqueued and the schedule moved to the next slot.
 * skipped: overlap is "skip" and a run is unfinished, so nothing was enqueued; the schedule still
 *   moved to the next slot.
 * stale: nothing changed. The slot was fired already (by another process), or the schedule was
 *   disabled, retimed or deleted since it was read.
 */
export type FireOutcome = "fired" | "skipped" | "stale";

export interface ExpireLeaseParams {
  id: string;
  /** The lease as it was read. Nothing changes unless the job still has exactly this lease. */
  leaseToken: string | null;
  leaseExpiresAt: number;
  now: number;
  /** Stored as the job's last error. */
  error: string;
  /** When to retry (the job goes to delayed), or null: out of attempts, the job goes to dead. */
  retryAt: number | null;
  /** Data for the "lease_expired" event. */
  event: Json;
}

export interface NewJobEvent {
  jobId: string;
  type: string;
  data: Json | null;
  at: number;
}

// Persistence contract
export interface Store {
  migrate(): Promise<number>;
  close(): Promise<void>;

  // jobs
  /** Insert one job as it is. The database refuses a second unfinished job with the same queue and unique key. */
  insertJob(job: Job): Promise<void>;
  /**
   * Insert jobs and events in one transaction: either everything is stored or nothing is.
   * Unique keys are checked in the same transaction: a job whose queue and unique key are held
   * by an unfinished job (stored earlier, or earlier in this batch) is not inserted, and its
   * events are dropped. Returns, for each job, the job now holding its place: the job itself if
   * it was inserted, otherwise the existing job with its key.
   */
  insertJobs(jobs: Job[], events: NewJobEvent[]): Promise<Job[]>;
  getJob(id: string): Promise<Job | undefined>;
  listJobs(filter: JobFilter): Promise<Page<Job>>;
  countJobs(queue?: string): Promise<QueueCounts>;
  countActive(queue: string): Promise<number>;
  oldestWaiting(queue: string): Promise<number | null>;
  /**
   * Lease the next job for a worker, atomically: picks the best waiting, due job in `queues`
   * (highest priority, then earliest run_at, then id), skipping paused queues and queues at
   * their concurrency limit, activates it, counts the attempt and records a "claimed" event.
   * Safe against concurrent claims from any number of connections and processes: a job is
   * leased to one worker at a time and a queue never exceeds its concurrency limit.
   */
  claimNext(params: ClaimParams): Promise<Job | undefined>;

  // The lease calls below only change a job that is active *and* holds `leaseToken`. They return
  // false, changing nothing, when the lease is no longer held (expired and requeued, cancelled,
  // finished, or claimed again by another worker).
  extendLease(id: string, leaseToken: string, leaseExpiresAt: number, now: number): Promise<boolean>;
  updateProgress(id: string, leaseToken: string, progress: number, now: number): Promise<boolean>;
  completeJob(id: string, leaseToken: string, result: Json | null, now: number): Promise<boolean>;
  scheduleRetry(id: string, leaseToken: string, error: string, runAt: number, now: number): Promise<boolean>;
  markDead(id: string, leaseToken: string, error: string, now: number): Promise<boolean>;
  /** Back to waiting without counting the claim: attempts goes down by one (not below 0). */
  releaseLease(id: string, leaseToken: string, now: number): Promise<boolean>;

  /**
   * End an expired lease and record a "lease_expired" event, but only if the job is still active
   * with the same lease token and expiry as when it was read. Returns false, changing nothing,
   * if a heartbeat renewed the lease, the job finished, failed or was cancelled, or another
   * reaper already ended the lease (and perhaps a new worker claimed the job).
   */
  expireLease(params: ExpireLeaseParams): Promise<boolean>;
  /**
   * Cancel a delayed, waiting or active job and record a "cancelled" event. A completed, dead
   * or cancelled job is left alone (changed: false). undefined if there is no such job.
   */
  cancelJob(id: string, now: number): Promise<StateChange | undefined>;
  /**
   * Put a dead or cancelled job back to waiting, due now, with no attempts used, and record a
   * "retried" event. Any other job is left alone (changed: false), and so is a job whose queue
   * and unique key another unfinished job holds (changed: false, with `heldBy`). undefined if
   * there is no such job.
   */
  retryJob(id: string, now: number): Promise<StateChange | undefined>;
  findExpiredLeases(now: number, limit: number): Promise<Job[]>;
  promoteDelayed(now: number, limit: number): Promise<number>;
  /** Deletes completed and cancelled jobs finished before `ts`. Dead jobs are kept. */
  deleteFinishedBefore(ts: number, limit: number): Promise<number>;

  // job events
  appendJobEvent(jobId: string, type: string, data: Json | null, at: number): Promise<void>;
  listJobEvents(jobId: string): Promise<JobEvent[]>;

  // queues
  getQueue(name: string): Promise<QueueSettings | undefined>;
  upsertQueue(settings: QueueSettings): Promise<void>;
  listQueues(): Promise<QueueSettings[]>;

  // schedules
  insertSchedule(schedule: Schedule): Promise<void>;
  /** Change only the given columns. Returns false if there is no such schedule. */
  updateSchedule(id: string, patch: SchedulePatch): Promise<boolean>;
  /**
   * Fire one slot of a schedule, atomically: only if the schedule is still enabled, on this slot
   * and on the same cron and timezone (so the slot fires once however many schedulers run);
   * then, unless overlap is "skip" and a run from the schedule is unfinished, insert the job
   * with its "enqueued" event; and move the schedule to `nextRunAt`.
   */
  fireSchedule(params: FireScheduleParams): Promise<FireOutcome>;
  getSchedule(id: string): Promise<Schedule | undefined>;
  getScheduleByName(name: string): Promise<Schedule | undefined>;
  listSchedules(): Promise<Schedule[]>;
  deleteSchedule(id: string): Promise<boolean>;
  findDueSchedules(now: number, limit: number): Promise<Schedule[]>;

  // webhooks
  insertWebhook(webhook: Webhook): Promise<void>;
  listWebhooks(): Promise<Webhook[]>;
  getWebhook(id: string): Promise<Webhook | undefined>;
  deleteWebhook(id: string): Promise<boolean>;
  insertDelivery(delivery: WebhookDelivery): Promise<void>;
  updateDelivery(delivery: WebhookDelivery): Promise<void>;
  listDeliveries(webhookId: string, limit: number): Promise<WebhookDelivery[]>;
  /** Deliveries still to be (re)tried, across all webhooks, soonest `nextAttemptAt` first. */
  findPendingDeliveries(limit: number): Promise<WebhookDelivery[]>;

  // tokens
  insertToken(token: ApiToken): Promise<void>;
  findTokenByHash(hash: string): Promise<ApiToken | undefined>;
  listTokens(): Promise<ApiToken[]>;
  revokeToken(id: string, now: number): Promise<boolean>;
  touchToken(id: string, now: number): Promise<void>;
}

export function emptyCounts(): QueueCounts {
  const states: JobState[] = ["delayed", "waiting", "active", "completed", "dead", "cancelled"];
  return Object.fromEntries(states.map((s) => [s, 0])) as QueueCounts;
}
