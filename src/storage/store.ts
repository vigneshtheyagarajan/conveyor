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

export interface ClaimCandidateQuery {
  queues: string[];
  now: number;
  /** Queues to skip (paused or at their concurrency limit). */
  exclude?: string[];
}

export interface NewJobEvent {
  jobId: string;
  type: string;
  data: Json | null;
  at: number;
}

export interface ActivateParams {
  workerId: string;
  leaseToken: string;
  leaseExpiresAt: number;
  now: number;
}

// Persistence contract
export interface Store {
  migrate(): Promise<number>;
  close(): Promise<void>;

  // jobs
  insertJob(job: Job): Promise<void>;
  /** Insert jobs and events in one transaction: either everything is stored or nothing is. */
  insertJobs(jobs: Job[], events: NewJobEvent[]): Promise<void>;
  getJob(id: string): Promise<Job | undefined>;
  findActiveByUniqueKey(queue: string, uniqueKey: string): Promise<Job | undefined>;
  listJobs(filter: JobFilter): Promise<Page<Job>>;
  countJobs(queue?: string): Promise<QueueCounts>;
  countActive(queue: string): Promise<number>;
  oldestWaiting(queue: string): Promise<number | null>;
  findClaimCandidate(query: ClaimCandidateQuery): Promise<Job | undefined>;
  activateJob(id: string, params: ActivateParams): Promise<void>;
  extendLease(id: string, leaseExpiresAt: number, now: number): Promise<void>;
  updateProgress(id: string, progress: number, now: number): Promise<void>;
  completeJob(id: string, result: Json | null, now: number): Promise<void>;
  scheduleRetry(id: string, error: string, runAt: number, now: number): Promise<void>;
  markDead(id: string, error: string, now: number): Promise<void>;
  cancelJob(id: string, now: number): Promise<void>;
  requeueJob(id: string, now: number, resetAttempts: boolean): Promise<void>;
  findExpiredLeases(now: number, limit: number): Promise<Job[]>;
  /** Back to waiting without counting the claim: attempts goes down by one (not below 0). */
  releaseLease(id: string, now: number): Promise<void>;
  promoteDelayed(now: number, limit: number): Promise<number>;
  /** Deletes completed and cancelled jobs finished before `ts`. Dead jobs are kept. */
  deleteFinishedBefore(ts: number, limit: number): Promise<number>;
  hasUnfinishedForSchedule(scheduleId: string): Promise<boolean>;

  // job events
  appendJobEvent(jobId: string, type: string, data: Json | null, at: number): Promise<void>;
  listJobEvents(jobId: string): Promise<JobEvent[]>;

  // queues
  getQueue(name: string): Promise<QueueSettings | undefined>;
  upsertQueue(settings: QueueSettings): Promise<void>;
  listQueues(): Promise<QueueSettings[]>;

  // schedules
  insertSchedule(schedule: Schedule): Promise<void>;
  updateSchedule(schedule: Schedule): Promise<void>;
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
