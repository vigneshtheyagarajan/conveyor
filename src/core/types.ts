import type { BackoffStrategy } from "../util/backoff.js";
import type { Json } from "../util/json.js";

export type JobState = "delayed" | "waiting" | "active" | "completed" | "dead" | "cancelled";

export const JOB_STATES: JobState[] = ["delayed", "waiting", "active", "completed", "dead", "cancelled"];
export const TERMINAL_STATES: JobState[] = ["completed", "dead", "cancelled"];

export interface Job {
  id: string;
  queue: string;
  name: string;
  payload: Json;
  state: JobState;
  /** Higher runs first. */
  priority: number;
  /** Number of times the job has been claimed. */
  attempts: number;
  maxAttempts: number;
  backoff: BackoffStrategy | null;
  /** Hard limit for one attempt; 0 means no limit. */
  timeoutMs: number;
  /** Earliest time the job may run (ms since epoch). */
  runAt: number;
  createdAt: number;
  updatedAt: number;
  startedAt: number | null;
  finishedAt: number | null;
  workerId: string | null;
  leaseToken: string | null;
  leaseExpiresAt: number | null;
  progress: number;
  result: Json | null;
  lastError: string | null;
  uniqueKey: string | null;
  scheduleId: string | null;
}

export interface EnqueueOptions {
  priority?: number;
  /** Delay in ms from now. Ignored if runAt is set. */
  delayMs?: number;
  runAt?: number;
  maxAttempts?: number;
  backoff?: BackoffStrategy;
  timeoutMs?: number;
  uniqueKey?: string;
  scheduleId?: string;
}

export interface EnqueueRequest extends EnqueueOptions {
  queue: string;
  name: string;
  payload?: Json;
}

export interface JobFilter {
  queue?: string;
  state?: JobState;
  name?: string;
  scheduleId?: string;
  /** Return jobs with id < cursor (ids are time-sortable). */
  cursor?: string;
  limit?: number;
}

export interface Page<T> {
  items: T[];
  nextCursor: string | null;
}

export interface QueueSettings {
  name: string;
  paused: boolean;
  /** Max jobs active at once across all workers; 0 = unlimited. */
  concurrency: number;
  /** Token bucket: at most rateLimitMax claims per rateLimitIntervalMs. 0 = unlimited. */
  rateLimitMax: number;
  rateLimitIntervalMs: number;
  defaultMaxAttempts: number;
  createdAt: number;
  updatedAt: number;
}

export type QueueCounts = Record<JobState, number>;

export interface QueueStats extends QueueSettings {
  counts: QueueCounts;
  oldestWaitingAt: number | null;
}

export type OverlapPolicy = "allow" | "skip";

export interface Schedule {
  id: string;
  name: string;
  queue: string;
  jobName: string;
  payload: Json;
  cron: string;
  timezone: string;
  enabled: boolean;
  /** "skip" does not enqueue if the previous run is still unfinished. */
  overlap: OverlapPolicy;
  nextRunAt: number;
  lastRunAt: number | null;
  lastJobId: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface Webhook {
  id: string;
  url: string;
  secret: string;
  /** Event names, or ["*"]. */
  events: string[];
  /** Restrict to these queues; empty = all. */
  queues: string[];
  enabled: boolean;
  createdAt: number;
}

export type DeliveryStatus = "pending" | "succeeded" | "failed";

export interface WebhookDelivery {
  id: string;
  webhookId: string;
  event: string;
  body: string;
  status: DeliveryStatus;
  attempts: number;
  lastStatusCode: number | null;
  lastError: string | null;
  nextAttemptAt: number | null;
  createdAt: number;
  updatedAt: number;
}

export type Scope = "jobs:read" | "jobs:write" | "workers" | "admin";
export const ALL_SCOPES: Scope[] = ["jobs:read", "jobs:write", "workers", "admin"];

export interface ApiToken {
  id: string;
  name: string;
  /** sha256 of the secret, hex. */
  hash: string;
  scopes: Scope[];
  createdAt: number;
  lastUsedAt: number | null;
  revokedAt: number | null;
}

export interface JobEvent {
  id: number;
  jobId: string;
  type: string;
  data: Json | null;
  at: number;
}
