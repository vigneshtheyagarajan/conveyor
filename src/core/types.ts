import type { BackoffStrategy } from "../util/backoff.js";
import type { Json } from "../util/json.js";

/**
 * `blocked`: waiting for its dependencies to finish. Not claimable; it moves to `waiting` or
 * `delayed` once every dependency has settled, or to `cancelled` when one fails under the
 * `fail` policy.
 */
export type JobState = "blocked" | "delayed" | "waiting" | "active" | "completed" | "dead" | "cancelled";

export const JOB_STATES: JobState[] = ["blocked", "delayed", "waiting", "active", "completed", "dead", "cancelled"];
export const TERMINAL_STATES: JobState[] = ["completed", "dead", "cancelled"];
/** States a job can still leave: everything that is not terminal. */
export const UNFINISHED_STATES: JobState[] = JOB_STATES.filter((s) => !TERMINAL_STATES.includes(s));

/**
 * What a dependent does when a dependency ends dead or cancelled instead of completed.
 * - `fail` (default): the dependent is cancelled without running.
 * - `ignore`: the dependency counts as settled; the dependent runs and doesn't see it.
 * - `continue`: the dependency counts as settled; the dependent runs and sees its state and error.
 */
export type FailurePolicy = "fail" | "ignore" | "continue";
export const FAILURE_POLICIES: FailurePolicy[] = ["fail", "ignore", "continue"];

/** One dependency of a job. */
export interface Dependency {
  /** The job that has to finish first. */
  jobId: string;
  onFailure: FailurePolicy;
}

/** A dependency as given to enqueue: a job id, or a job id with a policy. */
export type DependencyInput = string | { jobId: string; onFailure?: FailurePolicy };

/** One edge of the dependency graph: `childId` waits for `parentId`. */
export interface DependencyEdge {
  childId: string;
  parentId: string;
  onFailure: FailurePolicy;
}

/** A job at the other end of a dependency edge, with the edge's policy. */
export interface LinkedJob {
  onFailure: FailurePolicy;
  job: Job;
}

/**
 * A dependency of a flow job: the key of another job in the flow (a string or `{key}`), or
 * the id of a job that already exists (`{jobId}`).
 */
export type FlowDependencyInput = string | { key: string; onFailure?: FailurePolicy } | { jobId: string; onFailure?: FailurePolicy };

/** One job of a flow. Flow jobs can't have a uniqueKey. */
export interface FlowJobInput extends Omit<EnqueueRequest, "dependsOn" | "uniqueKey"> {
  /** Names the job within the flow, for other jobs' dependsOn. */
  key: string;
  dependsOn?: FlowDependencyInput[];
}

export interface FlowRequest {
  jobs: FlowJobInput[];
  /** Policy for every dependency that doesn't name one. Default "fail". */
  onFailure?: FailurePolicy;
}

/**
 * Derived from the flow's jobs: `running` while any is unfinished; once all have finished,
 * `completed` if every one completed, `failed` if any is dead, otherwise `cancelled`.
 */
export type FlowStatus = "running" | "completed" | "failed" | "cancelled";

export type FlowEventName = "flow.completed" | "flow.failed";

/**
 * A flow's verdict: `flow.failed` as soon as one of its jobs is dead or cancelled (directly,
 * by its dependency, or by cancelFlow), `flow.completed` once every job has completed. Recorded
 * once, by whichever happens first; later changes (such as retries) don't record another.
 */
export interface FlowOutcome {
  flowId: string;
  event: FlowEventName;
  /** For flow.failed: whether a job died or was cancelled. Dead wins if both. */
  reason: "dead" | "cancelled" | null;
  /** For flow.failed: the job that died or was cancelled; null when cancelFlow cancelled the flow. */
  jobId: string | null;
  at: number;
  /** When a webhook dispatcher claimed it for delivery; null until then. */
  dispatchedAt: number | null;
}

export interface Flow {
  id: string;
  status: FlowStatus;
  /** The verdict, once there is one. */
  outcome: FlowOutcome | null;
  counts: Record<JobState, number>;
  /** In dependency order: every job comes after the flow jobs it depends on. */
  jobs: Job[];
  /** Every dependency of the flow's jobs, including ones on jobs outside the flow. */
  edges: DependencyEdge[];
}

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
  /** The flow this job was submitted in, and its key there. Null for jobs enqueued on their own. */
  flowId: string | null;
  flowKey: string | null;
  /** The dependency whose failure or cancellation cancelled this job without running it. */
  terminatedBy: string | null;
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
  /**
   * Jobs that must finish first. The job stays `blocked` until they have; see FailurePolicy for
   * what happens when one doesn't complete. Every job must already exist.
   */
  dependsOn?: DependencyInput[];
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
