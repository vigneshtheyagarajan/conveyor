import type {
  ApiToken,
  Dependency,
  DependencyEdge,
  Job,
  JobEvent,
  JobFilter,
  FlowOutcome,
  JobState,
  LinkedJob,
  Page,
  QueueCounts,
  QueueSettings,
  Schedule,
  Webhook,
  WebhookDelivery,
} from "../core/types.js";
import { JOB_STATES } from "../core/types.js";
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

/** A flow to insert: its jobs in dependency order, their edges and history entries. */
export interface NewFlow {
  id: string;
  /** Every job has `flowId` = `id` and a `flowKey` unique in the flow. */
  jobs: Job[];
  dependencies: DependencyEdge[];
  events: NewJobEvent[];
}

/** What cancelling a flow changed. */
export interface FlowCancellation {
  /** The flow's jobs that were unfinished, now cancelled. */
  jobs: JobTransition[];
  /** Jobs outside the flow that depend on them, settled by their policies. */
  dependents: JobTransition[];
  /** Verdicts recorded: this flow's flow.failed (unless it had one), and any other flow finished by the cascade. */
  flowOutcomes: FlowOutcome[];
}

/** One job's state change. */
export interface JobTransition {
  /** The job after the change. */
  job: Job;
  from: JobState;
  /** For a job leaving `blocked`: how long it was blocked, in ms. */
  blockedMs?: number;
  /**
   * For a change made because of another job (a dependent unblocked, cancelled or brought back
   * by a retry): that job's id. Null when nothing specific caused it (e.g. the reconcile sweep).
   */
  cause: string | null;
}

/** The result of a transition: the job it was asked for, and what happened to its dependents. */
export interface TransitionResult {
  job: Job;
  from: JobState;
  /** Dependents changed as a consequence, in the order they changed. */
  dependents: JobTransition[];
  /** Flow verdicts this transition recorded (at most one per flow, ever). */
  flowOutcomes: FlowOutcome[];
}

/** What the reconcile sweep changed. */
export interface ReconcileResult {
  jobs: JobTransition[];
  flowOutcomes: FlowOutcome[];
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
  /**
   * Insert jobs, their history entries and their dependency edges in one transaction: either
   * everything is stored or nothing is. A job with dependencies must be inserted as `blocked`;
   * it is then settled against its parents' current states in the same transaction (so it may
   * leave `blocked` straight away, at its `updatedAt`) and those changes are returned. Throws a ValidationError if
   * a parent doesn't exist and a ConflictError if a `fail` parent is already dead or cancelled.
   */
  insertJobs(jobs: Job[], events: NewJobEvent[], dependencies?: DependencyEdge[]): Promise<JobTransition[]>;
  /**
   * Insert a flow in one transaction, like insertJobs, after checking that it is one: every
   * job belongs to it, keys are unique, and `jobs` is in dependency order (each edge between
   * flow jobs points to an earlier job), which also rules out cycles. Nothing is stored if any
   * check fails. Returns the jobs released at once (their dependencies had all completed).
   */
  insertFlow(flow: NewFlow): Promise<JobTransition[]>;
  /**
   * Cancel every unfinished job of a flow, and settle their dependents outside the flow, in
   * one transaction. Undefined if the flow has no jobs.
   */
  cancelFlow(flowId: string, now: number): Promise<FlowCancellation | undefined>;
  /** A flow's jobs, in insertion (dependency) order. */
  listFlowJobs(flowId: string): Promise<Job[]>;
  /** The dependencies of a flow's jobs (including ones outside the flow), by child then parent id. */
  listFlowDependencies(flowId: string): Promise<DependencyEdge[]>;
  getJob(id: string): Promise<Job | undefined>;
  /** A job's dependencies (the jobs it waits for), ordered by id. */
  listDependencies(jobId: string): Promise<Dependency[]>;
  /**
   * A job's dependencies with their jobs, ordered by id: all of them, or with `after` and
   * `limit` one page. `visibleOnly` leaves out what a running dependent doesn't see: `ignore`
   * dependencies that ended dead or cancelled (the rule of isVisibleToDependent in
   * core/dependencies.ts).
   */
  listDependencyJobs(jobId: string, opts?: { after?: string; limit?: number; visibleOnly?: boolean }): Promise<LinkedJob[]>;
  /** The jobs that depend on a job, newest first, paged like listJobs. */
  listDependentJobs(jobId: string, page: { cursor?: string; limit?: number }): Promise<Page<LinkedJob>>;
  findActiveByUniqueKey(queue: string, uniqueKey: string): Promise<Job | undefined>;
  listJobs(filter: JobFilter): Promise<Page<Job>>;
  countJobs(queue?: string): Promise<QueueCounts>;
  countActive(queue: string): Promise<number>;
  oldestWaiting(queue: string): Promise<number | null>;
  findClaimCandidate(query: ClaimCandidateQuery): Promise<Job | undefined>;
  activateJob(id: string, params: ActivateParams): Promise<void>;
  extendLease(id: string, leaseExpiresAt: number, now: number): Promise<void>;
  updateProgress(id: string, progress: number, now: number): Promise<void>;

  // Transitions. Each one checks the state it expects (and, for a leased job, the lease token),
  // then writes the change, its history entry and the effect on dependents in one transaction.
  // It returns undefined, and changes nothing, if the job isn't in that state (or doesn't
  // exist). Implementations must make the dependent check atomic with the change: two parents
  // finishing at once, in different processes, must not both decide the other is unfinished.

  /** active (holding `leaseToken`) -> completed. Unblocks dependents whose parents have all settled. */
  completeJob(id: string, leaseToken: string, result: Json | null, now: number): Promise<TransitionResult | undefined>;
  /** active (holding `leaseToken`) -> delayed until `runAt`, for another attempt. */
  scheduleRetry(id: string, leaseToken: string, error: string, runAt: number, now: number): Promise<TransitionResult | undefined>;
  /**
   * active (holding `leaseToken`) -> dead, and the dependents' failure policies applied. Every
   * path that ends a job as dead goes through this method (the reaper too, with the expired
   * lease's token), so propagation can't be skipped.
   */
  markDead(id: string, leaseToken: string, error: string, now: number): Promise<TransitionResult | undefined>;
  /** blocked | delayed | waiting | active -> cancelled, and the dependents' policies applied. */
  cancelJob(id: string, now: number): Promise<TransitionResult | undefined>;
  /**
   * dead | cancelled -> waiting (or blocked, for a job with dependencies), attempts reset.
   * Dependents cancelled because of this job come back as blocked, transitively. Throws a
   * ConflictError if one of the job's own `fail` dependencies is still dead or cancelled.
   */
  retryJob(id: string, now: number): Promise<TransitionResult | undefined>;
  /**
   * The reaper: active with the lease `leaseToken` (as findExpiredLeases returned it), expired
   * before `now` -> waiting. Changes nothing if the worker finished, heartbeated or was replaced
   * since.
   */
  requeueExpired(id: string, leaseToken: string | null, now: number): Promise<TransitionResult | undefined>;
  /** active (holding `leaseToken`) -> waiting without counting the claim: attempts goes down by one (not below 0). */
  releaseLease(id: string, leaseToken: string, now: number): Promise<TransitionResult | undefined>;
  /**
   * Safety net: settle up to `limit` blocked jobs whose parents no longer keep them blocked.
   * The transitions above already do this, so in normal operation it finds nothing.
   */
  reconcileBlocked(now: number, limit: number): Promise<ReconcileResult>;

  // Flow verdicts. Every transition that finishes a flow job (or cancels a flow) records the
  // flow's verdict, if it decides one, in its own transaction. The primary key on the flow id
  // makes a second verdict impossible, however many processes race.

  getFlowOutcome(flowId: string): Promise<FlowOutcome | undefined>;
  /** Verdicts no dispatcher has claimed yet, oldest first. */
  listPendingFlowOutcomes(limit: number): Promise<FlowOutcome[]>;
  /**
   * Claim a verdict for delivery and store its webhook deliveries, in one transaction. False,
   * storing nothing, if another dispatcher claimed it first.
   */
  claimFlowOutcome(flowId: string, now: number, deliveries: WebhookDelivery[]): Promise<boolean>;

  findExpiredLeases(now: number, limit: number): Promise<Job[]>;
  promoteDelayed(now: number, limit: number): Promise<number>;
  /**
   * Deletes completed and cancelled jobs finished before `ts`, except those with a dependent that
   * is unfinished. Dead jobs are kept.
   */
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
  return Object.fromEntries(JOB_STATES.map((s) => [s, 0])) as QueueCounts;
}
