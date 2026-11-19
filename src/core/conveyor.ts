import { ConflictError, NotFoundError, QueuePausedError, ValidationError } from "./errors.js";
import { EventBus } from "./events.js";
import { silentLogger, type Logger } from "./logger.js";
import { DEFAULT_CONFIG, type ConveyorConfig } from "./config.js";
import { randomUUID } from "node:crypto";
import { dependencyResult, type DependencyResult } from "./dependencies.js";
import { flowStatus, MAX_DEPENDENCIES, planFlow, stateCounts } from "./flow.js";
import type {
  DependencyEdge,
  DependencyInput,
  EnqueueRequest,
  FailurePolicy,
  Flow,
  FlowOutcome,
  FlowRequest,
  Job,
  LinkedJob,
  JobFilter,
  Page,
  QueueSettings,
  QueueStats,
} from "./types.js";
import { FAILURE_POLICIES } from "./types.js";
import type { JobTransition, Store } from "../storage/store.js";
import { systemClock, type Clock } from "../util/clock.js";
import { jobId } from "../util/ids.js";
import { DEFAULT_BACKOFF } from "../util/backoff.js";
import { NAME_PATTERN } from "../util/validate.js";

export interface ConveyorOptions {
  store: Store;
  config?: Partial<ConveyorConfig>;
  clock?: Clock;
  logger?: Logger;
  events?: EventBus;
}

const MAX_PRIORITY = 1000;

export { MAX_DEPENDENCIES, MAX_FLOW_JOBS } from "./flow.js";

/** Largest page of GET /v1/jobs/:id/dependencies. */
const MAX_DEPENDENCY_PAGE = 500;

/**
 * Latest run time a job can have: the last instant whose ISO-8601 form has a four-digit year
 * (the API renders times that way). Earliest is the Unix epoch.
 */
export const MAX_RUN_AT = Date.UTC(9999, 11, 31, 23, 59, 59, 999);

/**
 * Longest delay accepted anywhere a delay is given (delayMs, retryInMs, backoff settings):
 * 100 years. That keeps every computed run time far inside MAX_RUN_AT, and a longer delay is
 * almost certainly a unit mistake.
 */
export const MAX_DELAY_MS = 100 * 365.25 * 24 * 60 * 60 * 1000;

const RUN_AT_RANGE = `between ${new Date(0).toISOString()} and ${new Date(MAX_RUN_AT).toISOString()}`;

/** Throws a ValidationError unless `value` is a delay between 0 and MAX_DELAY_MS. */
export function assertDelay(value: unknown, name: string): void {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new ValidationError(`${name} must be a non-negative number`);
  }
  if (value > MAX_DELAY_MS) throw new ValidationError(`${name} must be at most ${MAX_DELAY_MS} (100 years)`);
}

// Producer-side API
export class Conveyor {
  readonly store: Store;
  readonly config: ConveyorConfig;
  readonly clock: Clock;
  readonly logger: Logger;
  readonly events: EventBus;

  constructor(opts: ConveyorOptions) {
    this.store = opts.store;
    this.config = { ...DEFAULT_CONFIG, ...opts.config };
    this.clock = opts.clock ?? systemClock;
    this.logger = opts.logger ?? silentLogger;
    this.events = opts.events ?? new EventBus();
  }

  async init(): Promise<void> {
    await this.store.migrate();
  }

  async enqueue(req: EnqueueRequest): Promise<Job> {
    const [job] = await this.enqueueMany([req]);
    return job!;
  }

  /**
   * Enqueue several jobs atomically: if any request is invalid or the insert fails, no job is
   * stored. Unique keys are de-duplicated within the batch as well as against existing jobs.
   */
  async enqueueBulk(reqs: EnqueueRequest[]): Promise<Job[]> {
    if (reqs.length > 1000) throw new ValidationError("at most 1000 jobs per bulk request");
    return this.enqueueMany(reqs);
  }

  private async enqueueMany(reqs: EnqueueRequest[]): Promise<Job[]> {
    const deps = reqs.map((r) => this.validate(r));
    const now = this.clock.now();
    const results: Job[] = [];
    const created: Job[] = [];
    const edges: DependencyEdge[] = [];
    const byUniqueKey = new Map<string, Job>();
    const queues = new Map<string, QueueSettings>();

    for (const [i, req] of reqs.entries()) {
      const key = req.uniqueKey ? `${req.queue}\u0000${req.uniqueKey}` : undefined;
      if (key) {
        const existing = byUniqueKey.get(key) ?? (await this.store.findActiveByUniqueKey(req.queue, req.uniqueKey!));
        if (existing) {
          byUniqueKey.set(key, existing);
          results.push(existing);
          continue;
        }
      }
      let queue = queues.get(req.queue);
      if (!queue) queues.set(req.queue, (queue = await this.queueSettings(req.queue)));
      const job = this.newJob(req, queue, now, deps[i]!.length > 0);
      for (const d of deps[i]!) edges.push({ childId: job.id, parentId: d.jobId, onFailure: d.onFailure });
      if (key) byUniqueKey.set(key, job);
      created.push(job);
      results.push(job);
    }

    // Dependencies are checked and settled inside the insert's transaction: a job whose
    // dependencies have all finished already comes back unblocked.
    const settled = await this.store.insertJobs(
      created,
      created.map((job) => ({ jobId: job.id, type: "enqueued", data: { state: job.state }, at: now })),
      edges,
    );
    for (const job of created) {
      this.events.emit("job.enqueued", job);
      this.logger.debug("job enqueued", { jobId: job.id, queue: job.queue, name: job.name });
    }
    this.announce(settled);
    const latest = new Map(settled.map((t) => [t.job.id, t.job]));
    return results.map((job) => latest.get(job.id) ?? job);
  }

  private newJob(req: EnqueueRequest, queue: QueueSettings, now: number, blocked: boolean): Job {
    const runAt = req.runAt ?? now + (req.delayMs ?? 0);
    return {
      id: jobId(now),
      queue: req.queue,
      name: req.name,
      payload: req.payload ?? null,
      state: blocked ? "blocked" : runAt > now ? "delayed" : "waiting",
      priority: req.priority ?? 0,
      attempts: 0,
      maxAttempts: req.maxAttempts ?? queue.defaultMaxAttempts,
      backoff: req.backoff ?? DEFAULT_BACKOFF,
      timeoutMs: req.timeoutMs ?? 0,
      runAt,
      createdAt: now,
      updatedAt: now,
      startedAt: null,
      finishedAt: null,
      workerId: null,
      leaseToken: null,
      leaseExpiresAt: null,
      progress: 0,
      result: null,
      lastError: null,
      uniqueKey: req.uniqueKey ?? null,
      scheduleId: req.scheduleId ?? null,
      flowId: null,
      flowKey: null,
      terminatedBy: null,
    };
  }

  /** Emit flow.completed / flow.failed for verdicts a transition recorded. Call after it committed. */
  announceFlows(outcomes: FlowOutcome[]): void {
    for (const outcome of outcomes) {
      this.events.emit(outcome.event, outcome);
      this.logger.info(outcome.event, { flowId: outcome.flowId, reason: outcome.reason, jobId: outcome.jobId });
    }
  }

  /**
   * Emit the events for jobs a transition changed as a side effect: dependents unblocked or
   * cancelled, and dependents brought back by a retry. Call after the transition committed.
   */
  announce(transitions: JobTransition[]): void {
    for (const { job, from, cause, blockedMs } of transitions) {
      if (job.state === "cancelled") {
        this.events.emit("job.cancelled", job);
        this.logger.info("job cancelled by its dependency", { jobId: job.id, dependencyId: cause });
      } else if (from === "blocked") {
        this.events.emit("job.unblocked", job, blockedMs ?? 0);
      } else {
        this.events.emit("job.enqueued", job);
      }
    }
  }

  /**
   * Submit a flow: jobs that depend on each other by key, and on existing jobs by id. Either
   * the whole flow is stored or, if anything is wrong with it, nothing is: see planFlow for the
   * structural checks. A `fail` dependency on an existing job that is already dead or
   * cancelled is a ConflictError; with `ignore` or `continue` it counts as settled at once.
   */
  async enqueueFlow(req: FlowRequest): Promise<Flow> {
    const plan = planFlow(req);
    for (const node of plan.jobs) {
      try {
        this.validate({ ...node, dependsOn: undefined });
      } catch (err) {
        // validate() only throws ValidationErrors; name the job it was about.
        throw new ValidationError(`job ${node.key}: ${(err as Error).message}`);
      }
    }
    const now = this.clock.now();
    const flowId = randomUUID();
    const blocked = new Set(plan.edges.map((e) => e.childKey));
    const queues = new Map<string, QueueSettings>();
    const ids = new Map<string, string>();
    const jobs: Job[] = [];
    for (const node of plan.jobs) {
      let queue = queues.get(node.queue);
      if (!queue) queues.set(node.queue, (queue = await this.queueSettings(node.queue)));
      const { key, dependsOn: _deps, ...req } = node;
      // Created in dependency order, so ids (which sort by creation) do too.
      const job = { ...this.newJob(req, queue, now, blocked.has(key)), flowId, flowKey: key };
      ids.set(key, job.id);
      jobs.push(job);
    }
    const dependencies: DependencyEdge[] = plan.edges.map((e) => ({
      childId: ids.get(e.childKey)!,
      parentId: "key" in e.parent ? ids.get(e.parent.key)! : e.parent.jobId,
      onFailure: e.onFailure,
    }));
    const settled = await this.store.insertFlow({
      id: flowId,
      jobs,
      dependencies,
      events: jobs.map((job) => ({ jobId: job.id, type: "enqueued", data: { state: job.state, flowId }, at: now })),
    });
    for (const job of jobs) this.events.emit("job.enqueued", job);
    this.announce(settled);
    this.logger.debug("flow enqueued", { flowId, jobs: jobs.length });
    return this.getFlow(flowId);
  }

  async getFlow(id: string): Promise<Flow> {
    const jobs = await this.store.listFlowJobs(id);
    if (jobs.length === 0) throw new NotFoundError(`flow ${id} not found`);
    return {
      id,
      status: flowStatus(jobs),
      outcome: (await this.store.getFlowOutcome(id)) ?? null,
      counts: stateCounts(jobs),
      jobs,
      edges: await this.store.listFlowDependencies(id),
    };
  }

  /**
   * Cancel every unfinished job of a flow, running ones included (their workers find out at
   * their next heartbeat). Jobs outside the flow that depend on them are settled by their
   * policies. A ConflictError if every job has already finished.
   */
  async cancelFlow(id: string): Promise<Flow> {
    const res = await this.store.cancelFlow(id, this.clock.now());
    if (!res) throw new NotFoundError(`flow ${id} not found`);
    if (res.jobs.length === 0) throw new ConflictError(`flow ${id} has already finished`);
    for (const t of res.jobs) this.events.emit("job.cancelled", t.job);
    this.announce(res.dependents);
    this.announceFlows(res.flowOutcomes);
    return this.getFlow(id);
  }

  /**
   * Every dependency of a job as it sees them, with results: what ctx.dependency(key) looks up.
   * ctx.dependencies() is this filtered by isVisibleToDependent.
   */
  async dependencyResults(job: Job): Promise<DependencyResult[]> {
    return (await this.store.listDependencyJobs(job.id)).map((link) => dependencyResult(job, link));
  }

  /**
   * A page of a job's dependencies with results, ordered by id: what GET
   * /v1/jobs/:id/dependencies returns. By default it is what the job itself sees (`ignore`
   * dependencies that ended dead or cancelled are left out); `all` includes them.
   */
  async dependencyResultsPage(id: string, page: { cursor?: string; limit?: number; all?: boolean } = {}): Promise<Page<DependencyResult>> {
    const job = await this.getJob(id);
    const limit = Math.min(Math.max(page.limit ?? 50, 1), MAX_DEPENDENCY_PAGE);
    const links = await this.store.listDependencyJobs(id, { after: page.cursor, limit: limit + 1, visibleOnly: !page.all });
    const items = links.slice(0, limit).map((link) => dependencyResult(job, link));
    return { items, nextCursor: links.length > limit ? items[items.length - 1]!.id : null };
  }

  /** The jobs that depend on a job, newest first. */
  async dependents(id: string, page: { cursor?: string; limit?: number } = {}): Promise<Page<LinkedJob>> {
    await this.getJob(id);
    return this.store.listDependentJobs(id, page);
  }

  async getJob(id: string): Promise<Job> {
    const job = await this.store.getJob(id);
    if (!job) throw new NotFoundError(`job ${id} not found`);
    return job;
  }

  listJobs(filter: JobFilter): Promise<Page<Job>> {
    return this.store.listJobs(filter);
  }

  /** Cancel an unfinished job. Its dependents are settled by their failure policies. */
  async cancel(id: string): Promise<Job> {
    const res = await this.store.cancelJob(id, this.clock.now());
    if (!res) throw new ConflictError(`job ${id} is already ${(await this.getJob(id)).state}`);
    this.events.emit("job.cancelled", res.job);
    this.announce(res.dependents);
    this.announceFlows(res.flowOutcomes);
    return res.job;
  }

  /**
   * Re-run a dead or cancelled job from scratch. Dependents that were cancelled because it
   * failed come back too, blocked until it finishes.
   */
  async retry(id: string): Promise<Job> {
    const res = await this.store.retryJob(id, this.clock.now());
    if (!res) {
      const job = await this.getJob(id);
      throw new ConflictError(`only dead or cancelled jobs can be retried (job is ${job.state})`);
    }
    this.events.emit("job.enqueued", res.job);
    this.announce(res.dependents);
    return res.job;
  }

  /** Retry every dead job in a queue (up to `limit`). Returns how many were retried. */
  async retryAllDead(queue: string, limit = 1000): Promise<number> {
    return (await this.retryDead(queue, limit)).retried;
  }

  /**
   * Retry every dead job in a queue (up to `limit`). A job that can't be retried (a dependency
   * it was created with no longer exists) is skipped, with the reason, and stays dead.
   */
  async retryDead(queue: string, limit = 1000): Promise<{ retried: number; skipped: { jobId: string; error: string }[] }> {
    const page = await this.store.listJobs({ queue, state: "dead", limit });
    const skipped: { jobId: string; error: string }[] = [];
    for (const job of page.items) {
      try {
        await this.retry(job.id);
      } catch (err) {
        if (!(err instanceof ConflictError)) throw err;
        skipped.push({ jobId: job.id, error: err.message });
        this.logger.warn("dead job not retried", { jobId: job.id, error: err.message });
      }
    }
    return { retried: page.items.length - skipped.length, skipped };
  }

  async jobEvents(id: string) {
    await this.getJob(id);
    return this.store.listJobEvents(id);
  }

  async queueSettings(name: string): Promise<QueueSettings> {
    const existing = await this.store.getQueue(name);
    if (existing) return existing;
    const now = this.clock.now();
    return {
      name,
      paused: false,
      concurrency: 0,
      rateLimitMax: 0,
      rateLimitIntervalMs: 0,
      defaultMaxAttempts: 3,
      createdAt: now,
      updatedAt: now,
    };
  }

  async updateQueue(
    name: string,
    patch: Partial<Pick<QueueSettings, "concurrency" | "rateLimitMax" | "rateLimitIntervalMs" | "defaultMaxAttempts" | "paused">>,
  ): Promise<QueueSettings> {
    if (!NAME_PATTERN.test(name)) throw new ValidationError("invalid queue name");
    const current = await this.queueSettings(name);
    const next: QueueSettings = { ...current, ...patch, updatedAt: this.clock.now() };
    if (next.concurrency < 0 || next.rateLimitMax < 0 || next.rateLimitIntervalMs < 0) {
      throw new ValidationError("queue limits must be non-negative");
    }
    if (next.rateLimitMax > 0 && next.rateLimitIntervalMs === 0) {
      throw new ValidationError("rateLimitIntervalMs is required when rateLimitMax is set");
    }
    if (next.defaultMaxAttempts < 1) throw new ValidationError("defaultMaxAttempts must be >= 1");
    await this.store.upsertQueue(next);
    if (patch.paused !== undefined && patch.paused !== current.paused) {
      this.events.emit(patch.paused ? "queue.paused" : "queue.resumed", name);
    }
    return next;
  }

  pause(name: string) {
    return this.updateQueue(name, { paused: true });
  }

  resume(name: string) {
    return this.updateQueue(name, { paused: false });
  }

  async queueStats(name: string): Promise<QueueStats> {
    const settings = await this.queueSettings(name);
    return {
      ...settings,
      counts: await this.store.countJobs(name),
      oldestWaitingAt: await this.store.oldestWaiting(name),
    };
  }

  async listQueues(): Promise<QueueStats[]> {
    const queues = await this.store.listQueues();
    return Promise.all(queues.map((q) => this.queueStats(q.name)));
  }

  /** Fails fast if a queue is paused; used by API producers that opt in. */
  async assertAccepting(queue: string): Promise<void> {
    const q = await this.queueSettings(queue);
    if (q.paused) throw new QueuePausedError(queue);
  }

  /** Check a request; returns its dependencies in normal form. */
  private validate(req: EnqueueRequest): { jobId: string; onFailure: FailurePolicy }[] {
    if (!req.queue || !NAME_PATTERN.test(req.queue)) throw new ValidationError("invalid queue name");
    if (!req.name || !NAME_PATTERN.test(req.name)) throw new ValidationError("invalid job name");
    if (req.priority !== undefined && (!Number.isInteger(req.priority) || Math.abs(req.priority) > MAX_PRIORITY)) {
      throw new ValidationError(`priority must be an integer between -${MAX_PRIORITY} and ${MAX_PRIORITY}`);
    }
    if (req.maxAttempts !== undefined && (!Number.isInteger(req.maxAttempts) || req.maxAttempts < 1)) {
      throw new ValidationError("maxAttempts must be a positive integer");
    }
    if (req.runAt !== undefined) {
      if (!Number.isFinite(req.runAt)) throw new ValidationError("runAt must be a finite timestamp (ms since epoch)");
      if (req.runAt < 0 || req.runAt > MAX_RUN_AT) throw new ValidationError(`runAt must be ${RUN_AT_RANGE}`);
    }
    if (req.delayMs !== undefined) assertDelay(req.delayMs, "delayMs");
    if (req.backoff) {
      const b = req.backoff as unknown as Record<string, unknown>;
      for (const key of ["delayMs", "stepMs", "baseMs", "maxMs"]) {
        if (key in b) assertDelay(b[key], `backoff.${key}`);
      }
    }
    if (req.timeoutMs !== undefined && (!Number.isFinite(req.timeoutMs) || req.timeoutMs < 0)) {
      throw new ValidationError("timeoutMs must be a non-negative number");
    }
    if (req.uniqueKey !== undefined && (req.uniqueKey.length === 0 || req.uniqueKey.length > 256)) {
      throw new ValidationError("uniqueKey must be 1-256 characters");
    }
    const size = Buffer.byteLength(JSON.stringify(req.payload ?? null));
    if (size > this.config.maxPayloadBytes) {
      throw new ValidationError(`payload is ${size} bytes; limit is ${this.config.maxPayloadBytes}`);
    }
    return parseDependencies(req.dependsOn);
  }
}

function parseDependencies(raw: DependencyInput[] | undefined): { jobId: string; onFailure: FailurePolicy }[] {
  if (raw === undefined) return [];
  if (!Array.isArray(raw)) throw new ValidationError("dependsOn must be an array");
  if (raw.length > MAX_DEPENDENCIES) throw new ValidationError(`a job can have at most ${MAX_DEPENDENCIES} dependencies`);
  const seen = new Set<string>();
  return raw.map((entry) => {
    const dep = typeof entry === "string" ? { jobId: entry } : entry;
    if (typeof dep !== "object" || dep === null || typeof dep.jobId !== "string" || dep.jobId.length === 0) {
      throw new ValidationError("each dependency must be a job id or {jobId, onFailure}");
    }
    const onFailure = dep.onFailure ?? "fail";
    if (!FAILURE_POLICIES.includes(onFailure)) {
      throw new ValidationError(`onFailure must be one of ${FAILURE_POLICIES.join(", ")}`);
    }
    if (seen.has(dep.jobId)) throw new ValidationError(`dependency ${dep.jobId} is listed twice`);
    seen.add(dep.jobId);
    return { jobId: dep.jobId, onFailure };
  });
}
