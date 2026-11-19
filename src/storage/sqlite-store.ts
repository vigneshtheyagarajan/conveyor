import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite";
import { ConflictError, ValidationError } from "../core/errors.js";
import type {
  ApiToken,
  Dependency,
  DependencyEdge,
  FailurePolicy,
  FlowOutcome,
  Job,
  JobEvent,
  JobFilter,
  JobState,
  LinkedJob,
  Page,
  QueueCounts,
  QueueSettings,
  Schedule,
  Scope,
  Webhook,
  WebhookDelivery,
} from "../core/types.js";
import { UNFINISHED_STATES } from "../core/types.js";
import type { BackoffStrategy } from "../util/backoff.js";
import { safeParse, type Json } from "../util/json.js";
import { MIGRATIONS } from "./migrations.js";
import {
  emptyCounts,
  type ActivateParams,
  type ClaimCandidateQuery,
  type FlowCancellation,
  type ReconcileResult,
  type JobTransition,
  type NewFlow,
  type NewJobEvent,
  type Store,
  type TransitionResult,
} from "./store.js";

type Row = Record<string, SQLInputValue>;

const MAX_PAGE = 500;

/** The states a dependency can end in other than completed. */
const FAILED_STATES: JobState[] = ["dead", "cancelled"];

const sqlList = (states: JobState[]) => states.map((st) => `'${st}'`).join(", ");
const UNFINISHED_SQL = sqlList(UNFINISHED_STATES);
const FAILED_SQL = sqlList(FAILED_STATES);

export interface SqliteStoreOptions {
  /** Busy timeout for concurrent writers (other processes). */
  busyTimeoutMs?: number;
}

export class SqliteStore implements Store {
  readonly db: DatabaseSync;
  private statements = new Map<string, StatementSync>();

  constructor(path = ":memory:", options: SqliteStoreOptions = {}) {
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA busy_timeout = ${options.busyTimeoutMs ?? 5000}`);
    if (path !== ":memory:") this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA foreign_keys = ON");
    this.db.exec("PRAGMA synchronous = NORMAL");
  }

  private stmt(sql: string): StatementSync {
    let s = this.statements.get(sql);
    if (!s) {
      s = this.db.prepare(sql);
      this.statements.set(sql, s);
    }
    return s;
  }

  private run(sql: string, ...params: SQLInputValue[]) {
    return this.stmt(sql).run(...params);
  }

  private get(sql: string, ...params: SQLInputValue[]): Row | undefined {
    return this.stmt(sql).get(...params) as Row | undefined;
  }

  private all(sql: string, ...params: SQLInputValue[]): Row[] {
    return this.stmt(sql).all(...params) as Row[];
  }

  async migrate(): Promise<number> {
    const current = Number((this.db.prepare("PRAGMA user_version").get() as Row).user_version);
    if (current > MIGRATIONS.length) {
      // Written by a newer release; running older code against it could corrupt data.
      throw new Error(
        `database schema version ${current} is newer than this version of Conveyor supports (${MIGRATIONS.length}); ` +
          "upgrade Conveyor to open it",
      );
    }
    for (let i = current; i < MIGRATIONS.length; i++) {
      this.db.exec("BEGIN");
      try {
        this.db.exec(MIGRATIONS[i]!);
        this.db.exec(`PRAGMA user_version = ${i + 1}`);
        this.db.exec("COMMIT");
      } catch (err) {
        this.db.exec("ROLLBACK");
        throw err;
      }
    }
    return MIGRATIONS.length;
  }

  async close(): Promise<void> {
    this.statements.clear();
    this.db.close();
  }

  /**
   * Run `fn` in a write transaction and return its result.
   *
   * BEGIN IMMEDIATE takes the database's write lock before the first read, so the transaction
   * reads the latest committed data and nothing can commit between its reads and its writes.
   * (With a deferred BEGIN, a transaction that reads and then writes fails with SQLITE_BUSY if
   * another connection committed in between; busy_timeout doesn't help with that.) Another
   * process's transaction waits for the lock, up to busy_timeout.
   *
   * `fn` must be synchronous. This connection is shared by every caller in the process, and an
   * await inside would let another caller's statements run inside this transaction.
   */
  private tx<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = fn();
      this.db.exec("COMMIT");
      return result;
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  async insertJob(job: Job): Promise<void> {
    this.insertJobRow(job);
  }

  async insertJobs(jobs: Job[], events: NewJobEvent[], dependencies: DependencyEdge[] = []): Promise<JobTransition[]> {
    return this.tx(() => this.insertBatch(jobs, events, dependencies));
  }

  async insertFlow(flow: NewFlow): Promise<JobTransition[]> {
    return this.tx(() => {
      const position = new Map<string, number>();
      const keys = new Set<string>();
      flow.jobs.forEach((job, i) => {
        if (job.flowId !== flow.id) throw new ValidationError(`job ${job.id} does not belong to flow ${flow.id}`);
        if (job.flowKey === null || keys.has(job.flowKey)) throw new ValidationError(`flow keys must be present and unique (${job.flowKey})`);
        keys.add(job.flowKey);
        position.set(job.id, i);
      });
      for (const d of flow.dependencies) {
        const child = position.get(d.childId);
        if (child === undefined) throw new ValidationError(`dependency of ${d.childId}, which is not in flow ${flow.id}`);
        const parent = position.get(d.parentId);
        // An edge to a later (or the same) job means the order isn't a topological one, which
        // is always the case when there is a cycle.
        if (parent !== undefined && parent >= child) {
          throw new ValidationError(`flow jobs are not in dependency order: ${d.childId} depends on ${d.parentId}`);
        }
      }
      return this.insertBatch(flow.jobs, flow.events, flow.dependencies);
    });
  }

  /** insertJobs' and insertFlow's work, inside their transaction. */
  private insertBatch(jobs: Job[], events: NewJobEvent[], dependencies: DependencyEdge[]): JobTransition[] {
    for (const job of jobs) this.insertJobRow(job);
    for (const e of events) this.insertEventRow(e.jobId, e.type, e.data, e.at);
    const children = new Set<string>();
    for (const d of dependencies) {
      // Read inside the transaction: a parent that finishes after this read can't commit
      // until this insert has, and then it sees the new edge and settles the child itself.
      const parent = this.get("SELECT state FROM jobs WHERE id = ?", d.parentId);
      if (!parent) throw new ValidationError(`dependency ${d.parentId} not found`);
      if (d.onFailure === "fail" && FAILED_STATES.includes(parent.state as JobState)) {
        throw new ConflictError(`dependency ${d.parentId} is already ${parent.state}`);
      }
      this.run(
        "INSERT INTO job_dependencies (child_id, parent_id, on_failure) VALUES (?, ?, ?)",
        d.childId,
        d.parentId,
        d.onFailure,
      );
      children.add(d.childId);
    }
    const settled: JobTransition[] = [];
    for (const id of children) {
      const job = this.jobRow(id);
      const t = job && this.settleChild(id, job.updatedAt, null);
      if (t) settled.push(t);
    }
    return settled;
  }

  async cancelFlow(flowId: string, now: number): Promise<FlowCancellation | undefined> {
    return this.tx(() => {
      const jobs = this.all("SELECT * FROM jobs WHERE flow_id = ? ORDER BY id", flowId).map(toJob);
      if (jobs.length === 0) return undefined;
      const cancelled: JobTransition[] = [];
      // Cancel every unfinished job directly first, so that none of them is instead cancelled
      // (or released) by its dependency's cancellation.
      for (const job of jobs.filter((j) => UNFINISHED_STATES.includes(j.state))) {
        this.run(
          `UPDATE jobs SET state = 'cancelled', finished_at = ?, updated_at = ?,
             lease_token = NULL, lease_expires_at = NULL
           WHERE id = ? AND state = ?`,
          now,
          now,
          job.id,
          job.state,
        );
        this.insertEventRow(job.id, "cancelled", { previousState: job.state, flowId }, now);
        cancelled.push({ job: this.jobRow(job.id)!, from: job.state, cause: null });
      }
      const dependents = cancelled.flatMap((t) => this.settleDependents(t.job.id, now));
      const flowOutcomes: FlowOutcome[] = [];
      if (cancelled.length > 0) {
        const own = this.insertFlowOutcome(flowId, "flow.failed", "cancelled", null, now);
        if (own) flowOutcomes.push(own);
      }
      // Dependents outside the flow may have finished their own flows.
      flowOutcomes.push(...this.recordFlowOutcomes(dependents.map((t) => t.job.id), now));
      return { jobs: cancelled, dependents, flowOutcomes };
    });
  }

  async listFlowJobs(flowId: string): Promise<Job[]> {
    return this.all("SELECT * FROM jobs WHERE flow_id = ? ORDER BY id", flowId).map(toJob);
  }

  async listFlowDependencies(flowId: string): Promise<DependencyEdge[]> {
    return this.all(
      `SELECT d.child_id, d.parent_id, d.on_failure FROM job_dependencies d
         JOIN jobs c ON c.id = d.child_id WHERE c.flow_id = ? ORDER BY d.child_id, d.parent_id`,
      flowId,
    ).map((r) => ({ childId: s(r.child_id), parentId: s(r.parent_id), onFailure: s(r.on_failure) as FailurePolicy }));
  }

  private insertJobRow(job: Job): void {
    this.run(
      `INSERT INTO jobs (id, queue, name, payload, state, priority, attempts, max_attempts, backoff,
        timeout_ms, run_at, created_at, updated_at, started_at, finished_at, worker_id, lease_token,
        lease_expires_at, progress, result, last_error, unique_key, schedule_id, flow_id, flow_key,
        terminated_by)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      job.id,
      job.queue,
      job.name,
      JSON.stringify(job.payload ?? null),
      job.state,
      job.priority,
      job.attempts,
      job.maxAttempts,
      job.backoff ? JSON.stringify(job.backoff) : null,
      job.timeoutMs,
      job.runAt,
      job.createdAt,
      job.updatedAt,
      job.startedAt,
      job.finishedAt,
      job.workerId,
      job.leaseToken,
      job.leaseExpiresAt,
      job.progress,
      job.result === null ? null : JSON.stringify(job.result),
      job.lastError,
      job.uniqueKey,
      job.scheduleId,
      job.flowId ?? null,
      job.flowKey ?? null,
      job.terminatedBy ?? null,
    );
  }

  async getJob(id: string): Promise<Job | undefined> {
    return this.jobRow(id);
  }

  private jobRow(id: string): Job | undefined {
    const row = this.get("SELECT * FROM jobs WHERE id = ?", id);
    return row ? toJob(row) : undefined;
  }

  async listDependencies(jobId: string): Promise<Dependency[]> {
    return this.all("SELECT parent_id, on_failure FROM job_dependencies WHERE child_id = ? ORDER BY parent_id", jobId).map(
      (r) => ({ jobId: s(r.parent_id), onFailure: s(r.on_failure) as FailurePolicy }),
    );
  }

  async listDependencyJobs(jobId: string, opts: { after?: string; limit?: number; visibleOnly?: boolean } = {}): Promise<LinkedJob[]> {
    return this.all(
      `SELECT d.on_failure AS edge_on_failure, p.* FROM job_dependencies d
         JOIN jobs p ON p.id = d.parent_id
        WHERE d.child_id = ? AND (? IS NULL OR d.parent_id > ?)
          AND (? = 0 OR d.on_failure <> 'ignore' OR p.state NOT IN (${FAILED_SQL}))
        ORDER BY d.parent_id LIMIT ?`,
      jobId,
      opts.after ?? null,
      opts.after ?? null,
      opts.visibleOnly ? 1 : 0,
      opts.limit ?? -1,
    ).map((r) => ({ onFailure: s(r.edge_on_failure) as FailurePolicy, job: toJob(r) }));
  }

  async listDependentJobs(jobId: string, page: { cursor?: string; limit?: number }): Promise<Page<LinkedJob>> {
    const limit = Math.min(Math.max(page.limit ?? 50, 1), MAX_PAGE);
    const rows = this.all(
      `SELECT d.on_failure AS edge_on_failure, c.* FROM job_dependencies d
         JOIN jobs c ON c.id = d.child_id
        WHERE d.parent_id = ? AND (? IS NULL OR d.child_id < ?)
        ORDER BY d.child_id DESC LIMIT ?`,
      jobId,
      page.cursor ?? null,
      page.cursor ?? null,
      limit + 1,
    );
    const items = rows.slice(0, limit).map((r) => ({ onFailure: s(r.edge_on_failure) as FailurePolicy, job: toJob(r) }));
    return { items, nextCursor: rows.length > limit ? items[items.length - 1]!.job.id : null };
  }

  async findActiveByUniqueKey(queue: string, uniqueKey: string): Promise<Job | undefined> {
    const row = this.get(
      `SELECT * FROM jobs WHERE queue = ? AND unique_key = ?
         AND state IN (${UNFINISHED_SQL}) ORDER BY id LIMIT 1`,
      queue,
      uniqueKey,
    );
    return row ? toJob(row) : undefined;
  }

  async listJobs(filter: JobFilter): Promise<Page<Job>> {
    const where: string[] = [];
    const params: SQLInputValue[] = [];
    if (filter.queue) {
      where.push("queue = ?");
      params.push(filter.queue);
    }
    if (filter.state) {
      where.push("state = ?");
      params.push(filter.state);
    }
    if (filter.name) {
      where.push("name = ?");
      params.push(filter.name);
    }
    if (filter.scheduleId) {
      where.push("schedule_id = ?");
      params.push(filter.scheduleId);
    }
    if (filter.cursor) {
      where.push("id < ?");
      params.push(filter.cursor);
    }
    const limit = Math.min(Math.max(filter.limit ?? 50, 1), MAX_PAGE);
    const sql =
      "SELECT * FROM jobs" +
      (where.length ? ` WHERE ${where.join(" AND ")}` : "") +
      " ORDER BY id DESC LIMIT ?";
    const rows = this.db.prepare(sql).all(...params, limit + 1) as Row[];
    const items = rows.slice(0, limit).map(toJob);
    const nextCursor = rows.length > limit ? items[items.length - 1]!.id : null;
    return { items, nextCursor };
  }

  async countJobs(queue?: string): Promise<QueueCounts> {
    const rows = queue
      ? this.all("SELECT state, COUNT(*) AS n FROM jobs WHERE queue = ? GROUP BY state", queue)
      : this.all("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state");
    const counts = emptyCounts();
    for (const r of rows) counts[r.state as JobState] = Number(r.n);
    return counts;
  }

  async countActive(queue: string): Promise<number> {
    const row = this.get("SELECT COUNT(*) AS n FROM jobs WHERE queue = ? AND state = 'active'", queue);
    return Number(row?.n ?? 0);
  }

  async oldestWaiting(queue: string): Promise<number | null> {
    const row = this.get(
      "SELECT MIN(run_at) AS t FROM jobs WHERE queue = ? AND state = 'waiting'",
      queue,
    );
    return row?.t == null ? null : Number(row.t);
  }

  async findClaimCandidate(query: ClaimCandidateQuery): Promise<Job | undefined> {
    const queues = query.queues.filter((q) => !query.exclude?.includes(q));
    if (queues.length === 0) return undefined;
    const placeholders = queues.map(() => "?").join(", ");
    const row = this.db
      .prepare(
        `SELECT * FROM jobs
          WHERE queue IN (${placeholders}) AND state = 'waiting' AND run_at <= ?
          ORDER BY priority DESC, run_at ASC, id ASC
          LIMIT 1`,
      )
      .get(...queues, query.now) as Row | undefined;
    return row ? toJob(row) : undefined;
  }

  async activateJob(id: string, p: ActivateParams): Promise<void> {
    this.run(
      `UPDATE jobs SET state = 'active', attempts = attempts + 1, worker_id = ?, lease_token = ?,
         lease_expires_at = ?, started_at = ?, updated_at = ?, progress = 0
       WHERE id = ?`,
      p.workerId,
      p.leaseToken,
      p.leaseExpiresAt,
      p.now,
      p.now,
      id,
    );
  }

  async extendLease(id: string, leaseExpiresAt: number, now: number): Promise<void> {
    this.run(
      "UPDATE jobs SET lease_expires_at = ?, updated_at = ? WHERE id = ? AND state = 'active'",
      leaseExpiresAt,
      now,
      id,
    );
  }

  async updateProgress(id: string, progress: number, now: number): Promise<void> {
    this.run("UPDATE jobs SET progress = ?, updated_at = ? WHERE id = ?", progress, now, id);
  }

  async completeJob(id: string, leaseToken: string, result: Json | null, now: number): Promise<TransitionResult | undefined> {
    return this.tx(() => {
      const res = this.run(
        `UPDATE jobs SET state = 'completed', result = ?, finished_at = ?, updated_at = ?,
           progress = 1, lease_token = NULL, lease_expires_at = NULL, last_error = NULL
         WHERE id = ? AND state = 'active' AND lease_token = ?`,
        result === null || result === undefined ? null : JSON.stringify(result),
        now,
        now,
        id,
        leaseToken,
      );
      if (Number(res.changes) === 0) return undefined;
      this.insertEventRow(id, "completed", null, now);
      return this.transitioned(id, "active", this.settleDependents(id, now), now);
    });
  }

  async scheduleRetry(id: string, leaseToken: string, error: string, runAt: number, now: number): Promise<TransitionResult | undefined> {
    return this.tx(() => {
      const res = this.run(
        `UPDATE jobs SET state = 'delayed', run_at = ?, last_error = ?, updated_at = ?,
           worker_id = NULL, lease_token = NULL, lease_expires_at = NULL
         WHERE id = ? AND state = 'active' AND lease_token = ?`,
        runAt,
        error,
        now,
        id,
        leaseToken,
      );
      if (Number(res.changes) === 0) return undefined;
      this.insertEventRow(id, "retry_scheduled", { error, delayMs: runAt - now }, now);
      return this.transitioned(id, "active", []);
    });
  }

  async markDead(id: string, leaseToken: string, error: string, now: number): Promise<TransitionResult | undefined> {
    return this.tx(() => {
      const res = this.run(
        `UPDATE jobs SET state = 'dead', last_error = ?, finished_at = ?, updated_at = ?,
           lease_token = NULL, lease_expires_at = NULL
         WHERE id = ? AND state = 'active' AND lease_token = ?`,
        error,
        now,
        now,
        id,
        leaseToken,
      );
      if (Number(res.changes) === 0) return undefined;
      const attempts = Number(this.get("SELECT attempts FROM jobs WHERE id = ?", id)!.attempts);
      this.insertEventRow(id, "dead", { error, attempts }, now);
      return this.transitioned(id, "active", this.settleDependents(id, now), now);
    });
  }

  async cancelJob(id: string, now: number): Promise<TransitionResult | undefined> {
    return this.tx(() => {
      const before = this.jobRow(id);
      if (!before || !UNFINISHED_STATES.includes(before.state)) return undefined;
      this.run(
        `UPDATE jobs SET state = 'cancelled', finished_at = ?, updated_at = ?,
           lease_token = NULL, lease_expires_at = NULL
         WHERE id = ? AND state = ?`,
        now,
        now,
        id,
        before.state,
      );
      this.insertEventRow(id, "cancelled", { previousState: before.state }, now);
      return this.transitioned(id, before.state, this.settleDependents(id, now), now);
    });
  }

  async retryJob(id: string, now: number): Promise<TransitionResult | undefined> {
    return this.tx(() => {
      const before = this.jobRow(id);
      if (!before || !FAILED_STATES.includes(before.state)) return undefined;
      const parents = this.parentRows(id);
      // A job never runs without a dependency it was created with.
      const missing = parents.filter((p) => p.state === null).map((p) => p.id);
      if (missing.length === 1) throw new ConflictError(`job ${id} can't be retried: its dependency ${missing[0]} no longer exists`);
      if (missing.length > 1) throw new ConflictError(`job ${id} can't be retried: its dependencies ${missing.join(", ")} no longer exist`);
      const failed = parents.find((p) => p.onFailure === "fail" && FAILED_STATES.includes(p.state!));
      if (failed) {
        throw new ConflictError(`dependency ${failed.id} is ${failed.state}; retry it first`);
      }
      const state: JobState = parents.some((p) => UNFINISHED_STATES.includes(p.state!)) ? "blocked" : "waiting";
      this.run(
        `UPDATE jobs SET state = ?, run_at = ?, updated_at = ?, finished_at = NULL, worker_id = NULL,
           lease_token = NULL, lease_expires_at = NULL, attempts = 0, terminated_by = NULL
         WHERE id = ? AND state = ?`,
        state,
        now,
        now,
        id,
        before.state,
      );
      this.insertEventRow(id, "retried", { previousState: before.state, state }, now);
      return this.transitioned(id, before.state, this.reviveDependents(id, now));
    });
  }

  /**
   * Bring back, as blocked, the dependents that were cancelled because `id` failed, and theirs,
   * transitively. Each is then settled: it stays blocked (`id` is unfinished again) unless
   * another of its `fail` dependencies is still dead or cancelled, which cancels it again.
   */
  private reviveDependents(id: string, now: number): JobTransition[] {
    const out: JobTransition[] = [];
    const queue = [id];
    for (let i = 0; i < queue.length; i++) {
      const parentId = queue[i]!;
      for (const r of this.all("SELECT id FROM jobs WHERE terminated_by = ? AND state = 'cancelled' ORDER BY id", parentId)) {
        const childId = s(r.id);
        this.run(
          `UPDATE jobs SET state = 'blocked', updated_at = ?, finished_at = NULL, terminated_by = NULL,
             last_error = NULL, attempts = 0
           WHERE id = ? AND state = 'cancelled'`,
          now,
          childId,
        );
        this.insertEventRow(childId, "retried", { previousState: "cancelled", state: "blocked", dependencyId: parentId }, now);
        out.push({ job: this.jobRow(childId)!, from: "cancelled", cause: parentId });
        const settled = this.settleChild(childId, now, null);
        if (settled) out.push(settled);
        else queue.push(childId);
      }
    }
    return out;
  }

  async requeueExpired(id: string, leaseToken: string | null, now: number): Promise<TransitionResult | undefined> {
    return this.tx(() => {
      const before = this.jobRow(id);
      const res = this.run(
        `UPDATE jobs SET state = 'waiting', run_at = ?, updated_at = ?,
           worker_id = NULL, lease_token = NULL, lease_expires_at = NULL
         WHERE id = ? AND state = 'active' AND lease_token IS ? AND lease_expires_at < ?`,
        now,
        now,
        id,
        leaseToken,
        now,
      );
      if (Number(res.changes) === 0) return undefined;
      this.insertEventRow(id, "lease_expired", { workerId: before!.workerId }, now);
      return this.transitioned(id, "active", []);
    });
  }

  /** Hand the job back untouched: it keeps its place, and the claim's attempt is returned. */
  async releaseLease(id: string, leaseToken: string, now: number): Promise<TransitionResult | undefined> {
    return this.tx(() => {
      const res = this.run(
        `UPDATE jobs SET state = 'waiting', updated_at = ?, worker_id = NULL, lease_token = NULL,
           lease_expires_at = NULL, attempts = MAX(attempts - 1, 0)
         WHERE id = ? AND state = 'active' AND lease_token = ?`,
        now,
        id,
        leaseToken,
      );
      if (Number(res.changes) === 0) return undefined;
      this.insertEventRow(id, "released", null, now);
      return this.transitioned(id, "active", []);
    });
  }

  async reconcileBlocked(now: number, limit: number): Promise<ReconcileResult> {
    return this.tx(() => {
      const rows = this.all(
        `SELECT j.id FROM jobs j WHERE j.state = 'blocked' AND (
           NOT EXISTS (SELECT 1 FROM job_dependencies d JOIN jobs p ON p.id = d.parent_id
                        WHERE d.child_id = j.id AND p.state IN (${UNFINISHED_SQL}))
           OR EXISTS (SELECT 1 FROM job_dependencies d JOIN jobs p ON p.id = d.parent_id
                       WHERE d.child_id = j.id AND d.on_failure = 'fail' AND p.state IN (${FAILED_SQL}))
           OR EXISTS (SELECT 1 FROM job_dependencies d
                       WHERE d.child_id = j.id AND NOT EXISTS (SELECT 1 FROM jobs p WHERE p.id = d.parent_id)))
         ORDER BY j.id LIMIT ?`,
        limit,
      );
      const out: JobTransition[] = [];
      for (const r of rows) {
        const t = this.settleChild(s(r.id), now, null);
        if (!t) continue; // already settled by an earlier job's cascade in this batch
        out.push(t);
        if (t.job.state === "cancelled") out.push(...this.settleDependents(t.job.id, now));
      }
      return { jobs: out, flowOutcomes: this.recordFlowOutcomes(out.map((t) => t.job.id), now) };
    });
  }

  /**
   * The result of a transition of `id`. With `now`, the transition finished jobs (this one or
   * dependents): record the verdicts of the flows they belong to, if that decides any.
   */
  private transitioned(id: string, from: JobState, dependents: JobTransition[], now?: number): TransitionResult {
    const flowOutcomes = now === undefined ? [] : this.recordFlowOutcomes([id, ...dependents.map((t) => t.job.id)], now);
    return { job: this.jobRow(id)!, from, dependents, flowOutcomes };
  }

  /** Decide and record the verdict of each flow these jobs belong to, where there is one to record. */
  private recordFlowOutcomes(jobIds: string[], now: number): FlowOutcome[] {
    const flows = new Set<string>();
    for (const id of jobIds) {
      const flowId = this.get("SELECT flow_id FROM jobs WHERE id = ?", id)?.flow_id;
      if (flowId !== null && flowId !== undefined) flows.add(s(flowId));
    }
    return [...flows].flatMap((flowId) => this.decideFlowOutcome(flowId, now) ?? []);
  }

  /**
   * flow.failed as soon as a job is dead (reason "dead") or cancelled (reason "cancelled"),
   * flow.completed once every job has completed; otherwise no verdict yet.
   */
  private decideFlowOutcome(flowId: string, now: number): FlowOutcome | undefined {
    if (this.get("SELECT 1 AS x FROM flow_events WHERE flow_id = ?", flowId)) return undefined;
    const r = this.get(
      `SELECT MIN(CASE WHEN state = 'dead' THEN id END) AS dead_id,
              MIN(CASE WHEN state = 'cancelled' THEN id END) AS cancelled_id,
              SUM(CASE WHEN state = 'completed' THEN 0 ELSE 1 END) AS not_completed
         FROM jobs WHERE flow_id = ?`,
      flowId,
    )!;
    if (r.dead_id !== null) return this.insertFlowOutcome(flowId, "flow.failed", "dead", s(r.dead_id), now);
    if (r.cancelled_id !== null) return this.insertFlowOutcome(flowId, "flow.failed", "cancelled", s(r.cancelled_id), now);
    if (Number(r.not_completed) === 0) return this.insertFlowOutcome(flowId, "flow.completed", null, null, now);
    return undefined;
  }

  /**
   * Record a verdict unless the flow has one. Inside a write transaction the check above already
   * rules that out; the primary key makes sure of it.
   */
  private insertFlowOutcome(flowId: string, event: FlowOutcome["event"], reason: FlowOutcome["reason"], jobId: string | null, now: number): FlowOutcome | undefined {
    const res = this.run(
      "INSERT INTO flow_events (flow_id, event, reason, job_id, at) VALUES (?, ?, ?, ?, ?) ON CONFLICT (flow_id) DO NOTHING",
      flowId,
      event,
      reason,
      jobId,
      now,
    );
    return Number(res.changes) === 0 ? undefined : { flowId, event, reason, jobId, at: now, dispatchedAt: null };
  }

  async getFlowOutcome(flowId: string): Promise<FlowOutcome | undefined> {
    const row = this.get("SELECT * FROM flow_events WHERE flow_id = ?", flowId);
    return row ? toFlowOutcome(row) : undefined;
  }

  async listPendingFlowOutcomes(limit: number): Promise<FlowOutcome[]> {
    return this.all("SELECT * FROM flow_events WHERE dispatched_at IS NULL ORDER BY at, flow_id LIMIT ?", limit).map(toFlowOutcome);
  }

  async claimFlowOutcome(flowId: string, now: number, deliveries: WebhookDelivery[]): Promise<boolean> {
    return this.tx(() => {
      const res = this.run("UPDATE flow_events SET dispatched_at = ? WHERE flow_id = ? AND dispatched_at IS NULL", now, flowId);
      if (Number(res.changes) === 0) return false;
      for (const d of deliveries) this.insertDeliveryRow(d);
      return true;
    });
  }

  /** A job's dependencies with their current states; `state` is null for one that was deleted. */
  private parentRows(childId: string): { id: string; onFailure: FailurePolicy; state: JobState | null; lastError: string | null }[] {
    return this.all(
      `SELECT d.parent_id, d.on_failure, p.state, p.last_error FROM job_dependencies d
         LEFT JOIN jobs p ON p.id = d.parent_id WHERE d.child_id = ? ORDER BY d.parent_id`,
      childId,
    ).map((r) => ({
      id: s(r.parent_id),
      onFailure: s(r.on_failure) as FailurePolicy,
      state: sn(r.state) as JobState | null,
      lastError: sn(r.last_error),
    }));
  }

  /**
   * The child check. For a blocked job, compare its dependencies' current states with its
   * policies: a `fail` dependency that is dead or cancelled cancels it at once; otherwise it
   * stays blocked while any dependency is unfinished, and is released when none is. Returns the
   * change, or undefined if there was none. Must run inside a transaction.
   */
  private settleChild(childId: string, now: number, cause: string | null): JobTransition | undefined {
    const row = this.get("SELECT state FROM jobs WHERE id = ?", childId);
    if (row?.state !== "blocked") return undefined;
    const parents = this.parentRows(childId);
    // A deleted dependency ends the job whatever its policy: it must never run without it.
    // (Retention doesn't delete a job an unfinished job depends on, so only a deletion behind
    // the store's back gets here.)
    const failed =
      parents.find((p) => p.state === null) ?? parents.find((p) => p.onFailure === "fail" && FAILED_STATES.includes(p.state!));
    if (failed) {
      const error =
        failed.state === null
          ? `dependency ${failed.id} no longer exists`
          : failed.state === "dead"
            ? `dependency ${failed.id} is dead${failed.lastError === null ? "" : `: ${failed.lastError}`}`
            : `dependency ${failed.id} was cancelled`;
      this.run(
        `UPDATE jobs SET state = 'cancelled', finished_at = ?, updated_at = ?, terminated_by = ?, last_error = ?
         WHERE id = ? AND state = 'blocked'`,
        now,
        now,
        failed.id,
        error,
        childId,
      );
      this.insertEventRow(childId, "cancelled", { previousState: "blocked", dependencyId: failed.id, dependencyState: failed.state ?? "deleted" }, now);
      return { job: this.jobRow(childId)!, from: "blocked", cause: failed.id };
    }
    if (parents.some((p) => UNFINISHED_STATES.includes(p.state!))) return undefined;
    // Nothing updates a blocked job, so updated_at is when it became blocked.
    const blockedSince = Number(this.get("SELECT updated_at FROM jobs WHERE id = ?", childId)!.updated_at);
    // run_at becomes "now" (unless it is later): the queue orders by it, and queue lag and wait
    // time are measured from it, so time spent blocked must not count as time spent waiting.
    this.run(
      `UPDATE jobs SET state = CASE WHEN run_at > ? THEN 'delayed' ELSE 'waiting' END,
         run_at = MAX(run_at, ?), updated_at = ?
       WHERE id = ? AND state = 'blocked'`,
      now,
      now,
      now,
      childId,
    );
    const job = this.jobRow(childId)!;
    this.insertEventRow(childId, "unblocked", { state: job.state }, now);
    return { job, from: "blocked", cause, blockedMs: Math.max(0, now - blockedSince) };
  }

  /**
   * Settle every dependent of `parentId` after it finished; dependents that get cancelled
   * settle theirs in turn. Must run inside the transaction that finished `parentId`.
   */
  private settleDependents(parentId: string, now: number): JobTransition[] {
    const out: JobTransition[] = [];
    const queue = [parentId];
    for (let i = 0; i < queue.length; i++) {
      const parent = queue[i]!;
      for (const r of this.all("SELECT child_id FROM job_dependencies WHERE parent_id = ? ORDER BY child_id", parent)) {
        const t = this.settleChild(s(r.child_id), now, parent);
        if (!t) continue;
        out.push(t);
        if (t.job.state === "cancelled") queue.push(t.job.id);
      }
    }
    return out;
  }

  async findExpiredLeases(now: number, limit: number): Promise<Job[]> {
    return this.all(
      `SELECT * FROM jobs WHERE state = 'active' AND lease_expires_at < ?
        ORDER BY lease_expires_at LIMIT ?`,
      now,
      limit,
    ).map(toJob);
  }

  async promoteDelayed(now: number, limit: number): Promise<number> {
    const res = this.run(
      `UPDATE jobs SET state = 'waiting', updated_at = ?
        WHERE id IN (SELECT id FROM jobs WHERE state = 'delayed' AND run_at <= ? LIMIT ?)`,
      now,
      now,
      limit,
    );
    return Number(res.changes);
  }

  /**
   * Retention: deletes completed and cancelled jobs. Dead jobs are kept for inspection, and so is
   * any job with an unfinished dependent, which may still read its result or state.
   */
  async deleteFinishedBefore(ts: number, limit: number): Promise<number> {
    const res = this.run(
      `DELETE FROM jobs WHERE id IN (
         SELECT j.id FROM jobs j
          WHERE j.state IN ('completed', 'cancelled') AND j.finished_at < ?
            AND NOT EXISTS (SELECT 1 FROM job_dependencies d JOIN jobs c ON c.id = d.child_id
                             WHERE d.parent_id = j.id AND c.state IN (${UNFINISHED_SQL}))
          LIMIT ?)`,
      ts,
      limit,
    );
    // A verdict goes once its flow has no jobs left: while any remain, it must stay, or a later
    // transition could record a second one.
    this.run(
      "DELETE FROM flow_events WHERE at < ? AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.flow_id = flow_events.flow_id)",
      ts,
    );
    return Number(res.changes);
  }

  async hasUnfinishedForSchedule(scheduleId: string): Promise<boolean> {
    const row = this.get(
      `SELECT 1 AS x FROM jobs WHERE schedule_id = ? AND state IN (${UNFINISHED_SQL}) LIMIT 1`,
      scheduleId,
    );
    return row !== undefined;
  }

  async appendJobEvent(jobId: string, type: string, data: Json | null, at: number): Promise<void> {
    this.insertEventRow(jobId, type, data, at);
  }

  private insertEventRow(jobId: string, type: string, data: Json | null, at: number): void {
    this.run(
      "INSERT INTO job_events (job_id, type, data, at) VALUES (?, ?, ?, ?)",
      jobId,
      type,
      data === null ? null : JSON.stringify(data),
      at,
    );
  }

  async listJobEvents(jobId: string): Promise<JobEvent[]> {
    return this.all("SELECT * FROM job_events WHERE job_id = ? ORDER BY id", jobId).map((r) => ({
      id: Number(r.id),
      jobId: String(r.job_id),
      type: String(r.type),
      data: safeParse<Json | null>(r.data as string | null, null),
      at: Number(r.at),
    }));
  }

  async getQueue(name: string): Promise<QueueSettings | undefined> {
    const row = this.get("SELECT * FROM queues WHERE name = ?", name);
    return row ? toQueue(row) : undefined;
  }

  async upsertQueue(q: QueueSettings): Promise<void> {
    this.run(
      `INSERT INTO queues (name, paused, concurrency, rate_limit_max, rate_limit_interval_ms,
         default_max_attempts, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(name) DO UPDATE SET paused = excluded.paused, concurrency = excluded.concurrency,
         rate_limit_max = excluded.rate_limit_max,
         rate_limit_interval_ms = excluded.rate_limit_interval_ms,
         default_max_attempts = excluded.default_max_attempts, updated_at = excluded.updated_at`,
      q.name,
      q.paused ? 1 : 0,
      q.concurrency,
      q.rateLimitMax,
      q.rateLimitIntervalMs,
      q.defaultMaxAttempts,
      q.createdAt,
      q.updatedAt,
    );
  }

  async listQueues(): Promise<QueueSettings[]> {
    // include queues that only exist implicitly via jobs
    const rows = this.all(
      `SELECT q.* FROM queues q
       UNION ALL
       SELECT DISTINCT j.queue AS name, 0, 0, 0, 0, 3, MIN(j.created_at), MIN(j.created_at)
         FROM jobs j WHERE j.queue NOT IN (SELECT name FROM queues) GROUP BY j.queue
       ORDER BY name`,
    );
    return rows.map(toQueue);
  }

  async insertSchedule(s: Schedule): Promise<void> {
    this.run(
      `INSERT INTO schedules (id, name, queue, job_name, payload, cron, timezone, enabled, overlap,
         next_run_at, last_run_at, last_job_id, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      s.id,
      s.name,
      s.queue,
      s.jobName,
      JSON.stringify(s.payload ?? null),
      s.cron,
      s.timezone,
      s.enabled ? 1 : 0,
      s.overlap,
      s.nextRunAt,
      s.lastRunAt,
      s.lastJobId,
      s.createdAt,
      s.updatedAt,
    );
  }

  async updateSchedule(s: Schedule): Promise<void> {
    this.run(
      `UPDATE schedules SET name = ?, queue = ?, job_name = ?, payload = ?, cron = ?, timezone = ?,
         enabled = ?, overlap = ?, next_run_at = ?, last_run_at = ?, last_job_id = ?, updated_at = ?
       WHERE id = ?`,
      s.name,
      s.queue,
      s.jobName,
      JSON.stringify(s.payload ?? null),
      s.cron,
      s.timezone,
      s.enabled ? 1 : 0,
      s.overlap,
      s.nextRunAt,
      s.lastRunAt,
      s.lastJobId,
      s.updatedAt,
      s.id,
    );
  }

  async getSchedule(id: string): Promise<Schedule | undefined> {
    const row = this.get("SELECT * FROM schedules WHERE id = ?", id);
    return row ? toSchedule(row) : undefined;
  }

  async getScheduleByName(name: string): Promise<Schedule | undefined> {
    const row = this.get("SELECT * FROM schedules WHERE name = ?", name);
    return row ? toSchedule(row) : undefined;
  }

  async listSchedules(): Promise<Schedule[]> {
    return this.all("SELECT * FROM schedules ORDER BY name").map(toSchedule);
  }

  async deleteSchedule(id: string): Promise<boolean> {
    return Number(this.run("DELETE FROM schedules WHERE id = ?", id).changes) > 0;
  }

  async findDueSchedules(now: number, limit: number): Promise<Schedule[]> {
    return this.all(
      "SELECT * FROM schedules WHERE enabled = 1 AND next_run_at <= ? ORDER BY next_run_at LIMIT ?",
      now,
      limit,
    ).map(toSchedule);
  }

  async insertWebhook(w: Webhook): Promise<void> {
    this.run(
      "INSERT INTO webhooks (id, url, secret, events, queues, enabled, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      w.id,
      w.url,
      w.secret,
      JSON.stringify(w.events),
      JSON.stringify(w.queues),
      w.enabled ? 1 : 0,
      w.createdAt,
    );
  }

  async listWebhooks(): Promise<Webhook[]> {
    return this.all("SELECT * FROM webhooks ORDER BY created_at").map(toWebhook);
  }

  async getWebhook(id: string): Promise<Webhook | undefined> {
    const row = this.get("SELECT * FROM webhooks WHERE id = ?", id);
    return row ? toWebhook(row) : undefined;
  }

  async deleteWebhook(id: string): Promise<boolean> {
    return Number(this.run("DELETE FROM webhooks WHERE id = ?", id).changes) > 0;
  }

  async insertDelivery(d: WebhookDelivery): Promise<void> {
    this.insertDeliveryRow(d);
  }

  private insertDeliveryRow(d: WebhookDelivery): void {
    this.run(
      `INSERT INTO webhook_deliveries (id, webhook_id, event, body, status, attempts, last_status_code,
         last_error, next_attempt_at, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      d.id,
      d.webhookId,
      d.event,
      d.body,
      d.status,
      d.attempts,
      d.lastStatusCode,
      d.lastError,
      d.nextAttemptAt,
      d.createdAt,
      d.updatedAt,
    );
  }

  async updateDelivery(d: WebhookDelivery): Promise<void> {
    this.run(
      `UPDATE webhook_deliveries SET status = ?, attempts = ?, last_status_code = ?, last_error = ?,
         next_attempt_at = ?, updated_at = ? WHERE id = ?`,
      d.status,
      d.attempts,
      d.lastStatusCode,
      d.lastError,
      d.nextAttemptAt,
      d.updatedAt,
      d.id,
    );
  }

  async listDeliveries(webhookId: string, limit: number): Promise<WebhookDelivery[]> {
    return this.all(
      "SELECT * FROM webhook_deliveries WHERE webhook_id = ? ORDER BY created_at DESC LIMIT ?",
      webhookId,
      limit,
    ).map(toDelivery);
  }

  async findPendingDeliveries(limit: number): Promise<WebhookDelivery[]> {
    return this.all(
      `SELECT * FROM webhook_deliveries WHERE status = 'pending'
        ORDER BY next_attempt_at, created_at, id LIMIT ?`,
      limit,
    ).map(toDelivery);
  }

  async insertToken(t: ApiToken): Promise<void> {
    this.run(
      "INSERT INTO api_tokens (id, name, hash, scopes, created_at, last_used_at, revoked_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      t.id,
      t.name,
      t.hash,
      JSON.stringify(t.scopes),
      t.createdAt,
      t.lastUsedAt,
      t.revokedAt,
    );
  }

  async findTokenByHash(hash: string): Promise<ApiToken | undefined> {
    const row = this.get("SELECT * FROM api_tokens WHERE hash = ?", hash);
    return row ? toToken(row) : undefined;
  }

  async listTokens(): Promise<ApiToken[]> {
    return this.all("SELECT * FROM api_tokens ORDER BY created_at").map(toToken);
  }

  async revokeToken(id: string, now: number): Promise<boolean> {
    const res = this.run(
      "UPDATE api_tokens SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
      now,
      id,
    );
    return Number(res.changes) > 0;
  }

  async touchToken(id: string, now: number): Promise<void> {
    this.run("UPDATE api_tokens SET last_used_at = ? WHERE id = ?", now, id);
  }
}

const n = (v: unknown): number => Number(v);
const nn = (v: unknown): number | null => (v === null || v === undefined ? null : Number(v));
const s = (v: unknown): string => String(v);
const sn = (v: unknown): string | null => (v === null || v === undefined ? null : String(v));

function toJob(r: Row): Job {
  return {
    id: s(r.id),
    queue: s(r.queue),
    name: s(r.name),
    payload: safeParse<Json>(r.payload as string, null),
    state: s(r.state) as JobState,
    priority: n(r.priority),
    attempts: n(r.attempts),
    maxAttempts: n(r.max_attempts),
    backoff: safeParse<BackoffStrategy | null>(r.backoff as string | null, null),
    timeoutMs: n(r.timeout_ms),
    runAt: n(r.run_at),
    createdAt: n(r.created_at),
    updatedAt: n(r.updated_at),
    startedAt: nn(r.started_at),
    finishedAt: nn(r.finished_at),
    workerId: sn(r.worker_id),
    leaseToken: sn(r.lease_token),
    leaseExpiresAt: nn(r.lease_expires_at),
    progress: n(r.progress),
    result: safeParse<Json | null>(r.result as string | null, null),
    lastError: sn(r.last_error),
    uniqueKey: sn(r.unique_key),
    scheduleId: sn(r.schedule_id),
    flowId: sn(r.flow_id),
    flowKey: sn(r.flow_key),
    terminatedBy: sn(r.terminated_by),
  };
}

function toFlowOutcome(r: Row): FlowOutcome {
  return {
    flowId: s(r.flow_id),
    event: s(r.event) as FlowOutcome["event"],
    reason: sn(r.reason) as FlowOutcome["reason"],
    jobId: sn(r.job_id),
    at: n(r.at),
    dispatchedAt: nn(r.dispatched_at),
  };
}

function toQueue(r: Row): QueueSettings {
  return {
    name: s(r.name),
    paused: n(r.paused) === 1,
    concurrency: n(r.concurrency),
    rateLimitMax: n(r.rate_limit_max),
    rateLimitIntervalMs: n(r.rate_limit_interval_ms),
    defaultMaxAttempts: n(r.default_max_attempts),
    createdAt: n(r.created_at),
    updatedAt: n(r.updated_at),
  };
}

function toSchedule(r: Row): Schedule {
  return {
    id: s(r.id),
    name: s(r.name),
    queue: s(r.queue),
    jobName: s(r.job_name),
    payload: safeParse<Json>(r.payload as string, null),
    cron: s(r.cron),
    timezone: s(r.timezone),
    enabled: n(r.enabled) === 1,
    overlap: s(r.overlap) as Schedule["overlap"],
    nextRunAt: n(r.next_run_at),
    lastRunAt: nn(r.last_run_at),
    lastJobId: sn(r.last_job_id),
    createdAt: n(r.created_at),
    updatedAt: n(r.updated_at),
  };
}

function toWebhook(r: Row): Webhook {
  return {
    id: s(r.id),
    url: s(r.url),
    secret: s(r.secret),
    events: safeParse<string[]>(r.events as string, []),
    queues: safeParse<string[]>(r.queues as string, []),
    enabled: n(r.enabled) === 1,
    createdAt: n(r.created_at),
  };
}

function toDelivery(r: Row): WebhookDelivery {
  return {
    id: s(r.id),
    webhookId: s(r.webhook_id),
    event: s(r.event),
    body: s(r.body),
    status: s(r.status) as WebhookDelivery["status"],
    attempts: n(r.attempts),
    lastStatusCode: nn(r.last_status_code),
    lastError: sn(r.last_error),
    nextAttemptAt: nn(r.next_attempt_at),
    createdAt: n(r.created_at),
    updatedAt: n(r.updated_at),
  };
}

function toToken(r: Row): ApiToken {
  return {
    id: s(r.id),
    name: s(r.name),
    hash: s(r.hash),
    scopes: safeParse<Scope[]>(r.scopes as string, []),
    createdAt: n(r.created_at),
    lastUsedAt: nn(r.last_used_at),
    revokedAt: nn(r.revoked_at),
  };
}
