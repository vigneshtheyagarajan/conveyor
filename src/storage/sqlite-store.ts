import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite";
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
  Scope,
  Webhook,
  WebhookDelivery,
} from "../core/types.js";
import type { BackoffStrategy } from "../util/backoff.js";
import { safeParse, type Json } from "../util/json.js";
import { MIGRATIONS } from "./migrations.js";
import {
  emptyCounts,
  type ClaimParams,
  type ExpireLeaseParams,
  type FireOutcome,
  type FireScheduleParams,
  type NewJobEvent,
  type SchedulePatch,
  type StateChange,
  type Store,
} from "./store.js";

type Row = Record<string, SQLInputValue>;

const MAX_PAGE = 500;
const SQLITE_BUSY = 5;

/** States in which a job holds its unique key (the partial unique index of migration 8). */
const UNFINISHED = "('delayed', 'waiting', 'active')";
const isUnfinished = (state: JobState) => state === "delayed" || state === "waiting" || state === "active";

export interface SqliteStoreOptions {
  /** Busy timeout for concurrent writers (other processes). */
  busyTimeoutMs?: number;
}

export class SqliteStore implements Store {
  readonly db: DatabaseSync;
  private statements = new Map<string, StatementSync>();

  constructor(path = ":memory:", options: SqliteStoreOptions = {}) {
    const busyTimeoutMs = options.busyTimeoutMs ?? 5000;
    this.db = new DatabaseSync(path);
    try {
      this.db.exec(`PRAGMA busy_timeout = ${busyTimeoutMs}`);
      if (path !== ":memory:") this.retryWhileBusy(() => this.db.exec("PRAGMA journal_mode = WAL"), busyTimeoutMs);
      this.db.exec("PRAGMA foreign_keys = ON");
      this.db.exec("PRAGMA synchronous = NORMAL");
    } catch (err) {
      this.db.close();
      throw err;
    }
  }

  /**
   * Switching a new file to WAL needs an exclusive lock, and SQLite reports SQLITE_BUSY at once
   * (without waiting out busy_timeout) when another connection holds a lock, as happens when
   * several processes open a new database at the same moment. Retry for up to the busy timeout,
   * as any other statement would wait. (Synchronous, like busy_timeout itself: the constructor
   * can't await.)
   */
  private retryWhileBusy(fn: () => void, timeoutMs: number): void {
    const deadline = Date.now() + timeoutMs;
    for (let waitMs = 2; ; waitMs = Math.min(waitMs * 2, 50)) {
      try {
        return fn();
      } catch (err) {
        const busy = (err as { errcode?: number }).errcode === SQLITE_BUSY;
        if (!busy || Date.now() >= deadline) throw err;
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, waitMs);
      }
    }
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

  /**
   * Runs `fn` in a transaction that takes the write lock up front (BEGIN IMMEDIATE), so other
   * connections and processes wait (up to busy_timeout) until it commits.
   *
   * `fn` must be synchronous: the connection is shared by all async code in this process, and
   * anything that ran between BEGIN and COMMIT would become part of the transaction.
   *
   * IMMEDIATE rather than a deferred BEGIN: in WAL mode a deferred transaction that reads and
   * then writes fails with SQLITE_BUSY_SNAPSHOT (which busy_timeout does not retry) if another
   * connection committed in between. Taking the lock first means every read inside `fn` sees
   * the latest data and nothing can change it before COMMIT.
   */
  private immediate<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = fn();
      this.db.exec("COMMIT");
      return result;
    } catch (err) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        // SQLite already rolled back; report the original error.
      }
      throw err;
    }
  }

  /**
   * Bring the schema up to date. Safe when several processes (say `serve` and workers starting
   * together on a new file) migrate at once: each migration runs in a BEGIN IMMEDIATE
   * transaction that re-reads the version under the write lock, so a process that waited for
   * the lock sees the migration another one just applied and skips it.
   */
  async migrate(): Promise<number> {
    const latest = MIGRATIONS.length;
    // Up to date (the usual case): no lock needed. A newer schema is refused before any write.
    if (this.assertKnownVersion() === latest) return latest;
    for (;;) {
      const applied = this.immediate(() => {
        const current = this.assertKnownVersion();
        if (current === latest) return false;
        this.db.exec(MIGRATIONS[current]!);
        this.db.exec(`PRAGMA user_version = ${current + 1}`);
        return true;
      });
      if (!applied) return latest;
    }
  }

  /** The schema version, refusing one written by a newer release. */
  private assertKnownVersion(): number {
    const current = Number((this.db.prepare("PRAGMA user_version").get() as Row).user_version);
    if (current > MIGRATIONS.length) {
      // Written by a newer release; running older code against it could corrupt data.
      throw new Error(
        `database schema version ${current} is newer than this version of Conveyor supports (${MIGRATIONS.length}); ` +
          "upgrade Conveyor to open it",
      );
    }
    return current;
  }

  async close(): Promise<void> {
    this.statements.clear();
    this.db.close();
  }

  async insertJob(job: Job): Promise<void> {
    this.insertJobRow(job);
  }

  async insertJobs(jobs: Job[], events: NewJobEvent[]): Promise<Job[]> {
    // The key check and the insert are in one transaction under the write lock, so no other
    // connection or process can insert the same key in between. (The unique index would refuse
    // it anyway; this turns that into "here is the existing job" instead of an error.)
    return this.immediate(() => {
      const notInserted = new Set<string>();
      const stored = jobs.map((job) => {
        const holder = job.uniqueKey !== null && isUnfinished(job.state) ? this.holderOfKey(job.queue, job.uniqueKey) : undefined;
        if (holder) {
          notInserted.add(job.id);
          return holder;
        }
        this.insertJobRow(job);
        return job;
      });
      for (const e of events) if (!notInserted.has(e.jobId)) this.insertEventRow(e.jobId, e.type, e.data, e.at);
      return stored;
    });
  }

  /** The unfinished job holding a queue's unique key, if any. */
  private holderOfKey(queue: string, uniqueKey: string): Job | undefined {
    const row = this.get(`SELECT * FROM jobs WHERE queue = ? AND unique_key = ? AND state IN ${UNFINISHED} LIMIT 1`, queue, uniqueKey);
    return row ? toJob(row) : undefined;
  }

  private insertJobRow(job: Job): void {
    this.run(
      `INSERT INTO jobs (id, queue, name, payload, state, priority, attempts, max_attempts, backoff,
        timeout_ms, run_at, created_at, updated_at, started_at, finished_at, worker_id, lease_token,
        lease_expires_at, progress, result, last_error, unique_key, schedule_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
    );
  }

  async getJob(id: string): Promise<Job | undefined> {
    const row = this.get("SELECT * FROM jobs WHERE id = ?", id);
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
    return this.activeIn(queue);
  }

  private activeIn(queue: string): number {
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

  async claimNext(p: ClaimParams): Promise<Job | undefined> {
    // A cheap look without the write lock first, so that idle workers polling empty (or full,
    // or paused) queues don't queue up for it. Only a hint: the decision is made again below,
    // inside the transaction, where nothing can change under it.
    if (this.pickClaimable(p) === undefined) return undefined;
    return this.immediate(() => {
      const id = this.pickClaimable(p);
      if (id === undefined) return undefined;
      const row = this.get(
        `UPDATE jobs SET state = 'active', attempts = attempts + 1, worker_id = ?, lease_token = ?,
           lease_expires_at = ?, started_at = ?, updated_at = ?, progress = 0
         WHERE id = ? AND state = 'waiting'
         RETURNING *`,
        p.workerId,
        p.leaseToken,
        p.leaseExpiresAt,
        p.now,
        p.now,
        id,
      );
      // Can't happen while the transaction holds the write lock; the state check is a second
      // line of defence.
      if (!row) return undefined;
      const job = toJob(row);
      this.insertEventRow(job.id, "claimed", { workerId: p.workerId, attempt: job.attempts }, p.now);
      return job;
    });
  }

  /** The id of the job claimNext would lease right now, if any. */
  private pickClaimable(p: ClaimParams): string | undefined {
    const queues = [...new Set(p.queues)].filter((q) => !p.exclude?.includes(q) && this.hasRoom(q));
    if (queues.length === 0) return undefined;
    const placeholders = queues.map(() => "?").join(", ");
    const row = this.get(
      `SELECT id FROM jobs
        WHERE queue IN (${placeholders}) AND state = 'waiting' AND run_at <= ?
        ORDER BY priority DESC, run_at ASC, id ASC
        LIMIT 1`,
      ...queues,
      p.now,
    );
    return row ? String(row.id) : undefined;
  }

  /** Whether a queue may start another job: not paused, and below its concurrency limit (0 = none). */
  private hasRoom(queue: string): boolean {
    const q = this.get("SELECT paused, concurrency FROM queues WHERE name = ?", queue);
    if (!q) return true; // never configured: defaults, no limits
    if (Number(q.paused) === 1) return false;
    const limit = Number(q.concurrency);
    return limit <= 0 || this.activeIn(queue) < limit;
  }

  // Lease calls: each changes the job only while it is active and still holds the caller's
  // lease token, and reports whether it did.

  async extendLease(id: string, leaseToken: string, leaseExpiresAt: number, now: number): Promise<boolean> {
    return this.changed(
      `UPDATE jobs SET lease_expires_at = ?, updated_at = ?
        WHERE id = ? AND state = 'active' AND lease_token = ?`,
      leaseExpiresAt,
      now,
      id,
      leaseToken,
    );
  }

  async updateProgress(id: string, leaseToken: string, progress: number, now: number): Promise<boolean> {
    return this.changed(
      "UPDATE jobs SET progress = ?, updated_at = ? WHERE id = ? AND state = 'active' AND lease_token = ?",
      progress,
      now,
      id,
      leaseToken,
    );
  }

  async completeJob(id: string, leaseToken: string, result: Json | null, now: number): Promise<boolean> {
    return this.changed(
      `UPDATE jobs SET state = 'completed', result = ?, finished_at = ?, updated_at = ?,
         progress = 1, lease_token = NULL, lease_expires_at = NULL, last_error = NULL
       WHERE id = ? AND state = 'active' AND lease_token = ?`,
      result === null || result === undefined ? null : JSON.stringify(result),
      now,
      now,
      id,
      leaseToken,
    );
  }

  async scheduleRetry(id: string, leaseToken: string, error: string, runAt: number, now: number): Promise<boolean> {
    return this.changed(
      `UPDATE jobs SET state = 'delayed', run_at = ?, last_error = ?, updated_at = ?,
         worker_id = NULL, lease_token = NULL, lease_expires_at = NULL
       WHERE id = ? AND state = 'active' AND lease_token = ?`,
      runAt,
      error,
      now,
      id,
      leaseToken,
    );
  }

  async markDead(id: string, leaseToken: string, error: string, now: number): Promise<boolean> {
    return this.changed(
      `UPDATE jobs SET state = 'dead', last_error = ?, finished_at = ?, updated_at = ?,
         lease_token = NULL, lease_expires_at = NULL
       WHERE id = ? AND state = 'active' AND lease_token = ?`,
      error,
      now,
      now,
      id,
      leaseToken,
    );
  }

  /** Hand the job back untouched: it keeps its place, and the claim's attempt is returned. */
  async releaseLease(id: string, leaseToken: string, now: number): Promise<boolean> {
    return this.changed(
      `UPDATE jobs SET state = 'waiting', updated_at = ?, worker_id = NULL, lease_token = NULL,
         lease_expires_at = NULL, attempts = MAX(attempts - 1, 0)
       WHERE id = ? AND state = 'active' AND lease_token = ?`,
      now,
      id,
      leaseToken,
    );
  }

  private changed(sql: string, ...params: SQLInputValue[]): boolean {
    return Number(this.run(sql, ...params).changes) > 0;
  }

  async expireLease(p: ExpireLeaseParams): Promise<boolean> {
    // Exactly the lease that was read: a heartbeat since then moved lease_expires_at, a new claim
    // changed lease_token, and finishing, failing or cancelling changed the state.
    const sameLease = "WHERE id = ? AND state = 'active' AND lease_token IS ? AND lease_expires_at = ?";
    return this.immediate(() => {
      const changed =
        p.retryAt === null
          ? this.changed(
              `UPDATE jobs SET state = 'dead', last_error = ?, finished_at = ?, updated_at = ?,
                 lease_token = NULL, lease_expires_at = NULL
               ${sameLease}`,
              p.error,
              p.now,
              p.now,
              p.id,
              p.leaseToken,
              p.leaseExpiresAt,
            )
          : this.changed(
              `UPDATE jobs SET state = 'delayed', run_at = ?, last_error = ?, updated_at = ?,
                 worker_id = NULL, lease_token = NULL, lease_expires_at = NULL
               ${sameLease}`,
              p.retryAt,
              p.error,
              p.now,
              p.id,
              p.leaseToken,
              p.leaseExpiresAt,
            );
      if (changed) this.insertEventRow(p.id, "lease_expired", p.event, p.now);
      return changed;
    });
  }

  async cancelJob(id: string, now: number): Promise<StateChange | undefined> {
    return this.guardedChange(
      id,
      `UPDATE jobs SET state = 'cancelled', finished_at = ?, updated_at = ?,
         lease_token = NULL, lease_expires_at = NULL
       WHERE id = ? AND state IN ('delayed', 'waiting', 'active')`,
      [now, now, id],
      "cancelled",
      now,
    );
  }

  async retryJob(id: string, now: number): Promise<StateChange | undefined> {
    return this.guardedChange(
      id,
      `UPDATE jobs SET state = 'waiting', run_at = ?, updated_at = ?, finished_at = NULL,
         worker_id = NULL, lease_token = NULL, lease_expires_at = NULL, attempts = 0
       WHERE id = ? AND state IN ('dead', 'cancelled')`,
      [now, now, id],
      "retried",
      now,
      // Coming back would make it unfinished again: not while another unfinished job has its key.
      (row) => {
        if (row.unique_key === null || !["dead", "cancelled"].includes(String(row.state))) return undefined;
        const holder = this.holderOfKey(String(row.queue), String(row.unique_key));
        return holder ? { jobId: holder.id, uniqueKey: String(row.unique_key) } : undefined;
      },
    );
  }

  /**
   * Runs an UPDATE whose WHERE clause carries the allowed source states, so it can never apply
   * to a job in any other state. In the same transaction, reads the job beforehand (to report
   * its state, and for `blockedBy`, which can refuse the change) and records `eventType` when
   * the update applied.
   */
  private guardedChange(
    id: string,
    update: string,
    params: SQLInputValue[],
    eventType: string,
    now: number,
    blockedBy?: (row: Row) => StateChange["heldBy"],
  ): StateChange | undefined {
    return this.immediate(() => {
      const row = this.get("SELECT state, queue, unique_key FROM jobs WHERE id = ?", id);
      if (!row) return undefined;
      const from = String(row.state) as JobState;
      const heldBy = blockedBy?.(row);
      if (heldBy) return { changed: false, from, heldBy };
      const changed = this.changed(update, ...params);
      if (changed) this.insertEventRow(id, eventType, { previousState: from }, now);
      return { changed, from };
    });
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

  /** Retention: deletes completed and cancelled jobs. Dead jobs are kept for inspection. */
  async deleteFinishedBefore(ts: number, limit: number): Promise<number> {
    const res = this.run(
      `DELETE FROM jobs WHERE id IN (
         SELECT id FROM jobs WHERE state IN ('completed', 'cancelled') AND finished_at < ?
         LIMIT ?)`,
      ts,
      limit,
    );
    return Number(res.changes);
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

  async updateSchedule(id: string, patch: SchedulePatch): Promise<boolean> {
    const sets: string[] = [];
    const params: SQLInputValue[] = [];
    for (const [key, value] of Object.entries(patch) as [keyof SchedulePatch, unknown][]) {
      if (value === undefined) continue;
      const column = SCHEDULE_COLUMNS[key];
      if (!column) throw new Error(`schedule has no column for ${String(key)}`);
      sets.push(`${column} = ?`);
      params.push(scheduleValue(key, value));
    }
    if (sets.length === 0) return (await this.getSchedule(id)) !== undefined;
    return this.changed(`UPDATE schedules SET ${sets.join(", ")} WHERE id = ?`, ...params, id);
  }

  async fireSchedule(p: FireScheduleParams): Promise<FireOutcome> {
    return this.immediate(() => {
      // Still this slot? Another scheduler may have fired it, or the schedule was disabled,
      // retimed or deleted since it was read. Either way, not ours to fire.
      const slot = "WHERE id = ? AND enabled = 1 AND next_run_at = ? AND cron = ? AND timezone = ?";
      const row = this.get(`SELECT overlap FROM schedules ${slot}`, p.scheduleId, p.slot, p.cron, p.timezone);
      if (!row) return "stale";
      const skip =
        String(row.overlap) === "skip" &&
        this.get(`SELECT 1 AS x FROM jobs WHERE schedule_id = ? AND state IN ${UNFINISHED} LIMIT 1`, p.scheduleId) !== undefined;
      if (!skip) {
        this.insertJobRow(p.job);
        this.insertEventRow(p.job.id, "enqueued", { state: p.job.state }, p.now);
      }
      // Only the timing columns: an edit made meanwhile to anything else is kept.
      this.run(
        `UPDATE schedules SET last_run_at = ?, next_run_at = ?, updated_at = ?,
           last_job_id = CASE WHEN ? THEN last_job_id ELSE ? END
         ${slot}`,
        p.slot,
        p.nextRunAt,
        p.now,
        skip ? 1 : 0,
        p.job.id,
        p.scheduleId,
        p.slot,
        p.cron,
        p.timezone,
      );
      return skip ? "skipped" : "fired";
    });
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

const SCHEDULE_COLUMNS: Record<keyof SchedulePatch, string> = {
  name: "name",
  queue: "queue",
  jobName: "job_name",
  payload: "payload",
  cron: "cron",
  timezone: "timezone",
  enabled: "enabled",
  overlap: "overlap",
  nextRunAt: "next_run_at",
  lastRunAt: "last_run_at",
  lastJobId: "last_job_id",
  updatedAt: "updated_at",
};

function scheduleValue(key: keyof SchedulePatch, value: unknown): SQLInputValue {
  if (key === "payload") return JSON.stringify(value ?? null);
  if (key === "enabled") return value ? 1 : 0;
  return value as SQLInputValue;
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
