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
import { emptyCounts, type ActivateParams, type ClaimCandidateQuery, type Store } from "./store.js";

type Row = Record<string, SQLInputValue>;

const MAX_PAGE = 500;

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

  async insertJob(job: Job): Promise<void> {
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

  async findActiveByUniqueKey(queue: string, uniqueKey: string): Promise<Job | undefined> {
    const row = this.get(
      `SELECT * FROM jobs WHERE queue = ? AND unique_key = ?
         AND state IN ('delayed', 'waiting', 'active') ORDER BY id LIMIT 1`,
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

  async completeJob(id: string, result: Json | null, now: number): Promise<void> {
    this.run(
      `UPDATE jobs SET state = 'completed', result = ?, finished_at = ?, updated_at = ?,
         progress = 1, lease_token = NULL, lease_expires_at = NULL, last_error = NULL
       WHERE id = ?`,
      result === null || result === undefined ? null : JSON.stringify(result),
      now,
      now,
      id,
    );
  }

  async scheduleRetry(id: string, error: string, runAt: number, now: number): Promise<void> {
    this.run(
      `UPDATE jobs SET state = 'delayed', run_at = ?, last_error = ?, updated_at = ?,
         worker_id = NULL, lease_token = NULL, lease_expires_at = NULL
       WHERE id = ?`,
      runAt,
      error,
      now,
      id,
    );
  }

  async markDead(id: string, error: string, now: number): Promise<void> {
    this.run(
      `UPDATE jobs SET state = 'dead', last_error = ?, finished_at = ?, updated_at = ?,
         lease_token = NULL, lease_expires_at = NULL
       WHERE id = ?`,
      error,
      now,
      now,
      id,
    );
  }

  async cancelJob(id: string, now: number): Promise<void> {
    this.run(
      `UPDATE jobs SET state = 'cancelled', finished_at = ?, updated_at = ?,
         lease_token = NULL, lease_expires_at = NULL
       WHERE id = ?`,
      now,
      now,
      id,
    );
  }

  async requeueJob(id: string, now: number, resetAttempts: boolean): Promise<void> {
    this.run(
      `UPDATE jobs SET state = 'waiting', run_at = ?, updated_at = ?, finished_at = NULL,
         worker_id = NULL, lease_token = NULL, lease_expires_at = NULL,
         attempts = CASE WHEN ? THEN 0 ELSE attempts END
       WHERE id = ?`,
      now,
      now,
      resetAttempts ? 1 : 0,
      id,
    );
  }

  async findExpiredLeases(now: number, limit: number): Promise<Job[]> {
    return this.all(
      `SELECT * FROM jobs WHERE state = 'active' AND lease_expires_at < ?
        ORDER BY lease_expires_at LIMIT ?`,
      now,
      limit,
    ).map(toJob);
  }

  async releaseLease(id: string, now: number): Promise<void> {
    this.run(
      `UPDATE jobs SET state = 'waiting', updated_at = ?, worker_id = NULL, lease_token = NULL,
         lease_expires_at = NULL
       WHERE id = ?`,
      now,
      id,
    );
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

  async deleteFinishedBefore(ts: number, limit: number): Promise<number> {
    const res = this.run(
      `DELETE FROM jobs WHERE id IN (
         SELECT id FROM jobs WHERE state IN ('completed', 'dead', 'cancelled') AND finished_at < ?
         LIMIT ?)`,
      ts,
      limit,
    );
    return Number(res.changes);
  }

  async hasUnfinishedForSchedule(scheduleId: string): Promise<boolean> {
    const row = this.get(
      `SELECT 1 AS x FROM jobs WHERE schedule_id = ? AND state IN ('delayed', 'waiting', 'active') LIMIT 1`,
      scheduleId,
    );
    return row !== undefined;
  }

  async appendJobEvent(jobId: string, type: string, data: Json | null, at: number): Promise<void> {
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
