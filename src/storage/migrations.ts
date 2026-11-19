// Applied in order, tracked with PRAGMA user_version. Append only.
export const MIGRATIONS: string[] = [
  // 1 - jobs
  `
  CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    name TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT 'null',
    state TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    backoff TEXT,
    timeout_ms INTEGER NOT NULL DEFAULT 0,
    run_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    worker_id TEXT,
    lease_token TEXT,
    lease_expires_at INTEGER,
    progress REAL NOT NULL DEFAULT 0,
    result TEXT,
    last_error TEXT
  );
  CREATE INDEX idx_jobs_claim ON jobs (queue, state, priority DESC, run_at, id);
  CREATE INDEX idx_jobs_lease ON jobs (state, lease_expires_at);
  `,
  // 2 - queues
  `
  CREATE TABLE queues (
    name TEXT PRIMARY KEY,
    paused INTEGER NOT NULL DEFAULT 0,
    concurrency INTEGER NOT NULL DEFAULT 0,
    rate_limit_max INTEGER NOT NULL DEFAULT 0,
    rate_limit_interval_ms INTEGER NOT NULL DEFAULT 0,
    default_max_attempts INTEGER NOT NULL DEFAULT 3,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
  `,
  // 3 - unique keys and job history
  `
  ALTER TABLE jobs ADD COLUMN unique_key TEXT;
  CREATE INDEX idx_jobs_unique ON jobs (queue, unique_key);
  CREATE TABLE job_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    data TEXT,
    at INTEGER NOT NULL
  );
  CREATE INDEX idx_job_events_job ON job_events (job_id, id);
  `,
  // 4 - schedules
  `
  CREATE TABLE schedules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    queue TEXT NOT NULL,
    job_name TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT 'null',
    cron TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    enabled INTEGER NOT NULL DEFAULT 1,
    overlap TEXT NOT NULL DEFAULT 'allow',
    next_run_at INTEGER NOT NULL,
    last_run_at INTEGER,
    last_job_id TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
  CREATE INDEX idx_schedules_due ON schedules (enabled, next_run_at);
  ALTER TABLE jobs ADD COLUMN schedule_id TEXT;
  CREATE INDEX idx_jobs_schedule ON jobs (schedule_id, state);
  `,
  // 5 - webhooks
  `
  CREATE TABLE webhooks (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    secret TEXT NOT NULL,
    events TEXT NOT NULL,
    queues TEXT NOT NULL DEFAULT '[]',
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL
  );
  CREATE TABLE webhook_deliveries (
    id TEXT PRIMARY KEY,
    webhook_id TEXT NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    event TEXT NOT NULL,
    body TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_status_code INTEGER,
    last_error TEXT,
    next_attempt_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
  CREATE INDEX idx_deliveries_webhook ON webhook_deliveries (webhook_id, created_at);
  `,
  // 6 - api tokens
  `
  CREATE TABLE api_tokens (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    hash TEXT NOT NULL UNIQUE,
    scopes TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    last_used_at INTEGER,
    revoked_at INTEGER
  );
  `,
  // 7 - find pending webhook deliveries to resume after a restart
  `
  CREATE INDEX idx_deliveries_pending ON webhook_deliveries (status, next_attempt_at);
  `,
  // 8 - at most one unfinished job per (queue, unique_key), enforced by the database.
  //
  // Older versions could create duplicates (two enqueues racing), and the unique index can't be
  // created over them. So first, for each (queue, unique_key) with several unfinished jobs, one
  // keeps the key: an active job if there is one (it is already running), otherwise the oldest
  // (the first enqueued). The others:
  //   - waiting or delayed: cancelled. They have not run, and not running a second copy is what
  //     the unique key is for.
  //   - active: keep running, but lose the key (unique_key = NULL). Cancelling a running job
  //     would not undo the run, only lose its result.
  // Every change is recorded in job_events with the id of the job that kept the key.
  `
  CREATE TEMP TABLE unique_key_duplicates AS
    SELECT id, state, unique_key, FIRST_VALUE(id) OVER w AS kept_id, ROW_NUMBER() OVER w AS n
      FROM jobs
     WHERE unique_key IS NOT NULL AND state IN ('delayed', 'waiting', 'active')
    WINDOW w AS (PARTITION BY queue, unique_key ORDER BY state = 'active' DESC, id);
  DELETE FROM unique_key_duplicates WHERE n = 1;

  INSERT INTO job_events (job_id, type, data, at)
    SELECT id,
           CASE WHEN state = 'active' THEN 'unique_key_released' ELSE 'cancelled' END,
           CASE WHEN state = 'active'
                THEN json_object('uniqueKey', unique_key, 'keptJobId', kept_id)
                ELSE json_object('previousState', state, 'reason', 'duplicate unique key', 'keptJobId', kept_id)
           END,
           CAST(ROUND((julianday('now') - 2440587.5) * 86400000) AS INTEGER)
      FROM unique_key_duplicates ORDER BY id;
  UPDATE jobs
     SET state = 'cancelled', lease_token = NULL, lease_expires_at = NULL,
         finished_at = CAST(ROUND((julianday('now') - 2440587.5) * 86400000) AS INTEGER),
         updated_at = CAST(ROUND((julianday('now') - 2440587.5) * 86400000) AS INTEGER)
   WHERE id IN (SELECT id FROM unique_key_duplicates WHERE state != 'active');
  UPDATE jobs
     SET unique_key = NULL, updated_at = CAST(ROUND((julianday('now') - 2440587.5) * 86400000) AS INTEGER)
   WHERE id IN (SELECT id FROM unique_key_duplicates WHERE state = 'active');
  DROP TABLE unique_key_duplicates;

  CREATE UNIQUE INDEX idx_jobs_unique_unfinished ON jobs (queue, unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('delayed', 'waiting', 'active');
  `,
];
