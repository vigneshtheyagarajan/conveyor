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
  // 8 - job dependencies and flows
  // Edges never change once written: whether a child can run is worked out from its parents'
  // states each time, so there is no counter to drift. terminated_by marks jobs cancelled
  // because a dependency failed, so retrying that dependency can bring them back.
  // parent_id has no foreign key: an edge outlives its parent (which retention may delete once
  // the child has finished) so that the child can never be retried without it. Edges go with
  // their child. Parents are checked to exist when the edge is inserted.
  `
  ALTER TABLE jobs ADD COLUMN flow_id TEXT;
  ALTER TABLE jobs ADD COLUMN flow_key TEXT;
  ALTER TABLE jobs ADD COLUMN terminated_by TEXT;
  CREATE UNIQUE INDEX idx_jobs_flow ON jobs (flow_id, flow_key) WHERE flow_id IS NOT NULL;
  CREATE INDEX idx_jobs_terminated_by ON jobs (terminated_by) WHERE terminated_by IS NOT NULL;
  CREATE TABLE job_dependencies (
    child_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    parent_id TEXT NOT NULL,
    on_failure TEXT NOT NULL DEFAULT 'fail' CHECK (on_failure IN ('fail', 'ignore', 'continue')),
    PRIMARY KEY (child_id, parent_id),
    CHECK (child_id <> parent_id)
  ) WITHOUT ROWID;
  CREATE INDEX idx_job_deps_parent ON job_dependencies (parent_id, child_id);
  -- A flow's verdict, recorded once by the transition that decides it (the primary key makes a
  -- second one impossible), and the outbox webhooks are delivered from: dispatched_at is set
  -- by the one dispatcher that claims it.
  CREATE TABLE flow_events (
    flow_id TEXT PRIMARY KEY,
    event TEXT NOT NULL CHECK (event IN ('flow.completed', 'flow.failed')),
    reason TEXT CHECK (reason IN ('dead', 'cancelled')),
    job_id TEXT,
    at INTEGER NOT NULL,
    dispatched_at INTEGER
  );
  CREATE INDEX idx_flow_events_pending ON flow_events (at) WHERE dispatched_at IS NULL;
  `,
];
