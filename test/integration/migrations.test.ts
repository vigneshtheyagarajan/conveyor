import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Job } from "../../src/core/types.js";
import { MIGRATIONS } from "../../src/storage/migrations.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import { tempDir } from "../helpers.js";

const T = Date.UTC(2026, 0, 5, 12, 0, 0);
const LATEST = MIGRATIONS.length;

/** Create a database file at schema version `version`, the way an older release left it. */
function createAtVersion(file: string, version: number): DatabaseSync {
  const db = new DatabaseSync(file);
  db.exec("PRAGMA journal_mode = WAL");
  for (let i = 0; i < version; i++) db.exec(MIGRATIONS[i]!);
  db.exec(`PRAGMA user_version = ${version}`);
  return db;
}

const userVersion = (db: DatabaseSync) => Number((db.prepare("PRAGMA user_version").get() as { user_version: number }).user_version);

/** Schema objects (tables, indexes) and their SQL, for comparing databases. */
function schemaOf(db: DatabaseSync) {
  return db.prepare("SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name").all();
}

// Rows written with only the columns each schema version had.
const OLD_JOB = {
  id: "job-old",
  queue: "emails",
  name: "send",
  payload: '{"to":"a@example.test","n":[1,2]}',
  state: "delayed",
  priority: 7,
  attempts: 2,
  max_attempts: 5,
  backoff: '{"type":"fixed","delayMs":250}',
  timeout_ms: 3000,
  run_at: T + 1000,
  created_at: T - 5000,
  updated_at: T - 10,
  started_at: T - 100,
  finished_at: null,
  worker_id: null,
  lease_token: null,
  lease_expires_at: null,
  progress: 0.5,
  result: null,
  last_error: "first try failed",
};

const EXPECTED_OLD_JOB: Job = {
  id: "job-old",
  queue: "emails",
  name: "send",
  payload: { to: "a@example.test", n: [1, 2] },
  state: "delayed",
  priority: 7,
  attempts: 2,
  maxAttempts: 5,
  backoff: { type: "fixed", delayMs: 250 },
  timeoutMs: 3000,
  runAt: T + 1000,
  createdAt: T - 5000,
  updatedAt: T - 10,
  startedAt: T - 100,
  finishedAt: null,
  workerId: null,
  leaseToken: null,
  leaseExpiresAt: null,
  progress: 0.5,
  result: null,
  lastError: "first try failed",
  uniqueKey: null,
  scheduleId: null,
};

function insertRow(db: DatabaseSync, table: string, row: Record<string, string | number | null>) {
  const cols = Object.keys(row);
  db.prepare(`INSERT INTO ${table} (${cols.join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`).run(...Object.values(row));
}

/** Seed one of everything that exists at `version`. */
function seed(db: DatabaseSync, version: number) {
  if (version >= 1) {
    insertRow(db, "jobs", OLD_JOB);
    insertRow(db, "jobs", { ...OLD_JOB, id: "job-done", state: "completed", result: '{"ok":true}', finished_at: T, last_error: null });
  }
  if (version >= 2) {
    insertRow(db, "queues", {
      name: "emails",
      paused: 1,
      concurrency: 4,
      rate_limit_max: 10,
      rate_limit_interval_ms: 1000,
      default_max_attempts: 6,
      created_at: T - 9000,
      updated_at: T - 8000,
    });
  }
  if (version >= 3) {
    insertRow(db, "jobs", { ...OLD_JOB, id: "job-unique", state: "waiting", unique_key: "user:1" });
    insertRow(db, "job_events", { job_id: "job-old", type: "enqueued", data: '{"state":"delayed"}', at: T - 5000 });
    insertRow(db, "job_events", { job_id: "job-old", type: "claimed", data: null, at: T - 100 });
  }
  if (version >= 4) {
    insertRow(db, "schedules", {
      id: "sched-1",
      name: "nightly",
      queue: "emails",
      job_name: "digest",
      payload: '{"kind":"daily"}',
      cron: "0 3 * * *",
      timezone: "Europe/London",
      enabled: 1,
      overlap: "skip",
      next_run_at: T + 3_600_000,
      last_run_at: T - 82_800_000,
      last_job_id: "job-old",
      created_at: T - 9000,
      updated_at: T - 1,
    });
    insertRow(db, "jobs", { ...OLD_JOB, id: "job-scheduled", state: "waiting", schedule_id: "sched-1" });
  }
  if (version >= 5) {
    insertRow(db, "webhooks", {
      id: "hook-1",
      url: "https://example.test/hook",
      secret: "whsec_x",
      events: '["job.completed","job.dead"]',
      queues: '["emails"]',
      enabled: 1,
      created_at: T - 9000,
    });
    insertRow(db, "webhook_deliveries", {
      id: "del-1",
      webhook_id: "hook-1",
      event: "job.completed",
      body: '{"event":"job.completed"}',
      status: "failed",
      attempts: 5,
      last_status_code: 503,
      last_error: "HTTP 503",
      next_attempt_at: null,
      created_at: T - 5000,
      updated_at: T - 4000,
    });
  }
  if (version >= 6) {
    insertRow(db, "api_tokens", {
      id: "tok-1",
      name: "ops",
      hash: "abc123",
      scopes: '["admin"]',
      created_at: T - 9000,
      last_used_at: T - 10,
      revoked_at: null,
    });
  }
}

/** Check that everything seed(version) wrote reads back correctly through the Store. */
async function expectSeededData(store: SqliteStore, version: number) {
  if (version >= 1) {
    expect(await store.getJob("job-old")).toEqual(EXPECTED_OLD_JOB);
    expect(await store.getJob("job-done")).toMatchObject({ state: "completed", result: { ok: true }, finishedAt: T });
  }
  if (version >= 2) {
    expect(await store.getQueue("emails")).toEqual({
      name: "emails",
      paused: true,
      concurrency: 4,
      rateLimitMax: 10,
      rateLimitIntervalMs: 1000,
      defaultMaxAttempts: 6,
      createdAt: T - 9000,
      updatedAt: T - 8000,
    });
  }
  if (version >= 3) {
    // The migrated job still holds its unique key: enqueueing the key again gives it back.
    const probe: Job = { ...EXPECTED_OLD_JOB, id: "probe", state: "waiting", uniqueKey: "user:1" };
    expect((await store.insertJobs([probe], []))[0]?.id).toBe("job-unique");
    expect((await store.listJobEvents("job-old")).map(({ type, data, at }) => ({ type, data, at }))).toEqual([
      { type: "enqueued", data: { state: "delayed" }, at: T - 5000 },
      { type: "claimed", data: null, at: T - 100 },
    ]);
  }
  if (version >= 4) {
    expect(await store.getSchedule("sched-1")).toEqual({
      id: "sched-1",
      name: "nightly",
      queue: "emails",
      jobName: "digest",
      payload: { kind: "daily" },
      cron: "0 3 * * *",
      timezone: "Europe/London",
      enabled: true,
      overlap: "skip",
      nextRunAt: T + 3_600_000,
      lastRunAt: T - 82_800_000,
      lastJobId: "job-old",
      createdAt: T - 9000,
      updatedAt: T - 1,
    });
    expect((await store.listJobs({ scheduleId: "sched-1" })).items.some((j) => ["delayed", "waiting", "active"].includes(j.state))).toBe(true);
  }
  if (version >= 5) {
    expect(await store.getWebhook("hook-1")).toEqual({
      id: "hook-1",
      url: "https://example.test/hook",
      secret: "whsec_x",
      events: ["job.completed", "job.dead"],
      queues: ["emails"],
      enabled: true,
      createdAt: T - 9000,
    });
    expect(await store.listDeliveries("hook-1", 10)).toEqual([
      {
        id: "del-1",
        webhookId: "hook-1",
        event: "job.completed",
        body: '{"event":"job.completed"}',
        status: "failed",
        attempts: 5,
        lastStatusCode: 503,
        lastError: "HTTP 503",
        nextAttemptAt: null,
        createdAt: T - 5000,
        updatedAt: T - 4000,
      },
    ]);
  }
  if (version >= 6) {
    expect(await store.findTokenByHash("abc123")).toEqual({
      id: "tok-1",
      name: "ops",
      hash: "abc123",
      scopes: ["admin"],
      createdAt: T - 9000,
      lastUsedAt: T - 10,
      revokedAt: null,
    });
  }
}

describe("migrations", () => {
  let tmp: ReturnType<typeof tempDir>;
  let store: SqliteStore | undefined;

  beforeEach(() => {
    tmp = tempDir();
  });

  afterEach(async () => {
    if (store?.db.isOpen) await store.close();
    store = undefined;
    tmp.cleanup();
  });

  it("has one entry per schema version", () => {
    expect(LATEST).toBe(8);
    for (const m of MIGRATIONS) expect(m.trim().length).toBeGreaterThan(0);
  });

  for (let version = 0; version <= LATEST; version++) {
    describe(`from schema version ${version}`, () => {
      it(`upgrades to version ${LATEST} and keeps all existing data`, async () => {
        const file = tmp.file();
        const old = createAtVersion(file, version);
        seed(old, version);
        old.close();

        store = new SqliteStore(file);
        expect(await store.migrate()).toBe(LATEST);
        expect(userVersion(store.db)).toBe(LATEST);
        await expectSeededData(store, version);
      });

      it("ends up with exactly the same schema as a fresh database", async () => {
        const file = tmp.file();
        const old = createAtVersion(file, version);
        seed(old, version);
        old.close();
        store = new SqliteStore(file);
        await store.migrate();

        const fresh = new SqliteStore(tmp.file("fresh.db"));
        await fresh.migrate();
        try {
          expect(schemaOf(store.db)).toEqual(schemaOf(fresh.db));
        } finally {
          await fresh.close();
        }
      });

      it("leaves a database that works for new writes and is safe to migrate again", async () => {
        const file = tmp.file();
        const old = createAtVersion(file, version);
        seed(old, version);
        old.close();
        store = new SqliteStore(file);
        await store.migrate();

        const job: Job = { ...EXPECTED_OLD_JOB, id: "job-new", uniqueKey: "k", scheduleId: "s" };
        await store.insertJob(job);
        await store.appendJobEvent("job-new", "enqueued", null, T);
        expect(await store.migrate()).toBe(LATEST);
        expect(await store.getJob("job-new")).toEqual(job);
        await expectSeededData(store, version);
      });
    });
  }

  it("keeps existing jobs' new columns empty after upgrading from before they existed", async () => {
    const file = tmp.file();
    const old = createAtVersion(file, 2);
    seed(old, 2);
    old.close();
    store = new SqliteStore(file);
    await store.migrate();
    expect(await store.getJob("job-old")).toMatchObject({ uniqueKey: null, scheduleId: null });
  });

  it("rolls back a migration that fails part-way and leaves the version unchanged", async () => {
    const file = tmp.file();
    store = new SqliteStore(file);
    await store.migrate();
    await store.insertJob({ ...EXPECTED_OLD_JOB, id: "survivor" });

    MIGRATIONS.push("CREATE TABLE half_done (x INTEGER); ALTER TABLE jobs ADD COLUMN extra TEXT; SELECT * FROM no_such_table;");
    try {
      await expect(store.migrate()).rejects.toThrow(/no_such_table/);
    } finally {
      MIGRATIONS.pop();
    }
    expect(userVersion(store.db)).toBe(LATEST);
    const tables = store.db.prepare("SELECT name FROM sqlite_master WHERE name = 'half_done'").all();
    expect(tables).toEqual([]);
    const columns = (store.db.prepare("PRAGMA table_info(jobs)").all() as { name: string }[]).map((c) => c.name);
    expect(columns).not.toContain("extra");
    expect((await store.getJob("survivor"))?.id).toBe("survivor");
  });

  it("applies a new migration on top of existing data", async () => {
    const file = tmp.file();
    store = new SqliteStore(file);
    await store.migrate();
    await store.insertJob({ ...EXPECTED_OLD_JOB, id: "before" });
    MIGRATIONS.push("ALTER TABLE jobs ADD COLUMN note TEXT NOT NULL DEFAULT 'n/a';");
    try {
      expect(await store.migrate()).toBe(LATEST + 1);
      expect(userVersion(store.db)).toBe(LATEST + 1);
      expect(store.db.prepare("SELECT note FROM jobs WHERE id = 'before'").get()).toEqual({ note: "n/a" });
    } finally {
      MIGRATIONS.pop();
    }
  });

  // Regression (was KB-14): migration 8 enforces one unfinished job per (queue, unique key).
  // A database from before can hold several (the race this fixes created them). Creating the
  // index would fail on those, so the migration first keeps one job per key: an active one if
  // there is one (it is running; cancelling it would not undo that), otherwise the oldest. The
  // other waiting or delayed duplicates are cancelled (they have not run, and not running them
  // is what the key is for). Other active duplicates keep running but lose their key. Each
  // change is recorded as a job event naming the job that kept the key.
  it("resolves unfinished jobs that share a unique key before enforcing it", async () => {
    const file = tmp.file();
    const old = createAtVersion(file, 7);
    const job = (id: string, over: Record<string, string | number | null>) =>
      insertRow(old, "jobs", { ...OLD_JOB, id, queue: "q", state: "waiting", last_error: null, ...over });
    // queue q, key k1: three unfinished, none active -> keep the oldest
    job("j1a", { unique_key: "k1" });
    job("j1b", { unique_key: "k1" });
    job("j1c", { unique_key: "k1", state: "delayed" });
    // queue q, key k2: one waiting, two active -> keep the first active one
    job("j2a", { unique_key: "k2" });
    job("j2b", { unique_key: "k2", state: "active", worker_id: "w1", lease_token: "t1", lease_expires_at: T + 1000 });
    job("j2c", { unique_key: "k2", state: "active", worker_id: "w2", lease_token: "t2", lease_expires_at: T + 1000 });
    // left alone: the same key in another queue, finished jobs with the key, a key held once
    job("j3", { unique_key: "k1", queue: "other" });
    job("j4", { unique_key: "k1", state: "completed", finished_at: T });
    job("j5", { unique_key: "k2", state: "dead", finished_at: T });
    job("j6", { unique_key: "k3" });
    old.close();

    store = new SqliteStore(file);
    const before = Date.now();
    expect(await store.migrate()).toBe(LATEST);
    const after = Date.now();

    const get = async (id: string) => (await store!.getJob(id))!;
    for (const id of ["j1a", "j2b", "j3", "j4", "j5", "j6"]) {
      const j = await get(id);
      expect({ id, changed: j.updatedAt !== OLD_JOB.updated_at }).toEqual({ id, changed: false });
    }
    for (const [id, kept, previous] of [
      ["j1b", "j1a", "waiting"],
      ["j1c", "j1a", "delayed"],
      ["j2a", "j2b", "waiting"],
    ] as const) {
      const j = await get(id);
      expect(j).toMatchObject({ state: "cancelled", leaseToken: null, leaseExpiresAt: null });
      expect(j.finishedAt).toBeGreaterThanOrEqual(before);
      expect(j.finishedAt).toBeLessThanOrEqual(after);
      const [event] = await store.listJobEvents(id);
      expect(event).toMatchObject({ type: "cancelled", data: { previousState: previous, reason: "duplicate unique key", keptJobId: kept } });
    }
    expect(await get("j2c")).toMatchObject({ state: "active", uniqueKey: null, leaseToken: "t2" });
    expect((await store.listJobEvents("j2c"))[0]).toMatchObject({ type: "unique_key_released", data: { uniqueKey: "k2", keptJobId: "j2b" } });

    // From now on the database itself refuses a second unfinished job with a key.
    await expect(store.insertJob({ ...EXPECTED_OLD_JOB, id: "j7", state: "waiting", uniqueKey: "k1", queue: "q" })).rejects.toThrow(/UNIQUE/);
    await store.insertJob({ ...EXPECTED_OLD_JOB, id: "j8", state: "completed", uniqueKey: "k1", queue: "q" });
  });

  // Regression (was KB-15): a database migrated by a newer release was opened without
  // complaint, so older code (e.g. after a rollback) ran against a schema it didn't know.
  it("refuses a database from a newer schema version, naming both versions, and changes nothing", async () => {
    const file = tmp.file();
    const newer = createAtVersion(file, LATEST);
    seed(newer, LATEST);
    newer.exec(`PRAGMA user_version = ${LATEST + 2}`);
    const before = schemaOf(newer);
    newer.close();
    store = new SqliteStore(file);
    await expect(store.migrate()).rejects.toThrow(
      `database schema version ${LATEST + 2} is newer than this version of Conveyor supports (${LATEST})`,
    );
    expect(userVersion(store.db)).toBe(LATEST + 2);
    expect(schemaOf(store.db)).toEqual(before);
  });
});
