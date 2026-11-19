import { spawn } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import { tempDir } from "../helpers.js";
import { describeStoreContract, makeJob, T } from "./store-contract.js";

describeStoreContract("SqliteStore (temp file)", async () => {
  const tmp = tempDir();
  const store = new SqliteStore(tmp.file());
  return {
    store,
    cleanup: async () => {
      await store.close();
      tmp.cleanup();
    },
  };
});

describe("SqliteStore on a file", () => {
  let tmp: ReturnType<typeof tempDir>;
  const open: SqliteStore[] = [];
  const openStore = (file = tmp.file(), options = {}) => {
    const s = new SqliteStore(file, options);
    open.push(s);
    return s;
  };
  const pragma = (s: SqliteStore, name: string) => Object.values(s.db.prepare(`PRAGMA ${name}`).get() as object)[0];

  beforeEach(() => {
    tmp = tempDir();
  });

  afterEach(async () => {
    for (const s of open.splice(0)) if (s.db.isOpen) await s.close();
    tmp.cleanup();
  });

  it("uses WAL, foreign keys and a busy timeout", async () => {
    const s = openStore();
    expect(pragma(s, "journal_mode")).toBe("wal");
    expect(pragma(s, "foreign_keys")).toBe(1);
    expect(pragma(s, "busy_timeout")).toBe(5000);
    expect(pragma(openStore(tmp.file("other.db"), { busyTimeoutMs: 250 }), "busy_timeout")).toBe(250);
  });

  // Opening never waits longer than the busy timeout for the switch to WAL. (With several
  // processes opening a new file together, SQLite can report SQLITE_BUSY at once, and the
  // constructor retries; that case needs real processes: see cross-process.test.ts.) Here the
  // lock is never released.
  it("gives up switching a new file to WAL after the busy timeout", () => {
    const file = tmp.file("locked.db");
    const holder = new DatabaseSync(file);
    holder.exec("CREATE TABLE t (x); BEGIN EXCLUSIVE; INSERT INTO t VALUES (1)");
    const started = Date.now();
    expect(() => openStore(file, { busyTimeoutMs: 150 })).toThrow(/database is locked/);
    expect(Date.now() - started).toBeGreaterThanOrEqual(140);
    holder.exec("ROLLBACK");
    holder.close();
  });

  it("keeps data across close and reopen", async () => {
    const a = openStore();
    await a.migrate();
    const job = makeJob({ payload: { kept: true } });
    await a.insertJob(job);
    await a.close();
    const b = openStore();
    await b.migrate();
    expect(await b.getJob(job.id)).toEqual(job);
  });

  it("lets two connections to the same file see each other's writes", async () => {
    const a = openStore();
    const b = openStore();
    await a.migrate();
    await b.migrate();
    const job = makeJob({ state: "active", leaseToken: "tok" });
    await a.insertJob(job);
    expect(await b.getJob(job.id)).toEqual(job);
    await b.completeJob(job.id, "tok", { by: "b" }, T);
    expect((await a.getJob(job.id))?.state).toBe("completed");
  });

  it("fails every call after close", async () => {
    const s = openStore();
    await s.migrate();
    await s.close();
    await expect(s.getJob("x")).rejects.toThrow();
    await expect(s.insertJob(makeJob())).rejects.toThrow();
  });

  it("reads rows with unparseable JSON using safe fallbacks", async () => {
    const s = openStore();
    await s.migrate();
    const job = makeJob();
    await s.insertJob(job);
    s.db.prepare("UPDATE jobs SET payload = '{oops', result = 'nope', backoff = '[' WHERE id = ?").run(job.id);
    expect(await s.getJob(job.id)).toMatchObject({ payload: null, result: null, backoff: null });
  });

  it("uses an in-memory database without WAL by default", async () => {
    const s = new SqliteStore();
    open.push(s);
    expect(pragma(s, "journal_mode")).toBe("memory");
    await s.migrate();
    await s.insertJob(makeJob());
    expect((await s.listJobs({})).items).toHaveLength(1);
  });
});

// claimNext first looks for a candidate without a lock, then decides again inside BEGIN
// IMMEDIATE. These tests make another process claim a job after that first look and before
// claimNext gets the write lock: it must go by what it sees inside the transaction.
describe("claimNext against a claim committed by another process while it waited", () => {
  let tmp: ReturnType<typeof tempDir>;
  let store: SqliteStore;

  beforeEach(async () => {
    tmp = tempDir();
    store = new SqliteStore(tmp.file());
    await store.migrate();
  });

  afterEach(async () => {
    await store.close();
    tmp.cleanup();
  });

  /**
   * In another process: take the write lock, activate `jobId`, hold the lock for `holdMs`, then
   * commit. Resolves once the lock is held, with a promise for the child's exit.
   */
  async function claimElsewhere(jobId: string, holdMs: number) {
    const script = `
      const { DatabaseSync } = require("node:sqlite");
      const db = new DatabaseSync(process.argv[1]);
      db.exec("BEGIN IMMEDIATE");
      db.prepare("UPDATE jobs SET state = 'active', lease_token = 'other', worker_id = 'other' WHERE id = ?").run(process.argv[2]);
      process.stdout.write("locked\\n");
      setTimeout(() => { db.exec("COMMIT"); db.close(); }, Number(process.argv[3]));
    `;
    const child = spawn(process.execPath, ["-e", script, tmp.file(), jobId, String(holdMs)], { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    child.stderr!.on("data", (c) => (stderr += c));
    const exited = new Promise<number | null>((resolve) => child.on("exit", resolve));
    await new Promise<void>((resolve, reject) => {
      child.stdout!.on("data", (c) => String(c).includes("locked") && resolve());
      child.on("exit", (code) => reject(new Error(`child exited (${code}) before taking the lock: ${stderr}`)));
    });
    return { exited };
  }

  const claim = () => store.claimNext({ queues: ["q"], workerId: "me", leaseToken: "mine", leaseExpiresAt: T + 1000, now: T });

  it("takes the next job when its first choice was claimed", async () => {
    const first = makeJob({ priority: 10 });
    const second = makeJob();
    await store.insertJob(first);
    await store.insertJob(second);
    const other = await claimElsewhere(first.id, 300);
    // Blocks this thread until the other process commits (well inside the 5s busy timeout).
    expect((await claim())?.id).toBe(second.id);
    expect(await other.exited).toBe(0);
    expect(await store.getJob(first.id)).toMatchObject({ state: "active", leaseToken: "other" });
  });

  it("respects a concurrency limit that the other claim used up", async () => {
    await store.upsertQueue({ name: "q", paused: false, concurrency: 1, rateLimitMax: 0, rateLimitIntervalMs: 0, defaultMaxAttempts: 3, createdAt: T, updatedAt: T });
    // The other process takes a job claimNext would not pick, so claimNext's first choice is
    // still waiting: only the limit, checked inside the transaction, can stop it.
    const first = makeJob({ priority: 10 });
    const second = makeJob();
    await store.insertJob(first);
    await store.insertJob(second);
    const other = await claimElsewhere(second.id, 300);
    expect(await claim()).toBeUndefined();
    expect(await other.exited).toBe(0);
    expect(await store.countActive("q")).toBe(1);
    expect((await store.getJob(first.id))?.state).toBe("waiting");
  });
});
