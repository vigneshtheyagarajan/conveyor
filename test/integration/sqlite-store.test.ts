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
    const job = makeJob();
    await a.insertJob(job);
    expect(await b.getJob(job.id)).toEqual(job);
    await b.completeJob(job.id, { by: "b" }, T);
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
