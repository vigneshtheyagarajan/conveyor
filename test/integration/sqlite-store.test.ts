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

// The reconcile sweep is a safety net: every transition already settles dependents in its own
// transaction, so the only way to leave work for it is to change a job behind the store's back.
// These tests do that with raw SQL.
describe("SqliteStore.reconcileBlocked", () => {
  let s: SqliteStore;

  beforeEach(async () => {
    s = new SqliteStore();
    await s.migrate();
  });

  afterEach(async () => {
    await s.close();
  });

  const setState = (id: string, state: string) => s.db.prepare("UPDATE jobs SET state = ? WHERE id = ?").run(state, id);

  async function child(parents: string[], policy: "fail" | "continue" = "fail") {
    const job = makeJob({ state: "blocked", runAt: T - 100 });
    await s.insertJobs([job], [], parents.map((parentId) => ({ childId: job.id, parentId, onFailure: policy })));
    return job.id;
  }

  it("releases blocked jobs whose dependencies all settled, and cancels the ones a fail dependency ended, with their dependents", async () => {
    const [done, failed, open] = [makeJob(), makeJob(), makeJob()];
    for (const j of [done, failed, open]) await s.insertJob(j);
    const release = await child([done.id]);
    const cancel = await child([failed.id]);
    const cancelsToo = await child([cancel]);
    const keep = await child([done.id, open.id]);
    setState(done.id, "completed");
    setState(failed.id, "dead");

    const changed = (await s.reconcileBlocked(T + 5, 100)).jobs;
    expect(changed.map((t) => [t.job.id, t.job.state, t.cause])).toEqual([
      [release, "waiting", null],
      [cancel, "cancelled", failed.id],
      [cancelsToo, "cancelled", cancel],
    ]);
    expect((await s.getJob(keep))!.state).toBe("blocked");
    expect((await s.getJob(release))!.runAt).toBe(T + 5);
    expect(await s.reconcileBlocked(T + 6, 100)).toEqual({ jobs: [], flowOutcomes: [] });
  });

  // A job must never run without a dependency it was created with, whatever the policy.
  it("cancels a blocked job whose dependency was deleted, even with others still unfinished", async () => {
    const [p, q] = [makeJob(), makeJob()];
    for (const j of [p, q]) await s.insertJob(j);
    const c = await child([p.id, q.id], "continue");
    s.db.prepare("DELETE FROM jobs WHERE id = ?").run(p.id);
    const changed = (await s.reconcileBlocked(T, 100)).jobs;
    expect(changed.map((t) => [t.job.id, t.job.state, t.cause])).toEqual([[c, "cancelled", p.id]]);
    expect(await s.getJob(c)).toMatchObject({ terminatedBy: p.id, lastError: `dependency ${p.id} no longer exists` });
    expect((await s.listJobEvents(c)).at(-1)!.data).toEqual({ previousState: "blocked", dependencyId: p.id, dependencyState: "deleted" });
    await expect(s.retryJob(c, T + 1)).rejects.toThrow(`its dependency ${p.id} no longer exists`);
  });

  it("names every deleted dependency when refusing a retry", async () => {
    const [p, q] = [makeJob({ state: "completed" }), makeJob({ state: "completed" })];
    for (const j of [p, q]) await s.insertJob(j);
    const dead = makeJob({ state: "dead", finishedAt: T });
    await s.insertJobs([dead], [], [p, q].map((j) => ({ childId: dead.id, parentId: j.id, onFailure: "fail" as const })));
    s.db.prepare("DELETE FROM jobs WHERE id IN (?, ?)").run(p.id, q.id);
    await expect(s.retryJob(dead.id, T)).rejects.toThrow(`job ${dead.id} can't be retried: its dependencies ${[p.id, q.id].sort().join(", ")} no longer exist`);
  });

  it("handles at most `limit` jobs per call, counting cascades separately", async () => {
    const p = makeJob();
    await s.insertJob(p);
    const kids = [await child([p.id]), await child([p.id]), await child([p.id])];
    setState(p.id, "completed");
    expect((await s.reconcileBlocked(T, 2)).jobs).toHaveLength(2);
    expect((await s.reconcileBlocked(T, 2)).jobs).toHaveLength(1);
    for (const id of kids) expect((await s.getJob(id))!.state).toBe("waiting");
  });

  it("skips a job already settled by an earlier job's cascade in the same batch", async () => {
    const p = makeJob();
    await s.insertJob(p);
    const a = await child([p.id]);
    const b = await child([a, p.id]); // found by the sweep too, but a's cascade gets there first
    setState(p.id, "cancelled");
    const changed = (await s.reconcileBlocked(T, 100)).jobs;
    // b appears once. Both its dependencies have failed; the lowest id (p, older) is the cause.
    expect(changed.map((t) => [t.job.id, t.cause])).toEqual([
      [a, p.id],
      [b, p.id],
    ]);
  });
});
