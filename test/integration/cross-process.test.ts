// Unique keys, schedules and migrations with several OS processes on one SQLite file.
//
// Each test starts real processes (test/fixtures/race-roles.ts), each with its own database
// connection, releases them at the same moment and then checks the database and what every
// process reported. Where a test slows the store down ("storeDelayMs"), that only widens the gap
// between two store calls, so an operation that is one atomic store call is unaffected and a
// check-then-act across calls reliably collides. Real processes and real time; every wait has
// a deadline.
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Conveyor } from "../../src/core/conveyor.js";
import { Scheduler } from "../../src/scheduler/scheduler.js";
import { MIGRATIONS } from "../../src/storage/migrations.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import type { RaceLogLine, RaceRole } from "../fixtures/race-roles.js";
import { tempDir } from "../helpers.js";
import { killAll, readJsonLines, runTogether, spawnTs } from "../process-harness.js";

const CHILD = fileURLToPath(new URL("../fixtures/race-roles.ts", import.meta.url));

let tmp: ReturnType<typeof tempDir>;

beforeEach(() => {
  tmp = tempDir("conveyor-xproc-");
});

afterEach(() => {
  killAll();
  tmp.cleanup();
});

/** Run one process per role against `db`, all released together; returns every log line. */
async function run(db: string, roles: RaceRole[]): Promise<RaceLogLine[]> {
  const logs = roles.map((_, i) => tmp.file(`log-${i}-${Math.random().toString(36).slice(2)}.jsonl`));
  await runTogether(roles.map((r, i) => spawnTs(CHILD, [db, logs[i]!, JSON.stringify(r)], `${r.role} ${i}`)));
  return logs.flatMap((f) => readJsonLines<RaceLogLine>(f));
}

describe("unique keys across processes", () => {
  // Three processes enqueue the same ten keys at the same moment (one of them with
  // enqueueBulk). For every key there must be one job, and every caller must get that job back.
  it(
    "gives one job per key, and every caller gets that job back",
    async () => {
      const db = tmp.file();
      const store = new SqliteStore(db);
      await store.migrate();
      const keys = Array.from({ length: 10 }, (_, i) => `user:${i}`);
      const base = { role: "enqueuer" as const, queue: "q", keys, storeDelayMs: 20 };
      const lines = await run(db, [
        { ...base, bulk: false },
        { ...base, bulk: false },
        { ...base, bulk: true },
      ]);

      const jobs = (await store.listJobs({ queue: "q", limit: 500 })).items;
      const byKey = new Map<string, string[]>();
      for (const j of jobs) byKey.set(j.uniqueKey!, [...(byKey.get(j.uniqueKey!) ?? []), j.id]);
      // One job per key in the database...
      expect(Object.fromEntries([...byKey].filter(([, ids]) => ids.length !== 1))).toEqual({});
      expect(jobs).toHaveLength(keys.length);
      // ...and every caller got exactly that job back for every key.
      const got = lines.filter((l) => l.kind === "enqueued");
      expect(got).toHaveLength(3 * keys.length);
      expect(got.filter((l) => byKey.get(l.key)?.[0] !== l.jobId)).toEqual([]);
      // One "enqueued" event per job: the callers that got an existing job back created nothing.
      for (const j of jobs) expect((await store.listJobEvents(j.id)).filter((e) => e.type === "enqueued")).toHaveLength(1);
      await store.close();
    },
    60_000,
  );
});

describe("schedules across processes", () => {
  // Two `serve`-like processes tick the same schedules (three with overlap "allow", three with
  // "skip"), all of them due. Each slot must fire once, whichever process fires it.
  it(
    "fires each slot once when two schedulers tick at the same time",
    async () => {
      const db = tmp.file();
      const store = new SqliteStore(db);
      const conveyor = new Conveyor({ store });
      await conveyor.init();
      const scheduler = new Scheduler(conveyor);
      // Yearly, so no second slot comes due while the test runs.
      const schedules = [];
      for (let i = 0; i < 6; i++) {
        schedules.push(await scheduler.create({ name: `s${i}`, queue: "q", jobName: "x", cron: "0 0 1 1 *", overlap: i < 3 ? "allow" : "skip" }));
      }
      const slot = Date.now() - 1000;
      store.db.prepare("UPDATE schedules SET next_run_at = ?").run(slot); // due now

      const lines = await run(db, [
        { role: "scheduler", durationMs: 1500, storeDelayMs: 30 },
        { role: "scheduler", durationMs: 1500, storeDelayMs: 30 },
      ]);

      const fired = lines.flatMap((l) => (l.kind === "fired" ? l.jobIds : []));
      for (const s of schedules) {
        const jobs = (await conveyor.listJobs({ scheduleId: s.id })).items;
        expect({ schedule: s.name, jobs: jobs.length }).toEqual({ schedule: s.name, jobs: 1 });
        const after = await scheduler.get(s.id);
        expect(after).toMatchObject({ lastRunAt: slot, lastJobId: jobs[0]!.id });
        expect(after.nextRunAt).toBeGreaterThan(Date.now());
      }
      // Each process reports only the slots it fired itself.
      expect(fired.toSorted()).toEqual([...new Set(fired)].toSorted());
      expect(fired).toHaveLength(6);
      await store.close();
    },
    60_000,
  );
});

describe("migrations across processes", () => {
  // Regression (was KB-16): `serve` and workers starting together on a new file all open and
  // migrate it. Two things failed: the version check and the migration were separate steps
  // ("table jobs already exists"), and switching the new file to WAL reported SQLITE_BUSY at
  // once instead of waiting ("database is locked").
  it(
    "lets three processes migrate a fresh file at the same time",
    async () => {
      for (let round = 0; round < 4; round++) {
        const db = tmp.file(`fresh-${round}.db`);
        const lines = await run(db, [{ role: "migrator" }, { role: "migrator" }, { role: "migrator" }]);
        expect(lines).toHaveLength(3);
        expect(lines.map((l) => (l.kind === "migrated" ? { ok: l.ok, version: l.version, error: l.error } : l))).toEqual(
          Array.from({ length: 3 }, () => ({ ok: true, version: MIGRATIONS.length, error: null })),
        );
        const check = new DatabaseSync(db);
        expect(check.prepare("PRAGMA user_version").get()).toEqual({ user_version: MIGRATIONS.length });
        expect(check.prepare("SELECT COUNT(*) AS n FROM jobs").get()).toEqual({ n: 0 });
        check.close();
      }
    },
    120_000,
  );
});
