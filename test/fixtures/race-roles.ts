// Child process for test/integration/cross-process.test.ts. Run with `node --import tsx`.
//
//   race-roles.ts <db file> <log file> <options as JSON>
//
// Opens its own connection to the shared database, prints "ready", waits for "go" (see
// test/process-harness.ts) and then plays one role:
//
// - "enqueuer": enqueues one job per unique key (one by one, or in one enqueueBulk call) and
//   logs the id of the job it got back for each key.
// - "scheduler": what `serve` does for schedules: ticks the Scheduler every few ms for a while,
//   and logs the ids of the jobs its ticks fired.
// - "migrator": opens the (fresh) database file and migrates it, as `serve` and every worker do
//   when they start, and logs whether that worked. Opening is part of the race: it switches a
//   new file to WAL mode.
//
// storeDelayMs > 0 makes every store call take that much longer (after the call returns), which
// widens the gap between a read and a write based on it. An operation done in one store call
// is not affected.
import { appendFileSync } from "node:fs";
import { Conveyor } from "../../src/core/conveyor.js";
import { Scheduler } from "../../src/scheduler/scheduler.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import type { Store } from "../../src/storage/store.js";
import { readyThenGo } from "../process-harness.js";

export type RaceRole =
  | { role: "enqueuer"; queue: string; keys: string[]; bulk: boolean; storeDelayMs: number }
  | { role: "scheduler"; durationMs: number; storeDelayMs: number }
  | { role: "migrator" };

export type RaceLogLine =
  | { kind: "enqueued"; pid: number; key: string; jobId: string }
  | { kind: "fired"; pid: number; jobIds: string[] }
  | { kind: "migrated"; pid: number; ok: boolean; version: number | null; error: string | null };

const [dbPath, logFile, rawOptions] = process.argv.slice(2);
if (!dbPath || !logFile || !rawOptions) throw new Error("usage: race-roles.ts <db> <log> <options json>");
const opts = JSON.parse(rawOptions) as RaceRole;
const log = (line: RaceLogLine) => appendFileSync(logFile, JSON.stringify(line) + "\n");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function slowed(store: Store, ms: number): Store {
  if (ms <= 0) return store;
  return new Proxy(store, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof value !== "function") return value;
      return async (...args: unknown[]) => {
        const result = await value.apply(target, args);
        await sleep(ms);
        return result;
      };
    },
  });
}

if (opts.role === "migrator") {
  await readyThenGo();
  try {
    const sqlite = new SqliteStore(dbPath);
    log({ kind: "migrated", pid: process.pid, ok: true, version: await sqlite.migrate(), error: null });
    await sqlite.close();
  } catch (err) {
    log({ kind: "migrated", pid: process.pid, ok: false, version: null, error: (err as Error).message });
  }
} else {
  const sqlite = new SqliteStore(dbPath);
  const conveyor = new Conveyor({ store: slowed(sqlite, opts.storeDelayMs) });
  if (opts.role === "enqueuer") {
    await readyThenGo();
    const reqs = opts.keys.map((key) => ({ queue: opts.queue, name: "x", uniqueKey: key, payload: { pid: process.pid } }));
    const jobs = opts.bulk ? await conveyor.enqueueBulk(reqs) : [];
    if (!opts.bulk) for (const r of reqs) jobs.push(await conveyor.enqueue(r));
    jobs.forEach((job, i) => log({ kind: "enqueued", pid: process.pid, key: opts.keys[i]!, jobId: job.id }));
  } else {
    const scheduler = new Scheduler(conveyor);
    await readyThenGo();
    const until = Date.now() + opts.durationMs;
    const fired: string[] = [];
    while (Date.now() < until) {
      fired.push(...(await scheduler.tick()));
      await sleep(5);
    }
    log({ kind: "fired", pid: process.pid, jobIds: fired });
  }
  await sqlite.close();
}

process.exit(0);
