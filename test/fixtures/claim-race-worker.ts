// Child process for test/integration/claim-race.test.ts. Run with `node --import tsx`.
//
//   claim-race-worker.ts <db file> <log file> <options as JSON>
//
// Opens its own connection to the shared database and plays one role:
//
// - "worker": runs a real Worker on one queue. Every handler run appends a "start" line to the
//   log when it begins and an "end" line when it returns, so a run that is abandoned (its lease
//   lost) is still counted.
// - "reaper": runs Maintenance's reaper and promoter in a loop. On exit it appends one "reaper"
//   line: the ids of the jobs it saw with an expired lease, and how many it reaped.
//
// The process prints "ready" once it is set up and waits for "go" on stdin, so that all
// processes start together. It exits once every job in the queue is completed, or after a
// deadline.
//
// storeDelayMs > 0 makes every store call take that much longer, as on a busy disk or a loaded
// event loop. The delay is added after the call returns, so it widens the gap between two store
// calls (a read and the write based on it) without changing what any one call does. An
// operation done as one atomic store call is not affected; a check-then-act across several
// calls is exposed.
import { appendFileSync } from "node:fs";
import { Broker } from "../../src/broker/broker.js";
import { Maintenance } from "../../src/broker/maintenance.js";
import { Conveyor } from "../../src/core/conveyor.js";
import type { Job } from "../../src/core/types.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import type { Store } from "../../src/storage/store.js";
import { HandlerRegistry } from "../../src/worker/handlers.js";
import { Worker } from "../../src/worker/worker.js";
import { readyThenGo } from "../process-harness.js";

export interface ClaimRaceOptions {
  role: "worker" | "reaper";
  queue: string;
  /** Number of jobs in the queue; the process exits when this many are completed. */
  total: number;
  /** Worker loops in this process. */
  concurrency: number;
  storeDelayMs: number;
  /** How long each handler run takes. */
  handlerMs: number;
  leaseMs: number;
  heartbeatMs: number;
  /** Pause between reaper passes. */
  reaperIntervalMs: number;
  deadlineMs: number;
}

/** Lines in the log file. */
export type LogLine =
  | { kind: "start"; run: string; jobId: string; pid: number; t: number }
  | { kind: "end"; run: string; t: number }
  | { kind: "reaper"; pid: number; sawExpired: string[]; reaped: number };

const [dbPath, logFile, rawOptions] = process.argv.slice(2);
if (!dbPath || !logFile || !rawOptions) throw new Error("usage: claim-race-worker.ts <db> <log> <options json>");
const opts = JSON.parse(rawOptions) as ClaimRaceOptions;
const log = (line: LogLine) => appendFileSync(logFile, JSON.stringify(line) + "\n");

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const sawExpired: string[] = [];

function slowed(store: Store, ms: number): Store {
  return new Proxy(store, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof value !== "function") return value;
      return async (...args: unknown[]) => {
        const result = await value.apply(target, args);
        if (prop === "findExpiredLeases") sawExpired.push(...(result as Job[]).map((j) => j.id));
        if (ms > 0) await sleep(ms);
        return result;
      };
    },
  });
}

const sqlite = new SqliteStore(dbPath);
const store = slowed(sqlite, opts.storeDelayMs);
const conveyor = new Conveyor({ store, config: { leaseMs: opts.leaseMs, heartbeatMs: opts.heartbeatMs } });

await readyThenGo();

const deadline = Date.now() + opts.deadlineMs;
const allCompleted = async () => (await sqlite.countJobs(opts.queue)).completed >= opts.total;
let exitCode = 0;

if (opts.role === "worker") {
  let runs = 0;
  const handlers = new HandlerRegistry();
  handlers.register("record", async ({ job }) => {
    const run = `${process.pid}-${++runs}`;
    log({ kind: "start", run, jobId: job.id, pid: process.pid, t: Date.now() });
    // Ignores its abort signal, as much real handler code does: a run whose lease is lost keeps
    // going, so a job handed to a second worker really runs twice.
    if (opts.handlerMs > 0) await sleep(opts.handlerMs);
    log({ kind: "end", run, t: Date.now() });
  });
  const worker = new Worker(new Broker(conveyor), {
    queues: [opts.queue],
    handlers,
    concurrency: opts.concurrency,
    pollMs: 1,
    maxPollMs: 5,
    heartbeatMs: opts.heartbeatMs,
  });
  worker.start();
  while (!(await allCompleted())) {
    if (Date.now() > deadline) {
      exitCode = 2;
      break;
    }
    await sleep(10);
  }
  await worker.stop();
} else {
  const maintenance = new Maintenance(conveyor);
  let reaped = 0;
  while (!(await allCompleted())) {
    if (Date.now() > deadline) {
      exitCode = 2;
      break;
    }
    reaped += await maintenance.reap();
    await maintenance.promote();
    await sleep(opts.reaperIntervalMs);
  }
  log({ kind: "reaper", pid: process.pid, sawExpired, reaped });
}

await sqlite.close();
process.exit(exitCode);
