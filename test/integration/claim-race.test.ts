// Claims and reaping from several OS processes sharing one SQLite file.
//
// Each test enqueues jobs, starts child processes (test/fixtures/claim-race-worker.ts), each
// running a real Worker or a reaper with its own database connection, releases them at the
// same moment, and afterwards reads what every handler run and reaper recorded. Real processes
// and real time; every wait has a deadline.
//
// Two ways of making claims collide:
// - "natural": no fault injection, many jobs, several processes. Shows the problem is real, but
//   how often it shows up depends on CPU scheduling (on a single-core runner it may not).
// - "slow store": every store call in the children takes a few ms longer (see the fixture).
//   This widens the gap between reading a candidate and activating it, so two processes that
//   start together reliably pick the same job. A claim that is one atomic store operation
//   is not affected by the delay.
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Broker } from "../../src/broker/broker.js";
import { Conveyor } from "../../src/core/conveyor.js";
import type { Job } from "../../src/core/types.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import type { BackoffStrategy } from "../../src/util/backoff.js";
import type { ClaimRaceOptions, LogLine } from "../fixtures/claim-race-worker.js";
import { tempDir } from "../helpers.js";
import { killAll, readJsonLines, runTogether, spawnTs } from "../process-harness.js";

const CHILD = fileURLToPath(new URL("../fixtures/claim-race-worker.ts", import.meta.url));
const QUEUE = "race";

let tmp: ReturnType<typeof tempDir>;

beforeEach(() => {
  tmp = tempDir("conveyor-race-");
});

afterEach(() => {
  killAll();
  tmp.cleanup();
});

interface Scenario {
  /** Worker processes. */
  processes: number;
  jobs: number;
  /** Worker loops per process. 1 by default, so that every collision is between processes. */
  concurrency?: number;
  /** Extra time per store call in worker processes. */
  storeDelayMs?: number;
  handlerMs?: number;
  /** Queue concurrency limit (0 = unlimited). */
  queueConcurrency?: number;
  /** Lease and heartbeat for every process (default: far longer than the test). */
  leaseMs?: number;
  heartbeatMs?: number;
  backoff?: BackoffStrategy;
  /** Reaper processes, and the extra time per store call in them. */
  reapers?: number;
  reaperStoreDelayMs?: number;
  /** Runs after the jobs are enqueued, before any process starts. */
  before?: (conveyor: Conveyor, jobs: Job[]) => Promise<void>;
}

/** One handler run. `end` is null for a run that never finished (its lease was lost). */
interface RunRecord {
  jobId: string;
  pid: number;
  start: number;
  end: number | null;
}

interface ReaperReport {
  pid: number;
  sawExpired: string[];
  reaped: number;
}

interface Outcome {
  runs: RunRecord[];
  reapers: ReaperReport[];
  jobs: Job[];
  claimEvents: Map<string, number>;
  leaseExpiredEvents: Map<string, number>;
}

async function runScenario(s: Scenario): Promise<Outcome> {
  const db = tmp.file();
  const store = new SqliteStore(db);
  const conveyor = new Conveyor({ store, config: { leaseMs: s.leaseMs ?? 120_000 } });
  await conveyor.init();
  if (s.queueConcurrency) await conveyor.updateQueue(QUEUE, { concurrency: s.queueConcurrency });
  const enqueued = await conveyor.enqueueBulk(
    Array.from({ length: s.jobs }, (_, i) => ({ queue: QUEUE, name: "record", payload: i, ...(s.backoff ? { backoff: s.backoff } : {}) })),
  );
  await s.before?.(conveyor, enqueued);

  const base: ClaimRaceOptions = {
    role: "worker",
    queue: QUEUE,
    total: s.jobs,
    concurrency: s.concurrency ?? 1,
    storeDelayMs: s.storeDelayMs ?? 0,
    handlerMs: s.handlerMs ?? 0,
    leaseMs: s.leaseMs ?? 120_000,
    heartbeatMs: s.heartbeatMs ?? 60_000,
    reaperIntervalMs: 20,
    deadlineMs: 20_000,
  };
  const roles: ClaimRaceOptions[] = [
    ...Array.from({ length: s.processes }, () => base),
    ...Array.from({ length: s.reapers ?? 0 }, (): ClaimRaceOptions => ({ ...base, role: "reaper", storeDelayMs: s.reaperStoreDelayMs ?? 0 })),
  ];
  const logs = roles.map((_, i) => tmp.file(`log-${i}.jsonl`));
  const procs = roles.map((opts, i) => spawnTs(CHILD, [db, logs[i]!, JSON.stringify(opts)], `${opts.role} process ${i}`));
  await runTogether(procs);

  const lines = logs.flatMap((f) => readJsonLines<LogLine>(f));
  const runs = new Map<string, RunRecord>();
  const reapers: ReaperReport[] = [];
  for (const l of lines) {
    if (l.kind === "start") runs.set(l.run, { jobId: l.jobId, pid: l.pid, start: l.t, end: null });
    else if (l.kind === "end") runs.get(l.run)!.end = l.t;
    else reapers.push({ pid: l.pid, sawExpired: l.sawExpired, reaped: l.reaped });
  }
  const jobs = await Promise.all(enqueued.map((j) => conveyor.getJob(j.id)));
  const claimEvents = new Map<string, number>();
  const leaseExpiredEvents = new Map<string, number>();
  for (const j of enqueued) {
    const events = await store.listJobEvents(j.id);
    claimEvents.set(j.id, events.filter((e) => e.type === "claimed").length);
    leaseExpiredEvents.set(j.id, events.filter((e) => e.type === "lease_expired").length);
  }
  await store.close();
  return { runs: [...runs.values()], reapers, jobs, claimEvents, leaseExpiredEvents };
}

/** Jobs whose handler ran more than once, with the pid of every run. */
function ranMoreThanOnce(runs: RunRecord[]): Record<string, number[]> {
  const byJob = new Map<string, number[]>();
  for (const r of runs) byJob.set(r.jobId, [...(byJob.get(r.jobId) ?? []), r.pid]);
  return Object.fromEntries([...byJob].filter(([, pids]) => pids.length > 1));
}

/** Highest number of handler runs in progress at the same time, across all processes. */
function peakConcurrency(runs: RunRecord[]): number {
  // End before start at the same millisecond: a run that ended at t does not overlap one that
  // started at t.
  const points = runs.flatMap((r) => [
    { t: r.start, d: +1 },
    { t: r.end ?? Infinity, d: -1 },
  ]);
  points.sort((a, b) => a.t - b.t || a.d - b.d);
  let now = 0;
  let peak = 0;
  for (const p of points) peak = Math.max(peak, (now += p.d));
  return peak;
}

describe("claims from several processes sharing one database", () => {
  it.each([
    { name: "natural timing", processes: 4, jobs: 400, storeDelayMs: 0 },
    { name: "slow store", processes: 3, jobs: 15, storeDelayMs: 10 },
  ])(
    "runs every job exactly once ($name, $processes processes, $jobs jobs)",
    async ({ processes, jobs, storeDelayMs }) => {
      const { runs, jobs: finished, claimEvents } = await runScenario({ processes, jobs, storeDelayMs });

      // Every job ran at least once (the scenario only ends when all are completed)...
      expect(new Set(runs.map((r) => r.jobId)).size).toBe(jobs);
      // ...and none ran twice.
      expect(ranMoreThanOnce(runs)).toEqual({});
      expect(runs).toHaveLength(jobs);
      // The database agrees: one claim and one attempt per job.
      expect(finished.filter((j) => j.state !== "completed" || j.attempts !== 1).map((j) => `${j.id}: ${j.state}, attempts ${j.attempts}`)).toEqual([]);
      expect([...claimEvents].filter(([, n]) => n !== 1)).toEqual([]);
    },
    60_000,
  );

  it(
    "keeps a queue's concurrency limit across processes",
    async () => {
      const { runs } = await runScenario({ processes: 3, jobs: 9, storeDelayMs: 10, handlerMs: 50, queueConcurrency: 1 });
      expect(new Set(runs.map((r) => r.jobId)).size).toBe(9);
      expect(peakConcurrency(runs)).toBe(1);
    },
    60_000,
  );
});

describe("reapers in several processes", () => {
  // Workers heartbeat every 130ms on a 100ms lease, as a worker whose event loop lags a little
  // does: each lease is expired for about 30ms before the next heartbeat renews it. Two reaper
  // processes, slowed by 150ms per store call, read those expired leases, and the heartbeat
  // then lands between a reaper's read and its write. A reaper may only end a lease that is
  // still exactly as it read it.
  //
  // Three jobs were claimed by a worker that then crashed. Their leases really are abandoned:
  // both reapers see them, and exactly one must requeue each.
  it(
    "reaps only abandoned leases, once each: no double runs, no lost jobs",
    async () => {
      const crashed: string[] = [];
      const { runs, reapers, jobs, leaseExpiredEvents } = await runScenario({
        processes: 2,
        concurrency: 2,
        jobs: 16,
        handlerMs: 800,
        leaseMs: 100,
        heartbeatMs: 130,
        backoff: { type: "fixed", delayMs: 50 },
        reapers: 2,
        reaperStoreDelayMs: 150,
        before: async (conveyor) => {
          const broker = new Broker(conveyor, { leaseMs: 100 });
          for (let i = 0; i < 3; i++) crashed.push((await broker.claim([QUEUE], "crashed-worker"))!.job.id);
        },
      });

      // No lost jobs: every job completed (the processes only stop then, or fail at a deadline).
      expect(jobs.filter((j) => j.state !== "completed").map((j) => `${j.id}: ${j.state}`)).toEqual([]);
      // No double runs: every job's handler started exactly once.
      expect(ranMoreThanOnce(runs)).toEqual({});
      expect(runs).toHaveLength(16);
      // Each crashed lease was ended once, by one of the reapers; no live lease was.
      expect(jobs.map((j) => ({ id: j.id, attempts: j.attempts, leaseExpired: leaseExpiredEvents.get(j.id) }))).toEqual(
        jobs.map((j) => (crashed.includes(j.id) ? { id: j.id, attempts: 2, leaseExpired: 1 } : { id: j.id, attempts: 1, leaseExpired: 0 })),
      );
      expect(reapers).toHaveLength(2);
      expect(reapers.reduce((n, r) => n + r.reaped, 0)).toBe(3);
      // Not vacuous: the reapers did see live, heartbeating leases as expired.
      const sawLive = new Set(reapers.flatMap((r) => r.sawExpired).filter((id) => !crashed.includes(id)));
      expect(sawLive.size).toBeGreaterThan(0);
    },
    60_000,
  );
});
