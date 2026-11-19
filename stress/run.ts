// Multi-process stress test. Opt-in, not part of `npm test`:
//
//   npm run stress -- [--seed n] [--workers 4] [--concurrency 4] [--jobs 3000] [--enqueue-ms 15000]
//                     [--kill-every-ms 1000] [--restart-after-ms 500] [--lease-ms 2000]
//                     [--heartbeat-ms 300] [--deadline-ms 180000] [--keep]
//
// Several worker processes, a separate reaper process and an enqueuer share one SQLite file.
// The enqueuer adds jobs (normal, with unique keys, failing and retrying, timing out) while the
// workers run, and worker processes are killed with SIGKILL at random and restarted, so leases
// really expire and get reaped. Every handler run is logged. At the end it checks:
//
//   - every job ended completed or dead: none lost, none stuck
//   - no job completed twice
//   - no job ran twice for the same attempt (two workers holding it at once)
//   - no two runs of the same job overlapped in time
//   - no two jobs with the same unique key were unfinished at the same time
//   - each job's outcome matches how it was designed to behave
//
// The seed decides the job mix and the kill schedule; it is printed so a failing run can be
// repeated (process scheduling still varies between runs). Exits 1 if any check fails.
import { spawn, type ChildProcess } from "node:child_process";
import { randomInt } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { cpus, tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { SqliteStore } from "../src/storage/sqlite-store.js";
import { prng, type JobSpec, type StressLog, type StressOptions } from "./shared.js";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const ROLES = fileURLToPath(new URL("./roles.ts", import.meta.url));

const { values } = parseArgs({
  options: {
    seed: { type: "string" },
    workers: { type: "string", default: "4" },
    concurrency: { type: "string", default: "4" },
    jobs: { type: "string", default: "3000" },
    "enqueue-ms": { type: "string", default: "15000" },
    "kill-every-ms": { type: "string", default: "1000" },
    "restart-after-ms": { type: "string", default: "500" },
    "lease-ms": { type: "string", default: "2000" },
    "heartbeat-ms": { type: "string", default: "300" },
    "deadline-ms": { type: "string", default: "180000" },
    keep: { type: "boolean", default: false },
  },
});
const int = (name: keyof typeof values, min = 0) => {
  const n = Number(values[name]);
  if (!Number.isInteger(n) || n < min) throw new Error(`--${name} must be an integer >= ${min}`);
  return n;
};
const seed = values.seed === undefined ? randomInt(2 ** 31) : int("seed");
const workers = int("workers", 1);
const killEveryMs = int("kill-every-ms");
const restartAfterMs = int("restart-after-ms");
const deadlineMs = int("deadline-ms", 1);
const options: StressOptions = {
  concurrency: int("concurrency", 1),
  leaseMs: int("lease-ms", 1),
  heartbeatMs: int("heartbeat-ms", 1),
  seed,
  jobs: int("jobs", 1),
  enqueueMs: int("enqueue-ms"),
};

console.log(`conveyor stress test, seed ${seed}`);
console.log(
  `  ${workers} workers x concurrency ${options.concurrency}, 1 reaper, ${options.jobs} jobs over ~${options.enqueueMs}ms, ` +
    `kill every ~${killEveryMs || "-"}ms (restart after ${restartAfterMs}ms), lease ${options.leaseMs}ms, heartbeat ${options.heartbeatMs}ms`,
);
const repeat = Object.entries(values)
  .filter(([name, value]) => name !== "seed" && name !== "keep" && value !== undefined)
  .map(([name, value]) => ` --${name} ${value}`)
  .join("");
console.log(`  node ${process.version}, ${cpus().length} CPUs`);
console.log(`  repeat with: npm run stress -- --seed ${seed}${repeat}`);

const dir = mkdtempSync(join(tmpdir(), "conveyor-stress-"));
const db = join(dir, "stress.db");
const setup = new SqliteStore(db);
await setup.migrate();
await setup.close();

// --- processes ---------------------------------------------------------------------------

interface Proc {
  role: string;
  log: string;
  child: ChildProcess;
  ready: Promise<void>;
  exited: Promise<number | null>;
  stderr: () => string;
  killedAt: number | null;
}

let logSeq = 0;
function startProcess(role: "worker" | "reaper" | "enqueuer"): Proc {
  const log = join(dir, `${role}-${++logSeq}.jsonl`);
  const child = spawn(process.execPath, ["--import", "tsx", ROLES, role, db, log, JSON.stringify(options)], {
    cwd: ROOT,
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout!.on("data", (c) => (stdout += c));
  child.stderr!.on("data", (c) => (stderr += c));
  const exited = new Promise<number | null>((resolve) => child.on("exit", (code) => resolve(code)));
  const ready = new Promise<void>((resolve, reject) => {
    child.stdout!.on("data", () => stdout.includes("ready\n") && resolve());
    child.on("exit", (code) => reject(new Error(`${role} exited (${code}) before it was ready:\n${stderr}`)));
  });
  return { role, log, child, ready, exited, stderr: () => stderr, killedAt: null };
}

const random = prng(seed);
const all: Proc[] = [];
const reaper = startProcess("reaper");
const slots: Proc[] = Array.from({ length: workers }, () => startProcess("worker"));
const enqueuer = startProcess("enqueuer");
all.push(reaper, enqueuer, ...slots);
await Promise.all(all.map((p) => p.ready));
const started = Date.now();
for (const p of all) p.child.stdin!.write("go\n");

let enqueuerDone = false;
let stopping = false;
const crashes: string[] = [];
void enqueuer.exited.then((code) => {
  if (code === 0) enqueuerDone = true;
  else crashes.push(`enqueuer exited with ${code}:\n${enqueuer.stderr()}`);
});
const watch = (p: Proc) =>
  void p.exited.then((code) => {
    if (p.killedAt === null && !stopping) crashes.push(`${p.role} (${p.log}) exited with ${code}:\n${p.stderr()}`);
  });
[reaper, ...slots].forEach(watch); // the enqueuer is watched above: it exits when done

// --- run: kill and restart workers until every job has finished -----------------------------

const reader = new DatabaseSync(db);
reader.exec("PRAGMA busy_timeout = 5000");
const unfinished = () =>
  Number((reader.prepare("SELECT COUNT(*) AS n FROM jobs WHERE state IN ('delayed', 'waiting', 'active')").get() as { n: number }).n);

let kills = 0;
let restarts = 0;
let nextKill = killEveryMs > 0 ? started + killEveryMs * (0.5 + random()) : Infinity;
const pendingRestarts: Promise<void>[] = [];
let finishedAt: number | null = null;

while (Date.now() - started < deadlineMs && crashes.length === 0) {
  if (enqueuerDone && unfinished() === 0) {
    finishedAt = Date.now();
    break;
  }
  if (Date.now() >= nextKill) {
    const slot = Math.floor(random() * workers);
    const victim = slots[slot]!;
    if (victim.killedAt === null && victim.child.exitCode === null) {
      victim.killedAt = Date.now();
      victim.child.kill("SIGKILL");
      kills++;
      pendingRestarts.push(
        (async () => {
          await victim.exited;
          await new Promise((r) => setTimeout(r, restartAfterMs));
          if (stopping) return;
          const replacement = startProcess("worker");
          all.push(replacement);
          watch(replacement);
          await replacement.ready;
          replacement.child.stdin!.write("go\n");
          slots[slot] = replacement;
          restarts++;
        })(),
      );
    }
    nextKill = Date.now() + killEveryMs * (0.5 + random());
  }
  await new Promise((r) => setTimeout(r, 50));
}
const elapsedMs = (finishedAt ?? Date.now()) - started;

// --- clean shutdown ----------------------------------------------------------------------

stopping = true;
await Promise.all(pendingRestarts);
for (const p of all) if (p.child.exitCode === null && p.killedAt === null) p.child.stdin!.write("stop\n");
const shutdown = Promise.all(all.map((p) => p.exited));
const forced = await Promise.race([shutdown.then(() => false), new Promise<boolean>((r) => setTimeout(() => r(true), 15_000))]);
if (forced) for (const p of all) if (p.child.exitCode === null) p.child.kill("SIGKILL");

// --- checks ------------------------------------------------------------------------------

let malformed = 0;
const logLines = (p: Proc): StressLog[] =>
  existsSync(p.log)
    ? readFileSync(p.log, "utf8")
        .split("\n")
        .filter(Boolean)
        .flatMap((l) => {
          try {
            return [JSON.parse(l) as StressLog];
          } catch {
            malformed++; // a line cut short by SIGKILL
            return [];
          }
        })
    : [];

interface Run {
  jobId: string;
  attempt: number;
  start: number;
  end: number; // the kill time for a run in a killed process that never logged its end
  killed: boolean;
}
const runs: Run[] = [];
const completions = new Map<string, number>();
const enqueued: Extract<StressLog, { kind: "enqueued" }>[] = [];
for (const p of all) {
  const open = new Map<string, Extract<StressLog, { kind: "start" }>>();
  for (const line of logLines(p)) {
    if (line.kind === "start") open.set(line.run, line);
    else if (line.kind === "end") {
      const s = open.get(line.run);
      if (s) runs.push({ jobId: s.jobId, attempt: s.attempt, start: s.t, end: line.t, killed: false });
      open.delete(line.run);
    } else if (line.kind === "completed") completions.set(line.jobId, (completions.get(line.jobId) ?? 0) + 1);
    else if (line.kind === "enqueued") enqueued.push(line);
  }
  for (const s of open.values()) runs.push({ jobId: s.jobId, attempt: s.attempt, start: s.t, end: p.killedAt ?? Infinity, killed: p.killedAt !== null });
}

interface JobRow {
  id: string;
  queue: string;
  state: string;
  attempts: number;
  max_attempts: number;
  unique_key: string | null;
  payload: string;
  last_error: string | null;
  created_at: number;
  finished_at: number | null;
}
const jobs = reader.prepare("SELECT id, queue, state, attempts, max_attempts, unique_key, payload, last_error, created_at, finished_at FROM jobs").all() as unknown as JobRow[];
const byId = new Map(jobs.map((j) => [j.id, j]));
const eventCounts = new Map<string, number>();
for (const e of reader.prepare("SELECT job_id, type FROM job_events").all() as { job_id: string; type: string }[]) {
  const k = `${e.job_id} ${e.type}`;
  eventCounts.set(k, (eventCounts.get(k) ?? 0) + 1);
}
const events = (jobId: string, type: string) => eventCounts.get(`${jobId} ${type}`) ?? 0;
const errorEvents = reader.prepare("SELECT data FROM job_events WHERE type IN ('retry_scheduled', 'dead')").all() as { data: string }[];
const errorsMatching = (re: RegExp) => errorEvents.filter((e) => re.test(e.data)).length;
const leaseExpiries = (reader.prepare("SELECT COUNT(*) AS n FROM job_events WHERE type = 'lease_expired'").get() as { n: number }).n;
reader.close();

const created = new Set(enqueued.filter((e) => e.created).map((e) => e.jobId));
const problems: Record<string, string[]> = {};
const report = (check: string, items: string[]) => (problems[check] = items);

// Every job the enqueuer created exists and finished.
report("lost jobs (created, but not in the database)", [...created].filter((id) => !byId.has(id)));
report(
  "stuck jobs (not completed or dead)",
  jobs.filter((j) => j.state !== "completed" && j.state !== "dead").map((j) => `${j.id} ${j.state} attempts=${j.attempts}`),
);
// Every enqueue got back a job that holds its key (its own, or an existing one with the same key).
report(
  "enqueues answered with a job that doesn't exist or has another key",
  enqueued.filter((e) => !byId.has(e.jobId) || byId.get(e.jobId)!.unique_key !== e.key).map((e) => `${e.nonce} -> ${e.jobId}`),
);

// No job completed twice: per the database's history, and per the workers' own events.
report(
  "jobs completed more than once",
  jobs.filter((j) => events(j.id, "completed") > 1 || (completions.get(j.id) ?? 0) > 1).map((j) => `${j.id} (${events(j.id, "completed")} completed events, ${completions.get(j.id) ?? 0} completions)`),
);

// No job ran twice for the same attempt: that is two workers holding the same claim.
const perAttempt = new Map<string, number>();
for (const r of runs) perAttempt.set(`${r.jobId} attempt ${r.attempt}`, (perAttempt.get(`${r.jobId} attempt ${r.attempt}`) ?? 0) + 1);
report("duplicate runs (same job, same attempt)", [...perAttempt].filter(([, n]) => n > 1).map(([k, n]) => `${k}: ${n} runs`));

// No two runs of a job overlapped in time.
const runsByJob = new Map<string, Run[]>();
for (const r of runs) runsByJob.set(r.jobId, [...(runsByJob.get(r.jobId) ?? []), r]);
const overlaps: string[] = [];
for (const [jobId, rs] of runsByJob) {
  rs.sort((a, b) => a.start - b.start);
  for (let i = 1; i < rs.length; i++) {
    const prev = rs[i - 1]!;
    if (rs[i]!.start < prev.end) overlaps.push(`${jobId}: attempt ${rs[i]!.attempt} started ${prev.end - rs[i]!.start}ms before attempt ${prev.attempt} ended`);
  }
}
report("overlapping runs of the same job", overlaps);

// Unique keys: no two jobs with the same queue and key were unfinished at the same time.
const byKey = new Map<string, JobRow[]>();
for (const j of jobs) if (j.unique_key) byKey.set(`${j.queue} ${j.unique_key}`, [...(byKey.get(`${j.queue} ${j.unique_key}`) ?? []), j]);
const keyViolations: string[] = [];
for (const [key, js] of byKey) {
  js.sort((a, b) => a.created_at - b.created_at || a.id.localeCompare(b.id));
  for (let i = 1; i < js.length; i++) {
    const prev = js[i - 1]!;
    if (prev.finished_at === null || js[i]!.created_at < prev.finished_at) keyViolations.push(`${key}: ${js[i]!.id} created while ${prev.id} was unfinished`);
  }
}
report("unique key held by two unfinished jobs", keyViolations);

// Outcomes match the design. A job designed to succeed may still die of lease expiries (its
// worker was killed on its last attempts), but not of anything else.
const outcomes: string[] = [];
for (const j of jobs) {
  const spec = JSON.parse(j.payload) as JobSpec;
  const designedToDie = Math.max(spec.failUntil, spec.timeoutUntil) >= j.max_attempts;
  if (designedToDie && j.state === "completed") outcomes.push(`${j.id} (${spec.kind}) completed, but every attempt should fail`);
  if (!designedToDie && j.state === "dead" && !/lease expired/.test(j.last_error ?? "")) outcomes.push(`${j.id} (${spec.kind}) dead: ${j.last_error}`);
}
report("outcomes that don't match the job's design", outcomes);
if (crashes.length > 0) report("processes that crashed (not killed by the test)", crashes);

// --- report ------------------------------------------------------------------------------

const finished = jobs.filter((j) => j.state === "completed" || j.state === "dead").length;
const count = (state: string) => jobs.filter((j) => j.state === state).length;
const deadByLease = jobs.filter((j) => j.state === "dead" && /lease expired/.test(j.last_error ?? "")).length;
console.log("");
console.log(`  enqueue requests     ${enqueued.length} (${created.size} created, ${enqueued.length - created.size} returned an existing job with the same key)`);
console.log(`  jobs                 ${jobs.length}: ${count("completed")} completed, ${count("dead")} dead (${deadByLease} after lease expiries)`);
console.log(`  handler runs         ${runs.length} (${runs.filter((r) => r.killed).length} cut short by a kill)`);
console.log(`  kills / restarts     ${kills} / ${restarts}`);
console.log(`  leases reaped        ${leaseExpiries}`);
console.log(`  timed-out attempts   ${errorsMatching(/timed out/)}`);
console.log(`  planned failures     ${errorsMatching(/planned failure/)}`);
console.log(`  log lines cut short  ${malformed}`);
console.log(`  elapsed              ${(elapsedMs / 1000).toFixed(1)}s${finishedAt === null ? " (deadline reached or a process crashed)" : ""}`);
console.log(`  throughput           ${Math.round(finished / (elapsedMs / 1000))} jobs/s (finished jobs / elapsed, while enqueueing and killing)`);
console.log("");
let failed = false;
for (const [check, items] of Object.entries(problems)) {
  console.log(`  ${items.length === 0 ? "ok  " : "FAIL"} ${check}: ${items.length}`);
  for (const item of items.slice(0, 5)) console.log(`         ${item.split("\n")[0]}`);
  if (items.length > 5) console.log(`         ... and ${items.length - 5} more`);
  if (items.length > 0) failed = true;
}
console.log("");
console.log(failed ? `FAILED (seed ${seed})` : `passed (seed ${seed})`);
if (values.keep || failed) console.log(`  database and logs kept in ${dir}`);
else rmSync(dir, { recursive: true, force: true });
process.exit(failed ? 1 : 0);
