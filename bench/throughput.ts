// Throughput benchmark: several worker processes drain one queue in a shared SQLite file.
// Opt-in, not part of `npm test`:
//
//   npm run bench -- [--processes 4] [--jobs 5000] [--concurrency 1] [--runs 3]
//
// Each run uses a fresh database with `--jobs` no-op jobs already waiting, starts `--processes`
// child processes (bench/worker.ts, one real Worker each with `--concurrency` loops), releases
// them at the same moment and measures the time until every job is completed. Reports jobs per
// second for each run and the median, and how many handler runs were duplicates (a job run
// more than once).
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { cpus, tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { Conveyor } from "../src/core/conveyor.js";
import { SqliteStore } from "../src/storage/sqlite-store.js";
import type { BenchWorkerOptions } from "./worker.js";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const CHILD = fileURLToPath(new URL("./worker.ts", import.meta.url));
const QUEUE = "bench";

const { values } = parseArgs({
  options: {
    processes: { type: "string", default: "4" },
    jobs: { type: "string", default: "5000" },
    concurrency: { type: "string", default: "1" },
    runs: { type: "string", default: "3" },
  },
});
const positive = (name: string, raw: string | undefined) => {
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 1) throw new Error(`--${name} must be a positive integer`);
  return n;
};
const processes = positive("processes", values.processes);
const jobs = positive("jobs", values.jobs);
const concurrency = positive("concurrency", values.concurrency);
const runs = positive("runs", values.runs);

interface Result {
  seconds: number;
  jobsPerSecond: number;
  handlerRuns: number;
}

async function once(): Promise<Result> {
  const dir = mkdtempSync(join(tmpdir(), "conveyor-bench-"));
  try {
    const db = join(dir, "bench.db");
    const store = new SqliteStore(db);
    const conveyor = new Conveyor({ store });
    await conveyor.init();
    for (let i = 0; i < jobs; i += 1000) {
      await conveyor.enqueueBulk(Array.from({ length: Math.min(1000, jobs - i) }, () => ({ queue: QUEUE, name: "noop" })));
    }

    const opts: BenchWorkerOptions = { queue: QUEUE, total: jobs, concurrency };
    const children = Array.from({ length: processes }, () => {
      const child = spawn(process.execPath, ["--import", "tsx", CHILD, db, JSON.stringify(opts)], {
        cwd: ROOT,
        stdio: ["pipe", "pipe", "pipe"],
      });
      let stdout = "";
      let stderr = "";
      child.stdout!.on("data", (c) => (stdout += c));
      child.stderr!.on("data", (c) => (stderr += c));
      const ready = new Promise<void>((resolve, reject) => {
        child.stdout!.on("data", () => stdout.includes("ready\n") && resolve());
        child.on("exit", (code) => reject(new Error(`bench worker exited (${code}) before it was ready:\n${stderr}`)));
      });
      const exited = new Promise<number>((resolve, reject) =>
        child.on("exit", (code) => {
          const m = /runs (\d+)/.exec(stdout);
          if (code !== 0 || !m) reject(new Error(`bench worker exited with ${code}:\n${stderr}`));
          else resolve(Number(m[1]));
        }),
      );
      return { child, ready, exited };
    });

    await Promise.all(children.map((c) => c.ready));
    const started = performance.now();
    for (const c of children) c.child.stdin!.write("go\n");
    while ((await store.countJobs(QUEUE)).completed < jobs) await new Promise((r) => setTimeout(r, 5));
    const seconds = (performance.now() - started) / 1000;
    const handlerRuns = (await Promise.all(children.map((c) => c.exited))).reduce((a, b) => a + b, 0);
    await store.close();
    return { seconds, jobsPerSecond: jobs / seconds, handlerRuns };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

console.log(`conveyor throughput: ${processes} processes x concurrency ${concurrency}, ${jobs} no-op jobs, ${runs} runs`);
console.log(`node ${process.version}, ${cpus().length} CPUs`);
const results: Result[] = [];
for (let i = 0; i < runs; i++) {
  const r = await once();
  results.push(r);
  console.log(
    `run ${i + 1}: ${r.seconds.toFixed(2)}s, ${Math.round(r.jobsPerSecond)} jobs/s, ` +
      `${r.handlerRuns - jobs} duplicate handler runs`,
  );
}
const sorted = results.map((r) => r.jobsPerSecond).sort((a, b) => a - b);
const median = sorted[Math.floor(sorted.length / 2)]!;
const duplicates = results.reduce((n, r) => n + r.handlerRuns - jobs, 0);
console.log(`median: ${Math.round(median)} jobs/s; duplicate handler runs across all runs: ${duplicates}`);
