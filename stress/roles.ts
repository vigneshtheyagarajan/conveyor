// Child processes for stress/run.ts. Run with `node --import tsx`.
//
//   roles.ts <worker|reaper|enqueuer> <db file> <log file> <options as JSON>
//
// Each process opens its own connection to the shared database, prints "ready", and starts
// when it reads "go" on stdin. Workers and the reaper run until they read "stop" (then shut
// down cleanly) or are killed; the enqueuer exits when it has enqueued everything.
//
// Only uses APIs that existed before the concurrency work, so the same script can be run
// against the baseline commit.
import { appendFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { Broker } from "../src/broker/broker.js";
import { Maintenance } from "../src/broker/maintenance.js";
import { Conveyor } from "../src/core/conveyor.js";
import type { EnqueueRequest } from "../src/core/types.js";
import { SqliteStore } from "../src/storage/sqlite-store.js";
import { sleep } from "../src/util/clock.js";
import type { Json } from "../src/util/json.js";
import { HandlerRegistry } from "../src/worker/handlers.js";
import { Worker } from "../src/worker/worker.js";
import { prng, QUEUE, type JobSpec, type StressLog, type StressOptions } from "./shared.js";

const [role, dbPath, logFile, rawOptions] = process.argv.slice(2);
if (!role || !dbPath || !logFile || !rawOptions) throw new Error("usage: roles.ts <role> <db> <log> <options json>");
const opts = JSON.parse(rawOptions) as StressOptions;
const log = (line: StressLog) => appendFileSync(logFile, JSON.stringify(line) + "\n");

const store = new SqliteStore(dbPath);
const conveyor = new Conveyor({ store, config: { leaseMs: opts.leaseMs, heartbeatMs: opts.heartbeatMs, retentionMs: 0 } });

const commands = createInterface({ input: process.stdin });
const command = (name: string) => new Promise<void>((resolve) => commands.on("line", (l) => l.trim() === name && resolve()));
const go = command("go");
const stop = command("stop");

if (role === "worker") {
  let runs = 0;
  const handlers = new HandlerRegistry().register<JobSpec>("stress", async ({ job, signal }) => {
    const spec = job.payload;
    const run = `${process.pid}-${++runs}`;
    log({ kind: "start", run, jobId: job.id, attempt: job.attempts, pid: process.pid, t: Date.now() });
    let ok = false;
    try {
      if (job.attempts <= spec.timeoutUntil) await sleep(spec.stuckMs, spec.honorSignal ? signal : undefined);
      else await sleep(spec.workMs);
      if (job.attempts <= spec.failUntil) throw new Error(`planned failure on attempt ${job.attempts}`);
      ok = true;
      return { attempt: job.attempts } as Json;
    } finally {
      log({ kind: "end", run, t: Date.now(), ok });
    }
  });
  conveyor.events.on("job.completed", (j) => log({ kind: "completed", jobId: j.id, pid: process.pid, t: Date.now() }));
  const worker = new Worker(new Broker(conveyor), {
    queues: [QUEUE],
    handlers,
    concurrency: opts.concurrency,
    pollMs: 5,
    maxPollMs: 50,
    heartbeatMs: opts.heartbeatMs,
    shutdownTimeoutMs: 5000,
  });
  process.stdout.write("ready\n");
  await go;
  worker.start();
  await stop;
  await worker.stop();
} else if (role === "reaper") {
  const maintenance = new Maintenance(conveyor, { reaperIntervalMs: 100, promoteIntervalMs: 50 });
  process.stdout.write("ready\n");
  await go;
  maintenance.start();
  await stop;
  maintenance.stop();
} else if (role === "enqueuer") {
  // Its own stream of the seed, so the job mix doesn't depend on how many kills happen.
  const random = prng(opts.seed ^ 0x5eed);
  const pick = (n: number) => Math.floor(random() * n);
  let nonce = 0;
  const request = (): { req: EnqueueRequest; key: string | null } => {
    const r = random();
    const kind: JobSpec["kind"] = r < 0.5 ? "normal" : r < 0.7 ? "unique" : r < 0.85 ? "failing" : "timeout";
    const spec: JobSpec = {
      nonce: `${process.pid}-${++nonce}`,
      kind,
      // Jobs with a unique key run longer, so their key is often still held when the next
      // request for it arrives.
      workMs: kind === "unique" ? 100 + pick(300) : 5 + pick(45),
      // 1-5 planned failures with 4 attempts: some recover, some (4-5) end dead.
      failUntil: kind === "failing" ? 1 + pick(5) : 0,
      // Mostly one timed-out attempt, sometimes every attempt (dead).
      timeoutUntil: kind === "timeout" ? [1, 1, 2, 4][pick(4)]! : 0,
      stuckMs: 400,
      honorSignal: random() < 0.5,
    };
    const key = kind === "unique" ? `key-${pick(10)}` : null;
    const req: EnqueueRequest = {
      queue: QUEUE,
      name: "stress",
      payload: spec as unknown as Json,
      maxAttempts: 4,
      backoff: { type: "fixed", delayMs: 100 },
      ...(kind === "timeout" ? { timeoutMs: 150 } : {}),
      ...(key ? { uniqueKey: key } : {}),
    };
    return { req, key };
  };
  process.stdout.write("ready\n");
  await go;
  const pause = (opts.enqueueMs * 10) / opts.jobs; // average batch is about 10 jobs
  for (let sent = 0; sent < opts.jobs; ) {
    const batch = Array.from({ length: Math.min(1 + pick(20), opts.jobs - sent) }, request);
    sent += batch.length;
    const jobs =
      random() < 0.5 ? await conveyor.enqueueBulk(batch.map((b) => b.req)) : await Promise.all(batch.map((b) => conveyor.enqueue(b.req)));
    jobs.forEach((job, i) => {
      const mine = (batch[i]!.req.payload as unknown as JobSpec).nonce;
      log({ kind: "enqueued", nonce: mine, jobId: job.id, created: (job.payload as unknown as JobSpec).nonce === mine, key: batch[i]!.key });
    });
    await sleep(pause * (0.5 + random()));
  }
}

commands.close();
await store.close();
process.exit(0);
