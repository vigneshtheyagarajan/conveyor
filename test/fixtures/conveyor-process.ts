// A separate OS process with its own connection to a shared database file, for the
// multi-process tests in large-scale.test.ts. Started with child_process.fork and driven over
// IPC:
//   { op: "complete-at", at, jobId, token }  complete a leased job at wall-clock time `at`
//   { op: "work", queues, concurrency, log }  run a Worker; every handler run appends a line
//                                            to `log`, and every completion transaction its
//                                            duration
//   { op: "stop" }                            stop the worker, close the store and exit
import { appendFileSync } from "node:fs";
import { Broker } from "../../src/broker/broker.js";
import { Conveyor } from "../../src/core/conveyor.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import { HandlerRegistry } from "../../src/worker/handlers.js";
import { Worker } from "../../src/worker/worker.js";

export type ProcessOp =
  | { op: "complete-at"; at: number; jobId: string; token: string }
  | { op: "work"; queues: string[]; concurrency: number; log: string }
  | { op: "stop" };

export interface ProcessReply {
  ok: boolean;
  value?: string;
  error?: string;
  start?: number;
  end?: number;
}

const file = process.argv[2]!;
const store = new SqliteStore(file, { busyTimeoutMs: 30_000 });
const conveyor = new Conveyor({ store });
const broker = new Broker(conveyor, { leaseMs: 600_000 });
const now = () => performance.timeOrigin + performance.now();
let worker: Worker | undefined;

function handlers(log: string): HandlerRegistry {
  const line = (text: string) => appendFileSync(log, `${text}\n`);
  return new HandlerRegistry()
    .register("root", ({ job }) => {
      line(`run root ${job.id} ${process.pid} ${Date.now()}`);
      return { root: true };
    })
    .register("child", ({ job }) => {
      line(`run child ${job.id} ${process.pid} ${Date.now()}`);
      return { i: (job.payload as { i: number }).i };
    })
    .register("fanin", async ({ job, dependencies }) => {
      const deps = await dependencies();
      const completed = deps.filter((d) => d.state === "completed").length;
      const sum = deps.reduce((total, d) => total + ((d.result as { i: number } | null)?.i ?? 0), 0);
      line(`run fanin ${job.id} ${process.pid} ${Date.now()} ${deps.length} ${completed} ${sum}`);
      return { sum };
    });
}

process.on("message", async (msg: ProcessOp) => {
  if (msg.op === "complete-at") {
    while (Date.now() < msg.at) {
      /* spin until the agreed instant */
    }
    const start = now();
    try {
      const job = await broker.complete(msg.jobId, msg.token);
      process.send!({ ok: true, value: job.state, start, end: now() } satisfies ProcessReply);
    } catch (err) {
      process.send!({ ok: false, error: String(err), start, end: now() } satisfies ProcessReply);
    }
  } else if (msg.op === "work") {
    // Time every completion transaction, with how many dependents it changed.
    const completeJob = store.completeJob.bind(store);
    store.completeJob = async (...args) => {
      const t = now();
      const res = await completeJob(...args);
      appendFileSync(msg.log, `tx ${args[0]} ${(now() - t).toFixed(3)} ${res?.dependents.length ?? -1}\n`);
      return res;
    };
    worker = new Worker(broker, { queues: msg.queues, handlers: handlers(msg.log), concurrency: msg.concurrency, pollMs: 5, maxPollMs: 50 });
    worker.start();
    process.send!({ ok: true } satisfies ProcessReply);
  } else {
    await worker?.stop();
    await store.close();
    process.send!({ ok: true } satisfies ProcessReply);
    process.exit(0);
  }
});

await store.migrate();
process.send!({ ok: true, value: "ready" } satisfies ProcessReply);
