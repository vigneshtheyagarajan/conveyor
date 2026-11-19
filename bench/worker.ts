// Child process for bench/throughput.ts. Run with `node --import tsx`.
//
//   worker.ts <db file> <options as JSON>
//
// Runs a real Worker with a no-op handler against the shared database. Prints "ready", waits for
// "go" on stdin, and exits once every job in the queue is completed. On exit it prints
// "runs <n>": how many times the handler ran in this process.
import { createInterface } from "node:readline";
import { Broker } from "../src/broker/broker.js";
import { Conveyor } from "../src/core/conveyor.js";
import { SqliteStore } from "../src/storage/sqlite-store.js";
import { HandlerRegistry } from "../src/worker/handlers.js";
import { Worker } from "../src/worker/worker.js";

export interface BenchWorkerOptions {
  queue: string;
  total: number;
  concurrency: number;
}

const [dbPath, rawOptions] = process.argv.slice(2);
if (!dbPath || !rawOptions) throw new Error("usage: worker.ts <db> <options json>");
const opts = JSON.parse(rawOptions) as BenchWorkerOptions;

const store = new SqliteStore(dbPath);
const conveyor = new Conveyor({ store });
let runs = 0;
const handlers = new HandlerRegistry();
handlers.register("noop", () => {
  runs++;
});
// Default poll settings, as `conveyor worker` uses.
const worker = new Worker(new Broker(conveyor), { queues: [opts.queue], handlers, concurrency: opts.concurrency });

const stdin = createInterface({ input: process.stdin });
const go = new Promise<void>((resolve) => stdin.on("line", (l) => l.trim() === "go" && resolve()));
process.stdout.write("ready\n");
await go;
stdin.close();

worker.start();
while ((await store.countJobs(opts.queue)).completed < opts.total) await new Promise((r) => setTimeout(r, 20));
await worker.stop();
await store.close();
process.stdout.write(`runs ${runs}\n`);
process.exit(0);
