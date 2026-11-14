// Producer and worker in one process: npx tsx examples/embedded.ts
import { Broker, Conveyor, HandlerRegistry, Maintenance, SqliteStore, Worker, createLogger } from "../src/index.js";

const conveyor = new Conveyor({ store: new SqliteStore(":memory:"), logger: createLogger("info") });
await conveyor.init();

const handlers = new HandlerRegistry().register<{ n: number }>("square", async ({ job }) => ({
  value: job.payload.n * job.payload.n,
}));

const broker = new Broker(conveyor);
const maintenance = new Maintenance(conveyor);
const worker = new Worker(broker, { queues: ["math"], handlers, concurrency: 4 });
maintenance.start();
worker.start();

const jobs = await conveyor.enqueueBulk([1, 2, 3, 4, 5].map((n) => ({ queue: "math", name: "square", payload: { n } })));

await new Promise<void>((resolve) => {
  let done = 0;
  conveyor.events.on("job.completed", () => {
    if (++done === jobs.length) resolve();
  });
});

for (const { id } of jobs) {
  const job = await conveyor.getJob(id);
  console.log(job.id, job.state, job.result);
}

await worker.stop();
maintenance.stop();
