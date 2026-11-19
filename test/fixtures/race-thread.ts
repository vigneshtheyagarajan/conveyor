// One database connection in its own thread, for the cross-connection race tests in
// dependency-races.test.ts. It opens its own SqliteStore on the shared file, waits at a
// barrier shared with the other thread, then performs one operation as soon as both are there.
import { parentPort, workerData } from "node:worker_threads";
import { Broker } from "../../src/broker/broker.js";
import { Conveyor } from "../../src/core/conveyor.js";
import type { FlowRequest } from "../../src/core/types.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import { WebhookDispatcher } from "../../src/webhooks/dispatcher.js";

export type RaceOp = { round: number; /** Spin this long after the barrier (µs), to vary who goes first. */ delayUs?: number } & (
  | { op: "complete"; jobId: string; token: string }
  | { op: "enqueue"; dependsOn: string[] }
  | { op: "cancel"; jobId: string }
  | { op: "flow"; request: FlowRequest }
  | { op: "fail"; jobId: string; token: string }
  | { op: "deliver" }
);

export interface RaceResult {
  ok: boolean;
  value?: string;
  error?: string;
  /** Wall-clock interval of the operation, in ms (performance.timeOrigin + now()). */
  start: number;
  end: number;
  /** Flow events this thread's event bus saw during the operation. */
  flowEvents: string[];
}

const { file, barrier } = workerData as { file: string; barrier: SharedArrayBuffer };
const arrived = new Int32Array(barrier);
const conveyor = new Conveyor({ store: new SqliteStore(file) });
const broker = new Broker(conveyor);
let flowEvents: string[] = [];
conveyor.events.on("flow.completed", (o) => flowEvents.push(`flow.completed ${o.flowId}`));
conveyor.events.on("flow.failed", (o) => flowEvents.push(`flow.failed ${o.flowId}`));
let sent = 0;
const dispatcher = new WebhookDispatcher(conveyor, async () => {
  sent++;
  return new Response("ok");
});
const now = () => performance.timeOrigin + performance.now();

parentPort!.postMessage("ready");
parentPort!.on("message", async (msg: RaceOp | "close") => {
  if (msg === "close") {
    await conveyor.store.close();
    parentPort!.close();
    return;
  }
  // Spin rather than Atomics.wait: both threads leave the barrier within microseconds.
  Atomics.add(arrived, 0, 1);
  while (Atomics.load(arrived, 0) < msg.round * 2) {
    /* spin */
  }
  const until = now() + (msg.delayUs ?? 0) / 1000;
  while (now() < until) {
    /* spin */
  }
  flowEvents = [];
  const start = now();
  let result: RaceResult;
  try {
    const value =
      msg.op === "complete"
        ? (await broker.complete(msg.jobId, msg.token)).state
        : msg.op === "fail"
          ? (await broker.fail(msg.jobId, msg.token, "boom", { unrecoverable: true })).state
          : msg.op === "deliver"
            ? String(await dispatcher.deliverFlowOutcomes().then(() => sent))
            : msg.op === "cancel"
          ? (await conveyor.cancel(msg.jobId)).state
          : msg.op === "flow"
            ? (await conveyor.enqueueFlow(msg.request)).id
            : (await conveyor.enqueue({ queue: "kids", name: "child", dependsOn: msg.dependsOn })).id;
    result = { ok: true, value, start, end: now(), flowEvents };
  } catch (err) {
    result = { ok: false, error: String(err), start, end: now(), flowEvents };
  }
  parentPort!.postMessage(result);
});
