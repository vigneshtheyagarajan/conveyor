import { Worker as ThreadWorker } from "node:worker_threads";
import { fileURLToPath, pathToFileURL } from "node:url";
import { resolve } from "node:path";
import type { JobHandler } from "./handlers.js";
import type { Json } from "../util/json.js";

export interface ThreadedOptions {
  /**
   * Also pass the job's dependencies (what ctx.dependencies() returns) to the function, as its
   * second argument: `fn(payload, dependencies)`. Without it the function is called with the
   * payload only, as before.
   */
  dependencies?: boolean;
}

// Wrap a CPU-heavy function so it runs in a worker thread
export function threaded(modulePath: string, exportName = "default", options: ThreadedOptions = {}): JobHandler {
  const moduleUrl = pathToFileURL(resolve(modulePath)).href;
  return async (ctx) => {
    const { job, signal } = ctx;
    const workerData = options.dependencies
      ? { moduleUrl, exportName, payload: job.payload, dependencies: await ctx.dependencies() }
      : { moduleUrl, exportName, payload: job.payload };
    return new Promise<Json>((resolvePromise, reject) => {
      const thread = new ThreadWorker(new URL(import.meta.url), { workerData });
      const onAbort = () => {
        void thread.terminate();
        reject(signal.reason ?? new Error("aborted"));
      };
      signal.addEventListener("abort", onAbort, { once: true });
      thread.once("message", (msg: { ok: boolean; value?: Json; error?: string }) => {
        signal.removeEventListener("abort", onAbort);
        void thread.terminate();
        if (msg.ok) resolvePromise(msg.value ?? null);
        else reject(new Error(msg.error));
      });
      thread.once("error", (err) => {
        signal.removeEventListener("abort", onAbort);
        reject(err);
      });
      thread.once("exit", (code) => {
        if (code !== 0) reject(new Error(`worker thread exited with code ${code}`));
      });
    });
  };
}

const { isMainThread, parentPort, workerData } = await import("node:worker_threads");
if (!isMainThread && workerData?.moduleUrl && parentPort) {
  const port = parentPort;
  try {
    const mod = await import(workerData.moduleUrl);
    const fn = mod[workerData.exportName];
    if (typeof fn !== "function") throw new Error(`${workerData.exportName} is not a function`);
    // Only functions that opted in get a second argument.
    const value = await ("dependencies" in workerData ? fn(workerData.payload, workerData.dependencies) : fn(workerData.payload));
    port.postMessage({ ok: true, value });
  } catch (err) {
    port.postMessage({ ok: false, error: err instanceof Error ? err.message : String(err) });
  }
}

export const __threadRunnerFile = fileURLToPath(import.meta.url);
