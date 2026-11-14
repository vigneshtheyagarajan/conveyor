import { Worker as ThreadWorker } from "node:worker_threads";
import { fileURLToPath, pathToFileURL } from "node:url";
import { resolve } from "node:path";
import type { JobHandler } from "./handlers.js";
import type { Json } from "../util/json.js";

// Wrap a CPU-heavy function so it runs in a worker thread
export function threaded(modulePath: string, exportName = "default"): JobHandler {
  const moduleUrl = pathToFileURL(resolve(modulePath)).href;
  return ({ job, signal }) =>
    new Promise<Json>((resolvePromise, reject) => {
      const thread = new ThreadWorker(new URL(import.meta.url), {
        workerData: { moduleUrl, exportName, payload: job.payload },
      });
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
}

const { isMainThread, parentPort, workerData } = await import("node:worker_threads");
if (!isMainThread && workerData?.moduleUrl && parentPort) {
  const port = parentPort;
  try {
    const mod = await import(workerData.moduleUrl);
    const fn = mod[workerData.exportName];
    if (typeof fn !== "function") throw new Error(`${workerData.exportName} is not a function`);
    const value = await fn(workerData.payload);
    port.postMessage({ ok: true, value });
  } catch (err) {
    port.postMessage({ ok: false, error: err instanceof Error ? err.message : String(err) });
  }
}

export const __threadRunnerFile = fileURLToPath(import.meta.url);
