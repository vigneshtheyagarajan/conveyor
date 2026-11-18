// The half of thread-runner.ts that runs *inside* the worker thread. thread-runner.test.ts
// covers it end to end in real threads, but the coverage tool cannot see code running in
// another thread, so this runs the same code in-process with node:worker_threads mocked.
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";

const FIXTURE = pathToFileURL(fileURLToPath(new URL("../fixtures/thread-fns.mjs", import.meta.url))).href;

async function runInFakeThread(workerData: unknown) {
  const postMessage = vi.fn();
  vi.resetModules();
  vi.doMock("node:worker_threads", async (importOriginal) => ({
    ...(await importOriginal<typeof import("node:worker_threads")>()),
    isMainThread: false,
    parentPort: { postMessage },
    workerData,
  }));
  await import("../../src/worker/thread-runner.js");
  return postMessage.mock.calls.map((c) => c[0]);
}

afterEach(() => {
  vi.doUnmock("node:worker_threads");
  vi.resetModules();
});

describe("thread-runner inside the thread", () => {
  it("posts the export's return value", async () => {
    expect(await runInFakeThread({ moduleUrl: FIXTURE, exportName: "sum", payload: { values: [2, 3] } })).toEqual([{ ok: true, value: 5 }]);
  });

  it("posts the error message when the export throws", async () => {
    expect(await runInFakeThread({ moduleUrl: FIXTURE, exportName: "fail", payload: null })).toEqual([{ ok: false, error: "thread boom" }]);
    expect(await runInFakeThread({ moduleUrl: FIXTURE, exportName: "throwString", payload: null })).toEqual([{ ok: false, error: "plain string" }]);
  });

  it("posts an error when the export is not a function", async () => {
    expect(await runInFakeThread({ moduleUrl: FIXTURE, exportName: "notAFunction", payload: null })).toEqual([
      { ok: false, error: "notAFunction is not a function" },
    ]);
  });

  it("does nothing in a thread that was not started by threaded()", async () => {
    expect(await runInFakeThread(undefined)).toEqual([]);
  });
});
