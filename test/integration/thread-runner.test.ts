// These tests start real worker threads: running a handler in another thread is the feature
// being tested. They wait in real time for each thread's reply (tens of milliseconds), because
// fake timers cannot reach into another thread. There are no fixed sleeps.
import { fileURLToPath } from "node:url";
import { isMainThread, threadId } from "node:worker_threads";
import { describe, expect, it } from "vitest";
import type { Job } from "../../src/core/types.js";
import type { JobContext } from "../../src/worker/handlers.js";
import { threaded } from "../../src/worker/thread-runner.js";

const FIXTURE = fileURLToPath(new URL("../fixtures/thread-fns.mjs", import.meta.url));

function ctx(payload: unknown, signal = new AbortController().signal, dependencies: unknown[] = []): JobContext & { loads: number } {
  const c = {
    job: { id: "j1", payload } as Job,
    signal,
    loads: 0,
    dependencies: async () => {
      c.loads++;
      return dependencies;
    },
  };
  return c as unknown as JobContext & { loads: number };
}

describe("threaded()", () => {
  it("runs the default export in a separate thread with the job's payload", async () => {
    expect(isMainThread).toBe(true);
    const result = (await threaded(FIXTURE)(ctx({ n: 21 }))) as { doubled: number; threadId: number };
    expect(result.doubled).toBe(42);
    expect(result.threadId).not.toBe(threadId);
  });

  it("runs a named export, sync or async", async () => {
    expect(await threaded(FIXTURE, "sum")(ctx({ values: [1, 2, 3] }))).toBe(6);
    expect(await threaded(FIXTURE, "later")(ctx("x"))).toBe("later:x");
  });

  it("turns an undefined result into null", async () => {
    expect(await threaded(FIXTURE, "nothing")(ctx(null))).toBeNull();
  });

  it("calls the function with the payload only, unless asked for dependencies", async () => {
    const c = ctx({ n: 1 }, undefined, [{ id: "d1", result: 5 }]);
    expect(await threaded(FIXTURE, "args")(c)).toEqual({ count: 1, received: [{ n: 1 }] });
    expect(await threaded(FIXTURE, "args", {})(c)).toEqual({ count: 1, received: [{ n: 1 }] });
    expect(c.loads).toBe(0); // and doesn't load them
  });

  it("passes ctx.dependencies() as the second argument with { dependencies: true }", async () => {
    const deps = [
      { id: "d1", key: "fetch", name: "f", queue: "q", state: "completed", onFailure: "fail", result: { bytes: 3 }, error: null },
      { id: "d2", key: null, name: "g", queue: "q", state: "dead", onFailure: "continue", result: null, error: "boom" },
    ];
    const c = ctx({ n: 1 }, undefined, deps);
    expect(await threaded(FIXTURE, "args", { dependencies: true })(c)).toEqual({ count: 2, received: [{ n: 1 }, deps] });
    expect(c.loads).toBe(1);
  });

  it("rejects with the thread's error message", async () => {
    await expect(threaded(FIXTURE, "fail")(ctx(null))).rejects.toThrow("thread boom");
    await expect(threaded(FIXTURE, "throwString")(ctx(null))).rejects.toThrow("plain string");
  });

  it("rejects when the export is not a function", async () => {
    await expect(threaded(FIXTURE, "notAFunction")(ctx(null))).rejects.toThrow("notAFunction is not a function");
    await expect(threaded(FIXTURE, "missing")(ctx(null))).rejects.toThrow("missing is not a function");
  });

  it("rejects when the result cannot be sent back", async () => {
    await expect(threaded(FIXTURE, "unclonable")(ctx(null))).rejects.toThrow(/could not be cloned/);
  });

  it("rejects when the module cannot be loaded", async () => {
    await expect(threaded("/no/such/module.mjs")(ctx(null))).rejects.toThrow(/Cannot find module|ERR_MODULE_NOT_FOUND/);
  });

  it("rejects with an error thrown outside the handler's promise", async () => {
    await expect(threaded(FIXTURE, "crashLater")(ctx(null))).rejects.toThrow("uncaught in thread");
  });

  it("rejects when the thread exits with a non-zero code", async () => {
    await expect(threaded(FIXTURE, "exitHard")(ctx(null))).rejects.toThrow("worker thread exited with code 3");
  });

  it("terminates the thread and rejects with the abort reason when the signal aborts", async () => {
    const ac = new AbortController();
    const running = threaded(FIXTURE, "forever")(ctx(null, ac.signal));
    const reason = new Error("job timed out");
    setImmediate(() => ac.abort(reason));
    await expect(running).rejects.toBe(reason);
  });
});
