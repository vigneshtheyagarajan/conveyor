import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ALL_EVENTS, EventBus } from "../../src/core/events.js";
import type { Job } from "../../src/core/types.js";

const job = { id: "j1", queue: "q" } as Job;

describe("EventBus", () => {
  it("delivers arguments to listeners of that event only", () => {
    const bus = new EventBus();
    const completed = vi.fn();
    const enqueued = vi.fn();
    bus.on("job.completed", completed);
    bus.on("job.enqueued", enqueued);
    bus.emit("job.completed", job);
    expect(completed).toHaveBeenCalledWith(job);
    expect(enqueued).not.toHaveBeenCalled();
  });

  it("passes every argument", () => {
    const bus = new EventBus();
    const fn = vi.fn();
    bus.on("job.retrying", fn);
    bus.emit("job.retrying", job, "boom", 250);
    expect(fn).toHaveBeenCalledWith(job, "boom", 250);
  });

  it("returns an unsubscribe function", () => {
    const bus = new EventBus();
    const fn = vi.fn();
    const off = bus.on("queue.paused", fn);
    bus.emit("queue.paused", "q");
    off();
    bus.emit("queue.paused", "q");
    expect(fn).toHaveBeenCalledOnce();
  });

  it("isolates a throwing listener from the emitter and from other listeners", () => {
    const bus = new EventBus();
    const after = vi.fn();
    const onError = vi.fn();
    bus.onListenerError(onError);
    bus.on("job.completed", () => {
      throw new Error("listener broke");
    });
    bus.on("job.completed", after);
    expect(() => bus.emit("job.completed", job)).not.toThrow();
    expect(after).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(expect.objectContaining({ message: "listener broke" }), "job.completed");
  });

  it("swallows listener errors quietly when nobody listens for them", () => {
    const bus = new EventBus();
    bus.on("job.completed", () => {
      throw new Error("x");
    });
    expect(() => bus.emit("job.completed", job)).not.toThrow();
  });

  it("onAny receives every event with its name, and can unsubscribe from all of them", () => {
    const bus = new EventBus();
    const seen: unknown[][] = [];
    const off = bus.onAny((event, ...args) => seen.push([event, ...args]));
    bus.emit("job.enqueued", job);
    bus.emit("schedule.fired", "s1", "j1");
    off();
    bus.emit("job.enqueued", job);
    expect(seen).toEqual([
      ["job.enqueued", job],
      ["schedule.fired", "s1", "j1"],
    ]);
  });

  it("onAny covers every event name", () => {
    const bus = new EventBus();
    const names = new Set<string>();
    bus.onAny((event) => names.add(event));
    for (const e of ALL_EVENTS) bus.emit(e, job as never, "x" as never, 0 as never);
    expect([...names].sort()).toEqual([...ALL_EVENTS].sort());
    expect(new Set(ALL_EVENTS).size).toBe(ALL_EVENTS.length);
  });

  it("allows many listeners without a MaxListenersExceeded warning", () => {
    const bus = new EventBus();
    const warn = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    try {
      for (let i = 0; i < 50; i++) bus.on("job.enqueued", () => {});
      expect(warn).not.toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });
});

// Regression (was KB-10): `async` listeners return a promise that the bus ignored, so a
// rejection became a process-level unhandledRejection (fatal by default since Node 15).
describe("EventBus with async listeners", () => {
  // Route unhandled rejections to a local spy for the duration of each test, instead of
  // vitest's own handler (which would fail the whole run rather than this test).
  let unhandled: ReturnType<typeof vi.fn>;
  let saved: NodeJS.UnhandledRejectionListener[];
  beforeEach(() => {
    saved = process.listeners("unhandledRejection");
    process.removeAllListeners("unhandledRejection");
    unhandled = vi.fn();
    process.on("unhandledRejection", unhandled);
  });
  afterEach(() => {
    process.removeAllListeners("unhandledRejection");
    for (const l of saved) process.on("unhandledRejection", l);
  });

  /** Let pending rejections surface: unhandledRejection fires after the microtask queue drains. */
  const settle = () => new Promise((r) => setImmediate(r));

  it("reports a rejected async listener to onListenerError instead of leaving it unhandled", async () => {
    const bus = new EventBus();
    const onError = vi.fn();
    bus.onListenerError(onError);
    bus.on("job.completed", async () => {
      throw new Error("async listener broke");
    });
    bus.emit("job.completed", job);
    await settle();
    expect(unhandled).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalledWith(expect.objectContaining({ message: "async listener broke" }), "job.completed");
  });

  it("also covers async listeners registered with onAny", async () => {
    const bus = new EventBus();
    const onError = vi.fn();
    bus.onListenerError(onError);
    bus.onAny(async () => {
      throw new Error("any broke");
    });
    bus.emit("queue.paused", "q");
    await settle();
    expect(unhandled).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalledWith(expect.objectContaining({ message: "any broke" }), "queue.paused");
  });

  it("stays quiet when a rejected async listener has no error listener", async () => {
    const bus = new EventBus();
    bus.on("job.completed", () => Promise.reject(new Error("nobody listening")));
    bus.emit("job.completed", job);
    await settle();
    expect(unhandled).not.toHaveBeenCalled();
  });

  it("does not hold up emit or other listeners while an async listener runs", async () => {
    const bus = new EventBus();
    const order: string[] = [];
    bus.on("job.completed", async () => {
      await Promise.resolve();
      order.push("async done");
    });
    bus.on("job.completed", () => order.push("sync"));
    bus.emit("job.completed", job);
    order.push("emit returned");
    await settle();
    expect(order).toEqual(["sync", "emit returned", "async done"]);
  });
});
