import { describe, expect, it } from "vitest";
import { HandlerRegistry } from "../../src/worker/handlers.js";
import { Worker } from "../../src/worker/worker.js";
import { setup, waitFor } from "../helpers.js";

describe("job lifecycle", () => {
  it("runs a job to completion", async () => {
    const { conveyor, broker } = await setup();
    const handlers = new HandlerRegistry().register<{ a: number; b: number }>("add", ({ job }) => ({
      sum: job.payload.a + job.payload.b,
    }));
    const worker = new Worker(broker, { queues: ["math"], handlers, pollMs: 10 });
    worker.start();
    const job = await conveyor.enqueue({ queue: "math", name: "add", payload: { a: 2, b: 3 } });
    await waitFor(async () => (await conveyor.getJob(job.id)).state === "completed");
    expect((await conveyor.getJob(job.id)).result).toEqual({ sum: 5 });
    await worker.stop();
  });

  it("moves a job to dead after maxAttempts", async () => {
    const { conveyor, broker } = await setup();
    const job = await conveyor.enqueue({ queue: "q", name: "boom", maxAttempts: 2 });

    let lease = await broker.claim(["q"], "w1");
    await broker.fail(job.id, lease!.token, new Error("nope"), { retryInMs: 0 });
    await conveyor.store.promoteDelayed(Date.now() + 1, 10);

    lease = await broker.claim(["q"], "w1");
    const dead = await broker.fail(job.id, lease!.token, new Error("nope again"));
    expect(dead.state).toBe("dead");
    expect(dead.attempts).toBe(2);
  });

  it("returns the existing job for a duplicate unique key", async () => {
    const { conveyor } = await setup();
    const a = await conveyor.enqueue({ queue: "q", name: "sync", uniqueKey: "user:1" });
    const b = await conveyor.enqueue({ queue: "q", name: "sync", uniqueKey: "user:1" });
    expect(b.id).toBe(a.id);
  });
});
