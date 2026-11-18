import { describe, expect, it } from "vitest";
import { manualClock, setup } from "../helpers.js";

// Releasing a lease is a clean hand-back (for example on shutdown), not a failure: it must not
// use up one of the job's attempts.
describe("Broker.release", () => {
  it("does not count a released claim as an attempt", async () => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });

    let lease = await broker.claim(["q"], "w1");
    await broker.release(job.id, lease!.token);
    expect(await conveyor.getJob(job.id)).toMatchObject({ state: "waiting", attempts: 0, leaseToken: null });

    lease = await broker.claim(["q"], "w2");
    expect(lease!.job.attempts).toBe(1);
    const dead = await broker.fail(job.id, lease!.token, new Error("real failure"));
    expect(dead).toMatchObject({ state: "dead", attempts: 1, maxAttempts: 1 });
  });

  it("lets a job be released many times and still get all its attempts", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 2, backoff: { type: "fixed", delayMs: 0 } });
    for (let i = 0; i < 5; i++) {
      const lease = await broker.claim(["q"], "w");
      await broker.release(job.id, lease!.token);
    }
    let lease = await broker.claim(["q"], "w");
    expect((await broker.fail(job.id, lease!.token, "first")).state).toBe("delayed");
    await conveyor.store.promoteDelayed(conveyor.clock.now(), 10);
    lease = await broker.claim(["q"], "w");
    expect(await broker.fail(job.id, lease!.token, "second")).toMatchObject({ state: "dead", attempts: 2 });
  });

  it("records the release in the job's history", async () => {
    const { conveyor, broker } = await setup({ clock: manualClock() });
    const job = await conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await broker.claim(["q"], "w");
    await broker.release(job.id, lease!.token);
    expect((await conveyor.jobEvents(job.id)).map((e) => e.type)).toEqual(["enqueued", "claimed", "released"]);
  });
});
