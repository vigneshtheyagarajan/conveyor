import { describe, expect, it } from "vitest";
import { manualClock, setup } from "../helpers.js";

function named() {}

// Anything a handler can throw must be recorded as a failure and go through the normal
// retry path: retry with backoff while attempts remain, then dead.
// Regression (was KB-3): values JSON.stringify can't encode (undefined, functions, symbols)
// could not be stored, so fail() threw and the job stayed active until its lease expired.
const THROWN: [string, unknown, string][] = [
  ["undefined", undefined, "undefined"],
  ["null", null, "null"],
  ["a string", "boom", "boom"],
  ["an empty string", "", ""],
  ["a number", 42, "42"],
  ["a plain object", { code: "E1" }, '{"code":"E1"}'],
  ["a named function", named, "[function named]"],
  ["an anonymous function", () => {}, "[function]"],
  ["a symbol", Symbol("why"), "Symbol(why)"],
  ["an Error", new Error("bad"), "bad"],
];

describe("recording failures for any thrown value", () => {
  it.each(THROWN)("%s is retried with backoff, then dead", async (_label, thrown, message) => {
    const clock = manualClock();
    const { conveyor, broker } = await setup({ clock });
    const job = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 2, backoff: { type: "fixed", delayMs: 1000 } });

    let lease = await broker.claim(["q"], "w1");
    const retrying = await broker.fail(job.id, lease!.token, thrown);
    expect(retrying).toMatchObject({ state: "delayed", attempts: 1, lastError: message, runAt: clock.now() + 1000, leaseToken: null });

    clock.advance(1000);
    await conveyor.store.promoteDelayed(clock.now(), 10);
    lease = await broker.claim(["q"], "w1");
    const dead = await broker.fail(job.id, lease!.token, thrown);
    expect(dead).toMatchObject({ state: "dead", attempts: 2, lastError: message });

    const types = (await conveyor.jobEvents(job.id)).map((e) => e.type);
    expect(types).toEqual(["enqueued", "claimed", "retry_scheduled", "claimed", "dead"]);
  });
  // The same through a running Worker: see worker.test.ts.
});
