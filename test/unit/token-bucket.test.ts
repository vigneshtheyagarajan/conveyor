import { describe, expect, it } from "vitest";
import { TokenBucket } from "../../src/ratelimit/token-bucket.js";
import { ManualClock } from "../../src/util/clock.js";

describe("TokenBucket", () => {
  it("refills over time", () => {
    const clock = new ManualClock();
    const b = new TokenBucket(2, 1000, clock);
    expect(b.tryTake()).toBe(true);
    expect(b.tryTake()).toBe(true);
    expect(b.tryTake()).toBe(false);
    clock.advance(500);
    expect(b.tryTake()).toBe(true);
  });
});
