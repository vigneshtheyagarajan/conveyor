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

  it("starts full", () => {
    const b = new TokenBucket(5, 1000, new ManualClock());
    expect(b.available()).toBe(5);
  });

  it("refills in proportion to elapsed time", () => {
    const clock = new ManualClock();
    const b = new TokenBucket(10, 1000, clock);
    expect(b.tryTake(10)).toBe(true);
    clock.advance(100);
    expect(b.available()).toBeCloseTo(1);
    clock.advance(250);
    expect(b.available()).toBeCloseTo(3.5);
    expect(b.tryTake(4)).toBe(false);
    expect(b.tryTake(3)).toBe(true);
  });

  it("never holds more than its capacity", () => {
    const clock = new ManualClock();
    const b = new TokenBucket(3, 1000, clock);
    clock.advance(60_000);
    expect(b.available()).toBe(3);
    b.giveBack(5);
    expect(b.available()).toBe(3);
  });

  it("lets at most capacity + refill through over a window", () => {
    const clock = new ManualClock();
    const b = new TokenBucket(5, 1000, clock);
    let taken = 0;
    for (let ms = 0; ms < 2000; ms += 10) {
      while (b.tryTake()) taken++;
      clock.advance(10);
    }
    // 5 up front, then 5 per second for 2 seconds (the last refill lands after the loop).
    expect(taken).toBeGreaterThanOrEqual(14);
    expect(taken).toBeLessThanOrEqual(15);
  });

  it("does not take anything when it refuses", () => {
    const b = new TokenBucket(2, 1000, new ManualClock());
    expect(b.tryTake(3)).toBe(false);
    expect(b.available()).toBe(2);
  });

  it("giveBack returns tokens after an abandoned claim", () => {
    const b = new TokenBucket(2, 1000, new ManualClock());
    b.tryTake(2);
    b.giveBack();
    expect(b.tryTake()).toBe(true);
    expect(b.tryTake()).toBe(false);
  });

  it("msUntilAvailable is 0 when a token is available, otherwise the wait for one", () => {
    const clock = new ManualClock();
    const b = new TokenBucket(4, 1000, clock);
    expect(b.msUntilAvailable()).toBe(0);
    b.tryTake(4);
    expect(b.msUntilAvailable()).toBe(250);
    expect(b.msUntilAvailable(2)).toBe(500);
    clock.advance(100);
    expect(b.msUntilAvailable()).toBe(150);
  });

  it("msUntilAvailable is accurate: waiting that long is enough", () => {
    const clock = new ManualClock();
    const b = new TokenBucket(3, 700, clock);
    for (let i = 0; i < 20; i++) {
      while (b.tryTake()) {
        /* drain */
      }
      const wait = b.msUntilAvailable();
      expect(wait).toBeGreaterThan(0);
      clock.advance(wait - 1);
      expect(b.tryTake()).toBe(false);
      clock.advance(1);
      expect(b.tryTake()).toBe(true);
    }
  });

  it("ignores a clock that goes backwards", () => {
    const clock = new ManualClock(10_000);
    const b = new TokenBucket(2, 1000, clock);
    b.tryTake(2);
    clock.advance(-5000);
    expect(b.available()).toBe(0);
    clock.advance(5500);
    expect(b.available()).toBeCloseTo(1);
  });

  it.each([
    [0, 1000],
    [-1, 1000],
    [1, 0],
    [1, -5],
  ])("rejects capacity %i / interval %i", (capacity, interval) => {
    expect(() => new TokenBucket(capacity, interval, new ManualClock())).toThrow(RangeError);
  });
});

describe("TokenBucket initial tokens", () => {
  it("can start partly full, capped to [0, capacity]", () => {
    const clock = new ManualClock();
    expect(new TokenBucket(5, 1000, clock, 2).available()).toBe(2);
    expect(new TokenBucket(5, 1000, clock, 9).available()).toBe(5);
    expect(new TokenBucket(5, 1000, clock, -1).available()).toBe(0);
  });
});
