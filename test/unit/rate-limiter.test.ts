import { describe, expect, it } from "vitest";
import type { QueueSettings } from "../../src/core/types.js";
import { LocalRateLimiter } from "../../src/ratelimit/limiter.js";
import { ManualClock } from "../../src/util/clock.js";

function queue(name: string, rateLimitMax: number, rateLimitIntervalMs: number): QueueSettings {
  return { name, paused: false, concurrency: 0, rateLimitMax, rateLimitIntervalMs, defaultMaxAttempts: 3, createdAt: 0, updatedAt: 0 };
}

async function drain(limiter: LocalRateLimiter, q: QueueSettings, max = 100) {
  let n = 0;
  while (n < max && (await limiter.acquire(q))) n++;
  return n;
}

describe("LocalRateLimiter", () => {
  it("does not limit a queue without a rate limit", async () => {
    const limiter = new LocalRateLimiter(new ManualClock());
    expect(await drain(limiter, queue("q", 0, 0))).toBe(100);
    expect(await drain(limiter, queue("q", 5, 0))).toBe(100);
    expect(await drain(limiter, queue("q", 0, 1000))).toBe(100);
    expect(limiter.retryAfterMs("q")).toBe(0);
  });

  it("allows rateLimitMax claims per interval", async () => {
    const clock = new ManualClock();
    const limiter = new LocalRateLimiter(clock);
    const q = queue("q", 3, 1000);
    expect(await drain(limiter, q)).toBe(3);
    clock.advance(1000);
    expect(await drain(limiter, q)).toBe(3);
  });

  it("keeps a separate bucket per queue", async () => {
    const limiter = new LocalRateLimiter(new ManualClock());
    expect(await drain(limiter, queue("a", 2, 1000))).toBe(2);
    expect(await drain(limiter, queue("b", 2, 1000))).toBe(2);
  });

  it("release gives a token back", async () => {
    const limiter = new LocalRateLimiter(new ManualClock());
    const q = queue("q", 1, 1000);
    expect(await limiter.acquire(q)).toBe(true);
    expect(await limiter.acquire(q)).toBe(false);
    await limiter.release("q");
    expect(await limiter.acquire(q)).toBe(true);
  });

  it("release for an unknown queue is a no-op", async () => {
    await expect(new LocalRateLimiter(new ManualClock()).release("nope")).resolves.toBeUndefined();
  });

  it("retryAfterMs reports the wait for the next token", async () => {
    const clock = new ManualClock();
    const limiter = new LocalRateLimiter(clock);
    const q = queue("q", 2, 1000);
    await drain(limiter, q);
    expect(limiter.retryAfterMs("q")).toBe(500);
    expect(limiter.retryAfterMs("other")).toBe(0);
  });

  it("keeps the same bucket while the settings are unchanged", async () => {
    const limiter = new LocalRateLimiter(new ManualClock());
    await drain(limiter, queue("q", 2, 1000));
    expect(await limiter.acquire({ ...queue("q", 2, 1000), concurrency: 5, paused: true })).toBe(false);
  });

  it("stops limiting once the limit is removed", async () => {
    const limiter = new LocalRateLimiter(new ManualClock());
    await drain(limiter, queue("q", 1, 1000));
    expect(await drain(limiter, queue("q", 0, 0))).toBe(100);
    expect(limiter.retryAfterMs("q")).toBe(0);
  });

  it("defaults to the system clock", async () => {
    expect(await new LocalRateLimiter().acquire(queue("q", 1, 60_000))).toBe(true);
  });

  // Regression (was KB-8): any change to the settings built a new, full bucket.
  it("does not hand out a fresh burst when the limit changes", async () => {
    const limiter = new LocalRateLimiter(new ManualClock());
    expect(await drain(limiter, queue("q", 2, 60_000))).toBe(2);
    expect(await drain(limiter, queue("q", 3, 60_000))).toBe(0);
  });

  it("carries unused tokens over when the limit changes, capped at the new limit", async () => {
    const clock = new ManualClock();
    const limiter = new LocalRateLimiter(clock);
    expect(await drain(limiter, queue("q", 5, 60_000), 1)).toBe(1); // 4 left
    expect(await drain(limiter, queue("q", 2, 60_000))).toBe(2); // lowered: capped at 2
    expect(await drain(limiter, queue("q", 10, 60_000))).toBe(0); // raised: nothing new yet
    clock.advance(6_000); // 10 per minute => 1 token
    expect(await drain(limiter, queue("q", 10, 60_000))).toBe(1);
  });

  it("carries tokens over when only the interval changes", async () => {
    const limiter = new LocalRateLimiter(new ManualClock());
    await drain(limiter, queue("q", 3, 60_000));
    expect(await drain(limiter, queue("q", 3, 1_000))).toBe(0);
  });
});
