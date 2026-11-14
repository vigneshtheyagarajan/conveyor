import { describe, expect, it } from "vitest";
import { backoffDelay, parseBackoff } from "../../src/util/backoff.js";

describe("backoffDelay", () => {
  it("fixed", () => {
    expect(backoffDelay({ type: "fixed", delayMs: 500 }, 3)).toBe(500);
  });

  it("exponential without jitter doubles and caps", () => {
    const s = { type: "exponential", baseMs: 100, maxMs: 1000, jitter: false } as const;
    expect([1, 2, 3, 4, 5].map((a) => backoffDelay(s, a))).toEqual([100, 200, 400, 800, 1000]);
  });

  it("parseBackoff rejects unknown types", () => {
    expect(() => parseBackoff({ type: "random" })).toThrow();
  });
});
