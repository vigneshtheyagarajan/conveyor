import { describe, expect, it } from "vitest";
import { backoffDelay, DEFAULT_BACKOFF, parseBackoff } from "../../src/util/backoff.js";

describe("backoffDelay", () => {
  it("fixed", () => {
    expect(backoffDelay({ type: "fixed", delayMs: 500 }, 3)).toBe(500);
    expect(backoffDelay({ type: "fixed", delayMs: 500 }, 1)).toBe(500);
  });

  it("linear grows by one step per attempt and caps", () => {
    const s = { type: "linear", stepMs: 300, maxMs: 1000 } as const;
    expect([1, 2, 3, 4, 10].map((a) => backoffDelay(s, a))).toEqual([300, 600, 900, 1000, 1000]);
  });

  it("exponential without jitter doubles and caps", () => {
    const s = { type: "exponential", baseMs: 100, maxMs: 1000, jitter: false } as const;
    expect([1, 2, 3, 4, 5].map((a) => backoffDelay(s, a))).toEqual([100, 200, 400, 800, 1000]);
  });

  it("exponential stays at the cap for very large attempt numbers", () => {
    const s = { type: "exponential", baseMs: 100, maxMs: 1000, jitter: false } as const;
    expect(backoffDelay(s, 60)).toBe(1000);
    expect(backoffDelay(s, 5000)).toBe(1000);
  });

  it("exponential jitter stays between 50% and 100% of the un-jittered delay", () => {
    const s = { type: "exponential", baseMs: 1000, maxMs: 60_000, jitter: true } as const;
    expect(backoffDelay(s, 3, () => 0)).toBe(2000);
    expect(backoffDelay(s, 3, () => 0.5)).toBe(3000);
    expect(backoffDelay(s, 3, () => 0.999999)).toBe(4000);
    for (let attempt = 1; attempt <= 12; attempt++) {
      const raw = Math.min(1000 * 2 ** (attempt - 1), 60_000);
      for (const r of [0, 0.1, 0.37, 0.5, 0.81, 0.999]) {
        const d = backoffDelay(s, attempt, () => r);
        expect(d).toBeGreaterThanOrEqual(raw / 2);
        expect(d).toBeLessThanOrEqual(raw);
      }
    }
  });

  it("exponential jitter never exceeds the cap", () => {
    const s = { type: "exponential", baseMs: 1000, maxMs: 5000, jitter: true } as const;
    expect(backoffDelay(s, 20, () => 0.999999)).toBeLessThanOrEqual(5000);
  });

  it("treats a missing jitter flag as no jitter when calling backoffDelay directly", () => {
    // parseBackoff fills jitter in; a hand-built strategy without it gets no jitter.
    expect(backoffDelay({ type: "exponential", baseMs: 100, maxMs: 1000 }, 2, () => 0)).toBe(200);
  });

  it("the default strategy is exponential from 1s to 10min with jitter", () => {
    expect(DEFAULT_BACKOFF).toEqual({ type: "exponential", baseMs: 1000, maxMs: 600_000, jitter: true });
    expect(backoffDelay(DEFAULT_BACKOFF, 1, () => 0)).toBe(500);
    expect(backoffDelay(DEFAULT_BACKOFF, 50, () => 0.999999)).toBeLessThanOrEqual(600_000);
  });

  // Regression (was KB-5): 0 * 2 ** 1024 is 0 * Infinity = NaN.
  it("exponential with a zero base delay stays at 0 for every attempt", () => {
    const s = { type: "exponential", baseMs: 0, maxMs: 1000, jitter: false } as const;
    for (const attempt of [1, 2, 1024, 1025, 5000]) expect(backoffDelay(s, attempt)).toBe(0);
    const jittered = { ...s, jitter: true };
    expect(backoffDelay(jittered, 5000, () => 0.5)).toBe(0);
  });
});

describe("parseBackoff", () => {
  it("returns undefined for null and undefined", () => {
    expect(parseBackoff(undefined)).toBeUndefined();
    expect(parseBackoff(null)).toBeUndefined();
  });

  it("parses each strategy, keeping only known fields", () => {
    expect(parseBackoff({ type: "fixed", delayMs: 5, extra: 1 })).toEqual({ type: "fixed", delayMs: 5 });
    expect(parseBackoff({ type: "linear", stepMs: 10, maxMs: 100 })).toEqual({ type: "linear", stepMs: 10, maxMs: 100 });
    expect(parseBackoff({ type: "exponential", baseMs: 10, maxMs: 100, jitter: false })).toEqual({
      type: "exponential",
      baseMs: 10,
      maxMs: 100,
      jitter: false,
    });
  });

  it("defaults exponential jitter to true", () => {
    expect(parseBackoff({ type: "exponential", baseMs: 10, maxMs: 100 })).toMatchObject({ jitter: true });
  });

  it("accepts zero delays", () => {
    expect(parseBackoff({ type: "fixed", delayMs: 0 })).toEqual({ type: "fixed", delayMs: 0 });
  });

  it.each([
    ["a string", "fixed"],
    ["a number", 5],
    ["a boolean", true],
  ])("rejects %s", (_label, value) => {
    expect(() => parseBackoff(value)).toThrow(/backoff must be an object/);
  });

  it("parseBackoff rejects unknown types", () => {
    expect(() => parseBackoff({ type: "random" })).toThrow();
  });

  it("rejects unknown or missing types", () => {
    expect(() => parseBackoff({ type: "random" })).toThrow(/unknown backoff type: random/);
    expect(() => parseBackoff({})).toThrow(/unknown backoff type/);
  });

  it.each([
    [{ type: "fixed" }, "delayMs"],
    [{ type: "fixed", delayMs: -1 }, "delayMs"],
    [{ type: "fixed", delayMs: "5" }, "delayMs"],
    [{ type: "fixed", delayMs: Number.NaN }, "delayMs"],
    [{ type: "fixed", delayMs: Number.POSITIVE_INFINITY }, "delayMs"],
    [{ type: "linear", stepMs: 1 }, "maxMs"],
    [{ type: "linear", maxMs: 1 }, "stepMs"],
    [{ type: "exponential", baseMs: 1 }, "maxMs"],
    [{ type: "exponential", maxMs: 1, baseMs: -5 }, "baseMs"],
  ])("rejects %j (bad %s)", (value, field) => {
    expect(() => parseBackoff(value)).toThrow(new RegExp(`backoff\\.${field} must be a non-negative number`));
  });

  // Regression (was KB-4): jitter was coerced with Boolean(), so "false" turned jitter on.
  it("rejects a non-boolean jitter flag instead of coercing it", () => {
    for (const jitter of ["false", 0, 1, "yes", {}]) {
      expect(() => parseBackoff({ type: "exponential", baseMs: 10, maxMs: 100, jitter })).toThrow(
        "backoff.jitter must be a boolean",
      );
    }
  });

  it("treats a null jitter flag like a missing one", () => {
    expect(parseBackoff({ type: "exponential", baseMs: 10, maxMs: 100, jitter: null })).toMatchObject({ jitter: true });
  });
});
