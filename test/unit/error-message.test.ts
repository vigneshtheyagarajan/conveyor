import { describe, expect, it } from "vitest";
import { errorMessage } from "../../src/broker/broker.js";

describe("errorMessage", () => {
  it("uses an Error's message, or its name when the message is empty", () => {
    expect(errorMessage(new Error("boom"))).toBe("boom");
    expect(errorMessage(new TypeError(""))).toBe("TypeError");
  });

  it("passes strings through", () => {
    expect(errorMessage("plain")).toBe("plain");
    expect(errorMessage("")).toBe("");
  });

  it("JSON-encodes other values", () => {
    expect(errorMessage({ code: 42 })).toBe('{"code":42}');
    expect(errorMessage(7)).toBe("7");
    expect(errorMessage(null)).toBe("null");
  });

  it("falls back to String() for values JSON cannot encode", () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    expect(errorMessage(circular)).toBe("[object Object]");
    expect(errorMessage(10n)).toBe("10");
  });

  // Regression (was KB-3): JSON.stringify returns undefined for these, and the failure could
  // not be stored, leaving the job active until its lease expired.
  it.each([
    ["undefined", undefined, "undefined"],
    ["an anonymous function", () => {}, "[function]"],
    ["a named function", function cleanup() {}, "[function cleanup]"],
    ["a symbol", Symbol("s"), "Symbol(s)"],
  ])("returns a string for %s", (_label, value, expected) => {
    expect(errorMessage(value)).toBe(expected);
  });
});
