import { describe, expect, it } from "vitest";
import { ValidationError } from "../../src/core/errors.js";
import { asObject, bool, int, NAME_PATTERN, str, strArray } from "../../src/util/validate.js";

describe("asObject", () => {
  it("accepts plain objects", () => {
    const o = { a: 1 };
    expect(asObject(o)).toBe(o);
  });

  it.each([[null], [undefined], [[]], ["str"], [5], [true]])("rejects %j", (v) => {
    expect(() => asObject(v)).toThrow(ValidationError);
    expect(() => asObject(v)).toThrow("body must be an object");
  });

  it("names the value in the message", () => {
    expect(() => asObject(null, "payload")).toThrow("payload must be an object");
  });
});

describe("str", () => {
  it("returns strings and treats null/undefined as missing", () => {
    expect(str({ a: "x" }, "a")).toBe("x");
    expect(str({}, "a")).toBeUndefined();
    expect(str({ a: null }, "a")).toBeUndefined();
    expect(str({ a: "" }, "a")).toBe("");
  });

  it("enforces required", () => {
    expect(() => str({}, "a", { required: true })).toThrow("a is required");
    expect(() => str({ a: null }, "a", { required: true })).toThrow("a is required");
    expect(() => str({ a: "" }, "a", { required: true })).toThrow("a must not be empty");
  });

  it("rejects non-strings", () => {
    expect(() => str({ a: 5 }, "a")).toThrow("a must be a string");
    expect(() => str({ a: ["x"] }, "a")).toThrow(ValidationError);
  });

  it("enforces max length and pattern", () => {
    expect(str({ a: "abc" }, "a", { max: 3 })).toBe("abc");
    expect(() => str({ a: "abcd" }, "a", { max: 3 })).toThrow("a must be at most 3 characters");
    expect(() => str({ a: "a b" }, "a", { pattern: NAME_PATTERN })).toThrow("a has an invalid format");
  });
});

describe("int", () => {
  it("returns integers and treats null/undefined as missing", () => {
    expect(int({ a: 0 }, "a")).toBe(0);
    expect(int({ a: -7 }, "a")).toBe(-7);
    expect(int({}, "a")).toBeUndefined();
    expect(int({ a: null }, "a")).toBeUndefined();
    expect(() => int({}, "a", { required: true })).toThrow("a is required");
  });

  it.each([[1.5], ["1"], [Number.NaN], [Number.POSITIVE_INFINITY], [true]])("rejects %j", (v) => {
    expect(() => int({ a: v }, "a")).toThrow("a must be an integer");
  });

  it("enforces inclusive bounds", () => {
    expect(int({ a: 1 }, "a", { min: 1, max: 3 })).toBe(1);
    expect(int({ a: 3 }, "a", { min: 1, max: 3 })).toBe(3);
    expect(() => int({ a: 0 }, "a", { min: 1 })).toThrow("a must be >= 1");
    expect(() => int({ a: 4 }, "a", { max: 3 })).toThrow("a must be <= 3");
  });
});

describe("bool", () => {
  it("accepts booleans only", () => {
    expect(bool({ a: true }, "a")).toBe(true);
    expect(bool({ a: false }, "a")).toBe(false);
    expect(bool({}, "a")).toBeUndefined();
    expect(bool({ a: null }, "a")).toBeUndefined();
    expect(() => bool({ a: "true" }, "a")).toThrow("a must be a boolean");
    expect(() => bool({ a: 1 }, "a")).toThrow(ValidationError);
  });
});

describe("strArray", () => {
  it("accepts arrays of strings only", () => {
    expect(strArray({ a: ["x", "y"] }, "a")).toEqual(["x", "y"]);
    expect(strArray({ a: [] }, "a")).toEqual([]);
    expect(strArray({}, "a")).toBeUndefined();
    expect(() => strArray({ a: "x" }, "a")).toThrow("a must be an array of strings");
    expect(() => strArray({ a: ["x", 1] }, "a")).toThrow("a must be an array of strings");
  });
});

describe("NAME_PATTERN", () => {
  it.each(["a", "Z9", "email", "send-email", "user.sync", "ns:job_1", "a".repeat(128)])("accepts %j", (name) => {
    expect(NAME_PATTERN.test(name)).toBe(true);
  });

  it.each(["", "-lead", ".lead", "_lead", "has space", "slash/name", "a".repeat(129), "ünïcode", "new\nline"])(
    "rejects %j",
    (name) => {
      expect(NAME_PATTERN.test(name)).toBe(false);
    },
  );
});
