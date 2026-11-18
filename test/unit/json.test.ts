import { describe, expect, it } from "vitest";
import { canonicalJson, safeParse } from "../../src/util/json.js";

describe("safeParse", () => {
  it("parses valid JSON", () => {
    expect(safeParse('{"a":[1,"x",null]}', null)).toEqual({ a: [1, "x", null] });
    expect(safeParse("null", "fallback")).toBeNull();
    expect(safeParse("0", 5)).toBe(0);
  });

  it("returns the fallback for null, undefined, empty and invalid input", () => {
    expect(safeParse(null, "f")).toBe("f");
    expect(safeParse(undefined, "f")).toBe("f");
    expect(safeParse("", "f")).toBe("f");
    expect(safeParse("{nope", "f")).toBe("f");
  });
});

describe("canonicalJson", () => {
  it("sorts object keys at every depth", () => {
    expect(canonicalJson({ b: 1, a: { d: [{ z: 1, y: 2 }], c: null } })).toBe('{"a":{"c":null,"d":[{"y":2,"z":1}]},"b":1}');
  });

  it("gives the same string for objects that differ only in key order", () => {
    expect(canonicalJson({ x: 1, y: { p: 1, q: 2 } })).toBe(canonicalJson({ y: { q: 2, p: 1 }, x: 1 }));
  });

  it("keeps array order and handles scalars", () => {
    expect(canonicalJson([3, 1, 2])).toBe("[3,1,2]");
    expect(canonicalJson("s")).toBe('"s"');
    expect(canonicalJson(7)).toBe("7");
    expect(canonicalJson(null)).toBe("null");
  });

  it("drops undefined properties like JSON.stringify", () => {
    expect(canonicalJson({ b: undefined, a: 1 })).toBe('{"a":1}');
  });
});
