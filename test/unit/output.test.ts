import { afterEach, describe, expect, it, vi } from "vitest";
import { print, table } from "../../src/cli/output.js";

describe("table", () => {
  it("prints (none) for no rows", () => {
    expect(table([], ["id"])).toBe("(none)");
  });

  it("aligns columns under upper-cased headers and trims trailing space", () => {
    const out = table(
      [
        { id: "a1", state: "waiting", attempts: 0 },
        { id: "b", state: "completed", attempts: 12 },
      ],
      ["id", "state", "attempts"],
    );
    expect(out).toBe(["ID  STATE      ATTEMPTS", "a1  waiting    0", "b   completed  12"].join("\n"));
  });

  it("formats nulls, timestamps and objects", () => {
    const out = table([{ a: null, b: undefined, t: Date.UTC(2026, 0, 5, 12), o: { x: 1 }, n: 42 }], ["a", "b", "t", "o", "n"]);
    expect(out.split("\n")[1]).toBe('-  -  2026-01-05T12:00:00Z  {"x":1}  42');
  });

  it("keeps sub-second precision when a timestamp has it", () => {
    expect(table([{ t: Date.UTC(2026, 0, 5, 12) + 5 }], ["t"]).split("\n")[1]).toBe("2026-01-05T12:00:00.005Z");
  });
});

describe("print", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("writes strings as-is and everything else (or --json) as pretty JSON", () => {
    const out: string[] = [];
    vi.spyOn(process.stdout, "write").mockImplementation((chunk) => (out.push(String(chunk)), true));
    print("hello", false);
    print("hello", true);
    print({ a: 1 }, false);
    expect(out).toEqual(["hello\n", '"hello"\n', '{\n  "a": 1\n}\n']);
  });
});
