import { describe, expect, it } from "vitest";
import { JOB_STATES, TERMINAL_STATES } from "../../src/core/types.js";
import { emptyCounts } from "../../src/storage/store.js";

describe("emptyCounts", () => {
  it("has a zero for every job state", () => {
    expect(emptyCounts()).toEqual(Object.fromEntries(JOB_STATES.map((s) => [s, 0])));
  });

  it("returns a fresh object each time", () => {
    const a = emptyCounts();
    a.waiting = 5;
    expect(emptyCounts().waiting).toBe(0);
  });
});

describe("job states", () => {
  it("terminal states are a subset of all states", () => {
    expect(TERMINAL_STATES.every((s) => JOB_STATES.includes(s))).toBe(true);
    expect([...TERMINAL_STATES].sort()).toEqual(["cancelled", "completed", "dead"]);
  });
});
