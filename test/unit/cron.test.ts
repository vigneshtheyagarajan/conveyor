import { describe, expect, it } from "vitest";
import { nextRun, parseCron } from "../../src/scheduler/cron.js";

const at = (iso: string) => Date.parse(iso);

describe("parseCron", () => {
  it("parses steps and lists", () => {
    const c = parseCron("*/15 9,17 * * MON-FRI");
    expect([...c.minutes]).toEqual([0, 15, 30, 45]);
    expect([...c.hours]).toEqual([9, 17]);
    expect([...c.daysOfWeek]).toEqual([1, 2, 3, 4, 5]);
  });

  it("rejects bad expressions", () => {
    expect(() => parseCron("* * *")).toThrow();
    expect(() => parseCron("61 * * * *")).toThrow();
  });
});

describe("nextRun", () => {
  it("finds the next minute boundary", () => {
    expect(nextRun("* * * * *", at("2026-03-10T10:15:30Z"))).toBe(at("2026-03-10T10:16:00Z"));
  });

  it("handles daily schedules in UTC", () => {
    expect(nextRun("@daily", at("2026-03-10T10:15:00Z"))).toBe(at("2026-03-11T00:00:00Z"));
  });
});
