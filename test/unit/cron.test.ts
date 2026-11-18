import { describe, expect, it } from "vitest";
import { CronParseError, isValidTimezone, nextRun, parseCron, tzOffsetMs, upcomingRuns } from "../../src/scheduler/cron.js";

const at = (iso: string) => Date.parse(iso);
const iso = (ms: number) => new Date(ms).toISOString();
/** Consecutive runs starting after `from`, as ISO strings (easier to read in failures). */
const runs = (expr: string, from: string, tz: string, count: number) =>
  upcomingRuns(expr, at(from), tz, count).map(iso);

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

  it("expands * to the full range of each field", () => {
    const c = parseCron("* * * * *");
    expect(c.minutes.size).toBe(60);
    expect(c.hours.size).toBe(24);
    expect([...c.daysOfMonth]).toEqual(Array.from({ length: 31 }, (_, i) => i + 1));
    expect([...c.months]).toEqual(Array.from({ length: 12 }, (_, i) => i + 1));
    expect([...c.daysOfWeek]).toEqual([0, 1, 2, 3, 4, 5, 6]);
  });

  it("supports stepped ranges and a step from a single start value", () => {
    expect([...parseCron("10-50/20 * * * *").minutes]).toEqual([10, 30, 50]);
    expect([...parseCron("5/20 * * * *").minutes]).toEqual([5, 25, 45]);
    expect([...parseCron("* * * * */7").daysOfWeek]).toEqual([0]);
    expect([...parseCron("*/100 * * * *").minutes]).toEqual([0]);
  });

  it("accepts month and weekday names case-insensitively, including in ranges and steps", () => {
    const c = parseCron("0 0 * jan-mar,Dec sun,Wed-fri");
    expect([...c.months]).toEqual([1, 2, 3, 12]);
    expect([...c.daysOfWeek].sort()).toEqual([0, 3, 4, 5]);
    expect([...parseCron("0 0 * * MON/2").daysOfWeek]).toEqual([1, 3, 5]);
  });

  it("treats day-of-week 7 as Sunday", () => {
    expect([...parseCron("0 0 * * 7").daysOfWeek]).toEqual([0]);
    expect([...parseCron("0 0 * * 5-7").daysOfWeek].sort()).toEqual([0, 5, 6]);
    expect([...parseCron("0 0 * * 0-7").daysOfWeek].sort()).toEqual([0, 1, 2, 3, 4, 5, 6]);
  });

  it("expands macros, case-insensitively and ignoring surrounding whitespace", () => {
    const same = (a: string, b: string) => {
      const x = parseCron(a);
      const y = parseCron(b);
      expect([x.minutes, x.hours, x.daysOfMonth, x.months, x.daysOfWeek]).toEqual([
        y.minutes,
        y.hours,
        y.daysOfMonth,
        y.months,
        y.daysOfWeek,
      ]);
    };
    same("@yearly", "0 0 1 1 *");
    same("@annually", "0 0 1 1 *");
    same("@monthly", "0 0 1 * *");
    same("@weekly", "0 0 * * 0");
    same("@daily", "0 0 * * *");
    same("@midnight", "0 0 * * *");
    same("@hourly", "0 * * * *");
    same("  @DAILY ", "0 0 * * *");
    expect(parseCron("@daily").source).toBe("@daily");
  });

  it("tolerates extra whitespace between fields", () => {
    expect([...parseCron(" 5\t 4  *  * * ").minutes]).toEqual([5]);
  });

  it.each([
    ["", "no fields"],
    ["* * *", "too few fields"],
    ["* * * * * *", "six fields (seconds are not supported)"],
    ["60 * * * *", "minute out of range"],
    ["* 24 * * *", "hour out of range"],
    ["* * 0 * *", "day-of-month 0"],
    ["* * 32 * *", "day-of-month out of range"],
    ["* * * 0 *", "month 0"],
    ["* * * 13 *", "month out of range"],
    ["* * * * 8", "day-of-week out of range"],
    ["30-10 * * * *", "backwards range"],
    ["* * * * FRI-MON", "backwards named range"],
    ["*/0 * * * *", "zero step"],
    ["*/ * * * *", "empty step"],
    ["*/x * * * *", "non-numeric step"],
    ["1,,2 * * * *", "empty list item"],
    ["1, * * * *", "trailing comma"],
    ["* * * FOO *", "unknown month name"],
    ["* * * * MONDAY", "unknown day name"],
    ["* * L * *", "Quartz L"],
    ["* * ? * *", "Quartz ?"],
    ["-5 * * * *", "range without a start"],
    ["5- * * * *", "range without an end"],
    ["*-5 * * * *", "star as a range bound"],
    ["+5 * * * *", "signed number"],
    ["@reboot", "unsupported macro"],
    ["@every 5m", "interval syntax"],
  ])("rejects %j (%s)", (expr) => {
    expect(() => parseCron(expr)).toThrow(CronParseError);
  });

  // Previously "1-5-9" parsed as 1-5 and "1/5/7" as 1/5: the extra parts were silently dropped.
  it.each([
    ["1-5-9 * * * *", "a range with three bounds"],
    ["1/5/7 * * * *", "two steps"],
    ["1-5/2/3 * * * *", "a stepped range with two steps"],
  ])("rejects %j (%s) instead of silently ignoring the extra part", (expr) => {
    expect(() => parseCron(expr)).toThrow(CronParseError);
  });

  it("names the offending expression in the error", () => {
    expect(() => parseCron("61 * * * *")).toThrow(/"61 \* \* \* \*".*out of range for minute/);
  });
});

describe("day-of-month / day-of-week (Vixie cron rules)", () => {
  // Vixie cron: a field that starts with "*" is unrestricted, even with a step ("*/2").
  // If either day field is unrestricted, a day must match both; if both are restricted,
  // matching either is enough.
  it("treats a day field that starts with * as unrestricted", () => {
    expect(parseCron("0 0 * * *").domRestricted).toBe(false);
    expect(parseCron("0 0 */2 * *").domRestricted).toBe(false);
    expect(parseCron("0 0 * * */2").dowRestricted).toBe(false);
    expect(parseCron("0 0 1-31 * *").domRestricted).toBe(true);
    expect(parseCron("0 0 * * 0-6").dowRestricted).toBe(true);
  });

  it("with a stepped-star day-of-month and a weekday, both must match", () => {
    // Odd days of the month that are also Mondays. Mondays in March 2026: 2, 9, 16, 23, 30.
    expect(runs("0 0 */2 * MON", "2026-03-01T00:00:00Z", "UTC", 3)).toEqual([
      "2026-03-09T00:00:00.000Z",
      "2026-03-23T00:00:00.000Z",
      "2026-04-13T00:00:00.000Z",
    ]);
  });

  it("with a stepped-star weekday and a restricted day-of-month, both must match", () => {
    // The 1st of a month that falls on Sun/Tue/Thu/Sat (*/2 = 0,2,4,6).
    // 2026: Feb 1 is Sun, Mar 1 Sun, Apr 1 Wed, May 1 Fri, Jun 1 Mon, Jul 1 Wed, Aug 1 Sat.
    expect(runs("0 0 1 * */2", "2026-01-15T00:00:00Z", "UTC", 3)).toEqual([
      "2026-02-01T00:00:00.000Z",
      "2026-03-01T00:00:00.000Z",
      "2026-08-01T00:00:00.000Z",
    ]);
  });

  it("with both day fields restricted, either may match", () => {
    // The 13th, or any Friday. March 2026: Fri 6, Fri 13, Fri 20.
    expect(runs("0 0 13 * FRI", "2026-03-01T00:00:00Z", "UTC", 3)).toEqual([
      "2026-03-06T00:00:00.000Z",
      "2026-03-13T00:00:00.000Z",
      "2026-03-20T00:00:00.000Z",
    ]);
  });

  it("with only day-of-month restricted, weekdays are ignored", () => {
    expect(runs("0 0 */10 * *", "2026-01-01T00:00:00Z", "UTC", 4)).toEqual([
      "2026-01-11T00:00:00.000Z",
      "2026-01-21T00:00:00.000Z",
      "2026-01-31T00:00:00.000Z",
      "2026-02-01T00:00:00.000Z",
    ]);
  });

  it("with only day-of-week restricted, days of the month are ignored", () => {
    expect(runs("0 0 * * MON", "2026-03-01T00:00:00Z", "UTC", 2)).toEqual([
      "2026-03-02T00:00:00.000Z",
      "2026-03-09T00:00:00.000Z",
    ]);
  });
});

describe("nextRun in UTC", () => {
  it("finds the next minute boundary", () => {
    expect(nextRun("* * * * *", at("2026-03-10T10:15:30Z"))).toBe(at("2026-03-10T10:16:00Z"));
  });

  it("is strictly after `after`, even when `after` is exactly a run time", () => {
    expect(nextRun("* * * * *", at("2026-03-10T10:15:00Z"))).toBe(at("2026-03-10T10:16:00Z"));
    expect(nextRun("0 10 * * *", at("2026-03-10T10:00:00Z"))).toBe(at("2026-03-11T10:00:00Z"));
    expect(nextRun("0 10 * * *", at("2026-03-10T09:59:59.999Z"))).toBe(at("2026-03-10T10:00:00Z"));
  });

  it("handles daily schedules in UTC", () => {
    expect(nextRun("@daily", at("2026-03-10T10:15:00Z"))).toBe(at("2026-03-11T00:00:00Z"));
  });

  it("accepts a pre-parsed expression", () => {
    expect(nextRun(parseCron("30 * * * *"), at("2026-03-10T10:15:00Z"))).toBe(at("2026-03-10T10:30:00Z"));
  });

  it("rolls over hours, days, months and years", () => {
    expect(nextRun("0 * * * *", at("2026-03-10T23:30:00Z"))).toBe(at("2026-03-11T00:00:00Z"));
    expect(nextRun("0 0 1 * *", at("2026-12-15T00:00:00Z"))).toBe(at("2027-01-01T00:00:00Z"));
    expect(nextRun("@yearly", at("2026-01-01T00:00:00Z"))).toBe(at("2027-01-01T00:00:00Z"));
  });

  it("skips months that do not have the requested day", () => {
    expect(runs("0 0 31 * *", "2026-01-31T00:00:00Z", "UTC", 3)).toEqual([
      "2026-03-31T00:00:00.000Z",
      "2026-05-31T00:00:00.000Z",
      "2026-07-31T00:00:00.000Z",
    ]);
  });

  it("finds 29 February in the next leap year", () => {
    expect(nextRun("0 0 29 2 *", at("2026-03-01T00:00:00Z"))).toBe(at("2028-02-29T00:00:00Z"));
  });

  it("throws when the expression can never match", () => {
    expect(() => nextRun("0 0 30 2 *", at("2026-01-01T00:00:00Z"))).toThrow(/no run time found/);
    expect(() => nextRun("0 0 31 4,6,9,11 *", at("2026-01-01T00:00:00Z"))).toThrow(/no run time found/);
  });

  it("upcomingRuns returns consecutive, strictly increasing runs", () => {
    expect(runs("*/20 * * * *", "2026-03-10T10:05:00Z", "UTC", 4)).toEqual([
      "2026-03-10T10:20:00.000Z",
      "2026-03-10T10:40:00.000Z",
      "2026-03-10T11:00:00.000Z",
      "2026-03-10T11:20:00.000Z",
    ]);
    expect(upcomingRuns("* * * * *", at("2026-03-10T10:05:00Z"))).toHaveLength(5);
  });
});

describe("nextRun in timezones without DST", () => {
  it("applies half-hour and 45-minute offsets", () => {
    // Kolkata is UTC+5:30, Kathmandu UTC+5:45.
    expect(nextRun("0 9 * * *", at("2026-06-01T00:00:00Z"), "Asia/Kolkata")).toBe(at("2026-06-01T03:30:00Z"));
    expect(nextRun("0 9 * * *", at("2026-06-01T00:00:00Z"), "Asia/Kathmandu")).toBe(at("2026-06-01T03:15:00Z"));
  });

  it("uses the local date, not the UTC date, for day fields", () => {
    // 2026-03-09T22:00Z is Monday in UTC but already 07:00 on Tuesday 10 March in Tokyo (UTC+9),
    // so Tokyo's next Tuesday 06:00 is a week away while UTC's is the next morning.
    expect(nextRun("0 6 * * TUE", at("2026-03-09T22:00:00Z"), "Asia/Tokyo")).toBe(at("2026-03-16T21:00:00Z"));
    expect(nextRun("0 6 * * TUE", at("2026-03-09T22:00:00Z"), "UTC")).toBe(at("2026-03-10T06:00:00Z"));
  });
});

// 2026 transitions used below:
//   America/New_York  2026-03-08 02:00 EST -> 03:00 EDT (07:00Z); 2026-11-01 02:00 EDT -> 01:00 EST (06:00Z)
//   Europe/London     2026-03-29 01:00 GMT -> 02:00 BST (01:00Z); 2026-10-25 02:00 BST -> 01:00 GMT (01:00Z)
//   Australia/Sydney  2026-04-05 03:00 AEDT -> 02:00 AEST (16:00Z on the 4th); 2026-10-04 02:00 -> 03:00 (16:00Z on the 3rd)
describe("nextRun across daylight-saving changes", () => {
  it("keeps a daily job at the same wall-clock time after clocks go forward", () => {
    // 09:00 EDT = 13:00Z. Using the pre-change offset (EST) gives 14:00Z, i.e. 10:00 local.
    expect(iso(nextRun("0 9 * * *", at("2026-03-07T17:00:00Z"), "America/New_York"))).toBe("2026-03-08T13:00:00.000Z");
  });

  it("keeps a daily job at the same wall-clock time after clocks go back", () => {
    // 09:00 EST = 14:00Z.
    expect(iso(nextRun("0 9 * * *", at("2026-10-31T17:00:00Z"), "America/New_York"))).toBe("2026-11-01T14:00:00.000Z");
  });

  it("keeps a daily job on local time across a whole season", () => {
    expect(runs("0 9 * * *", "2026-03-06T12:00:00Z", "America/New_York", 4)).toEqual([
      "2026-03-06T14:00:00.000Z",
      "2026-03-07T14:00:00.000Z",
      "2026-03-08T13:00:00.000Z",
      "2026-03-09T13:00:00.000Z",
    ]);
  });

  it("handles the southern hemisphere, where DST ends in April", () => {
    // 09:00 AEDT (UTC+11) = 22:00Z the day before; 09:00 AEST (UTC+10) = 23:00Z the day before.
    expect(runs("0 9 * * *", "2026-04-03T12:00:00Z", "Australia/Sydney", 3)).toEqual([
      "2026-04-03T22:00:00.000Z",
      "2026-04-04T23:00:00.000Z",
      "2026-04-05T23:00:00.000Z",
    ]);
  });

  describe("fixed-time jobs (minute and hour fields do not start with *)", () => {
    it("run once, at the moment clocks jump, when their time is skipped", () => {
      // 02:30 does not exist on 2026-03-08 in New York; the run happens at 03:00 EDT (07:00Z).
      expect(runs("30 2 * * *", "2026-03-07T12:00:00Z", "America/New_York", 3)).toEqual([
        "2026-03-08T07:00:00.000Z",
        "2026-03-09T06:30:00.000Z",
        "2026-03-10T06:30:00.000Z",
      ]);
    });

    it("run once when several of their times fall in the skipped hour", () => {
      expect(runs("0,30 2 * * *", "2026-03-08T06:00:00Z", "America/New_York", 2)).toEqual([
        "2026-03-08T07:00:00.000Z",
        "2026-03-09T06:00:00.000Z",
      ]);
    });

    it("run once, at the first occurrence, when their time happens twice", () => {
      // 01:30 happens at 05:30Z (EDT) and again at 06:30Z (EST) on 2026-11-01.
      expect(runs("30 1 * * *", "2026-10-31T12:00:00Z", "America/New_York", 3)).toEqual([
        "2026-11-01T05:30:00.000Z",
        "2026-11-02T06:30:00.000Z",
        "2026-11-03T06:30:00.000Z",
      ]);
    });

    it("do not run again in the repeated hour when asked from inside it", () => {
      // Asked at 06:10Z (01:10 EST, second pass): 01:30 has already happened today.
      expect(iso(nextRun("30 1 * * *", at("2026-11-01T06:10:00Z"), "America/New_York"))).toBe(
        "2026-11-02T06:30:00.000Z",
      );
    });

    it("handle a skipped midnight (America/Santiago starts DST at 00:00)", () => {
      // 2026-09-06 00:00 (UTC-4) jumps to 01:00 (UTC-3) at 04:00Z.
      expect(runs("0 0 * * *", "2026-09-05T12:00:00Z", "America/Santiago", 2)).toEqual([
        "2026-09-06T04:00:00.000Z",
        "2026-09-07T03:00:00.000Z",
      ]);
    });
  });

  describe("interval jobs (minute or hour field starts with *)", () => {
    it("keep their real-time rhythm when clocks go back", () => {
      expect(runs("0 * * * *", "2026-11-01T04:30:00Z", "America/New_York", 4)).toEqual([
        "2026-11-01T05:00:00.000Z", // 01:00 EDT
        "2026-11-01T06:00:00.000Z", // 01:00 EST
        "2026-11-01T07:00:00.000Z", // 02:00 EST
        "2026-11-01T08:00:00.000Z",
      ]);
      expect(runs("*/30 * * * *", "2026-11-01T05:15:00Z", "America/New_York", 4)).toEqual([
        "2026-11-01T05:30:00.000Z",
        "2026-11-01T06:00:00.000Z",
        "2026-11-01T06:30:00.000Z",
        "2026-11-01T07:00:00.000Z",
      ]);
    });

    it("find the repeated hour even when asked just before the clocks go back", () => {
      // 05:50Z is 01:50 EDT; the next quarter hour is 01:00 EST (06:00Z), not 02:00 EST.
      expect(iso(nextRun("*/15 * * * *", at("2026-11-01T05:50:00Z"), "America/New_York"))).toBe(
        "2026-11-01T06:00:00.000Z",
      );
    });

    it("skip wall-clock times that do not exist when clocks go forward", () => {
      expect(runs("0 * * * *", "2026-03-08T05:30:00Z", "America/New_York", 3)).toEqual([
        "2026-03-08T06:00:00.000Z", // 01:00 EST
        "2026-03-08T07:00:00.000Z", // 03:00 EDT (02:00 never happens)
        "2026-03-08T08:00:00.000Z",
      ]);
    });

    it("handle Europe/London, which changes at 01:00 UTC", () => {
      expect(runs("0 * * * *", "2026-10-24T23:30:00Z", "Europe/London", 3)).toEqual([
        "2026-10-25T00:00:00.000Z", // 01:00 BST
        "2026-10-25T01:00:00.000Z", // 01:00 GMT
        "2026-10-25T02:00:00.000Z",
      ]);
    });
  });
});

describe("timezone helpers", () => {
  it("isValidTimezone accepts IANA names and rejects junk", () => {
    expect(isValidTimezone("UTC")).toBe(true);
    expect(isValidTimezone("Europe/London")).toBe(true);
    expect(isValidTimezone("Mars/Olympus_Mons")).toBe(false);
    expect(isValidTimezone("")).toBe(false);
  });

  it("tzOffsetMs reports the offset in effect at an instant", () => {
    const H = 3_600_000;
    expect(tzOffsetMs("UTC", at("2026-06-01T00:00:00Z"))).toBe(0);
    expect(tzOffsetMs("America/New_York", at("2026-01-15T12:00:00Z"))).toBe(-5 * H);
    expect(tzOffsetMs("America/New_York", at("2026-07-15T12:00:00Z"))).toBe(-4 * H);
    expect(tzOffsetMs("Asia/Kolkata", at("2026-07-15T12:34:56.789Z"))).toBe(5.5 * H);
  });
});
