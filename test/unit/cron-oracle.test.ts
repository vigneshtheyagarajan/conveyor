// Compares nextRun with a brute-force reference that walks real time minute by minute and
// asks Intl what the wall clock says. The reference does no offset arithmetic of its own, so
// it catches mistakes in nextRun's timezone and DST handling.
//
// Intended semantics (documented in src/scheduler/cron.ts):
//  - "interval" expressions (minute or hour field starts with *) fire at every instant whose
//    local wall-clock time matches.
//  - "fixed-time" expressions fire at the first instant a matching wall-clock time occurs,
//    and once at the moment clocks jump if DST skips a matching time.
import { describe, expect, it } from "vitest";
import { nextRun, parseCron } from "../../src/scheduler/cron.js";

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;

const formatters = new Map<string, Intl.DateTimeFormat>();
const wallCaches = new Map<string, Map<number, number>>();

/** Local wall-clock time at instant `u`, encoded as if it were a UTC timestamp. */
function wall(tz: string, u: number): number {
  let cache = wallCaches.get(tz);
  if (!cache) wallCaches.set(tz, (cache = new Map()));
  let w = cache.get(u);
  if (w === undefined) {
    let f = formatters.get(tz);
    if (!f) {
      f = new Intl.DateTimeFormat("en-US", {
        timeZone: tz,
        hourCycle: "h23",
        year: "numeric",
        month: "numeric",
        day: "numeric",
        hour: "numeric",
        minute: "numeric",
      });
      formatters.set(tz, f);
    }
    const p: Record<string, number> = {};
    for (const part of f.formatToParts(new Date(u))) p[part.type] = Number(part.value);
    w = Date.UTC(p.year!, p.month! - 1, p.day!, p.hour!, p.minute!);
    cache.set(u, w);
  }
  return w;
}

function reference(expr: string, after: number, tz: string): number {
  const c = parseCron(expr);
  const f = expr.trim().split(/\s+/);
  const fixed = !f[0]!.startsWith("*") && !f[1]!.startsWith("*");
  const domStar = f[2]!.startsWith("*");
  const dowStar = f[4]!.startsWith("*");
  const matches = (w: number) => {
    const d = new Date(w);
    if (!c.minutes.has(d.getUTCMinutes()) || !c.hours.has(d.getUTCHours()) || !c.months.has(d.getUTCMonth() + 1)) {
      return false;
    }
    const dom = c.daysOfMonth.has(d.getUTCDate());
    const dow = c.daysOfWeek.has(d.getUTCDay());
    return domStar || dowStar ? dom && dow : dom || dow;
  };

  let u = Math.floor(after / MINUTE) * MINUTE + MINUTE;
  let latestWall = -Infinity; // latest wall time seen before u, to spot repeated times
  for (let t = u - 4 * HOUR; t < u; t += MINUTE) latestWall = Math.max(latestWall, wall(tz, t));
  let prevWall = wall(tz, u - MINUTE);

  for (let i = 0; i < 40 * 24 * 60; i++, u += MINUTE) {
    const w = wall(tz, u);
    if (fixed) {
      for (let skipped = prevWall + MINUTE; skipped < w; skipped += MINUTE) {
        if (matches(skipped)) return u;
      }
      if (matches(w) && w > latestWall) return u;
    } else if (matches(w)) {
      return u;
    }
    latestWall = Math.max(latestWall, w);
    prevWall = w;
  }
  throw new Error(`reference found no run for ${expr} after ${new Date(after).toISOString()}`);
}

// Expressions that fire at least daily, so the reference stays cheap.
const EXPRESSIONS = [
  "0 9 * * *",
  "30 2 * * *",
  "30 1 * * *",
  "0 0 * * *",
  "0,30 1-3 * * *",
  "45 23 * * *",
  "*/15 * * * *",
  "0 * * * *",
  "45 */2 * * *",
  "10 2 * * *",
];

// 2026 DST transitions (UTC instants), including 30-minute (Lord Howe), 45-minute-offset
// (Chatham), half-hour (St John's) and midnight (Santiago) changes.
const TRANSITIONS: [string, string[]][] = [
  ["America/New_York", ["2026-03-08T07:00:00Z", "2026-11-01T06:00:00Z"]],
  ["Europe/London", ["2026-03-29T01:00:00Z", "2026-10-25T01:00:00Z"]],
  ["Australia/Sydney", ["2026-04-04T16:00:00Z", "2026-10-03T16:00:00Z"]],
  ["Australia/Lord_Howe", ["2026-04-04T15:00:00Z", "2026-10-03T15:30:00Z"]],
  ["America/Santiago", ["2026-04-05T03:00:00Z", "2026-09-06T04:00:00Z"]],
  ["Pacific/Chatham", ["2026-04-04T14:00:00Z", "2026-09-26T14:00:00Z"]],
  ["America/St_Johns", ["2026-03-08T05:30:00Z", "2026-11-01T04:30:00Z"]],
];

describe("nextRun matches a brute-force reference", () => {
  for (const [tz, transitions] of TRANSITIONS) {
    for (const transition of transitions) {
      it(`around ${tz} ${transition}`, () => {
        const t = Date.parse(transition);
        const mismatches: string[] = [];
        // Every 47 minutes (plus some seconds) from a day before to a few hours after.
        for (let after = t - 26 * HOUR + 13_000; after < t + 3 * HOUR; after += 47 * MINUTE) {
          for (const expr of EXPRESSIONS) {
            const want = reference(expr, after, tz);
            const got = nextRun(expr, after, tz);
            if (got !== want) {
              mismatches.push(
                `${expr} after ${new Date(after).toISOString()}: got ${new Date(got).toISOString()}, want ${new Date(want).toISOString()}`,
              );
            }
          }
        }
        expect(mismatches.slice(0, 10)).toEqual([]);
      });
    }
  }

  it("on ordinary days across the year in zones with and without DST", () => {
    const mismatches: string[] = [];
    for (const tz of ["UTC", "Asia/Kolkata", "Asia/Kathmandu", "America/New_York", "Australia/Sydney"]) {
      for (let day = 0; day < 365; day += 29) {
        const after = Date.UTC(2026, 0, 1) + day * 24 * HOUR + 7 * HOUR + 11 * MINUTE + 7_000;
        for (const expr of ["0 9 * * *", "*/15 * * * *", "45 */2 * * *"]) {
          const want = reference(expr, after, tz);
          const got = nextRun(expr, after, tz);
          if (got !== want) mismatches.push(`${tz} ${expr} after ${new Date(after).toISOString()}`);
        }
      }
    }
    expect(mismatches).toEqual([]);
  });
});
