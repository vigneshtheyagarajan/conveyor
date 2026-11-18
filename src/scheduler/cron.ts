// minute hour day-of-month month day-of-week, plus names, ranges, steps and @macros.
//
// Day matching follows Vixie cron. A day field that starts with "*" is unrestricted, even
// with a step such as "*/2". If both day fields are restricted, a day matching either one
// is enough ("0 0 13 * FRI" = the 13th or any Friday). Otherwise a day must match both
// ("0 0 */2 * MON" = odd-numbered days that are also Mondays).
//
// Times are wall-clock times in the schedule's timezone. Around daylight-saving changes:
//  - Interval expressions (the minute or hour field starts with "*", e.g. "*/15 * * * *",
//    "0 * * * *") run at every instant whose local time matches. Times skipped when clocks
//    go forward do not run; times repeated when clocks go back run again, so the job keeps
//    its real-time rhythm.
//  - Fixed-time expressions (neither field starts with "*", e.g. "30 2 * * *") run once a
//    day as expected: a time that happens twice runs only the first time, and a time that
//    is skipped runs once at the moment the clocks jump.
// The same rules, with examples, are in docs/architecture.md ("Daylight-saving time").

export interface CronExpression {
  source: string;
  minutes: Set<number>;
  hours: Set<number>;
  daysOfMonth: Set<number>;
  months: Set<number>;
  daysOfWeek: Set<number>;
  domRestricted: boolean;
  dowRestricted: boolean;
  /** Neither the minute nor the hour field starts with "*" (see DST notes above). */
  fixedTime: boolean;
}

export class CronParseError extends Error {
  constructor(expr: string, reason: string) {
    super(`invalid cron expression "${expr}": ${reason}`);
    this.name = "CronParseError";
  }
}

const MACROS: Record<string, string> = {
  "@yearly": "0 0 1 1 *",
  "@annually": "0 0 1 1 *",
  "@monthly": "0 0 1 * *",
  "@weekly": "0 0 * * 0",
  "@daily": "0 0 * * *",
  "@midnight": "0 0 * * *",
  "@hourly": "0 * * * *",
};

const MONTH_NAMES = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
const DAY_NAMES = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

interface FieldSpec {
  name: string;
  min: number;
  max: number;
  names?: string[];
  namesOffset?: number;
}

const FIELDS: FieldSpec[] = [
  { name: "minute", min: 0, max: 59 },
  { name: "hour", min: 0, max: 23 },
  { name: "day-of-month", min: 1, max: 31 },
  { name: "month", min: 1, max: 12, names: MONTH_NAMES, namesOffset: 1 },
  { name: "day-of-week", min: 0, max: 7, names: DAY_NAMES, namesOffset: 0 },
];

export function parseCron(source: string): CronExpression {
  const trimmed = source.trim();
  const expanded = MACROS[trimmed.toLowerCase()] ?? trimmed;
  const parts = expanded.split(/\s+/);
  if (parts.length !== 5) throw new CronParseError(source, `expected 5 fields, got ${parts.length}`);
  const sets = parts.map((p, i) => parseField(source, p, FIELDS[i]!));
  const dow = sets[4]!;
  if (dow.has(7)) {
    dow.delete(7);
    dow.add(0);
  }
  return {
    source,
    minutes: sets[0]!,
    hours: sets[1]!,
    daysOfMonth: sets[2]!,
    months: sets[3]!,
    daysOfWeek: dow,
    domRestricted: !parts[2]!.startsWith("*"),
    dowRestricted: !parts[4]!.startsWith("*"),
    fixedTime: !parts[0]!.startsWith("*") && !parts[1]!.startsWith("*"),
  };
}

function parseField(source: string, field: string, spec: FieldSpec): Set<number> {
  const out = new Set<number>();
  for (const item of field.split(",")) {
    if (item === "") throw new CronParseError(source, `empty item in ${spec.name}`);
    const slashParts = item.split("/");
    if (slashParts.length > 2) throw new CronParseError(source, `"${item}" has more than one step in ${spec.name}`);
    const [rangePart, stepPart] = slashParts;
    let step = 1;
    if (stepPart !== undefined) {
      step = Number(stepPart);
      if (!Number.isInteger(step) || step < 1) throw new CronParseError(source, `bad step in ${spec.name}`);
    }
    let lo: number;
    let hi: number;
    if (rangePart === "*") {
      lo = spec.min;
      hi = spec.name === "day-of-week" ? 6 : spec.max;
    } else if (rangePart!.includes("-")) {
      const bounds = rangePart!.split("-");
      if (bounds.length > 2) throw new CronParseError(source, `range ${rangePart} has more than two bounds in ${spec.name}`);
      const [a, b] = bounds;
      lo = value(source, a!, spec);
      hi = value(source, b!, spec);
      if (lo > hi) throw new CronParseError(source, `range ${rangePart} is backwards in ${spec.name}`);
    } else {
      lo = value(source, rangePart!, spec);
      hi = stepPart !== undefined ? (spec.name === "day-of-week" ? 6 : spec.max) : lo;
    }
    for (let v = lo; v <= hi; v += step) out.add(v);
  }
  return out;
}

function value(source: string, token: string, spec: FieldSpec): number {
  const upper = token.toUpperCase();
  if (spec.names) {
    const idx = spec.names.indexOf(upper);
    if (idx >= 0) return idx + (spec.namesOffset ?? 0);
  }
  if (!/^\d+$/.test(token)) throw new CronParseError(source, `"${token}" is not valid for ${spec.name}`);
  const n = Number(token);
  if (n < spec.min || n > spec.max) {
    throw new CronParseError(source, `${n} is out of range for ${spec.name} (${spec.min}-${spec.max})`);
  }
  return n;
}

const formatters = new Map<string, Intl.DateTimeFormat>();

function formatter(timeZone: string): Intl.DateTimeFormat {
  let f = formatters.get(timeZone);
  if (!f) {
    f = new Intl.DateTimeFormat("en-US", {
      timeZone,
      hourCycle: "h23",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
    formatters.set(timeZone, f);
  }
  return f;
}

export function isValidTimezone(tz: string): boolean {
  try {
    formatter(tz);
    return true;
  } catch {
    return false;
  }
}

/** Offset in ms to add to a UTC instant to get wall-clock time in `timeZone`. */
export function tzOffsetMs(timeZone: string, at: number): number {
  const parts = formatter(timeZone).formatToParts(new Date(at));
  const get = (t: string) => Number(parts.find((p) => p.type === t)!.value);
  const asUtc = Date.UTC(get("year"), get("month") - 1, get("day"), get("hour"), get("minute"), get("second"));
  return asUtc - Math.floor(at / 1000) * 1000;
}

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const SEARCH_YEARS = 5;
/** Larger than any real DST shift (at most 2h), and than any gap between a shift's effects. */
const DST_WINDOW = 3 * HOUR;
/** UTC offsets range from -12h to +14h, so a wall time maps to an instant within this of it. */
const MAX_OFFSET = 14 * HOUR;

function dayMatches(expr: CronExpression, d: Date): boolean {
  const dom = expr.daysOfMonth.has(d.getUTCDate());
  const dow = expr.daysOfWeek.has(d.getUTCDay());
  if (expr.domRestricted && expr.dowRestricted) return dom || dow;
  // An unrestricted ("*...") field still has to match: "*/2" only allows every other day.
  return dom && dow;
}

export function nextRun(expr: CronExpression | string, after: number, timeZone = "UTC"): number {
  const cron = typeof expr === "string" ? parseCron(expr) : expr;
  const offsetAt = (t: number) => tzOffsetMs(timeZone, t);

  // Walk candidate wall-clock times ("wall clock as UTC" values) in increasing order. When
  // clocks go back shortly after `after`, some later instants have earlier wall-clock times,
  // so start from the smaller of the offsets in effect now and just after.
  let local = Math.floor((after + Math.min(offsetAt(after), offsetAt(after + DST_WINDOW))) / MINUTE) * MINUTE;
  const horizon = new Date(local);
  horizon.setUTCFullYear(horizon.getUTCFullYear() + SEARCH_YEARS);

  let best: number | undefined;
  // Once a run is found, only wall times below this can still map to an earlier instant.
  let stopAt = Infinity;
  while (local <= horizon.getTime() && local < stopAt) {
    const d = new Date(local);
    if (!cron.months.has(d.getUTCMonth() + 1)) {
      local = Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1);
      continue;
    }
    if (!dayMatches(cron, d)) {
      local = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1);
      continue;
    }
    if (!cron.hours.has(d.getUTCHours())) {
      local = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours() + 1);
      continue;
    }
    if (!cron.minutes.has(d.getUTCMinutes())) {
      local += MINUTE;
      continue;
    }
    const run = runFor(cron, local, after, offsetAt);
    if (run !== undefined && (best === undefined || run < best)) {
      best = run;
      stopAt = best + Math.max(offsetAt(best), offsetAt(best - DST_WINDOW));
    }
    local += MINUTE;
  }
  if (best === undefined) {
    throw new Error(`no run time found for "${cron.source}" within ${SEARCH_YEARS} years`);
  }
  return best;
}

/** The run (if any, and after `after`) for a matching wall-clock time. */
function runFor(
  cron: CronExpression,
  local: number,
  after: number,
  offsetAt: (t: number) => number,
): number | undefined {
  const offsets = new Set([offsetAt(local - MAX_OFFSET), offsetAt(local), offsetAt(local + MAX_OFFSET)]);
  // Instants whose wall-clock time is `local`: none in a DST gap, two in an overlap.
  const instants = [...offsets]
    .map((o) => local - o)
    .filter((u) => offsetAt(u) === local - u)
    .sort((a, b) => a - b);

  if (instants.length === 0) {
    if (!cron.fixedTime) return undefined;
    const jump = gapEnd(local, [...offsets], offsetAt);
    return jump > after ? jump : undefined;
  }
  if (cron.fixedTime) return instants[0]! > after ? instants[0] : undefined;
  return instants.find((u) => u > after);
}

/** First instant after the DST gap that swallowed wall-clock time `local`. */
function gapEnd(local: number, offsets: number[], offsetAt: (t: number) => number): number {
  // Mapped with the later (larger) offset, `local` lands before the jump; with the earlier one, after it.
  let lo = local - Math.max(...offsets);
  let hi = local - Math.min(...offsets);
  const before = offsetAt(lo);
  while (hi - lo > MINUTE) {
    const mid = lo + Math.floor((hi - lo) / 2 / MINUTE) * MINUTE;
    if (offsetAt(mid) === before) lo = mid;
    else hi = mid;
  }
  return hi;
}

/** The next `count` run times, for previews. */
export function upcomingRuns(expr: string, after: number, timeZone = "UTC", count = 5): number[] {
  const cron = parseCron(expr);
  const out: number[] = [];
  let t = after;
  for (let i = 0; i < count; i++) {
    t = nextRun(cron, t, timeZone);
    out.push(t);
  }
  return out;
}
