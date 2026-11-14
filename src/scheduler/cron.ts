// minute hour day-of-month month day-of-week, plus names, ranges, steps and @macros.
// Like Vixie cron, if both day fields are restricted either one matching is enough.

export interface CronExpression {
  source: string;
  minutes: Set<number>;
  hours: Set<number>;
  daysOfMonth: Set<number>;
  months: Set<number>;
  daysOfWeek: Set<number>;
  domRestricted: boolean;
  dowRestricted: boolean;
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
    domRestricted: parts[2] !== "*",
    dowRestricted: parts[4] !== "*",
  };
}

function parseField(source: string, field: string, spec: FieldSpec): Set<number> {
  const out = new Set<number>();
  for (const item of field.split(",")) {
    if (item === "") throw new CronParseError(source, `empty item in ${spec.name}`);
    const [rangePart, stepPart] = item.split("/");
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
      const [a, b] = rangePart!.split("-");
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
const MAX_SEARCH_MINUTES = 5 * 366 * 24 * 60;

function dayMatches(expr: CronExpression, d: Date): boolean {
  const dom = expr.daysOfMonth.has(d.getUTCDate());
  const dow = expr.daysOfWeek.has(d.getUTCDay());
  if (expr.domRestricted && expr.dowRestricted) return dom || dow;
  if (expr.domRestricted) return dom;
  if (expr.dowRestricted) return dow;
  return true;
}

export function nextRun(expr: CronExpression | string, after: number, timeZone = "UTC"): number {
  const cron = typeof expr === "string" ? parseCron(expr) : expr;
  const offset = tzOffsetMs(timeZone, after);

  // Work in "wall clock as UTC" space, starting at the next whole minute.
  let local = Math.floor((after + offset) / MINUTE) * MINUTE + MINUTE;
  for (let i = 0; i < MAX_SEARCH_MINUTES; i++) {
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
    return local - offset;
  }
  throw new Error(`no run time found for "${cron.source}" within 5 years`);
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
