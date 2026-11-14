export function table(rows: Record<string, unknown>[], columns: string[]): string {
  if (rows.length === 0) return "(none)";
  const cells = rows.map((r) => columns.map((c) => fmt(r[c])));
  const widths = columns.map((c, i) => Math.max(c.length, ...cells.map((row) => row[i]!.length)));
  const line = (vals: string[]) => vals.map((v, i) => v.padEnd(widths[i]!)).join("  ").trimEnd();
  return [line(columns.map((c) => c.toUpperCase())), ...cells.map(line)].join("\n");
}

function fmt(v: unknown): string {
  if (v === null || v === undefined) return "-";
  if (typeof v === "number" && v > 1e12) return new Date(v).toISOString().replace(".000Z", "Z");
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export function print(value: unknown, json: boolean): void {
  if (json || typeof value !== "string") {
    process.stdout.write(JSON.stringify(value, null, 2) + "\n");
  } else {
    process.stdout.write(value + "\n");
  }
}
