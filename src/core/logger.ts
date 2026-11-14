export type LogLevel = "debug" | "info" | "warn" | "error" | "silent";

const ORDER: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };

export interface Logger {
  debug(msg: string, fields?: Record<string, unknown>): void;
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
  child(fields: Record<string, unknown>): Logger;
}

export function createLogger(
  level: LogLevel = "info",
  base: Record<string, unknown> = {},
  write: (line: string) => void = (line) => process.stderr.write(line + "\n"),
): Logger {
  const threshold = ORDER[level];
  const emit = (lvl: Exclude<LogLevel, "silent">, msg: string, fields?: Record<string, unknown>) => {
    if (ORDER[lvl] < threshold) return;
    const record: Record<string, unknown> = { ts: new Date().toISOString(), level: lvl, msg, ...base, ...fields };
    if (record.err instanceof Error) {
      const e = record.err;
      record.err = { name: e.name, message: e.message, stack: e.stack };
    }
    write(JSON.stringify(record));
  };
  return {
    debug: (m, f) => emit("debug", m, f),
    info: (m, f) => emit("info", m, f),
    warn: (m, f) => emit("warn", m, f),
    error: (m, f) => emit("error", m, f),
    child: (fields) => createLogger(level, { ...base, ...fields }, write),
  };
}

export const silentLogger: Logger = createLogger("silent");
