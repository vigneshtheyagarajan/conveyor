import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createLogger, silentLogger } from "../../src/core/logger.js";

const NOW = Date.UTC(2026, 0, 5, 12, 0, 0);

function capture(level: Parameters<typeof createLogger>[0], base?: Record<string, unknown>) {
  const lines: Record<string, unknown>[] = [];
  const logger = createLogger(level, base, (line) => lines.push(JSON.parse(line)));
  return { logger, lines };
}

describe("createLogger", () => {
  beforeEach(() => {
    vi.useFakeTimers({ now: NOW });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("writes one JSON object per line with time, level, message and fields", () => {
    const raw: string[] = [];
    createLogger("debug", { service: "conveyor" }, (l) => raw.push(l)).info("hello", { jobId: "j1" });
    expect(raw).toHaveLength(1);
    expect(raw[0]).not.toContain("\n");
    expect(JSON.parse(raw[0]!)).toEqual({
      ts: "2026-01-05T12:00:00.000Z",
      level: "info",
      msg: "hello",
      service: "conveyor",
      jobId: "j1",
    });
  });

  it.each([
    ["debug", ["debug", "info", "warn", "error"]],
    ["info", ["info", "warn", "error"]],
    ["warn", ["warn", "error"]],
    ["error", ["error"]],
    ["silent", []],
  ] as const)("at level %s writes %j", (level, expected) => {
    const { logger, lines } = capture(level);
    logger.debug("d");
    logger.info("i");
    logger.warn("w");
    logger.error("e");
    expect(lines.map((l) => l.level)).toEqual(expected);
  });

  it("child loggers add fields, and inherit the level and writer", () => {
    const { logger, lines } = capture("info", { service: "s" });
    const child = logger.child({ component: "worker" }).child({ workerId: "w1" });
    child.debug("hidden");
    child.info("shown", { extra: 1 });
    expect(lines).toEqual([
      { ts: "2026-01-05T12:00:00.000Z", level: "info", msg: "shown", service: "s", component: "worker", workerId: "w1", extra: 1 },
    ]);
  });

  it("lets call fields override base fields", () => {
    const { logger, lines } = capture("info", { component: "a" });
    logger.info("m", { component: "b" });
    expect(lines[0]!.component).toBe("b");
  });

  it("serialises an Error in `err` with its name, message and stack", () => {
    const { logger, lines } = capture("info");
    const err = new TypeError("boom");
    logger.error("failed", { err });
    expect(lines[0]!.err).toEqual({ name: "TypeError", message: "boom", stack: err.stack });
  });

  it("leaves a non-Error `err` as it is", () => {
    const { logger, lines } = capture("info");
    logger.error("failed", { err: "just a string" });
    expect(lines[0]!.err).toBe("just a string");
  });

  it("writes to stderr by default", () => {
    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      createLogger("info").warn("to stderr");
      expect(write).toHaveBeenCalledOnce();
      expect(String(write.mock.calls[0]![0])).toMatch(/"msg":"to stderr"[^\n]*\n$/);
    } finally {
      write.mockRestore();
    }
  });

  it("silentLogger writes nothing, including from children", () => {
    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      silentLogger.error("x");
      silentLogger.child({ a: 1 }).error("y");
      expect(write).not.toHaveBeenCalled();
    } finally {
      write.mockRestore();
    }
  });

  // Regression (was KB-7): fields were spread after ts/level/msg and could replace them.
  it("does not let fields overwrite ts, level or msg", () => {
    const { logger, lines } = capture("info");
    logger.error("disk full", { level: "debug", msg: "other", ts: "yesterday" });
    expect(lines[0]).toMatchObject({ level: "error", msg: "disk full", ts: "2026-01-05T12:00:00.000Z" });
  });
});
