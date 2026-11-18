import { describe, expect, it } from "vitest";
import { DEFAULT_CONFIG, loadConfig } from "../../src/core/config.js";

describe("loadConfig", () => {
  it("uses the defaults when nothing is set", () => {
    expect(loadConfig({})).toEqual(DEFAULT_CONFIG);
  });

  it("matches the documented defaults", () => {
    expect(DEFAULT_CONFIG).toEqual({
      dbPath: "./conveyor.db",
      host: "127.0.0.1",
      port: 7420,
      logLevel: "info",
      leaseMs: 30_000,
      heartbeatMs: 10_000,
      reaperIntervalMs: 5_000,
      schedulerIntervalMs: 1_000,
      webhookTimeoutMs: 5_000,
      webhookMaxAttempts: 5,
      retentionMs: 7 * 24 * 60 * 60 * 1000,
      maxPayloadBytes: 262_144,
    });
  });

  it("reads every variable", () => {
    const config = loadConfig({
      CONVEYOR_DB: "/data/q.db",
      CONVEYOR_HOST: "0.0.0.0",
      CONVEYOR_PORT: "8080",
      CONVEYOR_LOG_LEVEL: "debug",
      CONVEYOR_LEASE_MS: "60000",
      CONVEYOR_HEARTBEAT_MS: "20000",
      CONVEYOR_REAPER_INTERVAL_MS: "1000",
      CONVEYOR_SCHEDULER_INTERVAL_MS: "500",
      CONVEYOR_WEBHOOK_TIMEOUT_MS: "2500",
      CONVEYOR_WEBHOOK_MAX_ATTEMPTS: "3",
      CONVEYOR_RETENTION_MS: "0",
      CONVEYOR_MAX_PAYLOAD_BYTES: "1024",
    });
    expect(config).toEqual({
      dbPath: "/data/q.db",
      host: "0.0.0.0",
      port: 8080,
      logLevel: "debug",
      leaseMs: 60_000,
      heartbeatMs: 20_000,
      reaperIntervalMs: 1000,
      schedulerIntervalMs: 500,
      webhookTimeoutMs: 2500,
      webhookMaxAttempts: 3,
      retentionMs: 0,
      maxPayloadBytes: 1024,
    });
  });

  it("lets explicit overrides win over the environment", () => {
    const config = loadConfig({ CONVEYOR_PORT: "8080", CONVEYOR_DB: "env.db" }, { port: 9000, dbPath: "cli.db" });
    expect(config.port).toBe(9000);
    expect(config.dbPath).toBe("cli.db");
  });

  it("treats an empty numeric variable as unset", () => {
    expect(loadConfig({ CONVEYOR_PORT: "" }).port).toBe(7420);
  });

  it.each(["abc", "-1", "Infinity", "NaN"])("rejects %j as a number", (raw) => {
    expect(() => loadConfig({ CONVEYOR_LEASE_MS: raw })).toThrow(`CONVEYOR_LEASE_MS must be a non-negative number, got ${raw}`);
  });

  it.each(["debug", "info", "warn", "error", "silent"])("accepts log level %s", (level) => {
    expect(loadConfig({ CONVEYOR_LOG_LEVEL: level }).logLevel).toBe(level);
  });

  it("rejects an unknown log level", () => {
    expect(() => loadConfig({ CONVEYOR_LOG_LEVEL: "verbose" })).toThrow("invalid CONVEYOR_LOG_LEVEL: verbose");
  });

  it("requires the heartbeat to be shorter than the lease", () => {
    expect(() => loadConfig({ CONVEYOR_LEASE_MS: "1000", CONVEYOR_HEARTBEAT_MS: "1000" })).toThrow(
      "heartbeatMs must be smaller than leaseMs",
    );
    expect(() => loadConfig({}, { heartbeatMs: 40_000 })).toThrow("heartbeatMs must be smaller than leaseMs");
    expect(loadConfig({ CONVEYOR_LEASE_MS: "1000", CONVEYOR_HEARTBEAT_MS: "999" }).heartbeatMs).toBe(999);
  });

  // Regression (was part of KB-6): an empty CONVEYOR_DB opened a throwaway temporary SQLite
  // database and an empty CONVEYOR_HOST made the API listen on every interface. A blank
  // `CONVEYOR_DB=` line in an .env file was enough.
  it("treats an empty CONVEYOR_DB as unset", () => {
    expect(loadConfig({ CONVEYOR_DB: "" }).dbPath).toBe("./conveyor.db");
  });

  it("treats an empty CONVEYOR_HOST as unset (never every interface)", () => {
    expect(loadConfig({ CONVEYOR_HOST: "" }).host).toBe("127.0.0.1");
  });

  // Regression (was KB-6): an empty CONVEYOR_LOG_LEVEL stopped the process from starting.
  it("treats an empty CONVEYOR_LOG_LEVEL as unset", () => {
    expect(loadConfig({ CONVEYOR_LOG_LEVEL: "" }).logLevel).toBe("info");
  });
});
