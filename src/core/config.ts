import type { LogLevel } from "./logger.js";

export interface ConveyorConfig {
  dbPath: string;
  host: string;
  port: number;
  logLevel: LogLevel;
  /** How long a claimed job is leased to a worker before it can be reclaimed. */
  leaseMs: number;
  /** How often workers extend the lease of running jobs. */
  heartbeatMs: number;
  reaperIntervalMs: number;
  schedulerIntervalMs: number;
  webhookTimeoutMs: number;
  webhookMaxAttempts: number;
  /** Completed and cancelled jobs older than this are deleted by the janitor (dead jobs are kept). 0 disables. */
  retentionMs: number;
  maxPayloadBytes: number;
}

export const DEFAULT_CONFIG: ConveyorConfig = {
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
  maxPayloadBytes: 256 * 1024,
};

const LEVELS: LogLevel[] = ["debug", "info", "warn", "error", "silent"];

function numEnv(env: NodeJS.ProcessEnv, key: string, fallback: number): number {
  const raw = env[key];
  if (raw === undefined || raw === "") return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) throw new Error(`${key} must be a non-negative number, got ${raw}`);
  return n;
}

export function loadConfig(
  env: NodeJS.ProcessEnv = process.env,
  overrides: Partial<ConveyorConfig> = {},
): ConveyorConfig {
  const level = (env.CONVEYOR_LOG_LEVEL || DEFAULT_CONFIG.logLevel) as LogLevel;
  if (!LEVELS.includes(level)) throw new Error(`invalid CONVEYOR_LOG_LEVEL: ${level}`);
  const config: ConveyorConfig = {
    // || not ??: an empty value means "unset". "" would open a temporary database or
    // listen on every interface.
    dbPath: env.CONVEYOR_DB || DEFAULT_CONFIG.dbPath,
    host: env.CONVEYOR_HOST || DEFAULT_CONFIG.host,
    port: numEnv(env, "CONVEYOR_PORT", DEFAULT_CONFIG.port),
    logLevel: level,
    leaseMs: numEnv(env, "CONVEYOR_LEASE_MS", DEFAULT_CONFIG.leaseMs),
    heartbeatMs: numEnv(env, "CONVEYOR_HEARTBEAT_MS", DEFAULT_CONFIG.heartbeatMs),
    reaperIntervalMs: numEnv(env, "CONVEYOR_REAPER_INTERVAL_MS", DEFAULT_CONFIG.reaperIntervalMs),
    schedulerIntervalMs: numEnv(env, "CONVEYOR_SCHEDULER_INTERVAL_MS", DEFAULT_CONFIG.schedulerIntervalMs),
    webhookTimeoutMs: numEnv(env, "CONVEYOR_WEBHOOK_TIMEOUT_MS", DEFAULT_CONFIG.webhookTimeoutMs),
    webhookMaxAttempts: numEnv(env, "CONVEYOR_WEBHOOK_MAX_ATTEMPTS", DEFAULT_CONFIG.webhookMaxAttempts),
    retentionMs: numEnv(env, "CONVEYOR_RETENTION_MS", DEFAULT_CONFIG.retentionMs),
    maxPayloadBytes: numEnv(env, "CONVEYOR_MAX_PAYLOAD_BYTES", DEFAULT_CONFIG.maxPayloadBytes),
    ...overrides,
  };
  if (config.heartbeatMs >= config.leaseMs) {
    throw new Error("heartbeatMs must be smaller than leaseMs");
  }
  return config;
}
