import { randomBytes } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "../src/broker/broker.js";
import { Conveyor } from "../src/core/conveyor.js";
import type { ConveyorConfig } from "../src/core/config.js";
import { createLogger, type Logger } from "../src/core/logger.js";
import { SqliteStore } from "../src/storage/sqlite-store.js";
import type { Store } from "../src/storage/store.js";
import { ManualClock, type Clock } from "../src/util/clock.js";

export async function setup(opts: { clock?: Clock; config?: Partial<ConveyorConfig>; logger?: Logger } = {}) {
  const store = new SqliteStore(":memory:");
  const conveyor = new Conveyor({
    store,
    clock: opts.clock,
    logger: opts.logger,
    config: { leaseMs: 1000, heartbeatMs: 200, ...opts.config },
  });
  await conveyor.init();
  const broker = new Broker(conveyor, { random: () => 0.5 });
  return { store, conveyor, broker };
}

export function manualClock() {
  return new ManualClock(Date.UTC(2026, 0, 5, 12, 0, 0));
}

/** A fresh temporary directory for database files. Call cleanup() when done. */
export function tempDir(prefix = "conveyor-test-") {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  return {
    dir,
    file: (name = "conveyor.db") => join(dir, name),
    cleanup: () => rmSync(dir, { recursive: true, force: true }),
  };
}

/** A logger that keeps parsed log records in memory. */
export function memoryLogger(level: Parameters<typeof createLogger>[0] = "debug") {
  const records: Record<string, unknown>[] = [];
  const logger = createLogger(level, {}, (line) => records.push(JSON.parse(line)));
  return { logger, records };
}

/** How job ids were made before the FIFO fix: timestamp, then 10 independent random characters. */
export function legacyJobId(now: number, random = randomBytes(10)): string {
  const ALPHA = "0123456789abcdefghjkmnpqrstvwxyz";
  let ts = "";
  let n = now;
  for (let i = 0; i < 10; i++) {
    ts = ALPHA[n % 32] + ts;
    n = Math.floor(n / 32);
  }
  return ts + [...random].map((b) => ALPHA[b % 32]).join("");
}

/**
 * Wraps a store so that a hook can run right after its next call returns, before the caller
 * sees the result: a state change by someone else landing between a read and the write that
 * follows it. `fired` counts how often a hook ran, so a test can check it was not vacuous.
 */
export function interruptible(store: Store) {
  let hook: (() => Promise<void>) | undefined;
  let skip = 0;
  let fired = 0;
  const wrapped = new Proxy(store, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof value !== "function") return value;
      return async (...args: unknown[]) => {
        const result = await value.apply(target, args);
        const h = hook;
        if (h && skip > 0) skip--;
        else if (h) {
          hook = undefined;
          fired++;
          await h();
        }
        return result;
      };
    },
  });
  return {
    store: wrapped,
    /** Run `fn` after the next store call, or after the (skipCalls + 1)th call from now. */
    afterNextCall(fn: () => Promise<void>, skipCalls = 0) {
      hook = fn;
      skip = skipCalls;
    },
    get fired() {
      return fired;
    },
  };
}
