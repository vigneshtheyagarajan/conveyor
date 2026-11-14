import { Broker } from "../src/broker/broker.js";
import { Conveyor } from "../src/core/conveyor.js";
import type { ConveyorConfig } from "../src/core/config.js";
import { SqliteStore } from "../src/storage/sqlite-store.js";
import { ManualClock, type Clock } from "../src/util/clock.js";

export async function setup(opts: { clock?: Clock; config?: Partial<ConveyorConfig> } = {}) {
  const store = new SqliteStore(":memory:");
  const conveyor = new Conveyor({ store, clock: opts.clock, config: { leaseMs: 1000, heartbeatMs: 200, ...opts.config } });
  await conveyor.init();
  const broker = new Broker(conveyor, { random: () => 0.5 });
  return { store, conveyor, broker };
}

export function manualClock() {
  return new ManualClock(Date.UTC(2026, 0, 5, 12, 0, 0));
}

export async function waitFor(predicate: () => Promise<boolean> | boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timed out");
    await new Promise((r) => setTimeout(r, 10));
  }
}
