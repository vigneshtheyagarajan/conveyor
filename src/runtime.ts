import { Broker } from "./broker/broker.js";
import { Maintenance } from "./broker/maintenance.js";
import { ApiServer } from "./api/server.js";
import { loadConfig, type ConveyorConfig } from "./core/config.js";
import { Conveyor } from "./core/conveyor.js";
import { createLogger, type Logger } from "./core/logger.js";
import { conveyorMetrics } from "./metrics/conveyor-metrics.js";
import type { MetricsRegistry } from "./metrics/registry.js";
import { Scheduler } from "./scheduler/scheduler.js";
import { SqliteStore } from "./storage/sqlite-store.js";
import { WebhookDispatcher } from "./webhooks/dispatcher.js";

/** Everything `conveyor serve` runs, wired together. */
export interface Runtime {
  config: ConveyorConfig;
  logger: Logger;
  conveyor: Conveyor;
  broker: Broker;
  scheduler: Scheduler;
  maintenance: Maintenance;
  webhooks: WebhookDispatcher;
  metrics: MetricsRegistry;
  api: ApiServer;
  start(): Promise<void>;
  stop(): Promise<void>;
}

export async function createRuntime(overrides: Partial<ConveyorConfig> = {}): Promise<Runtime> {
  const config = loadConfig(process.env, overrides);
  const logger = createLogger(config.logLevel, { service: "conveyor" });
  const store = new SqliteStore(config.dbPath);
  const conveyor = new Conveyor({ store, config, logger });
  await conveyor.init();
  const broker = new Broker(conveyor);
  const scheduler = new Scheduler(conveyor);
  const maintenance = new Maintenance(conveyor);
  const webhooks = new WebhookDispatcher(conveyor);
  const metrics = conveyorMetrics(conveyor);
  const api = new ApiServer({ conveyor, broker, scheduler, webhooks, metrics });

  return {
    config,
    logger,
    conveyor,
    broker,
    scheduler,
    maintenance,
    webhooks,
    metrics,
    api,
    async start() {
      maintenance.start();
      scheduler.start();
      webhooks.start();
      const addr = await api.listen(config.port, config.host);
      logger.info("conveyor listening", { url: `http://${addr.address}:${addr.port}` });
    },
    async stop() {
      scheduler.stop();
      maintenance.stop();
      await api.close();
      await webhooks.stop();
      await store.close();
    },
  };
}
