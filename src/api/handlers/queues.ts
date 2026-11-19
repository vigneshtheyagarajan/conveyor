import type { Conveyor } from "../../core/conveyor.js";
import { asObject, bool, int } from "../../util/validate.js";
import type { Router } from "../router.js";

export function registerQueueRoutes(router: Router, conveyor: Conveyor): void {
  router.get("/v1/queues", async () => ({ body: { items: await conveyor.listQueues() } }), "jobs:read");

  router.get("/v1/queues/:name", async ({ params }) => ({ body: await conveyor.queueStats(params.name!) }), "jobs:read");

  router.put(
    "/v1/queues/:name",
    async ({ params, body }) => {
      const o = asObject(body);
      const patch = {
        concurrency: int(o, "concurrency", { min: 0 }),
        rateLimitMax: int(o, "rateLimitMax", { min: 0 }),
        rateLimitIntervalMs: int(o, "rateLimitIntervalMs", { min: 0 }),
        defaultMaxAttempts: int(o, "defaultMaxAttempts", { min: 1, max: 100 }),
        paused: bool(o, "paused"),
      };
      const clean = Object.fromEntries(Object.entries(patch).filter(([, v]) => v !== undefined));
      return { body: await conveyor.updateQueue(params.name!, clean) };
    },
    "admin",
  );

  router.post("/v1/queues/:name/pause", async ({ params }) => ({ body: await conveyor.pause(params.name!) }), "admin");
  router.post("/v1/queues/:name/resume", async ({ params }) => ({ body: await conveyor.resume(params.name!) }), "admin");

  router.post(
    "/v1/queues/:name/retry-dead",
    async ({ params }) => ({ body: await conveyor.retryDead(params.name!) }),
    "jobs:write",
  );
}
