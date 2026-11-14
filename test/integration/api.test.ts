import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ApiServer } from "../../src/api/server.js";
import { Scheduler } from "../../src/scheduler/scheduler.js";
import { WebhookDispatcher } from "../../src/webhooks/dispatcher.js";
import { setup } from "../helpers.js";

describe("HTTP API", () => {
  let api: ApiServer;
  let base: string;

  beforeEach(async () => {
    const { conveyor, broker } = await setup();
    api = new ApiServer({
      conveyor,
      broker,
      scheduler: new Scheduler(conveyor),
      webhooks: new WebhookDispatcher(conveyor),
    });
    const addr = await api.listen(0);
    base = `http://127.0.0.1:${addr.port}`;
  });

  afterEach(() => api.close());

  const post = (path: string, body: unknown) =>
    fetch(base + path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });

  it("enqueues and fetches a job", async () => {
    const res = await post("/v1/jobs", { queue: "default", name: "hello", payload: { x: 1 } });
    expect(res.status).toBe(201);
    const job = (await res.json()) as { id: string };
    const got = (await (await fetch(`${base}/v1/jobs/${job.id}`)).json()) as { payload: unknown };
    expect(got.payload).toEqual({ x: 1 });
  });

  it("rejects invalid input", async () => {
    const res = await post("/v1/jobs", { queue: "default" });
    expect(res.status).toBe(400);
  });
});
