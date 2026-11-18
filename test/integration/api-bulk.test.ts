// Uses a real HTTP server on 127.0.0.1 (random port): the behaviour under test is the HTTP
// response. No sleeps.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ApiServer } from "../../src/api/server.js";
import type { Conveyor } from "../../src/core/conveyor.js";
import { Scheduler } from "../../src/scheduler/scheduler.js";
import type { SqliteStore } from "../../src/storage/sqlite-store.js";
import { WebhookDispatcher } from "../../src/webhooks/dispatcher.js";
import { manualClock, setup } from "../helpers.js";

describe("POST /v1/jobs/bulk", () => {
  let api: ApiServer;
  let conveyor: Conveyor;
  let base: string;

  beforeEach(async () => {
    const s = await setup({ clock: manualClock() });
    conveyor = s.conveyor;
    api = new ApiServer({ conveyor, broker: s.broker, scheduler: new Scheduler(conveyor), webhooks: new WebhookDispatcher(conveyor) });
    base = `http://127.0.0.1:${(await api.listen(0)).port}`;
  });

  afterEach(() => api.close());

  const bulk = (jobs: unknown) =>
    fetch(`${base}/v1/jobs/bulk`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jobs }) });
  const count = async () => (await conveyor.listJobs({ limit: 500 })).items.length;

  it("creates every job and returns them in order", async () => {
    const res = await bulk([
      { queue: "q", name: "a", payload: 1 },
      { queue: "q", name: "b", payload: 2 },
    ]);
    expect(res.status).toBe(201);
    const body = (await res.json()) as { jobs: { name: string; payload: number }[] };
    expect(body.jobs.map((j) => [j.name, j.payload])).toEqual([
      ["a", 1],
      ["b", 2],
    ]);
    expect(await count()).toBe(2);
  });

  it.each([
    ["an invalid name", { queue: "q", name: "bad name" }, /invalid job name/],
    ["a missing queue", { name: "x" }, /queue is required/],
    ["a bad backoff", { queue: "q", name: "x", backoff: { type: "random" } }, /unknown backoff type/],
    ["a bad runAt", { queue: "q", name: "x", runAt: "not a date" }, /runAt must be an ISO-8601 timestamp/],
  ])("returns 400 and creates nothing when one job has %s", async (_label, badJob, message) => {
    const res = await bulk([{ queue: "q", name: "ok" }, badJob, { queue: "q", name: "ok" }]);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string; message: string } };
    expect(body.error.code).toBe("validation_error");
    expect(body.error.message).toMatch(message);
    expect(await count()).toBe(0);
  });

  it("returns an error and creates nothing when the database fails part-way", async () => {
    (conveyor.store as SqliteStore).db.exec(
      "CREATE TRIGGER fail_insert BEFORE INSERT ON jobs WHEN NEW.name = 'explode' BEGIN SELECT RAISE(ABORT, 'simulated insert failure'); END;",
    );
    const res = await bulk([
      { queue: "q", name: "ok" },
      { queue: "q", name: "ok" },
      { queue: "q", name: "explode" },
      { queue: "q", name: "ok" },
    ]);
    expect(res.status).toBe(500);
    expect(await res.json()).toEqual({ error: { code: "internal", message: "internal server error" } });
    expect(await count()).toBe(0);
  });

  it("rejects a body without a jobs array", async () => {
    const res = await fetch(`${base}/v1/jobs/bulk`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" });
    expect(res.status).toBe(400);
    expect(await count()).toBe(0);
  });
});
