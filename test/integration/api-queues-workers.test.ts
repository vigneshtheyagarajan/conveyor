import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { errorBody, startApi, type Api } from "./api-harness.js";

let api: Api;
beforeEach(async () => {
  api = await startApi();
});
afterEach(async () => {
  await api.close();
});

const T = Date.UTC(2026, 0, 5, 12, 0, 0);

describe("queues", () => {
  it("GET /v1/queues lists queues with counts, including ones that only have jobs", async () => {
    await api.conveyor.updateQueue("configured", { concurrency: 2 });
    await api.conveyor.enqueue({ queue: "implicit", name: "x" });
    const res = await api.req("GET", "/v1/queues");
    expect(res.status).toBe(200);
    expect(res.body.items.map((q: { name: string }) => q.name)).toEqual(["configured", "implicit"]);
    expect(res.body.items[1]).toMatchObject({ name: "implicit", concurrency: 0, counts: { waiting: 1, active: 0 } });
  });

  it("GET /v1/queues/:name returns settings and stats (defaults for a queue never seen)", async () => {
    await api.conveyor.enqueue({ queue: "q", name: "x" });
    const res = await api.req("GET", "/v1/queues/q");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      name: "q",
      paused: false,
      concurrency: 0,
      rateLimitMax: 0,
      rateLimitIntervalMs: 0,
      defaultMaxAttempts: 3,
      createdAt: T,
      updatedAt: T,
      counts: { delayed: 0, waiting: 1, active: 0, completed: 0, dead: 0, cancelled: 0 },
      oldestWaitingAt: T,
    });
    expect((await api.req("GET", "/v1/queues/never-seen")).body).toMatchObject({ name: "never-seen", counts: { waiting: 0 }, oldestWaitingAt: null });
  });

  it("PUT /v1/queues/:name updates only the given settings", async () => {
    expect((await api.req("PUT", "/v1/queues/q", { body: { concurrency: 3 } })).body).toMatchObject({ concurrency: 3, rateLimitMax: 0 });
    const res = await api.req("PUT", "/v1/queues/q", { body: { rateLimitMax: 10, rateLimitIntervalMs: 1000, defaultMaxAttempts: 5, paused: true } });
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ name: "q", concurrency: 3, rateLimitMax: 10, rateLimitIntervalMs: 1000, defaultMaxAttempts: 5, paused: true });
    expect(await api.conveyor.queueSettings("q")).toMatchObject({ concurrency: 3, paused: true });
  });

  it.each<[string, unknown, string]>([
    ["a negative concurrency", { concurrency: -1 }, "concurrency must be >= 0"],
    ["a fractional concurrency", { concurrency: 1.5 }, "concurrency must be an integer"],
    ["a string rate limit", { rateLimitMax: "10" }, "rateLimitMax must be an integer"],
    ["a rate limit without an interval", { rateLimitMax: 10 }, "rateLimitIntervalMs is required when rateLimitMax is set"],
    ["too many default attempts", { defaultMaxAttempts: 101 }, "defaultMaxAttempts must be <= 100"],
    ["a non-boolean paused", { paused: "yes" }, "paused must be a boolean"],
    ["a non-object body", [1], "body must be an object"],
  ])("PUT rejects %s with 400", async (_label, body, message) => {
    const res = await api.req("PUT", "/v1/queues/q", { body });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", message));
    expect(await api.conveyor.store.getQueue("q")).toBeUndefined();
  });

  it("PUT rejects an invalid queue name with 400", async () => {
    const res = await api.req("PUT", "/v1/queues/bad%20name", { body: { concurrency: 1 } });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", "invalid queue name"));
  });

  it("pause and resume stop and restart claims", async () => {
    await api.conveyor.enqueue({ queue: "q", name: "x" });
    const paused = await api.req("POST", "/v1/queues/q/pause");
    expect(paused.status).toBe(200);
    expect(paused.body).toMatchObject({ name: "q", paused: true });
    expect((await api.req("POST", "/v1/leases", { body: { queues: ["q"], workerId: "w" } })).status).toBe(204);
    const resumed = await api.req("POST", "/v1/queues/q/resume");
    expect(resumed.body).toMatchObject({ paused: false });
    expect((await api.req("POST", "/v1/leases", { body: { queues: ["q"], workerId: "w" } })).status).toBe(201);
  });

  it("POST /v1/queues/:name/retry-dead retries the queue's dead jobs and reports how many", async () => {
    for (let i = 0; i < 2; i++) {
      const job = await api.conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 1 });
      const lease = await api.broker.claim(["q"], "w");
      await api.broker.fail(job.id, lease!.token, "e");
    }
    const res = await api.req("POST", "/v1/queues/q/retry-dead");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ retried: 2 });
    expect((await api.conveyor.queueStats("q")).counts).toMatchObject({ dead: 0, waiting: 2 });
    expect((await api.req("POST", "/v1/queues/q/retry-dead")).body).toEqual({ retried: 0 });
  });
});

describe("worker protocol", () => {
  const lease = async (queues = ["q"]) => api.req("POST", "/v1/leases", { body: { queues, workerId: "http-1" } });

  it("POST /v1/leases answers 204 when there is nothing to do", async () => {
    const res = await lease();
    expect(res.status).toBe(204);
    expect(res.text).toBe("");
  });

  it("POST /v1/leases leases the next job", async () => {
    const job = await api.conveyor.enqueue({ queue: "q", name: "x", payload: { n: 1 } });
    const res = await lease();
    expect(res.status).toBe(201);
    expect(res.body).toEqual({
      job: expect.objectContaining({ id: job.id, state: "active", attempts: 1, workerId: "http-1", payload: { n: 1 } }),
      leaseToken: expect.stringMatching(/^[0-9a-f-]{36}$/),
      leaseExpiresAt: new Date(T + 1000).toISOString(),
    });
    expect(res.body.job).not.toHaveProperty("leaseToken");
  });

  it.each<[string, unknown, string]>([
    ["no queues", { workerId: "w" }, "queues is required"],
    ["an empty queue list", { queues: [], workerId: "w" }, "queues is required"],
    ["non-string queues", { queues: [1], workerId: "w" }, "queues must be an array of strings"],
    ["no worker id", { queues: ["q"] }, "workerId is required"],
    ["a long worker id", { queues: ["q"], workerId: "w".repeat(201) }, "workerId must be at most 200 characters"],
  ])("POST /v1/leases rejects %s with 400", async (_label, body, message) => {
    const res = await api.req("POST", "/v1/leases", { body });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", message));
  });

  it("runs a job through heartbeat, progress and complete", async () => {
    const job = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const { leaseToken } = (await lease()).body;

    api.clock.advance(500);
    const hb = await api.req("POST", `/v1/jobs/${job.id}/heartbeat`, { body: { leaseToken } });
    expect(hb.status).toBe(200);
    expect(hb.body).toEqual({ leaseExpiresAt: new Date(T + 1500).toISOString() });

    const progress = await api.req("POST", `/v1/jobs/${job.id}/progress`, { body: { leaseToken, progress: 0.5 } });
    expect(progress.status).toBe(204);
    expect((await api.conveyor.getJob(job.id)).progress).toBe(0.5);

    const done = await api.req("POST", `/v1/jobs/${job.id}/complete`, { body: { leaseToken, result: { sent: true } } });
    expect(done.status).toBe(200);
    expect(done.body).toMatchObject({ id: job.id, state: "completed", result: { sent: true }, progress: 1 });
  });

  it("completes with a null result when none is given", async () => {
    const job = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const { leaseToken } = (await lease()).body;
    expect((await api.req("POST", `/v1/jobs/${job.id}/complete`, { body: { leaseToken } })).body.result).toBeNull();
  });

  it("fail schedules a retry, honouring retryInMs, and unrecoverable goes to dead", async () => {
    const a = await api.conveyor.enqueue({ queue: "q", name: "x" });
    let { leaseToken } = (await lease()).body;
    const retry = await api.req("POST", `/v1/jobs/${a.id}/fail`, { body: { leaseToken, error: "smtp down", retryInMs: 250 } });
    expect(retry.status).toBe(200);
    expect(retry.body).toMatchObject({ state: "delayed", lastError: "smtp down", runAt: new Date(T + 250).toISOString() });

    const b = await api.conveyor.enqueue({ queue: "q", name: "x" });
    ({ leaseToken } = (await lease()).body);
    const dead = await api.req("POST", `/v1/jobs/${b.id}/fail`, { body: { leaseToken, unrecoverable: true } });
    expect(dead.body).toMatchObject({ state: "dead", lastError: "failed" });
  });

  it("release hands the job back without using an attempt", async () => {
    const job = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const { leaseToken } = (await lease()).body;
    const res = await api.req("POST", `/v1/jobs/${job.id}/release`, { body: { leaseToken } });
    expect(res.status).toBe(204);
    expect(await api.conveyor.getJob(job.id)).toMatchObject({ state: "waiting", attempts: 0 });
  });

  it.each(["heartbeat", "progress", "complete", "fail", "release"])("%s answers 409 lease_lost for a stale token", async (op) => {
    const job = await api.conveyor.enqueue({ queue: "q", name: "x" });
    await lease();
    const res = await api.req("POST", `/v1/jobs/${job.id}/${op}`, { body: { leaseToken: "stale", progress: 0.1 } });
    expect(res.status).toBe(409);
    expect(res.body).toEqual(errorBody("lease_lost", `lease for job ${job.id} is no longer held`));
  });

  it.each(["heartbeat", "progress", "complete", "fail", "release"])("%s answers 404 for an unknown job", async (op) => {
    const res = await api.req("POST", `/v1/jobs/nope/${op}`, { body: { leaseToken: "t", progress: 0.1 } });
    expect(res.status).toBe(404);
    expect(res.body).toEqual(errorBody("not_found", "job nope not found"));
  });

  it.each(["heartbeat", "progress", "complete", "fail", "release"])("%s answers 400 without a leaseToken", async (op) => {
    const res = await api.req("POST", `/v1/jobs/nope/${op}`, { body: { progress: 0.1 } });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", "leaseToken is required"));
  });

  it.each<[string, Record<string, unknown>, string]>([
    ["progress", { progress: "half" }, "progress must be a number"],
    ["fail", { error: 42 }, "error must be a string"],
    ["fail", { retryInMs: -1 }, "retryInMs must be >= 0"],
    ["fail", { unrecoverable: "yes" }, "unrecoverable must be a boolean"],
    ["fail", { retryInMs: 1e300 }, "retryInMs must be at most 3155760000000 (100 years)"],
  ])("%s rejects %j with 400", async (op, extra, message) => {
    const job = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const { leaseToken } = (await lease()).body;
    const res = await api.req("POST", `/v1/jobs/${job.id}/${op}`, { body: { leaseToken, ...extra } });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", message));
    expect((await api.conveyor.getJob(job.id)).state).toBe("active");
  });
});
