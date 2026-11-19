import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { errorBody, startApi, type Api } from "./api-harness.js";

let api: Api;
beforeEach(async () => {
  api = await startApi();
});
afterEach(async () => {
  await api.close();
});

const T = "2026-01-05T12:00:00.000Z";

describe("POST /v1/jobs", () => {
  it("creates a job and returns it with 201", async () => {
    const res = await api.req("POST", "/v1/jobs", {
      body: {
        queue: "emails",
        name: "send",
        payload: { to: "a@example.test" },
        priority: 5,
        maxAttempts: 7,
        timeoutMs: 3000,
        uniqueKey: "user:1",
        backoff: { type: "fixed", delayMs: 100 },
      },
    });
    expect(res.status).toBe(201);
    expect(res.body).toEqual({
      id: expect.stringMatching(/^[0-9a-z]{20}$/),
      queue: "emails",
      name: "send",
      state: "waiting",
      payload: { to: "a@example.test" },
      priority: 5,
      attempts: 0,
      maxAttempts: 7,
      progress: 0,
      result: null,
      lastError: null,
      uniqueKey: "user:1",
      scheduleId: null,
      flowId: null,
      flowKey: null,
      terminatedBy: null,
      timeoutMs: 3000,
      runAt: T,
      createdAt: T,
      startedAt: null,
      finishedAt: null,
      workerId: null,
      leaseExpiresAt: null,
    });
    expect((await api.conveyor.getJob(res.body.id)).backoff).toEqual({ type: "fixed", delayMs: 100 });
  });

  it("delays with delayMs or runAt", async () => {
    const delayed = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x", delayMs: 60_000 } });
    expect(delayed.body).toMatchObject({ state: "delayed", runAt: "2026-01-05T12:01:00.000Z" });
    const scheduled = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x", runAt: "2026-01-06T00:00:00Z" } });
    expect(scheduled.body).toMatchObject({ state: "delayed", runAt: "2026-01-06T00:00:00.000Z" });
  });

  it("returns the existing job for a duplicate unique key", async () => {
    const a = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x", uniqueKey: "k" } });
    const b = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x", uniqueKey: "k" } });
    expect(b.body.id).toBe(a.body.id);
  });

  it.each<[string, Record<string, unknown>, string]>([
    ["a missing queue", { name: "x" }, "queue is required"],
    ["a missing name", { queue: "q" }, "name is required"],
    ["an empty name", { queue: "q", name: "" }, "name must not be empty"],
    ["an invalid queue name", { queue: "a b", name: "x" }, "invalid queue name"],
    ["a non-integer priority", { queue: "q", name: "x", priority: 1.5 }, "priority must be an integer"],
    ["a priority out of range", { queue: "q", name: "x", priority: 5000 }, "priority must be an integer between -1000 and 1000"],
    ["a negative delay", { queue: "q", name: "x", delayMs: -1 }, "delayMs must be >= 0"],
    ["a string delay", { queue: "q", name: "x", delayMs: "10" }, "delayMs must be an integer"],
    ["too many attempts", { queue: "q", name: "x", maxAttempts: 101 }, "maxAttempts must be <= 100"],
    ["zero attempts", { queue: "q", name: "x", maxAttempts: 0 }, "maxAttempts must be >= 1"],
    ["a negative timeout", { queue: "q", name: "x", timeoutMs: -5 }, "timeoutMs must be >= 0"],
    ["a long unique key", { queue: "q", name: "x", uniqueKey: "k".repeat(257) }, "uniqueKey must be at most 256 characters"],
    ["an unparseable runAt", { queue: "q", name: "x", runAt: "tomorrow" }, "runAt must be an ISO-8601 timestamp"],
    ["a numeric runAt", { queue: "q", name: "x", runAt: 123 }, "runAt must be a string"],
    ["an unknown backoff", { queue: "q", name: "x", backoff: { type: "random" } }, "unknown backoff type: random"],
    ["a non-boolean jitter", { queue: "q", name: "x", backoff: { type: "exponential", baseMs: 1, maxMs: 2, jitter: "no" } }, "backoff.jitter must be a boolean"],
  ])("rejects %s with 400", async (_label, body, message) => {
    const res = await api.req("POST", "/v1/jobs", { body });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", message));
    expect((await api.conveyor.listJobs({})).items).toEqual([]);
  });

  it("rejects a payload over the size limit with 400", async () => {
    // The body limit is 4x the payload limit (120 bytes here), so this reaches the payload check.
    const small = await startApi({ config: { maxPayloadBytes: 30 } });
    try {
      const res = await small.req("POST", "/v1/jobs", { body: { queue: "q", name: "x", payload: "x".repeat(40) } });
      expect(res.status).toBe(400);
      expect(res.body).toEqual(errorBody("validation_error", "payload is 42 bytes; limit is 30"));
    } finally {
      await small.close();
    }
  });

  // Regression (was KB-25): delayMs 1e300 passed validation; the job was stored with a run_at
  // no Date can represent, and rendering it threw (HTTP 500) after the job had been created.
  it.each<[string, Record<string, unknown>, string]>([
    ["a delay of 1e300", { delayMs: 1e300 }, "delayMs must be at most 3155760000000 (100 years)"],
    ["a runAt after 9999", { runAt: "+010000-01-01T00:00:00.000Z" }, "runAt must be between 1970-01-01T00:00:00.000Z and 9999-12-31T23:59:59.999Z"],
    ["the largest date JavaScript has", { runAt: "+275760-09-13T00:00:00.000Z" }, "runAt must be between 1970-01-01T00:00:00.000Z and 9999-12-31T23:59:59.999Z"],
    ["a huge backoff", { backoff: { type: "fixed", delayMs: 1e300 } }, "backoff.delayMs must be at most 3155760000000 (100 years)"],
  ])("rejects %s with 400 and creates nothing", async (_label, extra, message) => {
    const res = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x", ...extra } });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", message));
    expect((await api.conveyor.listJobs({})).items).toEqual([]);
  });

  it("accepts a runAt at the very end of 9999", async () => {
    const res = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x", runAt: "9999-12-31T23:59:59.999Z" } });
    expect(res.status).toBe(201);
    expect(res.body.runAt).toBe("9999-12-31T23:59:59.999Z");
  });
});

describe("GET /v1/jobs", () => {
  async function seed() {
    const ids: string[] = [];
    for (let i = 0; i < 7; i++) {
      const queue = i % 2 ? "b" : "a";
      ids.push((await api.conveyor.enqueue({ queue, name: i < 5 ? "x" : "y" })).id);
      api.clock.advance(1);
    }
    await api.conveyor.cancel(ids[0]!);
    return ids;
  }

  it("lists newest first, with a cursor for the next page", async () => {
    const ids = await seed();
    const first = await api.req("GET", "/v1/jobs?limit=3");
    expect(first.status).toBe(200);
    expect(first.body.items.map((j: { id: string }) => j.id)).toEqual([ids[6], ids[5], ids[4]]);
    expect(first.body.nextCursor).toBe(ids[4]);
    const seen = [...first.body.items];
    let cursor = first.body.nextCursor;
    while (cursor) {
      const page = await api.req("GET", `/v1/jobs?limit=3&cursor=${cursor}`);
      seen.push(...page.body.items);
      cursor = page.body.nextCursor;
    }
    expect(seen.map((j) => j.id)).toEqual([...ids].reverse());
  });

  it("filters by queue, state, name and schedule", async () => {
    const ids = await seed();
    const byQueue = await api.req("GET", "/v1/jobs?queue=b");
    expect(byQueue.body.items.map((j: { id: string }) => j.id)).toEqual([ids[5], ids[3], ids[1]]);
    const cancelled = await api.req("GET", "/v1/jobs?state=cancelled");
    expect(cancelled.body.items.map((j: { id: string }) => j.id)).toEqual([ids[0]]);
    const named = await api.req("GET", "/v1/jobs?name=y&queue=a");
    expect(named.body.items.map((j: { id: string }) => j.id)).toEqual([ids[6]]);
    expect((await api.req("GET", "/v1/jobs?scheduleId=none")).body).toEqual({ items: [], nextCursor: null });
  });

  it("never exposes lease tokens", async () => {
    await api.conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await api.broker.claim(["q"], "w");
    const res = await api.req("GET", "/v1/jobs");
    expect(res.text).not.toContain(lease!.token);
    expect(res.body.items[0]).not.toHaveProperty("leaseToken");
  });

  it("caps the page size at 500", async () => {
    await api.conveyor.enqueueBulk(Array.from({ length: 501 }, () => ({ queue: "q", name: "x" })));
    const res = await api.req("GET", "/v1/jobs?limit=1000");
    expect(res.body.items).toHaveLength(500);
    expect(res.body.nextCursor).not.toBeNull();
  });

  it.each([["0"], ["-1"], ["1.5"], ["abc"], [""]])("rejects limit=%s with 400", async (limit) => {
    const res = await api.req("GET", `/v1/jobs?limit=${limit}`);
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", "limit must be a positive integer"));
  });

  it("rejects an unknown state with 400", async () => {
    const res = await api.req("GET", "/v1/jobs?state=finished");
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", "unknown state finished"));
  });
});

describe("GET /v1/jobs/:id and /events", () => {
  it("returns the job and its history", async () => {
    const job = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const lease = await api.broker.claim(["q"], "w1");
    await api.broker.complete(job.id, lease!.token, { ok: true });
    const res = await api.req("GET", `/v1/jobs/${job.id}`);
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ id: job.id, state: "completed", result: { ok: true }, workerId: "w1", finishedAt: T });
    const events = await api.req("GET", `/v1/jobs/${job.id}/events`);
    expect(events.status).toBe(200);
    expect(events.body.items.map((e: { type: string }) => e.type)).toEqual(["enqueued", "claimed", "completed"]);
    expect(events.body.items[1]).toMatchObject({ jobId: job.id, data: { workerId: "w1", attempt: 1 }, at: Date.parse(T) });
  });

  it("returns 404 for an unknown job", async () => {
    for (const path of ["/v1/jobs/nope", "/v1/jobs/nope/events"]) {
      const res = await api.req("GET", path);
      expect(res.status).toBe(404);
      expect(res.body).toEqual(errorBody("not_found", "job nope not found"));
    }
  });
});

describe("POST /v1/jobs/:id/cancel and /retry", () => {
  it("cancels an unfinished job, then retries it", async () => {
    const job = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const cancelled = await api.req("POST", `/v1/jobs/${job.id}/cancel`);
    expect(cancelled.status).toBe(200);
    expect(cancelled.body).toMatchObject({ id: job.id, state: "cancelled", finishedAt: T });
    const retried = await api.req("POST", `/v1/jobs/${job.id}/retry`);
    expect(retried.status).toBe(200);
    expect(retried.body).toMatchObject({ id: job.id, state: "waiting", attempts: 0, finishedAt: null });
  });

  it("answers 409 when the job is in the wrong state", async () => {
    const job = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const retry = await api.req("POST", `/v1/jobs/${job.id}/retry`);
    expect(retry.status).toBe(409);
    expect(retry.body).toEqual(errorBody("conflict", "only dead or cancelled jobs can be retried (job is waiting)"));
    await api.conveyor.cancel(job.id);
    const cancel = await api.req("POST", `/v1/jobs/${job.id}/cancel`);
    expect(cancel.status).toBe(409);
    expect(cancel.body).toEqual(errorBody("conflict", `job ${job.id} is already cancelled`));
  });

  it("answers 404 for an unknown job", async () => {
    expect((await api.req("POST", "/v1/jobs/nope/cancel")).status).toBe(404);
    expect((await api.req("POST", "/v1/jobs/nope/retry")).status).toBe(404);
  });
});
