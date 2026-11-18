import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { hashToken } from "../../src/api/auth.js";
import { errorBody, startApi, type Api } from "./api-harness.js";

let api: Api;
beforeEach(async () => {
  api = await startApi();
});
afterEach(async () => {
  await api.close();
});

const T = "2026-01-05T12:00:00.000Z";
const schedule = (over: Record<string, unknown> = {}) => ({ name: "nightly", queue: "q", jobName: "report", cron: "0 3 * * *", ...over });

describe("schedules", () => {
  it("POST creates a schedule and returns it with 201", async () => {
    const res = await api.req("POST", "/v1/schedules", { body: schedule({ payload: { a: 1 }, timezone: "Europe/London", overlap: "skip", enabled: false }) });
    expect(res.status).toBe(201);
    expect(res.body).toEqual({
      id: expect.stringMatching(/^[0-9a-f-]{36}$/),
      name: "nightly",
      queue: "q",
      jobName: "report",
      payload: { a: 1 },
      cron: "0 3 * * *",
      timezone: "Europe/London",
      enabled: false,
      overlap: "skip",
      nextRunAt: "2026-01-06T03:00:00.000Z",
      lastRunAt: null,
      lastJobId: null,
      createdAt: T,
      updatedAt: T,
    });
  });

  it.each<[string, Record<string, unknown>, string | RegExp]>([
    ["a missing name", { name: undefined }, "name is required"],
    ["a missing cron", { cron: undefined }, "cron is required"],
    ["a bad cron", { cron: "99 * * * *" }, /99 is out of range for minute/],
    ["a cron that never runs", { cron: "0 0 30 2 *" }, 'cron expression "0 0 30 2 *" never runs'],
    ["an unknown timezone", { timezone: "Nowhere/Special" }, "unknown timezone Nowhere/Special"],
    // Previously a 500: the empty string skipped validation and failed later.
    ["an empty timezone", { timezone: "" }, "unknown timezone "],
    ["an unknown overlap policy", { overlap: "queue" }, "overlap must be 'allow' or 'skip'"],
    ["a non-boolean enabled", { enabled: "yes" }, "enabled must be a boolean"],
    ["an invalid job name", { jobName: "a b" }, "invalid job name"],
  ])("POST rejects %s with 400", async (_label, patch, message) => {
    const res = await api.req("POST", "/v1/schedules", { body: schedule(patch) });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", message));
    expect((await api.req("GET", "/v1/schedules")).body.items).toEqual([]);
  });

  it("POST answers 409 for a duplicate name", async () => {
    await api.req("POST", "/v1/schedules", { body: schedule() });
    const res = await api.req("POST", "/v1/schedules", { body: schedule() });
    expect(res.status).toBe(409);
    expect(res.body).toEqual(errorBody("conflict", "schedule nightly already exists"));
  });

  it("GET lists schedules by name", async () => {
    await api.req("POST", "/v1/schedules", { body: schedule({ name: "b" }) });
    await api.req("POST", "/v1/schedules", { body: schedule({ name: "a" }) });
    const res = await api.req("GET", "/v1/schedules");
    expect(res.status).toBe(200);
    expect(res.body.items.map((s: { name: string }) => s.name)).toEqual(["a", "b"]);
  });

  // Previously `upcoming` was computed from the wall clock rather than Conveyor's clock, so it
  // could disagree with the schedule's own nextRunAt.
  it("GET /:id includes the next five runs, starting from Conveyor's clock", async () => {
    const created = await api.req("POST", "/v1/schedules", { body: schedule({ cron: "0 */6 * * *" }) });
    const res = await api.req("GET", `/v1/schedules/${created.body.id}`);
    expect(res.status).toBe(200);
    expect(res.body.nextRunAt).toBe("2026-01-05T18:00:00.000Z");
    expect(res.body.upcoming).toEqual([
      "2026-01-05T18:00:00.000Z",
      "2026-01-06T00:00:00.000Z",
      "2026-01-06T06:00:00.000Z",
      "2026-01-06T12:00:00.000Z",
      "2026-01-06T18:00:00.000Z",
    ]);
  });

  it("PATCH changes only the given fields and recomputes the next run when the timing changes", async () => {
    const { id } = (await api.req("POST", "/v1/schedules", { body: schedule() })).body;
    const renamed = await api.req("PATCH", `/v1/schedules/${id}`, { body: { name: "renamed", payload: null } });
    expect(renamed.status).toBe(200);
    expect(renamed.body).toMatchObject({ name: "renamed", cron: "0 3 * * *", payload: null, nextRunAt: "2026-01-06T03:00:00.000Z" });
    const retimed = await api.req("PATCH", `/v1/schedules/${id}`, { body: { cron: "30 12 * * *" } });
    expect(retimed.body).toMatchObject({ name: "renamed", nextRunAt: "2026-01-05T12:30:00.000Z" });
  });

  it("PATCH answers 400, 404 and 409 like POST", async () => {
    const { id } = (await api.req("POST", "/v1/schedules", { body: schedule({ name: "a" }) })).body;
    await api.req("POST", "/v1/schedules", { body: schedule({ name: "b" }) });
    expect((await api.req("PATCH", `/v1/schedules/${id}`, { body: { cron: "nope" } })).status).toBe(400);
    expect((await api.req("PATCH", `/v1/schedules/${id}`, { body: { timezone: "" } })).status).toBe(400);
    expect((await api.req("PATCH", "/v1/schedules/nope", { body: {} })).status).toBe(404);
    const clash = await api.req("PATCH", `/v1/schedules/${id}`, { body: { name: "b" } });
    expect(clash.status).toBe(409);
    expect(clash.body).toEqual(errorBody("conflict", "schedule b already exists"));
  });

  it("POST /:id/trigger enqueues a run now", async () => {
    const { id } = (await api.req("POST", "/v1/schedules", { body: schedule({ payload: { manual: true } }) })).body;
    const res = await api.req("POST", `/v1/schedules/${id}/trigger`);
    expect(res.status).toBe(201);
    expect(await api.conveyor.getJob(res.body.jobId)).toMatchObject({ scheduleId: id, payload: { manual: true }, queue: "q", name: "report" });
    expect((await api.req("POST", "/v1/schedules/nope/trigger")).status).toBe(404);
  });

  it("DELETE removes a schedule, then answers 404", async () => {
    const { id } = (await api.req("POST", "/v1/schedules", { body: schedule() })).body;
    const res = await api.req("DELETE", `/v1/schedules/${id}`);
    expect(res.status).toBe(204);
    expect(res.text).toBe("");
    expect((await api.req("GET", `/v1/schedules/${id}`)).status).toBe(404);
    const again = await api.req("DELETE", `/v1/schedules/${id}`);
    expect(again.status).toBe(404);
    expect(again.body).toEqual(errorBody("not_found", `schedule ${id} not found`));
  });
});

describe("webhooks", () => {
  it("POST creates a webhook and returns its secret once", async () => {
    const res = await api.req("POST", "/v1/webhooks", { body: { url: "https://example.test/hook", events: ["job.completed"], queues: ["q"] } });
    expect(res.status).toBe(201);
    expect(res.body).toEqual({
      id: expect.any(String),
      url: "https://example.test/hook",
      events: ["job.completed"],
      queues: ["q"],
      enabled: true,
      createdAt: T,
      secret: expect.stringMatching(/^whsec_/),
    });
    const list = await api.req("GET", "/v1/webhooks");
    expect(list.status).toBe(200);
    expect(list.body.items).toEqual([{ ...res.body, secret: undefined }].map(({ secret: _s, ...rest }) => rest));
    expect(list.text).not.toContain(res.body.secret);
  });

  it.each<[string, Record<string, unknown>, string]>([
    ["a missing url", { events: ["*"] }, "url is required"],
    ["an invalid url", { url: "nope", events: ["*"] }, "url must be a valid URL"],
    ["no events", { url: "https://x.test" }, "events must be a non-empty array"],
    ["non-string events", { url: "https://x.test", events: [1] }, "events must be an array of strings"],
    ["an event that is never delivered", { url: "https://x.test", events: ["queue.paused"] }, "event queue.paused is not delivered to webhooks; use one of job.enqueued, job.completed, job.failed, job.retrying, job.dead, job.cancelled or *"],
    ["a non-string secret", { url: "https://x.test", events: ["*"], secret: 5 }, "secret must be a string"],
  ])("POST rejects %s with 400", async (_label, body, message) => {
    const res = await api.req("POST", "/v1/webhooks", { body });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", message));
  });

  it("GET /:id/deliveries lists deliveries, newest first", async () => {
    const { id } = (await api.req("POST", "/v1/webhooks", { body: { url: "https://example.test/hook", events: ["*"] } })).body;
    await api.webhooks.dispatch("job.completed", await api.conveyor.enqueue({ queue: "q", name: "x" }));
    const res = await api.req("GET", `/v1/webhooks/${id}/deliveries`);
    expect(res.status).toBe(200);
    expect(res.body.items).toEqual([expect.objectContaining({ webhookId: id, event: "job.completed", status: "succeeded", attempts: 1 })]);
    expect((await api.req("GET", "/v1/webhooks/nope/deliveries")).status).toBe(404);
  });

  it("DELETE removes a webhook, then answers 404", async () => {
    const { id } = (await api.req("POST", "/v1/webhooks", { body: { url: "https://example.test/hook", events: ["*"] } })).body;
    expect((await api.req("DELETE", `/v1/webhooks/${id}`)).status).toBe(204);
    expect((await api.req("GET", "/v1/webhooks")).body.items).toEqual([]);
    const again = await api.req("DELETE", `/v1/webhooks/${id}`);
    expect(again.status).toBe(404);
    expect(again.body).toEqual(errorBody("not_found", `webhook ${id} not found`));
  });
});

describe("tokens", () => {
  // Auth is on from the first request here (an admin token exists up front); see KB-24 for
  // what happens when the first token is created through the API.
  let authed: Api;
  beforeEach(async () => {
    authed = await startApi({ tokens: { root: ["admin"] } });
  });
  afterEach(async () => {
    await authed.close();
  });
  const asRoot = { as: "root" };

  it("POST creates a token and returns its secret once; GET never shows secrets or hashes", async () => {
    const res = await authed.req("POST", "/v1/tokens", { ...asRoot, body: { name: "ci", scopes: ["jobs:read", "jobs:write"] } });
    expect(res.status).toBe(201);
    expect(res.body).toMatchObject({ id: expect.any(String), name: "ci", scopes: ["jobs:read", "jobs:write"], lastUsedAt: null, revokedAt: null });
    expect(res.body.secret).toMatch(/^cvy_/);
    expect(res.body).not.toHaveProperty("hash");
    expect((await authed.store.findTokenByHash(hashToken(res.body.secret)))?.id).toBe(res.body.id);

    expect((await authed.req("GET", "/v1/tokens", { token: res.body.secret })).status).toBe(403); // not admin
    const all = await authed.req("GET", "/v1/tokens", asRoot);
    expect(all.status).toBe(200);
    expect(all.body.items.map((t: { name: string }) => t.name).sort()).toEqual(["ci", "root"]);
    expect(all.text).not.toContain(res.body.secret);
    expect(all.text).not.toContain(authed.secrets.root);
    for (const t of all.body.items) expect(t).not.toHaveProperty("hash");
  });

  it.each<[string, Record<string, unknown>, string]>([
    ["a missing name", { scopes: ["admin"] }, "name is required"],
    ["a long name", { name: "n".repeat(101), scopes: ["admin"] }, "name must be at most 100 characters"],
    ["no scopes", { name: "x" }, "at least one scope is required"],
    ["an unknown scope", { name: "x", scopes: ["root"] }, "unknown scope root"],
    ["non-string scopes", { name: "x", scopes: "admin" }, "scopes must be an array of strings"],
  ])("POST rejects %s with 400", async (_label, body, message) => {
    const res = await authed.req("POST", "/v1/tokens", { ...asRoot, body });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", message));
    expect(await authed.store.listTokens()).toHaveLength(1);
  });

  it("DELETE revokes a token: it stops working, and a second revoke is 404", async () => {
    const reader = (await authed.req("POST", "/v1/tokens", { ...asRoot, body: { name: "reader", scopes: ["jobs:read"] } })).body;
    expect((await authed.req("GET", "/v1/jobs", { token: reader.secret })).status).toBe(200);

    const res = await authed.req("DELETE", `/v1/tokens/${reader.id}`, asRoot);
    expect(res.status).toBe(204);
    expect((await authed.req("GET", "/v1/jobs", { token: reader.secret })).status).toBe(401);
    const listed = (await authed.req("GET", "/v1/tokens", asRoot)).body.items.find((t: { id: string }) => t.id === reader.id);
    expect(listed.revokedAt).toEqual(expect.any(String));

    const again = await authed.req("DELETE", `/v1/tokens/${reader.id}`, asRoot);
    expect(again.status).toBe(404);
    expect(again.body).toEqual(errorBody("not_found", `token ${reader.id} not found`));
  });
});
