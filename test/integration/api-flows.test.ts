// Contract tests for the flow routes and the dependency parts of the job routes, over real
// HTTP: status codes, bodies and error bodies. Every rejected request is checked to have
// stored nothing.
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
const JOB_FIELDS = [
  "id", "queue", "name", "state", "payload", "priority", "attempts", "maxAttempts", "progress", "result", "lastError",
  "uniqueKey", "scheduleId", "flowId", "flowKey", "terminatedBy", "timeoutMs", "runAt", "createdAt", "startedAt",
  "finishedAt", "workerId", "leaseExpiresAt",
].sort();

/** Everything stored: to check a rejected request stored nothing. */
async function stored() {
  const db = (api.store as import("../../src/storage/sqlite-store.js").SqliteStore).db;
  const rows = (sql: string) => JSON.stringify(db.prepare(sql).all());
  return { jobs: rows("SELECT * FROM jobs ORDER BY id"), events: rows("SELECT * FROM job_events ORDER BY id"), edges: rows("SELECT * FROM job_dependencies") };
}

async function deadJob() {
  const job = await api.conveyor.enqueue({ queue: "other", name: "x" });
  const lease = (await api.broker.claim(["other"], "w"))!;
  await api.broker.fail(job.id, lease.token, "boom", { unrecoverable: true });
  return job.id;
}

const node = (key: string, dependsOn?: unknown[], extra: Record<string, unknown> = {}) => ({ key, queue: "q", name: "x", ...(dependsOn ? { dependsOn } : {}), ...extra });

describe("POST /v1/flows", () => {
  it("creates the flow and returns it with 201", async () => {
    const existing = await api.conveyor.enqueue({ queue: "other", name: "x" });
    const res = await api.req("POST", "/v1/flows", {
      body: {
        onFailure: "continue",
        jobs: [
          node("report", ["fetch", { key: "resize", onFailure: "fail" }], { payload: { r: 1 }, priority: 3 }),
          node("resize", [{ jobId: existing.id }], { runAt: "2026-01-06T00:00:00Z" }),
          node("fetch", undefined, { delayMs: 1000, maxAttempts: 5 }),
        ],
      },
    });
    expect(res.status).toBe(201);
    expect(Object.keys(res.body).sort()).toEqual(["counts", "edges", "id", "jobs", "outcome", "status"]);
    expect(res.body).toMatchObject({ id: expect.stringMatching(/^[0-9a-f-]{36}$/), status: "running", outcome: null });
    expect(res.body.counts).toEqual({ blocked: 2, delayed: 1, waiting: 0, active: 0, completed: 0, dead: 0, cancelled: 0 });
    const jobs = res.body.jobs as Record<string, unknown>[];
    // resize waits only for a job outside the flow, so it is ready as early as fetch and comes
    // first, as it was given first.
    expect(jobs.map((j) => [j.flowKey, j.state])).toEqual([
      ["resize", "blocked"],
      ["fetch", "delayed"],
      ["report", "blocked"],
    ]);
    for (const j of jobs) {
      expect(Object.keys(j).sort()).toEqual(JOB_FIELDS);
      expect(j).toMatchObject({ flowId: res.body.id, createdAt: T, terminatedBy: null });
    }
    expect(jobs[1]).toMatchObject({ runAt: "2026-01-05T12:00:01.000Z", maxAttempts: 5 });
    expect(jobs[0]).toMatchObject({ runAt: "2026-01-06T00:00:00.000Z" });
    expect(jobs[2]).toMatchObject({ payload: { r: 1 }, priority: 3 });
    const id = Object.fromEntries(jobs.map((j) => [j.flowKey, j.id]));
    expect(res.body.edges).toEqual(
      expect.arrayContaining([
        { childId: id.report, parentId: id.fetch, onFailure: "continue" },
        { childId: id.report, parentId: id.resize, onFailure: "fail" },
        { childId: id.resize, parentId: existing.id, onFailure: "continue" },
      ]),
    );
    expect(res.body.edges).toHaveLength(3);
  });

  it.each<[string, () => Promise<unknown> | unknown, string | RegExp]>([
    ["a long cycle", () => ({ jobs: [node("A", ["D"]), node("B", ["A"]), node("C", ["B"]), node("D", ["C"])] }), "flow has a dependency cycle: A -> B -> C -> D -> A"],
    ["an unknown key", () => ({ jobs: [node("a", ["ghost"])] }), "job a depends on unknown key ghost"],
    ["an unknown existing job", () => ({ jobs: [node("a", [{ jobId: "no-such-job" }])] }), "dependency no-such-job not found"],
    ["a self-dependency", () => ({ jobs: [node("a", ["a"])] }), "job a depends on itself"],
    ["duplicate keys", () => ({ jobs: [node("a"), node("a")] }), "duplicate key a"],
    ["a duplicate edge", () => ({ jobs: [node("a"), node("b", ["a", "a"])] }), "job b lists dependency a twice"],
    ["more than 1000 jobs", () => ({ jobs: Array.from({ length: 1001 }, (_, i) => node(`j${i}`)) }), "a flow can have at most 1000 jobs"],
    ["a uniqueKey", () => ({ jobs: [node("a", undefined, { uniqueKey: "u" })] }), "job a: uniqueKey is not allowed in a flow"],
    ["an empty flow", () => ({ jobs: [] }), "a flow needs at least one job"],
    ["a body that is not an object", () => [node("a")], "body must be an object"],
    ["jobs that is not an array", () => ({ jobs: "a" }), "jobs must be an array"],
    ["a job that is not an object", () => ({ jobs: [node("a"), 7] }), "jobs[1] must be an object"],
    ["a job without a key", () => ({ jobs: [{ queue: "q", name: "x" }] }), "jobs[0]: key is required"],
    ["a job without a queue", () => ({ jobs: [node("a"), { key: "b", name: "x" }] }), "jobs[1]: queue is required"],
    ["an invalid runAt", () => ({ jobs: [node("a", undefined, { runAt: "tomorrow" })] }), "jobs[0]: runAt must be an ISO-8601 timestamp"],
    ["an invalid job name", () => ({ jobs: [node("a", undefined, { name: "-x" })] }), "job a: invalid job name"],
    ["an unknown policy", () => ({ onFailure: "sometimes", jobs: [node("a")] }), "onFailure must be one of fail, ignore, continue"],
  ])("is 400 for %s, and stores nothing", async (_label, body, message) => {
    await api.conveyor.enqueue({ queue: "q", name: "before" });
    const before = await stored();
    const res = await api.req("POST", "/v1/flows", { body: await body() });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", message));
    expect(await stored()).toEqual(before);
  });

  it("is 409 for a fail dependency on a job that is already dead or cancelled, and stores nothing", async () => {
    const dead = await deadJob();
    const cancelled = await api.conveyor.enqueue({ queue: "other", name: "x" });
    await api.conveyor.cancel(cancelled.id);
    for (const [id, state] of [
      [dead, "dead"],
      [cancelled.id, "cancelled"],
    ]) {
      const before = await stored();
      const res = await api.req("POST", "/v1/flows", { body: { jobs: [node("a"), node("b", ["a", { jobId: id }])] } });
      expect(res.status).toBe(409);
      expect(res.body).toEqual(errorBody("conflict", `dependency ${id} is already ${state}`));
      expect(await stored()).toEqual(before);
    }
  });

  it.each(["ignore", "continue"])("is 201 for a %s dependency on a dead job, which is settled straight away", async (onFailure) => {
    const dead = await deadJob();
    const res = await api.req("POST", "/v1/flows", { body: { jobs: [node("a", [{ jobId: dead, onFailure }])] } });
    expect(res.status).toBe(201);
    expect(res.body.jobs[0].state).toBe("waiting");
  });
});

describe("GET /v1/flows/:id", () => {
  it("returns the flow with its jobs, edges and derived status", async () => {
    const created = (await api.req("POST", "/v1/flows", { body: { jobs: [node("a"), node("b", ["a"])] } })).body;
    const res = await api.req("GET", `/v1/flows/${created.id}`);
    expect(res.status).toBe(200);
    expect(res.body).toEqual(created);

    for (let lease = await api.broker.claim(["q"], "w"); lease; lease = await api.broker.claim(["q"], "w")) {
      await api.broker.complete(lease.job.id, lease.token);
    }
    const done = await api.req("GET", `/v1/flows/${created.id}`);
    expect(done.body).toMatchObject({
      status: "completed",
      outcome: { event: "flow.completed", reason: null, jobId: null, at: T, dispatchedAt: null },
      counts: { completed: 2, blocked: 0 },
    });
  });

  it("is 404 for an unknown flow", async () => {
    const res = await api.req("GET", "/v1/flows/nope");
    expect(res.status).toBe(404);
    expect(res.body).toEqual(errorBody("not_found", "flow nope not found"));
  });
});

describe("POST /v1/flows/:id/cancel", () => {
  it("cancels the unfinished jobs and returns the flow", async () => {
    const created = (await api.req("POST", "/v1/flows", { body: { jobs: [node("a"), node("b", ["a"])] } })).body;
    const res = await api.req("POST", `/v1/flows/${created.id}/cancel`);
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      id: created.id,
      status: "cancelled",
      outcome: { event: "flow.failed", reason: "cancelled", jobId: null, at: T },
      counts: { cancelled: 2 },
    });
    expect(res.body.jobs.map((j: { state: string; finishedAt: string }) => [j.state, j.finishedAt])).toEqual([
      ["cancelled", T],
      ["cancelled", T],
    ]);
  });

  it("is 409 once every job has finished, and 404 for an unknown flow", async () => {
    const created = (await api.req("POST", "/v1/flows", { body: { jobs: [node("a")] } })).body;
    await api.req("POST", `/v1/flows/${created.id}/cancel`);
    const again = await api.req("POST", `/v1/flows/${created.id}/cancel`);
    expect(again.status).toBe(409);
    expect(again.body).toEqual(errorBody("conflict", `flow ${created.id} has already finished`));
    const unknown = await api.req("POST", "/v1/flows/nope/cancel");
    expect(unknown.status).toBe(404);
    expect(unknown.body).toEqual(errorBody("not_found", "flow nope not found"));
  });
});

describe("POST /v1/jobs with dependsOn", () => {
  it("creates a blocked job from ids and {jobId, onFailure}", async () => {
    const a = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const b = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const res = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "y", dependsOn: [a.id, { jobId: b.id, onFailure: "continue" }] } });
    expect(res.status).toBe(201);
    expect(res.body).toMatchObject({ state: "blocked", flowId: null, terminatedBy: null });
    expect(await api.conveyor.store.listDependencies(res.body.id)).toEqual(
      [
        { jobId: a.id, onFailure: "fail" },
        { jobId: b.id, onFailure: "continue" },
      ].sort((x, y) => x.jobId.localeCompare(y.jobId)),
    );
  });

  it("accepts a null dependsOn as none", async () => {
    const res = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "y", dependsOn: null } });
    expect(res.body.state).toBe("waiting");
  });

  it.each<[string, unknown, number, string, string | RegExp]>([
    ["dependsOn that is not an array", "abc", 400, "validation_error", "dependsOn must be an array"],
    ["an unknown policy", [{ jobId: "x", onFailure: "maybe" }], 400, "validation_error", "onFailure must be one of fail, ignore, continue"],
    ["an unknown job", ["no-such-job"], 400, "validation_error", "dependency no-such-job not found"],
  ])("is 400 for %s, and stores nothing", async (_label, dependsOn, status, code, message) => {
    const before = await stored();
    const res = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "y", dependsOn } });
    expect(res.status).toBe(status);
    expect(res.body).toEqual(errorBody(code, message));
    expect(await stored()).toEqual(before);
  });

  it("is 409 for a fail dependency on a dead job", async () => {
    const dead = await deadJob();
    const res = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "y", dependsOn: [dead] } });
    expect(res.status).toBe(409);
    expect(res.body).toEqual(errorBody("conflict", `dependency ${dead} is already dead`));
  });
});

describe("POST /v1/jobs/bulk with dependsOn", () => {
  it("creates every job with its dependencies", async () => {
    const p = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const res = await api.req("POST", "/v1/jobs/bulk", {
      body: { jobs: [{ queue: "q", name: "a", dependsOn: [p.id] }, { queue: "q", name: "b" }] },
    });
    expect(res.status).toBe(201);
    expect(res.body.jobs.map((j: { state: string }) => j.state)).toEqual(["blocked", "waiting"]);
  });

  it("stores none of the jobs if one has an unknown dependency", async () => {
    const before = await stored();
    const res = await api.req("POST", "/v1/jobs/bulk", {
      body: { jobs: [{ queue: "q", name: "a" }, { queue: "q", name: "b", dependsOn: ["no-such-job"] }] },
    });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", "dependency no-such-job not found"));
    expect(await stored()).toEqual(before);
  });
});

// GET /v1/jobs/:id/dependencies is covered in api-dependencies.test.ts.

describe("GET /v1/jobs/:id/dependents", () => {
  it("lists the jobs that depend on it, newest first, in pages", async () => {
    const p = await api.conveyor.enqueue({ queue: "q", name: "x" });
    const kids = [];
    for (let i = 0; i < 3; i++) kids.push(await api.conveyor.enqueue({ queue: "q", name: "k", dependsOn: [{ jobId: p.id, onFailure: "continue" }] }));
    const first = await api.req("GET", `/v1/jobs/${p.id}/dependents?limit=2`);
    expect(first.status).toBe(200);
    expect(first.body.items.map((l: { job: { id: string }; onFailure: string }) => [l.job.id, l.onFailure])).toEqual([
      [kids[2]!.id, "continue"],
      [kids[1]!.id, "continue"],
    ]);
    expect(Object.keys(first.body.items[0].job).sort()).toEqual(JOB_FIELDS);
    expect(first.body.nextCursor).toBe(kids[1]!.id);
    const rest = await api.req("GET", `/v1/jobs/${p.id}/dependents?limit=2&cursor=${first.body.nextCursor}`);
    expect(rest.body).toEqual({ items: [expect.objectContaining({ job: expect.objectContaining({ id: kids[0]!.id }) })], nextCursor: null });
  });

  it("is 400 for an invalid limit and 404 for an unknown job", async () => {
    const p = await api.conveyor.enqueue({ queue: "q", name: "x" });
    for (const limit of ["0", "abc", "1.5"]) {
      const res = await api.req("GET", `/v1/jobs/${p.id}/dependents?limit=${limit}`);
      expect(res.status).toBe(400);
      expect(res.body).toEqual(errorBody("validation_error", "limit must be a positive integer"));
    }
    const res = await api.req("GET", "/v1/jobs/nope/dependents");
    expect(res.status).toBe(404);
    expect(res.body).toEqual(errorBody("not_found", "job nope not found"));
  });
});
