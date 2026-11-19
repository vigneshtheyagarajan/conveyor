// What an HTTP worker sees of a job's dependencies: the summaries in the lease response, and
// the results from GET /v1/jobs/:id/dependencies. The same scenario as the in-process tests
// (dependency-results.test.ts) must give the same view.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { isVisibleToDependent } from "../../src/core/dependencies.js";
import { UnrecoverableError } from "../../src/core/errors.js";
import type { Job } from "../../src/core/types.js";
import { HandlerRegistry } from "../../src/worker/handlers.js";
import { Worker } from "../../src/worker/worker.js";
import { setup } from "../helpers.js";
import { errorBody, startApi, type Api } from "./api-harness.js";
import { byKeys, EXPECTED_STATES, EXPECTED_VIEWS, POLICY_FLOW } from "./dependency-scenario.js";

let api: Api;
beforeEach(async () => {
  api = await startApi();
});
afterEach(async () => {
  await api.close();
});

const RESULT_FIELDS = ["id", "key", "name", "queue", "state", "onFailure", "result", "error"].sort();

/** Run a flow to the end as an HTTP worker would, recording what each job was given. */
async function runOverHttp(flowId: string) {
  const leases: Record<string, unknown> = {};
  const results: Record<string, unknown> = {};
  for (;;) {
    const res = await api.req("POST", "/v1/leases", { body: { queues: ["q"], workerId: "http-worker" } });
    if (res.status === 204) break;
    expect(res.status).toBe(201);
    const { job, leaseToken, dependencies } = res.body;
    leases[job.flowKey] = dependencies;
    if (job.name === "produce") {
      await api.req("POST", `/v1/jobs/${job.id}/complete`, { body: { leaseToken, result: { value: job.payload.value } } });
    } else if (job.name === "explode") {
      await api.req("POST", `/v1/jobs/${job.id}/fail`, { body: { leaseToken, error: "boom", unrecoverable: true } });
    } else {
      const deps = await api.req("GET", `/v1/jobs/${job.id}/dependencies`);
      expect(deps.status).toBe(200);
      expect(deps.body.nextCursor).toBeNull();
      results[job.flowKey] = deps.body.items;
      await api.req("POST", `/v1/jobs/${job.id}/complete`, { body: { leaseToken, result: null } });
    }
  }
  return { leases, results, flow: (await api.req("GET", `/v1/flows/${flowId}`)).body as { jobs: Job[] } };
}

describe("an HTTP worker sees what an in-process handler sees", () => {
  it("each policy's view of a dead dependency, in the lease and from /dependencies", async () => {
    const created = (await api.req("POST", "/v1/flows", { body: POLICY_FLOW })).body;
    const { leases, results, flow } = await runOverHttp(created.id);
    expect(Object.fromEntries(flow.jobs.map((j) => [j.flowKey, j.state]))).toEqual(EXPECTED_STATES);

    // The same expectations as the in-process test.
    expect(byKeys(flow.jobs, results.viaIgnore)).toEqual(EXPECTED_VIEWS.viaIgnore);
    expect(byKeys(flow.jobs, results.viaContinue)).toEqual(EXPECTED_VIEWS.viaContinue);
    expect(results).not.toHaveProperty("viaFail"); // never leased
    // The lease lists the same dependencies, without results.
    for (const key of ["viaIgnore", "viaContinue"] as const) {
      expect(leases[key]).toEqual((results[key] as Record<string, unknown>[]).map(({ id, key: k, name, state }) => ({ id, key: k, name, state })));
    }
    expect(leases.ok).toEqual([]);

    // ?all=true also shows the failed dependency an ignore dependent doesn't see.
    const viaIgnore = flow.jobs.find((j) => j.flowKey === "viaIgnore")!;
    const all = (await api.req("GET", `/v1/jobs/${viaIgnore.id}/dependencies?all=true`)).body.items;
    expect(byKeys(flow.jobs, all)).toEqual([...EXPECTED_VIEWS.viaIgnore, { ...EXPECTED_VIEWS.viaContinue[1], onFailure: "ignore" }]);
  });

  it("and the two views are identical when run side by side", async () => {
    // In-process: a real worker on its own database, recording ctx.dependencies().
    const local = await setup();
    const seen: Record<string, unknown> = {};
    const registry = new HandlerRegistry()
      .register("produce", ({ job }) => ({ value: (job.payload as { value: number }).value }))
      .register("explode", () => {
        throw new UnrecoverableError("boom");
      })
      .register("read", async ({ job, dependencies }) => {
        seen[job.flowKey!] = await dependencies();
        return null;
      });
    const worker = new Worker(local.broker, { queues: ["q"], handlers: registry, pollMs: 1, maxPollMs: 5 });
    const localFlow = await local.conveyor.enqueueFlow(POLICY_FLOW);
    const done = new Promise<void>((resolve) => {
      const check = async () => {
        if ((await local.conveyor.getFlow(localFlow.id)).status !== "running") resolve();
      };
      for (const e of ["job.completed", "job.dead", "job.cancelled"] as const) local.conveyor.events.on(e, check);
    });
    worker.start();
    await done;
    await worker.stop();
    const localJobs = (await local.conveyor.getFlow(localFlow.id)).jobs;

    // Over HTTP.
    const created = (await api.req("POST", "/v1/flows", { body: POLICY_FLOW })).body;
    const { results, flow } = await runOverHttp(created.id);

    expect(byKeys(flow.jobs, results)).toEqual(byKeys(localJobs, seen));
  });
});

describe("retrying a job whose dependency retention deleted", () => {
  it("is 409 for POST /v1/jobs/:id/retry, naming the missing job; retry-dead lists it as skipped", async () => {
    const local = await startApi({ config: { retentionMs: 1000 } });
    try {
      const parent = await local.conveyor.enqueue({ queue: "pre", name: "x" });
      const child = await local.conveyor.enqueue({ queue: "q", name: "x", dependsOn: [parent.id], maxAttempts: 1 });
      const p = (await local.broker.claim(["pre"], "w"))!;
      await local.broker.complete(parent.id, p.token);
      const c = (await local.broker.claim(["q"], "w"))!;
      await local.broker.fail(child.id, c.token, "boom");
      local.clock.advance(5000);
      expect(await local.conveyor.store.deleteFinishedBefore(local.clock.now() - 1000, 100)).toBe(1);

      const message = `job ${child.id} can't be retried: its dependency ${parent.id} no longer exists`;
      const res = await local.req("POST", `/v1/jobs/${child.id}/retry`);
      expect(res.status).toBe(409);
      expect(res.body).toEqual(errorBody("conflict", message));
      const all = await local.req("POST", "/v1/queues/q/retry-dead");
      expect(all.body).toEqual({ retried: 0, skipped: [{ jobId: child.id, error: message }] });
      expect((await local.conveyor.getJob(child.id)).state).toBe("dead");
    } finally {
      await local.close();
    }
  });
});

describe("POST /v1/leases", () => {
  it("lists the job's dependencies as it sees them, without results", async () => {
    const a = await api.conveyor.enqueue({ queue: "pre", name: "alpha" });
    const b = await api.conveyor.enqueue({ queue: "pre", name: "beta" });
    const dead = await api.conveyor.enqueue({ queue: "pre", name: "gamma" });
    const child = await api.conveyor.enqueue({
      queue: "q",
      name: "x",
      dependsOn: [a.id, { jobId: b.id, onFailure: "continue" }, { jobId: dead.id, onFailure: "ignore" }],
    });
    for (const id of [a.id, b.id]) {
      const l = (await api.broker.claim(["pre"], "w"))!;
      await api.broker.complete(id, l.token, { secret: "not in the lease" });
    }
    const l = (await api.broker.claim(["pre"], "w"))!;
    await api.broker.fail(dead.id, l.token, "boom", { unrecoverable: true });

    const res = await api.req("POST", "/v1/leases", { body: { queues: ["q"], workerId: "w" } });
    expect(res.body.job.id).toBe(child.id);
    expect(res.body.dependencies).toEqual([
      { id: a.id, key: null, name: "alpha", state: "completed" },
      { id: b.id, key: null, name: "beta", state: "completed" },
    ]);
    expect(res.text).not.toContain("not in the lease");
  });
});

describe("GET /v1/jobs/:id/dependencies", () => {
  async function childOf(n: number) {
    const parents: Job[] = [];
    for (let i = 0; i < n; i++) parents.push(await api.conveyor.enqueue({ queue: "pre", name: `p${i}` }));
    for (const p of parents) {
      const l = (await api.broker.claim(["pre"], "w"))!;
      await api.broker.complete(p.id, l.token, { n: p.name });
    }
    const child = await api.conveyor.enqueue({ queue: "q", name: "x", dependsOn: parents.map((p) => p.id) });
    return { parents, child };
  }

  it("returns each dependency's result, in pages ordered by id", async () => {
    const { parents, child } = await childOf(5);
    const first = await api.req("GET", `/v1/jobs/${child.id}/dependencies?limit=2`);
    expect(first.status).toBe(200);
    expect(first.body.items.map((d: { id: string }) => d.id)).toEqual([parents[0]!.id, parents[1]!.id]);
    expect(Object.keys(first.body.items[0]).sort()).toEqual(RESULT_FIELDS);
    expect(first.body.items[0]).toEqual({ id: parents[0]!.id, key: null, name: "p0", queue: "pre", state: "completed", onFailure: "fail", result: { n: "p0" }, error: null });
    expect(first.body.nextCursor).toBe(parents[1]!.id);

    const seen = [...first.body.items];
    for (let cursor = first.body.nextCursor; cursor; ) {
      const page = await api.req("GET", `/v1/jobs/${child.id}/dependencies?limit=2&cursor=${cursor}`);
      seen.push(...page.body.items);
      cursor = page.body.nextCursor;
    }
    expect(seen.map((d) => d.id)).toEqual(parents.map((p) => p.id));
    expect((await api.req("GET", `/v1/jobs/${child.id}/dependencies`)).body.items).toHaveLength(5);
  });

  it("pages what the job sees: failed ignore dependencies don't leave gaps in a page", async () => {
    const parents: Job[] = [];
    for (let i = 0; i < 4; i++) parents.push(await api.conveyor.enqueue({ queue: "pre", name: `p${i}` }));
    const child = await api.conveyor.enqueue({ queue: "q", name: "x", dependsOn: parents.map((p) => ({ jobId: p.id, onFailure: "ignore" as const })) });
    for (const [i, p] of parents.entries()) {
      const l = (await api.broker.claim(["pre"], "w"))!;
      if (i % 2 === 0) await api.broker.fail(p.id, l.token, "boom", { unrecoverable: true });
      else await api.broker.complete(p.id, l.token);
    }
    const page = await api.req("GET", `/v1/jobs/${child.id}/dependencies?limit=2`);
    expect(page.body).toEqual({ items: [expect.objectContaining({ id: parents[1]!.id }), expect.objectContaining({ id: parents[3]!.id })], nextCursor: null });
    const all = await api.req("GET", `/v1/jobs/${child.id}/dependencies?all=true&limit=10`);
    expect(all.body.items.map((d: { id: string }) => d.id)).toEqual(parents.map((p) => p.id));
    expect((await api.req("GET", `/v1/jobs/${child.id}/dependencies?all=false`)).body.items).toHaveLength(2);
    // The filter is isVisibleToDependent's.
    expect(all.body.items.filter(isVisibleToDependent)).toEqual(page.body.items);
  });

  it("is empty for a job without dependencies", async () => {
    const job = await api.conveyor.enqueue({ queue: "q", name: "x" });
    expect((await api.req("GET", `/v1/jobs/${job.id}/dependencies`)).body).toEqual({ items: [], nextCursor: null });
  });

  it("is 400 for an invalid limit or all, and 404 for an unknown job", async () => {
    const { child } = await childOf(1);
    for (const q of ["limit=0", "limit=abc", "limit=2.5"]) {
      const res = await api.req("GET", `/v1/jobs/${child.id}/dependencies?${q}`);
      expect(res.status).toBe(400);
      expect(res.body).toEqual(errorBody("validation_error", "limit must be a positive integer"));
    }
    const bad = await api.req("GET", `/v1/jobs/${child.id}/dependencies?all=yes`);
    expect(bad.status).toBe(400);
    expect(bad.body).toEqual(errorBody("validation_error", "all must be true or false"));
    const missing = await api.req("GET", "/v1/jobs/nope/dependencies");
    expect(missing.status).toBe(404);
    expect(missing.body).toEqual(errorBody("not_found", "job nope not found"));
  });

  it("caps a page at 500", async () => {
    const parents = await api.conveyor.enqueueBulk(Array.from({ length: 501 }, () => ({ queue: "pre", name: "p" })));
    const child = await api.conveyor.enqueue({ queue: "q", name: "x", dependsOn: parents.map((p) => p.id) });
    const page = await api.req("GET", `/v1/jobs/${child.id}/dependencies?limit=100000`);
    expect(page.body.items).toHaveLength(500);
    expect(page.body.nextCursor).toBe(parents[499]!.id);
    expect((await api.req("GET", `/v1/jobs/${child.id}/dependencies?cursor=${page.body.nextCursor}`)).body.items.map((d: { id: string }) => d.id)).toEqual([parents[500]!.id]);
  });
});
