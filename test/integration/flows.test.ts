// Flows through Conveyor: enqueueFlow, getFlow, cancelFlow, and the job dependency queries.
// Rejected flows are checked against a database file through a second connection, to show
// that nothing at all was written.
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Broker } from "../../src/broker/broker.js";
import { Conveyor } from "../../src/core/conveyor.js";
import { ConflictError, LeaseLostError, NotFoundError, ValidationError } from "../../src/core/errors.js";
import type { EventName } from "../../src/core/events.js";
import type { FlowJobInput, FlowRequest, Job } from "../../src/core/types.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import { manualClock, tempDir } from "../helpers.js";

let tmp: ReturnType<typeof tempDir>;
let file: string;
let store: SqliteStore;
let conveyor: Conveyor;
let broker: Broker;
let observer: DatabaseSync;
let events: [EventName, string][];

beforeEach(async () => {
  tmp = tempDir();
  file = tmp.file();
  store = new SqliteStore(file);
  conveyor = new Conveyor({ store, clock: manualClock() });
  await conveyor.init();
  broker = new Broker(conveyor);
  observer = new DatabaseSync(file);
  events = [];
  conveyor.events.onAny((event, first) => events.push([event, (first as Job).id ?? (first as { flowId: string }).flowId]));
});

afterEach(async () => {
  observer.close();
  await store.close();
  tmp.cleanup();
});

const node = (key: string, dependsOn?: FlowJobInput["dependsOn"], extra: Partial<FlowJobInput> = {}): FlowJobInput => ({
  key,
  queue: "q",
  name: "x",
  ...(dependsOn ? { dependsOn } : {}),
  ...extra,
});
const byKey = (jobs: Job[]) => Object.fromEntries(jobs.map((j) => [j.flowKey, j]));
async function running(queue = "q") {
  const job = await conveyor.enqueue({ queue, name: "outside" });
  const lease = (await broker.claim([queue], "w"))!;
  expect(lease.job.id).toBe(job.id);
  return { id: job.id, token: lease.token };
}

/**
 * What another connection sees of the database: SQLite's data_version (which changes whenever
 * any other connection commits), the main file's bytes, and every row of the job tables. The
 * WAL file is left out: a large transaction that is rolled back can spill uncommitted pages
 * into it, which are never committed and are overwritten later.
 */
function fingerprint() {
  const rows = (sql: string) => JSON.stringify(observer.prepare(sql).all());
  return {
    dataVersion: (observer.prepare("PRAGMA data_version").get() as { data_version: number }).data_version,
    file: createHash("sha256").update(readFileSync(file)).digest("hex"),
    jobs: rows("SELECT * FROM jobs ORDER BY id"),
    events: rows("SELECT * FROM job_events ORDER BY id"),
    edges: rows("SELECT * FROM job_dependencies ORDER BY child_id, parent_id"),
  };
}

describe("enqueueFlow", () => {
  it("stores the jobs in dependency order, blocked on their dependencies, and returns the flow", async () => {
    const flow = await conveyor.enqueueFlow({
      jobs: [node("publish", ["thumb", { key: "preview", onFailure: "continue" }]), node("thumb", ["fetch"]), node("preview", ["fetch"]), node("fetch", undefined, { payload: { url: "u" } })],
    });
    expect(flow.status).toBe("running");
    expect(flow.jobs.map((j) => [j.flowKey, j.state])).toEqual([
      ["fetch", "waiting"],
      ["thumb", "blocked"],
      ["preview", "blocked"],
      ["publish", "blocked"],
    ]);
    // Ids sort by creation, and jobs are created in dependency order.
    expect(flow.jobs.map((j) => j.id)).toEqual(flow.jobs.map((j) => j.id).sort());
    expect(flow.jobs.every((j) => j.flowId === flow.id)).toBe(true);
    expect(flow.counts).toMatchObject({ waiting: 1, blocked: 3 });
    const k = byKey(flow.jobs);
    expect(flow.edges).toEqual(
      [
        { childId: k.thumb!.id, parentId: k.fetch!.id, onFailure: "fail" },
        { childId: k.preview!.id, parentId: k.fetch!.id, onFailure: "fail" },
        { childId: k.publish!.id, parentId: k.thumb!.id, onFailure: "fail" },
        { childId: k.publish!.id, parentId: k.preview!.id, onFailure: "continue" },
      ].sort((a, b) => `${a.childId}${a.parentId}`.localeCompare(`${b.childId}${b.parentId}`)),
    );
    expect(await conveyor.getFlow(flow.id)).toEqual(flow);
    expect((await conveyor.jobEvents(k.thumb!.id)).map((e) => [e.type, e.data])).toEqual([["enqueued", { state: "blocked", flowId: flow.id }]]);
    expect(events.filter(([e]) => e === "job.enqueued")).toHaveLength(4);
    expect(k.fetch!.payload).toEqual({ url: "u" });
  });

  it("runs to completion in dependency order", async () => {
    const flow = await conveyor.enqueueFlow({ jobs: [node("a"), node("b", ["a"]), node("c", ["b"])] });
    const order: string[] = [];
    for (let lease = await broker.claim(["q"], "w"); lease; lease = await broker.claim(["q"], "w")) {
      order.push(lease.job.flowKey!);
      await broker.complete(lease.job.id, lease.token);
    }
    expect(order).toEqual(["a", "b", "c"]);
    expect((await conveyor.getFlow(flow.id)).status).toBe("completed");
  });

  it("is failed when a job dies, with its fail dependents cancelled", async () => {
    const flow = await conveyor.enqueueFlow({ jobs: [node("a", undefined, { maxAttempts: 1 }), node("b", ["a"]), node("c", [{ key: "a", onFailure: "ignore" }])] });
    const lease = (await broker.claim(["q"], "w"))!;
    await broker.fail(lease.job.id, lease.token, "boom");
    const k = byKey((await conveyor.getFlow(flow.id)).jobs);
    expect([k.a!.state, k.b!.state, k.c!.state]).toEqual(["dead", "cancelled", "waiting"]);
    const next = (await broker.claim(["q"], "w"))!;
    await broker.complete(next.job.id, next.token);
    expect((await conveyor.getFlow(flow.id)).status).toBe("failed");
  });

  it("depends on existing jobs: blocked on running ones, released at once by completed ones", async () => {
    const p = await running("outside");
    const done = await running("outside");
    await broker.complete(done.id, done.token);
    const flow = await conveyor.enqueueFlow({ jobs: [node("onRunning", [{ jobId: p.id }]), node("onDone", [{ jobId: done.id }])] });
    const k = byKey(flow.jobs);
    expect([k.onRunning!.state, k.onDone!.state]).toEqual(["blocked", "waiting"]);
    expect(events).toContainEqual(["job.unblocked", k.onDone!.id]);
    await broker.complete(p.id, p.token);
    expect((await conveyor.getJob(k.onRunning!.id)).state).toBe("waiting");
    expect((await conveyor.dependents(p.id)).items.map((l) => l.job.id)).toEqual([k.onRunning!.id]);
  });

  it.each(["ignore", "continue"] as const)("settles a %s dependency on a job that is already dead or cancelled straight away", async (onFailure) => {
    const dead = await running("outside");
    await broker.fail(dead.id, dead.token, "boom", { unrecoverable: true });
    const cancelled = await conveyor.enqueue({ queue: "outside", name: "x" });
    await conveyor.cancel(cancelled.id);
    const flow = await conveyor.enqueueFlow({
      onFailure,
      jobs: [node("a", [{ jobId: dead.id }, { jobId: cancelled.id }]), node("b", ["a"])],
    });
    expect(flow.jobs.map((j) => j.state)).toEqual(["waiting", "blocked"]);
  });

  it("applies the flow's default policy to dependencies without one", async () => {
    const flow = await conveyor.enqueueFlow({ onFailure: "continue", jobs: [node("a"), node("b", ["a", { key: "c", onFailure: "fail" }]), node("c")] });
    expect(flow.edges.map((e) => e.onFailure).sort()).toEqual(["continue", "fail"]);
  });
});

describe("a rejected flow writes nothing", () => {
  let seeded: { dead: string; cancelled: string; active: string; done: string };

  beforeEach(async () => {
    const dead = await running();
    await broker.fail(dead.id, dead.token, "boom", { unrecoverable: true });
    const cancelled = await conveyor.enqueue({ queue: "q", name: "x" });
    await conveyor.cancel(cancelled.id);
    const active = await running();
    const done = await running();
    await broker.complete(done.id, done.token);
    seeded = { dead: dead.id, cancelled: cancelled.id, active: active.id, done: done.id };
    events.length = 0;
  });

  const cases: [string, () => FlowRequest, typeof ValidationError | typeof ConflictError, string | RegExp][] = [
    ["a long cycle", () => ({ jobs: [node("ok"), node("A", ["D", "ok"]), node("B", ["A"]), node("C", ["B"]), node("D", ["C"])] }), ValidationError, "flow has a dependency cycle: A -> B -> C -> D -> A"],
    ["a direct cycle", () => ({ jobs: [node("a", ["b"]), node("b", ["a"])] }), ValidationError, "flow has a dependency cycle: a -> b -> a"],
    ["an unknown key", () => ({ jobs: [node("a", ["ghost"])] }), ValidationError, "job a depends on unknown key ghost"],
    ["a self-dependency", () => ({ jobs: [node("a"), node("b", ["a", "b"])] }), ValidationError, "job b depends on itself"],
    ["duplicate keys", () => ({ jobs: [node("a"), node("a")] }), ValidationError, "duplicate key a"],
    ["a duplicate edge", () => ({ jobs: [node("a"), node("b", ["a", { key: "a" }])] }), ValidationError, "job b lists dependency a twice"],
    ["a duplicate edge to an existing job", () => ({ jobs: [node("b", [{ jobId: seeded.done }, { jobId: seeded.done, onFailure: "ignore" }])] }), ValidationError, /lists dependency .* twice/],
    ["more than 1000 jobs", () => ({ jobs: Array.from({ length: 1001 }, (_, i) => node(`j${i}`)) }), ValidationError, "a flow can have at most 1000 jobs"],
    ["a uniqueKey", () => ({ jobs: [node("a"), node("b", undefined, { uniqueKey: "u" } as never)] }), ValidationError, "job b: uniqueKey is not allowed in a flow"],
    ["an invalid job", () => ({ jobs: [node("a"), node("b", ["a"], { priority: 5000 })] }), ValidationError, /^job b: priority must be an integer/],
    ["an unknown existing job", () => ({ jobs: [node("a"), node("b", ["a", { jobId: "no-such-job" }])] }), ValidationError, "dependency no-such-job not found"],
    ["a fail dependency on a dead job", () => ({ jobs: [node("a"), node("b", ["a", { jobId: seeded.active }, { jobId: seeded.dead }])] }), ConflictError, /is already dead$/],
    ["a fail dependency on a cancelled job", () => ({ jobs: [node("a", [{ jobId: seeded.cancelled }])] }), ConflictError, /is already cancelled$/],
  ];

  it.each(cases)("%s", async (_label, build, errorClass, message) => {
    const before = fingerprint();
    const req = build();
    await expect(conveyor.enqueueFlow(req)).rejects.toThrow(errorClass);
    await expect(conveyor.enqueueFlow(req)).rejects.toThrow(message);
    expect(fingerprint()).toEqual(before);
    expect(events.filter(([e]) => e === "job.enqueued")).toHaveLength(0);
  });

  it("a large flow that fails at its last edge, after every job was inserted", async () => {
    const before = fingerprint();
    const jobs = Array.from({ length: 1000 }, (_, i) =>
      node(`j${i}`, i === 0 ? undefined : i === 999 ? [`j${i - 1}`, { jobId: seeded.dead }] : [`j${i - 1}`], { payload: "p".repeat(2000) }),
    );
    await expect(conveyor.enqueueFlow({ jobs })).rejects.toThrow(ConflictError);
    expect(fingerprint()).toEqual(before);
  });

  it("a database failure part-way through", async () => {
    store.db.exec(`CREATE TRIGGER fail_flow BEFORE INSERT ON jobs WHEN NEW.flow_key = 'explode' BEGIN SELECT RAISE(ABORT, 'simulated insert failure'); END;`);
    // Creating the trigger is a commit of its own: take the fingerprint after it.
    const before = fingerprint();
    await expect(conveyor.enqueueFlow({ jobs: [node("a"), node("b", ["a"]), node("explode", ["b"])] })).rejects.toThrow("simulated insert failure");
    expect(fingerprint()).toEqual(before);
  });

  it("and the database works normally afterwards", async () => {
    await expect(conveyor.enqueueFlow({ jobs: [node("a", ["a"])] })).rejects.toThrow();
    const before = fingerprint();
    await conveyor.enqueueFlow({ jobs: [node("a")] });
    expect(fingerprint().dataVersion).toBeGreaterThan(before.dataVersion);
  });
});

describe("cancelFlow", () => {
  it("cancels every unfinished job, running ones included, and settles dependents outside the flow", async () => {
    const flow = await conveyor.enqueueFlow({ jobs: [node("a"), node("b", ["a"]), node("c"), node("d", ["c"])] });
    const k = byKey(flow.jobs);
    const first = (await broker.claim(["q"], "w"))!; // a
    await broker.complete(first.job.id, first.token);
    const running = (await broker.claim(["q"], "w"))!; // b, now waiting
    expect(running.job.flowKey).toBe("b");
    const outsideFail = await conveyor.enqueue({ queue: "other", name: "x", dependsOn: [k.d!.id] });
    const outsideContinue = await conveyor.enqueue({ queue: "other", name: "x", dependsOn: [{ jobId: k.d!.id, onFailure: "continue" }] });
    events.length = 0;

    const cancelled = await conveyor.cancelFlow(flow.id);
    expect(cancelled.status).toBe("cancelled");
    expect(cancelled.jobs.map((j) => [j.flowKey, j.state])).toEqual([
      ["a", "completed"],
      ["b", "cancelled"],
      ["c", "cancelled"],
      ["d", "cancelled"],
    ]);
    expect(events).toEqual([
      ["job.cancelled", k.b!.id],
      ["job.cancelled", k.c!.id],
      ["job.cancelled", k.d!.id],
      ["job.cancelled", outsideFail.id],
      ["job.unblocked", outsideContinue.id],
      ["flow.failed", flow.id],
    ]);
    expect((await conveyor.getJob(outsideFail.id)).terminatedBy).toBe(k.d!.id);
    // The worker running b finds out when it reports back.
    await expect(broker.complete(running.job.id, running.token)).rejects.toThrow(LeaseLostError);
  });

  it("is a conflict once every job has finished, and not found for an unknown flow", async () => {
    const flow = await conveyor.enqueueFlow({ jobs: [node("a")] });
    await conveyor.cancelFlow(flow.id);
    await expect(conveyor.cancelFlow(flow.id)).rejects.toThrow(new ConflictError(`flow ${flow.id} has already finished`));
    await expect(conveyor.cancelFlow("nope")).rejects.toThrow(NotFoundError);
    await expect(conveyor.getFlow("nope")).rejects.toThrow(new NotFoundError("flow nope not found"));
  });

  it("a cancelled flow job can be retried on its own", async () => {
    const flow = await conveyor.enqueueFlow({ jobs: [node("a"), node("b", ["a"])] });
    await conveyor.cancelFlow(flow.id);
    const k = byKey(flow.jobs);
    expect((await conveyor.retry(k.a!.id)).state).toBe("waiting");
    // b was cancelled directly, so it isn't brought back with a.
    expect((await conveyor.getJob(k.b!.id)).state).toBe("cancelled");
    expect((await conveyor.getFlow(flow.id)).status).toBe("running");
  });
});

describe("dependencies and dependents", () => {
  it("list the jobs on either side of a job's edges, and 404 for an unknown job", async () => {
    const flow = await conveyor.enqueueFlow({ jobs: [node("a"), node("b"), node("c", ["a", { key: "b", onFailure: "ignore" }])] });
    const k = byKey(flow.jobs);
    expect((await conveyor.dependencyResultsPage(k.c!.id)).items.map((d) => [d.key, d.onFailure, d.state])).toEqual([
      ["a", "fail", "waiting"],
      ["b", "ignore", "waiting"],
    ]);
    expect((await conveyor.dependents(k.a!.id)).items.map((l) => l.job.flowKey)).toEqual(["c"]);
    await expect(conveyor.dependencyResultsPage("nope")).rejects.toThrow(NotFoundError);
    await expect(conveyor.dependents("nope")).rejects.toThrow(NotFoundError);
  });
});
