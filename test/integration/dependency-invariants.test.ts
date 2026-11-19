// Randomised test of the dependency graph. Each seed builds a random DAG of jobs through the
// public API and applies random operations to it: claim, complete, fail (with and without
// retries left), release, cancel, retry, expire leases and reap, run the reconcile sweep, add
// new dependents of existing jobs, submit small flows (depending on each other and on existing
// jobs), cancel flows, and delete old finished jobs (retention is 2s here), with random policies. Up to three operations run
// concurrently per step, so their awaits interleave.
//
// After every step the whole database is checked against the invariants below, and at the end
// every job is driven to a finished state: nothing may be left blocked.
//
// Reproduce a failure with the seed in its message: CONVEYOR_DAG_SEEDS=<seed> npx vitest run <this file>
import { describe, expect, it } from "vitest";
import { Maintenance } from "../../src/broker/maintenance.js";
import { ConflictError, LeaseLostError, NotFoundError, ValidationError } from "../../src/core/errors.js";
import type { EventName } from "../../src/core/events.js";
import type { Dependency, FailurePolicy, Job, JobEvent } from "../../src/core/types.js";
import { TERMINAL_STATES } from "../../src/core/types.js";
import { historyState, manualClock, rowState, setup } from "../helpers.js";

const SEEDS = process.env.CONVEYOR_DAG_SEEDS?.split(",").map(Number) ?? Array.from({ length: 20 }, (_, i) => i + 1);
const STEPS = 150;

/** mulberry32: a small seeded PRNG, so every run of a seed does the same thing. */
function prng(seed: number) {
  let a = seed >>> 0;
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    next,
    int: (n: number) => Math.floor(next() * n),
    pick: <T>(xs: T[]): T | undefined => (xs.length ? xs[Math.floor(next() * xs.length)] : undefined),
    chance: (p: number) => next() < p,
  };
}

interface Snapshot {
  jobs: Map<string, Job>;
  deps: Map<string, Dependency[]>;
  history: Map<string, JobEvent[]>;
  /** Flow verdicts by flow id. */
  verdicts: Map<string, string>;
}

const finished = (j: Job) => TERMINAL_STATES.includes(j.state);
const failed = (j: Job) => j.state === "dead" || j.state === "cancelled";

/** Every invariant of the graph, as a list of violations (empty when all hold). */
function violations({ jobs, deps, history, verdicts }: Snapshot): string[] {
  const out: string[] = [];
  // A flow.completed verdict means every job of the flow completed (and they can't change).
  for (const [flowId, event] of verdicts) {
    if (event !== "flow.completed") continue;
    for (const j of jobs.values()) if (j.flowId === flowId && j.state !== "completed") out.push(`flow ${flowId} completed but ${j.id} is ${j.state}`);
  }
  for (const job of jobs.values()) {
    const parents = (deps.get(job.id) ?? []).map((d) => ({ ...d, job: jobs.get(d.jobId) })).filter((p) => p.job);
    const fatal = parents.filter((p) => p.onFailure === "fail" && failed(p.job!));
    // An unfinished job has every dependency it was created with: retention doesn't delete
    // them, and a retry refuses if one is gone. (Edges outlive their parent.)
    if (!finished(job)) {
      for (const d of deps.get(job.id) ?? []) if (!jobs.has(d.jobId)) out.push(`${job.id} is ${job.state} but its dependency ${d.jobId} was deleted`);
    }
    const unfinished = parents.filter((p) => !finished(p.job!));

    // A blocked job is waiting for something that can still happen, and nothing has doomed it:
    // otherwise the transition that settled its last dependency should have moved it on.
    if (job.state === "blocked") {
      if (unfinished.length === 0) out.push(`${job.id} is blocked but all its dependencies have finished`);
      if (fatal.length > 0) out.push(`${job.id} is blocked but its fail dependency ${fatal[0]!.jobId} is ${fatal[0]!.job!.state}`);
    }
    // A job that was released (or ran) did so with every fail dependency completed, and a
    // completed job stays completed.
    if (["waiting", "delayed", "active", "completed", "dead"].includes(job.state)) {
      for (const p of parents) {
        if (p.onFailure === "fail" && p.job!.state !== "completed") {
          out.push(`${job.id} is ${job.state} but its fail dependency ${p.jobId} is ${p.job!.state}`);
        }
      }
    }
    // terminatedBy names a fail dependency that really did fail, and only on cancelled jobs.
    // (Retention may have deleted that dependency since: the cancelled job has finished.)
    if (job.terminatedBy !== null) {
      const by = parents.find((p) => p.jobId === job.terminatedBy);
      if (job.state !== "cancelled") out.push(`${job.id} has terminatedBy but is ${job.state}`);
      const deleted = !jobs.has(job.terminatedBy);
      if (!deleted && (!by || by.onFailure !== "fail" || !failed(by.job!))) {
        out.push(`${job.id} was terminated by ${job.terminatedBy}, which is not a failed fail dependency`);
      }
    }
    // The row and its history agree: every change was written with its history entry.
    const events = history.get(job.id) ?? [];
    if (historyState(events) !== rowState(job)) {
      out.push(`${job.id} is ${job.state} but its history says ${historyState(events)} (${events.map((e) => e.type).join(",")})`);
    }
    if (job.state === "active" && job.leaseToken === null) out.push(`${job.id} is active without a lease`);
    // A flow's jobs were created in dependency order, so an edge inside a flow points back.
    for (const p of parents) {
      if (job.flowId !== null && p.job!.flowId === job.flowId && p.jobId >= job.id) out.push(`${job.id} depends on a later job of its flow`);
    }
  }
  return out;
}

async function run(seed: number) {
  const r = prng(seed);
  const clock = manualClock();
  const { conveyor, broker, store } = await setup({ clock, config: { retentionMs: 2000 } }); // leaseMs 1000; store is a SqliteStore
  const maintenance = new Maintenance(conveyor);
  const leases = new Map<string, string>();
  const ids: string[] = [];
  /** Events emitted, per event name and job id. */
  const bus = new Map<string, number>();
  conveyor.events.onAny((event: EventName, first) => {
    const key = `${event} ${(first as Job).id ?? (first as { flowId: string }).flowId}`;
    bus.set(key, (bus.get(key) ?? 0) + 1);
  });

  async function allJobs(): Promise<Job[]> {
    const jobs: Job[] = [];
    for (let cursor: string | undefined; ; ) {
      const page = await store.listJobs({ limit: 500, cursor });
      jobs.push(...page.items);
      if (!page.nextCursor) return jobs;
      cursor = page.nextCursor;
    }
  }

  /** The whole database: every job, edge and history entry. */
  async function snapshot(): Promise<Snapshot> {
    const jobs = new Map((await allJobs()).map((j) => [j.id, j]));
    const deps = new Map<string, Dependency[]>();
    for (const e of store.db.prepare("SELECT child_id, parent_id, on_failure FROM job_dependencies ORDER BY parent_id").all() as Record<string, string>[]) {
      if (!deps.has(e.child_id!)) deps.set(e.child_id!, []);
      deps.get(e.child_id!)!.push({ jobId: e.parent_id!, onFailure: e.on_failure as FailurePolicy });
    }
    const history = new Map<string, JobEvent[]>();
    for (const e of store.db.prepare("SELECT id, job_id, type, data, at FROM job_events ORDER BY id").all() as Record<string, string | number | null>[]) {
      const id = String(e.job_id);
      const event = { id: Number(e.id), jobId: id, type: String(e.type), data: e.data === null ? null : JSON.parse(String(e.data)), at: Number(e.at) };
      if (!history.has(id)) history.set(id, []);
      history.get(id)!.push(event);
    }
    const verdicts = new Map(
      (store.db.prepare("SELECT flow_id, event FROM flow_events").all() as { flow_id: string; event: string }[]).map((r) => [r.flow_id, r.event]),
    );
    return { jobs, deps, history, verdicts };
  }
  let kept = 0;
  let deleted = 0;

  const jobsIn = async (pred: (j: Job) => boolean) => (await allJobs()).filter(pred);
  const policy = (): FailurePolicy => (r.chance(0.6) ? "fail" : r.chance(0.5) ? "ignore" : "continue");
  const takeLease = () => {
    const id = r.pick([...leases.keys()]);
    if (!id) return undefined;
    const token = leases.get(id)!;
    leases.delete(id);
    return { id, token };
  };

  // Operations. Each returns a thunk to run concurrently with the others in its step. Errors
  // that are a legitimate outcome of a race (the lease was lost, the job already finished, a
  // dependency already failed) are expected; anything else fails the test.
  const ops: (() => Promise<(() => Promise<unknown>) | undefined>)[] = [
    async () => {
      const parents = [...new Set(Array.from({ length: r.int(4) }, () => r.pick(ids)).filter((x): x is string => !!x))];
      const req = {
        queue: "q",
        name: "x",
        maxAttempts: 1 + r.int(2),
        delayMs: r.chance(0.2) ? r.int(3000) : undefined,
        dependsOn: parents.map((jobId) => ({ jobId, onFailure: policy() })),
      };
      return async () => ids.push((await conveyor.enqueue(req)).id);
    },
    async () => async () => {
      const lease = await broker.claim(["q"], "w");
      if (lease) leases.set(lease.job.id, lease.token);
    },
    async () => {
      const l = takeLease();
      return l && (() => broker.complete(l.id, l.token, { seed }));
    },
    async () => {
      const l = takeLease();
      const unrecoverable = r.chance(0.3);
      return l && (() => broker.fail(l.id, l.token, new Error("boom"), { unrecoverable }));
    },
    async () => {
      const l = takeLease();
      return l && (() => broker.release(l.id, l.token));
    },
    async () => {
      const job = r.pick(await jobsIn((j) => !finished(j)));
      return job && (() => conveyor.cancel(job.id));
    },
    async () => {
      const job = r.pick(await jobsIn(failed));
      return job && (() => conveyor.retry(job.id));
    },
    async () => {
      const ms = r.int(2500);
      return async () => {
        clock.advance(ms);
        await maintenance.promote();
        await maintenance.reap();
      };
    },
    async () => {
      const size = 2 + r.int(4);
      const jobs = Array.from({ length: size }, (_, i) => {
        const internal = Array.from({ length: r.int(3) }, () => r.int(size)).filter((p) => p < i);
        const external = r.chance(0.4) ? [r.pick(ids)].filter((x): x is string => !!x) : [];
        return {
          key: `n${i}`,
          queue: "q",
          name: "x",
          maxAttempts: 1 + r.int(2),
          dependsOn: [
            ...[...new Set(internal)].map((p) => ({ key: `n${p}`, onFailure: policy() })),
            ...external.map((jobId) => ({ jobId, onFailure: policy() })),
          ],
        };
      });
      // Given in a shuffled order: the flow puts them in dependency order itself.
      jobs.sort(() => r.next() - 0.5);
      return async () => ids.push(...(await conveyor.enqueueFlow({ jobs })).jobs.map((j) => j.id));
    },
    async () => {
      const flowId = r.pick([...new Set((await jobsIn((j) => j.flowId !== null)).map((j) => j.flowId!))]);
      return flowId ? () => conveyor.cancelFlow(flowId) : undefined;
    },
    async () => async () => {
      // How many old finished jobs only their unfinished dependents keep, before the cleanup.
      kept += Number(
        (
          store.db
            .prepare(
              `SELECT COUNT(*) AS n FROM jobs j WHERE j.state IN ('completed', 'cancelled') AND j.finished_at < ?
                 AND EXISTS (SELECT 1 FROM job_dependencies d JOIN jobs c ON c.id = d.child_id
                              WHERE d.parent_id = j.id AND c.state NOT IN ('completed', 'dead', 'cancelled'))`,
            )
            .get(clock.now() - 2000) as { n: number }
        ).n,
      );
      deleted += await maintenance.cleanup();
    },
    async () => async () => {
      const n = await maintenance.reconcile();
      if (n !== 0) throw new Error(`the reconcile sweep found ${n} jobs that a transition should have settled`);
    },
  ];
  const weights = [5, 4, 4, 2, 1, 1, 1, 1, 1, 1, 1, 1];
  const pickOp = () => {
    let x = r.int(weights.reduce((a, b) => a + b));
    for (let i = 0; i < weights.length; i++) if ((x -= weights[i]!) < 0) return ops[i]!;
    return ops[0]!;
  };

  // Also: a job picked at random may have been deleted by retention in the meantime.
  const expected = (err: unknown) =>
    err instanceof LeaseLostError ||
    err instanceof ConflictError ||
    err instanceof NotFoundError ||
    (err instanceof ValidationError && / not found$/.test(err.message));

  for (let step = 0; step < STEPS; step++) {
    const thunks = (await Promise.all(Array.from({ length: 1 + r.int(3) }, () => pickOp()()))).filter((t) => !!t);
    const results = await Promise.allSettled(thunks.map((t) => t()));
    for (const res of results) {
      if (res.status === "rejected" && !expected(res.reason)) throw new Error(`seed ${seed}, step ${step}: ${String(res.reason)}`);
    }
    expect(violations(await snapshot()), `seed ${seed}, step ${step}`).toEqual([]);
  }

  // Drive everything to the end: release expired leases, run every runnable job to completion.
  for (let round = 0; round < 1000; round++) {
    clock.advance(5000);
    await maintenance.promote();
    await maintenance.reap();
    let claimed = 0;
    for (let lease = await broker.claim(["q"], "drain"); lease; lease = await broker.claim(["q"], "drain")) {
      await broker.complete(lease.job.id, lease.token);
      claimed++;
    }
    if (claimed === 0 && (await jobsIn((j) => ["waiting", "delayed", "active"].includes(j.state))).length === 0) break;
  }
  const snap = await snapshot();
  expect(violations(snap), `seed ${seed}, after draining`).toEqual([]);
  // Liveness: a DAG whose jobs have all finished or are blocked can have no blocked job left,
  // because the earliest blocked job would be waiting only on finished jobs.
  expect([...snap.jobs.values()].filter((j) => !finished(j)).map((j) => `${j.id}:${j.state}`), `seed ${seed}, after draining`).toEqual([]);

  // Every flow that still has jobs has finished, so it has its verdict, announced once.
  const flowIds = new Set([...snap.jobs.values()].flatMap((j) => (j.flowId ? [j.flowId] : [])));
  for (const flowId of flowIds) {
    const event = snap.verdicts.get(flowId);
    expect(event, `seed ${seed}: verdict of flow ${flowId}`).toBeDefined();
    expect(bus.get(`${event} ${flowId}`), `seed ${seed}: ${event} events for ${flowId}`).toBe(1);
    const other = event === "flow.completed" ? "flow.failed" : "flow.completed";
    expect(bus.get(`${other} ${flowId}`), `seed ${seed}: ${other} events for ${flowId}`).toBeUndefined();
  }

  // Every change the store committed was announced on the event bus exactly once. (Jobs deleted
  // by retention took their history with them, so only the remaining ones can be compared.)
  for (const [id, events] of snap.history) {
    for (const [type, event] of [
      ["unblocked", "job.unblocked"],
      ["cancelled", "job.cancelled"],
      ["completed", "job.completed"],
      ["dead", "job.dead"],
    ]) {
      expect(bus.get(`${event} ${id}`) ?? 0, `seed ${seed}: ${event} events for ${id}`).toBe(events.filter((e) => e.type === type).length);
    }
  }

  return { kept, deleted, jobs: snap.jobs.size, flowJobs: [...snap.jobs.values()].filter((j) => j.flowId).length, states: [...snap.jobs.values()].map((j) => j.state), history: snap.history };
}

describe("random dependency graphs", () => {
  it(`keep every invariant under random concurrent operations (${SEEDS.length} seeds x ${STEPS} steps)`, async () => {
    const totals = { kept: 0, deleted: 0, jobs: 0, flowJobs: 0, flowCancels: 0, cascaded: 0, unblocked: 0, revived: 0, dead: 0, cancelledDirectly: 0 };
    for (const seed of SEEDS) {
      const res = await run(seed);
      totals.jobs += res.jobs;
      totals.kept += res.kept;
      totals.deleted += res.deleted;
      totals.flowJobs += res.flowJobs;
      const events = [...res.history.values()].flat();
      totals.cascaded += events.filter((e) => e.type === "cancelled" && (e.data as { dependencyId?: string })?.dependencyId).length;
      totals.cancelledDirectly += events.filter((e) => e.type === "cancelled" && !(e.data as { dependencyId?: string })?.dependencyId).length;
      totals.unblocked += events.filter((e) => e.type === "unblocked").length;
      totals.revived += events.filter((e) => e.type === "retried" && (e.data as { dependencyId?: string })?.dependencyId).length;
      totals.dead += events.filter((e) => e.type === "dead").length;
      totals.flowCancels += events.filter((e) => e.type === "cancelled" && (e.data as { flowId?: string })?.flowId).length;
    }
    // The operations must actually reach the interesting paths, or the invariants prove little.
    if (SEEDS.length >= 10) {
      expect(totals.unblocked).toBeGreaterThan(50);
      expect(totals.cascaded).toBeGreaterThan(20);
      expect(totals.revived).toBeGreaterThan(5);
      expect(totals.dead).toBeGreaterThan(20);
      expect(totals.flowJobs).toBeGreaterThan(100);
      expect(totals.deleted).toBeGreaterThan(100);
      expect(totals.kept).toBeGreaterThan(20);
      expect(totals.flowCancels).toBeGreaterThan(10);
    }
  }, 120_000);
});
