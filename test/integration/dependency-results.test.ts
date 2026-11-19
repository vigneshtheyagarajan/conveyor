// What an in-process handler sees of its dependencies: ctx.dependencies() and
// ctx.dependency(key). These run a real Worker (with real timers: the handlers are quick, and
// each test waits for the flow to finish rather than for a fixed time).
import { afterEach, describe, expect, it, vi } from "vitest";
import { DependencyError, type DependencyResult } from "../../src/core/dependencies.js";
import { UnrecoverableError } from "../../src/core/errors.js";
import type { Flow, FlowRequest } from "../../src/core/types.js";
import { HandlerRegistry, type JobHandler } from "../../src/worker/handlers.js";
import { Worker } from "../../src/worker/worker.js";
import { setup } from "../helpers.js";
import { byKeys, EXPECTED_STATES, EXPECTED_VIEWS, POLICY_FLOW } from "./dependency-scenario.js";

let worker: Worker | undefined;
afterEach(async () => {
  await worker?.stop();
  worker = undefined;
});

/** Run a flow to the end on an in-process worker with these handlers. */
async function runFlow(
  handlers: Record<string, JobHandler>,
  req: FlowRequest,
  prepare: (s: Awaited<ReturnType<typeof setup>>) => void = () => {},
) {
  const s = await setup();
  prepare(s);
  const registry = new HandlerRegistry();
  for (const [name, fn] of Object.entries(handlers)) registry.register(name, fn);
  worker = new Worker(s.broker, { queues: ["q"], handlers: registry, pollMs: 1, maxPollMs: 5 });
  const flow = await s.conveyor.enqueueFlow(req);
  const finished = new Promise<Flow>((resolve) => {
    const check = async () => {
      const now = await s.conveyor.getFlow(flow.id);
      if (now.status !== "running") resolve(now);
    };
    for (const e of ["job.completed", "job.dead", "job.cancelled"] as const) s.conveyor.events.on(e, check);
  });
  worker.start();
  return { ...s, flow: await finished };
}

const states = (flow: Flow) => Object.fromEntries(flow.jobs.map((j) => [j.flowKey, j.state]));

describe("ctx.dependencies() and ctx.dependency(key)", () => {
  it("a diamond: D reads the results of both B and C, which read A", async () => {
    const { flow } = await runFlow(
      {
        a: () => ({ a: 1 }),
        b: async ({ dependency }) => ({ b: (await dependency<{ a: number }>("A")).a + 1 }),
        c: async ({ dependency }) => ({ c: (await dependency<{ a: number }>("A")).a + 2 }),
        d: async ({ dependencies, dependency }) => {
          const deps = await dependencies();
          const b = await dependency<{ b: number }>("B");
          const c = await dependency<{ c: number }>("C");
          return { sum: b.b + c.c, seen: deps.map((d) => [d.key, d.state, d.result]) };
        },
      },
      {
        jobs: [
          { key: "D", queue: "q", name: "d", dependsOn: ["B", "C"] },
          { key: "B", queue: "q", name: "b", dependsOn: ["A"] },
          { key: "C", queue: "q", name: "c", dependsOn: ["A"] },
          { key: "A", queue: "q", name: "a" },
        ],
      },
    );
    expect(flow.status).toBe("completed");
    expect(flow.jobs.find((j) => j.flowKey === "D")!.result).toEqual({
      sum: 5,
      seen: [
        ["B", "completed", { b: 2 }],
        ["C", "completed", { c: 3 }],
      ],
    });
  });

  it("each policy's view of a dead dependency", async () => {
    const seen: Record<string, { deps: DependencyResult[]; ok: unknown; dead: unknown }> = {};
    const ran: string[] = [];
    const { flow } = await runFlow(
      {
        produce: ({ job }) => ({ value: (job.payload as { value: number }).value }),
        explode: () => {
          throw new UnrecoverableError("boom");
        },
        read: async ({ job, dependencies, dependency }) => {
          ran.push(job.flowKey!);
          seen[job.flowKey!] = {
            deps: await dependencies(),
            ok: await dependency("ok"),
            dead: await dependency("dead").catch((err: unknown) => err),
          };
          return null;
        },
      },
      POLICY_FLOW,
    );
    expect(states(flow)).toEqual(EXPECTED_STATES);
    expect(flow.status).toBe("failed");
    // fail: the dependent never runs.
    expect(ran.sort()).toEqual(["viaContinue", "viaIgnore"]);
    expect(flow.jobs.find((j) => j.flowKey === "viaFail")).toMatchObject({ terminatedBy: flow.jobs.find((j) => j.flowKey === "dead")!.id });
    // ignore and continue: as in EXPECTED_VIEWS (shared with the HTTP worker test).
    expect(byKeys(flow.jobs, seen.viaIgnore!.deps)).toEqual(EXPECTED_VIEWS.viaIgnore);
    expect(byKeys(flow.jobs, seen.viaContinue!.deps)).toEqual(EXPECTED_VIEWS.viaContinue);
    // dependency(key) gives the completed one's result, and throws for the dead one under
    // either policy: it looks at every dependency, not just the listed ones.
    const deadId = flow.jobs.find((j) => j.flowKey === "dead")!.id;
    for (const key of ["viaIgnore", "viaContinue"]) {
      expect(seen[key]!.ok).toEqual({ value: 42 });
      expect(seen[key]!.dead).toBeInstanceOf(DependencyError);
      expect(seen[key]!.dead).toMatchObject({
        message: `dependency "dead" (job ${deadId}) did not complete; it is dead: boom`,
        dependencyId: deadId,
        dependencyState: "dead",
      });
    }
  });

  it("loads the dependencies once per attempt, on first use, and only if asked", async () => {
    let load!: { mock: { calls: unknown[][] } };
    const before: number[] = [];
    const after: number[] = [];
    const { flow } = await runFlow(
      {
        a: () => 1,
        reads: async ({ dependencies, dependency }) => {
          before.push(load.mock.calls.length);
          await dependencies();
          await dependency("a");
          await dependencies();
          await dependency("a");
          after.push(load.mock.calls.length);
          return null;
        },
        ignores: () => null,
      },
      {
        jobs: [
          { key: "a", queue: "q", name: "a" },
          { key: "b", queue: "q", name: "reads", dependsOn: ["a"] },
          { key: "c", queue: "q", name: "ignores", dependsOn: ["a"] },
        ],
      },
      (s) => (load = vi.spyOn(s.store, "listDependencyJobs")),
    );
    expect(flow.status).toBe("completed");
    expect(after[0]! - before[0]!).toBe(1);
    // Only b asked; nothing was loaded for c.
    expect(load.mock.calls.map((c) => c[0])).toEqual([flow.jobs.find((j) => j.flowKey === "b")!.id]);
  });

  it("does not cache a failed load: the next call tries again", async () => {
    let outcome: unknown[] = [];
    const { flow } = await runFlow(
      {
        a: () => "A",
        b: async ({ dependency }) => {
          outcome = [await dependency("a").catch((err: Error) => err.message), await dependency("a")];
          return null;
        },
      },
      { jobs: [{ key: "a", queue: "q", name: "a" }, { key: "b", queue: "q", name: "b", dependsOn: ["a"] }] },
      (s) => vi.spyOn(s.store, "listDependencyJobs").mockRejectedValueOnce(new Error("database is locked")),
    );
    expect(flow.status).toBe("completed");
    expect(outcome).toEqual(["database is locked", "A"]);
  });

  it("finds dependencies outside the flow by job id; a job without dependencies has none", async () => {
    const s = await setup();
    const outside = await s.conveyor.enqueue({ queue: "elsewhere", name: "x" });
    const lease = (await s.broker.claim(["elsewhere"], "w"))!;
    await s.broker.complete(outside.id, lease.token, "from outside");
    let got: unknown[] = [];
    const { flow } = await runFlow(
      {
        lone: async ({ dependencies, dependency }) => {
          got.push(await dependencies(), await dependency("x").catch((err: Error) => err.message));
          return null;
        },
      },
      { jobs: [{ key: "lone", queue: "q", name: "lone" }] },
    );
    expect(flow.status).toBe("completed");
    expect(got).toEqual([[], `job ${flow.jobs[0]!.id} has no dependency "x" (its dependencies: none)`]);

    // A job on its own (not in a flow) names its dependency by id.
    got = [];
    const registry = new HandlerRegistry().register("child", async ({ dependencies, dependency }) => {
      got.push((await dependencies()).map((d) => [d.id, d.key]), await dependency(outside.id));
      return null;
    });
    const child = await s.conveyor.enqueue({ queue: "q", name: "child", dependsOn: [outside.id] });
    const w = new Worker(s.broker, { queues: ["q"], handlers: registry, pollMs: 1, maxPollMs: 5 });
    const done = new Promise<void>((resolve) => s.conveyor.events.on("job.completed", (j) => j.id === child.id && resolve()));
    w.start();
    await done;
    await w.stop();
    expect(got).toEqual([[[outside.id, null]], "from outside"]);
  });

  it("a DependencyError that escapes the handler fails the job without retrying", async () => {
    const { flow } = await runFlow(
      {
        a: () => null,
        b: async ({ dependency }) => dependency("no-such-key"),
      },
      { jobs: [{ key: "a", queue: "q", name: "a" }, { key: "b", queue: "q", name: "b", maxAttempts: 5, dependsOn: ["a"] }] },
    );
    const b = flow.jobs.find((j) => j.flowKey === "b")!;
    expect(b).toMatchObject({ state: "dead", attempts: 1, lastError: `job ${b.id} has no dependency "no-such-key" (its dependencies: a)` });
  });
});
