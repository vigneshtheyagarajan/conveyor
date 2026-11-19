// Large and multi-process tests of dependencies:
// - fan-out / fan-in with 1000 children, on an in-process worker and on several worker
//   processes sharing one database file;
// - a diamond whose middle jobs complete at the same moment in two processes.
// They use real time and real processes. Each waits for the work to finish (with a deadline),
// not for a fixed time. Set CONVEYOR_REPORT=1 to print the timings.
import { fork, type ChildProcess } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Broker } from "../../src/broker/broker.js";
import { Conveyor } from "../../src/core/conveyor.js";
import type { Job } from "../../src/core/types.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import { HandlerRegistry } from "../../src/worker/handlers.js";
import { Worker } from "../../src/worker/worker.js";
import type { ProcessOp, ProcessReply } from "../fixtures/conveyor-process.js";
import { tempDir } from "../helpers.js";

const CHILDREN = 1000;
const PROCESS = fileURLToPath(new URL("../fixtures/conveyor-process.ts", import.meta.url));
const report = (label: string, value: unknown) => {
  if (process.env.CONVEYOR_REPORT) console.log(`[timing] ${label}: ${JSON.stringify(value)}`);
};
const stats = (ms: number[]) => {
  const sorted = [...ms].sort((a, b) => a - b);
  const at = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
  return { n: ms.length, mean: +(ms.reduce((a, b) => a + b, 0) / ms.length).toFixed(2), p50: +at(0.5).toFixed(2), p99: +at(0.99).toFixed(2), max: +sorted.at(-1)!.toFixed(2) };
};

let tmp: ReturnType<typeof tempDir>;
let procs: ChildProcess[] = [];
beforeEach(() => {
  tmp = tempDir();
});
afterEach(() => {
  for (const p of procs) if (p.exitCode === null) p.kill("SIGKILL");
  procs = [];
  tmp.cleanup();
});

/** Enqueue root -> 1000 children -> fan-in. */
async function fanOutFanIn(conveyor: Conveyor) {
  const root = await conveyor.enqueue({ queue: "q", name: "root" });
  const children = await conveyor.enqueueBulk(Array.from({ length: CHILDREN }, (_, i) => ({ queue: "q", name: "child", payload: { i }, dependsOn: [root.id] })));
  const fanin = await conveyor.enqueue({ queue: "q", name: "fanin", dependsOn: children.map((c) => c.id) });
  expect(children.every((c) => c.state === "blocked")).toBe(true);
  expect(fanin.state).toBe("blocked");
  return { root, children, fanin };
}

/** Poll until `done` holds, or fail after `ms`. */
async function until(done: () => Promise<boolean> | boolean, ms: number, what: string) {
  const deadline = Date.now() + ms;
  while (!(await done())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

/**
 * The checks shared by the fan-out tests. `strict`: each job was claimed and completed exactly
 * once. Otherwise KB-11 (claims that aren't atomic) may lease a job twice, or even reopen one
 * that already completed. The dependency guarantees are checked either way: every job but the
 * root was released exactly once, every child only after the root first completed, and the
 * fan-in only after every child first completed. Order is taken from history ids, which follow
 * commit order. Not from their timestamps: those are read when a call starts, before it waits
 * for the write lock, so across processes a later commit can carry an earlier time.
 */
async function expectFanOutFanIn(store: SqliteStore, ids: { root: Job; children: Job[]; fanin: Job }, opts: { strict: boolean }) {
  const all = [ids.root, ...ids.children, ids.fanin];
  for (const j of all) expect((await store.getJob(j.id))!.state, j.id).toBe("completed");
  const rows = store.db
    .prepare(
      `SELECT job_id, SUM(type = 'claimed') AS claimed, SUM(type = 'completed') AS completed,
              SUM(type = 'unblocked') AS unblocked, MIN(CASE WHEN type = 'unblocked' THEN id END) AS released,
              MIN(CASE WHEN type = 'completed' THEN id END) AS firstCompleted
         FROM job_events GROUP BY job_id`,
    )
    .all() as { job_id: string; claimed: number; completed: number; unblocked: number; released: number; firstCompleted: number }[];
  expect(rows).toHaveLength(CHILDREN + 2);
  const by = new Map(rows.map((r) => [r.job_id, r]));
  for (const r of rows) {
    expect({ id: r.job_id, released: r.unblocked }).toEqual({ id: r.job_id, released: r.job_id === ids.root.id ? 0 : 1 });
    if (opts.strict) expect({ id: r.job_id, claimed: r.claimed, completed: r.completed }).toEqual({ id: r.job_id, claimed: 1, completed: 1 });
  }
  const rootDone = by.get(ids.root.id)!.firstCompleted;
  for (const c of ids.children) expect(by.get(c.id)!.released).toBeGreaterThanOrEqual(rootDone);
  const lastChildDone = Math.max(...ids.children.map((c) => by.get(c.id)!.firstCompleted));
  expect(by.get(ids.fanin.id)!.released).toBeGreaterThanOrEqual(lastChildDone);
  const fanin = (await store.getJob(ids.fanin.id))!;
  expect(fanin.result).toEqual({ sum: (CHILDREN * (CHILDREN - 1)) / 2 });
  return {
    extraClaims: rows.reduce((n, r) => n + r.claimed - 1, 0),
    extraCompletions: rows.reduce((n, r) => n + r.completed - 1, 0),
  };
}

/** Run the fan-out / fan-in on an in-process worker, timing each completion transaction. */
async function runInProcess(concurrency: number) {
  const store = new SqliteStore(tmp.file());
  const conveyor = new Conveyor({ store });
  await conveyor.init();
  const ids = await fanOutFanIn(conveyor);
  const tx: { id: string; ms: number; released: number }[] = [];
  const completeJob = store.completeJob.bind(store);
  store.completeJob = async (...args) => {
    const t = performance.now();
    const res = await completeJob(...args);
    if (res) tx.push({ id: args[0], ms: performance.now() - t, released: res.dependents.length });
    return res;
  };
  const runs = new Map<string, number>();
  const ran = (id: string) => runs.set(id, (runs.get(id) ?? 0) + 1);
  let faninSaw: unknown;
  const handlers = new HandlerRegistry()
    .register("root", ({ job }) => void ran(job.id))
    .register("child", ({ job }) => {
      ran(job.id);
      return { i: (job.payload as { i: number }).i };
    })
    .register("fanin", async ({ job, dependencies }) => {
      ran(job.id);
      const deps = await dependencies();
      faninSaw = { count: deps.length, completed: deps.filter((d) => d.state === "completed").length };
      return { sum: deps.reduce((t, d) => t + (d.result as { i: number }).i, 0) };
    });
  const worker = new Worker(new Broker(conveyor), { queues: ["q"], handlers, concurrency, pollMs: 1, maxPollMs: 10 });
  const started = performance.now();
  const done = new Promise<void>((resolve) => conveyor.events.on("job.completed", (j) => j.id === ids.fanin.id && resolve()));
  worker.start();
  await done;
  const total = performance.now() - started;
  await worker.stop();
  const checked = await expectFanOutFanIn(store, ids, { strict: concurrency === 1 });
  const claims = new Map(
    (store.db.prepare("SELECT job_id, COUNT(*) AS n FROM job_events WHERE type = 'claimed' GROUP BY job_id").all() as { job_id: string; n: number }[]).map(
      (r) => [r.job_id, r.n],
    ),
  );
  await store.close();
  return { ids, tx, runs, claims, faninSaw, total, ...checked };
}

describe("fan-out / fan-in with 1000 children", () => {
  it("on an in-process worker: each child runs once, then the fan-in runs once", async () => {
    const { ids, tx, runs, claims, faninSaw, total } = await runInProcess(1);
    expect(runs.size).toBe(CHILDREN + 2);
    expect([...runs.values()].every((n) => n === 1)).toBe(true);
    expect([...claims.values()].every((n) => n === 1)).toBe(true);
    expect(faninSaw).toEqual({ count: CHILDREN, completed: CHILDREN });

    const rootTx = tx.find((t) => t.id === ids.root.id)!;
    expect(rootTx.released).toBe(CHILDREN);
    const childTx = tx.filter((t) => t.id !== ids.root.id && t.id !== ids.fanin.id);
    const releasing = childTx.filter((t) => t.released === 1);
    expect(releasing).toHaveLength(1); // only the last child's completion releases the fan-in
    report("in-process: root completion, releasing 1000 children (ms)", +rootTx.ms.toFixed(2));
    report("in-process: each child completion, re-checking the fan-in's 1000 dependencies (ms)", stats(childTx.map((t) => t.ms)));
    report("in-process: the completion that released the fan-in (ms)", +releasing[0]!.ms.toFixed(2));
    report("in-process: whole run, 1002 jobs, concurrency 1 (ms)", +total.toFixed(0));
    // Generous bounds: they catch a pathology (such as quadratic work per transaction), not noise.
    expect(rootTx.ms).toBeLessThan(5000);
    expect(Math.max(...childTx.map((t) => t.ms))).toBeLessThan(5000);
  }, 120_000);

  // With concurrency > 1, the worker's loops can lease the same job at once (KB-11: claim() has
  // no guard), and a fan-out, which makes 1000 jobs ready in one go, makes that likely. Whatever
  // KB-11 does, the dependency guarantees hold: each job completes once and is released once, and
  // the fan-in is released once, after every child completed. Every extra run of a handler comes
  // with an extra claim (a double lease), never from a job being released twice or early.
  it("with concurrency 8: releases still happen once, and extra runs are KB-11's double leases", async () => {
    const { runs, claims, faninSaw } = await runInProcess(8);
    for (const [id, n] of runs) expect(n, id).toBe(claims.get(id));
    expect(faninSaw).toEqual({ count: CHILDREN, completed: CHILDREN });
  }, 120_000);

  // BUG(KB-11): with concurrency > 1, the worker's loops claim the same job in lockstep, so each
  // child's handler runs several times (in this test, typically once per loop).
  it.fails("with concurrency 8: each child's handler runs exactly once [KB-11]", async () => {
    const { runs } = await runInProcess(8);
    expect([...runs.values()].every((n) => n === 1)).toBe(true);
  }, 120_000);

  it("on four worker processes sharing one database file", async () => {
    const file = tmp.file();
    const store = new SqliteStore(file);
    const conveyor = new Conveyor({ store });
    await conveyor.init();
    const ids = await fanOutFanIn(conveyor);
    const log = tmp.file("runs.log");
    writeFileSync(log, "");

    const workers = await Promise.all([0, 1, 2, 3].map(() => startProcess(file)));
    const started = Date.now();
    await Promise.all(workers.map((w) => w.send({ op: "work", queues: ["q"], concurrency: 1, log })));
    await until(async () => (await store.getJob(ids.fanin.id))!.state === "completed", 120_000, "the fan-in to complete");
    const total = Date.now() - started;
    await Promise.all(workers.map((w) => w.send({ op: "stop" })));

    const lines = readFileSync(log, "utf8").trim().split("\n");
    const runs = lines.filter((l) => l.startsWith("run ")).map((l) => l.split(" "));
    const runsOf = new Map<string, number>();
    for (const r of runs) runsOf.set(r[2]!, (runsOf.get(r[2]!) ?? 0) + 1);
    const pids = new Set(runs.map((r) => r[3]));
    expect(pids.size).toBeGreaterThan(1); // the work really was spread over processes

    const { extraClaims, extraCompletions } = await expectFanOutFanIn(store, ids, { strict: false });
    // Every job ran; any extra run is a double lease (KB-11: claim() is not atomic across
    // processes either), never a second or early release.
    const claims = new Map(
      (store.db.prepare("SELECT job_id, COUNT(*) AS n FROM job_events WHERE type = 'claimed' GROUP BY job_id").all() as { job_id: string; n: number }[]).map(
        (r) => [r.job_id, r.n],
      ),
    );
    expect(runsOf.size).toBe(CHILDREN + 2);
    for (const [id, n] of runsOf) expect(n, id).toBe(claims.get(id));
    // The fan-in saw all 1000 dependencies. (Their states and results are checked via its
    // stored result: a child that KB-11 reopened could show as active to a run of the fan-in.)
    for (const r of runs.filter((r) => r[1] === "fanin")) expect(r[5]).toBe(String(CHILDREN));

    const tx = lines.filter((l) => l.startsWith("tx ")).map((l) => l.split(" "));
    const rootTx = tx.find((t) => t[1] === ids.root.id)!;
    const childTx = tx.filter((t) => t[1] !== ids.root.id && t[1] !== ids.fanin.id).map((t) => Number(t[2]));
    const extra = [...runsOf.values()].reduce((a, n) => a + n - 1, 0);
    report("4 processes: root completion, releasing 1000 children (ms)", Number(rootTx[2]));
    report("4 processes: each child completion, incl. waiting for the write lock (ms)", stats(childTx));
    report("4 processes: whole run, 1002 jobs (ms)", total);
    report("4 processes: handler runs beyond one per job (KB-11 double leases)", { extraRuns: extra, extraClaims, extraCompletions });
    report("4 processes: jobs run per process", Object.fromEntries([...pids].map((p) => [p, runs.filter((r) => r[3] === p).length])));
    await store.close();
  }, 180_000);
});

describe("a diamond whose middle jobs complete at the same moment in two processes", () => {
  it("releases D once, and D runs once", async () => {
    const file = tmp.file();
    const store = new SqliteStore(file);
    const conveyor = new Conveyor({ store });
    await conveyor.init();
    const broker = new Broker(conveyor, { leaseMs: 600_000 });
    const [p1, p2] = await Promise.all([startProcess(file), startProcess(file)]);
    const rounds = 60;
    let overlapped = 0;
    for (let round = 1; round <= rounds; round++) {
      const flow = await conveyor.enqueueFlow({
        jobs: [
          { key: "A", queue: "q", name: "x" },
          { key: "B", queue: "q", name: "x", dependsOn: ["A"] },
          { key: "C", queue: "q", name: "x", dependsOn: ["A"] },
          { key: "D", queue: "d", name: "x", dependsOn: ["B", "C"] },
        ],
      });
      const a = (await broker.claim(["q"], "main"))!;
      await broker.complete(a.job.id, a.token);
      const [b, c] = [(await broker.claim(["q"], "main"))!, (await broker.claim(["q"], "main"))!];
      // Both processes complete at the same wall-clock instant, spinning until then.
      const at = Date.now() + 25;
      const [rb, rc] = await Promise.all([
        p1.send({ op: "complete-at", at, jobId: b.job.id, token: b.token }),
        p2.send({ op: "complete-at", at, jobId: c.job.id, token: c.token }),
      ]);
      expect([rb.value, rc.value], `round ${round}: ${rb.error} ${rc.error}`).toEqual(["completed", "completed"]);
      if (rb.start! < rc.end! && rc.start! < rb.end!) overlapped++;

      const d = flow.jobs.find((j) => j.flowKey === "D")!;
      expect((await store.getJob(d.id))!.state, `round ${round}`).toBe("waiting");
      const lease = (await broker.claim(["d"], "main"))!;
      expect(lease.job.id).toBe(d.id);
      await broker.complete(d.id, lease.token);
      expect(await broker.claim(["d"], "main"), `round ${round}: D claimable again`).toBeUndefined();
      const types = (await store.listJobEvents(d.id)).map((e) => e.type);
      expect(types, `round ${round}`).toEqual(["enqueued", "unblocked", "claimed", "completed"]);
      expect((await conveyor.getFlow(flow.id)).outcome?.event).toBe("flow.completed");
    }
    report("diamond across two processes: rounds where the two completions overlapped", `${overlapped}/${rounds}`);
    expect(overlapped).toBeGreaterThan(rounds / 4);
    await Promise.all([p1.send({ op: "stop" }), p2.send({ op: "stop" })]);
    await store.close();
  }, 120_000);
});

/** Fork a conveyor-process on `file` and wait until it is ready. */
async function startProcess(file: string) {
  const child = fork(PROCESS, [file], { execArgv: ["--import", "tsx"], stdio: ["ignore", "ignore", "pipe", "ipc"] });
  procs.push(child);
  const replies: ((r: ProcessReply) => void)[] = [];
  child.on("message", (r: ProcessReply) => replies.shift()?.(r));
  const next = () => new Promise<ProcessReply>((resolve, reject) => {
    replies.push(resolve);
    child.once("exit", (code) => code !== 0 && reject(new Error(`process exited with ${code}`)));
  });
  expect((await next()).value).toBe("ready");
  return {
    child,
    send: (op: ProcessOp) => {
      const reply = next();
      child.send(op);
      return reply;
    },
  };
}
