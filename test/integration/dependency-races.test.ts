// Races between connections to one database file, the way `serve` and `conveyor worker`
// processes share it. Each side runs in its own thread with its own SqliteStore, and a
// spin barrier releases both at the same instant, many times over.
//
// The property under test is that a dependent is never left blocked when nothing is left to
// unblock it (a lost wakeup):
// - two dependencies completing at once, each in its own transaction, must not both decide
//   that the other is still unfinished;
// - a new dependent inserted while its dependency completes must either see the completion or
//   be seen by it, whether it is a single job or part of a flow;
// - a cancel, which reads the job before writing, must not fail or act on stale data when
//   another connection commits in between (this is what BEGIN IMMEDIATE is for);
// - a flow's last two jobs finishing at once record its verdict once, and two dispatchers
//   polling at once deliver it once.
//
// Real threads and real time: each round waits for both threads' replies. No fixed sleeps.
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Broker } from "../../src/broker/broker.js";
import { Conveyor } from "../../src/core/conveyor.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import type { RaceOp, RaceResult } from "../fixtures/race-thread.js";
import { tempDir } from "../helpers.js";

const THREAD = fileURLToPath(new URL("../fixtures/race-thread.mjs", import.meta.url));
const ROUNDS = 150;

let tmp: ReturnType<typeof tempDir>;
let threads: Worker[];
let conveyor: Conveyor;
let broker: Broker;

async function startThread(file: string, barrier: SharedArrayBuffer): Promise<Worker> {
  const w = new Worker(THREAD, { workerData: { file, barrier } });
  await new Promise<void>((resolve, reject) => {
    w.once("error", reject);
    w.once("message", (m) => (m === "ready" ? resolve() : reject(new Error(`unexpected ${String(m)}`))));
  });
  return w;
}

function send(w: Worker, op: RaceOp): Promise<RaceResult> {
  return new Promise((resolve, reject) => {
    w.once("error", reject);
    w.once("message", resolve);
    w.postMessage(op);
  });
}

/** Claim the oldest waiting job in "q" on the main connection. */
async function claim(): Promise<{ id: string; token: string }> {
  const lease = await broker.claim(["q"], "main");
  return { id: lease!.job.id, token: lease!.token };
}

const overlapped = (a: RaceResult, b: RaceResult) => a.start < b.end && b.start < a.end;

beforeEach(async () => {
  tmp = tempDir();
  const file = tmp.file();
  conveyor = new Conveyor({ store: new SqliteStore(file) });
  await conveyor.init();
  broker = new Broker(conveyor, { leaseMs: 600_000 });
  const barrier = new SharedArrayBuffer(4);
  threads = await Promise.all([startThread(file, barrier), startThread(file, barrier)]);
});

afterEach(async () => {
  await Promise.all(threads.map((w) => new Promise((resolve) => (w.once("exit", resolve), w.postMessage("close")))));
  await conveyor.store.close();
  tmp.cleanup();
});

describe("two connections, one database file", () => {
  it("two dependencies completing at the same moment release their dependent exactly once", async () => {
    let concurrent = 0;
    for (let round = 1; round <= ROUNDS; round++) {
      await conveyor.enqueue({ queue: "q", name: "parent" });
      await conveyor.enqueue({ queue: "q", name: "parent" });
      const [a, b] = [await claim(), await claim()];
      const child = await conveyor.enqueue({ queue: "kids", name: "child", dependsOn: [a.id, b.id] });

      const [ra, rb] = await Promise.all([
        send(threads[0]!, { round, op: "complete", jobId: a.id, token: a.token }),
        send(threads[1]!, { round, op: "complete", jobId: b.id, token: b.token }),
      ]);
      expect([ra, rb].map((r) => r.error ?? r.value), `round ${round}`).toEqual(["completed", "completed"]);
      if (overlapped(ra, rb)) concurrent++;

      expect((await conveyor.getJob(child.id)).state, `round ${round}`).toBe("waiting");
      const unblocked = (await conveyor.jobEvents(child.id)).filter((e) => e.type === "unblocked");
      expect(unblocked, `round ${round}`).toHaveLength(1);
    }
    // The rounds must really have run side by side, or this test proves nothing.
    expect(concurrent).toBeGreaterThan(ROUNDS / 10);
  });

  it("a new dependent inserted while its dependency completes is never left blocked", async () => {
    let concurrent = 0;
    const seen = { insertedFirst: 0, completedFirst: 0 };
    for (let round = 1; round <= ROUNDS; round++) {
      await conveyor.enqueue({ queue: "q", name: "parent" });
      const p = await claim();

      const [rc, ri] = await Promise.all([
        // Delay the completion by 0-380µs so that both orders happen often.
        send(threads[0]!, { round, op: "complete", jobId: p.id, token: p.token, delayUs: (round % 20) * 20 }),
        send(threads[1]!, { round, op: "enqueue", dependsOn: [p.id] }),
      ]);
      expect(rc.error ?? rc.value, `round ${round}`).toBe("completed");
      expect(ri.ok, `round ${round}: ${ri.error}`).toBe(true);
      if (overlapped(rc, ri)) concurrent++;

      const childId = ri.value!;
      expect((await conveyor.getJob(childId)).state, `round ${round}`).toBe("waiting");
      const events = await conveyor.jobEvents(childId);
      expect(events.map((e) => e.type), `round ${round}`).toEqual(["enqueued", "unblocked"]);
      // Which transaction committed first: history ids are global and increasing.
      const completion = (await conveyor.jobEvents(p.id)).find((e) => e.type === "completed")!;
      if (events[0]!.id < completion.id) seen.insertedFirst++;
      else seen.completedFirst++;
    }
    expect(concurrent).toBeGreaterThan(ROUNDS / 10);
    expect(seen.insertedFirst).toBeGreaterThan(ROUNDS / 20);
    expect(seen.completedFirst).toBeGreaterThan(ROUNDS / 20);
  });

  it("a dependency cancelled while another completes cancels the dependent, with neither call failing", async () => {
    let concurrent = 0;
    for (let round = 1; round <= ROUNDS; round++) {
      await conveyor.enqueue({ queue: "q", name: "parent" });
      const a = await claim();
      const b = await conveyor.enqueue({ queue: "other", name: "parent" });
      const child = await conveyor.enqueue({ queue: "kids", name: "child", dependsOn: [a.id, b.id] });

      const [rc, rx] = await Promise.all([
        send(threads[0]!, { round, op: "complete", jobId: a.id, token: a.token, delayUs: (round % 10) * 20 }),
        send(threads[1]!, { round, op: "cancel", jobId: b.id }),
      ]);
      expect([rc, rx].map((r) => r.error ?? r.value), `round ${round}`).toEqual(["completed", "cancelled"]);
      if (overlapped(rc, rx)) concurrent++;
      expect(await conveyor.getJob(child.id), `round ${round}`).toMatchObject({ state: "cancelled", terminatedBy: b.id });
    }
    expect(concurrent).toBeGreaterThan(ROUNDS / 10);
  });

  it("a flow whose jobs depend on an existing job that completes during the insert is settled correctly", async () => {
    let concurrent = 0;
    const seen = { insertedFirst: 0, completedFirst: 0 };
    for (let round = 1; round <= ROUNDS; round++) {
      await conveyor.enqueue({ queue: "q", name: "parent" });
      const p = await claim();
      const request = {
        jobs: [
          { key: "after", queue: "kids", name: "x", dependsOn: [{ jobId: p.id }] },
          { key: "then", queue: "kids", name: "x", dependsOn: ["after"] },
          { key: "both", queue: "kids", name: "x", dependsOn: ["free", { jobId: p.id, onFailure: "continue" as const }] },
          { key: "free", queue: "kids", name: "x" },
        ],
      };
      const [rc, rf] = await Promise.all([
        send(threads[0]!, { round, op: "complete", jobId: p.id, token: p.token, delayUs: (round % 20) * 25 }),
        send(threads[1]!, { round, op: "flow", request }),
      ]);
      expect(rc.error ?? rc.value, `round ${round}`).toBe("completed");
      expect(rf.ok, `round ${round}: ${rf.error}`).toBe(true);
      if (overlapped(rc, rf)) concurrent++;

      const flow = await conveyor.getFlow(rf.value!);
      const state = Object.fromEntries(flow.jobs.map((j) => [j.flowKey, j.state]));
      // "after" waited only for p, which has completed; the others still wait for flow jobs.
      expect(state, `round ${round}`).toEqual({ after: "waiting", then: "blocked", both: "blocked", free: "waiting" });
      const after = flow.jobs.find((j) => j.flowKey === "after")!;
      expect((await conveyor.jobEvents(after.id)).map((e) => e.type), `round ${round}`).toEqual(["enqueued", "unblocked"]);
      const enqueued = (await conveyor.jobEvents(after.id))[0]!;
      const completion = (await conveyor.jobEvents(p.id)).find((e) => e.type === "completed")!;
      if (enqueued.id < completion.id) seen.insertedFirst++;
      else seen.completedFirst++;
    }
    expect(concurrent).toBeGreaterThan(ROUNDS / 10);
    expect(seen.insertedFirst).toBeGreaterThan(ROUNDS / 20);
    expect(seen.completedFirst).toBeGreaterThan(ROUNDS / 20);
  });

  describe.each<[string, ["complete" | "fail", "complete" | "fail"], string]>([
    ["both complete", ["complete", "complete"], "flow.completed"],
    ["one completes and one dies", ["complete", "fail"], "flow.failed"],
    ["both die", ["fail", "fail"], "flow.failed"],
  ])("a flow's last two jobs finishing at the same moment: %s", (_label, ops, expected) => {
    it(`records ${expected} once, and two dispatchers deliver it once`, async () => {
      const store = conveyor.store as SqliteStore;
      await store.insertWebhook({
        id: "hook",
        url: "https://example.test/hook",
        secret: "s",
        events: ["flow.completed", "flow.failed"],
        queues: [],
        enabled: true,
        createdAt: Date.now(),
      });
      const rounds = 100;
      let concurrentFinish = 0;
      let concurrentDeliver = 0;
      let sentBefore = [0, 0];
      let barrier = 0;
      for (let round = 1; round <= rounds; round++) {
        const flow = await conveyor.enqueueFlow({
          jobs: [
            { key: "a", queue: "q", name: "x", maxAttempts: 1 },
            { key: "b", queue: "q", name: "x", maxAttempts: 1 },
          ],
        });
        const [a, b] = [await claim(), await claim()];
        const finished = await Promise.all([
          send(threads[0]!, { round: ++barrier, op: ops[0], jobId: a.id, token: a.token, delayUs: (round % 10) * 15 }),
          send(threads[1]!, { round: barrier, op: ops[1], jobId: b.id, token: b.token }),
        ]);
        expect(finished.map((r) => r.ok), `round ${round}: ${finished.map((r) => r.error)}`).toEqual([true, true]);
        if (overlapped(finished[0]!, finished[1]!)) concurrentFinish++;
        // Exactly one of the two transactions recorded the verdict (and told its own process).
        expect(finished.flatMap((r) => r.flowEvents), `round ${round}`).toEqual([`${expected} ${flow.id}`]);
        const rows = store.db.prepare("SELECT event FROM flow_events WHERE flow_id = ?").all(flow.id);
        expect(rows, `round ${round}`).toEqual([{ event: expected }]);

        const delivered = await Promise.all([send(threads[0]!, { round: ++barrier, op: "deliver" }), send(threads[1]!, { round: barrier, op: "deliver" })]);
        if (overlapped(delivered[0]!, delivered[1]!)) concurrentDeliver++;
        const sent = delivered.map((r) => Number(r.value));
        expect(sent[0]! - sentBefore[0]! + (sent[1]! - sentBefore[1]!), `round ${round}: webhook calls`).toBe(1);
        sentBefore = sent;
      }
      const deliveries = store.db.prepare("SELECT event, COUNT(*) AS n FROM webhook_deliveries GROUP BY event").all();
      expect(deliveries).toEqual([{ event: expected, n: rounds }]);
      expect(concurrentFinish).toBeGreaterThan(rounds / 10);
      expect(concurrentDeliver).toBeGreaterThan(rounds / 10);
    });
  });
});
