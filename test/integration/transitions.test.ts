// State transitions must be all-or-nothing and must not overwrite each other. Each operation
// that moves a job (complete, fail, cancel, retry, release, the reaper's requeue) checks the
// state it expects and writes the change and its history entry in one transaction. When two of
// them race, exactly one wins and the job ends up in the winner's state.
//
// The races here are real interleavings inside one process: every Store call is async, so two
// operations started together alternate between their awaits.
import { describe, expect, it } from "vitest";
import { Maintenance } from "../../src/broker/maintenance.js";
import type { Conveyor } from "../../src/core/conveyor.js";
import type { Job } from "../../src/core/types.js";
import type { SqliteStore } from "../../src/storage/sqlite-store.js";
import { historyState, manualClock, rowState, setup } from "../helpers.js";

type Outcome = { ok: true; value: unknown } | { ok: false; error: unknown };

async function race(...ops: (() => Promise<unknown>)[]): Promise<Outcome[]> {
  const settled = await Promise.allSettled(ops.map((op) => op()));
  return settled.map((s) => (s.status === "fulfilled" ? { ok: true, value: s.value } : { ok: false, error: s.reason }));
}

/** The job's row and its history agree about the state it is in. */
async function expectHistoryMatches(conveyor: Conveyor, id: string): Promise<Job> {
  const job = await conveyor.getJob(id);
  const events = await conveyor.jobEvents(id);
  expect(historyState(events), `history of ${id}: ${events.map((e) => e.type).join(", ")}`).toBe(rowState(job));
  return job;
}

async function activeJob(opts: { maxAttempts?: number } = {}) {
  const clock = manualClock();
  const ctx = await setup({ clock });
  const job = await ctx.conveyor.enqueue({ queue: "q", name: "x", maxAttempts: opts.maxAttempts ?? 3 });
  const lease = (await ctx.broker.claim(["q"], "w"))!;
  return { ...ctx, clock, job, token: lease.token };
}

describe("racing transitions: exactly one wins", () => {
  for (const order of ["complete first", "cancel first"] as const) {
    it(`complete vs cancel (${order})`, async () => {
      const { conveyor, broker, job, token } = await activeJob();
      const complete = () => broker.complete(job.id, token, { done: true });
      const cancel = () => conveyor.cancel(job.id);
      const outcomes = await race(...(order === "complete first" ? [complete, cancel] : [cancel, complete]));
      expect(outcomes.filter((o) => o.ok)).toHaveLength(1);
      const winner = outcomes.find((o) => o.ok) as { value: Job };
      const final = await expectHistoryMatches(conveyor, job.id);
      expect(final.state).toBe(winner.value.state);
    });
  }

  // Failing with a retry, or releasing, leaves the job unfinished, so a cancel that comes after
  // one of them is legitimate and both succeed. What must never happen is the other way round:
  // a cancelled job rescheduled or handed back to the queue.
  for (const [label, op] of [
    ["fail with a retry", (c: Awaited<ReturnType<typeof activeJob>>) => c.broker.fail(c.job.id, c.token, new Error("boom"))],
    ["release", (c: Awaited<ReturnType<typeof activeJob>>) => c.broker.release(c.job.id, c.token)],
  ] as const) {
    for (const order of [`${label} first`, "cancel first"]) {
      it(`${label} vs cancel (${order}): the cancel always sticks`, async () => {
        const c = await activeJob();
        const other = () => op(c);
        const cancel = () => c.conveyor.cancel(c.job.id);
        const outcomes = await race(...(order === "cancel first" ? [cancel, other] : [other, cancel]));
        expect(outcomes[order === "cancel first" ? 0 : 1]!.ok).toBe(true);
        expect((await expectHistoryMatches(c.conveyor, c.job.id)).state).toBe("cancelled");
      });
    }
  }

  for (const order of ["fail first", "cancel first"] as const) {
    it(`fail to dead vs cancel (${order})`, async () => {
      const { conveyor, broker, job, token } = await activeJob({ maxAttempts: 1 });
      const fail = () => broker.fail(job.id, token, new Error("boom"));
      const cancel = () => conveyor.cancel(job.id);
      const outcomes = await race(...(order === "fail first" ? [fail, cancel] : [cancel, fail]));
      expect(outcomes.filter((o) => o.ok)).toHaveLength(1);
      const winner = outcomes.find((o) => o.ok) as { value: Job };
      const final = await expectHistoryMatches(conveyor, job.id);
      expect(final.state).toBe(winner.value.state);
    });
  }

  for (const order of ["reap first", "complete first"] as const) {
    it(`reaper vs a late completion (${order}): a completed job is never requeued`, async () => {
      const { conveyor, broker, clock, job, token } = await activeJob();
      clock.advance(5000); // the lease has expired, but the reaper has not run yet
      const m = new Maintenance(conveyor);
      const reap = () => m.reap();
      const complete = () => broker.complete(job.id, token, { late: true });
      const outcomes = await race(...(order === "reap first" ? [reap, complete] : [complete, reap]));
      const reaped = (outcomes[order === "reap first" ? 0 : 1] as { value: number }).value;
      const completed = outcomes[order === "reap first" ? 1 : 0]!.ok;
      expect(reaped + (completed ? 1 : 0)).toBe(1);
      const final = await expectHistoryMatches(conveyor, job.id);
      expect(final.state).toBe(completed ? "completed" : "waiting");
    });
  }

  it("two retries of the same job: one succeeds, the other is a conflict", async () => {
    const { conveyor, broker, job, token } = await activeJob({ maxAttempts: 1 });
    await broker.fail(job.id, token, "boom");
    const outcomes = await race(
      () => conveyor.retry(job.id),
      () => conveyor.retry(job.id),
    );
    expect(outcomes.filter((o) => o.ok)).toHaveLength(1);
    expect((outcomes.find((o) => !o.ok) as { error: Error }).error.message).toMatch(/only dead or cancelled/);
    expect((await conveyor.jobEvents(job.id)).filter((e) => e.type === "retried")).toHaveLength(1);
  });

  it("two cancels of the same job: one succeeds, the other is a conflict", async () => {
    const { conveyor, job } = await activeJob();
    const outcomes = await race(
      () => conveyor.cancel(job.id),
      () => conveyor.cancel(job.id),
    );
    expect(outcomes.filter((o) => o.ok)).toHaveLength(1);
    expect((await conveyor.jobEvents(job.id)).filter((e) => e.type === "cancelled")).toHaveLength(1);
  });
});

describe("a transition and its history entry are stored together", () => {
  /** Make the database reject history entries of one type, the way a full disk would. */
  function failEventsOfType(conveyor: Conveyor, type: string) {
    (conveyor.store as SqliteStore).db.exec(
      `CREATE TRIGGER fail_event BEFORE INSERT ON job_events WHEN NEW.type = '${type}' BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END;`,
    );
  }

  it.each([
    ["complete", "completed", (c: Awaited<ReturnType<typeof activeJob>>) => c.broker.complete(c.job.id, c.token)],
    ["fail with a retry", "retry_scheduled", (c: Awaited<ReturnType<typeof activeJob>>) => c.broker.fail(c.job.id, c.token, "e")],
    ["cancel", "cancelled", (c: Awaited<ReturnType<typeof activeJob>>) => c.conveyor.cancel(c.job.id)],
    ["release", "released", (c: Awaited<ReturnType<typeof activeJob>>) => c.broker.release(c.job.id, c.token)],
  ])("%s changes nothing if its history entry can't be written", async (_label, type, op) => {
    const c = await activeJob();
    failEventsOfType(c.conveyor, type);
    await expect(op(c)).rejects.toThrow("simulated write failure");
    expect(await c.conveyor.getJob(c.job.id)).toMatchObject({ state: "active", leaseToken: c.token });
  });

  it("fail to dead changes nothing if its history entry can't be written", async () => {
    const c = await activeJob({ maxAttempts: 1 });
    failEventsOfType(c.conveyor, "dead");
    await expect(c.broker.fail(c.job.id, c.token, "e")).rejects.toThrow("simulated write failure");
    expect(await c.conveyor.getJob(c.job.id)).toMatchObject({ state: "active", leaseToken: c.token });
  });

  it("retry changes nothing if its history entry can't be written", async () => {
    const c = await activeJob({ maxAttempts: 1 });
    await c.broker.fail(c.job.id, c.token, "e");
    failEventsOfType(c.conveyor, "retried");
    await expect(c.conveyor.retry(c.job.id)).rejects.toThrow("simulated write failure");
    expect((await c.conveyor.getJob(c.job.id)).state).toBe("dead");
  });

  it("the reaper's requeue changes nothing if its history entry can't be written", async () => {
    const c = await activeJob();
    c.clock.advance(5000);
    failEventsOfType(c.conveyor, "lease_expired");
    await expect(new Maintenance(c.conveyor).reap()).rejects.toThrow("simulated write failure");
    expect(await c.conveyor.getJob(c.job.id)).toMatchObject({ state: "active", leaseToken: c.token });
  });
});
