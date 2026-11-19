import { afterEach, describe, expect, it, vi } from "vitest";
import { ConflictError, NotFoundError, ValidationError } from "../../src/core/errors.js";
import type { Schedule } from "../../src/core/types.js";
import { Scheduler, type ScheduleInput } from "../../src/scheduler/scheduler.js";
import type { ManualClock } from "../../src/util/clock.js";
import { manualClock, memoryLogger, setup } from "../helpers.js";

const MIN = 60_000;
const at = (iso: string) => Date.parse(iso);
const iso = (ms: number | null) => (ms === null ? null : new Date(ms).toISOString());

async function scheduler(opts: { clock?: ManualClock } & Omit<Parameters<typeof setup>[0] & object, "clock"> = {}) {
  const clock = opts.clock ?? manualClock(); // 2026-01-05T12:00:00Z
  const s = await setup({ ...opts, clock });
  return { ...s, clock, scheduler: new Scheduler(s.conveyor) };
}

const input = (over: Partial<ScheduleInput> = {}): ScheduleInput => ({
  name: "every-5",
  queue: "q",
  jobName: "report",
  cron: "*/5 * * * *",
  ...over,
});

describe("Scheduler.create", () => {
  it("stores the schedule with its first run computed from now", async () => {
    const { scheduler: s, clock } = await scheduler();
    const created = await s.create(input({ payload: { a: 1 } }));
    expect(created).toMatchObject({
      name: "every-5",
      queue: "q",
      jobName: "report",
      payload: { a: 1 },
      cron: "*/5 * * * *",
      timezone: "UTC",
      enabled: true,
      overlap: "allow",
      nextRunAt: at("2026-01-05T12:05:00Z"),
      lastRunAt: null,
      lastJobId: null,
      createdAt: clock.now(),
    });
    expect(await s.get(created.id)).toEqual(created);
  });

  it("computes the first run in the schedule's timezone", async () => {
    const { scheduler: s } = await scheduler();
    const created = await s.create(input({ cron: "0 9 * * *", timezone: "America/New_York" }));
    expect(iso(created.nextRunAt)).toBe("2026-01-05T14:00:00.000Z");
  });

  it.each<[string, Partial<ScheduleInput>, RegExp]>([
    ["an invalid schedule name", { name: "bad name" }, /invalid schedule name/],
    ["an invalid queue name", { queue: "" }, /invalid queue name/],
    ["an invalid job name", { jobName: "-x" }, /invalid job name/],
    ["a malformed cron expression", { cron: "61 * * * *" }, /out of range for minute/],
    ["an unknown timezone", { timezone: "Mars/Base" }, /unknown timezone Mars\/Base/],
    ["an unknown overlap policy", { overlap: "queue" as never }, /overlap must be 'allow' or 'skip'/],
    // Previously passed validation and then failed in nextRun() with a plain Error (HTTP 500).
    ["a cron expression that can never run", { cron: "0 0 30 2 *" }, /never runs/],
  ])("rejects %s", async (_label, patch, message) => {
    const { scheduler: s } = await scheduler();
    const err = await s.create(input(patch)).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ValidationError);
    expect((err as Error).message).toMatch(message);
    expect(await s.list()).toEqual([]);
  });

  it("rejects a duplicate name", async () => {
    const { scheduler: s } = await scheduler();
    await s.create(input());
    await expect(s.create(input())).rejects.toThrow(ConflictError);
  });

  // Regression (was KB-19): `timezone: ""` skipped validation and later failed with a RangeError.
  it("rejects an empty timezone as a validation error, on create and update", async () => {
    const { scheduler: s } = await scheduler();
    await expect(s.create(input({ timezone: "" }))).rejects.toThrow(new ValidationError("unknown timezone "));
    const sched = await s.create(input());
    await expect(s.update(sched.id, { timezone: "" })).rejects.toThrow(ValidationError);
    expect(await s.list()).toHaveLength(1);
  });
});

describe("Scheduler.tick", () => {
  it("enqueues a job for each due schedule and moves on to the next slot", async () => {
    const { scheduler: s, conveyor, clock } = await scheduler();
    const fired: [string, string][] = [];
    conveyor.events.on("schedule.fired", (scheduleId, jobId) => fired.push([scheduleId, jobId]));
    const sched = await s.create(input({ payload: { report: "daily" } }));

    expect(await s.tick()).toEqual([]);
    clock.advance(5 * MIN);
    const [jobId] = await s.tick();
    expect(await conveyor.getJob(jobId!)).toMatchObject({ queue: "q", name: "report", payload: { report: "daily" }, scheduleId: sched.id });
    expect(fired).toEqual([[sched.id, jobId]]);
    expect(await s.get(sched.id)).toMatchObject({
      lastRunAt: at("2026-01-05T12:05:00Z"),
      lastJobId: jobId,
      nextRunAt: at("2026-01-05T12:10:00Z"),
      updatedAt: clock.now(),
    });
    expect(await s.tick()).toEqual([]);
  });

  it("fires when the tick is a little late, and keeps to the schedule's slots", async () => {
    const { scheduler: s, clock } = await scheduler();
    const sched = await s.create(input());
    clock.advance(5 * MIN + 900);
    expect(await s.tick()).toHaveLength(1);
    expect(iso((await s.get(sched.id)).nextRunAt)).toBe("2026-01-05T12:10:00.000Z");
  });

  // After the process has been down across several slots, a schedule runs once and then
  // continues from the next future slot, never once per missed slot.
  it("runs once after downtime across many slots, then continues from the next future slot", async () => {
    const { scheduler: s, conveyor, clock } = await scheduler();
    const sched = await s.create(input());
    clock.advance(60 * MIN + 30_000); // down until 13:00:30; 12 slots missed

    const first = await s.tick();
    expect(first).toHaveLength(1);
    expect(iso((await s.get(sched.id)).nextRunAt)).toBe("2026-01-05T13:05:00.000Z");
    for (let i = 0; i < 5; i++) expect(await s.tick()).toEqual([]);

    clock.advance(4 * MIN + 30_000); // 13:05
    expect(await s.tick()).toHaveLength(1);
    expect((await conveyor.listJobs({ scheduleId: sched.id })).items).toHaveLength(2);
  });

  it("runs a daily schedule once after being down for several days", async () => {
    const { scheduler: s, conveyor, clock } = await scheduler();
    const sched = await s.create(input({ name: "daily", cron: "0 3 * * *" }));
    clock.advance(3 * 24 * 60 * MIN); // 2026-01-08T12:00
    expect(await s.tick()).toHaveLength(1);
    expect(await s.tick()).toEqual([]);
    expect(iso((await s.get(sched.id)).nextRunAt)).toBe("2026-01-09T03:00:00.000Z");
    expect((await conveyor.listJobs({ scheduleId: sched.id })).items).toHaveLength(1);
  });

  it("ignores disabled schedules", async () => {
    const { scheduler: s, clock } = await scheduler();
    await s.create(input({ enabled: false }));
    clock.advance(10 * MIN);
    expect(await s.tick()).toEqual([]);
  });

  it("follows local time across a DST change", async () => {
    const clock = manualClock();
    clock.advance(at("2026-03-07T12:00:00Z") - clock.now());
    const { scheduler: s } = await scheduler({ clock });
    const sched = await s.create(input({ name: "nine", cron: "0 9 * * *", timezone: "America/New_York" }));
    expect(iso(sched.nextRunAt)).toBe("2026-03-07T14:00:00.000Z"); // 09:00 EST
    clock.advance(at("2026-03-07T14:00:00Z") - clock.now());
    await s.tick();
    expect(iso((await s.get(sched.id)).nextRunAt)).toBe("2026-03-08T13:00:00.000Z"); // 09:00 EDT
  });

  it("keeps firing the other schedules when one of them fails", async () => {
    const { logger, records } = memoryLogger();
    const { scheduler: s, conveyor, clock } = await scheduler({ logger });
    const broken = await s.create(input({ name: "broken" }));
    await conveyor.store.updateSchedule(broken.id, { jobName: "not a valid name", nextRunAt: clock.now() + MIN }); // e.g. written by an older release
    const good = await s.create(input({ name: "good" }));
    clock.advance(5 * MIN);

    const fired = await s.tick();
    expect(fired).toHaveLength(1);
    expect((await conveyor.getJob(fired[0]!)).scheduleId).toBe(good.id);
    expect(records).toContainEqual(expect.objectContaining({ level: "error", msg: "schedule failed to fire", schedule: "broken" }));
  });
});

describe("overlap policy", () => {
  it("skip: does not enqueue while the previous run is unfinished, but still moves on", async () => {
    const { scheduler: s, conveyor, broker, clock } = await scheduler();
    const sched = await s.create(input({ overlap: "skip" }));
    clock.advance(5 * MIN);
    const [firstJob] = await s.tick();

    clock.advance(5 * MIN); // 12:10, first job still waiting
    expect(await s.tick()).toEqual([]);
    expect(await s.get(sched.id)).toMatchObject({ lastJobId: firstJob, nextRunAt: at("2026-01-05T12:15:00Z") });

    const lease = await broker.claim(["q"], "w");
    clock.advance(5 * MIN); // 12:15, first job active
    expect(await s.tick()).toEqual([]);

    await broker.complete(lease!.job.id, lease!.token);
    clock.advance(5 * MIN); // 12:20
    expect(await s.tick()).toHaveLength(1);
    expect((await conveyor.listJobs({ scheduleId: sched.id })).items).toHaveLength(2);
  });

  it("allow: enqueues every slot even if earlier runs are unfinished", async () => {
    const { scheduler: s, conveyor, clock } = await scheduler();
    const sched = await s.create(input());
    for (let i = 0; i < 3; i++) {
      clock.advance(5 * MIN);
      expect(await s.tick()).toHaveLength(1);
    }
    expect((await conveyor.listJobs({ scheduleId: sched.id, state: "waiting" })).items).toHaveLength(3);
  });
});

describe("pausing and resuming", () => {
  it("a disabled schedule does not run, and re-enabling it starts from the next future slot", async () => {
    const { scheduler: s, conveyor, clock } = await scheduler();
    const sched = await s.create(input());
    await s.update(sched.id, { enabled: false });
    clock.advance(60 * MIN + 30_000); // 13:00:30
    expect(await s.tick()).toEqual([]);

    const resumed = await s.update(sched.id, { enabled: true });
    expect(iso(resumed.nextRunAt)).toBe("2026-01-05T13:05:00.000Z");
    expect(await s.tick()).toEqual([]);
    clock.advance(4 * MIN + 30_000);
    expect(await s.tick()).toHaveLength(1);
    expect((await conveyor.listJobs({ scheduleId: sched.id })).items).toHaveLength(1);
  });

  it("still enqueues into a paused queue; the jobs run once the queue is resumed", async () => {
    const { scheduler: s, conveyor, broker, clock } = await scheduler();
    await s.create(input());
    await conveyor.pause("q");
    clock.advance(5 * MIN);
    const [jobId] = await s.tick();
    expect(await broker.claim(["q"], "w")).toBeUndefined();
    await conveyor.resume("q");
    expect((await broker.claim(["q"], "w"))?.job.id).toBe(jobId);
  });
});

describe("Scheduler.update", () => {
  it("recomputes the next run when the cron or timezone changes, not for other fields", async () => {
    const { scheduler: s, clock } = await scheduler();
    const sched = await s.create(input());
    clock.advance(MIN);
    expect((await s.update(sched.id, { payload: { x: 1 }, queue: "q2", jobName: "j2", overlap: "skip" })).nextRunAt).toBe(sched.nextRunAt);
    const hourly = await s.update(sched.id, { cron: "0 * * * *" });
    expect(iso(hourly.nextRunAt)).toBe("2026-01-05T13:00:00.000Z");
    const tokyo = await s.update(sched.id, { cron: "0 9 * * *", timezone: "Asia/Tokyo" });
    expect(iso(tokyo.nextRunAt)).toBe("2026-01-06T00:00:00.000Z");
    expect(await s.get(sched.id)).toMatchObject({ payload: { x: 1 }, queue: "q2", jobName: "j2", overlap: "skip", updatedAt: clock.now() });
  });

  it("can set the payload to null", async () => {
    const { scheduler: s } = await scheduler();
    const sched = await s.create(input({ payload: { a: 1 } }));
    expect((await s.update(sched.id, { payload: null })).payload).toBeNull();
  });

  it("validates the result", async () => {
    const { scheduler: s } = await scheduler();
    const sched = await s.create(input());
    await expect(s.update(sched.id, { cron: "nope" })).rejects.toThrow(ValidationError);
    await expect(s.update(sched.id, { cron: "0 0 31 2 *" })).rejects.toThrow(ValidationError);
    await expect(s.update("nope", {})).rejects.toThrow(NotFoundError);
  });

  // Previously the UNIQUE constraint surfaced as a plain Error (HTTP 500).
  it("refuses to rename a schedule to a name that is taken", async () => {
    const { scheduler: s } = await scheduler();
    await s.create(input({ name: "a" }));
    const b = await s.create(input({ name: "b" }));
    await expect(s.update(b.id, { name: "a" })).rejects.toThrow(ConflictError);
    expect((await s.update(b.id, { name: "b" })).name).toBe("b");
    expect((await s.update(b.id, { name: "c" })).name).toBe("c");
  });
});

describe("trigger, list and remove", () => {
  it("trigger enqueues a run now without changing the schedule's timing", async () => {
    const { scheduler: s, conveyor } = await scheduler();
    const sched = await s.create(input({ payload: { manual: true } }));
    const jobId = await s.trigger(sched.id);
    expect(await conveyor.getJob(jobId)).toMatchObject({ scheduleId: sched.id, payload: { manual: true } });
    expect(await s.get(sched.id)).toEqual(sched);
    await expect(s.trigger("nope")).rejects.toThrow(NotFoundError);
  });

  it("lists by name and removes", async () => {
    const { scheduler: s } = await scheduler();
    const b = await s.create(input({ name: "b" }));
    await s.create(input({ name: "a" }));
    expect((await s.list()).map((x) => x.name)).toEqual(["a", "b"]);
    await s.remove(b.id);
    await expect(s.remove(b.id)).rejects.toThrow(NotFoundError);
    await expect(s.get(b.id)).rejects.toThrow(NotFoundError);
  });
});

// start()/stop() use setInterval, so these tests use vitest's fake timers for the interval;
// the schedule times still come from the ManualClock.
describe("Scheduler.start / stop", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("ticks on every interval until stopped", async () => {
    vi.useFakeTimers();
    const { scheduler: s, conveyor, clock } = await scheduler();
    const sched = await s.create(input({ cron: "* * * * *" }));
    s.start(1000);
    s.start(1000); // idempotent
    for (let i = 0; i < 3; i++) {
      clock.advance(MIN);
      await vi.advanceTimersByTimeAsync(1000);
    }
    expect((await conveyor.listJobs({ scheduleId: sched.id })).items).toHaveLength(3);
    s.stop();
    clock.advance(MIN);
    await vi.advanceTimersByTimeAsync(5000);
    expect((await conveyor.listJobs({ scheduleId: sched.id })).items).toHaveLength(3);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("uses the configured interval by default and logs a failing tick instead of throwing", async () => {
    vi.useFakeTimers();
    const { logger, records } = memoryLogger();
    const { scheduler: s, conveyor } = await scheduler({ logger, config: { schedulerIntervalMs: 250 } });
    vi.spyOn(conveyor.store, "findDueSchedules").mockRejectedValue(new Error("db unavailable"));
    s.start();
    await vi.advanceTimersByTimeAsync(250);
    expect(records).toContainEqual(expect.objectContaining({ level: "error", msg: "scheduler tick failed" }));
    s.stop();
  });

  it("never runs two ticks at once", async () => {
    vi.useFakeTimers();
    const { scheduler: s, conveyor } = await scheduler();
    let inFlight = 0;
    let maxInFlight = 0;
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    vi.spyOn(conveyor.store, "findDueSchedules").mockImplementation(async () => {
      maxInFlight = Math.max(maxInFlight, ++inFlight);
      await gate;
      inFlight--;
      return [];
    });
    s.start(100);
    await vi.advanceTimersByTimeAsync(1000);
    release();
    await vi.advanceTimersByTimeAsync(100);
    s.stop();
    expect(maxInFlight).toBe(1);
  });
});
