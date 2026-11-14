import { randomUUID } from "node:crypto";
import type { Conveyor } from "../core/conveyor.js";
import { ConflictError, NotFoundError, ValidationError } from "../core/errors.js";
import type { Logger } from "../core/logger.js";
import type { OverlapPolicy, Schedule } from "../core/types.js";
import type { Json } from "../util/json.js";
import { NAME_PATTERN } from "../util/validate.js";
import { isValidTimezone, nextRun, parseCron } from "./cron.js";

export interface ScheduleInput {
  name: string;
  queue: string;
  jobName: string;
  cron: string;
  payload?: Json;
  timezone?: string;
  enabled?: boolean;
  overlap?: OverlapPolicy;
}

// Turns cron schedules into jobs
export class Scheduler {
  private timer?: NodeJS.Timeout;
  private ticking = false;
  private logger: Logger;

  constructor(private conveyor: Conveyor) {
    this.logger = conveyor.logger.child({ component: "scheduler" });
  }

  private get store() {
    return this.conveyor.store;
  }

  start(intervalMs = this.conveyor.config.schedulerIntervalMs): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      if (this.ticking) return;
      this.ticking = true;
      this.tick()
        .catch((err) => this.logger.error("scheduler tick failed", { err }))
        .finally(() => (this.ticking = false));
    }, intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  /** Fire all due schedules once. Returns the ids of jobs enqueued. */
  async tick(): Promise<string[]> {
    const now = this.conveyor.clock.now();
    const due = await this.store.findDueSchedules(now, 100);
    const fired: string[] = [];
    for (const schedule of due) {
      const jobId = await this.fire(schedule);
      if (jobId) fired.push(jobId);
    }
    return fired;
  }

  private async fire(schedule: Schedule): Promise<string | null> {
    const now = this.conveyor.clock.now();
    let jobId: string | null = null;

    if (schedule.overlap === "skip" && (await this.store.hasUnfinishedForSchedule(schedule.id))) {
      this.logger.info("skipping run; previous run still in progress", { schedule: schedule.name });
    } else {
      const job = await this.conveyor.enqueue({
        queue: schedule.queue,
        name: schedule.jobName,
        payload: schedule.payload,
        scheduleId: schedule.id,
      });
      jobId = job.id;
      this.conveyor.events.emit("schedule.fired", schedule.id, job.id);
    }

    const updated: Schedule = {
      ...schedule,
      lastRunAt: schedule.nextRunAt,
      lastJobId: jobId ?? schedule.lastJobId,
      nextRunAt: nextRun(schedule.cron, schedule.nextRunAt, schedule.timezone),
      updatedAt: now,
    };
    await this.store.updateSchedule(updated);
    return jobId;
  }

  async create(input: ScheduleInput): Promise<Schedule> {
    this.validate(input);
    if (await this.store.getScheduleByName(input.name)) {
      throw new ConflictError(`schedule ${input.name} already exists`);
    }
    const now = this.conveyor.clock.now();
    const timezone = input.timezone ?? "UTC";
    const schedule: Schedule = {
      id: randomUUID(),
      name: input.name,
      queue: input.queue,
      jobName: input.jobName,
      payload: input.payload ?? null,
      cron: input.cron,
      timezone,
      enabled: input.enabled ?? true,
      overlap: input.overlap ?? "allow",
      nextRunAt: nextRun(input.cron, now, timezone),
      lastRunAt: null,
      lastJobId: null,
      createdAt: now,
      updatedAt: now,
    };
    await this.store.insertSchedule(schedule);
    return schedule;
  }

  async update(id: string, patch: Partial<ScheduleInput>): Promise<Schedule> {
    const current = await this.get(id);
    const merged = {
      name: patch.name ?? current.name,
      queue: patch.queue ?? current.queue,
      jobName: patch.jobName ?? current.jobName,
      cron: patch.cron ?? current.cron,
      payload: patch.payload !== undefined ? patch.payload : current.payload,
      timezone: patch.timezone ?? current.timezone,
      enabled: patch.enabled ?? current.enabled,
      overlap: patch.overlap ?? current.overlap,
    };
    this.validate(merged);
    const now = this.conveyor.clock.now();
    const timingChanged =
      merged.cron !== current.cron || merged.timezone !== current.timezone || (merged.enabled && !current.enabled);
    const next: Schedule = {
      ...current,
      ...merged,
      nextRunAt: timingChanged ? nextRun(merged.cron, now, merged.timezone) : current.nextRunAt,
      updatedAt: now,
    };
    await this.store.updateSchedule(next);
    return next;
  }

  async get(id: string): Promise<Schedule> {
    const s = await this.store.getSchedule(id);
    if (!s) throw new NotFoundError(`schedule ${id} not found`);
    return s;
  }

  list(): Promise<Schedule[]> {
    return this.store.listSchedules();
  }

  async remove(id: string): Promise<void> {
    if (!(await this.store.deleteSchedule(id))) throw new NotFoundError(`schedule ${id} not found`);
  }

  /** Enqueue a run now without changing the schedule's timing. */
  async trigger(id: string): Promise<string> {
    const s = await this.get(id);
    const job = await this.conveyor.enqueue({
      queue: s.queue,
      name: s.jobName,
      payload: s.payload,
      scheduleId: s.id,
    });
    this.conveyor.events.emit("schedule.fired", s.id, job.id);
    return job.id;
  }

  private validate(input: ScheduleInput): void {
    if (!NAME_PATTERN.test(input.name)) throw new ValidationError("invalid schedule name");
    if (!NAME_PATTERN.test(input.queue)) throw new ValidationError("invalid queue name");
    if (!NAME_PATTERN.test(input.jobName)) throw new ValidationError("invalid job name");
    try {
      parseCron(input.cron);
    } catch (err) {
      throw new ValidationError((err as Error).message);
    }
    if (input.timezone && !isValidTimezone(input.timezone)) {
      throw new ValidationError(`unknown timezone ${input.timezone}`);
    }
    if (input.overlap && input.overlap !== "allow" && input.overlap !== "skip") {
      throw new ValidationError("overlap must be 'allow' or 'skip'");
    }
  }
}
