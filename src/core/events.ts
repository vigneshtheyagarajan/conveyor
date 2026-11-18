import { EventEmitter } from "node:events";
import type { Job } from "./types.js";

export interface ConveyorEvents {
  "job.enqueued": [job: Job];
  "job.claimed": [job: Job, workerId: string];
  "job.completed": [job: Job];
  "job.failed": [job: Job, error: string];
  "job.retrying": [job: Job, error: string, delayMs: number];
  "job.dead": [job: Job, error: string];
  "job.cancelled": [job: Job];
  "job.reaped": [job: Job];
  "job.progress": [job: Job, progress: number];
  "queue.paused": [queue: string];
  "queue.resumed": [queue: string];
  "schedule.fired": [scheduleId: string, jobId: string];
}

export type EventName = keyof ConveyorEvents;

/**
 * Typed wrapper over EventEmitter. Listener errors are isolated: a listener that throws, or an
 * async listener whose promise rejects, is reported to onListenerError and never reaches the
 * emitter or the process.
 */
export class EventBus {
  private emitter = new EventEmitter();

  constructor() {
    this.emitter.setMaxListeners(100);
  }

  on<E extends EventName>(event: E, listener: (...args: ConveyorEvents[E]) => void): () => void {
    const report = (err: unknown) => this.emitter.emit("listenerError", err, event);
    const wrapped = (...args: unknown[]) => {
      try {
        const result: unknown = listener(...(args as ConveyorEvents[E]));
        if (isThenable(result)) result.then(undefined, report);
      } catch (err) {
        report(err);
      }
    };
    this.emitter.on(event, wrapped);
    return () => this.emitter.off(event, wrapped);
  }

  onAny(listener: (event: EventName, ...args: unknown[]) => void): () => void {
    const offs = (ALL_EVENTS as EventName[]).map((e) =>
      this.on(e, (...args: unknown[]) => listener(e, ...args)),
    );
    return () => offs.forEach((off) => off());
  }

  onListenerError(listener: (err: unknown, event: string) => void): void {
    this.emitter.on("listenerError", listener);
  }

  emit<E extends EventName>(event: E, ...args: ConveyorEvents[E]): void {
    this.emitter.emit(event, ...args);
  }
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return typeof (value as PromiseLike<unknown> | null)?.then === "function";
}

export const ALL_EVENTS: EventName[] = [
  "job.enqueued",
  "job.claimed",
  "job.completed",
  "job.failed",
  "job.retrying",
  "job.dead",
  "job.cancelled",
  "job.reaped",
  "job.progress",
  "queue.paused",
  "queue.resumed",
  "schedule.fired",
];
