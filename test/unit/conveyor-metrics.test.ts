import { describe, expect, it } from "vitest";
import type { Conveyor } from "../../src/core/conveyor.js";
import { EventBus } from "../../src/core/events.js";
import type { Job, QueueCounts, QueueSettings } from "../../src/core/types.js";
import { conveyorMetrics } from "../../src/metrics/conveyor-metrics.js";
import { emptyCounts } from "../../src/storage/store.js";

function fakeConveyor(queues: Record<string, Partial<QueueCounts>>) {
  const events = new EventBus();
  const store = {
    listQueues: async () => Object.keys(queues).map((name) => ({ name }) as QueueSettings),
    countJobs: async (q: string) => ({ ...emptyCounts(), ...queues[q] }),
  };
  return { conveyor: { events, store } as unknown as Conveyor, events, queues };
}

const job = (over: Partial<Job>) => ({ queue: "q", runAt: 0, startedAt: null, finishedAt: null, ...over }) as Job;
const line = (text: string, prefix: string) => text.split("\n").find((l) => l.startsWith(prefix));

describe("conveyorMetrics", () => {
  it("counts lifecycle events per queue", async () => {
    const { conveyor, events } = fakeConveyor({});
    const reg = conveyorMetrics(conveyor);
    events.emit("job.enqueued", job({}));
    events.emit("job.enqueued", job({ queue: "other" }));
    events.emit("job.failed", job({}), "e");
    events.emit("job.dead", job({}), "e");
    events.emit("job.reaped", job({}));
    const text = await reg.render();
    expect(line(text, 'conveyor_jobs_enqueued_total{queue="q"}')).toBe('conveyor_jobs_enqueued_total{queue="q"} 1');
    expect(line(text, 'conveyor_jobs_enqueued_total{queue="other"}')).toBe('conveyor_jobs_enqueued_total{queue="other"} 1');
    expect(line(text, "conveyor_jobs_failed_total{")).toBe('conveyor_jobs_failed_total{queue="q"} 1');
    expect(line(text, "conveyor_jobs_dead_total{")).toBe('conveyor_jobs_dead_total{queue="q"} 1');
    expect(line(text, "conveyor_jobs_reaped_total{")).toBe('conveyor_jobs_reaped_total{queue="q"} 1');
  });

  it("observes wait time (run_at to claim) and duration (claim to completion) in seconds", async () => {
    const { conveyor, events } = fakeConveyor({});
    const reg = conveyorMetrics(conveyor);
    events.emit("job.claimed", job({ runAt: 1000, startedAt: 3500 }), "w");
    events.emit("job.claimed", job({ runAt: 5000, startedAt: 4000 }), "w"); // clock skew: clamp to 0
    events.emit("job.claimed", job({ startedAt: null }), "w"); // ignored
    events.emit("job.completed", job({ startedAt: 1000, finishedAt: 1250 }));
    events.emit("job.completed", job({ startedAt: null, finishedAt: 1250 }));
    const text = await reg.render();
    expect(line(text, "conveyor_job_wait_seconds_sum")).toBe('conveyor_job_wait_seconds_sum{queue="q"} 2.5');
    expect(line(text, "conveyor_job_wait_seconds_count")).toBe('conveyor_job_wait_seconds_count{queue="q"} 2');
    expect(line(text, "conveyor_job_duration_seconds_sum")).toBe('conveyor_job_duration_seconds_sum{queue="q"} 0.25');
    expect(line(text, "conveyor_job_duration_seconds_count")).toBe('conveyor_job_duration_seconds_count{queue="q"} 1');
    expect(line(text, "conveyor_jobs_completed_total{")).toBe('conveyor_jobs_completed_total{queue="q"} 2');
  });

  it("reports queue depth by state from the store at scrape time", async () => {
    const { conveyor, queues } = fakeConveyor({ q: { waiting: 3, active: 1 } });
    const reg = conveyorMetrics(conveyor);
    let text = await reg.render();
    expect(line(text, 'conveyor_queue_jobs{queue="q",state="waiting"}')).toBe('conveyor_queue_jobs{queue="q",state="waiting"} 3');
    expect(line(text, 'conveyor_queue_jobs{queue="q",state="dead"}')).toBe('conveyor_queue_jobs{queue="q",state="dead"} 0');
    expect(text.match(/^conveyor_queue_jobs\{/gm)).toHaveLength(6);

    delete queues.q;
    queues.r = { completed: 2 };
    text = await reg.render();
    expect(text).not.toContain('queue="q",state=');
    expect(line(text, 'conveyor_queue_jobs{queue="r",state="completed"}')).toBe('conveyor_queue_jobs{queue="r",state="completed"} 2');
  });
});
