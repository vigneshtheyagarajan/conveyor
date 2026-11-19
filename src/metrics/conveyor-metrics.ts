import type { Conveyor } from "../core/conveyor.js";
import { JOB_STATES } from "../core/types.js";
import { MetricsRegistry } from "./registry.js";

/** Wire standard metrics to a Conveyor instance's events and store. */
export function conveyorMetrics(conveyor: Conveyor): MetricsRegistry {
  const reg = new MetricsRegistry();
  const enqueued = reg.counter("conveyor_jobs_enqueued_total", "Jobs enqueued");
  const completed = reg.counter("conveyor_jobs_completed_total", "Jobs completed successfully");
  const failed = reg.counter("conveyor_jobs_failed_total", "Job attempts that failed");
  const dead = reg.counter("conveyor_jobs_dead_total", "Jobs moved to the dead state");
  const reaped = reg.counter("conveyor_jobs_reaped_total", "Jobs whose lease expired");
  const duration = reg.histogram("conveyor_job_duration_seconds", "Time from claim to completion");
  const waitTime = reg.histogram("conveyor_job_wait_seconds", "Time from run_at to claim");
  const depth = reg.gauge("conveyor_queue_jobs", "Jobs by queue and state (including blocked: waiting for dependencies)");
  const blocked = reg.histogram(
    "conveyor_job_blocked_seconds",
    "Time a job spent blocked on its dependencies before it was released",
    [0.1, 1, 10, 60, 300, 900, 3600, 14400, 86400],
  );
  const cancelledByDependency = reg.counter(
    "conveyor_jobs_cancelled_by_dependency_total",
    "Jobs cancelled without running because a dependency failed or was cancelled",
  );
  const flows = reg.counter("conveyor_flows_finished_total", "Flow verdicts recorded, by event");

  const ev = conveyor.events;
  ev.on("job.enqueued", (j) => enqueued.inc({ queue: j.queue }));
  ev.on("job.claimed", (j) => {
    if (j.startedAt) waitTime.observe({ queue: j.queue }, Math.max(0, j.startedAt - j.runAt) / 1000);
  });
  ev.on("job.completed", (j) => {
    completed.inc({ queue: j.queue });
    if (j.startedAt && j.finishedAt) duration.observe({ queue: j.queue }, (j.finishedAt - j.startedAt) / 1000);
  });
  ev.on("job.failed", (j) => failed.inc({ queue: j.queue }));
  ev.on("job.dead", (j) => dead.inc({ queue: j.queue }));
  ev.on("job.reaped", (j) => reaped.inc({ queue: j.queue }));
  ev.on("job.unblocked", (j, blockedMs) => blocked.observe({ queue: j.queue }, blockedMs / 1000));
  ev.on("job.cancelled", (j) => {
    if (j.terminatedBy !== null) cancelledByDependency.inc({ queue: j.queue });
  });
  ev.on("flow.completed", () => flows.inc({ event: "flow.completed" }));
  ev.on("flow.failed", () => flows.inc({ event: "flow.failed" }));

  reg.collect(async () => {
    depth.reset();
    for (const q of await conveyor.store.listQueues()) {
      const counts = await conveyor.store.countJobs(q.name);
      for (const state of JOB_STATES) depth.set({ queue: q.name, state }, counts[state]);
    }
  });
  return reg;
}
