import type { ApiToken, Flow, Job, LinkedJob, Schedule, Webhook } from "../core/types.js";

const iso = (ms: number | null) => (ms == null ? null : new Date(ms).toISOString());

export function jobView(j: Job) {
  return {
    id: j.id,
    queue: j.queue,
    name: j.name,
    state: j.state,
    payload: j.payload,
    priority: j.priority,
    attempts: j.attempts,
    maxAttempts: j.maxAttempts,
    progress: j.progress,
    result: j.result,
    lastError: j.lastError,
    uniqueKey: j.uniqueKey,
    scheduleId: j.scheduleId,
    flowId: j.flowId,
    flowKey: j.flowKey,
    terminatedBy: j.terminatedBy,
    timeoutMs: j.timeoutMs,
    runAt: iso(j.runAt),
    createdAt: iso(j.createdAt),
    startedAt: iso(j.startedAt),
    finishedAt: iso(j.finishedAt),
    workerId: j.workerId,
    leaseExpiresAt: iso(j.leaseExpiresAt),
  };
}

/** A job at the other end of a dependency edge. */
export function linkedJobView(l: LinkedJob) {
  return { onFailure: l.onFailure, job: jobView(l.job) };
}

export function flowView(f: Flow) {
  const outcome = f.outcome && {
    event: f.outcome.event,
    reason: f.outcome.reason,
    jobId: f.outcome.jobId,
    at: iso(f.outcome.at),
    dispatchedAt: iso(f.outcome.dispatchedAt),
  };
  return { id: f.id, status: f.status, outcome, counts: f.counts, jobs: f.jobs.map(jobView), edges: f.edges };
}

export function scheduleView(s: Schedule) {
  return {
    ...s,
    nextRunAt: iso(s.nextRunAt),
    lastRunAt: iso(s.lastRunAt),
    createdAt: iso(s.createdAt),
    updatedAt: iso(s.updatedAt),
  };
}

/** The secret is only returned when the webhook is created. */
export function webhookView(w: Webhook, includeSecret = false) {
  const { secret, ...rest } = w;
  return { ...rest, createdAt: iso(w.createdAt), ...(includeSecret ? { secret } : {}) };
}

export function tokenView(t: ApiToken) {
  const { hash: _hash, ...rest } = t;
  return {
    ...rest,
    createdAt: iso(t.createdAt),
    lastUsedAt: iso(t.lastUsedAt),
    revokedAt: iso(t.revokedAt),
  };
}
