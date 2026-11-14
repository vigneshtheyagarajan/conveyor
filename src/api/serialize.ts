import type { ApiToken, Job, Schedule, Webhook } from "../core/types.js";

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
    timeoutMs: j.timeoutMs,
    runAt: iso(j.runAt),
    createdAt: iso(j.createdAt),
    startedAt: iso(j.startedAt),
    finishedAt: iso(j.finishedAt),
    workerId: j.workerId,
    leaseExpiresAt: iso(j.leaseExpiresAt),
  };
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
