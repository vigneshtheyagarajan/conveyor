# Changelog

## Unreleased

New: job dependencies. `enqueue({ ..., dependsOn: [id, { jobId, onFailure }] })` makes a job
wait for other jobs in the new `blocked` state. When a dependency ends dead or cancelled, the
edge's policy decides: `fail` (default) cancels the dependent without running it (with
`terminatedBy` set), `ignore` and `continue` let it run. `retry()` brings back the dependents
a failure cancelled, and refuses (409, naming it) to retry a job whose dependency retention has
deleted. Migration 8 adds the `job_dependencies` and `flow_events` tables, the `flow_id`,
`flow_key` and `terminated_by` columns, and a unique index on `(flow_id, flow_key)`. See the
README's "Dependencies and flows" and docs/architecture.md.

New: flows. `conveyor.enqueueFlow({ jobs, onFailure })` submits a graph of new jobs that name
each other by `key` and may depend on existing jobs by id, all or nothing; `getFlow` returns the
jobs, edges and a derived status (`running`, `completed`, `failed`, `cancelled`); `cancelFlow`
cancels every unfinished job of a flow. HTTP: `POST /v1/flows`, `GET /v1/flows/:id`,
`POST /v1/flows/:id/cancel`, `dependsOn` on `POST /v1/jobs` and `/v1/jobs/bulk`, and
`GET /v1/jobs/:id/dependencies` and `/dependents`. CLI: `enqueue --depends-on ... [--on-failure p]`,
`flow-submit <file|->`, `flow <id>`, `flow-cancel <id>`. Jobs returned by the API gain `flowId`,
`flowKey` and `terminatedBy`.

New: dependency results. In-process handlers read their dependencies with `ctx.dependencies()`
and `ctx.dependency(key)` (loaded lazily, cached per attempt; `dependency` throws a
`DependencyError` if the dependency didn't complete). Under `ignore` a failed dependency is left
out of the list; under `continue` it is listed with its state and error. HTTP leases include
`dependencies` (`{id, key, name, state}`), and `GET /v1/jobs/:id/dependencies` now returns
results, paged (`cursor`, `limit`, and `all=true` for failed `ignore` dependencies).
`threaded(path, name, { dependencies: true })` passes them to the thread function as a second
argument.

New: flow webhooks. `flow.failed` (as soon as a job in the flow is dead or cancelled) and
`flow.completed` (once every job has completed); one verdict per flow, delivered once, from the
database, so they are sent whichever process ran the jobs. `GET /v1/flows/:id` shows the
verdict as `outcome`.

New metrics: `conveyor_job_blocked_seconds`, `conveyor_jobs_cancelled_by_dependency_total` and
`conveyor_flows_finished_total`; `conveyor_queue_jobs` includes `state="blocked"`.

Retention keeps completed and cancelled jobs while a job that depends on them is unfinished.

`POST /v1/queues/:name/retry-dead` returns `{retried, skipped}`: jobs that can't be retried are
skipped with the reason instead of stopping the batch (`Conveyor.retryDead`; `retryAllDead`
still returns the count). `conveyor dlq --retry` lists them.

Known limitations: see docs/architecture.md ("retry the whole flow" is not built).

Breaking changes for `Store` implementations:

- Transitions are guarded and return what they changed: `completeJob(id, leaseToken, result, now)`,
  `scheduleRetry(id, leaseToken, error, runAt, now)`, `markDead(id, leaseToken, error, now)`,
  `releaseLease(id, leaseToken, now)`, `cancelJob(id, now)` and the new `retryJob(id, now)` and
  `requeueExpired(id, leaseToken, now)` return a `TransitionResult` (the job, its previous state,
  the dependents changed as a consequence and the flow verdicts recorded), or `undefined` without changing anything if the
  job isn't in the expected state or the lease token doesn't match. Each writes its history
  entry itself, in the same transaction; callers no longer call `appendJobEvent` for them.
- `requeueJob` is removed: use `retryJob` (dead/cancelled -> waiting) or `requeueExpired` (the
  reaper).
- `insertJobs(jobs, events, dependencies?)` takes dependency edges and returns the jobs it
  released at once.
- `deleteFinishedBefore` must keep jobs that an unfinished job depends on.
- New: `listDependencies(jobId)`, `listDependencyJobs(jobId, { after, limit, visibleOnly })`, `listDependentJobs(jobId, page)`,
  `reconcileBlocked(now, limit)` (returns `{ jobs, flowOutcomes }`), for flows `insertFlow(flow)`,
  `cancelFlow(flowId, now)`, `listFlowJobs(flowId)` and `listFlowDependencies(flowId)`, and for
  verdicts `getFlowOutcome`, `listPendingFlowOutcomes` and `claimFlowOutcome`.
- Transitions that finish a flow job must record the flow's verdict in the same transaction, at
  most once per flow. A retry must refuse a job whose dependency no longer exists: edges are
  kept when their dependency is deleted.
- `Job` has three new fields: `flowId`, `flowKey` and `terminatedBy`.
- Every transition must check the dependents it affects in the same transaction as the change
  (see "Why unblocking can't be missed" in docs/architecture.md).

Behaviour changes:

- New job state `blocked`. It appears in queue counts (`counts.blocked`), the `conveyor_queue_jobs`
  metric, `GET /v1/jobs?state=blocked`, `conveyor jobs --state blocked` and a new column in
  `conveyor queues`. A blocked job counts as unfinished for `uniqueKey` and for schedules with
  `overlap: "skip"`.
- Completing, failing, cancelling, retrying and releasing a job, and the reaper's requeue, are
  atomic and can't overwrite each other: of two that race, exactly one changes the job (fixes
  KB-13). A completion or failure reported for a job that was cancelled or reaped in the meantime
  gets `LeaseLostError`; a second cancel or retry gets a conflict.
- A change to a job and its history entry are written together; before, a failed history
  write could leave the change without its entry.
- New in-process event `job.unblocked` (with how long the job was blocked; not delivered to
  webhooks); in-process workers wake up on it. A dependent cancelled because its dependency failed emits `job.cancelled`.
- `Maintenance` runs a new reconcile sweep (`reconcileIntervalMs`, default 5s).

- Jobs with the same priority now run strictly first-in-first-out, including jobs enqueued in the same millisecond.
- `enqueueBulk` / `POST /v1/jobs/bulk` is all-or-nothing: if any job is invalid or the insert fails, no job is created.
- Releasing a lease (for example on worker shutdown) no longer uses up one of the job's attempts.
- A worker that reaches its shutdown timeout hands running jobs back instead of failing them.
- Retention (`CONVEYOR_RETENTION_MS`) deletes completed and cancelled jobs only; dead jobs are kept.
- After downtime, a schedule runs once and continues from its next future slot, instead of once per missed slot.
- Cron: Vixie cron rules for `*`-prefixed day fields, correct wall-clock times across daylight-saving changes (see docs/architecture.md), and stricter parsing (`1-5-9`, `1/5/7` are rejected).
- Webhooks can only subscribe to the events they receive (`job.enqueued`, `job.completed`, `job.failed`, `job.retrying`, `job.dead`, `job.cancelled`, `*`).
- `migrate()` refuses a database written by a newer version of Conveyor.
- Pending webhook deliveries survive a restart: `start()` resumes them at their stored next attempt time (new migration 7 adds an index for this; `Store` gains `findPendingDeliveries`).
- `Worker.stop()` always finishes within `shutdownTimeoutMs`: jobs whose handlers ignore the abort signal are handed back, and a job claimed while stopping is handed back without running.
- CLI: `--help` exits 0; number options must be integers (`--delay abc` is an error, not NaN); `token-revoke` of an unknown or revoked token exits 1; `cancel` and `retry` now actually cancel/retry without `--json` (they used to only print a message) and exit 1 when nothing changed; `jobs --state` rejects unknown states.
- Limits: `runAt` must be between 1970-01-01 and 9999-12-31T23:59:59.999Z, and every delay (`delayMs`, `retryInMs`, backoff settings) is at most 100 years (`MAX_RUN_AT`, `MAX_DELAY_MS`). Larger values are a 400 instead of a stored job that could not be displayed.
- API: a body over the size limit is answered with 413 instead of a dropped connection; `GET /v1/schedules/:id` computes `upcoming` from Conveyor's clock.

Fixes: an empty schedule timezone is a 400; `stop()` no longer waits out a poll interval; any thrown value (including `undefined`) is recorded as a job failure; NaN/Infinity `runAt` and `progress` are rejected as validation errors; malformed URL encoding returns 400; a schedule whose cron never matches, or a rename onto an existing name, is rejected with 400/409; one failing schedule no longer stops the others; deleted webhooks are no longer retried; rejected promises from async event listeners are reported to `onListenerError`; empty `CONVEYOR_DB`, `CONVEYOR_HOST` and `CONVEYOR_LOG_LEVEL` fall back to their defaults; changing a rate limit no longer grants a fresh burst; `stop()` no longer leaves a timer running.

## 0.1.0

First release.

- SQLite-backed queues with priorities, delayed jobs, retries with backoff and dead jobs
- Leases with heartbeats and a reaper for crashed workers
- In-process workers, worker-thread handlers, and an HTTP worker protocol
- Cron schedules with timezones
- Unique jobs, queue pause/resume, concurrency and rate limits
- Signed webhooks, scoped API tokens, Prometheus metrics, CLI
