# Changelog

## Unreleased

Behaviour changes:

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
