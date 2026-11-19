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
- Claims are atomic: `Store.claimNext()` replaces `findClaimCandidate()` + `activateJob()` and picks and leases a job in one `BEGIN IMMEDIATE` transaction. Concurrent claims (worker loops, HTTP workers, several processes) no longer lease the same job twice, and queue concurrency limits hold across processes.
- Lease calls are conditional on the lease token: `extendLease`, `updateProgress`, `completeJob`, `scheduleRetry`, `markDead` and `releaseLease` take the token and return whether they changed the job. A worker whose lease was lost between its check and its write now gets `LeaseLostError` instead of overwriting the job's new holder.
- Rate limits are documented as per process. A queue that is out of rate-limit tokens no longer blocks the other queues in the same claim.
- An expired lease counts as a failed attempt. The reaper retries the job after its normal backoff (it used to go straight back to `waiting`), or moves it to dead once it has no attempts left, with an error such as "lease expired 3 times; worker may be crashing" (it used to be retried forever). Reaping now also emits `job.failed` and `job.retrying` / `job.dead`, so webhooks and metrics see these failures. Log messages: "lease expired, retry scheduled" (warn) and "lease expired, job is dead" (error).
- The reaper only ends a lease that is unchanged since it read it (same token and expiry), so a heartbeat or completion in between wins and any number of reapers can run at once. Store: `expireLease()` replaces `requeueJob()`.
- Unique keys are enforced by the database (migration 8: a partial unique index, one unfinished job per queue and key). The check and the insert are one transaction, so concurrent enqueues with the same key, from any number of processes, create one job and the others get it back, as before. On upgrade, a database that already holds duplicates keeps one job per key (an active one, else the oldest); the other waiting or delayed duplicates are cancelled, and other active ones lose their key but keep running; each change is recorded as a job event. Store: `insertJobs()` returns the stored or existing job for each input; `findActiveByUniqueKey()` is removed.
- Retrying a dead or cancelled job whose unique key a newer unfinished job holds is refused with a 409 until that job finishes.
- Schedules fire each slot once, however many `serve` processes run: `Store.fireSchedule()` checks the slot (compare-and-set on `next_run_at`), applies overlap "skip", inserts the job and advances the schedule in one transaction. A tick writes only the timing columns, and `Scheduler.update` / `PATCH /v1/schedules/:id` write only the fields given, so an edit made during a tick is kept and an edit can no longer make a slot fire twice. Store: `updateSchedule(id, patch)` replaces `updateSchedule(schedule)`; `hasUnfinishedForSchedule()` is removed. `Conveyor` gains `buildJob()` and `announceEnqueued()`.
- Several processes can open and migrate a new database file at the same time: each migration re-reads the schema version inside a `BEGIN IMMEDIATE` transaction, and switching a new file to WAL is retried while it is locked.
- A timed-out job keeps its lease until its handler has stopped: the worker aborts the signal, keeps heartbeating and records the failure only when the handler returns or throws (or when `stop()` gives up on it). A retry can no longer run next to the timed-out handler, even one that ignores its signal; such a handler holds its job and worker slot until it returns.
- An enqueue wakes every idle worker loop, not only the one that went idle last, and so does resuming one of the worker's queues (it used to wait out the poll backoff, up to `maxPollMs`). Both are in-process events: work that appears through another process is found by polling.
- `npm run stress`: an opt-in multi-process stress test (workers killed with SIGKILL and restarted, a separate reaper, an enqueuer adding jobs while they run) that checks no job is lost, stuck, completed twice or run twice at once, and that unique keys hold. See `stress/README.md`.
- Every `Worker.stop()` call returns the same promise. A second call used to return at once, while running jobs were still being finished or handed back.
- Documented: what happens to handlers that ignore their abort signal, and that CPU-heavy synchronous handlers block heartbeats (at-least-once; use `threaded()`).
- Cancel and retry check the state and change it in one step. `cancel` applies only to delayed, waiting or active jobs and never overwrites a job that completed or died meanwhile; `retry` applies only to dead or cancelled jobs, so of two concurrent retries one gets a 409. Store: `cancelJob()` and the new `retryJob()` return `{ changed, from }` and record their event themselves. `retryAllDead` skips jobs someone else retried meanwhile and returns how many it retried.

Fixes: an empty schedule timezone is a 400; `stop()` no longer waits out a poll interval; any thrown value (including `undefined`) is recorded as a job failure; NaN/Infinity `runAt` and `progress` are rejected as validation errors; malformed URL encoding returns 400; a schedule whose cron never matches, or a rename onto an existing name, is rejected with 400/409; one failing schedule no longer stops the others; deleted webhooks are no longer retried; rejected promises from async event listeners are reported to `onListenerError`; empty `CONVEYOR_DB`, `CONVEYOR_HOST` and `CONVEYOR_LOG_LEVEL` fall back to their defaults; changing a rate limit no longer grants a fresh burst; `stop()` no longer leaves a timer running; a worker running the same job twice (its lease expired and another of its loops claimed it again) tracks both runs, so `stop()` no longer waits forever for the second one; a failure reported after the lease was lost is logged as a warning, not an error.

## 0.1.0

First release.

- SQLite-backed queues with priorities, delayed jobs, retries with backoff and dead jobs
- Leases with heartbeats and a reaper for crashed workers
- In-process workers, worker-thread handlers, and an HTTP worker protocol
- Cron schedules with timezones
- Unique jobs, queue pause/resume, concurrency and rate limits
- Signed webhooks, scoped API tokens, Prometheus metrics, CLI
