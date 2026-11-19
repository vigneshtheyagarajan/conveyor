# Architecture

```
            producers (app code, HTTP API, CLI, scheduler)
                              |
                         Conveyor  ── EventBus ──> webhooks, metrics
                              |
                            Store (SQLite)
                              |
                           Broker  (claim / heartbeat / complete / fail)
                         /         \
              in-process Worker    HTTP worker endpoints (/v1/leases ...)

           Maintenance: reaper, delayed-job promoter, janitor
```

## Components

| Path | Responsibility |
| --- | --- |
| `src/core/conveyor.ts` | Producer API: enqueue, cancel, retry, queue settings and stats |
| `src/core/events.ts` | Typed in-process event bus |
| `src/storage/` | `Store` interface, SQLite implementation, migrations |
| `src/broker/broker.ts` | Lease protocol used by every kind of worker |
| `src/broker/maintenance.ts` | Reaper (expired leases), promoter (delayed → waiting), janitor (deletes old completed and cancelled jobs; dead jobs are kept) |
| `src/worker/` | In-process worker, handler registry, worker-thread runner |
| `src/scheduler/` | Cron parser and the scheduler that turns schedules into jobs |
| `src/ratelimit/` | Token bucket and per-queue limiter (in memory, per process) |
| `src/webhooks/` | Delivery of job events to HTTP endpoints, signing |
| `src/api/` | Router, auth, HTTP server and route handlers |
| `src/metrics/` | Prometheus-format metrics |
| `src/cli/` | `conveyor` command |

## Job states

```
 enqueue ─┬─> delayed ──(run_at reached)──┐
          └─> waiting <───────────────────┘<──── retry(): dead | cancelled
                 │
               claim
                 ▼
               active ──complete──> completed
                 │
     fail or lease expired ──attempts left──> delayed (backoff)
                 │
                 └──no attempts left / unrecoverable──> dead

 cancel(): delayed | waiting | active ──> cancelled
```

Every transition is applied by a store update whose `WHERE` clause repeats the state (and,
for lease calls, the lease token) it starts from. A transition whose starting point changed
in the meantime (another worker, a reaper, a cancel, a retry, possibly in another process)
changes nothing and is reported: `LeaseLostError` for lease calls, `ConflictError` for cancel
and retry.

## Claims

`Store.claimNext()` picks and leases a job in one step. In `SqliteStore` it runs
in a `BEGIN IMMEDIATE` transaction, which holds SQLite's write lock, so claims
from every connection and process sharing the file take turns. Inside the
transaction it skips paused queues and queues at their `concurrency` limit,
takes the best waiting, due job (priority, then `run_at`, then id), marks it
active and records the `claimed` event. A job is therefore leased to one worker
at a time, and a queue's concurrency limit holds across processes.

Before taking the lock, `claimNext` runs the same selection without a
transaction and returns at once if nothing is claimable, so idle workers don't
queue up for the lock. That first look is only a hint; the decision is made
again inside the transaction.

Rate limits are enforced per process, before the claim: `Broker.claim()`
takes a token from each requested queue's in-memory bucket, leaves out queues
that have none (so they don't block the other queues), and gives back the
tokens of queues that did not supply the job. With several processes, each
has its own buckets.

## Leases

A worker that claims a job receives a lease token and an expiry
(`leaseMs`, 30s by default). While the handler runs, the worker sends a
heartbeat every `heartbeatMs` to extend the lease. Every call that changes the
job (heartbeat, progress, complete, fail, release) must present the token, and
the store's update only applies while the job is active and still holds that
token. If the lease was lost in the meantime (reaped, cancelled, claimed by
another worker), nothing changes and the call fails with `LeaseLostError`
(HTTP 409).

If a worker crashes, its lease expires and the reaper ends it. An expired lease
counts as a failed attempt: the job is retried after its normal backoff (so a
job that keeps crashing its worker doesn't hammer the next one), and once it has
used all its attempts it goes to dead with an error like "lease expired 3 times;
worker may be crashing". Like any failure this emits `job.failed`, then
`job.retrying` or `job.dead`, as well as `job.reaped`. Delivery is therefore
**at-least-once**: handlers should be idempotent.

Every `conveyor worker` process runs a reaper, and so does `serve`. A reaper only
ends a lease that is still exactly as it read it (same token, same expiry), so a
heartbeat or completion that lands after the read wins, and a lease another
reaper already ended is left alone.

## Handlers and their signal

A worker runs a handler with an `AbortSignal`. JavaScript can't stop a running function from
outside, so what happens when the signal fires depends on the handler honouring it:

| Signal fires because | Reason | What the worker does | If the handler ignores the signal |
| --- | --- | --- | --- |
| The job ran longer than `timeoutMs` | `JobTimeoutError` | Keeps the job's lease (heartbeats go on) and waits for the handler to stop, then records the failure: retry after backoff, or dead. | The job stays active and holds its worker slot until the handler returns, however long that takes. Its result is discarded. The retry never overlaps it. |
| The lease was lost: the job was cancelled, or reaped | `LeaseLostError` | Stops waiting at once; the lease is already gone (a reaped job may already run elsewhere). The result is discarded. | It keeps running alongside whatever happens to the job next, and the loop takes new work. |
| `stop()` reached `shutdownTimeoutMs` | `WorkerShutdownError` | Releases the job (no attempt used). A job that had already timed out is failed instead. | It keeps running after `stop()` resolves, while its job is back in the queue. Exit the process after `stop()`; `conveyor worker` does. |

`stop()` hands back everything within `shutdownTimeoutMs`: jobs claimed while stopping are
released unrun, running jobs finish or are released, so no job waits out a lease expiry after a
clean shutdown. Every call to `stop()` returns the same promise.

### CPU-heavy handlers

A handler that runs synchronous CPU-heavy code blocks the event loop, and with it the worker's
heartbeats. Once the lease expires, a reaper (in any process) counts the attempt as failed and
the job is retried, possibly in another process while the first run is still going. After
`maxAttempts` such expiries the job is dead ("lease expired N times; worker may be crashing").
This is the at-least-once guarantee at work, not something the worker can prevent: it can't
heartbeat while it is blocked.

Run such work in a worker thread with `threaded(modulePath, exportName)`. The event loop stays
free, heartbeats keep the lease, and the thread is terminated when the signal aborts (timeout,
lost lease or shutdown). Raising `leaseMs` only helps if you know an upper bound for the
blocking time.

## Scheduling

Each schedule stores its `next_run_at`. The scheduler polls for due schedules,
enqueues a job (tagged with `schedule_id`) and advances `next_run_at` using the
cron expression and the schedule's IANA timezone.

Firing a slot is one atomic store step (`fireSchedule`). It checks that the schedule is still
enabled and still on that slot (a compare-and-set on `next_run_at`), applies the overlap
policy, inserts the job and moves to the next slot. So each slot fires once even with several
`serve` processes. A tick writes only `next_run_at`, `last_run_at`, `last_job_id` and
`updated_at`, and an edit writes only the fields it changes, so neither undoes the other.

### Cron expressions

Five fields: `minute hour day-of-month month day-of-week`, with lists (`1,15`),
ranges (`1-5`), steps (`*/15`, `10-50/20`), month and weekday names (`JAN`,
`MON-FRI`; `7` is also Sunday) and the macros `@yearly`/`@annually`, `@monthly`,
`@weekly`, `@daily`/`@midnight` and `@hourly`.

Day matching follows Vixie cron. A day field that starts with `*` is
unrestricted, even with a step such as `*/2`. If both day fields are
restricted, a day matching either is enough (`0 0 13 * FRI` runs on the 13th
and on every Friday). Otherwise a day must match both (`0 0 */2 * MON` runs
on odd-numbered days that are Mondays).

### Daylight-saving time

Times are wall-clock times in the schedule's timezone (`UTC` by default). When
the clocks change, what happens depends on the kind of expression:

| Expression | Example | Clocks go forward (a time is skipped) | Clocks go back (a time happens twice) |
| --- | --- | --- | --- |
| **Interval**: the minute or hour field starts with `*` | `*/15 * * * *`, `0 * * * *` | Skipped times do not run | The repeated times run again |
| **Fixed-time**: neither field starts with `*` | `30 2 * * *`, `0 9 * * MON-FRI` | Runs once, at the moment the clocks jump | Runs once, at the first occurrence |

Interval jobs therefore keep their real-time rhythm (an hourly job still runs
every 60 minutes across the change), and fixed-time jobs run exactly once on
the day of the change. For example, in `America/New_York` on 8 March 2026 the
clocks jump from 02:00 to 03:00: `30 2 * * *` runs at 03:00, and `0 * * * *`
runs at 01:00 and then 03:00. On 1 November 2026 01:00-01:59 happens twice:
`30 1 * * *` runs only at the first 01:30, while `0 * * * *` runs at both
01:00s.

## Storage

`Store` is async so other backends can implement it. The bundled
`SqliteStore` uses `node:sqlite` in WAL mode, so several processes (for example
one `serve` and several `worker` processes) can share one database file.

Every process migrates on start. Each migration runs in a `BEGIN IMMEDIATE`
transaction that re-reads `user_version`, so processes starting together on a
new file all succeed.

Unique keys are enforced by the database: a partial unique index allows one
unfinished job per `(queue, unique_key)`. `insertJobs` checks keys in the same
transaction as the insert and returns the existing job for a key that is taken.

## Concurrency and delivery guarantees

Conveyor runs as several processes on one SQLite file: `serve`, any number of `conveyor worker`
processes, and HTTP workers talking to `serve`. Each process also runs many async tasks on
a single connection. All of them may act on the same job at the same moment.

One rule keeps that safe: **the decision and the write are one atomic step.** Either the
operation is a store method that runs synchronously inside `BEGIN IMMEDIATE` (see
`SqliteStore.immediate`), or it is a single `UPDATE` whose `WHERE` clause repeats the
precondition, with the changed-row count checked. Two details make this work:

- The transaction must not span an `await`, because every async task in the process shares
  the connection.
- `IMMEDIATE` takes the write lock first, so reads inside the transaction are current. A
  deferred transaction that reads and then writes fails with `SQLITE_BUSY_SNAPSHOT` when
  another process commits in between, and `busy_timeout` doesn't retry that.

Nothing reads state in one call and then writes by id in another.

### What is guaranteed

| Guarantee | How it is enforced | Tests |
| --- | --- | --- |
| A job is leased to one worker at a time, however many loops, HTTP workers and processes claim | `claimNext` picks and activates the job in one `BEGIN IMMEDIATE` transaction; the `UPDATE` also requires `state = 'waiting'` | `claim-race.test.ts` (processes), store contract `claimNext`, `worker.test.ts` "runs each job once when its loops claim at the same moment" |
| A queue's concurrency limit holds across processes, and paused queues are skipped | The limit and the pause are checked inside that transaction; the lock-free pre-check is only a hint | `claim-race.test.ts`, `sqlite-store.test.ts` "claimNext against a claim committed by another process", `broker.test.ts` |
| Only the current lease holder can change a running job (heartbeat, progress, complete, fail, release) | Each `UPDATE` requires `state = 'active' AND lease_token = ?`; no row changed means `LeaseLostError` | `broker-stale-worker.test.ts`, store contract "without the lease" |
| An expired lease is ended at most once, and only if it is unchanged since the reaper read it: a heartbeat or completion in between wins, and any number of reapers can run | `expireLease` requires the same token and `lease_expires_at`; only a reaper whose update applied logs and emits | `lifecycle-races.test.ts` "a reaper whose expired lease changed", `claim-race.test.ts` "reapers in several processes" |
| An expired lease is a failed attempt: retried after backoff, dead after `maxAttempts` | The reaper decides from the attempts it read; the decision is valid because the lease (and so the attempt count) is unchanged when its update applies | `maintenance.test.ts` |
| Cancel never overwrites a completed or dead job, and a cancelled job is never claimed | `cancelJob`'s `UPDATE` requires `delayed`, `waiting` or `active`; `claimNext` requires `waiting` | `lifecycle-races.test.ts` "cancel", store contract |
| Of concurrent retries of one job, exactly one requeues it; the others get a 409 | `retryJob`'s `UPDATE` requires `dead` or `cancelled` | `lifecycle-races.test.ts` "retry" |
| At most one unfinished job per queue and unique key, across processes; a duplicate enqueue gets the existing job back, and retry can't revive a job whose key a newer job holds | Partial unique index (migration 8); `insertJobs` and `retryJob` check the key in the same transaction as their write | `cross-process.test.ts`, `lifecycle-races.test.ts` "unique keys", store contract, `migrations.test.ts` |
| A schedule slot fires once, however many schedulers run; overlap "skip" and the enqueue see the same state; ticks and edits don't undo each other | `fireSchedule` does the slot compare-and-set on `next_run_at`, the overlap check, the insert and the move to the next slot in one transaction; ticks and edits write only the columns they change | `cross-process.test.ts`, `lifecycle-races.test.ts` "scheduler", store contract `fireSchedule` |
| Processes that open and migrate a new file at the same moment all succeed | Each migration runs in `BEGIN IMMEDIATE` after re-reading `user_version`; switching a new file to WAL is retried while it is locked | `cross-process.test.ts` "migrations across processes" |
| The retry of a timed-out job never overlaps the timed-out handler | The worker keeps the lease (heartbeating) until the handler has stopped, and only then records the failure | `worker.test.ts` "timeouts" |
| A clean shutdown hands every job back within `shutdownTimeoutMs` | Unstarted claims are released; running jobs finish or are released (failed, if already timed out); every `stop()` call returns the same promise | `worker.test.ts` "shutdown" |

`npm run stress` checks these end to end, under SIGKILLs of worker processes: one lease holder
at a time, lease calls only by the holder (no job completes twice), reaping, expired leases as
failed attempts, unique keys and the timeout rule (see `stress/README.md`). It does not use
queue concurrency limits, cancel or retry; the tests in the table cover those. `npm run bench`
measures claim throughput (see `bench/README.md`).

### What is only at-least-once

A job's handler can run more than once, and a handler must be idempotent. This happens when:

- **The worker process dies mid-run.** Its lease expires, the reaper records a failed attempt
  and the job runs again; the first run may have done part of its work.
- **A run finishes but its completion isn't recorded.** For example, the process dies between
  the handler returning and `complete()`.
- **A run fails or times out.** It is retried. For timeouts the retry starts only after the
  handler has stopped, so the runs don't overlap.
- **The handler blocks the event loop longer than `leaseMs`** (CPU-heavy synchronous code).
  Heartbeats stop, the lease expires, and the retry can run at the same time as the blocked
  run, in another process. Use `threaded()` (see "CPU-heavy handlers").
- **The lease is lost while the handler runs** (the job was cancelled, or reaped after blocked
  heartbeats) **and the handler ignores its signal.** It keeps running next to whatever happens
  to the job next.
- **`stop()` gives up on a handler that ignores its signal, and the process doesn't exit.** The
  handler keeps running while its job is back in the queue.

The result of a run whose lease was lost, or that timed out, is discarded even if the run
finishes.

### Not enforced across processes

- **Rate limits are per process.** Each process has its own in-memory token buckets, so N
  processes can together claim up to N times `rateLimitMax` per interval. A limited queue does
  not block the other queues in the same claim.
- **Wake-ups are per process.** `job.enqueued` and `queue.resumed` are in-process events. A
  worker notices jobs enqueued, or queues resumed, through another process (for example
  `conveyor enqueue`, or the API of a separate `serve`) only by polling, within `pollMs` to
  `maxPollMs` (5s by default).
- **Events are per process** (webhooks, metrics). Jobs finished by a `conveyor worker` process
  produce no webhooks (KB-23).
- **Lost leases are signalled, not enforced.** Conveyor can't stop a handler that ignores
  `LeaseLostError`; it only makes sure such a handler can no longer change the job.
- **One host.** Lease expiry compares times taken by different processes, and SQLite's WAL
  mode needs every process on the same machine (not a network filesystem), so all processes
  share one clock.
