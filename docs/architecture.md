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

           Maintenance: reaper, delayed-job promoter, janitor, reconcile sweep
```

## Components

| Path | Responsibility |
| --- | --- |
| `src/core/conveyor.ts` | Producer API: enqueue, cancel, retry, queue settings and stats |
| `src/core/events.ts` | Typed in-process event bus |
| `src/core/flow.ts` | Flow validation and ordering (`planFlow`), flow status |
| `src/core/dependencies.ts` | What a running job sees of its dependencies (`dependencyResult`, `isVisibleToDependent`, `DependencyError`) |
| `src/storage/` | `Store` interface, SQLite implementation, migrations |
| `src/broker/broker.ts` | Lease protocol used by every kind of worker |
| `src/broker/maintenance.ts` | Reaper (expired leases), promoter (delayed → waiting), janitor (deletes old completed and cancelled jobs; dead jobs are kept), reconcile sweep (blocked jobs whose dependencies settled) |
| `src/worker/` | In-process worker, handler registry, worker-thread runner |
| `src/scheduler/` | Cron parser and the scheduler that turns schedules into jobs |
| `src/ratelimit/` | Token bucket and per-queue limiter |
| `src/webhooks/` | Delivery of job events and flow verdicts to HTTP endpoints, signing |
| `src/api/` | Router, auth, HTTP server and route handlers |
| `src/metrics/` | Prometheus-format metrics |
| `src/cli/` | `conveyor` command |

## Job states

```
 enqueue ─┬─> blocked ──(every dependency settled)──┬─> waiting
          │      │                                  └─> delayed (run_at still ahead)
          │      └──(a `fail` dependency ended dead or cancelled)──> cancelled
          ├─> delayed ──(run_at reached)──┐
          └─> waiting <───────────────────┘<──── retry() / lease expired / release
                 │
               claim
                 ▼
               active ──complete──> completed
                 │
                fail ──attempts left──> delayed (backoff)
                 │
                 └──no attempts left / unrecoverable──> dead

 cancel(): blocked | delayed | waiting | active ──> cancelled
 retry():  dead | cancelled ──> waiting (blocked while a dependency is unfinished)
```

Every transition checks the state it expects (and, for a leased job, the lease token) and
writes the change, its history entry and its effect on dependents in one transaction. If two
race (a cancel and a completion, the reaper and a late completion), exactly one of them
changes the job; the other gets a conflict or `LeaseLostError`.

## Dependencies

A job can depend on other jobs (`dependsOn`, or as part of a flow). The edges are stored in
`job_dependencies` and never change once written. Whether a job can run is worked out from its
dependencies' current states each time, so there is no counter that a missed or repeated update
could leave wrong.

### The blocked state

A job with dependencies is inserted `blocked`. `blocked` is not terminal and not claimable
(`findClaimCandidate` only takes `waiting` jobs). The *child check* (`settleChild` in
`sqlite-store.ts`) decides what a blocked job does next:

1. If a dependency was deleted, or a `fail` dependency ended `dead` or `cancelled`, the job is
   cancelled without running. `terminatedBy` names that dependency, and the job's own
   dependents are checked in turn.
2. Otherwise, if any dependency is unfinished, it stays blocked.
3. Otherwise it is released: `waiting`, or `delayed` if it was given a later run time. Its
   `run_at` becomes the release time (or stays later), so time spent blocked doesn't count as
   queue lag or wait time.

So a job is blocked exactly when one of its dependencies is unfinished and none has failed in a
way its policy treats as fatal. A dependency that is retrying (in backoff) is unfinished, not
failed. Propagation only ever cancels: it never makes a dependent `dead`.

| `onFailure` | The dependent, when the dependency ends dead or cancelled | What it sees of it |
| --- | --- | --- |
| `fail` (default) | cancelled without running | – |
| `ignore` | released (the dependency counts as settled) | nothing |
| `continue` | released (the dependency counts as settled) | its state and error |

Enqueueing with a `fail` dependency that is already dead or cancelled is refused (409); with
`ignore` or `continue` the dependency counts as settled at once. A dependency that doesn't exist
is a validation error.

### Why unblocking can't be missed

The danger is a lost wakeup: a dependent left `blocked` with nothing left that will release it.
There are two ways to get one:

- two dependencies finish at the same moment in different processes, and each decides the
  other is still unfinished;
- a new dependent is inserted while its dependency completes: the insert reads the dependency
  as unfinished, and the completion looks for dependents before the new one exists.

Both are check-then-act races, and both are closed the same way: the check happens in the same
transaction as the change. Completing a job, marking it dead, cancelling it, cancelling a flow
and inserting jobs each run in one `BEGIN IMMEDIATE` transaction that writes the change, writes
its history entry and then runs the child check on the affected dependents against the current
states. SQLite has a single writer, and `BEGIN IMMEDIATE` takes the write lock before the first
read, so whichever transaction commits second sees the other's change. (A deferred `BEGIN`
that reads and then writes fails with `SQLITE_BUSY` instead when another connection committed
in between.) The transaction body is synchronous: the connection is shared by everything in the
process, and an `await` inside would let another caller's statements into it.

`test/integration/dependency-races.test.ts` runs these races for real: two threads with their
own connections to one file, released together by a spin barrier, hundreds of times. Planting
either race (checking before writing, or reading the dependency before the insert's
transaction) makes those tests fail within a few rounds.

Another `Store` backend needs the same guarantee. Under snapshot isolation, the first race is
write skew: it needs serializable transactions or row locks on the dependents.

As a safety net, `Maintenance` runs a reconcile sweep (every 5s by default) that runs the child
check on any blocked job whose dependencies no longer keep it blocked. It should find nothing,
and logs a warning if it does.

### Flows

`enqueueFlow` submits a graph of new jobs at once. `planFlow` checks the graph's structure and
sorts it topologically (Kahn's algorithm, keeping the given order where dependencies allow). It
rejects cycles and names their keys in run order. Jobs are created in that order, so their ids,
which sort by creation time, do too. `Store.insertFlow` then does the rest in one transaction:
it checks again that the order is topological (so the database never holds a cycle, whoever
calls it), checks the existing jobs the flow depends on, inserts the jobs, their edges and
history, and runs the child check on each job. Any failure rolls the whole flow back.

A flow's jobs carry `flow_id` and `flow_key` (unique together), and its status is derived from
their states. `cancelFlow` cancels every unfinished job of the flow directly (so none is cancelled
or released by another's cancellation), then settles their dependents outside the flow.

### Flow verdicts and webhooks

A flow gets one verdict: `flow.failed` as soon as one of its jobs is dead or cancelled,
`flow.completed` once every job has completed. The verdict has to be decided once, even when the
flow's last two jobs finish at the same moment in different processes. So the once-only check
lives where the lost-wakeup check does: in the transition's own transaction. After a change
that finishes a flow job, the same transaction looks at the flow's jobs and, if that decides a
verdict, inserts a row in `flow_events`. The flow id is the primary key, and the insert is
`ON CONFLICT DO NOTHING`, so a second verdict is impossible. Only the transaction whose insert
wrote a row reports the verdict. Of two transactions finishing the last jobs at once, the one
that commits second is the only one that sees the flow decided.

Delivery can't rely on the in-process event bus, as job webhooks do (KB-23): the deciding
transaction may run in a `conveyor worker` process with no dispatcher. So `flow_events` is also
an outbox. Every `WebhookDispatcher` polls it (every second, and immediately when a verdict is
recorded in its own process) and *claims* each verdict in one transaction that sets
`dispatched_at` only if it is still null and stores the webhook deliveries. Two dispatchers that
find the same verdict can't both claim it. From then on, delivery and retries are the normal
webhook delivery machinery, which survives restarts. The race tests cover recording and
delivering under concurrency for every combination of the last two jobs completing or dying.

### Reading dependency results

A running job sees its dependencies through one function, `dependencyResult`, and one rule,
`isVisibleToDependent`: an `ignore` dependency that ended dead or cancelled is left out, and
everything else is listed, with its result if it completed and its last error if it didn't.
In-process handlers get the list from `ctx.dependencies()` (loaded once per attempt) and single
results from `ctx.dependency(key)`, which throws a `DependencyError` for a dependency that didn't
complete. HTTP workers get summaries in the lease and pages of results from
`GET /v1/jobs/:id/dependencies`. Paging is done in SQL (`Store.listDependencyJobs(..., {
visibleOnly })`), and a store contract test checks that the SQL filter matches
`isVisibleToDependent` for every policy and state.

### Retries

`retry()` of a dead or cancelled job brings back, as `blocked`, the dependents its failure
cancelled (those whose `terminatedBy` is it), transitively. Each is then checked again, so one
with another failed `fail` dependency is cancelled again. Retrying a dependent directly while its
`fail` dependency is still dead or cancelled is refused.

A job is never retried without a dependency it was created with. Edges outlive their
dependency: `parent_id` has no foreign key, and an edge is deleted only with its dependent. So a
retry can see that a dependency is gone, and refuses with a 409 naming it. The alternatives were
recording the dependency ids on the job, which would be a second copy of the edges, or having
retention keep the dependencies of dead and cancelled jobs. Dead jobs are kept forever, so that
would pin whole job rows (payloads and results) indefinitely, where a leftover edge is a few
dozen bytes. The child check treats a missing dependency as fatal for the same reason.

### Retention

The janitor deletes completed and cancelled jobs older than the retention period, except those
an unfinished job depends on: that job may still read their results. Once every dependent has
finished, normal retention applies, measured from when the job itself finished, so a job that was
kept past its time goes at the next cleanup. A flow verdict is deleted once its flow has no jobs
left (and it is older than the retention period). Deleting it earlier would let a later
transition record a second verdict.

### Known limitations

- There is no "retry the whole flow". Retrying a failed job brings back what its failure
  cancelled. Jobs cancelled by `cancelFlow` were cancelled directly and are retried one by one.
- A failed `ignore` or `continue` dependency that is retried after its dependent was released
  is shown to the dependent in its current state (e.g. `waiting`), not the one it was released
  on.
- A flow's verdict is final. After `flow.failed`, retrying jobs until the flow completes doesn't
  send `flow.completed`.
- Job webhooks and all metrics only see changes made in their own process (KB-23). Flow
  webhooks don't have this limitation: they are delivered from the database.
- A dependency whose lease keeps expiring is requeued forever (KB-1), so its dependents stay
  blocked until KB-1 is fixed. The fix must go through `markDead`, which settles dependents.
- Claiming isn't atomic (KB-11), and a fan-out makes that visible: when one completion releases
  many jobs at once, a worker with `concurrency` above 1 can run each of them several times,
  and worker processes can reopen a job another one completed. Dependents are still released
  exactly once, after every dependency first completed, but handlers must be idempotent, as
  at-least-once delivery always required.
- Results have no size limit yet (`maxResultBytes` is a follow-up). A job with many large
  dependency results loads them all in `ctx.dependencies()`; over HTTP they are paged.

## Leases

A worker that claims a job receives a lease token and an expiry
(`leaseMs`, 30s by default). While the handler runs, the worker sends a
heartbeat every `heartbeatMs` to extend the lease. Every call that changes the
job (heartbeat, progress, complete, fail, release) must present the token.

If a worker crashes, its lease expires and the reaper moves the job back to
`waiting` so another worker can take it. Delivery is therefore
**at-least-once**: handlers should be idempotent.

## Scheduling

Each schedule stores its `next_run_at`. The scheduler polls for due schedules,
enqueues a job (tagged with `schedule_id`) and advances `next_run_at` using the
cron expression and the schedule's IANA timezone.

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
