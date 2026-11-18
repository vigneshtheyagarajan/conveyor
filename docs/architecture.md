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
| `src/ratelimit/` | Token bucket and per-queue limiter |
| `src/webhooks/` | Delivery of job events to HTTP endpoints, signing |
| `src/api/` | Router, auth, HTTP server and route handlers |
| `src/metrics/` | Prometheus-format metrics |
| `src/cli/` | `conveyor` command |

## Job states

```
 enqueue ─┬─> delayed ──(run_at reached)──┐
          └─> waiting <───────────────────┘<──── retry() / lease expired
                 │
               claim
                 ▼
               active ──complete──> completed
                 │
                fail ──attempts left──> delayed (backoff)
                 │
                 └──no attempts left / unrecoverable──> dead

 cancel(): delayed | waiting | active ──> cancelled
```

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
