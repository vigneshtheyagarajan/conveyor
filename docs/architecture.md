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
| `src/broker/maintenance.ts` | Reaper (expired leases), promoter (delayed → waiting), janitor (retention) |
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

## Storage

`Store` is async so other backends can implement it. The bundled
`SqliteStore` uses `node:sqlite` in WAL mode, so several processes (for example
one `serve` and several `worker` processes) can share one database file.
