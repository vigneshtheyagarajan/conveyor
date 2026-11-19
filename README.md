# Conveyor

A self-hosted background job queue and worker service for Node.js, written in
TypeScript and backed by SQLite.

- **Queues and priorities**: higher priority runs first, FIFO within a priority.
- **Delayed jobs** and **retries** with fixed, linear or exponential backoff (with jitter).
- **Dead jobs**: jobs that run out of attempts are kept for inspection and can be retried.
- **Leases and heartbeats**: crashed workers' jobs are picked up again automatically.
- **Recurring jobs**: cron expressions with IANA timezones and an overlap policy.
- **Unique jobs**: `uniqueKey` stops the same job being queued twice.
- **Queue controls**: pause and resume, global concurrency limits, token-bucket rate limits (per process, see below).
- **Webhooks**: job lifecycle events, signed with HMAC-SHA256, with delivery history.
- **HTTP API** with scoped API tokens, including a worker protocol so workers can be written in any language.
- **Prometheus metrics**, structured JSON logs, and a `conveyor` command-line tool.
- No runtime dependencies. It uses Node's built-in `node:sqlite`.

> Requires Node.js 22.13 or newer.

## Quick start

```bash
npm install
npm run build

# create the database
node dist/cli/index.js migrate

# start the API, scheduler, reaper and webhook dispatcher
node dist/cli/index.js serve

# in another terminal: start a worker with the example handlers
npx tsx src/cli/index.ts worker --handlers examples/handlers.ts --queues default,email --concurrency 4

# enqueue a job
curl -s localhost:7420/v1/jobs -H 'content-type: application/json' \
  -d '{"queue":"email","name":"send-email","payload":{"to":"a@example.com","subject":"hi"}}'
```

## Embedding

```ts
import { Broker, Conveyor, HandlerRegistry, Maintenance, SqliteStore, Worker } from "conveyor-queue";

const conveyor = new Conveyor({ store: new SqliteStore("./conveyor.db") });
await conveyor.init();

const handlers = new HandlerRegistry().register<{ to: string }>("send-email", async ({ job, progress }) => {
  await sendEmail(job.payload.to);
  await progress(1);
  return { sent: true };
});

const worker = new Worker(new Broker(conveyor), { queues: ["email"], handlers, concurrency: 4 });
new Maintenance(conveyor).start();
worker.start();

await conveyor.enqueue({ queue: "email", name: "send-email", payload: { to: "a@example.com" }, maxAttempts: 5 });
```

Handlers receive an `AbortSignal` that fires on timeout, when the job's lease is lost (it was
cancelled, or reaped), or when `stop()` runs out of patience; honour it. Throw
`UnrecoverableError` to fail a job without retrying it. See
[Handlers and their signal](docs/architecture.md#handlers-and-their-signal) for what happens to
a handler that ignores it.

CPU-heavy synchronous work must run in a worker thread, with
`threaded("./path/to/module.js", "exportName")`. A handler that blocks the event loop also
blocks the worker's heartbeats: once its lease expires, the job counts as failed and runs
again, possibly elsewhere while the first run is still going (at-least-once). A threaded
handler keeps the event loop free, and its thread is terminated when the signal aborts.

## Concepts

| Term | Meaning |
| --- | --- |
| Queue | A named stream of jobs. Created on first use; settings via `PUT /v1/queues/:name`. A queue's `concurrency` limit holds across all processes sharing the database. Its rate limit (`rateLimitMax` per `rateLimitIntervalMs`) is enforced **per process**: each `serve` and `worker` process has its own token bucket, so N processes can together claim up to N times the configured rate. |
| Job | `{queue, name, payload}` plus options. `name` selects the handler. |
| Lease | Time-limited claim on a job by one worker, extended by heartbeats. |
| Reaper | Ends leases that expired (crashed or stalled worker). This counts as a failed attempt: the job is retried after its backoff, or goes to dead once it has no attempts left. Safe to run in any number of processes. |
| Schedule | A cron expression that enqueues a job each time it fires. |

Delivery is **at-least-once**. Write idempotent handlers. See [docs/architecture.md](docs/architecture.md).

### Enqueue options

| Option | Default | Description |
| --- | --- | --- |
| `priority` | `0` | -1000..1000, higher first |
| `delayMs` / `runAt` | now | Earliest run time. `delayMs` is at most 100 years; `runAt` must be between 1970-01-01 and 9999-12-31T23:59:59.999Z |
| `maxAttempts` | queue default (3) | Including the first attempt |
| `backoff` | exponential 1s → 10m, jitter | `{type: "fixed"|"linear"|"exponential", ...}`; each delay setting is at most 100 years |
| `timeoutMs` | `0` (none) | Per-attempt limit. The handler's signal aborts; the failure is recorded, and the retry can start, only once the handler has stopped |
| `uniqueKey` | – | Return the existing job while one with this key is unfinished (in the same queue; enforced by the database, across processes). A dead or cancelled job can't be retried while a newer job holds its key |

## HTTP API

Authentication is off until the first API token exists. After that every
endpoint except `/health` and `/metrics` needs `Authorization: Bearer <secret>`.

```bash
node dist/cli/index.js token-create ops --scopes admin
```

Scopes: `jobs:read`, `jobs:write`, `workers`, `admin` (admin covers all scopes).

| Method | Path | Scope |
| --- | --- | --- |
| GET | `/health` | – |
| GET | `/metrics` | – |
| POST | `/v1/jobs` | jobs:write |
| POST | `/v1/jobs/bulk` | jobs:write |
| GET | `/v1/jobs?queue=&state=&name=&cursor=&limit=` | jobs:read |
| GET | `/v1/jobs/:id` | jobs:read |
| GET | `/v1/jobs/:id/events` | jobs:read |
| POST | `/v1/jobs/:id/cancel` | jobs:write |
| POST | `/v1/jobs/:id/retry` | jobs:write |
| GET | `/v1/queues` | jobs:read |
| GET | `/v1/queues/:name` | jobs:read |
| PUT | `/v1/queues/:name` | admin |
| POST | `/v1/queues/:name/pause` · `/resume` | admin |
| POST | `/v1/queues/:name/retry-dead` | jobs:write |
| POST | `/v1/leases` | workers |
| POST | `/v1/jobs/:id/heartbeat` · `/progress` · `/complete` · `/fail` · `/release` | workers |
| GET/POST | `/v1/schedules` | jobs:read / admin |
| GET/PATCH/DELETE | `/v1/schedules/:id` | jobs:read / admin |
| POST | `/v1/schedules/:id/trigger` | jobs:write |
| GET/POST | `/v1/webhooks` | admin |
| DELETE | `/v1/webhooks/:id` | admin |
| GET | `/v1/webhooks/:id/deliveries` | admin |
| GET/POST | `/v1/tokens` | admin |
| DELETE | `/v1/tokens/:id` | admin |

Errors use one shape: `{"error": {"code": "validation_error", "message": "..."}}`.

`examples/http-worker.mjs` is a complete worker that uses only the HTTP protocol.

### Webhook signatures

Webhooks can subscribe to `job.enqueued`, `job.completed`, `job.failed`,
`job.retrying`, `job.dead` and `job.cancelled`, or `*` for all of them.

Each delivery carries `conveyor-signature: t=<unix>,v1=<hex>`, where `v1` is
HMAC-SHA256 of `"<t>.<raw body>"` using the webhook secret. Verify it with
`verifyWebhook(secret, rawBody, header)` from this package.

## CLI

```
conveyor migrate | serve | worker
conveyor enqueue <queue> <name> [--payload json] [--delay ms] [--priority n] [--unique key]
conveyor jobs [--queue q] [--state s] | job <id> | cancel <id> | retry <id>
conveyor queues | pause <q> | resume <q> | queue-set <q> [...] | dlq <q> [--retry]
conveyor schedules | schedule-add <name> --queue q --job j --cron "expr" [--tz zone] | schedule-rm <id>
conveyor token-create <name> --scopes a,b | tokens | token-revoke <id>
```

Run `conveyor --help` for details.

## Configuration

Environment variables (see `.env.example`):

| Variable | Default |
| --- | --- |
| `CONVEYOR_DB` | `./conveyor.db` |
| `CONVEYOR_HOST` / `CONVEYOR_PORT` | `127.0.0.1` / `7420` |
| `CONVEYOR_LOG_LEVEL` | `info` |
| `CONVEYOR_LEASE_MS` | `30000` |
| `CONVEYOR_HEARTBEAT_MS` | `10000` |
| `CONVEYOR_REAPER_INTERVAL_MS` | `5000` |
| `CONVEYOR_SCHEDULER_INTERVAL_MS` | `1000` |
| `CONVEYOR_WEBHOOK_TIMEOUT_MS` | `5000` |
| `CONVEYOR_WEBHOOK_MAX_ATTEMPTS` | `5` |
| `CONVEYOR_RETENTION_MS` | 7 days. Completed and cancelled jobs older than this are deleted; dead jobs are kept. `0` keeps everything |
| `CONVEYOR_MAX_PAYLOAD_BYTES` | `262144` |

## Development

```bash
npm run typecheck
npm test
npm run dev -- --help
```

## License

[MIT](LICENSE) © 2026 Vignesh Theyagarajan
