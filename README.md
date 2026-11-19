# Conveyor

A self-hosted background job queue and worker service for Node.js, written in
TypeScript and backed by SQLite.

- **Queues and priorities**: higher priority runs first, FIFO within a priority.
- **Delayed jobs** and **retries** with fixed, linear or exponential backoff (with jitter).
- **Dead jobs**: jobs that run out of attempts are kept for inspection and can be retried.
- **Leases and heartbeats**: crashed workers' jobs are picked up again automatically.
- **Recurring jobs**: cron expressions with IANA timezones and an overlap policy.
- **Unique jobs**: `uniqueKey` stops the same job being queued twice.
- **Job dependencies and flows**: a job can wait for other jobs, and a whole graph of jobs can be submitted at once; failures and cancellations propagate by policy.
- **Queue controls**: pause and resume, global concurrency limits, token-bucket rate limits.
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

Handlers receive an `AbortSignal` that fires on timeout or a forced shutdown.
Throw `UnrecoverableError` to fail a job without retrying it. CPU-heavy work can
run in a worker thread with `threaded("./path/to/module.js", "exportName")`.

Jobs can depend on other jobs; see [Dependencies and flows](#dependencies-and-flows).

## Concepts

| Term | Meaning |
| --- | --- |
| Queue | A named stream of jobs. Created on first use; settings via `PUT /v1/queues/:name`. |
| Job | `{queue, name, payload}` plus options. `name` selects the handler. |
| Lease | Time-limited claim on a job by one worker, extended by heartbeats. |
| Dependency | A job that must finish before another can run. The waiting job is `blocked`. |
| Reaper | Requeues jobs whose lease expired (crashed or stalled worker). |
| Schedule | A cron expression that enqueues a job each time it fires. |

Delivery is **at-least-once**. Write idempotent handlers. See [docs/architecture.md](docs/architecture.md).

### Enqueue options

| Option | Default | Description |
| --- | --- | --- |
| `priority` | `0` | -1000..1000, higher first |
| `delayMs` / `runAt` | now | Earliest run time. `delayMs` is at most 100 years; `runAt` must be between 1970-01-01 and 9999-12-31T23:59:59.999Z |
| `maxAttempts` | queue default (3) | Including the first attempt |
| `backoff` | exponential 1s → 10m, jitter | `{type: "fixed"|"linear"|"exponential", ...}`; each delay setting is at most 100 years |
| `timeoutMs` | `0` (none) | Per-attempt limit; the handler's signal aborts |
| `uniqueKey` | – | Return the existing job while one with this key is unfinished |
| `dependsOn` | – | Job ids (or `{jobId, onFailure}`) that must finish first; at most 1000. The job is `blocked` until they have |

## Dependencies and flows

A job can wait for other jobs: give it `dependsOn`, or submit a whole graph of jobs at once as a
**flow**. A job with dependencies starts `blocked`. It can't be claimed until every dependency
has finished, and then moves to `waiting` (or `delayed`, if it was given a later run time).

### Failure policies

Each dependency has an `onFailure` policy that decides what happens to the dependent when that
dependency ends `dead` or `cancelled` instead of `completed`:

| `onFailure` | The dependent | What it sees of the failed dependency |
| --- | --- | --- |
| `fail` (default) | is cancelled without running (`terminatedBy` names the dependency), and so are its own `fail` dependents | – |
| `ignore` | runs | nothing: left out of `ctx.dependencies()` |
| `continue` | runs | listed in `ctx.dependencies()` with its state (`dead` or `cancelled`) and error |

A dependency that is only retrying (in backoff after a failed attempt) hasn't failed: the
dependent stays blocked. Cancelling a job applies its dependents' policies the same way.

### Example

`examples/flow.json` fetches an image, makes a thumbnail and a preview from it, and publishes
both. Publishing needs the thumbnail; the preview is optional:

```json
{
  "jobs": [
    { "key": "fetch", "queue": "default", "name": "fetch-image", "payload": { "url": "https://example.com/cat.jpg" } },
    { "key": "thumb", "queue": "default", "name": "resize", "payload": { "width": 128 }, "dependsOn": ["fetch"] },
    { "key": "preview", "queue": "default", "name": "resize", "payload": { "width": 1024 }, "dependsOn": ["fetch"] },
    { "key": "publish", "queue": "default", "name": "publish", "dependsOn": ["thumb", { "key": "preview", "onFailure": "continue" }] }
  ]
}
```

```bash
curl -s localhost:7420/v1/flows -H 'content-type: application/json' -d @examples/flow.json
conveyor flow-submit examples/flow.json      # the same from the CLI
```

```ts
const flow = await conveyor.enqueueFlow(JSON.parse(readFileSync("examples/flow.json", "utf8")));

handlers.register("publish", async ({ dependency, dependencies }) => {
  const thumb = await dependency<{ url: string }>("thumb"); // its result; throws if it didn't complete
  const preview = (await dependencies()).find((d) => d.key === "preview");
  // With "continue", a failed preview is listed with state "dead" (or "cancelled") and its error.
  return { thumb: thumb.url, preview: preview?.state === "completed" ? preview.result : null };
});
```

A single job can depend on existing jobs by id:
`enqueue({ queue, name, dependsOn: [id, { jobId, onFailure: "ignore" }] })`
(`POST /v1/jobs` and `/v1/jobs/bulk` take the same `dependsOn`; the CLI has
`enqueue --depends-on id,id --on-failure ignore`). With a `uniqueKey` that matches an unfinished job,
enqueue returns that job as it is, whatever `dependsOn` says, just as it ignores a different
payload.

### Flows

Jobs in a flow name each other by `key` and can also depend on existing jobs with
`{jobId, onFailure}`; the flow's `onFailure` is the default for dependencies that don't name
one. Each job takes the usual enqueue options except `uniqueKey`. A flow is stored all at once
or not at all. It is rejected with 400, with nothing stored, for a cycle (the error names its
keys, e.g. `A -> B -> C -> A`), an unknown key, a job that depends on itself, a repeated key or
dependency, more than 1000 jobs, a `uniqueKey`, or an existing job that doesn't exist. A `fail`
dependency on an existing job that is already dead or cancelled is a 409; with `ignore` or
`continue` it counts as settled straight away.

`GET /v1/flows/:id` (or `conveyor flow <id>`) returns the jobs in dependency order, the edges,
counts by state, a `status` (`running`, then `completed`, `failed` if any job is dead, or
`cancelled`) and the flow's `outcome` once it has one (see below). `POST /v1/flows/:id/cancel`
(`conveyor flow-cancel <id>`) cancels every unfinished job of the flow, running ones included.

### Reading results

In-process handlers use `ctx.dependency(key)` for one result (by its key in the flow, or the job
id of a dependency outside it) and `ctx.dependencies()` for the list, with each dependency's
`id`, `key`, `name`, `queue`, `state`, `onFailure`, `result` and `error`. Both load on first use
and are cached for the attempt. `dependency(key)` throws a `DependencyError` (an
`UnrecoverableError`, so it fails the job without retries if not caught) for a dependency that
didn't complete, whatever its policy, or a key that isn't one. A threaded handler gets the list
as a second argument with `threaded(path, exportName, { dependencies: true })`.

HTTP workers find the dependencies in the lease (`POST /v1/leases` returns `dependencies` as
`{id, key, name, state}`, without results) and read the results from
`GET /v1/jobs/:id/dependencies`, in pages ordered by id (`?limit=` up to 500, `?cursor=` from
`nextCursor`; the token needs `jobs:read`). Both apply the same `ignore`/`continue` rule as
`ctx.dependencies()`. The two views differ only in that `ctx.dependency(key)` also reaches a
failed `ignore` dependency (to throw for it), where over HTTP `?all=true` lists those too; and
over HTTP the list is paged. `examples/http-worker.mjs` shows the HTTP side.

### Flow webhooks

Webhooks can subscribe to a flow's verdict:

- `flow.failed` as soon as a job in the flow is dead or cancelled (directly, by its dependency,
  or by cancelling the flow); `reason` is `dead` or `cancelled` and `jobId` names the job.
- `flow.completed` once every job in the flow has completed.

A flow gets one verdict, whichever comes first, and it is delivered once, even when its last
jobs finish at the same moment in different processes: the verdict is recorded in the database
by the transition that decides it, and `serve` delivers it from there. So flow webhooks are
sent whichever process ran the jobs, unlike job webhooks (KB-23). Retrying jobs after
`flow.failed` doesn't send a later `flow.completed`. A webhook limited to some queues gets a
flow's events if any of the flow's jobs is in one of them. The body's `data` has the flow's
`id`, `reason`, `jobId`, `at`, `status`, `counts` and `jobs` (`id`, `key`, `queue`, `name`,
`state`).

### Retries and retention

Retrying a failed dependency brings back, blocked, the dependents its failure cancelled. A job
is never retried without a dependency it was created with: if retention has deleted one, the
retry is refused with 409 naming the missing job (and `retry-dead` reports it as skipped).

Retention (`CONVEYOR_RETENTION_MS`) never deletes a finished job while a job that depends on it
is unfinished; once all its dependents have finished, normal retention applies.

### Metrics

`conveyor_queue_jobs{state="blocked"}` counts blocked jobs per queue,
`conveyor_job_blocked_seconds` is how long jobs were blocked before being released,
`conveyor_jobs_cancelled_by_dependency_total` counts jobs cancelled because a dependency failed,
and `conveyor_flows_finished_total{event}` counts flow verdicts. (Like the other event metrics,
the last three count what happens in the process serving `/metrics`.)

Known limitations are listed in [docs/architecture.md](docs/architecture.md#known-limitations).

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
| GET | `/v1/jobs/:id/dependencies` · `/dependents?cursor=&limit=` | jobs:read |
| POST | `/v1/flows` | jobs:write |
| GET | `/v1/flows/:id` | jobs:read |
| POST | `/v1/flows/:id/cancel` | jobs:write |
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
`job.retrying`, `job.dead`, `job.cancelled`, `flow.completed` and `flow.failed`, or `*` for
all of them.

Each delivery carries `conveyor-signature: t=<unix>,v1=<hex>`, where `v1` is
HMAC-SHA256 of `"<t>.<raw body>"` using the webhook secret. Verify it with
`verifyWebhook(secret, rawBody, header)` from this package.

## CLI

```
conveyor migrate | serve | worker
conveyor enqueue <queue> <name> [--payload json] [--delay ms] [--priority n] [--unique key]
                 [--depends-on id,id] [--on-failure fail|ignore|continue]
conveyor flow-submit <file.json | -> | flow <id> | flow-cancel <id>
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
| `CONVEYOR_RETENTION_MS` | 7 days. Completed and cancelled jobs older than this are deleted, unless a job that depends on them is unfinished; dead jobs are kept. `0` keeps everything |
| `CONVEYOR_MAX_PAYLOAD_BYTES` | `262144` |

## Development

```bash
npm run typecheck
npm test
npm run dev -- --help
```

## License

[MIT](LICENSE) © 2026 Vignesh Theyagarajan
