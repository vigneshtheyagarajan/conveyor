#!/usr/bin/env node
import { readFileSync, realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { Broker } from "../broker/broker.js";
import { Maintenance } from "../broker/maintenance.js";
import { loadConfig } from "../core/config.js";
import { Conveyor } from "../core/conveyor.js";
import { createLogger } from "../core/logger.js";
import { JOB_STATES, type FailurePolicy, type Flow, type FlowRequest, type JobState, type Scope } from "../core/types.js";
import { createRuntime } from "../runtime.js";
import { Scheduler } from "../scheduler/scheduler.js";
import { SqliteStore } from "../storage/sqlite-store.js";
import { createToken } from "../api/auth.js";
import { NotFoundError } from "../core/errors.js";
import { HandlerRegistry } from "../worker/handlers.js";
import { Worker } from "../worker/worker.js";
import { print, table } from "./output.js";

const HELP = `conveyor - background job queue

Usage: conveyor <command> [options]

Server
  migrate                               create or upgrade the database
  serve                                 run API, scheduler, reaper and webhooks
  worker --handlers <file> --queues a,b [--concurrency n]

Jobs
  enqueue <queue> <name> [--payload json] [--delay ms] [--priority n] [--unique key]
          [--depends-on id,id] [--on-failure fail|ignore|continue]
  jobs [--queue q] [--state s] [--limit n]
  job <id>                              show a job and its history
  cancel <id>
  retry <id>

Flows
  flow-submit <file.json>               submit a flow ({"jobs": [...]}); - reads stdin
  flow <id>                             show a flow's status and jobs
  flow-cancel <id>                      cancel a flow's unfinished jobs

Queues
  queues                                list queues with counts
  pause <queue> | resume <queue>
  queue-set <queue> [--concurrency n] [--rate-max n] [--rate-interval ms] [--max-attempts n]
  dlq <queue> [--retry]                 list (or retry) dead jobs

Schedules
  schedules
  schedule-add <name> --queue q --job name --cron "expr" [--tz zone] [--payload json] [--skip-overlap]
  schedule-rm <id>

Tokens
  token-create <name> --scopes jobs:read,jobs:write,workers,admin
  tokens
  token-revoke <id>

Global options
  --db <path>     database file (default $CONVEYOR_DB or ./conveyor.db)
  --json          machine-readable output
`;

/** Run the CLI. Returns the process exit code; errors are reported on stderr. */
export async function main(argv: string[]): Promise<number> {
  try {
    return await run(argv);
  } catch (err) {
    process.stderr.write(`error: ${err instanceof Error ? err.message : String(err)}\n`);
    if (err instanceof UsageError) process.stderr.write("run `conveyor --help` for usage\n");
    return 1;
  }
}

async function run(argv: string[]): Promise<number> {
  const { values, positionals } = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      db: { type: "string" },
      json: { type: "boolean", default: false },
      help: { type: "boolean", short: "h", default: false },
      payload: { type: "string" },
      delay: { type: "string" },
      priority: { type: "string" },
      unique: { type: "string" },
      "depends-on": { type: "string" },
      "on-failure": { type: "string" },
      queue: { type: "string" },
      queues: { type: "string" },
      state: { type: "string" },
      limit: { type: "string" },
      handlers: { type: "string" },
      concurrency: { type: "string" },
      "rate-max": { type: "string" },
      "rate-interval": { type: "string" },
      "max-attempts": { type: "string" },
      retry: { type: "boolean", default: false },
      job: { type: "string" },
      cron: { type: "string" },
      tz: { type: "string" },
      "skip-overlap": { type: "boolean", default: false },
      scopes: { type: "string" },
      port: { type: "string" },
      host: { type: "string" },
    },
  });

  const [command, ...args] = positionals;
  if (values.help) {
    process.stdout.write(HELP);
    return 0;
  }
  if (!command) {
    process.stdout.write(HELP);
    return 1;
  }

  const json = values.json!;
  const port = int(values.port, "--port", { min: 0, max: 65535 });
  const overrides = {
    ...(values.db ? { dbPath: values.db } : {}),
    ...(port !== undefined ? { port } : {}),
    ...(values.host ? { host: values.host } : {}),
  };

  if (command === "serve") {
    const runtime = await createRuntime(overrides);
    await runtime.start();
    await waitForSignal();
    runtime.logger.info("shutting down");
    await runtime.stop();
    return 0;
  }

  const config = loadConfig(process.env, overrides);
  const logger = createLogger(command === "worker" ? config.logLevel : "warn");
  const store = new SqliteStore(config.dbPath);
  const conveyor = new Conveyor({ store, config, logger });
  await conveyor.init();
  const num = (flag: keyof typeof values, opts?: { min?: number }) => int(values[flag] as string | undefined, `--${flag}`, opts);
  const need = (v: string | undefined, what: string) => {
    if (!v) throw new UsageError(`${what} is required`);
    return v;
  };

  try {
    switch (command) {
      case "migrate":
        print(`database ready at ${config.dbPath}`, json);
        return 0;

      case "worker": {
        const concurrency = num("concurrency", { min: 1 }) ?? 1;
        const handlers = await HandlerRegistry.fromModule(need(values.handlers, "--handlers"));
        const queues = need(values.queues, "--queues").split(",").map((q) => q.trim());
        const broker = new Broker(conveyor);
        const maintenance = new Maintenance(conveyor);
        const worker = new Worker(broker, { queues, handlers, concurrency });
        maintenance.start();
        worker.start();
        await waitForSignal();
        maintenance.stop();
        await worker.stop();
        return 0;
      }

      case "enqueue": {
        const [queue, name] = args;
        const job = await conveyor.enqueue({
          queue: need(queue, "queue"),
          name: need(name, "job name"),
          payload: values.payload ? JSON.parse(values.payload) : null,
          delayMs: num("delay", { min: 0 }),
          priority: num("priority"),
          uniqueKey: values.unique,
          dependsOn: values["depends-on"]
            ?.split(",")
            .map((id) => id.trim())
            .filter(Boolean)
            .map((jobId) => ({ jobId, onFailure: (values["on-failure"] ?? "fail") as FailurePolicy })),
        });
        print(json ? job : `enqueued ${job.id} (${job.state})`, json);
        return 0;
      }

      case "jobs": {
        if (values.state !== undefined && !JOB_STATES.includes(values.state as JobState)) {
          throw new UsageError(`--state must be one of ${JOB_STATES.join(", ")}; got "${values.state}"`);
        }
        const page = await conveyor.listJobs({
          queue: values.queue,
          state: values.state as JobState | undefined,
          limit: num("limit", { min: 1 }) ?? 20,
        });
        print(json ? page : table(page.items as never, ["id", "queue", "name", "state", "attempts", "runAt", "lastError"]), json);
        return 0;
      }

      case "job": {
        const id = need(args[0], "job id");
        const job = await conveyor.getJob(id);
        const events = await conveyor.jobEvents(id);
        if (json) print({ job, events }, true);
        else {
          print(JSON.stringify(job, null, 2), false);
          print(table(events as never, ["at", "type", "data"]), false);
        }
        return 0;
      }

      case "cancel": {
        const job = await conveyor.cancel(need(args[0], "job id"));
        print(json ? job : "cancelled", json);
        return 0;
      }

      case "retry": {
        const job = await conveyor.retry(need(args[0], "job id"));
        print(json ? job : "requeued", json);
        return 0;
      }

      case "flow-submit": {
        const file = need(args[0], "flow file");
        let parsed: unknown;
        try {
          parsed = JSON.parse(file === "-" ? readFileSync(0, "utf8") : readFileSync(file, "utf8"));
        } catch (err) {
          throw new UsageError(`could not read flow from ${file}: ${(err as Error).message}`);
        }
        const flow = await conveyor.enqueueFlow((Array.isArray(parsed) ? { jobs: parsed } : parsed) as FlowRequest);
        print(json ? flow : `flow ${flow.id} (${flow.status})\n${flowTable(flow)}`, json);
        return 0;
      }

      case "flow": {
        const flow = await conveyor.getFlow(need(args[0], "flow id"));
        print(json ? flow : `flow ${flow.id} (${flow.status})\n${flowTable(flow)}`, json);
        return 0;
      }

      case "flow-cancel": {
        const id = need(args[0], "flow id");
        const before = await conveyor.getFlow(id);
        const flow = await conveyor.cancelFlow(id);
        const cancelled = flow.counts.cancelled - before.counts.cancelled;
        print(json ? flow : `cancelled ${cancelled} job${cancelled === 1 ? "" : "s"}`, json);
        return 0;
      }

      case "queues": {
        const queues = await conveyor.listQueues();
        const rows = queues.map((q) => ({ name: q.name, paused: q.paused, ...q.counts, concurrency: q.concurrency }));
        print(json ? queues : table(rows, ["name", "paused", "blocked", "waiting", "delayed", "active", "completed", "dead", "concurrency"]), json);
        return 0;
      }

      case "pause":
        await conveyor.pause(need(args[0], "queue"));
        print("paused", json);
        return 0;

      case "resume":
        await conveyor.resume(need(args[0], "queue"));
        print("resumed", json);
        return 0;

      case "queue-set": {
        const patch = Object.fromEntries(
          Object.entries({
            concurrency: num("concurrency", { min: 0 }),
            rateLimitMax: num("rate-max", { min: 0 }),
            rateLimitIntervalMs: num("rate-interval", { min: 0 }),
            defaultMaxAttempts: num("max-attempts", { min: 1 }),
          }).filter(([, v]) => v !== undefined),
        );
        print(await conveyor.updateQueue(need(args[0], "queue"), patch), json);
        return 0;
      }

      case "dlq": {
        const queue = need(args[0], "queue");
        if (values.retry) {
          const res = await conveyor.retryDead(queue);
          const lines = [`retried ${res.retried} jobs`, ...res.skipped.map((s) => `skipped ${s.jobId}: ${s.error}`)];
          print(json ? res : lines.join("\n"), json);
          return 0;
        }
        const page = await conveyor.listJobs({ queue, state: "dead", limit: num("limit", { min: 1 }) ?? 50 });
        print(json ? page : table(page.items as never, ["id", "name", "attempts", "finishedAt", "lastError"]), json);
        return 0;
      }

      case "schedules": {
        const list = await new Scheduler(conveyor).list();
        print(json ? list : table(list as never, ["id", "name", "cron", "timezone", "queue", "jobName", "enabled", "nextRunAt"]), json);
        return 0;
      }

      case "schedule-add": {
        const s = await new Scheduler(conveyor).create({
          name: need(args[0], "schedule name"),
          queue: need(values.queue, "--queue"),
          jobName: need(values.job, "--job"),
          cron: need(values.cron, "--cron"),
          timezone: values.tz,
          payload: values.payload ? JSON.parse(values.payload) : null,
          overlap: values["skip-overlap"] ? "skip" : "allow",
        });
        print(json ? s : `created ${s.id}; next run ${new Date(s.nextRunAt).toISOString()}`, json);
        return 0;
      }

      case "schedule-rm":
        await new Scheduler(conveyor).remove(need(args[0], "schedule id"));
        print("removed", json);
        return 0;

      case "token-create": {
        const scopes = need(values.scopes, "--scopes").split(",") as Scope[];
        const { token, secret } = await createToken(store, need(args[0], "token name"), scopes);
        print(json ? { id: token.id, secret } : `token ${token.id}\nsecret ${secret}\n(store the secret now; it is not shown again)`, json);
        return 0;
      }

      case "tokens": {
        const tokens = (await store.listTokens()).map(({ hash: _h, ...t }) => t);
        print(json ? tokens : table(tokens, ["id", "name", "scopes", "createdAt", "lastUsedAt", "revokedAt"]), json);
        return 0;
      }

      case "token-revoke": {
        const id = need(args[0], "token id");
        if (!(await store.revokeToken(id, Date.now()))) throw new NotFoundError(`token ${id} not found`);
        print("revoked", json);
        return 0;
      }

      default:
        throw new UsageError(`unknown command ${command}`);
    }
  } finally {
    await store.close();
  }
}

class UsageError extends Error {}

/** A flow's jobs, with dependencies shown by key (or id, for jobs outside the flow). */
function flowTable(flow: Flow): string {
  const keyOf = new Map(flow.jobs.map((j) => [j.id, j.flowKey]));
  const rows = flow.jobs.map((j) => ({
    key: j.flowKey,
    id: j.id,
    queue: j.queue,
    name: j.name,
    state: j.state,
    dependsOn:
      flow.edges
        .filter((e) => e.childId === j.id)
        .map((e) => keyOf.get(e.parentId) ?? e.parentId)
        .join(",") || null,
  }));
  return table(rows, ["key", "id", "queue", "name", "state", "dependsOn"]);
}

/** Parse an integer option strictly: "abc", "", "1.5" and "1e3" are errors, not NaN or 0. */
function int(value: string | undefined, flag: string, opts: { min?: number; max?: number } = {}): number | undefined {
  if (value === undefined) return undefined;
  if (!/^-?\d+$/.test(value.trim())) throw new UsageError(`${flag} must be an integer, got "${value}"`);
  const n = Number(value);
  if (opts.min !== undefined && n < opts.min) throw new UsageError(`${flag} must be >= ${opts.min}, got ${n}`);
  if (opts.max !== undefined && n > opts.max) throw new UsageError(`${flag} must be <= ${opts.max}, got ${n}`);
  return n;
}

function waitForSignal(): Promise<void> {
  return new Promise((resolve) => {
    const done = () => {
      process.off("SIGINT", done);
      process.off("SIGTERM", done);
      resolve();
    };
    process.on("SIGINT", done);
    process.on("SIGTERM", done);
  });
}

/** True when this file is the program being run (directly, or through the npm bin symlink). */
function isEntryPoint(): boolean {
  try {
    return process.argv[1] !== undefined && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url);
  } catch {
    return false;
  }
}

if (isEntryPoint()) {
  void main(process.argv.slice(2)).then((code) => process.exit(code));
}
