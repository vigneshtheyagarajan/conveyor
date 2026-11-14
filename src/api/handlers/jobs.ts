import type { Conveyor } from "../../core/conveyor.js";
import { ValidationError } from "../../core/errors.js";
import type { EnqueueRequest, JobState } from "../../core/types.js";
import { JOB_STATES } from "../../core/types.js";
import { parseBackoff } from "../../util/backoff.js";
import type { Json } from "../../util/json.js";
import { asObject, int, str } from "../../util/validate.js";
import type { Router } from "../router.js";
import { jobView } from "../serialize.js";

export function parseEnqueue(raw: unknown): EnqueueRequest {
  const o = asObject(raw);
  let backoff;
  try {
    backoff = parseBackoff(o.backoff);
  } catch (err) {
    throw new ValidationError((err as Error).message);
  }
  const runAtRaw = str(o, "runAt");
  let runAt: number | undefined;
  if (runAtRaw) {
    runAt = Date.parse(runAtRaw);
    if (Number.isNaN(runAt)) throw new ValidationError("runAt must be an ISO-8601 timestamp");
  }
  return {
    queue: str(o, "queue", { required: true })!,
    name: str(o, "name", { required: true })!,
    payload: (o.payload ?? null) as Json,
    priority: int(o, "priority"),
    delayMs: int(o, "delayMs", { min: 0 }),
    runAt,
    maxAttempts: int(o, "maxAttempts", { min: 1, max: 100 }),
    timeoutMs: int(o, "timeoutMs", { min: 0 }),
    uniqueKey: str(o, "uniqueKey", { max: 256 }),
    backoff,
  };
}

export function registerJobRoutes(router: Router, conveyor: Conveyor): void {
  router.post(
    "/v1/jobs",
    async ({ body }) => {
      const req = parseEnqueue(body);
      const job = await conveyor.enqueue(req);
      return { status: 201, body: jobView(job) };
    },
    "jobs:write",
  );

  router.post(
    "/v1/jobs/bulk",
    async ({ body }) => {
      const o = asObject(body);
      if (!Array.isArray(o.jobs)) throw new ValidationError("jobs must be an array");
      const jobs = await conveyor.enqueueBulk(o.jobs.map(parseEnqueue));
      return { status: 201, body: { jobs: jobs.map(jobView) } };
    },
    "jobs:write",
  );

  router.get(
    "/v1/jobs",
    async ({ query }) => {
      const state = query.get("state") ?? undefined;
      if (state && !JOB_STATES.includes(state as JobState)) throw new ValidationError(`unknown state ${state}`);
      const limit = query.has("limit") ? Number(query.get("limit")) : undefined;
      if (limit !== undefined && (!Number.isInteger(limit) || limit < 1)) {
        throw new ValidationError("limit must be a positive integer");
      }
      const page = await conveyor.listJobs({
        queue: query.get("queue") ?? undefined,
        state: state as JobState | undefined,
        name: query.get("name") ?? undefined,
        scheduleId: query.get("scheduleId") ?? undefined,
        cursor: query.get("cursor") ?? undefined,
        limit,
      });
      return { body: { items: page.items.map(jobView), nextCursor: page.nextCursor } };
    },
    "jobs:read",
  );

  router.get("/v1/jobs/:id", async ({ params }) => ({ body: jobView(await conveyor.getJob(params.id!)) }), "jobs:read");

  router.get(
    "/v1/jobs/:id/events",
    async ({ params }) => ({ body: { items: await conveyor.jobEvents(params.id!) } }),
    "jobs:read",
  );

  router.post(
    "/v1/jobs/:id/cancel",
    async ({ params }) => ({ body: jobView(await conveyor.cancel(params.id!)) }),
    "jobs:write",
  );

  router.post(
    "/v1/jobs/:id/retry",
    async ({ params }) => ({ body: jobView(await conveyor.retry(params.id!)) }),
    "jobs:write",
  );
}
