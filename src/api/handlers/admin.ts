import type { Store } from "../../storage/store.js";
import type { Scope } from "../../core/types.js";
import { NotFoundError, ValidationError } from "../../core/errors.js";
import type { ScheduleInput, Scheduler } from "../../scheduler/scheduler.js";
import type { WebhookDispatcher } from "../../webhooks/dispatcher.js";
import { upcomingRuns } from "../../scheduler/cron.js";
import type { Json } from "../../util/json.js";
import { asObject, bool, str, strArray } from "../../util/validate.js";
import { createToken } from "../auth.js";
import type { Router } from "../router.js";
import { scheduleView, tokenView, webhookView } from "../serialize.js";

export function registerScheduleRoutes(router: Router, scheduler: Scheduler): void {
  const parse = (body: unknown, partial: boolean) => {
    const o = asObject(body);
    const overlap = str(o, "overlap");
    return {
      name: str(o, "name", { required: !partial }),
      queue: str(o, "queue", { required: !partial }),
      jobName: str(o, "jobName", { required: !partial }),
      cron: str(o, "cron", { required: !partial }),
      timezone: str(o, "timezone"),
      enabled: bool(o, "enabled"),
      overlap: overlap as "allow" | "skip" | undefined,
      payload: o.payload as Json | undefined,
    };
  };

  router.get("/v1/schedules", async () => ({ body: { items: (await scheduler.list()).map(scheduleView) } }), "jobs:read");

  router.post(
    "/v1/schedules",
    async ({ body }) => {
      const input = parse(body, false);
      const s = await scheduler.create(input as ScheduleInput);
      return { status: 201, body: scheduleView(s) };
    },
    "admin",
  );

  router.get(
    "/v1/schedules/:id",
    async ({ params }) => {
      const s = await scheduler.get(params.id!);
      return { body: { ...scheduleView(s), upcoming: upcomingRuns(s.cron, Date.now(), s.timezone).map((t) => new Date(t).toISOString()) } };
    },
    "jobs:read",
  );

  router.patch(
    "/v1/schedules/:id",
    async ({ params, body }) => {
      const patch = Object.fromEntries(Object.entries(parse(body, true)).filter(([, v]) => v !== undefined));
      return { body: scheduleView(await scheduler.update(params.id!, patch)) };
    },
    "admin",
  );

  router.delete(
    "/v1/schedules/:id",
    async ({ params }) => {
      await scheduler.remove(params.id!);
      return { status: 204 };
    },
    "admin",
  );

  router.post(
    "/v1/schedules/:id/trigger",
    async ({ params }) => ({ status: 201, body: { jobId: await scheduler.trigger(params.id!) } }),
    "jobs:write",
  );
}

export function registerWebhookRoutes(router: Router, webhooks: WebhookDispatcher): void {
  router.get("/v1/webhooks", async () => ({ body: { items: (await webhooks.list()).map((w) => webhookView(w)) } }), "admin");

  router.post(
    "/v1/webhooks",
    async ({ body }) => {
      const o = asObject(body);
      const hook = await webhooks.create({
        url: str(o, "url", { required: true })!,
        events: strArray(o, "events") ?? [],
        queues: strArray(o, "queues"),
        secret: str(o, "secret"),
      });
      return { status: 201, body: webhookView(hook, true) };
    },
    "admin",
  );

  router.delete(
    "/v1/webhooks/:id",
    async ({ params }) => {
      await webhooks.remove(params.id!);
      return { status: 204 };
    },
    "admin",
  );

  router.get(
    "/v1/webhooks/:id/deliveries",
    async ({ params }) => ({ body: { items: await webhooks.deliveries(params.id!) } }),
    "admin",
  );
}

export function registerTokenRoutes(router: Router, store: Store): void {
  router.get("/v1/tokens", async () => ({ body: { items: (await store.listTokens()).map(tokenView) } }), "admin");

  router.post(
    "/v1/tokens",
    async ({ body }) => {
      const o = asObject(body);
      const scopes = (strArray(o, "scopes") ?? []) as Scope[];
      const { token, secret } = await createToken(store, str(o, "name", { required: true, max: 100 })!, scopes);
      return { status: 201, body: { ...tokenView(token), secret } };
    },
    "admin",
  );

  router.delete(
    "/v1/tokens/:id",
    async ({ params }) => {
      if (!params.id) throw new ValidationError("id is required");
      if (!(await store.revokeToken(params.id, Date.now()))) throw new NotFoundError(`token ${params.id} not found`);
      return { status: 204 };
    },
    "admin",
  );
}
