import type { Broker } from "../../broker/broker.js";
import { ValidationError } from "../../core/errors.js";
import type { Json } from "../../util/json.js";
import { asObject, bool, int, str, strArray } from "../../util/validate.js";
import type { Router } from "../router.js";
import { jobView } from "../serialize.js";

// Lease protocol over HTTP so workers written in any language can process jobs
export function registerWorkerRoutes(router: Router, broker: Broker): void {
  router.post(
    "/v1/leases",
    async ({ body }) => {
      const o = asObject(body);
      const queues = strArray(o, "queues");
      if (!queues || queues.length === 0) throw new ValidationError("queues is required");
      const workerId = str(o, "workerId", { required: true, max: 200 })!;
      const lease = await broker.claim(queues, workerId);
      if (!lease) return { status: 204 };
      return {
        status: 201,
        body: { job: jobView(lease.job), leaseToken: lease.token, leaseExpiresAt: new Date(lease.expiresAt).toISOString() },
      };
    },
    "workers",
  );

  router.post(
    "/v1/jobs/:id/heartbeat",
    async ({ params, body }) => {
      const o = asObject(body);
      const expiresAt = await broker.heartbeat(params.id!, str(o, "leaseToken", { required: true })!);
      return { body: { leaseExpiresAt: new Date(expiresAt).toISOString() } };
    },
    "workers",
  );

  router.post(
    "/v1/jobs/:id/progress",
    async ({ params, body }) => {
      const o = asObject(body);
      const progress = o.progress;
      if (typeof progress !== "number") throw new ValidationError("progress must be a number");
      await broker.progress(params.id!, str(o, "leaseToken", { required: true })!, progress);
      return { status: 204 };
    },
    "workers",
  );

  router.post(
    "/v1/jobs/:id/complete",
    async ({ params, body }) => {
      const o = asObject(body);
      const job = await broker.complete(params.id!, str(o, "leaseToken", { required: true })!, (o.result ?? null) as Json);
      return { body: jobView(job) };
    },
    "workers",
  );

  router.post(
    "/v1/jobs/:id/fail",
    async ({ params, body }) => {
      const o = asObject(body);
      const job = await broker.fail(params.id!, str(o, "leaseToken", { required: true })!, str(o, "error") ?? "failed", {
        unrecoverable: bool(o, "unrecoverable"),
        retryInMs: int(o, "retryInMs", { min: 0 }),
      });
      return { body: jobView(job) };
    },
    "workers",
  );

  router.post(
    "/v1/jobs/:id/release",
    async ({ params, body }) => {
      const o = asObject(body);
      await broker.release(params.id!, str(o, "leaseToken", { required: true })!);
      return { status: 204 };
    },
    "workers",
  );
}
