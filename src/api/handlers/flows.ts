import type { Conveyor } from "../../core/conveyor.js";
import { ValidationError } from "../../core/errors.js";
import type { FailurePolicy, FlowDependencyInput, FlowJobInput } from "../../core/types.js";
import { asObject, str } from "../../util/validate.js";
import type { Router } from "../router.js";
import { flowView } from "../serialize.js";
import { parseEnqueue } from "./jobs.js";

/** One job of a POST /v1/flows body: an enqueue request with a key and key-based dependsOn. */
function parseFlowJob(raw: unknown, i: number): FlowJobInput {
  try {
    const o = asObject(raw, `jobs[${i}]`);
    return {
      ...parseEnqueue(o),
      key: str(o, "key", { required: true })!,
      // Checked by planFlow: keys, {key, onFailure} or {jobId, onFailure}.
      dependsOn: (o.dependsOn ?? undefined) as FlowDependencyInput[] | undefined,
    };
  } catch (err) {
    if (err instanceof ValidationError && !err.message.startsWith(`jobs[${i}]`)) {
      throw new ValidationError(`jobs[${i}]: ${err.message}`);
    }
    throw err;
  }
}

export function registerFlowRoutes(router: Router, conveyor: Conveyor): void {
  router.post(
    "/v1/flows",
    async ({ body }) => {
      const o = asObject(body);
      if (!Array.isArray(o.jobs)) throw new ValidationError("jobs must be an array");
      const flow = await conveyor.enqueueFlow({
        jobs: o.jobs.map(parseFlowJob),
        onFailure: str(o, "onFailure") as FailurePolicy | undefined,
      });
      return { status: 201, body: flowView(flow) };
    },
    "jobs:write",
  );

  router.get("/v1/flows/:id", async ({ params }) => ({ body: flowView(await conveyor.getFlow(params.id!)) }), "jobs:read");

  router.post(
    "/v1/flows/:id/cancel",
    async ({ params }) => ({ body: flowView(await conveyor.cancelFlow(params.id!)) }),
    "jobs:write",
  );
}
