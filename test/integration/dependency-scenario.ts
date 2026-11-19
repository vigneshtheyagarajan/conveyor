// One scenario run by both the in-process tests (dependency-results.test.ts) and the HTTP
// worker tests (api-dependencies.test.ts), so that both views of a job's dependencies are
// checked against the same expectations.
import type { FlowRequest, Job } from "../../src/core/types.js";

/**
 * "ok" completes with a result, "dead" fails without retries, and three jobs depend on both,
 * each with a different policy for "dead".
 */
export const POLICY_FLOW: FlowRequest = {
  jobs: [
    { key: "ok", queue: "q", name: "produce", payload: { value: 42 } },
    { key: "dead", queue: "q", name: "explode", maxAttempts: 1 },
    { key: "viaFail", queue: "q", name: "read", dependsOn: ["ok", { key: "dead", onFailure: "fail" }] },
    { key: "viaIgnore", queue: "q", name: "read", dependsOn: ["ok", { key: "dead", onFailure: "ignore" }] },
    { key: "viaContinue", queue: "q", name: "read", dependsOn: ["ok", { key: "dead", onFailure: "continue" }] },
  ],
};

const OK = { id: "<ok>", key: "ok", name: "produce", queue: "q", state: "completed", onFailure: "fail", result: { value: 42 }, error: null };
const DEAD = { id: "<dead>", key: "dead", name: "explode", queue: "q", state: "dead", result: null, error: "boom" };

/** What each reading job sees of its dependencies, with ids replaced by `<key>`. */
export const EXPECTED_VIEWS = {
  // Under ignore the failed dependency isn't there at all.
  viaIgnore: [OK],
  // Under continue it is, with its state and error.
  viaContinue: [OK, { ...DEAD, onFailure: "continue" }],
};

/** How the flow ends: the fail dependent is cancelled without running; the others run. */
export const EXPECTED_STATES = { ok: "completed", dead: "dead", viaFail: "cancelled", viaIgnore: "completed", viaContinue: "completed" };

/** Replace a flow's job ids with `<key>` anywhere in a value, so runs can be compared. */
export function byKeys(jobs: Job[], value: unknown): unknown {
  let text = JSON.stringify(value);
  for (const j of jobs) text = text.split(j.id).join(`<${j.flowKey}>`);
  return JSON.parse(text);
}
