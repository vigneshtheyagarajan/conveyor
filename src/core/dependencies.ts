import { UnrecoverableError } from "./errors.js";
import type { FailurePolicy, Job, JobState, LinkedJob } from "./types.js";
import type { Json } from "../util/json.js";

/**
 * One dependency as a running job sees it. In-process handlers get these from
 * ctx.dependencies(); HTTP workers from GET /v1/jobs/:id/dependencies (and, without `result` and
 * `error`, in the lease). Both come from dependencyResult() and are filtered by
 * isVisibleToDependent(), so the two views agree.
 */
export interface DependencyResult {
  id: string;
  /** The dependency's key, if it is in the same flow as the dependent; otherwise null (use `id`). */
  key: string | null;
  name: string;
  queue: string;
  /** `completed`, or (under `continue`) `dead` or `cancelled`. */
  state: JobState;
  onFailure: FailurePolicy;
  /** Its result if it completed, otherwise null. */
  result: Json | null;
  /** Why it didn't complete (its last error), if it didn't; otherwise null. */
  error: string | null;
}

/** The dependency summary in a lease: no result or error. */
export type DependencySummary = Pick<DependencyResult, "id" | "key" | "name" | "state">;

export function dependencyResult(dependent: Pick<Job, "flowId">, link: LinkedJob): DependencyResult {
  const { job, onFailure } = link;
  const completed = job.state === "completed";
  return {
    id: job.id,
    key: dependent.flowId !== null && job.flowId === dependent.flowId ? job.flowKey : null,
    name: job.name,
    queue: job.queue,
    state: job.state,
    onFailure,
    result: completed ? job.result : null,
    error: completed ? null : job.lastError,
  };
}

/**
 * Whether a dependent sees this dependency in its list: always, except that an `ignore`
 * dependency that failed (ended dead or cancelled) is left out. A `fail` one always completed,
 * or the dependent wouldn't be running; a failed `continue` one is shown with its state and
 * error. `Store.listDependencyJobs(id, { visibleOnly: true })` applies the same rule in SQL.
 */
export function isVisibleToDependent(d: Pick<DependencyResult, "onFailure" | "state">): boolean {
  return d.onFailure !== "ignore" || (d.state !== "dead" && d.state !== "cancelled");
}

export function dependencySummary(d: DependencyResult): DependencySummary {
  return { id: d.id, key: d.key, name: d.name, state: d.state };
}

/**
 * Thrown by ctx.dependency(key) when the dependency didn't complete, or there is no dependency
 * with that key or id. Unrecoverable: retrying the job doesn't change its dependencies, so a
 * handler that lets it through fails the job without using up its attempts.
 */
export class DependencyError extends UnrecoverableError {
  constructor(
    message: string,
    /** The dependency's id, or null if there is no such dependency. */
    readonly dependencyId: string | null,
    /** The state it ended in, or null if there is no such dependency. */
    readonly dependencyState: JobState | null,
  ) {
    super(message);
    this.name = "DependencyError";
  }
}

/** ctx.dependency(key): the result of the dependency with this key (or id), or a DependencyError. */
export function dependencyOutput(jobId: string, all: DependencyResult[], key: string): Json | null {
  const found = all.find((d) => d.key === key) ?? all.find((d) => d.id === key);
  if (!found) {
    const known = all.map((d) => d.key ?? d.id).join(", ") || "none";
    throw new DependencyError(`job ${jobId} has no dependency "${key}" (its dependencies: ${known})`, null, null);
  }
  if (found.state !== "completed") {
    const why = found.error === null ? "" : `: ${found.error}`;
    throw new DependencyError(`dependency "${key}" (job ${found.id}) did not complete; it is ${found.state}${why}`, found.id, found.state);
  }
  return found.result;
}
