import { ValidationError } from "./errors.js";
import {
  FAILURE_POLICIES,
  JOB_STATES,
  TERMINAL_STATES,
  type FailurePolicy,
  type FlowJobInput,
  type FlowRequest,
  type FlowStatus,
  type Job,
  type JobState,
} from "./types.js";
import { NAME_PATTERN } from "../util/validate.js";

/** Most jobs one flow can have. */
export const MAX_FLOW_JOBS = 1000;
/** Most dependencies one job can have. */
export const MAX_DEPENDENCIES = 1000;

/** A dependency of a flow job, resolved: another job of the flow, or an existing job. */
export interface PlannedEdge {
  childKey: string;
  parent: { key: string } | { jobId: string };
  onFailure: FailurePolicy;
}

export interface FlowPlan {
  /** The jobs in dependency order: each comes after the flow jobs it depends on. */
  jobs: FlowJobInput[];
  edges: PlannedEdge[];
}

function policy(value: unknown, where: string): FailurePolicy {
  if (!FAILURE_POLICIES.includes(value as FailurePolicy)) {
    throw new ValidationError(`${where}onFailure must be one of ${FAILURE_POLICIES.join(", ")}`);
  }
  return value as FailurePolicy;
}

/**
 * Check a flow's structure and put its jobs in dependency order. Rejects (with a
 * ValidationError, before anything is written) an empty or oversized flow, a missing, invalid
 * or repeated key, a uniqueKey, a dependency on an unknown key or on the job itself, a
 * dependency listed twice, and a cycle, naming the keys in it. Each job's own fields (queue,
 * name, payload, ...) are checked by Conveyor; dependencies on existing jobs are checked when
 * the flow is inserted.
 */
export function planFlow(req: FlowRequest): FlowPlan {
  if (req === null || typeof req !== "object" || !Array.isArray(req.jobs)) throw new ValidationError("jobs must be an array");
  const jobs = req.jobs;
  if (jobs.length === 0) throw new ValidationError("a flow needs at least one job");
  if (jobs.length > MAX_FLOW_JOBS) throw new ValidationError(`a flow can have at most ${MAX_FLOW_JOBS} jobs`);
  const defaultPolicy = req.onFailure === undefined ? "fail" : policy(req.onFailure, "");

  const index = new Map<string, number>();
  jobs.forEach((job, i) => {
    if (job === null || typeof job !== "object" || Array.isArray(job)) throw new ValidationError(`jobs[${i}] must be an object`);
    if (typeof job.key !== "string" || !NAME_PATTERN.test(job.key)) {
      throw new ValidationError(`jobs[${i}].key must be 1-128 letters, digits and _.:- starting with a letter or digit`);
    }
    if (index.has(job.key)) throw new ValidationError(`duplicate key ${job.key}`);
    if ((job as { uniqueKey?: unknown }).uniqueKey !== undefined) {
      throw new ValidationError(`job ${job.key}: uniqueKey is not allowed in a flow`);
    }
    index.set(job.key, i);
  });

  const edges: PlannedEdge[] = [];
  /** For each job (by index), the indexes of the flow jobs it depends on, in the order given. */
  const parents: number[][] = jobs.map(() => []);
  jobs.forEach((job, i) => {
    const deps = job.dependsOn;
    if (deps === undefined) return;
    const where = `job ${job.key}: `;
    if (!Array.isArray(deps)) throw new ValidationError(`${where}dependsOn must be an array`);
    if (deps.length > MAX_DEPENDENCIES) throw new ValidationError(`${where}at most ${MAX_DEPENDENCIES} dependencies`);
    const seen = new Set<string>();
    for (const entry of deps) {
      const dep = typeof entry === "string" ? { key: entry } : entry;
      const hasKey = typeof (dep as { key?: unknown })?.key === "string";
      const hasId = typeof (dep as { jobId?: unknown })?.jobId === "string";
      if (typeof dep !== "object" || dep === null || hasKey === hasId) {
        throw new ValidationError(`${where}each dependency must be a key, {key, onFailure} or {jobId, onFailure}`);
      }
      const onFailure = dep.onFailure === undefined ? defaultPolicy : policy(dep.onFailure, where);
      if ("key" in dep) {
        if (dep.key === job.key) throw new ValidationError(`job ${job.key} depends on itself`);
        const parent = index.get(dep.key);
        if (parent === undefined) throw new ValidationError(`job ${job.key} depends on unknown key ${dep.key}`);
        if (seen.has(`key:${dep.key}`)) throw new ValidationError(`job ${job.key} lists dependency ${dep.key} twice`);
        seen.add(`key:${dep.key}`);
        parents[i]!.push(parent);
        edges.push({ childKey: job.key, parent: { key: dep.key }, onFailure });
      } else {
        if (dep.jobId.length === 0) throw new ValidationError(`${where}jobId must not be empty`);
        if (seen.has(`id:${dep.jobId}`)) throw new ValidationError(`job ${job.key} lists dependency ${dep.jobId} twice`);
        seen.add(`id:${dep.jobId}`);
        edges.push({ childKey: job.key, parent: { jobId: dep.jobId }, onFailure });
      }
    }
  });

  return { jobs: topologicalOrder(jobs, parents), edges };
}

/**
 * Kahn's algorithm, taking ready jobs in the order they were given so the result is stable.
 * Throws a ValidationError naming the keys of a cycle if there is one.
 */
function topologicalOrder(jobs: FlowJobInput[], parents: number[][]): FlowJobInput[] {
  const children: number[][] = jobs.map(() => []);
  const waiting = parents.map((ps) => ps.length);
  parents.forEach((ps, child) => ps.forEach((p) => children[p]!.push(child)));

  const ready = waiting.flatMap((n, i) => (n === 0 ? [i] : []));
  const order: number[] = [];
  while (ready.length > 0) {
    const next = ready.shift()!;
    order.push(next);
    for (const child of children[next]!) {
      if (--waiting[child]! === 0) {
        // Keep `ready` sorted by position so equal-rank jobs keep their input order.
        const at = ready.findIndex((r) => r > child);
        ready.splice(at === -1 ? ready.length : at, 0, child);
      }
    }
  }
  if (order.length < jobs.length) throw new ValidationError(`flow has a dependency cycle: ${describeCycle(jobs, parents, waiting)}`);
  return order.map((i) => jobs[i]!);
}

/**
 * Find one cycle among the jobs Kahn's algorithm couldn't order and describe it in run order,
 * starting from its earliest job: "a -> b -> c -> a" means a must finish before b, and so on.
 */
function describeCycle(jobs: FlowJobInput[], parents: number[][], waiting: number[]): string {
  // Every unordered job still waits for at least one unordered job, so walking from one to an
  // unordered parent must eventually come back to a job already on the path.
  const stuck = (i: number) => waiting[i]! > 0;
  const path: number[] = [];
  const seenAt = new Map<number, number>();
  let at = waiting.findIndex((n) => n > 0);
  while (!seenAt.has(at)) {
    seenAt.set(at, path.length);
    path.push(at);
    at = parents[at]!.find(stuck)!;
  }
  // `path` follows "waits for"; reverse it for run order, then start at the earliest job.
  const cycle = path.slice(seenAt.get(at)).reverse();
  const first = cycle.indexOf(Math.min(...cycle));
  const rotated = [...cycle.slice(first), ...cycle.slice(0, first)];
  return [...rotated, rotated[0]!].map((i) => jobs[i]!.key).join(" -> ");
}

/** A flow's status, derived from its jobs. */
export function flowStatus(jobs: Pick<Job, "state">[]): FlowStatus {
  if (jobs.some((j) => !TERMINAL_STATES.includes(j.state))) return "running";
  if (jobs.every((j) => j.state === "completed")) return "completed";
  return jobs.some((j) => j.state === "dead") ? "failed" : "cancelled";
}

export function stateCounts(jobs: Pick<Job, "state">[]): Record<JobState, number> {
  const counts = Object.fromEntries(JOB_STATES.map((s) => [s, 0])) as Record<JobState, number>;
  for (const j of jobs) counts[j.state]++;
  return counts;
}
