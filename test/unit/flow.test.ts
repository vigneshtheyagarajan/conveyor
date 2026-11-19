import { describe, expect, it } from "vitest";
import { ValidationError } from "../../src/core/errors.js";
import { flowStatus, MAX_FLOW_JOBS, planFlow, stateCounts } from "../../src/core/flow.js";
import type { FlowJobInput, FlowRequest, JobState } from "../../src/core/types.js";

const job = (key: string, dependsOn?: FlowJobInput["dependsOn"], extra: Partial<FlowJobInput> = {}): FlowJobInput => ({
  key,
  queue: "q",
  name: "x",
  ...(dependsOn ? { dependsOn } : {}),
  ...extra,
});
const keys = (req: FlowRequest) => planFlow(req).jobs.map((j) => j.key);
const rejects = (req: unknown, message: string | RegExp) => {
  expect(() => planFlow(req as FlowRequest)).toThrow(ValidationError);
  expect(() => planFlow(req as FlowRequest)).toThrow(message);
};

describe("planFlow: dependency order", () => {
  it("puts every job after the jobs it depends on", () => {
    expect(keys({ jobs: [job("d", ["b", "c"]), job("b", ["a"]), job("c", ["a"]), job("a")] })).toEqual(["a", "b", "c", "d"]);
  });

  it("keeps the given order as far as dependencies allow", () => {
    // Each time, the earliest given job whose dependencies are placed comes next.
    expect(keys({ jobs: [job("z"), job("y", ["x"]), job("x"), job("w")] })).toEqual(["z", "x", "y", "w"]);
    expect(keys({ jobs: [job("c", ["root"]), job("b", ["root"]), job("root")] })).toEqual(["root", "c", "b"]);
  });

  it("resolves keys, {key}, and {jobId} dependencies, with the flow's default policy", () => {
    const plan = planFlow({
      onFailure: "continue",
      jobs: [job("a"), job("b", ["a", { key: "c", onFailure: "ignore" }, { jobId: "existing" }, { jobId: "other", onFailure: "fail" }]), job("c")],
    });
    expect(plan.edges).toEqual([
      { childKey: "b", parent: { key: "a" }, onFailure: "continue" },
      { childKey: "b", parent: { key: "c" }, onFailure: "ignore" },
      { childKey: "b", parent: { jobId: "existing" }, onFailure: "continue" },
      { childKey: "b", parent: { jobId: "other" }, onFailure: "fail" },
    ]);
    expect(planFlow({ jobs: [job("a"), job("b", ["a"])] }).edges[0]!.onFailure).toBe("fail");
  });

  it("accepts exactly the maximum number of jobs", () => {
    const jobs = Array.from({ length: MAX_FLOW_JOBS }, (_, i) => job(`j${i}`, i ? [`j${i - 1}`] : undefined));
    expect(planFlow({ jobs }).jobs).toHaveLength(MAX_FLOW_JOBS);
  });
});

describe("planFlow: cycles", () => {
  it("names the keys of a long cycle in run order, from the earliest job", () => {
    // A runs before B, B before C, C before D, and D before A.
    rejects({ jobs: [job("A", ["D"]), job("B", ["A"]), job("C", ["B"]), job("D", ["C"])] }, "flow has a dependency cycle: A -> B -> C -> D -> A");
  });

  it("finds the cycle when other jobs lead into or out of it", () => {
    rejects(
      {
        jobs: [job("start"), job("x", ["start", "z"]), job("y", ["x"]), job("z", ["y"]), job("after", ["z"]), job("free")],
      },
      "flow has a dependency cycle: x -> y -> z -> x",
    );
  });

  it("reports a two-job cycle", () => {
    rejects({ jobs: [job("a", ["b"]), job("b", ["a"])] }, "flow has a dependency cycle: a -> b -> a");
  });

  it("reports one cycle when there are several", () => {
    rejects({ jobs: [job("a", ["b"]), job("b", ["a"]), job("c", ["d"]), job("d", ["c"])] }, /^flow has a dependency cycle: a -> b -> a$/);
  });
});

describe("planFlow: rejected requests", () => {
  it.each<[string, unknown, string | RegExp]>([
    ["a missing jobs array", {}, "jobs must be an array"],
    ["a null request", null, "jobs must be an array"],
    ["an empty flow", { jobs: [] }, "a flow needs at least one job"],
    ["too many jobs", { jobs: Array.from({ length: MAX_FLOW_JOBS + 1 }, (_, i) => job(`j${i}`)) }, "a flow can have at most 1000 jobs"],
    ["a job that is not an object", { jobs: [job("a"), "b"] }, "jobs[1] must be an object"],
    ["a missing key", { jobs: [{ queue: "q", name: "x" }] }, /jobs\[0\]\.key must be/],
    ["an invalid key", { jobs: [job("has space")] }, /jobs\[0\]\.key must be/],
    ["a duplicate key", { jobs: [job("a"), job("b"), job("a")] }, "duplicate key a"],
    ["a uniqueKey", { jobs: [job("a", undefined, { uniqueKey: "u" } as never)] }, "job a: uniqueKey is not allowed in a flow"],
    ["a self-dependency", { jobs: [job("a", ["a"])] }, "job a depends on itself"],
    ["a self-dependency by {key}", { jobs: [job("a", [{ key: "a" }])] }, "job a depends on itself"],
    ["an unknown key", { jobs: [job("a", ["nope"])] }, "job a depends on unknown key nope"],
    ["a key listed twice", { jobs: [job("a"), job("b", ["a", { key: "a", onFailure: "ignore" }])] }, "job b lists dependency a twice"],
    ["a job id listed twice", { jobs: [job("b", [{ jobId: "x" }, { jobId: "x" }])] }, "job b lists dependency x twice"],
    ["dependsOn that is not an array", { jobs: [job("a", "b" as never)] }, "job a: dependsOn must be an array"],
    ["a dependency with both key and jobId", { jobs: [job("a"), job("b", [{ key: "a", jobId: "x" } as never])] }, /job b: each dependency must be/],
    ["a dependency with neither", { jobs: [job("b", [{ onFailure: "fail" } as never])] }, /job b: each dependency must be/],
    ["a numeric dependency", { jobs: [job("b", [5 as never])] }, /job b: each dependency must be/],
    ["an empty jobId", { jobs: [job("b", [{ jobId: "" }])] }, "job b: jobId must not be empty"],
    ["an unknown policy", { jobs: [job("a"), job("b", [{ key: "a", onFailure: "retry" as never }])] }, "job b: onFailure must be one of fail, ignore, continue"],
    ["an unknown flow policy", { onFailure: "maybe", jobs: [job("a")] }, "onFailure must be one of fail, ignore, continue"],
    ["too many dependencies", { jobs: [job("b", Array.from({ length: 1001 }, (_, i) => ({ jobId: `x${i}` })))] }, "job b: at most 1000 dependencies"],
  ])("rejects %s", (_label, req, message) => {
    rejects(req, message);
  });
});

describe("flowStatus", () => {
  const of = (...states: JobState[]) => flowStatus(states.map((state) => ({ state })));
  it("is running while any job is unfinished", () => {
    expect(of("completed", "blocked")).toBe("running");
    expect(of("dead", "active")).toBe("running");
  });
  it("is completed when every job completed, failed if any is dead, otherwise cancelled", () => {
    expect(of("completed", "completed")).toBe("completed");
    expect(of("completed", "dead", "cancelled")).toBe("failed");
    expect(of("completed", "cancelled")).toBe("cancelled");
  });
});

describe("stateCounts", () => {
  it("counts every state, with zeros", () => {
    expect(stateCounts([{ state: "blocked" }, { state: "blocked" }, { state: "dead" }])).toEqual({
      blocked: 2,
      delayed: 0,
      waiting: 0,
      active: 0,
      completed: 0,
      dead: 1,
      cancelled: 0,
    });
  });
});
