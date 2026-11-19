import { describe, expect, it } from "vitest";
import { DependencyError, dependencyOutput, dependencyResult, dependencySummary, isVisibleToDependent } from "../../src/core/dependencies.js";
import { UnrecoverableError } from "../../src/core/errors.js";
import type { FailurePolicy, Job, JobState } from "../../src/core/types.js";
import { makeJob } from "../integration/store-contract.js";

describe("dependencyResult", () => {
  const child = makeJob({ flowId: "f1", flowKey: "child" });

  it("gives a completed dependency's result, and no error", () => {
    const parent = makeJob({ state: "completed", result: { n: 1 }, lastError: "an earlier attempt failed", flowId: "f1", flowKey: "fetch" });
    expect(dependencyResult(child, { onFailure: "fail", job: parent })).toEqual({
      id: parent.id,
      key: "fetch",
      name: parent.name,
      queue: parent.queue,
      state: "completed",
      onFailure: "fail",
      result: { n: 1 },
      error: null,
    });
  });

  it("gives a dependency that didn't complete its error, and no result", () => {
    const parent = makeJob({ state: "dead", result: { partial: true }, lastError: "boom", flowId: "f1", flowKey: "x" });
    expect(dependencyResult(child, { onFailure: "continue", job: parent })).toMatchObject({ state: "dead", result: null, error: "boom" });
  });

  it("gives a key only for a dependency in the same flow", () => {
    const inOtherFlow = makeJob({ flowId: "f2", flowKey: "fetch" });
    const standalone = makeJob();
    expect(dependencyResult(child, { onFailure: "fail", job: inOtherFlow }).key).toBeNull();
    expect(dependencyResult(child, { onFailure: "fail", job: standalone }).key).toBeNull();
    expect(dependencyResult(makeJob(), { onFailure: "fail", job: standalone }).key).toBeNull();
  });

  it("summarises without result or error", () => {
    const r = dependencyResult(child, { onFailure: "fail", job: makeJob({ state: "completed", result: 5, flowId: "f1", flowKey: "k" }) });
    expect(dependencySummary(r)).toEqual({ id: r.id, key: "k", name: r.name, state: "completed" });
  });
});

describe("isVisibleToDependent", () => {
  const table: [FailurePolicy, JobState, boolean][] = [];
  for (const onFailure of ["fail", "ignore", "continue"] as FailurePolicy[]) {
    for (const state of ["blocked", "delayed", "waiting", "active", "completed", "dead", "cancelled"] as JobState[]) {
      table.push([onFailure, state, !(onFailure === "ignore" && (state === "dead" || state === "cancelled"))]);
    }
  }
  it.each(table)("%s dependency that is %s: visible = %s", (onFailure, state, visible) => {
    expect(isVisibleToDependent({ onFailure, state })).toBe(visible);
  });
});

describe("dependencyOutput (ctx.dependency)", () => {
  const child = makeJob({ id: "child", flowId: "f1", flowKey: "child" });
  const results = (...jobs: [Partial<Job>, FailurePolicy][]) => jobs.map(([j, onFailure]) => dependencyResult(child, { onFailure, job: makeJob({ flowId: "f1", ...j }) }));
  const all = results(
    [{ id: "j-ok", flowKey: "ok", state: "completed", result: { v: 1 } }, "fail"],
    [{ id: "j-dead", flowKey: "dead", state: "dead", lastError: "boom" }, "continue"],
    // ctx.dependency looks at every dependency, including ignored ones that failed.
    [{ id: "j-gone", flowKey: "gone", state: "cancelled", lastError: null }, "ignore"],
    [{ id: "j-outside", flowId: "other", flowKey: "ok", state: "completed", result: "outside" }, "fail"],
  );

  it("returns a completed dependency's result, by key or by id", () => {
    expect(dependencyOutput("child", all, "ok")).toEqual({ v: 1 });
    expect(dependencyOutput("child", all, "j-ok")).toEqual({ v: 1 });
    // A job outside the flow has no key here, even if its own flow gave it one.
    expect(dependencyOutput("child", all, "j-outside")).toBe("outside");
  });

  it("throws a DependencyError naming the dependency and how it ended if it didn't complete", () => {
    expect(() => dependencyOutput("child", all, "dead")).toThrow(new DependencyError('dependency "dead" (job j-dead) did not complete; it is dead: boom', "j-dead", "dead"));
    expect(() => dependencyOutput("child", all, "gone")).toThrow('dependency "gone" (job j-gone) did not complete; it is cancelled');
    try {
      dependencyOutput("child", all, "dead");
    } catch (err) {
      expect(err).toBeInstanceOf(UnrecoverableError);
      expect(err).toMatchObject({ name: "DependencyError", dependencyId: "j-dead", dependencyState: "dead" });
    }
  });

  it("throws a DependencyError listing the dependencies for an unknown key", () => {
    expect(() => dependencyOutput("child", all, "nope")).toThrow('job child has no dependency "nope" (its dependencies: ok, dead, gone, j-outside)');
    expect(() => dependencyOutput("child", [], "nope")).toThrow('job child has no dependency "nope" (its dependencies: none)');
  });
});
