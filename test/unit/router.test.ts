import { describe, expect, it } from "vitest";
import { ValidationError } from "../../src/core/errors.js";
import { Router, type RouteHandler } from "../../src/api/router.js";

const h: RouteHandler = async () => undefined;

function build() {
  return new Router()
    .get("/v1/jobs", h, "jobs:read")
    .post("/v1/jobs", h, "jobs:write")
    .post("/v1/jobs/bulk", h, "jobs:write")
    .get("/v1/jobs/:id", h, "jobs:read")
    .post("/v1/jobs/:id/cancel", h, "jobs:write")
    .put("/v1/queues/:name", h, "admin")
    .patch("/v1/schedules/:id", h, "admin")
    .delete("/v1/schedules/:id", h, "admin")
    .get("/health", h);
}

describe("Router", () => {
  it("matches static routes by method and path", () => {
    const m = build().match("GET", "/v1/jobs");
    expect(m && "route" in m && m.route.pattern).toBe("/v1/jobs");
    const p = build().match("POST", "/v1/jobs");
    expect(p && "route" in p && p.route.scope).toBe("jobs:write");
  });

  it("extracts named params", () => {
    const m = build().match("POST", "/v1/jobs/abc123/cancel");
    expect(m).toMatchObject({ route: { pattern: "/v1/jobs/:id/cancel" }, params: { id: "abc123" } });
  });

  it("percent-decodes params, including encoded slashes", () => {
    const m = build().match("PUT", "/v1/queues/a%2Fb%20c");
    expect(m).toMatchObject({ params: { name: "a/b c" } });
  });

  it("prefers the first registered route when several match", () => {
    const m = build().match("POST", "/v1/jobs/bulk");
    expect(m).toMatchObject({ route: { pattern: "/v1/jobs/bulk" }, params: {} });
  });

  it("accepts a trailing slash", () => {
    expect(build().match("GET", "/v1/jobs/")).toMatchObject({ route: { pattern: "/v1/jobs" } });
    expect(build().match("GET", "/v1/jobs/x1/")).toMatchObject({ params: { id: "x1" } });
  });

  it("does not let a param span path segments", () => {
    expect(build().match("GET", "/v1/jobs/a/b")).toBeUndefined();
  });

  it("does not match prefixes or suffixes", () => {
    expect(build().match("GET", "/v1/jobsx")).toBeUndefined();
    expect(build().match("GET", "/api/v1/jobs")).toBeUndefined();
    expect(build().match("GET", "/health/extra")).toBeUndefined();
  });

  it("lists the allowed methods when the path exists but the method does not", () => {
    expect(build().match("DELETE", "/v1/jobs")).toEqual({ allowed: ["GET", "POST"] });
    expect(build().match("GET", "/v1/schedules/s1")).toEqual({ allowed: ["PATCH", "DELETE"] });
  });

  it("returns undefined for unknown paths", () => {
    expect(build().match("GET", "/nope")).toBeUndefined();
  });

  it("keeps routes without a scope public", () => {
    const m = build().match("GET", "/health");
    expect(m && "route" in m && m.route.scope).toBeUndefined();
  });

  // Previously decodeURIComponent's URIError escaped, and the server answered 500.
  it("rejects malformed percent-encoding in a param as a validation error", () => {
    expect(() => build().match("GET", "/v1/jobs/%E0%A4%A")).toThrow(ValidationError);
    expect(() => build().match("GET", "/v1/jobs/%")).toThrow(ValidationError);
  });
});
