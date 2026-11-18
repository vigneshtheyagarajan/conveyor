import { request } from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import { memoryLogger } from "../helpers.js";
import { errorBody, startApi, type Api } from "./api-harness.js";

let api: Api;
afterEach(async () => {
  await api?.close();
});

/** Send a raw request with node:http (fetch can't stream a body in pieces or skip content-length). */
function rawPost(base: string, path: string, chunks: string[], headers: Record<string, string>) {
  return new Promise<{ status: number; body: string }>((resolve, reject) => {
    const url = new URL(path, base);
    const r = request(url, { method: "POST", headers }, (res) => {
      let body = "";
      res.on("data", (c) => (body += c));
      res.on("end", () => resolve({ status: res.statusCode!, body }));
    });
    r.on("error", reject);
    for (const c of chunks) r.write(c);
    r.end();
  });
}

describe("routing", () => {
  it("answers unknown paths with 404 and the error body", async () => {
    api = await startApi();
    const res = await api.req("GET", "/v1/nope");
    expect(res.status).toBe(404);
    expect(res.body).toEqual(errorBody("not_found", "route not found"));
    expect(res.headers.get("content-type")).toBe("application/json; charset=utf-8");
  });

  it.each([
    ["DELETE", "/v1/jobs", "GET, POST"],
    ["PUT", "/v1/jobs/abc", "GET"],
    ["GET", "/v1/jobs/abc/cancel", "POST"],
    ["GET", "/v1/leases", "POST"],
    ["POST", "/v1/queues/q", "GET, PUT"],
    ["PUT", "/v1/schedules/s1", "GET, PATCH, DELETE"],
    ["POST", "/health", "GET"],
  ])("answers %s %s with 405 and Allow: %s", async (method, path, allow) => {
    api = await startApi();
    const res = await api.req(method, path);
    expect(res.status).toBe(405);
    expect(res.headers.get("allow")?.split(", ").sort()).toEqual(allow.split(", ").sort());
    expect(res.body).toEqual(errorBody("method_not_allowed", "method not allowed"));
  });

  it("checks the route before authentication: 404 and 405 don't need a token", async () => {
    api = await startApi({ tokens: { admin: ["admin"] } });
    expect((await api.req("GET", "/v1/nope")).status).toBe(404);
    expect((await api.req("DELETE", "/v1/jobs")).status).toBe(405);
    expect((await api.req("GET", "/v1/jobs")).status).toBe(401);
  });

  it("accepts a trailing slash and ignores the query string when routing", async () => {
    api = await startApi();
    expect((await api.req("GET", "/v1/jobs/")).status).toBe(200);
    expect((await api.req("GET", "/health?x=1")).status).toBe(200);
  });

  it("rejects malformed percent-encoding with 400", async () => {
    api = await startApi();
    const res = await api.req("GET", "/v1/jobs/%E0%A4%A");
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", "malformed percent-encoding in URL path"));
  });
});

describe("health and metrics", () => {
  it("GET /health is public and reports ok", async () => {
    api = await startApi({ tokens: { admin: ["admin"] } });
    const res = await api.req("GET", "/health");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ status: "ok", time: expect.stringMatching(/^\d{4}-\d\d-\d\dT/) });
  });

  it("GET /metrics is public Prometheus text when metrics are enabled", async () => {
    api = await startApi({ tokens: { admin: ["admin"] }, metrics: true });
    await api.conveyor.enqueue({ queue: "q", name: "x" });
    const res = await api.req("GET", "/metrics");
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/plain; version=0.0.4");
    expect(res.text).toContain('conveyor_jobs_enqueued_total{queue="q"} 1');
  });

  it("GET /metrics is 404 when metrics are not enabled", async () => {
    api = await startApi();
    expect((await api.req("GET", "/metrics")).status).toBe(404);
  });
});

describe("request ids", () => {
  it("echoes x-request-id, or generates one, on success and on error", async () => {
    api = await startApi();
    expect((await api.req("GET", "/health", { headers: { "x-request-id": "abc-123" } })).headers.get("x-request-id")).toBe("abc-123");
    expect((await api.req("GET", "/v1/jobs/missing")).headers.get("x-request-id")).toMatch(/^[0-9a-f-]{36}$/);
    expect((await api.req("DELETE", "/v1/jobs", { headers: { "x-request-id": "r1" } })).headers.get("x-request-id")).toBe("r1");
  });
});

describe("request bodies", () => {
  it("rejects invalid JSON with 400", async () => {
    api = await startApi();
    const res = await api.req("POST", "/v1/jobs", { body: "{nope" });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("bad_request", "invalid JSON body"));
  });

  it("rejects a body that is not JSON with 415", async () => {
    api = await startApi();
    const res = await api.req("POST", "/v1/jobs", { body: "queue=q", headers: { "content-type": "application/x-www-form-urlencoded" } });
    expect(res.status).toBe(415);
    expect(res.body).toEqual(errorBody("bad_request", "content-type must be application/json"));
  });

  it("accepts a JSON content type with parameters", async () => {
    api = await startApi();
    const res = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x" }, headers: { "content-type": "application/json; charset=utf-8" } });
    expect(res.status).toBe(201);
  });

  it.each([
    ["no body", undefined],
    ["a JSON array", "[]"],
    ["JSON null", "null"],
    ["a JSON string", '"q"'],
  ])("rejects %s where an object is expected with 400", async (_label, body) => {
    api = await startApi();
    const res = await api.req("POST", "/v1/jobs", { body, headers: body === undefined ? {} : { "content-type": "application/json" } });
    expect(res.status).toBe(400);
    expect(res.body).toEqual(errorBody("validation_error", "body must be an object"));
  });

  it("ignores a body on GET and DELETE", async () => {
    api = await startApi();
    const res = await fetch(`${api.base}/v1/schedules/none`, { method: "DELETE", body: "{nope", headers: { "content-type": "text/plain" } });
    expect(res.status).toBe(404);
  });

  // Previously the server destroyed the request stream, so the client saw a connection reset
  // instead of the 413 response.
  it("answers an oversized body with 413, and creates nothing", async () => {
    api = await startApi({ maxBodyBytes: 1000 });
    const res = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x", payload: "a".repeat(5000) } });
    expect(res.status).toBe(413);
    expect(res.body).toEqual(errorBody("bad_request", "body exceeds 1000 bytes"));
    expect((await api.conveyor.listJobs({})).items).toEqual([]);
  });

  it("answers an oversized chunked body (no content-length) with 413", async () => {
    api = await startApi({ maxBodyBytes: 1000 });
    const chunk = "x".repeat(400);
    const res = await rawPost(api.base, "/v1/jobs", ['{"queue":"q","name":"x","payload":"', chunk, chunk, chunk, chunk, '"}'], {
      "content-type": "application/json",
      "transfer-encoding": "chunked",
    });
    expect(res.status).toBe(413);
    expect(JSON.parse(res.body)).toEqual(errorBody("bad_request", "body exceeds 1000 bytes"));
  });

  it("accepts a body right at the limit", async () => {
    api = await startApi({ maxBodyBytes: 1000 });
    const base = JSON.stringify({ queue: "q", name: "x", payload: "" });
    const body = JSON.stringify({ queue: "q", name: "x", payload: "a".repeat(1000 - base.length) });
    expect(body.length).toBe(1000);
    expect((await api.req("POST", "/v1/jobs", { body })).status).toBe(201);
  });

  it("defaults the body limit to four times the payload limit", async () => {
    api = await startApi({ config: { maxPayloadBytes: 100 } });
    const res = await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x", payload: "a".repeat(450) } });
    expect(res.status).toBe(413);
    expect(res.body.error.message).toBe("body exceeds 400 bytes");
  });

  it("keeps serving requests on the same server after a 413", async () => {
    api = await startApi({ maxBodyBytes: 1000 });
    await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x", payload: "a".repeat(5000) } });
    expect((await api.req("POST", "/v1/jobs", { body: { queue: "q", name: "x" } })).status).toBe(201);
  });
});

describe("unexpected errors", () => {
  it("answer 500 with a generic body and log the details", async () => {
    const { logger, records } = memoryLogger();
    api = await startApi({ logger });
    vi.spyOn(api.conveyor.store, "getJob").mockRejectedValueOnce(new Error("disk I/O error"));
    const res = await api.req("GET", "/v1/jobs/abc", { headers: { "x-request-id": "req-9" } });
    expect(res.status).toBe(500);
    expect(res.body).toEqual(errorBody("internal", "internal server error"));
    expect(res.text).not.toContain("disk I/O error");
    expect(records).toContainEqual(
      expect.objectContaining({ level: "error", msg: "unhandled error", requestId: "req-9", path: "/v1/jobs/abc", err: expect.objectContaining({ message: "disk I/O error" }) }),
    );
  });
});
