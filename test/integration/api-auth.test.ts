import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Scope } from "../../src/core/types.js";
import { errorBody, startApi, type Api } from "./api-harness.js";

// Every route and the scope it needs, as documented in the README.
const ROUTES: [method: string, pattern: string, scope: Scope | null][] = [
  ["GET", "/health", null],
  ["GET", "/metrics", null],
  ["POST", "/v1/jobs", "jobs:write"],
  ["POST", "/v1/jobs/bulk", "jobs:write"],
  ["GET", "/v1/jobs", "jobs:read"],
  ["GET", "/v1/jobs/:id", "jobs:read"],
  ["GET", "/v1/jobs/:id/events", "jobs:read"],
  ["GET", "/v1/jobs/:id/dependencies", "jobs:read"],
  ["GET", "/v1/jobs/:id/dependents", "jobs:read"],
  ["POST", "/v1/flows", "jobs:write"],
  ["GET", "/v1/flows/:id", "jobs:read"],
  ["POST", "/v1/flows/:id/cancel", "jobs:write"],
  ["POST", "/v1/jobs/:id/cancel", "jobs:write"],
  ["POST", "/v1/jobs/:id/retry", "jobs:write"],
  ["GET", "/v1/queues", "jobs:read"],
  ["GET", "/v1/queues/:name", "jobs:read"],
  ["PUT", "/v1/queues/:name", "admin"],
  ["POST", "/v1/queues/:name/pause", "admin"],
  ["POST", "/v1/queues/:name/resume", "admin"],
  ["POST", "/v1/queues/:name/retry-dead", "jobs:write"],
  ["POST", "/v1/leases", "workers"],
  ["POST", "/v1/jobs/:id/heartbeat", "workers"],
  ["POST", "/v1/jobs/:id/progress", "workers"],
  ["POST", "/v1/jobs/:id/complete", "workers"],
  ["POST", "/v1/jobs/:id/fail", "workers"],
  ["POST", "/v1/jobs/:id/release", "workers"],
  ["GET", "/v1/schedules", "jobs:read"],
  ["POST", "/v1/schedules", "admin"],
  ["GET", "/v1/schedules/:id", "jobs:read"],
  ["PATCH", "/v1/schedules/:id", "admin"],
  ["DELETE", "/v1/schedules/:id", "admin"],
  ["POST", "/v1/schedules/:id/trigger", "jobs:write"],
  ["GET", "/v1/webhooks", "admin"],
  ["POST", "/v1/webhooks", "admin"],
  ["DELETE", "/v1/webhooks/:id", "admin"],
  ["GET", "/v1/webhooks/:id/deliveries", "admin"],
  ["GET", "/v1/tokens", "admin"],
  ["POST", "/v1/tokens", "admin"],
  ["DELETE", "/v1/tokens/:id", "admin"],
];

const SCOPED = ROUTES.filter((r): r is [string, string, Scope] => r[2] !== null);
const concrete = (pattern: string) => pattern.replace(/:\w+/g, "nope");
/** A token that has some scope, but not this one (and not admin). */
const otherScope = (scope: Scope): string => (scope === "jobs:read" ? "writer" : "reader");

let api: Api;
beforeAll(async () => {
  api = await startApi({
    metrics: true,
    tokens: { admin: ["admin"], reader: ["jobs:read"], writer: ["jobs:write"], worker: ["workers"], revoked: ["admin"] },
  });
  const revoked = (await api.store.listTokens()).find((t) => t.name === "revoked")!;
  await api.store.revokeToken(revoked.id, api.clock.now());
});
afterAll(async () => {
  await api.close();
});

describe("route table", () => {
  it("covers exactly the routes the server registers", () => {
    const registered = api.api.router.routes.map((r) => [r.method, r.pattern, r.scope ?? null]);
    expect(registered.sort()).toEqual([...ROUTES].sort());
  });
});

describe("public routes", () => {
  it.each(ROUTES.filter((r) => r[2] === null))("%s %s needs no token", async (method, pattern) => {
    expect((await api.req(method, concrete(pattern))).status).toBe(200);
  });
});

describe.each(SCOPED)("%s %s (%s)", (method, pattern, scope) => {
  const path = concrete(pattern);

  it("is 401 without a token", async () => {
    const res = await api.req(method, path);
    expect(res.status).toBe(401);
    expect(res.body).toEqual(errorBody("unauthorized", "missing or invalid API token"));
  });

  it("is 401 with an unknown, revoked or malformed token", async () => {
    expect((await api.req(method, path, { token: "cvy_not-a-real-token" })).status).toBe(401);
    expect((await api.req(method, path, { as: "revoked" })).status).toBe(401);
    expect((await api.req(method, path, { headers: { authorization: `Token ${api.secrets.admin}` } })).status).toBe(401);
  });

  if (scope !== "admin") {
    it("is 403 for a token without the scope", async () => {
      const res = await api.req(method, path, { as: otherScope(scope) });
      expect(res.status).toBe(403);
      expect(res.body).toEqual(errorBody("forbidden", `token lacks scope ${scope}`));
    });

    it("is allowed for a token with the scope", async () => {
      const as = { "jobs:read": "reader", "jobs:write": "writer", workers: "worker" }[scope];
      expect([401, 403]).not.toContain((await api.req(method, path, { as, body: {} })).status);
    });
  } else {
    it("is 403 for every non-admin token", async () => {
      for (const as of ["reader", "writer", "worker"]) {
        const res = await api.req(method, path, { as });
        expect(res.status).toBe(403);
        expect(res.body).toEqual(errorBody("forbidden", "token lacks scope admin"));
      }
    });
  }

  it("is allowed for an admin token", async () => {
    expect([401, 403]).not.toContain((await api.req(method, path, { as: "admin", body: {} })).status);
  });
});

describe("when no token exists yet", () => {
  it("every route is open", async () => {
    const open = await startApi();
    try {
      for (const [method, pattern] of SCOPED) {
        expect([401, 403], `${method} ${pattern}`).not.toContain((await open.req(method, concrete(pattern), { body: {} })).status);
      }
    } finally {
      await open.close();
    }
  });

  it("creating the first token through the API turns authentication on", async () => {
    const open = await startApi();
    try {
      const created = await open.req("POST", "/v1/tokens", { body: { name: "first", scopes: ["admin"] } });
      expect(created.status).toBe(201);
      // The "do tokens exist" answer is cached for up to 2 seconds (see KB-24), so check with the token.
      expect((await open.req("GET", "/v1/tokens", { token: created.body.secret })).status).toBe(200);
    } finally {
      await open.close();
    }
  });

  // BUG(KB-24): tokensExist() caches "no tokens" for 2 seconds and creating a token does not
  // clear that cache, so right after the first token is created the API stays open.
  it.fails("requires the new token immediately after the first one is created [KB-24]", async () => {
    const open = await startApi();
    try {
      await open.req("GET", "/v1/tokens"); // the server has now seen "no tokens"
      await open.req("POST", "/v1/tokens", { body: { name: "first", scopes: ["admin"] } });
      expect((await open.req("GET", "/v1/tokens")).status).toBe(401);
    } finally {
      await open.close();
    }
  });
});

describe("token use", () => {
  it("records when a token was last used", async () => {
    const fresh = await startApi({ tokens: { worker: ["workers"] } });
    try {
      expect((await fresh.store.listTokens())[0]!.lastUsedAt).toBeNull();
      await fresh.req("POST", "/v1/leases", { as: "worker", body: { queues: ["q"], workerId: "w" } });
      expect((await fresh.store.listTokens())[0]!.lastUsedAt).toEqual(expect.any(Number));
    } finally {
      await fresh.close();
    }
  });
});
