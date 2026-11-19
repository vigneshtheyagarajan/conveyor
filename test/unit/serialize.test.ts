import { describe, expect, it } from "vitest";
import { jobView, scheduleView, tokenView, webhookView } from "../../src/api/serialize.js";
import type { ApiToken, Job, Schedule, Webhook } from "../../src/core/types.js";

const T = Date.UTC(2026, 0, 5, 12, 0, 0);
const ISO = "2026-01-05T12:00:00.000Z";

const job: Job = {
  id: "j1",
  queue: "q",
  name: "n",
  payload: { a: 1 },
  state: "active",
  priority: 5,
  attempts: 1,
  maxAttempts: 3,
  backoff: { type: "fixed", delayMs: 10 },
  timeoutMs: 0,
  runAt: T,
  createdAt: T,
  updatedAt: T,
  startedAt: T,
  finishedAt: null,
  workerId: "w1",
  leaseToken: "secret-lease-token",
  leaseExpiresAt: T + 30_000,
  progress: 0.5,
  result: null,
  lastError: null,
  uniqueKey: "u",
  flowId: null,
  flowKey: null,
  terminatedBy: null,
  scheduleId: null,
};

describe("jobView", () => {
  it("renders times as ISO strings and keeps nulls", () => {
    const v = jobView(job);
    expect(v.runAt).toBe(ISO);
    expect(v.createdAt).toBe(ISO);
    expect(v.startedAt).toBe(ISO);
    expect(v.finishedAt).toBeNull();
    expect(v.leaseExpiresAt).toBe("2026-01-05T12:00:30.000Z");
  });

  it("never exposes the lease token", () => {
    // Anyone with jobs:read could otherwise complete or fail another worker's job.
    expect(jobView(job)).not.toHaveProperty("leaseToken");
    expect(JSON.stringify(jobView(job))).not.toContain("secret-lease-token");
  });

  it("has exactly the documented fields", () => {
    expect(Object.keys(jobView(job)).sort()).toEqual(
      [
        "id", "queue", "name", "state", "payload", "priority", "attempts", "maxAttempts", "progress",
        "result", "lastError", "uniqueKey", "scheduleId", "timeoutMs", "runAt", "createdAt",
        "startedAt", "finishedAt", "workerId", "leaseExpiresAt", "flowId", "flowKey", "terminatedBy",
      ].sort(),
    );
  });
});

describe("scheduleView", () => {
  it("renders times as ISO strings", () => {
    const s: Schedule = {
      id: "s1", name: "nightly", queue: "q", jobName: "j", payload: null, cron: "0 0 * * *", timezone: "UTC",
      enabled: true, overlap: "allow", nextRunAt: T, lastRunAt: null, lastJobId: null, createdAt: T, updatedAt: T,
    };
    expect(scheduleView(s)).toEqual({ ...s, nextRunAt: ISO, lastRunAt: null, createdAt: ISO, updatedAt: ISO });
  });
});

describe("webhookView", () => {
  const w: Webhook = { id: "w1", url: "https://x.test/h", secret: "whsec_x", events: ["*"], queues: [], enabled: true, createdAt: T };

  it("hides the secret by default", () => {
    expect(webhookView(w)).toEqual({ id: "w1", url: "https://x.test/h", events: ["*"], queues: [], enabled: true, createdAt: ISO });
  });

  it("includes the secret when asked (at creation)", () => {
    expect(webhookView(w, true).secret).toBe("whsec_x");
  });
});

describe("tokenView", () => {
  it("never exposes the hash and renders times as ISO strings", () => {
    const t: ApiToken = { id: "t1", name: "ops", hash: "deadbeef", scopes: ["admin"], createdAt: T, lastUsedAt: null, revokedAt: T };
    expect(tokenView(t)).toEqual({ id: "t1", name: "ops", scopes: ["admin"], createdAt: ISO, lastUsedAt: null, revokedAt: ISO });
  });
});
