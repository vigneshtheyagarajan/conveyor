// createRuntime() is what `conveyor serve` runs. These tests start it for real: a temporary
// database file and an HTTP server on 127.0.0.1 with a random port. No sleeps.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createRuntime, type Runtime } from "../../src/runtime.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import { tempDir } from "../helpers.js";

describe("createRuntime", () => {
  let tmp: ReturnType<typeof tempDir>;
  let runtime: Runtime | undefined;

  beforeEach(() => {
    tmp = tempDir();
    vi.stubEnv("CONVEYOR_DB", "");
    vi.stubEnv("CONVEYOR_PORT", "");
  });

  afterEach(async () => {
    if (runtime?.conveyor.store && (runtime.conveyor.store as SqliteStore).db.isOpen) await runtime.stop();
    runtime = undefined;
    vi.unstubAllEnvs();
    tmp.cleanup();
  });

  async function started(overrides = {}) {
    runtime = await createRuntime({ dbPath: tmp.file(), port: 0, logLevel: "silent", ...overrides });
    const listen = vi.spyOn(runtime.api, "listen");
    await runtime.start();
    const addr = await listen.mock.results[0]!.value;
    return { runtime, base: `http://127.0.0.1:${addr.port}` };
  }

  it("creates and migrates the database and wires everything to it", async () => {
    runtime = await createRuntime({ dbPath: tmp.file(), port: 0, logLevel: "silent" });
    expect(runtime.config).toMatchObject({ dbPath: tmp.file(), port: 0, logLevel: "silent" });
    expect(runtime.broker.conveyor).toBe(runtime.conveyor);
    const job = await runtime.conveyor.enqueue({ queue: "q", name: "x" });
    await runtime.stop();
    runtime = undefined;

    const store = new SqliteStore(tmp.file());
    try {
      expect((await store.getJob(job.id))?.name).toBe("x");
    } finally {
      await store.close();
    }
  });

  it("reads configuration from the environment, with overrides winning", async () => {
    vi.stubEnv("CONVEYOR_LEASE_MS", "45000");
    vi.stubEnv("CONVEYOR_RETENTION_MS", "1000");
    runtime = await createRuntime({ dbPath: tmp.file(), port: 0, logLevel: "silent", retentionMs: 5 });
    expect(runtime.config).toMatchObject({ leaseMs: 45_000, retentionMs: 5 });
    expect(runtime.broker.leaseMs).toBe(45_000);
  });

  it("serves the API, health and metrics once started", async () => {
    const { base } = await started();
    expect((await fetch(`${base}/health`)).status).toBe(200);

    const res = await fetch(`${base}/v1/jobs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ queue: "emails", name: "send" }),
    });
    expect(res.status).toBe(201);

    const metrics = await (await fetch(`${base}/metrics`)).text();
    expect(metrics).toContain('conveyor_jobs_enqueued_total{queue="emails"} 1');
    expect(metrics).toContain('conveyor_queue_jobs{queue="emails",state="waiting"} 1');
  });

  it("starts the scheduler, maintenance and webhooks, and stops them all", async () => {
    runtime = await createRuntime({ dbPath: tmp.file(), port: 0, logLevel: "silent" });
    const spies = {
      schedulerStart: vi.spyOn(runtime.scheduler, "start"),
      maintenanceStart: vi.spyOn(runtime.maintenance, "start"),
      webhooksStart: vi.spyOn(runtime.webhooks, "start"),
      schedulerStop: vi.spyOn(runtime.scheduler, "stop"),
      maintenanceStop: vi.spyOn(runtime.maintenance, "stop"),
      webhooksStop: vi.spyOn(runtime.webhooks, "stop"),
      apiClose: vi.spyOn(runtime.api, "close"),
    };
    await runtime.start();
    expect(spies.schedulerStart).toHaveBeenCalledOnce();
    expect(spies.maintenanceStart).toHaveBeenCalledOnce();
    expect(spies.webhooksStart).toHaveBeenCalledOnce();

    await runtime.stop();
    expect(spies.schedulerStop).toHaveBeenCalledOnce();
    expect(spies.maintenanceStop).toHaveBeenCalledOnce();
    expect(spies.webhooksStop).toHaveBeenCalledOnce();
    expect(spies.apiClose).toHaveBeenCalledOnce();
    expect((runtime.conveyor.store as SqliteStore).db.isOpen).toBe(false);
  });

  it("stops accepting connections after stop()", async () => {
    const { runtime: rt, base } = await started();
    await rt.stop();
    await expect(fetch(`${base}/health`)).rejects.toThrow();
  });

  it("fails to start with invalid configuration", async () => {
    vi.stubEnv("CONVEYOR_LOG_LEVEL", "loud");
    await expect(createRuntime({ dbPath: tmp.file() })).rejects.toThrow("invalid CONVEYOR_LOG_LEVEL: loud");
  });
});
