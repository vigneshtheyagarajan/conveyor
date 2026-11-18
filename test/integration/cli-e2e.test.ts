// End-to-end tests of the real `conveyor` binary. beforeAll compiles the package (as
// `npm run build` does) into a temp directory; each test runs dist/cli/index.js as a child
// process against a temporary database.
//
// Real processes and real time are needed here: `serve` and `worker` run until they get a
// signal, so these tests wait for log lines, poll the database until a job is done, and
// send SIGTERM. Every wait has a deadline; there are no fixed sleeps.
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { mkdirSync, symlinkSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import { tempDir } from "../helpers.js";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const HANDLERS = fileURLToPath(new URL("../fixtures/cli-handlers.mjs", import.meta.url));

let build: ReturnType<typeof tempDir>;
let bin: string;
let tmp: ReturnType<typeof tempDir>;
let db: string;
const children: ChildProcess[] = [];

beforeAll(() => {
  build = tempDir("conveyor-build-");
  const tsc = createRequire(import.meta.url).resolve("typescript/bin/tsc");
  execFileSync(process.execPath, [tsc, "-p", join(ROOT, "tsconfig.build.json"), "--outDir", join(build.dir, "dist")], { cwd: ROOT });
  bin = join(build.dir, "dist", "cli", "index.js");
}, 120_000);

afterAll(() => {
  build.cleanup();
});

beforeEach(() => {
  tmp = tempDir();
  db = tmp.file();
});

afterEach(() => {
  for (const c of children.splice(0)) if (c.exitCode === null) c.kill("SIGKILL");
  tmp.cleanup();
});

const env = () => {
  const e: NodeJS.ProcessEnv = { ...process.env, CONVEYOR_LOG_LEVEL: "info" };
  for (const k of ["CONVEYOR_DB", "CONVEYOR_PORT", "CONVEYOR_HOST"]) delete e[k];
  return e;
};

/** Node 22 warns that node:sqlite is experimental on every run; that is not our output. */
const ourStderr = (s: string) =>
  s
    .split("\n")
    .filter((l) => !/ExperimentalWarning: SQLite|--trace-warnings/.test(l))
    .join("\n");

function start(args: string[], entry = bin) {
  const child = spawn(process.execPath, [entry, ...args, "--db", db], { env: env(), stdio: ["ignore", "pipe", "pipe"] });
  children.push(child);
  let stdout = "";
  let stderr = "";
  child.stdout!.on("data", (c) => (stdout += c));
  child.stderr!.on("data", (c) => (stderr += c));
  const exited = new Promise<number | null>((resolve) => child.on("exit", (code) => resolve(code)));
  return {
    child,
    exited,
    output: () => ({ stdout, stderr: ourStderr(stderr) }),
    /** Wait until stderr has a JSON log line with this message. */
    async logLine(msg: string, timeoutMs = 10_000): Promise<Record<string, unknown>> {
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        for (const line of stderr.split("\n")) {
          if (!line.startsWith("{")) continue;
          const rec = JSON.parse(line) as Record<string, unknown>;
          if (rec.msg === msg) return rec;
        }
        if (Date.now() > deadline || child.exitCode !== null) throw new Error(`no "${msg}" log line; stderr:\n${stderr}`);
        await new Promise((r) => setTimeout(r, 20));
      }
    },
  };
}

async function run(args: string[], entry = bin) {
  const p = start(args, entry);
  const code = await p.exited;
  return { code, ...p.output() };
}

async function withStore<T>(fn: (store: SqliteStore) => Promise<T>): Promise<T> {
  const store = new SqliteStore(db);
  try {
    await store.migrate();
    return await fn(store);
  } finally {
    await store.close();
  }
}

describe("conveyor (built binary)", () => {
  it("--help prints usage and exits 0", async () => {
    const res = await run(["--help"]);
    expect(res.code).toBe(0);
    expect(res.stdout).toContain("Usage: conveyor <command> [options]");
    expect(res.stderr).toBe("");
  });

  it("migrates, enqueues, lists and cancels against a database file", async () => {
    expect(await run(["migrate"])).toMatchObject({ code: 0, stdout: `database ready at ${db}\n`, stderr: "" });
    const enq = await run(["enqueue", "emails", "send", "--payload", '{"to":"a@example.test"}', "--json"]);
    expect(enq.code).toBe(0);
    const job = JSON.parse(enq.stdout);
    expect(job).toMatchObject({ queue: "emails", name: "send", state: "waiting", payload: { to: "a@example.test" } });

    const list = await run(["jobs", "--json"]);
    expect(JSON.parse(list.stdout).items.map((j: { id: string }) => j.id)).toEqual([job.id]);
    const cancelled = await run(["cancel", job.id, "--json"]);
    expect(JSON.parse(cancelled.stdout)).toMatchObject({ id: job.id, state: "cancelled" });
    expect((await withStore((s) => s.getJob(job.id)))?.state).toBe("cancelled");
  });

  it("exits 1 with a message on stderr for bad input", async () => {
    expect(await run(["frobnicate"])).toEqual({ code: 1, stdout: "", stderr: "error: unknown command frobnicate\nrun `conveyor --help` for usage\n" });
    const nan = await run(["enqueue", "q", "x", "--delay", "abc"]);
    expect(nan.code).toBe(1);
    expect(nan.stderr).toBe('error: --delay must be an integer, got "abc"\nrun `conveyor --help` for usage\n');
    expect(await run(["job", "nope"])).toMatchObject({ code: 1, stderr: "error: job nope not found\n" });
  });

  it("works through an npm-style bin symlink", async () => {
    const binDir = join(tmp.dir, "node_modules", ".bin");
    mkdirSync(binDir, { recursive: true });
    symlinkSync(bin, join(binDir, "conveyor"));
    const res = await run(["migrate"], join(binDir, "conveyor"));
    expect(res).toMatchObject({ code: 0, stdout: `database ready at ${db}\n` });
  });

  it("serve answers HTTP requests and exits 0 on SIGTERM", async () => {
    const server = start(["serve", "--port", "0"]);
    const { url } = (await server.logLine("conveyor listening")) as { url: string };
    expect(url).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);

    expect((await fetch(`${url}/health`)).status).toBe(200);
    const created = await fetch(`${url}/v1/jobs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ queue: "q", name: "x" }),
    });
    expect(created.status).toBe(201);
    const { id } = (await created.json()) as { id: string };

    server.child.kill("SIGTERM");
    expect(await server.exited).toBe(0);
    await server.logLine("shutting down");
    expect((await withStore((s) => s.getJob(id)))?.state).toBe("waiting");
  });

  it("worker runs jobs with the given handlers and exits 0 on SIGTERM", async () => {
    await run(["enqueue", "q", "echo", "--payload", '{"n":7}']);
    const worker = start(["worker", "--handlers", HANDLERS, "--queues", "q", "--concurrency", "2"]);
    await worker.logLine("worker started");
    const deadline = Date.now() + 10_000;
    let job;
    do {
      job = await withStore(async (s) => (await s.listJobs({})).items[0]);
      if (job?.state === "completed" || Date.now() > deadline) break;
      await new Promise((r) => setTimeout(r, 50));
    } while (true);
    expect(job).toMatchObject({ state: "completed", result: { echo: { n: 7 } } });

    worker.child.kill("SIGTERM");
    expect(await worker.exited).toBe(0);
    await worker.logLine("worker stopped");
  });

  // BUG(KB-23): the `worker` command never starts a WebhookDispatcher and events are
  // in-process, so jobs run by `conveyor worker` never produce webhooks (only events raised
  // inside `serve` do). Waits up to 3 seconds for a delivery that never comes.
  it.fails("delivers webhooks for jobs completed by a CLI worker [KB-23]", async () => {
    const received: string[] = [];
    const { createServer } = await import("node:http");
    const receiver = createServer((req, res) => {
      received.push(String(req.headers["conveyor-event"]));
      res.end("ok");
    });
    await new Promise<void>((r) => receiver.listen(0, "127.0.0.1", r));
    try {
      const port = (receiver.address() as { port: number }).port;
      await withStore((s) =>
        s.insertWebhook({ id: "h", url: `http://127.0.0.1:${port}/`, secret: "s", events: ["job.completed"], queues: [], enabled: true, createdAt: 0 }),
      );
      await run(["enqueue", "q", "echo"]);
      const worker = start(["worker", "--handlers", HANDLERS, "--queues", "q"]);
      const deadline = Date.now() + 3000;
      while (!received.includes("job.completed") && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50));
      worker.child.kill("SIGTERM");
      await worker.exited;
      expect(received).toContain("job.completed");
    } finally {
      receiver.close();
    }
  });
});
