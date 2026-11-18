// The CLI's main() in-process, against a temporary database file. The long-running commands
// (serve, worker) are tested end to end in cli-e2e.test.ts.
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { main } from "../../src/cli/index.js";
import { SqliteStore } from "../../src/storage/sqlite-store.js";
import { tempDir } from "../helpers.js";

let tmp: ReturnType<typeof tempDir>;
let db: string;

beforeEach(() => {
  tmp = tempDir();
  db = tmp.file();
  vi.stubEnv("CONVEYOR_DB", "");
  vi.stubEnv("CONVEYOR_LOG_LEVEL", "");
});

afterEach(() => {
  vi.unstubAllEnvs();
  tmp.cleanup();
});

/** Run the CLI with --db pointing at the temp database. */
async function cli(...args: string[]) {
  const out: string[] = [];
  const err: string[] = [];
  const o = vi.spyOn(process.stdout, "write").mockImplementation((c) => (out.push(String(c)), true));
  const e = vi.spyOn(process.stderr, "write").mockImplementation((c) => (err.push(String(c)), true));
  try {
    const code = await main([...args, "--db", db]);
    return { code, stdout: out.join(""), stderr: err.join("") };
  } finally {
    o.mockRestore();
    e.mockRestore();
  }
}

/** Look at the database the CLI wrote. */
async function withStore<T>(fn: (store: SqliteStore) => Promise<T>): Promise<T> {
  const store = new SqliteStore(db);
  try {
    await store.migrate();
    return await fn(store);
  } finally {
    await store.close();
  }
}

const jobs = () => withStore(async (s) => (await s.listJobs({ limit: 500 })).items);

describe("help and usage errors", () => {
  // Previously `conveyor --help` printed the help and then exited with 1.
  it("--help prints usage and exits 0", async () => {
    const res = await cli("--help");
    expect(res.code).toBe(0);
    expect(res.stdout).toContain("Usage: conveyor <command> [options]");
    expect(res.stderr).toBe("");
    expect((await cli("enqueue", "--help")).code).toBe(0);
    expect((await cli("-h")).code).toBe(0);
  });

  it("no command prints usage and exits 1", async () => {
    const res = await main([]);
    expect(res).toBe(1);
  });

  it("an unknown command exits 1 with an error and a hint", async () => {
    const res = await cli("frobnicate");
    expect(res.code).toBe(1);
    expect(res.stderr).toBe("error: unknown command frobnicate\nrun `conveyor --help` for usage\n");
    expect(res.stdout).toBe("");
  });

  it("an unknown option exits 1 with an error", async () => {
    const res = await cli("jobs", "--bogus");
    expect(res.code).toBe(1);
    expect(res.stderr).toMatch(/^error: Unknown option '--bogus'/);
  });

  it.each([
    [["enqueue"], "queue is required"],
    [["enqueue", "q"], "job name is required"],
    [["job"], "job id is required"],
    [["pause"], "queue is required"],
    [["queue-set"], "queue is required"],
    [["schedule-add", "nightly", "--queue", "q", "--job", "j"], "--cron is required"],
    [["token-create", "ops"], "--scopes is required"],
    [["worker", "--queues", "q"], "--handlers is required"],
  ])("%j exits 1: %s", async (args, message) => {
    const res = await cli(...args);
    expect(res.code).toBe(1);
    expect(res.stderr).toBe(`error: ${message}\nrun \`conveyor --help\` for usage\n`);
  });
});

describe("number options", () => {
  // Previously these were passed through Number(): "abc" became NaN and "" became 0, giving
  // confusing validation errors, SQLite errors, silently wrong values, or (for the worker)
  // a worker with no job loops that did nothing.
  it.each([
    [["enqueue", "q", "x", "--delay", "abc"], "--delay"],
    [["enqueue", "q", "x", "--delay", ""], "--delay"],
    [["enqueue", "q", "x", "--priority", "high"], "--priority"],
    [["enqueue", "q", "x", "--priority", ""], "--priority"],
    [["enqueue", "q", "x", "--priority", "1.5"], "--priority"],
    [["jobs", "--limit", "abc"], "--limit"],
    [["dlq", "q", "--limit", "ten"], "--limit"],
    [["queue-set", "q", "--concurrency", "abc"], "--concurrency"],
    [["queue-set", "q", "--rate-max", "x"], "--rate-max"],
    [["queue-set", "q", "--rate-interval", "1s"], "--rate-interval"],
    [["queue-set", "q", "--max-attempts", "many"], "--max-attempts"],
    [["serve", "--port", "http"], "--port"],
  ])("%j is rejected with a clear message", async (args, flag) => {
    const res = await cli(...args);
    expect(res.code).toBe(1);
    expect(res.stderr).toMatch(new RegExp(`^error: ${flag} must be an integer, got "[^"]*"\\n`));
    expect(await jobs()).toEqual([]);
    expect(await withStore((s) => s.getQueue("q"))).toBeUndefined();
  });

  it("rejects a non-numeric worker concurrency instead of starting a worker that does nothing", async () => {
    const handlers = fileURLToPath(new URL("../fixtures/cli-handlers.mjs", import.meta.url));
    const running = cli("worker", "--handlers", handlers, "--queues", "q", "--concurrency", "abc");
    const settled = await Promise.race([running, new Promise((r) => setTimeout(() => r("still running"), 2000))]);
    if (settled === "still running") {
      process.emit("SIGINT"); // stop the worker the old code started
      await running;
    }
    expect(settled).toMatchObject({ code: 1, stderr: expect.stringMatching(/^error: --concurrency must be an integer, got "abc"/) });
  });

  it.each([
    [["jobs", "--limit", "0"], "error: --limit must be >= 1, got 0"],
    [["enqueue", "q", "x", "--delay=-5"], "error: --delay must be >= 0, got -5"],
    [["queue-set", "q", "--max-attempts", "0"], "error: --max-attempts must be >= 1, got 0"],
    [["serve", "--port", "70000"], "error: --port must be <= 65535, got 70000"],
    [["worker", "--queues", "q", "--handlers", "x.mjs", "--concurrency", "0"], "error: --concurrency must be >= 1, got 0"],
  ])("%j is out of range", async (args, message) => {
    const res = await cli(...args);
    expect(res.code).toBe(1);
    expect(res.stderr.split("\n")[0]).toBe(message);
  });

  it("rejects a delay over 100 years without writing anything", async () => {
    const res = await cli("enqueue", "q", "x", "--delay", "99999999999999999999");
    expect(res).toEqual({ code: 1, stdout: "", stderr: "error: delayMs must be at most 3155760000000 (100 years)\n" });
    expect(await jobs()).toEqual([]);
  });

  it("accepts negative priorities (written --priority=-5, as parseArgs requires) and zero", async () => {
    expect((await cli("enqueue", "q", "x", "--priority=-5", "--delay", "0")).code).toBe(0);
    expect((await jobs())[0]).toMatchObject({ priority: -5, state: "waiting" });
  });
});

describe("migrate", () => {
  it("creates the database", async () => {
    const res = await cli("migrate");
    expect(res).toEqual({ code: 0, stdout: `database ready at ${db}\n`, stderr: "" });
    expect(await withStore(async (s) => s.db.prepare("PRAGMA user_version").get())).toEqual({ user_version: 7 });
  });

  it("uses CONVEYOR_DB when --db is not given", async () => {
    vi.stubEnv("CONVEYOR_DB", db);
    const o = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    try {
      expect(await main(["migrate"])).toBe(0);
      expect(String(o.mock.calls[0]![0])).toBe(`database ready at ${db}\n`);
    } finally {
      o.mockRestore();
    }
  });

  it("refuses a database from a newer version", async () => {
    await withStore(async (s) => s.db.exec("PRAGMA user_version = 99"));
    const res = await cli("migrate");
    expect(res.code).toBe(1);
    expect(res.stderr).toMatch(/^error: database schema version 99 is newer than this version of Conveyor supports \(7\)/);
  });
});

describe("enqueue", () => {
  it("enqueues a job and prints its id", async () => {
    const res = await cli("enqueue", "emails", "send", "--payload", '{"to":"a@example.test"}', "--priority", "3");
    expect(res.code).toBe(0);
    const [job] = await jobs();
    expect(res.stdout).toBe(`enqueued ${job!.id} (waiting)\n`);
    expect(job).toMatchObject({ queue: "emails", name: "send", payload: { to: "a@example.test" }, priority: 3 });
  });

  it("delays, de-duplicates, and prints JSON with --json", async () => {
    const first = await cli("enqueue", "q", "x", "--delay", "60000", "--unique", "k", "--json");
    expect(first.code).toBe(0);
    const job = JSON.parse(first.stdout);
    expect(job).toMatchObject({ queue: "q", name: "x", state: "delayed", uniqueKey: "k" });
    const again = JSON.parse((await cli("enqueue", "q", "x", "--unique", "k", "--json")).stdout);
    expect(again.id).toBe(job.id);
    expect(await jobs()).toHaveLength(1);
  });

  it("reports invalid input", async () => {
    const badJson = await cli("enqueue", "q", "x", "--payload", "{oops");
    expect(badJson.code).toBe(1);
    expect(badJson.stderr).toMatch(/^error: .*JSON/);
    const badName = await cli("enqueue", "q", "bad name");
    expect(badName).toMatchObject({ code: 1, stderr: "error: invalid job name\n" });
    expect(await jobs()).toEqual([]);
  });
});

describe("jobs, job, cancel, retry", () => {
  it("lists jobs as a table or JSON, with filters and a limit", async () => {
    await cli("enqueue", "a", "x");
    await cli("enqueue", "b", "y");
    const table = await cli("jobs");
    expect(table.code).toBe(0);
    const lines = table.stdout.trimEnd().split("\n");
    expect(lines[0]).toMatch(/^ID\s+QUEUE\s+NAME\s+STATE\s+ATTEMPTS\s+RUNAT\s+LASTERROR$/);
    expect(lines).toHaveLength(3);
    expect(lines[1]).toMatch(/\bb\s+y\s+waiting\s+0\s+\d{4}-\d\d-\d\dT[\d:.]+Z\s+-$/);

    const json = JSON.parse((await cli("jobs", "--json", "--queue", "a")).stdout);
    expect(json.items.map((j: { queue: string }) => j.queue)).toEqual(["a"]);
    expect(JSON.parse((await cli("jobs", "--json", "--limit", "1")).stdout).items).toHaveLength(1);
    expect(JSON.parse((await cli("jobs", "--json", "--state", "dead")).stdout).items).toEqual([]);
    expect((await cli("jobs", "--queue", "none")).stdout).toBe("(none)\n");
  });

  // Regression (was KB-26): an unknown --state went straight to the query, so a typo such as
  // "--state daed" silently listed nothing.
  it("rejects an unknown --state, listing the valid ones", async () => {
    await cli("enqueue", "q", "x");
    for (const args of [["jobs", "--state", "daed"], ["jobs", "--state", "daed", "--json"], ["jobs", "--state", ""]]) {
      const res = await cli(...args);
      expect(res).toEqual({
        code: 1,
        stdout: "",
        stderr: `error: --state must be one of delayed, waiting, active, completed, dead, cancelled; got "${args[2]}"\nrun \`conveyor --help\` for usage\n`,
      });
    }
    expect((await cli("jobs", "--state", "waiting", "--json")).code).toBe(0);
  });

  it("shows one job with its history", async () => {
    await cli("enqueue", "q", "x");
    const [job] = await jobs();
    const res = await cli("job", job!.id);
    expect(res.code).toBe(0);
    expect(res.stdout).toContain(`"id": "${job!.id}"`);
    expect(res.stdout).toMatch(/AT\s+TYPE\s+DATA\n\S+\s+enqueued\s+\{"state":"waiting"\}/);
    const json = JSON.parse((await cli("job", job!.id, "--json")).stdout);
    expect(json.job.id).toBe(job!.id);
    expect(json.events.map((e: { type: string }) => e.type)).toEqual(["enqueued"]);
    expect(await cli("job", "nope")).toMatchObject({ code: 1, stderr: "error: job nope not found\n" });
  });

  it("cancels and retries with --json", async () => {
    await cli("enqueue", "q", "x");
    const [job] = await jobs();
    expect(await cli("retry", job!.id, "--json")).toMatchObject({ code: 1, stderr: "error: only dead or cancelled jobs can be retried (job is waiting)\n" });
    expect(JSON.parse((await cli("cancel", job!.id, "--json")).stdout)).toMatchObject({ id: job!.id, state: "cancelled" });
    expect(await cli("cancel", job!.id, "--json")).toMatchObject({ code: 1, stderr: `error: job ${job!.id} is already cancelled\n` });
    expect(JSON.parse((await cli("retry", job!.id, "--json")).stdout)).toMatchObject({ id: job!.id, state: "waiting" });
    expect(await cli("cancel", "nope", "--json")).toMatchObject({ code: 1, stderr: "error: job nope not found\n" });
  });

  // Regression (was KB-27): without --json, `cancel` and `retry` never called Conveyor: the
  // code was `print(json ? await conveyor.cancel(id) : "cancelled")`, so they printed success,
  // exited 0 and changed nothing, even with no id at all.
  it("cancel <id> cancels the job", async () => {
    await cli("enqueue", "q", "x");
    const [job] = await jobs();
    expect(await cli("cancel", job!.id)).toEqual({ code: 0, stdout: "cancelled\n", stderr: "" });
    expect((await jobs())[0]!.state).toBe("cancelled");
  });

  it("retry <id> requeues the job", async () => {
    await cli("enqueue", "q", "x");
    const [job] = await jobs();
    await withStore((s) => s.cancelJob(job!.id, Date.now()));
    expect((await jobs())[0]!.state).toBe("cancelled");
    expect(await cli("retry", job!.id)).toEqual({ code: 0, stdout: "requeued\n", stderr: "" });
    expect((await jobs())[0]).toMatchObject({ state: "waiting", attempts: 0 });
  });

  it.each([[[] as string[]], [["--json"]]])("cancel and retry exit 1 when nothing changed (extra args: %j)", async (extra) => {
    await cli("enqueue", "q", "x");
    const [job] = await jobs();
    const before = await jobs();
    for (const [args, stderr] of [
      [["cancel", "nope"], "error: job nope not found\n"],
      [["retry", "nope"], "error: job nope not found\n"],
      [["retry", job!.id], "error: only dead or cancelled jobs can be retried (job is waiting)\n"],
    ] as const) {
      expect(await cli(...args, ...extra)).toEqual({ code: 1, stdout: "", stderr });
    }
    expect(await jobs()).toEqual(before);
    await cli("cancel", job!.id, ...extra);
    expect(await cli("cancel", job!.id, ...extra)).toEqual({ code: 1, stdout: "", stderr: `error: job ${job!.id} is already cancelled\n` });
  });

  it("cancel and retry without an id are usage errors", async () => {
    for (const cmd of ["cancel", "retry"]) {
      expect(await cli(cmd)).toEqual({ code: 1, stdout: "", stderr: "error: job id is required\nrun `conveyor --help` for usage\n" });
    }
  });
});

describe("queues", () => {
  it("lists queues with counts", async () => {
    await cli("enqueue", "emails", "send");
    await cli("queue-set", "emails", "--concurrency", "2");
    const res = await cli("queues");
    expect(res.code).toBe(0);
    const lines = res.stdout.trimEnd().split("\n");
    expect(lines[0]).toMatch(/^NAME\s+PAUSED\s+WAITING\s+DELAYED\s+ACTIVE\s+COMPLETED\s+DEAD\s+CONCURRENCY$/);
    expect(lines[1]).toMatch(/^emails\s+false\s+1\s+0\s+0\s+0\s+0\s+2$/);
    expect(JSON.parse((await cli("queues", "--json")).stdout)[0]).toMatchObject({ name: "emails", counts: { waiting: 1 } });
  });

  it("pauses, resumes and changes settings", async () => {
    expect(await cli("pause", "q")).toMatchObject({ code: 0, stdout: "paused\n" });
    expect((await withStore((s) => s.getQueue("q")))?.paused).toBe(true);
    expect(await cli("resume", "q")).toMatchObject({ code: 0, stdout: "resumed\n" });
    const set = await cli("queue-set", "q", "--rate-max", "10", "--rate-interval", "1000", "--max-attempts", "5");
    expect(set.code).toBe(0);
    expect(JSON.parse(set.stdout)).toMatchObject({ name: "q", paused: false, rateLimitMax: 10, rateLimitIntervalMs: 1000, defaultMaxAttempts: 5 });
    expect(await cli("queue-set", "q", "--rate-max", "10", "--rate-interval", "0")).toMatchObject({
      code: 1,
      stderr: "error: rateLimitIntervalMs is required when rateLimitMax is set\n",
    });
  });

  it("lists and retries dead jobs", async () => {
    await withStore(async (s) => {
      await s.insertJob({
        id: "dead-1", queue: "q", name: "x", payload: null, state: "dead", priority: 0, attempts: 3, maxAttempts: 3, backoff: null,
        timeoutMs: 0, runAt: 0, createdAt: 0, updatedAt: 0, startedAt: 0, finishedAt: 1, workerId: null, leaseToken: null,
        leaseExpiresAt: null, progress: 0, result: null, lastError: "boom", uniqueKey: null, scheduleId: null,
      });
    });
    const list = await cli("dlq", "q");
    expect(list.code).toBe(0);
    expect(list.stdout).toMatch(/dead-1\s+x\s+3\s+\S+\s+boom/);
    expect(await cli("dlq", "q", "--retry")).toMatchObject({ code: 0, stdout: "retried 1 jobs\n" });
    expect((await jobs())[0]).toMatchObject({ state: "waiting", attempts: 0 });
  });
});

describe("schedules", () => {
  it("adds, lists and removes schedules", async () => {
    const add = await cli("schedule-add", "nightly", "--queue", "q", "--job", "report", "--cron", "0 3 * * *", "--tz", "Europe/London", "--payload", '{"a":1}', "--skip-overlap");
    expect(add.code).toBe(0);
    expect(add.stdout).toMatch(/^created [0-9a-f-]{36}; next run \d{4}-\d\d-\d\dT03:00:00\.000Z\n$/);
    const [s] = await withStore((st) => st.listSchedules());
    expect(s).toMatchObject({ name: "nightly", queue: "q", jobName: "report", timezone: "Europe/London", overlap: "skip", payload: { a: 1 } });

    const list = await cli("schedules");
    expect(list.stdout).toMatch(/nightly\s+0 3 \* \* \*\s+Europe\/London\s+q\s+report\s+true/);
    expect(JSON.parse((await cli("schedules", "--json")).stdout)).toHaveLength(1);

    expect(await cli("schedule-rm", s!.id)).toMatchObject({ code: 0, stdout: "removed\n" });
    expect(await cli("schedule-rm", s!.id)).toMatchObject({ code: 1, stderr: `error: schedule ${s!.id} not found\n` });
  });

  it("rejects a bad schedule", async () => {
    expect(await cli("schedule-add", "n", "--queue", "q", "--job", "j", "--cron", "0 0 30 2 *")).toMatchObject({
      code: 1,
      stderr: 'error: cron expression "0 0 30 2 *" never runs\n',
    });
    expect(await cli("schedule-add", "n", "--queue", "q", "--job", "j", "--cron", "* * * * *", "--tz", "Mars/Base")).toMatchObject({
      code: 1,
      stderr: "error: unknown timezone Mars/Base\n",
    });
  });
});

describe("tokens", () => {
  it("creates, lists and revokes tokens without ever listing secrets or hashes", async () => {
    const created = await cli("token-create", "ops", "--scopes", "jobs:read,admin");
    expect(created.code).toBe(0);
    const [, id, secret] = created.stdout.match(/^token (\S+)\nsecret (cvy_\S+)\n\(store the secret now; it is not shown again\)\n$/)!;
    const json = JSON.parse((await cli("token-create", "ci", "--scopes", "workers", "--json")).stdout);
    expect(json).toEqual({ id: expect.any(String), secret: expect.stringMatching(/^cvy_/) });

    const list = await cli("tokens");
    expect(list.code).toBe(0);
    expect(list.stdout).toMatch(/ops\s+\["jobs:read","admin"\]/);
    expect(list.stdout).not.toContain(secret);
    const listed = JSON.parse((await cli("tokens", "--json")).stdout);
    expect(listed.map((t: { name: string }) => t.name)).toEqual(["ops", "ci"]);
    for (const t of listed) expect(t).not.toHaveProperty("hash");

    expect(await cli("token-revoke", id!)).toMatchObject({ code: 0, stdout: "revoked\n" });
    expect((await withStore((s) => s.listTokens()))[0]!.revokedAt).toEqual(expect.any(Number));
  });

  // Previously an unknown (or already revoked) id printed "revoked" and exited 0.
  it("reports revoking an unknown or already-revoked token as an error", async () => {
    expect(await cli("token-revoke", "nope")).toMatchObject({ code: 1, stdout: "", stderr: "error: token nope not found\n" });
    const { id } = JSON.parse((await cli("token-create", "ops", "--scopes", "admin", "--json")).stdout);
    await cli("token-revoke", id);
    expect(await cli("token-revoke", id)).toMatchObject({ code: 1, stderr: `error: token ${id} not found\n` });
  });

  it("rejects an unknown scope", async () => {
    expect(await cli("token-create", "ops", "--scopes", "root")).toMatchObject({ code: 1, stderr: "error: unknown scope root\n" });
  });
});
