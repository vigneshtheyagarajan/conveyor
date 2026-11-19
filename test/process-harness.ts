// Running test fixtures as real OS processes, released at the same moment.
//
// A fixture script (run with `node --import tsx`) does its setup, prints "ready", waits for a
// "go" line on stdin and then does the work under test. `runTogether()` waits until every
// process is ready, releases them all at once and waits for them to exit. Every wait has a
// deadline; call `killAll()` in afterEach so a failed test leaves no process behind.
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const live: ChildProcess[] = [];

export interface Spawned {
  label: string;
  child: ChildProcess;
  ready: Promise<void>;
  exited: Promise<number | null>;
  stdout(): string;
  stderr(): string;
}

/** Start `script` (a TypeScript file) in its own Node process. */
export function spawnTs(script: string, args: string[], label: string): Spawned {
  const child = spawn(process.execPath, ["--import", "tsx", script, ...args], { cwd: ROOT, stdio: ["pipe", "pipe", "pipe"] });
  live.push(child);
  let stdout = "";
  let stderr = "";
  child.stdout!.on("data", (c) => (stdout += c));
  child.stderr!.on("data", (c) => (stderr += c));
  const exited = new Promise<number | null>((resolve) => child.on("exit", (code) => resolve(code)));
  const ready = new Promise<void>((resolve, reject) => {
    child.stdout!.on("data", () => stdout.includes("ready\n") && resolve());
    child.on("exit", (code) => reject(new Error(`${label} exited (${code}) before it was ready:\n${stderr}`)));
  });
  return { label, child, ready, exited, stdout: () => stdout, stderr: () => stderr };
}

/** Wait until every process is ready, release them together, and wait until all exited with 0. */
export async function runTogether(procs: Spawned[], deadlineMs = 30_000): Promise<void> {
  await withDeadline(Promise.all(procs.map((p) => p.ready)), deadlineMs, "processes to start");
  for (const p of procs) p.child.stdin!.write("go\n");
  const codes = await withDeadline(Promise.all(procs.map((p) => p.exited)), deadlineMs, "processes to finish");
  codes.forEach((code, i) => {
    if (code !== 0) throw new Error(`${procs[i]!.label} exited with ${code}:\n${procs[i]!.stderr()}`);
  });
}

export function killAll(): void {
  for (const c of live.splice(0)) if (c.exitCode === null) c.kill("SIGKILL");
}

export async function withDeadline<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timed out after ${ms}ms waiting for ${what}`)), ms);
  });
  try {
    return await Promise.race([p, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

/** Parse a file of JSON lines (empty if the file does not exist). */
export function readJsonLines<T>(file: string): T[] {
  if (!existsSync(file)) return [];
  return readFileSync(file, "utf8")
    .split("\n")
    .filter(Boolean)
    .map((l) => JSON.parse(l) as T);
}

/** For fixture scripts: print "ready" and resolve when "go" arrives on stdin. */
export async function readyThenGo(): Promise<void> {
  const { createInterface } = await import("node:readline");
  const stdin = createInterface({ input: process.stdin });
  const go = new Promise<void>((resolve) => stdin.on("line", (l) => l.trim() === "go" && resolve()));
  process.stdout.write("ready\n");
  await go;
  stdin.close();
}
