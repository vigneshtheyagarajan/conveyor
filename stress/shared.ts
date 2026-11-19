// Definitions shared by the stress test's orchestrator (run.ts) and its child processes
// (roles.ts). Kept apart from roles.ts, which is an entry point: importing it would run it.
export const QUEUE = "stress";

/** How a job behaves; stored as its payload, so every run of it behaves the same way. */
export interface JobSpec {
  /** Unique per enqueue request: tells the enqueuer whether it created the job it got back. */
  nonce: string;
  kind: "normal" | "unique" | "failing" | "timeout";
  /** How long a normal attempt takes. */
  workMs: number;
  /** Attempts up to this number throw. */
  failUntil: number;
  /** Attempts up to this number run past the job's timeout (for stuckMs). */
  timeoutUntil: number;
  stuckMs: number;
  /** Whether a timed-out attempt stops when its signal aborts, or ignores it. */
  honorSignal: boolean;
}

export interface StressOptions {
  concurrency: number;
  leaseMs: number;
  heartbeatMs: number;
  seed: number;
  jobs: number;
  enqueueMs: number;
}

export type StressLog =
  | { kind: "start"; run: string; jobId: string; attempt: number; pid: number; t: number }
  | { kind: "end"; run: string; t: number; ok: boolean }
  | { kind: "completed"; jobId: string; pid: number; t: number }
  | { kind: "enqueued"; nonce: string; jobId: string; created: boolean; key: string | null };

/** Small, seedable PRNG (mulberry32), so a run's job mix and kill schedule can be repeated. */
export function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
