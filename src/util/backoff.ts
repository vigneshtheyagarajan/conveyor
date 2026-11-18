export type BackoffStrategy =
  | { type: "fixed"; delayMs: number }
  | { type: "exponential"; baseMs: number; maxMs: number; jitter?: boolean }
  | { type: "linear"; stepMs: number; maxMs: number };

export const DEFAULT_BACKOFF: BackoffStrategy = {
  type: "exponential",
  baseMs: 1000,
  maxMs: 10 * 60 * 1000,
  jitter: true,
};

// Delay before the next attempt
export function backoffDelay(
  strategy: BackoffStrategy,
  attempt: number,
  random: () => number = Math.random,
): number {
  switch (strategy.type) {
    case "fixed":
      return strategy.delayMs;
    case "linear":
      return Math.min(strategy.stepMs * attempt, strategy.maxMs);
    case "exponential": {
      // A zero base stays zero (0 * 2 ** 1024 would be 0 * Infinity = NaN).
      const raw = strategy.baseMs === 0 ? 0 : Math.min(strategy.baseMs * 2 ** (attempt - 1), strategy.maxMs);
      if (!strategy.jitter) return raw;
      // full jitter between 50% and 100% of the computed delay
      return Math.round(raw / 2 + random() * (raw / 2));
    }
  }
}

function jitterFlag(value: unknown): boolean {
  if (value === undefined || value === null) return true;
  if (typeof value !== "boolean") throw new TypeError("backoff.jitter must be a boolean");
  return value;
}

export function parseBackoff(value: unknown): BackoffStrategy | undefined {
  if (value == null) return undefined;
  if (typeof value !== "object") throw new TypeError("backoff must be an object");
  const v = value as Record<string, unknown>;
  const num = (k: string): number => {
    const n = v[k];
    if (typeof n !== "number" || !Number.isFinite(n) || n < 0) {
      throw new TypeError(`backoff.${k} must be a non-negative number`);
    }
    return n;
  };
  switch (v.type) {
    case "fixed":
      return { type: "fixed", delayMs: num("delayMs") };
    case "linear":
      return { type: "linear", stepMs: num("stepMs"), maxMs: num("maxMs") };
    case "exponential":
      return {
        type: "exponential",
        baseMs: num("baseMs"),
        maxMs: num("maxMs"),
        jitter: jitterFlag(v.jitter),
      };
    default:
      throw new TypeError(`unknown backoff type: ${String(v.type)}`);
  }
}
