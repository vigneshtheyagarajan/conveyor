export interface Clock {
  now(): number;
  setTimeout(fn: () => void, ms: number): TimerHandle;
  clearTimeout(handle: TimerHandle): void;
}

export type TimerHandle = { readonly id: unknown };

export const systemClock: Clock = {
  now: () => Date.now(),
  setTimeout(fn, ms) {
    const t = setTimeout(fn, ms);
    t.unref?.();
    return { id: t };
  },
  clearTimeout(handle) {
    clearTimeout(handle.id as NodeJS.Timeout);
  },
};

/** Manually advanced clock for tests. */
export class ManualClock implements Clock {
  private current: number;
  private timers = new Map<number, { at: number; fn: () => void }>();
  private nextId = 1;

  constructor(start = Date.UTC(2026, 0, 1)) {
    this.current = start;
  }

  now(): number {
    return this.current;
  }

  setTimeout(fn: () => void, ms: number): TimerHandle {
    const id = this.nextId++;
    this.timers.set(id, { at: this.current + nodeDelay(ms), fn });
    return { id };
  }

  clearTimeout(handle: TimerHandle): void {
    this.timers.delete(handle.id as number);
  }

  /** Advance time, firing due timers in order. */
  advance(ms: number): void {
    const target = this.current + ms;
    for (;;) {
      let nextId: number | undefined;
      let nextAt = Infinity;
      for (const [id, t] of this.timers) {
        if (t.at <= target && t.at < nextAt) {
          nextAt = t.at;
          nextId = id;
        }
      }
      if (nextId === undefined) break;
      const timer = this.timers.get(nextId)!;
      this.timers.delete(nextId);
      this.current = timer.at;
      timer.fn();
    }
    this.current = target;
  }

  pendingTimers(): number {
    return this.timers.size;
  }
}

const TIMEOUT_MAX = 2 ** 31 - 1;

/** Node's setTimeout treats delays below 1ms, above TIMEOUT_MAX, or NaN as 1ms. */
function nodeDelay(ms: number): number {
  return ms >= 1 && ms <= TIMEOUT_MAX ? ms : 1;
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const t = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      resolve();
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
