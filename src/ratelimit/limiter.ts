import type { QueueSettings } from "../core/types.js";
import type { Clock } from "../util/clock.js";
import { systemClock } from "../util/clock.js";
import { TokenBucket } from "./token-bucket.js";

export interface RateLimiter {
  /** Returns true if a job from `queue` may be claimed now. */
  acquire(queue: QueueSettings): Promise<boolean>;
  release(queue: string): Promise<void>;
  /** Hint for how long to wait before the queue has capacity again. */
  retryAfterMs(queue: string): number;
}

/** Per-queue token buckets held in this process. */
export class LocalRateLimiter implements RateLimiter {
  private buckets = new Map<string, TokenBucket>();

  constructor(private clock: Clock = systemClock) {}

  private bucketFor(q: QueueSettings): TokenBucket | undefined {
    if (q.rateLimitMax <= 0 || q.rateLimitIntervalMs <= 0) {
      this.buckets.delete(q.name);
      return undefined;
    }
    let bucket = this.buckets.get(q.name);
    if (!bucket || bucket.capacity !== q.rateLimitMax || bucket.intervalMs !== q.rateLimitIntervalMs) {
      // Carry unused tokens over so that changing the limit doesn't grant a fresh burst.
      const carried = bucket?.available();
      bucket = new TokenBucket(q.rateLimitMax, q.rateLimitIntervalMs, this.clock, carried);
      this.buckets.set(q.name, bucket);
    }
    return bucket;
  }

  async acquire(q: QueueSettings): Promise<boolean> {
    const bucket = this.bucketFor(q);
    return bucket ? bucket.tryTake() : true;
  }

  async release(queue: string): Promise<void> {
    this.buckets.get(queue)?.giveBack();
  }

  retryAfterMs(queue: string): number {
    return this.buckets.get(queue)?.msUntilAvailable() ?? 0;
  }
}
