import type { Clock } from "../util/clock.js";
import { systemClock } from "../util/clock.js";

// Classic token bucket
export class TokenBucket {
  private tokens: number;
  private lastRefill: number;

  constructor(
    readonly capacity: number,
    readonly intervalMs: number,
    private clock: Clock = systemClock,
  ) {
    if (capacity <= 0 || intervalMs <= 0) throw new RangeError("capacity and intervalMs must be positive");
    this.tokens = capacity;
    this.lastRefill = clock.now();
  }

  private refill(): void {
    const now = this.clock.now();
    const elapsed = now - this.lastRefill;
    if (elapsed <= 0) return;
    const added = (elapsed / this.intervalMs) * this.capacity;
    this.tokens = Math.min(this.capacity, this.tokens + added);
    this.lastRefill = now;
  }

  tryTake(count = 1): boolean {
    this.refill();
    if (this.tokens >= count) {
      this.tokens -= count;
      return true;
    }
    return false;
  }

  /** Put tokens back, e.g. when a claim was abandoned. */
  giveBack(count = 1): void {
    this.tokens = Math.min(this.capacity, this.tokens + count);
  }

  /** ms until `count` tokens will be available. */
  msUntilAvailable(count = 1): number {
    this.refill();
    if (this.tokens >= count) return 0;
    return Math.ceil(((count - this.tokens) / this.capacity) * this.intervalMs);
  }

  available(): number {
    this.refill();
    return this.tokens;
  }
}
