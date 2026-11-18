import { randomBytes, randomUUID } from "node:crypto";

const ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz";

let lastTime = -1;
let lastRandom: number[] = [];

// 10 chars of timestamp + 10 random, so ids sort by creation time. Within one millisecond the
// random part of the previous id is incremented instead (as in monotonic ULIDs), so ids made
// by this process in the same millisecond also sort in creation order. Claims rely on this
// for first-in-first-out order.
export function jobId(now: number = Date.now()): string {
  let ts = "";
  let n = now;
  for (let i = 0; i < 10; i++) {
    ts = ALPHABET[n % 32] + ts;
    n = Math.floor(n / 32);
  }
  if (now === lastTime) {
    increment(lastRandom);
  } else {
    lastTime = now;
    lastRandom = [...randomBytes(10)].map((b) => b % 32);
    // Start in the lower half so incrementing can't overflow: that would take 2^45 ids in
    // one millisecond.
    lastRandom[0] = lastRandom[0]! % 16;
  }
  return ts + lastRandom.map((d) => ALPHABET[d]).join("");
}

/** Add one to a base-32 digit array in place. */
function increment(digits: number[]): void {
  for (let i = digits.length - 1; i >= 0; i--) {
    if (digits[i]! < 31) {
      digits[i]!++;
      return;
    }
    digits[i] = 0;
  }
}

export function leaseToken(): string {
  return randomUUID();
}

export function apiTokenSecret(): string {
  return "cvy_" + randomBytes(24).toString("base64url");
}
