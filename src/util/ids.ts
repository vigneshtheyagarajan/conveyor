import { randomBytes, randomUUID } from "node:crypto";

const ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz";

// 10 chars of timestamp + 10 random, so ids sort by creation time.
export function jobId(now: number = Date.now()): string {
  let ts = "";
  let n = now;
  for (let i = 0; i < 10; i++) {
    ts = ALPHABET[n % 32] + ts;
    n = Math.floor(n / 32);
  }
  const bytes = randomBytes(10);
  let rand = "";
  for (const b of bytes) rand += ALPHABET[b % 32];
  return ts + rand;
}

export function leaseToken(): string {
  return randomUUID();
}

export function apiTokenSecret(): string {
  return "cvy_" + randomBytes(24).toString("base64url");
}
