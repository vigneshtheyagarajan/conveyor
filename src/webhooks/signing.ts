import { createHmac } from "node:crypto";

export const SIGNATURE_HEADER = "conveyor-signature";

/** Header value: `t=<unix seconds>,v1=<hex hmac-sha256 of "t.body">`. */
export function sign(secret: string, body: string, timestampSec: number): string {
  const mac = createHmac("sha256", secret).update(`${timestampSec}.${body}`).digest("hex");
  return `t=${timestampSec},v1=${mac}`;
}

// Verify a signature header
export function verify(
  secret: string,
  body: string,
  header: string,
  nowSec = Math.floor(Date.now() / 1000),
  toleranceSec = 300,
): boolean {
  const parts = Object.fromEntries(
    header.split(",").map((kv) => {
      const i = kv.indexOf("=");
      return [kv.slice(0, i).trim(), kv.slice(i + 1).trim()];
    }),
  );
  const t = Number(parts.t);
  if (!Number.isFinite(t) || !parts.v1) return false;
  if (Math.abs(nowSec - t) > toleranceSec) return false;
  const expected = sign(secret, body, t).split("v1=")[1];
  return expected === parts.v1;
}
