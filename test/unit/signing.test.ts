import { createHmac } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SIGNATURE_HEADER, sign, verify } from "../../src/webhooks/signing.js";

const SECRET = "whsec_test";
const BODY = '{"event":"job.completed","data":{"id":"j1"}}';
const T = 1_767_614_400; // 2026-01-05T12:00:00Z

// The contract from the README: v1 = hex HMAC-SHA256 of "<t>.<raw body>" with the webhook secret.
const expectedMac = (secret: string, body: string, t: number) =>
  createHmac("sha256", secret).update(`${t}.${body}`).digest("hex");

describe("sign", () => {
  it("produces t=<unix seconds>,v1=<hex HMAC-SHA256 of t.body>", () => {
    expect(sign(SECRET, BODY, T)).toBe(`t=${T},v1=${expectedMac(SECRET, BODY, T)}`);
  });

  it("uses the conveyor-signature header name", () => {
    expect(SIGNATURE_HEADER).toBe("conveyor-signature");
  });
});

describe("verify", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("accepts its own signatures", () => {
    expect(verify(SECRET, BODY, sign(SECRET, BODY, T), T)).toBe(true);
  });

  it("rejects a changed body, a different secret, or a changed timestamp", () => {
    const header = sign(SECRET, BODY, T);
    expect(verify(SECRET, BODY + " ", header, T)).toBe(false);
    expect(verify("whsec_other", BODY, header, T)).toBe(false);
    const movedT = header.replace(`t=${T}`, `t=${T + 1}`);
    expect(verify(SECRET, BODY, movedT, T)).toBe(false);
  });

  it("accepts timestamps within the tolerance in either direction and rejects the rest", () => {
    const header = sign(SECRET, BODY, T);
    expect(verify(SECRET, BODY, header, T + 300)).toBe(true);
    expect(verify(SECRET, BODY, header, T - 300)).toBe(true);
    expect(verify(SECRET, BODY, header, T + 301)).toBe(false);
    expect(verify(SECRET, BODY, header, T - 301)).toBe(false);
    expect(verify(SECRET, BODY, header, T + 1000, 1000)).toBe(true);
  });

  it("checks against the current time by default", () => {
    vi.useFakeTimers({ now: T * 1000 + 999 });
    expect(verify(SECRET, BODY, sign(SECRET, BODY, T))).toBe(true);
    vi.setSystemTime((T + 301) * 1000);
    expect(verify(SECRET, BODY, sign(SECRET, BODY, T))).toBe(false);
  });

  it("tolerates spaces around the parts", () => {
    expect(verify(SECRET, BODY, ` t=${T} , v1=${expectedMac(SECRET, BODY, T)} `, T)).toBe(true);
  });

  it.each([
    ["an empty header", ""],
    ["garbage", "nonsense"],
    ["a missing v1", `t=${T}`],
    ["a missing t", `v1=${expectedMac(SECRET, BODY, T)}`],
    ["a non-numeric t", `t=soon,v1=${expectedMac(SECRET, BODY, T)}`],
    ["an empty v1", `t=${T},v1=`],
    ["a truncated v1", `t=${T},v1=${expectedMac(SECRET, BODY, T).slice(0, 63)}`],
  ])("rejects %s", (_label, header) => {
    expect(verify(SECRET, BODY, header, T)).toBe(false);
  });

  // BUG(KB-9, not tested): verify() compares the MAC with ===, which is not constant-time.
  // It should use crypto.timingSafeEqual. See test/KNOWN_BUGS.md.
});
