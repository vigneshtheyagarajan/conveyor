import { afterEach, describe, expect, it, vi } from "vitest";
import { apiTokenSecret, jobId, leaseToken } from "../../src/util/ids.js";
import { legacyJobId } from "../helpers.js";

const ALPHABET = /^[0-9abcdefghjkmnpqrstvwxyz]+$/;

describe("jobId", () => {
  it("is 20 characters from a lowercase, unambiguous alphabet (no i, l, o, u)", () => {
    const id = jobId(Date.UTC(2026, 0, 1));
    expect(id).toHaveLength(20);
    expect(id).toMatch(ALPHABET);
  });

  it("encodes the timestamp in the first 10 characters", () => {
    expect(jobId(0).slice(0, 10)).toBe("0000000000");
    expect(jobId(31).slice(0, 10)).toBe("000000000z");
    expect(jobId(32).slice(0, 10)).toBe("0000000010");
  });

  it("sorts by creation time across a wide range of timestamps", () => {
    const times = [0, 1, 31, 32, 1_000, Date.UTC(2001, 8, 9), Date.UTC(2026, 0, 1), Date.UTC(2026, 0, 1) + 1, Date.UTC(2286, 10, 20)];
    const ids = times.map((t) => jobId(t));
    expect([...ids].sort()).toEqual(ids);
  });

  // Claims break run_at ties by id, so ids must sort in creation order even within one
  // millisecond; otherwise jobs enqueued together run in random order (FIFO broken).
  it("sorts in creation order within the same millisecond", () => {
    const t = Date.UTC(2026, 0, 1);
    const ids = Array.from({ length: 500 }, () => jobId(t));
    expect([...ids].sort()).toEqual(ids);
    expect(new Set(ids.map((id) => id.slice(0, 10)))).toEqual(new Set([jobId(t).slice(0, 10)]));
  });

  it("keeps sorting in creation order when the millisecond changes", () => {
    const t = Date.UTC(2026, 0, 1);
    const ids = [jobId(t), jobId(t), jobId(t + 1), jobId(t + 1), jobId(t + 2)];
    expect([...ids].sort()).toEqual(ids);
  });

  it("differs between calls at the same millisecond", () => {
    const t = Date.UTC(2026, 0, 1);
    const ids = new Set(Array.from({ length: 1000 }, () => jobId(t)));
    expect(ids.size).toBe(1000);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("defaults to the current time", () => {
    const now = Date.UTC(2026, 4, 17, 8, 30);
    vi.useFakeTimers({ now });
    expect(jobId().slice(0, 10)).toBe(jobId(now).slice(0, 10));
  });
});

describe("leaseToken", () => {
  it("is a random UUID", () => {
    const a = leaseToken();
    expect(a).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(leaseToken()).not.toBe(a);
  });
});

describe("apiTokenSecret", () => {
  it("has a recognisable prefix and 24 random bytes of base64url", () => {
    const s = apiTokenSecret();
    expect(s).toMatch(/^cvy_[A-Za-z0-9_-]{32}$/);
    expect(apiTokenSecret()).not.toBe(s);
  });
});

describe("ids created before the FIFO change", () => {
  const t = Date.UTC(2026, 0, 1);
  const allLow = Buffer.alloc(10, 0); // "0000000000"
  const allHigh = Buffer.alloc(10, 31); // "zzzzzzzzzz"

  it("have the same shape and alphabet as new ones", () => {
    for (const id of [legacyJobId(t), legacyJobId(t, allHigh), jobId(t)]) {
      expect(id).toHaveLength(20);
      expect(id).toMatch(ALPHABET);
    }
    expect(legacyJobId(t).slice(0, 10)).toBe(jobId(t).slice(0, 10));
  });

  it("sort by creation time against new ids from any other millisecond", () => {
    // The 10-character timestamp prefix decides; the random part only matters within a millisecond.
    expect(legacyJobId(t, allHigh) < jobId(t + 1)).toBe(true);
    expect(jobId(t) < legacyJobId(t + 1, allLow)).toBe(true);
    const mixed = [legacyJobId(t - 5, allHigh), jobId(t - 4), legacyJobId(t - 3, allLow), jobId(t - 2), jobId(t - 2), legacyJobId(t - 1)];
    expect([...mixed].sort()).toEqual(mixed);
  });

  it("keep their arbitrary order among themselves within one millisecond (already stored; nothing to fix)", () => {
    const a = legacyJobId(t, allHigh);
    const b = legacyJobId(t, allLow); // created second, sorts first
    expect([a, b].sort()).toEqual([b, a]);
  });
});
