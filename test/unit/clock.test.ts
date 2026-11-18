import { afterEach, describe, expect, it, vi } from "vitest";
import { ManualClock, sleep, systemClock } from "../../src/util/clock.js";

describe("ManualClock", () => {
  it("starts at the given time (default 2026-01-01T00:00Z) and only moves when advanced", () => {
    expect(new ManualClock().now()).toBe(Date.UTC(2026, 0, 1));
    const c = new ManualClock(1000);
    expect(c.now()).toBe(1000);
    expect(c.now()).toBe(1000);
    c.advance(250);
    expect(c.now()).toBe(1250);
  });

  it("fires due timers in time order, with now() set to each timer's due time", () => {
    const c = new ManualClock(0);
    const log: string[] = [];
    c.setTimeout(() => log.push(`b@${c.now()}`), 200);
    c.setTimeout(() => log.push(`a@${c.now()}`), 100);
    c.setTimeout(() => log.push(`c@${c.now()}`), 300);
    c.advance(250);
    expect(log).toEqual(["a@100", "b@200"]);
    expect(c.now()).toBe(250);
    expect(c.pendingTimers()).toBe(1);
    c.advance(50);
    expect(log).toEqual(["a@100", "b@200", "c@300"]);
    expect(c.pendingTimers()).toBe(0);
  });

  it("fires timers due at the same time in the order they were scheduled", () => {
    const c = new ManualClock(0);
    const log: number[] = [];
    for (let i = 0; i < 5; i++) c.setTimeout(() => log.push(i), 10);
    c.advance(10);
    expect(log).toEqual([0, 1, 2, 3, 4]);
  });

  it("fires timers scheduled by other timers if they fall within the advance", () => {
    const c = new ManualClock(0);
    const log: number[] = [];
    c.setTimeout(() => {
      log.push(c.now());
      c.setTimeout(() => log.push(c.now()), 30);
    }, 50);
    c.advance(100);
    expect(log).toEqual([50, 80]);
  });

  it("does not fire a timer that was cleared", () => {
    const c = new ManualClock(0);
    const fn = vi.fn();
    const t = c.setTimeout(fn, 10);
    c.clearTimeout(t);
    c.advance(100);
    expect(fn).not.toHaveBeenCalled();
    expect(c.pendingTimers()).toBe(0);
  });

  // Node clamps delays below 1ms (and NaN / oversized delays) to 1ms. ManualClock used 0ms, so a
  // timer that re-armed itself with setTimeout(fn, 0) fired forever inside a single advance().
  it("treats delays below 1ms like Node does, so a self-rescheduling 0ms timer cannot spin forever", () => {
    const c = new ManualClock(0);
    let calls = 0;
    const tick = () => {
      calls++;
      if (calls < 10_000) c.setTimeout(tick, 0); // the guard keeps the old behaviour from hanging the test
    };
    c.setTimeout(tick, 0);
    c.advance(5);
    expect(calls).toBe(5);
  });

  it.each([
    [-50, 1],
    [Number.NaN, 1],
    [0.5, 1],
    [2 ** 31, 1],
  ])("schedules a %s ms delay %s ms ahead", (delay, expected) => {
    const c = new ManualClock(0);
    const fn = vi.fn();
    c.setTimeout(fn, delay);
    c.advance(expected - 1);
    expect(fn).not.toHaveBeenCalled();
    c.advance(1);
    expect(fn).toHaveBeenCalledOnce();
  });
});

describe("systemClock", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("reads Date.now()", () => {
    vi.useFakeTimers({ now: 1_234_567 });
    expect(systemClock.now()).toBe(1_234_567);
  });

  it("schedules and clears real timers", () => {
    vi.useFakeTimers();
    const fired = vi.fn();
    const cleared = vi.fn();
    systemClock.setTimeout(fired, 100);
    systemClock.clearTimeout(systemClock.setTimeout(cleared, 100));
    vi.advanceTimersByTime(100);
    expect(fired).toHaveBeenCalledOnce();
    expect(cleared).not.toHaveBeenCalled();
  });

  it("does not keep the process alive with its timers", () => {
    const handle = systemClock.setTimeout(() => {}, 60_000);
    expect((handle.id as NodeJS.Timeout).hasRef()).toBe(false);
    systemClock.clearTimeout(handle);
  });
});

// sleep() takes no Clock, so these use vitest's fake timers rather than ManualClock.
describe("sleep", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("resolves after the delay", async () => {
    vi.useFakeTimers();
    let done = false;
    const p = sleep(1000).then(() => (done = true));
    await vi.advanceTimersByTimeAsync(999);
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await p;
    expect(done).toBe(true);
  });

  it("resolves early, and clears its timer, when the signal aborts", async () => {
    vi.useFakeTimers();
    const ac = new AbortController();
    const p = sleep(60_000, ac.signal);
    ac.abort();
    await p;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("resolves immediately for an already-aborted signal", async () => {
    vi.useFakeTimers();
    await sleep(60_000, AbortSignal.abort());
    expect(vi.getTimerCount()).toBe(0);
  });

  it("removes its abort listener once the delay elapses", async () => {
    vi.useFakeTimers();
    const ac = new AbortController();
    const remove = vi.spyOn(ac.signal, "removeEventListener");
    const p = sleep(10, ac.signal);
    await vi.advanceTimersByTimeAsync(10);
    await p;
    expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
  });
});
