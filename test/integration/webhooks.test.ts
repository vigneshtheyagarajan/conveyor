// Retry timers and the request timeout use global timers, so these tests use vitest's fake
// timers for them. Timestamps and signatures come from the ManualClock. fetch is a fake; no
// network and no sleeps.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Conveyor } from "../../src/core/conveyor.js";
import { NotFoundError, ValidationError } from "../../src/core/errors.js";
import type { Job, Webhook } from "../../src/core/types.js";
import type { ManualClock } from "../../src/util/clock.js";
import { WebhookDispatcher, type FetchLike, type WebhookInput } from "../../src/webhooks/dispatcher.js";
import { SIGNATURE_HEADER, verify } from "../../src/webhooks/signing.js";
import { manualClock, memoryLogger, setup } from "../helpers.js";

interface Call {
  url: string;
  headers: Record<string, string>;
  body: string;
  signal: AbortSignal;
}

let clock: ManualClock;

beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(Math, "random").mockReturnValue(0.5); // retry jitter
  clock = manualClock();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

/** Let queued async work (dispatches, retries due now) run. */
const flush = () => vi.advanceTimersByTimeAsync(0);

async function dispatcher(config = {}) {
  const { logger, records } = memoryLogger();
  const s = await setup({ clock, logger, config: { webhookMaxAttempts: 3, webhookTimeoutMs: 1000, ...config } });
  const calls: Call[] = [];
  let respond: (call: Call) => Response | Promise<Response> = () => new Response("ok", { status: 200 });
  const fetchImpl: FetchLike = async (url, init) => {
    const call = { url, headers: init.headers as Record<string, string>, body: String(init.body), signal: init.signal! };
    calls.push(call);
    return respond(call);
  };
  const d = new WebhookDispatcher(s.conveyor, fetchImpl);
  return {
    ...s,
    d,
    calls,
    records,
    respond: (fn: typeof respond) => (respond = fn),
    hook: (over: Partial<WebhookInput> = {}) => d.create({ url: "https://example.test/hook", events: ["*"], ...over }),
  };
}

const job = (over: Partial<Job> = {}) =>
  ({ id: "j1", queue: "q", name: "send", state: "completed", attempts: 1, result: { ok: true }, lastError: null, ...over }) as Job;

describe("managing webhooks", () => {
  it("creates a webhook with a generated secret", async () => {
    const { d } = await dispatcher();
    const hook = await d.create({ url: "https://example.test", events: ["job.completed"], queues: ["emails"] });
    expect(hook).toMatchObject({ url: "https://example.test/", events: ["job.completed"], queues: ["emails"], enabled: true, createdAt: clock.now() });
    expect(hook.secret).toMatch(/^whsec_[A-Za-z0-9_-]{32}$/);
    expect(await d.list()).toEqual([hook]);
  });

  it("uses the secret it is given and defaults to all queues", async () => {
    const { d } = await dispatcher();
    expect(await d.create({ url: "http://localhost:9000/h", events: ["*"], secret: "mine" })).toMatchObject({ secret: "mine", queues: [] });
  });

  it.each<[string, Partial<WebhookInput>, RegExp]>([
    ["an invalid URL", { url: "not a url" }, /url must be a valid URL/],
    ["a non-HTTP URL", { url: "ftp://example.test/x" }, /url must be http or https/],
    ["no events", { events: [] }, /events must be a non-empty array/],
    ["an unknown event", { events: ["job.exploded"] }, /unknown event job.exploded/],
  ])("rejects %s", async (_label, patch, message) => {
    const { d } = await dispatcher();
    await expect(d.create({ url: "https://example.test/h", events: ["*"], ...patch })).rejects.toThrow(message);
    expect(await d.list()).toEqual([]);
  });

  // Previously accepted, but the dispatcher only sends job lifecycle events, so these
  // subscriptions silently never fired.
  it.each(["job.claimed", "job.progress", "job.reaped", "job.unblocked", "queue.paused", "queue.resumed", "schedule.fired"])(
    "rejects a subscription to %s, which is never delivered",
    async (event) => {
      const { d } = await dispatcher();
      const err = await d.create({ url: "https://example.test/h", events: ["job.completed", event] }).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(ValidationError);
      expect((err as Error).message).toMatch(`event ${event} is not delivered to webhooks`);
    },
  );

  it("removes webhooks", async () => {
    const { d } = await dispatcher();
    const hook = await d.create({ url: "https://example.test/h", events: ["*"] });
    await d.remove(hook.id);
    expect(await d.list()).toEqual([]);
    await expect(d.remove(hook.id)).rejects.toThrow(NotFoundError);
    await expect(d.deliveries(hook.id)).rejects.toThrow(NotFoundError);
  });
});

describe("delivery", () => {
  it("POSTs a signed JSON body describing the job and records the delivery", async () => {
    const { d, calls, hook } = await dispatcher();
    const h = await hook({ secret: "s3cret" });
    await d.dispatch("job.completed", job());

    expect(calls).toHaveLength(1);
    const call = calls[0]!;
    expect(call.url).toBe("https://example.test/hook");
    const body = JSON.parse(call.body);
    expect(body).toEqual({
      id: expect.stringMatching(/^[0-9a-f-]{36}$/),
      event: "job.completed",
      createdAt: new Date(clock.now()).toISOString(),
      data: { id: "j1", queue: "q", name: "send", state: "completed", attempts: 1, result: { ok: true }, lastError: null },
    });
    expect(call.headers).toMatchObject({
      "content-type": "application/json",
      "user-agent": "conveyor-webhooks/1",
      "conveyor-event": "job.completed",
    });
    const nowSec = Math.floor(clock.now() / 1000);
    expect(call.headers[SIGNATURE_HEADER]).toMatch(new RegExp(`^t=${nowSec},v1=[0-9a-f]{64}$`));
    expect(verify("s3cret", call.body, call.headers[SIGNATURE_HEADER]!, nowSec)).toBe(true);
    expect(verify("wrong", call.body, call.headers[SIGNATURE_HEADER]!, nowSec)).toBe(false);

    const [delivery] = await d.deliveries(h.id);
    expect(delivery).toEqual({
      id: call.headers["conveyor-delivery"],
      webhookId: h.id,
      event: "job.completed",
      body: call.body,
      status: "succeeded",
      attempts: 1,
      lastStatusCode: 200,
      lastError: null,
      nextAttemptAt: null,
      createdAt: clock.now(),
      updatedAt: clock.now(),
    });
  });

  it("only sends to enabled webhooks subscribed to the event and the job's queue", async () => {
    const { d, calls, conveyor, hook } = await dispatcher();
    const all = await hook({ url: "https://all.test/" });
    const completedOnly = await hook({ url: "https://completed.test/", events: ["job.completed"] });
    await hook({ url: "https://dead.test/", events: ["job.dead"] });
    const emailsOnly = await hook({ url: "https://emails.test/", queues: ["emails"] });
    const disabled: Webhook = { ...all, id: "disabled", url: "https://disabled.test/", enabled: false };
    await conveyor.store.insertWebhook(disabled); // there is no API to disable one yet
    d.invalidate();

    await d.dispatch("job.completed", job({ queue: "emails" }));
    await d.dispatch("job.completed", job({ queue: "other" }));
    const urls = calls.map((c) => c.url);
    expect(urls.filter((u) => u === all.url)).toHaveLength(2);
    expect(urls.filter((u) => u === completedOnly.url)).toHaveLength(2);
    expect(urls.filter((u) => u === emailsOnly.url)).toHaveLength(1);
    expect(urls).not.toContain("https://dead.test/");
    expect(urls).not.toContain("https://disabled.test/");
  });

  it("sends the same event body to every matching webhook", async () => {
    const { d, calls, hook } = await dispatcher();
    await hook({ url: "https://a.test/" });
    await hook({ url: "https://b.test/" });
    await d.dispatch("job.dead", job({ state: "dead", lastError: "boom" }));
    expect(calls).toHaveLength(2);
    expect(calls[0]!.body).toBe(calls[1]!.body);
    expect(calls[0]!.headers["conveyor-delivery"]).not.toBe(calls[1]!.headers["conveyor-delivery"]);
  });

  it("does nothing when no webhook matches", async () => {
    const { d, calls } = await dispatcher();
    await d.dispatch("job.completed", job());
    expect(calls).toEqual([]);
  });

  it("delivers the job lifecycle events as they happen once started", async () => {
    const { d, calls, conveyor, broker, hook } = await dispatcher();
    await hook();
    d.start();
    const j = await conveyor.enqueue({ queue: "q", name: "x", maxAttempts: 2, backoff: { type: "fixed", delayMs: 0 } });
    let lease = await broker.claim(["q"], "w");
    await broker.fail(j.id, lease!.token, "first");
    await conveyor.store.promoteDelayed(clock.now(), 10);
    lease = await broker.claim(["q"], "w");
    await broker.fail(j.id, lease!.token, "second");
    const other = await conveyor.enqueue({ queue: "q", name: "y" });
    lease = await broker.claim(["q"], "w");
    await broker.complete(other.id, lease!.token);
    const cancelled = await conveyor.enqueue({ queue: "q", name: "z" });
    await conveyor.cancel(cancelled.id);
    await flush();

    const events = calls.map((c) => [c.headers["conveyor-event"], JSON.parse(c.body).data.id]);
    expect(events).toEqual(
      expect.arrayContaining([
        ["job.enqueued", j.id],
        ["job.failed", j.id],
        ["job.retrying", j.id],
        ["job.dead", j.id],
        ["job.completed", other.id],
        ["job.cancelled", cancelled.id],
      ]),
    );
    expect(events).toHaveLength(9); // 3 enqueued, 2 failed, 1 retrying, 1 dead, 1 completed, 1 cancelled
    await d.stop();
  });

  it("picks up webhooks created by another process within 5 seconds", async () => {
    const { d, calls, conveyor } = await dispatcher();
    await d.dispatch("job.completed", job()); // caches "no webhooks"
    await conveyor.store.insertWebhook({ id: "elsewhere", url: "https://x.test/", secret: "s", events: ["*"], queues: [], enabled: true, createdAt: 0 });
    await d.dispatch("job.completed", job());
    expect(calls).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(5001);
    await d.dispatch("job.completed", job());
    expect(calls).toHaveLength(1);
  });
});

// Regression (was KB-18): retries lived only in an in-memory timer, so a restart dropped them and
// the deliveries stayed "pending" forever.
describe("retries across a restart", () => {
  /** A second dispatcher on the same database, as after a process restart. */
  function restarted(conveyor: Conveyor, calls: Call[], status = 200) {
    return new WebhookDispatcher(conveyor, async (url, init) => {
      calls.push({ url, headers: init.headers as Record<string, string>, body: String(init.body), signal: init.signal! });
      return new Response(null, { status });
    });
  }

  it("resumes a pending retry at its stored next attempt time", async () => {
    const { d, conveyor, calls, hook, respond } = await dispatcher({ webhookMaxAttempts: 5 });
    const h = await hook({ secret: "s" });
    respond(() => new Response("down", { status: 503 }));
    await d.dispatch("job.completed", job());
    const [pending] = await d.deliveries(h.id);
    expect(pending).toMatchObject({ status: "pending", attempts: 1, nextAttemptAt: clock.now() + 1500 });
    await d.stop(); // the process stops 1000ms into the 1500ms wait

    clock.advance(1000);
    const d2 = restarted(conveyor, calls);
    d2.start();
    await vi.advanceTimersByTimeAsync(499);
    expect(calls).toHaveLength(1);
    clock.advance(500);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toHaveLength(2);
    expect(calls[1]!.body).toBe(calls[0]!.body);
    expect(calls[1]!.headers["conveyor-delivery"]).toBe(pending!.id);
    expect(verify("s", calls[1]!.body, calls[1]!.headers[SIGNATURE_HEADER]!, Math.floor(clock.now() / 1000))).toBe(true);
    expect((await d2.deliveries(h.id))[0]).toMatchObject({ id: pending!.id, status: "succeeded", attempts: 2, nextAttemptAt: null });
    await d2.stop();
  });

  it("retries straight away a delivery that became due while the process was down", async () => {
    const { d, conveyor, calls, hook, respond } = await dispatcher({ webhookMaxAttempts: 5 });
    const h = await hook();
    respond(() => new Response("down", { status: 503 }));
    await d.dispatch("job.completed", job());
    await d.stop();

    clock.advance(60 * 60 * 1000);
    const d2 = restarted(conveyor, calls);
    d2.start();
    await flush();
    expect(calls).toHaveLength(2);
    expect((await d2.deliveries(h.id))[0]).toMatchObject({ status: "succeeded", attempts: 2 });
    await d2.stop();
  });

  it("keeps counting attempts across the restart and gives up at the limit", async () => {
    const { d, conveyor, calls, hook, respond } = await dispatcher({ webhookMaxAttempts: 3 });
    const h = await hook();
    respond(() => new Response("down", { status: 503 }));
    await d.dispatch("job.completed", job());
    clock.advance(1500);
    await vi.advanceTimersByTimeAsync(1500); // attempt 2
    await d.stop();

    const d2 = restarted(conveyor, calls, 503);
    d2.start();
    clock.advance(60 * 60 * 1000);
    await vi.advanceTimersByTimeAsync(60 * 60 * 1000);
    expect(calls).toHaveLength(3);
    expect((await d2.deliveries(h.id))[0]).toMatchObject({ status: "failed", attempts: 3, nextAttemptAt: null });
    await d2.stop();
  });

  it("sends a delivery that was stored but never attempted before the process stopped", async () => {
    const { conveyor, calls, hook } = await dispatcher();
    const h = await hook();
    await conveyor.store.insertDelivery({
      id: "orphan",
      webhookId: h.id,
      event: "job.completed",
      body: '{"event":"job.completed"}',
      status: "pending",
      attempts: 0,
      lastStatusCode: null,
      lastError: null,
      nextAttemptAt: clock.now(),
      createdAt: clock.now(),
      updatedAt: clock.now(),
    });
    const d2 = restarted(conveyor, calls);
    d2.start();
    await flush();
    expect(calls.map((c) => c.headers["conveyor-delivery"])).toEqual(["orphan"]);
    expect((await d2.deliveries(h.id))[0]).toMatchObject({ status: "succeeded", attempts: 1 });
    await d2.stop();
  });

  it("does not resend deliveries that succeeded or failed, or send to a disabled webhook", async () => {
    const { d, conveyor, calls, hook, respond } = await dispatcher({ webhookMaxAttempts: 1 });
    const h = await hook();
    await d.dispatch("job.completed", job()); // succeeds
    respond(() => new Response("down", { status: 503 }));
    await d.dispatch("job.completed", job()); // fails permanently (1 attempt)
    await conveyor.store.insertWebhook({ ...h, id: "off", enabled: false });
    await conveyor.store.insertDelivery({
      id: "to-disabled",
      webhookId: "off",
      event: "job.completed",
      body: "{}",
      status: "pending",
      attempts: 1,
      lastStatusCode: 503,
      lastError: "HTTP 503",
      nextAttemptAt: clock.now(),
      createdAt: clock.now(),
      updatedAt: clock.now(),
    });
    await d.stop();

    const before = calls.length;
    const d2 = restarted(conveyor, calls);
    d2.start();
    await vi.advanceTimersByTimeAsync(60 * 60 * 1000);
    expect(calls.length).toBe(before);
    await d2.stop();
  });
});

describe("retries", () => {
  it("retries a failed delivery with backoff until it succeeds", async () => {
    const { d, calls, hook, respond } = await dispatcher();
    const h = await hook({ secret: "s" });
    respond(() => new Response("nope", { status: 500 }));
    await d.dispatch("job.completed", job());

    let [delivery] = await d.deliveries(h.id);
    // 2s doubled per attempt, jittered between 50% and 100% (Math.random() = 0.5 here): 1500ms.
    expect(delivery).toMatchObject({ status: "pending", attempts: 1, lastStatusCode: 500, lastError: "HTTP 500", nextAttemptAt: clock.now() + 1500 });

    respond(() => new Response(null, { status: 204 }));
    clock.advance(1500);
    await vi.advanceTimersByTimeAsync(1499);
    expect(calls).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toHaveLength(2);
    [delivery] = await d.deliveries(h.id);
    expect(delivery).toMatchObject({ status: "succeeded", attempts: 2, lastStatusCode: 204, lastError: null, nextAttemptAt: null, updatedAt: clock.now() });
    // The retry is signed at the time it is sent, with the same body.
    expect(calls[1]!.body).toBe(calls[0]!.body);
    expect(verify("s", calls[1]!.body, calls[1]!.headers[SIGNATURE_HEADER]!, Math.floor(clock.now() / 1000))).toBe(true);
  });

  it("records network errors and retries them", async () => {
    const { d, hook, respond } = await dispatcher();
    const h = await hook();
    respond(() => {
      throw new TypeError("fetch failed");
    });
    await d.dispatch("job.completed", job());
    expect((await d.deliveries(h.id))[0]).toMatchObject({ status: "pending", attempts: 1, lastStatusCode: null, lastError: "fetch failed" });
  });

  it("aborts a request that takes longer than the webhook timeout, and retries it", async () => {
    const { d, hook, respond } = await dispatcher({ webhookTimeoutMs: 1000 });
    const h = await hook();
    let aborted = false;
    respond(
      (call) =>
        new Promise((_, reject) =>
          call.signal.addEventListener("abort", () => {
            aborted = true;
            reject(new DOMException("This operation was aborted", "AbortError"));
          }),
        ),
    );
    const dispatching = d.dispatch("job.completed", job());
    await vi.advanceTimersByTimeAsync(999);
    expect(aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await dispatching;
    expect(aborted).toBe(true);
    expect((await d.deliveries(h.id))[0]).toMatchObject({ status: "pending", attempts: 1, lastError: "This operation was aborted" });
  });

  it("gives up after webhookMaxAttempts and marks the delivery failed", async () => {
    const { d, calls, hook, respond, records } = await dispatcher({ webhookMaxAttempts: 3 });
    const h = await hook();
    respond(() => new Response("down", { status: 503 }));
    await d.dispatch("job.completed", job());
    await vi.advanceTimersByTimeAsync(60 * 60 * 1000);
    expect(calls).toHaveLength(3);
    expect((await d.deliveries(h.id))[0]).toMatchObject({ status: "failed", attempts: 3, lastStatusCode: 503, lastError: "HTTP 503", nextAttemptAt: null });
    expect(records).toContainEqual(expect.objectContaining({ level: "warn", msg: "webhook delivery failed permanently", webhookId: h.id }));
    expect(vi.getTimerCount()).toBe(0);
  });

  it("waits longer before each retry", async () => {
    const { d, calls, hook, respond } = await dispatcher({ webhookMaxAttempts: 4 });
    await hook();
    respond(() => new Response("down", { status: 503 }));
    await d.dispatch("job.completed", job());
    const gaps: number[] = [];
    let last = 0;
    for (let t = 0; t < 60_000 && calls.length < 4; t += 100) {
      await vi.advanceTimersByTimeAsync(100);
      if (calls.length > gaps.length + 1) {
        gaps.push(t + 100 - last);
        last = t + 100;
      }
    }
    expect(gaps).toEqual([1500, 3000, 6000]);
  });

  // Previously a deleted webhook kept receiving retries until it ran out of attempts.
  it("stops retrying when the webhook is deleted", async () => {
    const { d, calls, hook, respond } = await dispatcher({ webhookMaxAttempts: 5 });
    const h = await hook();
    respond(() => new Response("down", { status: 503 }));
    await d.dispatch("job.completed", job());
    await d.remove(h.id);
    await vi.advanceTimersByTimeAsync(60 * 60 * 1000);
    expect(calls).toHaveLength(1);
  });

  it("stops retrying when the webhook is disabled", async () => {
    const { d, calls, conveyor, hook, respond } = await dispatcher({ webhookMaxAttempts: 5 });
    const h = await hook();
    respond(() => new Response("down", { status: 503 }));
    await d.dispatch("job.completed", job());
    // No API disables a webhook yet; rewrite the row as another process might.
    await conveyor.store.deleteWebhook(h.id);
    await conveyor.store.insertWebhook({ ...h, enabled: false });
    await vi.advanceTimersByTimeAsync(60 * 60 * 1000);
    expect(calls).toHaveLength(1);
  });

  it("stop() cancels pending retries, stops listening and waits for deliveries in flight", async () => {
    const { d, calls, conveyor, hook, respond } = await dispatcher({ webhookMaxAttempts: 5 });
    await hook();
    d.start();
    respond(() => new Response("down", { status: 503 }));
    await d.dispatch("job.completed", job());

    let finish!: () => void;
    respond(() => new Promise((r) => (finish = () => r(new Response("ok")))));
    await conveyor.enqueue({ queue: "q", name: "x" }); // in flight until finish()
    await flush();
    let stopped = false;
    const stopping = d.stop().then(() => (stopped = true));
    await flush();
    expect(stopped).toBe(false);
    finish();
    await stopping;

    const before = calls.length;
    await conveyor.enqueue({ queue: "q", name: "x" });
    await vi.advanceTimersByTimeAsync(60 * 60 * 1000);
    expect(calls.length).toBe(before);
  });

  it("logs a dispatch that fails before sending instead of throwing", async () => {
    const { d, conveyor, records, hook } = await dispatcher();
    await hook();
    d.start();
    vi.spyOn(conveyor.store, "insertDelivery").mockRejectedValueOnce(new Error("disk full"));
    await conveyor.enqueue({ queue: "q", name: "x" });
    await flush();
    expect(records).toContainEqual(expect.objectContaining({ level: "error", msg: "webhook dispatch failed", err: expect.objectContaining({ message: "disk full" }) }));
    await d.stop();
  });
});
