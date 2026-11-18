import { randomBytes, randomUUID } from "node:crypto";
import type { Conveyor } from "../core/conveyor.js";
import { NotFoundError, ValidationError } from "../core/errors.js";
import type { EventName } from "../core/events.js";
import { ALL_EVENTS } from "../core/events.js";
import type { Logger } from "../core/logger.js";
import type { Job, Webhook, WebhookDelivery } from "../core/types.js";
import { backoffDelay } from "../util/backoff.js";
import { SIGNATURE_HEADER, sign } from "./signing.js";

export interface WebhookInput {
  url: string;
  events: string[];
  queues?: string[];
  secret?: string;
}

export type FetchLike = (url: string, init: RequestInit) => Promise<Response>;

/** At most this many pending deliveries are resumed by start(). */
const RESUME_LIMIT = 10_000;

const JOB_EVENTS = [
  "job.enqueued",
  "job.completed",
  "job.failed",
  "job.retrying",
  "job.dead",
  "job.cancelled",
] as const satisfies readonly EventName[];

// Delivers job lifecycle events to registered HTTP endpoints
export class WebhookDispatcher {
  private offs: (() => void)[] = [];
  private logger: Logger;
  private inflight = new Set<Promise<void>>();
  private retryTimers = new Set<NodeJS.Timeout>();
  private cache?: { at: number; hooks: Webhook[] };

  constructor(
    private conveyor: Conveyor,
    private fetchImpl: FetchLike = fetch,
  ) {
    this.logger = conveyor.logger.child({ component: "webhooks" });
  }

  start(): void {
    for (const event of JOB_EVENTS) {
      this.offs.push(
        this.conveyor.events.on(event, (...args: unknown[]) => {
          this.track(this.dispatch(event, args[0] as Job));
        }),
      );
    }
    this.track(this.resumePending());
  }

  /**
   * Pick up deliveries that were still pending when the process last stopped, each at its
   * stored next attempt time (immediately if that has passed).
   */
  private async resumePending(): Promise<void> {
    const pending = await this.conveyor.store.findPendingDeliveries(RESUME_LIMIT);
    if (pending.length === RESUME_LIMIT) {
      this.logger.warn("more pending webhook deliveries than can be resumed at once", { resumed: RESUME_LIMIT });
    }
    const now = this.conveyor.clock.now();
    for (const delivery of pending) {
      this.scheduleRetry(delivery.webhookId, delivery, Math.max(0, (delivery.nextAttemptAt ?? now) - now));
    }
    if (pending.length) this.logger.info("resuming pending webhook deliveries", { count: pending.length });
  }

  private scheduleRetry(webhookId: string, delivery: WebhookDelivery, delayMs: number): void {
    const t = setTimeout(() => {
      this.retryTimers.delete(t);
      this.track(this.retry(webhookId, delivery));
    }, delayMs);
    t.unref();
    this.retryTimers.add(t);
  }

  async stop(): Promise<void> {
    this.offs.forEach((off) => off());
    this.offs = [];
    this.retryTimers.forEach(clearTimeout);
    this.retryTimers.clear();
    await Promise.allSettled([...this.inflight]);
  }

  private track(p: Promise<void>): void {
    this.inflight.add(p);
    p.catch((err) => this.logger.error("webhook dispatch failed", { err })).finally(() => this.inflight.delete(p));
  }

  private async hooks(): Promise<Webhook[]> {
    const now = Date.now();
    if (!this.cache || now - this.cache.at > 5000) {
      this.cache = { at: now, hooks: await this.conveyor.store.listWebhooks() };
    }
    return this.cache.hooks;
  }

  invalidate(): void {
    this.cache = undefined;
  }

  async dispatch(event: EventName, job: Job): Promise<void> {
    const hooks = (await this.hooks()).filter(
      (h) =>
        h.enabled &&
        (h.events.includes("*") || h.events.includes(event)) &&
        (h.queues.length === 0 || h.queues.includes(job.queue)),
    );
    if (hooks.length === 0) return;
    const now = this.conveyor.clock.now();
    const body = JSON.stringify({
      id: randomUUID(),
      event,
      createdAt: new Date(now).toISOString(),
      data: {
        id: job.id,
        queue: job.queue,
        name: job.name,
        state: job.state,
        attempts: job.attempts,
        result: job.result,
        lastError: job.lastError,
      },
    });
    await Promise.all(
      hooks.map(async (hook) => {
        const delivery: WebhookDelivery = {
          id: randomUUID(),
          webhookId: hook.id,
          event,
          body,
          status: "pending",
          attempts: 0,
          lastStatusCode: null,
          lastError: null,
          nextAttemptAt: now,
          createdAt: now,
          updatedAt: now,
        };
        await this.conveyor.store.insertDelivery(delivery);
        await this.attempt(hook, delivery);
      }),
    );
  }

  private async attempt(hook: Webhook, delivery: WebhookDelivery): Promise<void> {
    const { config, clock, store } = this.conveyor;
    delivery.attempts++;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), config.webhookTimeoutMs);
    try {
      const ts = Math.floor(clock.now() / 1000);
      const res = await this.fetchImpl(hook.url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "user-agent": "conveyor-webhooks/1",
          [SIGNATURE_HEADER]: sign(hook.secret, delivery.body, ts),
          "conveyor-delivery": delivery.id,
          "conveyor-event": delivery.event,
        },
        body: delivery.body,
        signal: controller.signal,
      });
      delivery.lastStatusCode = res.status;
      if (res.ok) {
        delivery.status = "succeeded";
        delivery.lastError = null;
        delivery.nextAttemptAt = null;
      } else {
        delivery.lastError = `HTTP ${res.status}`;
      }
    } catch (err) {
      delivery.lastError = (err as Error).message;
    } finally {
      clearTimeout(timer);
    }

    const now = clock.now();
    delivery.updatedAt = now;
    if (delivery.status !== "succeeded") {
      if (delivery.attempts >= config.webhookMaxAttempts) {
        delivery.status = "failed";
        delivery.nextAttemptAt = null;
        this.logger.warn("webhook delivery failed permanently", { webhookId: hook.id, deliveryId: delivery.id });
      } else {
        const delay = backoffDelay({ type: "exponential", baseMs: 2000, maxMs: 300_000, jitter: true }, delivery.attempts);
        delivery.nextAttemptAt = now + delay;
        this.scheduleRetry(hook.id, delivery, delay);
      }
    }
    await store.updateDelivery(delivery);
  }

  /** Retry with the webhook's current state; give up quietly if it was deleted or disabled. */
  private async retry(webhookId: string, delivery: WebhookDelivery): Promise<void> {
    const hook = await this.conveyor.store.getWebhook(webhookId);
    if (!hook || !hook.enabled) {
      this.logger.info("webhook removed or disabled; not retrying", { webhookId, deliveryId: delivery.id });
      return;
    }
    await this.attempt(hook, delivery);
  }

  async create(input: WebhookInput): Promise<Webhook> {
    let url: URL;
    try {
      url = new URL(input.url);
    } catch {
      throw new ValidationError("url must be a valid URL");
    }
    if (url.protocol !== "https:" && url.protocol !== "http:") {
      throw new ValidationError("url must be http or https");
    }
    if (!Array.isArray(input.events) || input.events.length === 0) {
      throw new ValidationError("events must be a non-empty array");
    }
    for (const e of input.events) {
      if (e === "*" || (JOB_EVENTS as readonly string[]).includes(e)) continue;
      if (ALL_EVENTS.includes(e as EventName)) {
        // A real event, but only job lifecycle events are sent to webhooks.
        throw new ValidationError(`event ${e} is not delivered to webhooks; use one of ${JOB_EVENTS.join(", ")} or *`);
      }
      throw new ValidationError(`unknown event ${e}`);
    }
    const hook: Webhook = {
      id: randomUUID(),
      url: url.toString(),
      secret: input.secret ?? "whsec_" + randomBytes(24).toString("base64url"),
      events: input.events,
      queues: input.queues ?? [],
      enabled: true,
      createdAt: this.conveyor.clock.now(),
    };
    await this.conveyor.store.insertWebhook(hook);
    this.invalidate();
    return hook;
  }

  async remove(id: string): Promise<void> {
    if (!(await this.conveyor.store.deleteWebhook(id))) throw new NotFoundError(`webhook ${id} not found`);
    this.invalidate();
  }

  list(): Promise<Webhook[]> {
    return this.conveyor.store.listWebhooks();
  }

  async deliveries(id: string, limit = 50): Promise<WebhookDelivery[]> {
    if (!(await this.conveyor.store.getWebhook(id))) throw new NotFoundError(`webhook ${id} not found`);
    return this.conveyor.store.listDeliveries(id, limit);
  }
}
