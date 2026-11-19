import { randomBytes, randomUUID } from "node:crypto";
import type { Conveyor } from "../core/conveyor.js";
import { NotFoundError, ValidationError } from "../core/errors.js";
import type { EventName } from "../core/events.js";
import { ALL_EVENTS } from "../core/events.js";
import type { Logger } from "../core/logger.js";
import { flowStatus, stateCounts } from "../core/flow.js";
import type { FlowOutcome, Job, Webhook, WebhookDelivery } from "../core/types.js";
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

const FLOW_EVENTS = ["flow.completed", "flow.failed"] as const satisfies readonly EventName[];

/** The events a webhook can subscribe to (or "*" for all of them). */
export const WEBHOOK_EVENTS: readonly EventName[] = [...JOB_EVENTS, ...FLOW_EVENTS];

export interface WebhookDispatcherOptions {
  /** How often to look for flow verdicts to deliver, recorded by this or any other process. Default 1000. */
  flowPollMs?: number;
}

// Delivers job lifecycle events and flow verdicts to registered HTTP endpoints.
//
// Job events are delivered from this process's event bus, so only changes made in this process
// are sent (KB-23). Flow verdicts are delivered from the database instead: the transition that
// decides a verdict records it in flow_events, in the same transaction, whichever process it
// runs in, and every dispatcher polls for unclaimed verdicts. A dispatcher claims a verdict and
// stores its deliveries in one transaction, so each verdict is delivered once, however many
// processes record or dispatch.
export class WebhookDispatcher {
  private offs: (() => void)[] = [];
  private logger: Logger;
  private inflight = new Set<Promise<void>>();
  private retryTimers = new Set<NodeJS.Timeout>();
  private cache?: { at: number; hooks: Webhook[] };
  private flowTimer?: NodeJS.Timeout;
  private polling = false;
  private pollAgain = false;

  constructor(
    private conveyor: Conveyor,
    private fetchImpl: FetchLike = fetch,
    private opts: WebhookDispatcherOptions = {},
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
    // A verdict recorded in this process is picked up at once; others at the next poll.
    for (const event of FLOW_EVENTS) this.offs.push(this.conveyor.events.on(event, () => this.pollFlows()));
    this.flowTimer = setInterval(() => this.pollFlows(), this.opts.flowPollMs ?? 1000);
    this.flowTimer.unref();
    this.track(this.resumePending());
    this.pollFlows();
  }

  /** Start delivering pending flow verdicts, unless that is already running (then run again after it). */
  private pollFlows(): void {
    if (this.polling) {
      this.pollAgain = true;
      return;
    }
    this.polling = true;
    this.track(
      this.deliverFlowOutcomes()
        .then(() => undefined)
        .finally(() => {
          this.polling = false;
          if (this.pollAgain) {
            this.pollAgain = false;
            this.pollFlows();
          }
        }),
    );
  }

  /**
   * Deliver every flow verdict no dispatcher has claimed yet. Returns how many this call
   * claimed; a verdict another dispatcher claimed first is left to it.
   */
  async deliverFlowOutcomes(): Promise<number> {
    const batch = 100;
    let claimed = 0;
    for (;;) {
      const pending = await this.conveyor.store.listPendingFlowOutcomes(batch);
      for (const outcome of pending) if (await this.deliverFlowOutcome(outcome)) claimed++;
      if (pending.length < batch) return claimed;
    }
  }

  private async deliverFlowOutcome(outcome: FlowOutcome): Promise<boolean> {
    const { store, clock } = this.conveyor;
    const jobs = await store.listFlowJobs(outcome.flowId);
    const queues = new Set(jobs.map((j) => j.queue));
    // A webhook limited to some queues gets a flow's events if any of its jobs is in one.
    const hooks = (await this.hooks()).filter(
      (h) =>
        h.enabled &&
        (h.events.includes("*") || h.events.includes(outcome.event)) &&
        (h.queues.length === 0 || h.queues.some((q) => queues.has(q))),
    );
    const now = clock.now();
    const body = JSON.stringify({
      id: randomUUID(),
      event: outcome.event,
      createdAt: new Date(now).toISOString(),
      data: {
        id: outcome.flowId,
        reason: outcome.reason,
        jobId: outcome.jobId,
        at: new Date(outcome.at).toISOString(),
        status: flowStatus(jobs),
        counts: stateCounts(jobs),
        jobs: jobs.map((j) => ({ id: j.id, key: j.flowKey, queue: j.queue, name: j.name, state: j.state })),
      },
    });
    const deliveries = hooks.map((hook) => newDelivery(hook.id, outcome.event, body, now));
    if (!(await store.claimFlowOutcome(outcome.flowId, now, deliveries))) return false;
    await Promise.all(deliveries.map((d, i) => this.attempt(hooks[i]!, d)));
    return true;
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
    if (this.flowTimer) clearInterval(this.flowTimer);
    this.flowTimer = undefined;
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
        const delivery = newDelivery(hook.id, event, body, now);
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
      if (e === "*" || (WEBHOOK_EVENTS as readonly string[]).includes(e)) continue;
      if (ALL_EVENTS.includes(e as EventName)) {
        // A real event, but only job lifecycle events and flow verdicts are sent to webhooks.
        throw new ValidationError(`event ${e} is not delivered to webhooks; use one of ${WEBHOOK_EVENTS.join(", ")} or *`);
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

function newDelivery(webhookId: string, event: string, body: string, now: number): WebhookDelivery {
  return {
    id: randomUUID(),
    webhookId,
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
}
