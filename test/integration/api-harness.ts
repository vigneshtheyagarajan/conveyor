// A real ApiServer on 127.0.0.1 (random port) over an in-memory store with a ManualClock.
// The API is tested over real HTTP because status codes, headers and body handling are the
// contract. No sleeps.
import { expect } from "vitest";
import { ApiServer } from "../../src/api/server.js";
import { createToken } from "../../src/api/auth.js";
import type { Scope } from "../../src/core/types.js";
import type { ConveyorConfig } from "../../src/core/config.js";
import type { Logger } from "../../src/core/logger.js";
import { conveyorMetrics } from "../../src/metrics/conveyor-metrics.js";
import { Scheduler } from "../../src/scheduler/scheduler.js";
import { WebhookDispatcher, type FetchLike } from "../../src/webhooks/dispatcher.js";
import { manualClock, setup } from "../helpers.js";

export interface ApiResponse<T = any> {
  status: number;
  headers: Headers;
  body: T;
  text: string;
}

export interface HarnessOptions {
  /** Create these tokens before the first request, so auth is enforced from the start. */
  tokens?: Record<string, Scope[]>;
  config?: Partial<ConveyorConfig>;
  maxBodyBytes?: number;
  metrics?: boolean;
  fetchImpl?: FetchLike;
  logger?: Logger;
}

export async function startApi(opts: HarnessOptions = {}) {
  const clock = manualClock();
  const s = await setup({ clock, config: opts.config, logger: opts.logger });
  const scheduler = new Scheduler(s.conveyor);
  const webhooks = new WebhookDispatcher(s.conveyor, opts.fetchImpl ?? (async () => new Response("ok")));
  const secrets: Record<string, string> = {};
  for (const [name, scopes] of Object.entries(opts.tokens ?? {})) {
    secrets[name] = (await createToken(s.store, name, scopes, clock.now())).secret;
  }
  const api = new ApiServer({
    conveyor: s.conveyor,
    broker: s.broker,
    scheduler,
    webhooks,
    metrics: opts.metrics ? conveyorMetrics(s.conveyor) : undefined,
    maxBodyBytes: opts.maxBodyBytes,
  });
  const { port } = await api.listen(0);
  const base = `http://127.0.0.1:${port}`;

  /** Send a request. `body` is JSON-encoded unless it is a string. `as` picks a token by name. */
  async function req<T = any>(
    method: string,
    path: string,
    init: { body?: unknown; as?: string; token?: string; headers?: Record<string, string> } = {},
  ): Promise<ApiResponse<T>> {
    const headers: Record<string, string> = { ...init.headers };
    const token = init.token ?? (init.as ? secrets[init.as] : undefined);
    if (token) headers.authorization = `Bearer ${token}`;
    let body: string | undefined;
    if (init.body !== undefined && method !== "GET" && method !== "HEAD") {
      body = typeof init.body === "string" ? init.body : JSON.stringify(init.body);
      headers["content-type"] ??= "application/json";
    }
    const res = await fetch(base + path, { method, headers, body });
    const text = await res.text();
    let parsed: unknown = text;
    try {
      parsed = text ? JSON.parse(text) : undefined;
    } catch {
      // not JSON (e.g. /metrics)
    }
    return { status: res.status, headers: res.headers, body: parsed as T, text };
  }

  return { ...s, clock, scheduler, webhooks, api, base, secrets, req, close: () => api.close() };
}

export type Api = Awaited<ReturnType<typeof startApi>>;

/** The error body every failing endpoint must return. */
export function errorBody(code: string, message?: string | RegExp) {
  return {
    error: {
      code,
      message: message === undefined ? expect.any(String) : typeof message === "string" ? message : expect.stringMatching(message),
    },
  };
}
