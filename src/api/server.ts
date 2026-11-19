import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import type { Broker } from "../broker/broker.js";
import type { Conveyor } from "../core/conveyor.js";
import { ConveyorError } from "../core/errors.js";
import type { Logger } from "../core/logger.js";
import type { MetricsRegistry } from "../metrics/registry.js";
import type { Scheduler } from "../scheduler/scheduler.js";
import type { WebhookDispatcher } from "../webhooks/dispatcher.js";
import { Authenticator, tokensExist } from "./auth.js";
import { registerScheduleRoutes, registerTokenRoutes, registerWebhookRoutes } from "./handlers/admin.js";
import { registerFlowRoutes } from "./handlers/flows.js";
import { registerJobRoutes } from "./handlers/jobs.js";
import { registerQueueRoutes } from "./handlers/queues.js";
import { registerWorkerRoutes } from "./handlers/workers.js";
import { Router, type HandlerResult, type RequestContext } from "./router.js";

const STATUS_BY_CODE: Record<string, number> = {
  validation_error: 400,
  unauthorized: 401,
  forbidden: 403,
  not_found: 404,
  conflict: 409,
  lease_lost: 409,
  queue_paused: 423,
};

export interface ApiServerOptions {
  conveyor: Conveyor;
  broker: Broker;
  scheduler: Scheduler;
  webhooks: WebhookDispatcher;
  metrics?: MetricsRegistry;
  maxBodyBytes?: number;
}

export class ApiServer {
  readonly router = new Router();
  private server?: Server;
  private auth: Authenticator;
  private logger: Logger;
  private maxBodyBytes: number;

  constructor(private opts: ApiServerOptions) {
    const { conveyor } = opts;
    this.logger = conveyor.logger.child({ component: "api" });
    this.auth = new Authenticator(conveyor.store, tokensExist(conveyor.store));
    this.maxBodyBytes = opts.maxBodyBytes ?? conveyor.config.maxPayloadBytes * 4;

    this.router.get("/health", async () => ({ body: { status: "ok", time: new Date().toISOString() } }));
    if (opts.metrics) {
      const metrics = opts.metrics;
      this.router.get("/metrics", async () => ({
        text: await metrics.render(),
        headers: { "content-type": "text/plain; version=0.0.4" },
      }));
    }
    registerJobRoutes(this.router, conveyor);
    registerFlowRoutes(this.router, conveyor);
    registerQueueRoutes(this.router, conveyor);
    registerWorkerRoutes(this.router, opts.broker);
    registerScheduleRoutes(this.router, opts.scheduler);
    registerWebhookRoutes(this.router, opts.webhooks);
    registerTokenRoutes(this.router, conveyor.store);
  }

  /** Handle one request. Exposed for tests that don't want a socket. */
  async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const requestId = (req.headers["x-request-id"] as string) || randomUUID();
    const started = Date.now();
    const url = new URL(req.url ?? "/", "http://localhost");
    const method = (req.method ?? "GET").toUpperCase();
    let status = 500;
    try {
      const matched = this.router.match(method, url.pathname);
      if (!matched) {
        status = 404;
        return this.send(res, 404, { error: { code: "not_found", message: "route not found" } }, requestId);
      }
      if ("allowed" in matched) {
        status = 405;
        res.setHeader("allow", matched.allowed.join(", "));
        return this.send(res, 405, { error: { code: "method_not_allowed", message: "method not allowed" } }, requestId);
      }
      const { route, params } = matched;
      const token = await this.auth.authenticate(req.headers.authorization, route.scope);
      const body = method === "GET" || method === "DELETE" ? undefined : await this.readJson(req);
      const ctx: RequestContext = { req, res, method, path: url.pathname, params, query: url.searchParams, body, token, requestId };
      const result: HandlerResult = (await route.handler(ctx)) ?? {};
      status = result.status ?? (result.body === undefined && result.text === undefined ? 204 : 200);
      for (const [k, v] of Object.entries(result.headers ?? {})) res.setHeader(k, v);
      if (result.text !== undefined) {
        res.writeHead(status, { "x-request-id": requestId });
        res.end(result.text);
        return;
      }
      this.send(res, status, result.body, requestId);
    } catch (err) {
      if (err instanceof ConveyorError) {
        status = STATUS_BY_CODE[err.code] ?? 400;
        this.send(res, status, { error: { code: err.code, message: err.message } }, requestId);
      } else if (err instanceof BodyError) {
        status = err.status;
        // The rest of an oversized upload is discarded; don't reuse the connection for it.
        if (status === 413) res.setHeader("connection", "close");
        this.send(res, status, { error: { code: "bad_request", message: err.message } }, requestId);
      } else {
        status = 500;
        this.logger.error("unhandled error", { err, requestId, path: url.pathname });
        this.send(res, 500, { error: { code: "internal", message: "internal server error" } }, requestId);
      }
    } finally {
      this.logger.debug("request", { method, path: url.pathname, status, ms: Date.now() - started, requestId });
    }
  }

  private send(res: ServerResponse, status: number, body: unknown, requestId: string): void {
    if (res.headersSent) return;
    if (status === 204 || body === undefined) {
      res.writeHead(status, { "x-request-id": requestId });
      res.end();
      return;
    }
    const data = JSON.stringify(body);
    res.writeHead(status, {
      "content-type": "application/json; charset=utf-8",
      "content-length": Buffer.byteLength(data),
      "x-request-id": requestId,
    });
    res.end(data);
  }

  private readJson(req: IncomingMessage): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      let size = 0;
      let tooLarge = false;
      const refuse = () => {
        tooLarge = true;
        chunks.length = 0;
        // Keep reading (and discarding) the upload rather than destroying the socket, so the
        // client gets the 413 response instead of a connection reset.
        req.resume();
        reject(new BodyError(413, `body exceeds ${this.maxBodyBytes} bytes`));
      };
      if (Number(req.headers["content-length"]) > this.maxBodyBytes) refuse();
      req.on("data", (chunk: Buffer) => {
        if (tooLarge) return;
        size += chunk.length;
        if (size > this.maxBodyBytes) return refuse();
        chunks.push(chunk);
      });
      req.on("end", () => {
        if (tooLarge) return;
        if (size === 0) return resolve(undefined);
        const type = req.headers["content-type"] ?? "";
        if (!type.includes("application/json")) {
          return reject(new BodyError(415, "content-type must be application/json"));
        }
        try {
          resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
        } catch {
          reject(new BodyError(400, "invalid JSON body"));
        }
      });
      req.on("error", reject);
    });
  }

  listen(port: number, host = "127.0.0.1"): Promise<AddressInfo> {
    this.server = createServer((req, res) => {
      void this.handle(req, res);
    });
    return new Promise((resolve, reject) => {
      this.server!.once("error", reject);
      this.server!.listen(port, host, () => resolve(this.server!.address() as AddressInfo));
    });
  }

  close(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.server) return resolve();
      this.server.close((err) => (err ? reject(err) : resolve()));
      this.server.closeAllConnections?.();
    });
  }
}

class BodyError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}
