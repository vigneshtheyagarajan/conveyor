import type { IncomingMessage, ServerResponse } from "node:http";
import type { ApiToken, Scope } from "../core/types.js";

export interface RequestContext {
  req: IncomingMessage;
  res: ServerResponse;
  method: string;
  path: string;
  params: Record<string, string>;
  query: URLSearchParams;
  body: unknown;
  token?: ApiToken;
  requestId: string;
}

export interface HandlerResult {
  status?: number;
  body?: unknown;
  headers?: Record<string, string>;
  /** Raw text body (e.g. metrics). */
  text?: string;
}

export type RouteHandler = (ctx: RequestContext) => Promise<HandlerResult | void>;

export interface Route {
  method: string;
  pattern: string;
  regex: RegExp;
  keys: string[];
  handler: RouteHandler;
  /** Required scope; undefined means public. */
  scope?: Scope;
}

export class Router {
  readonly routes: Route[] = [];

  add(method: string, pattern: string, handler: RouteHandler, scope?: Scope): this {
    const keys: string[] = [];
    const regex = new RegExp(
      "^" +
        pattern.replace(/\//g, "\\/").replace(/:(\w+)/g, (_, k: string) => {
          keys.push(k);
          return "([^/]+)";
        }) +
        "\\/?$",
    );
    this.routes.push({ method, pattern, regex, keys, handler, scope });
    return this;
  }

  get(p: string, h: RouteHandler, scope?: Scope) {
    return this.add("GET", p, h, scope);
  }
  post(p: string, h: RouteHandler, scope?: Scope) {
    return this.add("POST", p, h, scope);
  }
  put(p: string, h: RouteHandler, scope?: Scope) {
    return this.add("PUT", p, h, scope);
  }
  patch(p: string, h: RouteHandler, scope?: Scope) {
    return this.add("PATCH", p, h, scope);
  }
  delete(p: string, h: RouteHandler, scope?: Scope) {
    return this.add("DELETE", p, h, scope);
  }

  match(method: string, path: string): { route: Route; params: Record<string, string> } | { allowed: string[] } | undefined {
    const allowed: string[] = [];
    for (const route of this.routes) {
      const m = route.regex.exec(path);
      if (!m) continue;
      if (route.method !== method) {
        allowed.push(route.method);
        continue;
      }
      const params: Record<string, string> = {};
      route.keys.forEach((k, i) => (params[k] = decodeURIComponent(m[i + 1]!)));
      return { route, params };
    }
    return allowed.length ? { allowed } : undefined;
  }
}
