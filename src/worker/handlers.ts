import type { Job } from "../core/types.js";
import type { Logger } from "../core/logger.js";
import type { DependencyResult } from "../core/dependencies.js";
import type { Json } from "../util/json.js";

export interface JobContext<P = Json> {
  job: Job & { payload: P };
  /** Aborted when the job times out, is cancelled, or the worker is shutting down hard. */
  signal: AbortSignal;
  logger: Logger;
  /** Report progress between 0 and 1. */
  progress(value: number): Promise<void>;
  /** Extend the lease immediately (normally done automatically). */
  heartbeat(): Promise<void>;
  /**
   * The jobs this job depended on, with their results, ordered by id. Loaded on the first call
   * (of this or dependency()) and cached for the rest of the attempt. A dependency with the
   * `ignore` policy that failed (dead or cancelled) is left out; with `continue` it is included,
   * with its state and error.
   */
  dependencies(): Promise<DependencyResult[]>;
  /**
   * The result of one dependency, by its key in the flow or its job id. Throws a DependencyError
   * if that dependency didn't complete (whatever its policy), or there is no such dependency.
   */
  dependency<R = Json>(key: string): Promise<R>;
}

export type JobHandler<P = Json, R = Json | void> = (ctx: JobContext<P>) => Promise<R> | R;

export class HandlerRegistry {
  private handlers = new Map<string, JobHandler<any, any>>();

  register<P = Json, R = Json | void>(name: string, handler: JobHandler<P, R>): this {
    if (this.handlers.has(name)) throw new Error(`handler ${name} is already registered`);
    this.handlers.set(name, handler as JobHandler<any, any>);
    return this;
  }

  get(name: string): JobHandler<any, any> | undefined {
    return this.handlers.get(name);
  }

  names(): string[] {
    return [...this.handlers.keys()].sort();
  }

  static async fromModule(path: string): Promise<HandlerRegistry> {
    const { pathToFileURL } = await import("node:url");
    const { resolve } = await import("node:path");
    const mod = await import(pathToFileURL(resolve(path)).href);
    const table = (mod.handlers ?? mod.default) as Record<string, JobHandler> | undefined;
    if (!table || typeof table !== "object") {
      throw new Error(`${path} must export an object of handlers (default or "handlers")`);
    }
    const registry = new HandlerRegistry();
    for (const [name, fn] of Object.entries(table)) {
      if (typeof fn !== "function") throw new Error(`handler ${name} in ${path} is not a function`);
      registry.register(name, fn);
    }
    return registry;
  }
}
