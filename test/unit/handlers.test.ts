import { describe, expect, it } from "vitest";
import { HandlerRegistry } from "../../src/worker/handlers.js";

const fixture = (name: string) => new URL(`../fixtures/${name}`, import.meta.url).pathname;

describe("HandlerRegistry", () => {
  it("registers and looks up handlers by name", () => {
    const a = () => 1;
    const reg = new HandlerRegistry().register("b", () => 2).register("a", a);
    expect(reg.get("a")).toBe(a);
    expect(reg.get("missing")).toBeUndefined();
    expect(reg.names()).toEqual(["a", "b"]);
  });

  it("refuses to register a name twice", () => {
    const reg = new HandlerRegistry().register("a", () => 1);
    expect(() => reg.register("a", () => 2)).toThrow("handler a is already registered");
  });

  describe("fromModule", () => {
    it("loads the default export", async () => {
      const reg = await HandlerRegistry.fromModule(fixture("handlers-default.mjs"));
      expect(reg.names()).toEqual(["greet", "noop"]);
    });

    it("prefers a `handlers` export over the default export", async () => {
      const reg = await HandlerRegistry.fromModule(fixture("handlers-named.mjs"));
      expect(reg.names()).toEqual(["named"]);
    });

    it("rejects a module without a handler table", async () => {
      await expect(HandlerRegistry.fromModule(fixture("handlers-none.mjs"))).rejects.toThrow(/must export an object of handlers/);
      await expect(HandlerRegistry.fromModule(fixture("handlers-not-object.mjs"))).rejects.toThrow(/must export an object of handlers/);
    });

    it("rejects entries that are not functions", async () => {
      await expect(HandlerRegistry.fromModule(fixture("handlers-bad-entry.mjs"))).rejects.toThrow(/handler broken .* is not a function/);
    });

    it("fails clearly for a missing file", async () => {
      await expect(HandlerRegistry.fromModule(fixture("does-not-exist.mjs"))).rejects.toThrow();
    });
  });
});
