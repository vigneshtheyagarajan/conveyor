import { afterEach, describe, expect, it, vi } from "vitest";
import { Authenticator, createToken, hashToken, hasScope, tokensExist } from "../../src/api/auth.js";
import { ForbiddenError, UnauthorizedError, ValidationError } from "../../src/core/errors.js";
import type { ApiToken, Scope } from "../../src/core/types.js";
import type { Store } from "../../src/storage/store.js";

const T = Date.UTC(2026, 0, 5, 12, 0, 0);

/** Just the token methods of a Store, in memory. */
function tokenStore(initial: ApiToken[] = []) {
  const tokens = [...initial];
  const touched: [string, number][] = [];
  const store = {
    insertToken: async (t: ApiToken) => void tokens.push(t),
    findTokenByHash: async (hash: string) => tokens.find((t) => t.hash === hash),
    listTokens: async () => [...tokens],
    touchToken: async (id: string, now: number) => void touched.push([id, now]),
    revokeToken: async (id: string, now: number) => {
      const t = tokens.find((x) => x.id === id && !x.revokedAt);
      if (t) t.revokedAt = now;
      return Boolean(t);
    },
  };
  return { store: store as unknown as Store, tokens, touched };
}

function token(secret: string, scopes: Scope[], extra: Partial<ApiToken> = {}): ApiToken {
  return { id: `id-${secret}`, name: secret, hash: hashToken(secret), scopes, createdAt: T, lastUsedAt: null, revokedAt: null, ...extra };
}

describe("hashToken", () => {
  it("is the hex SHA-256 of the secret", () => {
    expect(hashToken("abc")).toBe("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  });
});

describe("hasScope", () => {
  it("grants a scope the token has, and everything to admin", () => {
    expect(hasScope(token("a", ["jobs:read"]), "jobs:read")).toBe(true);
    expect(hasScope(token("a", ["jobs:read"]), "jobs:write")).toBe(false);
    expect(hasScope(token("a", ["jobs:write"]), "admin")).toBe(false);
    for (const s of ["jobs:read", "jobs:write", "workers", "admin"] as Scope[]) {
      expect(hasScope(token("a", ["admin"]), s)).toBe(true);
    }
  });
});

describe("createToken", () => {
  it("stores only the hash and returns the secret once", async () => {
    const { store, tokens } = tokenStore();
    const { token: t, secret } = await createToken(store, "ops", ["jobs:read", "workers"], T);
    expect(secret).toMatch(/^cvy_/);
    expect(tokens).toEqual([t]);
    expect(t).toMatchObject({ name: "ops", hash: hashToken(secret), scopes: ["jobs:read", "workers"], createdAt: T, lastUsedAt: null, revokedAt: null });
    expect(JSON.stringify(t)).not.toContain(secret);
  });

  it("validates name and scopes", async () => {
    const { store, tokens } = tokenStore();
    await expect(createToken(store, "", ["admin"])).rejects.toThrow(ValidationError);
    await expect(createToken(store, "x", [])).rejects.toThrow("at least one scope is required");
    await expect(createToken(store, "x", ["root" as Scope])).rejects.toThrow("unknown scope root");
    expect(tokens).toEqual([]);
  });
});

describe("Authenticator", () => {
  const required = async () => true;

  it("lets public routes through without looking at the header", async () => {
    const { store } = tokenStore();
    await expect(new Authenticator(store, required).authenticate(undefined, undefined, T)).resolves.toBeUndefined();
  });

  it("lets everything through while auth is not required", async () => {
    const { store } = tokenStore();
    await expect(new Authenticator(store, async () => false).authenticate(undefined, "admin", T)).resolves.toBeUndefined();
  });

  it.each([
    ["no header", undefined],
    ["a non-Bearer scheme", "Basic abc"],
    ["an empty bearer token", "Bearer "],
    ["an unknown secret", "Bearer cvy_nope"],
  ])("rejects %s with 401", async (_label, header) => {
    const { store } = tokenStore([token("good", ["admin"])]);
    await expect(new Authenticator(store, required).authenticate(header, "jobs:read", T)).rejects.toThrow(UnauthorizedError);
  });

  it("rejects a revoked token with 401", async () => {
    const { store } = tokenStore([token("old", ["admin"], { revokedAt: T - 1 })]);
    await expect(new Authenticator(store, required).authenticate("Bearer old", "jobs:read", T)).rejects.toThrow(UnauthorizedError);
  });

  it("rejects a token without the scope with 403", async () => {
    const { store } = tokenStore([token("reader", ["jobs:read"])]);
    await expect(new Authenticator(store, required).authenticate("Bearer reader", "jobs:write", T)).rejects.toThrow(
      new ForbiddenError("token lacks scope jobs:write"),
    );
  });

  it("returns the token for a valid secret, ignoring surrounding spaces", async () => {
    const t = token("good", ["jobs:write"]);
    const { store } = tokenStore([t]);
    await expect(new Authenticator(store, required).authenticate("Bearer  good ", "jobs:write", T)).resolves.toEqual(t);
  });

  it("records use at most once a minute", async () => {
    const { store, touched } = tokenStore([token("a", ["admin"]), token("b", ["admin"], { lastUsedAt: T - 30_000 }), token("c", ["admin"], { lastUsedAt: T - 60_001 })]);
    const auth = new Authenticator(store, required);
    for (const s of ["a", "b", "c"]) await auth.authenticate(`Bearer ${s}`, "admin", T);
    expect(touched).toEqual([
      ["id-a", T],
      ["id-c", T],
    ]);
  });

  // BUG(KB-2): auth is only enforced while an unrevoked token exists. Revoking the last token
  // (say, after a leak) turns authentication off entirely on the next restart, or within 2s
  // for a server that has not yet seen a token.
  it.fails("keeps requiring a token after every token has been revoked [KB-2]", async () => {
    const { store } = tokenStore([token("leaked", ["admin"], { revokedAt: T })]);
    const auth = new Authenticator(store, tokensExist(store));
    await expect(auth.authenticate(undefined, "admin", T)).rejects.toThrow(UnauthorizedError);
  });
});

describe("tokensExist", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("is false with no tokens and true once an active token exists", async () => {
    const { store, tokens } = tokenStore();
    expect(await tokensExist(store)()).toBe(false);
    tokens.push(token("a", ["admin"]));
    expect(await tokensExist(store)()).toBe(true);
  });

  it("caches a negative answer for 2 seconds", async () => {
    vi.useFakeTimers({ now: T });
    const { store, tokens } = tokenStore();
    const exists = tokensExist(store);
    expect(await exists()).toBe(false);
    tokens.push(token("a", ["admin"]));
    vi.setSystemTime(T + 1999);
    expect(await exists()).toBe(false);
    vi.setSystemTime(T + 2000);
    expect(await exists()).toBe(true);
  });

  it("stays true for the life of the process once tokens have been seen", async () => {
    const { store, tokens } = tokenStore([token("a", ["admin"])]);
    const exists = tokensExist(store);
    expect(await exists()).toBe(true);
    tokens[0]!.revokedAt = T;
    expect(await exists()).toBe(true);
  });
});
