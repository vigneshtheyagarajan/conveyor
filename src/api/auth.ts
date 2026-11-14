import { createHash, randomUUID } from "node:crypto";
import type { Store } from "../storage/store.js";
import type { ApiToken, Scope } from "../core/types.js";
import { ALL_SCOPES } from "../core/types.js";
import { ForbiddenError, UnauthorizedError, ValidationError } from "../core/errors.js";
import { apiTokenSecret } from "../util/ids.js";

export function hashToken(secret: string): string {
  return createHash("sha256").update(secret).digest("hex");
}

export async function createToken(
  store: Store,
  name: string,
  scopes: Scope[],
  now = Date.now(),
): Promise<{ token: ApiToken; secret: string }> {
  if (!name) throw new ValidationError("token name is required");
  if (scopes.length === 0) throw new ValidationError("at least one scope is required");
  for (const s of scopes) {
    if (!ALL_SCOPES.includes(s)) throw new ValidationError(`unknown scope ${s}`);
  }
  const secret = apiTokenSecret();
  const token: ApiToken = {
    id: randomUUID(),
    name,
    hash: hashToken(secret),
    scopes,
    createdAt: now,
    lastUsedAt: null,
    revokedAt: null,
  };
  await store.insertToken(token);
  return { token, secret };
}

export function hasScope(token: ApiToken, scope: Scope): boolean {
  return token.scopes.includes("admin") || token.scopes.includes(scope);
}

const TOUCH_INTERVAL_MS = 60_000;

export class Authenticator {
  constructor(
    private store: Store,
    /** When false (no tokens configured), every request is allowed. */
    private required: () => Promise<boolean>,
  ) {}

  async authenticate(header: string | undefined, scope: Scope | undefined, now = Date.now()): Promise<ApiToken | undefined> {
    if (!scope) return undefined;
    if (!(await this.required())) return undefined;
    if (!header?.startsWith("Bearer ")) throw new UnauthorizedError();
    const secret = header.slice("Bearer ".length).trim();
    const token = await this.store.findTokenByHash(hashToken(secret));
    if (!token || token.revokedAt) throw new UnauthorizedError();
    if (!hasScope(token, scope)) throw new ForbiddenError(`token lacks scope ${scope}`);
    if (!token.lastUsedAt || now - token.lastUsedAt > TOUCH_INTERVAL_MS) {
      await this.store.touchToken(token.id, now);
    }
    return token;
  }
}

/** Auth is enforced once at least one active token exists. */
export function tokensExist(store: Store): () => Promise<boolean> {
  let cached: { at: number; value: boolean } | undefined;
  return async () => {
    if (cached?.value) return true;
    const now = Date.now();
    if (cached && now - cached.at < 2000) return cached.value;
    const tokens = await store.listTokens();
    cached = { at: now, value: tokens.some((t) => !t.revokedAt) };
    return cached.value;
  };
}
