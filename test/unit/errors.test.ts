import { describe, expect, it } from "vitest";
import {
  ConflictError,
  ConveyorError,
  ForbiddenError,
  LeaseLostError,
  NotFoundError,
  QueuePausedError,
  UnauthorizedError,
  UnrecoverableError,
  ValidationError,
} from "../../src/core/errors.js";

describe("errors", () => {
  it.each([
    [new ValidationError("bad"), "validation_error", "ValidationError", "bad"],
    [new NotFoundError("gone"), "not_found", "NotFoundError", "gone"],
    [new ConflictError("clash"), "conflict", "ConflictError", "clash"],
    [new LeaseLostError("j1"), "lease_lost", "LeaseLostError", "lease for job j1 is no longer held"],
    [new UnauthorizedError(), "unauthorized", "UnauthorizedError", "missing or invalid API token"],
    [new ForbiddenError(), "forbidden", "ForbiddenError", "token does not have the required scope"],
    [new QueuePausedError("q"), "queue_paused", "QueuePausedError", "queue q is paused"],
  ])("%s has code %s", (err, code, name, message) => {
    expect(err).toBeInstanceOf(ConveyorError);
    expect(err).toBeInstanceOf(Error);
    expect(err.code).toBe(code);
    expect(err.name).toBe(name);
    expect(err.message).toBe(message);
  });

  it("allows custom messages for auth errors", () => {
    expect(new UnauthorizedError("nope").message).toBe("nope");
    expect(new ForbiddenError("token lacks scope admin").message).toBe("token lacks scope admin");
  });

  it("UnrecoverableError is a plain Error, not a ConveyorError", () => {
    const e = new UnrecoverableError("give up");
    expect(e).toBeInstanceOf(Error);
    expect(e).not.toBeInstanceOf(ConveyorError);
    expect(e.name).toBe("UnrecoverableError");
    expect(e.message).toBe("give up");
  });
});
