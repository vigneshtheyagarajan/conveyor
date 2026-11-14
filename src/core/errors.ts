export class ConveyorError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
    this.name = new.target.name;
  }
}

export class ValidationError extends ConveyorError {
  constructor(message: string) {
    super("validation_error", message);
  }
}

export class NotFoundError extends ConveyorError {
  constructor(message: string) {
    super("not_found", message);
  }
}

export class ConflictError extends ConveyorError {
  constructor(message: string) {
    super("conflict", message);
  }
}

export class LeaseLostError extends ConveyorError {
  constructor(jobId: string) {
    super("lease_lost", `lease for job ${jobId} is no longer held`);
  }
}

export class UnauthorizedError extends ConveyorError {
  constructor(message = "missing or invalid API token") {
    super("unauthorized", message);
  }
}

export class ForbiddenError extends ConveyorError {
  constructor(message = "token does not have the required scope") {
    super("forbidden", message);
  }
}

export class QueuePausedError extends ConveyorError {
  constructor(queue: string) {
    super("queue_paused", `queue ${queue} is paused`);
  }
}

/** Thrown by job handlers to fail a job without retrying it. */
export class UnrecoverableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnrecoverableError";
  }
}
