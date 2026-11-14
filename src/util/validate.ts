import { ValidationError } from "../core/errors.js";

type Obj = Record<string, unknown>;

export function asObject(value: unknown, name = "body"): Obj {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new ValidationError(`${name} must be an object`);
  }
  return value as Obj;
}

export function str(o: Obj, key: string, opts: { required?: boolean; max?: number; pattern?: RegExp } = {}): string | undefined {
  const v = o[key];
  if (v === undefined || v === null) {
    if (opts.required) throw new ValidationError(`${key} is required`);
    return undefined;
  }
  if (typeof v !== "string") throw new ValidationError(`${key} must be a string`);
  if (opts.required && v.length === 0) throw new ValidationError(`${key} must not be empty`);
  if (opts.max !== undefined && v.length > opts.max) {
    throw new ValidationError(`${key} must be at most ${opts.max} characters`);
  }
  if (opts.pattern && !opts.pattern.test(v)) throw new ValidationError(`${key} has an invalid format`);
  return v;
}

export function int(o: Obj, key: string, opts: { required?: boolean; min?: number; max?: number } = {}): number | undefined {
  const v = o[key];
  if (v === undefined || v === null) {
    if (opts.required) throw new ValidationError(`${key} is required`);
    return undefined;
  }
  if (typeof v !== "number" || !Number.isInteger(v)) throw new ValidationError(`${key} must be an integer`);
  if (opts.min !== undefined && v < opts.min) throw new ValidationError(`${key} must be >= ${opts.min}`);
  if (opts.max !== undefined && v > opts.max) throw new ValidationError(`${key} must be <= ${opts.max}`);
  return v;
}

export function bool(o: Obj, key: string): boolean | undefined {
  const v = o[key];
  if (v === undefined || v === null) return undefined;
  if (typeof v !== "boolean") throw new ValidationError(`${key} must be a boolean`);
  return v;
}

export function strArray(o: Obj, key: string): string[] | undefined {
  const v = o[key];
  if (v === undefined || v === null) return undefined;
  if (!Array.isArray(v) || v.some((x) => typeof x !== "string")) {
    throw new ValidationError(`${key} must be an array of strings`);
  }
  return v as string[];
}

export const NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/;
