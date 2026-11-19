// Functions run inside a worker thread by the thread-runner tests.
import { threadId } from "node:worker_threads";

export default function double(payload) {
  return { doubled: payload.n * 2, threadId };
}

export function sum(payload) {
  return payload.values.reduce((a, b) => a + b, 0);
}

export async function later(payload) {
  await new Promise((r) => setTimeout(r, 5));
  return `later:${payload}`;
}

export function nothing() {
  return undefined;
}

export function fail() {
  throw new Error("thread boom");
}

export function throwString() {
  throw "plain string";
}

export function forever() {
  return new Promise(() => setInterval(() => {}, 1000));
}

export function exitHard() {
  process.exit(3);
}

export function unclonable() {
  return () => "functions cannot be posted";
}

export const notAFunction = 42;

export function crashLater() {
  setTimeout(() => {
    throw new Error("uncaught in thread");
  }, 0);
  return new Promise(() => {});
}

/** Reports exactly what it was called with. */
export function args(...received) {
  return { count: received.length, received };
}
