// npx tsx src/cli/index.ts worker --handlers examples/handlers.ts --queues default,email
import { setTimeout as delay } from "node:timers/promises";
import { UnrecoverableError, type JobHandler } from "../src/index.js";

const sendEmail: JobHandler<{ to: string; subject: string }> = async ({ job, logger }) => {
  if (!job.payload?.to?.includes("@")) throw new UnrecoverableError("invalid recipient");
  await delay(100);
  logger.info("email sent", { to: job.payload.to });
  return { messageId: `msg_${job.id}` };
};

const resizeImage: JobHandler<{ url: string; sizes: number[] }> = async ({ job, progress, signal }) => {
  const sizes = job.payload.sizes ?? [128, 512];
  for (let i = 0; i < sizes.length; i++) {
    if (signal.aborted) throw signal.reason;
    await delay(200);
    await progress((i + 1) / sizes.length);
  }
  return { variants: sizes.map((s) => `${job.payload.url}?w=${s}`) };
};

/** Fails the first two attempts to demonstrate retries. */
const flaky: JobHandler = async ({ job }) => {
  if (job.attempts < 3) throw new Error(`transient failure on attempt ${job.attempts}`);
  return { ok: true };
};

const slow: JobHandler<{ ms?: number }> = async ({ job, signal }) => {
  await delay(job.payload?.ms ?? 5000, undefined, { signal });
  return null;
};

export default {
  "send-email": sendEmail,
  "resize-image": resizeImage,
  flaky,
  slow,
};
