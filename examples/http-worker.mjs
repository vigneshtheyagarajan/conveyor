// A worker in plain JavaScript that talks to `conveyor serve` over HTTP.
//   CONVEYOR_URL=http://127.0.0.1:7420 CONVEYOR_TOKEN=... node examples/http-worker.mjs
const base = process.env.CONVEYOR_URL ?? "http://127.0.0.1:7420";
const headers = { "content-type": "application/json" };
if (process.env.CONVEYOR_TOKEN) headers.authorization = `Bearer ${process.env.CONVEYOR_TOKEN}`;
const workerId = `http-worker-${process.pid}`;

async function call(path, body) {
  const res = await fetch(base + path, { method: "POST", headers, body: JSON.stringify(body) });
  if (res.status === 204) return null;
  const data = await res.json();
  if (!res.ok) throw new Error(`${path}: ${res.status} ${JSON.stringify(data)}`);
  return data;
}

for (;;) {
  const lease = await call("/v1/leases", { queues: ["default"], workerId });
  if (!lease) {
    await new Promise((r) => setTimeout(r, 1000));
    continue;
  }
  const { job, leaseToken } = lease;
  console.log("processing", job.id, job.name);
  try {
    await call(`/v1/jobs/${job.id}/complete`, { leaseToken, result: { handledBy: workerId } });
  } catch (err) {
    await call(`/v1/jobs/${job.id}/fail`, { leaseToken, error: String(err) });
  }
}
