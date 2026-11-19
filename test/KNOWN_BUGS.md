# Known bugs (open)

Open bugs found while building the test suite that are deliberately **not fixed** yet. Each one
that can be tested has an `it.fails(...)` test tagged `[KB-n]` and a `BUG(KB-n)` comment.
`it.fails` passes while the bug exists. When someone fixes the bug the test goes red. At that
point change `it.fails` to `it`, keep it as a regression test, and delete the entry here.

IDs are never reused. Fixed bugs are removed from this file (their tests stay, marked
"Regression (was KB-n)"). Find everything with `grep -rn "KB-" test/`.

| ID | Area | Summary | Owner | Test |
| --- | --- | --- | --- | --- |
| KB-2 | API auth | Revoking every token turns authentication off | security work | `test/unit/auth.test.ts` |
| KB-9 | Webhooks | `verify()` compares signatures with `===`, not in constant time | security work | none (see below) |
| KB-23 | CLI worker | Jobs run by `conveyor worker` never produce webhooks | design change (cross-process events) | `test/integration/cli-e2e.test.ts` |
| KB-24 | API auth | Right after the first token is created through the API, auth stays off for up to 2s | security work | `test/integration/api-auth.test.ts` |

## Details

**KB-2. Auth switches off after every token is revoked.** `tokensExist()` only enforces auth
while an unrevoked token exists. The server process remembers a positive answer, but after a
restart (or in a process that has not seen a token yet) revoking the last token, for example
after a leak, leaves every endpoint open, including `POST /v1/tokens`.

**KB-9. Non-constant-time signature check.** `verify()` should use `crypto.timingSafeEqual`
on equal-length buffers. Not unit-tested: a timing difference isn't something a unit test can
reliably observe.

**KB-23. CLI workers don't trigger webhooks.** Events go over an in-process `EventBus`, and only
`serve` starts a `WebhookDispatcher`. A job completed, retried or dead-lettered by a
`conveyor worker` process (and a job enqueued with `conveyor enqueue`) therefore never produces a
webhook. HTTP workers are not affected, because their calls are handled inside `serve`. The
end-to-end test runs a real worker and waits 3 seconds for a `job.completed` delivery that never
arrives.

This needs a cross-process event path, which is a design change rather than a test fix. Options:

1. **Deliver from the database in `serve`.** Every lifecycle change already writes a
   `job_events` row in the same process that makes the change. `serve` could tail `job_events`
   (poll for rows after the last id it handled, remembered in the database) and turn them into
   webhook deliveries. Deliveries then come from one place, and nothing is lost while `serve`
   is down; it catches up on start. Costs: a polling delay (up to the poll interval), a new
   cursor table or column, and `job_events` rows must carry enough data (queue, state, result,
   last error) to build the webhook body. Today some are `null`.
2. **Outbox table.** Like option 1, but each process inserts webhook-relevant events into a
   dedicated `event_outbox` table in the same transaction as the job change. `serve` delivers
   and deletes them. It keeps `job_events` as history only and makes the delivery contract
   explicit, at the cost of a new table and of writing inside the existing store transactions.
3. **A dispatcher in every process.** Start a `WebhookDispatcher` in `conveyor worker` (and for
   `conveyor enqueue`) too. Simplest change, but every process needs network access to webhook
   targets. Deliveries stop when a short-lived process exits, so it must wait for them. With
   KB-18's resume-on-start, several processes could also pick up the same pending delivery.
4. **Document it.** State that webhooks only fire for changes made through `serve` (API
   and HTTP workers) and recommend HTTP workers when webhooks matter.

Option 1 or 2 matches the existing architecture (SQLite as the shared medium between
processes) best. Metrics have the same limitation: counters only see events in their own process.

**KB-24. First token through the API.** `tokensExist()` caches "no tokens" for 2 seconds, and
`POST /v1/tokens` doesn't clear that cache. So for up to 2 seconds after the first token is
created, every endpoint, including token creation, still answers without authentication.
