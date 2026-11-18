# Known bugs (open)

Open bugs found while building the test suite that are deliberately **not fixed** yet. Each one
that can be tested has an `it.fails(...)` test tagged `[KB-n]` and a `BUG(KB-n)` comment.
`it.fails` passes while the bug exists. When someone fixes the bug the test goes red. At that
point change `it.fails` to `it`, keep it as a regression test, and delete the entry here.

IDs are never reused. Fixed bugs are removed from this file (their tests stay, marked
"Regression (was KB-n)"). Find everything with `grep -rn "KB-" test/`.

| ID | Area | Summary | Owner | Test |
| --- | --- | --- | --- | --- |
| KB-1 | Reaper | Expired leases are requeued without checking `maxAttempts`, so a job that crashes its worker retries forever | concurrency work | `test/integration/maintenance.test.ts` |
| KB-2 | API auth | Revoking every token turns authentication off | security work | `test/unit/auth.test.ts` |
| KB-9 | Webhooks | `verify()` compares signatures with `===`, not in constant time | security work | none (see below) |
| KB-11 | Store/broker | Two concurrent claims can both lease the same job: `activateJob` has no `state = 'waiting'` guard | concurrency work | `test/integration/store-contract.ts` |
| KB-12 | Broker | A queue's `concurrency` limit is exceeded by parallel claims (count, then activate) | concurrency work | `test/integration/broker.test.ts` |
| KB-13 | Store | `cancelJob` (and the reaper's `requeueJob`) update by id only, so they can overwrite a job that finished in between | concurrency work | `test/integration/store-contract.ts` |
| KB-14 | Store | Nothing in the database enforces `uniqueKey`, so concurrent enqueues can both insert | concurrency work | `test/integration/store-contract.ts` |
| KB-16 | Migrations | Suspected: two processes migrating a fresh database at the same time can fail | concurrency work | none (see below) |
| KB-17 | Broker | A rate-limited queue blocks every other queue in the same claim | concurrency work | `test/integration/broker.test.ts` |
| KB-23 | CLI worker | Jobs run by `conveyor worker` never produce webhooks | design change (cross-process events) | `test/integration/cli-e2e.test.ts` |
| KB-24 | API auth | Right after the first token is created through the API, auth stays off for up to 2s | security work | `test/integration/api-auth.test.ts` |

## Details

**KB-1. Reaper ignores `maxAttempts`.** `Maintenance.reap()` calls `requeueJob(id, now, false)`
for every expired lease. The intended behaviour is that a job with no attempts left goes to
`dead`. The test shows a job with `maxAttempts: 2` still `waiting` after two expired leases.

**KB-2. Auth switches off after every token is revoked.** `tokensExist()` only enforces auth
while an unrevoked token exists. The server process remembers a positive answer, but after a
restart (or in a process that has not seen a token yet) revoking the last token, for example
after a leak, leaves every endpoint open, including `POST /v1/tokens`.

**KB-9. Non-constant-time signature check.** `verify()` should use `crypto.timingSafeEqual`
on equal-length buffers. Not unit-tested: a timing difference isn't something a unit test can
reliably observe.

**KB-11, KB-12, KB-13, KB-14. Races between claims, cancels, the reaper and enqueues.** The
store's updates are unconditional (`WHERE id = ?`), and the broker and Conveyor check state in
one call and write in another. The tests state the guard each operation needs.

**KB-16. Concurrent first-time migration (suspected, not reproduced).** `migrate()` reads
`user_version` outside the transaction and uses a deferred `BEGIN`. If `serve` and a `worker`
start together on a new database file, both can decide to run migration 1. The second then
fails with "table jobs already exists" once the first commits. It needs `BEGIN IMMEDIATE` and a
re-read of the version inside the transaction. Not tested: it needs two processes, because a
single process runs `migrate()` synchronously.

**KB-17. Rate-limited queue blocks other queues.** From the first review. `claim()` picks the
single best job across all the requested queues and only then asks that queue's rate limiter.
If the limiter says no, `claim()` returns nothing, even when another queue in the same claim has
work. The fix belongs in `claim()`: skip rate-limited queues, like paused and full ones. Left to
the concurrency work, which is rewriting `claim()`.

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
