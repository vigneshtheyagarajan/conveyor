# Stress test

Opt-in; not part of `npm test`. Takes about 30 seconds with the defaults.

```sh
npm run stress -- [--seed n] [--workers 4] [--concurrency 4] [--jobs 3000] [--enqueue-ms 15000]
                  [--kill-every-ms 1000] [--restart-after-ms 500] [--lease-ms 2000]
                  [--heartbeat-ms 300] [--deadline-ms 180000] [--keep]
```

What runs, all on one SQLite file (`stress/roles.ts`, one process each):

- `--workers` worker processes, each a real `Worker` with `--concurrency` loops and no reaper of
  its own.
- One reaper process (the reaper and the delayed-job promoter).
- One enqueuer, which adds `--jobs` jobs in random batches (half `enqueueBulk`, half concurrent
  `enqueue` calls) over about `--enqueue-ms` while the workers run.

The job mix:

- Half are plain jobs.
- 20% use one of 10 unique keys and run longer, so their key is often still held when another
  request for it comes in.
- 15% fail on their first 1 to 5 attempts (4 are allowed, so some end dead).
- 15% run past their 150ms timeout on their first 1, 2 or 4 attempts. Half of those honour
  the abort signal; half ignore it.

Each job's behaviour is in its payload, so the checker knows what each job should end as.

Every `--kill-every-ms` (±50%), the orchestrator (`stress/run.ts`) kills a random worker process
with SIGKILL and starts a new one `--restart-after-ms` later. The killed worker's leases really
expire (`--lease-ms`) and are reaped. A run ends when the enqueuer is done and no job is
delayed, waiting or active, or at `--deadline-ms`.

Every handler run logs its start and end, and workers log each job they complete. At the end it
checks:

| Check | How |
| --- | --- |
| No job lost or stuck | Every job the enqueuer created exists, and is completed or dead |
| Enqueue answers are right | Every enqueue got back an existing job with its own unique key |
| No job completed twice | At most one `completed` event per job, in the database and in the workers' logs |
| No duplicate runs | No two runs of the same job with the same attempt number (two workers holding one claim) |
| No overlapping runs | A job's runs don't overlap in time. A run in a killed process ends at the kill. |
| Unique keys hold | No two jobs with the same queue and key were unfinished at the same time |
| Outcomes match the design | A job designed to fail every attempt is dead. A job designed to succeed completed, or died only of expired leases (it was killed on its last attempts). |

It prints the numbers (enqueues, jobs by outcome, runs, kills and restarts, reaped leases,
timed-out attempts, jobs per second) and the result of each check, and exits 1 if any check
fails, keeping the database and logs for inspection (`--keep` keeps them anyway).

The seed (random unless `--seed` is given) decides the job mix and the kill schedule, and is
printed with the full command to repeat the run. Process scheduling still differs from run to
run, so a repeat runs the same jobs and kill schedule but not exactly the same interleaving.

"Jobs per second" here is finished jobs over the whole run, which is bounded by the enqueue
rate, retries and lease expiries after kills. It is not a measure of claim throughput; use
`npm run bench` for that.

## Results

4 CPUs, Node v22.21.0, default options (4 workers × 4 loops, 3000 jobs over ~15s, a kill about
every second, 2s leases).

| Run | Code | Enqueues (created / existing returned) | Completed / dead | Runs (cut short by kills) | Kills / restarts | Reaped | Timed-out attempts | Elapsed | Jobs/s | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| seed 1436829396 | current | 2464 / 536 | 2173 / 291 | 4341 (96) | 25 / 25 | 96 | 817 | 27.2s | 90 | all checks pass |
| seed 1304298515 | current | 2498 / 502 | 2221 / 277 | 4399 (110) | 30 / 29 | 111 | 813 | 28.1s | 89 | all checks pass |
| seed 1555172324 | current | 2476 / 524 | 2186 / 290 | 4384 (85) | 24 / 23 | 85 | 883 | 29.0s | 85 | all checks pass |
| seed 1878187243 | current | 2515 / 485 | 2245 / 270 | 4361 (83) | 22 / 22 | 83 | 829 | 27.3s | 92 | all checks pass |
| seed 1158653485 | current | 2463 / 537 | 2164 / 299 | 4351 (93) | 25 / 25 | 94 | 787 | 27.7s | 89 | all checks pass |
| seed 1573730750 | current | 2492 / 508 | 2200 / 292 | 4433 (100) | 27 / 27 | 101 | 827 | 28.5s | 87 | all checks pass |
| seed 1436829396 | baseline commit | 2497 / 503 | 2221 / 276 | 4498 (94) | 20 / 20 | 74 | 823 | 21.2s | 118 | **fails 4 checks** |

On the current code (six runs) no job was lost, stuck, completed twice, run twice for one
attempt or run in overlap, and no unique key was held twice. 5–14 dead jobs per run died of
expired leases (killed on their last attempts), as designed.

The baseline commit (before the concurrency work) fails:

- duplicate runs: 27 (one job ran its attempt 4 four times)
- overlapping runs: 179 (mostly timed-out handlers overlapping their retries)
- unique key held by two unfinished jobs: 5
- outcomes that don't match the design: 15. Jobs designed to time out on every attempt
  completed, because double claims and the old reaper took them past `maxAttempts`.

Its higher jobs/s comes from recording timeouts at once instead of waiting for the handler,
and from running work twice. "Completed twice" did not show up in that run: the race behind it
(the old reaper reviving a job that completed between its read and its write) has a narrow
window. `test/integration/lifecycle-races.test.ts` forces it deterministically.
