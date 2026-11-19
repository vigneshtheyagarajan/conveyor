# Benchmarks

Opt-in; not part of `npm test`.

## Throughput

```sh
npm run bench -- [--processes 4] [--jobs 5000] [--concurrency 1] [--runs 3]
```

Several worker processes (each a real `Worker` with `--concurrency` loops and a no-op handler,
started by `bench/worker.ts`) drain `--jobs` waiting jobs from one SQLite file. The clock starts
when every process is released and stops when every job is `completed`. The script prints jobs
per second for each run and the median, and how many handler runs were duplicates (a job
whose handler ran more than once).

Numbers depend on the machine; compare runs made on the same one.

## Results

5000 no-op jobs, 3 runs each, median jobs/s. "Duplicates" is the number of extra handler runs
across all 3 runs (15000 jobs in total).

Machine: 4 CPUs, Node v22.21.0.

| Code | 4 processes × 1 | 4 processes × 4 | 1 process × 1 |
| --- | --- | --- | --- |
| Baseline (before the concurrency work) | 1345 jobs/s, 2925 duplicates | 1338 jobs/s, 7198 duplicates | 1893 jobs/s, 0 duplicates |
| Step 1: atomic `claimNext`, lease-token checks | 2174 jobs/s, 0 duplicates | 2369 jobs/s, 0 duplicates | 2640 jobs/s, 0 duplicates |
| Step 2: guarded reaper, cancel and retry | 1356 jobs/s, 0 duplicates | 1527 jobs/s, 0 duplicates | 1499 jobs/s, 0 duplicates |
| Steps 4–5: unique keys, atomic schedule firing, concurrent migrations | 2249 jobs/s, 0 duplicates | 2222 jobs/s, 0 duplicates | 3014 jobs/s, 0 duplicates |
| Step 6: worker timeouts, wake-up, shutdown | 1908 jobs/s, 0 duplicates | 2448 jobs/s, 0 duplicates | 2164 jobs/s, 0 duplicates |

This machine's numbers drift by about ±20% between sessions. To separate the change from
the drift, step 1 was also measured against the baseline commit (checked out in a separate
worktree with the same bench scripts), alternating the two, 3 rounds each. Each figure is the
median of one `--runs 3` invocation:

| Config | Baseline | Step 1 | Change (median of rounds) |
| --- | --- | --- | --- |
| 4 processes × 1 | 1847, 1856, 1947 (≈2150 duplicates per invocation) | 2305, 2591, 2364 (0 duplicates) | 1856 → 2364, +27% |
| 1 process × 1 | 2092, 2165, 1835 | 2824, 2273, 2300 | 2092 → 2300, +10% |

Why step 1 is faster rather than slower, despite serialising claims on the write lock:
the old claim made two separate autocommit writes (activate, then the `claimed` event), each its
own WAL commit, re-prepared the candidate query on every call and re-read the job afterwards.
`claimNext` does one commit, uses cached statements and `RETURNING`. With several processes the
old code also spent part of its time on duplicate runs and their failed completions.

Step 2's absolute numbers are lower only because the machine was slower that session: the
baseline commit, run alternately with step 2 right then, was slower by as much (3 rounds each,
medians of `--runs 3` invocations):

| Config | Baseline | Step 2 | Change (median of rounds) |
| --- | --- | --- | --- |
| 4 processes × 1 | 932, 1003, 1048 (≈2900 duplicates per invocation) | 1355, 1293, 1455 (0 duplicates) | 1003 → 1355, +35% |
| 1 process × 1 | 1428, 1502, 1431 | 1732, 1722, 1633 | 1431 → 1722, +20% |

Against the baseline, step 2 does as well as step 1 (+27% / +10%), or better. That is expected:
step 2 changes the reaper, cancel and retry, none of which is on the benchmark's path (its
workers run no reaper and nothing is cancelled or retried). Throughput did not move.

Steps 4–5, alternated with the baseline commit in the same way (3 rounds each):

| Config | Baseline | Steps 4–5 | Change (median of rounds) |
| --- | --- | --- | --- |
| 4 processes × 1 | 1889, 2126, 1478 (≈2300 duplicates per invocation) | 2397, 2387, 2397 (0 duplicates) | 1889 → 2397, +27% |
| 1 process × 1 | 2007, 1464, 1668 | 1769, 1961, 1875 | 1668 → 1875, +12% |

In line with steps 1 and 2, so throughput did not move. Single-process numbers were noisy in
this session: in round 1 the baseline came out ahead (2007 vs 1769). Nothing in steps 4–5 is on
the benchmark's timed path: its jobs have no unique key (the partial index holds no rows for
them), there are no schedules, and it migrates before the clock starts.

Step 6 changes the worker loop, which is on the benchmark's path. Alternated with the baseline
commit (3 rounds each, now also for 4 × 4):

| Config | Baseline | Step 6 | Change (median of rounds) |
| --- | --- | --- | --- |
| 4 processes × 1 | 1394, 1651, 1623 (≈2500 duplicates per invocation) | 2226, 1897, 2102 (0 duplicates) | 1623 → 2102, +30% |
| 4 processes × 4 | 1283, 1395, 1415 (≈7000 duplicates per invocation) | 2423, 2504, 2227 (0 duplicates) | 1395 → 2423, +74% |
| 1 process × 1 | 2027, 2061, 2191 | 2400, 2188, 2686 | 2061 → 2400, +16% |

The 4 × 1 and 1 × 1 gains are in line with steps 1–5, so step 6 cost nothing measurable. The
wake-up change plays no part here: the jobs are enqueued before the workers start, and
`job.enqueued` is an in-process event. The 4 × 4 gain is the largest because the baseline ran
nearly half of its 15000 handler runs twice.
