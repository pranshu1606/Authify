# DECISIONS

All numbers below were measured on this repo, on one Windows 11 laptop
(12 logical CPUs, 7.7 GB RAM, Python 3.10, PyArrow 25), against the data from
`python -m generator.generate --seed 42`: **6 files, 2.59 GiB, 60,480,001 lines,
59,272,426 valid readings**. Raw measurements: `benchmarks/results.json`,
`benchmarks/duckdb_results.json`, and the `_report.json` written by each
compaction.

**Hours spent:** about 2.7 hours of working time with the AI assistant,
measured from the session transcript timestamps (Mon 28 Sep 21:45–22:56,
Wed 30 Sep 14:39–16:01, and two short sessions on Thu 1 Oct). Long runs such
as the 25-minute partition benchmark happened inside those sessions. The
figure does not include reading the brief beforehand or recording the video.

## Key assumptions

The four that shape the design most; the full list is under
[Assumptions](#assumptions-each-is-a-setting-you-could-change).

1. **The process gets 512 MB, and the data is five times that.** I capped the
   whole container at 512 MB with swap off and generated 2.59 GiB, with the
   largest single file (836 MB) bigger than the cap. Why: the brief asks for a
   stated ceiling that the data really exceeds, and a kernel-enforced limit
   can't be quietly dodged. This number drives the partitioning choice: pass 2
   sorts one partition in memory, so partitions must stay small enough to sort
   under it.
2. **Every query names exactly one tank.** The brief rules out multi-tank
   queries, so I partition by day *and* tank: a query opens one small file and
   reads about 1.5 row groups. Why it matters: if cross-tank questions ("all
   tanks at 02:00") became common, the same query would open 10 files per day,
   and partitioning by day alone would win again.
3. **The logs are static during a run, and each run recompacts everything.**
   The brief says this is a batch job with no live appends. That keeps
   compaction simple and correct, but it costs about 4.6 minutes per run today
   and would take around 40 minutes at 10x volume. Because the logs are
   append-only, the natural next step is to compact only each file's new tail.
4. **A bad line is counted, never repaired, and identical readings are never
   merged.** Tank id `7.5` is rejected, not rounded to 8. A line that appears
   in two files is returned twice. Why: guessing wrong about a sensor reading
   is worse than reporting it as unusable, and an identical line in two files
   cannot be told apart from a probe that genuinely reported the same value
   twice. Every rejected line gets exactly one reason, and
   `lines = valid + Σ rejects` is checked on every run.

## What I chose to run, and why

A hand-rolled, two-pass **partitioned sort** in plain Python, writing
**Parquet through PyArrow**, queried through `pyarrow.dataset`; a Flask page;
one Docker Compose service. Three runtime dependencies (`pyarrow`, `flask`,
`psutil`).

- **Pass 1** streams every line with a length-capped `readline` (a corrupt
  file with no newlines cannot become one giant line), parses it strictly into
  either a reading or exactly one reject reason, and appends valid readings to
  the buffer of the partition given by **the reading's own timestamp and
  tank** — never by the file it came from. When 2M readings are buffered in
  total, all buffers are spilled to Arrow IPC run files. Memory in this pass
  does not depend on input size: 81–98 MB peak whatever the scheme.
- **Pass 2** takes one partition at a time, sorts it by (tank_id, timestamp)
  and writes one zstd Parquet file with 65,536-row row groups, so min/max
  statistics let a query skip almost everything. The new store is built next
  to the old one and swapped in by rename, with a `_report.json` that
  reconciles every file's line/valid/per-reason counts against the
  generator's manifest.

This is what makes the split window correct *by construction*: after pass 1,
tank 7's readings from `sensors_02` and `sensors_04` are in the same
partition, and the query opens every partition the window overlaps. It is
also why I kept the pipeline in code I can read line by line rather than in
an engine: the thing being graded is exactly the part an engine would hide.

The brief says the stack is named, but the text I worked from names only
Python, a partitioned columnar store and a browser page, so I made no
substitution I know of. Parquet/PyArrow is the store the grading rubric itself
mentions; Flask is the smallest server that serves a page and JSON; Docker
Compose is there because `mem_limit` lets the kernel enforce the ceiling.

## What it cost

- **Setup time:** low for the libraries (pip install, no services). Most of
  the time went into making the numbers trustworthy: the generator's manifest
  as ground truth, per-file reconciliation, peak memory from the OS high-water
  mark in a fresh process per run (the counter is per process, so reusing a
  process would carry one run's peak into the next), and a brute-force
  acceptance check that re-reads all 60.5M raw lines.
- **Speed:** pure-Python parsing is the bottleneck. Compaction takes ~277 s,
  of which ~244 s is pass 1 (≈4 µs per line); parsing alone runs at 0.49M
  lines/s. I accepted this because the brief asks for minutes, and per-line
  code gives every rejected line an exact reason.
- **Moving parts:** one container, one web process, one child process per
  compaction (so a crash or memory blow-up cannot take the page down),
  progress through a JSON file. Temporary disk: staging runs are about the
  size of the valid data in Arrow form and are deleted per partition.
- **Operational surprises that cost time:** on Windows, `os.replace` of the
  progress file fails while the web server has it open, which killed 1 run in
  5 until progress became best-effort; Arrow cannot multi-key sort
  dictionary-encoded columns, so results are decoded before sorting.
- **A query that was fast natively and slow in the container.** The first
  query engine counted matching rows (to enforce the 500k cap) and then read
  them again. Natively that cost nothing visible (12–31 ms); inside the 512 MB
  container, where the page cache is small, the same query took 82–525 ms.
  Row-group metadata already gives an upper bound on the answer for free, so
  the engine now reads only the row groups whose statistics can match, once,
  and only counts exactly when that bound exceeds the cap: 9–19 ms warm and
  60 ms cold in the container, same answers (acceptance re-run).

## What I seriously considered and rejected

**DuckDB doing the whole compaction** (`COPY (SELECT … FROM read_csv(…) ORDER BY
tank_id, timestamp) TO … (PARTITION_BY (date))`, the same validity rules as
`TRY_CAST`/`try_strptime`, `memory_limit` set). I built it
(`benchmarks/duckdb_spike.py`) and ran it on the same 2.59 GiB:

| DuckDB run | result | peak RSS |
|---|---|---|
| sorted, 4 threads, memory_limit 400 MB | out of memory after 1.4 s | 473 MB |
| sorted, 1 thread, 400 MB | out of memory after 32 s | **628 MB** |
| sorted, 1 thread, 250 MB | out of memory after 2.4 s | 318 MB |
| unsorted, 4 threads, 400 MB | done in **38.9 s** | **527 MB** |
| unsorted, 4 threads, 250 MB | done in 41.8 s | 481 MB |

It is 6–7x faster when it works, and I would take that trade if it were
only about speed. It was wrong for this brief for three measured reasons:
the sorted write (which is what gives the query its row-group pruning) ran
out of memory at every limit I tried; process RSS ran to about twice
`memory_limit`, past the 512 MB ceiling, because the limit governs DuckDB's
buffer pool rather than the process; and it **silently turned 18,429 corrupt
lines into readings for the wrong tank** — `TRY_CAST('7.5' AS SMALLINT)` is 8.
That is the exact failure the brief warns about: an answer that looks complete
and is wrong. Its reject table also logs one row per missing column (598,153
rows for 371,654 bad lines), so honest per-line counts need more SQL. Each of
these is fixable, but each fix is a rule I would have to know about in
advance, inside an engine whose memory I cannot bound.

Also considered: **pandas `read_csv(chunksize=…)`** — bounded, but it coerces
or drops bad rows per chunk rather than giving one reason per line, and holds
Python objects per row; not measured. **Partitioning by hour, by day, by nothing**
— measured below.

## The tradeoff I settled on: partition by day *and* tank

Same input, same code, four points on the fine/coarse line, 32 queries each
(the two planted windows plus 30 random two-hour windows); **all four returned
identical answers to every query**:

| scheme | partitions | compaction | staging runs | peak memory | query median (first run) | p95 | median (cache warm) | row groups read |
|---|---|---|---|---|---|---|---|---|
| none | 1 | 291 s | 30 | 213 MB | 354 ms | 493 ms | 285 ms | 29.7 of 919 |
| day | 14 | **252 s** | 120 | 332 MB | **24 ms** | 41 ms | 22 ms | 3.4 of 67 |
| **day + tank** | 140 | 277 s | 1,141 | **134 MB** | 31 ms | **38 ms** | 12 ms | **1.5 of 7** |
| hour | 336 | 304 s | 2,163 | 84 MB | 70 ms | 81 ms | 12 ms | 3.4 of 9 |

I started with daily partitions and planned to defend them; the numbers moved
me to day + tank.

- **What it costs at compaction, against daily:** +25 s (+10%), 9.5x more
  staging runs, 10x more files (140 files of ~1.4 MB — small).
- **What it buys:** peak memory falls from 332 MB to 134 MB, because pass 2
  sorts one tank-day (~423k rows) instead of one day (4.23M rows); a query
  opens one small file and reads 1.5 row groups; warm queries are ~2x faster,
  cold p95 is the best of the four. Every query in this brief names exactly one
  tank, so tank is the most selective key there is.
- **What the coarse direction costs:** one partition makes queries 11–15x
  slower (354 ms median) and, at 59M rows, is too big to sort in budget, so it
  is written unsorted and pruning mostly stops working.
- **What the fine direction costs:** hourly has the lowest memory but the
  slowest compaction, 2,163 staging runs, and cold queries 2.3x slower than
  day + tank, because a two-hour window usually opens three files.
- **Why not daily, which compacts fastest:** 332 MB of a 512 MB container that
  also runs the web server, and at 10x volume a day is 42M rows — far past the
  sort limit — so every day would be written unsorted. Day + tank at 10x is
  ~4.2M rows per partition, still sorted in budget.

## Assumptions (each is a setting you could change)

1. **Memory ceiling 512 MB**, enforced for the whole container
   (`mem_limit: 512m`, swap off). Data: 5.2x that; the largest single file is
   1.63x.
2. **Logs are static during a run** and each run recompacts everything. No
   incremental or streaming compaction (out of scope).
3. **Timestamps are naive UTC**, exactly `YYYY-MM-DDTHH:MM:SS.mmm`; anything
   else is `bad_timestamp` (so `2026-09-22 02:00:00` with a space is rejected).
4. **The query window is inclusive at both ends** — a reading at exactly 04:00:00.000
   is in "02:00 to 04:00".
5. **No de-duplication.** An identical line in two files cannot be told apart
   from a probe that reported the same value twice, so both are returned
   (tested).
6. **Strict validity:** tank id must be ASCII digits in 1–999 (`7.5`, `T7`,
   `-3`, `0` are rejected, never rounded); metric names are case-sensitive;
   `nan`/`inf` are bad values; dissolved oxygen must be in 0–20 mg/L and
   temperature in −2–40 °C, otherwise `out_of_range`.
7. **A rejected line is counted, never repaired.** Nothing is dropped
   without a number: `lines = valid + Σ rejects` is checked every run.
8. **A query returns at most 500,000 readings.** A bigger window is refused
   with its exact count rather than truncated (one tank for the whole
   fortnight is 5.9M).
9. **Sorting is an optimisation, not a correctness requirement.** A partition
   bigger than `MAX_SORT_ROWS` = 5M (derived from the measured ~60 bytes per
   row sorted, about 380 MB) is written unsorted and flagged in the report;
   its answers stay correct, only slower.
10. **Counts by metric and by source file in the query response are
    metadata, not aggregations of the readings.** They are there to show that
    a split window really came from two files; the raw readings are always
    returned.

## If the volume were 10x

Pass-1 memory does not change (buffer is capped by rows). Pass 2 on day + tank
would sort ~4.2M rows per partition, the same load as today's daily partition,
which measured 332 MB — still under the ceiling. What breaks first is
**compaction time**: pass 1 is linear, so ~40 minutes, then staging-run count
(~11,000 files at today's spill size). Query time stays in tens of
milliseconds because a query still reads one or two row groups. Daily
partitioning would break on memory first.

## What I would do next

- **Incremental compaction.** The logs are append-only, so remember each
  file's byte offset, parse only the new tail, and rewrite only the partitions
  it touches. This is the biggest real win and removes the 10x time problem.
- **Vectorised parsing.** Read columns as strings with `pyarrow.csv` and
  validate with regex and compute kernels, keeping the per-reason counts; pass 1
  is ~88% of compaction time.
- **External merge instead of the unsorted fallback** for partitions over the
  sort budget, so oversized partitions stay fast to query.
- **Fewer staging files:** one append-only IPC stream per partition instead of
  one file per spill.
- **Parallel pass 1** with the row budget split across worker processes.
