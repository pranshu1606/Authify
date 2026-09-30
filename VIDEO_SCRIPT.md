# Video script (target 8–9 minutes)

Setup before recording: `docker compose up --build` already run and the input
generated (page section 1 shows the files). Have a terminal open in the repo,
the browser on http://localhost:8000, and `DECISIONS.md` open in an editor.
Every number below comes from the real runs; say them as they appear on screen.

---

## 0. What it is (0:00–0:45)

**Show:** the page, section 1.

> "This is a batch pipeline for a fish-farm's sensor logs. The generator wrote six
> append-only CSV files — 2.59 GB, 60 million lines, five times my 512 MB memory
> ceiling; the biggest file alone is 836 MB. Two percent of the lines are broken
> on purpose, and readings arrive out of order because tanks buffer and flush
> independently."

**Show:** terminal `docker compose ps` / `docker stats --no-stream` → the 512 MiB limit.

> "Everything runs in one container started by one command, `docker compose up
> --build`, and the container has a hard 512 MB memory limit with swap off, so the
> ceiling is enforced by the kernel, not just claimed."

## 1. Architecture (0:45–1:45)

**Show:** the diagram in README.md.

> "Generator → raw logs → compaction in two passes → Parquet partitioned by day
> and tank → a query engine → Flask → this page.
> Pass 1 streams every line — never a whole file — parses it strictly, and puts
> each valid reading in the partition of *its own* timestamp and tank. Buffers
> are capped at two million readings and spilled to disk. Pass 2 takes one
> partition at a time, sorts it by tank and time, and writes Parquet."

**Show:** click **Run compaction**; point at the live progress line and the memory figure.

> "While it runs you can see progress and memory. It runs as a child process, so
> the page stays up."

(It takes ~4–5 minutes — keep talking through section 2 while it runs, or cut.)

## 2. Question 1 — partitioning (1:45–3:30)

**Show:** the table in DECISIONS.md ("The tradeoff I settled on").

> "I compacted the same data four ways and ran the same 32 queries against each.
> All four returned identical answers — partitioning only changes cost.
> One partition: cheap to build, but a query takes 354 ms median because it scans
> a 59-million-row file. Hourly: 336 partitions, the slowest compaction at 304 s,
> 2,163 staging files, and cold queries at 70 ms because a two-hour window opens
> three files. Daily compacted fastest, 252 s, 24 ms queries — that was my
> starting choice.
> I moved to day plus tank: 140 partitions. It costs 10 % more compaction time
> than daily and ten times the files. What it buys is memory: peak drops from
> 332 MB to 134 MB, because pass 2 sorts one tank-day instead of a whole day, and a
> query reads 1.5 row groups of one small file. And at ten times the volume a
> daily partition would be 42 million rows — too big to sort under the ceiling —
> while a tank-day would still fit."

## 3. Question 2 — the rejected approach (3:30–4:45)

**Show:** `benchmarks/duckdb_results.json` or the DuckDB table in DECISIONS.md.

> "The serious alternative was DuckDB doing the whole compaction in one SQL
> statement with a memory limit. I built it and ran it on the same data. It is
> six times faster when it works — about 40 seconds. But with the sort it ran out
> of memory at every limit I tried, even single-threaded; its process memory went
> to 628 MB, past my ceiling, because the memory limit only covers its buffer
> pool. And without the sort, it wrote 18,429 more rows than there are valid
> readings: those were corrupt lines with tank id '7.5', and TRY_CAST rounded
> them to tank 8. That's exactly the brief's failure — a plausible answer that's
> wrong. So I kept compaction in code where every rejected line has a reason."

## 4. Question 3 — split-file correctness (4:45–6:30)

**Show:** section 1 of the page — the planted windows table (tank 7, Tuesday
02:00–04:00: 17,354 readings in sensors_02, 18,648 in sensors_04).

> "The generator plants this window across two files on purpose. The fix for
> out-of-order, split data is where a reading goes: its partition comes from its
> own timestamp and tank, never from which file it arrived in."

**Show:** click **query this** → "Expected 36,002 — got 36,002: MATCH",
"by source file", "row groups read", and scroll the table to show
`source_file` switching between sensors_02 and sensors_04.

**Show:** terminal, `tests/test_split_window.py` — the two hand-written files.

> "The test is independent of my generator: two hand-written files, tank 7 at
> 02:10 and 03:20 in one, 02:05, 02:40 and 03:45 in the other, out of order, plus
> a malformed look-alike and readings one millisecond outside each edge. The
> query must return exactly those five, in order, under every partition scheme
> and even when every row is spilled separately."

**Show:** `python -m pytest tests/test_split_window.py -q` (passes).

> "And the test has teeth: a broken compactor that only reads one file returns
> 02:10 and 03:20 — plausible, and wrong — and this test fails on it."

**Show (optional):** `python -m benchmarks.acceptance` output → the brute-force
re-scan of all 60 million raw lines matches the query reading for reading,
including an unplanted window that happened to be spread over all six files.

## 5. Run report (6:30–7:15)

**Show:** section 3 of the page after compaction finishes.

> "The report: 60,480,001 lines read, 59,272,426 valid, 929,022 malformed and
> 278,553 out of range, each by reason with example lines and line numbers, and
> every file's counts reconciled against what the generator says it wrote."

## 6. Question 4 — ten times the volume (7:15–8:00)

> "Pass-1 memory doesn't change — the buffer is capped by rows. Pass 2 would sort
> about 4.2 million rows per tank-day, which is exactly what a daily partition
> costs today — 332 MB measured, still under the ceiling. What breaks first is
> compaction time: pass 1 is pure Python at about four microseconds a line, so
> roughly forty minutes. I'd make compaction incremental — the logs are
> append-only, so only parse each file's new tail and rewrite the partitions it
> touches — and vectorise parsing with Arrow's CSV reader."

## 7. Question 5 — where the AI was wrong (8:00–9:00)

Pick one or two; all happened in the transcript.

1. **Memory setting it guessed.** "It set the sort limit to 8 million rows
   without measuring. The measured cost is about 60 bytes per sorted row, so 8
   million would have been ~560 MB — over the ceiling. I caught it by deriving
   the number from the full run and lowered it to 5 million."
2. **Timestamp formatting.** "It wrote `%f` for milliseconds with a comment
   saying that's what it does — true in Python, false in Arrow. The first real
   query crashed; we checked Arrow's actual output instead of guessing again."
3. **Partitioning.** "The initial plan was daily partitions, and I was going to
   defend it. The benchmark showed daily has 2.5x the memory and is the first
   to break at 10x, so I changed the decision."
4. **Query read the data twice.** "The query it wrote counted rows and then
   read them again. It looked fine on my laptop — 20 ms — but inside the
   memory-capped container it was up to half a second. Reading only the row
   groups the statistics allow, once, brought it to 9–19 ms."
5. **Generator bug.** "Its first generator said the midnight window was split
   across two files; looking at the output showed it was all in one. The
   alternation counter reset every hour."

Close: "One command, bounded memory with the ceiling enforced, every reading in
the window from whichever file it landed in, and a report whose numbers add up."
