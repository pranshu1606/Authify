# Fish-farm sensor log compactor

A batch pipeline in Python that takes append-only sensor logs too big to load
into memory, compacts them into a Parquet store partitioned by day and tank, and answers
"what was the oxygen in tank 7 between 02:00 and 04:00 last Tuesday" in tens of
milliseconds — returning **every** reading in the window, whichever raw file it
landed in. A plain browser page runs the compaction, shows a run report of what
was processed / skipped / unparseable, and runs queries.

**Memory ceiling: 512 MB.** The generated input is **2.59 GiB (5.2x the ceiling)**;
the largest single log file (836 MB) is itself 1.63x the ceiling.

## Start it (one command)

Prerequisites: Docker with Compose v2 (Docker Desktop on Windows/macOS), about
6 GB of free disk, port 8000 free.

```bash
docker compose up --build
```

Then open **http://localhost:8000**.

On first start the container generates the input logs in the background
(seed 42, ~3–5 minutes; the page shows "generating input logs..." and refreshes
itself). Then, on the page:

1. **Run compaction** — progress and live memory are shown; ~4–6 minutes.
2. Read the **run report** — files, lines, valid readings, rejected lines by
   reason with example lines, and reconciliation against the generator.
3. **Query** — tank + start + end, or click *query this* next to one of the
   deliberately split windows to see expected vs. actual counts.

The container runs with `mem_limit: 512m` and swap disabled, so the ceiling is
enforced by the kernel for everything in it (web server + compaction).
Generated data lives in the `sensor-data` volume, so restarts don't regenerate
it; `docker compose down -v` deletes it.

## Architecture

```
generator/generate.py        seeded generator -> data/raw/sensors_0N.csv + manifest.json (ground truth)
        |
compactor/records.py         streaming reader (capped readline) + strict per-line parser -> reading | reject reason
compactor/compact.py         pass 1: route each reading to the partition of ITS OWN day + tank,
                                      bounded buffers spilled to Arrow IPC runs (SPILL_ROWS = 2M)
                             pass 2: one partition at a time: sort by (tank_id, timestamp) -> zstd Parquet
                             atomic swap of the store + _report.json (counts, reasons, reconciliation)
        |
data/compacted/date=YYYY-MM-DD/tank=NNN/part-0.parquet
        |
query/engine.py              open every partition the window overlaps, filter tank + [start, end],
                             row-group statistics skip the rest
        |
web/app.py + templates/      Flask JSON API + one HTML page; compaction runs as a child process
```

Why readings split across files are never missed: a reading's partition is
decided by its own timestamp and tank, never by the file it came from or the order it
arrived in. After compaction, readings of one tank-window from any number of
files sit in the same partition(s), and the query opens every partition that
overlaps the window. `source_file` is kept per reading so the page can show
where each one came from.

## Running without Docker

Python 3.10+.

```bash
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt

python -m generator.generate --seed 42            # data/raw, ~3 min, 2.59 GiB
python -m compactor.compact                       # data/compacted, ~4-6 min
python -m query.engine --tank 7 --start 2026-09-22T02:00 --end 2026-09-22T04:00
python -m web.app                                 # http://localhost:8000
```

Generator options: `--tanks 10 --days 14 --interval-ms 400 --files 6 --messy-rate 0.02 --out data/raw --overwrite`.
A small dataset for experimenting: `python -m generator.generate --interval-ms 60000 --out data/raw_small`.

## Tests, acceptance check, benchmarks

```bash
python -m pytest                       # 64 tests, ~90 s, small generated data
python -m benchmarks.acceptance        # against the real 2.59 GiB data + store: brute-force re-scan vs query
python -m benchmarks.run               # compacts the same input 4 ways and times 32 queries on each (~25 min)
python -m benchmarks.duckdb_spike      # the alternative we considered: DuckDB doing the whole compaction
```

The split-file case is `tests/test_split_window.py`: two hand-written files
hold tank 7 readings at 02:10/03:20 and 02:05/02:40/03:45, out of order, with
malformed look-alikes and 1 ms-outside neighbours. The query must return all
five in time order, attributed to the right file, under every partition scheme
and with spilling forced after every row. A deliberately broken compactor that
only reads one file returns `['02:10', '03:20']` and fails that test.

## The data

`python -m generator.generate --seed 42` (deterministic: same seed, same bytes):

| | |
|---|---|
| files | 6 (836, 556, 415, 327, 280, 237 MB) |
| lines | 60,480,001 |
| valid readings | 59,272,426 |
| malformed lines | 929,022 (wrong field count 371,654 · bad timestamp 185,453 · bad value 185,964 · bad tank id 92,988 · unknown metric 92,963) |
| out-of-range lines | 278,553 |
| messy fraction | 2.00% |
| tanks / days | 10 tanks (ids 1–10), 2026-09-14 .. 2026-09-27, one reading per metric every 400 ms |

Disorder comes from simulated per-tank buffered flushes: batches of 200–2000
readings go to randomly chosen files, are shuffled across tanks, 8% are
delayed 1–6 hours and 20% are internally scrambled. Two windows are split on
purpose and recorded in `data/raw/manifest.json`:

| window | tank | expected readings | split across |
|---|---|---|---|
| Tue 2026-09-22 02:00–04:00 | 7 | 36,002 | sensors_02 (17,354) + sensors_04 (18,648) |
| 2026-09-24 23:00 – 09-25 01:00 (crosses midnight) | 3 | 36,002 | sensors_01 (17,743) + sensors_05 (18,259) |

Line format (CSV, no header): `2026-09-22T02:13:21.400,7,dissolved_oxygen,6.82`.
Timestamps are naive and treated as UTC. Valid ranges: dissolved oxygen
0–20 mg/L, temperature −2–40 °C (see `schema.py`).

## Example

```bash
python -m query.engine --tank 7 --start 2026-09-22T02:00 --end 2026-09-22T04:00 --show 2
```

```
{
  "tank_id": 7,
  "start": "2026-09-22T02:00:00.000",
  "end": "2026-09-22T04:00:00.000",
  "count": 36002,
  "by_metric": {"dissolved_oxygen": 18001, "temperature": 18001},
  "by_source_file": {"sensors_02.csv": 17354, "sensors_04.csv": 18648},
  "partitions_opened": 1,
  "row_groups_read": 2,
  "row_groups_total": 7,
  "elapsed_ms": 31.7,
  "note": null
}
{'timestamp': '2026-09-22T02:00:00.000', 'metric': 'dissolved_oxygen', 'value': 2.51, 'source_file': 'sensors_04.csv'}
{'timestamp': '2026-09-22T02:00:00.000', 'metric': 'temperature', 'value': 11.36, 'source_file': 'sensors_04.csv'}
```

HTTP: `GET /api/query?tank=7&start=2026-09-22T02:00&end=2026-09-22T04:00`
(add `&format=csv` for a CSV of every reading). The window is inclusive at both
ends. Other endpoints: `POST /api/compact`, `GET /api/compact/status`,
`GET /api/report`, `GET /api/dataset`.

## Partitioning

Partitioned by **day and tank** (`date=YYYY-MM-DD/tank=NNN`, 140 partitions),
each one Parquet file sorted by (tank_id, timestamp) with 65,536-row row
groups. Chosen by compacting the same 2.59 GiB four ways and running the same
32 queries on each (`python -m benchmarks.run`); all four gave identical
answers:

| scheme | partitions | compaction | peak memory | query median (first run) | p95 | row groups read |
|---|---|---|---|---|---|---|
| none | 1 | 291 s | 213 MB | 354 ms | 493 ms | 29.7 |
| day | 14 | 252 s | 332 MB | 24 ms | 41 ms | 3.4 |
| **day + tank** | 140 | 277 s | **134 MB** | 31 ms | **38 ms** | **1.5** |
| hour | 336 | 304 s | 84 MB | 70 ms | 81 ms | 3.4 |

Why this point on the line, and what it costs in each direction:
[DECISIONS.md](DECISIONS.md).

## Memory

| stage | peak RSS (measured) |
|---|---|
| streaming scan of the 836 MB file alone | 17 MB |
| compaction pass 1 (read, route, spill) | 98 MB |
| compaction overall (pass 2 sorts one tank-day in memory) | 134 MB |
| same, if partitioned by day only (sorts 4.23M rows) | 332 MB |
| declared ceiling | 512 MB |

Peak RSS is read from the OS high-water mark (`peak_wset` on Windows,
`ru_maxrss` on Linux) and cross-checked by sampling every 50 ms; both are in
the run report.

## Repository

```
schema.py              line format, valid ranges, reject reasons (shared by generator and compactor)
generator/generate.py  input generator
compactor/records.py   bounded line reader + strict parser
compactor/compact.py   two-pass compaction, run report
query/engine.py        tank/time-range query
web/                   Flask app + page
tests/                 pytest suite
benchmarks/            partition benchmark, acceptance check, DuckDB spike
docker/start.sh        container entrypoint
DECISIONS.md           choices, costs, rejected alternatives, tradeoffs, next steps
```
