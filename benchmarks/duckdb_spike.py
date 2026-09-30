"""Spike of the alternative we considered: let DuckDB do the whole compaction.

    python -m benchmarks.duckdb_spike [--raw data/raw] [--out data/bench/duckdb]

One SQL statement reads every CSV, applies the same validity rules as
compactor/records.py through TRY_CAST / try_strptime, sorts by (tank, time)
and writes date-partitioned Parquet, under memory_limit. We measure time,
peak RSS, and whether its row count matches our compactor's valid count.
"""

import argparse
import json
import shutil
import time
from pathlib import Path

import duckdb

from compactor.compact import PeakMemory


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--out", default="data/bench/duckdb")
    p.add_argument("--memory-limit", default="400MB")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--no-sort", action="store_true", help="drop ORDER BY to isolate the sort")
    a = p.parse_args(argv)
    out, tmp = Path(a.out), Path(a.out + "_spill")
    shutil.rmtree(out, ignore_errors=True)
    shutil.rmtree(tmp, ignore_errors=True)
    manifest = json.loads((Path(a.raw) / "manifest.json").read_text())

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.memory_limit}'")
    con.execute(f"SET threads={a.threads}")
    con.execute(f"SET temp_directory='{tmp.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    src = (Path(a.raw).as_posix() + "/*.csv")
    sql = f"""
    COPY (
      SELECT ts AS timestamp, tank_id, metric, value, strftime(ts, '%Y-%m-%d') AS date
      FROM (
        SELECT try_strptime(c0, '%Y-%m-%dT%H:%M:%S.%g') AS ts,
               TRY_CAST(c1 AS SMALLINT) AS tank_id, c2 AS metric, TRY_CAST(c3 AS DOUBLE) AS value
        FROM read_csv('{src}', header=false, delim=',', quote='', escape='', auto_detect=false,
                      columns={{'c0': 'VARCHAR', 'c1': 'VARCHAR', 'c2': 'VARCHAR', 'c3': 'VARCHAR'}},
                      ignore_errors=true, store_rejects=true)
      )
      WHERE ts IS NOT NULL AND tank_id BETWEEN 1 AND 999 AND isfinite(value)
        AND ((metric = 'dissolved_oxygen' AND value BETWEEN 0 AND 20)
          OR (metric = 'temperature' AND value BETWEEN -2 AND 40))
      {'' if a.no_sort else 'ORDER BY tank_id, timestamp'}
    ) TO '{out.as_posix()}' (FORMAT parquet, PARTITION_BY (date), COMPRESSION zstd)
    """
    t0 = time.perf_counter()
    error = None
    with PeakMemory() as mem:
        try:
            con.execute(sql)
            rows = con.execute(f"SELECT count(*) FROM read_parquet('{out.as_posix()}/**/*.parquet')").fetchone()[0]
            rejected_by_reader = con.execute("SELECT count(*) FROM reject_errors").fetchone()[0]
        except duckdb.Error as e:
            error, rows, rejected_by_reader = f"{type(e).__name__}: {str(e).splitlines()[0]}", None, None
        elapsed = time.perf_counter() - t0
    ours = manifest["totals"]["valid"]
    result = {
        "memory_limit": a.memory_limit, "threads": a.threads, "sorted": not a.no_sort, "error": error,
        "seconds": round(elapsed, 1),
        "peak_rss_mb": round((mem.os_peak() or mem.peak) / 2**20, 1),
        "rows_written": rows, "our_valid_rows": ours, "row_difference": None if rows is None else rows - ours,
        "lines_rejected_by_csv_reader": rejected_by_reader,
        "partitions": len(list(out.glob("date=*"))),
        "store_mb": round(sum(f.stat().st_size for f in out.rglob("*.parquet")) / 2**20, 1),
    }
    print(json.dumps(result, indent=2))
    log = Path("benchmarks/duckdb_results.json")
    runs = json.loads(log.read_text()) if log.exists() else []
    runs = runs if isinstance(runs, list) else []
    log.write_text(json.dumps(runs + [result], indent=2))
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
