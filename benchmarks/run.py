"""Partitioning benchmark: same input, four points on the fine/coarse line.

    python -m benchmarks.run                 # compact + query every scheme (~25 min on 2.6 GB)
    python -m benchmarks.run --skip-compact  # re-run only the queries against existing stores

Each compaction runs in its own subprocess so its peak-memory figure is its
own (the OS high-water mark is per process). Every scheme must return the
same answer for every query; a mismatch aborts the benchmark.
"""

import argparse
import json
import random
import statistics
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from query.engine import query

SCHEMES = ["none", "day", "day_tank", "hour"]


def query_suite(manifest, n_random, seed=2026):
    rng = random.Random(seed)
    qs = [(w["tank_id"], datetime.fromisoformat(w["start"]), datetime.fromisoformat(w["end"]))
          for w in manifest["planted_windows"]]
    start = datetime.fromisoformat(manifest["params"]["start"])
    minutes = manifest["params"]["days"] * 24 * 60 - 120
    for _ in range(n_random):
        s = start + timedelta(minutes=rng.randint(0, minutes))
        qs.append((rng.randint(1, manifest["params"]["tanks"]), s, s + timedelta(hours=2)))
    return qs


def compact_scheme(raw, out, scheme):
    t0 = time.perf_counter()
    subprocess.run([sys.executable, "-m", "compactor.compact", "--raw", str(raw), "--out", str(out),
                    "--scheme", scheme], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"  compacted {scheme} in {time.perf_counter() - t0:.0f}s", flush=True)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--bench-dir", default="data/bench")
    p.add_argument("--schemes", nargs="+", default=SCHEMES)
    p.add_argument("--queries", type=int, default=30)
    p.add_argument("--skip-compact", action="store_true")
    p.add_argument("--out", default="benchmarks/results.json")
    a = p.parse_args(argv)
    raw, bench = Path(a.raw), Path(a.bench_dir)
    manifest = json.loads((raw / "manifest.json").read_text())
    qs = query_suite(manifest, a.queries)
    results, answers = {}, {}
    for scheme in a.schemes:
        store = bench / scheme
        if not a.skip_compact:
            compact_scheme(raw, store, scheme)
        rep = json.loads((store / "_report.json").read_text())
        times, rgs, parts, counts = [], [], [], []
        for tank, s, e in qs:
            r = query(store, tank, s, e)
            times.append(r["elapsed_ms"])
            rgs.append((r["row_groups_read"], r["row_groups_total"]))
            parts.append(r["partitions_opened"])
            counts.append(r["count"])
        warm = [query(store, t, s, e)["elapsed_ms"] for t, s, e in qs]  # second pass: OS cache warm
        answers[scheme] = counts
        files = list(store.rglob("*.parquet"))
        results[scheme] = {
            "partitions": rep["output"]["partitions"],
            "parquet_files": len(files),
            "store_mb": round(sum(f.stat().st_size for f in files) / 2**20, 1),
            "compact_seconds": rep["elapsed_seconds"],
            "pass1_seconds": rep["pass1_read_seconds"],
            "pass2_seconds": rep["pass2_write_seconds"],
            "staging_runs": rep["output"]["staging_runs"],
            "unsorted_partitions": len(rep["output"]["unsorted_partitions"]),
            "peak_rss_mb": rep["memory"]["peak_rss_mb_os"] or rep["memory"]["peak_rss_mb_sampled"],
            "peak_rss_mb_pass1": rep["memory"].get("peak_rss_mb_pass1"),
            "query_ms_median_first": round(statistics.median(times), 1),
            "query_ms_p95_first": round(sorted(times)[int(0.95 * (len(times) - 1))], 1),
            "query_ms_median_warm": round(statistics.median(warm), 1),
            "avg_partitions_opened": round(statistics.mean(parts), 2),
            "avg_row_groups_read": round(statistics.mean(r for r, _ in rgs), 1),
            "avg_row_groups_in_opened_files": round(statistics.mean(t for _, t in rgs), 1),
            "reconciled": rep["reconciliation"]["ok"],
        }
        print(f"  {scheme}: {results[scheme]}", flush=True)
    ref = next(iter(answers.values()))
    for scheme, c in answers.items():
        if c != ref:
            sys.exit(f"ANSWER MISMATCH between schemes: {scheme} {c} vs {ref}")
    out = {"input_bytes": manifest["totals"]["bytes"], "input_lines": manifest["totals"]["lines"],
           "queries": len(qs), "query_rows_total": sum(ref), "all_schemes_same_answers": True,
           "results": results}
    Path(a.out).write_text(json.dumps(out, indent=2))
    cols = ["partitions", "parquet_files", "store_mb", "compact_seconds", "staging_runs", "peak_rss_mb",
            "query_ms_median_first", "query_ms_p95_first", "query_ms_median_warm", "avg_row_groups_read"]
    print("\n| scheme | " + " | ".join(cols) + " |\n|" + "---|" * (len(cols) + 1))
    for s, r in results.items():
        print(f"| {s} | " + " | ".join(str(r[c]) for c in cols) + " |")


if __name__ == "__main__":
    main()
