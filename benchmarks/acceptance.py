"""Full-scale acceptance check against the real generated data and store.

    python -m benchmarks.acceptance [--raw data/raw] [--store data/compacted]

Independent of the query engine: it re-scans every raw line (streaming, via
the same strict parser) to build the expected answer for each planted
window and one extra window, then compares it reading-for-reading with
what query() returns. Also checks the size/ceiling claims and the report.
"""

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from compactor.records import format_ts, iter_lines, parse_line
from query.engine import query, readings
from schema import MEMORY_CEILING_MB, METRICS

EPOCH = datetime(1970, 1, 1)


def ms(dt):
    return int((dt - EPOCH).total_seconds() * 1000)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--store", default="data/compacted")
    a = p.parse_args(argv)
    raw, store = Path(a.raw), Path(a.store)
    manifest = json.loads((raw / "manifest.json").read_text())
    report = json.loads((store / "_report.json").read_text())
    ceiling = MEMORY_CEILING_MB * 2**20
    windows = [(w["name"], w["tank_id"], datetime.fromisoformat(w["start"]), datetime.fromisoformat(w["end"]))
               for w in manifest["planted_windows"]]
    windows.append(("tank2_saturday_1400_1600_unplanted", 2, datetime(2026, 9, 26, 14), datetime(2026, 9, 26, 16)))

    print(f"scanning {manifest['totals']['lines']:,} raw lines for the expected answers...", flush=True)
    t0 = time.perf_counter()
    expected = {name: Counter() for name, *_ in windows}
    bounds = [(name, tank, ms(s), ms(e)) for name, tank, s, e in windows]
    for f in sorted(raw.glob("*.csv")):
        for line in iter_lines(f):
            r = parse_line(line)
            if type(r) is tuple:
                for name, tank, lo, hi in bounds:
                    if r[1] == tank and lo <= r[0] <= hi:
                        expected[name][(format_ts(r[0]), METRICS[r[2]], r[3], f.name)] += 1
    print(f"  done in {time.perf_counter() - t0:.0f}s", flush=True)

    checks = {
        f"raw data > ceiling ({manifest['totals']['bytes'] / ceiling:.1f}x)": manifest["totals"]["bytes"] > ceiling,
        "largest single file > ceiling": max(f["bytes"] for f in manifest["files"]) > ceiling,
        f"compaction peak {report['memory']['peak_rss_mb_os']} MB < {MEMORY_CEILING_MB} MB":
            (report["memory"]["peak_rss_mb_os"] or report["memory"]["peak_rss_mb_sampled"]) < MEMORY_CEILING_MB,
        "run report reconciles with generator": report["reconciliation"]["ok"],
    }
    for name, tank, s, e in windows:
        r = query(store, tank, s, e)
        got = Counter((x["timestamp"], x["metric"], x["value"], x["source_file"]) for x in readings(r["table"]))
        files = sorted(r["by_source_file"])
        checks[f"{name}: {r['count']:,} readings == brute force {sum(expected[name].values()):,} "
               f"(files {files}, {r['elapsed_ms']} ms)"] = got == expected[name] and r["count"] > 0
    for w in manifest["planted_windows"]:
        checks[f"{w['name']}: split across {len(w['by_file'])} files as planted"] = len(w["by_file"]) >= 2
    width = max(map(len, checks))
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")
    ok = all(checks.values())
    print("ACCEPTANCE", "PASSED" if ok else "FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
