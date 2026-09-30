"""Out-of-core compaction: raw CSV logs -> partitioned, sorted Parquet store.

    python -m compactor.compact --raw data/raw --out data/compacted --scheme day

Pass 1 (streaming). Every line of every file is parsed; each valid reading is
routed to the partition of ITS OWN timestamp (never of the file it came from)
and appended to a compact in-memory buffer. When SPILL_ROWS readings are
buffered in total, every buffer is written to staging as an Arrow IPC "run"
file and cleared. Memory: O(SPILL_ROWS), independent of input size.

Pass 2 (one partition at a time). A partition's runs are read back, sorted by
(tank_id, timestamp) and written as one Parquet file whose row groups carry
min/max statistics, so a query can skip most of the file. Memory: O(largest
partition). A partition too big to sort within budget is written unsorted
and flagged: sorting only speeds queries up, correctness never depends on it.

The new store is built next to the old one and swapped in by rename, so a
failed run never leaves a half-written store behind.
"""

import argparse
import json
import os
import shutil
import sys
import threading
import time
from array import array
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psutil
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from compactor.records import iter_lines, parse_line
from schema import MEMORY_CEILING_MB, METRICS

DAY_MS = 86_400_000
HOUR_MS = 3_600_000
EPOCH = datetime(1970, 1, 1)

SPILL_ROWS = 2_000_000  # pass-1 buffer budget: ~2M x 20 B = ~40 MB of payload
MAX_SORT_ROWS = 8_000_000  # pass-2: bigger partitions are written unsorted (see docstring)
ROW_GROUP_ROWS = 65_536
REJECT_SAMPLES_PER_REASON = 5

STAGING_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms")),
    ("tank_id", pa.int16()),
    ("metric", pa.int8()),
    ("value", pa.float64()),
    ("source_file", pa.int8()),
])


def _day(ts):
    return (EPOCH + timedelta(milliseconds=ts)).strftime("%Y-%m-%d")


# Partition schemes: key function (cheap, runs per row) + key -> relative directory.
SCHEMES = {
    "day": (lambda ts, tank: ts // DAY_MS,
            lambda k: f"date={_day(k * DAY_MS)}"),
    "hour": (lambda ts, tank: ts // HOUR_MS,
             lambda k: f"date={_day(k * HOUR_MS)}/hour={k % 24:02d}"),
    "day_tank": (lambda ts, tank: (ts // DAY_MS, tank),
                 lambda k: f"date={_day(k[0] * DAY_MS)}/tank={k[1]:03d}"),
    # Coarse extreme, kept for the benchmark: everything in one partition.
    "none": (lambda ts, tank: 0, lambda k: "all"),
}


class _Buffer:
    """Column buffers for one partition, as typed arrays (~20 bytes per reading)."""

    __slots__ = ("ts", "tank", "metric", "value", "source")

    def __init__(self):
        self.ts, self.tank, self.metric = array("q"), array("h"), array("b")
        self.value, self.source = array("d"), array("b")

    def to_batch(self):
        n = len(self.ts)
        cols = [pa.Array.from_buffers(t, n, [None, pa.py_buffer(a)])
                for t, a in zip(STAGING_SCHEMA.types, (self.ts, self.tank, self.metric, self.value, self.source))]
        return pa.RecordBatch.from_arrays(cols, schema=STAGING_SCHEMA)


class PeakMemory:
    """Samples this process's RSS in a background thread; also reads the OS high-water mark."""

    def __init__(self, interval=0.05):
        self.proc = psutil.Process()
        self.peak = self.proc.memory_info().rss
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.wait(self.interval):
            self.peak = max(self.peak, self.proc.memory_info().rss)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()
        self.peak = max(self.peak, self.proc.memory_info().rss)

    def os_peak(self):
        """Exact peak resident set from the OS, when it provides one."""
        mi = self.proc.memory_info()
        if hasattr(mi, "peak_wset"):  # Windows
            return mi.peak_wset
        try:
            import resource  # Linux: ru_maxrss is in KiB
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        except ImportError:
            return None


def _final_table(batches, source_names):
    """Staging batches -> table in the stored schema (metric/source as dictionary strings)."""
    t = pa.Table.from_batches(batches, schema=STAGING_SCHEMA)
    metric = pa.chunked_array([pa.DictionaryArray.from_arrays(c, pa.array(METRICS)) for c in t["metric"].chunks],
                              type=pa.dictionary(pa.int8(), pa.string()))
    source = pa.chunked_array([pa.DictionaryArray.from_arrays(c, pa.array(source_names))
                               for c in t["source_file"].chunks],
                              type=pa.dictionary(pa.int8(), pa.string()))
    return t.set_column(2, "metric", metric).set_column(4, "source_file", source)


def compact(raw_dir, out_dir, scheme="day", spill_rows=SPILL_ROWS, row_group_rows=ROW_GROUP_ROWS,
            max_sort_rows=MAX_SORT_ROWS, on_progress=None):
    raw_dir, out_dir = Path(raw_dir), Path(out_dir)
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"raw log directory {raw_dir} does not exist; run the generator first")
    key_fn, key_dir = SCHEMES[scheme]
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    progress = on_progress or (lambda **kw: None)

    files = sorted(raw_dir.glob("*.csv"))
    if len(files) > 127:
        raise ValueError("source_file is stored as int8: at most 127 raw files per run")
    total_bytes = sum(f.stat().st_size for f in files)
    work = out_dir.parent / (out_dir.name + ".tmp")
    staging = work / "_staging"
    shutil.rmtree(work, ignore_errors=True)
    staging.mkdir(parents=True)

    buffers, rows_by_key, runs_by_key = {}, Counter(), Counter()
    per_file, skipped, samples = [], [], {}
    state = {"buffered": 0, "spills": 0}

    def spill():
        for key, buf in buffers.items():
            if not buf.ts:
                continue
            d = staging / key_dir(key)
            d.mkdir(parents=True, exist_ok=True)
            with pa.OSFile(str(d / f"run-{runs_by_key[key]:05d}.arrow"), "wb") as sink:
                with pa.ipc.new_file(sink, STAGING_SCHEMA) as w:
                    w.write_batch(buf.to_batch())
            runs_by_key[key] += 1
        buffers.clear()
        state["buffered"] = 0
        state["spills"] += 1

    with PeakMemory() as mem:
        # ---------------- pass 1: stream, validate, route, spill ----------------
        bytes_done = 0
        for fidx, path in enumerate(files):
            fstat = {"file": path.name, "bytes": path.stat().st_size, "lines": 0, "valid": 0, "rejects": Counter()}
            try:
                for lineno, raw in enumerate(iter_lines(path), 1):
                    r = parse_line(raw)
                    if type(r) is tuple:
                        ts, tank, metric, value = r
                        key = key_fn(ts, tank)
                        buf = buffers.get(key)
                        if buf is None:
                            buf = buffers[key] = _Buffer()
                        buf.ts.append(ts)
                        buf.tank.append(tank)
                        buf.metric.append(metric)
                        buf.value.append(value)
                        buf.source.append(fidx)
                        rows_by_key[key] += 1
                        fstat["valid"] += 1
                        state["buffered"] += 1
                        if state["buffered"] >= spill_rows:
                            spill()
                    else:
                        fstat["rejects"][r] += 1
                        s = samples.setdefault(r, [])
                        if len(s) < REJECT_SAMPLES_PER_REASON:
                            text = "<line longer than limit>" if not isinstance(raw, bytes) else \
                                raw.rstrip(b"\r\n").decode("utf-8", "replace")[:120]
                            s.append({"file": path.name, "line": lineno, "text": text})
                    if lineno % 1_000_000 == 0:
                        progress(phase="read", file=path.name, lines=sum(f["lines"] for f in per_file) + lineno,
                                 fraction=(bytes_done + fstat["bytes"] * 0.5) / max(total_bytes, 1))
                    fstat["lines"] = lineno
            except OSError as e:  # unreadable file: record it and keep going with the others
                skipped.append({"file": path.name, "reason": f"{type(e).__name__}: {e}",
                                "lines_read_before_error": fstat["lines"]})
            bytes_done += fstat["bytes"]
            fstat["rejects"] = dict(sorted(fstat["rejects"].items()))
            per_file.append(fstat)
        spill()
        pass1_s = time.perf_counter() - t0
        pass1_peak = max(mem.peak, mem.proc.memory_info().rss)

        # ---------------- pass 2: per partition, sort + write Parquet ----------------
        source_names = [f.name for f in files]
        unsorted, out_files, out_rows, tanks = [], 0, 0, set()
        for i, key in enumerate(sorted(rows_by_key)):
            rel = key_dir(key)
            part_dir = work / rel
            part_dir.mkdir(parents=True, exist_ok=True)
            runs = sorted((staging / rel).glob("run-*.arrow"))
            target = part_dir / "part-0.parquet"
            if rows_by_key[key] <= max_sort_rows:
                batches = []
                for r in runs:
                    with pa.OSFile(str(r), "rb") as src:
                        batches.extend(pa.ipc.open_file(src).read_all().to_batches())
                table = _final_table(batches, source_names)
                del batches
                table = table.sort_by([("tank_id", "ascending"), ("timestamp", "ascending")])
                pq.write_table(table, target, row_group_size=row_group_rows, compression="zstd")
                tanks.update(pc.unique(table["tank_id"]).to_pylist())
                out_rows += table.num_rows
                del table
            else:  # too big to sort in budget: stream runs straight through, unsorted
                unsorted.append(rel)
                with pq.ParquetWriter(target, _final_table([], source_names).schema, compression="zstd") as w:
                    for r in runs:
                        with pa.OSFile(str(r), "rb") as src:
                            t = _final_table(pa.ipc.open_file(src).read_all().to_batches(), source_names)
                        w.write_table(t, row_group_size=row_group_rows)
                        tanks.update(pc.unique(t["tank_id"]).to_pylist())
                        out_rows += t.num_rows
            out_files += 1
            shutil.rmtree(staging / rel)
            progress(phase="write", partition=rel, fraction=(i + 1) / len(rows_by_key))
        shutil.rmtree(staging)
    elapsed = time.perf_counter() - t0

    rejects = Counter()
    for f in per_file:
        rejects.update(f["rejects"])
    lines = sum(f["lines"] for f in per_file)
    valid = sum(f["valid"] for f in per_file)
    report = {
        "started_at": started.isoformat(timespec="seconds"),
        "elapsed_seconds": round(elapsed, 1),
        "pass1_read_seconds": round(pass1_s, 1),
        "pass2_write_seconds": round(elapsed - pass1_s, 1),
        "settings": {"scheme": scheme, "spill_rows": spill_rows, "row_group_rows": row_group_rows,
                     "max_sort_rows": max_sort_rows, "memory_ceiling_mb": MEMORY_CEILING_MB},
        "memory": {"peak_rss_mb_sampled": round(mem.peak / 2**20, 1),
                   "peak_rss_mb_pass1": round(pass1_peak / 2**20, 1),
                   "peak_rss_mb_os": round(mem.os_peak() / 2**20, 1) if mem.os_peak() else None,
                   "ceiling_mb": MEMORY_CEILING_MB},
        "input": {"files_discovered": len(files), "files_processed": len(files) - len(skipped),
                  "files_skipped": skipped, "bytes": total_bytes},
        "totals": {"lines_read": lines, "valid": valid, "rejected": sum(rejects.values()),
                   "malformed": sum(v for k, v in rejects.items() if k != "out_of_range"),
                   "out_of_range": rejects.get("out_of_range", 0),
                   "rejects_by_reason": dict(sorted(rejects.items()))},
        "reject_samples": samples,
        "output": {"partitions": len(rows_by_key), "files": out_files, "rows": out_rows,
                   "spill_rounds": state["spills"], "staging_runs": sum(runs_by_key.values()),
                   "unsorted_partitions": unsorted, "tanks": sorted(tanks),
                   "bytes": sum(p.stat().st_size for p in work.rglob("*.parquet"))},
        "per_file": per_file,
    }
    report["reconciliation"] = reconcile(report, raw_dir / "manifest.json")
    (work / "_report.json").write_text(json.dumps(report, indent=2))

    # Swap the finished store in. Readers see either the old store or the new one.
    old = out_dir.parent / (out_dir.name + ".old")
    shutil.rmtree(old, ignore_errors=True)
    if out_dir.exists():
        os.replace(out_dir, old)
    os.replace(work, out_dir)
    shutil.rmtree(old, ignore_errors=True)
    return report


def reconcile(report, manifest_path):
    """Check the run's numbers add up, internally and against the generator's manifest."""
    t = report["totals"]
    checks = {
        "lines == valid + rejected": t["lines_read"] == t["valid"] + t["rejected"],
        "output rows == valid": report["output"]["rows"] == t["valid"],
    }
    if manifest_path.exists():
        m = json.loads(manifest_path.read_text())
        expected = {f["file"]: f for f in m["files"]}
        for f in report["per_file"]:
            e = expected.get(f["file"])
            checks[f"{f['file']} matches generator"] = bool(e) and (
                e["lines"], e["valid"], e["rejects"]) == (f["lines"], f["valid"], f["rejects"])
    return {"ok": all(checks.values()), "checks": checks}


def main(argv=None):
    p = argparse.ArgumentParser(description="Compact raw sensor logs into a partitioned Parquet store.")
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--out", default="data/compacted")
    p.add_argument("--scheme", choices=sorted(SCHEMES), default="day")
    p.add_argument("--spill-rows", type=int, default=SPILL_ROWS)
    p.add_argument("--row-group-rows", type=int, default=ROW_GROUP_ROWS)
    p.add_argument("--progress-file", help="also write progress as JSON here (read by the web page)")
    a = p.parse_args(argv)
    proc = psutil.Process()

    def show(**kw):
        kw["rss_mb"] = round(proc.memory_info().rss / 2**20, 1)
        print(f"  [{kw['phase']}] {kw['fraction']:6.1%}  {kw.get('file') or kw.get('partition')}"
              f"  rss {kw['rss_mb']} MB", file=sys.stderr, flush=True)
        if a.progress_file:
            try:
                tmp = Path(a.progress_file + ".tmp")
                tmp.write_text(json.dumps(kw))
                os.replace(tmp, a.progress_file)  # readers never see a half-written file
            except OSError:
                # Best effort only: on Windows the replace fails while the web server has the file
                # open. Progress is cosmetic and must never fail the compaction; skip this update.
                pass

    try:
        r = compact(a.raw, a.out, a.scheme, a.spill_rows, a.row_group_rows, on_progress=show)
    except (FileNotFoundError, ValueError) as e:
        sys.exit(f"error: {e}")
    summary = {k: r[k] for k in ("elapsed_seconds", "pass1_read_seconds", "pass2_write_seconds",
                                 "memory", "input", "totals", "output", "reconciliation")}
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
