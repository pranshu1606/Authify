"""Time-range query for one tank over the compacted Parquet store.

    python -m query.engine --tank 7 --start 2026-09-22T02:00 --end 2026-09-22T04:00

Correctness: compaction put every reading in the partition of its own
timestamp, so the only partitions that can hold a match are the ones the
window overlaps. We open all of those (never a single "likely" file) and
filter tank + time inside them. Which raw file a reading came from plays no
part in finding it; it is only reported, as source_file.

Speed: partitions outside the window are never opened, and inside a file the
(tank_id, timestamp) sort gives each row group tight min/max statistics, so
Arrow skips row groups that cannot match.

The window is inclusive at both ends: [start, end].
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from compactor.compact import DAY_MS, EPOCH, HOUR_MS, SCHEMES
from schema import TANK_ID_RANGE

MAX_RESULT_ROWS = 500_000
REPORT_NAME = "_report.json"


class QueryError(ValueError):
    """A problem with the request that the user can fix; the message is shown as-is."""


class StoreMissing(QueryError):
    pass


def parse_time(value, name):
    """Accept 'YYYY-MM-DDTHH:MM[:SS[.fff]]' (what <input type=datetime-local> sends). UTC assumed."""
    if not isinstance(value, str) or not value.strip():
        raise QueryError(f"{name} time is required, e.g. 2026-09-22T02:00")
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise QueryError(f"{name} time {value!r} is not a valid timestamp; use YYYY-MM-DDTHH:MM[:SS]") from None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def parse_tank(value):
    try:
        tank = int(str(value).strip())
    except ValueError:
        raise QueryError(f"tank {value!r} is not a whole number") from None
    if not TANK_ID_RANGE[0] <= tank <= TANK_ID_RANGE[1]:
        raise QueryError(f"tank must be between {TANK_ID_RANGE[0]} and {TANK_ID_RANGE[1]}")
    return tank


def load_report(store):
    path = Path(store) / REPORT_NAME
    if not path.exists():
        raise StoreMissing("No compacted store yet: run the compaction first.")
    return json.loads(path.read_text())


def _to_ms(dt):
    return int((dt - EPOCH).total_seconds() * 1000)


def candidate_partitions(scheme, tank, start, end):
    """Relative directories of every partition that can contain [start, end] for this tank."""
    _, key_dir = SCHEMES[scheme]
    step = HOUR_MS if scheme == "hour" else DAY_MS
    for k in range(_to_ms(start) // step, _to_ms(end) // step + 1):
        yield key_dir((k, tank) if scheme == "day_tank" else k)


def query(store, tank, start, end):
    """All readings for `tank` with start <= timestamp <= end, sorted by time then metric.

    `tank`, `start`, `end` may be raw strings (from the web form) or already parsed.
    """
    t0 = time.perf_counter()
    tank = parse_tank(tank)
    start = start if isinstance(start, datetime) else parse_time(start, "start")
    end = end if isinstance(end, datetime) else parse_time(end, "end")
    if start > end:
        raise QueryError("start time must not be after end time")
    store = Path(store)
    report = load_report(store)
    scheme = report["settings"]["scheme"]

    files = []
    for rel in candidate_partitions(scheme, tank, start, end):
        files.extend(sorted(str(p) for p in (store / rel).glob("*.parquet")))
    ts_type = pa.timestamp("ms")
    flt = ((ds.field("tank_id") == tank)
           & (ds.field("timestamp") >= pa.scalar(start, ts_type))
           & (ds.field("timestamp") <= pa.scalar(end, ts_type)))

    rg_total = rg_read = 0
    if files:
        dataset = ds.dataset(files, format="parquet")
        for frag in dataset.get_fragments():
            rg_total += frag.num_row_groups
            rg_read += len(frag.split_by_row_group(flt))  # row groups whose statistics can match
        count = dataset.count_rows(filter=flt)
        if count > MAX_RESULT_ROWS:
            raise QueryError(f"that window holds {count:,} readings for tank {tank}; a single query returns "
                             f"at most {MAX_RESULT_ROWS:,}, so please narrow the window")
        table = dataset.to_table(filter=flt, columns=["timestamp", "metric", "value", "source_file"])
        # Stored dictionary-encoded (1 byte/row); decode the small result so it can be sorted.
        for i in (1, 3):
            table = table.set_column(i, table.field(i).name, pc.cast(table.column(i), pa.string()))
        table = table.sort_by([("timestamp", "ascending"), ("metric", "ascending")])
    else:
        table = pa.table({"timestamp": pa.array([], ts_type), "metric": pa.array([], pa.string()),
                          "value": pa.array([], pa.float64()), "source_file": pa.array([], pa.string())})

    note = None
    if table.num_rows == 0:
        known = report["output"].get("tanks")
        if known and tank not in known:
            note = f"tank {tank} does not appear in the store (tanks present: {min(known)}-{max(known)})"
        else:
            note = "no readings for this tank in this window"
    return {
        "tank_id": tank,
        "start": start.isoformat(timespec="milliseconds"),
        "end": end.isoformat(timespec="milliseconds"),
        "count": table.num_rows,
        "by_metric": _value_counts(table["metric"]),
        "by_source_file": _value_counts(table["source_file"]),
        "partitions_opened": len(files),
        "row_groups_read": rg_read,
        "row_groups_total": rg_total,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "note": note,
        "table": table,
    }


def _value_counts(col):
    if len(col) == 0:
        return {}
    vc = pc.value_counts(pc.cast(col, pa.string()))
    return dict(sorted(zip(vc.field("values").to_pylist(), vc.field("counts").to_pylist())))


def readings(table):
    """Arrow result -> list of plain dicts for JSON."""
    ts = pc.strftime(table["timestamp"], "%Y-%m-%dT%H:%M:%S").to_pylist()  # Arrow prints SS.mmm for ms timestamps
    return [{"timestamp": t, "metric": m, "value": v, "source_file": s}
            for t, m, v, s in zip(ts, table["metric"].to_pylist(), table["value"].to_pylist(),
                                  table["source_file"].to_pylist())]


def main(argv=None):
    p = argparse.ArgumentParser(description="Readings for one tank in a time window (inclusive).")
    p.add_argument("--store", default="data/compacted")
    p.add_argument("--tank", required=True)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--show", type=int, default=10, help="print the first N readings")
    a = p.parse_args(argv)
    try:
        r = query(a.store, a.tank, a.start, a.end)
    except QueryError as e:
        sys.exit(f"error: {e}")
    rows = readings(r.pop("table").slice(0, a.show))
    print(json.dumps(r, indent=2))
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
