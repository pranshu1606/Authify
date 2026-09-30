import random
from collections import Counter
from datetime import datetime, timedelta

import pytest

from compactor.compact import compact
from compactor.records import format_ts, iter_lines, parse_line
from generator.generate import generate
from query.engine import MAX_RESULT_ROWS, QueryError, StoreMissing, query, readings
from schema import METRICS


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    base = tmp_path_factory.mktemp("q")
    manifest = generate(base / "raw", seed=5, interval_ms=60_000, progress=False)
    compact(base / "raw", base / "out", spill_rows=20_000)
    return base, manifest


def _brute_force(raw_dir, tank, start, end):
    """Oracle: scan every raw line; independent of partitions, sorting and Parquet."""
    lo, hi = format_ts_bounds(start, end)
    out = Counter()
    for f in sorted(raw_dir.glob("*.csv")):
        for line in iter_lines(f):
            r = parse_line(line)
            if type(r) is tuple and r[1] == tank and lo <= r[0] <= hi:
                out[(format_ts(r[0]), METRICS[r[2]], r[3], f.name)] += 1
    return out


def format_ts_bounds(start, end):
    epoch = datetime(1970, 1, 1)
    return (int((start - epoch).total_seconds() * 1000), int((end - epoch).total_seconds() * 1000))


def _got(r):
    return Counter((x["timestamp"], x["metric"], x["value"], x["source_file"]) for x in readings(r["table"]))


def test_matches_brute_force_scan_on_random_windows(world):
    base, _ = world
    rng = random.Random(1)
    for _ in range(8):
        tank = rng.randint(1, 10)
        start = datetime(2026, 9, 14) + timedelta(minutes=rng.randint(0, 13 * 24 * 60))
        end = start + timedelta(minutes=rng.choice([1, 120, 600, 1500]))
        r = query(base / "out", tank, start, end)
        assert _got(r) == _brute_force(base / "raw", tank, start, end), (tank, start, end)


def test_planted_windows_match_manifest(world):
    base, manifest = world
    for w in manifest["planted_windows"]:
        r = query(base / "out", w["tank_id"], w["start"], w["end"])
        assert r["count"] == w["expected_count"]
        assert r["by_source_file"] == w["by_file"]
        assert r["by_metric"] == w["by_metric"]


def test_only_requested_tank_and_range(world):
    base, _ = world
    start, end = datetime(2026, 9, 20, 10), datetime(2026, 9, 20, 12)
    r4 = query(base / "out", 4, start, end)
    rows = readings(r4["table"])
    assert r4["count"] == len(rows) > 0
    assert all("2026-09-20T10:00:00.000" <= x["timestamp"] <= "2026-09-20T12:00:00.000" for x in rows)
    # Tank 5 logs at exactly the same timestamps; none of its readings may leak into tank 4's answer.
    assert _got(r4) == _brute_force(base / "raw", 4, start, end)
    assert not _got(r4) & _brute_force(base / "raw", 5, start, end)


def test_results_sorted_by_time_then_metric(world):
    base, _ = world
    rows = readings(query(base / "out", 2, "2026-09-18T00:00", "2026-09-19T00:00")["table"])
    keys = [(x["timestamp"], x["metric"]) for x in rows]
    assert keys == sorted(keys)


def test_empty_results(world):
    base, _ = world
    r = query(base / "out", 3, "2030-01-01T00:00", "2030-01-01T02:00")
    assert r["count"] == 0 and readings(r["table"]) == [] and r["partitions_opened"] == 0
    r = query(base / "out", 42, "2026-09-20T00:00", "2026-09-20T02:00")
    assert r["count"] == 0 and "does not appear" in r["note"]


@pytest.mark.parametrize("tank,start,end,msg", [
    ("seven", "2026-09-20T00:00", "2026-09-20T01:00", "whole number"),
    ("0", "2026-09-20T00:00", "2026-09-20T01:00", "between"),
    ("7", "", "2026-09-20T01:00", "required"),
    ("7", "yesterday", "2026-09-20T01:00", "not a valid timestamp"),
    ("7", "2026-09-20T02:00", "2026-09-20T01:00", "must not be after"),
])
def test_bad_input_gives_useful_error(world, tank, start, end, msg):
    base, _ = world
    with pytest.raises(QueryError, match=msg):
        query(base / "out", tank, start, end)


def test_oversized_window_is_refused_not_truncated(world, monkeypatch):
    base, _ = world
    import query.engine as eng
    monkeypatch.setattr(eng, "MAX_RESULT_ROWS", 100)
    with pytest.raises(QueryError, match="narrow the window"):
        eng.query(base / "out", 1, "2026-09-14T00:00", "2026-09-27T23:59")
    assert MAX_RESULT_ROWS == 500_000


def test_missing_store(tmp_path):
    with pytest.raises(StoreMissing, match="run the compaction"):
        query(tmp_path, 7, "2026-09-22T02:00", "2026-09-22T04:00")


def test_absurdly_wide_window_is_bounded_by_the_store_not_the_calendar(world):
    base, _ = world
    import time
    t0 = time.perf_counter()
    r = query(base / "out", 7, "2026-09-22T02:00", "9999-12-31T23:59")  # refuses (too many rows) or answers
    assert time.perf_counter() - t0 < 5
    assert r["partitions_opened"] == 6  # 2026-09-22 .. 2026-09-27: only partitions that exist
