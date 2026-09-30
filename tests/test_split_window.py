"""The case the brief says separates a correct answer from a plausible one:
one tank's two-hour window split across two raw files, out of order.

These files are written by hand, so the expected answer does not depend on
the generator being right.
"""

import pytest

from compactor.compact import SCHEMES, compact
from query.engine import query, readings

FILE_001 = """\
2026-09-22T03:20:00.000,7,dissolved_oxygen,3.20
2026-09-22T02:10:00.000,3,dissolved_oxygen,8.00
2026-09-22T02:10:00.000,7,dissolved_oxygen,2.10
2026-09-22T01:59:59.999,7,dissolved_oxygen,1.99
2026-09-22T02:13:2,7,dissolved_oxygen,9.99
"""
FILE_002 = """\
2026-09-22T03:45:00.000,7,dissolved_oxygen,3.45
2026-09-22T04:00:00.001,7,dissolved_oxygen,4.00
2026-09-22T02:40:00.000,7,dissolved_oxygen,2.40
2026-09-22T02:40:00.000,8,dissolved_oxygen,7.77
2026-09-22T02:05:00.000,7,dissolved_oxygen,2.05
2026-09-22T03:00:00.000,7,dissolv"""  # writer died mid-line

EXPECTED = [
    ("2026-09-22T02:05:00.000", 2.05, "file_002.csv"),
    ("2026-09-22T02:10:00.000", 2.10, "file_001.csv"),
    ("2026-09-22T02:40:00.000", 2.40, "file_002.csv"),
    ("2026-09-22T03:20:00.000", 3.20, "file_001.csv"),
    ("2026-09-22T03:45:00.000", 3.45, "file_002.csv"),
]


def _build(tmp_path, scheme, spill_rows, extra=None):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "file_001.csv").write_text(FILE_001, newline="\n")
    (raw / "file_002.csv").write_text(FILE_002, newline="\n")
    for name, text in (extra or {}).items():
        (raw / name).write_text(text, newline="\n")
    report = compact(raw, tmp_path / "out", scheme=scheme, spill_rows=spill_rows)
    return tmp_path / "out", report


@pytest.mark.parametrize("scheme", sorted(SCHEMES))
@pytest.mark.parametrize("spill_rows", [1, 1_000_000])
def test_split_window_returns_every_reading_from_both_files(tmp_path, scheme, spill_rows):
    store, report = _build(tmp_path, scheme, spill_rows)
    r = query(store, 7, "2026-09-22T02:00", "2026-09-22T04:00")
    got = [(x["timestamp"], x["value"], x["source_file"]) for x in readings(r["table"])]
    assert got == EXPECTED  # all five, in time order, each attributed to the file it came from
    assert r["by_source_file"] == {"file_001.csv": 2, "file_002.csv": 3}
    # and the lines that are not readings were counted, not lost
    assert report["totals"]["rejects_by_reason"] == {"bad_timestamp": 1, "wrong_field_count": 1}
    assert report["reconciliation"]["ok"]


@pytest.mark.parametrize("scheme", sorted(SCHEMES))
def test_window_edges_are_inclusive_and_one_ms_outside_is_excluded(tmp_path, scheme):
    edges = ("2026-09-22T02:00:00.000,7,dissolved_oxygen,2.00\n"
             "2026-09-22T04:00:00.000,7,dissolved_oxygen,4.44\n")
    store, _ = _build(tmp_path, scheme, 1, {"file_003.csv": edges})
    got = [x["timestamp"] for x in readings(query(store, 7, "2026-09-22T02:00", "2026-09-22T04:00")["table"])]
    assert got[0] == "2026-09-22T02:00:00.000" and got[-1] == "2026-09-22T04:00:00.000"
    assert len(got) == 7
    assert "2026-09-22T01:59:59.999" not in got and "2026-09-22T04:00:00.001" not in got


@pytest.mark.parametrize("scheme", sorted(SCHEMES))
def test_window_across_midnight_and_across_files(tmp_path, scheme):
    a = "2026-09-22T23:30:00.000,7,temperature,14.00\n2026-09-23T00:45:00.000,7,temperature,13.50\n"
    b = "2026-09-23T00:15:00.000,7,temperature,13.80\n2026-09-22T23:50:00.000,7,temperature,13.90\n"
    store, _ = _build(tmp_path, scheme, 1, {"late_a.csv": a, "late_b.csv": b})
    r = query(store, 7, "2026-09-22T23:00", "2026-09-23T01:00")
    assert [x["timestamp"][11:16] for x in readings(r["table"])] == ["23:30", "23:50", "00:15", "00:45"]
    assert r["by_source_file"] == {"late_a.csv": 2, "late_b.csv": 2}
