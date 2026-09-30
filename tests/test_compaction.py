import json

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pytest

from compactor.compact import compact
from generator.generate import generate

SMALL = dict(seed=11, interval_ms=300_000, progress=False)


def _store(out):
    return ds.dataset(out, format="parquet", partitioning="hive").to_table()


@pytest.fixture(scope="module")
def small_run(tmp_path_factory):
    base = tmp_path_factory.mktemp("run")
    manifest = generate(base / "raw", **SMALL)
    report = compact(base / "raw", base / "out", spill_rows=5_000)  # force many spills
    return base, manifest, report


def test_malformed_lines_do_not_crash_and_are_counted(small_run):
    _, manifest, report = small_run
    assert report["totals"]["rejected"] == sum(manifest["totals"]["rejects"].values()) > 0
    assert report["totals"]["rejects_by_reason"] == manifest["totals"]["rejects"]
    assert all(report["reject_samples"][r] for r in manifest["totals"]["rejects"])


def test_report_reconciles_with_generator(small_run):
    _, manifest, report = small_run
    assert report["reconciliation"]["ok"], report["reconciliation"]
    assert report["totals"]["lines_read"] == manifest["totals"]["lines"]
    assert report["output"]["rows"] == report["totals"]["valid"] == manifest["totals"]["valid"]
    assert report["input"]["files_discovered"] == report["input"]["files_processed"] == 6
    assert report["output"]["spill_rounds"] > 10  # the bounded-buffer path was really exercised


def test_out_of_order_input_is_sorted_within_partitions(small_run):
    base, _, report = small_run
    assert report["output"]["partitions"] == 140 and not report["output"]["unsorted_partitions"]  # 14 days x 10 tanks
    for f in (base / "out").rglob("*.parquet"):
        t = ds.dataset(f).to_table()
        key = pc.add(pc.multiply(pc.cast(t["tank_id"], "int64"), 10**13), pc.cast(t["timestamp"], "int64"))
        assert pc.all(pc.greater_equal(key[1:], key[:-1])).as_py(), f


def test_every_valid_reading_lands_in_its_own_day(small_run):
    base, _, _ = small_run
    t = _store(base / "out")
    days = pc.strftime(t["timestamp"], "%Y-%m-%d")
    assert pc.all(pc.equal(days, pc.cast(t["date"], "string"))).as_py()
    assert pc.all(pc.equal(pc.cast(t["tank_id"], "int64"), pc.cast(t["tank"], "int64"))).as_py()


def test_unreadable_file_is_skipped_and_reported(tmp_path):
    generate(tmp_path / "raw", **SMALL)
    (tmp_path / "raw" / "broken.csv").mkdir()  # a "file" that cannot be opened for reading
    report = compact(tmp_path / "raw", tmp_path / "out")
    assert report["input"]["files_discovered"] == 7
    assert report["input"]["files_processed"] == 6
    assert report["input"]["files_skipped"][0]["file"] == "broken.csv"
    assert report["output"]["rows"] == report["totals"]["valid"]


def test_oversized_partition_falls_back_to_unsorted_without_losing_rows(tmp_path):
    m = generate(tmp_path / "raw", **SMALL)
    report = compact(tmp_path / "raw", tmp_path / "out", spill_rows=5_000, max_sort_rows=100)
    assert len(report["output"]["unsorted_partitions"]) == 140
    assert _store(tmp_path / "out").num_rows == m["totals"]["valid"]


def test_rerun_replaces_store_atomically(tmp_path):
    generate(tmp_path / "raw", **SMALL)
    compact(tmp_path / "raw", tmp_path / "out")
    first = _store(tmp_path / "out").num_rows
    compact(tmp_path / "raw", tmp_path / "out")
    assert _store(tmp_path / "out").num_rows == first  # replaced, not appended twice
    assert not (tmp_path / "out.tmp").exists() and not (tmp_path / "out.old").exists()
    assert json.loads((tmp_path / "out" / "_report.json").read_text())["reconciliation"]["ok"]


def test_missing_raw_dir_gives_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="run the generator"):
        compact(tmp_path / "nope", tmp_path / "out")


def test_empty_file_and_garbage_only_file(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "empty.csv").write_bytes(b"")
    (raw / "garbage.csv").write_bytes(b"\x00\x01\x02\n\xff\xfe not a log at all\n,,,\n\n")
    report = compact(raw, tmp_path / "out")
    assert report["input"]["files_processed"] == 2
    assert report["totals"]["lines_read"] == 4 and report["totals"]["valid"] == 0
    assert report["totals"]["rejects_by_reason"] == {"bad_timestamp": 1, "empty_line": 1, "wrong_field_count": 2}
    assert report["output"]["rows"] == 0 and report["reconciliation"]["ok"]


def test_identical_reading_in_two_files_is_kept_twice(tmp_path):
    """Assumption (DECISIONS.md): no de-duplication. An identical line in two files cannot be told
    apart from a probe that genuinely reported the same value twice, so both are kept and reported."""
    raw = tmp_path / "raw"
    raw.mkdir()
    line = "2026-09-22T02:30:00.000,7,dissolved_oxygen,2.50\n"
    (raw / "a.csv").write_text(line, newline="\n")
    (raw / "b.csv").write_text(line, newline="\n")
    compact(raw, tmp_path / "out")
    from query.engine import query
    r = query(tmp_path / "out", 7, "2026-09-22T02:00", "2026-09-22T04:00")
    assert r["count"] == 2 and r["by_source_file"] == {"a.csv": 1, "b.csv": 1}
