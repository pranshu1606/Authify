from collections import Counter

import pytest

from compactor.records import MAX_LINE_BYTES, format_ts, iter_lines, parse_line, parse_timestamp
from generator.generate import BAD_METRICS, BAD_NUMBERS, BAD_TANK_IDS, BAD_TIMESTAMPS, generate

GOOD = b"2026-09-22T02:13:21.400,7,dissolved_oxygen,6.82\n"


def test_good_line():
    ts, tank, metric, value = parse_line(GOOD)
    assert format_ts(ts) == "2026-09-22T02:13:21.400"
    assert (tank, metric, value) == (7, 0, 6.82)
    assert parse_line(GOOD.replace(b"\n", b"\r\n"))[1] == 7  # CRLF tolerated


@pytest.mark.parametrize("line,reason", [
    (b"\n", "empty_line"),
    (b"2026-09-22T02:13:21.400,7,dissolved_oxygen\n", "wrong_field_count"),
    (b"2026-09-22T02:13:21.400,7,dissolved_oxygen,6.8,1\n", "wrong_field_count"),
    (b"2026-09-22T02:1", "wrong_field_count"),                           # truncated, no newline
    (b"2026-09-22 02:13:21.400,7,temperature,12.1\n", "bad_timestamp"),  # space instead of T
    (b"2026-02-29T00:00:00.000,7,temperature,12.1\n", "bad_timestamp"),  # 2026 is not a leap year
    (b"2026-09-22T02:60:00.000,7,temperature,12.1\n", "bad_timestamp"),
    (b"2026-09-22T02:13:21.400,1000,temperature,12.1\n", "bad_tank_id"),
    (b"2026-09-22T02:13:21.400,7,temperature,-2.01\n", "out_of_range"),
    (b"2026-09-22T02:13:21.400,7,dissolved_oxygen,20.5\n", "out_of_range"),
    (b"\xff\xfe\x00garbage,\x81,\x82,\x83\n", "bad_timestamp"),          # non-UTF-8 bytes don't crash
])
def test_rejects(line, reason):
    assert parse_line(line) == reason


def test_every_generator_defect_is_rejected_with_the_right_reason():
    ts, tank, metric, value = "2026-09-22T02:13:21.400", "7", "temperature", "12.00"
    cases = ([(f"{b},{tank},{metric},{value}", "bad_timestamp") for b in BAD_TIMESTAMPS]
             + [(f"{ts},{b},{metric},{value}", "bad_tank_id") for b in BAD_TANK_IDS]
             + [(f"{ts},{tank},{b},{value}", "unknown_metric") for b in BAD_METRICS]
             + [(f"{ts},{tank},{metric},{b}", "bad_value") for b in BAD_NUMBERS])
    for line, reason in cases:
        assert parse_line(line.encode() + b"\n") == reason, line


def test_boundary_values_are_valid():
    assert parse_line(b"2026-09-22T02:13:21.400,7,dissolved_oxygen,0.00\n")[3] == 0.0
    assert parse_line(b"2026-09-22T02:13:21.400,7,temperature,40.00\n")[3] == 40.0
    assert parse_timestamp(b"2026-09-22T00:00:00.000") % 86_400_000 == 0


def test_oversized_line_is_bounded_and_rejected(tmp_path):
    p = tmp_path / "x.csv"
    p.write_bytes(GOOD + b"A" * (MAX_LINE_BYTES * 50) + b"\n" + GOOD)
    results = [parse_line(l) for l in iter_lines(p)]
    assert results[1] == "line_too_long" and len(results) == 3
    assert type(results[2]) is tuple  # reader resynchronised on the next line


def test_parser_reconciles_with_generator_manifest(tmp_path):
    m = generate(tmp_path, seed=3, interval_ms=300_000, progress=False)
    for f in m["files"]:
        c = Counter()
        for raw in iter_lines(tmp_path / f["file"]):
            r = parse_line(raw)
            c["valid" if type(r) is tuple else r] += 1
        assert c.pop("valid") == f["valid"], f["file"]
        assert dict(c) == f["rejects"], f["file"]
