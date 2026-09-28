"""Streaming line reader and strict parser for raw sensor logs.

    python -m compactor.records data/raw/sensors_01.csv   # scan one file, print running counts

Memory is bounded by construction: iter_lines() never holds more than one
line of at most MAX_LINE_BYTES (plus the 1 MiB read buffer), whatever the file
size. Lines are parsed as bytes (int(b"7") and float(b"6.82") work directly),
which skips a UTF-8 decode per line; garbage bytes just fail validation.
"""

import math
import sys
import time
from collections import Counter
from datetime import datetime, timedelta

from schema import METRICS, TANK_ID_RANGE, TIMESTAMP_LEN, VALID_RANGES

MAX_LINE_BYTES = 1024  # a valid line is < 60 bytes; anything this long is garbage
READ_BUFFER_BYTES = 1 << 20

METRIC_CODES = {m.encode(): i for i, m in enumerate(METRICS)}  # b"dissolved_oxygen" -> 0
RANGES_BY_CODE = [VALID_RANGES[m] for m in METRICS]
TOO_LONG = object()  # sentinel yielded by iter_lines for an oversized line

_EPOCH = datetime(1970, 1, 1)
_hour_cache = {}  # b"2026-09-22T02" -> epoch ms of that hour; one entry per distinct hour


def iter_lines(path):
    """Yield each raw line (bytes, newline included) or TOO_LONG. Never reads a whole file."""
    with open(path, "rb", buffering=READ_BUFFER_BYTES) as f:
        while True:
            line = f.readline(MAX_LINE_BYTES)
            if not line:
                return
            if len(line) == MAX_LINE_BYTES and not line.endswith(b"\n"):
                while True:  # discard the rest of the oversized line in bounded pieces
                    rest = f.readline(MAX_LINE_BYTES)
                    if not rest or rest.endswith(b"\n"):
                        break
                yield TOO_LONG
            else:
                yield line


def _hour_ms(prefix):
    """Epoch ms for b'YYYY-MM-DDTHH', or None if it is not a real calendar hour."""
    ms = _hour_cache.get(prefix)
    if ms is None:
        if not (prefix[:4].isdigit() and prefix[4:5] == b"-" and prefix[5:7].isdigit() and prefix[7:8] == b"-"
                and prefix[8:10].isdigit() and prefix[10:11] == b"T" and prefix[11:13].isdigit()):
            return None
        try:
            dt = datetime.strptime(prefix.decode("ascii"), "%Y-%m-%dT%H")
        except ValueError:  # e.g. 2026-09-31 or hour 25
            return None
        ms = int((dt - _EPOCH).total_seconds()) * 1000
        if len(_hour_cache) < 100_000:
            _hour_cache[prefix] = ms
    return ms


def parse_timestamp(ts):
    """Strict YYYY-MM-DDTHH:MM:SS.mmm (bytes) -> epoch milliseconds (UTC), or None."""
    if len(ts) != TIMESTAMP_LEN or ts[13:14] != b":" or ts[16:17] != b":" or ts[19:20] != b".":
        return None
    base = _hour_ms(ts[:13])
    mm, ss, frac = ts[14:16], ts[17:19], ts[20:23]
    if base is None or not (mm.isdigit() and ss.isdigit() and frac.isdigit()):
        return None
    minute, second = int(mm), int(ss)
    if minute > 59 or second > 59:
        return None
    return base + minute * 60_000 + second * 1000 + int(frac)


def parse_line(line):
    """Parse one raw line (bytes).

    Returns (ts_ms, tank_id, metric_code, value) for a good reading, or a reject
    reason (str, one of schema.REJECT_REASONS) for a bad one. Never raises.
    """
    if line is TOO_LONG:
        return "line_too_long"
    line = line.rstrip(b"\r\n")
    if not line.strip():
        return "empty_line"
    fields = line.split(b",")
    if len(fields) != 4:
        return "wrong_field_count"
    ts_raw, tank_raw, metric_raw, value_raw = fields

    ts = parse_timestamp(ts_raw)
    if ts is None:
        return "bad_timestamp"

    if not tank_raw.isdigit():  # ASCII digits only: rejects "", "-3", "7.5", "T7", " 7"
        return "bad_tank_id"
    tank = int(tank_raw)
    if not TANK_ID_RANGE[0] <= tank <= TANK_ID_RANGE[1]:
        return "bad_tank_id"

    metric = METRIC_CODES.get(metric_raw)
    if metric is None:
        return "unknown_metric"

    try:
        value = float(value_raw)
    except ValueError:
        return "bad_value"
    if not math.isfinite(value):  # float() happily accepts "nan" and "inf"
        return "bad_value"
    lo, hi = RANGES_BY_CODE[metric]
    if not lo <= value <= hi:
        return "out_of_range"
    return ts, tank, metric, value


def format_ts(ts_ms):
    """Epoch ms -> 'YYYY-MM-DDTHH:MM:SS.mmm' (inverse of parse_timestamp)."""
    return (_EPOCH + timedelta(milliseconds=ts_ms)).isoformat(timespec="milliseconds")


def scan(path, every=5_000_000):
    """Stream one file, validating every line; print a running count and RSS."""
    import psutil

    proc = psutil.Process()
    counts = Counter()
    peak_rss = proc.memory_info().rss
    t0 = time.perf_counter()
    lines = 0
    for lines, raw in enumerate(iter_lines(path), 1):
        r = parse_line(raw)
        counts["valid" if type(r) is tuple else r] += 1
        if lines % every == 0:
            peak_rss = max(peak_rss, proc.memory_info().rss)
            print(f"  {lines:>12,} lines  {time.perf_counter() - t0:6.1f}s  rss {peak_rss / 2**20:6.1f} MB",
                  flush=True)
    elapsed = time.perf_counter() - t0
    peak_rss = max(peak_rss, proc.memory_info().rss)
    return lines, counts, elapsed, peak_rss


if __name__ == "__main__":
    import os

    for p in sys.argv[1:]:
        print(f"scanning {p} ({os.path.getsize(p) / 2**20:.1f} MB)")
        n, c, secs, rss = scan(p)
        print(f"done: {n:,} lines in {secs:.1f}s ({n / secs / 1e6:.2f} M lines/s), peak rss {rss / 2**20:.1f} MB")
        print(f"      {dict(sorted(c.items()))}")
