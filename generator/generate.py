"""Deterministic generator for messy, out-of-order, append-only sensor logs.

    python -m generator.generate --seed 42

How disorder is produced (mirrors the brief: "tanks buffer their readings and
flush independently"):
  * every tank's readings are cut into flush batches of random size;
  * each batch is appended to a randomly chosen log file (weighted, so file
    sizes are uneven and the largest file alone exceeds the memory ceiling);
  * the batches of an hour are shuffled across tanks before being written;
  * some batches are held back and flushed 1-6 hours late;
  * some batches are internally scrambled.
So readings are out of order within a file and across files, and any tank's
two-hour window is spread over several files.

On top of that, PLANTED_WINDOWS are routed deliberately: every reading of that
tank inside that window goes to exactly two named files, alternating batch by
batch. manifest.json records the exact expected counts so tests (and the web
page) can check that a query finds all of them.

Memory: the generator holds at most a few hours of batches at once, never the
whole dataset.
"""

import argparse
import json
import math
import os
import random
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from schema import MEMORY_CEILING_MB, METRICS, VALID_RANGES

HOUR_MS = 3_600_000
DEFAULT_START = datetime(2026, 9, 14)

# Kind of injected defect -> reject reason the compactor must assign to it.
INJECTION_KINDS = {
    "truncated": "wrong_field_count",
    "missing_field": "wrong_field_count",
    "bad_number": "bad_value",
    "bad_timestamp": "bad_timestamp",
    "bad_tank_id": "bad_tank_id",
    "unknown_metric": "unknown_metric",
    "out_of_range": "out_of_range",
}
INJECTION_WEIGHTS = {
    "truncated": 2,
    "missing_field": 2,
    "bad_number": 2,
    "bad_timestamp": 2,
    "bad_tank_id": 1,
    "unknown_metric": 1,
    "out_of_range": 3,
}
BAD_TIMESTAMPS = ("2026-09-31T02:00:00.000", "2026-09-22T25:61:00.000", "22/09/2026 02:00",
                  "not-a-time", "2026-09-22T02:13:2", "")
BAD_TANK_IDS = ("T7", "", "7.5", "0", "-3")
BAD_METRICS = ("ph", "salinity", "Dissolved_Oxygen", "dissolved_oxyg")
BAD_NUMBERS = ("abc", "", "NaN", "6..82", "7.1e", "inf")

BATCH_SIZE = (200, 2000)
PLANTED_BATCH_SIZE = (20, 200)
DELAY_PROBABILITY = 0.08
MAX_DELAY_HOURS = 6
SCRAMBLE_PROBABILITY = 0.20


@dataclass(frozen=True)
class PlantedWindow:
    name: str
    tank_id: int
    start: datetime  # inclusive
    end: datetime  # inclusive
    files: tuple  # the two file indices the window is split across


PLANTED_WINDOWS = (
    # "What was the oxygen in tank 7 between 02:00 and 04:00 last Tuesday?"
    PlantedWindow("tank7_tuesday_0200_0400", 7, datetime(2026, 9, 22, 2), datetime(2026, 9, 22, 4), (1, 3)),
    # Same problem, but the window also crosses midnight (two daily partitions).
    PlantedWindow("tank3_across_midnight", 3, datetime(2026, 9, 24, 23), datetime(2026, 9, 25, 1), (0, 4)),
)


def file_weights(n_files):
    """Uneven file sizes: the first file gets the largest share."""
    raw = [1.0 / (i + 2) for i in range(n_files)]
    total = sum(raw)
    return [w / total for w in raw]


class Batch:
    """One flush from one tank: lines destined for one file, plus their accounting."""

    __slots__ = ("file_idx", "lines", "valid", "rejects")

    def __init__(self, file_idx):
        self.file_idx = file_idx
        self.lines = []
        self.valid = 0
        self.rejects = Counter()


class Generator:
    def __init__(self, seed, tanks, days, interval_ms, n_files, start, messy_rate):
        if HOUR_MS % interval_ms:
            raise ValueError("interval_ms must divide one hour evenly")
        self.rng = random.Random(seed)
        self.tanks = list(range(1, tanks + 1))
        self.days = days
        self.interval_ms = interval_ms
        self.n_files = n_files
        self.start = start
        self.messy_rate = messy_rate
        self.weights = file_weights(n_files)
        self.kinds = list(INJECTION_WEIGHTS)
        self.kind_weights = list(INJECTION_WEIGHTS.values())
        self.injected = Counter()
        self.end = start + timedelta(days=days)
        self.planted = [w for w in PLANTED_WINDOWS
                        if w.tank_id in self.tanks and start <= w.start and w.end < self.end
                        and max(w.files) < n_files]
        self.planted_counts = {w.name: Counter() for w in self.planted}
        self.planted_turn = Counter()  # alternation state per window; must survive hour boundaries
        rng = self.rng
        self.baseline = {t: (rng.uniform(6.5, 8.5), rng.uniform(12.0, 16.0)) for t in self.tanks}

    # ---- values -------------------------------------------------------------

    def _values(self, tank, hour_start, minute):
        """Smooth daily cycle: oxygen dips at night. Tank 7 has a crash on the Tuesday night."""
        do_base, temp_base = self.baseline[tank]
        frac = (hour_start.hour + minute / 60.0) / 24.0
        do = do_base + 1.2 * math.sin(2 * math.pi * (frac - 0.35))
        temp = temp_base + 1.5 * math.sin(2 * math.pi * (frac - 0.40))
        if tank == 7 and hour_start.date() == datetime(2026, 9, 22).date() and 1 <= hour_start.hour < 5:
            do = 2.5
        return do, temp

    def _messy(self, ts, tank, metric, value_str):
        """Replace a good line with a defective one. Returns (line, kind)."""
        rng = self.rng
        kind = rng.choices(self.kinds, self.kind_weights)[0]
        fields = [ts, str(tank), metric, value_str]
        if kind == "truncated":
            good = ",".join(fields)
            line = good[: rng.randint(1, good.rindex(","))]  # always loses the last field
        elif kind == "missing_field":
            del fields[rng.randrange(4)]
            line = ",".join(fields)
        elif kind == "bad_number":
            fields[3] = rng.choice(BAD_NUMBERS)
            line = ",".join(fields)
        elif kind == "bad_timestamp":
            fields[0] = rng.choice(BAD_TIMESTAMPS)
            line = ",".join(fields)
        elif kind == "bad_tank_id":
            fields[1] = rng.choice(BAD_TANK_IDS)
            line = ",".join(fields)
        elif kind == "unknown_metric":
            fields[2] = rng.choice(BAD_METRICS)
            line = ",".join(fields)
        else:  # out_of_range
            lo, hi = VALID_RANGES[metric]
            bad = lo - rng.uniform(0.5, 10) if rng.random() < 0.5 else hi + rng.uniform(0.5, 40)
            fields[3] = f"{bad:.2f}"
            line = ",".join(fields)
        self.injected[kind] += 1
        return line + "\n", kind

    # ---- batching -----------------------------------------------------------

    def _new_batch(self):
        return Batch(self.rng.choices(range(self.n_files), self.weights)[0]), self.rng.randint(*BATCH_SIZE)

    def _hour_batches(self, hour_start):
        """All flush batches produced by all tanks during one hour."""
        rng = self.rng
        prefix = hour_start.strftime("%Y-%m-%dT%H:")
        hour_end = hour_start + timedelta(hours=1)
        batches = []
        for tank in self.tanks:
            windows = [w for w in self.planted
                       if w.tank_id == tank and w.start < hour_end and w.end >= hour_start]
            planted_state = {w.name: [None, 0] for w in windows}  # open batch, room left
            batch, room = self._new_batch()
            for offset in range(0, HOUR_MS, self.interval_ms):
                minute, rem = divmod(offset, 60_000)
                ts = f"{prefix}{minute:02d}:{rem // 1000:02d}.{rem % 1000:03d}"
                do, temp = self._values(tank, hour_start, minute)
                target = None
                if windows:
                    moment = hour_start + timedelta(milliseconds=offset)
                    target = next((w for w in windows if w.start <= moment <= w.end), None)
                for metric, centre, spread in (("dissolved_oxygen", do, 0.3), ("temperature", temp, 0.2)):
                    lo, hi = VALID_RANGES[metric]
                    value = min(max(centre + (rng.random() - 0.5) * spread, lo + 0.01), hi - 0.01)
                    value_str = f"{value:.2f}"
                    if target is not None:
                        # Planted readings are always valid, alternate between the two files.
                        state = planted_state[target.name]
                        if state[1] == 0:
                            if state[0] is not None:
                                batches.append(state[0])
                            self.planted_turn[target.name] += 1
                            state[0] = Batch(target.files[self.planted_turn[target.name] % 2])
                            state[1] = rng.randint(*PLANTED_BATCH_SIZE)
                        state[0].lines.append(f"{ts},{tank},{metric},{value_str}\n")
                        state[0].valid += 1
                        state[1] -= 1
                        self.planted_counts[target.name][(state[0].file_idx, metric)] += 1
                        continue
                    if rng.random() < self.messy_rate:
                        line, kind = self._messy(ts, tank, metric, value_str)
                        batch.rejects[INJECTION_KINDS[kind]] += 1
                    else:
                        line = f"{ts},{tank},{metric},{value_str}\n"
                        batch.valid += 1
                    batch.lines.append(line)
                    room -= 1
                    if room == 0:
                        batches.append(batch)
                        batch, room = self._new_batch()
            if batch.lines:
                batches.append(batch)
            batches.extend(s[0] for s in planted_state.values() if s[0] is not None and s[0].lines)
        for b in batches:
            if rng.random() < SCRAMBLE_PROBABILITY:
                rng.shuffle(b.lines)
        rng.shuffle(batches)
        return batches

    # ---- driver -------------------------------------------------------------

    def run(self, out_dir, progress=True):
        out_dir.mkdir(parents=True, exist_ok=True)
        paths = [out_dir / f"sensors_{i + 1:02d}.csv" for i in range(self.n_files)]
        stats = [{"file": p.name, "lines": 0, "valid": 0, "rejects": Counter()} for p in paths]
        handles = [open(p, "a", encoding="utf-8", newline="\n") for p in paths]  # append-only
        pending = {}  # hour index -> delayed batches
        hours = self.days * 24
        t0 = time.perf_counter()
        try:
            for h in range(hours):
                to_write = pending.pop(h, [])
                for b in self._hour_batches(self.start + timedelta(hours=h)):
                    if self.rng.random() < DELAY_PROBABILITY:
                        pending.setdefault(h + self.rng.randint(1, MAX_DELAY_HOURS), []).append(b)
                    else:
                        to_write.append(b)
                self.rng.shuffle(to_write)
                self._write(to_write, handles, stats)
                if progress and (h + 1) % 24 == 0:
                    lines = sum(s["lines"] for s in stats)
                    print(f"  day {(h + 1) // 24}/{self.days}: {lines:,} lines, "
                          f"{time.perf_counter() - t0:.0f}s", file=sys.stderr, flush=True)
            for h in sorted(pending):  # tanks that were still holding buffers at the end
                self._write(pending[h], handles, stats)
            # A log whose writer died mid-line: last line of the biggest file has no newline.
            handles[0].write("2026-09-27T23:59:59.600,4,temperat")
            stats[0]["lines"] += 1
            stats[0]["rejects"]["wrong_field_count"] += 1
            self.injected["truncated_at_eof"] += 1
        finally:
            for f in handles:
                f.close()
        for s, p in zip(stats, paths):
            s["bytes"] = os.path.getsize(p)
            s["rejects"] = dict(sorted(s["rejects"].items()))
        return stats, time.perf_counter() - t0

    def _write(self, batches, handles, stats):
        for b in batches:
            handles[b.file_idx].write("".join(b.lines))
            s = stats[b.file_idx]
            s["lines"] += len(b.lines)
            s["valid"] += b.valid
            s["rejects"].update(b.rejects)

    def planted_manifest(self, files):
        out = []
        for w in self.planted:
            counts = self.planted_counts[w.name]
            if len({fi for fi, _ in counts}) < 2:
                raise RuntimeError(f"planted window {w.name} was not split across two files")
            out.append({
                "name": w.name,
                "tank_id": w.tank_id,
                "start": w.start.isoformat(timespec="milliseconds"),
                "end": w.end.isoformat(timespec="milliseconds"),
                "expected_count": sum(counts.values()),
                "by_metric": {m: sum(c for (_, mm), c in counts.items() if mm == m) for m in METRICS},
                "by_file": {files[i]: sum(c for (fi, _), c in counts.items() if fi == i)
                            for i in sorted({fi for fi, _ in counts})},
            })
        return out


def generate(out_dir, seed=42, tanks=10, days=14, interval_ms=400, n_files=6,
             start=DEFAULT_START, messy_rate=0.02, overwrite=False, progress=True):
    out_dir = Path(out_dir)
    existing = list(out_dir.glob("*.csv")) if out_dir.exists() else []
    if existing:
        if not overwrite:
            raise FileExistsError(f"{out_dir} already has {len(existing)} log files; pass --overwrite")
        for p in existing:
            p.unlink()
    gen = Generator(seed, tanks, days, interval_ms, n_files, start, messy_rate)
    stats, elapsed = gen.run(out_dir, progress)

    rejects = Counter()
    for s in stats:
        rejects.update(s["rejects"])
    manifest = {
        "seed": seed,
        "params": {"tanks": tanks, "days": days, "interval_ms": interval_ms, "files": n_files,
                   "start": start.isoformat(), "messy_rate": messy_rate},
        "memory_ceiling_mb": MEMORY_CEILING_MB,
        "files": stats,
        "totals": {
            "files": len(stats),
            "bytes": sum(s["bytes"] for s in stats),
            "lines": sum(s["lines"] for s in stats),
            "valid": sum(s["valid"] for s in stats),
            "rejects": dict(sorted(rejects.items())),
            "injected_by_kind": dict(sorted(gen.injected.items())),
        },
        "planted_windows": gen.planted_manifest([s["file"] for s in stats]),
        "generation_seconds": round(elapsed, 1),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def print_summary(m):
    t = m["totals"]
    ceiling = m["memory_ceiling_mb"] * 1024 * 1024
    malformed = sum(v for k, v in t["rejects"].items() if k != "out_of_range")
    oor = t["rejects"].get("out_of_range", 0)
    largest = max(m["files"], key=lambda s: s["bytes"])
    print(f"Files generated:   {t['files']}")
    for s in m["files"]:
        print(f"  {s['file']}  {s['bytes'] / 2**20:8.1f} MB  {s['lines']:>11,} lines")
    print(f"Total lines:       {t['lines']:,}")
    print(f"Valid readings:    {t['valid']:,}")
    print(f"Malformed lines:   {malformed:,}")
    print(f"Out-of-range:      {oor:,}")
    print(f"Messy fraction:    {(malformed + oor) / t['lines']:.2%}")
    print(f"Rejects by reason: {t['rejects']}")
    print(f"Total bytes:       {t['bytes']:,} ({t['bytes'] / 2**30:.2f} GiB)")
    print(f"Memory ceiling:    {m['memory_ceiling_mb']} MB -> data is {t['bytes'] / ceiling:.1f}x the ceiling; "
          f"largest file {largest['file']} is {largest['bytes'] / ceiling:.2f}x")
    for w in m["planted_windows"]:
        print(f"Planted window:    {w['name']}: tank {w['tank_id']} {w['start']} .. {w['end']} "
              f"-> {w['expected_count']} readings split across {w['by_file']}")
    print(f"Elapsed:           {m['generation_seconds']}s")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data/raw")
    p.add_argument("--tanks", type=int, default=10)
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--interval-ms", type=int, default=400, help="sampling interval per tank per metric")
    p.add_argument("--files", type=int, default=6)
    p.add_argument("--messy-rate", type=float, default=0.02)
    p.add_argument("--overwrite", action="store_true")
    a = p.parse_args(argv)
    try:
        m = generate(a.out, a.seed, a.tanks, a.days, a.interval_ms, a.files,
                     messy_rate=a.messy_rate, overwrite=a.overwrite)
    except (FileExistsError, ValueError) as e:
        sys.exit(f"error: {e}")
    print_summary(m)


if __name__ == "__main__":
    main()
