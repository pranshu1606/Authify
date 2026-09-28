import re

from generator.generate import generate
from schema import METRICS

STRICT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3},[1-9]\d*,(dissolved_oxygen|temperature),-?\d+\.\d+$")
SMALL = dict(seed=7, tanks=10, days=14, interval_ms=300_000, progress=False)


def test_schema_and_counts_reconcile(tmp_path):
    m = generate(tmp_path, **SMALL)
    t = m["totals"]
    assert t["files"] == 6
    assert t["lines"] == t["valid"] + sum(t["rejects"].values())
    lines = 0
    for f in sorted(tmp_path.glob("*.csv")):
        for line in f.read_text().splitlines():
            lines += 1
            if STRICT.match(line):
                ts, tank, metric, value = line.split(",")
                assert metric in METRICS and 1 <= int(tank) <= 10
    assert lines == t["lines"]
    assert 0.01 <= sum(t["rejects"].values()) / t["lines"] <= 0.03


def test_same_seed_same_bytes(tmp_path):
    generate(tmp_path / "a", **SMALL)
    generate(tmp_path / "b", **SMALL)
    for f in (tmp_path / "a").glob("*.csv"):
        assert f.read_bytes() == (tmp_path / "b" / f.name).read_bytes()


def test_planted_windows_split_across_two_files(tmp_path):
    m = generate(tmp_path, **SMALL)
    names = {w["name"] for w in m["planted_windows"]}
    assert names == {"tank7_tuesday_0200_0400", "tank3_across_midnight"}
    for w in m["planted_windows"]:
        assert len(w["by_file"]) == 2 and all(c > 0 for c in w["by_file"].values())
        assert sum(w["by_file"].values()) == w["expected_count"]


def test_data_is_out_of_order_within_files(tmp_path):
    generate(tmp_path, **SMALL)
    for f in tmp_path.glob("*.csv"):
        ts = [l[:23] for l in f.read_text().splitlines() if STRICT.match(l)]
        assert any(b < a for a, b in zip(ts, ts[1:])), f.name
