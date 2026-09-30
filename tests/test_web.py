import time

import pytest

import web.app as webapp
from generator.generate import generate


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(webapp, "RAW_DIR", tmp_path / "raw")
    monkeypatch.setattr(webapp, "STORE_DIR", tmp_path / "compacted")
    monkeypatch.setattr(webapp, "PROGRESS_FILE", tmp_path / "progress.json")
    monkeypatch.setattr(webapp, "LOG_FILE", tmp_path / "compaction.log")
    monkeypatch.setattr(webapp, "GEN_LOG", tmp_path / "generate.log")
    webapp._job.update(proc=None, started_at=None, finished_at=None, returncode=None)
    return webapp.app.test_client(), tmp_path


def test_full_flow_generate_compact_query(client):
    c, tmp = client
    assert c.get("/api/report").status_code == 404
    assert "run the compaction" in c.get("/api/query?tank=7&start=2026-09-22T02:00&end=2026-09-22T04:00").json["error"]
    assert c.post("/api/compact").status_code == 400  # nothing generated yet

    m = generate(tmp / "raw", seed=9, interval_ms=300_000, progress=False)
    assert c.get("/api/dataset").json["totals"]["lines"] == m["totals"]["lines"]
    assert c.get("/").status_code == 200

    assert c.post("/api/compact").status_code == 202
    assert c.post("/api/compact").status_code in (409, 202)  # 409 while the first is still running
    deadline = time.time() + 120
    while (s := c.get("/api/compact/status").json)["state"] == "running" and time.time() < deadline:
        time.sleep(0.2)
    assert s["state"] == "done", s

    rep = c.get("/api/report").json
    assert rep["reconciliation"]["ok"] and rep["totals"]["lines_read"] == m["totals"]["lines"]

    w = m["planted_windows"][0]
    r = c.get(f"/api/query?tank={w['tank_id']}&start={w['start']}&end={w['end']}").json
    assert r["count"] == len(r["readings"]) == w["expected_count"]
    assert r["by_source_file"] == w["by_file"]

    csv = c.get(f"/api/query?tank={w['tank_id']}&start={w['start']}&end={w['end']}&format=csv")
    assert csv.mimetype == "text/csv"
    assert len(csv.data.decode().strip().splitlines()) == w["expected_count"] + 1  # + header


@pytest.mark.parametrize("qs", ["tank=x&start=2026-09-22T02:00&end=2026-09-22T04:00",
                                "tank=7&start=&end=2026-09-22T04:00",
                                "tank=7&start=2026-09-22T05:00&end=2026-09-22T04:00"])
def test_bad_query_is_400_with_message(client, qs):
    c, tmp = client
    (tmp / "compacted").mkdir()
    (tmp / "compacted" / "_report.json").write_text('{"settings": {"scheme": "day"}, "output": {}}')
    r = c.get("/api/query?" + qs)
    assert r.status_code == 400 and r.json["error"]


def test_compaction_refused_while_generator_is_still_writing(client):
    c, tmp = client
    (tmp / "raw").mkdir()
    (tmp / "raw" / "sensors_01.csv").write_text("2026-09-22T02:30:00.000,7,temperature,12.00\n")  # partial
    (tmp / "generate.log").write_text("  day 3/14: 12,901,990 lines, 36s\n")  # no manifest yet
    r = c.post("/api/compact")
    assert r.status_code == 400 and "still writing" in r.json["error"]
    d = c.get("/api/dataset")
    assert d.status_code == 404 and d.json["generating"] and "day 3/14" in d.json["error"]
