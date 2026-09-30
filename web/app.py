"""Web layer: one page + a small JSON API. No business logic lives here.

    python -m web.app            # http://localhost:8000

Compaction runs as a child process (python -m compactor.compact), not a thread:
it is CPU-bound Python, so a thread would make the page sluggish through the
GIL, and a crash or memory blow-up in compaction must not take the page down.
Progress comes back through a small JSON file the child rewrites atomically.
"""

import csv
import io
import json
import os
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, Response, jsonify, render_template, request

from query.engine import QueryError, StoreMissing, load_report, query, readings
from schema import MEMORY_CEILING_MB

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
RAW_DIR = DATA_DIR / "raw"
STORE_DIR = DATA_DIR / "compacted"
PROGRESS_FILE = DATA_DIR / "compaction_progress.json"
LOG_FILE = DATA_DIR / "compaction.log"
GEN_LOG = DATA_DIR / "generate.log"  # written by docker/start.sh while it generates in the background

app = Flask(__name__)
_lock = threading.Lock()
_job = {"proc": None, "started_at": None, "finished_at": None, "returncode": None}


def _job_state():
    proc = _job["proc"]
    if proc is None:
        return "idle"
    rc = proc.poll()
    if rc is None:
        return "running"
    if _job["returncode"] is None:
        _job["returncode"] = rc
        _job["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return "done" if rc == 0 else "failed"


def _generating():
    return GEN_LOG.exists() and not (RAW_DIR / "manifest.json").exists()


@app.get("/")
def index():
    return render_template("index.html", ceiling=MEMORY_CEILING_MB)


@app.post("/api/compact")
def start_compaction():
    with _lock:
        if _job_state() == "running":
            return jsonify(error="a compaction is already running"), 409
        if not (RAW_DIR / "manifest.json").exists():  # written last, so its presence = generator finished
            msg = "the generator is still writing the logs; try again when it finishes" if _generating()                 else f"no generated logs in {RAW_DIR}; run the generator first"
            return jsonify(error=msg), 400
        PROGRESS_FILE.unlink(missing_ok=True)
        log = open(LOG_FILE, "w")
        _job.update(started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    finished_at=None, returncode=None,
                    proc=subprocess.Popen(
                        [sys.executable, "-m", "compactor.compact", "--raw", str(RAW_DIR),
                         "--out", str(STORE_DIR), "--progress-file", str(PROGRESS_FILE)],
                        stdout=log, stderr=subprocess.STDOUT))
        log.close()  # the child keeps its own handle
    return jsonify(state="running", started_at=_job["started_at"]), 202


@app.get("/api/compact/status")
def compaction_status():
    with _lock:
        state = _job_state()
    body = {"state": state, "started_at": _job["started_at"], "finished_at": _job["finished_at"],
            "returncode": _job["returncode"], "progress": None, "error": None}
    try:
        body["progress"] = json.loads(PROGRESS_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        pass  # not written yet, or (Windows) being replaced by the child right now; next poll gets it
    if state == "failed":
        lines = LOG_FILE.read_text(errors="replace").strip().splitlines() if LOG_FILE.exists() else []
        body["error"] = "\n".join(lines[-15:]) or f"compaction exited with code {_job['returncode']}"
    return jsonify(body)


@app.get("/api/report")
def report():
    try:
        return jsonify(load_report(STORE_DIR))
    except StoreMissing as e:
        return jsonify(error=str(e)), 404


@app.get("/api/dataset")
def dataset():
    path = RAW_DIR / "manifest.json"
    if not path.exists():
        if _generating():
            tail = GEN_LOG.read_text(errors="replace").strip().splitlines()[-1:] or ["starting"]
            return jsonify(generating=True, error=f"generating input logs... {tail[0].strip()}"), 404
        return jsonify(error=f"no generated dataset in {RAW_DIR}; run the generator first"), 404
    m = json.loads(path.read_text())
    return jsonify({k: m[k] for k in ("seed", "params", "memory_ceiling_mb", "files", "totals", "planted_windows")})


@app.get("/api/query")
def run_query():
    args = request.args
    try:
        r = query(STORE_DIR, args.get("tank", ""), args.get("start", ""), args.get("end", ""))
    except StoreMissing as e:
        return jsonify(error=str(e)), 409
    except QueryError as e:
        return jsonify(error=str(e)), 400
    table = r.pop("table")
    if args.get("format") == "csv":
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=["timestamp", "tank_id", "metric", "value", "source_file"])
        w.writeheader()
        for row in readings(table):
            w.writerow({"tank_id": r["tank_id"], **row})
        name = f"tank{r['tank_id']}_{r['start'][:16]}_{r['end'][:16]}.csv".replace(":", "")
        return Response(buf.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": f"attachment; filename={name}"})
    r["readings"] = readings(table)
    return jsonify(r)


def main():
    port = int(os.environ.get("PORT", "8000"))
    # Flask's built-in threaded server: single user on one machine, which is this brief's scope.
    app.run(host=os.environ.get("HOST", "127.0.0.1"), port=port, threaded=True)


if __name__ == "__main__":
    main()
