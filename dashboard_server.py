"""
Migration Dashboard Server
Serves the HTML dashboard + exposes a simple API that reads
migration_state.json and Airflow REST API.
"""

import json
import os
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder=".")
CORS(app)

STATE_FILE = Path(os.environ.get("STATE_FILE", "migration_state.json"))
AIRFLOW_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASS", "admin")


def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"overall_status": "no_data", "databases": {}}


def get_airflow_runs() -> list:
    """Fetch recent DAG runs from Airflow API."""
    try:
        import urllib.request
        import urllib.error
        import base64

        token = base64.b64encode(f"{AIRFLOW_USER}:{AIRFLOW_PASS}".encode()).decode()
        url = f"{AIRFLOW_URL}/api/v1/dags/neon_db_migration/dagRuns?limit=10&order_by=-execution_date"
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {token}"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            return data.get("dag_runs", [])
    except Exception as e:
        return [{"error": str(e)}]


@app.route("/api/state")
def api_state():
    return jsonify(load_state())


@app.route("/api/runs")
def api_runs():
    return jsonify(get_airflow_runs())


@app.route("/api/logs")
def api_logs():
    """Return last 100 lines from latest log file."""
    log_dir = Path(os.environ.get("LOG_DIR", "logs"))
    logs = sorted(log_dir.glob("migration_*.log"), reverse=True)
    if not logs:
        return jsonify({"lines": ["No log files found"]})
    try:
        with open(logs[0]) as f:
            lines = f.readlines()[-100:]
        return jsonify({"file": logs[0].name, "lines": lines})
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/health")
def api_health():
    return jsonify({"status": "ok", "timestamp": datetime.utcnow().isoformat()})


@app.route("/")
def index():
    return send_from_directory(".", "dashboard.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8081))
    print(f"🚀 Dashboard running at http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)