"""
Runs the quality pipeline and appends the result to a run-history log,
so quality metrics can be tracked across runs (regression detection).
"""
import json
import os
import datetime
from quality_pipeline import run

HISTORY_PATH = "output/run_history.jsonl"

if __name__ == "__main__":
    report = run()
    report["run_timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"
    with open(HISTORY_PATH, "a") as f:
        f.write(json.dumps(report) + "\n")
    print(f"Appended run to {HISTORY_PATH}")
