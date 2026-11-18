"""Inspect scheduler evidence independently of the QA process being monitored."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path


def check(runs, now, maximum_age_hours):
    scheduled = [r for r in runs if r["event"] == "schedule"]
    if not scheduled:
        return "missing", "No scheduled daily QA run exists."
    latest = max(scheduled, key=lambda r: r["created_at"])
    created = datetime.fromisoformat(latest["created_at"].replace("Z", "+00:00"))
    if (now - created).total_seconds() > maximum_age_hours * 3600:
        return "stale", f"Latest daily run is older than {maximum_age_hours} hours."
    return run_health(latest, now, created)


def run_health(latest, now, created):
    if latest["status"] != "completed":
        if (now - created).total_seconds() > 21600:
            return "stalled", "Daily run has not completed within six hours."
        return "running", "Daily run is in progress."
    if latest["conclusion"] != "success":
        return "failed", f"Latest daily run concluded {latest['conclusion']}."
    return "healthy", "A recent daily run completed successfully."


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=Path, required=True)
    parser.add_argument("--maximum-age-hours", type=int, default=30)
    args = parser.parse_args()
    status, message = check(
        json.loads(args.runs.read_text())["workflow_runs"],
        datetime.now(UTC),
        args.maximum_age_hours,
    )
    print(f"{status}: {message}")
    return int(status not in ("healthy", "running"))


if __name__ == "__main__":
    raise SystemExit(main())
