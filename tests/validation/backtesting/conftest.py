"""Trust Matrix scoreboard plumbing (VIB-5081).

Collects the outcome of every test tagged ``@pytest.mark.trust_cell`` and
prints the matrix scoreboard at the end of the run so CI logs always carry
the current matrix state. When ``TRUST_MATRIX_JSON`` is set, also writes the
JSON artifact to that path.

xdist-safe: the cell id travels on ``report.user_properties`` (serialized
from workers to the controller); the controller renders the scoreboard in
``pytest_terminal_summary`` from ``terminalreporter.stats``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """Stamp the trust-cell id onto the call-phase report (worker side)."""
    outcome = yield
    report = outcome.get_result()
    if report.when != "call":
        return
    marker = item.get_closest_marker("trust_cell")
    if marker and marker.args:
        report.user_properties.append(("trust_cell", str(marker.args[0])))


def pytest_terminal_summary(terminalreporter, exitstatus: int, config: pytest.Config) -> None:
    """Render the scoreboard from collected cell outcomes (controller side)."""
    from tests.validation.backtesting.trust_matrix import (
        render_scoreboard,
        scoreboard_json,
        status_from_category,
    )

    statuses: dict[str, str] = {}
    for category in ("passed", "failed", "error", "xfailed", "xpassed", "skipped"):
        for report in terminalreporter.stats.get(category, []):
            for name, value in getattr(report, "user_properties", []) or []:
                if name == "trust_cell":
                    statuses[str(value)] = status_from_category(category)

    if not statuses:
        return

    terminalreporter.section("Backtest Trust Matrix (VIB-5081)")
    terminalreporter.write_line(render_scoreboard(statuses))

    artifact_path = os.environ.get("TRUST_MATRIX_JSON")
    if artifact_path:
        path = Path(artifact_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(scoreboard_json(statuses), indent=2) + "\n")
        terminalreporter.write_line(f"Trust matrix JSON artifact written to {path}")
