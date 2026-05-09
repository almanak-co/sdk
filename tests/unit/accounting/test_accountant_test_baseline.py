"""Run the Accountant Test against each per-primitive expected_baseline.sqlite + assert cells match expected_cells.json.

VIB-4162 (T2). The fixtures committed alongside this test are produced by
``tests/fixtures/accounting/_generate_post_t2_baselines.py``. Drift in the
post-T2 cell statuses must be intentional and accompanied by a fixture
regeneration.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from almanak.framework.accounting.accountant_test import run_against_sqlite

_FIXTURE_BASE = Path(__file__).resolve().parents[2] / "fixtures" / "accounting"


@pytest.mark.parametrize("primitive", ["lp", "looping", "perp"])
def test_accountant_baseline_against_expected_cells(primitive: str) -> None:
    db_path = _FIXTURE_BASE / primitive / "expected_baseline.sqlite"
    expected_path = _FIXTURE_BASE / primitive / "expected_cells.json"
    assert db_path.exists(), f"missing fixture SQLite: {db_path}"
    assert expected_path.exists(), f"missing expected_cells.json: {expected_path}"

    expected = json.loads(expected_path.read_text())

    # Row-count sanity (the test contract names these explicitly).
    conn = sqlite3.connect(str(db_path))
    try:
        ledger_count = conn.execute("SELECT COUNT(*) FROM transaction_ledger").fetchone()[0]
        ae_count = conn.execute("SELECT COUNT(*) FROM accounting_events").fetchone()[0]
        snap_count = conn.execute("SELECT COUNT(*) FROM portfolio_snapshots").fetchone()[0]
    finally:
        conn.close()
    assert ledger_count == expected["ledger_row_count"], (
        f"ledger row count drift: expected {expected['ledger_row_count']}, got {ledger_count}"
    )
    assert ae_count == expected["accounting_events_row_count"], (
        f"accounting_events row count drift: expected {expected['accounting_events_row_count']}, got {ae_count}"
    )
    assert snap_count > 0

    report = run_against_sqlite(db_path, primitive=primitive, strict_lifecycle=True)  # type: ignore[arg-type]
    actual = {c.cell_id: c.status for c in report.cells}
    assert actual == expected["cells"], (
        f"cell status drift for primitive={primitive}: "
        f"missing/changed cells = "
        f"{ {k: (expected['cells'].get(k), actual.get(k)) for k in set(expected['cells']) | set(actual) if expected['cells'].get(k) != actual.get(k)} }"
    )
