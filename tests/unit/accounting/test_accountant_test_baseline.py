"""Run the Accountant Test against each per-primitive expected_baseline.sqlite + assert cells match expected_cells.json.

VIB-4162 (T2). The fixtures committed alongside this test are produced by
``tests/fixtures/accounting/_generate_post_t2_baselines.py``. Drift in the
post-T2 cell statuses must be intentional and accompanied by a fixture
regeneration.
"""

from __future__ import annotations

import json
import shutil
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


def test_synthetic_perp_without_endpoint_scope_cannot_certify_zero_gap(tmp_path: Path) -> None:
    db = tmp_path / "unscoped-perp.sqlite"
    shutil.copy2(_FIXTURE_BASE / "perp" / "expected_baseline.sqlite", db)
    with sqlite3.connect(db) as conn:
        for row_id, raw in conn.execute("SELECT id, positions_json FROM portfolio_snapshots").fetchall():
            envelope = json.loads(raw)
            envelope["metadata"].pop("wallet_scope")
            conn.execute("UPDATE portfolio_snapshots SET positions_json=? WHERE id=?", (json.dumps(envelope), row_id))
    report = run_against_sqlite(db, primitive="perp", strict_lifecycle=True)
    g6 = next(cell for cell in report.cells if cell.cell_id == "G6")
    assert g6.decomposition["gap_usd"] == "0.00"
    assert g6.decomposition["inventory_reval_confidence"] == "unmeasured_identity"
    # Cannot certify a zero gap: XFAIL is unmeasured, not PASS and not a books FAIL.
    assert g6.status == "XFAIL"
    assert not g6.is_pass()


@pytest.mark.parametrize("primitive", ["lp", "looping", "perp", "settlement"])
def test_synthetic_fixture_regenerates_with_identical_observed_scope(primitive: str, tmp_path: Path) -> None:
    from tests.fixtures.accounting import _generate_baselines

    generator = getattr(_generate_baselines, f"generate_{primitive}_fixture")
    first = tmp_path / "first.sqlite"
    second = tmp_path / "second.sqlite"
    generator(first)
    generator(second)
    assert first.read_bytes() == second.read_bytes()
    # SQLite file headers encode the writer's library version. Compare the
    # complete schema and persisted values across environments, not those bytes.
    with (
        sqlite3.connect(first) as generated,
        sqlite3.connect(_FIXTURE_BASE / primitive / "expected_baseline.sqlite") as committed,
    ):
        assert list(generated.iterdump()) == list(committed.iterdump())
