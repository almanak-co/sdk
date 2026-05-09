"""FixtureLifecycleError fires when a primitive fixture is missing canonical lifecycle steps.

VIB-4162 (T2). Asserts:

* The canonical LP fixture passes the lifecycle assertion silently.
* A fixture that exercised only LP_OPEN (missing LP_CLOSE) raises
  ``FixtureLifecycleError`` with a structured diagnostic naming the
  missing step.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from almanak.framework.accounting.accountant_test import (
    FixtureLifecycleError,
    run_against_sqlite,
)
from tests.fixtures.accounting._generate_baselines import generate_lp_fixture


def test_canonical_lp_fixture_passes_lifecycle(tmp_path: Path) -> None:
    db = tmp_path / "lp.sqlite"
    generate_lp_fixture(db)
    # Should not raise.
    report = run_against_sqlite(db, primitive="lp", strict_lifecycle=True)
    assert report.primitive == "lp"


def test_lp_fixture_missing_close_raises(tmp_path: Path) -> None:
    db = tmp_path / "lp_missing_close.sqlite"
    generate_lp_fixture(db)
    # Surgically delete every LP_CLOSE row from the ledger so the lifecycle
    # assertion sees only LP_OPEN.
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("DELETE FROM transaction_ledger WHERE intent_type = 'LP_CLOSE'")
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(FixtureLifecycleError) as excinfo:
        run_against_sqlite(db, primitive="lp", strict_lifecycle=True)
    assert "LP_CLOSE" in str(excinfo.value)
    assert "primitive=lp" in str(excinfo.value)


def test_strict_lifecycle_default_off(tmp_path: Path) -> None:
    """Production callers default strict_lifecycle=False so partial DBs do not crash."""
    db = tmp_path / "lp_open_only.sqlite"
    generate_lp_fixture(db)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("DELETE FROM transaction_ledger WHERE intent_type = 'LP_CLOSE'")
        conn.commit()
    finally:
        conn.close()
    # Default strict_lifecycle=False: should NOT raise.
    run_against_sqlite(db, primitive="lp")
