"""Per-primitive matching_policy_version isolation.

VIB-4162 (T2 D2.M2). The three-step bump scenario in the UAT card:

1. Baseline: G13 PASSes for LP / Lending / Perp at their declared versions.
2. LP-bump: bump LP to v99, regenerate ONLY the LP fixture; LP G13 PASSes
   at v99, Lending PASSes at v3, Perp PASSes at v1 — sibling fixtures
   bytes are unchanged.
3. Intra-primitive drift: inject a v2 SUPPLY row into the looping fixture;
   Lending G13 FAILs with the per-primitive diagnostic, LP / Perp still
   PASS independently.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from almanak.framework.accounting import accountant_test as at
from almanak.framework.accounting import payload_schemas
from almanak.framework.accounting.payload_schemas import MATCHING_POLICY_VERSIONS
from almanak.framework.primitives.types import Primitive
from tests.fixtures.accounting._generate_baselines import (
    generate_lp_fixture,
    generate_looping_fixture,
    generate_perp_fixture,
)


def test_baseline_versions() -> None:
    """MATCHING_POLICY_VERSIONS exposes the declared per-primitive defaults."""
    assert MATCHING_POLICY_VERSIONS[Primitive.LP] == 3
    assert MATCHING_POLICY_VERSIONS[Primitive.LENDING] == 3
    assert MATCHING_POLICY_VERSIONS[Primitive.PERP] == 1
    # Every Primitive has an entry — no KeyError on writer lookup.
    for member in Primitive:
        assert member in MATCHING_POLICY_VERSIONS


def test_lp_bump_isolation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Bumping LP to v99 must not contaminate Lending or Perp scoring."""
    monkeypatch.setitem(MATCHING_POLICY_VERSIONS, Primitive.LP, 99)

    lp_db = tmp_path / "lp.sqlite"
    generate_lp_fixture(lp_db)
    looping_db = tmp_path / "looping.sqlite"
    generate_looping_fixture(looping_db)
    perp_db = tmp_path / "perp.sqlite"
    generate_perp_fixture(perp_db)

    lp_report = at.run_against_sqlite(lp_db, primitive="lp", strict_lifecycle=True)
    looping_report = at.run_against_sqlite(looping_db, primitive="looping", strict_lifecycle=True)
    perp_report = at.run_against_sqlite(perp_db, primitive="perp", strict_lifecycle=True)

    g13 = {c.cell_id: c for r in (lp_report, looping_report, perp_report) for c in r.cells if c.cell_id == "G13"}
    # All three primitives' G13 cells must be PASS (no intra-primitive drift).
    assert lp_report.cells_blocked_by_payload_errors == []
    assert next(c for c in lp_report.cells if c.cell_id == "G13").status == "PASS"
    assert next(c for c in looping_report.cells if c.cell_id == "G13").status == "PASS"
    assert next(c for c in perp_report.cells if c.cell_id == "G13").status == "PASS"

    # SQL evidence: LP fixture stamps v99, lending v3, perp v1.
    def _versions(db: Path) -> set[int]:
        conn = sqlite3.connect(str(db))
        try:
            rows = conn.execute(
                "SELECT DISTINCT json_extract(payload_json, '$.matching_policy_version') FROM accounting_events"
            ).fetchall()
            return {int(r[0]) for r in rows if r[0] is not None}
        finally:
            conn.close()

    lp_versions = _versions(lp_db)
    # The LP DB also has SWAP events stamped at SWAP's primitive version (v3).
    assert 99 in lp_versions, f"LP fixture should contain v99 events; got {lp_versions}"
    assert _versions(looping_db) == {3}
    assert _versions(perp_db) == {1, 3}  # PERP v1 + the entry/exit SWAP v3


def test_g13_fail_on_intra_primitive_drift(tmp_path: Path) -> None:
    """A second SUPPLY event stamped at v2 in the same fixture must FAIL G13 with the per-primitive diagnostic."""
    db = tmp_path / "looping.sqlite"
    generate_looping_fixture(db)

    # Inject a SUPPLY event at version 2.
    conn = sqlite3.connect(str(db))
    try:
        rogue_payload = {
            "event_type": "SUPPLY",
            "protocol": "aave_v3",
            "asset": "USDC",
            "amount": "1.0",
            "amount_usd": "1.0",
            "confidence": "HIGH",
            "schema_version": payload_schemas.SCHEMA_VERSION,
            "formula_version": payload_schemas.FORMULA_VERSION,
            "matching_policy_version": 2,  # the drift
        }
        # VIB-4540: drift must stamp the SAME deployment_id as the fixture
        # rows, otherwise ``run_against_sqlite`` sees 2 deployments and
        # raises before G13 evaluates. Drift is intra-primitive within
        # one deployment, never cross-deployment.
        conn.execute(
            """
            INSERT INTO accounting_events
            (id, deployment_id, cycle_id, execution_mode, timestamp,
             chain, protocol, wallet_address, event_type, position_key,
             ledger_entry_id, tx_hash, confidence, payload_json, schema_version)
            VALUES ('drift-1', 'AccountantBaseline:fixture',
                    'c', 'paper', '2026-05-09T00:00:00+00:00',
                    'arbitrum', 'aave_v3', 'wallet', 'SUPPLY',
                    'lending:arbitrum:aave_v3:wallet:USDC',
                    'le-drift', '0xdrift', 'HIGH', ?, 1)
            """,
            (json.dumps(rogue_payload),),
        )
        conn.commit()
    finally:
        conn.close()

    report = at.run_against_sqlite(db, primitive="looping", strict_lifecycle=True)
    g13 = next(c for c in report.cells if c.cell_id == "G13")
    assert g13.status == "FAIL"
    assert "primitive=lending" in g13.diagnostic
    assert "[2, 3]" in g13.diagnostic, f"Expected [2, 3] in diagnostic; got: {g13.diagnostic}"
