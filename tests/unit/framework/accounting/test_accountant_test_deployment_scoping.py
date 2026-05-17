"""VIB-4540: accountant_test cells must score against ONE deployment, not the whole DB.

`_table_rows` previously executed ``SELECT * FROM <table>`` with no
``WHERE deployment_id`` clause. When a strategy folder DB accumulates
multiple deployments over time (the common case once a user re-runs a
strategy), cells like L3 / G14 / G15 read rows from older deployments
and produce contaminated scores.

These tests exercise the deployment-scoping contract that ``run_against_sqlite``
must enforce: auto-pick the singleton, fail loud on ambiguity, isolate
when an explicit ``deployment_id`` is supplied.
"""

from __future__ import annotations

import sqlite3
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.accounting.accountant_test import (
    MultipleDeploymentsError,
    _table_rows,
    run_against_sqlite,
)


def _make_minimal_schema(conn: sqlite3.Connection) -> None:
    """Minimal looping-shaped schema with the columns each cell reads."""
    conn.executescript(
        """
        CREATE TABLE transaction_ledger (
            id TEXT PRIMARY KEY, cycle_id TEXT, strategy_id TEXT,
            deployment_id TEXT, execution_mode TEXT, timestamp TEXT,
            intent_type TEXT, token_in TEXT, amount_in TEXT, token_out TEXT,
            amount_out TEXT, effective_price TEXT, slippage_bps INTEGER,
            gas_used INTEGER, gas_usd TEXT, tx_hash TEXT, chain TEXT,
            protocol TEXT, success INTEGER, error TEXT,
            extracted_data_json TEXT, price_inputs_json TEXT,
            pre_state_json TEXT, post_state_json TEXT,
            schema_version INTEGER DEFAULT 1, formula_version INTEGER DEFAULT 1,
            matching_policy_version INTEGER DEFAULT 1
        );
        CREATE TABLE position_events (
            id TEXT PRIMARY KEY, deployment_id TEXT, cycle_id TEXT,
            position_id TEXT, position_type TEXT, event_type TEXT,
            timestamp TEXT, protocol TEXT, chain TEXT, value_usd TEXT,
            tx_hash TEXT, ledger_entry_id TEXT
        );
        CREATE TABLE accounting_events (
            id TEXT PRIMARY KEY, deployment_id TEXT, strategy_id TEXT,
            cycle_id TEXT, timestamp TEXT, event_type TEXT, protocol TEXT,
            chain TEXT, asset TEXT, amount TEXT, amount_usd TEXT,
            payload_json TEXT, ledger_entry_id TEXT, schema_version INTEGER,
            formula_version INTEGER, matching_policy_version INTEGER,
            primitive_version INTEGER
        );
        CREATE TABLE portfolio_snapshots (
            id TEXT PRIMARY KEY, strategy_id TEXT, deployment_id TEXT,
            cycle_id TEXT, iteration_number INTEGER, timestamp TEXT,
            total_value_usd TEXT, available_cash_usd TEXT, execution_mode TEXT,
            positions_json TEXT, wallet_balances_json TEXT
        );
        CREATE TABLE portfolio_metrics (
            strategy_id TEXT PRIMARY KEY, total_value_usd TEXT,
            initial_value_usd TEXT, initial_timestamp TEXT, pnl_usd TEXT,
            pnl_pct TEXT, gas_spent_usd TEXT, last_updated TEXT,
            cycle_id TEXT, deployment_id TEXT
        );
        CREATE TABLE position_state_snapshots (
            id TEXT PRIMARY KEY, strategy_id TEXT, deployment_id TEXT,
            cycle_id TEXT, position_type TEXT, protocol TEXT, chain TEXT,
            timestamp TEXT, value_usd TEXT, health_factor TEXT,
            supply_balance TEXT, borrow_balance TEXT
        );
        """
    )


def _seed_deployment(
    conn: sqlite3.Connection,
    deployment_id: str,
    *,
    health_factor: str = "1.5",
    snapshot_count: int = 3,
) -> None:
    """Insert one deployment worth of rows. Different ``health_factor``
    values let tests distinguish "scored against deployment A" from
    "scored against deployment B" by inspecting cell diagnostics or
    by reading rows back via ``_table_rows``."""
    for i in range(snapshot_count):
        conn.execute(
            "INSERT INTO position_state_snapshots "
            "(id, strategy_id, deployment_id, cycle_id, position_type, protocol, chain, "
            " timestamp, value_usd, health_factor, supply_balance, borrow_balance) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"pss-{deployment_id}-{i}",
                f"strat-{deployment_id}",
                deployment_id,
                f"c-{deployment_id}-{i}",
                "LENDING",
                "morpho_blue",
                "ethereum",
                f"2026-05-17T00:00:0{i}Z",
                "100.0",
                health_factor,
                "0.05",
                "20.0",
            ),
        )
    # Minimal snapshot + metrics + ledger rows so the report can render.
    conn.execute(
        "INSERT INTO portfolio_snapshots "
        "(id, strategy_id, deployment_id, cycle_id, iteration_number, timestamp, "
        " total_value_usd, available_cash_usd, execution_mode, positions_json, "
        " wallet_balances_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            f"ps-{deployment_id}-0",
            f"strat-{deployment_id}",
            deployment_id,
            f"c-{deployment_id}-0",
            0,
            "2026-05-17T00:00:00Z",
            "100.0",
            "0",
            "live",
            "[]",
            "{}",
        ),
    )
    conn.execute(
        "INSERT INTO portfolio_metrics "
        "(strategy_id, total_value_usd, initial_value_usd, initial_timestamp, pnl_usd, "
        " pnl_pct, gas_spent_usd, last_updated, cycle_id, deployment_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            f"strat-{deployment_id}",
            "100.0",
            "100.0",
            "2026-05-17T00:00:00Z",
            "0",
            "0",
            "0",
            "2026-05-17T00:00:00Z",
            f"c-{deployment_id}-0",
            deployment_id,
        ),
    )
    conn.commit()


def _make_db(tmp_path: Path, deployments: list[dict[str, Any]]) -> Path:
    """Build a single-file SQLite DB seeded with one or more deployments."""
    db = tmp_path / "almanak_state.db"
    conn = sqlite3.connect(db)
    try:
        _make_minimal_schema(conn)
        for spec in deployments:
            _seed_deployment(conn, **spec)
    finally:
        conn.close()
    return db


class TestAccountantTestDeploymentScoping:
    """VIB-4540: cells must score against ONE deployment, not the whole DB."""

    def test_single_deployment_baseline(self, tmp_path: Path) -> None:
        """Against a single-deployment DB, ``run_against_sqlite`` works
        without ``deployment_id`` (auto-pick singleton preserves the
        matrix-runner contract)."""
        db = _make_db(tmp_path, [{"deployment_id": "A", "snapshot_count": 3}])
        report = run_against_sqlite(db, primitive="looping")
        assert report.strategy_id == "strat-A"

    def test_multi_deployment_db_no_filter_raises(self, tmp_path: Path) -> None:
        """If the DB has >1 deployment and no ``deployment_id`` is supplied,
        fail loud. Silent contamination was the original bug; auto-pick-first
        would just hide it. The error message must list the candidate
        deployments so the caller knows what to choose."""
        db = _make_db(
            tmp_path,
            [
                {"deployment_id": "clean", "health_factor": "1.5"},
                {"deployment_id": "broken", "health_factor": "0.5"},
            ],
        )
        with pytest.raises(MultipleDeploymentsError) as excinfo:
            run_against_sqlite(db, primitive="looping")
        msg = str(excinfo.value)
        assert "clean" in msg
        assert "broken" in msg
        # Programmatic access for callers that want to render their own UX.
        assert sorted(excinfo.value.deployment_ids) == ["broken", "clean"]

    def test_multi_deployment_db_with_filter_isolates(self, tmp_path: Path) -> None:
        """When supplied, ``deployment_id`` MUST scope cell data. The
        `position_state_snapshots` rows belonging to the OTHER deployment
        must not leak into this run's score."""
        db = _make_db(
            tmp_path,
            [
                {"deployment_id": "clean", "health_factor": "1.5"},
                {"deployment_id": "broken", "health_factor": "0.5"},
            ],
        )
        clean = run_against_sqlite(db, primitive="looping", deployment_id="clean")
        broken = run_against_sqlite(db, primitive="looping", deployment_id="broken")

        # strategy_id is taken from portfolio_metrics[0], which is now
        # deployment-scoped — proves the upstream rows were filtered.
        assert clean.strategy_id == "strat-clean"
        assert broken.strategy_id == "strat-broken"

    def test_table_rows_filter_applies_to_position_state_snapshots(
        self, tmp_path: Path
    ) -> None:
        """The specific table that bit us: ``position_state_snapshots`` was being
        read across deployments because the cell SQL had no WHERE. With the
        scoped helper, only the filtered deployment's rows are returned."""
        db = _make_db(
            tmp_path,
            [
                {"deployment_id": "A", "snapshot_count": 4, "health_factor": "1.5"},
                {"deployment_id": "B", "snapshot_count": 7, "health_factor": "0.5"},
            ],
        )
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        try:
            rows_a = _table_rows(conn, "position_state_snapshots", deployment_id="A")
            rows_b = _table_rows(conn, "position_state_snapshots", deployment_id="B")
        finally:
            conn.close()

        assert len(rows_a) == 4
        assert len(rows_b) == 7
        assert all(r["deployment_id"] == "A" for r in rows_a)
        assert all(r["deployment_id"] == "B" for r in rows_b)
        assert all(Decimal(str(r["health_factor"])) == Decimal("1.5") for r in rows_a)
        assert all(Decimal(str(r["health_factor"])) == Decimal("0.5") for r in rows_b)

    def test_table_rows_no_filter_preserves_back_compat(self, tmp_path: Path) -> None:
        """Existing callers that pass no ``deployment_id`` see the original
        unfiltered behaviour (preserves any external caller this PR didn't
        find)."""
        db = _make_db(
            tmp_path,
            [
                {"deployment_id": "A", "snapshot_count": 2},
                {"deployment_id": "B", "snapshot_count": 3},
            ],
        )
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        try:
            all_rows = _table_rows(conn, "position_state_snapshots")
        finally:
            conn.close()
        assert len(all_rows) == 5

    def test_matrix_runner_path_still_works(self, tmp_path: Path) -> None:
        """``scripts/qa/run_accounting_matrix.py`` runs against single-deployment
        fixture DBs; the new optional kwarg must be a no-op for them (auto-pick
        singleton). Without this regression guard, every matrix fixture would
        need a ``--deployment-id`` flag, breaking the harness."""
        db = _make_db(tmp_path, [{"deployment_id": "matrix_fixture", "snapshot_count": 2}])
        # No deployment_id passed — auto-pick the only candidate.
        report = run_against_sqlite(db, primitive="looping")
        assert report.strategy_id == "strat-matrix_fixture"
        assert report.cells  # report rendered without error

    def test_empty_db_proceeds_unfiltered(self, tmp_path: Path) -> None:
        """A DB with no deployments at all (very early or empty fixture)
        still runs — ``deployment_id`` resolution returns None and tables
        return their (empty) row sets."""
        db = tmp_path / "empty.db"
        conn = sqlite3.connect(db)
        try:
            _make_minimal_schema(conn)
            conn.commit()
        finally:
            conn.close()
        report = run_against_sqlite(db, primitive="looping")
        assert report.strategy_id == ""
