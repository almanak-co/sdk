"""VIB-6545 — snapshot ordering is chronological, shared, and fail-closed.

``iteration_number`` is process-local: a mid-run restart resets it to 1, and
restart-resume is the SDK-sanctioned way to apply a config change today
(VIB-6539). ``evaluate_cells``' former ``(iteration_number, timestamp)`` key
therefore filed the terminal teardown snapshot in the MIDDLE of the series on
any restarted run, and every cell reading ``snapshots[-1]`` measured a window
ending before the close.

The frozen mainnet bundle ``20260804-2310-gmxrt-vib6522-5513`` (committed as
``tests/fixtures/accounting/perp/vib6541_gmx_arb_mainnet.sqlite``) is the
measured proof and the primary negative control here: pre-fix G6 reported
``wallet_pnl = +$0.0149`` — a PROFIT — on a run that lost $0.52, because the
mis-ordered window ended before the PERP_CLOSE. The direction matters: the
defect HIDES losses.

Ordering authority: ``_snapshots_in_time_order`` — timestamp-primary with a
persistence-order (``id``) tie-break, ``None`` when any timestamp is
unmeasured/unparseable. Cells that elect endpoints (G4, G5, G6, PEN3) refuse
on ``None``; nothing falls back to row order.
"""

from __future__ import annotations

import random
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.accounting.accountant_test import (
    _cell_g4_capital_deployed,
    _cell_g6_reconciliation,
    _snapshots_in_time_order,
    run_against_sqlite,
)

_RUN1_BUNDLE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "accounting"
    / "perp"
    / "vib6541_gmx_arb_mainnet.sqlite"
)

# The chronologically correct wallet-method delta on the bundle, from the
# VIB-6545 ticket: equity(s4 @ 02:35:46, post-teardown) − equity(s1 @ 02:23:06).
_RUN1_TRUE_WALLET_PNL = Decimal("-0.25629428309556212158870000")
# What the broken sort reported: equity(s3 @ 02:34:20) − equity(s1) — a window
# ending BEFORE the 02:35:43 PERP_CLOSE, i.e. a profit on a losing run.
_RUN1_MISWINDOWED_WALLET_PNL = Decimal("0.0149")


@pytest.fixture(scope="module")
def run1_report():
    if not _RUN1_BUNDLE.is_file():  # pragma: no cover - fixture is committed
        pytest.skip(f"missing frozen bundle fixture: {_RUN1_BUNDLE}")
    return run_against_sqlite(_RUN1_BUNDLE, primitive="perp")


class TestTheRestartedBundleFlips:
    """NEGATIVE CONTROLS — measured on the frozen restarted mainnet DB."""

    def test_g6_wallet_pnl_is_the_chronological_delta(self, run1_report) -> None:
        """Pre-fix this read +$0.0149 (a profit) on a run that lost $0.52."""
        wallet = Decimal(run1_report.g6_decomposition["wallet_pnl_usd"])
        assert wallet == _RUN1_TRUE_WALLET_PNL

    def test_g6_no_longer_reports_the_miswindowed_profit(self, run1_report) -> None:
        """The exact pre-fix number, pinned so a regression is unambiguous."""
        wallet = Decimal(run1_report.g6_decomposition["wallet_pnl_usd"])
        assert abs(wallet - _RUN1_MISWINDOWED_WALLET_PNL) > Decimal("0.2")
        assert wallet < 0  # the run lost money; the window must see the close

    def test_g4_reads_the_torn_down_terminal_snapshot(self, run1_report) -> None:
        """Pre-fix G4 reported deployed=$3.0017 on a torn-down wallet. The
        terminal snapshot (02:35:46, post-teardown) has deployed=0 and all
        equity in cash."""
        g4 = next(c for c in run1_report.cells if c.cell_id == "G4")
        assert "deployed=$0" in g4.diagnostic
        assert "14.4660673221159025452913" in g4.diagnostic


class TestOrderingAuthority:
    def _restart_shaped_rows(self) -> list[dict]:
        """The bundle's exact shape, minimal: iteration resets mid-series."""
        return [
            {"id": 1, "iteration_number": 1, "timestamp": "2026-08-05T02:23:06+00:00"},
            {"id": 2, "iteration_number": 6, "timestamp": "2026-08-05T02:28:32+00:00"},
            {"id": 3, "iteration_number": 11, "timestamp": "2026-08-05T02:34:20+00:00"},
            {"id": 4, "iteration_number": 1, "timestamp": "2026-08-05T02:35:46+00:00"},
        ]

    def test_time_beats_iteration_number(self) -> None:
        ordered = _snapshots_in_time_order(self._restart_shaped_rows())
        assert ordered is not None
        assert [s["id"] for s in ordered] == [1, 2, 3, 4]

    def test_the_order_is_shuffle_invariant(self) -> None:
        """The order must be a function of the DATA, not of the caller's row
        order — ``_table_rows`` issues no ORDER BY, so input order is an
        accident of SQLite."""
        rows = self._restart_shaped_rows()
        rng = random.Random(6545)
        for _ in range(20):
            shuffled = rows[:]
            rng.shuffle(shuffled)
            ordered = _snapshots_in_time_order(shuffled)
            assert ordered is not None
            assert [s["id"] for s in ordered] == [1, 2, 3, 4]

    def test_equal_timestamps_break_ties_by_persistence_order(self) -> None:
        """Both snapshot writers stamp whole seconds, so same-second rows are
        real. ``id`` (AUTOINCREMENT) is persistence order — the only recorded
        approximation of event order inside one second."""
        rows = [
            {"id": 2, "iteration_number": 2, "timestamp": "2026-08-05T02:23:06+00:00"},
            {"id": 1, "iteration_number": 1, "timestamp": "2026-08-05T02:23:06+00:00"},
        ]
        ordered = _snapshots_in_time_order(rows)
        assert ordered is not None
        assert [s["id"] for s in ordered] == [1, 2]

    def test_an_unparseable_timestamp_refuses(self) -> None:
        rows = self._restart_shaped_rows()
        rows[2]["timestamp"] = "not-a-time"
        assert _snapshots_in_time_order(rows) is None

    def test_a_missing_timestamp_refuses(self) -> None:
        rows = self._restart_shaped_rows()
        rows[0]["timestamp"] = ""
        assert _snapshots_in_time_order(rows) is None


def _snap(sid: int, iteration: int, ts: str, total: str = "10", cash: str = "5") -> dict:
    return {
        "id": sid,
        "iteration_number": iteration,
        "timestamp": ts,
        "total_value_usd": total,
        "available_cash_usd": cash,
    }


class TestEndpointCellsRefuseUnorderableInput:
    """Fail-closed direction: an endpoint elected by row order is a wrong
    answer wearing a green. Both cells must FAIL loudly instead."""

    def test_g4_refuses(self) -> None:
        snaps = [
            _snap(1, 1, "2026-08-05T02:23:06+00:00"),
            _snap(2, 2, ""),  # unmeasured time
        ]
        cell = _cell_g4_capital_deployed(snaps)
        assert cell.status == "FAIL"
        assert "cannot order snapshots by time" in cell.diagnostic

    def test_g4_scores_orderable_input(self) -> None:
        """Liveness control for the guard above — same rows, measured times."""
        snaps = [
            _snap(1, 1, "2026-08-05T02:23:06+00:00"),
            _snap(2, 1, "2026-08-05T02:35:46+00:00", total="0", cash="7"),
        ]
        cell = _cell_g4_capital_deployed(snaps)
        assert cell.status == "PASS"
        assert "deployed=$0" in cell.diagnostic  # the chronological terminal row

    def test_g6_refuses(self) -> None:
        snaps = [
            _snap(1, 1, "2026-08-05T02:23:06+00:00"),
            _snap(2, 2, "garbage"),
        ]
        cell, decomp = _cell_g6_reconciliation(snaps, [], [], [], "perp", {}, {})
        assert cell.status == "FAIL"
        assert "cannot order snapshots by time" in cell.diagnostic
        assert decomp == {}


class TestTheSharedSortIsChronological:
    """End-to-end through ``run_against_sqlite`` on a synthetic restarted DB —
    proves ``evaluate_cells``' shared canonicalization, not just the helper."""

    def test_g4_terminal_endpoint_survives_a_restart(self, tmp_path: Path) -> None:
        db = tmp_path / "restart.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.executescript(
                """
                CREATE TABLE transaction_ledger (
                    id TEXT PRIMARY KEY, deployment_id TEXT, cycle_id TEXT, timestamp TEXT,
                    intent_type TEXT, chain TEXT, protocol TEXT, tx_hash TEXT, gas_usd TEXT,
                    success INTEGER, price_inputs_json TEXT);
                CREATE TABLE accounting_events (
                    id TEXT PRIMARY KEY, deployment_id TEXT, cycle_id TEXT, execution_mode TEXT,
                    timestamp TEXT, chain TEXT, protocol TEXT, wallet_address TEXT, event_type TEXT,
                    position_key TEXT, ledger_entry_id TEXT, tx_hash TEXT, confidence TEXT,
                    payload_json TEXT, schema_version INTEGER);
                CREATE TABLE position_events (
                    id TEXT PRIMARY KEY, deployment_id TEXT, cycle_id TEXT, timestamp TEXT,
                    position_id TEXT, position_type TEXT, event_type TEXT, chain TEXT,
                    protocol TEXT, value_usd TEXT, tx_hash TEXT, ledger_entry_id TEXT);
                CREATE TABLE portfolio_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, deployment_id TEXT, cycle_id TEXT,
                    execution_mode TEXT, timestamp TEXT, iteration_number INTEGER,
                    total_value_usd TEXT, available_cash_usd TEXT, deployed_capital_usd TEXT,
                    value_confidence TEXT, positions_json TEXT, token_prices_json TEXT,
                    wallet_balances_json TEXT, chain TEXT, created_at TEXT);
                CREATE TABLE portfolio_metrics (
                    deployment_id TEXT PRIMARY KEY, initial_value_usd TEXT, initial_timestamp TEXT,
                    deposits_usd TEXT, withdrawals_usd TEXT, gas_spent_usd TEXT,
                    total_value_usd TEXT, positions_json TEXT, cycle_id TEXT,
                    execution_mode TEXT, is_complete INTEGER, updated_at TEXT);
                CREATE TABLE position_state_snapshots (
                    id TEXT PRIMARY KEY, deployment_id TEXT, timestamp TEXT);
                CREATE TABLE position_registry (
                    physical_identity_hash TEXT PRIMARY KEY, deployment_id TEXT, primitive TEXT,
                    status TEXT, closed_tx TEXT);
                """
            )
            dep = "deployment:vib6545"
            rows = [
                # phase 1
                (1, "2026-08-05T02:23:06+00:00", "10", "5"),
                (6, "2026-08-05T02:28:32+00:00", "10", "5"),
                # phase 2 after a restart: iteration resets, chronologically last,
                # torn down (deployed collapsed to cash)
                (1, "2026-08-05T02:35:46+00:00", "0", "14"),
            ]
            for iteration, ts, total, cash in rows:
                conn.execute(
                    "INSERT INTO portfolio_snapshots "
                    "(deployment_id, cycle_id, execution_mode, timestamp, iteration_number,"
                    " total_value_usd, available_cash_usd, deployed_capital_usd,"
                    " value_confidence, positions_json, token_prices_json,"
                    " wallet_balances_json, chain, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (dep, "c1", "paper", ts, iteration, total, cash, "0", "HIGH", "[]", "{}", "[]", "arbitrum", ts),
                )
            conn.execute(
                "INSERT INTO portfolio_metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (dep, "15", "2026-08-05T02:23:06+00:00", "0", "0", "0", "14", "[]", "c1", "paper", 1, "x"),
            )
            conn.commit()
        finally:
            conn.close()

        report = run_against_sqlite(db, primitive="perp")
        g4 = next(c for c in report.cells if c.cell_id == "G4")
        # Pre-fix the (iteration_number, timestamp) sort filed the post-restart
        # teardown row SECOND and G4 read the 02:28:32 pre-close state.
        assert "deployed=$0" in g4.diagnostic
        assert "cash=$14" in g4.diagnostic
