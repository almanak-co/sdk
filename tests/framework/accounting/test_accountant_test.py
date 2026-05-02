"""Unit tests for the Accountant Test fixture (Accounting-AttemptNo17 §3 D1).

Tests use synthetic in-memory SQLite databases so they're hermetic and
don't depend on a strategy run.
"""

from __future__ import annotations

import sqlite3
import tempfile
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.accounting.accountant_test import (
    _snapshot_equity,
    run_against_sqlite,
)


def test_snapshot_equity_sums_deployed_and_cash():
    s = {"total_value_usd": "10.0", "available_cash_usd": "5.0"}
    assert _snapshot_equity(s) == Decimal("15.0")


def test_snapshot_equity_post_teardown_pure_cash():
    # All positions closed; equity is now pure cash. NOT a null measurement.
    s = {"total_value_usd": "0", "available_cash_usd": "22.987"}
    assert _snapshot_equity(s) == Decimal("22.987")


def test_snapshot_equity_pre_oracle_pure_deployed():
    # Cash unmeasured but deployed populated.
    s = {"total_value_usd": "10.0", "available_cash_usd": None}
    assert _snapshot_equity(s) == Decimal("10.0")


def test_snapshot_equity_returns_none_only_when_both_unmeasured():
    s = {"total_value_usd": None, "available_cash_usd": None}
    assert _snapshot_equity(s) is None
    s = {"total_value_usd": "", "available_cash_usd": ""}
    assert _snapshot_equity(s) is None


def _make_db_with_minimal_lp_run() -> Path:
    """Build a synthetic LP-shaped DB that should pass G1 / G7 / G10 and
    keep Track-C-dependent cells (G14, G15) as XFAIL."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    path = Path(tmp.name)

    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(
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
            id TEXT PRIMARY KEY, cycle_id TEXT, deployment_id TEXT,
            execution_mode TEXT, position_id TEXT, position_type TEXT,
            event_type TEXT, timestamp TEXT, protocol TEXT, chain TEXT,
            token0 TEXT, token1 TEXT, amount0 TEXT, amount1 TEXT,
            value_usd TEXT, tick_lower INTEGER, tick_upper INTEGER,
            liquidity TEXT, in_range INTEGER, fees_token0 TEXT,
            fees_token1 TEXT, leverage TEXT, entry_price TEXT,
            mark_price TEXT, unrealized_pnl TEXT, is_long INTEGER,
            tx_hash TEXT, gas_usd TEXT, ledger_entry_id TEXT,
            protocol_fees_usd TEXT, attribution_json TEXT,
            attribution_version INTEGER
        );
        CREATE TABLE accounting_events (
            id TEXT PRIMARY KEY, cycle_id TEXT, deployment_id TEXT,
            strategy_id TEXT, execution_mode TEXT, timestamp TEXT,
            chain TEXT, protocol TEXT, wallet_address TEXT,
            event_type TEXT, position_key TEXT, ledger_entry_id TEXT,
            tx_hash TEXT, confidence TEXT, payload_json TEXT,
            schema_version INTEGER DEFAULT 1
        );
        CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY, strategy_id TEXT, deployment_id TEXT,
            cycle_id TEXT, execution_mode TEXT, timestamp TEXT,
            iteration_number INTEGER, total_value_usd TEXT,
            available_cash_usd TEXT, deployed_capital_usd TEXT,
            wallet_total_value_usd TEXT, value_confidence TEXT,
            positions_json TEXT, token_prices_json TEXT,
            wallet_balances_json TEXT, chain TEXT, created_at TEXT
        );
        CREATE TABLE portfolio_metrics (
            strategy_id TEXT PRIMARY KEY, initial_value_usd TEXT,
            initial_timestamp TEXT, deposits_usd TEXT, withdrawals_usd TEXT,
            gas_spent_usd TEXT, total_value_usd TEXT, positions_json TEXT,
            cycle_id TEXT, deployment_id TEXT, execution_mode TEXT,
            is_complete INTEGER, updated_at TEXT
        );
        """
    )
    # 1 LP_OPEN ledger row, with all four audit columns populated
    cur.execute(
        """
        INSERT INTO transaction_ledger (id, cycle_id, strategy_id, deployment_id,
            execution_mode, timestamp, intent_type, token_in, amount_in,
            token_out, amount_out, gas_used, gas_usd, tx_hash, chain, protocol,
            success, extracted_data_json, price_inputs_json, pre_state_json,
            post_state_json, schema_version, formula_version, matching_policy_version)
        VALUES ('led-1', 'cyc-1', 'lp-test', 'lp-test', 'live', '2026-05-01T00:00:00Z',
            'LP_OPEN', 'WETH', '0.001', 'USDC', '3.0', 500000, '0.05',
            '0xdeadbeef', 'arbitrum', 'uniswap_v3', 1,
            '{"lp_open_data": {"tick_lower": 100, "tick_upper": 200, "liquidity": "1234567"}}',
            '{"WETH": {"price_usd": "3000", "oracle_source": "chainlink", "fetched_at": "", "confidence": "HIGH"}, "USDC": {"price_usd": "1.0", "oracle_source": "chainlink", "fetched_at": "", "confidence": "HIGH"}}',
            '{}', '{}', 1, 1, 1)
        """
    )
    cur.execute(
        """
        INSERT INTO position_events (id, cycle_id, deployment_id, position_id,
            position_type, event_type, timestamp, protocol, chain, token0,
            token1, tick_lower, tick_upper, liquidity, fees_token0,
            fees_token1, tx_hash, ledger_entry_id)
        VALUES ('pe-1', 'cyc-1', 'lp-test', 'pos-1', 'LP', 'LP_OPEN',
            '2026-05-01T00:00:00Z', 'uniswap_v3', 'arbitrum', 'WETH', 'USDC',
            100, 200, '1234567', '0', '0', '0xdeadbeef', 'led-1')
        """
    )
    # accounting_event with HIGH confidence + valid payload
    cur.execute(
        """
        INSERT INTO accounting_events (id, cycle_id, deployment_id, strategy_id,
            timestamp, chain, protocol, event_type, position_key,
            ledger_entry_id, tx_hash, confidence, payload_json, schema_version)
        VALUES ('ae-1', 'cyc-1', 'lp-test', 'lp-test', '2026-05-01T00:00:00Z',
            'arbitrum', 'uniswap_v3', 'LP_OPEN', 'pos-1', 'led-1', '0xdeadbeef',
            'HIGH',
            '{"event_type": "LP_OPEN", "protocol": "uniswap_v3", "position_key": "pos-1", "pool_address": "weth-usdc-500", "token0": "WETH", "token1": "USDC", "amount0": "0.001", "amount1": "3.0", "amount0_usd": "3.0", "amount1_usd": "3.0", "cost_basis_usd": "6.0", "tick_lower": 100, "tick_upper": 200, "liquidity": 1234567, "confidence": "HIGH", "matching_policy_version": 1}',
            1)
        """
    )
    # 3 snapshots so G6 has endpoints + G8 time-series check is meaningful.
    for i, val in enumerate(["10.0", "10.5", "11.0"]):
        cur.execute(
            """
            INSERT INTO portfolio_snapshots (strategy_id, deployment_id,
                cycle_id, execution_mode, timestamp, iteration_number,
                total_value_usd, available_cash_usd, deployed_capital_usd,
                value_confidence, chain)
            VALUES ('lp-test', 'lp-test', 'cyc-1', 'live', ?, ?, ?, '0', ?, 'HIGH', 'arbitrum')
            """,
            (f"2026-05-01T00:0{i}:00Z", i, val, val),
        )
    cur.execute(
        """
        INSERT INTO portfolio_metrics (strategy_id, initial_value_usd,
            initial_timestamp, total_value_usd, cycle_id, deployment_id,
            execution_mode, is_complete)
        VALUES ('lp-test', '10.0', '2026-05-01T00:00:00Z', '11.0', 'cyc-1',
            'lp-test', 'live', 0)
        """
    )
    conn.commit()
    conn.close()
    return path


def test_accountant_test_lp_minimum_viable():
    """Track A acceptance — event-driven cells should pass with synthetic data."""
    db_path = _make_db_with_minimal_lp_run()
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        # Check the ones that should pass on this synthetic data
        cells_by_id = {c.cell_id: c for c in report.cells}
        # G1: tx_hash present
        assert cells_by_id["G1"].status == "PASS", cells_by_id["G1"].diagnostic
        # G2: gas_usd populated
        assert cells_by_id["G2"].status == "PASS", cells_by_id["G2"].diagnostic
        # G7: cycle_id everywhere
        assert cells_by_id["G7"].status == "PASS", cells_by_id["G7"].diagnostic
        # G9: confidence on all USD numbers
        assert cells_by_id["G9"].status == "PASS", cells_by_id["G9"].diagnostic
        # G10: 1:1 ledger:intents
        assert cells_by_id["G10"].status == "PASS"
        # G12: shaped price_inputs
        assert cells_by_id["G12"].status == "PASS", cells_by_id["G12"].diagnostic
        # G13: matching_policy_version present
        assert cells_by_id["G13"].status == "PASS", cells_by_id["G13"].diagnostic
        # LP1: ticks present on position_events
        assert cells_by_id["LP1"].status == "PASS", cells_by_id["LP1"].diagnostic
        # G14 / G15 / LP2 are Track-C-dependent and stay XFAIL until the
        # position_state_snapshots materializer is wired (VIB-3866 truth).
        assert cells_by_id["G14"].status == "XFAIL"
        assert cells_by_id["G15"].status == "XFAIL", cells_by_id["G15"].diagnostic
        assert cells_by_id["LP2"].status == "XFAIL"
    finally:
        db_path.unlink(missing_ok=True)


def test_accountant_test_lp_red_when_columns_empty():
    """Universal-red baseline: empty audit columns produce FAIL on G2/G12/G13."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    path = Path(tmp.name)
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE transaction_ledger (
                id TEXT, cycle_id TEXT, strategy_id TEXT, deployment_id TEXT,
                execution_mode TEXT, timestamp TEXT, intent_type TEXT,
                gas_used INTEGER, gas_usd TEXT, tx_hash TEXT, chain TEXT,
                protocol TEXT, success INTEGER, extracted_data_json TEXT,
                price_inputs_json TEXT, pre_state_json TEXT,
                post_state_json TEXT
            );
            CREATE TABLE position_events (id TEXT, cycle_id TEXT, deployment_id TEXT, event_type TEXT, position_id TEXT);
            CREATE TABLE accounting_events (id TEXT, cycle_id TEXT, deployment_id TEXT, event_type TEXT, payload_json TEXT, confidence TEXT);
            CREATE TABLE portfolio_snapshots (id INTEGER, total_value_usd TEXT, value_confidence TEXT, iteration_number INTEGER, timestamp TEXT);
            CREATE TABLE portfolio_metrics (strategy_id TEXT, initial_value_usd TEXT);
            """
        )
        cur.execute(
            "INSERT INTO transaction_ledger (id, cycle_id, intent_type, gas_used, gas_usd, tx_hash, success, price_inputs_json) "
            "VALUES ('l1', 'cyc', 'SWAP', 100000, '', '0xabc', 1, '')"
        )
        conn.commit()
        conn.close()
        report = run_against_sqlite(path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G2"].status == "FAIL"
        assert cells["G12"].status == "FAIL"
        assert cells["G13"].status == "FAIL"
    finally:
        path.unlink(missing_ok=True)


@pytest.mark.parametrize("primitive", ["lp", "looping", "perp"])
def test_accountant_test_runs_for_each_primitive(primitive):
    """Smoke test that every primitive produces a report with the right cell count."""
    db_path = _make_db_with_minimal_lp_run()
    try:
        report = run_against_sqlite(db_path, primitive=primitive)
        # 15 generic + 6 primitive = 21 cells per primitive
        assert report.total_cells == 21
        # Markdown serialization works
        md = report.format_markdown()
        assert "# Accountant Test —" in md
        assert "## Score" in md
        assert "## G6 decomposition" in md
    finally:
        db_path.unlink(missing_ok=True)


# ─── VIB-3865: G15 must be XFAIL pending Track C ────────────────────────


def _make_db_with_only_snapshots(snapshot_values: list[str]) -> Path:
    """Build a DB that has the bare minimum: portfolio_snapshots only.

    This is the case where the legacy G15 telescoping tautology would have
    falsely scored PASS (Σ deltas ≡ endpoint delta is identity). The fix
    must keep G15 as XFAIL because position_state_snapshots is not wired.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    path = Path(tmp.name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE transaction_ledger (id TEXT, gas_used INTEGER, gas_usd TEXT, intent_type TEXT, tx_hash TEXT, success INTEGER, price_inputs_json TEXT);
        CREATE TABLE position_events (id TEXT, event_type TEXT, position_id TEXT);
        CREATE TABLE accounting_events (id TEXT, event_type TEXT, payload_json TEXT, confidence TEXT);
        CREATE TABLE portfolio_snapshots (id INTEGER PRIMARY KEY, total_value_usd TEXT, available_cash_usd TEXT, deployed_capital_usd TEXT, value_confidence TEXT, iteration_number INTEGER, timestamp TEXT, chain TEXT);
        CREATE TABLE portfolio_metrics (strategy_id TEXT, initial_value_usd TEXT);
        """
    )
    for i, val in enumerate(snapshot_values):
        cur.execute(
            "INSERT INTO portfolio_snapshots (total_value_usd, available_cash_usd, deployed_capital_usd, value_confidence, iteration_number, timestamp, chain) "
            "VALUES (?, ?, ?, 'HIGH', ?, ?, 'arbitrum')",
            (val, val, val, i, f"2026-05-01T00:0{i}:00Z"),
        )
    conn.commit()
    conn.close()
    return path


def test_g15_xfail_when_position_state_snapshots_absent():
    """The pre-fix G15 was a telescoping identity: it always PASSed when all
    snapshot equities were measured. The fix must downgrade to XFAIL pending
    Track C — a falsely-passing reconciliation cell would re-introduce the
    Codex-flagged regression (VIB-3865)."""
    db_path = _make_db_with_only_snapshots(["10.0", "10.5", "11.0"])
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G15"].status == "XFAIL", cells["G15"].diagnostic
        assert "Track C" in cells["G15"].diagnostic or "Track-C" in cells["G15"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


def test_g15_xfail_when_position_state_snapshots_present_but_predicate_unimplemented():
    """Even with rows present, the cell stays XFAIL until the Track-C-aware
    predicate is implemented. Belt-and-braces — the test must not succeed
    on the existence of rows alone (that would re-create the tautology
    in a different form)."""
    db_path = _make_db_with_only_snapshots(["10.0", "10.5", "11.0"])
    try:
        # Add a position_state_snapshots table with a stub row so the
        # Track-C "rows present" branch fires.
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE position_state_snapshots (id TEXT, value_usd TEXT, iteration_number INTEGER)"
        )
        cur.execute(
            "INSERT INTO position_state_snapshots VALUES ('p1', '10.0', 0)"
        )
        conn.commit()
        conn.close()

        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G15"].status == "XFAIL"
        assert "predicate is not yet implemented" in cells["G15"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


# ─── VIB-3865: G6 PERP bucket — perp PnL must NOT pollute Σ_lp ─────────


def _make_db_with_perp_close(realized_pnl_usd: str) -> Path:
    """Synthetic perp DB: one PERP_OPEN, one PERP_CLOSE with realized_pnl_usd.
    Pre-fix bug: the realized_pnl accumulated into Σ_lp. Fix: it lands in Σ_perp.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    path = Path(tmp.name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE transaction_ledger (id TEXT, cycle_id TEXT, intent_type TEXT, gas_used INTEGER, gas_usd TEXT, tx_hash TEXT, chain TEXT, success INTEGER, price_inputs_json TEXT);
        CREATE TABLE position_events (id TEXT, cycle_id TEXT, event_type TEXT, position_id TEXT, tx_hash TEXT, ledger_entry_id TEXT, leverage TEXT, entry_price TEXT, mark_price TEXT, unrealized_pnl TEXT);
        CREATE TABLE accounting_events (id TEXT, cycle_id TEXT, event_type TEXT, payload_json TEXT, confidence TEXT, position_key TEXT, tx_hash TEXT, ledger_entry_id TEXT);
        CREATE TABLE portfolio_snapshots (id INTEGER PRIMARY KEY, total_value_usd TEXT, available_cash_usd TEXT, value_confidence TEXT, iteration_number INTEGER, timestamp TEXT, chain TEXT);
        CREATE TABLE portfolio_metrics (strategy_id TEXT, initial_value_usd TEXT);
        """
    )
    # Two snapshots so G6 has endpoints
    cur.execute(
        "INSERT INTO portfolio_snapshots (total_value_usd, available_cash_usd, value_confidence, iteration_number, timestamp, chain) "
        "VALUES ('100.0', '0', 'HIGH', 0, '2026-05-01T00:00:00Z', 'arbitrum')"
    )
    cur.execute(
        "INSERT INTO portfolio_snapshots (total_value_usd, available_cash_usd, value_confidence, iteration_number, timestamp, chain) "
        "VALUES ('0', ?, 'HIGH', 1, '2026-05-01T00:01:00Z', 'arbitrum')",
        (str(Decimal("100") + Decimal(realized_pnl_usd)),),
    )
    cur.execute(
        "INSERT INTO accounting_events (id, cycle_id, event_type, payload_json, confidence) "
        "VALUES ('ae-1', 'cyc', 'PERP_CLOSE', ?, 'HIGH')",
        (
            f'{{"event_type": "PERP_CLOSE", "realized_pnl_usd": "{realized_pnl_usd}", '
            f'"funding_paid_usd": "0", "funding_received_usd": "0"}}',
        ),
    )
    conn.commit()
    conn.close()
    return path


def test_g6_perp_close_realized_pnl_lands_in_perp_bucket_not_lp():
    db_path = _make_db_with_perp_close("5.0")
    try:
        report = run_against_sqlite(db_path, primitive="perp")
        decomp = report.g6_decomposition
        # The PnL must be attributed to perp, not lp.
        assert decomp["Σ_perp_usd"] == "5.0", decomp
        assert decomp["Σ_lp_usd"] == "0", decomp
        # Component PnL is the sum of all buckets minus gas (gas=0 in this fixture)
        assert Decimal(decomp["component_pnl_usd"]) == Decimal("5.0"), decomp
    finally:
        db_path.unlink(missing_ok=True)


def test_g6_perp_loss_lands_negative_in_perp_bucket():
    db_path = _make_db_with_perp_close("-3.5")
    try:
        report = run_against_sqlite(db_path, primitive="perp")
        decomp = report.g6_decomposition
        assert decomp["Σ_perp_usd"] == "-3.5", decomp
        assert decomp["Σ_lp_usd"] == "0", decomp
    finally:
        db_path.unlink(missing_ok=True)
