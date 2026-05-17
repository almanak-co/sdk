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
        # 15 generic + 6 primitive + 1 cell #22 (VIB-4201/T15) = 22 cells per primitive.
        # Cell #22 (registry coherence) is informational; gating still measured on the
        # 21 non-L5_22 cells per the format_markdown contract.
        assert report.total_cells == 22
        cell_ids = {c.cell_id for c in report.cells}
        assert "L5_22" in cell_ids
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


def test_g15_passes_when_track_c_rows_present_and_no_snapshots_carry_positions():
    """VIB-3891: once Track C is wired, G15 evaluates a real coverage
    contract. When rows exist but the strategy never reported open
    positions on any portfolio_snapshot (cash-only / pre-deploy), the
    cell PASSes — penalising a position-less strategy would be wrong.

    Pre-VIB-3891 this case was an intentional XFAIL placeholder
    ("predicate not yet implemented"). The assertion below documents
    the new contract.
    """
    db_path = _make_db_with_only_snapshots(["10.0", "10.5", "11.0"])
    try:
        # Add the Track C table with a stub row.
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE position_state_snapshots (id TEXT, snapshot_id INTEGER, "
            "position_type TEXT, position_id TEXT, value_usd TEXT, iteration_number INTEGER)"
        )
        cur.execute(
            "INSERT INTO position_state_snapshots VALUES ('p1', 1, 'LP', 'pos-1', '10.0', 0)"
        )
        conn.commit()
        conn.close()

        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        # Cash-only strategy → snapshots have no positions_json → no
        # coverage to check → PASS by the "nothing to reconcile" branch.
        assert cells["G15"].status == "PASS", cells["G15"].diagnostic
        assert "cash-only" in cells["G15"].diagnostic or "pre-deploy" in cells["G15"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


def test_g15_fails_when_snapshots_have_positions_but_track_c_rows_missing():
    """VIB-3891: a snapshot reporting 2 open positions but only 1 Track C
    row tied to it is a partial-write coverage gap — G15 must FAIL."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    db_path = Path(tmp.name)
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE transaction_ledger (id TEXT, gas_used INTEGER, gas_usd TEXT, intent_type TEXT, tx_hash TEXT, success INTEGER, price_inputs_json TEXT);
            CREATE TABLE position_events (id TEXT, event_type TEXT, position_id TEXT);
            CREATE TABLE accounting_events (id TEXT, event_type TEXT, payload_json TEXT, confidence TEXT);
            CREATE TABLE portfolio_snapshots (
                id INTEGER PRIMARY KEY, total_value_usd TEXT, available_cash_usd TEXT,
                deployed_capital_usd TEXT, value_confidence TEXT, iteration_number INTEGER,
                timestamp TEXT, chain TEXT, positions_json TEXT
            );
            CREATE TABLE portfolio_metrics (strategy_id TEXT, initial_value_usd TEXT);
            CREATE TABLE position_state_snapshots (
                id INTEGER PRIMARY KEY, snapshot_id INTEGER, position_type TEXT,
                position_id TEXT, captured_at TEXT
            );
            """
        )
        # Snapshot id=1 reports 2 open positions; only 1 Track C row.
        cur.execute(
            "INSERT INTO portfolio_snapshots VALUES (1, '10', '0', '10', 'HIGH', 0, "
            "'2026-05-01T00:00:00Z', 'arbitrum', "
            "'[{\"position_type\":\"LP\",\"protocol\":\"uniswap_v3\",\"position_id\":\"pos-1\","
            "\"value_usd\":\"5\",\"label\":\"a\"},{\"position_type\":\"LP\","
            "\"protocol\":\"uniswap_v3\",\"position_id\":\"pos-2\",\"value_usd\":\"5\","
            "\"label\":\"b\"}]')"
        )
        cur.execute(
            "INSERT INTO portfolio_snapshots VALUES (2, '10', '0', '10', 'HIGH', 1, "
            "'2026-05-01T00:01:00Z', 'arbitrum', '[]')"
        )
        cur.execute(
            "INSERT INTO position_state_snapshots VALUES (1, 1, 'LP', 'pos-1', '2026-05-01T00:00:00Z')"
        )
        conn.commit()
        conn.close()

        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G15"].status == "FAIL", cells["G15"].diagnostic
        assert "under- or over-counted" in cells["G15"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


def test_g15_passes_when_track_c_covers_every_snapshot_with_positions():
    """VIB-3891: full coverage → PASS. Two positions per snapshot, two
    Track C rows per snapshot."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    db_path = Path(tmp.name)
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE transaction_ledger (id TEXT, gas_used INTEGER, gas_usd TEXT, intent_type TEXT, tx_hash TEXT, success INTEGER, price_inputs_json TEXT);
            CREATE TABLE position_events (id TEXT, event_type TEXT, position_id TEXT);
            CREATE TABLE accounting_events (id TEXT, event_type TEXT, payload_json TEXT, confidence TEXT);
            CREATE TABLE portfolio_snapshots (
                id INTEGER PRIMARY KEY, total_value_usd TEXT, available_cash_usd TEXT,
                deployed_capital_usd TEXT, value_confidence TEXT, iteration_number INTEGER,
                timestamp TEXT, chain TEXT, positions_json TEXT
            );
            CREATE TABLE portfolio_metrics (strategy_id TEXT, initial_value_usd TEXT);
            CREATE TABLE position_state_snapshots (
                id INTEGER PRIMARY KEY, snapshot_id INTEGER, position_type TEXT,
                position_id TEXT, captured_at TEXT, in_range INTEGER, liquidity TEXT
            );
            """
        )
        cur.execute(
            "INSERT INTO portfolio_snapshots VALUES (1, '10', '0', '10', 'HIGH', 0, "
            "'2026-05-01T00:00:00Z', 'arbitrum', "
            "'[{\"position_type\":\"LP\",\"protocol\":\"u\",\"position_id\":\"pos-1\","
            "\"value_usd\":\"5\",\"label\":\"a\"},{\"position_type\":\"LP\","
            "\"protocol\":\"u\",\"position_id\":\"pos-2\",\"value_usd\":\"5\",\"label\":\"b\"}]')"
        )
        cur.executemany(
            "INSERT INTO position_state_snapshots "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (1, 1, "LP", "pos-1", "2026-05-01T00:00:00Z", 1, "1234"),
                (2, 1, "LP", "pos-2", "2026-05-01T00:00:00Z", 0, "5678"),
            ],
        )
        conn.commit()
        conn.close()

        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G15"].status == "PASS", cells["G15"].diagnostic
        # LP2 also fires now: 1/2 in-range = 50%.
        assert cells["LP2"].status == "PASS", cells["LP2"].diagnostic
        assert "1/2" in cells["LP2"].diagnostic
        # LP6: both rows have non-zero liquidity.
        assert cells["LP6"].status == "PASS", cells["LP6"].diagnostic
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
    # PERP_CLOSE payload is validated against PerpCloseEventPayload (VIB-3868
    # — frozen schema). Provide the required fields so the test exercises
    # the bucket-routing path it claims to test, not the schema-mismatch
    # path.
    cur.execute(
        "INSERT INTO accounting_events (id, cycle_id, event_type, payload_json, confidence) "
        "VALUES ('ae-1', 'cyc', 'PERP_CLOSE', ?, 'HIGH')",
        (
            f'{{"event_type": "PERP_CLOSE", "protocol": "gmx_v2", '
            f'"position_key": "perp:gmx_v2:arbitrum:0x0:eth/usd", '
            f'"market": "ETH-USD", "is_long": true, "size": "0.5", '
            f'"realized_pnl_usd": "{realized_pnl_usd}", '
            f'"funding_paid_usd": "0", "funding_received_usd": "0", '
            f'"confidence": "HIGH"}}',
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


# ─── VIB-3868: strict Pydantic payload validation ───────────────────────


def _make_db_with_swap_payload(payload_json: str) -> Path:
    """Synthetic DB with a single SWAP ledger row + paired accounting_events
    row whose payload caller can shape — used to exercise the typed-read
    rail in cells that read ``accounting_events.payload_json``.
    """
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
            amount_out TEXT, gas_used INTEGER, gas_usd TEXT, tx_hash TEXT,
            chain TEXT, protocol TEXT, success INTEGER,
            extracted_data_json TEXT, price_inputs_json TEXT,
            pre_state_json TEXT, post_state_json TEXT,
            schema_version INTEGER, formula_version INTEGER,
            matching_policy_version INTEGER
        );
        CREATE TABLE position_events (id TEXT, cycle_id TEXT, event_type TEXT, position_id TEXT);
        CREATE TABLE accounting_events (
            id TEXT, cycle_id TEXT, deployment_id TEXT, strategy_id TEXT,
            timestamp TEXT, chain TEXT, protocol TEXT, event_type TEXT,
            position_key TEXT, ledger_entry_id TEXT, tx_hash TEXT,
            confidence TEXT, payload_json TEXT
        );
        CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY, total_value_usd TEXT,
            available_cash_usd TEXT, value_confidence TEXT,
            iteration_number INTEGER, timestamp TEXT, chain TEXT
        );
        CREATE TABLE portfolio_metrics (strategy_id TEXT, initial_value_usd TEXT);
        """
    )
    cur.execute(
        "INSERT INTO transaction_ledger (id, cycle_id, strategy_id, deployment_id, "
        "timestamp, intent_type, token_in, amount_in, token_out, amount_out, "
        "gas_used, gas_usd, tx_hash, chain, protocol, success, "
        "price_inputs_json, schema_version, formula_version, matching_policy_version) "
        "VALUES ('led-1', 'cyc-1', 's', 's', '2026-05-01T00:00:00Z', 'SWAP', 'WETH', "
        "'0.001', 'USDC', '3.0', 100000, '0.05', '0xabc', 'arbitrum', 'uniswap_v3', 1, "
        "'{\"WETH\": {\"price_usd\": \"3000\", \"oracle_source\": \"chainlink\"}}', 1, 1, 1)"
    )
    cur.execute(
        "INSERT INTO accounting_events (id, cycle_id, deployment_id, strategy_id, "
        "timestamp, chain, protocol, event_type, position_key, ledger_entry_id, "
        "tx_hash, confidence, payload_json) "
        "VALUES ('ae-1', 'cyc-1', 's', 's', '2026-05-01T00:00:00Z', 'arbitrum', "
        "'uniswap_v3', 'SWAP', 'pos-1', 'led-1', '0xabc', 'HIGH', ?)",
        (payload_json,),
    )
    cur.execute(
        "INSERT INTO portfolio_snapshots (total_value_usd, available_cash_usd, "
        "value_confidence, iteration_number, timestamp, chain) "
        "VALUES ('10', '0', 'HIGH', 0, '2026-05-01T00:00:00Z', 'arbitrum')"
    )
    cur.execute(
        "INSERT INTO portfolio_snapshots (total_value_usd, available_cash_usd, "
        "value_confidence, iteration_number, timestamp, chain) "
        "VALUES ('10.5', '0', 'HIGH', 1, '2026-05-01T00:01:00Z', 'arbitrum')"
    )
    conn.commit()
    conn.close()
    return path


_VALID_SWAP_PAYLOAD = (
    '{"event_type": "SWAP", "protocol": "uniswap_v3", '
    '"token_in": "WETH", "token_out": "USDC", '
    '"amount_in": "0.001", "amount_out": "3.0", '
    '"amount_in_usd": "3.0", "amount_out_usd": "3.0", '
    '"realized_pnl_usd": "0", "confidence": "HIGH", '
    '"matching_policy_version": 1}'
)


def test_payload_with_missing_required_field_blocks_dependent_cell():
    """VIB-3868 acceptance A: payload missing a Pydantic-required field →
    the cell using that field FAILs with the validation error embedded.

    Codex P1 audit follow-up (2026-05-02): writer-side fields that the
    Accountant Test projects from the row column (``protocol``) or from
    legacy field names (``amount_token`` → ``amount``) no longer trigger
    a validation error because the read-side projection fills them in
    before pydantic runs. To still exercise "missing required field
    surfaces in the report", drop a field that the projection does NOT
    touch — ``token_in`` is required by ``SwapEventPayload`` and has no
    projection source.
    """
    # SwapEventPayload requires `token_in` — drop it (not projected).
    bad_payload = (
        '{"event_type": "SWAP", "protocol": "uniswap_v3", "token_out": "USDC", '
        '"amount_in": "0.001", "amount_out": "3.0", "confidence": "HIGH"}'
    )
    db_path = _make_db_with_swap_payload(bad_payload)
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        # G6 reads SWAP payloads → must FAIL with the schema-mismatch
        # diagnostic (not silently pass on an empty Σ_swaps).
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G6"].status == "FAIL", cells["G6"].diagnostic
        assert "Pydantic" in cells["G6"].diagnostic or "validation" in cells["G6"].diagnostic
        # G1 also reads the SwapEventPayload to enforce USD pillar; with the
        # payload unusable the SWAP USD pillar is missing → G1 FAILs.
        assert cells["G1"].status == "FAIL", cells["G1"].diagnostic
        # The error is surfaced on the report for human review.
        assert len(report.payload_validation_errors) == 1
        rec = report.payload_validation_errors[0]
        assert rec["event_type"] == "SWAP"
        assert "token_in" in rec["error"]
        # Cells blocked tracking populated.
        assert "G6" in report.cells_blocked_by_payload_errors
    finally:
        db_path.unlink(missing_ok=True)


def test_g1_strict_usd_fails_on_swap_with_null_amount_in_usd():
    """VIB-3868 acceptance B: SWAP ledger row paired with payload missing
    ``amount_in_usd`` → G1 FAILs (USD pillar of money trail unmet)."""
    no_in_usd = (
        '{"event_type": "SWAP", "protocol": "uniswap_v3", '
        '"token_in": "WETH", "token_out": "USDC", '
        '"amount_in": "0.001", "amount_out": "3.0", '
        '"amount_out_usd": "3.0", "confidence": "HIGH"}'
    )
    db_path = _make_db_with_swap_payload(no_in_usd)
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G1"].status == "FAIL", cells["G1"].diagnostic
        assert "amount_in_usd" in cells["G1"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


def test_g1_strict_usd_fails_on_swap_with_null_amount_out_usd():
    """Symmetric to the amount_in_usd test."""
    no_out_usd = (
        '{"event_type": "SWAP", "protocol": "uniswap_v3", '
        '"token_in": "WETH", "token_out": "USDC", '
        '"amount_in": "0.001", "amount_out": "3.0", '
        '"amount_in_usd": "3.0", "confidence": "HIGH"}'
    )
    db_path = _make_db_with_swap_payload(no_out_usd)
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G1"].status == "FAIL", cells["G1"].diagnostic
        assert "amount_out_usd" in cells["G1"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


def test_g1_strict_usd_passes_when_both_usd_fields_present():
    """Belt-and-braces: when the SwapEventPayload has both USD valuations,
    G1 PASSes (the original happy path is preserved)."""
    db_path = _make_db_with_swap_payload(_VALID_SWAP_PAYLOAD)
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G1"].status == "PASS", cells["G1"].diagnostic
        assert "USD valuations" in cells["G1"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


def test_g1_strict_usd_fails_when_swap_has_no_paired_acct_event():
    """A SWAP ledger row with no matching accounting_events row is also an
    USD-pillar gap — the typed payload is the only place USD lives, and a
    missing payload row is functionally identical to a missing field."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    path = Path(tmp.name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE transaction_ledger (
            id TEXT PRIMARY KEY, cycle_id TEXT, strategy_id TEXT, timestamp TEXT,
            intent_type TEXT, token_in TEXT, amount_in TEXT, token_out TEXT,
            amount_out TEXT, gas_used INTEGER, gas_usd TEXT, tx_hash TEXT,
            chain TEXT, success INTEGER, price_inputs_json TEXT,
            schema_version INTEGER, formula_version INTEGER, matching_policy_version INTEGER
        );
        CREATE TABLE position_events (id TEXT, event_type TEXT, position_id TEXT);
        CREATE TABLE accounting_events (id TEXT, event_type TEXT, ledger_entry_id TEXT,
            payload_json TEXT, confidence TEXT);
        CREATE TABLE portfolio_snapshots (id INTEGER, total_value_usd TEXT, value_confidence TEXT, iteration_number INTEGER, timestamp TEXT);
        CREATE TABLE portfolio_metrics (strategy_id TEXT);
        """
    )
    cur.execute(
        "INSERT INTO transaction_ledger VALUES "
        "('l1', 'cyc', 's', '2026-05-01T00:00Z', 'SWAP', 'A', '1', 'B', '2', "
        "100000, '0.05', '0x1', 'arbitrum', 1, '{\"A\": {\"price_usd\": \"1\", \"oracle_source\": \"x\"}}', 1, 1, 1)"
    )
    conn.commit()
    conn.close()
    try:
        report = run_against_sqlite(path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G1"].status == "FAIL"
        assert "no SwapEventPayload" in cells["G1"].diagnostic
    finally:
        path.unlink(missing_ok=True)


# ─── VIB-3868 (C): G10 cycle-level atomicity ─────────────────────────────


def _make_db_with_cycle_rows(rows: list[tuple[str, str, int]]) -> Path:
    """Build a DB with the given ledger rows: each tuple is
    ``(intent_type, tx_hash, success)``. All rows share cycle_id='cyc-1'.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    path = Path(tmp.name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE transaction_ledger (
            id TEXT PRIMARY KEY, cycle_id TEXT, strategy_id TEXT, timestamp TEXT,
            intent_type TEXT, gas_used INTEGER, gas_usd TEXT, tx_hash TEXT,
            chain TEXT, success INTEGER, price_inputs_json TEXT,
            schema_version INTEGER, formula_version INTEGER, matching_policy_version INTEGER
        );
        CREATE TABLE position_events (id TEXT, event_type TEXT, position_id TEXT);
        CREATE TABLE accounting_events (id TEXT, event_type TEXT, payload_json TEXT, confidence TEXT);
        CREATE TABLE portfolio_snapshots (id INTEGER, total_value_usd TEXT, value_confidence TEXT, iteration_number INTEGER, timestamp TEXT);
        CREATE TABLE portfolio_metrics (strategy_id TEXT);
        """
    )
    for i, (it, txh, succ) in enumerate(rows):
        cur.execute(
            "INSERT INTO transaction_ledger VALUES "
            "(?, 'cyc-1', 's', ?, ?, 100000, '0.05', ?, 'arbitrum', ?, "
            "'{\"A\": {\"price_usd\": \"1\", \"oracle_source\": \"x\"}}', 1, 1, 1)",
            (f"l-{i}", f"2026-05-01T00:0{i}:00Z", it, txh, succ),
        )
    conn.commit()
    conn.close()
    return path


def test_g10_passes_on_three_uniform_success_rows_in_one_cycle():
    """VIB-3868 acceptance: 3 rows in same cycle_id, all SUCCESS → G10 PASS."""
    rows = [("APPROVE", "0x1", 1), ("SUPPLY", "0x2", 1), ("BORROW", "0x3", 1)]
    db_path = _make_db_with_cycle_rows(rows)
    try:
        report = run_against_sqlite(db_path, primitive="looping")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G10"].status == "PASS", cells["G10"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


def test_g10_fails_on_mixed_status_rows_in_one_cycle():
    """VIB-3868 acceptance: 3 rows in same cycle_id, 2 SUCCESS + 1 FAIL →
    G10 FAILs (cycle-level atomicity violation)."""
    rows = [("APPROVE", "0x1", 1), ("SUPPLY", "0x2", 1), ("BORROW", "0x3", 0)]
    db_path = _make_db_with_cycle_rows(rows)
    try:
        report = run_against_sqlite(db_path, primitive="looping")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G10"].status == "FAIL", cells["G10"].diagnostic
        assert "mixed-status" in cells["G10"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


def test_g10_passes_on_uniform_failure_rows_in_one_cycle():
    """A cycle that uniformly reverted is acceptable for G10 — the contract
    is "all succeed OR all revert", not "must succeed"."""
    rows = [("APPROVE", "0x1", 0), ("SUPPLY", "0x2", 0)]
    db_path = _make_db_with_cycle_rows(rows)
    try:
        report = run_against_sqlite(db_path, primitive="looping")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G10"].status == "PASS", cells["G10"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


def test_g10_still_catches_dup_writes_for_same_intent():
    """Backward compat with VIB-3865: dup detection still fires."""
    rows = [("SWAP", "0x1", 1), ("SWAP", "0x1", 1)]
    db_path = _make_db_with_cycle_rows(rows)
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G10"].status == "FAIL"
        assert "duplicated" in cells["G10"].diagnostic
    finally:
        db_path.unlink(missing_ok=True)


# ─── VIB-3869: G6 null-count audit + leveraged-notional tolerance ───────


def _swap_payload(*, amount_in_usd: str | None, realized_pnl_usd: str | None) -> str:
    """Build a Pydantic-compliant SwapEventPayload with optionally-null
    economic fields for G6 input testing."""
    in_usd = "null" if amount_in_usd is None else f'"{amount_in_usd}"'
    rpnl = "null" if realized_pnl_usd is None else f'"{realized_pnl_usd}"'
    return (
        '{"event_type": "SWAP", "protocol": "uniswap_v3", '
        '"token_in": "WETH", "token_out": "USDC", '
        '"amount_in": "0.001", "amount_out": "3.0", '
        f'"amount_in_usd": {in_usd}, "amount_out_usd": "3.0", '
        f'"realized_pnl_usd": {rpnl}, "confidence": "HIGH", '
        '"matching_policy_version": 1}'
    )


def _make_db_with_n_swaps(
    payloads: list[str],
    initial_equity: str = "100",
    final_equity: str = "100",
) -> Path:
    """N SWAP rows in one cycle, two snapshots framing the run."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    path = Path(tmp.name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    # VIB-4540: tables that ``run_against_sqlite`` scans for deployment
    # ids (``transaction_ledger`` / ``accounting_events`` / ``portfolio_snapshots``)
    # must carry the ``deployment_id`` column so the scoped reads find rows.
    # Pre-VIB-4540 this fixture was unfiltered and the column could be omitted.
    cur.executescript(
        """
        CREATE TABLE transaction_ledger (
            id TEXT PRIMARY KEY, cycle_id TEXT, strategy_id TEXT,
            deployment_id TEXT, timestamp TEXT,
            intent_type TEXT, token_in TEXT, amount_in TEXT, token_out TEXT,
            amount_out TEXT, gas_used INTEGER, gas_usd TEXT, tx_hash TEXT,
            chain TEXT, success INTEGER, price_inputs_json TEXT,
            schema_version INTEGER, formula_version INTEGER, matching_policy_version INTEGER
        );
        CREATE TABLE position_events (id TEXT, cycle_id TEXT, deployment_id TEXT, event_type TEXT, position_id TEXT);
        CREATE TABLE accounting_events (
            id TEXT, cycle_id TEXT, deployment_id TEXT, strategy_id TEXT,
            timestamp TEXT, chain TEXT, protocol TEXT, event_type TEXT,
            position_key TEXT, ledger_entry_id TEXT, tx_hash TEXT,
            confidence TEXT, payload_json TEXT
        );
        CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY, deployment_id TEXT, total_value_usd TEXT,
            available_cash_usd TEXT, value_confidence TEXT,
            iteration_number INTEGER, timestamp TEXT, chain TEXT
        );
        CREATE TABLE portfolio_metrics (strategy_id TEXT, deployment_id TEXT, initial_value_usd TEXT);
        """
    )
    for i, pj in enumerate(payloads):
        cur.execute(
            "INSERT INTO transaction_ledger VALUES "
            "(?, 'cyc-1', 's', 's', ?, 'SWAP', 'WETH', '0.001', 'USDC', '3.0', "
            "100000, '0', ?, 'arbitrum', 1, "
            "'{\"WETH\": {\"price_usd\": \"3000\", \"oracle_source\": \"chainlink\"}}', 1, 1, 1)",
            (f"led-{i}", f"2026-05-01T00:0{i}:00Z", f"0x{i:x}"),
        )
        cur.execute(
            "INSERT INTO accounting_events VALUES "
            "(?, 'cyc-1', 's', 's', ?, 'arbitrum', 'uniswap_v3', 'SWAP', 'pos-1', "
            "?, ?, 'HIGH', ?)",
            (f"ae-{i}", f"2026-05-01T00:0{i}:00Z", f"led-{i}", f"0x{i:x}", pj),
        )
    cur.execute(
        "INSERT INTO portfolio_snapshots (deployment_id, total_value_usd, available_cash_usd, "
        "value_confidence, iteration_number, timestamp, chain) "
        "VALUES ('s', ?, '0', 'HIGH', 0, '2026-05-01T00:00:00Z', 'arbitrum')",
        (initial_equity,),
    )
    cur.execute(
        "INSERT INTO portfolio_snapshots (deployment_id, total_value_usd, available_cash_usd, "
        "value_confidence, iteration_number, timestamp, chain) "
        "VALUES ('s', ?, '0', 'HIGH', 1, '2026-05-01T00:10:00Z', 'arbitrum')",
        (final_equity,),
    )
    conn.commit()
    conn.close()
    return path


def test_g6_fails_with_null_count_when_all_swap_rpnl_are_null():
    """VIB-3869 acceptance: 4 SWAP rows with ``realized_pnl_usd=null`` on
    every payload → G6 FAILs and the diagnostic surfaces the null count.
    Pre-fix this would silently sum to zero and mis-reconcile."""
    payloads = [_swap_payload(amount_in_usd="3", realized_pnl_usd=None) for _ in range(4)]
    db_path = _make_db_with_n_swaps(payloads)
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G6"].status == "FAIL", cells["G6"].diagnostic
        assert "Σ_swaps_usd_null_count" in cells["G6"].diagnostic
        # Decomposition surfaces the count for triage.
        decomp = report.g6_decomposition
        assert decomp["Σ_swaps_usd_null_count"] == "4"
    finally:
        db_path.unlink(missing_ok=True)


def test_g6_fails_when_capital_is_small_and_gap_exceeds_floor():
    """VIB-3869 acceptance: $5 starting capital, $0.40 wallet/component gap
    → previously PASSed under the $0.50 floor, now FAILs above the new
    $0.10 floor with $10 notional_traded → ε = max(0.25% × $10, $0.10) = $0.10.

    Setup: one SWAP with `amount_in_usd=10` (the notional source) and
    `realized_pnl_usd=0`. Snapshot delta encodes the wallet PnL gap.
    """
    payloads = [_swap_payload(amount_in_usd="10", realized_pnl_usd="0")]
    # initial=$5, final=$5.40 → wallet_pnl=$0.40, component=$0 → gap=$0.40
    db_path = _make_db_with_n_swaps(payloads, initial_equity="5", final_equity="5.40")
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G6"].status == "FAIL", cells["G6"].diagnostic
        decomp = report.g6_decomposition
        assert decomp["ε_floor_usd"] == "$0.10"
        assert Decimal(decomp["ε_threshold_usd"]) == Decimal("0.10")
        assert Decimal(decomp["gap_usd"]) == Decimal("0.40")
    finally:
        db_path.unlink(missing_ok=True)


def test_g6_passes_when_capital_is_small_and_gap_under_floor():
    """VIB-3869 acceptance: same $5 capital, $0.04 gap → ε = $0.10 floor →
    PASS. Belt and braces: small capital is no longer a free pass for
    larger drift (that's the prior bug), but legitimately tight runs still
    reconcile."""
    payloads = [_swap_payload(amount_in_usd="10", realized_pnl_usd="0")]
    db_path = _make_db_with_n_swaps(payloads, initial_equity="5", final_equity="5.04")
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G6"].status == "PASS", cells["G6"].diagnostic
        decomp = report.g6_decomposition
        assert Decimal(decomp["gap_usd"]) == Decimal("0.04")
    finally:
        db_path.unlink(missing_ok=True)


def test_g6_tolerance_scales_with_notional_for_lp():
    """The ε threshold must be at least 0.25% of notional traded for LP —
    i.e. on a $10,000 notional run the threshold should be $25, not $0.10.
    """
    # Two synthetic SWAPs each contributing $5,000 notional → $10,000 total.
    payloads = [
        _swap_payload(amount_in_usd="5000", realized_pnl_usd="0"),
        _swap_payload(amount_in_usd="5000", realized_pnl_usd="0"),
    ]
    db_path = _make_db_with_n_swaps(payloads, initial_equity="100", final_equity="100")
    try:
        report = run_against_sqlite(db_path, primitive="lp")
        decomp = report.g6_decomposition
        # 0.25% × $10,000 = $25 — well above the $0.10 floor.
        assert Decimal(decomp["ε_threshold_usd"]) == Decimal("25.0000")
        assert Decimal(decomp["ε_scaling_base_usd"]) == Decimal("10000")
    finally:
        db_path.unlink(missing_ok=True)


def test_g6_perp_tolerance_uses_max_perp_notional():
    """Perp scaling uses max(size × price), not notional_traded."""
    db_path = _make_db_with_perp_close("5.0")
    try:
        report = run_against_sqlite(db_path, primitive="perp")
        decomp = report.g6_decomposition
        # The perp test fixture sets `size=0.5` and the payload has no
        # `exit_price` (so the exit-side notional is 0). max_perp_notional
        # = 0 → ε = floor $0.10.
        assert decomp["ε_scaling_base_label"] == "max_perp_notional"
        # The eps is $0.10 floor since no priced size in fixture.
        assert Decimal(decomp["ε_threshold_usd"]) == Decimal("0.10")
    finally:
        db_path.unlink(missing_ok=True)
