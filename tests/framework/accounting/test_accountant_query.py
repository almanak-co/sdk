"""Unit tests for the filtered Accountant Test reporting API (VIB-3870)."""

from __future__ import annotations

import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from almanak.framework.accounting.reporting.accountant_query import (
    AccountingReportFilter,
    TaxPeriod,
    accountant_report_from_db,
)


# ─── TaxPeriod resolution ────────────────────────────────────────────────


def test_tax_period_resolves_full_fiscal_year():
    tp = TaxPeriod.from_label("FY2026")
    assert tp.since == datetime(2026, 1, 1, tzinfo=UTC)
    assert tp.until == datetime(2027, 1, 1, tzinfo=UTC)


def test_tax_period_resolves_q1():
    tp = TaxPeriod.from_label("Q1-2026")
    assert tp.since == datetime(2026, 1, 1, tzinfo=UTC)
    assert tp.until == datetime(2026, 4, 1, tzinfo=UTC)


def test_tax_period_resolves_q2():
    tp = TaxPeriod.from_label("Q2-2026")
    assert tp.since == datetime(2026, 4, 1, tzinfo=UTC)
    assert tp.until == datetime(2026, 7, 1, tzinfo=UTC)


def test_tax_period_resolves_q4_crosses_year_boundary():
    tp = TaxPeriod.from_label("Q4-2026")
    assert tp.since == datetime(2026, 10, 1, tzinfo=UTC)
    assert tp.until == datetime(2027, 1, 1, tzinfo=UTC)


def test_tax_period_unknown_label_raises():
    with pytest.raises(ValueError, match="Unrecognised"):
        TaxPeriod.from_label("not-a-period")


def test_filter_resolved_window_pulls_from_tax_period():
    filt = AccountingReportFilter(tax_period="FY2026")
    since, until = filt.resolved_window()
    assert since == datetime(2026, 1, 1, tzinfo=UTC)
    assert until == datetime(2027, 1, 1, tzinfo=UTC)


def test_filter_rejects_mixing_tax_period_with_explicit_window():
    filt = AccountingReportFilter(tax_period="FY2026", since=datetime(2026, 1, 1, tzinfo=UTC))
    with pytest.raises(ValueError, match="mutually exclusive"):
        filt.resolved_window()


# ─── Filtered queries ────────────────────────────────────────────────────


def _make_db_with_two_strategies_two_quarters() -> Path:
    """DB with two strategies × two quarters of accounting events.

    The cell matrix doesn't care about strategy/quarter as long as they
    flow through the cells correctly — this fixture just gives the
    reporting API enough rows to demonstrate filtering.
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
            deployment_id TEXT, timestamp TEXT, intent_type TEXT,
            token_in TEXT, amount_in TEXT, token_out TEXT, amount_out TEXT,
            gas_used INTEGER, gas_usd TEXT, tx_hash TEXT, chain TEXT,
            success INTEGER, price_inputs_json TEXT,
            schema_version INTEGER, formula_version INTEGER,
            matching_policy_version INTEGER
        );
        CREATE TABLE position_events (id TEXT, cycle_id TEXT, deployment_id TEXT, event_type TEXT, position_id TEXT, timestamp TEXT);
        CREATE TABLE accounting_events (
            id TEXT, cycle_id TEXT, deployment_id TEXT, strategy_id TEXT,
            timestamp TEXT, chain TEXT, protocol TEXT, event_type TEXT,
            position_key TEXT, ledger_entry_id TEXT, tx_hash TEXT,
            confidence TEXT, payload_json TEXT
        );
        CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY, strategy_id TEXT, deployment_id TEXT,
            cycle_id TEXT, total_value_usd TEXT, available_cash_usd TEXT,
            value_confidence TEXT, iteration_number INTEGER, timestamp TEXT, chain TEXT
        );
        CREATE TABLE portfolio_metrics (strategy_id TEXT, deployment_id TEXT, initial_value_usd TEXT);
        """
    )
    rows: list[tuple[str, str, str, str]] = [
        # (id, strategy_id, cycle_id, timestamp)
        ("led-A1", "stratA", "cyc-A1", "2026-02-01T00:00:00+00:00"),  # FY2026 Q1
        ("led-A2", "stratA", "cyc-A2", "2026-05-01T00:00:00+00:00"),  # FY2026 Q2
        ("led-B1", "stratB", "cyc-B1", "2026-02-01T00:00:00+00:00"),  # FY2026 Q1
        ("led-B2", "stratB", "cyc-B2", "2026-05-01T00:00:00+00:00"),  # FY2026 Q2
    ]
    for rid, sid, cid, ts in rows:
        cur.execute(
            "INSERT INTO transaction_ledger VALUES "
            "(?, ?, ?, ?, ?, 'SWAP', 'WETH', '0.001', 'USDC', '3.0', "
            "100000, '0', ?, 'arbitrum', 1, "
            "'{\"WETH\": {\"price_usd\": \"3000\", \"oracle_source\": \"chainlink\"}}', 1, 1, 1)",
            (rid, cid, sid, sid, ts, f"0x{rid}"),
        )
        cur.execute(
            "INSERT INTO accounting_events VALUES "
            "(?, ?, ?, ?, ?, 'arbitrum', 'uniswap_v3', 'SWAP', 'pos-1', ?, ?, 'HIGH', "
            "'{\"event_type\": \"SWAP\", \"protocol\": \"uniswap_v3\", "
            "\"token_in\": \"WETH\", \"token_out\": \"USDC\", "
            "\"amount_in\": \"0.001\", \"amount_out\": \"3.0\", "
            "\"amount_in_usd\": \"3.0\", \"amount_out_usd\": \"3.0\", "
            "\"realized_pnl_usd\": \"0\", \"confidence\": \"HIGH\", "
            "\"matching_policy_version\": 1}')",
            (f"ae-{rid}", cid, sid, sid, ts, rid, f"0x{rid}"),
        )
        cur.execute(
            "INSERT INTO portfolio_snapshots (strategy_id, deployment_id, cycle_id, "
            "total_value_usd, available_cash_usd, value_confidence, iteration_number, "
            "timestamp, chain) VALUES (?, ?, ?, '10', '0', 'HIGH', 0, ?, 'arbitrum')",
            (sid, sid, cid, ts),
        )
    # One trailing snapshot per strategy so G6 has ≥2 snapshot endpoints.
    cur.execute(
        "INSERT INTO portfolio_snapshots (strategy_id, deployment_id, cycle_id, "
        "total_value_usd, available_cash_usd, value_confidence, iteration_number, "
        "timestamp, chain) VALUES ('stratA', 'stratA', 'cyc-A2', '10', '0', 'HIGH', 1, "
        "'2026-06-01T00:00:00+00:00', 'arbitrum')"
    )
    cur.execute(
        "INSERT INTO portfolio_snapshots (strategy_id, deployment_id, cycle_id, "
        "total_value_usd, available_cash_usd, value_confidence, iteration_number, "
        "timestamp, chain) VALUES ('stratB', 'stratB', 'cyc-B2', '10', '0', 'HIGH', 1, "
        "'2026-06-01T00:00:00+00:00', 'arbitrum')"
    )
    cur.execute(
        "INSERT INTO portfolio_metrics VALUES ('stratA', 'stratA', '10')"
    )
    cur.execute(
        "INSERT INTO portfolio_metrics VALUES ('stratB', 'stratB', '10')"
    )
    conn.commit()
    conn.close()
    return path


def test_filtered_report_by_strategy_id_returns_only_that_strategys_rows():
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        report_a = accountant_report_from_db(
            db_path, primitive="lp", strategy_id="stratA"
        )
        report_b = accountant_report_from_db(
            db_path, primitive="lp", strategy_id="stratB"
        )
        # Both filters narrow the on-chain footprint to the strategy's
        # 2 ledger rows. The filter is doing its job.
        assert len(report_a.on_chain_footprint) == 2
        assert len(report_b.on_chain_footprint) == 2
        assert report_a.strategy_id == "stratA"
        assert report_b.strategy_id == "stratB"
        # No cross-strategy leakage in tx_hashes.
        a_hashes = {tx["tx_hash"] for tx in report_a.on_chain_footprint}
        b_hashes = {tx["tx_hash"] for tx in report_b.on_chain_footprint}
        assert not (a_hashes & b_hashes)
    finally:
        db_path.unlink(missing_ok=True)


def test_filtered_report_by_cycle_ids_only_includes_listed_cycles():
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        report = accountant_report_from_db(
            db_path, primitive="lp", cycle_ids=["cyc-A1", "cyc-B1"]
        )
        # Both Q1 cycles → 2 footprint rows total.
        assert len(report.on_chain_footprint) == 2
    finally:
        db_path.unlink(missing_ok=True)


def test_filtered_report_by_tax_period_q1_excludes_q2_rows():
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        report = accountant_report_from_db(
            db_path, primitive="lp", tax_period="Q1-2026"
        )
        # Q1 has 2 ledger rows (one per strategy), Q2 has 2.
        assert len(report.on_chain_footprint) == 2
    finally:
        db_path.unlink(missing_ok=True)


def test_filtered_report_combines_strategy_and_quarter():
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        report = accountant_report_from_db(
            db_path, primitive="lp", strategy_id="stratA", tax_period="Q2-2026"
        )
        # 1 row: stratA × Q2.
        assert len(report.on_chain_footprint) == 1
        assert report.on_chain_footprint[0]["tx_hash"] == "0xled-A2"
    finally:
        db_path.unlink(missing_ok=True)


def test_filtered_report_with_explicit_since_until():
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        report = accountant_report_from_db(
            db_path,
            primitive="lp",
            since=datetime(2026, 4, 1, tzinfo=UTC),
            until=datetime(2026, 6, 1, tzinfo=UTC),
        )
        # Same window as Q2-2026 minus the trailing June snapshot — 2 ledger rows.
        assert len(report.on_chain_footprint) == 2
    finally:
        db_path.unlink(missing_ok=True)


def test_filtered_report_accepts_existing_connection():
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        conn = sqlite3.connect(db_path)
        try:
            report = accountant_report_from_db(conn, primitive="lp", strategy_id="stratA")
            assert len(report.on_chain_footprint) == 2
        finally:
            conn.close()
    finally:
        db_path.unlink(missing_ok=True)


def test_filtered_report_no_matching_rows_evaluates_cleanly():
    """Filter that matches nothing shouldn't crash the cell evaluators —
    each cell handles the empty case explicitly per AttemptNo17 §1."""
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        report = accountant_report_from_db(
            db_path, primitive="lp", strategy_id="strat-does-not-exist"
        )
        assert len(report.on_chain_footprint) == 0
        # G1 FAILs because ledger empty — that's the expected behaviour
        # for an over-restrictive filter, not a regression.
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G1"].status == "FAIL"
    finally:
        db_path.unlink(missing_ok=True)
