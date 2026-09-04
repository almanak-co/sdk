"""Unit tests for the filtered Accountant Test reporting API (VIB-3870)."""

from __future__ import annotations

import sqlite3
import tempfile
from datetime import UTC, datetime
from itertools import product
from pathlib import Path

import pytest

from almanak.framework.accounting.reporting.accountant_query import (
    AccountingReportFilter,
    TaxPeriod,
    _filtered_rows,
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
            id TEXT PRIMARY KEY, cycle_id TEXT, deployment_id TEXT, timestamp TEXT, intent_type TEXT,
            token_in TEXT, amount_in TEXT, token_out TEXT, amount_out TEXT,
            gas_used INTEGER, gas_usd TEXT, tx_hash TEXT, chain TEXT,
            success INTEGER, price_inputs_json TEXT,
            schema_version INTEGER, formula_version INTEGER,
            matching_policy_version INTEGER
        );
        CREATE TABLE position_events (id TEXT, cycle_id TEXT, deployment_id TEXT, event_type TEXT, position_id TEXT, timestamp TEXT);
        CREATE TABLE accounting_events (
            id TEXT, cycle_id TEXT, deployment_id TEXT,
            timestamp TEXT, chain TEXT, protocol TEXT, event_type TEXT,
            position_key TEXT, ledger_entry_id TEXT, tx_hash TEXT,
            confidence TEXT, payload_json TEXT
        );
        CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY, deployment_id TEXT,
            cycle_id TEXT, total_value_usd TEXT, available_cash_usd TEXT,
            value_confidence TEXT, iteration_number INTEGER, timestamp TEXT, chain TEXT
        );
        CREATE TABLE portfolio_metrics (deployment_id TEXT, initial_value_usd TEXT);
        """
    )
    rows: list[tuple[str, str, str, str]] = [
        # (id, deployment_id, cycle_id, timestamp)
        ("led-A1", "stratA", "cyc-A1", "2026-02-01T00:00:00+00:00"),  # FY2026 Q1
        ("led-A2", "stratA", "cyc-A2", "2026-05-01T00:00:00+00:00"),  # FY2026 Q2
        ("led-B1", "stratB", "cyc-B1", "2026-02-01T00:00:00+00:00"),  # FY2026 Q1
        ("led-B2", "stratB", "cyc-B2", "2026-05-01T00:00:00+00:00"),  # FY2026 Q2
    ]
    for rid, sid, cid, ts in rows:
        cur.execute(
            "INSERT INTO transaction_ledger VALUES "
            "(?, ?, ?, ?, 'SWAP', 'WETH', '0.001', 'USDC', '3.0', "
            "100000, '0', ?, 'arbitrum', 1, "
            '\'{"WETH": {"price_usd": "3000", "oracle_source": "chainlink"}}\', 1, 1, 1)',
            (rid, cid, sid, ts, f"0x{rid}"),
        )
        cur.execute(
            "INSERT INTO accounting_events VALUES "
            "(?, ?, ?, ?, 'arbitrum', 'uniswap_v3', 'SWAP', 'pos-1', ?, ?, 'HIGH', "
            '\'{"event_type": "SWAP", "protocol": "uniswap_v3", '
            '"token_in": "WETH", "token_out": "USDC", '
            '"amount_in": "0.001", "amount_out": "3.0", '
            '"amount_in_usd": "3.0", "amount_out_usd": "3.0", '
            '"realized_pnl_usd": "0", "confidence": "HIGH", '
            '"matching_policy_version": 1}\')',
            (f"ae-{rid}", cid, sid, ts, rid, f"0x{rid}"),
        )
        cur.execute(
            "INSERT INTO portfolio_snapshots (deployment_id, cycle_id, "
            "total_value_usd, available_cash_usd, value_confidence, iteration_number, "
            "timestamp, chain) VALUES (?, ?, '10', '0', 'HIGH', 0, ?, 'arbitrum')",
            (sid, cid, ts),
        )
    # One trailing snapshot per strategy so G6 has ≥2 snapshot endpoints.
    cur.execute(
        "INSERT INTO portfolio_snapshots (deployment_id, cycle_id, "
        "total_value_usd, available_cash_usd, value_confidence, iteration_number, "
        "timestamp, chain) VALUES ('stratA', 'cyc-A2', '10', '0', 'HIGH', 1, "
        "'2026-06-01T00:00:00+00:00', 'arbitrum')"
    )
    cur.execute(
        "INSERT INTO portfolio_snapshots (deployment_id, cycle_id, "
        "total_value_usd, available_cash_usd, value_confidence, iteration_number, "
        "timestamp, chain) VALUES ('stratB', 'cyc-B2', '10', '0', 'HIGH', 1, "
        "'2026-06-01T00:00:00+00:00', 'arbitrum')"
    )
    cur.execute("INSERT INTO portfolio_metrics VALUES ('stratA', '10')")
    cur.execute("INSERT INTO portfolio_metrics VALUES ('stratB', '10')")
    conn.commit()
    conn.close()
    return path


def test_filtered_report_by_deployment_id_returns_only_that_strategys_rows():
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        report_a = accountant_report_from_db(db_path, primitive="lp", deployment_id="stratA")
        report_b = accountant_report_from_db(db_path, primitive="lp", deployment_id="stratB")
        # Both filters narrow the on-chain footprint to the strategy's
        # 2 ledger rows. The filter is doing its job.
        assert len(report_a.on_chain_footprint) == 2
        assert len(report_b.on_chain_footprint) == 2
        assert report_a.deployment_id == "stratA"
        assert report_b.deployment_id == "stratB"
        # No cross-strategy leakage in tx_hashes.
        a_hashes = {tx["tx_hash"] for tx in report_a.on_chain_footprint}
        b_hashes = {tx["tx_hash"] for tx in report_b.on_chain_footprint}
        assert not (a_hashes & b_hashes)
    finally:
        db_path.unlink(missing_ok=True)


def test_filtered_report_by_cycle_ids_only_includes_listed_cycles():
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        report = accountant_report_from_db(db_path, primitive="lp", cycle_ids=["cyc-A1", "cyc-B1"])
        # Both Q1 cycles → 2 footprint rows total.
        assert len(report.on_chain_footprint) == 2
    finally:
        db_path.unlink(missing_ok=True)


def test_filtered_report_by_tax_period_q1_excludes_q2_rows():
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        report = accountant_report_from_db(db_path, primitive="lp", tax_period="Q1-2026")
        # Q1 has 2 ledger rows (one per strategy), Q2 has 2.
        assert len(report.on_chain_footprint) == 2
    finally:
        db_path.unlink(missing_ok=True)


def test_filtered_report_combines_strategy_and_quarter():
    db_path = _make_db_with_two_strategies_two_quarters()
    try:
        report = accountant_report_from_db(db_path, primitive="lp", deployment_id="stratA", tax_period="Q2-2026")
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
            report = accountant_report_from_db(conn, primitive="lp", deployment_id="stratA")
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
        report = accountant_report_from_db(db_path, primitive="lp", deployment_id="strat-does-not-exist")
        assert len(report.on_chain_footprint) == 0
        # G1 FAILs because ledger empty — that's the expected behaviour
        # for an over-restrictive filter, not a regression.
        cells = {c.cell_id: c for c in report.cells}
        assert cells["G1"].status == "FAIL"
    finally:
        db_path.unlink(missing_ok=True)


_JAN_1 = datetime(2026, 1, 1, tzinfo=UTC)
_MAR_1 = datetime(2026, 3, 1, tzinfo=UTC)
_APR_1 = datetime(2026, 4, 1, tzinfo=UTC)
_FILTER_ROWS = (
    ("first", "dep-a", "cycle-1", _JAN_1.isoformat(), "arbitrum", "position-1", "lp:arb:1", None, 0, ""),
    (
        "second",
        "dep-a",
        "cycle-2",
        _APR_1.isoformat(),
        "base",
        "position-2",
        "lp:base:2",
        "12.5",
        0,
        "parsed",
    ),
    (
        "third",
        "dep-b",
        "cycle-1",
        _MAR_1.isoformat(),
        "arbitrum",
        "position-3",
        "lending:arb:3",
        "0",
        0,
        "",
    ),
    ("malformed", "dep-a", "cycle-1", "not-a-timestamp", "optimism", "position-4", "perp:op:4", None, 0, ""),
)
_DEPLOYMENT_FILTERS = (None, "", "dep-a", "missing")
_DEPLOYMENT_SET_FILTERS = (None, (), ("dep-a",), ("dep-b", "dep-a"))
_CYCLE_FILTERS = (None, (), ("cycle-1",), ("cycle-2", "cycle-1"))
_WINDOW_FILTERS = (
    ({}, None, None),
    ({"since": _JAN_1}, _JAN_1.isoformat(), None),
    ({"until": _APR_1}, None, _APR_1.isoformat()),
    ({"since": _JAN_1, "until": _APR_1}, _JAN_1.isoformat(), _APR_1.isoformat()),
    ({"tax_period": "Q1-2026"}, _JAN_1.isoformat(), _APR_1.isoformat()),
)


def _make_filter_db(table: str = "transaction_ledger", timestamp_column: str = "timestamp") -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        f"""
        CREATE TABLE {table} (
            id TEXT,
            deployment_id TEXT,
            cycle_id TEXT,
            {timestamp_column} TEXT,
            chain TEXT,
            position_id TEXT,
            position_key TEXT,
            optional_amount TEXT,
            measured_zero INTEGER,
            parser_value TEXT
        )
        """
    )
    conn.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", _FILTER_ROWS)
    return conn


@pytest.mark.parametrize(
    ("deployment_id", "deployment_ids", "cycle_ids", "window_case"),
    product(_DEPLOYMENT_FILTERS, _DEPLOYMENT_SET_FILTERS, _CYCLE_FILTERS, _WINDOW_FILTERS),
)
def test_filtered_rows_exhaustive_filter_truth_table(
    deployment_id: str | None,
    deployment_ids: tuple[str, ...] | None,
    cycle_ids: tuple[str, ...] | None,
    window_case: tuple[dict[str, datetime | str], str | None, str | None],
) -> None:
    window, since_text, until_text = window_case
    filt = AccountingReportFilter(
        deployment_id=deployment_id,
        deployment_ids=deployment_ids,
        cycle_ids=cycle_ids,
        **window,
    )
    conn = _make_filter_db()
    try:
        actual_ids = [row["id"] for row in _filtered_rows(conn, "transaction_ledger", filt)]
    finally:
        conn.close()

    expected_ids: list[str] = []
    if deployment_ids == () or cycle_ids == ():
        assert actual_ids == expected_ids
        return
    for row_id, row_deployment, row_cycle, timestamp, *_ in _FILTER_ROWS:
        if deployment_id and row_deployment != deployment_id:
            continue
        if deployment_ids is not None and row_deployment not in deployment_ids:
            continue
        if cycle_ids is not None and row_cycle not in cycle_ids:
            continue
        if since_text and timestamp < since_text:
            continue
        if until_text and timestamp >= until_text:
            continue
        expected_ids.append(row_id)
    assert actual_ids == expected_ids


def test_filtered_rows_golden_preserves_order_identity_and_empty_zero_values() -> None:
    conn = _make_filter_db()
    try:
        rows = _filtered_rows(conn, "transaction_ledger", AccountingReportFilter())
    finally:
        conn.close()

    assert rows == [
        {
            "id": row_id,
            "deployment_id": deployment_id,
            "cycle_id": cycle_id,
            "timestamp": timestamp,
            "chain": chain,
            "position_id": position_id,
            "position_key": position_key,
            "optional_amount": optional_amount,
            "measured_zero": measured_zero,
            "parser_value": parser_value,
        }
        for (
            row_id,
            deployment_id,
            cycle_id,
            timestamp,
            chain,
            position_id,
            position_key,
            optional_amount,
            measured_zero,
            parser_value,
        ) in _FILTER_ROWS
    ]


def test_filtered_rows_supports_and_restores_custom_connection_row_factory() -> None:
    conn = _make_filter_db()

    def dict_factory(cursor: sqlite3.Cursor, row: tuple[object, ...]) -> dict[str, object]:
        return {column[0]: row[index] for index, column in enumerate(cursor.description)}

    conn.row_factory = dict_factory
    try:
        rows = _filtered_rows(conn, "transaction_ledger", AccountingReportFilter(deployment_id="dep-a"))
        assert conn.row_factory is dict_factory
    finally:
        conn.close()

    assert [row["id"] for row in rows] == ["first", "second", "malformed"]


@pytest.mark.parametrize(
    ("table", "timestamp_column"),
    (
        ("transaction_ledger", "timestamp"),
        ("position_events", "timestamp"),
        ("accounting_events", "timestamp"),
        ("portfolio_snapshots", "timestamp"),
        ("position_state_snapshots", "captured_at"),
    ),
)
def test_filtered_rows_table_map_uses_exact_timestamp_and_identity_columns(
    table: str,
    timestamp_column: str,
) -> None:
    conn = _make_filter_db(table, timestamp_column)
    try:
        rows = _filtered_rows(
            conn,
            table,
            AccountingReportFilter(
                deployment_id="dep-a",
                deployment_ids=("dep-a", "dep-b"),
                cycle_ids=("cycle-1",),
                since=_JAN_1,
                until=_APR_1,
            ),
        )
    finally:
        conn.close()

    assert [row["id"] for row in rows] == ["first"]
    assert rows[0]["deployment_id"] == "dep-a"
    assert rows[0]["chain"] == "arbitrum"
    assert rows[0]["position_id"] == "position-1"
    assert rows[0]["position_key"] == "lp:arb:1"


def test_filtered_rows_portfolio_metrics_keeps_lifetime_values_for_unsupported_period_filters() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE portfolio_metrics (deployment_id TEXT, initial_value_usd TEXT)")
    conn.executemany("INSERT INTO portfolio_metrics VALUES (?, ?)", (("dep-a", "0"), ("dep-b", None)))
    try:
        rows = _filtered_rows(
            conn,
            "portfolio_metrics",
            AccountingReportFilter(
                deployment_id="dep-a",
                cycle_ids=(),
                since=_JAN_1,
                until=_APR_1,
            ),
        )
    finally:
        conn.close()

    assert rows == [{"deployment_id": "dep-a", "initial_value_usd": "0"}]


@pytest.mark.parametrize(
    ("create_sql", "filt"),
    (
        (None, AccountingReportFilter(tax_period="invalid")),
        (
            "CREATE TABLE transaction_ledger (id TEXT, cycle_id TEXT, timestamp TEXT)",
            AccountingReportFilter(deployment_id="dep-a", tax_period="invalid"),
        ),
        (
            "CREATE TABLE transaction_ledger (id TEXT, deployment_id TEXT, cycle_id TEXT, timestamp TEXT)",
            AccountingReportFilter(deployment_ids=(), tax_period="invalid"),
        ),
        (
            "CREATE TABLE transaction_ledger (id TEXT, deployment_id TEXT, timestamp TEXT)",
            AccountingReportFilter(cycle_ids=("cycle-1",), tax_period="invalid"),
        ),
        (
            "CREATE TABLE transaction_ledger (id TEXT, deployment_id TEXT, cycle_id TEXT, timestamp TEXT)",
            AccountingReportFilter(cycle_ids=(), tax_period="invalid"),
        ),
    ),
)
def test_filtered_rows_fail_closed_precedes_window_validation(
    create_sql: str | None,
    filt: AccountingReportFilter,
) -> None:
    conn = sqlite3.connect(":memory:")
    if create_sql:
        conn.execute(create_sql)
    try:
        assert _filtered_rows(conn, "transaction_ledger", filt) == []
    finally:
        conn.close()


def test_filtered_rows_window_validation_precedes_unsupported_timestamp_filter() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE portfolio_metrics (deployment_id TEXT)")
    try:
        with pytest.raises(ValueError, match="mutually exclusive"):
            _filtered_rows(
                conn,
                "portfolio_metrics",
                AccountingReportFilter(tax_period="FY2026", since=_JAN_1),
            )
    finally:
        conn.close()


def test_filtered_rows_missing_timestamp_column_fails_closed() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE transaction_ledger (id TEXT, deployment_id TEXT, cycle_id TEXT)")
    conn.execute("INSERT INTO transaction_ledger VALUES ('row', 'dep-a', 'cycle-1')")
    try:
        assert _filtered_rows(conn, "transaction_ledger", AccountingReportFilter(since=_JAN_1)) == []
    finally:
        conn.close()


def test_filtered_rows_unknown_table_error_precedes_filter_validation() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        with pytest.raises(ValueError, match="unknown table 'not_allowed'"):
            _filtered_rows(conn, "not_allowed", AccountingReportFilter(tax_period="invalid"))
    finally:
        conn.close()
