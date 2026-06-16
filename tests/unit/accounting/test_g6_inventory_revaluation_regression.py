"""Regression-pin: the ambient inventory revaluation term moves G6 FAIL → PASS.

This is the offline proof for the G6 inventory-revaluation lane (blueprint 27
§11.5). It builds a self-contained synthetic SQLite DB that is a CLEAN LP
round-trip (the LP legs reconcile exactly) EXCEPT the wallet also holds an idle
WETH position that appreciates between the two endpoint snapshots. The wallet
(equity) method captures that appreciation; the typed component sum does not —
so WITHOUT the ambient revaluation term G6 reports a spurious gap and FAILs, and
WITH the term G6 reconciles and PASSes.

The test asserts BOTH directions on the SAME DB:
  * with the term wired in (production behaviour) → G6 PASS, gap ≈ 0
  * the decomposition records the exact ``Σ_inventory_reval_usd`` = the gap the
    naive (term-less) component sum would have left open.

It also pins the dashboard-parity contract: ``compute_reconciliation`` given the
same endpoint snapshots produces the identical component PnL as the Accountant
Test G6 cell.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from almanak.framework.accounting.accountant_test import run_against_sqlite

_DEP = "deployment:g6inv"
_CHAIN = "arbitrum"
_PROTO = "uniswap_v3"
_WALLET = "0x000000000000000000000000000000000000aaaa"
_CYCLE = "cycle-g6inv-001"

_DDL = (
    """
    CREATE TABLE transaction_ledger (
        id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL, deployment_id TEXT NOT NULL,
        execution_mode TEXT DEFAULT '', timestamp TEXT NOT NULL, intent_type TEXT NOT NULL,
        token_in TEXT, amount_in TEXT, token_out TEXT, amount_out TEXT,
        effective_price TEXT, slippage_bps REAL, gas_used INTEGER, gas_usd TEXT,
        tx_hash TEXT, chain TEXT, protocol TEXT, success BOOLEAN NOT NULL DEFAULT 1,
        error TEXT, extracted_data_json TEXT DEFAULT '', price_inputs_json TEXT DEFAULT '',
        pre_state_json TEXT DEFAULT '', post_state_json TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE accounting_events (
        id TEXT PRIMARY KEY, deployment_id TEXT NOT NULL, cycle_id TEXT NOT NULL,
        execution_mode TEXT NOT NULL, timestamp TEXT NOT NULL, chain TEXT NOT NULL,
        protocol TEXT NOT NULL, wallet_address TEXT NOT NULL, event_type TEXT NOT NULL,
        position_key TEXT NOT NULL, ledger_entry_id TEXT, tx_hash TEXT, confidence TEXT NOT NULL,
        payload_json TEXT NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE portfolio_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT, deployment_id TEXT NOT NULL,
        cycle_id TEXT DEFAULT '', execution_mode TEXT DEFAULT '', timestamp TEXT NOT NULL,
        iteration_number INTEGER DEFAULT 0, total_value_usd TEXT NOT NULL,
        available_cash_usd TEXT NOT NULL, deployed_capital_usd TEXT DEFAULT '0',
        wallet_total_value_usd TEXT DEFAULT '0', value_confidence TEXT DEFAULT 'HIGH',
        positions_json TEXT NOT NULL, token_prices_json TEXT DEFAULT '{}',
        wallet_balances_json TEXT DEFAULT '[]', chain TEXT, created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE position_events (
        id TEXT PRIMARY KEY, deployment_id TEXT NOT NULL, cycle_id TEXT DEFAULT '',
        execution_mode TEXT DEFAULT '', position_id TEXT NOT NULL, position_type TEXT NOT NULL,
        event_type TEXT NOT NULL, timestamp TEXT NOT NULL, protocol TEXT, chain TEXT,
        token0 TEXT, token1 TEXT, amount0 TEXT, amount1 TEXT, value_usd TEXT,
        tick_lower INTEGER, tick_upper INTEGER, liquidity TEXT, in_range BOOLEAN,
        fees_token0 TEXT, fees_token1 TEXT, leverage TEXT, entry_price TEXT, mark_price TEXT,
        unrealized_pnl TEXT, is_long BOOLEAN, tx_hash TEXT, gas_usd TEXT, ledger_entry_id TEXT,
        protocol_fees_usd TEXT DEFAULT '', attribution_json TEXT DEFAULT '{}',
        attribution_version INTEGER DEFAULT 0
    )
    """,
    """
    CREATE TABLE portfolio_metrics (
        deployment_id TEXT PRIMARY KEY, initial_value_usd TEXT NOT NULL,
        initial_timestamp TEXT NOT NULL, deposits_usd TEXT DEFAULT '0',
        withdrawals_usd TEXT DEFAULT '0', gas_spent_usd TEXT DEFAULT '0',
        total_value_usd TEXT DEFAULT '0', positions_json TEXT DEFAULT '[]',
        cycle_id TEXT, execution_mode TEXT DEFAULT '', is_complete BOOLEAN DEFAULT 1,
        updated_at TEXT NOT NULL
    )
    """,
)


_TS_BASE = datetime(2026, 5, 9, 0, 0, 0, tzinfo=UTC)


def _ts(off: int) -> str:
    # Build from base + timedelta so offsets ≥ 60s roll over into valid ISO
    # timestamps (a raw f"...:{off:02d}" yields "00:00:60", which trips strict
    # datetime parsing).
    return (_TS_BASE + timedelta(seconds=off)).isoformat()


def _wallet_balances(weth_qty: str, weth_price: str) -> str:
    return json.dumps(
        [
            {
                "symbol": "WETH",
                "balance": weth_qty,
                "value_usd": str(Decimal(weth_qty) * Decimal(weth_price)),
                "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "price_usd": weth_price,
            }
        ]
    )


def _build_db(path: Path) -> None:
    """A clean net-zero LP round-trip + an idle WETH bag that appreciates.

    LP legs: open $1000 → close returns $1000 principal, $0 fees, gas $0 ⇒ the
    LP component PnL is exactly $0. The ONLY thing that moves wallet equity is
    the idle 1 WETH appreciating from $2000 → $2500 (+$500). Without the ambient
    revaluation term the component sum is $0 and G6's gap is $500 → FAIL; with
    the term the component sum is $500 and the gap is $0 → PASS.
    """
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    try:
        for ddl in _DDL:
            conn.execute(ddl)

        lp_key = "lp:arbitrum:uniswap_v3:wallet:WETH-USDC-3000"

        # LP_OPEN — deploy $1000 (cost basis $1000).
        conn.execute(
            "INSERT INTO transaction_ledger (id, cycle_id, deployment_id, execution_mode, timestamp,"
            " intent_type, token_in, amount_in, token_out, amount_out, gas_usd, tx_hash, chain,"
            " protocol, success) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
            ("tl-1", _CYCLE, _DEP, "paper", _ts(0), "LP_OPEN", "USDC", "1000", "LP-NFT", "1",
             "0", "0xopen", _CHAIN, _PROTO),
        )
        conn.execute(
            "INSERT INTO accounting_events (id, deployment_id, cycle_id, execution_mode, timestamp,"
            " chain, protocol, wallet_address, event_type, position_key, ledger_entry_id, tx_hash,"
            " confidence, payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("ae-1", _DEP, _CYCLE, "paper", _ts(0), _CHAIN, _PROTO, _WALLET, "LP_OPEN", lp_key,
             "tl-1", "0xopen", "HIGH",
             json.dumps({
                 "event_type": "LP_OPEN", "protocol": _PROTO, "position_key": lp_key,
                 "pool_address": "0xpool", "token0": "WETH", "token1": "USDC",
                 "amount0": "0.25", "amount1": "500.0", "amount0_usd": "500.0", "amount1_usd": "500.0",
                 "cost_basis_usd": "1000.0", "tick_lower": -201000, "tick_upper": -199000,
                 "liquidity": 1234567890, "current_tick": -200000, "in_range": True, "confidence": "HIGH",
             })),
        )

        # LP_CLOSE — return $1000 principal, $0 realized PnL, $0 fees, $0 gas.
        conn.execute(
            "INSERT INTO transaction_ledger (id, cycle_id, deployment_id, execution_mode, timestamp,"
            " intent_type, token_in, amount_in, token_out, amount_out, gas_usd, tx_hash, chain,"
            " protocol, success) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
            ("tl-2", _CYCLE, _DEP, "paper", _ts(30), "LP_CLOSE", "LP-NFT", "1", "USDC", "1000",
             "0", "0xclose", _CHAIN, _PROTO),
        )
        conn.execute(
            "INSERT INTO accounting_events (id, deployment_id, cycle_id, execution_mode, timestamp,"
            " chain, protocol, wallet_address, event_type, position_key, ledger_entry_id, tx_hash,"
            " confidence, payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("ae-2", _DEP, _CYCLE, "paper", _ts(30), _CHAIN, _PROTO, _WALLET, "LP_CLOSE", lp_key,
             "tl-2", "0xclose", "HIGH",
             json.dumps({
                 "event_type": "LP_CLOSE", "protocol": _PROTO, "position_key": lp_key,
                 "pool_address": "0xpool", "token0": "WETH", "token1": "USDC",
                 "amount0": "0.25", "amount1": "500.0", "amount0_usd": "500.0", "amount1_usd": "500.0",
                 "fees0_collected": "0", "fees1_collected": "0", "fees_total_usd": "0.0",
                 "realized_pnl_usd": "0.0", "il_usd": "0.0", "hodl_value_usd": "1000.0",
                 "confidence": "HIGH",
             })),
        )

        # Two endpoint snapshots. Equity = total_value_usd + available_cash_usd.
        # The idle WETH (1 unit) lives in available_cash_usd at its marked value,
        # so equity moves from $2000 → $2500 as WETH appreciates. deployed = 0 at
        # both endpoints (round-trip ends flat on the LP).
        #
        # initial: equity = 0 (LP not yet deployed in this snapshot) + 2000 cash = 2000
        # final:   equity = 0 + 2500 cash = 2500   ⇒ wallet_pnl = +500.
        conn.execute(
            "INSERT INTO portfolio_snapshots (deployment_id, cycle_id, execution_mode, timestamp,"
            " iteration_number, total_value_usd, available_cash_usd, deployed_capital_usd,"
            " wallet_total_value_usd, value_confidence, positions_json, token_prices_json,"
            " wallet_balances_json, chain, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (_DEP, _CYCLE, "paper", _ts(0), 1, "0", "2000.0", "0", "2000.0", "HIGH", "[]",
             "{}", _wallet_balances("1", "2000"), _CHAIN, _ts(0)),
        )
        conn.execute(
            "INSERT INTO portfolio_snapshots (deployment_id, cycle_id, execution_mode, timestamp,"
            " iteration_number, total_value_usd, available_cash_usd, deployed_capital_usd,"
            " wallet_total_value_usd, value_confidence, positions_json, token_prices_json,"
            " wallet_balances_json, chain, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (_DEP, _CYCLE, "paper", _ts(60), 2, "0", "2500.0", "0", "2500.0", "HIGH", "[]",
             "{}", _wallet_balances("1", "2500"), _CHAIN, _ts(60)),
        )

        conn.execute(
            "INSERT INTO portfolio_metrics (deployment_id, initial_value_usd, initial_timestamp,"
            " gas_spent_usd, total_value_usd, cycle_id, execution_mode, is_complete, updated_at)"
            " VALUES (?,?,?,?,?,?,?,1,?)",
            (_DEP, "2000.0", _ts(0), "0", "2500.0", _CYCLE, "paper", _ts(60)),
        )
        conn.commit()
    finally:
        conn.close()


def _g6(report) -> tuple[str, dict]:
    for c in report.cells:
        if c.cell_id == "G6":
            return c.status, c.decomposition
    raise AssertionError("G6 cell not found")


def test_inventory_term_moves_g6_to_pass(tmp_path: Path) -> None:
    """With the lane wired in, the appreciating idle bag reconciles → G6 PASS."""
    db = tmp_path / "g6_inv.sqlite"
    _build_db(db)
    report = run_against_sqlite(db, primitive="lp")
    status, decomp = _g6(report)

    # wallet_pnl = +$500 (idle WETH 1 × ($2500 − $2000)).
    assert decomp["wallet_pnl_usd"] == "500.0"
    # The ambient term explains exactly the otherwise-unexplained $500.
    assert decomp["Σ_inventory_reval_usd"] == "500"
    assert decomp["inventory_reval_confidence"] == "measured"
    # component_pnl now equals wallet_pnl ⇒ gap collapses, G6 PASSes.
    assert Decimal(decomp["gap_usd"]) <= Decimal(decomp["ε_threshold_usd"])
    assert status == "PASS", f"expected PASS, got {status}: {decomp}"


def test_removing_the_term_reproduces_the_old_fail_gap(tmp_path: Path) -> None:
    """Subtracting the term from component_pnl reproduces the pre-fix $500 gap.

    This pins that the term is load-bearing: the same DB, with the inventory
    revaluation removed, leaves the exact spurious gap the naive component sum
    would have reported (and would FAIL G6 on).
    """
    db = tmp_path / "g6_inv.sqlite"
    _build_db(db)
    _, decomp = _g6(run_against_sqlite(db, primitive="lp"))

    component_with_term = Decimal(decomp["component_pnl_usd"])
    term = Decimal(decomp["Σ_inventory_reval_usd"])
    component_without_term = component_with_term - term

    wallet = Decimal(decomp["wallet_pnl_usd"])
    naive_gap = abs(wallet - component_without_term)
    eps = Decimal(decomp["ε_threshold_usd"])

    # The LP legs net to $0, so the naive component PnL is $0 and the naive gap
    # is the full $500 ambient appreciation — well above ε ⇒ the old FAIL.
    assert component_without_term == Decimal("0")
    assert naive_gap == Decimal("500")
    assert naive_gap > eps


def test_other_global_cells_unaffected(tmp_path: Path) -> None:
    """G4 / G5 / G8 stay PASS — the lane only touches the G6 component sum."""
    db = tmp_path / "g6_inv.sqlite"
    _build_db(db)
    report = run_against_sqlite(db, primitive="lp")
    by_id = {c.cell_id: c.status for c in report.cells}
    for cell in ("G4", "G5", "G8"):
        assert by_id.get(cell) == "PASS", f"{cell} = {by_id.get(cell)} (expected PASS)"


def test_dashboard_parity_same_component_pnl(tmp_path: Path) -> None:
    """``compute_reconciliation`` == the Accountant Test G6 component, same DB.

    The dashboard reconciliation, given the SAME endpoint snapshots and events,
    must fold in the SAME ambient term and land the SAME ``component_pnl_usd`` as
    the harness G6 cell — they cannot drift.
    """
    from almanak.framework.dashboard.quant_aggregations import (
        CostStack,
        compute_reconciliation,
    )

    db = tmp_path / "g6_inv.sqlite"
    _build_db(db)
    report = run_against_sqlite(db, primitive="lp")
    _, decomp = _g6(report)

    # Reload the same rows the harness scored.
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        snaps = [dict(r) for r in conn.execute(
            "SELECT * FROM portfolio_snapshots WHERE deployment_id=? ORDER BY timestamp", (_DEP,)
        )]
        events = [dict(r) for r in conn.execute(
            "SELECT * FROM accounting_events WHERE deployment_id=? ORDER BY timestamp", (_DEP,)
        )]
    finally:
        conn.close()

    initial = Decimal(snaps[0]["total_value_usd"]) + Decimal(snaps[0]["available_cash_usd"])
    nav = Decimal(snaps[-1]["total_value_usd"]) + Decimal(snaps[-1]["available_cash_usd"])

    recon = compute_reconciliation(
        initial_value_usd=initial,
        nav_usd=nav,
        cost_stack=CostStack(),  # gas $0 in this fixture
        accounting_events=events,
        snapshot_initial=snaps[0],
        snapshot_final=snaps[-1],
        deployment_id=_DEP,
    )

    assert recon.sum_inventory_reval == Decimal(decomp["Σ_inventory_reval_usd"])
    assert recon.component_pnl_usd == Decimal(decomp["component_pnl_usd"])
    assert recon.gap_usd <= recon.epsilon_usd
