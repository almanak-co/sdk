"""G6 distinguishes legitimate no-prior-basis SWAP from an unmeasured gap (VIB-4394).

The opening SWAP of a strategy that sells pre-existing wallet inventory (or any
acquiring swap) correctly emits ``realized_pnl_usd=None`` — there is no prior
FIFO lot to realize against. Before VIB-4394 the Accountant Test G6 cell counted
that ``None`` as ``Σ_swaps_usd_null_count`` and FAILed, conflating a legitimate
measured state with a measurement gap.

This offline regression builds two self-contained synthetic SQLite DBs that are
otherwise identical CLEAN round-trips and asserts on the SAME G6 cell:

  * A SWAP with measured ``amount_in_usd`` but ``realized_pnl_usd=None`` (no prior
    basis) → does NOT increment ``Σ_swaps_usd_null_count``; lands in the new,
    non-failing ``Σ_swaps_no_prior_basis_count``; G6 PASSes.
  * A SWAP with UNMEASURED amounts (``amount_in_usd`` absent) → still increments
    ``Σ_swaps_usd_null_count`` → ``has_nulls`` → G6 FAILs. The gap-catching
    behaviour is preserved.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from almanak.framework.accounting.accountant_test import run_against_sqlite

_DEP = "deployment:g64394"
_CHAIN = "arbitrum"
_PROTO = "uniswap_v3"
_WALLET = "0x000000000000000000000000000000000000aaaa"
_CYCLE = "cycle-g64394-001"

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
    return (_TS_BASE + timedelta(seconds=off)).isoformat()


def _wallet_balances(usdc_qty: str) -> str:
    # A flat USDC bag (stable, no revaluation) so the ambient inventory term is 0
    # and the SWAP bucket is the only thing G6 evaluates.
    return json.dumps(
        [
            {
                "symbol": "USDC",
                "balance": usdc_qty,
                "value_usd": usdc_qty,
                "address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                "price_usd": "1",
            }
        ]
    )


def _swap_payload(*, measured: bool) -> str:
    """An acquiring SWAP (USDC→WETH) with no prior FIFO lot → realized_pnl None.

    ``measured=True``  : amounts + amount_in_usd present (the legitimate
                         no-prior-basis case — a real opening swap).
    ``measured=False`` : amounts unmeasured (the receipt parser could not resolve
                         them) — a genuine measurement gap that must still FAIL G6.
    """
    if measured:
        payload = {
            "event_type": "SWAP",
            "protocol": _PROTO,
            "token_in": "USDC",
            "token_out": "WETH",
            "amount_in": "1000",
            "amount_out": "0.4",
            "amount_in_usd": "1000",
            "amount_out_usd": "1000",
            "realized_pnl_usd": None,  # no prior FIFO lot to realize against
            "realized_pnl_usd_matched": None,
            "cost_basis_recorded": True,  # token_out acquisition lot recorded
            "confidence": "HIGH",
            "swap_position_key": f"swap:{_CHAIN}:{_WALLET.lower()}",
        }
    else:
        payload = {
            "event_type": "SWAP",
            "protocol": _PROTO,
            "token_in": "USDC",
            "token_out": "WETH",
            "amount_in": None,  # receipt parser could not resolve amounts
            "amount_out": None,
            "amount_in_usd": None,
            "amount_out_usd": None,
            "realized_pnl_usd": None,
            "realized_pnl_usd_matched": None,
            "confidence": "UNAVAILABLE",
            "unavailable_reason": "amount_in/out unmeasured (decimals unresolved)",
            "swap_position_key": f"swap:{_CHAIN}:{_WALLET.lower()}",
        }
    return json.dumps(payload)


def _build_db(path: Path, *, measured: bool) -> None:
    """A net-flat round-trip whose only typed event is an acquiring SWAP.

    Equity is flat across the two endpoint snapshots ($1000 → $1000): the strategy
    swapped $1000 USDC into $1000 WETH (zero slippage in the fixture), so wallet
    PnL = 0. The SWAP itself realizes nothing (no prior basis). With VIB-4394 the
    measured case reconciles (component PnL 0, gap 0, no failing nulls) → G6 PASS;
    the unmeasured case is a real null → G6 FAIL.
    """
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    try:
        for ddl in _DDL:
            conn.execute(ddl)

        # The SWAP ledger row (drives the lifecycle-step presence check).
        conn.execute(
            "INSERT INTO transaction_ledger (id, cycle_id, deployment_id, execution_mode,"
            " timestamp, intent_type, token_in, amount_in, token_out, amount_out,"
            " gas_usd, tx_hash, chain, protocol, success) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
            (
                "ldg-swap-1", _CYCLE, _DEP, "paper", _ts(30), "SWAP",
                "USDC", "1000" if measured else "", "WETH", "0.4" if measured else "",
                "0", "0xswap", _CHAIN, _PROTO,
            ),
        )

        conn.execute(
            "INSERT INTO accounting_events (id, deployment_id, cycle_id, execution_mode,"
            " timestamp, chain, protocol, wallet_address, event_type, position_key,"
            " ledger_entry_id, tx_hash, confidence, payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "ae-swap-1", _DEP, _CYCLE, "paper", _ts(30), _CHAIN, _PROTO, _WALLET,
                "SWAP", "", "ldg-swap-1", "0xswap",
                "HIGH" if measured else "UNAVAILABLE", _swap_payload(measured=measured),
            ),
        )

        # Two endpoint snapshots — equity flat at $1000 (USDC bag is stable).
        for it, off in ((1, 0), (2, 60)):
            conn.execute(
                "INSERT INTO portfolio_snapshots (deployment_id, cycle_id, execution_mode, timestamp,"
                " iteration_number, total_value_usd, available_cash_usd, deployed_capital_usd,"
                " wallet_total_value_usd, value_confidence, positions_json, token_prices_json,"
                " wallet_balances_json, chain, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    _DEP, _CYCLE, "paper", _ts(off), it, "0", "1000.0", "0",
                    "1000.0", "HIGH", "[]", "{}", _wallet_balances("1000"), _CHAIN, _ts(off),
                ),
            )

        conn.execute(
            "INSERT INTO portfolio_metrics (deployment_id, initial_value_usd, initial_timestamp,"
            " gas_spent_usd, total_value_usd, cycle_id, execution_mode, is_complete, updated_at)"
            " VALUES (?,?,?,?,?,?,?,1,?)",
            (_DEP, "1000.0", _ts(0), "0", "1000.0", _CYCLE, "paper", _ts(60)),
        )
        conn.commit()
    finally:
        conn.close()


def _g6(report) -> tuple[str, dict]:
    for c in report.cells:
        if c.cell_id == "G6":
            return c.status, c.decomposition
    raise AssertionError("G6 cell not found")


def test_measured_no_prior_basis_swap_does_not_fail_g6(tmp_path: Path) -> None:
    """A measured acquiring SWAP with realized_pnl=None is a legitimate state."""
    db = tmp_path / "g6_4394_measured.sqlite"
    _build_db(db, measured=True)
    status, decomp = _g6(run_against_sqlite(db, primitive="lp"))

    # The legitimate no-prior-basis SWAP is NOT counted as a failing null...
    assert decomp["Σ_swaps_usd_null_count"] == "0"
    # ...it lands in the new non-failing forensic bucket...
    assert decomp["Σ_swaps_no_prior_basis_count"] == "1"
    # ...and G6 reconciles (equity flat, component PnL 0, no failing nulls).
    assert status == "PASS", decomp


def test_unmeasured_amount_swap_still_fails_g6(tmp_path: Path) -> None:
    """An UNMEASURED-amount SWAP is a real gap — still FAILs G6 (regression guard)."""
    db = tmp_path / "g6_4394_unmeasured.sqlite"
    _build_db(db, measured=False)
    status, decomp = _g6(run_against_sqlite(db, primitive="lp"))

    # Unmeasured amounts → the failing null bucket, NOT the legitimate one.
    assert decomp["Σ_swaps_usd_null_count"] == "1"
    assert decomp["Σ_swaps_no_prior_basis_count"] == "0"
    assert status == "FAIL", decomp
