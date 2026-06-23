"""Regression-pin (VIB-4256): G6 sees gas EXACTLY ONCE — wallet ≡ component.

VIB-4256's title was "G6 looping reconciliation gap is gas double-count". The
verified root cause is structural: gas is a *flow* that must show up on BOTH
sides of the G6 identity, ONCE each, with no skew:

  * Wallet (equity) side — gas is paid in the chain's NATIVE token, which lives
    in ``wallet_balances`` (VIB-4225 ACC-02 ``_resolve_native_gas`` /
    ``_append_native_gas_to_wallet``). As gas is spent the native balance drops,
    so ``equity_final − equity_initial`` already carries ``−Σ_gas``.
  * Component side — ``_cell_g6_reconciliation`` subtracts ``sum_gas`` (=
    ``Σ transaction_ledger.gas_usd``) from ``component_pnl``.

Because the native balance is now IN the snapshot equity, both sides move by the
same ``−Σ_gas`` and the gap collapses. The "double-count" the ticket observed was
the *asymmetric* world: gas on the component side, but the native-gas drain
MISSING from the wallet side (or vice-versa), leaving a gap ≈ Σ_gas. This file is
the offline proof that the two sides are symmetric.

Fixture: a CLEAN swap round-trip (``realized_pnl_usd = 0``) whose ONLY economic
effect is gas spend. ``sum_swap = 0`` so ``component_pnl = −Σ_gas``; the native
ETH bag drops by exactly Σ_gas between the two endpoint snapshots so
``wallet_pnl = −Σ_gas``. Gap → 0 → G6 PASS. The inverse test pins that an
asymmetric wallet side (native drain NOT reflected in equity) re-opens the exact
``Σ_gas`` gap the ticket reported.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from almanak.framework.accounting.accountant_test import run_against_sqlite

_DEP = "deployment:g6gas4256"
_CHAIN = "arbitrum"
_PROTO = "uniswap_v3"
_WALLET = "0x000000000000000000000000000000000000bbbb"
_CYCLE = "cycle-g6gas-001"

# Σ_gas for the fixture: one swap burning $1.50 of native ETH gas.
_GAS_USD = Decimal("1.50")
# ETH marked flat at $2000 across both endpoints so the ONLY equity move is the
# native-balance drain (no ambient revaluation term in play — VIB-4256 is about
# gas symmetry, not price drift).
_ETH_PRICE = Decimal("2000")
# Native ETH held: 1.0 ETH initially, dropping by (gas_usd / price) after the swap.
_ETH_INITIAL = Decimal("1.0")
_ETH_FINAL = _ETH_INITIAL - (_GAS_USD / _ETH_PRICE)  # 1.0 − 0.00075 = 0.99925


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


def _wallet_balances(eth_qty: Decimal) -> str:
    """A single native-ETH row — the gas-paying token held in the wallet."""
    return json.dumps(
        [
            {
                "symbol": "ETH",
                "balance": str(eth_qty),
                "value_usd": str(eth_qty * _ETH_PRICE),
                "address": "",
                "price_usd": str(_ETH_PRICE),
            }
        ]
    )


def _build_db(path: Path, *, native_drain_reflected: bool) -> None:
    """A clean swap whose only economic effect is gas.

    ``native_drain_reflected=True`` (production): the final snapshot's native ETH
    balance has dropped by ``gas_usd / price``, so wallet equity already carries
    ``−Σ_gas``. With the component side ALSO subtracting ``sum_gas``, the gap is 0.

    ``native_drain_reflected=False`` (the buggy asymmetric world the ticket
    reported): the native ETH balance is UNCHANGED across both endpoints, so the
    wallet side does NOT see the gas drain while the component side still
    subtracts it — re-opening the exact ``Σ_gas`` gap.
    """
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    try:
        for ddl in _DDL:
            conn.execute(ddl)

        swap_key = "swap:arbitrum:uniswap_v3:wallet:USDC-USDT"

        # SWAP — USDC→USDT at par, realized PnL $0, gas $1.50. The swap itself is
        # economically flat; the ONLY money that moves is the native gas burn.
        conn.execute(
            "INSERT INTO transaction_ledger (id, cycle_id, deployment_id, execution_mode, timestamp,"
            " intent_type, token_in, amount_in, token_out, amount_out, gas_usd, tx_hash, chain,"
            " protocol, success, price_inputs_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)",
            (
                "tl-swap",
                _CYCLE,
                _DEP,
                "paper",
                _ts(10),
                "SWAP",
                "USDC",
                "500",
                "USDT",
                "500",
                str(_GAS_USD),
                "0xswap",
                _CHAIN,
                _PROTO,
                json.dumps({"USDC": "1.0", "USDT": "1.0", "ETH": str(_ETH_PRICE)}),
            ),
        )
        conn.execute(
            "INSERT INTO accounting_events (id, deployment_id, cycle_id, execution_mode, timestamp,"
            " chain, protocol, wallet_address, event_type, position_key, ledger_entry_id, tx_hash,"
            " confidence, payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "ae-swap",
                _DEP,
                _CYCLE,
                "paper",
                _ts(10),
                _CHAIN,
                _PROTO,
                _WALLET,
                "SWAP",
                swap_key,
                "tl-swap",
                "0xswap",
                "HIGH",
                json.dumps(
                    {
                        "event_type": "SWAP",
                        "protocol": _PROTO,
                        "position_key": swap_key,
                        "token_in": "USDC",
                        "token_out": "USDT",
                        "amount_in": "500.0",
                        "amount_out": "500.0",
                        "amount_in_usd": "500.0",
                        "amount_out_usd": "500.0",
                        "realized_pnl_usd": "0.0",
                        "confidence": "HIGH",
                    }
                ),
            ),
        )

        # Two endpoint snapshots. Equity = total_value_usd + available_cash_usd.
        # The native ETH bag is the cash. deployed = 0 at both endpoints (no open
        # protocol position — a flat swap).
        eth_final = _ETH_FINAL if native_drain_reflected else _ETH_INITIAL
        # available_cash mirrors the native ETH value (the only wallet asset).
        cash_initial = _ETH_INITIAL * _ETH_PRICE  # $2000.00
        cash_final = eth_final * _ETH_PRICE  # $1998.50 (reflected) / $2000.00 (asymmetric)

        conn.execute(
            "INSERT INTO portfolio_snapshots (deployment_id, cycle_id, execution_mode, timestamp,"
            " iteration_number, total_value_usd, available_cash_usd, deployed_capital_usd,"
            " wallet_total_value_usd, value_confidence, positions_json, token_prices_json,"
            " wallet_balances_json, chain, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                _DEP,
                _CYCLE,
                "paper",
                _ts(0),
                1,
                "0",
                str(cash_initial),
                "0",
                str(cash_initial),
                "HIGH",
                "[]",
                "{}",
                _wallet_balances(_ETH_INITIAL),
                _CHAIN,
                _ts(0),
            ),
        )
        conn.execute(
            "INSERT INTO portfolio_snapshots (deployment_id, cycle_id, execution_mode, timestamp,"
            " iteration_number, total_value_usd, available_cash_usd, deployed_capital_usd,"
            " wallet_total_value_usd, value_confidence, positions_json, token_prices_json,"
            " wallet_balances_json, chain, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                _DEP,
                _CYCLE,
                "paper",
                _ts(60),
                2,
                "0",
                str(cash_final),
                "0",
                str(cash_final),
                "HIGH",
                "[]",
                "{}",
                _wallet_balances(eth_final),
                _CHAIN,
                _ts(60),
            ),
        )

        conn.execute(
            "INSERT INTO portfolio_metrics (deployment_id, initial_value_usd, initial_timestamp,"
            " gas_spent_usd, total_value_usd, cycle_id, execution_mode, is_complete, updated_at)"
            " VALUES (?,?,?,?,?,?,?,1,?)",
            (_DEP, str(cash_initial), _ts(0), str(_GAS_USD), str(cash_final), _CYCLE, "paper", _ts(60)),
        )
        conn.commit()
    finally:
        conn.close()


def _g6(report) -> tuple[str, dict]:
    for c in report.cells:
        if c.cell_id == "G6":
            return c.status, c.decomposition
    raise AssertionError("G6 cell not found")


def test_gas_appears_once_on_each_side_g6_reconciles(tmp_path: Path) -> None:
    """Production: native-drain in equity ≡ component sum_gas ⇒ gap 0, no double-count."""
    db = tmp_path / "g6_gas.sqlite"
    _build_db(db, native_drain_reflected=True)
    status, decomp = _g6(run_against_sqlite(db, primitive="looping"))

    # Wallet side carries exactly −Σ_gas (the native ETH bag dropped by $1.50).
    assert Decimal(decomp["wallet_pnl_usd"]) == -_GAS_USD
    # Component side subtracts the SAME Σ_gas (decomposition reports it as −sum_gas).
    assert Decimal(decomp["Σ_gas_usd"]) == -_GAS_USD
    # A flat swap contributes no realized PnL, so component_pnl is purely −Σ_gas.
    assert Decimal(decomp["component_pnl_usd"]) == -_GAS_USD
    # Both sides moved by the same −Σ_gas ⇒ the gap collapses ⇒ G6 PASS.
    assert Decimal(decomp["gap_usd"]) <= Decimal(decomp["ε_threshold_usd"])
    assert status == "PASS", f"expected PASS, got {status}: {decomp}"


def test_missing_native_drain_reopens_the_exact_gas_gap(tmp_path: Path) -> None:
    """Asymmetric (the ticket's bug): native drain NOT in equity ⇒ gap ≈ Σ_gas.

    Pins that the native-gas balance in ``wallet_balances`` is load-bearing for
    G6: with the native ETH bag held FLAT across both endpoints, the wallet side
    shows ``wallet_pnl = 0`` while the component side still subtracts ``sum_gas``,
    leaving the exact ``Σ_gas`` gap VIB-4256 originally reported. This is the
    failure the production native-in-equity path prevents.
    """
    db = tmp_path / "g6_gas_asym.sqlite"
    _build_db(db, native_drain_reflected=False)
    status, decomp = _g6(run_against_sqlite(db, primitive="looping"))

    # Wallet side is FLAT — the gas drain never reached equity.
    assert Decimal(decomp["wallet_pnl_usd"]) == Decimal("0")
    # Component side still subtracts Σ_gas — so the two sides disagree by Σ_gas.
    assert Decimal(decomp["component_pnl_usd"]) == -_GAS_USD
    assert Decimal(decomp["gap_usd"]) == _GAS_USD
    assert Decimal(decomp["gap_usd"]) > Decimal(decomp["ε_threshold_usd"])
    assert status == "FAIL", f"expected FAIL (asymmetric), got {status}: {decomp}"
