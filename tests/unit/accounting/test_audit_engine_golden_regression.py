"""Golden-output regression test for the consolidated audit engine (VIB-4228).

Anchors the engine output to four byte-for-byte golden files (one per primitive)
PLUS independent assertions for Empty ≠ Zero, the AST static checks, and CLI
back-compat. The fixture DBs are built on-disk under each test's ``tmp_path``
fixture using the live SDK schema (``SQLiteStore.initialize()`` →
``_create_schema()``) so a column rename in ``almanak/framework/state/backends/sqlite.py``
breaks this test instead of silently shadowing accounting rows.

Pass ``UAT_UPDATE_GOLDENS=1`` to regenerate every golden file in-place. Without
the flag the test compares byte-for-byte; a 1-character drift fails fast.

Card: ``docs/internal/uat-cards/VIB-4228.md`` (frozen at SHA
``15bb0264ebddf8c87b69449033dceb251a0149b7`` — Phase 1 ``SPEC_OK`` pass 4).
"""

from __future__ import annotations

import ast
import asyncio
import importlib
import io
import os
import sqlite3
import subprocess
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pytest

GOLDEN_DIR = Path(__file__).parent / "golden"
REPO_ROOT = Path(__file__).resolve().parents[3]
ENGINE_DIR = REPO_ROOT / "strategies" / "accounting"


# ─── Live schema bootstrap ────────────────────────────────────────────────


def _bootstrap_live_schema(db_path: str) -> None:
    """Drive the live SDK schema-creation path.

    ``SQLiteStore.initialize()`` (sqlite.py:599) calls ``_create_schema()``
    (sqlite.py:673) which ``executescript``s the module-level ``SCHEMA_SQL``
    constant. Using the SDK's own bootstrap means a column rename anywhere
    under ``almanak/framework/state/backends/sqlite.py`` propagates here
    automatically — no hand-rolled DDL to drift.
    """
    from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

    async def _init() -> None:
        store = SQLiteStore(SQLiteConfig(db_path=db_path))
        await store.initialize()
        await store.close()

    asyncio.run(_init())


# ─── Fixture builders (one per primitive) ─────────────────────────────────
#
# Each builder inserts a small, deterministic set of rows. Every numeric
# column is exercised across THREE states so the Empty ≠ Zero contract is
# pinned by paired assertions:
#
#   * ``None`` / ``""``      → unmeasured; renders as em-dash ``"—"``.
#   * ``"0"``                → measured zero; renders as ``"$0.0000"``.
#   * ``"<garbage>"``        → unparseable; renders as em-dash ``"—"``.
#
# Rows are NOT randomised. Timestamps are fixed strings so the golden
# files are deterministic across machines / Python versions / locales.


def _build_lp_fixture(tmp_path: Path) -> Path:
    db_path = tmp_path / "lp.db"
    _bootstrap_live_schema(str(db_path))
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # portfolio_snapshots: first (None totals) + mid (measured-zero) + final
    # (measured non-zero). The first / final pair drives Q5; the mid row
    # tests the measured-zero negative control.
    cur.executemany(
        "INSERT INTO portfolio_snapshots (strategy_id, deployment_id, cycle_id, "
        "execution_mode, timestamp, iteration_number, total_value_usd, "
        "available_cash_usd, deployed_capital_usd, wallet_total_value_usd, "
        "value_confidence, positions_json, token_prices_json, "
        "wallet_balances_json, chain, created_at) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "AccountingQuantLPStrategy:lp-test",
                "AccountingQuantLPStrategy:lp-test",
                "cyc-1",
                "live",
                "2026-05-10T00:00:00",
                0,
                "",  # NOT NULL — empty string = "parser didn't emit", _show()→"—"
                "",  # NOT NULL — same Empty≠Zero pin
                None,  # nullable
                "0",
                "HIGH",
                "[]",
                "{}",
                '[{"symbol":"USDC","balance":"100"}]',
                "arbitrum",
                "2026-05-10T00:00:00",
            ),
            (
                "AccountingQuantLPStrategy:lp-test",
                "AccountingQuantLPStrategy:lp-test",
                "cyc-2",
                "live",
                "2026-05-10T00:00:30",
                1,
                "0",
                "0",
                "0",
                "0",
                "HIGH",
                "[]",
                "{}",
                "[]",
                "arbitrum",
                "2026-05-10T00:00:30",
            ),
            (
                "AccountingQuantLPStrategy:lp-test",
                "AccountingQuantLPStrategy:lp-test",
                "cyc-3",
                "live",
                "2026-05-10T00:01:00",
                2,
                "100.5000",
                "10.0000",
                "90.5000",
                "100.5000",
                "HIGH",
                "[]",
                "{}",
                '[{"symbol":"USDC","balance":"50"},{"symbol":"WETH","balance":"0.02"}]',
                "arbitrum",
                "2026-05-10T00:01:00",
            ),
        ],
    )

    # transaction_ledger: SWAP + LP_OPEN + LP_CLOSE. Mix of None / "0" /
    # measured / garbage gas_usd to pin Empty ≠ Zero rendering.
    cur.executemany(
        "INSERT INTO transaction_ledger (id, cycle_id, strategy_id, deployment_id, "
        "execution_mode, timestamp, intent_type, token_in, amount_in, token_out, "
        "amount_out, effective_price, slippage_bps, gas_used, gas_usd, tx_hash, "
        "chain, protocol, success, error) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "tx-1",
                "cyc-1",
                "AccountingQuantLPStrategy:lp-test",
                "AccountingQuantLPStrategy:lp-test",
                "live",
                "2026-05-10T00:00:10",
                "SWAP",
                "USDC",
                "50.0",
                "WETH",
                "0.02",
                "2500.0",
                5.0,
                120000,
                "0.50",
                "0xabc123def456789",
                "arbitrum",
                "uniswap_v3",
                1,
                None,
            ),
            (
                "tx-2",
                "cyc-1",
                "AccountingQuantLPStrategy:lp-test",
                "AccountingQuantLPStrategy:lp-test",
                "live",
                "2026-05-10T00:00:20",
                "LP_OPEN",
                "USDC",
                "50.0",
                "LP-NFT",
                "1",
                None,
                None,
                250000,
                None,
                "0xdef789abc123456",
                "arbitrum",
                "uniswap_v3",
                1,
                None,
            ),
            (
                "tx-3",
                "cyc-3",
                "AccountingQuantLPStrategy:lp-test",
                "AccountingQuantLPStrategy:lp-test",
                "live",
                "2026-05-10T00:01:00",
                "LP_CLOSE",
                "LP-NFT",
                "1",
                "USDC",
                "51.0",
                None,
                None,
                180000,
                "0",
                "0xfeebee0123456789",
                "arbitrum",
                "uniswap_v3",
                1,
                None,
            ),
        ],
    )

    # position_events: OPEN + CLOSE for one LP position. Q6 reads CLOSE
    # rows for fees_token0/fees_token1.
    cur.executemany(
        "INSERT INTO position_events (id, deployment_id, cycle_id, execution_mode, "
        "position_id, position_type, event_type, timestamp, protocol, chain, "
        "token0, token1, amount0, amount1, value_usd, tick_lower, tick_upper, "
        "liquidity, in_range, fees_token0, fees_token1) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "pe-1",
                "AccountingQuantLPStrategy:lp-test",
                "cyc-1",
                "live",
                "lp-pos-1",
                "LP",
                "OPEN",
                "2026-05-10T00:00:20",
                "uniswap_v3",
                "arbitrum",
                "USDC",
                "WETH",
                "50.0",
                "0.02",
                "100.0",
                -887220,
                887220,
                "1234567890",
                1,
                None,
                None,
            ),
            (
                "pe-2",
                "AccountingQuantLPStrategy:lp-test",
                "cyc-3",
                "live",
                "lp-pos-1",
                "LP",
                "CLOSE",
                "2026-05-10T00:01:00",
                "uniswap_v3",
                "arbitrum",
                "USDC",
                "WETH",
                "51.0",
                "0.0",
                "51.0",
                -887220,
                887220,
                "0",
                1,
                "0.05",
                "0.000001",
            ),
        ],
    )

    # portfolio_metrics: one row with mixed None / "0" / measured fields.
    cur.execute(
        "INSERT INTO portfolio_metrics (strategy_id, initial_value_usd, "
        "initial_timestamp, deposits_usd, withdrawals_usd, gas_spent_usd, "
        "total_value_usd, positions_json, deployment_id, execution_mode, "
        "is_complete, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "AccountingQuantLPStrategy:lp-test",
            "100.0000",
            "2026-05-10T00:00:00",
            "0",
            None,
            "0.50",
            "100.5000",
            "[]",
            "AccountingQuantLPStrategy:lp-test",
            "live",
            1,
            "2026-05-10T00:01:00",
        ),
    )

    # accounting_events: one Layer-5 row.
    cur.execute(
        "INSERT INTO accounting_events (id, deployment_id, strategy_id, cycle_id, "
        "execution_mode, timestamp, chain, protocol, wallet_address, event_type, "
        "position_key, ledger_entry_id, tx_hash, confidence, payload_json, "
        "schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "ae-1",
            "AccountingQuantLPStrategy:lp-test",
            "AccountingQuantLPStrategy:lp-test",
            "cyc-1",
            "live",
            "2026-05-10T00:00:20",
            "arbitrum",
            "uniswap_v3",
            "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
            "LP_OPEN",
            "uniswap_v3:lp-pos-1",
            "tx-2",
            "0xdef789abc123456",
            "HIGH",
            "{}",
            1,
        ),
    )

    # accounting_outbox: one processed row + one pending row.
    cur.executemany(
        "INSERT INTO accounting_outbox (id, deployment_id, strategy_id, cycle_id, "
        "ledger_entry_id, intent_type, status, attempts, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "ob-1",
                "AccountingQuantLPStrategy:lp-test",
                "AccountingQuantLPStrategy:lp-test",
                "cyc-1",
                "tx-2",
                "LP_OPEN",
                "processed",
                1,
                "2026-05-10T00:00:20",
                "2026-05-10T00:00:21",
            ),
            (
                "ob-2",
                "AccountingQuantLPStrategy:lp-test",
                "AccountingQuantLPStrategy:lp-test",
                "cyc-3",
                "tx-3",
                "LP_CLOSE",
                "pending",
                0,
                "2026-05-10T00:01:00",
                "2026-05-10T00:01:00",
            ),
        ],
    )

    conn.commit()
    conn.close()
    return db_path


def _build_looping_fixture(tmp_path: Path) -> Path:
    db_path = tmp_path / "looping.db"
    _bootstrap_live_schema(str(db_path))
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.executemany(
        "INSERT INTO portfolio_snapshots (strategy_id, deployment_id, cycle_id, "
        "execution_mode, timestamp, iteration_number, total_value_usd, "
        "available_cash_usd, deployed_capital_usd, wallet_total_value_usd, "
        "value_confidence, positions_json, token_prices_json, "
        "wallet_balances_json, chain, created_at) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "AccountingQuantLoopingStrategy:loop-test",
                "AccountingQuantLoopingStrategy:loop-test",
                "cyc-1",
                "live",
                "2026-05-10T00:00:00",
                0,
                "",
                "",
                None,
                "0",
                "HIGH",
                "[]",
                "{}",
                '[{"symbol":"USDC","balance":"1000"}]',
                "arbitrum",
                "2026-05-10T00:00:00",
            ),
            (
                "AccountingQuantLoopingStrategy:loop-test",
                "AccountingQuantLoopingStrategy:loop-test",
                "cyc-2",
                "live",
                "2026-05-10T00:01:00",
                1,
                "1000.0000",
                "100.0000",
                "900.0000",
                "1000.0000",
                "HIGH",
                "[]",
                "{}",
                '[{"symbol":"USDC","balance":"100"}]',
                "arbitrum",
                "2026-05-10T00:01:00",
            ),
        ],
    )

    cur.executemany(
        "INSERT INTO transaction_ledger (id, cycle_id, strategy_id, deployment_id, "
        "execution_mode, timestamp, intent_type, token_in, amount_in, token_out, "
        "amount_out, effective_price, slippage_bps, gas_used, gas_usd, tx_hash, "
        "chain, protocol, success, error) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "tx-1",
                "cyc-1",
                "AccountingQuantLoopingStrategy:loop-test",
                "AccountingQuantLoopingStrategy:loop-test",
                "live",
                "2026-05-10T00:00:10",
                "SUPPLY",
                "USDC",
                "900.0",
                None,
                None,
                None,
                None,
                200000,
                "0.30",
                "0xa1",
                "arbitrum",
                "aave_v3",
                1,
                None,
            ),
            (
                "tx-2",
                "cyc-1",
                "AccountingQuantLoopingStrategy:loop-test",
                "AccountingQuantLoopingStrategy:loop-test",
                "live",
                "2026-05-10T00:00:20",
                "BORROW",
                "USDT",
                "300.0",
                None,
                None,
                None,
                None,
                180000,
                None,
                "0xa2",
                "arbitrum",
                "aave_v3",
                1,
                None,
            ),
            (
                "tx-3",
                "cyc-2",
                "AccountingQuantLoopingStrategy:loop-test",
                "AccountingQuantLoopingStrategy:loop-test",
                "live",
                "2026-05-10T00:00:30",
                "SWAP",
                "USDT",
                "300.0",
                "USDC",
                "299.5",
                "0.998333",
                5.0,
                120000,
                "0.20",
                "0xa3",
                "arbitrum",
                "uniswap_v3",
                1,
                None,
            ),
        ],
    )

    cur.execute(
        "INSERT INTO position_events (id, deployment_id, cycle_id, execution_mode, "
        "position_id, position_type, event_type, timestamp, protocol, chain, "
        "token0, token1, amount0, amount1, value_usd) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "pe-1",
            "AccountingQuantLoopingStrategy:loop-test",
            "cyc-1",
            "live",
            "aave-pos-1",
            "LP",
            "OPEN",
            "2026-05-10T00:00:10",
            "aave_v3",
            "arbitrum",
            "USDC",
            "USDT",
            "900.0",
            "300.0",
            None,
        ),
    )

    cur.execute(
        "INSERT INTO portfolio_metrics (strategy_id, initial_value_usd, "
        "initial_timestamp, deposits_usd, withdrawals_usd, gas_spent_usd, "
        "total_value_usd, positions_json, deployment_id, execution_mode, "
        "is_complete, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "AccountingQuantLoopingStrategy:loop-test",
            "1000.0000",
            "2026-05-10T00:00:00",
            "0",
            None,
            None,
            "1000.0000",
            "[]",
            "AccountingQuantLoopingStrategy:loop-test",
            "live",
            0,
            "2026-05-10T00:01:00",
        ),
    )

    cur.execute(
        "INSERT INTO accounting_events (id, deployment_id, strategy_id, cycle_id, "
        "execution_mode, timestamp, chain, protocol, wallet_address, event_type, "
        "position_key, confidence, payload_json, schema_version) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "ae-1",
            "AccountingQuantLoopingStrategy:loop-test",
            "AccountingQuantLoopingStrategy:loop-test",
            "cyc-1",
            "live",
            "2026-05-10T00:00:10",
            "arbitrum",
            "aave_v3",
            "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
            "SUPPLY",
            "aave_v3:USDC",
            "HIGH",
            "{}",
            1,
        ),
    )

    cur.execute(
        "INSERT INTO accounting_outbox (id, deployment_id, strategy_id, cycle_id, "
        "ledger_entry_id, intent_type, status, attempts, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "ob-1",
            "AccountingQuantLoopingStrategy:loop-test",
            "AccountingQuantLoopingStrategy:loop-test",
            "cyc-1",
            "tx-1",
            "SUPPLY",
            "processed",
            1,
            "2026-05-10T00:00:10",
            "2026-05-10T00:00:11",
        ),
    )

    conn.commit()
    conn.close()
    return db_path


def _build_perp_fixture(tmp_path: Path) -> Path:
    db_path = tmp_path / "perp.db"
    _bootstrap_live_schema(str(db_path))
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Perp historically reads ``amount`` from wallet_balances_json instead of
    # ``balance`` (perp/audit.py:85 quirk preserved per VIB-4228 D3.5). The
    # fixture sets ``amount`` so perp reads the value, AND ``balance`` so the
    # value rendered for lp/looping/ta would have been ``"100"`` if they read
    # this row — proving perp's quirk doesn't accidentally read ``balance``.
    cur.execute(
        "INSERT INTO portfolio_snapshots (strategy_id, deployment_id, cycle_id, "
        "execution_mode, timestamp, iteration_number, total_value_usd, "
        "available_cash_usd, deployed_capital_usd, wallet_total_value_usd, "
        "value_confidence, positions_json, token_prices_json, "
        "wallet_balances_json, chain, created_at) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "AccountingQuantPerpStrategy:perp-test",
            "AccountingQuantPerpStrategy:perp-test",
            "cyc-1",
            "live",
            "2026-05-10T00:00:00",
            0,
            "5000.0000",
            "5000.0000",
            "0",
            "5000.0000",
            "HIGH",
            "[]",
            "{}",
            '[{"symbol":"USDC","amount":"5000","balance":"100"}]',
            "arbitrum",
            "2026-05-10T00:00:00",
        ),
    )

    cur.execute(
        "INSERT INTO transaction_ledger (id, cycle_id, strategy_id, deployment_id, "
        "execution_mode, timestamp, intent_type, token_in, amount_in, token_out, "
        "amount_out, effective_price, slippage_bps, gas_used, gas_usd, tx_hash, "
        "chain, protocol, success, error) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "tx-1",
            "cyc-1",
            "AccountingQuantPerpStrategy:perp-test",
            "AccountingQuantPerpStrategy:perp-test",
            "live",
            "2026-05-10T00:00:10",
            "PERP_OPEN",
            "USDC",
            "1000.0",
            "BTC-PERP",
            "0.01",
            "100000.0",
            10.0,
            300000,
            "0.80",
            "0xb1",
            "arbitrum",
            "gmx_v2",
            1,
            None,
        ),
    )

    cur.execute(
        "INSERT INTO portfolio_metrics (strategy_id, initial_value_usd, "
        "initial_timestamp, deposits_usd, withdrawals_usd, gas_spent_usd, "
        "total_value_usd, positions_json, deployment_id, execution_mode, "
        "is_complete, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "AccountingQuantPerpStrategy:perp-test",
            "5000.0000",
            "2026-05-10T00:00:00",
            "0",
            None,
            "0.80",
            "5000.0000",
            "[]",
            "AccountingQuantPerpStrategy:perp-test",
            "live",
            0,
            "2026-05-10T00:00:10",
        ),
    )

    cur.execute(
        "INSERT INTO accounting_outbox (id, deployment_id, strategy_id, cycle_id, "
        "ledger_entry_id, intent_type, status, attempts, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "ob-1",
            "AccountingQuantPerpStrategy:perp-test",
            "AccountingQuantPerpStrategy:perp-test",
            "cyc-1",
            "tx-1",
            "PERP_OPEN",
            "pending",
            0,
            "2026-05-10T00:00:10",
            "2026-05-10T00:00:10",
        ),
    )

    conn.commit()
    conn.close()
    return db_path


def _build_ta_fixture(tmp_path: Path) -> Path:
    db_path = tmp_path / "ta.db"
    _bootstrap_live_schema(str(db_path))
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # TA filters by strategy_id. Two distinct ids exercise the
    # ``_latest_strategy_id`` auto-discover path (max-timestamp wins) AND the
    # explicit ``--strategy-id`` filter (D2.3). Auto-discover picks id "newer"
    # because its rows have the latest timestamps.
    #
    # The "newer" strategy_id has three rows exercising all three Empty≠Zero
    # states for gas_usd (None / "0" / measured), so D3.4's auto-discover-path
    # paired control finds all three substrings without needing --strategy-id.
    rows = [
        (
            "tx-A1",
            "cyc-A1",
            "AccountingQuantTAStrategy:older",
            "2026-05-10T00:00:00",
            "SWAP",
            "USDC",
            "100.0",
            "WETH",
            "0.04",
            None,
            None,
            "0xta-old",
        ),
        (
            "tx-B1",
            "cyc-B1",
            "AccountingQuantTAStrategy:newer",
            "2026-05-10T00:01:00",
            "SWAP",
            "USDC",
            "200.0",
            "WETH",
            "0.08",
            None,
            200000,
            "0xta-new-1",
        ),  # gas_usd=None → renders 'gas=—'
        (
            "tx-B2",
            "cyc-B2",
            "AccountingQuantTAStrategy:newer",
            "2026-05-10T00:02:00",
            "SWAP",
            "WETH",
            "0.08",
            "USDC",
            "200.5",
            "0",
            180000,
            "0xta-new-2",
        ),  # gas_usd="0" → renders 'gas=$0.00000000'
        (
            "tx-B3",
            "cyc-B3",
            "AccountingQuantTAStrategy:newer",
            "2026-05-10T00:03:00",
            "SWAP",
            "USDC",
            "200.5",
            "WETH",
            "0.0805",
            "0.40",
            120000,
            "0xta-new-3",
        ),  # measured → 'gas=$0.40000000'
    ]
    for r in rows:
        cur.execute(
            "INSERT INTO transaction_ledger (id, cycle_id, strategy_id, "
            "deployment_id, execution_mode, timestamp, intent_type, token_in, "
            "amount_in, token_out, amount_out, gas_usd, gas_used, tx_hash, "
            "extracted_data_json, success, error) VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (r[0], r[1], r[2], r[2], "live", r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], "{}", 1, None),
        )

    conn.commit()
    conn.close()
    return db_path


FIXTURE_BUILDERS = {
    "lp": _build_lp_fixture,
    "looping": _build_looping_fixture,
    "perp": _build_perp_fixture,
    "ta": _build_ta_fixture,
}


# ─── Shared helpers ───────────────────────────────────────────────────────


def _run_prose_main(primitive: str, argv: list[str]) -> tuple[int, str]:
    """Invoke the prose module's ``main(argv)`` and capture stdout."""
    module = importlib.import_module(f"strategies.accounting._prose.{primitive}")
    importlib.reload(module)  # keep state isolated across parametrised runs
    captured = io.StringIO()
    with redirect_stdout(captured):
        rc = module.main(argv)
    return rc, captured.getvalue()


def _golden_path(primitive: str) -> Path:
    return GOLDEN_DIR / f"audit_{primitive}.txt"


# ─── D1.1 — golden byte-equality, four primitives ─────────────────────────


@pytest.mark.parametrize("primitive", ["lp", "looping", "perp", "ta"])
def test_d1_1_golden_byte_equality(primitive: str, tmp_path: Path) -> None:
    db_path = FIXTURE_BUILDERS[primitive](tmp_path)
    rc, captured = _run_prose_main(primitive, [str(db_path)])
    assert rc == 0, f"{primitive}: prose main returned {rc}"

    # The DB path itself appears in the captured output. Replace with a
    # placeholder so the golden file is portable across machines.
    sanitised = captured.replace(str(db_path), f"<TMPDIR>/{primitive}.db")
    golden = _golden_path(primitive)

    if os.environ.get("UAT_UPDATE_GOLDENS"):
        golden.parent.mkdir(parents=True, exist_ok=True)
        golden.write_text(sanitised)
        pytest.skip(f"UAT_UPDATE_GOLDENS=1 set — wrote {golden}; re-run without flag")

    assert golden.is_file(), f"Golden file missing at {golden}. Run with UAT_UPDATE_GOLDENS=1 to regenerate."
    assert sanitised == golden.read_text(), (
        f"Golden drift for {primitive}. Expected vs actual differ; first differing line indicates the regression."
    )


# ─── D1.2 — CLI back-compat shim (subprocess) ─────────────────────────────


@pytest.mark.parametrize("primitive", ["lp", "looping", "perp", "ta"])
def test_d1_2_cli_shim_byte_equality(primitive: str, tmp_path: Path) -> None:
    shim_path = ENGINE_DIR / primitive / "audit.py"
    assert shim_path.is_file(), (
        f"Shim not present at {shim_path}. The PR must commit a 5-line back-compat shim alongside the prose module."
    )
    db_path = FIXTURE_BUILDERS[primitive](tmp_path)

    proc = subprocess.run(
        [sys.executable, str(shim_path), str(db_path)],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        check=False,
    )
    assert proc.returncode == 0, f"{primitive} shim CLI returned {proc.returncode}; stderr: {proc.stderr}"
    sanitised = proc.stdout.replace(str(db_path), f"<TMPDIR>/{primitive}.db")
    assert sanitised == _golden_path(primitive).read_text(), (
        f"{primitive} CLI shim output drifted from golden file. "
        f"Subprocess form must equal in-process prose.main([db_path])."
    )


# ─── D1.3 — TA ``--strategy-id`` filter ──────────────────────────────────


def test_d1_3_ta_strategy_id_filter(tmp_path: Path) -> None:
    """Each `--strategy-id` selects exactly its own ledger rows by tx_hash."""
    db_path = _build_ta_fixture(tmp_path)
    rc_a, captured_a = _run_prose_main("ta", [str(db_path), "--strategy-id", "AccountingQuantTAStrategy:older"])
    rc_b, captured_b = _run_prose_main("ta", [str(db_path), "--strategy-id", "AccountingQuantTAStrategy:newer"])
    assert rc_a == 0 and rc_b == 0
    older_only = "0xta-old"
    newer_hashes = ("0xta-new-1", "0xta-new-2", "0xta-new-3")
    assert older_only in captured_a, "older strategy_id must select its own row"
    assert all(h not in captured_a for h in newer_hashes), "older strategy_id must NOT leak rows from newer"
    assert all(h in captured_b for h in newer_hashes)
    assert older_only not in captured_b


def test_d2_3_ta_auto_discover_picks_latest_strategy(tmp_path: Path) -> None:
    """`_latest_strategy_id` orders by MAX(timestamp) DESC; "newer" wins."""
    db_path = _build_ta_fixture(tmp_path)
    rc, captured = _run_prose_main("ta", [str(db_path)])  # no --strategy-id
    assert rc == 0
    assert "newer" in captured, "auto-discover must select 'newer' strategy_id"
    assert "0xta-old" not in captured, "auto-discover must NOT print rows from the older strategy_id"


# ─── D1.4 — AST static checks ─────────────────────────────────────────────


def _audit_files() -> list[Path]:
    return [
        ENGINE_DIR / "_audit_engine.py",
        *sorted((ENGINE_DIR / "_prose").glob("*.py")),
        *(ENGINE_DIR / sub / "audit.py" for sub in ("lp", "looping", "ta", "perp")),
    ]


class _NoBigDInFstrings(ast.NodeVisitor):
    """Fail if any ``f"…{_D(x):...}…"`` exists in the audit code."""

    def __init__(self) -> None:
        self.violations: list[tuple[str, int]] = []

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        for part in node.values:
            if isinstance(part, ast.FormattedValue):
                value = part.value
                if isinstance(value, ast.Call) and isinstance(value.func, ast.Name):
                    if value.func.id == "_D":
                        self.violations.append(("_D in f-string", node.lineno))
        self.generic_visit(node)


class _NoLowercaseDAnywhere(ast.NodeVisitor):
    """Fail if `_d` (lowercase) appears as a Name, FunctionDef, or import alias."""

    def __init__(self) -> None:
        self.violations: list[tuple[str, int]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        if node.name == "_d":
            self.violations.append(("_d FunctionDef", node.lineno))
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id == "_d":
            self.violations.append(("_d Name", node.lineno))

    def visit_alias(self, node: ast.alias) -> None:
        if node.name == "_d" or node.asname == "_d":
            self.violations.append(("_d import alias", node.lineno))


def test_d1_4_no_d_lowercase_anywhere() -> None:
    """`_d` (lowercase, the Empty≠Zero violator) must not appear post-fix."""
    visitor = _NoLowercaseDAnywhere()
    for path in _audit_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        visitor.visit(tree)
    assert not visitor.violations, f"_d (lowercase) found post-consolidation: {visitor.violations}"


def test_d1_4_no_big_d_in_fstring() -> None:
    """`_D(...)` must not be routed into an f-string (display path → use _show)."""
    visitor = _NoBigDInFstrings()
    for path in _audit_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        visitor.visit(tree)
    assert not visitor.violations, f"_D routed into f-string post-consolidation: {visitor.violations}"


def test_d1_4_show_returns_emdash_for_empty() -> None:
    """`_show()` body must contain `return "—"` reachable for None/"" input."""
    src = (ENGINE_DIR / "_audit_engine.py").read_text()
    tree = ast.parse(src)
    show_func = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_show")
    body_src = ast.unparse(show_func)
    assert "return '—'" in body_src or 'return "—"' in body_src, (
        "_show() must return em-dash U+2014 for unmeasured input"
    )


# ─── D3 — silent-failure pinning ──────────────────────────────────────────


@pytest.mark.parametrize("primitive", ["lp", "looping", "perp", "ta"])
def test_d3_1_missing_db(primitive: str, tmp_path: Path) -> None:
    """F1: missing DB → rc=1 with ERROR: prefix."""
    missing = tmp_path / "does-not-exist.db"
    rc, captured = _run_prose_main(primitive, [str(missing)])
    assert rc == 1, f"{primitive}: missing-DB rc must be 1; got {rc}"
    assert captured.startswith("ERROR: DB not found at "), (
        f"{primitive}: missing-DB output must start with 'ERROR:' (got: {captured!r})"
    )


@pytest.mark.parametrize("primitive", ["lp", "looping", "perp"])
def test_d3_2_empty_db_full_grid(primitive: str, tmp_path: Path) -> None:
    """F2 for full-grid primitives: empty DB → rc=0 with all section headers."""
    db_path = tmp_path / f"{primitive}_empty.db"
    _bootstrap_live_schema(str(db_path))
    rc, captured = _run_prose_main(primitive, [str(db_path)])
    assert rc == 0
    # Every section that prints a header MUST print it even on empty DB.
    assert "=== Q5 — Wallet snapshots (n=0) ===" in captured
    assert "=== Q1 — Money flow (transaction_ledger n=0) ===" in captured
    assert "=== Q3 — Gas per TX ===" in captured
    assert "=== Q7 — Portfolio metrics (n=0) ===" in captured


def test_d3_2_empty_db_ta(tmp_path: Path) -> None:
    """F2 for TA: empty DB → rc=0 + 'No transaction_ledger rows found.'"""
    db_path = tmp_path / "ta_empty.db"
    _bootstrap_live_schema(str(db_path))
    rc, captured = _run_prose_main("ta", [str(db_path)])
    assert rc == 0
    assert "No transaction_ledger rows found." in captured


def test_d3_3_garbage_decimal_renders_emdash(tmp_path: Path) -> None:
    """F3: a row with ``gas_usd='not-a-decimal'`` renders ``cost=—``."""
    db_path = tmp_path / "lp_garbage.db"
    _bootstrap_live_schema(str(db_path))
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO transaction_ledger (id, cycle_id, strategy_id, "
        "deployment_id, execution_mode, timestamp, intent_type, gas_usd, "
        "gas_used, success) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "tx-garbage",
            "cyc-x",
            "AccountingQuantLPStrategy:garbage",
            "AccountingQuantLPStrategy:garbage",
            "live",
            "2026-05-10T00:00:00",
            "SWAP",
            "not-a-decimal",
            100000,
            1,
        ),
    )
    conn.commit()
    conn.close()
    rc, captured = _run_prose_main("lp", [str(db_path)])
    assert rc == 0
    # Per-row gas line in Q3 renders the garbage value as em-dash.
    assert "gas=100000  cost=—" in captured, "garbage gas_usd must render as em-dash, not as $0.0000 or a stack trace"


def test_d3_4_empty_vs_zero_paired_lp(tmp_path: Path) -> None:
    """F6 paired control for LP: Q3 has all three Empty≠Zero states."""
    db_path = _build_lp_fixture(tmp_path)
    _, captured = _run_prose_main("lp", [str(db_path)])
    # Positive control: first snapshot has total_value_usd="" → 'total=—'.
    assert "total=—" in captured, "LP unmeasured snapshot must render 'total=—'"
    # Q3 prints all three states for gas_usd (one per ledger row):
    # measured non-zero, measured zero, unmeasured. All three substrings must
    # be present so a fix that flattens any pair of these to a single output
    # fails the test.
    assert "cost=$0.5000" in captured, "measured non-zero must render '$0.5000'"
    assert "cost=$0.0000" in captured, "measured zero must render '$0.0000' — _show() must NOT flatten 0 to '—'"
    assert "cost=—" in captured, "unmeasured must render '—' — _show() must NOT print '$0.0000' for None"


def test_d3_4_empty_vs_zero_paired_ta(tmp_path: Path) -> None:
    """F6 paired control for TA: None → '—', '0' → '$0.00000000'."""
    db_path = _build_ta_fixture(tmp_path)
    _, captured = _run_prose_main("ta", [str(db_path)])
    assert "gas=—" in captured, "TA None gas_usd row must render 'gas=—'"
    assert "gas=$0.00000000" in captured, "TA measured-zero gas_usd row must render 'gas=$0.00000000', not '—'"


@pytest.mark.parametrize(
    ("primitive", "expected"),
    [
        # Python's tuple repr uses single quotes by default; the engine prints
        # the list-of-tuples directly via str(), so the assertions match the
        # `('USDC', '<value>')` form, not double-quoted JSON-style.
        ("lp", "('USDC', '100')"),
        ("looping", "('USDC', '1000')"),
        ("ta", "newer"),  # TA fixture has no wallet section; sanity-check only
        ("perp", "('USDC', '5000')"),
    ],
)
def test_d3_5_wallet_field_quirk_preserved(primitive: str, expected: str, tmp_path: Path) -> None:
    """F4: lp/looping/ta read 'balance'; perp reads 'amount'. Quirk preserved."""
    db_path = FIXTURE_BUILDERS[primitive](tmp_path)
    _, captured = _run_prose_main(primitive, [str(db_path)])
    assert expected in captured, (
        f"{primitive}: expected wallet token reading {expected!r} "
        f"(perp uses 'amount' field per known quirk; lp/looping/ta use 'balance')"
    )


def test_d3_6_corrupt_db_raises_with_error_prefix(tmp_path: Path) -> None:
    """F5: non-SQLite file → rc != 0 AND first stdout line starts with 'ERROR:'."""
    bad_db = tmp_path / "garbage.db"
    bad_db.write_text("this is not a sqlite database file at all")
    rc, captured = _run_prose_main("lp", [str(bad_db)])
    assert rc != 0, "corrupted DB must produce non-zero rc"
    first_line = captured.splitlines()[0] if captured else ""
    assert first_line.startswith("ERROR:"), f"first line must start with 'ERROR:'; got {first_line!r}"


# ─── D2.2 — line-count budget ─────────────────────────────────────────────


def test_d2_2_loc_budget() -> None:
    """Per Spec Drift §1: total ≤ 600 LOC across engine + prose + shims."""
    total = sum(len(p.read_text().splitlines()) for p in _audit_files())
    assert total <= 600, (
        f"audit-engine LOC budget exceeded: {total} > 600. See docs/internal/uat-cards/VIB-4228.md §Spec Drift §1."
    )
