"""CLI integration tests for VIB-4907 headline suppression.

Drives ``strat pnl`` against a synthesized SQLite DB that mirrors the
canonical RSI mainnet pattern: two byte-identical snapshots bracketing a
successful SWAP, with the latter snapshot's ``cycle_id`` starting with
``teardown-``.  The text + JSON outputs are then asserted to verify the
suppression renders without breaking deterministic components.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak.framework.cli.strat_pnl import strat_pnl
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.portfolio.models import (
    PortfolioMetrics,
    PortfolioSnapshot,
    TokenBalance,
    ValueConfidence,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


_DEPLOYMENT_ID = "uniswap_rsi:abc123def456"
_BASE_TS = datetime(2026, 5, 30, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db", prefix="strat_pnl_suppr_")
    os.close(fd)
    yield Path(path)
    for ext in ("", "-wal", "-shm", "-journal"):
        try:
            os.unlink(str(path) + ext)
        except FileNotFoundError:
            pass


def _identical_snapshot(cycle_id: str, ts: datetime) -> PortfolioSnapshot:
    """Build a snapshot whose JSON columns are byte-identical across calls.

    The Empty≠Zero discipline matters here: the wallet_balances list and
    token_prices dict are constructed deterministically with the same
    insertion order so the canonical JSON dump matches between calls.
    """
    return PortfolioSnapshot(
        timestamp=ts,
        deployment_id=_DEPLOYMENT_ID,
        # Position-scoped per VIB-3614 — the WETH "pseudo-position" surfaces a
        # non-zero ``total_value_usd`` even though the wallet's real value is
        # in cash; this mirrors the RSI-mainnet shape that triggered F4.
        total_value_usd=Decimal("6.47"),
        available_cash_usd=Decimal("25.80"),
        value_confidence=ValueConfidence.HIGH,
        deployed_capital_usd=Decimal("0"),
        wallet_total_value_usd=Decimal("32.27"),
        wallet_balances=[
            TokenBalance(
                symbol="WETH",
                balance=Decimal("0.001"),
                value_usd=Decimal("6.47"),
                address="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                price_usd=Decimal("6470.0"),
            ),
            TokenBalance(
                symbol="USDC",
                balance=Decimal("25.80"),
                value_usd=Decimal("25.80"),
                address="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                price_usd=Decimal("1.0"),
            ),
        ],
        token_prices={
            "arbitrum:0x82af49447d8a07e3bd95bd0d56f35241523fbab1": {
                "price_usd": "6470.0",
                "symbol": "WETH",
                "decimals": 18,
            },
            "arbitrum:0xaf88d065e77c8cc2239327c5edb3a432268e5831": {
                "price_usd": "1.0",
                "symbol": "USDC",
                "decimals": 6,
            },
        },
        chain="arbitrum",
        iteration_number=0,
        cycle_id=cycle_id,
    )


def _make_swap(ts: datetime, success: bool = True) -> LedgerEntry:
    return LedgerEntry(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=ts,
        intent_type="SWAP",
        token_in="USDC",
        amount_in="10",
        token_out="WETH",
        amount_out="0.0015",
        gas_used=100000,
        gas_usd="0.01",
        chain="arbitrum",
        protocol="uniswap_v3",
        success=success,
    )


def _make_metrics() -> PortfolioMetrics:
    """Metrics that would produce a misleading +$10 headline under the cascade."""
    return PortfolioMetrics(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=_BASE_TS,
        # The bug: initial baseline was short-circuited to WETH-position value,
        # so the lifetime PnL formula reports a +$10 swing that has no
        # on-chain reality.  F4 hides the headline; F3 (cascade) will fix it.
        initial_value_usd=Decimal("6.47"),
        total_value_usd=Decimal("16.47"),
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
        gas_spent_usd=Decimal("0.02"),
    )


async def _seed_fallback_pattern(db_path: Path) -> None:
    """Seed the canonical F4 / VIB-4907 trigger pattern into a fresh DB."""
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()
    try:
        await store.save_portfolio_metrics(_make_metrics())
        # Pre-teardown snapshot — iteration cycle.
        pre = _identical_snapshot(cycle_id="iter-1", ts=_BASE_TS)
        # Successful SWAP between the two snapshots.
        swap = _make_swap(ts=_BASE_TS + timedelta(seconds=30))
        # Post-teardown snapshot — byte-identical JSON columns to ``pre``
        # (the F2 cache-staleness fingerprint) but ``cycle_id`` carries
        # the teardown- prefix that uniquely identifies the bracket.
        post = _identical_snapshot(cycle_id="teardown-xyz789", ts=_BASE_TS + timedelta(seconds=60))

        await store.save_portfolio_snapshot(pre)
        await store.save_ledger_entry(swap)
        await store.save_portfolio_snapshot(post)
    finally:
        await store.close()


async def _seed_no_fallback_pattern(db_path: Path) -> None:
    """Seed two snapshots that legitimately differ — suppression must NOT fire."""
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()
    try:
        await store.save_portfolio_metrics(_make_metrics())
        pre = _identical_snapshot(cycle_id="iter-1", ts=_BASE_TS)
        swap = _make_swap(ts=_BASE_TS + timedelta(seconds=30))
        # Post snapshot differs — wallet balance moved to reflect the SWAP.
        post = PortfolioSnapshot(
            timestamp=_BASE_TS + timedelta(seconds=60),
            deployment_id=_DEPLOYMENT_ID,
            total_value_usd=Decimal("16.47"),
            available_cash_usd=Decimal("15.80"),
            value_confidence=ValueConfidence.HIGH,
            deployed_capital_usd=Decimal("0"),
            wallet_total_value_usd=Decimal("32.27"),
            wallet_balances=[
                TokenBalance(
                    symbol="WETH",
                    balance=Decimal("0.0025"),
                    value_usd=Decimal("16.175"),
                    address="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                    price_usd=Decimal("6470.0"),
                ),
                TokenBalance(
                    symbol="USDC",
                    balance=Decimal("15.80"),
                    value_usd=Decimal("15.80"),
                    address="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                    price_usd=Decimal("1.0"),
                ),
            ],
            token_prices={
                "arbitrum:0x82af49447d8a07e3bd95bd0d56f35241523fbab1": {
                    "price_usd": "6470.0",
                    "symbol": "WETH",
                    "decimals": 18,
                },
                "arbitrum:0xaf88d065e77c8cc2239327c5edb3a432268e5831": {
                    "price_usd": "1.0",
                    "symbol": "USDC",
                    "decimals": 6,
                },
            },
            chain="arbitrum",
            iteration_number=0,
            cycle_id="teardown-xyz789",
        )

        await store.save_portfolio_snapshot(pre)
        await store.save_ledger_entry(swap)
        await store.save_portfolio_snapshot(post)
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# Positive: suppression fires on the canonical pattern.
# ---------------------------------------------------------------------------


def test_text_output_suppresses_headline_on_fallback_pattern(temp_db_path: Path) -> None:
    asyncio.run(_seed_fallback_pattern(temp_db_path))

    result = CliRunner().invoke(
        strat_pnl,
        ["-s", _DEPLOYMENT_ID, "--db", str(temp_db_path)],
    )

    assert result.exit_code == 0, result.output
    assert "Headline PnL:     unavailable" in result.output
    assert "Reason:" in result.output
    assert "VIB-4906" in result.output and "VIB-4907" in result.output
    # The misleading gross/net headline numbers must NOT appear.
    assert "Gross PnL:" not in result.output
    assert "Net PnL:" not in result.output
    assert "Net strategy NAV:" not in result.output
    # Deterministic friction components ARE preserved.
    assert "Gas costs:" in result.output
    assert "Protocol fees:" in result.output
    assert "Slippage:" in result.output
    assert "Impermanent loss:" in result.output
    # Deterministic trade stats also preserved.
    assert "Trade count:" in result.output


def test_json_output_carries_suppression_fields(temp_db_path: Path) -> None:
    asyncio.run(_seed_fallback_pattern(temp_db_path))

    result = CliRunner().invoke(
        strat_pnl,
        ["-s", _DEPLOYMENT_ID, "--db", str(temp_db_path), "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema_version"] == 1  # additive change — version unchanged
    assert payload["headline_suppressed"] is True
    assert isinstance(payload["headline_suppression_reason"], str)
    assert "VIB-4906" in payload["headline_suppression_reason"]
    # Additive contract: the original (misleading) numbers stay in the payload
    # so debugging consumers can see what the suppression flag is hiding.
    assert payload["gross_pnl_usd"] is not None
    assert payload["net_pnl_usd"] is not None
    # Deterministic fields unchanged.
    assert payload["gas_usd"] is not None
    assert payload["trade_count"] >= 1


# ---------------------------------------------------------------------------
# Negative: legitimately differing snapshots → no suppression, original
# headline rendered as before.
# ---------------------------------------------------------------------------


def test_text_output_does_not_suppress_when_snapshots_differ(temp_db_path: Path) -> None:
    asyncio.run(_seed_no_fallback_pattern(temp_db_path))

    result = CliRunner().invoke(
        strat_pnl,
        ["-s", _DEPLOYMENT_ID, "--db", str(temp_db_path)],
    )

    assert result.exit_code == 0, result.output
    assert "Headline PnL:" not in result.output
    assert "Gross PnL:" in result.output
    assert "Net PnL:" in result.output


def test_json_output_reports_unsuppressed_when_no_fallback(temp_db_path: Path) -> None:
    asyncio.run(_seed_no_fallback_pattern(temp_db_path))

    result = CliRunner().invoke(
        strat_pnl,
        ["-s", _DEPLOYMENT_ID, "--db", str(temp_db_path), "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["headline_suppressed"] is False
    assert payload["headline_suppression_reason"] is None
