"""Loader-tier test: ``load_accounting_data`` populates ``recent_snapshots``.

This is the seam where the SWAP-class fallback detector consumes the
snapshot window — pin the contract so the F4 / VIB-4907 path doesn't
silently lose its input.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio

from almanak.framework.accounting.reporting import load_accounting_data
from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    TokenBalance,
    ValueConfidence,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


_DEPLOYMENT_ID = "deployment:loader-snap-window"
_BASE_TS = datetime(2026, 5, 30, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db", prefix="loader_snaps_")
    os.close(fd)
    yield path
    for ext in ("", "-wal", "-shm", "-journal"):
        try:
            os.unlink(path + ext)
        except FileNotFoundError:
            pass


def _snap(ts: datetime, cycle_id: str = "iter") -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=ts,
        deployment_id=_DEPLOYMENT_ID,
        total_value_usd=Decimal("100"),
        available_cash_usd=Decimal("100"),
        value_confidence=ValueConfidence.HIGH,
        deployed_capital_usd=Decimal("0"),
        wallet_total_value_usd=Decimal("100"),
        wallet_balances=[
            TokenBalance(
                symbol="USDC",
                balance=Decimal("100"),
                value_usd=Decimal("100"),
                address="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                price_usd=Decimal("1.0"),
            ),
        ],
        token_prices={"arbitrum:0xusdc": {"price_usd": "1.0", "symbol": "USDC", "decimals": 6}},
        chain="arbitrum",
        iteration_number=0,
        cycle_id=cycle_id,
    )


async def _seed_snapshots(db_path: str, count: int) -> None:
    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    await store.initialize()
    try:
        for i in range(count):
            await store.save_portfolio_snapshot(_snap(_BASE_TS + timedelta(minutes=i), cycle_id=f"iter-{i}"))
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_default_window_is_two(temp_db_path) -> None:
    await _seed_snapshots(temp_db_path, count=3)

    data = await load_accounting_data(temp_db_path, _DEPLOYMENT_ID)

    assert len(data.recent_snapshots) == 2
    # Oldest-first within the window.
    assert data.recent_snapshots[0].timestamp < data.recent_snapshots[1].timestamp
    # The window is the latest two — not snapshot 0.
    assert data.recent_snapshots[0].cycle_id == "iter-1"
    assert data.recent_snapshots[1].cycle_id == "iter-2"


@pytest.mark.asyncio
async def test_snapshot_equals_tail_of_window(temp_db_path) -> None:
    """The legacy ``snapshot`` field stays the latest of ``recent_snapshots``."""
    await _seed_snapshots(temp_db_path, count=3)

    data = await load_accounting_data(temp_db_path, _DEPLOYMENT_ID)

    assert data.snapshot is not None
    assert data.snapshot.timestamp == data.recent_snapshots[-1].timestamp


@pytest.mark.asyncio
async def test_empty_deployment_returns_empty_window_and_none_snapshot(temp_db_path) -> None:
    # No seeding.
    data = await load_accounting_data(temp_db_path, _DEPLOYMENT_ID)

    assert data.recent_snapshots == []
    assert data.snapshot is None


@pytest.mark.asyncio
async def test_custom_window_size(temp_db_path) -> None:
    await _seed_snapshots(temp_db_path, count=5)

    data = await load_accounting_data(temp_db_path, _DEPLOYMENT_ID, snapshot_window=4)

    assert len(data.recent_snapshots) == 4
    assert data.recent_snapshots[0].cycle_id == "iter-1"
    assert data.recent_snapshots[-1].cycle_id == "iter-4"


@pytest.mark.asyncio
async def test_window_clamped_to_one_when_invalid(temp_db_path) -> None:
    """Loader treats ``snapshot_window < 1`` as 1 rather than raising."""
    await _seed_snapshots(temp_db_path, count=3)

    data = await load_accounting_data(temp_db_path, _DEPLOYMENT_ID, snapshot_window=0)

    assert len(data.recent_snapshots) == 1
    # Still the latest.
    assert data.recent_snapshots[0].cycle_id == "iter-2"
