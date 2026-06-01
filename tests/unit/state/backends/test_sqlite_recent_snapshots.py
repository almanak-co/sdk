"""Unit tests for ``SQLiteStore.get_recent_snapshots`` (VIB-4907).

The detector at :mod:`almanak.framework.accounting.reporting.swap_class_fallback`
expects oldest-first ordering; these tests pin that contract plus the
empty-deployment + limit edge cases so a future change to the SQL can't
silently flip ordering or quietly raise.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio

from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    TokenBalance,
    ValueConfidence,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


@pytest.fixture
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db", prefix="recent_snaps_")
    os.close(fd)
    yield path
    for ext in ("", "-wal", "-shm", "-journal"):
        try:
            os.unlink(path + ext)
        except FileNotFoundError:
            pass


@pytest_asyncio.fixture
async def store(temp_db_path):
    s = SQLiteStore(SQLiteConfig(db_path=temp_db_path, wal_mode=True))
    await s.initialize()
    yield s
    await s.close()


def _snap(deployment_id: str, ts: datetime, total_usd: str = "100") -> PortfolioSnapshot:
    """Build a minimal snapshot at a specific timestamp."""
    return PortfolioSnapshot(
        timestamp=ts,
        deployment_id=deployment_id,
        total_value_usd=Decimal(total_usd),
        available_cash_usd=Decimal("100"),
        value_confidence=ValueConfidence.HIGH,
        deployed_capital_usd=Decimal("0"),
        wallet_total_value_usd=Decimal(total_usd),
        wallet_balances=[
            TokenBalance(
                symbol="USDC",
                balance=Decimal("100"),
                value_usd=Decimal("100"),
                address="0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
                price_usd=Decimal("1"),
            ),
        ],
        token_prices={"arbitrum:0xusdc": {"price_usd": "1.0", "symbol": "USDC", "decimals": 6}},
        chain="arbitrum",
        iteration_number=0,
    )


_BASE_TS = datetime(2026, 5, 30, 12, 0, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_empty_deployment_returns_empty_list(store) -> None:
    result = await store.get_recent_snapshots("deployment:never-wrote")
    assert result == []


@pytest.mark.asyncio
async def test_returns_oldest_first_within_window(store) -> None:
    """Three snapshots written; limit=2 returns the two newest, oldest-first."""
    dep = "deployment:recent-3"
    for i in range(3):
        await store.save_portfolio_snapshot(_snap(dep, _BASE_TS + timedelta(minutes=i)))

    result = await store.get_recent_snapshots(dep, limit=2)

    assert len(result) == 2
    # Oldest-first: index 0 is older than index 1.
    assert result[0].timestamp < result[1].timestamp
    # The window is the LATEST 2: it must not include minute 0.
    assert result[0].timestamp == _BASE_TS + timedelta(minutes=1)
    assert result[1].timestamp == _BASE_TS + timedelta(minutes=2)


@pytest.mark.asyncio
async def test_limit_one_matches_get_latest_snapshot(store) -> None:
    dep = "deployment:limit-1"
    for i in range(3):
        await store.save_portfolio_snapshot(_snap(dep, _BASE_TS + timedelta(minutes=i)))

    window = await store.get_recent_snapshots(dep, limit=1)
    latest = await store.get_latest_snapshot(dep)

    assert len(window) == 1
    assert latest is not None
    assert window[0].timestamp == latest.timestamp


@pytest.mark.asyncio
async def test_limit_zero_returns_empty(store) -> None:
    dep = "deployment:limit-0"
    await store.save_portfolio_snapshot(_snap(dep, _BASE_TS))

    result = await store.get_recent_snapshots(dep, limit=0)

    assert result == []


@pytest.mark.asyncio
async def test_negative_limit_returns_empty_does_not_raise(store) -> None:
    dep = "deployment:negative"
    await store.save_portfolio_snapshot(_snap(dep, _BASE_TS))

    result = await store.get_recent_snapshots(dep, limit=-3)

    assert result == []


@pytest.mark.asyncio
async def test_limit_exceeds_available_returns_all(store) -> None:
    dep = "deployment:few"
    await store.save_portfolio_snapshot(_snap(dep, _BASE_TS))

    result = await store.get_recent_snapshots(dep, limit=10)

    assert len(result) == 1
    assert result[0].timestamp == _BASE_TS


@pytest.mark.asyncio
async def test_deployment_scoping(store) -> None:
    """Snapshots from other deployments must not bleed into the result."""
    await store.save_portfolio_snapshot(_snap("deployment:mine", _BASE_TS))
    await store.save_portfolio_snapshot(_snap("deployment:other", _BASE_TS + timedelta(minutes=1)))

    result = await store.get_recent_snapshots("deployment:mine", limit=5)

    assert len(result) == 1
    assert result[0].deployment_id == "deployment:mine"
