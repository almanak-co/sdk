"""Unit tests for _fetch_latest_token_prices oracle/snapshot branches (VIB-3420)."""
from __future__ import annotations

import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from almanak.framework.observability.pnl_attributor import _fetch_latest_token_prices


class _SnapshotStore:
    """Store stub that returns a canned snapshot."""

    def __init__(self, prices: dict | None) -> None:
        self._prices = prices

    async def get_latest_snapshot(self, deployment_id: str):
        if self._prices is None:
            return None
        snap = MagicMock()
        snap.token_prices = self._prices
        return snap


class _EmptyStore:
    """Store stub with no snapshot support."""
    pass


class _PriceResult:
    def __init__(self, price) -> None:
        self.price = price


@pytest.mark.asyncio
async def test_snapshot_takes_precedence_over_oracle():
    """Snapshot prices are returned and the oracle is never called."""
    store = _SnapshotStore({"WETH": "3000", "USDC": "1.00"})
    oracle = AsyncMock()

    result = await _fetch_latest_token_prices(
        store, "deploy-1", token0="WETH", token1="USDC", price_oracle=oracle
    )

    assert result == {"WETH": "3000", "USDC": "1.00"}
    oracle.get_aggregated_price.assert_not_called()


@pytest.mark.asyncio
async def test_dict_oracle_returned_as_is_when_snapshot_missing():
    """Plain dict oracle is returned as-is so _price_for_token can match it."""
    store = _SnapshotStore(None)
    dict_oracle = {"WETH": Decimal("3000"), "USDC": Decimal("1")}

    result = await _fetch_latest_token_prices(
        store, "deploy-1", token0="WETH", token1="USDC", price_oracle=dict_oracle
    )

    assert result is dict_oracle


@pytest.mark.asyncio
async def test_empty_dict_oracle_returns_none():
    """Empty dict oracle counts as unavailable."""
    store = _SnapshotStore(None)

    result = await _fetch_latest_token_prices(
        store, "deploy-1", token0="WETH", token1="USDC", price_oracle={}
    )

    assert result is None


@pytest.mark.asyncio
async def test_async_oracle_collects_per_token_prices():
    """Async oracle is called per token; successful results are collected."""
    store = _SnapshotStore(None)

    async def fake_get_price(token, chain=None):
        if token == "WETH":
            return _PriceResult(Decimal("3000"))
        return _PriceResult(Decimal("1.00"))

    oracle = MagicMock()
    oracle.get_aggregated_price = fake_get_price

    result = await _fetch_latest_token_prices(
        store, "deploy-1", token0="WETH", token1="USDC", chain="arbitrum", price_oracle=oracle
    )

    assert result is not None
    assert result["weth"] == "3000"
    assert result["usdc"] == "1.00"


@pytest.mark.asyncio
async def test_async_oracle_partial_failure_returns_available_prices():
    """When one token's oracle call raises, the other token's price is still returned."""
    store = _SnapshotStore(None)

    async def fake_get_price(token, chain=None):
        if token == "WETH":
            return _PriceResult(Decimal("3000"))
        raise RuntimeError("oracle unavailable for token")

    oracle = MagicMock()
    oracle.get_aggregated_price = fake_get_price

    result = await _fetch_latest_token_prices(
        store, "deploy-1", token0="WETH", token1="USDC", price_oracle=oracle
    )

    assert result == {"weth": "3000"}


@pytest.mark.asyncio
async def test_returns_none_when_both_sources_unavailable():
    """None is returned when snapshot is absent and no oracle is provided."""
    store = _SnapshotStore(None)

    result = await _fetch_latest_token_prices(store, "deploy-1", token0="WETH", token1="USDC")

    assert result is None


@pytest.mark.asyncio
async def test_returns_none_when_store_has_no_snapshot_method():
    """Stores without get_latest_snapshot fall through to oracle path."""
    result = await _fetch_latest_token_prices(
        _EmptyStore(), "deploy-1", token0="WETH", token1="USDC"
    )

    assert result is None


@pytest.mark.asyncio
async def test_async_oracle_all_tokens_fail_returns_none():
    """When all per-token oracle calls fail, returns None (not empty dict)."""
    store = _SnapshotStore(None)

    async def always_fail(token, chain=None):
        raise RuntimeError("oracle down")

    oracle = MagicMock()
    oracle.get_aggregated_price = always_fail

    result = await _fetch_latest_token_prices(
        store, "deploy-1", token0="WETH", token1="USDC", price_oracle=oracle
    )

    assert result is None
