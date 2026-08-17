"""Unit tests for _fetch_latest_token_prices oracle/snapshot branches (VIB-3420)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.observability.pnl_attributor import _fetch_latest_token_prices


class _SnapshotStore:
    """Store stub that returns a canned snapshot."""

    def __init__(self, prices: dict | None, *, timestamp: datetime | None = None) -> None:
        self._prices = prices
        self._timestamp = timestamp or datetime(2026, 8, 14, 12, tzinfo=UTC)

    async def get_latest_snapshot(self, deployment_id: str):
        if self._prices is None:
            return None
        snap = MagicMock()
        snap.token_prices = self._prices
        snap.timestamp = self._timestamp
        return snap


class _EmptyStore:
    """Store stub with no snapshot support."""

    pass


class _PriceResult:
    def __init__(self, price, *, observed_at: datetime | None = None) -> None:
        self.price = price
        self.source = "chainlink"
        self.timestamp = observed_at or datetime(2026, 8, 14, 12, tzinfo=UTC)
        self.confidence = 1.0
        self.stale = False


@pytest.mark.asyncio
async def test_snapshot_takes_precedence_over_oracle():
    """Snapshot prices are returned and the oracle is never called."""
    store = _SnapshotStore({"WETH": "3000", "USDC": "1.00"})
    oracle = AsyncMock()

    result = await _fetch_latest_token_prices(store, "deploy-1", token0="WETH", token1="USDC", price_oracle=oracle)

    assert result == {"WETH": "3000", "USDC": "1.00"}
    oracle.get_aggregated_price.assert_not_called()


@pytest.mark.asyncio
async def test_dict_oracle_returned_as_is_when_snapshot_missing():
    """Plain dict oracle is returned as-is so _price_for_token can match it."""
    store = _SnapshotStore(None)
    dict_oracle = {"WETH": Decimal("3000"), "USDC": Decimal("1")}

    result = await _fetch_latest_token_prices(store, "deploy-1", token0="WETH", token1="USDC", price_oracle=dict_oracle)

    assert result is dict_oracle


@pytest.mark.asyncio
async def test_empty_dict_oracle_returns_none():
    """Empty dict oracle counts as unavailable."""
    store = _SnapshotStore(None)

    result = await _fetch_latest_token_prices(store, "deploy-1", token0="WETH", token1="USDC", price_oracle={})

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
    assert result["weth"]["price_usd"] == "3000"
    assert result["weth"]["oracle_source"] == "chainlink"
    assert result["weth"]["observed_at"] == "2026-08-14T12:00:00+00:00"
    assert result["usdc"]["price_usd"] == "1.00"


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

    result = await _fetch_latest_token_prices(store, "deploy-1", token0="WETH", token1="USDC", price_oracle=oracle)

    assert result is not None
    assert result["weth"]["price_usd"] == "3000"
    assert result["weth"]["confidence"] == "HIGH"


@pytest.mark.asyncio
async def test_returns_none_when_both_sources_unavailable():
    """None is returned when snapshot is absent and no oracle is provided."""
    store = _SnapshotStore(None)

    result = await _fetch_latest_token_prices(store, "deploy-1", token0="WETH", token1="USDC")

    assert result is None


@pytest.mark.asyncio
async def test_returns_none_when_store_has_no_snapshot_method():
    """Stores without get_latest_snapshot fall through to oracle path."""
    result = await _fetch_latest_token_prices(_EmptyStore(), "deploy-1", token0="WETH", token1="USDC")

    assert result is None


@pytest.mark.asyncio
async def test_async_oracle_all_tokens_fail_returns_none():
    """When all per-token oracle calls fail, returns None (not empty dict)."""
    store = _SnapshotStore(None)

    async def always_fail(token, chain=None):
        raise RuntimeError("oracle down")

    oracle = MagicMock()
    oracle.get_aggregated_price = always_fail

    result = await _fetch_latest_token_prices(store, "deploy-1", token0="WETH", token1="USDC", price_oracle=oracle)

    assert result is None


@pytest.mark.asyncio
async def test_as_of_oracle_accepts_event_time_and_rejects_future_observation():
    event_time = datetime(2026, 8, 14, 12, tzinfo=UTC)

    async def event_time_price(token, chain=None):
        return _PriceResult(Decimal("3000"), observed_at=event_time)

    oracle = MagicMock()
    oracle.get_aggregated_price = event_time_price
    result = await _fetch_latest_token_prices(
        _EmptyStore(),
        "deploy-1",
        token0="WETH",
        chain="arbitrum",
        price_oracle=oracle,
        as_of=event_time,
    )

    assert result is not None
    assert result["weth"]["observed_at"] == event_time.isoformat()

    async def future_price(token, chain=None):
        return _PriceResult(Decimal("3000"), observed_at=event_time + timedelta(microseconds=1))

    oracle.get_aggregated_price = future_price
    assert (
        await _fetch_latest_token_prices(
            _EmptyStore(),
            "deploy-1",
            token0="WETH",
            chain="arbitrum",
            price_oracle=oracle,
            as_of=event_time,
        )
        is None
    )


class _AsOfSnapshotStore:
    def __init__(self, prices: dict) -> None:
        self.prices = prices
        self.calls: list[tuple[str, datetime]] = []

    async def get_snapshot_at(self, deployment_id: str, timestamp: datetime):
        self.calls.append((deployment_id, timestamp))
        return MagicMock(token_prices=self.prices, timestamp=timestamp - timedelta(seconds=1))


def _priced(observed_at: datetime, *, confidence: str = "HIGH") -> dict:
    return {
        "price_usd": "3000",
        "symbol": "WETH",
        "oracle_source": "chainlink",
        "observed_at": observed_at.isoformat(),
        "fetched_at": observed_at.isoformat(),
        "confidence": confidence,
        "raw_confidence": 1.0,
        "stale": confidence == "STALE",
    }


@pytest.mark.asyncio
async def test_as_of_uses_snapshot_at_or_before_event_and_accepts_five_minute_price():
    event_time = datetime(2026, 8, 14, 12, tzinfo=UTC)
    store = _AsOfSnapshotStore({"arbitrum:WETH": _priced(event_time - timedelta(minutes=5))})

    result = await _fetch_latest_token_prices(
        store,
        "deploy-1",
        token0="WETH",
        chain="arbitrum",
        as_of=event_time,
    )

    assert result is not None
    assert result["arbitrum:WETH"]["price_usd"] == "3000"
    assert store.calls == [("deploy-1", event_time)]


@pytest.mark.asyncio
async def test_as_of_rejects_price_older_than_five_minutes():
    event_time = datetime(2026, 8, 14, 12, tzinfo=UTC)
    store = _AsOfSnapshotStore({"WETH": _priced(event_time - timedelta(minutes=5, seconds=1))})

    result = await _fetch_latest_token_prices(store, "deploy-1", token0="WETH", as_of=event_time)

    assert result is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_case",
    ["future", "missing_timestamp", "missing_confidence", "stale"],
)
async def test_as_of_rejects_each_invalid_price_independently(invalid_case):
    event_time = datetime(2026, 8, 14, 12, tzinfo=UTC)
    price = _priced(event_time)
    if invalid_case == "future":
        price = _priced(event_time + timedelta(seconds=1))
    elif invalid_case == "missing_timestamp":
        price.pop("observed_at")
        price.pop("fetched_at")
    elif invalid_case == "missing_confidence":
        price.pop("confidence")
    else:
        price = _priced(event_time, confidence="STALE")
    store = _AsOfSnapshotStore({"WETH": price})

    result = await _fetch_latest_token_prices(
        store,
        "deploy-1",
        token0="WETH",
        as_of=event_time,
    )

    assert result is None
