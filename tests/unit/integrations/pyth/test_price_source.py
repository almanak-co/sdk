"""Focused contracts for the gateway-owned Pyth price source."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, PriceResult
from almanak.integrations.pyth.gateway.price_source import PYTH_FEED_IDS, PythPriceSource


def _hermes_session(*, price: str = "100", publish_time: int | None = None) -> MagicMock:
    price_payload: dict[str, object] = {"price": price, "conf": "1", "expo": -2}
    if publish_time is not None:
        price_payload["publish_time"] = publish_time
    response = MagicMock(status=200)
    response.json = AsyncMock(return_value={"parsed": [{"price": price_payload}]})
    context = AsyncMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.get.return_value = context
    return session


@pytest.mark.asyncio
async def test_rejects_non_solana_resolved_tokens_before_hermes_call() -> None:
    source = PythPriceSource()
    with patch.object(source, "_fetch_price", AsyncMock()) as fetch:
        with pytest.raises(DataSourceUnavailable, match="chain_mismatch:ethereum!=solana"):
            await source.get_price("SOL", resolved_token=SimpleNamespace(chain="ethereum"))
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_rejects_non_usd_quote_before_hermes_call() -> None:
    source = PythPriceSource()
    with patch.object(source, "_fetch_price", AsyncMock()) as fetch:
        with pytest.raises(DataSourceUnavailable, match="only supports USD"):
            await source.get_price("SOL", "EUR")
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_quote_case_uses_one_cache_entry() -> None:
    source = PythPriceSource()
    result = PriceResult(
        price=Decimal("1"),
        source="pyth",
        timestamp=datetime.now(UTC),
        confidence=1.0,
        stale=False,
    )
    with patch.object(source, "_fetch_price", AsyncMock(return_value=result)) as fetch:
        assert await source.get_price("SOL", "usd") is result
        assert await source.get_price("SOL", "USD") is result
    fetch.assert_awaited_once()
    assert list(source._cache) == ["SOL/USD"]


@pytest.mark.asyncio
@pytest.mark.parametrize("price", ["0", "-1"])
async def test_rejects_non_positive_hermes_prices(price: str) -> None:
    source = PythPriceSource()
    session = _hermes_session(price=price, publish_time=int(time.time()))
    with patch.object(source, "_get_session", AsyncMock(return_value=session)):
        with pytest.raises(DataSourceUnavailable, match="Non-positive price"):
            await source._fetch_price(PYTH_FEED_IDS["SOL"], "SOL")


@pytest.mark.asyncio
@pytest.mark.parametrize("publish_time", [None, 0, -1])
async def test_rejects_missing_or_non_positive_publish_time(publish_time: int | None) -> None:
    source = PythPriceSource()
    session = _hermes_session(publish_time=publish_time)
    with patch.object(source, "_get_session", AsyncMock(return_value=session)):
        with pytest.raises(DataSourceUnavailable, match="Missing publish_time"):
            await source._fetch_price(PYTH_FEED_IDS["SOL"], "SOL")


def test_spread_and_staleness_confidence_reductions_are_combined() -> None:
    source = PythPriceSource()
    confidence = source._calculate_confidence(
        price_int=100_000,
        conf_int=2_000,
        publish_time=int(time.time()) - 120,
    )
    assert confidence == 0.8
