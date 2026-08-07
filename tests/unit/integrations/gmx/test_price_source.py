"""Focused contracts for the gateway-owned GMX venue ticker price source (ALM-3177)."""

from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from almanak.connectors._base.gateway_capabilities import VenueTickerPrice
from almanak.framework.data.interfaces import DataSourceUnavailable, PriceResult
from almanak.integrations import INTEGRATION_REGISTRY
from almanak.integrations._base import PriceSourceScope
from almanak.integrations.gmx.gateway.factory import GmxTickerPriceSourceFactory
from almanak.integrations.gmx.gateway.price_source import GmxTickerPriceSource


class _FakeProvider:
    """Capability stub returning a fixed synthetic-index page."""

    def __init__(
        self,
        page: dict[str, VenueTickerPrice] | None = None,
        error: Exception | None = None,
        integration: str = "gmx",
    ) -> None:
        self._page = page or {}
        self._error = error
        self._integration = integration
        self.fetch = AsyncMock(side_effect=self._fetch)

    def ticker_price_venue(self) -> str:
        return "gmx_v2"

    def ticker_price_integration(self) -> str:
        return self._integration

    def ticker_price_chains(self) -> frozenset[str]:
        return frozenset({"arbitrum", "avalanche"})

    async def _fetch(self, *, chain: str) -> dict[str, VenueTickerPrice]:
        if self._error is not None:
            raise self._error
        return self._page

    async def fetch_ticker_prices(self, *, chain: str) -> dict[str, VenueTickerPrice]:
        return await self.fetch(chain=chain)


def _entry(symbol: str = "XMR", price: str = "369", age_seconds: float = 0.0) -> VenueTickerPrice:
    return VenueTickerPrice(
        symbol=symbol,
        price_usd=Decimal(price),
        updated_at=int(time.time() - age_seconds),
    )


def _source(provider: _FakeProvider | None) -> GmxTickerPriceSource:
    source = GmxTickerPriceSource(chain="arbitrum")
    source._provider = provider
    return source


@pytest.mark.asyncio
async def test_fresh_synthetic_symbol_prices_from_venue_observation() -> None:
    entry = _entry()
    source = _source(_FakeProvider(page={"XMR": entry}))

    result = await source.get_price("xmr")

    assert isinstance(result, PriceResult)
    assert result.price == Decimal("369")
    assert result.source == "gmx_ticker"
    assert result.stale is False
    assert result.confidence == 0.95
    assert int(result.timestamp.timestamp()) == entry.updated_at


@pytest.mark.asyncio
async def test_aging_observation_is_served_stale_with_reduced_confidence() -> None:
    source = _source(_FakeProvider(page={"XMR": _entry(age_seconds=120)}))

    result = await source.get_price("XMR")

    assert result.stale is True
    assert result.confidence == 0.7


@pytest.mark.asyncio
async def test_observation_past_cutoff_is_a_miss_not_a_price() -> None:
    source = _source(_FakeProvider(page={"XMR": _entry(age_seconds=600)}))

    with pytest.raises(DataSourceUnavailable, match="implausible age"):
        await source.get_price("XMR")


@pytest.mark.asyncio
async def test_future_dated_observation_is_a_miss_not_a_fresh_price() -> None:
    """A venue timestamp ahead of the local clock (beyond skew tolerance) must
    never be served fresh at full confidence."""
    source = _source(_FakeProvider(page={"XMR": _entry(age_seconds=-3600)}))

    with pytest.raises(DataSourceUnavailable, match="implausible age"):
        await source.get_price("XMR")


@pytest.mark.asyncio
async def test_small_future_clock_skew_is_tolerated() -> None:
    source = _source(_FakeProvider(page={"XMR": _entry(age_seconds=-30)}))

    result = await source.get_price("XMR")

    assert result.stale is False
    assert result.confidence == 0.95


@pytest.mark.asyncio
async def test_unknown_symbol_misses_so_aggregator_falls_through() -> None:
    source = _source(_FakeProvider(page={"XMR": _entry()}))

    with pytest.raises(DataSourceUnavailable, match="deployed identity|not a synthetic GMX index symbol"):
        await source.get_price("WETH")


@pytest.mark.asyncio
async def test_registry_deployed_symbol_is_gated_before_any_fetch() -> None:
    """GMX marks CRV's index row synthetic, but CRV has a deployed Arbitrum
    contract in the SDK token registry — the venue feed must never join the
    aggregation vote for it (P1, PR #3637 review)."""
    provider = _FakeProvider(page={"CRV": _entry(symbol="CRV", price="0.5")})
    source = _source(provider)

    with pytest.raises(DataSourceUnavailable, match="deployed identity on arbitrum"):
        await source.get_price("CRV")
    provider.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_registry_unknown_synthetic_is_still_served() -> None:
    """XMR has no deployed identity anywhere — the gate must not block it."""
    source = _source(_FakeProvider(page={"XMR": _entry()}))

    result = await source.get_price("XMR")

    assert result.price == Decimal("369")


def test_provider_with_foreign_integration_identity_is_not_selected() -> None:
    """A second venue's capability on the same chain must never be routed
    under this source's name (P1, PR #3637 review): selection matches the
    connector-declared integration identity, not chain alone."""
    foreign = _FakeProvider(integration="othervenue")
    with patch(
        "almanak.integrations.gmx.gateway.price_source.GATEWAY_REGISTRY"
    ) as registry:
        registry.capability_providers.return_value = [foreign]
        source = GmxTickerPriceSource(chain="arbitrum")

    assert source._provider is None


@pytest.mark.asyncio
async def test_non_usd_quote_is_a_miss() -> None:
    source = _source(_FakeProvider(page={"XMR": _entry()}))

    with pytest.raises(DataSourceUnavailable, match="Only USD quote"):
        await source.get_price("XMR", quote="EUR")


@pytest.mark.asyncio
async def test_cross_chain_resolved_token_is_rejected_before_any_fetch() -> None:
    provider = _FakeProvider(page={"XMR": _entry()})
    source = _source(provider)

    with pytest.raises(DataSourceUnavailable, match="chain_mismatch:base!=arbitrum"):
        await source.get_price("XMR", resolved_token=SimpleNamespace(chain="base"))
    provider.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_chain_resolved_token_is_served() -> None:
    source = _source(_FakeProvider(page={"XMR": _entry()}))

    result = await source.get_price("XMR", resolved_token=SimpleNamespace(chain="arbitrum"))

    assert result.price == Decimal("369")


@pytest.mark.asyncio
async def test_missing_provider_is_a_miss() -> None:
    source = _source(None)

    with pytest.raises(DataSourceUnavailable, match="No venue ticker provider"):
        await source.get_price("XMR")


@pytest.mark.asyncio
async def test_feed_outage_is_a_miss_not_a_crash() -> None:
    source = _source(_FakeProvider(error=RuntimeError("boom")))

    with pytest.raises(DataSourceUnavailable, match="unavailable"):
        await source.get_price("XMR")


def test_factory_contract_and_manifest_policy() -> None:
    factory = GmxTickerPriceSourceFactory()

    assert factory.name == "gmx"
    assert factory.scope is PriceSourceScope.CHAIN
    assert factory.order == 10
    assert factory.supports(None) is False
    assert factory.supports("arbitrum") is True
    assert factory.supports("avalanche") is True
    assert factory.supports("base") is False
    # No exclusive group: joining hypercore's "venue_oracle" group would block
    # Binance on Arbitrum/Avalanche via price_source_blocked_by_groups.
    assert INTEGRATION_REGISTRY.price_source_policy("gmx") == (None, frozenset())


def test_factory_builds_chain_scoped_source() -> None:
    factory = GmxTickerPriceSourceFactory()

    source = factory.build(chain="arbitrum", settings=SimpleNamespace())

    assert isinstance(source, GmxTickerPriceSource)
    assert source.source_name == "gmx_ticker"
    # The constructor's registry lookup must resolve the real GMX capability
    # provider — every other test injects _provider, so without this assertion
    # a broken lookup would pass the suite while production permanently misses.
    assert source._provider is not None
    assert "arbitrum" in source._provider.ticker_price_chains()
    with pytest.raises(ValueError, match="requires a chain"):
        factory.build(chain=None, settings=SimpleNamespace())
