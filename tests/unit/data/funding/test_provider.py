"""Unit tests for the gateway-backed funding rate provider.

These tests exercise :class:`GatewayFundingRateProvider` against a fake
gateway stub. No network calls — the gateway service itself is covered
by ``tests/gateway/test_funding_rate_service_ssl.py`` and friends.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.funding import (
    HOURS_PER_YEAR,
    SUPPORTED_MARKETS,
    SUPPORTED_VENUES,
    FundingRate,
    FundingRateSpread,
    FundingRateUnavailableError,
    GatewayFundingRateProvider,
    MarketNotSupportedError,
    Venue,
    VenueNotSupportedError,
)


# =============================================================================
# Fake gateway stub
# =============================================================================


def _make_response(
    *,
    venue: str = "gmx_v2",
    market: str = "ETH-USD",
    rate_hourly: str = "0.00001",
    next_funding_time: int | None = None,
    open_interest_long: str = "125000000",
    open_interest_short: str = "118000000",
    mark_price: str = "3000",
    index_price: str = "3000",
    is_live_data: bool = True,
    success: bool = True,
    error: str = "",
) -> SimpleNamespace:
    """Build a duck-typed FundingRateResponse used by the fake stub."""
    rate_hourly_dec = Decimal(rate_hourly)
    rate_8h = rate_hourly_dec * Decimal("8")
    rate_annualized = rate_hourly_dec * Decimal(str(HOURS_PER_YEAR))
    if next_funding_time is None:
        next_funding_time = int((datetime.now(UTC) + timedelta(hours=1)).timestamp())
    return SimpleNamespace(
        venue=venue,
        market=market,
        rate_hourly=str(rate_hourly_dec),
        rate_8h=str(rate_8h),
        rate_annualized=str(rate_annualized),
        next_funding_time=next_funding_time,
        open_interest_long=open_interest_long,
        open_interest_short=open_interest_short,
        mark_price=mark_price,
        index_price=index_price,
        is_live_data=is_live_data,
        success=success,
        error=error,
    )


def _make_spread_response(rate_a: SimpleNamespace, rate_b: SimpleNamespace) -> SimpleNamespace:
    # Wire convention is absolute spread (matches gateway-side
    # funding_rate_service.GetFundingRateSpread); the SDK derives the
    # signed spread from venue_a_rate vs venue_b_rate.
    wire_spread_hourly = abs(Decimal(rate_a.rate_hourly) - Decimal(rate_b.rate_hourly))
    spread_annualized = wire_spread_hourly * Decimal(str(HOURS_PER_YEAR))
    return SimpleNamespace(
        spread_hourly=str(wire_spread_hourly),
        spread_annualized=str(spread_annualized),
        venue_a_rate=rate_a,
        venue_b_rate=rate_b,
        success=True,
        error="",
    )


class _FakeFundingRateStub:
    """Records requests and returns responses from a per-venue table."""

    def __init__(self, responses: dict[tuple[str, str], SimpleNamespace] | None = None) -> None:
        self.responses: dict[tuple[str, str], SimpleNamespace] = responses or {}
        self.calls: list[Any] = []
        self.spread_calls: list[Any] = []

    def GetFundingRate(self, request, timeout: float | None = None) -> SimpleNamespace:  # noqa: N802
        self.calls.append(request)
        key = (request.venue, request.market)
        if key in self.responses:
            return self.responses[key]
        return _make_response(venue=request.venue, market=request.market)

    def GetFundingRateSpread(self, request, timeout: float | None = None) -> SimpleNamespace:  # noqa: N802
        self.spread_calls.append(request)
        rate_a = self.responses.get(
            (request.venue_a, request.market),
            _make_response(venue=request.venue_a, market=request.market),
        )
        rate_b = self.responses.get(
            (request.venue_b, request.market),
            _make_response(venue=request.venue_b, market=request.market),
        )
        return _make_spread_response(rate_a, rate_b)


def _make_provider(
    responses: dict[tuple[str, str], SimpleNamespace] | None = None,
    *,
    chain: str = "arbitrum",
    cache_ttl_seconds: float = 10.0,
) -> tuple[GatewayFundingRateProvider, _FakeFundingRateStub]:
    stub = _FakeFundingRateStub(responses)
    client = MagicMock()
    client.funding_rate = stub
    client.config = SimpleNamespace(timeout=5.0)
    return GatewayFundingRateProvider(client, chain=chain, cache_ttl_seconds=cache_ttl_seconds), stub


# =============================================================================
# Provider initialization
# =============================================================================


class TestProviderInitialization:
    def test_default_chain(self) -> None:
        provider, _ = _make_provider()
        assert provider.chain == "arbitrum"

    def test_custom_chain_lowercased(self) -> None:
        provider, _ = _make_provider(chain="ARBITRUM")
        assert provider.chain == "arbitrum"

    def test_custom_cache_ttl(self) -> None:
        provider, _ = _make_provider(cache_ttl_seconds=42.0)
        assert provider._cache_ttl_seconds == 42.0


# =============================================================================
# get_funding_rate
# =============================================================================


class TestGetFundingRate:
    @pytest.mark.asyncio
    async def test_returns_rich_funding_rate(self) -> None:
        provider, stub = _make_provider(
            {("gmx_v2", "ETH-USD"): _make_response(rate_hourly="0.000012")}
        )

        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")

        assert isinstance(rate, FundingRate)
        assert rate.venue == "gmx_v2"
        assert rate.market == "ETH-USD"
        assert rate.rate_hourly == Decimal("0.000012")
        assert rate.rate_8h == Decimal("0.000012") * Decimal("8")
        assert rate.rate_annualized == Decimal("0.000012") * Decimal(str(HOURS_PER_YEAR))
        assert rate.is_live_data is True
        assert rate.next_funding_time is not None
        assert rate.open_interest_long == Decimal("125000000")
        # Request was forwarded with the configured chain.
        assert stub.calls[0].chain == "arbitrum"

    @pytest.mark.asyncio
    async def test_string_venue_accepted(self) -> None:
        provider, _ = _make_provider()
        rate = await provider.get_funding_rate("gmx_v2", "eth-usd")
        assert rate.venue == "gmx_v2"
        assert rate.market == "ETH-USD"

    @pytest.mark.asyncio
    async def test_unsupported_venue_raises(self) -> None:
        provider, _ = _make_provider()
        with pytest.raises(VenueNotSupportedError) as exc_info:
            await provider.get_funding_rate("invalid_venue", "ETH-USD")
        assert exc_info.value.venue == "invalid_venue"

    @pytest.mark.asyncio
    async def test_unsupported_market_raises(self) -> None:
        provider, _ = _make_provider()
        with pytest.raises(MarketNotSupportedError) as exc_info:
            await provider.get_funding_rate(Venue.GMX_V2, "INVALID-USD")
        assert exc_info.value.market == "INVALID-USD"
        assert exc_info.value.venue == "gmx_v2"

    @pytest.mark.asyncio
    async def test_gateway_failure_raises_unavailable(self) -> None:
        provider, _ = _make_provider(
            {("gmx_v2", "ETH-USD"): _make_response(success=False, error="boom")}
        )
        with pytest.raises(FundingRateUnavailableError) as exc_info:
            await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        assert exc_info.value.venue == "gmx_v2"
        assert exc_info.value.market == "ETH-USD"
        assert "boom" in exc_info.value.reason

    @pytest.mark.asyncio
    async def test_transport_error_raises_unavailable(self) -> None:
        provider, stub = _make_provider()

        def _raise(*_a: Any, **_kw: Any) -> SimpleNamespace:
            raise RuntimeError("transport down")

        stub.GetFundingRate = _raise  # type: ignore[assignment]

        with pytest.raises(FundingRateUnavailableError) as exc_info:
            await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        assert "transport down" in exc_info.value.reason


# =============================================================================
# Caching
# =============================================================================


class TestCaching:
    @pytest.mark.asyncio
    async def test_second_call_hits_cache(self) -> None:
        provider, stub = _make_provider(cache_ttl_seconds=60.0)
        await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        assert len(stub.calls) == 1

    @pytest.mark.asyncio
    async def test_clear_cache(self) -> None:
        provider, stub = _make_provider(cache_ttl_seconds=60.0)
        await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        provider.clear_cache()
        await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        assert len(stub.calls) == 2


# =============================================================================
# get_funding_rate_spread
# =============================================================================


class TestGetFundingRateSpread:
    @pytest.mark.asyncio
    async def test_signed_spread_from_rates(self) -> None:
        provider, _ = _make_provider(
            {
                ("gmx_v2", "ETH-USD"): _make_response(venue="gmx_v2", rate_hourly="0.00001"),
                ("hyperliquid", "ETH-USD"): _make_response(venue="hyperliquid", rate_hourly="0.00002"),
            }
        )

        spread = await provider.get_funding_rate_spread(
            "ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID
        )

        assert isinstance(spread, FundingRateSpread)
        assert spread.market == "ETH-USD"
        assert spread.venue_a == "gmx_v2"
        assert spread.venue_b == "hyperliquid"
        assert spread.rate_a.rate_hourly == Decimal("0.00001")
        assert spread.rate_b.rate_hourly == Decimal("0.00002")
        # spread_8h = (rate_a - rate_b) * 8
        assert spread.spread_8h == Decimal("-0.00008")

    @pytest.mark.asyncio
    async def test_recommended_direction_signed(self) -> None:
        provider, _ = _make_provider(
            {
                ("gmx_v2", "ETH-USD"): _make_response(venue="gmx_v2", rate_hourly="0.0001"),
                ("hyperliquid", "ETH-USD"): _make_response(venue="hyperliquid", rate_hourly="0.00001"),
            }
        )

        spread = await provider.get_funding_rate_spread(
            "ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID
        )

        assert spread.is_profitable is True
        assert spread.recommended_direction == "short_a_long_b"

    @pytest.mark.asyncio
    async def test_unsupported_venue_for_spread(self) -> None:
        provider, _ = _make_provider()
        with pytest.raises(VenueNotSupportedError):
            await provider.get_funding_rate_spread("ETH-USD", "bogus_venue", Venue.HYPERLIQUID)


# =============================================================================
# get_rates_for_market
# =============================================================================


class TestGetRatesForMarket:
    @pytest.mark.asyncio
    async def test_returns_all_known_venues_by_default(self) -> None:
        provider, _ = _make_provider()
        rates = await provider.get_rates_for_market("ETH-USD")
        assert set(rates.keys()) == set(SUPPORTED_VENUES)

    @pytest.mark.asyncio
    async def test_swallows_per_venue_errors(self) -> None:
        provider, _ = _make_provider(
            {
                ("gmx_v2", "ETH-USD"): _make_response(success=False, error="rpc down"),
                ("hyperliquid", "ETH-USD"): _make_response(venue="hyperliquid"),
            }
        )
        rates = await provider.get_rates_for_market("ETH-USD")
        assert "gmx_v2" not in rates
        assert "hyperliquid" in rates


# =============================================================================
# Data class behavior (shared with previous tests; kept here as smoke-checks)
# =============================================================================


class TestFundingRateDataClass:
    def test_is_positive_negative(self) -> None:
        positive = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.00001"),
            rate_8h=Decimal("0.00008"),
            rate_annualized=Decimal("0.0876"),
        )
        negative = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("-0.00001"),
            rate_8h=Decimal("-0.00008"),
            rate_annualized=Decimal("-0.0876"),
        )
        assert positive.is_positive is True
        assert positive.is_negative is False
        assert negative.is_positive is False
        assert negative.is_negative is True

    def test_to_dict(self) -> None:
        rate = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.00001"),
            rate_8h=Decimal("0.00008"),
            rate_annualized=Decimal("0.0876"),
            mark_price=Decimal("2500"),
            index_price=Decimal("2500"),
        )
        data = rate.to_dict()
        assert data["venue"] == "gmx_v2"
        assert data["rate_hourly"] == "0.00001"
        assert data["rate_percent_8h"] == 0.008
        assert data["mark_price"] == 2500.0


class TestFundingRateSpreadDataClass:
    @staticmethod
    def _spread(spread_8h: str) -> FundingRateSpread:
        rate = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0"),
            rate_8h=Decimal("0"),
            rate_annualized=Decimal("0"),
        )
        return FundingRateSpread(
            market="ETH-USD",
            venue_a="gmx_v2",
            venue_b="hyperliquid",
            rate_a=rate,
            rate_b=rate,
            spread_8h=Decimal(spread_8h),
            spread_annualized=Decimal(spread_8h) * Decimal(str(HOURS_PER_YEAR)) / Decimal("8"),
        )

    def test_is_profitable_above_threshold(self) -> None:
        assert self._spread("0.00072").is_profitable is True
        assert self._spread("0.00072").recommended_direction == "short_a_long_b"

    def test_is_profitable_negative_above_threshold(self) -> None:
        assert self._spread("-0.0005").is_profitable is True
        assert self._spread("-0.0005").recommended_direction == "short_b_long_a"

    def test_is_profitable_below_threshold(self) -> None:
        assert self._spread("-0.000008").is_profitable is False
        assert self._spread("-0.000008").recommended_direction is None


# =============================================================================
# Constants
# =============================================================================


class TestConstants:
    def test_supported_venues(self) -> None:
        assert "gmx_v2" in SUPPORTED_VENUES
        assert "hyperliquid" in SUPPORTED_VENUES
        assert len(SUPPORTED_VENUES) == 2

    def test_supported_markets(self) -> None:
        assert "ETH-USD" in SUPPORTED_MARKETS["gmx_v2"]
        assert "ETH-USD" in SUPPORTED_MARKETS["hyperliquid"]

    def test_hours_per_year(self) -> None:
        assert HOURS_PER_YEAR == 8760
