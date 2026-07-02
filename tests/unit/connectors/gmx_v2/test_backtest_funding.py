"""Unit tests for GMX V2 Funding Rate Provider.

This module tests the GMXFundingProvider class in connectors/gmx_v2/backtest_funding.py.
The provider is a thin ``RateHistoryService`` client since VIB-4851 Phase D —
tests mock ``fetch_funding_points`` (the gateway seam), never HTTP:
- Provider initialization and configuration
- Manifest-derived supported chains
- Carry-forward hourly grid built from measured history
- Fallback behavior (never raises; LOW-confidence fill on failure)
- Current-rate convenience method
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
from almanak.connectors.gmx_v2.backtest_funding import (
    DATA_SOURCE,
    DEFAULT_REQUESTS_PER_MINUTE,
    GMXClientConfig,
    GMXFundingProvider,
)
from almanak.framework.backtesting.pnl.providers.base import BacktestProviderConfig
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
    FundingHistoryPoint,
)
from almanak.framework.backtesting.pnl.providers.rate_limiter import TokenBucketRateLimiter
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.interfaces import DataSourceUnavailable

_GATEWAY_SEAM = "almanak.connectors.gmx_v2.backtest_funding.fetch_funding_points"


def _points(start: datetime, rates: list[str]) -> list[FundingHistoryPoint]:
    """Hourly points starting at ``start`` with the given rates."""
    t0 = int(start.timestamp())
    return [FundingHistoryPoint(timestamp=t0 + 3600 * i, rate_hourly=Decimal(rate)) for i, rate in enumerate(rates)]


class TestGMXFundingProviderInitialization:
    """Tests for provider initialization."""

    def test_init_default(self):
        """Defaults: arbitrum chain, default throttle, owns its limiter."""
        provider = GMXFundingProvider()
        assert provider.config.chain == "arbitrum"
        assert provider.config.requests_per_minute == DEFAULT_REQUESTS_PER_MINUTE
        assert provider._owns_rate_limiter is True

    def test_init_with_custom_config(self):
        """Custom config is stored as-is."""
        config = GMXClientConfig(
            requests_per_minute=10,
            timeout_seconds=15,
            chain="avalanche",
            fallback_rate=Decimal("0.0002"),
        )
        provider = GMXFundingProvider(config=config)
        assert provider.config is config
        assert provider.config.chain == "avalanche"
        assert provider.config.fallback_rate == Decimal("0.0002")

    def test_init_with_provided_rate_limiter(self):
        """An injected rate limiter is used and not owned."""
        limiter = TokenBucketRateLimiter(requests_per_minute=5)
        provider = GMXFundingProvider(rate_limiter=limiter)
        assert provider.rate_limiter is limiter
        assert provider._owns_rate_limiter is False

    def test_supported_chains_property_returns_copy(self):
        """Mutating the returned list does not poison subsequent reads."""
        provider = GMXFundingProvider()
        chains = provider.supported_chains
        chains.append("ethereum")
        assert "ethereum" not in provider.supported_chains

    def test_for_backtest_defaults_to_arbitrum(self):
        """Factory defaults to the connector's canonical backtest chain."""
        provider = GMXFundingProvider.for_backtest(BacktestProviderConfig())
        assert provider.config.chain == "arbitrum"
        assert provider.config.fallback_rate == Decimal("0.0001")

    def test_for_backtest_uses_declared_chain_and_fallback_rate(self):
        """Declared chain config resolves through the funding-history registry."""
        provider = GMXFundingProvider.for_backtest(
            BacktestProviderConfig(
                chain="avalanche",
                funding_fallback_rate=Decimal("0"),
            )
        )
        assert provider.config.chain == "avalanche"
        assert provider.config.fallback_rate == Decimal("0")

    def test_for_backtest_warns_on_unsupported_chain(self, caplog: pytest.LogCaptureFixture):
        """Unsupported configured chains fall back visibly to Arbitrum."""
        caplog.set_level("WARNING", logger="almanak.connectors.gmx_v2.backtest_funding")

        provider = GMXFundingProvider.for_backtest(
            BacktestProviderConfig(
                chain="ethereum",
                funding_fallback_rate=Decimal("0.0007"),
            )
        )

        assert provider.config.chain == "arbitrum"
        assert provider.config.fallback_rate == Decimal("0.0007")
        assert "unsupported chain 'ethereum'" in caplog.text
        assert "falling back to arbitrum" in caplog.text

    def test_for_backtest_warns_when_declared_chain_has_no_enum(
        self,
        caplog: pytest.LogCaptureFixture,
    ):
        """A malformed manifest chain still falls back visibly instead of raising."""
        caplog.set_level("WARNING", logger="almanak.connectors.gmx_v2.backtest_funding")

        with patch.object(FundingHistoryRegistry, "declared_chains", return_value=("not_in_enum",)):
            provider = GMXFundingProvider.for_backtest(BacktestProviderConfig(chain="not_in_enum"))

        assert provider.config.chain == "arbitrum"
        assert "not a registered chain" in caplog.text
        assert "falling back to arbitrum" in caplog.text


class TestSupportedChains:
    """Tests for the manifest-derived chain set."""

    def test_supported_chains_include_required_networks(self):
        """The GMX V2 connector declares arbitrum + avalanche funding data."""
        provider = GMXFundingProvider()
        assert provider.supported_chains == ["arbitrum", "avalanche"]

    def test_unsupported_chain_is_rejected(self):
        """A chain outside the declared set fails validation."""
        provider = GMXFundingProvider()
        with pytest.raises(ValueError) as exc_info:
            provider._validate_chain("ethereum")
        assert "Unsupported chain" in str(exc_info.value)


class TestGetFundingRates:
    """Tests for the historical funding-rate grid."""

    @pytest.mark.asyncio
    async def test_get_funding_rates_success(self):
        """Measured points fill the hourly grid at HIGH confidence."""
        provider = GMXFundingProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, 2, tzinfo=UTC)

        with patch(_GATEWAY_SEAM, return_value=_points(start, ["0.0001", "0.0002", "0.0003"])) as seam:
            rates = await provider.get_funding_rates("ETH-USD", start, end)

        assert [r.rate for r in rates] == [Decimal("0.0001"), Decimal("0.0002"), Decimal("0.0003")]
        assert all(r.source_info.source == DATA_SOURCE for r in rates)
        assert all(r.source_info.confidence == DataConfidence.HIGH for r in rates)

        kwargs = seam.call_args.kwargs
        assert kwargs["venue"] == "gmx_v2"
        assert kwargs["chain"] == "arbitrum"
        assert kwargs["market"] == "ETH-USD"

    @pytest.mark.asyncio
    async def test_carry_forward_fills_gaps(self):
        """Hours without a fresh point carry the latest measured rate forward."""
        provider = GMXFundingProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, 4, tzinfo=UTC)

        with patch(_GATEWAY_SEAM, return_value=_points(start, ["0.0001", "0.0002"])):
            rates = await provider.get_funding_rates("ETH-USD", start, end)

        assert len(rates) == 5
        assert [str(r.rate) for r in rates] == ["0.0001", "0.0002", "0.0002", "0.0002", "0.0002"]

    @pytest.mark.asyncio
    async def test_hours_before_first_point_fall_back(self):
        """Grid hours before the first measured point use the fallback rate."""
        provider = GMXFundingProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, 2, tzinfo=UTC)
        late = _points(datetime(2024, 1, 1, 1, tzinfo=UTC), ["0.0005"])

        with patch(_GATEWAY_SEAM, return_value=late):
            rates = await provider.get_funding_rates("ETH-USD", start, end)

        assert rates[0].source_info.source == "fallback"
        assert rates[0].source_info.confidence == DataConfidence.LOW
        assert rates[0].rate == provider.config.fallback_rate
        assert [str(r.rate) for r in rates[1:]] == ["0.0005", "0.0005"]

    @pytest.mark.asyncio
    async def test_get_funding_rates_adds_timezone_if_missing(self):
        """Naive datetimes are treated as UTC."""
        provider = GMXFundingProvider()
        start = datetime(2024, 1, 1)  # noqa: DTZ001 - deliberate naive input
        end = datetime(2024, 1, 1, 1)  # noqa: DTZ001

        with patch(_GATEWAY_SEAM, return_value=[]):
            rates = await provider.get_funding_rates("ETH-USD", start, end)

        assert len(rates) == 2
        assert all(r.source_info.timestamp.tzinfo is not None for r in rates)


class TestErrorHandling:
    """get_funding_rates never raises — every failure degrades to fallback."""

    @pytest.mark.asyncio
    async def test_gateway_unavailable_returns_fallback(self):
        """A failed gateway round-trip yields a LOW-confidence fill."""
        provider = GMXFundingProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, 3, tzinfo=UTC)

        with patch(_GATEWAY_SEAM, side_effect=DataSourceUnavailable(source="gateway", reason="down")):
            rates = await provider.get_funding_rates("ETH-USD", start, end)

        assert len(rates) == 4
        assert all(r.source_info.source == "fallback" for r in rates)
        assert all(r.source_info.confidence == DataConfidence.LOW for r in rates)
        assert all(r.rate == provider.config.fallback_rate for r in rates)

    @pytest.mark.asyncio
    async def test_unexpected_error_returns_fallback(self):
        """Even unexpected errors degrade to the fallback fill."""
        provider = GMXFundingProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, 1, tzinfo=UTC)

        with patch(_GATEWAY_SEAM, side_effect=RuntimeError("boom")):
            rates = await provider.get_funding_rates("ETH-USD", start, end)

        assert len(rates) == 2
        assert all(r.source_info.source == "fallback" for r in rates)


class TestGetCurrentFundingRate:
    """Tests for the current-rate convenience method."""

    @pytest.mark.asyncio
    async def test_get_current_funding_rate_success(self):
        """The latest measured point in the trailing window wins."""
        provider = GMXFundingProvider()
        recent = _points(datetime(2024, 1, 1, tzinfo=UTC), ["0.0001", "0.0009"])

        with patch(_GATEWAY_SEAM, return_value=recent):
            result = await provider.get_current_funding_rate("ETH-USD")

        assert result.rate == Decimal("0.0009")
        assert result.source_info.source == DATA_SOURCE
        assert result.source_info.confidence == DataConfidence.HIGH

    @pytest.mark.asyncio
    async def test_get_current_funding_rate_no_data(self):
        """No measured point in the window degrades to fallback."""
        provider = GMXFundingProvider()

        with patch(_GATEWAY_SEAM, return_value=[]):
            result = await provider.get_current_funding_rate("ETH-USD")

        assert result.source_info.source == "fallback"
        assert result.rate == provider.config.fallback_rate

    @pytest.mark.asyncio
    async def test_chain_override_is_validated(self):
        """An undeclared chain override degrades to fallback (never raises)."""
        provider = GMXFundingProvider()

        with patch(_GATEWAY_SEAM, return_value=[]):
            result = await provider.get_current_funding_rate("ETH-USD", chain="ethereum")

        assert result.source_info.source == "fallback"


class TestContextManager:
    """Tests for async context manager support."""

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """The provider supports async-with (close is a compat no-op)."""
        async with GMXFundingProvider() as provider:
            assert provider.config.chain == "arbitrum"
        await provider.close()
