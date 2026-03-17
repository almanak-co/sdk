"""Tests for funding_rate_provider wiring through IntentStrategy -> MarketSnapshot.

Validates VIB-1243: funding_rate_provider was not passed from IntentStrategy
to MarketSnapshot in create_market_snapshot(), causing market.funding_rate()
to always raise ValueError.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest


class _StubStrategy:
    """Minimal concrete IntentStrategy for testing create_market_snapshot() wiring."""

    @staticmethod
    def _create(chain="arbitrum", wallet_address="0xtest"):
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        class _Stub(IntentStrategy):
            def decide(self, market):
                return None

            def get_open_positions(self):
                return None

            def generate_teardown_intents(self, mode, market=None):
                return []

        return _Stub(config={}, chain=chain, wallet_address=wallet_address)


class TestFundingRateProviderWiring:
    """Test that IntentStrategy.create_market_snapshot() passes funding_rate_provider."""

    def test_funding_rate_provider_wired_through_create_market_snapshot(self):
        """When IntentStrategy has _funding_rate_provider set, it flows to MarketSnapshot."""
        strategy = _StubStrategy._create(chain="arbitrum")
        mock_provider = MagicMock()
        strategy._funding_rate_provider = mock_provider

        snapshot = strategy.create_market_snapshot()
        assert snapshot._funding_rate_provider is mock_provider

    def test_no_funding_rate_provider_by_default(self):
        """Without funding_rate_provider set, create_market_snapshot() still works."""
        strategy = _StubStrategy._create(chain="arbitrum")
        snapshot = strategy.create_market_snapshot()
        assert snapshot._funding_rate_provider is None

    def test_funding_rate_raises_without_provider(self):
        """market.funding_rate() raises ValueError when no provider configured."""
        strategy = _StubStrategy._create(chain="arbitrum")
        snapshot = strategy.create_market_snapshot()

        with pytest.raises(ValueError, match="No funding rate provider"):
            snapshot.funding_rate("gmx_v2", "ETH-USD")

    def test_funding_rate_calls_provider_when_wired(self):
        """market.funding_rate() delegates to provider when wired."""
        from decimal import Decimal

        from almanak.framework.data.funding import FundingRate, Venue

        strategy = _StubStrategy._create(chain="arbitrum")

        mock_rate = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.0001"),
            rate_8h=Decimal("0.0008"),
            rate_annualized=Decimal("0.0876"),
        )
        mock_provider = MagicMock()
        mock_provider.get_funding_rate = AsyncMock(return_value=mock_rate)
        strategy._funding_rate_provider = mock_provider

        snapshot = strategy.create_market_snapshot()
        result = snapshot.funding_rate("gmx_v2", "ETH-USD")

        mock_provider.get_funding_rate.assert_awaited_once()
        call_args = mock_provider.get_funding_rate.await_args.args
        assert str(call_args[0]) == "gmx_v2"
        assert call_args[1] == "ETH-USD"
        assert result.rate_hourly == Decimal("0.0001")
        assert result.venue == "gmx_v2"

    def test_funding_rate_spread_calls_provider_when_wired(self):
        """market.funding_rate_spread() delegates to provider when wired."""
        from decimal import Decimal

        from almanak.framework.data.funding import FundingRate, FundingRateSpread, Venue

        strategy = _StubStrategy._create(chain="arbitrum")

        mock_rate_a = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.0002"),
            rate_8h=Decimal("0.0016"),
            rate_annualized=Decimal("0.1752"),
        )
        mock_rate_b = FundingRate(
            venue="hyperliquid",
            market="ETH-USD",
            rate_hourly=Decimal("0.0001"),
            rate_8h=Decimal("0.0008"),
            rate_annualized=Decimal("0.0876"),
        )
        mock_spread = FundingRateSpread(
            market="ETH-USD",
            venue_a="gmx_v2",
            venue_b="hyperliquid",
            rate_a=mock_rate_a,
            rate_b=mock_rate_b,
            spread_8h=Decimal("0.0008"),
            spread_annualized=Decimal("0.0876"),
        )
        mock_provider = MagicMock()
        mock_provider.get_funding_rate_spread = AsyncMock(return_value=mock_spread)
        strategy._funding_rate_provider = mock_provider

        snapshot = strategy.create_market_snapshot()
        result = snapshot.funding_rate_spread("ETH-USD", "gmx_v2", "hyperliquid")

        mock_provider.get_funding_rate_spread.assert_awaited_once()
        assert result.market == "ETH-USD"
        assert result.spread_8h == Decimal("0.0008")

    def test_funding_rate_spread_raises_without_provider(self):
        """market.funding_rate_spread() raises ValueError when no provider configured."""
        strategy = _StubStrategy._create(chain="arbitrum")
        snapshot = strategy.create_market_snapshot()

        with pytest.raises(ValueError, match="No funding rate provider"):
            snapshot.funding_rate_spread("ETH-USD", "gmx_v2", "hyperliquid")

    def test_init_has_funding_rate_provider_attribute(self):
        """IntentStrategy.__init__() sets _funding_rate_provider to None."""
        strategy = _StubStrategy._create()
        assert hasattr(strategy, "_funding_rate_provider")
        assert strategy._funding_rate_provider is None
