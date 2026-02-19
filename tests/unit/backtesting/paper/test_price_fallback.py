"""Tests for price provider fallback chain in PaperTrader."""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.paper.config import PaperTraderConfig
from almanak.framework.backtesting.paper.engine import PaperTrader
from almanak.framework.backtesting.pnl.providers.chainlink import (
    ChainlinkStaleDataError,
)
from almanak.framework.data.interfaces import AllDataSourcesFailed
from almanak.framework.data.price.dex_twap import LowLiquidityWarning, TWAPResult


@dataclass
class MockPortfolioTracker:
    """Mock portfolio tracker for testing."""

    initial_balances: dict[str, Decimal] = field(default_factory=dict)
    current_balances: dict[str, Decimal] = field(default_factory=dict)

    def start_session(self, **kwargs: Any) -> None:
        pass

    def record_trade(self, trade: Any) -> None:
        pass


@dataclass
class MockForkManager:
    """Mock fork manager for testing."""

    is_running: bool = False
    current_block: int | None = None

    async def start(self) -> None:
        self.is_running = True
        self.current_block = 12345

    async def stop(self) -> None:
        self.is_running = False

    def get_rpc_url(self) -> str:
        return "http://localhost:8546"

    async def reset_to_latest(self) -> None:
        pass


class TestPriceSourceOrder:
    """Tests for price source fallback order configuration."""

    def test_auto_mode_sets_full_fallback_chain(self):
        """Test that 'auto' mode sets up Chainlink -> TWAP -> CoinGecko order."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="auto",
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko:
            mock_coingecko.return_value = MagicMock()
            trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

        assert trader._price_source_order == ["chainlink", "twap", "coingecko"]

    def test_coingecko_mode_sets_single_source(self):
        """Test that 'coingecko' mode uses only CoinGecko."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="coingecko",
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko:
            mock_coingecko.return_value = MagicMock()
            trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

        assert trader._price_source_order == ["coingecko"]

    def test_chainlink_mode_sets_single_source(self):
        """Test that 'chainlink' mode uses only Chainlink (with fallback to CoinGecko)."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="chainlink",
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko:
            mock_coingecko.return_value = MagicMock()
            trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

        assert trader._price_source_order == ["chainlink"]


class TestPriceFallbackBehavior:
    """Tests for price provider fallback behavior.

    Note: These tests specifically test the fallback/degraded mode behavior when
    strict_price_mode=False. Production backtests should use strict_price_mode=True.
    """

    @pytest.fixture
    def trader_with_mock_aggregator(self):
        """Create a PaperTrader with a mock price aggregator (no Chainlink/TWAP)."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="auto",
            strict_price_mode=False,  # Explicitly test fallback behavior
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko, patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
        ) as mock_chainlink, patch(
            "almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"
        ) as mock_twap:
            mock_coingecko.return_value = MagicMock()
            mock_chainlink.return_value = MagicMock()
            mock_twap.return_value = MagicMock()
            trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

        # Set providers to None so tests simulate "not initialized" state
        # Tests that want to test Chainlink/TWAP should use trader_with_mock_providers
        trader._chainlink_provider = None
        trader._twap_provider = None

        # Create a mock aggregator
        trader._price_aggregator = MagicMock()
        return trader

    @pytest.mark.asyncio
    async def test_fallback_chain_attempts_providers_in_order(
        self, trader_with_mock_aggregator, caplog
    ):
        """Test that Chainlink, TWAP, then CoinGecko are attempted in order."""
        trader = trader_with_mock_aggregator

        # Chainlink and TWAP providers not initialized (None), so should skip to CoinGecko
        trader._chainlink_provider = None
        trader._twap_provider = None

        # Mock CoinGecko success
        mock_result = MagicMock()
        mock_result.price = Decimal("3500")
        mock_result.stale = False
        mock_result.confidence = 1.0
        trader._price_aggregator.get_aggregated_price = AsyncMock(
            return_value=mock_result
        )

        # Capture logs from the specific logger
        with caplog.at_level(
            logging.DEBUG, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("ETH")

        assert price == Decimal("3500")
        # Check that Chainlink and TWAP were skipped because providers not initialized
        assert any(
            "Chainlink provider not initialized" in record.message
            for record in caplog.records
        )
        assert any(
            "TWAP provider not initialized" in record.message
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_coingecko_success_logs_provider(
        self, trader_with_mock_aggregator, caplog
    ):
        """Test that successful CoinGecko fetch logs the provider used."""
        trader = trader_with_mock_aggregator

        mock_result = MagicMock()
        mock_result.price = Decimal("3500")
        mock_result.stale = False
        mock_result.confidence = 1.0
        trader._price_aggregator.get_aggregated_price = AsyncMock(
            return_value=mock_result
        )

        # Capture logs from the specific logger
        with caplog.at_level(
            logging.INFO, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("ETH")

        assert price == Decimal("3500")
        # Check for price log with provider info (format: "Price for X: $Y (provider: Z)")
        assert any(
            "provider: coingecko" in record.message
            and "Price for" in record.message
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_stale_coingecko_data_logged(
        self, trader_with_mock_aggregator, caplog
    ):
        """Test that stale data from CoinGecko is logged with warning."""
        trader = trader_with_mock_aggregator

        mock_result = MagicMock()
        mock_result.price = Decimal("3500")
        mock_result.stale = True
        mock_result.confidence = 0.7
        trader._price_aggregator.get_aggregated_price = AsyncMock(
            return_value=mock_result
        )

        # Capture logs from the specific logger
        with caplog.at_level(
            logging.DEBUG, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("ETH")

        assert price == Decimal("3500")
        # Check warning about stale data
        assert any(
            "stale data" in record.message.lower() for record in caplog.records
        )
        # Check that "stale" is mentioned in the provider log
        assert any(
            "coingecko (stale)" in record.message
            and "Price for" in record.message
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_all_providers_fail_uses_fallback(
        self, trader_with_mock_aggregator, caplog
    ):
        """Test that when all providers fail, hardcoded fallback is used."""
        trader = trader_with_mock_aggregator

        # Mock CoinGecko failure
        trader._price_aggregator.get_aggregated_price = AsyncMock(
            side_effect=AllDataSourcesFailed(errors={"coingecko": "Rate limited"})
        )

        # Capture logs from the specific logger
        with caplog.at_level(
            logging.DEBUG, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("ETH")

        # Should fall back to hardcoded ETH price
        assert price == Decimal("3000")
        assert any(
            "All price providers failed" in record.message for record in caplog.records
        )
        assert any(
            "hardcoded_fallback" in record.message
            and "Price for" in record.message
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_stablecoins_return_one_dollar(self, trader_with_mock_aggregator):
        """Test that stablecoins always return $1 without calling providers."""
        trader = trader_with_mock_aggregator
        trader._price_aggregator.get_aggregated_price = AsyncMock(
            side_effect=Exception("Should not be called")
        )

        for stablecoin in ["USDC", "USDT", "DAI", "FRAX", "LUSD", "BUSD"]:
            price = await trader._get_token_price(stablecoin)
            assert price == Decimal("1"), f"Expected $1 for {stablecoin}"

    @pytest.mark.asyncio
    async def test_weth_maps_to_eth(self, trader_with_mock_aggregator):
        """Test that WETH is looked up as ETH."""
        trader = trader_with_mock_aggregator

        mock_result = MagicMock()
        mock_result.price = Decimal("3500")
        mock_result.stale = False
        mock_result.confidence = 1.0
        trader._price_aggregator.get_aggregated_price = AsyncMock(
            return_value=mock_result
        )

        price = await trader._get_token_price("WETH")

        assert price == Decimal("3500")
        # Verify the aggregator was called with "ETH" not "WETH"
        trader._price_aggregator.get_aggregated_price.assert_called_once_with(
            "ETH", "USD"
        )

    @pytest.mark.asyncio
    async def test_price_caching(self, trader_with_mock_aggregator):
        """Test that prices are cached and subsequent calls use cache."""
        trader = trader_with_mock_aggregator

        mock_result = MagicMock()
        mock_result.price = Decimal("3500")
        mock_result.stale = False
        mock_result.confidence = 1.0
        trader._price_aggregator.get_aggregated_price = AsyncMock(
            return_value=mock_result
        )

        # First call should hit the aggregator
        price1 = await trader._get_token_price("ETH")
        assert price1 == Decimal("3500")
        assert trader._price_aggregator.get_aggregated_price.call_count == 1

        # Second call should use cache
        price2 = await trader._get_token_price("ETH")
        assert price2 == Decimal("3500")
        assert trader._price_aggregator.get_aggregated_price.call_count == 1  # Still 1

    @pytest.mark.asyncio
    async def test_unexpected_error_continues_fallback(
        self, trader_with_mock_aggregator, caplog
    ):
        """Test that unexpected errors trigger fallback to hardcoded prices."""
        trader = trader_with_mock_aggregator

        # Mock unexpected exception
        trader._price_aggregator.get_aggregated_price = AsyncMock(
            side_effect=RuntimeError("Network error")
        )

        with caplog.at_level(logging.WARNING):
            price = await trader._get_token_price("LINK")

        # Should fall back to hardcoded LINK price
        assert price == Decimal("15")
        assert any(
            "Unexpected error" in record.message for record in caplog.records
        )


class TestSyncPriceFallback:
    """Tests for synchronous price getter."""

    def test_sync_uses_cached_price(self):
        """Test that sync getter uses cached prices."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="auto",
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko:
            mock_coingecko.return_value = MagicMock()
            trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

        # Manually populate cache
        trader._price_cache["ETH"] = Decimal("4000")

        price = trader._get_token_price_sync("ETH")
        assert price == Decimal("4000")

    def test_sync_returns_hardcoded_fallback_when_cache_empty(self):
        """Test that sync getter returns hardcoded fallback when cache is empty.

        Note: This tests fallback behavior with strict_price_mode=False.
        Production backtests should use strict_price_mode=True.
        """
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="auto",
            strict_price_mode=False,  # Explicitly test fallback behavior
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko:
            mock_coingecko.return_value = MagicMock()
            trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

        # Cache is empty, should use hardcoded fallback
        price = trader._get_token_price_sync("ETH")
        assert price == Decimal("3000")

    def test_sync_stablecoins_return_one(self):
        """Test that sync getter returns $1 for stablecoins."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="auto",
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko:
            mock_coingecko.return_value = MagicMock()
            trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

        for stablecoin in ["USDC", "USDT", "DAI"]:
            price = trader._get_token_price_sync(stablecoin)
            assert price == Decimal("1"), f"Expected $1 for {stablecoin}"


class TestChainlinkProviderIntegration:
    """Tests for Chainlink price provider integration."""

    @pytest.fixture
    def trader_with_mock_providers(self):
        """Create a PaperTrader with mock Chainlink and CoinGecko providers."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="auto",
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko, patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
        ) as mock_chainlink, patch(
            "almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"
        ) as mock_twap:
            mock_coingecko.return_value = MagicMock()
            mock_chainlink.return_value = MagicMock()
            mock_twap.return_value = MagicMock()
            trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

        # Create mock providers
        trader._chainlink_provider = MagicMock()
        trader._twap_provider = MagicMock()
        trader._price_aggregator = MagicMock()
        return trader

    @pytest.mark.asyncio
    async def test_chainlink_success_returns_price(
        self, trader_with_mock_providers, caplog
    ):
        """Test that Chainlink price is returned when available."""
        trader = trader_with_mock_providers

        # Mock Chainlink success
        trader._chainlink_provider.get_latest_price = AsyncMock(
            return_value=Decimal("3500")
        )

        with caplog.at_level(
            logging.DEBUG, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("ETH")

        assert price == Decimal("3500")
        # Verify Chainlink was used
        trader._chainlink_provider.get_latest_price.assert_called_once_with(
            "ETH", raise_on_stale=False
        )
        # CoinGecko should NOT be called
        trader._price_aggregator.get_aggregated_price.assert_not_called()

    @pytest.mark.asyncio
    async def test_chainlink_stale_data_falls_back_to_twap(
        self, trader_with_mock_providers, caplog
    ):
        """Test that stale Chainlink data triggers fallback to TWAP."""
        trader = trader_with_mock_providers

        # Mock Chainlink returning None (stale data with raise_on_stale=False)
        trader._chainlink_provider.get_latest_price = AsyncMock(return_value=None)

        # Mock TWAP success
        mock_twap_result = MagicMock(spec=TWAPResult)
        mock_twap_result.price = Decimal("3480")
        mock_twap_result.is_low_liquidity = False
        mock_twap_result.tick = 195000
        mock_twap_result.window_seconds = 300
        trader._twap_provider.calculate_twap = AsyncMock(return_value=mock_twap_result)

        with caplog.at_level(
            logging.DEBUG, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("ETH")

        assert price == Decimal("3480")
        # Verify both Chainlink and TWAP were called
        trader._chainlink_provider.get_latest_price.assert_called_once()
        trader._twap_provider.calculate_twap.assert_called_once_with(
            "ETH", raise_on_low_liquidity=False
        )
        # Check log message indicates fallback
        assert any(
            "stale or unavailable" in record.message.lower()
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_chainlink_raises_stale_error_falls_back(
        self, trader_with_mock_providers, caplog
    ):
        """Test that ChainlinkStaleDataError triggers fallback."""
        trader = trader_with_mock_providers
        from datetime import UTC, datetime

        # Mock Chainlink raising stale error
        trader._chainlink_provider.get_latest_price = AsyncMock(
            side_effect=ChainlinkStaleDataError(
                token="ETH",
                age_seconds=7200,
                heartbeat_seconds=3600,
                updated_at=datetime.now(UTC),
            )
        )

        # Mock TWAP success
        mock_twap_result = MagicMock(spec=TWAPResult)
        mock_twap_result.price = Decimal("3450")
        mock_twap_result.is_low_liquidity = False
        mock_twap_result.tick = 195000
        mock_twap_result.window_seconds = 300
        trader._twap_provider.calculate_twap = AsyncMock(return_value=mock_twap_result)

        with caplog.at_level(
            logging.DEBUG, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("ETH")

        assert price == Decimal("3450")
        assert any(
            "stale" in record.message.lower() and "chainlink" in record.message.lower()
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_chainlink_value_error_falls_back(
        self, trader_with_mock_providers, caplog
    ):
        """Test that ValueError (token not supported) triggers fallback."""
        trader = trader_with_mock_providers

        # Mock Chainlink raising ValueError (no feed for token)
        trader._chainlink_provider.get_latest_price = AsyncMock(
            side_effect=ValueError("No Chainlink feed available for OBSCURE on arbitrum")
        )

        # Mock TWAP failure too
        trader._twap_provider.calculate_twap = AsyncMock(return_value=None)

        # Mock CoinGecko success
        mock_result = MagicMock()
        mock_result.price = Decimal("0.001")
        mock_result.stale = False
        mock_result.confidence = 0.9
        trader._price_aggregator.get_aggregated_price = AsyncMock(
            return_value=mock_result
        )

        with caplog.at_level(
            logging.DEBUG, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("OBSCURE")

        assert price == Decimal("0.001")
        # Verify all three providers were attempted
        trader._chainlink_provider.get_latest_price.assert_called_once()
        trader._twap_provider.calculate_twap.assert_called_once()
        trader._price_aggregator.get_aggregated_price.assert_called_once()


class TestTWAPProviderIntegration:
    """Tests for TWAP price provider integration."""

    @pytest.fixture
    def trader_with_mock_providers(self):
        """Create a PaperTrader with mock providers."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="auto",
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko, patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
        ) as mock_chainlink, patch(
            "almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"
        ) as mock_twap:
            mock_coingecko.return_value = MagicMock()
            mock_chainlink.return_value = MagicMock()
            mock_twap.return_value = MagicMock()
            trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

        trader._chainlink_provider = MagicMock()
        trader._twap_provider = MagicMock()
        trader._price_aggregator = MagicMock()
        return trader

    @pytest.mark.asyncio
    async def test_twap_success_after_chainlink_fail(
        self, trader_with_mock_providers, caplog
    ):
        """Test that TWAP is used when Chainlink fails."""
        trader = trader_with_mock_providers

        # Mock Chainlink returning None
        trader._chainlink_provider.get_latest_price = AsyncMock(return_value=None)

        # Mock TWAP success
        mock_twap_result = MagicMock(spec=TWAPResult)
        mock_twap_result.price = Decimal("3520")
        mock_twap_result.is_low_liquidity = False
        mock_twap_result.tick = 195100
        mock_twap_result.window_seconds = 300
        trader._twap_provider.calculate_twap = AsyncMock(return_value=mock_twap_result)

        with caplog.at_level(
            logging.DEBUG, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("ARB")

        assert price == Decimal("3520")
        # CoinGecko should NOT be called
        trader._price_aggregator.get_aggregated_price.assert_not_called()

    @pytest.mark.asyncio
    async def test_twap_low_liquidity_logged(
        self, trader_with_mock_providers, caplog
    ):
        """Test that low liquidity TWAP price is logged appropriately."""
        trader = trader_with_mock_providers

        # Mock Chainlink returning None
        trader._chainlink_provider.get_latest_price = AsyncMock(return_value=None)

        # Mock TWAP with low liquidity
        mock_twap_result = MagicMock(spec=TWAPResult)
        mock_twap_result.price = Decimal("0.05")
        mock_twap_result.is_low_liquidity = True
        mock_twap_result.tick = 50000
        mock_twap_result.window_seconds = 300
        trader._twap_provider.calculate_twap = AsyncMock(return_value=mock_twap_result)

        with caplog.at_level(
            logging.INFO, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("NEWTOKEN")

        assert price == Decimal("0.05")
        # Check that provider is logged with low_liquidity note
        assert any(
            "twap (low_liquidity)" in record.message
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_twap_raises_low_liquidity_warning_falls_back(
        self, trader_with_mock_providers, caplog
    ):
        """Test that LowLiquidityWarning exception triggers fallback to CoinGecko."""
        trader = trader_with_mock_providers

        # Mock Chainlink returning None
        trader._chainlink_provider.get_latest_price = AsyncMock(return_value=None)

        # Mock TWAP raising LowLiquidityWarning
        trader._twap_provider.calculate_twap = AsyncMock(
            side_effect=LowLiquidityWarning(
                token="SMALL",
                pool_address="0x1234",
                liquidity_usd=Decimal("50000"),
                threshold_usd=Decimal("100000"),
            )
        )

        # Mock CoinGecko success
        mock_result = MagicMock()
        mock_result.price = Decimal("0.01")
        mock_result.stale = False
        mock_result.confidence = 0.8
        trader._price_aggregator.get_aggregated_price = AsyncMock(
            return_value=mock_result
        )

        with caplog.at_level(
            logging.DEBUG, logger="almanak.framework.backtesting.paper.engine"
        ):
            price = await trader._get_token_price("SMALL")

        assert price == Decimal("0.01")
        assert any(
            "low liquidity" in record.message.lower()
            for record in caplog.records
        )


class TestProviderInitialization:
    """Tests for provider initialization in _init_price_provider."""

    def test_chainlink_provider_initialized_for_supported_chain(self):
        """Test that Chainlink provider is initialized for supported chains."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="auto",
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko, patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
        ) as mock_chainlink, patch(
            "almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"
        ) as mock_twap:
            mock_coingecko.return_value = MagicMock()
            mock_chainlink.return_value = MagicMock()
            mock_twap.return_value = MagicMock()

            _trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

            # Verify Chainlink was initialized with correct chain
            mock_chainlink.assert_called_once()
            call_args = mock_chainlink.call_args
            assert call_args[1]["chain"] == "arbitrum"
            assert call_args[1]["rpc_url"] == "https://arb1.arbitrum.io/rpc"
            # _trader is used implicitly through the mocks; keep reference to avoid GC
            assert _trader._backtest_id is None  # Not run yet

    def test_twap_provider_initialized_for_supported_chain(self):
        """Test that TWAP provider is initialized for supported chains."""
        config = PaperTraderConfig(
            chain="base",
            rpc_url="https://base-mainnet.g.alchemy.com/v2/...",
            strategy_id="test",
            price_source="auto",
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko, patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
        ) as mock_chainlink, patch(
            "almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"
        ) as mock_twap:
            mock_coingecko.return_value = MagicMock()
            mock_chainlink.return_value = MagicMock()
            mock_twap.return_value = MagicMock()

            _trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

            # Verify TWAP was initialized with correct chain
            mock_twap.assert_called_once()
            call_args = mock_twap.call_args
            assert call_args[1]["chain"] == "base"
            assert call_args[1]["twap_window_seconds"] == 300  # 5 minute window
            # _trader is used implicitly through the mocks; keep reference to avoid GC
            assert _trader._backtest_id is None  # Not run yet

    def test_providers_not_initialized_for_coingecko_only_mode(self):
        """Test that Chainlink/TWAP are NOT initialized when price_source='coingecko'."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            strategy_id="test",
            price_source="coingecko",
        )
        fork_manager = MockForkManager()
        portfolio_tracker = MockPortfolioTracker()

        with patch(
            "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
        ) as mock_coingecko, patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
        ) as mock_chainlink, patch(
            "almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"
        ) as mock_twap:
            mock_coingecko.return_value = MagicMock()

            _trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=portfolio_tracker,
                config=config,
            )

            # Verify Chainlink and TWAP were NOT initialized
            mock_chainlink.assert_not_called()
            mock_twap.assert_not_called()
            # _trader is used implicitly through the mocks; keep reference to avoid GC
            assert _trader._backtest_id is None  # Not run yet
