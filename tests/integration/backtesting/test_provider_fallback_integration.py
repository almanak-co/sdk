"""Integration tests for Chainlink/TWAP data provider fallback behavior.

These tests verify graceful fallback when primary data providers are unavailable:
- Chainlink provider fails (no RPC URL) -> falls back to CoinGecko
- TWAP provider fails (no RPC URL) -> falls back to CoinGecko
- Verify data_source_warnings populated correctly
- Verify backtest completes without error

Requirements:
    - No external dependencies required (uses mock providers)

To run:
    uv run pytest tests/integration/backtesting/test_provider_fallback_integration.py -v -s
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import BacktestResult
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    HistoricalDataCapability,
    HistoricalDataConfig,
    MarketState,
)
from almanak.framework.backtesting.pnl.engine import PnLBacktester
from almanak.framework.backtesting.pnl.providers.aggregated import (
    AggregatedDataProvider,
)
from almanak.framework.intents import HoldIntent

# =============================================================================
# Mock Data Providers for Fallback Testing
# =============================================================================


@dataclass
class FailingChainlinkProvider:
    """Mock Chainlink provider that always fails.

    Simulates the behavior when Chainlink provider has no RPC URL
    or RPC calls fail. Used to verify fallback behavior.
    """

    provider_name: str = "chainlink_failing"
    _chain: str = "arbitrum"

    @property
    def historical_capability(self) -> HistoricalDataCapability:
        """Return PRE_CACHE capability (what real Chainlink would return)."""
        return HistoricalDataCapability.PRE_CACHE

    async def get_price(
        self,
        token: str,
        timestamp: datetime | None = None,
        raise_on_stale: bool = True,
    ) -> Decimal:
        """Always fail - simulates missing RPC or network error."""
        raise ValueError(
            f"Chainlink provider failed: No RPC URL configured. "
            f"Cannot fetch price for {token}"
        )

    async def get_latest_price(self, token: str) -> Decimal | None:
        """Always fail - simulates missing RPC."""
        raise ValueError("Chainlink provider failed: No RPC URL configured")

    async def iterate(
        self, config: HistoricalDataConfig
    ) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Fail iteration - no data available."""
        raise ValueError("Chainlink provider cannot iterate without RPC URL")
        yield  # Make this a generator (unreachable but required for type checking)


@dataclass
class FailingTWAPProvider:
    """Mock TWAP provider that always fails.

    Simulates the behavior when TWAP provider has no RPC URL
    or cannot access on-chain oracle data.
    """

    provider_name: str = "twap_failing"
    _chain: str = "arbitrum"

    @property
    def historical_capability(self) -> HistoricalDataCapability:
        """Return CURRENT_ONLY capability (what real TWAP would return)."""
        return HistoricalDataCapability.CURRENT_ONLY

    async def get_price(
        self,
        token: str,
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Always fail - simulates missing RPC or pool not found."""
        raise ValueError(
            f"TWAP provider failed: Cannot query on-chain oracle for {token}. "
            f"No RPC URL configured or pool not found."
        )

    async def get_latest_price(self, token: str) -> Decimal | None:
        """Always fail - simulates missing RPC."""
        raise ValueError("TWAP provider failed: No RPC URL configured")

    async def iterate(
        self, config: HistoricalDataConfig
    ) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Fail iteration - no data available."""
        raise ValueError("TWAP provider cannot iterate without RPC URL")
        yield  # Make this a generator


@dataclass
class MockCoinGeckoProvider:
    """Mock CoinGecko provider that always succeeds.

    Simulates the fallback provider that returns prices from API.
    This is the expected fallback when on-chain providers fail.
    """

    provider_name: str = "coingecko_mock"
    _prices: dict[str, Decimal] = field(default_factory=dict)
    _call_count: int = 0
    _tick_count: int = 0

    def __post_init__(self) -> None:
        """Initialize default prices."""
        self._prices = {
            "WETH": Decimal("3000"),
            "ETH": Decimal("3000"),
            "USDC": Decimal("1"),
            "BTC": Decimal("45000"),
            "WBTC": Decimal("45000"),
        }

    @property
    def historical_capability(self) -> HistoricalDataCapability:
        """Return FULL capability - CoinGecko has historical data."""
        return HistoricalDataCapability.FULL

    async def get_price(
        self,
        token: str,
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Return mock price - always succeeds."""
        self._call_count += 1
        token_upper = token.upper()
        if token_upper not in self._prices:
            raise ValueError(f"Unknown token: {token}")
        return self._prices[token_upper]

    async def get_latest_price(self, token: str) -> Decimal | None:
        """Return mock price."""
        self._call_count += 1
        return self._prices.get(token.upper())

    async def iterate(
        self, config: HistoricalDataConfig
    ) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Generate market states for the backtest period."""
        current_time = config.start_time
        while current_time <= config.end_time:
            chain = config.chains[0] if config.chains else "arbitrum"
            market_state = MarketState(
                timestamp=current_time,
                prices=self._prices.copy(),
                chain=chain,
            )
            self._tick_count += 1
            yield current_time, market_state
            current_time += timedelta(seconds=config.interval_seconds)


@dataclass
class MockIteratingProvider:
    """Mock provider that successfully iterates and serves prices for fallback tests.

    Used for testing fallback behavior during backtest iteration.
    """

    provider_name: str = "mock_iterating"
    _prices: dict[str, Decimal] = field(default_factory=dict)
    _tick_count: int = 0

    def __post_init__(self) -> None:
        """Initialize default prices."""
        self._prices = {
            "WETH": Decimal("3000"),
            "ETH": Decimal("3000"),
            "USDC": Decimal("1"),
        }

    @property
    def historical_capability(self) -> HistoricalDataCapability:
        """Return FULL capability."""
        return HistoricalDataCapability.FULL

    async def get_price(
        self,
        token: str,
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Return mock price."""
        token_upper = token.upper()
        if token_upper not in self._prices:
            raise ValueError(f"Unknown token: {token}")
        return self._prices[token_upper]

    async def iterate(
        self, config: HistoricalDataConfig
    ) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Generate market states for the backtest period."""
        current_time = config.start_time
        while current_time <= config.end_time:
            chain = config.chains[0] if config.chains else "arbitrum"
            market_state = MarketState(
                timestamp=current_time,
                prices=self._prices.copy(),
                chain=chain,
            )
            self._tick_count += 1
            yield current_time, market_state
            current_time += timedelta(seconds=config.interval_seconds)


# =============================================================================
# Mock Strategy
# =============================================================================


class MockHoldStrategy:
    """Simple strategy that always returns HoldIntent.

    Used for testing provider fallback without any trading complexity.
    """

    def __init__(self, strategy_id: str = "test_provider_fallback"):
        self.strategy_id = strategy_id

    def decide(self, market: MarketState) -> HoldIntent:
        """Always return HoldIntent - no trading."""
        return HoldIntent(reason="Testing provider fallback")


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def base_config() -> PnLBacktestConfig:
    """Create a base backtest config for testing."""
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),  # 6 hour backtest
        interval_seconds=3600,  # 1 hour intervals
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        chain="arbitrum",
        institutional_mode=False,
    )


@pytest.fixture
def mock_strategy() -> MockHoldStrategy:
    """Create a mock strategy for testing."""
    return MockHoldStrategy()


@pytest.fixture
def failing_chainlink() -> FailingChainlinkProvider:
    """Create a failing Chainlink provider."""
    return FailingChainlinkProvider()


@pytest.fixture
def failing_twap() -> FailingTWAPProvider:
    """Create a failing TWAP provider."""
    return FailingTWAPProvider()


@pytest.fixture
def mock_coingecko() -> MockCoinGeckoProvider:
    """Create a mock CoinGecko provider."""
    return MockCoinGeckoProvider()


@pytest.fixture
def mock_iterating_provider() -> MockIteratingProvider:
    """Create a mock iterating provider."""
    return MockIteratingProvider()


# =============================================================================
# Test Classes: Chainlink Fallback
# =============================================================================


class TestChainlinkFallbackToCoinGecko:
    """Test graceful fallback when Chainlink provider fails."""

    @pytest.mark.asyncio
    async def test_chainlink_fails_coingecko_succeeds(
        self,
        failing_chainlink: FailingChainlinkProvider,
        mock_coingecko: MockCoinGeckoProvider,
    ) -> None:
        """Verify fallback to CoinGecko when Chainlink fails."""
        # Create aggregated provider with Chainlink first, then CoinGecko
        aggregated = AggregatedDataProvider(
            providers=[failing_chainlink, mock_coingecko],
            provider_names=["chainlink", "coingecko"],
        )

        # Should successfully get price from CoinGecko fallback
        result = await aggregated.get_price_with_source("ETH", datetime.now(UTC))

        assert result is not None
        assert result.price == Decimal("3000")
        assert result.source == "coingecko"

    @pytest.mark.asyncio
    async def test_chainlink_fallback_stats_tracked(
        self,
        failing_chainlink: FailingChainlinkProvider,
        mock_coingecko: MockCoinGeckoProvider,
    ) -> None:
        """Verify fallback statistics are tracked correctly."""
        aggregated = AggregatedDataProvider(
            providers=[failing_chainlink, mock_coingecko],
            provider_names=["chainlink", "coingecko"],
        )

        # Make several price requests
        for _ in range(3):
            await aggregated.get_price("ETH", datetime.now(UTC))

        # Check stats
        stats = aggregated.stats
        assert stats.total_requests == 3
        assert stats.provider_hits.get("coingecko", 0) == 3
        assert stats.provider_failures.get("chainlink", 0) == 3
        assert stats.fallback_count == 3

    @pytest.mark.asyncio
    async def test_chainlink_fallback_backtest_completes(
        self,
        mock_coingecko: MockCoinGeckoProvider,
        mock_strategy: MockHoldStrategy,
        base_config: PnLBacktestConfig,
    ) -> None:
        """Verify backtest completes successfully with CoinGecko fallback."""
        # Use the mock CoinGecko provider directly (simulates successful fallback)
        backtester = PnLBacktester(
            data_provider=mock_coingecko,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, base_config)

        assert result is not None
        assert result.success is True
        # Should have no compliance violations with FULL capability provider
        assert result.institutional_compliance is True


class TestChainlinkProviderWarnings:
    """Test that data_source_warnings are populated correctly."""

    @pytest.mark.asyncio
    async def test_pre_cache_provider_generates_warning(
        self,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify PRE_CACHE capability provider generates appropriate warnings."""

        @dataclass
        class PreCacheProvider:
            """Provider with PRE_CACHE capability."""

            provider_name: str = "pre_cache_provider"
            _prices: dict[str, Decimal] = field(default_factory=dict)

            def __post_init__(self) -> None:
                self._prices = {"WETH": Decimal("3000"), "USDC": Decimal("1")}

            @property
            def historical_capability(self) -> HistoricalDataCapability:
                return HistoricalDataCapability.PRE_CACHE

            async def iterate(
                self, config: HistoricalDataConfig
            ) -> AsyncIterator[tuple[datetime, MarketState]]:
                current = config.start_time
                while current <= config.end_time:
                    chain = config.chains[0] if config.chains else "arbitrum"
                    yield current, MarketState(
                        timestamp=current,
                        prices=self._prices.copy(),
                        chain=chain,
                    )
                    current += timedelta(seconds=config.interval_seconds)

        provider = PreCacheProvider()
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 3, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,
        )

        backtester = PnLBacktester(
            data_provider=provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        # PRE_CACHE should generate a warning about requiring pre-cached data
        assert len(result.data_source_warnings) > 0

        warning_text = " ".join(result.data_source_warnings)
        assert "PRE_CACHE" in warning_text or "pre-cache" in warning_text.lower()


# =============================================================================
# Test Classes: TWAP Fallback
# =============================================================================


class TestTWAPFallbackToCoinGecko:
    """Test graceful fallback when TWAP provider fails."""

    @pytest.mark.asyncio
    async def test_twap_fails_coingecko_succeeds(
        self,
        failing_twap: FailingTWAPProvider,
        mock_coingecko: MockCoinGeckoProvider,
    ) -> None:
        """Verify fallback to CoinGecko when TWAP fails."""
        # Create aggregated provider with TWAP first, then CoinGecko
        aggregated = AggregatedDataProvider(
            providers=[failing_twap, mock_coingecko],
            provider_names=["twap", "coingecko"],
        )

        # Should successfully get price from CoinGecko fallback
        result = await aggregated.get_price_with_source("WETH", datetime.now(UTC))

        assert result is not None
        assert result.price == Decimal("3000")
        assert result.source == "coingecko"

    @pytest.mark.asyncio
    async def test_twap_fallback_stats_tracked(
        self,
        failing_twap: FailingTWAPProvider,
        mock_coingecko: MockCoinGeckoProvider,
    ) -> None:
        """Verify fallback statistics are tracked for TWAP failures."""
        aggregated = AggregatedDataProvider(
            providers=[failing_twap, mock_coingecko],
            provider_names=["twap", "coingecko"],
        )

        # Make price requests
        await aggregated.get_price("ETH", datetime.now(UTC))
        await aggregated.get_price("WETH", datetime.now(UTC))

        stats = aggregated.stats
        assert stats.total_requests == 2
        assert stats.provider_hits.get("coingecko", 0) == 2
        assert stats.provider_failures.get("twap", 0) == 2
        assert stats.fallback_count == 2

    @pytest.mark.asyncio
    async def test_twap_current_only_generates_warning(
        self,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify CURRENT_ONLY capability generates data source warning."""

        @dataclass
        class CurrentOnlyProvider:
            """Provider with CURRENT_ONLY capability (like TWAP)."""

            provider_name: str = "twap_provider"
            _prices: dict[str, Decimal] = field(default_factory=dict)

            def __post_init__(self) -> None:
                self._prices = {"WETH": Decimal("3000"), "USDC": Decimal("1")}

            @property
            def historical_capability(self) -> HistoricalDataCapability:
                return HistoricalDataCapability.CURRENT_ONLY

            async def iterate(
                self, config: HistoricalDataConfig
            ) -> AsyncIterator[tuple[datetime, MarketState]]:
                current = config.start_time
                while current <= config.end_time:
                    chain = config.chains[0] if config.chains else "arbitrum"
                    yield current, MarketState(
                        timestamp=current,
                        prices=self._prices.copy(),
                        chain=chain,
                    )
                    current += timedelta(seconds=config.interval_seconds)

        provider = CurrentOnlyProvider()
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 3, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,
        )

        backtester = PnLBacktester(
            data_provider=provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        # CURRENT_ONLY should generate warning
        assert len(result.data_source_warnings) > 0

        warning_text = " ".join(result.data_source_warnings)
        assert "CURRENT_ONLY" in warning_text


# =============================================================================
# Test Classes: Combined Fallback Chain
# =============================================================================


class TestChainlinkTwapCoinGeckoFallbackChain:
    """Test the full fallback chain: Chainlink -> TWAP -> CoinGecko."""

    @pytest.mark.asyncio
    async def test_full_fallback_chain(
        self,
        failing_chainlink: FailingChainlinkProvider,
        failing_twap: FailingTWAPProvider,
        mock_coingecko: MockCoinGeckoProvider,
    ) -> None:
        """Verify fallback through entire chain when both on-chain providers fail."""
        # Full fallback chain: Chainlink -> TWAP -> CoinGecko
        aggregated = AggregatedDataProvider(
            providers=[failing_chainlink, failing_twap, mock_coingecko],
            provider_names=["chainlink", "twap", "coingecko"],
        )

        # Should successfully get price from CoinGecko after both failures
        result = await aggregated.get_price_with_source("ETH", datetime.now(UTC))

        assert result is not None
        assert result.price == Decimal("3000")
        assert result.source == "coingecko"

    @pytest.mark.asyncio
    async def test_full_fallback_chain_stats(
        self,
        failing_chainlink: FailingChainlinkProvider,
        failing_twap: FailingTWAPProvider,
        mock_coingecko: MockCoinGeckoProvider,
    ) -> None:
        """Verify stats track all providers in fallback chain."""
        aggregated = AggregatedDataProvider(
            providers=[failing_chainlink, failing_twap, mock_coingecko],
            provider_names=["chainlink", "twap", "coingecko"],
        )

        # Request price
        await aggregated.get_price("WETH", datetime.now(UTC))

        stats = aggregated.stats
        assert stats.provider_failures.get("chainlink", 0) == 1
        assert stats.provider_failures.get("twap", 0) == 1
        assert stats.provider_hits.get("coingecko", 0) == 1
        # Fallback was triggered (we fell back through the chain)
        assert stats.fallback_count == 1

    @pytest.mark.asyncio
    async def test_all_providers_fail_raises_error(
        self,
        failing_chainlink: FailingChainlinkProvider,
        failing_twap: FailingTWAPProvider,
    ) -> None:
        """Verify ValueError raised when all providers fail."""

        @dataclass
        class FailingCoinGecko:
            provider_name: str = "coingecko_failing"

            @property
            def historical_capability(self) -> HistoricalDataCapability:
                return HistoricalDataCapability.FULL

            async def get_price(self, token: str, timestamp: datetime | None = None) -> Decimal:
                raise ValueError("CoinGecko API unavailable")

        failing_coingecko = FailingCoinGecko()

        aggregated = AggregatedDataProvider(
            providers=[failing_chainlink, failing_twap, failing_coingecko],
            provider_names=["chainlink", "twap", "coingecko"],
        )

        with pytest.raises(ValueError) as exc_info:
            await aggregated.get_price("ETH", datetime.now(UTC))

        # Error message should mention all providers failed
        error_msg = str(exc_info.value)
        assert "All providers failed" in error_msg


# =============================================================================
# Test Classes: Backtest with Fallback
# =============================================================================


class TestBacktestWithProviderFallback:
    """Test that backtests complete successfully with provider fallback."""

    @pytest.mark.asyncio
    async def test_backtest_completes_with_fallback(
        self,
        mock_iterating_provider: MockIteratingProvider,
        mock_strategy: MockHoldStrategy,
        base_config: PnLBacktestConfig,
    ) -> None:
        """Verify backtest completes successfully when using fallback."""
        backtester = PnLBacktester(
            data_provider=mock_iterating_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, base_config)

        assert result is not None
        assert result.success is True
        # Should complete without errors
        assert result.error is None

    @pytest.mark.asyncio
    async def test_backtest_tracks_data_source_capabilities(
        self,
        mock_iterating_provider: MockIteratingProvider,
        mock_strategy: MockHoldStrategy,
        base_config: PnLBacktestConfig,
    ) -> None:
        """Verify data_source_capabilities populated in backtest result."""
        backtester = PnLBacktester(
            data_provider=mock_iterating_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, base_config)

        assert result is not None
        # Should have capabilities dict populated
        assert isinstance(result.data_source_capabilities, dict)
        assert len(result.data_source_capabilities) > 0

        # Should show FULL capability
        capabilities_str = str(result.data_source_capabilities)
        assert "FULL" in capabilities_str

    @pytest.mark.asyncio
    async def test_backtest_result_serialization_with_warnings(
        self,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify backtest result with warnings serializes correctly."""

        @dataclass
        class CurrentOnlyWithWarnings:
            provider_name: str = "current_only_provider"
            _prices: dict[str, Decimal] = field(default_factory=dict)

            def __post_init__(self) -> None:
                self._prices = {"WETH": Decimal("3000"), "USDC": Decimal("1")}

            @property
            def historical_capability(self) -> HistoricalDataCapability:
                return HistoricalDataCapability.CURRENT_ONLY

            async def iterate(
                self, config: HistoricalDataConfig
            ) -> AsyncIterator[tuple[datetime, MarketState]]:
                current = config.start_time
                while current <= config.end_time:
                    chain = config.chains[0] if config.chains else "arbitrum"
                    yield current, MarketState(
                        timestamp=current,
                        prices=self._prices.copy(),
                        chain=chain,
                    )
                    current += timedelta(seconds=config.interval_seconds)

        provider = CurrentOnlyWithWarnings()
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 3, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,
        )

        backtester = PnLBacktester(
            data_provider=provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)
        assert result is not None

        # Serialize and deserialize
        result_dict = result.to_dict()
        restored = BacktestResult.from_dict(result_dict)

        # Verify warnings preserved
        assert restored.data_source_warnings == result.data_source_warnings
        assert restored.data_source_capabilities == result.data_source_capabilities


# =============================================================================
# Test Classes: Edge Cases
# =============================================================================


class TestProviderFallbackEdgeCases:
    """Test edge cases in provider fallback behavior."""

    @pytest.mark.asyncio
    async def test_single_provider_no_fallback(
        self,
        mock_coingecko: MockCoinGeckoProvider,
    ) -> None:
        """Verify single provider works without fallback."""
        aggregated = AggregatedDataProvider(
            providers=[mock_coingecko],
            provider_names=["coingecko"],
        )

        result = await aggregated.get_price_with_source("ETH", datetime.now(UTC))

        assert result is not None
        assert result.price == Decimal("3000")
        assert result.source == "coingecko"
        # No fallback should be recorded
        assert aggregated.stats.fallback_count == 0

    @pytest.mark.asyncio
    async def test_first_provider_succeeds_no_fallback(
        self,
        mock_coingecko: MockCoinGeckoProvider,
        failing_twap: FailingTWAPProvider,
    ) -> None:
        """Verify no fallback when first provider succeeds."""
        # CoinGecko first (succeeds), TWAP second (would fail)
        aggregated = AggregatedDataProvider(
            providers=[mock_coingecko, failing_twap],
            provider_names=["coingecko", "twap"],
        )

        result = await aggregated.get_price_with_source("ETH", datetime.now(UTC))

        assert result is not None
        assert result.source == "coingecko"
        # Should not try TWAP since CoinGecko succeeded
        assert aggregated.stats.provider_failures.get("twap", 0) == 0
        assert aggregated.stats.fallback_count == 0

    @pytest.mark.asyncio
    async def test_unknown_token_falls_through(
        self,
        failing_chainlink: FailingChainlinkProvider,
        mock_coingecko: MockCoinGeckoProvider,
    ) -> None:
        """Verify unknown token raises error after trying all providers."""
        aggregated = AggregatedDataProvider(
            providers=[failing_chainlink, mock_coingecko],
            provider_names=["chainlink", "coingecko"],
        )

        # Request unknown token
        with pytest.raises(ValueError) as exc_info:
            await aggregated.get_price("UNKNOWN_TOKEN", datetime.now(UTC))

        # Should mention all providers failed
        error_msg = str(exc_info.value)
        assert "All providers failed" in error_msg or "Unknown token" in error_msg

    @pytest.mark.asyncio
    async def test_reset_stats_clears_history(
        self,
        failing_chainlink: FailingChainlinkProvider,
        mock_coingecko: MockCoinGeckoProvider,
    ) -> None:
        """Verify reset_stats() clears accumulated statistics."""
        aggregated = AggregatedDataProvider(
            providers=[failing_chainlink, mock_coingecko],
            provider_names=["chainlink", "coingecko"],
        )

        # Make requests
        await aggregated.get_price("ETH", datetime.now(UTC))
        await aggregated.get_price("WETH", datetime.now(UTC))

        # Verify stats accumulated
        assert aggregated.stats.total_requests == 2
        assert aggregated.stats.fallback_count == 2

        # Reset stats
        aggregated.reset_stats()

        # Verify cleared
        assert aggregated.stats.total_requests == 0
        assert aggregated.stats.fallback_count == 0
        assert len(aggregated.stats.provider_hits) == 0
        assert len(aggregated.stats.provider_failures) == 0
