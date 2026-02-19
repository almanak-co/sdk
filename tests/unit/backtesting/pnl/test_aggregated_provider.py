"""Integration tests for AggregatedDataProvider fallback behavior.

This module tests the AggregatedDataProvider class, covering:
- Fallback when primary provider fails
- All providers failing raises ValueError
- Data source tracking (data_source field)
- Statistics tracking for provider hits, failures, and fallbacks
- Configuration-based provider creation
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.backtesting.pnl.providers.aggregated import (
    AggregatedDataProvider,
    FallbackStats,
    PriceData,
    PriceWithSource,
    ProviderConfig,
)


class MockProvider:
    """Mock data provider for testing."""

    def __init__(
        self,
        name: str,
        should_fail: bool = False,
        price: Decimal | None = None,
        is_stale: bool = False,
    ) -> None:
        """Initialize mock provider.

        Args:
            name: Provider name
            should_fail: If True, get_price raises an exception
            price: Price to return (if not failing)
            is_stale: If True, is_data_stale returns True
        """
        self._name = name
        self._should_fail = should_fail
        self._price = price if price is not None else Decimal("3000")
        self._is_stale = is_stale
        self.get_price_calls: list[tuple[str, datetime]] = []

    @property
    def provider_name(self) -> str:
        return self._name

    @property
    def supported_tokens(self) -> list[str]:
        return ["ETH", "BTC", "USDC"]

    @property
    def supported_chains(self) -> list[str]:
        return ["ethereum", "arbitrum"]

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        """Mock get_price that can fail or return a price."""
        self.get_price_calls.append((token, timestamp))
        if self._should_fail:
            raise ValueError(f"{self._name} failed to get price for {token}")
        return self._price

    def is_data_stale(self, token: str) -> bool:
        """Return whether data is stale."""
        return self._is_stale


class TestFallbackWhenPrimaryFails:
    """Tests for fallback when the primary provider fails."""

    @pytest.mark.asyncio
    async def test_fallback_to_second_provider_when_first_fails(self):
        """Test that when the first provider fails, the second is tried."""
        # Create mock providers - first fails, second succeeds
        primary = MockProvider("primary", should_fail=True)
        secondary = MockProvider("secondary", price=Decimal("2950"))

        aggregated = AggregatedDataProvider(
            providers=[primary, secondary],
            provider_names=["primary", "secondary"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await aggregated.get_price_with_source("ETH", timestamp)

        assert result.price == Decimal("2950")
        assert result.source == "secondary"
        assert len(primary.get_price_calls) == 1
        assert len(secondary.get_price_calls) == 1

    @pytest.mark.asyncio
    async def test_fallback_to_third_provider_when_first_two_fail(self):
        """Test fallback chain continues to third provider."""
        provider1 = MockProvider("provider1", should_fail=True)
        provider2 = MockProvider("provider2", should_fail=True)
        provider3 = MockProvider("provider3", price=Decimal("2900"))

        aggregated = AggregatedDataProvider(
            providers=[provider1, provider2, provider3],
            provider_names=["provider1", "provider2", "provider3"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await aggregated.get_price_with_source("ETH", timestamp)

        assert result.price == Decimal("2900")
        assert result.source == "provider3"
        # All providers should have been called
        assert len(provider1.get_price_calls) == 1
        assert len(provider2.get_price_calls) == 1
        assert len(provider3.get_price_calls) == 1

    @pytest.mark.asyncio
    async def test_fallback_stats_recorded_on_failure(self):
        """Test that fallback statistics are recorded when fallback occurs."""
        primary = MockProvider("primary", should_fail=True)
        secondary = MockProvider("secondary", price=Decimal("2950"))

        aggregated = AggregatedDataProvider(
            providers=[primary, secondary],
            provider_names=["primary", "secondary"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        await aggregated.get_price_with_source("ETH", timestamp)

        stats = aggregated.stats
        assert stats.provider_failures.get("primary", 0) == 1
        assert stats.provider_hits.get("secondary", 0) == 1
        assert stats.fallback_count == 1
        assert stats.total_requests == 1

    @pytest.mark.asyncio
    async def test_no_fallback_when_primary_succeeds(self):
        """Test that secondary provider is not called when primary succeeds."""
        primary = MockProvider("primary", price=Decimal("3000"))
        secondary = MockProvider("secondary", price=Decimal("2950"))

        aggregated = AggregatedDataProvider(
            providers=[primary, secondary],
            provider_names=["primary", "secondary"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await aggregated.get_price_with_source("ETH", timestamp)

        assert result.price == Decimal("3000")
        assert result.source == "primary"
        assert len(primary.get_price_calls) == 1
        assert len(secondary.get_price_calls) == 0  # Secondary not called

    @pytest.mark.asyncio
    async def test_fallback_when_primary_returns_zero_price(self):
        """Test fallback when primary returns invalid zero price."""
        primary = MockProvider("primary", price=Decimal("0"))
        secondary = MockProvider("secondary", price=Decimal("2950"))

        aggregated = AggregatedDataProvider(
            providers=[primary, secondary],
            provider_names=["primary", "secondary"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await aggregated.get_price_with_source("ETH", timestamp)

        assert result.price == Decimal("2950")
        assert result.source == "secondary"

    @pytest.mark.asyncio
    async def test_fallback_when_primary_returns_negative_price(self):
        """Test fallback when primary returns invalid negative price."""
        primary = MockProvider("primary", price=Decimal("-100"))
        secondary = MockProvider("secondary", price=Decimal("2950"))

        aggregated = AggregatedDataProvider(
            providers=[primary, secondary],
            provider_names=["primary", "secondary"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await aggregated.get_price_with_source("ETH", timestamp)

        assert result.price == Decimal("2950")
        assert result.source == "secondary"


class TestAllProvidersFailing:
    """Tests for when all providers fail."""

    @pytest.mark.asyncio
    async def test_all_providers_fail_raises_value_error(self):
        """Test that ValueError is raised when all providers fail."""
        provider1 = MockProvider("chainlink", should_fail=True)
        provider2 = MockProvider("coingecko", should_fail=True)

        aggregated = AggregatedDataProvider(
            providers=[provider1, provider2],
            provider_names=["chainlink", "coingecko"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        with pytest.raises(ValueError) as exc_info:
            await aggregated.get_price("ETH", timestamp)

        assert "All providers failed" in str(exc_info.value)
        assert "ETH" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_error_message_includes_all_provider_errors(self):
        """Test that error message includes details from each provider."""
        provider1 = MockProvider("chainlink", should_fail=True)
        provider2 = MockProvider("twap", should_fail=True)
        provider3 = MockProvider("coingecko", should_fail=True)

        aggregated = AggregatedDataProvider(
            providers=[provider1, provider2, provider3],
            provider_names=["chainlink", "twap", "coingecko"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        with pytest.raises(ValueError) as exc_info:
            await aggregated.get_price("ETH", timestamp)

        error_msg = str(exc_info.value)
        # All provider names should be in the error message
        assert "chainlink" in error_msg
        assert "twap" in error_msg
        assert "coingecko" in error_msg

    @pytest.mark.asyncio
    async def test_all_failures_recorded_in_stats(self):
        """Test that all failures are recorded in statistics."""
        provider1 = MockProvider("chainlink", should_fail=True)
        provider2 = MockProvider("coingecko", should_fail=True)

        aggregated = AggregatedDataProvider(
            providers=[provider1, provider2],
            provider_names=["chainlink", "coingecko"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        try:
            await aggregated.get_price("ETH", timestamp)
        except ValueError:
            pass

        stats = aggregated.stats
        assert stats.provider_failures.get("chainlink", 0) == 1
        assert stats.provider_failures.get("coingecko", 0) == 1
        assert stats.provider_hits.get("chainlink", 0) == 0
        assert stats.provider_hits.get("coingecko", 0) == 0


class TestDataSourceTracking:
    """Tests for data_source tracking in price results."""

    @pytest.mark.asyncio
    async def test_data_source_field_in_price_data(self):
        """Test that get_price_data returns PriceData with data_source field."""
        primary = MockProvider("chainlink", price=Decimal("3000"))

        aggregated = AggregatedDataProvider(
            providers=[primary],
            provider_names=["chainlink"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await aggregated.get_price_data("ETH", timestamp)

        assert isinstance(result, PriceData)
        assert result.price == Decimal("3000")
        assert result.data_source == "chainlink"
        assert result.timestamp == timestamp

    @pytest.mark.asyncio
    async def test_data_source_changes_on_fallback(self):
        """Test that data_source reflects which provider served the price."""
        primary = MockProvider("chainlink", should_fail=True)
        secondary = MockProvider("coingecko", price=Decimal("2950"))

        aggregated = AggregatedDataProvider(
            providers=[primary, secondary],
            provider_names=["chainlink", "coingecko"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await aggregated.get_price_data("ETH", timestamp)

        assert result.data_source == "coingecko"

    @pytest.mark.asyncio
    async def test_source_field_in_price_with_source(self):
        """Test that get_price_with_source returns PriceWithSource with source field."""
        primary = MockProvider("twap", price=Decimal("2980"))

        aggregated = AggregatedDataProvider(
            providers=[primary],
            provider_names=["twap"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await aggregated.get_price_with_source("ETH", timestamp)

        assert isinstance(result, PriceWithSource)
        assert result.source == "twap"
        assert result.price == Decimal("2980")

    @pytest.mark.asyncio
    async def test_stale_data_tracked_in_source(self):
        """Test that stale data flag is propagated to result."""
        primary = MockProvider("chainlink", price=Decimal("3000"), is_stale=True)

        aggregated = AggregatedDataProvider(
            providers=[primary],
            provider_names=["chainlink"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await aggregated.get_price_with_source("ETH", timestamp)

        assert result.is_stale is True

    @pytest.mark.asyncio
    async def test_data_source_serialization(self):
        """Test that PriceData serializes correctly with data_source."""
        primary = MockProvider("chainlink", price=Decimal("3000"))

        aggregated = AggregatedDataProvider(
            providers=[primary],
            provider_names=["chainlink"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await aggregated.get_price_data("ETH", timestamp)
        serialized = result.to_dict()

        assert serialized["price"] == "3000"
        assert serialized["data_source"] == "chainlink"
        assert "timestamp" in serialized


class TestFallbackStats:
    """Tests for FallbackStats tracking."""

    def test_fallback_stats_initialization(self):
        """Test FallbackStats initializes with zero counts."""
        stats = FallbackStats()
        assert stats.total_requests == 0
        assert stats.provider_hits == {}
        assert stats.provider_failures == {}
        assert stats.fallback_count == 0

    def test_record_success_increments_hits(self):
        """Test that record_success increments provider hits."""
        stats = FallbackStats()
        stats.record_success("chainlink")
        stats.record_success("chainlink")
        stats.record_success("coingecko")

        assert stats.provider_hits["chainlink"] == 2
        assert stats.provider_hits["coingecko"] == 1
        assert stats.total_requests == 3

    def test_record_failure_increments_failures(self):
        """Test that record_failure increments provider failures."""
        stats = FallbackStats()
        stats.record_failure("chainlink")
        stats.record_failure("chainlink")
        stats.record_failure("twap")

        assert stats.provider_failures["chainlink"] == 2
        assert stats.provider_failures["twap"] == 1

    def test_record_fallback_increments_count(self):
        """Test that record_fallback increments fallback count."""
        stats = FallbackStats()
        stats.record_fallback()
        stats.record_fallback()

        assert stats.fallback_count == 2

    def test_hit_rate_calculation(self):
        """Test hit rate calculation for a provider."""
        stats = FallbackStats()
        stats.record_success("chainlink")
        stats.record_success("chainlink")
        stats.record_failure("chainlink")

        hit_rate = stats.get_hit_rate("chainlink")
        assert hit_rate == pytest.approx(0.667, rel=0.01)  # 2/3

    def test_hit_rate_zero_when_no_requests(self):
        """Test hit rate is 0 when provider has no requests."""
        stats = FallbackStats()
        hit_rate = stats.get_hit_rate("unknown_provider")
        assert hit_rate == 0.0

    def test_stats_to_dict_serialization(self):
        """Test FallbackStats serialization."""
        stats = FallbackStats()
        stats.record_success("chainlink")
        stats.record_failure("twap")
        stats.record_fallback()

        serialized = stats.to_dict()

        assert serialized["total_requests"] == 1
        assert serialized["provider_hits"]["chainlink"] == 1
        assert serialized["provider_failures"]["twap"] == 1
        assert serialized["fallback_count"] == 1

    @pytest.mark.asyncio
    async def test_stats_reset(self):
        """Test that reset_stats clears all statistics."""
        primary = MockProvider("primary", should_fail=True)
        secondary = MockProvider("secondary", price=Decimal("2950"))

        aggregated = AggregatedDataProvider(
            providers=[primary, secondary],
            provider_names=["primary", "secondary"],
        )

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        await aggregated.get_price_with_source("ETH", timestamp)

        # Stats should have data
        assert aggregated.stats.total_requests > 0

        # Reset stats
        aggregated.reset_stats()

        # Stats should be cleared
        assert aggregated.stats.total_requests == 0
        assert aggregated.stats.fallback_count == 0


class TestProviderConfig:
    """Tests for ProviderConfig dataclass."""

    def test_provider_config_creation(self):
        """Test creating ProviderConfig with all fields."""
        config = ProviderConfig(
            provider_type="chainlink",
            chain="arbitrum",
            rpc_url="https://arb-mainnet.example.com",
            api_key="test_key",
            cache_ttl_seconds=120,
            priority=10,
        )

        assert config.provider_type == "chainlink"
        assert config.chain == "arbitrum"
        assert config.rpc_url == "https://arb-mainnet.example.com"
        assert config.api_key == "test_key"
        assert config.cache_ttl_seconds == 120
        assert config.priority == 10

    def test_provider_config_defaults(self):
        """Test ProviderConfig default values."""
        config = ProviderConfig(provider_type="coingecko")

        assert config.chain == "arbitrum"
        assert config.rpc_url == ""
        assert config.api_key == ""
        assert config.cache_ttl_seconds == 60
        assert config.priority is None

    def test_provider_config_to_dict_masks_api_key(self):
        """Test that API key is masked in serialization."""
        config = ProviderConfig(
            provider_type="coingecko",
            api_key="secret_key_12345",
        )

        serialized = config.to_dict()

        assert serialized["api_key"] == "***"

    def test_provider_config_to_dict_empty_api_key(self):
        """Test that empty API key serializes to None."""
        config = ProviderConfig(provider_type="chainlink")

        serialized = config.to_dict()

        assert serialized["api_key"] is None

    def test_provider_config_from_dict(self):
        """Test creating ProviderConfig from dictionary."""
        data = {
            "provider_type": "twap",
            "chain": "base",
            "rpc_url": "https://base.example.com",
            "cache_ttl_seconds": 90,
        }

        config = ProviderConfig.from_dict(data)

        assert config.provider_type == "twap"
        assert config.chain == "base"
        assert config.rpc_url == "https://base.example.com"
        assert config.cache_ttl_seconds == 90


class TestAggregatedProviderInitialization:
    """Tests for AggregatedDataProvider initialization."""

    def test_init_requires_at_least_one_provider(self):
        """Test that initialization fails with empty provider list."""
        with pytest.raises(ValueError) as exc_info:
            AggregatedDataProvider(providers=[])

        assert "At least one provider is required" in str(exc_info.value)

    def test_init_provider_names_must_match_providers(self):
        """Test that provider_names count must match providers count."""
        provider1 = MockProvider("p1")
        provider2 = MockProvider("p2")

        with pytest.raises(ValueError) as exc_info:
            AggregatedDataProvider(
                providers=[provider1, provider2],
                provider_names=["only_one"],
            )

        assert "Number of names" in str(exc_info.value)

    def test_init_auto_generates_names_from_provider(self):
        """Test that names are auto-generated from provider_name property."""
        provider1 = MockProvider("chainlink")
        provider2 = MockProvider("coingecko")

        aggregated = AggregatedDataProvider(
            providers=[provider1, provider2],
            # No provider_names specified
        )

        assert aggregated.provider_names == ["chainlink", "coingecko"]

    def test_init_explicit_names_override_provider_names(self):
        """Test that explicit names override provider_name property."""
        provider1 = MockProvider("internal_name_1")
        provider2 = MockProvider("internal_name_2")

        aggregated = AggregatedDataProvider(
            providers=[provider1, provider2],
            provider_names=["custom_name_1", "custom_name_2"],
        )

        assert aggregated.provider_names == ["custom_name_1", "custom_name_2"]


class TestAggregatedProviderProperties:
    """Tests for AggregatedDataProvider properties."""

    def test_provider_name_property(self):
        """Test provider_name returns 'aggregated'."""
        provider = MockProvider("test")
        aggregated = AggregatedDataProvider(providers=[provider])

        assert aggregated.provider_name == "aggregated"

    def test_supported_tokens_union(self):
        """Test supported_tokens is union of all providers."""
        # Create simple mock objects with supported_tokens attributes
        provider1 = MagicMock()
        provider1.provider_name = "p1"
        provider1.supported_tokens = ["ETH", "BTC"]

        provider2 = MagicMock()
        provider2.provider_name = "p2"
        provider2.supported_tokens = ["BTC", "USDC", "LINK"]

        aggregated = AggregatedDataProvider(providers=[provider1, provider2])
        tokens = aggregated.supported_tokens

        assert "ETH" in tokens
        assert "BTC" in tokens
        assert "USDC" in tokens
        assert "LINK" in tokens

    def test_providers_returns_copy(self):
        """Test that providers property returns a copy."""
        provider = MockProvider("test")
        aggregated = AggregatedDataProvider(providers=[provider])

        providers = aggregated.providers
        providers.append(MockProvider("extra"))

        # Original should be unchanged
        assert len(aggregated.providers) == 1


class TestLoggingBehavior:
    """Tests for logging behavior during fallback."""

    @pytest.mark.asyncio
    async def test_info_log_on_fallback(self, caplog):
        """Test that INFO level log is emitted on fallback."""
        import logging

        primary = MockProvider("primary", should_fail=True)
        secondary = MockProvider("secondary", price=Decimal("2950"))

        aggregated = AggregatedDataProvider(
            providers=[primary, secondary],
            provider_names=["primary", "secondary"],
        )

        with caplog.at_level(
            logging.INFO, logger="almanak.framework.backtesting.pnl.providers.aggregated"
        ):
            timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
            await aggregated.get_price_with_source("ETH", timestamp)

        # Should have an INFO log about fallback
        info_logs = [r for r in caplog.records if r.levelno == logging.INFO]
        assert any("fallback" in r.message.lower() for r in info_logs)

    @pytest.mark.asyncio
    async def test_debug_log_on_primary_success(self, caplog):
        """Test that DEBUG level log is emitted when primary succeeds."""
        import logging

        primary = MockProvider("primary", price=Decimal("3000"))

        aggregated = AggregatedDataProvider(
            providers=[primary],
            provider_names=["primary"],
        )

        with caplog.at_level(
            logging.DEBUG,
            logger="almanak.framework.backtesting.pnl.providers.aggregated",
        ):
            timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
            await aggregated.get_price_with_source("ETH", timestamp)

        # Should have a DEBUG log about the price
        debug_logs = [r for r in caplog.records if r.levelno == logging.DEBUG]
        assert any("primary" in r.message.lower() for r in debug_logs)


class TestAsyncContextManager:
    """Tests for async context manager support."""

    @pytest.mark.asyncio
    async def test_context_manager_entry_returns_self(self):
        """Test that entering context returns the provider."""
        provider = MockProvider("test")
        aggregated = AggregatedDataProvider(providers=[provider])

        async with aggregated as agg:
            assert agg is aggregated

    @pytest.mark.asyncio
    async def test_context_manager_closes_providers(self):
        """Test that exiting context calls close on providers."""
        provider = MockProvider("test")
        provider.close = AsyncMock()

        aggregated = AggregatedDataProvider(providers=[provider])

        async with aggregated:
            pass

        provider.close.assert_called_once()


__all__ = [
    "TestFallbackWhenPrimaryFails",
    "TestAllProvidersFailing",
    "TestDataSourceTracking",
    "TestFallbackStats",
    "TestProviderConfig",
    "TestAggregatedProviderInitialization",
    "TestAggregatedProviderProperties",
    "TestLoggingBehavior",
    "TestAsyncContextManager",
]
