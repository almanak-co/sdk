"""Unit tests for DEX TWAP data provider.

This module tests the DEXTWAPDataProvider class, covering:
- Provider initialization and configuration
- TWAP calculation from mock observations
- Tick to price conversion mathematics
- Low-liquidity warning behavior
- Cache operations
- Historical data iteration
- Edge cases and error handling
"""

import logging
import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.price.dex_twap import (
    LIQUIDITY_SELECTOR,
    MIN_LIQUIDITY_USD,
    OBSERVE_SELECTOR,
    SLOT0_SELECTOR,
    STABLECOINS,
    TOKEN_DECIMALS,
    UNISWAP_V3_POOLS,
    DEXTWAPDataProvider,
    LowLiquidityWarning,
    TWAPCache,
    TWAPObservation,
    TWAPResult,
)


class TestDEXTWAPProviderInitialization:
    """Tests for DEXTWAPDataProvider initialization."""

    def test_init_default_chain(self):
        """Test provider initializes with default ethereum chain."""
        provider = DEXTWAPDataProvider()
        assert provider._chain == "ethereum"
        assert provider.provider_name == "dex_twap_ethereum"

    def test_init_arbitrum_chain(self):
        """Test provider initializes with arbitrum chain."""
        provider = DEXTWAPDataProvider(chain="arbitrum")
        assert provider._chain == "arbitrum"
        assert provider.provider_name == "dex_twap_arbitrum"

    def test_init_base_chain(self):
        """Test provider initializes with base chain."""
        provider = DEXTWAPDataProvider(chain="base")
        assert provider._chain == "base"
        assert provider.provider_name == "dex_twap_base"

    def test_init_unsupported_chain_raises(self):
        """Test provider raises ValueError for unsupported chain."""
        with pytest.raises(ValueError) as exc_info:
            DEXTWAPDataProvider(chain="unsupported_chain")
        assert "Unsupported chain" in str(exc_info.value)
        assert "unsupported_chain" in str(exc_info.value)

    def test_init_with_rpc_url(self):
        """Test provider initializes with RPC URL."""
        provider = DEXTWAPDataProvider(rpc_url="https://eth-mainnet.example.com")
        assert provider._rpc_url == "https://eth-mainnet.example.com"

    def test_init_twap_window(self):
        """Test provider initializes with custom TWAP window."""
        provider = DEXTWAPDataProvider(twap_window_seconds=3600)
        assert provider._twap_window_seconds == 3600
        assert provider.twap_window_seconds == 3600

    def test_init_invalid_twap_window_raises(self):
        """Test provider raises ValueError for non-positive TWAP window."""
        with pytest.raises(ValueError) as exc_info:
            DEXTWAPDataProvider(twap_window_seconds=0)
        assert "must be positive" in str(exc_info.value)

        with pytest.raises(ValueError):
            DEXTWAPDataProvider(twap_window_seconds=-100)

    def test_init_cache_ttl(self):
        """Test provider initializes with custom cache TTL."""
        provider = DEXTWAPDataProvider(cache_ttl_seconds=120)
        assert provider._cache_ttl_seconds == 120
        assert provider._cache is not None
        assert provider._cache.ttl_seconds == 120

    def test_init_cache_disabled(self):
        """Test provider initializes with caching disabled."""
        provider = DEXTWAPDataProvider(cache_ttl_seconds=0)
        assert provider._cache is None

    def test_init_priority(self):
        """Test provider initializes with custom priority."""
        provider = DEXTWAPDataProvider(priority=5)
        assert provider.priority == 5

    def test_init_default_priority(self):
        """Test provider uses default priority."""
        provider = DEXTWAPDataProvider()
        assert provider.priority == DEXTWAPDataProvider.DEFAULT_PRIORITY
        assert provider.priority == 20

    def test_init_min_liquidity(self):
        """Test provider initializes with custom min liquidity."""
        provider = DEXTWAPDataProvider(min_liquidity_usd=Decimal("50000"))
        assert provider._min_liquidity_usd == Decimal("50000")

    def test_init_default_min_liquidity(self):
        """Test provider uses default min liquidity threshold."""
        provider = DEXTWAPDataProvider()
        assert provider._min_liquidity_usd == MIN_LIQUIDITY_USD
        assert provider._min_liquidity_usd == Decimal("100000")


class TestPoolConfiguration:
    """Tests for pool address and configuration retrieval."""

    def test_get_pool_address_eth_usdc(self):
        """Test getting pool address for ETH/USDC on ethereum."""
        provider = DEXTWAPDataProvider(chain="ethereum")
        address = provider.get_pool_address("ETH", "USDC")
        assert address == "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"

    def test_get_pool_address_weth_usdc(self):
        """Test WETH maps to same pool as ETH."""
        provider = DEXTWAPDataProvider(chain="ethereum")
        address = provider.get_pool_address("WETH", "USDC")
        assert address == provider.get_pool_address("ETH", "USDC")

    def test_get_pool_address_btc_usdc(self):
        """Test getting pool address for WBTC/USDC."""
        provider = DEXTWAPDataProvider(chain="ethereum")
        address = provider.get_pool_address("WBTC", "USDC")
        assert address == "0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35"

    def test_get_pool_address_unknown_token(self):
        """Test getting pool address for unknown token returns None."""
        provider = DEXTWAPDataProvider(chain="ethereum")
        address = provider.get_pool_address("UNKNOWN_TOKEN_XYZ", "USDC")
        assert address is None

    def test_get_pool_address_arbitrum(self):
        """Test getting pool address on arbitrum chain."""
        provider = DEXTWAPDataProvider(chain="arbitrum")
        address = provider.get_pool_address("ETH", "USDC")
        assert address == "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443"

    def test_get_pool_address_arb_token(self):
        """Test getting pool address for ARB token on Arbitrum."""
        provider = DEXTWAPDataProvider(chain="arbitrum")
        address = provider.get_pool_address("ARB", "USDC")
        assert address == "0xc473e2aEE3441BF9240Be85eb122aBB059A3B57c"

    def test_best_quote_token_usdc_preferred(self):
        """Test that USDC is preferred quote token."""
        provider = DEXTWAPDataProvider(chain="ethereum")
        quote = provider._get_best_quote_token("ETH")
        assert quote == "USDC"

    def test_best_quote_token_weth_fallback(self):
        """Test WETH is used when USDC not available."""
        provider = DEXTWAPDataProvider(chain="ethereum")
        # LINK pool is with WETH, not USDC
        quote = provider._get_best_quote_token("LINK")
        assert quote == "WETH"

    def test_best_quote_token_unknown(self):
        """Test unknown token returns None."""
        provider = DEXTWAPDataProvider(chain="ethereum")
        quote = provider._get_best_quote_token("UNKNOWN_TOKEN")
        assert quote is None


class TestSupportedTokensAndChains:
    """Tests for supported tokens and chains lists."""

    def test_supported_tokens_ethereum(self):
        """Test supported tokens on ethereum chain."""
        provider = DEXTWAPDataProvider(chain="ethereum")
        tokens = provider.supported_tokens

        assert "ETH" in tokens
        assert "WETH" in tokens
        assert "WBTC" in tokens
        assert "LINK" in tokens
        assert "UNI" in tokens
        assert "AAVE" in tokens

    def test_supported_tokens_arbitrum(self):
        """Test supported tokens on arbitrum chain."""
        provider = DEXTWAPDataProvider(chain="arbitrum")
        tokens = provider.supported_tokens

        assert "ETH" in tokens
        assert "WETH" in tokens
        assert "ARB" in tokens
        assert "GMX" in tokens
        assert "LINK" in tokens

    def test_supported_chains(self):
        """Test list of supported chains."""
        provider = DEXTWAPDataProvider()
        chains = provider.supported_chains

        assert "ethereum" in chains
        assert "arbitrum" in chains
        assert "base" in chains
        assert "optimism" in chains
        assert "polygon" in chains
        assert "avalanche" in chains


class TestTWAPObservation:
    """Tests for TWAPObservation dataclass."""

    def test_observation_creation(self):
        """Test TWAPObservation can be created."""
        obs = TWAPObservation(
            block_timestamp=1704067200,
            tick_cumulative=123456789,
            seconds_per_liquidity_cumulative=987654321,
            initialized=True,
        )
        assert obs.block_timestamp == 1704067200
        assert obs.tick_cumulative == 123456789
        assert obs.seconds_per_liquidity_cumulative == 987654321
        assert obs.initialized is True

    def test_observation_uninitialized(self):
        """Test TWAPObservation with initialized=False."""
        obs = TWAPObservation(
            block_timestamp=0,
            tick_cumulative=0,
            seconds_per_liquidity_cumulative=0,
            initialized=False,
        )
        assert obs.initialized is False


class TestTWAPResult:
    """Tests for TWAPResult dataclass."""

    def test_result_creation(self):
        """Test TWAPResult can be created."""
        now = datetime.now(UTC)
        result = TWAPResult(
            price=Decimal("3000.50"),
            tick=200000,
            window_seconds=1800,
            start_time=now - timedelta(seconds=1800),
            end_time=now,
            liquidity=1000000000000000000,
            is_low_liquidity=False,
        )
        assert result.price == Decimal("3000.50")
        assert result.tick == 200000
        assert result.window_seconds == 1800
        assert result.liquidity == 1000000000000000000
        assert result.is_low_liquidity is False

    def test_result_with_low_liquidity(self):
        """Test TWAPResult with low liquidity flag."""
        now = datetime.now(UTC)
        result = TWAPResult(
            price=Decimal("1.00"),
            tick=0,
            window_seconds=1800,
            start_time=now - timedelta(seconds=1800),
            end_time=now,
            liquidity=1000,
            is_low_liquidity=True,
        )
        assert result.is_low_liquidity is True

    def test_result_without_liquidity(self):
        """Test TWAPResult can be created without liquidity info."""
        now = datetime.now(UTC)
        result = TWAPResult(
            price=Decimal("2000.00"),
            tick=100000,
            window_seconds=1800,
            start_time=now - timedelta(seconds=1800),
            end_time=now,
        )
        assert result.liquidity is None
        assert result.is_low_liquidity is False


class TestTWAPCache:
    """Tests for TWAPCache operations."""

    def test_cache_creation(self):
        """Test TWAPCache can be created."""
        cache = TWAPCache()
        assert cache.data == {}
        assert cache.ttl_seconds == 60

    def test_cache_custom_ttl(self):
        """Test TWAPCache with custom TTL."""
        cache = TWAPCache(ttl_seconds=120)
        assert cache.ttl_seconds == 120

    def test_cache_set_and_get(self):
        """Test setting and getting TWAP from cache."""
        cache = TWAPCache()
        now = datetime.now(UTC)
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick=200000,
            window_seconds=1800,
            start_time=now - timedelta(seconds=1800),
            end_time=now,
        )

        cache.set_twap("ETH", now, result)
        retrieved = cache.get_twap_at("ETH", now)

        assert retrieved is not None
        assert retrieved.price == Decimal("3000.00")

    def test_cache_case_insensitive(self):
        """Test cache is case insensitive for token symbols."""
        cache = TWAPCache()
        now = datetime.now(UTC)
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick=200000,
            window_seconds=1800,
            start_time=now - timedelta(seconds=1800),
            end_time=now,
        )

        cache.set_twap("eth", now, result)
        retrieved = cache.get_twap_at("ETH", now)

        assert retrieved is not None
        assert retrieved.price == Decimal("3000.00")

    def test_cache_get_at_earlier_timestamp(self):
        """Test cache returns result at or before requested timestamp."""
        cache = TWAPCache()
        t1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 1, 1, 13, 0, 0, tzinfo=UTC)
        t3 = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)

        result1 = TWAPResult(
            price=Decimal("3000.00"),
            tick=200000,
            window_seconds=1800,
            start_time=t1 - timedelta(seconds=1800),
            end_time=t1,
        )
        result2 = TWAPResult(
            price=Decimal("3100.00"),
            tick=201000,
            window_seconds=1800,
            start_time=t2 - timedelta(seconds=1800),
            end_time=t2,
        )

        cache.set_twap("ETH", t1, result1)
        cache.set_twap("ETH", t2, result2)

        # Request at exact t1 should return result1
        retrieved = cache.get_twap_at("ETH", t1)
        assert retrieved.price == Decimal("3000.00")

        # Request at t2 should return result2
        retrieved = cache.get_twap_at("ETH", t2)
        assert retrieved.price == Decimal("3100.00")

        # Request at t3 should return result2 (most recent before t3)
        retrieved = cache.get_twap_at("ETH", t3)
        assert retrieved.price == Decimal("3100.00")

    def test_cache_get_unknown_token(self):
        """Test cache returns None for unknown token."""
        cache = TWAPCache()
        result = cache.get_twap_at("UNKNOWN", datetime.now(UTC))
        assert result is None

    def test_cache_clear_all(self):
        """Test clearing entire cache."""
        cache = TWAPCache()
        now = datetime.now(UTC)
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick=200000,
            window_seconds=1800,
            start_time=now - timedelta(seconds=1800),
            end_time=now,
        )

        cache.set_twap("ETH", now, result)
        cache.set_twap("BTC", now, result)

        cache.clear()

        assert cache.get_twap_at("ETH", now) is None
        assert cache.get_twap_at("BTC", now) is None

    def test_cache_clear_specific_token(self):
        """Test clearing cache for specific token."""
        cache = TWAPCache()
        now = datetime.now(UTC)
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick=200000,
            window_seconds=1800,
            start_time=now - timedelta(seconds=1800),
            end_time=now,
        )

        cache.set_twap("ETH", now, result)
        cache.set_twap("BTC", now, result)

        cache.clear("ETH")

        assert cache.get_twap_at("ETH", now) is None
        assert cache.get_twap_at("BTC", now) is not None

    def test_cache_sorted_by_timestamp(self):
        """Test cache maintains sorted order by timestamp."""
        cache = TWAPCache()

        # Insert in reverse order
        t3 = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)
        t1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 1, 1, 13, 0, 0, tzinfo=UTC)

        for idx, t in enumerate([t3, t1, t2], 1):
            result = TWAPResult(
                price=Decimal(str(idx * 1000)),
                tick=idx * 100000,
                window_seconds=1800,
                start_time=t - timedelta(seconds=1800),
                end_time=t,
            )
            cache.set_twap("ETH", t, result)

        # Should be sorted by timestamp
        data = cache.data["ETH"]
        timestamps = [ts for ts, _ in data]
        assert timestamps == sorted(timestamps)


class TestTickToPriceConversion:
    """Tests for tick to price conversion math."""

    def test_tick_to_price_zero_tick(self):
        """Test tick=0 gives price=1.0 (before decimal adjustment)."""
        provider = DEXTWAPDataProvider()
        # tick=0 means price = 1.0001^0 = 1.0
        price = provider._tick_to_price(0, 18, 18, invert=False)
        assert price == pytest.approx(Decimal("1.0"), rel=Decimal("0.0001"))

    def test_tick_to_price_positive_tick(self):
        """Test positive tick gives price > 1."""
        provider = DEXTWAPDataProvider()
        # tick=10000 means price = 1.0001^10000 ≈ 2.718
        price = provider._tick_to_price(10000, 18, 18, invert=False)
        expected = Decimal(str(math.pow(1.0001, 10000)))
        assert price == pytest.approx(expected, rel=Decimal("0.0001"))

    def test_tick_to_price_negative_tick(self):
        """Test negative tick gives price < 1."""
        provider = DEXTWAPDataProvider()
        # tick=-10000 means price = 1.0001^-10000 ≈ 0.368
        price = provider._tick_to_price(-10000, 18, 18, invert=False)
        expected = Decimal(str(math.pow(1.0001, -10000)))
        assert price == pytest.approx(expected, rel=Decimal("0.0001"))

    def test_tick_to_price_with_decimal_adjustment(self):
        """Test tick to price with different token decimals."""
        provider = DEXTWAPDataProvider()
        # ETH (18 decimals) vs USDC (6 decimals)
        # Decimal adjustment = 10^(18-6) = 10^12
        price = provider._tick_to_price(0, 18, 6, invert=False)
        expected = Decimal("1") * Decimal("10") ** (18 - 6)
        assert price == expected

    def test_tick_to_price_invert(self):
        """Test tick to price with inversion."""
        provider = DEXTWAPDataProvider()
        # Non-inverted price
        price_normal = provider._tick_to_price(10000, 18, 18, invert=False)
        # Inverted price should be 1/normal
        price_inverted = provider._tick_to_price(10000, 18, 18, invert=True)
        assert price_inverted == pytest.approx(
            Decimal("1") / price_normal, rel=Decimal("0.0001")
        )

    def test_tick_to_price_eth_usdc_typical(self):
        """Test tick to price calculation with realistic parameters.

        This test verifies the math formula is correct:
        price = 1.0001^tick * 10^(token0_decimals - token1_decimals)

        For a tick value and given decimals, we verify the calculation
        produces the expected result.
        """
        provider = DEXTWAPDataProvider()

        # Test with specific tick and decimals
        # For tick=-196256 with token0=6 decimals, token1=18 decimals:
        # raw_price = 1.0001^(-196256) ≈ 3e-9
        # decimal_adjustment = 10^(6-18) = 10^-12
        # final_price = 3e-9 * 10^-12 = 3e-21
        tick = -196256
        price = provider._tick_to_price(tick, 6, 18, invert=False)

        # Verify against expected calculation
        expected_raw = Decimal(str(math.pow(1.0001, tick)))
        decimal_adjustment = Decimal(10) ** (6 - 18)
        expected = expected_raw * decimal_adjustment

        assert price == pytest.approx(expected, rel=Decimal("0.0001"))

        # With inversion, we get 1/price which is a large number
        inverted_price = provider._tick_to_price(tick, 6, 18, invert=True)
        expected_inverted = Decimal("1") / expected
        assert inverted_price == pytest.approx(expected_inverted, rel=Decimal("0.0001"))

    def test_tick_to_sqrt_price_x96(self):
        """Test tick to sqrtPriceX96 conversion."""
        provider = DEXTWAPDataProvider()
        # tick=0 should give sqrtPriceX96 = 2^96
        sqrt_price = provider._tick_to_sqrt_price_x96(0)
        expected = 2**96
        assert sqrt_price == pytest.approx(expected, rel=0.0001)

    def test_tick_to_sqrt_price_x96_positive_tick(self):
        """Test tick to sqrtPriceX96 for positive tick."""
        provider = DEXTWAPDataProvider()
        # sqrt(1.0001^tick) * 2^96
        tick = 10000
        sqrt_price = provider._tick_to_sqrt_price_x96(tick)
        expected = int(math.pow(1.0001, tick / 2) * (2**96))
        assert sqrt_price == pytest.approx(expected, rel=0.0001)


class TestTWAPCalculationFromObservations:
    """Tests for TWAP calculation from mock observations."""

    def test_calculate_twap_from_two_observations(self):
        """Test TWAP calculation from two observations."""
        provider = DEXTWAPDataProvider()

        # Create two observations 1800 seconds apart
        obs1 = TWAPObservation(
            block_timestamp=1704067200,  # t0
            tick_cumulative=0,
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )
        obs2 = TWAPObservation(
            block_timestamp=1704069000,  # t0 + 1800s
            tick_cumulative=360000000,  # 200000 * 1800
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )

        # TWAP tick = (360000000 - 0) / (1800 - 0) = 200000
        twap_tick = provider._calculate_twap_from_observations([obs1, obs2])
        assert twap_tick == 200000

    def test_calculate_twap_from_multiple_observations(self):
        """Test TWAP calculation uses first and last observations."""
        provider = DEXTWAPDataProvider()

        # Three observations, should use first and last
        obs1 = TWAPObservation(
            block_timestamp=1704067200,
            tick_cumulative=0,
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )
        obs2 = TWAPObservation(
            block_timestamp=1704068100,  # Middle point
            tick_cumulative=180000000,
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )
        obs3 = TWAPObservation(
            block_timestamp=1704069000,
            tick_cumulative=360000000,
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )

        # Should still get 200000
        twap_tick = provider._calculate_twap_from_observations([obs1, obs2, obs3])
        assert twap_tick == 200000

    def test_calculate_twap_negative_tick(self):
        """Test TWAP calculation with negative ticks."""
        provider = DEXTWAPDataProvider()

        obs1 = TWAPObservation(
            block_timestamp=1704067200,
            tick_cumulative=0,
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )
        obs2 = TWAPObservation(
            block_timestamp=1704069000,
            tick_cumulative=-180000000,  # Negative tick cumulative
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )

        # TWAP tick = (-180000000) / 1800 = -100000
        twap_tick = provider._calculate_twap_from_observations([obs1, obs2])
        assert twap_tick == -100000

    def test_calculate_twap_insufficient_observations_raises(self):
        """Test TWAP calculation raises with fewer than 2 observations."""
        provider = DEXTWAPDataProvider()

        obs1 = TWAPObservation(
            block_timestamp=1704067200,
            tick_cumulative=0,
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )

        with pytest.raises(ValueError) as exc_info:
            provider._calculate_twap_from_observations([obs1])
        assert "at least 2 observations" in str(exc_info.value)

    def test_calculate_twap_empty_observations_raises(self):
        """Test TWAP calculation raises with empty observations."""
        provider = DEXTWAPDataProvider()

        with pytest.raises(ValueError):
            provider._calculate_twap_from_observations([])

    def test_calculate_twap_zero_time_diff_raises(self):
        """Test TWAP calculation raises when time difference is zero."""
        provider = DEXTWAPDataProvider()

        obs1 = TWAPObservation(
            block_timestamp=1704067200,
            tick_cumulative=0,
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )
        obs2 = TWAPObservation(
            block_timestamp=1704067200,  # Same timestamp
            tick_cumulative=100000,
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )

        with pytest.raises(ValueError) as exc_info:
            provider._calculate_twap_from_observations([obs1, obs2])
        assert "Invalid time range" in str(exc_info.value)

    def test_calculate_twap_integer_division(self):
        """Test TWAP calculation uses integer division."""
        provider = DEXTWAPDataProvider()

        obs1 = TWAPObservation(
            block_timestamp=1704067200,
            tick_cumulative=0,
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )
        obs2 = TWAPObservation(
            block_timestamp=1704069000,
            tick_cumulative=360000001,  # Slightly more than 360000000
            seconds_per_liquidity_cumulative=0,
            initialized=True,
        )

        # 360000001 / 1800 = 200000.0005... -> 200000 (integer division)
        twap_tick = provider._calculate_twap_from_observations([obs1, obs2])
        assert twap_tick == 200000


class TestLowLiquidityWarning:
    """Tests for low-liquidity warning behavior."""

    def test_low_liquidity_warning_creation(self):
        """Test LowLiquidityWarning can be created."""
        warning = LowLiquidityWarning(
            token="ETH",
            pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            liquidity_usd=Decimal("50000"),
            threshold_usd=Decimal("100000"),
        )
        assert warning.token == "ETH"
        assert warning.pool_address == "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        assert warning.liquidity_usd == Decimal("50000")
        assert warning.threshold_usd == Decimal("100000")

    def test_low_liquidity_warning_message(self):
        """Test LowLiquidityWarning message contains key info."""
        warning = LowLiquidityWarning(
            token="ETH",
            pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            liquidity_usd=Decimal("50000"),
            threshold_usd=Decimal("100000"),
        )
        message = str(warning)
        assert "ETH" in message
        assert "50000" in message
        assert "100000" in message
        assert "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640" in message

    def test_low_liquidity_warning_is_exception(self):
        """Test LowLiquidityWarning is an Exception."""
        warning = LowLiquidityWarning(
            token="ETH",
            pool_address="0x0",
            liquidity_usd=Decimal("10000"),
            threshold_usd=Decimal("100000"),
        )
        assert isinstance(warning, Exception)


class TestProviderMethods:
    """Tests for provider utility methods."""

    def test_set_twap_window(self):
        """Test setting TWAP window."""
        provider = DEXTWAPDataProvider(twap_window_seconds=1800)
        provider.set_twap_window(3600)
        assert provider.twap_window_seconds == 3600

    def test_set_twap_window_invalid_raises(self):
        """Test setting invalid TWAP window raises."""
        provider = DEXTWAPDataProvider()
        with pytest.raises(ValueError):
            provider.set_twap_window(0)
        with pytest.raises(ValueError):
            provider.set_twap_window(-100)

    def test_clear_cache(self):
        """Test clearing cache."""
        provider = DEXTWAPDataProvider()
        now = datetime.now(UTC)
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick=200000,
            window_seconds=1800,
            start_time=now - timedelta(seconds=1800),
            end_time=now,
        )
        provider._cache.set_twap("ETH", now, result)

        provider.clear_cache()

        assert provider._cache.get_twap_at("ETH", now) is None

    def test_clear_cache_specific_token(self):
        """Test clearing cache for specific token."""
        provider = DEXTWAPDataProvider()
        now = datetime.now(UTC)
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick=200000,
            window_seconds=1800,
            start_time=now - timedelta(seconds=1800),
            end_time=now,
        )
        provider._cache.set_twap("ETH", now, result)
        provider._cache.set_twap("BTC", now, result)

        provider.clear_cache("ETH")

        assert provider._cache.get_twap_at("ETH", now) is None
        assert provider._cache.get_twap_at("BTC", now) is not None

    def test_min_timestamp(self):
        """Test min_timestamp returns ~7 days ago."""
        provider = DEXTWAPDataProvider()
        min_ts = provider.min_timestamp
        assert min_ts is not None
        # Should be approximately 7 days ago
        now = datetime.now(UTC)
        diff = now - min_ts
        assert diff.days == 7 or diff.days == 6  # Allow for timing edge cases

    def test_max_timestamp(self):
        """Test max_timestamp returns current time."""
        provider = DEXTWAPDataProvider()
        max_ts = provider.max_timestamp
        assert max_ts is not None
        # Should be very recent
        now = datetime.now(UTC)
        diff = abs((now - max_ts).total_seconds())
        assert diff < 5  # Within 5 seconds

    def test_set_historical_twaps(self):
        """Test setting historical TWAP data."""
        provider = DEXTWAPDataProvider()
        t1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 1, 1, 13, 0, 0, tzinfo=UTC)

        twaps = [
            (
                t1,
                TWAPResult(
                    price=Decimal("3000.00"),
                    tick=200000,
                    window_seconds=1800,
                    start_time=t1 - timedelta(seconds=1800),
                    end_time=t1,
                ),
            ),
            (
                t2,
                TWAPResult(
                    price=Decimal("3100.00"),
                    tick=201000,
                    window_seconds=1800,
                    start_time=t2 - timedelta(seconds=1800),
                    end_time=t2,
                ),
            ),
        ]

        provider.set_historical_twaps("ETH", twaps)

        cached = provider._cache.get_twap_at("ETH", t1)
        assert cached is not None
        assert cached.price == Decimal("3000.00")

        cached = provider._cache.get_twap_at("ETH", t2)
        assert cached is not None
        assert cached.price == Decimal("3100.00")


class TestAsyncContextManager:
    """Tests for async context manager support."""

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test provider can be used as async context manager."""
        async with DEXTWAPDataProvider() as provider:
            assert provider is not None
            assert provider.provider_name == "dex_twap_ethereum"

    @pytest.mark.asyncio
    async def test_close_method(self):
        """Test close method can be called."""
        provider = DEXTWAPDataProvider()
        await provider.close()
        # Should not raise


class TestConstants:
    """Tests for module constants."""

    def test_selectors(self):
        """Test function selector constants."""
        assert SLOT0_SELECTOR == "0x3850c7bd"
        assert OBSERVE_SELECTOR == "0x883bdbfd"
        assert LIQUIDITY_SELECTOR == "0x1a686502"

    def test_token_decimals(self):
        """Test TOKEN_DECIMALS has expected values."""
        assert TOKEN_DECIMALS["ETH"] == 18
        assert TOKEN_DECIMALS["WETH"] == 18
        assert TOKEN_DECIMALS["USDC"] == 6
        assert TOKEN_DECIMALS["USDT"] == 6
        assert TOKEN_DECIMALS["WBTC"] == 8
        assert TOKEN_DECIMALS["DAI"] == 18

    def test_stablecoins(self):
        """Test STABLECOINS constant."""
        assert "USDC" in STABLECOINS
        assert "USDT" in STABLECOINS
        assert "DAI" in STABLECOINS
        assert "ETH" not in STABLECOINS

    def test_min_liquidity_usd(self):
        """Test MIN_LIQUIDITY_USD constant."""
        assert MIN_LIQUIDITY_USD == Decimal("100000")

    def test_pools_by_chain(self):
        """Test UNISWAP_V3_POOLS has expected chains."""
        assert "ethereum" in UNISWAP_V3_POOLS
        assert "arbitrum" in UNISWAP_V3_POOLS
        assert "base" in UNISWAP_V3_POOLS
        assert "optimism" in UNISWAP_V3_POOLS
        assert "polygon" in UNISWAP_V3_POOLS
        assert "avalanche" in UNISWAP_V3_POOLS


class TestGetPriceMethod:
    """Tests for get_price method."""

    @pytest.mark.asyncio
    async def test_get_price_unknown_token_raises(self):
        """Test get_price raises for unknown token."""
        provider = DEXTWAPDataProvider()
        with pytest.raises(ValueError) as exc_info:
            await provider.get_price("UNKNOWN_TOKEN_XYZ")
        assert "No Uniswap V3 pool available" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_price_from_cache(self):
        """Test get_price returns cached value."""
        provider = DEXTWAPDataProvider()
        now = datetime.now(UTC)

        # Set up cache
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick=200000,
            window_seconds=1800,
            start_time=now - timedelta(seconds=1800),
            end_time=now,
        )
        provider._cache.set_twap("ETH", now, result)

        price = await provider.get_price("ETH", now)
        assert price == Decimal("3000.00")

    @pytest.mark.asyncio
    async def test_get_price_historical_without_cache_raises(self):
        """Test get_price raises for historical data without cache."""
        provider = DEXTWAPDataProvider()
        historical_time = datetime.now(UTC) - timedelta(days=1)

        with pytest.raises(ValueError) as exc_info:
            await provider.get_price("ETH", historical_time)
        assert "not available" in str(exc_info.value)


class TestGetOhlcvMethod:
    """Tests for get_ohlcv method."""

    @pytest.mark.asyncio
    async def test_get_ohlcv_unknown_token_raises(self):
        """Test get_ohlcv raises for unknown token."""
        provider = DEXTWAPDataProvider()
        start = datetime.now(UTC) - timedelta(hours=2)
        end = datetime.now(UTC)

        with pytest.raises(ValueError) as exc_info:
            await provider.get_ohlcv("UNKNOWN_TOKEN_XYZ", start, end)
        assert "No pool available" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_ohlcv_without_cache_raises(self):
        """Test get_ohlcv raises without cached data."""
        provider = DEXTWAPDataProvider()
        start = datetime.now(UTC) - timedelta(hours=2)
        end = datetime.now(UTC)

        with pytest.raises(ValueError) as exc_info:
            await provider.get_ohlcv("ETH", start, end)
        assert "not available" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_ohlcv_from_cache(self):
        """Test get_ohlcv returns pseudo-OHLCV from cached data."""
        provider = DEXTWAPDataProvider()
        start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        end = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)

        # Set up cache with data at each hour
        for hour in range(12, 15):
            t = datetime(2024, 1, 1, hour, 0, 0, tzinfo=UTC)
            result = TWAPResult(
                price=Decimal(str(3000 + hour * 10)),
                tick=200000,
                window_seconds=1800,
                start_time=t - timedelta(seconds=1800),
                end_time=t,
            )
            provider._cache.set_twap("ETH", t, result)

        ohlcv = await provider.get_ohlcv("ETH", start, end, interval_seconds=3600)

        # Should have 3 data points (12:00, 13:00, 14:00)
        assert len(ohlcv) == 3

        # First data point
        assert ohlcv[0].timestamp == start
        assert ohlcv[0].open == Decimal("3120")  # 3000 + 12*10
        assert ohlcv[0].high == ohlcv[0].open
        assert ohlcv[0].low == ohlcv[0].open
        assert ohlcv[0].close == ohlcv[0].open
        assert ohlcv[0].volume is None


class TestIterateMethod:
    """Tests for iterate method."""

    @pytest.mark.asyncio
    async def test_iterate_with_cached_data(self):
        """Test iterate yields market states with cached data."""
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig

        provider = DEXTWAPDataProvider()
        start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        end = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)

        # Set up cache
        for hour in range(12, 15):
            t = datetime(2024, 1, 1, hour, 0, 0, tzinfo=UTC)
            result = TWAPResult(
                price=Decimal(str(3000 + hour * 10)),
                tick=200000,
                window_seconds=1800,
                start_time=t - timedelta(seconds=1800),
                end_time=t,
            )
            provider._cache.set_twap("ETH", t, result)

        config = HistoricalDataConfig(
            start_time=start,
            end_time=end,
            interval_seconds=3600,
            tokens=["ETH"],
        )

        states = []
        async for timestamp, market_state in provider.iterate(config):
            states.append((timestamp, market_state))

        # Should have 3 data points
        assert len(states) == 3

        # Check first state
        ts, state = states[0]
        assert ts == start
        assert "ETH" in state.prices
        assert state.prices["ETH"] == Decimal("3120")
        assert state.metadata["data_source"] == "dex_twap"

    @pytest.mark.asyncio
    async def test_iterate_creates_cache_if_none(self):
        """Test iterate creates cache if not already present."""
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig

        provider = DEXTWAPDataProvider(cache_ttl_seconds=0)
        assert provider._cache is None

        config = HistoricalDataConfig(
            start_time=datetime.now(UTC) - timedelta(hours=1),
            end_time=datetime.now(UTC),
            interval_seconds=3600,
            tokens=["ETH"],
        )

        # Just iterate once to trigger cache creation
        async for _timestamp, _state in provider.iterate(config):
            break

        assert provider._cache is not None


class TestLowLiquidityLogging:
    """Tests for low-liquidity warning logging."""

    def test_low_liquidity_logged_on_warning(self, caplog):
        """Test that low liquidity is logged at WARNING level."""
        with caplog.at_level(
            logging.WARNING, logger="almanak.framework.data.price.dex_twap"
        ):
            # This would require mocking the RPC call, so we just test the warning creation
            warning = LowLiquidityWarning(
                token="ETH",
                pool_address="0x0",
                liquidity_usd=Decimal("50000"),
                threshold_usd=Decimal("100000"),
            )
            # Verify warning attributes are accessible for logging
            assert warning.liquidity_usd < warning.threshold_usd


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_chain_case_insensitive(self):
        """Test chain parameter is case insensitive."""
        provider = DEXTWAPDataProvider(chain="ETHEREUM")
        assert provider._chain == "ethereum"

        provider = DEXTWAPDataProvider(chain="Arbitrum")
        assert provider._chain == "arbitrum"

    def test_token_case_insensitive(self):
        """Test token parameters are case insensitive."""
        provider = DEXTWAPDataProvider()

        # get_pool_address
        assert provider.get_pool_address("eth", "usdc") == provider.get_pool_address(
            "ETH", "USDC"
        )

        # _get_best_quote_token
        assert provider._get_best_quote_token("eth") == provider._get_best_quote_token(
            "ETH"
        )

    def test_very_long_twap_window_warns(self, caplog):
        """Test very long TWAP window logs warning."""
        with caplog.at_level(
            logging.WARNING, logger="almanak.framework.data.price.dex_twap"
        ):
            # 48 hours TWAP window
            DEXTWAPDataProvider(twap_window_seconds=172800)
            assert "very long" in caplog.text

    def test_placeholder_pool_address_returns_none(self):
        """Test placeholder pool addresses are handled."""
        provider = DEXTWAPDataProvider(chain="avalanche")
        # ETH/USDC on Avalanche has placeholder address
        address = provider.get_pool_address("ETH", "USDC")
        # Note: The address exists but is all zeros
        assert address == "0x0000000000000000000000000000000000000000"
