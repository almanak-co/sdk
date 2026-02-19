"""Unit tests for TWAP (Time-Weighted Average Price) data provider.

This module tests the TWAPDataProvider class in providers/twap.py, covering:
- Provider initialization and configuration
- TWAP calculation from tick cumulatives
- Tick to price conversion mathematics
- Pool address registry
- Cache operations
- Error handling for insufficient history
"""

import math
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.providers.twap import (
    ARBITRUM_POOLS,
    ARCHIVE_RPC_CHAINS,
    ARCHIVE_RPC_URL_ENV_PATTERN,
    DEFAULT_TWAP_WINDOW_SECONDS,
    ETHEREUM_POOLS,
    OBSERVE_SELECTOR,
    TOKEN_TO_POOL,
    UNISWAP_V3_POOLS,
    CachedTWAP,
    TWAPDataProvider,
    TWAPInsufficientHistoryError,
    TWAPObservation,
    TWAPPoolNotFoundError,
    TWAPResult,
)


class TestTWAPProviderInitialization:
    """Tests for TWAPDataProvider initialization."""

    def test_init_default_chain(self):
        """Test provider initializes with default arbitrum chain."""
        provider = TWAPDataProvider()
        assert provider._chain == "arbitrum"
        assert provider.provider_name == "twap_arbitrum"

    def test_init_ethereum_chain(self):
        """Test provider initializes with ethereum chain."""
        provider = TWAPDataProvider(chain="ethereum")
        assert provider._chain == "ethereum"
        assert provider.provider_name == "twap_ethereum"

    def test_init_base_chain(self):
        """Test provider initializes with base chain."""
        provider = TWAPDataProvider(chain="base")
        assert provider._chain == "base"
        assert provider.provider_name == "twap_base"

    def test_init_unsupported_chain_raises(self):
        """Test provider raises ValueError for unsupported chain."""
        with pytest.raises(ValueError) as exc_info:
            TWAPDataProvider(chain="unsupported_chain")
        assert "Unsupported chain" in str(exc_info.value)
        assert "unsupported_chain" in str(exc_info.value)

    def test_init_with_rpc_url(self):
        """Test provider initializes with RPC URL."""
        provider = TWAPDataProvider(rpc_url="https://eth-mainnet.example.com")
        assert provider._rpc_url == "https://eth-mainnet.example.com"

    def test_init_with_archive_rpc_env_var(self, monkeypatch):
        """Test provider uses ARCHIVE_RPC_URL_{CHAIN} env var when no rpc_url provided."""
        archive_url = "https://archive.arbitrum.example.com"
        monkeypatch.setenv("ARCHIVE_RPC_URL_ARBITRUM", archive_url)

        provider = TWAPDataProvider(chain="arbitrum")  # No rpc_url specified
        assert provider._rpc_url == archive_url

    def test_init_explicit_rpc_url_overrides_env_var(self, monkeypatch):
        """Test explicit rpc_url parameter takes precedence over env var."""
        monkeypatch.setenv("ARCHIVE_RPC_URL_ARBITRUM", "https://archive.arbitrum.example.com")

        explicit_url = "https://explicit.arbitrum.example.com"
        provider = TWAPDataProvider(chain="arbitrum", rpc_url=explicit_url)
        assert provider._rpc_url == explicit_url

    def test_init_ethereum_archive_rpc_env_var(self, monkeypatch):
        """Test provider uses ARCHIVE_RPC_URL_ETHEREUM env var for Ethereum chain."""
        archive_url = "https://archive.ethereum.example.com"
        monkeypatch.setenv("ARCHIVE_RPC_URL_ETHEREUM", archive_url)

        provider = TWAPDataProvider(chain="ethereum")
        assert provider._rpc_url == archive_url

    def test_init_observation_window(self):
        """Test provider initializes with custom observation window."""
        provider = TWAPDataProvider(observation_window_seconds=600)
        assert provider._observation_window_seconds == 600
        assert provider.observation_window_seconds == 600

    def test_init_default_observation_window(self):
        """Test provider uses default 1800s (30 minute) observation window."""
        provider = TWAPDataProvider()
        assert provider.observation_window_seconds == 1800

    def test_init_cache_ttl(self):
        """Test provider initializes with custom cache TTL."""
        provider = TWAPDataProvider(cache_ttl_seconds=120)
        assert provider._cache_ttl_seconds == 120

    def test_init_priority(self):
        """Test provider initializes with custom priority."""
        provider = TWAPDataProvider(priority=5)
        assert provider.priority == 5

    def test_init_default_priority(self):
        """Test provider uses default priority of 20."""
        provider = TWAPDataProvider()
        assert provider.priority == TWAPDataProvider.DEFAULT_PRIORITY
        assert provider.priority == 20

    def test_chain_case_insensitive(self):
        """Test chain parameter is case insensitive."""
        provider = TWAPDataProvider(chain="ETHEREUM")
        assert provider._chain == "ethereum"

        provider = TWAPDataProvider(chain="Arbitrum")
        assert provider._chain == "arbitrum"


class TestPoolConfiguration:
    """Tests for pool address and configuration retrieval."""

    def test_get_pool_address_eth_ethereum(self):
        """Test getting pool address for ETH on ethereum."""
        provider = TWAPDataProvider(chain="ethereum")
        address = provider.get_pool_address("ETH")
        assert address == "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"

    def test_get_pool_address_weth(self):
        """Test WETH maps to same pool as ETH."""
        provider = TWAPDataProvider(chain="ethereum")
        assert provider.get_pool_address("WETH") == provider.get_pool_address("ETH")

    def test_get_pool_address_arbitrum(self):
        """Test getting pool address on arbitrum chain."""
        provider = TWAPDataProvider(chain="arbitrum")
        address = provider.get_pool_address("ETH")
        assert address == "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443"

    def test_get_pool_address_unknown_token(self):
        """Test getting pool address for unknown token returns None."""
        provider = TWAPDataProvider(chain="ethereum")
        address = provider.get_pool_address("UNKNOWN_TOKEN_XYZ")
        assert address is None

    def test_get_pool_key_eth(self):
        """Test getting pool key for ETH."""
        provider = TWAPDataProvider(chain="ethereum")
        pool_key = provider.get_pool_key("ETH")
        assert pool_key == "WETH/USDC-500"

    def test_get_pool_key_arb(self):
        """Test getting pool key for ARB on Arbitrum."""
        provider = TWAPDataProvider(chain="arbitrum")
        pool_key = provider.get_pool_key("ARB")
        assert pool_key == "ARB/WETH-3000"


class TestSupportedTokensAndChains:
    """Tests for supported tokens and chains lists."""

    def test_supported_tokens_ethereum(self):
        """Test supported tokens on ethereum chain."""
        provider = TWAPDataProvider(chain="ethereum")
        tokens = provider.supported_tokens

        assert "ETH" in tokens
        assert "WETH" in tokens
        assert "BTC" in tokens or "WBTC" in tokens

    def test_supported_tokens_arbitrum(self):
        """Test supported tokens on arbitrum chain."""
        provider = TWAPDataProvider(chain="arbitrum")
        tokens = provider.supported_tokens

        assert "ETH" in tokens
        assert "WETH" in tokens
        assert "ARB" in tokens
        assert "GMX" in tokens

    def test_supported_chains(self):
        """Test list of supported chains."""
        provider = TWAPDataProvider()
        chains = provider.supported_chains

        assert "ethereum" in chains
        assert "arbitrum" in chains
        assert "base" in chains
        assert "optimism" in chains
        assert "polygon" in chains


class TestTWAPObservation:
    """Tests for TWAPObservation dataclass."""

    def test_observation_creation(self):
        """Test TWAPObservation can be created."""
        obs = TWAPObservation(
            tick_cumulative=123456789,
            seconds_per_liquidity_cumulative_x128=987654321,
        )
        assert obs.tick_cumulative == 123456789
        assert obs.seconds_per_liquidity_cumulative_x128 == 987654321

    def test_observation_with_timestamp(self):
        """Test TWAPObservation with timestamp."""
        now = datetime.now(UTC)
        obs = TWAPObservation(
            tick_cumulative=0,
            seconds_per_liquidity_cumulative_x128=0,
            timestamp=now,
        )
        assert obs.timestamp == now


class TestTWAPResult:
    """Tests for TWAPResult dataclass."""

    def test_result_creation(self):
        """Test TWAPResult can be created."""
        result = TWAPResult(
            price=Decimal("3000.50"),
            tick_twap=200000,
            observation_window_seconds=300,
            pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            token0_is_base=True,
        )
        assert result.price == Decimal("3000.50")
        assert result.tick_twap == 200000
        assert result.observation_window_seconds == 300
        assert result.token0_is_base is True


class TestCachedTWAP:
    """Tests for CachedTWAP dataclass."""

    def test_cached_twap_creation(self):
        """Test CachedTWAP can be created."""
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick_twap=200000,
            observation_window_seconds=300,
            pool_address="0x0",
            token0_is_base=True,
        )
        cached = CachedTWAP(
            price=Decimal("3000.00"),
            result=result,
            ttl_seconds=60,
        )
        assert cached.price == Decimal("3000.00")
        assert cached.ttl_seconds == 60

    def test_cached_twap_is_expired(self):
        """Test CachedTWAP expiration check."""
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick_twap=200000,
            observation_window_seconds=300,
            pool_address="0x0",
            token0_is_base=True,
        )
        cached = CachedTWAP(
            price=Decimal("3000.00"),
            result=result,
            ttl_seconds=60,
        )
        # Fresh cache should not be expired
        assert cached.is_expired is False

    def test_cached_twap_age_seconds(self):
        """Test CachedTWAP age calculation."""
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick_twap=200000,
            observation_window_seconds=300,
            pool_address="0x0",
            token0_is_base=True,
        )
        cached = CachedTWAP(
            price=Decimal("3000.00"),
            result=result,
            ttl_seconds=60,
        )
        # Fresh cache should have very small age
        assert cached.age_seconds < 1


class TestTickToPriceConversion:
    """Tests for tick to price conversion math.

    TWAP formula: price = 1.0001^tick
    With decimal adjustment: price = 1.0001^tick * 10^(token0_decimals - token1_decimals)
    """

    def test_tick_to_price_zero_tick(self):
        """Test tick=0 gives price=1.0 (before decimal adjustment)."""
        provider = TWAPDataProvider()
        # tick=0 means price = 1.0001^0 = 1.0
        price = provider._tick_to_price(0, 18, 18, invert=False)
        assert price == pytest.approx(Decimal("1.0"), rel=Decimal("0.0001"))

    def test_tick_to_price_positive_tick(self):
        """Test positive tick gives price > 1."""
        provider = TWAPDataProvider()
        # tick=10000 means price = 1.0001^10000 ≈ 2.718
        price = provider._tick_to_price(10000, 18, 18, invert=False)
        expected = Decimal(str(math.pow(1.0001, 10000)))
        assert price == pytest.approx(expected, rel=Decimal("0.0001"))

    def test_tick_to_price_negative_tick(self):
        """Test negative tick gives price < 1."""
        provider = TWAPDataProvider()
        # tick=-10000 means price = 1.0001^-10000 ≈ 0.368
        price = provider._tick_to_price(-10000, 18, 18, invert=False)
        expected = Decimal(str(math.pow(1.0001, -10000)))
        assert price == pytest.approx(expected, rel=Decimal("0.0001"))

    def test_tick_to_price_with_decimal_adjustment(self):
        """Test tick to price with different token decimals.

        ETH (18 decimals) vs USDC (6 decimals)
        Decimal adjustment = 10^(18-6) = 10^12
        """
        provider = TWAPDataProvider()
        price = provider._tick_to_price(0, 18, 6, invert=False)
        expected = Decimal("1") * Decimal("10") ** (18 - 6)
        assert price == expected

    def test_tick_to_price_invert(self):
        """Test tick to price with inversion."""
        provider = TWAPDataProvider()
        # Non-inverted price
        price_normal = provider._tick_to_price(10000, 18, 18, invert=False)
        # Inverted price should be 1/normal
        price_inverted = provider._tick_to_price(10000, 18, 18, invert=True)
        assert price_inverted == pytest.approx(
            Decimal("1") / price_normal, rel=Decimal("0.0001")
        )

    def test_tick_to_price_realistic_eth_usdc(self):
        """Test tick to price with realistic ETH/USDC parameters.

        For WETH/USDC pool (token0=WETH 18 decimals, token1=USDC 6 decimals):
        - A tick around 200000 corresponds to ~$3000 ETH
        - The formula: price = 1.0001^tick * 10^(18-6)
        """
        provider = TWAPDataProvider()

        # WETH/USDC tick for ~$3000 ETH price
        # 1.0001^tick * 10^12 = 3000
        # tick ≈ 80000 (approx)
        tick = 80000
        price = provider._tick_to_price(tick, 18, 6, invert=False)

        # Verify calculation
        expected_raw = Decimal(str(math.pow(1.0001, tick)))
        decimal_adjustment = Decimal(10) ** (18 - 6)
        expected = expected_raw * decimal_adjustment

        assert price == pytest.approx(expected, rel=Decimal("0.0001"))


class TestTWAPCalculationFromObservations:
    """Tests for TWAP calculation from tick cumulative observations."""

    def test_calculate_twap_from_two_observations(self):
        """Test TWAP calculation from two observations.

        TWAP tick = (tickCumulative_now - tickCumulative_ago) / seconds
        """
        provider = TWAPDataProvider()

        # Create observations 300 seconds apart (5 minutes)
        # Tick cumulative: 200000 * 300 = 60000000
        obs1 = TWAPObservation(
            tick_cumulative=0,
            seconds_per_liquidity_cumulative_x128=0,
        )
        obs2 = TWAPObservation(
            tick_cumulative=60000000,  # 200000 tick * 300 seconds
            seconds_per_liquidity_cumulative_x128=0,
        )

        # Calculate TWAP
        # (60000000 - 0) / 300 = 200000
        price, tick_twap = provider._calculate_twap_from_observations(
            [obs1, obs2],
            seconds_elapsed=300,
            token0_decimals=18,
            token1_decimals=6,
            invert=False,
        )

        assert tick_twap == 200000

    def test_calculate_twap_negative_tick(self):
        """Test TWAP calculation with negative ticks."""
        provider = TWAPDataProvider()

        obs1 = TWAPObservation(
            tick_cumulative=0,
            seconds_per_liquidity_cumulative_x128=0,
        )
        obs2 = TWAPObservation(
            tick_cumulative=-30000000,  # -100000 tick * 300 seconds
            seconds_per_liquidity_cumulative_x128=0,
        )

        price, tick_twap = provider._calculate_twap_from_observations(
            [obs1, obs2],
            seconds_elapsed=300,
            token0_decimals=18,
            token1_decimals=6,
            invert=False,
        )

        assert tick_twap == -100000

    def test_calculate_twap_insufficient_observations_raises(self):
        """Test TWAP calculation raises with fewer than 2 observations."""
        provider = TWAPDataProvider()

        obs1 = TWAPObservation(
            tick_cumulative=0,
            seconds_per_liquidity_cumulative_x128=0,
        )

        with pytest.raises(ValueError) as exc_info:
            provider._calculate_twap_from_observations(
                [obs1],
                seconds_elapsed=300,
                token0_decimals=18,
                token1_decimals=6,
                invert=False,
            )
        assert "at least 2 observations" in str(exc_info.value)

    def test_calculate_twap_integer_division(self):
        """Test TWAP calculation uses integer division."""
        provider = TWAPDataProvider()

        obs1 = TWAPObservation(tick_cumulative=0, seconds_per_liquidity_cumulative_x128=0)
        obs2 = TWAPObservation(
            tick_cumulative=60000001,  # Slightly more than 60000000
            seconds_per_liquidity_cumulative_x128=0,
        )

        # 60000001 // 300 = 200000 (integer division)
        price, tick_twap = provider._calculate_twap_from_observations(
            [obs1, obs2],
            seconds_elapsed=300,
            token0_decimals=18,
            token1_decimals=6,
            invert=False,
        )

        assert tick_twap == 200000


class TestTokenBaseDetection:
    """Tests for determining if token is base (token0) in pool."""

    def test_is_token_base_eth_in_weth_usdc(self):
        """Test ETH is base token in WETH/USDC pool."""
        provider = TWAPDataProvider()
        assert provider._is_token_base("WETH/USDC-500", "ETH") is True
        assert provider._is_token_base("WETH/USDC-500", "WETH") is True

    def test_is_token_base_usdc_not_base(self):
        """Test USDC is not base token in WETH/USDC pool."""
        provider = TWAPDataProvider()
        # USDC is token1 in WETH/USDC pool
        assert provider._is_token_base("WETH/USDC-500", "USDC") is False

    def test_is_token_base_arb_in_arb_weth(self):
        """Test ARB is base in ARB/WETH pool."""
        provider = TWAPDataProvider()
        assert provider._is_token_base("ARB/WETH-3000", "ARB") is True


class TestExceptions:
    """Tests for TWAP-specific exceptions."""

    def test_twap_insufficient_history_error(self):
        """Test TWAPInsufficientHistoryError creation and message."""
        error = TWAPInsufficientHistoryError(
            token="ETH",
            pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            requested_seconds=300,
            available_seconds=100,
        )
        assert error.token == "ETH"
        assert error.pool_address == "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        assert error.requested_seconds == 300
        assert error.available_seconds == 100
        assert "insufficient history" in str(error)

    def test_twap_pool_not_found_error(self):
        """Test TWAPPoolNotFoundError creation and message."""
        error = TWAPPoolNotFoundError(
            token="UNKNOWN_TOKEN",
            chain="ethereum",
        )
        assert error.token == "UNKNOWN_TOKEN"
        assert error.chain == "ethereum"
        assert "No TWAP pool available" in str(error)


class TestStablecoinPricing:
    """Tests for stablecoin pricing."""

    @pytest.mark.asyncio
    async def test_stablecoin_returns_one(self):
        """Test stablecoins return $1 price."""
        provider = TWAPDataProvider()

        for stable in ["USDC", "USDT", "DAI", "FRAX"]:
            price = await provider.get_latest_price(stable)
            assert price == Decimal("1")

    def test_stablecoin_sync_returns_one(self):
        """Test stablecoins return $1 price (sync)."""
        provider = TWAPDataProvider()

        for stable in ["USDC", "USDT", "DAI"]:
            price = provider.get_latest_price_sync(stable)
            assert price == Decimal("1")


class TestCacheOperations:
    """Tests for cache operations."""

    def test_clear_cache_all(self):
        """Test clearing entire cache."""
        provider = TWAPDataProvider()

        # Add to cache
        result = TWAPResult(
            price=Decimal("3000.00"),
            tick_twap=200000,
            observation_window_seconds=300,
            pool_address="0x0",
            token0_is_base=True,
        )
        provider._cache["ETH"] = CachedTWAP(
            price=Decimal("3000.00"),
            result=result,
            ttl_seconds=60,
        )
        provider._cache["BTC"] = CachedTWAP(
            price=Decimal("50000.00"),
            result=result,
            ttl_seconds=60,
        )

        provider.clear_cache()

        assert "ETH" not in provider._cache
        assert "BTC" not in provider._cache

    def test_clear_cache_specific_token(self):
        """Test clearing cache for specific token."""
        provider = TWAPDataProvider()

        result = TWAPResult(
            price=Decimal("3000.00"),
            tick_twap=200000,
            observation_window_seconds=300,
            pool_address="0x0",
            token0_is_base=True,
        )
        provider._cache["ETH"] = CachedTWAP(
            price=Decimal("3000.00"),
            result=result,
            ttl_seconds=60,
        )
        provider._cache["BTC"] = CachedTWAP(
            price=Decimal("50000.00"),
            result=result,
            ttl_seconds=60,
        )

        provider.clear_cache("ETH")

        assert "ETH" not in provider._cache
        assert "BTC" in provider._cache


class TestTimestampProperties:
    """Tests for min/max timestamp properties."""

    def test_min_timestamp(self):
        """Test min_timestamp returns observation window ago."""
        provider = TWAPDataProvider(observation_window_seconds=300)
        min_ts = provider.min_timestamp

        assert min_ts is not None
        now = datetime.now(UTC)
        diff = (now - min_ts).total_seconds()
        # Should be approximately observation_window_seconds ago
        assert 290 < diff < 310

    def test_max_timestamp(self):
        """Test max_timestamp returns current time."""
        provider = TWAPDataProvider()
        max_ts = provider.max_timestamp

        assert max_ts is not None
        now = datetime.now(UTC)
        diff = abs((now - max_ts).total_seconds())
        assert diff < 5  # Within 5 seconds


class TestAsyncContextManager:
    """Tests for async context manager support."""

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test provider can be used as async context manager."""
        async with TWAPDataProvider() as provider:
            assert provider is not None
            assert provider.provider_name == "twap_arbitrum"

    @pytest.mark.asyncio
    async def test_close_method(self):
        """Test close method can be called."""
        provider = TWAPDataProvider()
        await provider.close()
        # Should not raise


class TestModuleConstants:
    """Tests for module constants."""

    def test_observe_selector(self):
        """Test OBSERVE_SELECTOR constant."""
        assert OBSERVE_SELECTOR == "0x883bdbfd"

    def test_pools_by_chain(self):
        """Test UNISWAP_V3_POOLS has expected chains."""
        assert "ethereum" in UNISWAP_V3_POOLS
        assert "arbitrum" in UNISWAP_V3_POOLS
        assert "base" in UNISWAP_V3_POOLS
        assert "optimism" in UNISWAP_V3_POOLS
        assert "polygon" in UNISWAP_V3_POOLS

    def test_ethereum_pools_have_eth_usdc(self):
        """Test Ethereum pools include ETH/USDC."""
        assert "WETH/USDC-500" in ETHEREUM_POOLS

    def test_arbitrum_pools_have_eth_usdc(self):
        """Test Arbitrum pools include ETH/USDC."""
        assert "WETH/USDC-500" in ARBITRUM_POOLS

    def test_token_to_pool_mapping(self):
        """Test TOKEN_TO_POOL has common tokens."""
        assert "ETH" in TOKEN_TO_POOL
        assert "WETH" in TOKEN_TO_POOL
        assert "ethereum" in TOKEN_TO_POOL["ETH"]
        assert "arbitrum" in TOKEN_TO_POOL["ETH"]

    def test_default_twap_window_seconds(self):
        """Test DEFAULT_TWAP_WINDOW_SECONDS is 30 minutes (1800 seconds)."""
        assert DEFAULT_TWAP_WINDOW_SECONDS == 1800

    def test_archive_rpc_url_env_pattern(self):
        """Test ARCHIVE_RPC_URL_ENV_PATTERN format string."""
        assert ARCHIVE_RPC_URL_ENV_PATTERN == "ARCHIVE_RPC_URL_{chain}"
        # Verify formatting works
        assert ARCHIVE_RPC_URL_ENV_PATTERN.format(chain="ETHEREUM") == "ARCHIVE_RPC_URL_ETHEREUM"
        assert ARCHIVE_RPC_URL_ENV_PATTERN.format(chain="ARBITRUM") == "ARCHIVE_RPC_URL_ARBITRUM"

    def test_archive_rpc_chains(self):
        """Test ARCHIVE_RPC_CHAINS has all supported chains."""
        assert "ETHEREUM" in ARCHIVE_RPC_CHAINS
        assert "ARBITRUM" in ARCHIVE_RPC_CHAINS
        assert "BASE" in ARCHIVE_RPC_CHAINS
        assert "OPTIMISM" in ARCHIVE_RPC_CHAINS
        assert "POLYGON" in ARCHIVE_RPC_CHAINS


class TestEncodeObserveCall:
    """Tests for observe() calldata encoding."""

    def test_encode_observe_single_value(self):
        """Test encoding observe() call with single secondsAgo."""
        provider = TWAPDataProvider()
        calldata = provider._encode_observe_call([300])

        # Should start with selector
        assert calldata.startswith(OBSERVE_SELECTOR)

        # Should have proper ABI encoding structure
        # selector (10 chars with 0x) + offset (64) + length (64) + element (64)
        assert len(calldata) == 10 + 64 + 64 + 64

    def test_encode_observe_two_values(self):
        """Test encoding observe() call with two secondsAgos."""
        provider = TWAPDataProvider()
        calldata = provider._encode_observe_call([300, 0])

        # Should start with selector
        assert calldata.startswith(OBSERVE_SELECTOR)

        # selector (10 with 0x) + offset (64) + length (64) + 2 elements (64 each)
        assert len(calldata) == 10 + 64 + 64 + 64 + 64


class TestGetPrice:
    """Tests for get_price method."""

    @pytest.mark.asyncio
    async def test_get_price_unknown_token_raises(self):
        """Test get_price raises for unknown token without RPC."""
        provider = TWAPDataProvider()
        with pytest.raises(TWAPPoolNotFoundError):
            await provider.get_latest_price("UNKNOWN_TOKEN_XYZ")
