"""DEX TWAP (Time-Weighted Average Price) Data Provider.

This module provides a concrete implementation of the HistoricalDataProvider
protocol that calculates TWAP from DEX pool observations, primarily using
Uniswap V3's built-in TWAP oracle functionality.

Uniswap V3 pools maintain tick accumulator data that can be used to calculate
time-weighted average prices over configurable windows. This provides a
manipulation-resistant price source directly from on-chain data.

Key Features:
    - Configurable TWAP window (default 30 minutes)
    - Supports multiple chains (Ethereum, Arbitrum, Base, etc.)
    - Uses Uniswap V3 pool observations for TWAP calculation
    - Handles low-liquidity pools with warnings
    - Caches calculated TWAP values for efficiency

Example:
    from almanak.framework.data.price.dex_twap import DEXTWAPDataProvider
    from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig
    from datetime import datetime

    provider = DEXTWAPDataProvider(
        rpc_url="https://arb-mainnet.g.alchemy.com/v2/...",
        chain="arbitrum",
        twap_window_seconds=1800,  # 30 minute TWAP
    )

    # Get a single TWAP price
    price = await provider.get_price("ETH", datetime.now())

    # Or iterate for backtesting
    config = HistoricalDataConfig(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 7),
        interval_seconds=3600,
        tokens=["ETH", "ARB"],
    )
    async for timestamp, market_state in provider.iterate(config):
        eth_price = market_state.get_price("ETH")
"""

import logging
import math
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.pnl.data_provider import (
    OHLCV,
    HistoricalDataConfig,
    MarketState,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Uniswap V3 Pool Constants
# =============================================================================

# Uniswap V3 Pool function selectors
# slot0() returns: sqrtPriceX96, tick, observationIndex, observationCardinality, etc.
SLOT0_SELECTOR = "0x3850c7bd"

# observe(uint32[] secondsAgos) returns: tickCumulatives, secondsPerLiquidityCumulativeX128s
OBSERVE_SELECTOR = "0x883bdbfd"

# observations(uint256 index) returns: blockTimestamp, tickCumulative, secondsPerLiquidityOutside, initialized
OBSERVATIONS_SELECTOR = "0x252c09d7"

# liquidity() returns: uint128
LIQUIDITY_SELECTOR = "0x1a686502"


# =============================================================================
# Uniswap V3 Pool Addresses by Chain
# =============================================================================

# Major trading pairs for TWAP calculation
# Format: {token: {quote_token: pool_address}}
# Using ETH/USDC 0.05% fee tier pools as primary reference

ETHEREUM_POOLS: dict[str, dict[str, str]] = {
    "ETH": {"USDC": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"},  # ETH/USDC 0.05%
    "WETH": {"USDC": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"},
    "WBTC": {"USDC": "0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35"},  # WBTC/USDC 0.3%
    "BTC": {"USDC": "0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35"},
    "LINK": {"WETH": "0xa6Cc3C2531FdaA6Ae1A3CA84c2855806728693e8"},  # LINK/ETH 0.3%
    "UNI": {"WETH": "0x1d42064Fc4Beb5F8aAF85F4617AE8b3b5B8Bd801"},  # UNI/ETH 0.3%
    "AAVE": {"WETH": "0x5aB53EE1d50eeF2C1DD3d5402789cd27bB52c1bB"},  # AAVE/ETH 0.3%
}

ARBITRUM_POOLS: dict[str, dict[str, str]] = {
    "ETH": {"USDC": "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443"},  # ETH/USDC 0.05%
    "WETH": {"USDC": "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443"},
    "ARB": {"USDC": "0xc473e2aEE3441BF9240Be85eb122aBB059A3B57c"},  # ARB/USDC 0.05%
    "WBTC": {"WETH": "0x2f5e87C9312fa29aed5c179E456625D79015299c"},  # WBTC/ETH 0.05%
    "GMX": {"WETH": "0x80A9ae39310abf666A87C743d6ebBD0E8C42158E"},  # GMX/ETH 0.3%
    "LINK": {"WETH": "0x468b88941e7Cc0B88c1869d68ab6b570bCEF62Ff"},  # LINK/ETH 0.3%
}

BASE_POOLS: dict[str, dict[str, str]] = {
    "ETH": {"USDC": "0xd0b53D9277642d899DF5C87A3966A349A798F224"},  # ETH/USDC 0.05%
    "WETH": {"USDC": "0xd0b53D9277642d899DF5C87A3966A349A798F224"},
    "CBETH": {"WETH": "0x10648BA41B8565907Cfa1496765fA4D95390aa0d"},  # cbETH/WETH 0.05%
}

OPTIMISM_POOLS: dict[str, dict[str, str]] = {
    "ETH": {"USDC": "0x85149247691df622eaF1a8Bd0CaFd40BC45154a9"},  # ETH/USDC 0.05%
    "WETH": {"USDC": "0x85149247691df622eaF1a8Bd0CaFd40BC45154a9"},
    "OP": {"USDC": "0x1C3140aB59d6cAf9fa7459C6f83D4B52ba881d36"},  # OP/USDC 0.3%
    "WBTC": {"WETH": "0x73B14a78a0D396C521f954532d43fd5fFe385216"},  # WBTC/WETH 0.05%
}

POLYGON_POOLS: dict[str, dict[str, str]] = {
    "ETH": {"USDC": "0x45dDa9cb7c25131DF268515131f647d726f50608"},  # WETH/USDC 0.05%
    "WETH": {"USDC": "0x45dDa9cb7c25131DF268515131f647d726f50608"},
    "MATIC": {"USDC": "0xA374094527e1673A86dE625aa59517c5dE346d32"},  # MATIC/USDC 0.05%
    "WBTC": {"WETH": "0x50eaEDB835021E4A108B7290636d62E9765cc6d7"},  # WBTC/WETH 0.05%
}

AVALANCHE_POOLS: dict[str, dict[str, str]] = {
    "AVAX": {"USDC": "0xfAe3f424a0a47706811521E3ee268f00cFb5c45E"},  # WAVAX/USDC 0.3%
    "WAVAX": {"USDC": "0xfAe3f424a0a47706811521E3ee268f00cFb5c45E"},
    "ETH": {"USDC": "0x0000000000000000000000000000000000000000"},  # Placeholder
}

# Combined pools by chain
UNISWAP_V3_POOLS: dict[str, dict[str, dict[str, str]]] = {
    "ethereum": ETHEREUM_POOLS,
    "arbitrum": ARBITRUM_POOLS,
    "base": BASE_POOLS,
    "optimism": OPTIMISM_POOLS,
    "polygon": POLYGON_POOLS,
    "avalanche": AVALANCHE_POOLS,
}

# Token decimals for price conversion
TOKEN_DECIMALS: dict[str, int] = {
    "ETH": 18,
    "WETH": 18,
    "USDC": 6,
    "USDT": 6,
    "DAI": 18,
    "WBTC": 8,
    "BTC": 8,
    "LINK": 18,
    "UNI": 18,
    "AAVE": 18,
    "ARB": 18,
    "OP": 18,
    "GMX": 18,
    "MATIC": 18,
    "AVAX": 18,
    "WAVAX": 18,
    "CBETH": 18,
}

# Stablecoin quote tokens (always quoted in USD)
STABLECOINS = {"USDC", "USDT", "DAI"}

# Minimum liquidity threshold (in USD) for valid TWAP
MIN_LIQUIDITY_USD = Decimal("100000")  # $100k minimum


# =============================================================================
# TWAP Data Structures
# =============================================================================


@dataclass
class TWAPObservation:
    """A single Uniswap V3 pool observation.

    Attributes:
        block_timestamp: Unix timestamp of the observation
        tick_cumulative: Cumulative tick value at this point
        seconds_per_liquidity_cumulative: Cumulative seconds per liquidity
        initialized: Whether this observation slot has been initialized
    """

    block_timestamp: int
    tick_cumulative: int
    seconds_per_liquidity_cumulative: int
    initialized: bool


@dataclass
class TWAPResult:
    """Result of a TWAP calculation.

    Attributes:
        price: The calculated TWAP price in USD
        tick: The time-weighted average tick
        window_seconds: Actual TWAP window used
        start_time: Start of the TWAP window
        end_time: End of the TWAP window
        liquidity: Pool liquidity at calculation time
        is_low_liquidity: Whether the pool has low liquidity
    """

    price: Decimal
    tick: int
    window_seconds: int
    start_time: datetime
    end_time: datetime
    liquidity: int | None = None
    is_low_liquidity: bool = False


@dataclass
class TWAPCache:
    """Cache for TWAP calculations.

    Attributes:
        data: Dictionary mapping token symbols to list of (timestamp, TWAPResult) tuples
        fetched_at: When the cache was last updated
        ttl_seconds: Time-to-live for cache entries
    """

    data: dict[str, list[tuple[datetime, TWAPResult]]] = field(default_factory=dict)
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ttl_seconds: int = 60

    def get_twap_at(self, token: str, timestamp: datetime) -> TWAPResult | None:
        """Get TWAP result at or just before a specific timestamp.

        Args:
            token: Token symbol
            timestamp: Target timestamp

        Returns:
            TWAPResult at the timestamp or None if not available
        """
        token_upper = token.upper()
        if token_upper not in self.data:
            return None

        results = self.data[token_upper]
        if not results:
            return None

        # Find result at or just before timestamp
        result: TWAPResult | None = None
        for ts, twap in results:
            if ts <= timestamp:
                result = twap
            else:
                break

        return result

    def set_twap(self, token: str, timestamp: datetime, result: TWAPResult) -> None:
        """Cache a TWAP result.

        Args:
            token: Token symbol
            timestamp: Timestamp for this result
            result: TWAPResult to cache
        """
        token_upper = token.upper()
        if token_upper not in self.data:
            self.data[token_upper] = []

        self.data[token_upper].append((timestamp, result))
        # Keep sorted by timestamp
        self.data[token_upper].sort(key=lambda x: x[0])

    def clear(self, token: str | None = None) -> None:
        """Clear the cache.

        Args:
            token: Specific token to clear, or None to clear all
        """
        if token is not None:
            self.data.pop(token.upper(), None)
        else:
            self.data.clear()


# =============================================================================
# Low Liquidity Exception
# =============================================================================


class LowLiquidityWarning(Exception):
    """Warning raised when pool liquidity is below threshold.

    Attributes:
        token: Token symbol
        pool_address: Pool contract address
        liquidity_usd: Estimated pool liquidity in USD
        threshold_usd: Minimum required liquidity
    """

    def __init__(
        self,
        token: str,
        pool_address: str,
        liquidity_usd: Decimal,
        threshold_usd: Decimal,
    ):
        self.token = token
        self.pool_address = pool_address
        self.liquidity_usd = liquidity_usd
        self.threshold_usd = threshold_usd
        super().__init__(
            f"Low liquidity for {token} pool {pool_address}: ${liquidity_usd:.0f} (threshold: ${threshold_usd:.0f})"
        )


# =============================================================================
# DEX TWAP Data Provider Implementation
# =============================================================================


class DEXTWAPDataProvider:
    """DEX TWAP historical data provider implementation.

    Implements the HistoricalDataProvider protocol to provide time-weighted
    average prices calculated from Uniswap V3 pool observations.

    The TWAP is calculated using Uniswap V3's observe() function, which returns
    tick accumulators that can be used to compute manipulation-resistant average
    prices over any time window.

    Attributes:
        chain: Blockchain network identifier
        rpc_url: RPC endpoint URL for on-chain queries
        twap_window_seconds: TWAP calculation window (default 1800 = 30 minutes)

    Example:
        provider = DEXTWAPDataProvider(
            rpc_url="https://arb-mainnet.g.alchemy.com/v2/...",
            chain="arbitrum",
            twap_window_seconds=1800,
        )

        # Get a single TWAP price
        price = await provider.get_price("ETH", datetime.now())
    """

    # Supported chains
    _SUPPORTED_CHAINS = list(UNISWAP_V3_POOLS.keys())

    # Default provider priority (lower = higher priority)
    DEFAULT_PRIORITY = 20  # Between Chainlink (10) and CoinGecko (50)

    # Default TWAP window: 30 minutes
    DEFAULT_TWAP_WINDOW = 1800

    def __init__(
        self,
        chain: str = "ethereum",
        rpc_url: str = "",
        twap_window_seconds: int = 1800,
        cache_ttl_seconds: int = 60,
        priority: int | None = None,
        min_liquidity_usd: Decimal | None = None,
    ) -> None:
        """Initialize the DEX TWAP data provider.

        Args:
            chain: Blockchain network identifier (ethereum, arbitrum, base, etc.)
            rpc_url: RPC endpoint URL for on-chain queries. Required for live
                    queries. If not provided, provider operates in offline mode.
            twap_window_seconds: TWAP calculation window in seconds (default 1800 = 30 min).
                                Must be positive and typically 60-3600 seconds.
            cache_ttl_seconds: TTL for cached TWAP values (default 60 seconds).
                              Set to 0 to disable caching.
            priority: Provider priority for registry (lower = higher priority).
                     Defaults to DEFAULT_PRIORITY (20).
            min_liquidity_usd: Minimum pool liquidity in USD for valid TWAP.
                              Defaults to MIN_LIQUIDITY_USD ($100k).
        """
        self._chain = chain.lower()
        self._rpc_url = rpc_url
        self._twap_window_seconds = twap_window_seconds
        self._cache_ttl_seconds = cache_ttl_seconds
        self._priority = priority if priority is not None else self.DEFAULT_PRIORITY
        self._min_liquidity_usd = min_liquidity_usd if min_liquidity_usd is not None else MIN_LIQUIDITY_USD

        # Validate chain
        if self._chain not in UNISWAP_V3_POOLS:
            available = ", ".join(UNISWAP_V3_POOLS.keys())
            raise ValueError(f"Unsupported chain: {chain}. Available chains: {available}")

        # Validate TWAP window
        if twap_window_seconds <= 0:
            raise ValueError("twap_window_seconds must be positive")
        if twap_window_seconds > 86400:  # 24 hours max
            logger.warning(
                f"TWAP window {twap_window_seconds}s is very long (>24h), "
                "consider using a shorter window for more responsive prices"
            )

        # Get pools for this chain
        self._pools = UNISWAP_V3_POOLS[self._chain]

        # Initialize cache
        self._cache: TWAPCache | None = None
        if cache_ttl_seconds > 0:
            self._cache = TWAPCache(ttl_seconds=cache_ttl_seconds)

        logger.info(
            "Initialized DEXTWAPDataProvider",
            extra={
                "chain": self._chain,
                "twap_window_seconds": self._twap_window_seconds,
                "available_tokens": len(self._pools),
                "rpc_url": "configured" if rpc_url else "not configured",
                "cache_ttl_seconds": cache_ttl_seconds,
                "priority": self._priority,
            },
        )

    @property
    def priority(self) -> int:
        """Return the provider priority for registry selection."""
        return self._priority

    @property
    def twap_window_seconds(self) -> int:
        """Return the TWAP window in seconds."""
        return self._twap_window_seconds

    def set_twap_window(self, seconds: int) -> None:
        """Update the TWAP window.

        Args:
            seconds: New TWAP window in seconds (must be positive)

        Raises:
            ValueError: If seconds is not positive
        """
        if seconds <= 0:
            raise ValueError("TWAP window must be positive")
        self._twap_window_seconds = seconds
        logger.debug(f"Updated TWAP window to {seconds}s")

    def get_pool_address(self, token: str, quote_token: str = "USDC") -> str | None:
        """Get the Uniswap V3 pool address for a token pair.

        Args:
            token: Base token symbol (e.g., "ETH", "ARB")
            quote_token: Quote token symbol (default "USDC")

        Returns:
            Pool contract address or None if not available
        """
        token_upper = token.upper()
        quote_upper = quote_token.upper()

        token_pools = self._pools.get(token_upper, {})
        return token_pools.get(quote_upper)

    def _get_best_quote_token(self, token: str) -> str | None:
        """Get the best available quote token for a token.

        Priority: USDC > USDT > WETH > DAI

        Args:
            token: Token symbol

        Returns:
            Best quote token or None if no pool available
        """
        token_pools = self._pools.get(token.upper(), {})
        if not token_pools:
            return None

        # Priority order for quote tokens
        for quote in ["USDC", "USDT", "WETH", "DAI"]:
            if quote in token_pools:
                return quote

        # Return first available
        return next(iter(token_pools.keys()), None)

    # =========================================================================
    # Tick Math Utilities (from Uniswap V3 math)
    # =========================================================================

    def _tick_to_sqrt_price_x96(self, tick: int) -> int:
        """Convert tick to sqrtPriceX96.

        Uses the Uniswap V3 formula: sqrt(1.0001^tick) * 2^96

        Args:
            tick: The tick value

        Returns:
            sqrtPriceX96 value
        """
        # sqrt(1.0001^tick) = 1.0001^(tick/2)
        sqrt_ratio = math.pow(1.0001, tick / 2)
        return int(sqrt_ratio * (2**96))

    def _tick_to_price(
        self,
        tick: int,
        token0_decimals: int,
        token1_decimals: int,
        invert: bool = False,
    ) -> Decimal:
        """Convert tick to human-readable price.

        In Uniswap V3, price = 1.0001^tick represents the ratio of token1/token0.

        Args:
            tick: The tick value
            token0_decimals: Decimals of token0
            token1_decimals: Decimals of token1
            invert: If True, return token0/token1 instead of token1/token0

        Returns:
            Human-readable price
        """
        # price = 1.0001^tick
        raw_price = Decimal(str(math.pow(1.0001, tick)))

        # Adjust for decimals: price * 10^(token0_decimals - token1_decimals)
        decimal_adjustment = Decimal(10) ** (token0_decimals - token1_decimals)
        price = raw_price * decimal_adjustment

        if invert and price != 0:
            return Decimal("1") / price
        return price

    # =========================================================================
    # On-Chain Query Methods
    # =========================================================================

    async def _query_observe(
        self,
        pool_address: str,
        seconds_agos: list[int],
    ) -> list[TWAPObservation] | None:
        """Query observe() from a Uniswap V3 pool.

        The observe() function returns tick accumulators at specified times
        in the past, which can be used to calculate TWAP.

        Args:
            pool_address: Pool contract address
            seconds_agos: List of seconds ago to query (e.g., [1800, 0] for 30-min TWAP)

        Returns:
            List of TWAPObservation or None if query fails
        """
        if not self._rpc_url:
            return None

        try:
            from web3 import Web3

            web3 = Web3(Web3.HTTPProvider(self._rpc_url))
            pool_checksum = web3.to_checksum_address(pool_address)

            # Encode the observe() call
            # observe(uint32[] secondsAgos) returns (int56[] tickCumulatives, uint160[] secondsPerLiquidityCumulativeX128s)
            # Encode array: offset (32 bytes) + length (32 bytes) + elements (32 bytes each)
            data = OBSERVE_SELECTOR

            # Array offset (points to where array data starts)
            data += (32).to_bytes(32, byteorder="big").hex()

            # Array length
            data += len(seconds_agos).to_bytes(32, byteorder="big").hex()

            # Array elements (each padded to 32 bytes)
            for seconds_ago in seconds_agos:
                data += seconds_ago.to_bytes(32, byteorder="big").hex()

            result = web3.eth.call({"to": pool_checksum, "data": data})  # type: ignore[typeddict-item]

            # Decode response
            # Returns two dynamic arrays: int56[] tickCumulatives, uint160[] secondsPerLiquidityCumulativeX128s
            # Layout: offset1 (32) + offset2 (32) + array1_len + array1_data + array2_len + array2_data

            if len(result) < 64:
                logger.warning(f"Unexpected response length from observe(): {len(result)}")
                return None

            # Get offsets
            offset1 = int.from_bytes(result[0:32], byteorder="big")
            offset2 = int.from_bytes(result[32:64], byteorder="big")

            # Read tickCumulatives array
            array1_len = int.from_bytes(result[offset1 : offset1 + 32], byteorder="big")
            tick_cumulatives = []
            for i in range(array1_len):
                start = offset1 + 32 + i * 32
                # int56 is signed, stored in 32 bytes
                val = int.from_bytes(result[start : start + 32], byteorder="big", signed=True)
                tick_cumulatives.append(val)

            # Read secondsPerLiquidityCumulativeX128s array
            array2_len = int.from_bytes(result[offset2 : offset2 + 32], byteorder="big")
            seconds_per_liquidity = []
            for i in range(array2_len):
                start = offset2 + 32 + i * 32
                val = int.from_bytes(result[start : start + 32], byteorder="big")
                seconds_per_liquidity.append(val)

            # Create observations (we don't have exact timestamps, use seconds_agos)
            current_time = int(datetime.now(UTC).timestamp())
            observations = []
            for i, seconds_ago in enumerate(seconds_agos):
                observations.append(
                    TWAPObservation(
                        block_timestamp=current_time - seconds_ago,
                        tick_cumulative=tick_cumulatives[i],
                        seconds_per_liquidity_cumulative=seconds_per_liquidity[i],
                        initialized=True,
                    )
                )

            return observations

        except Exception as e:
            logger.error(f"Failed to query observe() from pool {pool_address}: {e}")
            return None

    def _query_observe_sync(
        self,
        pool_address: str,
        seconds_agos: list[int],
    ) -> list[TWAPObservation] | None:
        """Synchronous version of _query_observe.

        Args:
            pool_address: Pool contract address
            seconds_agos: List of seconds ago to query

        Returns:
            List of TWAPObservation or None if query fails
        """
        if not self._rpc_url:
            return None

        try:
            from web3 import Web3

            web3 = Web3(Web3.HTTPProvider(self._rpc_url))
            pool_checksum = web3.to_checksum_address(pool_address)

            # Encode observe() call (same as async version)
            data = OBSERVE_SELECTOR
            data += (32).to_bytes(32, byteorder="big").hex()
            data += len(seconds_agos).to_bytes(32, byteorder="big").hex()
            for seconds_ago in seconds_agos:
                data += seconds_ago.to_bytes(32, byteorder="big").hex()

            result = web3.eth.call({"to": pool_checksum, "data": data})  # type: ignore[typeddict-item]

            if len(result) < 64:
                return None

            offset1 = int.from_bytes(result[0:32], byteorder="big")
            offset2 = int.from_bytes(result[32:64], byteorder="big")

            array1_len = int.from_bytes(result[offset1 : offset1 + 32], byteorder="big")
            tick_cumulatives = []
            for i in range(array1_len):
                start = offset1 + 32 + i * 32
                val = int.from_bytes(result[start : start + 32], byteorder="big", signed=True)
                tick_cumulatives.append(val)

            array2_len = int.from_bytes(result[offset2 : offset2 + 32], byteorder="big")
            seconds_per_liquidity = []
            for i in range(array2_len):
                start = offset2 + 32 + i * 32
                val = int.from_bytes(result[start : start + 32], byteorder="big")
                seconds_per_liquidity.append(val)

            current_time = int(datetime.now(UTC).timestamp())
            observations = []
            for i, seconds_ago in enumerate(seconds_agos):
                observations.append(
                    TWAPObservation(
                        block_timestamp=current_time - seconds_ago,
                        tick_cumulative=tick_cumulatives[i],
                        seconds_per_liquidity_cumulative=seconds_per_liquidity[i],
                        initialized=True,
                    )
                )

            return observations

        except Exception as e:
            logger.error(f"Failed to query observe() from pool {pool_address}: {e}")
            return None

    async def _query_liquidity(self, pool_address: str) -> int | None:
        """Query current liquidity from a Uniswap V3 pool.

        Args:
            pool_address: Pool contract address

        Returns:
            Current liquidity or None if query fails
        """
        if not self._rpc_url:
            return None

        try:
            from web3 import Web3

            web3 = Web3(Web3.HTTPProvider(self._rpc_url))
            pool_checksum = web3.to_checksum_address(pool_address)

            result = web3.eth.call({"to": pool_checksum, "data": LIQUIDITY_SELECTOR})  # type: ignore[typeddict-item]

            if len(result) < 32:
                return None

            # liquidity is uint128, but we read as uint256
            return int.from_bytes(result[0:32], byteorder="big")

        except Exception as e:
            logger.error(f"Failed to query liquidity from pool {pool_address}: {e}")
            return None

    def _query_liquidity_sync(self, pool_address: str) -> int | None:
        """Synchronous version of _query_liquidity.

        Args:
            pool_address: Pool contract address

        Returns:
            Current liquidity or None if query fails
        """
        if not self._rpc_url:
            return None

        try:
            from web3 import Web3

            web3 = Web3(Web3.HTTPProvider(self._rpc_url))
            pool_checksum = web3.to_checksum_address(pool_address)

            result = web3.eth.call({"to": pool_checksum, "data": LIQUIDITY_SELECTOR})  # type: ignore[typeddict-item]

            if len(result) < 32:
                return None

            return int.from_bytes(result[0:32], byteorder="big")

        except Exception as e:
            logger.error(f"Failed to query liquidity from pool {pool_address}: {e}")
            return None

    # =========================================================================
    # TWAP Calculation
    # =========================================================================

    def _calculate_twap_from_observations(
        self,
        observations: list[TWAPObservation],
    ) -> int:
        """Calculate TWAP tick from observations.

        TWAP tick = (tickCumulative[now] - tickCumulative[ago]) / seconds

        Args:
            observations: List of observations (oldest first, newest last)

        Returns:
            Time-weighted average tick
        """
        if len(observations) < 2:
            raise ValueError("Need at least 2 observations for TWAP calculation")

        oldest = observations[0]
        newest = observations[-1]

        tick_diff = newest.tick_cumulative - oldest.tick_cumulative
        time_diff = newest.block_timestamp - oldest.block_timestamp

        if time_diff <= 0:
            raise ValueError("Invalid time range for TWAP calculation")

        return tick_diff // time_diff

    async def calculate_twap(
        self,
        token: str,
        window_seconds: int | None = None,
        raise_on_low_liquidity: bool = False,
    ) -> TWAPResult | None:
        """Calculate TWAP for a token.

        Args:
            token: Token symbol (e.g., "ETH", "ARB")
            window_seconds: TWAP window (defaults to configured window)
            raise_on_low_liquidity: If True, raise LowLiquidityWarning when pool
                                    liquidity is below threshold. Default is False.

        Returns:
            TWAPResult or None if calculation fails

        Raises:
            LowLiquidityWarning: If raise_on_low_liquidity=True and pool liquidity
                                is below the configured minimum threshold.
        """
        token_upper = token.upper()
        window = window_seconds or self._twap_window_seconds

        # Find best pool for this token
        quote_token = self._get_best_quote_token(token_upper)
        if quote_token is None:
            logger.warning(f"No pool available for {token_upper} on {self._chain}")
            return None

        pool_address = self.get_pool_address(token_upper, quote_token)
        if pool_address is None or pool_address == "0x0000000000000000000000000000000000000000":
            logger.warning(f"Invalid pool address for {token_upper}/{quote_token}")
            return None

        # Query observations at [window_seconds, 0] seconds ago
        observations = await self._query_observe(pool_address, [window, 0])
        if observations is None or len(observations) < 2:
            logger.warning(f"Failed to get observations for {token_upper}")
            return None

        # Calculate TWAP tick
        try:
            twap_tick = self._calculate_twap_from_observations(observations)
        except ValueError as e:
            logger.warning(f"TWAP calculation failed for {token_upper}: {e}")
            return None

        # Get token decimals
        token_decimals = TOKEN_DECIMALS.get(token_upper, 18)
        quote_decimals = TOKEN_DECIMALS.get(quote_token, 6)

        # Convert tick to price
        # In Uniswap V3, token0 is typically the lower address
        # For ETH/USDC pools, ETH is usually token0 and USDC is token1
        # price = token1/token0 = USDC/ETH, so we need to invert for ETH/USD
        invert = quote_token in STABLECOINS

        price = self._tick_to_price(
            twap_tick,
            token_decimals,
            quote_decimals,
            invert=invert,
        )

        # If quote is not a stablecoin, need to convert via another step
        if quote_token not in STABLECOINS:
            # Get quote token price in USD
            quote_price = await self.get_price(quote_token)
            if quote_price is not None:
                price = price * quote_price

        # Query liquidity for warning check
        liquidity = await self._query_liquidity(pool_address)
        is_low_liquidity = False
        estimated_liquidity_usd = Decimal("0")

        if liquidity is not None:
            # Estimate liquidity in USD terms
            # Liquidity value depends on current price - rough estimate
            # For ETH/USDC: liquidity represents sqrt(k), so liquidity^2 / price gives TVL estimate
            # Simplified: use liquidity * price / 10^15 as rough USD estimate
            try:
                # Convert raw liquidity to approximate USD value
                # This is a rough heuristic - actual TVL calculation is more complex
                estimated_liquidity_usd = Decimal(str(liquidity)) * price / Decimal("1e15")
            except (ValueError, ZeroDivisionError):
                estimated_liquidity_usd = Decimal("0")

            if estimated_liquidity_usd < self._min_liquidity_usd:
                is_low_liquidity = True
                logger.warning(
                    f"Low liquidity detected for {token_upper} pool {pool_address}: "
                    f"~${estimated_liquidity_usd:,.0f} (threshold: ${self._min_liquidity_usd:,.0f})"
                )

                if raise_on_low_liquidity:
                    raise LowLiquidityWarning(
                        token=token_upper,
                        pool_address=pool_address,
                        liquidity_usd=estimated_liquidity_usd,
                        threshold_usd=self._min_liquidity_usd,
                    )

        current_time = datetime.now(UTC)

        return TWAPResult(
            price=price,
            tick=twap_tick,
            window_seconds=window,
            start_time=current_time - timedelta(seconds=window),
            end_time=current_time,
            liquidity=liquidity,
            is_low_liquidity=is_low_liquidity,
        )

    def calculate_twap_sync(
        self,
        token: str,
        window_seconds: int | None = None,
        raise_on_low_liquidity: bool = False,
    ) -> TWAPResult | None:
        """Synchronous version of calculate_twap.

        Args:
            token: Token symbol
            window_seconds: TWAP window (defaults to configured window)
            raise_on_low_liquidity: If True, raise LowLiquidityWarning when pool
                                    liquidity is below threshold. Default is False.

        Returns:
            TWAPResult or None if calculation fails

        Raises:
            LowLiquidityWarning: If raise_on_low_liquidity=True and pool liquidity
                                is below the configured minimum threshold.
        """
        token_upper = token.upper()
        window = window_seconds or self._twap_window_seconds

        quote_token = self._get_best_quote_token(token_upper)
        if quote_token is None:
            return None

        pool_address = self.get_pool_address(token_upper, quote_token)
        if pool_address is None or pool_address == "0x0000000000000000000000000000000000000000":
            return None

        observations = self._query_observe_sync(pool_address, [window, 0])
        if observations is None or len(observations) < 2:
            return None

        try:
            twap_tick = self._calculate_twap_from_observations(observations)
        except ValueError:
            return None

        token_decimals = TOKEN_DECIMALS.get(token_upper, 18)
        quote_decimals = TOKEN_DECIMALS.get(quote_token, 6)
        invert = quote_token in STABLECOINS

        price = self._tick_to_price(
            twap_tick,
            token_decimals,
            quote_decimals,
            invert=invert,
        )

        # Query liquidity for warning check (sync version)
        liquidity = self._query_liquidity_sync(pool_address)
        is_low_liquidity = False
        estimated_liquidity_usd = Decimal("0")

        if liquidity is not None:
            try:
                estimated_liquidity_usd = Decimal(str(liquidity)) * price / Decimal("1e15")
            except (ValueError, ZeroDivisionError):
                estimated_liquidity_usd = Decimal("0")

            if estimated_liquidity_usd < self._min_liquidity_usd:
                is_low_liquidity = True
                logger.warning(
                    f"Low liquidity detected for {token_upper} pool {pool_address}: "
                    f"~${estimated_liquidity_usd:,.0f} (threshold: ${self._min_liquidity_usd:,.0f})"
                )

                if raise_on_low_liquidity:
                    raise LowLiquidityWarning(
                        token=token_upper,
                        pool_address=pool_address,
                        liquidity_usd=estimated_liquidity_usd,
                        threshold_usd=self._min_liquidity_usd,
                    )

        current_time = datetime.now(UTC)

        return TWAPResult(
            price=price,
            tick=twap_tick,
            window_seconds=window,
            start_time=current_time - timedelta(seconds=window),
            end_time=current_time,
            liquidity=liquidity,
            is_low_liquidity=is_low_liquidity,
        )

    # =========================================================================
    # HistoricalDataProvider Protocol Implementation
    # =========================================================================

    async def get_price(
        self,
        token: str,
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Get the TWAP price of a token at a specific timestamp.

        For historical timestamps, returns cached data.
        For current/recent timestamps, calculates live TWAP.

        Args:
            token: Token symbol (e.g., "ETH", "ARB", "WBTC")
            timestamp: The point in time (None for current TWAP)

        Returns:
            TWAP price in USD at the specified timestamp

        Raises:
            ValueError: If price data is not available
        """
        token_upper = token.upper()

        # Check if token has a pool on this chain
        if token_upper not in self._pools:
            raise ValueError(f"No Uniswap V3 pool available for {token} on {self._chain}")

        # Current time handling
        current_time = datetime.now(UTC)
        if timestamp is None:
            timestamp = current_time

        # Normalize timestamp timezone
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        # Check if requesting recent data (within last 5 minutes)
        time_diff = abs((current_time - timestamp).total_seconds())
        is_live_request = time_diff < 300

        # Check cache first
        if self._cache is not None:
            cached = self._cache.get_twap_at(token_upper, timestamp)
            if cached is not None:
                return cached.price

        # For live requests, calculate TWAP
        if is_live_request and self._rpc_url:
            result = await self.calculate_twap(token_upper)
            if result is not None:
                # Cache the result
                if self._cache is not None:
                    self._cache.set_twap(token_upper, timestamp, result)
                return result.price

            raise ValueError(f"Failed to calculate TWAP for {token}")

        # Historical data requires pre-cached data
        raise ValueError(
            f"Historical TWAP data for {token} at {timestamp} not available. "
            "DEX TWAP provider requires RPC access for live queries or "
            "pre-cached data for historical queries."
        )

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for a token over a time range.

        Note: TWAP provides single prices, not OHLCV data. This method
        generates pseudo-OHLCV using the TWAP price for all O/H/L/C values.

        Args:
            token: Token symbol
            start: Start of time range (inclusive)
            end: End of time range (inclusive)
            interval_seconds: Candle interval in seconds (default: 3600 = 1 hour)

        Returns:
            List of OHLCV data points

        Raises:
            ValueError: If data is not available
        """
        token_upper = token.upper()

        if token_upper not in self._pools:
            raise ValueError(f"No pool available for {token} on {self._chain}")

        # Check cache
        if self._cache is None or token_upper not in self._cache.data:
            raise ValueError(
                f"OHLCV data for {token} not available. "
                "DEX TWAP provider requires pre-cached data for historical OHLCV."
            )

        ohlcv_list: list[OHLCV] = []
        current = start
        if current.tzinfo is None:
            current = current.replace(tzinfo=UTC)

        end_tz = end
        if end_tz.tzinfo is None:
            end_tz = end_tz.replace(tzinfo=UTC)

        interval = timedelta(seconds=interval_seconds)

        while current <= end_tz:
            twap = self._cache.get_twap_at(token_upper, current)
            if twap is not None:
                ohlcv = OHLCV(
                    timestamp=current,
                    open=twap.price,
                    high=twap.price,
                    low=twap.price,
                    close=twap.price,
                    volume=None,
                )
                ohlcv_list.append(ohlcv)
            current += interval

        return ohlcv_list

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical market states.

        For each timestamp in the configured range, yields a MarketState
        with TWAP prices for the requested tokens.

        Note: This method requires either:
        1. RPC access for live TWAP calculation (slow, many RPC calls)
        2. Pre-cached TWAP data via set_historical_twaps()

        Args:
            config: Configuration specifying time range, interval, and tokens

        Yields:
            Tuples of (timestamp, MarketState) for each time point
        """
        logger.info(
            f"Starting DEX TWAP iteration from {config.start_time} to {config.end_time} "
            f"with {config.interval_seconds}s interval for tokens: {config.tokens}"
        )

        # Initialize cache if needed
        if self._cache is None:
            self._cache = TWAPCache()

        current_time = config.start_time
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=UTC)

        end_time = config.end_time
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        interval = timedelta(seconds=config.interval_seconds)
        data_points = 0

        while current_time <= end_time:
            prices: dict[str, Decimal] = {}
            ohlcv_data: dict[str, OHLCV] = {}

            for token in config.tokens:
                token_upper = token.upper()

                # Try to get from cache first
                cached = self._cache.get_twap_at(token_upper, current_time)
                if cached is not None:
                    prices[token_upper] = cached.price

                    if config.include_ohlcv:
                        ohlcv_data[token_upper] = OHLCV(
                            timestamp=current_time,
                            open=cached.price,
                            high=cached.price,
                            low=cached.price,
                            close=cached.price,
                            volume=None,
                        )

            # Create MarketState
            market_state = MarketState(
                timestamp=current_time,
                prices=prices,
                ohlcv=ohlcv_data if config.include_ohlcv else {},
                chain=config.chains[0] if config.chains else self._chain,
                block_number=None,
                gas_price_gwei=None,
                metadata={"data_source": "dex_twap", "twap_window": self._twap_window_seconds},
            )

            yield (current_time, market_state)

            current_time += interval
            data_points += 1

        logger.info(f"Completed DEX TWAP iteration with {data_points} data points")

    def set_historical_twaps(
        self,
        token: str,
        twaps: list[tuple[datetime, TWAPResult]],
    ) -> None:
        """Set historical TWAP data for a token.

        This method allows pre-loading TWAP data for backtesting.

        Args:
            token: Token symbol
            twaps: List of (timestamp, TWAPResult) tuples, sorted by timestamp
        """
        if self._cache is None:
            self._cache = TWAPCache(ttl_seconds=self._cache_ttl_seconds)

        token_upper = token.upper()
        self._cache.data[token_upper] = sorted(twaps, key=lambda x: x[0])
        logger.info(f"Loaded {len(twaps)} historical TWAPs for {token_upper}")

    def clear_cache(self, token: str | None = None) -> None:
        """Clear the TWAP cache.

        Args:
            token: Specific token to clear, or None to clear all
        """
        if self._cache is None:
            return

        self._cache.clear(token)
        logger.debug(f"Cleared cache for {token if token else 'all tokens'}")

    @property
    def provider_name(self) -> str:
        """Return the unique name of this data provider."""
        return f"dex_twap_{self._chain}"

    @property
    def supported_tokens(self) -> list[str]:
        """Return list of supported token symbols for the current chain."""
        return list(self._pools.keys())

    @property
    def supported_chains(self) -> list[str]:
        """Return list of supported chain identifiers."""
        return self._SUPPORTED_CHAINS.copy()

    @property
    def min_timestamp(self) -> datetime | None:
        """Return the earliest timestamp with available data.

        Uniswap V3 pools typically have observation history going back
        several weeks, limited by the observation cardinality.
        """
        # Conservative estimate - most pools have ~10 days of observations
        return datetime.now(UTC) - timedelta(days=7)

    @property
    def max_timestamp(self) -> datetime | None:
        """Return the latest timestamp with available data."""
        return datetime.now(UTC)

    async def close(self) -> None:
        """Close any resources (for API compatibility)."""
        pass

    async def __aenter__(self) -> "DEXTWAPDataProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


__all__ = [
    "DEXTWAPDataProvider",
    "TWAPObservation",
    "TWAPResult",
    "TWAPCache",
    "LowLiquidityWarning",
    "UNISWAP_V3_POOLS",
    "TOKEN_DECIMALS",
    "SLOT0_SELECTOR",
    "OBSERVE_SELECTOR",
    "OBSERVATIONS_SELECTOR",
    "LIQUIDITY_SELECTOR",
]
