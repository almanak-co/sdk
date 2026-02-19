"""DEX TWAP (Time-Weighted Average Price) Provider for PnL backtesting.

This module provides a concrete implementation of a price data provider
using Uniswap V3 oracle functionality to calculate TWAP prices.

Uniswap V3 pools store cumulative tick data that can be used to calculate
time-weighted average prices over any historical window. This provider
queries the pool's observe() function to compute accurate on-chain TWAP prices.

Key Features:
    - Fetches TWAP prices from Uniswap V3 pool oracles
    - Supports multiple chains (Ethereum, Arbitrum, Base, Optimism, Polygon)
    - Configurable observation window (default: 1800 seconds / 30 minutes)
    - Handles cases where pool has insufficient history
    - Caches fetched data to minimize RPC calls
    - Uses archive RPC nodes for historical state (via ARCHIVE_RPC_URL_{CHAIN} env vars)

Example:
    import os
    from almanak.framework.backtesting.pnl.providers.twap import TWAPDataProvider

    # Set archive RPC URL environment variable (or pass directly)
    os.environ["ARCHIVE_RPC_URL_ARBITRUM"] = "https://arb-mainnet.g.alchemy.com/v2/..."

    provider = TWAPDataProvider(chain="arbitrum")

    # Get 30-minute TWAP price
    price = await provider.get_latest_price("ETH")

    # Get historical TWAP price (requires archive RPC)
    from datetime import datetime, UTC
    historical_price = await provider._get_historical_price("ETH", datetime(2024, 1, 15, tzinfo=UTC))
"""

import logging
import math
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from ..data_provider import OHLCV, HistoricalDataCapability, HistoricalDataConfig, MarketState

logger = logging.getLogger(__name__)


# =============================================================================
# Uniswap V3 Function Selectors
# =============================================================================

# observe(uint32[] secondsAgos) function selector
# Returns: (int56[] tickCumulatives, uint160[] secondsPerLiquidityCumulativeX128s)
OBSERVE_SELECTOR = "0x883bdbfd"

# slot0() function selector - get current tick and other slot data
# Returns: (sqrtPriceX96, tick, observationIndex, observationCardinality, ...)
SLOT0_SELECTOR = "0x3850c7bd"

# token0() function selector
TOKEN0_SELECTOR = "0x0dfe1681"

# token1() function selector
TOKEN1_SELECTOR = "0xd21220a7"


# =============================================================================
# Uniswap V3 Pool Addresses by Chain
# =============================================================================

# Ethereum Mainnet Uniswap V3 pools (Chain ID: 1)
# Format: "TOKEN0/TOKEN1-FEE" -> pool_address
# Using WETH/USDC pools as primary price sources
ETHEREUM_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",  # 0.05% fee tier
    "WETH/USDC-3000": "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",  # 0.3% fee tier
    "WETH/USDT-3000": "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",
    "WBTC/WETH-3000": "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD",
    "LINK/WETH-3000": "0xa6Cc3C2531FdaA6Ae1A3CA84c2855806728693e8",
    "UNI/WETH-3000": "0x1d42064Fc4Beb5F8aAF85F4617AE8b3b5B8Bd801",
    "AAVE/WETH-3000": "0x5aB53EE1d50eeF2C1DD3d5402789cd27bB52c1bB",
    "WSTETH/WETH-100": "0x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa",  # 0.01% fee tier
}

# Arbitrum One Uniswap V3 pools (Chain ID: 42161)
ARBITRUM_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
    "WETH/USDC-3000": "0x17c14D2c404D167802b16C450cF23B453F6c4f4a",
    "WETH/USDT-500": "0x641C00A822e8b671738d32a431a4Fb6074E5c79d",
    "WBTC/WETH-500": "0x2f5e87C9312fa29aed5c179E456625D79015299c",
    "ARB/WETH-3000": "0xC6F780497A95e246EB9449f5e4770916DCd6396A",
    "GMX/WETH-10000": "0x80A9ae39310abf666A87C743d6ebBD0E8C42158E",  # 1% fee tier
    "LINK/WETH-3000": "0x468b88941e7Cc0B88c1869d68ab6b570bCEF62Ff",
}

# Base Uniswap V3 pools (Chain ID: 8453)
BASE_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0xd0b53D9277642d899DF5C87A3966A349A798F224",
    "WETH/USDbC-500": "0x4C36388bE6F416A29C8d8Eee81C771cE6bE14B18",
    "CBETH/WETH-500": "0x10648BA41B8565907Cfa1496765fA4D95390aa0d",
}

# Optimism Uniswap V3 pools (Chain ID: 10)
OPTIMISM_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0x85149247691df622eaF1a8Bd0CaFd40BC45154a9",
    "WETH/USDC-3000": "0x85C31FFA3F6B9a59a6c8d85c9d68c1f0BB9F63A7",
    "WETH/USDT-500": "0xc858A329Bf053BE78D6239C4A4343B8FbD21472b",
    "OP/WETH-3000": "0x68F5C0A2DE713a54991E01858Fd27a3832401849",
    "WSTETH/WETH-100": "0x04F6C85A1B00F6D9B75f91FD23835974Cc07E65c",
}

# Polygon Uniswap V3 pools (Chain ID: 137)
POLYGON_POOLS: dict[str, str] = {
    "WETH/USDC-500": "0x45dDa9cb7c25131DF268515131f647d726f50608",
    "WMATIC/USDC-500": "0xA374094527e1673A86dE625aa59517c5dE346d32",
    "WMATIC/WETH-500": "0x86f1d8390222A3691C28938eC7404A1661E618e0",
    "WBTC/WETH-500": "0x50eaEDB835021E4A108B7290636d62E9765cc6d7",
}

# Combined pools by chain
UNISWAP_V3_POOLS: dict[str, dict[str, str]] = {
    "ethereum": ETHEREUM_POOLS,
    "arbitrum": ARBITRUM_POOLS,
    "base": BASE_POOLS,
    "optimism": OPTIMISM_POOLS,
    "polygon": POLYGON_POOLS,
}


# =============================================================================
# Token to Pool Mapping
# =============================================================================

# Maps token symbol to the preferred pool for USD pricing
# Uses WETH as intermediate for most tokens
TOKEN_TO_POOL: dict[str, dict[str, str]] = {
    "ETH": {
        "ethereum": "WETH/USDC-500",
        "arbitrum": "WETH/USDC-500",
        "base": "WETH/USDC-500",
        "optimism": "WETH/USDC-500",
        "polygon": "WETH/USDC-500",
    },
    "WETH": {
        "ethereum": "WETH/USDC-500",
        "arbitrum": "WETH/USDC-500",
        "base": "WETH/USDC-500",
        "optimism": "WETH/USDC-500",
        "polygon": "WETH/USDC-500",
    },
    "BTC": {
        "ethereum": "WBTC/WETH-3000",
        "arbitrum": "WBTC/WETH-500",
        "polygon": "WBTC/WETH-500",
    },
    "WBTC": {
        "ethereum": "WBTC/WETH-3000",
        "arbitrum": "WBTC/WETH-500",
        "polygon": "WBTC/WETH-500",
    },
    "LINK": {
        "ethereum": "LINK/WETH-3000",
        "arbitrum": "LINK/WETH-3000",
    },
    "UNI": {
        "ethereum": "UNI/WETH-3000",
    },
    "AAVE": {
        "ethereum": "AAVE/WETH-3000",
    },
    "ARB": {
        "arbitrum": "ARB/WETH-3000",
    },
    "GMX": {
        "arbitrum": "GMX/WETH-10000",
    },
    "OP": {
        "optimism": "OP/WETH-3000",
    },
    "MATIC": {
        "polygon": "WMATIC/USDC-500",
    },
    "WMATIC": {
        "polygon": "WMATIC/USDC-500",
    },
    "WSTETH": {
        "ethereum": "WSTETH/WETH-100",
        "optimism": "WSTETH/WETH-100",
    },
    "STETH": {
        "ethereum": "WSTETH/WETH-100",
        "optimism": "WSTETH/WETH-100",
    },
    "CBETH": {
        "base": "CBETH/WETH-500",
    },
}


# =============================================================================
# Average Block Time by Chain (for timestamp to block estimation)
# =============================================================================

# Average block time in seconds for each supported chain
# These are approximate values and may vary over time
CHAIN_BLOCK_TIME_SECONDS: dict[str, float] = {
    "ethereum": 12.0,  # ~12s per block post-merge
    "arbitrum": 0.25,  # ~250ms per block (L2 batched)
    "base": 2.0,  # ~2s per block
    "optimism": 2.0,  # ~2s per block
    "polygon": 2.0,  # ~2s per block
}

# Maximum number of blocks to search for timestamp matching
MAX_BLOCK_SEARCH_ITERATIONS = 20

# Archive RPC URL environment variable pattern
ARCHIVE_RPC_URL_ENV_PATTERN = "ARCHIVE_RPC_URL_{chain}"

# Supported chains for archive RPC URLs
ARCHIVE_RPC_CHAINS = ["ETHEREUM", "ARBITRUM", "BASE", "OPTIMISM", "POLYGON"]

# Default TWAP observation window in seconds (30 minutes)
DEFAULT_TWAP_WINDOW_SECONDS = 1800


# =============================================================================
# TWAP Exceptions
# =============================================================================


class TWAPInsufficientHistoryError(Exception):
    """Raised when pool lacks sufficient observation history for TWAP calculation.

    This can happen when:
    - The pool is newly deployed
    - The observation cardinality hasn't been expanded
    - The requested observation window exceeds available history

    Attributes:
        token: Token symbol
        pool_address: Address of the Uniswap V3 pool
        requested_seconds: Requested observation window
        available_seconds: Available observation history
    """

    def __init__(
        self,
        token: str,
        pool_address: str,
        requested_seconds: int,
        available_seconds: int | None = None,
    ):
        self.token = token
        self.pool_address = pool_address
        self.requested_seconds = requested_seconds
        self.available_seconds = available_seconds
        msg = f"Pool {pool_address} has insufficient history for {token} TWAP: requested {requested_seconds}s"
        if available_seconds is not None:
            msg += f", available ~{available_seconds}s"
        super().__init__(msg)


class TWAPPoolNotFoundError(Exception):
    """Raised when no pool is available for the requested token/chain."""

    def __init__(self, token: str, chain: str):
        self.token = token
        self.chain = chain
        super().__init__(f"No TWAP pool available for {token} on {chain}")


# =============================================================================
# TWAP Data Provider Implementation
# =============================================================================


@dataclass
class TWAPObservation:
    """Represents tick cumulative data from a Uniswap V3 pool observation.

    Attributes:
        tick_cumulative: Cumulative sum of tick values over time
        seconds_per_liquidity_cumulative_x128: Cumulative seconds per liquidity
        timestamp: When this observation was recorded
    """

    tick_cumulative: int
    seconds_per_liquidity_cumulative_x128: int
    timestamp: datetime | None = None


@dataclass
class TWAPResult:
    """Result of a TWAP calculation.

    Attributes:
        price: The calculated TWAP price in USD
        tick_twap: The time-weighted average tick
        observation_window_seconds: Actual observation window used
        pool_address: Address of the pool used
        token0_is_base: Whether token0 is the base token (token being priced)
    """

    price: Decimal
    tick_twap: int
    observation_window_seconds: int
    pool_address: str
    token0_is_base: bool


@dataclass
class CachedTWAP:
    """A single cached TWAP entry with TTL tracking.

    Attributes:
        price: The cached TWAP price
        result: Full TWAP calculation result
        fetched_at: When the TWAP was calculated
        ttl_seconds: Time-to-live for this cache entry
    """

    price: Decimal
    result: TWAPResult
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ttl_seconds: int = 60

    @property
    def is_expired(self) -> bool:
        """Check if this cache entry has expired based on TTL."""
        age = (datetime.now(UTC) - self.fetched_at).total_seconds()
        return age > self.ttl_seconds

    @property
    def age_seconds(self) -> float:
        """Get the age of this cache entry in seconds."""
        return (datetime.now(UTC) - self.fetched_at).total_seconds()


class TWAPDataProvider:
    """Uniswap V3 TWAP data provider implementation.

    Implements price fetching using Uniswap V3 oracle functionality.
    Queries pool.observe() to get tick cumulatives and calculates
    time-weighted average prices.

    TWAP Calculation:
        1. Query observe([secondsAgo, 0]) to get tick cumulatives at two points
        2. Calculate tick TWAP: (tickCumulative1 - tickCumulative0) / secondsAgo
        3. Convert tick to price: price = 1.0001^tick * (10^decimals0 / 10^decimals1)

    Attributes:
        chain: Blockchain network identifier
        rpc_url: RPC endpoint URL for on-chain queries
        observation_window_seconds: TWAP observation window (default: 300s = 5 minutes)

    Example:
        provider = TWAPDataProvider(
            chain="arbitrum",
            rpc_url="https://arb-mainnet.g.alchemy.com/v2/...",
        )
        price = await provider.get_latest_price("ETH")
    """

    # Supported chains
    _SUPPORTED_CHAINS = list(UNISWAP_V3_POOLS.keys())

    # Default provider priority (lower = higher priority)
    DEFAULT_PRIORITY = 20  # Lower priority than Chainlink (10)

    def __init__(
        self,
        chain: str = "arbitrum",
        rpc_url: str = "",
        observation_window_seconds: int | None = None,
        cache_ttl_seconds: int = 60,
        priority: int | None = None,
    ) -> None:
        """Initialize the TWAP data provider.

        Args:
            chain: Blockchain network identifier (ethereum, arbitrum, base, etc.)
            rpc_url: RPC endpoint URL for on-chain queries.
                     If not provided, will try ARCHIVE_RPC_URL_{CHAIN} env var,
                     then fall back to no RPC (cache-only mode).
            observation_window_seconds: TWAP observation window in seconds.
                                       Default 1800 (30 minutes).
            cache_ttl_seconds: TTL for cached TWAP data in seconds (default 60).
            priority: Provider priority for registry selection (lower = higher priority).
        """
        self._chain = chain.lower()
        self._observation_window_seconds = (
            observation_window_seconds if observation_window_seconds is not None else DEFAULT_TWAP_WINDOW_SECONDS
        )
        self._cache_ttl_seconds = cache_ttl_seconds
        self._priority = priority if priority is not None else self.DEFAULT_PRIORITY

        # Resolve RPC URL - try provided URL first, then env var
        self._rpc_url = rpc_url or self._get_archive_rpc_url()

        # Validate chain
        if self._chain not in UNISWAP_V3_POOLS:
            available = ", ".join(UNISWAP_V3_POOLS.keys())
            raise ValueError(f"Unsupported chain: {chain}. Available chains: {available}")

        # Get pools for this chain
        self._pools = UNISWAP_V3_POOLS[self._chain]

        # TWAP cache
        self._cache: dict[str, CachedTWAP] = {}

        # ETH price cache for two-hop pricing
        self._eth_price_cache: Decimal | None = None
        self._eth_price_fetched_at: datetime | None = None

        # Archive node access tracking (checked lazily on first historical query)
        self._archive_access_verified: bool = False
        self._has_archive_access: bool = False

        # Historical price cache: token -> list of (timestamp, price) tuples
        self._historical_cache: dict[str, list[tuple[datetime, Decimal]]] = {}

        # Block number cache: timestamp -> block_number
        self._block_cache: dict[int, int] = {}

        # Reference block info for timestamp estimation
        self._reference_block: int | None = None
        self._reference_timestamp: int | None = None

        logger.info(
            "Initialized TWAPDataProvider",
            extra={
                "chain": self._chain,
                "available_pools": len(self._pools),
                "observation_window_seconds": self._observation_window_seconds,
                "rpc_url": "configured" if self._rpc_url else "not configured",
                "cache_ttl_seconds": cache_ttl_seconds,
                "priority": self._priority,
            },
        )

    def _get_archive_rpc_url(self) -> str:
        """Get archive RPC URL from environment variable.

        Looks for ARCHIVE_RPC_URL_{CHAIN} environment variable.
        For example, for chain='arbitrum', looks for ARCHIVE_RPC_URL_ARBITRUM.

        Returns:
            Archive RPC URL or empty string if not configured.
        """
        env_var_name = ARCHIVE_RPC_URL_ENV_PATTERN.format(chain=self._chain.upper())
        url = os.environ.get(env_var_name, "")
        if url:
            logger.debug(f"Using archive RPC URL from {env_var_name}")
        return url

    @property
    def priority(self) -> int:
        """Return the provider priority for registry selection."""
        return self._priority

    @property
    def observation_window_seconds(self) -> int:
        """Return the TWAP observation window in seconds."""
        return self._observation_window_seconds

    def get_pool_address(self, token: str) -> str | None:
        """Get the Uniswap V3 pool address for a token on the current chain.

        Args:
            token: Token symbol (e.g., "ETH", "BTC")

        Returns:
            Pool contract address or None if not available
        """
        token_upper = token.upper()
        chain_pools = TOKEN_TO_POOL.get(token_upper, {})
        pool_key = chain_pools.get(self._chain)
        if pool_key is None:
            return None
        return self._pools.get(pool_key)

    def get_pool_key(self, token: str) -> str | None:
        """Get the pool key (e.g., 'WETH/USDC-500') for a token.

        Args:
            token: Token symbol

        Returns:
            Pool key or None if not available
        """
        token_upper = token.upper()
        chain_pools = TOKEN_TO_POOL.get(token_upper, {})
        return chain_pools.get(self._chain)

    # =========================================================================
    # On-Chain Query Methods
    # =========================================================================

    def _encode_observe_call(self, seconds_agos: list[int]) -> str:
        """Encode the observe() function call data.

        Args:
            seconds_agos: Array of seconds ago to observe (e.g., [300, 0])

        Returns:
            Hex-encoded calldata
        """
        # observe(uint32[] secondsAgos)
        # ABI encoding:
        # - selector (4 bytes)
        # - offset to array (32 bytes) = 0x20
        # - array length (32 bytes)
        # - array elements (32 bytes each, uint32 padded to uint256)

        # Calculate dynamic data offset (points to start of array)
        offset = 32  # 0x20

        # Array length
        length = len(seconds_agos)

        # Build calldata
        calldata = OBSERVE_SELECTOR  # selector
        calldata += offset.to_bytes(32, byteorder="big").hex()  # offset
        calldata += length.to_bytes(32, byteorder="big").hex()  # array length

        # Array elements (uint32 padded to 32 bytes)
        for sec in seconds_agos:
            calldata += sec.to_bytes(32, byteorder="big").hex()

        return calldata

    async def _query_observe(
        self,
        pool_address: str,
        seconds_agos: list[int],
    ) -> list[TWAPObservation] | None:
        """Query observe() from a Uniswap V3 pool.

        Args:
            pool_address: Address of the Uniswap V3 pool contract
            seconds_agos: Array of seconds ago to observe (e.g., [300, 0])

        Returns:
            List of TWAPObservation for each secondsAgo, or None if query fails
        """
        if not self._rpc_url:
            logger.debug("No RPC URL configured, cannot query observe()")
            return None

        try:
            from web3 import Web3

            web3 = Web3(Web3.HTTPProvider(self._rpc_url))
            pool_checksum = web3.to_checksum_address(pool_address)

            # Encode and execute call
            calldata = self._encode_observe_call(seconds_agos)
            result = web3.eth.call({"to": pool_checksum, "data": calldata})  # type: ignore[typeddict-item]

            # Decode response
            # Returns: (int56[] tickCumulatives, uint160[] secondsPerLiquidityCumulativeX128s)
            # ABI encoding:
            # - offset to tickCumulatives array (32 bytes)
            # - offset to secondsPerLiquidity array (32 bytes)
            # - tickCumulatives array: length (32) + elements (32 each, int56 padded)
            # - secondsPerLiquidity array: length (32) + elements (32 each, uint160 padded)

            if len(result) < 128:  # Minimum expected response
                logger.warning(f"Unexpected response length from observe(): {len(result)}")
                return None

            # Parse offsets
            offset_ticks = int.from_bytes(result[0:32], byteorder="big")
            offset_liquidity = int.from_bytes(result[32:64], byteorder="big")

            # Parse tickCumulatives array
            tick_array_start = offset_ticks
            tick_array_len = int.from_bytes(result[tick_array_start : tick_array_start + 32], byteorder="big")

            tick_cumulatives: list[int] = []
            for i in range(tick_array_len):
                element_start = tick_array_start + 32 + (i * 32)
                # int56 is signed, stored in last 7 bytes but we read as int256
                raw_value = int.from_bytes(
                    result[element_start : element_start + 32],
                    byteorder="big",
                    signed=True,
                )
                tick_cumulatives.append(raw_value)

            # Parse secondsPerLiquidityCumulativeX128s array
            liq_array_start = offset_liquidity
            liq_array_len = int.from_bytes(result[liq_array_start : liq_array_start + 32], byteorder="big")

            liquidity_cumulatives: list[int] = []
            for i in range(liq_array_len):
                element_start = liq_array_start + 32 + (i * 32)
                raw_value = int.from_bytes(result[element_start : element_start + 32], byteorder="big")
                liquidity_cumulatives.append(raw_value)

            # Build observations
            observations: list[TWAPObservation] = []
            for i in range(len(tick_cumulatives)):
                obs = TWAPObservation(
                    tick_cumulative=tick_cumulatives[i],
                    seconds_per_liquidity_cumulative_x128=(
                        liquidity_cumulatives[i] if i < len(liquidity_cumulatives) else 0
                    ),
                )
                observations.append(obs)

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
            pool_address: Address of the Uniswap V3 pool contract
            seconds_agos: Array of seconds ago to observe

        Returns:
            List of TWAPObservation or None if query fails
        """
        if not self._rpc_url:
            return None

        try:
            from web3 import Web3

            web3 = Web3(Web3.HTTPProvider(self._rpc_url))
            pool_checksum = web3.to_checksum_address(pool_address)

            calldata = self._encode_observe_call(seconds_agos)
            result = web3.eth.call({"to": pool_checksum, "data": calldata})  # type: ignore[typeddict-item]

            if len(result) < 128:
                return None

            # Parse offsets
            offset_ticks = int.from_bytes(result[0:32], byteorder="big")
            offset_liquidity = int.from_bytes(result[32:64], byteorder="big")

            # Parse tickCumulatives array
            tick_array_start = offset_ticks
            tick_array_len = int.from_bytes(result[tick_array_start : tick_array_start + 32], byteorder="big")

            tick_cumulatives: list[int] = []
            for i in range(tick_array_len):
                element_start = tick_array_start + 32 + (i * 32)
                raw_value = int.from_bytes(
                    result[element_start : element_start + 32],
                    byteorder="big",
                    signed=True,
                )
                tick_cumulatives.append(raw_value)

            # Parse liquidity array
            liq_array_start = offset_liquidity
            liq_array_len = int.from_bytes(result[liq_array_start : liq_array_start + 32], byteorder="big")

            liquidity_cumulatives: list[int] = []
            for i in range(liq_array_len):
                element_start = liq_array_start + 32 + (i * 32)
                raw_value = int.from_bytes(result[element_start : element_start + 32], byteorder="big")
                liquidity_cumulatives.append(raw_value)

            observations: list[TWAPObservation] = []
            for i in range(len(tick_cumulatives)):
                obs = TWAPObservation(
                    tick_cumulative=tick_cumulatives[i],
                    seconds_per_liquidity_cumulative_x128=(
                        liquidity_cumulatives[i] if i < len(liquidity_cumulatives) else 0
                    ),
                )
                observations.append(obs)

            return observations

        except Exception as e:
            logger.error(f"Failed to query observe() from pool {pool_address}: {e}")
            return None

    # =========================================================================
    # Historical Block Queries (Archive Node)
    # =========================================================================

    async def _verify_archive_access(self) -> bool:
        """Verify that the RPC endpoint supports historical (archive) queries.

        Tests by querying a recent block with state_override to confirm
        the node can access historical state.

        Returns:
            True if archive access is available, False otherwise
        """
        if self._archive_access_verified:
            return self._has_archive_access

        if not self._rpc_url:
            self._archive_access_verified = True
            self._has_archive_access = False
            return False

        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3

            web3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_url))

            # Get latest block to use as reference
            latest_block = await web3.eth.get_block("latest")
            self._reference_block = latest_block["number"]
            self._reference_timestamp = latest_block["timestamp"]

            # Try to query a block ~1000 blocks ago
            # If this fails with "missing trie node", the node is not archive
            test_block = max(1, latest_block["number"] - 1000)
            test_block_data = await web3.eth.get_block(test_block)

            if test_block_data is not None:
                # Successfully queried historical block - archive access available
                self._archive_access_verified = True
                self._has_archive_access = True
                logger.info(
                    "Archive node access verified for TWAP provider",
                    extra={"chain": self._chain, "test_block": test_block},
                )
                return True

        except Exception as e:
            error_msg = str(e).lower()
            # Common errors indicating non-archive node
            if "missing trie node" in error_msg or "pruned" in error_msg:
                logger.warning(
                    "RPC endpoint does not support archive queries (pruned/non-archive node). "
                    "Historical TWAP iteration will use current prices."
                )
            else:
                logger.warning(f"Failed to verify archive access: {e}")

        self._archive_access_verified = True
        self._has_archive_access = False
        return False

    async def _get_block_number_at_timestamp(self, target_timestamp: int) -> int | None:
        """Estimate the block number at a given Unix timestamp.

        Uses binary search with block time estimation to find the nearest block.

        Args:
            target_timestamp: Unix timestamp to find block for

        Returns:
            Block number closest to the timestamp, or None if unavailable
        """
        # Check cache first
        if target_timestamp in self._block_cache:
            return self._block_cache[target_timestamp]

        if not self._rpc_url:
            return None

        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3

            web3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_url))

            # Get reference block if not set
            if self._reference_block is None or self._reference_timestamp is None:
                latest = await web3.eth.get_block("latest")
                self._reference_block = latest["number"]
                self._reference_timestamp = latest["timestamp"]

            # Get average block time for this chain
            block_time = CHAIN_BLOCK_TIME_SECONDS.get(self._chain, 12.0)

            # Estimate block number based on time difference
            time_diff = self._reference_timestamp - target_timestamp
            estimated_blocks_diff = int(time_diff / block_time)
            estimated_block = max(1, self._reference_block - estimated_blocks_diff)

            # Binary search to refine the estimate
            low_block = max(1, estimated_block - 1000)
            high_block = min(self._reference_block, estimated_block + 1000)

            best_block = estimated_block
            best_diff = float("inf")

            for _ in range(MAX_BLOCK_SEARCH_ITERATIONS):
                if low_block >= high_block:
                    break

                mid_block = (low_block + high_block) // 2
                try:
                    block_data = await web3.eth.get_block(mid_block)
                    block_ts = block_data["timestamp"]
                    diff = abs(block_ts - target_timestamp)

                    if diff < best_diff:
                        best_diff = diff
                        best_block = mid_block

                    if block_ts < target_timestamp:
                        low_block = mid_block + 1
                    elif block_ts > target_timestamp:
                        high_block = mid_block - 1
                    else:
                        # Exact match
                        break
                except Exception:
                    # Block might not exist, narrow the range
                    high_block = mid_block - 1

            # Cache the result
            self._block_cache[target_timestamp] = best_block
            return best_block

        except Exception as e:
            logger.debug(f"Failed to get block at timestamp {target_timestamp}: {e}")
            return None

    async def _query_observe_at_block(
        self,
        pool_address: str,
        seconds_agos: list[int],
        block_number: int,
    ) -> list[TWAPObservation] | None:
        """Query observe() from a Uniswap V3 pool at a specific historical block.

        This method requires an archive node to access historical state.

        Args:
            pool_address: Address of the Uniswap V3 pool contract
            seconds_agos: Array of seconds ago to observe (e.g., [300, 0])
            block_number: Historical block number to query at

        Returns:
            List of TWAPObservation for each secondsAgo, or None if query fails
        """
        if not self._rpc_url:
            return None

        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3

            web3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_url))
            pool_checksum = web3.to_checksum_address(pool_address)

            # Encode and execute call at specific block
            calldata = self._encode_observe_call(seconds_agos)
            result = await web3.eth.call(
                {"to": pool_checksum, "data": calldata},  # type: ignore[typeddict-item]
                block_identifier=block_number,
            )

            # Decode response (same logic as _query_observe)
            if len(result) < 128:
                logger.debug(f"Unexpected response length from observe() at block {block_number}")
                return None

            # Parse offsets
            offset_ticks = int.from_bytes(result[0:32], byteorder="big")
            offset_liquidity = int.from_bytes(result[32:64], byteorder="big")

            # Parse tickCumulatives array
            tick_array_start = offset_ticks
            tick_array_len = int.from_bytes(result[tick_array_start : tick_array_start + 32], byteorder="big")

            tick_cumulatives: list[int] = []
            for i in range(tick_array_len):
                element_start = tick_array_start + 32 + (i * 32)
                raw_value = int.from_bytes(
                    result[element_start : element_start + 32],
                    byteorder="big",
                    signed=True,
                )
                tick_cumulatives.append(raw_value)

            # Parse liquidity array
            liq_array_start = offset_liquidity
            liq_array_len = int.from_bytes(result[liq_array_start : liq_array_start + 32], byteorder="big")

            liquidity_cumulatives: list[int] = []
            for i in range(liq_array_len):
                element_start = liq_array_start + 32 + (i * 32)
                raw_value = int.from_bytes(result[element_start : element_start + 32], byteorder="big")
                liquidity_cumulatives.append(raw_value)

            observations: list[TWAPObservation] = []
            for i in range(len(tick_cumulatives)):
                obs = TWAPObservation(
                    tick_cumulative=tick_cumulatives[i],
                    seconds_per_liquidity_cumulative_x128=(
                        liquidity_cumulatives[i] if i < len(liquidity_cumulatives) else 0
                    ),
                )
                observations.append(obs)

            return observations

        except Exception as e:
            error_msg = str(e).lower()
            if "missing trie node" in error_msg or "pruned" in error_msg:
                # Archive access not available
                logger.debug(f"Archive access not available for block {block_number}")
                return None
            logger.debug(f"Failed to query observe() at block {block_number}: {e}")
            return None

    async def _get_historical_price(
        self,
        token: str,
        target_timestamp: datetime,
    ) -> Decimal | None:
        """Get TWAP price for a token at a specific historical timestamp.

        Requires archive node access to query pool state at historical blocks.

        Args:
            token: Token symbol (e.g., "ETH", "BTC")
            target_timestamp: Historical timestamp to query

        Returns:
            TWAP price in USD at that timestamp, or None if unavailable
        """
        token_upper = token.upper()

        # Stablecoins always return $1
        stables = {"USDC", "USDT", "DAI", "FRAX", "LUSD", "BUSD", "USD"}
        if token_upper in stables:
            return Decimal("1")

        # Get pool info
        pool_address = self.get_pool_address(token_upper)
        pool_key = self.get_pool_key(token_upper)

        if pool_address is None or pool_key is None:
            return None

        # Get block number for timestamp
        unix_timestamp = int(target_timestamp.timestamp())
        block_number = await self._get_block_number_at_timestamp(unix_timestamp)

        if block_number is None:
            return None

        # Query observations at historical block
        seconds_agos = [self._observation_window_seconds, 0]
        observations = await self._query_observe_at_block(pool_address, seconds_agos, block_number)

        if observations is None or len(observations) < 2:
            return None

        # Determine pool configuration
        is_usdc_pair = "USDC" in pool_key.upper()
        token0_is_base = self._is_token_base(pool_key, token_upper)

        if is_usdc_pair:
            # Direct USD pricing
            token0_decimals = 18  # WETH
            token1_decimals = 6  # USDC
            invert = not token0_is_base

            price, _ = self._calculate_twap_from_observations(
                observations,
                self._observation_window_seconds,
                token0_decimals,
                token1_decimals,
                invert,
            )
        else:
            # Two-hop pricing via WETH
            token0_decimals = 18 if "WETH" not in pool_key.split("/")[0] else 8
            token1_decimals = 18  # WETH
            invert = not token0_is_base

            token_eth_price, _ = self._calculate_twap_from_observations(
                observations,
                self._observation_window_seconds,
                token0_decimals,
                token1_decimals,
                invert,
            )

            # Get ETH/USD price at same timestamp
            eth_price = await self._get_historical_eth_price(target_timestamp)
            if eth_price is None:
                return None

            price = token_eth_price * eth_price

        return price

    async def _get_historical_eth_price(self, target_timestamp: datetime) -> Decimal | None:
        """Get historical ETH/USD TWAP price for two-hop conversion.

        Args:
            target_timestamp: Historical timestamp to query

        Returns:
            ETH price in USD at that timestamp, or None if unavailable
        """
        # Get ETH/USDC pool
        eth_pool_address = self.get_pool_address("ETH")
        if eth_pool_address is None:
            return None

        # Get block number for timestamp
        unix_timestamp = int(target_timestamp.timestamp())
        block_number = await self._get_block_number_at_timestamp(unix_timestamp)

        if block_number is None:
            return None

        # Query observations at historical block
        seconds_agos = [self._observation_window_seconds, 0]
        observations = await self._query_observe_at_block(eth_pool_address, seconds_agos, block_number)

        if observations is None or len(observations) < 2:
            return None

        # WETH/USDC: token0=WETH(18), token1=USDC(6)
        price, _ = self._calculate_twap_from_observations(
            observations,
            self._observation_window_seconds,
            token0_decimals=18,
            token1_decimals=6,
            invert=False,
        )

        return price

    # =========================================================================
    # TWAP Calculation
    # =========================================================================

    def _tick_to_price(
        self,
        tick: int,
        token0_decimals: int = 18,
        token1_decimals: int = 6,
        invert: bool = False,
    ) -> Decimal:
        """Convert a Uniswap V3 tick to a price.

        Uniswap V3 tick formula: price = 1.0001^tick
        This gives token1/token0 price.

        Args:
            tick: The tick value
            token0_decimals: Decimals of token0 (default 18 for WETH)
            token1_decimals: Decimals of token1 (default 6 for USDC)
            invert: If True, return token0/token1 instead of token1/token0

        Returns:
            Price as Decimal
        """
        # Base price from tick: 1.0001^tick
        base_price = Decimal(str(math.pow(1.0001, tick)))

        # Adjust for decimal difference
        decimal_adjustment = Decimal(10 ** (token0_decimals - token1_decimals))
        adjusted_price = base_price * decimal_adjustment

        if invert:
            if adjusted_price == 0:
                return Decimal("0")
            return Decimal("1") / adjusted_price

        return adjusted_price

    def _calculate_twap_from_observations(
        self,
        observations: list[TWAPObservation],
        seconds_elapsed: int,
        token0_decimals: int = 18,
        token1_decimals: int = 6,
        invert: bool = False,
    ) -> tuple[Decimal, int]:
        """Calculate TWAP price from tick cumulative observations.

        TWAP tick = (tickCumulative_now - tickCumulative_ago) / secondsElapsed

        Args:
            observations: List of [ago_observation, now_observation]
            seconds_elapsed: Time between observations
            token0_decimals: Decimals of token0
            token1_decimals: Decimals of token1
            invert: If True, return inverted price

        Returns:
            Tuple of (TWAP price, TWAP tick)
        """
        if len(observations) < 2:
            raise ValueError("Need at least 2 observations for TWAP calculation")

        tick_cumulative_ago = observations[0].tick_cumulative
        tick_cumulative_now = observations[1].tick_cumulative

        # Calculate average tick over the window
        tick_diff = tick_cumulative_now - tick_cumulative_ago
        tick_twap = tick_diff // seconds_elapsed

        # Convert tick to price
        price = self._tick_to_price(
            tick_twap,
            token0_decimals,
            token1_decimals,
            invert,
        )

        return price, tick_twap

    def _is_token_base(self, pool_key: str, token: str) -> bool:
        """Determine if the token is the base (token0) in the pool.

        For WETH/USDC pool:
        - WETH is token0
        - USDC is token1
        - Price returned is USDC/WETH (how much USDC per WETH)

        Args:
            pool_key: Pool identifier (e.g., 'WETH/USDC-500')
            token: Token being priced

        Returns:
            True if token is token0, False if token1
        """
        token_upper = token.upper()
        # Parse pool key: "TOKEN0/TOKEN1-FEE"
        pair_part = pool_key.split("-")[0]  # "TOKEN0/TOKEN1"
        tokens = pair_part.split("/")  # ["TOKEN0", "TOKEN1"]

        if len(tokens) != 2:
            return True  # Default assumption

        # Check if our token matches token0 or is ETH (matches WETH)
        token0 = tokens[0].upper()
        if token_upper == token0:
            return True
        if token_upper == "ETH" and token0 == "WETH":
            return True

        return False

    # =========================================================================
    # Price Fetching Methods
    # =========================================================================

    async def get_latest_price(
        self,
        token: str,
        use_cache: bool = True,
    ) -> Decimal | None:
        """Get the latest TWAP price for a token.

        Queries the Uniswap V3 pool's observe() function to calculate
        a time-weighted average price over the configured window.

        For tokens paired with WETH (not USDC), performs two-hop pricing:
        1. Get token/WETH TWAP
        2. Get WETH/USDC TWAP
        3. Multiply for token/USDC price

        Args:
            token: Token symbol (e.g., "ETH", "BTC", "LINK")
            use_cache: If True, check cache before querying on-chain. Default True.

        Returns:
            TWAP price in USD, or None if unavailable.

        Raises:
            TWAPPoolNotFoundError: If no pool is available for the token
            TWAPInsufficientHistoryError: If pool lacks sufficient history
        """
        token_upper = token.upper()

        # Stablecoins always return $1
        stables = {"USDC", "USDT", "DAI", "FRAX", "LUSD", "BUSD", "USD"}
        if token_upper in stables:
            return Decimal("1")

        # Check cache first
        if use_cache and token_upper in self._cache:
            cached = self._cache[token_upper]
            if not cached.is_expired:
                logger.debug(f"TWAP cache hit for {token}: ${cached.price:.4f} (age: {cached.age_seconds:.1f}s)")
                return cached.price

        # Get pool info
        pool_address = self.get_pool_address(token_upper)
        pool_key = self.get_pool_key(token_upper)

        if pool_address is None or pool_key is None:
            raise TWAPPoolNotFoundError(token_upper, self._chain)

        # Query observations
        seconds_agos = [self._observation_window_seconds, 0]
        observations = await self._query_observe(pool_address, seconds_agos)

        if observations is None or len(observations) < 2:
            raise TWAPInsufficientHistoryError(
                token_upper,
                pool_address,
                self._observation_window_seconds,
            )

        # Determine pool configuration
        is_usdc_pair = "USDC" in pool_key.upper()
        token0_is_base = self._is_token_base(pool_key, token_upper)

        # For USDC pairs (e.g., WETH/USDC), price is USDC per token
        # For WETH pairs (e.g., WBTC/WETH), need to multiply by ETH price
        if is_usdc_pair:
            # Direct USD pricing
            token0_decimals = 18  # WETH
            token1_decimals = 6  # USDC
            invert = not token0_is_base  # Invert if token is token1

            price, tick_twap = self._calculate_twap_from_observations(
                observations,
                self._observation_window_seconds,
                token0_decimals,
                token1_decimals,
                invert,
            )
        else:
            # Two-hop pricing via WETH
            # First, get token/WETH price
            token0_decimals = 18 if "WETH" not in pool_key.split("/")[0] else 8  # WBTC is 8
            token1_decimals = 18  # WETH
            invert = not token0_is_base

            token_eth_price, tick_twap = self._calculate_twap_from_observations(
                observations,
                self._observation_window_seconds,
                token0_decimals,
                token1_decimals,
                invert,
            )

            # Get ETH/USD price for conversion
            eth_price = await self._get_eth_price_for_conversion()
            if eth_price is None:
                logger.warning(f"Could not get ETH price for {token} two-hop pricing")
                return None

            price = token_eth_price * eth_price

        # Cache the result
        result = TWAPResult(
            price=price,
            tick_twap=tick_twap,
            observation_window_seconds=self._observation_window_seconds,
            pool_address=pool_address,
            token0_is_base=token0_is_base,
        )
        self._cache[token_upper] = CachedTWAP(
            price=price,
            result=result,
            ttl_seconds=self._cache_ttl_seconds,
        )

        logger.debug(f"TWAP price for {token}: ${price:.4f} (tick={tick_twap})")
        return price

    async def _get_eth_price_for_conversion(self) -> Decimal | None:
        """Get ETH price for two-hop conversion.

        Caches the ETH price to avoid redundant queries within the same session.

        Returns:
            ETH price in USD, or None if unavailable
        """
        # Check cached ETH price
        if self._eth_price_fetched_at is not None and self._eth_price_cache is not None:
            age = (datetime.now(UTC) - self._eth_price_fetched_at).total_seconds()
            if age < self._cache_ttl_seconds:
                return self._eth_price_cache

        # Query ETH/USDC pool
        eth_pool_address = self.get_pool_address("ETH")
        if eth_pool_address is None:
            return None

        seconds_agos = [self._observation_window_seconds, 0]
        observations = await self._query_observe(eth_pool_address, seconds_agos)

        if observations is None or len(observations) < 2:
            return None

        # WETH/USDC: token0=WETH(18), token1=USDC(6)
        # Price is USDC per WETH
        price, _ = self._calculate_twap_from_observations(
            observations,
            self._observation_window_seconds,
            token0_decimals=18,
            token1_decimals=6,
            invert=False,
        )

        self._eth_price_cache = price
        self._eth_price_fetched_at = datetime.now(UTC)

        return price

    def get_latest_price_sync(
        self,
        token: str,
        use_cache: bool = True,
    ) -> Decimal | None:
        """Synchronous version of get_latest_price.

        Args:
            token: Token symbol
            use_cache: If True, check cache before querying. Default True.

        Returns:
            TWAP price in USD, or None if unavailable
        """
        token_upper = token.upper()

        # Stablecoins
        stables = {"USDC", "USDT", "DAI", "FRAX", "LUSD", "BUSD", "USD"}
        if token_upper in stables:
            return Decimal("1")

        # Check cache
        if use_cache and token_upper in self._cache:
            cached = self._cache[token_upper]
            if not cached.is_expired:
                return cached.price

        # Get pool info
        pool_address = self.get_pool_address(token_upper)
        pool_key = self.get_pool_key(token_upper)

        if pool_address is None or pool_key is None:
            return None

        # Query observations
        seconds_agos = [self._observation_window_seconds, 0]
        observations = self._query_observe_sync(pool_address, seconds_agos)

        if observations is None or len(observations) < 2:
            return None

        # Calculate price
        is_usdc_pair = "USDC" in pool_key.upper()
        token0_is_base = self._is_token_base(pool_key, token_upper)

        if is_usdc_pair:
            token0_decimals = 18
            token1_decimals = 6
            invert = not token0_is_base

            price, tick_twap = self._calculate_twap_from_observations(
                observations,
                self._observation_window_seconds,
                token0_decimals,
                token1_decimals,
                invert,
            )
        else:
            # Two-hop pricing - simplified sync version
            token0_decimals = 18 if "WETH" not in pool_key.split("/")[0] else 8
            token1_decimals = 18
            invert = not token0_is_base

            token_eth_price, tick_twap = self._calculate_twap_from_observations(
                observations,
                self._observation_window_seconds,
                token0_decimals,
                token1_decimals,
                invert,
            )

            # Get cached ETH price or query
            eth_price = self._get_eth_price_sync()
            if eth_price is None:
                return None

            price = token_eth_price * eth_price

        # Cache
        result = TWAPResult(
            price=price,
            tick_twap=tick_twap,
            observation_window_seconds=self._observation_window_seconds,
            pool_address=pool_address,
            token0_is_base=token0_is_base,
        )
        self._cache[token_upper] = CachedTWAP(
            price=price,
            result=result,
            ttl_seconds=self._cache_ttl_seconds,
        )

        return price

    def _get_eth_price_sync(self) -> Decimal | None:
        """Synchronous ETH price for two-hop conversion."""
        if self._eth_price_fetched_at is not None and self._eth_price_cache is not None:
            age = (datetime.now(UTC) - self._eth_price_fetched_at).total_seconds()
            if age < self._cache_ttl_seconds:
                return self._eth_price_cache

        eth_pool_address = self.get_pool_address("ETH")
        if eth_pool_address is None:
            return None

        seconds_agos = [self._observation_window_seconds, 0]
        observations = self._query_observe_sync(eth_pool_address, seconds_agos)

        if observations is None or len(observations) < 2:
            return None

        price, _ = self._calculate_twap_from_observations(
            observations,
            self._observation_window_seconds,
            token0_decimals=18,
            token1_decimals=6,
            invert=False,
        )

        self._eth_price_cache = price
        self._eth_price_fetched_at = datetime.now(UTC)

        return price

    async def get_price(
        self,
        token: str,
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Get the price of a token at a specific timestamp.

        Note: TWAP provider primarily supports live/near-live prices.
        Historical TWAP requires archive node access and is not currently supported.

        Args:
            token: Token symbol
            timestamp: The point in time (ignored for TWAP - always returns current)

        Returns:
            Price in USD

        Raises:
            ValueError: If price is not available
        """
        price = await self.get_latest_price(token)
        if price is None:
            raise ValueError(f"TWAP price not available for {token}")
        return price

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for a token over a time range.

        Note: TWAP provides spot prices only, not OHLCV.
        Returns pseudo-OHLCV with current TWAP for all values.

        Args:
            token: Token symbol
            start: Start of the time range
            end: End of the time range
            interval_seconds: Candle interval in seconds

        Returns:
            List of OHLCV data points with TWAP price
        """
        price = await self.get_latest_price(token)
        if price is None:
            return []

        # Generate pseudo-OHLCV
        ohlcv_list: list[OHLCV] = []
        current = start
        if current.tzinfo is None:
            current = current.replace(tzinfo=UTC)

        end_tz = end
        if end_tz.tzinfo is None:
            end_tz = end_tz.replace(tzinfo=UTC)

        interval = timedelta(seconds=interval_seconds)

        while current <= end_tz:
            ohlcv = OHLCV(
                timestamp=current,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=None,
            )
            ohlcv_list.append(ohlcv)
            current += interval

        return ohlcv_list

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical market states using actual historical TWAP prices.

        When archive node access is available, this method fetches actual historical
        prices by querying pool observe() at historical block numbers. Without archive
        access, it falls back to using current prices with a warning.

        Args:
            config: Configuration specifying time range, interval, and tokens

        Yields:
            Tuples of (timestamp, MarketState) for each time point
        """
        # Check for archive node access
        has_archive = await self._verify_archive_access()

        if has_archive:
            logger.info(
                f"Starting TWAP iteration with historical prices from {config.start_time} "
                f"to {config.end_time} (archive node available)"
            )
        else:
            logger.warning(
                f"Starting TWAP iteration from {config.start_time} to {config.end_time} "
                f"WITHOUT archive access - falling back to current TWAP prices. "
                f"For accurate historical backtests, use an archive node RPC endpoint."
            )

        # Generate timestamps
        current_time = config.start_time
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=UTC)

        end_time = config.end_time
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        interval = timedelta(seconds=config.interval_seconds)
        data_points = 0
        historical_price_hits = 0
        fallback_price_hits = 0

        # If no archive access, pre-fetch current prices once
        current_prices: dict[str, Decimal] = {}
        if not has_archive:
            for token in config.tokens:
                try:
                    price = await self.get_latest_price(token)
                    if price is not None:
                        current_prices[token.upper()] = price
                except (TWAPPoolNotFoundError, TWAPInsufficientHistoryError) as e:
                    logger.warning(f"Skipping {token}: {e}")

        while current_time <= end_time:
            prices: dict[str, Decimal] = {}
            ohlcv_data: dict[str, OHLCV] = {}

            if has_archive:
                # Fetch actual historical prices at this timestamp
                for token in config.tokens:
                    try:
                        price = await self._get_historical_price(token, current_time)
                        if price is not None:
                            prices[token.upper()] = price
                            historical_price_hits += 1
                        else:
                            # Fallback to current price for this token
                            fallback_price = await self.get_latest_price(token)
                            if fallback_price is not None:
                                prices[token.upper()] = fallback_price
                                fallback_price_hits += 1
                    except (TWAPPoolNotFoundError, TWAPInsufficientHistoryError) as e:
                        logger.debug(f"Skipping {token} at {current_time}: {e}")
            else:
                # Use pre-fetched current prices
                prices = current_prices.copy()
                fallback_price_hits += len(prices)

            if config.include_ohlcv:
                for token, price in prices.items():
                    ohlcv_data[token] = OHLCV(
                        timestamp=current_time,
                        open=price,
                        high=price,
                        low=price,
                        close=price,
                        volume=None,
                    )

            # Determine data source for metadata
            data_source = "twap_historical" if has_archive else "twap_current"

            # Get block number if archive available
            block_number: int | None = None
            if has_archive:
                block_number = await self._get_block_number_at_timestamp(int(current_time.timestamp()))

            market_state = MarketState(
                timestamp=current_time,
                prices=prices,
                ohlcv=ohlcv_data if config.include_ohlcv else {},
                chain=config.chains[0] if config.chains else self._chain,
                block_number=block_number,
                gas_price_gwei=None,
                metadata={
                    "data_source": data_source,
                    "archive_available": has_archive,
                },
            )

            yield (current_time, market_state)

            current_time += interval
            data_points += 1

            # Log progress periodically
            if data_points % 100 == 0:
                logger.debug(
                    f"TWAP iteration progress: {data_points} points, "
                    f"historical_hits={historical_price_hits}, fallback_hits={fallback_price_hits}"
                )

        logger.info(
            f"Completed TWAP iteration with {data_points} data points "
            f"(historical: {historical_price_hits}, fallback: {fallback_price_hits})"
        )

    def clear_cache(self, token: str | None = None) -> None:
        """Clear the TWAP cache.

        Args:
            token: Specific token to clear, or None to clear all
        """
        if token is not None:
            self._cache.pop(token.upper(), None)
            self._historical_cache.pop(token.upper(), None)
        else:
            self._cache.clear()
            self._eth_price_cache = None
            self._eth_price_fetched_at = None
            self._historical_cache.clear()
            self._block_cache.clear()

    @property
    def provider_name(self) -> str:
        """Return the unique name of this data provider."""
        return f"twap_{self._chain}"

    @property
    def supported_tokens(self) -> list[str]:
        """Return list of supported token symbols for the current chain."""
        supported: list[str] = []
        for token, chain_pools in TOKEN_TO_POOL.items():
            if self._chain in chain_pools:
                supported.append(token)
        return supported

    @property
    def supported_chains(self) -> list[str]:
        """Return list of supported chain identifiers."""
        return self._SUPPORTED_CHAINS.copy()

    @property
    def min_timestamp(self) -> datetime | None:
        """Return the earliest timestamp with available data."""
        # TWAP only provides current prices
        return datetime.now(UTC) - timedelta(seconds=self._observation_window_seconds)

    @property
    def max_timestamp(self) -> datetime | None:
        """Return the latest timestamp with available data."""
        return datetime.now(UTC)

    @property
    def historical_capability(self) -> HistoricalDataCapability:
        """Return the historical data capability of this provider.

        Returns FULL when archive node access has been verified (can query
        pool observe() at historical blocks). Returns CURRENT_ONLY when no
        archive access is available and only current prices can be fetched.

        Note: Archive access is verified lazily on first historical query,
        so this property may initially return CURRENT_ONLY until iterate()
        or a historical query is called. Once verified, the status is cached.
        """
        # If archive access has been verified, return appropriate capability
        if self._archive_access_verified:
            return HistoricalDataCapability.FULL if self._has_archive_access else HistoricalDataCapability.CURRENT_ONLY

        # If RPC URL is configured, we potentially have FULL capability
        # (will be verified on first use)
        if self._rpc_url:
            # Indicate potential capability, but mark that it needs verification
            # Return CURRENT_ONLY until explicitly verified to avoid false positives
            return HistoricalDataCapability.CURRENT_ONLY

        return HistoricalDataCapability.CURRENT_ONLY

    async def close(self) -> None:
        """Close any resources."""
        pass

    async def __aenter__(self) -> "TWAPDataProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


__all__ = [
    "TWAPDataProvider",
    "TWAPObservation",
    "TWAPResult",
    "CachedTWAP",
    "TWAPInsufficientHistoryError",
    "TWAPPoolNotFoundError",
    "UNISWAP_V3_POOLS",
    "TOKEN_TO_POOL",
    "OBSERVE_SELECTOR",
    "SLOT0_SELECTOR",
    "ETHEREUM_POOLS",
    "ARBITRUM_POOLS",
    "BASE_POOLS",
    "OPTIMISM_POOLS",
    "POLYGON_POOLS",
    # Configuration constants
    "ARCHIVE_RPC_URL_ENV_PATTERN",
    "ARCHIVE_RPC_CHAINS",
    "DEFAULT_TWAP_WINDOW_SECONDS",
]
