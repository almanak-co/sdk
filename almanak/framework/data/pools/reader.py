"""Pool price readers for the Quant Data Layer.

Reads live prices from concentrated-liquidity DEX pool contracts (Uniswap V3,
Aerodrome CL, PancakeSwap V3) by decoding slot0() responses and converting
sqrtPriceX96 to human-readable prices using token decimals from TokenResolver.

All readers share the same slot0()-based ABI and differ only in factory
addresses and known pool registries.

All returns are wrapped in DataEnvelope[PoolPrice] with provenance metadata
including block number, finality, and source identification.

Example:
    from almanak.framework.data.pools.reader import UniswapV3PoolPriceReader

    reader = UniswapV3PoolPriceReader(rpc_call=my_rpc_call_fn)
    envelope = reader.read_pool_price("0x88e6A0c2...", "ethereum")
    print(envelope.price)       # Decimal("1823.45")
    print(envelope.meta.source) # "alchemy_rpc"

    # Use PoolReaderRegistry for dynamic dispatch:
    from almanak.framework.data.pools.reader import PoolReaderRegistry
    registry = PoolReaderRegistry(rpc_call=my_rpc_call_fn)
    reader = registry.get_reader("base", "aerodrome")
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Uniswap V3 slot0() function selector
SLOT0_SELECTOR = "0x3850c7bd"

# Uniswap V3 liquidity() function selector
LIQUIDITY_SELECTOR = "0x1a686502"

# Uniswap V3 token0() / token1() / fee() selectors
TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"
FEE_SELECTOR = "0xddca3f43"

# Q96 = 2**96 for sqrtPriceX96 decoding
Q96 = 2**96

# Uniswap V3 factory addresses per chain (same on most chains)
UNISWAP_V3_FACTORY: dict[str, str] = {
    "ethereum": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "arbitrum": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "optimism": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "polygon": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "base": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
}

# Uniswap V3 pool init code hash for CREATE2 address computation
POOL_INIT_CODE_HASH = "0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54"

# getPool(address,address,uint24) selector on factory
GET_POOL_SELECTOR = "0x1698ee82"

# Well-known Uniswap V3 pool addresses for fast lookup
# Format: {chain: {(token_a_lower, token_b_lower, fee_tier): pool_address}}
_KNOWN_POOLS: dict[str, dict[tuple[str, str, int], str]] = {
    "ethereum": {
        # USDC/WETH 0.05%
        (
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            500,
        ): "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        # USDC/WETH 0.3%
        (
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            3000,
        ): "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",
    },
    "arbitrum": {
        # WETH/USDC 0.05% (keys sorted: 0x82af < 0xaf88)
        (
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            500,
        ): "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
        # WETH/USDC 0.3%
        (
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            3000,
        ): "0xc473e2aEE3441BF9240Be85eb122aBB059A3B57c",
    },
    "base": {
        # WETH/USDC 0.05% (keys sorted: 0x4200 < 0x8335)
        (
            "0x4200000000000000000000000000000000000006",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            500,
        ): "0xd0b53D9277642d899DF5C87A3966A349A798F224",
    },
}

# ---------------------------------------------------------------------------
# Aerodrome CL factory addresses and known pools (Base only)
# ---------------------------------------------------------------------------

AERODROME_CL_FACTORY: dict[str, str] = {
    "base": "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A",
}

_AERODROME_KNOWN_POOLS: dict[str, dict[tuple[str, str, int], str]] = {
    "base": {
        # USDC/WETH tick spacing 1 (0.01%)
        (
            "0x4200000000000000000000000000000000000006",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            100,
        ): "0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59",
        # USDC/WETH tick spacing 100 (1%)
        (
            "0x4200000000000000000000000000000000000006",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            200,
        ): "0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d",
    },
}

# ---------------------------------------------------------------------------
# PancakeSwap V3 factory addresses and known pools
# ---------------------------------------------------------------------------

PANCAKESWAP_V3_FACTORY: dict[str, str] = {
    "ethereum": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
    "arbitrum": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
    "base": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
    "bsc": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
}

_PANCAKESWAP_KNOWN_POOLS: dict[str, dict[tuple[str, str, int], str]] = {
    "arbitrum": {
        # USDC/WETH 0.05%
        (
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            500,
        ): "0xd9E2A1A61B6e61b275ceC326465D417E52c1A621",
    },
    "ethereum": {
        # USDC/WETH 0.05%
        (
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            500,
        ): "0x6CA298D2983aB03Aa1dA7679389D955A4eFEE15C",
    },
}

# RpcCallFn type: (chain, to_address, calldata_hex) -> bytes
# This abstracts over gateway RPC vs direct Web3 calls.
RpcCallFn = Any  # Callable[[str, str, str], bytes] — but we avoid typing import cost


# ---------------------------------------------------------------------------
# PoolPrice dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PoolPrice:
    """Price data from a single on-chain DEX pool.

    Attributes:
        price: Human-readable price of token0 in terms of token1.
        tick: Current pool tick from slot0.
        liquidity: Current in-range liquidity from the pool.
        fee_tier: Pool fee in basis points (e.g. 500 = 0.05%).
        block_number: Block at which the data was read.
        timestamp: UTC datetime of the observation.
        pool_address: Address of the pool contract.
        token0_decimals: Decimals of token0.
        token1_decimals: Decimals of token1.
    """

    price: Decimal
    tick: int
    liquidity: int
    fee_tier: int
    block_number: int
    timestamp: datetime
    pool_address: str = ""
    token0_decimals: int = 18
    token1_decimals: int = 6


# ---------------------------------------------------------------------------
# sqrtPriceX96 decoding
# ---------------------------------------------------------------------------


def decode_sqrt_price_x96(
    sqrt_price_x96: int,
    token0_decimals: int,
    token1_decimals: int,
) -> Decimal:
    """Decode sqrtPriceX96 from Uniswap V3 slot0 into a human-readable price.

    In Uniswap V3, sqrtPriceX96 = sqrt(price) * 2^96 where
    price = token1_raw / token0_raw (in raw units, not human-readable).

    Human-readable price (token0 in terms of token1):
        price = (sqrtPriceX96 / 2^96)^2 * 10^(token0_decimals - token1_decimals)

    Args:
        sqrt_price_x96: The sqrtPriceX96 value from slot0.
        token0_decimals: Decimals of token0.
        token1_decimals: Decimals of token1.

    Returns:
        Human-readable price of token0 in terms of token1.
    """
    sqrt_price = Decimal(sqrt_price_x96) / Decimal(Q96)
    raw_price = sqrt_price * sqrt_price
    # Adjust for decimal difference
    decimal_adjustment = Decimal(10) ** (token0_decimals - token1_decimals)
    return raw_price * decimal_adjustment


# ---------------------------------------------------------------------------
# slot0 response decoding
# ---------------------------------------------------------------------------


def decode_slot0(data: bytes) -> tuple[int, int]:
    """Decode slot0() return data into (sqrtPriceX96, tick).

    slot0() returns 7 values packed in 7 x 32-byte words:
        sqrtPriceX96 (uint160), tick (int24), ...

    Args:
        data: Raw bytes from eth_call for slot0().

    Returns:
        Tuple of (sqrtPriceX96, tick).

    Raises:
        DataUnavailableError: If response too short.
    """
    if len(data) < 64:
        raise DataUnavailableError(
            data_type="pool_price",
            instrument="unknown",
            reason=f"slot0() response too short: {len(data)} bytes (need >= 64)",
        )
    sqrt_price_x96 = int.from_bytes(data[0:32], byteorder="big")
    # tick is int24, stored as int256 (signed 32 bytes)
    tick = int.from_bytes(data[32:64], byteorder="big", signed=True)
    return sqrt_price_x96, tick


def decode_uint(data: bytes) -> int:
    """Decode a single uint256 return value."""
    if len(data) < 32:
        raise DataUnavailableError(
            data_type="pool_price",
            instrument="unknown",
            reason=f"uint response too short: {len(data)} bytes",
        )
    return int.from_bytes(data[0:32], byteorder="big")


def decode_address(data: bytes) -> str:
    """Decode a single address return value (rightmost 20 bytes of word)."""
    if len(data) < 32:
        raise DataUnavailableError(
            data_type="pool_price",
            instrument="unknown",
            reason=f"address response too short: {len(data)} bytes",
        )
    return "0x" + data[12:32].hex()


# ---------------------------------------------------------------------------
# UniswapV3PoolPriceReader
# ---------------------------------------------------------------------------


class UniswapV3PoolPriceReader:
    """Reads live prices from Uniswap V3 pool contracts.

    Wraps all results in DataEnvelope[PoolPrice] with provenance metadata.
    Uses a simple time-based cache with configurable TTL.

    Subclasses (AerodromePoolReader, PancakeSwapV3PoolReader) override
    ``_factory_addresses`` and ``_known_pools`` to provide protocol-specific
    pool registries while reusing the shared slot0()-based reading logic.

    Args:
        rpc_call: Callable(chain, to_address, calldata_hex) -> bytes.
            Abstracts over gateway RPC or direct Web3 eth_call.
        token_resolver: Optional TokenResolver for decimal lookups.
            If None, decimals are read from the pool's token contracts.
        cache_ttl_seconds: Cache TTL in seconds (default 2 for L2, 12 for L1).
        source_name: Source identifier for DataMeta (default "alchemy_rpc").
    """

    # Protocol-specific config — overridden by subclasses
    _factory_addresses: dict[str, str] = UNISWAP_V3_FACTORY
    _known_pools: dict[str, dict[tuple[str, str, int], str]] = _KNOWN_POOLS
    protocol_name: str = "uniswap_v3"

    def __init__(
        self,
        rpc_call: RpcCallFn,
        token_resolver: Any | None = None,
        cache_ttl_seconds: float = 2.0,
        source_name: str = "alchemy_rpc",
    ) -> None:
        self._rpc_call = rpc_call
        self._token_resolver = token_resolver
        self._cache_ttl = cache_ttl_seconds
        self._source_name = source_name
        # Cache: {(pool_address_lower, chain): (mono_time, envelope)}
        self._cache: dict[tuple[str, str], tuple[float, DataEnvelope[PoolPrice]]] = {}

    # ----- public API -----

    def read_pool_price(
        self,
        pool_address: str,
        chain: str,
        block_number: int | None = None,
        finality: str = "latest",
    ) -> DataEnvelope[PoolPrice]:
        """Read the current price from a Uniswap V3 pool.

        Calls slot0() and liquidity() on the pool contract, decodes
        sqrtPriceX96 into a human-readable price using token decimals,
        and returns the result wrapped in a DataEnvelope.

        Args:
            pool_address: Pool contract address.
            chain: Chain name (e.g. "arbitrum", "ethereum").
            block_number: Optional block number (if known from RPC response).
            finality: Block finality tag (default "latest").

        Returns:
            DataEnvelope[PoolPrice] with provenance metadata.

        Raises:
            DataUnavailableError: If RPC call fails or response is invalid.
        """
        chain_lower = chain.lower()
        pool_lower = pool_address.lower()
        cache_key = (pool_lower, chain_lower)

        # Check cache
        now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None:
            cached_time, cached_envelope = cached
            if now - cached_time < self._cache_ttl:
                # Return cached with updated cache_hit flag
                return DataEnvelope(
                    value=cached_envelope.value,
                    meta=DataMeta(
                        source=cached_envelope.meta.source,
                        observed_at=cached_envelope.meta.observed_at,
                        block_number=cached_envelope.meta.block_number,
                        finality=cached_envelope.meta.finality,
                        staleness_ms=int((now - cached_time) * 1000),
                        latency_ms=0,
                        confidence=cached_envelope.meta.confidence,
                        cache_hit=True,
                    ),
                    classification=DataClassification.EXECUTION_GRADE,
                )

        start_time = time.monotonic()

        try:
            # Read slot0
            slot0_data = self._rpc_call(chain_lower, pool_address, SLOT0_SELECTOR)
            sqrt_price_x96, tick = decode_slot0(slot0_data)

            # Read liquidity
            liquidity_data = self._rpc_call(chain_lower, pool_address, LIQUIDITY_SELECTOR)
            liquidity = decode_uint(liquidity_data)

            # Get token decimals
            token0_decimals, token1_decimals, fee_tier = self._get_pool_metadata(pool_address, chain_lower)

            # Decode price
            price = decode_sqrt_price_x96(sqrt_price_x96, token0_decimals, token1_decimals)

        except DataUnavailableError:
            raise
        except Exception as e:
            raise DataUnavailableError(
                data_type="pool_price",
                instrument=pool_address,
                reason=f"RPC call failed for pool {pool_address} on {chain_lower}: {e}",
            ) from e

        latency_ms = int((time.monotonic() - start_time) * 1000)
        observed_at = datetime.now(UTC)
        effective_block = block_number or 0

        pool_price = PoolPrice(
            price=price,
            tick=tick,
            liquidity=liquidity,
            fee_tier=fee_tier,
            block_number=effective_block,
            timestamp=observed_at,
            pool_address=pool_address,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
        )

        meta = DataMeta(
            source=self._source_name,
            observed_at=observed_at,
            block_number=effective_block if effective_block > 0 else None,
            finality=finality,
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=1.0,
            cache_hit=False,
        )

        envelope = DataEnvelope(
            value=pool_price,
            meta=meta,
            classification=DataClassification.EXECUTION_GRADE,
        )

        # Store in cache
        self._cache[cache_key] = (time.monotonic(), envelope)

        logger.debug(
            "pool_price_read",
            extra={
                "pool": pool_address,
                "chain": chain_lower,
                "price": str(price),
                "tick": tick,
                "latency_ms": latency_ms,
            },
        )

        return envelope

    def resolve_pool_address(
        self,
        token_a: str,
        token_b: str,
        chain: str,
        fee_tier: int = 3000,
    ) -> str | None:
        """Resolve a pool address for a token pair.

        First checks the static registry of known pools. If not found,
        calls the factory contract's getPool() method.

        Args:
            token_a: Token address or symbol for token A.
            token_b: Token address or symbol for token B.
            chain: Chain name.
            fee_tier: Fee tier in basis points (default 3000 = 0.3%).

        Returns:
            Pool address string, or None if not found.
        """
        chain_lower = chain.lower()

        # Resolve symbols to addresses if needed
        addr_a = self._resolve_to_address(token_a, chain_lower)
        addr_b = self._resolve_to_address(token_b, chain_lower)

        if addr_a is None or addr_b is None:
            return None

        # Sort addresses (lower address first, matching Uniswap's convention)
        a_lower = addr_a.lower()
        b_lower = addr_b.lower()
        sorted_a, sorted_b = (a_lower, b_lower) if a_lower < b_lower else (b_lower, a_lower)

        # Check static registry
        chain_pools = self._known_pools.get(chain_lower, {})
        known = chain_pools.get((sorted_a, sorted_b, fee_tier))
        if known:
            return known

        # Try factory getPool() call
        factory = self._factory_addresses.get(chain_lower)
        if factory is None:
            return None

        try:
            # getPool(address,address,uint24) -> address
            # Encode: selector + address_a (padded) + address_b (padded) + fee (padded)
            calldata = GET_POOL_SELECTOR
            calldata += sorted_a.replace("0x", "").zfill(64)
            calldata += sorted_b.replace("0x", "").zfill(64)
            calldata += hex(fee_tier)[2:].zfill(64)

            result = self._rpc_call(chain_lower, factory, calldata)
            pool_addr = decode_address(result)

            # Check for zero address (pool doesn't exist)
            if pool_addr == "0x" + "0" * 40:
                return None

            return pool_addr
        except Exception:
            logger.debug(
                "Failed to resolve pool via factory for %s/%s on %s",
                token_a,
                token_b,
                chain_lower,
            )
            return None

    def clear_cache(self) -> None:
        """Clear the price cache."""
        self._cache.clear()

    # ----- internal helpers -----

    def _get_pool_metadata(
        self,
        pool_address: str,
        chain: str,
    ) -> tuple[int, int, int]:
        """Get token decimals and fee tier for a pool.

        Tries TokenResolver first, falls back to on-chain reads.

        Returns:
            (token0_decimals, token1_decimals, fee_tier)
        """
        # Read token addresses and fee from pool contract
        token0_data = self._rpc_call(chain, pool_address, TOKEN0_SELECTOR)
        token0_addr = decode_address(token0_data)

        token1_data = self._rpc_call(chain, pool_address, TOKEN1_SELECTOR)
        token1_addr = decode_address(token1_data)

        fee_data = self._rpc_call(chain, pool_address, FEE_SELECTOR)
        fee_tier = decode_uint(fee_data)

        # Resolve decimals
        token0_decimals = self._get_token_decimals(token0_addr, chain)
        token1_decimals = self._get_token_decimals(token1_addr, chain)

        return token0_decimals, token1_decimals, fee_tier

    def _get_token_decimals(self, token_address: str, chain: str) -> int:
        """Get decimals for a token, using TokenResolver if available.

        Args:
            token_address: Token contract address.
            chain: Chain name.

        Returns:
            Token decimals.

        Raises:
            DataUnavailableError: If decimals cannot be determined.
        """
        # Try TokenResolver first
        if self._token_resolver is not None:
            try:
                resolved = self._token_resolver.resolve(token_address, chain)
                return resolved.decimals
            except Exception:
                pass

        # Fall back to on-chain decimals() call
        # decimals() selector = 0x313ce567
        try:
            data = self._rpc_call(chain, token_address, "0x313ce567")
            return decode_uint(data)
        except Exception as e:
            raise DataUnavailableError(
                data_type="pool_price",
                instrument=token_address,
                reason=f"Cannot determine decimals for token {token_address} on {chain}: {e}",
            ) from e

    def _resolve_to_address(self, token: str, chain: str) -> str | None:
        """Resolve a token symbol or address to an address.

        If the input looks like an address (starts with 0x, 42 chars), return as-is.
        Otherwise, try TokenResolver.

        Returns:
            Lowercase address, or None if unresolvable.
        """
        if token.startswith("0x") and len(token) == 42:
            return token.lower()

        if self._token_resolver is not None:
            try:
                resolved = self._token_resolver.resolve(token, chain)
                return resolved.address.lower()
            except Exception:
                pass

        return None


# ---------------------------------------------------------------------------
# AerodromePoolReader
# ---------------------------------------------------------------------------


class AerodromePoolReader(UniswapV3PoolPriceReader):
    """Reads live prices from Aerodrome CL pool contracts on Base.

    Aerodrome concentrated-liquidity pools use the same slot0() ABI as
    Uniswap V3 but with different factory addresses and pool registries.

    Args:
        rpc_call: Callable(chain, to_address, calldata_hex) -> bytes.
        token_resolver: Optional TokenResolver for decimal lookups.
        cache_ttl_seconds: Cache TTL in seconds (default 2).
        source_name: Source identifier for DataMeta (default "alchemy_rpc").
    """

    _factory_addresses: dict[str, str] = AERODROME_CL_FACTORY
    _known_pools: dict[str, dict[tuple[str, str, int], str]] = _AERODROME_KNOWN_POOLS
    protocol_name: str = "aerodrome"


# ---------------------------------------------------------------------------
# PancakeSwapV3PoolReader
# ---------------------------------------------------------------------------


class PancakeSwapV3PoolReader(UniswapV3PoolPriceReader):
    """Reads live prices from PancakeSwap V3 pool contracts.

    PancakeSwap V3 pools use the same slot0() ABI as Uniswap V3
    but with different factory addresses and pool registries.

    Args:
        rpc_call: Callable(chain, to_address, calldata_hex) -> bytes.
        token_resolver: Optional TokenResolver for decimal lookups.
        cache_ttl_seconds: Cache TTL in seconds (default 2).
        source_name: Source identifier for DataMeta (default "alchemy_rpc").
    """

    _factory_addresses: dict[str, str] = PANCAKESWAP_V3_FACTORY
    _known_pools: dict[str, dict[tuple[str, str, int], str]] = _PANCAKESWAP_KNOWN_POOLS
    protocol_name: str = "pancakeswap_v3"


# ---------------------------------------------------------------------------
# PoolReaderRegistry
# ---------------------------------------------------------------------------

# Default mapping of protocol names to reader classes
_PROTOCOL_READER_CLASSES: dict[str, type[UniswapV3PoolPriceReader]] = {
    "uniswap_v3": UniswapV3PoolPriceReader,
    "aerodrome": AerodromePoolReader,
    "pancakeswap_v3": PancakeSwapV3PoolReader,
}


class PoolReaderRegistry:
    """Maps (chain, protocol) to PoolReader instances for dynamic dispatch.

    Lazily creates reader instances on first access. All readers share
    the same ``rpc_call`` and ``token_resolver`` from the registry.

    Example:
        registry = PoolReaderRegistry(rpc_call=my_rpc_fn)
        reader = registry.get_reader("base", "aerodrome")
        envelope = reader.read_pool_price("0x...", "base")

    Args:
        rpc_call: Callable(chain, to_address, calldata_hex) -> bytes.
        token_resolver: Optional TokenResolver for decimal lookups.
        cache_ttl_seconds: Cache TTL in seconds (default 2).
        source_name: Source identifier for DataMeta (default "alchemy_rpc").
    """

    def __init__(
        self,
        rpc_call: RpcCallFn,
        token_resolver: Any | None = None,
        cache_ttl_seconds: float = 2.0,
        source_name: str = "alchemy_rpc",
    ) -> None:
        self._rpc_call = rpc_call
        self._token_resolver = token_resolver
        self._cache_ttl = cache_ttl_seconds
        self._source_name = source_name
        # Lazy cache: {protocol_name: reader_instance}
        self._readers: dict[str, UniswapV3PoolPriceReader] = {}
        # Registered protocol classes (copy defaults; can be extended)
        self._protocol_classes: dict[str, type[UniswapV3PoolPriceReader]] = dict(_PROTOCOL_READER_CLASSES)

    def get_reader(self, chain: str, protocol: str) -> UniswapV3PoolPriceReader:
        """Get or create a pool reader for a given protocol.

        Args:
            chain: Chain name (used for validation but readers are protocol-scoped).
            protocol: Protocol name (e.g. "uniswap_v3", "aerodrome", "pancakeswap_v3").

        Returns:
            A pool reader instance for the protocol.

        Raises:
            ValueError: If protocol is not registered.
        """
        protocol_lower = protocol.lower()

        cached = self._readers.get(protocol_lower)
        if cached is not None:
            return cached

        reader_cls = self._protocol_classes.get(protocol_lower)
        if reader_cls is None:
            available = ", ".join(sorted(self._protocol_classes))
            raise ValueError(f"Unknown protocol '{protocol}'. Available: {available}")

        reader = reader_cls(
            rpc_call=self._rpc_call,
            token_resolver=self._token_resolver,
            cache_ttl_seconds=self._cache_ttl,
            source_name=self._source_name,
        )
        self._readers[protocol_lower] = reader
        return reader

    def register_protocol(
        self,
        protocol_name: str,
        reader_class: type[UniswapV3PoolPriceReader],
    ) -> None:
        """Register a custom protocol reader class.

        Args:
            protocol_name: Protocol identifier (e.g. "sushiswap_v3").
            reader_class: Reader class (must be UniswapV3PoolPriceReader or subclass).
        """
        self._protocol_classes[protocol_name.lower()] = reader_class
        # Clear cached instance if overriding
        self._readers.pop(protocol_name.lower(), None)

    @property
    def supported_protocols(self) -> list[str]:
        """List of registered protocol names."""
        return sorted(self._protocol_classes)

    def protocols_for_chain(self, chain: str) -> list[str]:
        """List protocols that have factory addresses for a given chain.

        Args:
            chain: Chain name (e.g. "base", "arbitrum").

        Returns:
            List of protocol names with factory support on this chain.
        """
        chain_lower = chain.lower()
        result = []
        for name, cls in self._protocol_classes.items():
            if chain_lower in cls._factory_addresses or chain_lower in cls._known_pools:
                result.append(name)
        return sorted(result)
