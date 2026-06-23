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
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.concentrated_liquidity_math import Q96
from almanak.connectors._strategy_base.v3_pool_abi import (
    V3_FEE_SELECTOR,
    V3_GET_POOL_SELECTOR,
    V3_LIQUIDITY_SELECTOR,
    V3_SLOT0_SELECTOR,
    V3_TOKEN0_SELECTOR,
    V3_TOKEN1_SELECTOR,
    encode_get_pool,
)
from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY
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

SLOT0_SELECTOR = V3_SLOT0_SELECTOR
LIQUIDITY_SELECTOR = V3_LIQUIDITY_SELECTOR
TOKEN0_SELECTOR = V3_TOKEN0_SELECTOR
TOKEN1_SELECTOR = V3_TOKEN1_SELECTOR
FEE_SELECTOR = V3_FEE_SELECTOR
GET_POOL_SELECTOR = V3_GET_POOL_SELECTOR

_UNISWAP_POOL_READER_SPEC = POOL_READER_REGISTRY.require("uniswap_v3")
_AERODROME_POOL_READER_SPEC = POOL_READER_REGISTRY.require("aerodrome")
_PANCAKESWAP_POOL_READER_SPEC = POOL_READER_REGISTRY.require("pancakeswap_v3")
_SUSHISWAP_POOL_READER_SPEC = POOL_READER_REGISTRY.require("sushiswap_v3")

# Historical module-level aliases are preserved for tests and callers that
# imported them directly. The data is connector-owned and manifest-loaded.
UNISWAP_V3_FACTORY = _UNISWAP_POOL_READER_SPEC.factory_addresses
_KNOWN_POOLS = _UNISWAP_POOL_READER_SPEC.known_pools
AERODROME_CL_FACTORY = _AERODROME_POOL_READER_SPEC.factory_addresses
_AERODROME_KNOWN_POOLS = _AERODROME_POOL_READER_SPEC.known_pools
PANCAKESWAP_V3_FACTORY = _PANCAKESWAP_POOL_READER_SPEC.factory_addresses
_PANCAKESWAP_KNOWN_POOLS = _PANCAKESWAP_POOL_READER_SPEC.known_pools
SUSHISWAP_V3_FACTORY = _SUSHISWAP_POOL_READER_SPEC.factory_addresses
_SUSHISWAP_KNOWN_POOLS = _SUSHISWAP_POOL_READER_SPEC.known_pools

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
    _factory_addresses: Mapping[str, str] = UNISWAP_V3_FACTORY
    _known_pools: Mapping[str, Mapping[tuple[str, str, int], str]] = _KNOWN_POOLS
    protocol_name: str = "uniswap_v3"
    _get_pool_selector: str = GET_POOL_SELECTOR
    # Discriminator values swept during pool resolution. For Uniswap-style DEXs
    # these are fee tiers (uint24); tick-spacing forks (Aerodrome Slipstream)
    # override with their tick spacings. Consumed by ``resolve_best_pool_address``.
    _candidate_pool_keys: tuple[int, ...] = (100, 500, 3000, 10000)

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
            calldata = encode_get_pool(self._get_pool_selector, sorted_a, sorted_b, fee_tier)
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

    def resolve_best_pool_address(
        self,
        token_a: str,
        token_b: str,
        chain: str,
        fee_tiers: list[int] | None = None,
    ) -> str | None:
        """Resolve the highest-liquidity pool for a pair across fee tiers.

        Enumerates the standard fee tiers, resolves each candidate pool, reads
        its in-range liquidity, and returns the deepest. This deliberately
        avoids ``resolve_pool_address``'s blind ``fee_tier=3000`` default
        (VIB-4924 C1): on Base the canonical WETH/USDC pool is the 0.05% tier,
        so a default-3000 resolution would pick the thin 0.3% pool and feed a
        wrong / manipulable source into an EXECUTION_GRADE TWAP.

        Candidates that resolve but cannot be read are skipped. Among readable
        candidates the deepest by liquidity wins; ties keep the first seen
        (tier order). Returns None only when no candidate resolves at all — the
        caller surfaces that as a structured "cannot resolve pool" error.

        Args:
            token_a: Token address or symbol for token A.
            token_b: Token address or symbol for token B.
            chain: Chain name.
            fee_tiers: Fee tiers to enumerate (default ``[100, 500, 3000, 10000]``).

        Returns:
            The highest-liquidity pool address, or None if none resolve.
        """
        if fee_tiers is None:
            # Use the protocol's own discriminator set: fee tiers for Uniswap
            # forks, tick spacings for Aerodrome Slipstream. A blind Uniswap
            # fee-tier list would never resolve a tick-spacing-keyed pool.
            fee_tiers = list(self._candidate_pool_keys)

        chain_lower = chain.lower()
        best_addr: str | None = None
        best_liquidity = -1
        for fee_tier in fee_tiers:
            pool_addr = self.resolve_pool_address(token_a, token_b, chain_lower, fee_tier)
            if pool_addr is None:
                continue
            try:
                liquidity = self.read_pool_price(pool_addr, chain_lower).value.liquidity
            except DataUnavailableError:
                # Pool resolved but unreadable (e.g. uninitialized) — still a
                # candidate the caller could use, but we cannot rank it. Keep it
                # as a last-resort fallback if nothing readable shows up.
                if best_addr is None:
                    best_addr = pool_addr
                continue
            if liquidity > best_liquidity:
                best_liquidity = liquidity
                best_addr = pool_addr
        return best_addr

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

    _factory_addresses: Mapping[str, str] = AERODROME_CL_FACTORY
    _known_pools: Mapping[str, Mapping[tuple[str, str, int], str]] = _AERODROME_KNOWN_POOLS
    protocol_name: str = "aerodrome"
    _get_pool_selector: str = "0x28af8d0b"  # int24 (tick_spacing), not v3 uint24 (fee_tier)
    # Slipstream keys pools by TICK SPACING, not Uniswap fee tier — getPool's
    # third arg is the tick spacing. Snapshot of the Base CL factory's
    # ``tickSpacings()`` (governance-extensible — keep in sync if it grows).
    _candidate_pool_keys: tuple[int, ...] = (1, 10, 50, 100, 200, 2000)


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

    _factory_addresses: Mapping[str, str] = PANCAKESWAP_V3_FACTORY
    _known_pools: Mapping[str, Mapping[tuple[str, str, int], str]] = _PANCAKESWAP_KNOWN_POOLS
    protocol_name: str = "pancakeswap_v3"
    # PancakeSwap V3 uses a 2500 (0.25%) tier where Uniswap uses 3000 (0.3%).
    _candidate_pool_keys: tuple[int, ...] = (100, 500, 2500, 10000)


# ---------------------------------------------------------------------------
# SushiSwapV3PoolReader
# ---------------------------------------------------------------------------


class SushiSwapV3PoolReader(UniswapV3PoolPriceReader):
    """Reads live prices from SushiSwap V3 pool contracts.

    SushiSwap V3 is a standard Uniswap-V3 fork: identical slot0()/getPool ABI
    and the canonical Uniswap fee tiers, just different factory addresses. It
    inherits the default fee-tier candidate sweep from the base reader.

    Args:
        rpc_call: Callable(chain, to_address, calldata_hex) -> bytes.
        token_resolver: Optional TokenResolver for decimal lookups.
        cache_ttl_seconds: Cache TTL in seconds (default 2).
        source_name: Source identifier for DataMeta (default "alchemy_rpc").
    """

    _factory_addresses: Mapping[str, str] = SUSHISWAP_V3_FACTORY
    _known_pools: Mapping[str, Mapping[tuple[str, str, int], str]] = _SUSHISWAP_KNOWN_POOLS
    protocol_name: str = "sushiswap_v3"


# ---------------------------------------------------------------------------
# PoolReaderRegistry
# ---------------------------------------------------------------------------

# Default mapping of protocol names to reader classes
_PROTOCOL_READER_CLASSES: dict[str, type[UniswapV3PoolPriceReader]] = {
    "uniswap_v3": UniswapV3PoolPriceReader,
    "aerodrome": AerodromePoolReader,  # legacy name used by existing demo strategies
    "aerodrome_slipstream": AerodromePoolReader,  # canonical name used by executor / CLI
    "pancakeswap_v3": PancakeSwapV3PoolReader,
    "sushiswap_v3": SushiSwapV3PoolReader,
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
