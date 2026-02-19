"""TWAP and LWAP price aggregation across multiple on-chain DEX pools.

Provides manipulation-resistant aggregated prices by combining data from
multiple pools using time-weighting (TWAP via Uniswap V3 oracle observe())
or liquidity-weighting (LWAP across all known pools for a pair).

All returns are wrapped in DataEnvelope[AggregatedPrice] with EXECUTION_GRADE
classification (fail-closed, no off-chain fallback).

Example:
    from almanak.framework.data.pools.aggregation import PriceAggregator

    aggregator = PriceAggregator(
        pool_registry=my_registry,
        rpc_call=my_rpc_fn,
    )
    envelope = aggregator.twap("0xpool...", "arbitrum", window_seconds=300)
    print(envelope.price)  # Aggregated TWAP price

    envelope = aggregator.lwap("WETH", "USDC", "arbitrum")
    print(envelope.price)       # Liquidity-weighted average price
    print(envelope.sources)     # List of PoolContribution
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)
from almanak.framework.data.pools.reader import (
    PoolPrice,
    PoolReaderRegistry,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# observe(uint32[]) selector on Uniswap V3 pool
OBSERVE_SELECTOR = "0x883bdbfd"

# Default minimum liquidity threshold in USD equivalent
DEFAULT_MIN_LIQUIDITY_USD = Decimal("10000")

# RPC call function type alias
RpcCallFn = Any  # Callable[[str, str, str], bytes]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PoolContribution:
    """A single pool's contribution to an aggregated price.

    Attributes:
        pool_address: Address of the pool contract.
        protocol: Protocol name (e.g. "uniswap_v3", "aerodrome").
        price: Price from this pool.
        weight: Weight in the aggregation (liquidity share for LWAP, 1.0 for TWAP).
        liquidity: Raw in-range liquidity from the pool.
    """

    pool_address: str
    protocol: str
    price: Decimal
    weight: float
    liquidity: int = 0


@dataclass(frozen=True)
class AggregatedPrice:
    """Aggregated price from multiple pools or time periods.

    Attributes:
        price: The aggregated price value.
        sources: List of pool contributions with individual prices and weights.
        block_range: Tuple of (min_block, max_block) covered by the aggregation.
        method: Aggregation method used ("twap" or "lwap").
        window_seconds: Time window in seconds (for TWAP).
        pool_count: Number of pools used in aggregation.
    """

    price: Decimal
    sources: list[PoolContribution] = field(default_factory=list)
    block_range: tuple[int, int] = (0, 0)
    method: str = "lwap"
    window_seconds: int = 0
    pool_count: int = 0


# ---------------------------------------------------------------------------
# observe() response decoding
# ---------------------------------------------------------------------------


def _encode_observe_calldata(seconds_agos: list[int]) -> str:
    """Encode observe(uint32[]) calldata.

    The observe function takes a dynamic array of uint32 seconds-ago values.
    ABI encoding:
        - offset to array (32 bytes, value=0x20)
        - array length (32 bytes)
        - each element (32 bytes each)

    Args:
        seconds_agos: List of seconds-ago values (e.g. [300, 0]).

    Returns:
        Hex-encoded calldata string starting with selector.
    """
    # Offset to the dynamic array = 0x20 (32 bytes)
    offset = "0000000000000000000000000000000000000000000000000000000000000020"
    length = hex(len(seconds_agos))[2:].zfill(64)
    elements = "".join(hex(s)[2:].zfill(64) for s in seconds_agos)
    return OBSERVE_SELECTOR + offset + length + elements


def _decode_observe_response(data: bytes, count: int) -> tuple[list[int], list[int]]:
    """Decode observe() return data into (tickCumulatives, secondsPerLiquidityCumulatives).

    observe() returns two dynamic arrays:
        int56[] tickCumulatives
        uint160[] secondsPerLiquidityCumulativeX128s

    ABI layout:
        word 0: offset to tickCumulatives array
        word 1: offset to secondsPerLiquidityCumulatives array
        Then each array: length word + data words

    Args:
        data: Raw bytes from eth_call.
        count: Expected number of elements in each array.

    Returns:
        Tuple of (tickCumulatives, secondsPerLiquidityCumulatives).

    Raises:
        DataUnavailableError: If response is too short.
    """
    # Minimum: 2 offset words + 2 length words + count*2 data words
    min_bytes = (2 + 2 + count * 2) * 32
    if len(data) < min_bytes:
        raise DataUnavailableError(
            data_type="twap",
            instrument="unknown",
            reason=f"observe() response too short: {len(data)} bytes (need >= {min_bytes})",
        )

    # Read offsets
    offset_ticks = int.from_bytes(data[0:32], byteorder="big")
    offset_spl = int.from_bytes(data[32:64], byteorder="big")

    # Read tickCumulatives array
    tick_length = int.from_bytes(data[offset_ticks : offset_ticks + 32], byteorder="big")
    tick_cumulatives = []
    for i in range(tick_length):
        start = offset_ticks + 32 + i * 32
        # int56 stored as int256 (signed)
        val = int.from_bytes(data[start : start + 32], byteorder="big", signed=True)
        tick_cumulatives.append(val)

    # Read secondsPerLiquidityCumulativeX128s array
    spl_length = int.from_bytes(data[offset_spl : offset_spl + 32], byteorder="big")
    spl_cumulatives = []
    for i in range(spl_length):
        start = offset_spl + 32 + i * 32
        val = int.from_bytes(data[start : start + 32], byteorder="big")
        spl_cumulatives.append(val)

    return tick_cumulatives, spl_cumulatives


def _tick_to_price(tick: int, token0_decimals: int, token1_decimals: int) -> Decimal:
    """Convert a Uniswap V3 tick to a human-readable price.

    price = 1.0001^tick * 10^(token0_decimals - token1_decimals)

    Args:
        tick: Pool tick value.
        token0_decimals: Decimals of token0.
        token1_decimals: Decimals of token1.

    Returns:
        Human-readable price of token0 in terms of token1.
    """
    # Use Decimal for precision
    base = Decimal("1.0001")
    raw_price = base**tick
    decimal_adjustment = Decimal(10) ** (token0_decimals - token1_decimals)
    return raw_price * decimal_adjustment


# ---------------------------------------------------------------------------
# PriceAggregator
# ---------------------------------------------------------------------------


class PriceAggregator:
    """Aggregates prices across pools using TWAP and LWAP methods.

    TWAP uses Uniswap V3's built-in oracle (observe()) for time-weighted
    average prices. LWAP reads live prices from multiple pools and weights
    them by in-range liquidity.

    All results are EXECUTION_GRADE: fail-closed with no off-chain fallback.

    Args:
        pool_registry: PoolReaderRegistry for reading pool prices.
        rpc_call: RPC call function for direct contract reads (observe()).
        min_liquidity_usd: Minimum liquidity in USD to include a pool (default $10k).
        reference_price_usd: Reference price for converting liquidity to USD.
            If None, liquidity filtering uses raw liquidity values and the
            threshold is treated as raw liquidity units.
    """

    def __init__(
        self,
        pool_registry: PoolReaderRegistry,
        rpc_call: RpcCallFn,
        min_liquidity_usd: Decimal = DEFAULT_MIN_LIQUIDITY_USD,
        reference_price_usd: Decimal | None = None,
    ) -> None:
        self._registry = pool_registry
        self._rpc_call = rpc_call
        self._min_liquidity_usd = min_liquidity_usd
        self._reference_price_usd = reference_price_usd

    def twap(
        self,
        pool_address: str,
        chain: str,
        window_seconds: int = 300,
        token0_decimals: int = 18,
        token1_decimals: int = 6,
        protocol: str = "uniswap_v3",
    ) -> DataEnvelope[AggregatedPrice]:
        """Calculate TWAP using Uniswap V3 oracle observe().

        Calls observe([window_seconds, 0]) on the pool contract to get
        tick cumulatives, then derives the arithmetic mean tick and converts
        to a price.

        Args:
            pool_address: Pool contract address.
            chain: Chain name.
            window_seconds: Time window in seconds (default 300 = 5 min).
            token0_decimals: Decimals of token0 (default 18).
            token1_decimals: Decimals of token1 (default 6).
            protocol: Protocol name for source attribution.

        Returns:
            DataEnvelope[AggregatedPrice] with TWAP price.

        Raises:
            DataUnavailableError: If observe() call fails or returns invalid data.
        """
        chain_lower = chain.lower()
        start_time = time.monotonic()

        try:
            # Call observe([window_seconds, 0])
            calldata = _encode_observe_calldata([window_seconds, 0])
            response = self._rpc_call(chain_lower, pool_address, calldata)
            tick_cumulatives, _ = _decode_observe_response(response, 2)

            # TWAP tick = (tickCumulative[1] - tickCumulative[0]) / window_seconds
            tick_diff = tick_cumulatives[1] - tick_cumulatives[0]
            # Integer division matching Uniswap's convention (truncate toward zero)
            if tick_diff < 0:
                avg_tick = -((-tick_diff) // window_seconds)
            else:
                avg_tick = tick_diff // window_seconds

            twap_price = _tick_to_price(avg_tick, token0_decimals, token1_decimals)

        except DataUnavailableError:
            raise
        except Exception as e:
            raise DataUnavailableError(
                data_type="twap",
                instrument=pool_address,
                reason=f"TWAP observe() failed for {pool_address} on {chain_lower}: {e}",
            ) from e

        latency_ms = int((time.monotonic() - start_time) * 1000)

        contribution = PoolContribution(
            pool_address=pool_address,
            protocol=protocol,
            price=twap_price,
            weight=1.0,
        )

        aggregated = AggregatedPrice(
            price=twap_price,
            sources=[contribution],
            block_range=(0, 0),
            method="twap",
            window_seconds=window_seconds,
            pool_count=1,
        )

        meta = DataMeta(
            source="alchemy_rpc",
            observed_at=datetime.now(UTC),
            finality="latest",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=1.0,
            cache_hit=False,
        )

        logger.debug(
            "twap_calculated",
            extra={
                "pool": pool_address,
                "chain": chain_lower,
                "window_seconds": window_seconds,
                "price": str(twap_price),
                "avg_tick": avg_tick,
                "latency_ms": latency_ms,
            },
        )

        return DataEnvelope(
            value=aggregated,
            meta=meta,
            classification=DataClassification.EXECUTION_GRADE,
        )

    def lwap(
        self,
        token_a: str,
        token_b: str,
        chain: str,
        fee_tiers: list[int] | None = None,
        protocols: list[str] | None = None,
    ) -> DataEnvelope[AggregatedPrice]:
        """Calculate liquidity-weighted average price across pools.

        Reads live prices from all known pools for the given pair, filters
        out pools below the minimum liquidity threshold, then computes
        LWAP = sum(price_i * liquidity_i) / sum(liquidity_i).

        Falls back to single-pool price if only one pool is available.

        Args:
            token_a: Token A symbol or address.
            token_b: Token B symbol or address.
            chain: Chain name.
            fee_tiers: Fee tiers to search (default: [100, 500, 3000, 10000]).
            protocols: Protocols to search (default: all registered for chain).

        Returns:
            DataEnvelope[AggregatedPrice] with LWAP price.

        Raises:
            DataUnavailableError: If no pools found for the pair (fail-closed).
        """
        chain_lower = chain.lower()
        start_time = time.monotonic()

        if fee_tiers is None:
            fee_tiers = [100, 500, 3000, 10000]

        if protocols is None:
            protocols = self._registry.protocols_for_chain(chain_lower)

        if not protocols:
            raise DataUnavailableError(
                data_type="lwap",
                instrument=f"{token_a}/{token_b}",
                reason=f"No protocols registered for chain '{chain_lower}'",
            )

        # Collect pool prices from all protocols and fee tiers
        pool_prices: list[tuple[PoolPrice, str]] = []  # (pool_price, protocol)

        for protocol in protocols:
            try:
                reader = self._registry.get_reader(chain_lower, protocol)
            except ValueError:
                continue

            for fee_tier in fee_tiers:
                pool_addr = reader.resolve_pool_address(token_a, token_b, chain_lower, fee_tier)
                if pool_addr is None:
                    continue

                try:
                    envelope = reader.read_pool_price(pool_addr, chain_lower)
                    pool_prices.append((envelope.value, protocol))
                except DataUnavailableError:
                    logger.debug(
                        "lwap_pool_read_failed",
                        extra={"pool": pool_addr, "protocol": protocol, "chain": chain_lower},
                    )
                    continue

        if not pool_prices:
            raise DataUnavailableError(
                data_type="lwap",
                instrument=f"{token_a}/{token_b}",
                reason=f"No pools found for {token_a}/{token_b} on {chain_lower}",
            )

        # Filter by minimum liquidity
        filtered = self._filter_by_liquidity(pool_prices)

        # If filtering removed all pools, use unfiltered (best-effort)
        if not filtered:
            logger.warning(
                "lwap_all_pools_below_threshold",
                extra={
                    "pair": f"{token_a}/{token_b}",
                    "chain": chain_lower,
                    "threshold": str(self._min_liquidity_usd),
                    "pool_count": len(pool_prices),
                },
            )
            filtered = pool_prices

        # Single pool fallback
        if len(filtered) == 1:
            pp, proto = filtered[0]
            contribution = PoolContribution(
                pool_address=pp.pool_address,
                protocol=proto,
                price=pp.price,
                weight=1.0,
                liquidity=pp.liquidity,
            )
            aggregated = AggregatedPrice(
                price=pp.price,
                sources=[contribution],
                block_range=(pp.block_number, pp.block_number),
                method="lwap",
                pool_count=1,
            )
        else:
            # Compute LWAP
            total_liquidity = sum(pp.liquidity for pp, _ in filtered)
            if total_liquidity == 0:
                # Equal weighting if all liquidities are zero
                weight = Decimal(1) / Decimal(len(filtered))
                weighted_price = sum((pp.price * weight for pp, _ in filtered), Decimal("0"))
                contributions = [
                    PoolContribution(
                        pool_address=pp.pool_address,
                        protocol=proto,
                        price=pp.price,
                        weight=float(weight),
                        liquidity=pp.liquidity,
                    )
                    for pp, proto in filtered
                ]
            else:
                weighted_price = Decimal(0)
                contributions = []
                for pp, proto in filtered:
                    w = Decimal(pp.liquidity) / Decimal(total_liquidity)
                    weighted_price += pp.price * w
                    contributions.append(
                        PoolContribution(
                            pool_address=pp.pool_address,
                            protocol=proto,
                            price=pp.price,
                            weight=float(w),
                            liquidity=pp.liquidity,
                        )
                    )

            block_numbers = [pp.block_number for pp, _ in filtered]
            aggregated = AggregatedPrice(
                price=weighted_price,
                sources=contributions,
                block_range=(min(block_numbers), max(block_numbers)),
                method="lwap",
                pool_count=len(filtered),
            )

        latency_ms = int((time.monotonic() - start_time) * 1000)

        meta = DataMeta(
            source="alchemy_rpc",
            observed_at=datetime.now(UTC),
            finality="latest",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=1.0,
            cache_hit=False,
        )

        logger.debug(
            "lwap_calculated",
            extra={
                "pair": f"{token_a}/{token_b}",
                "chain": chain_lower,
                "price": str(aggregated.price),
                "pool_count": aggregated.pool_count,
                "latency_ms": latency_ms,
            },
        )

        return DataEnvelope(
            value=aggregated,
            meta=meta,
            classification=DataClassification.EXECUTION_GRADE,
        )

    def _filter_by_liquidity(
        self,
        pool_prices: list[tuple[PoolPrice, str]],
    ) -> list[tuple[PoolPrice, str]]:
        """Filter out pools below the minimum liquidity threshold.

        If reference_price_usd is set, converts raw liquidity to USD estimate.
        Otherwise, compares raw liquidity against the threshold directly.

        Args:
            pool_prices: List of (PoolPrice, protocol) tuples.

        Returns:
            Filtered list of pools above the threshold.
        """
        threshold = self._min_liquidity_usd
        filtered = []
        for pp, proto in pool_prices:
            if self._reference_price_usd is not None:
                # Rough USD estimate: liquidity * reference_price / 10^token0_decimals
                # This is a simplified estimate; real USD conversion would need
                # both token prices and the liquidity distribution
                liquidity_usd = Decimal(pp.liquidity) * self._reference_price_usd / Decimal(10) ** pp.token0_decimals
                if liquidity_usd >= threshold:
                    filtered.append((pp, proto))
            else:
                # Compare raw liquidity against threshold
                if Decimal(pp.liquidity) >= threshold:
                    filtered.append((pp, proto))
        return filtered
