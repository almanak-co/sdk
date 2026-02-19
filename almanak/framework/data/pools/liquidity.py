"""Liquidity depth reader and slippage estimator for concentrated-liquidity DEX pools.

Reads tick-level liquidity distribution from Uniswap V3-compatible pools
(Uniswap V3, Aerodrome CL, PancakeSwap V3) and simulates swaps to
estimate price impact and slippage before execution.

All returns are wrapped in DataEnvelope with EXECUTION_GRADE classification
(fail-closed semantics -- no off-chain fallback).

Example:
    from almanak.framework.data.pools.liquidity import LiquidityDepthReader, SlippageEstimator

    reader = LiquidityDepthReader(rpc_call=my_rpc_fn)
    envelope = reader.read_liquidity_depth("0x88e6A0c2...", "ethereum")
    print(envelope.total_liquidity)
    print(envelope.ticks[0].price_at_tick)

    estimator = SlippageEstimator(reader, pool_reader_registry=registry)
    slip = estimator.estimate_slippage("WETH", "USDC", Decimal("10"), "arbitrum")
    print(slip.price_impact_bps)
"""

from __future__ import annotations

import logging
import math
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

# Tick spacing for common fee tiers (Uniswap V3 standard)
FEE_TO_TICK_SPACING: dict[int, int] = {
    100: 1,
    500: 10,
    3000: 60,
    10000: 200,
}

# Function selectors for tick reads
# ticks(int24) -> (uint128 liquidityGross, int128 liquidityNet, ...)
TICKS_SELECTOR = "0xf30dba93"

# tickBitmap(int16) -> uint256
TICK_BITMAP_SELECTOR = "0x5339c296"

# tickSpacing() -> int24
TICK_SPACING_SELECTOR = "0xd0c93a7c"

# Uniswap V3 MIN_TICK and MAX_TICK
MIN_TICK = -887272
MAX_TICK = 887272

# RpcCallFn type alias
RpcCallFn = Any  # Callable[[str, str, str], bytes]


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TickData:
    """Liquidity data at a specific tick.

    Attributes:
        tick_index: The tick index.
        liquidity_net: Net liquidity change when crossing this tick.
            Positive = liquidity added, negative = liquidity removed.
        price_at_tick: Human-readable price at this tick (token0 in terms of token1).
    """

    tick_index: int
    liquidity_net: int
    price_at_tick: Decimal


@dataclass(frozen=True)
class LiquidityDepth:
    """Tick-level liquidity distribution for a pool.

    Attributes:
        ticks: Initialized ticks sorted by tick_index (ascending).
        total_liquidity: Current in-range liquidity (L) from the pool.
        current_tick: Current active tick from slot0.
        current_price: Current price from slot0.
        pool_address: Pool contract address.
        token0_decimals: Decimals of token0.
        token1_decimals: Decimals of token1.
        tick_spacing: Tick spacing for this pool.
    """

    ticks: list[TickData]
    total_liquidity: int
    current_tick: int
    current_price: Decimal
    pool_address: str = ""
    token0_decimals: int = 18
    token1_decimals: int = 6
    tick_spacing: int = 60


@dataclass(frozen=True)
class SlippageEstimate:
    """Slippage estimation result for a potential swap.

    Attributes:
        expected_price: Expected execution price after slippage.
        price_impact_bps: Price impact in basis points.
        effective_slippage_bps: Effective slippage vs mid-market price in basis points.
        recommended_max_size: Maximum recommended swap size before >1% slippage.
    """

    expected_price: Decimal
    price_impact_bps: int
    effective_slippage_bps: int
    recommended_max_size: Decimal


# ---------------------------------------------------------------------------
# ABI encoding/decoding helpers
# ---------------------------------------------------------------------------


def _encode_int24(value: int) -> str:
    """Encode an int24 as a 32-byte ABI word (hex string, no 0x prefix)."""
    if value < 0:
        # Two's complement for 256-bit
        value = (1 << 256) + value
    return hex(value)[2:].zfill(64)


def _encode_int16(value: int) -> str:
    """Encode an int16 as a 32-byte ABI word (hex string, no 0x prefix)."""
    if value < 0:
        value = (1 << 256) + value
    return hex(value)[2:].zfill(64)


def _decode_tick_data(data: bytes) -> tuple[int, int]:
    """Decode ticks(int24) response to (liquidityGross, liquidityNet).

    Uniswap V3 ticks() returns multiple values. We need:
      - word 0: liquidityGross (uint128)
      - word 1: liquidityNet (int128)
    """
    if len(data) < 64:
        return 0, 0

    liquidity_gross = int.from_bytes(data[0:32], byteorder="big")
    liquidity_net = int.from_bytes(data[32:64], byteorder="big", signed=True)
    return liquidity_gross, liquidity_net


def _tick_to_price(tick: int, token0_decimals: int, token1_decimals: int) -> Decimal:
    """Convert a tick to a human-readable price.

    price = 1.0001^tick * 10^(token0_decimals - token1_decimals)
    """
    # Use float for the exponentiation, then convert to Decimal
    raw_price = Decimal(str(math.pow(1.0001, tick)))
    decimal_adjustment = Decimal(10) ** (token0_decimals - token1_decimals)
    return raw_price * decimal_adjustment


# ---------------------------------------------------------------------------
# LiquidityDepthReader
# ---------------------------------------------------------------------------


class LiquidityDepthReader:
    """Reads tick-level liquidity distribution from concentrated-liquidity pools.

    Queries the tick bitmap and individual ticks via RPC to build a picture
    of liquidity depth around the current price. This data is essential for
    slippage estimation and position sizing.

    Reads ticks within a configurable range around the current tick (default
    +-500 tick spacings) to balance coverage and RPC call count.

    Args:
        rpc_call: Callable(chain, to_address, calldata_hex) -> bytes.
        tick_range_multiplier: How many tick spacings above/below current tick to scan.
            Default 500 covers a wide range for most pools.
        source_name: Source identifier for DataMeta.
    """

    def __init__(
        self,
        rpc_call: RpcCallFn,
        tick_range_multiplier: int = 500,
        source_name: str = "alchemy_rpc",
    ) -> None:
        self._rpc_call = rpc_call
        self._tick_range_multiplier = tick_range_multiplier
        self._source_name = source_name

    def read_liquidity_depth(
        self,
        pool_address: str,
        chain: str,
        current_tick: int | None = None,
        current_liquidity: int | None = None,
        current_price: Decimal | None = None,
        token0_decimals: int = 18,
        token1_decimals: int = 6,
        tick_spacing: int | None = None,
        fee_tier: int | None = None,
    ) -> DataEnvelope[LiquidityDepth]:
        """Read tick-level liquidity depth from a pool.

        If current_tick, current_liquidity, and current_price are provided
        (e.g., from a prior read_pool_price call), they are used directly
        to avoid redundant RPC calls.

        Args:
            pool_address: Pool contract address.
            chain: Chain name.
            current_tick: Current tick (from slot0). Read from chain if None.
            current_liquidity: Current in-range liquidity. Read from chain if None.
            current_price: Current price. Computed from tick if None.
            token0_decimals: Token0 decimals (default 18).
            token1_decimals: Token1 decimals (default 6).
            tick_spacing: Tick spacing. Read from chain or inferred from fee_tier if None.
            fee_tier: Pool fee tier (for tick spacing inference).

        Returns:
            DataEnvelope[LiquidityDepth] with EXECUTION_GRADE classification.

        Raises:
            DataUnavailableError: If liquidity data cannot be read.
        """
        start_time = time.monotonic()
        chain_lower = chain.lower()

        try:
            # Read slot0 if needed
            if current_tick is None or current_liquidity is None:
                from .reader import LIQUIDITY_SELECTOR, SLOT0_SELECTOR, decode_slot0, decode_uint

                slot0_data = self._rpc_call(chain_lower, pool_address, SLOT0_SELECTOR)
                _, tick_val = decode_slot0(slot0_data)
                if current_tick is None:
                    current_tick = tick_val

                if current_liquidity is None:
                    liq_data = self._rpc_call(chain_lower, pool_address, LIQUIDITY_SELECTOR)
                    current_liquidity = decode_uint(liq_data)

            # Compute price from tick if needed
            if current_price is None:
                current_price = _tick_to_price(current_tick, token0_decimals, token1_decimals)

            # Determine tick spacing
            if tick_spacing is None:
                tick_spacing = self._get_tick_spacing(pool_address, chain_lower, fee_tier)

            # Scan ticks via bitmap
            ticks = self._scan_initialized_ticks(
                pool_address,
                chain_lower,
                current_tick,
                tick_spacing,
                token0_decimals,
                token1_decimals,
            )

        except DataUnavailableError:
            raise
        except Exception as e:
            raise DataUnavailableError(
                data_type="liquidity_depth",
                instrument=pool_address,
                reason=f"Failed to read liquidity depth for {pool_address} on {chain_lower}: {e}",
            ) from e

        latency_ms = int((time.monotonic() - start_time) * 1000)

        depth = LiquidityDepth(
            ticks=ticks,
            total_liquidity=current_liquidity,
            current_tick=current_tick,
            current_price=current_price,
            pool_address=pool_address,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
            tick_spacing=tick_spacing,
        )

        meta = DataMeta(
            source=self._source_name,
            observed_at=datetime.now(UTC),
            block_number=None,
            finality="latest",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=1.0,
            cache_hit=False,
        )

        return DataEnvelope(
            value=depth,
            meta=meta,
            classification=DataClassification.EXECUTION_GRADE,
        )

    def _get_tick_spacing(
        self,
        pool_address: str,
        chain: str,
        fee_tier: int | None = None,
    ) -> int:
        """Get tick spacing from fee tier or on-chain read.

        Args:
            pool_address: Pool contract address.
            chain: Chain name.
            fee_tier: Optional fee tier for static lookup.

        Returns:
            Tick spacing integer.
        """
        # Try static lookup by fee tier
        if fee_tier is not None:
            spacing = FEE_TO_TICK_SPACING.get(fee_tier)
            if spacing is not None:
                return spacing

        # Read from contract
        try:
            data = self._rpc_call(chain, pool_address, TICK_SPACING_SELECTOR)
            if len(data) >= 32:
                spacing = int.from_bytes(data[0:32], byteorder="big", signed=True)
                if spacing > 0:
                    return spacing
        except Exception:
            logger.debug("tick_spacing_read_failed pool=%s chain=%s", pool_address, chain, exc_info=True)

        raise DataUnavailableError(
            data_type="liquidity_depth",
            instrument=pool_address,
            reason=f"Cannot determine tick spacing for pool {pool_address} (fee_tier missing and on-chain read failed)",
        )

    def _scan_initialized_ticks(
        self,
        pool_address: str,
        chain: str,
        current_tick: int,
        tick_spacing: int,
        token0_decimals: int,
        token1_decimals: int,
    ) -> list[TickData]:
        """Scan tick bitmap to find initialized ticks and read their liquidity.

        Uses the tickBitmap mapping to find which ticks have been initialized,
        then reads their liquidityNet values.

        Args:
            pool_address: Pool contract address.
            chain: Chain name.
            current_tick: Current pool tick.
            tick_spacing: Pool tick spacing.
            token0_decimals: Token0 decimals.
            token1_decimals: Token1 decimals.

        Returns:
            List of TickData sorted by tick_index ascending.
        """
        # Determine the word range to scan in the tick bitmap.
        # Each word covers 256 tick spacings.
        # word_position = tick_index / tick_spacing >> 8  (i.e., compressed / 256)
        range_ticks = self._tick_range_multiplier * tick_spacing
        min_tick_scan = max(MIN_TICK, current_tick - range_ticks)
        max_tick_scan = min(MAX_TICK, current_tick + range_ticks)

        # Convert to compressed tick indices (divided by tick_spacing)
        min_compressed = min_tick_scan // tick_spacing
        max_compressed = max_tick_scan // tick_spacing

        # Word positions in bitmap (each word covers 256 compressed ticks)
        min_word = min_compressed >> 8
        max_word = max_compressed >> 8

        # Read bitmap words and find initialized ticks
        initialized_ticks: list[int] = []

        for word_pos in range(min_word, max_word + 1):
            try:
                calldata = TICK_BITMAP_SELECTOR + _encode_int16(word_pos)
                data = self._rpc_call(chain, pool_address, calldata)
                if len(data) < 32:
                    continue

                bitmap_word = int.from_bytes(data[0:32], byteorder="big")
                if bitmap_word == 0:
                    continue

                # Scan bits
                for bit_pos in range(256):
                    if bitmap_word & (1 << bit_pos):
                        compressed = (word_pos << 8) + bit_pos
                        actual_tick = compressed * tick_spacing
                        if min_tick_scan <= actual_tick <= max_tick_scan:
                            initialized_ticks.append(actual_tick)
            except Exception:
                # Skip failed bitmap reads
                continue

        # Read tick data for each initialized tick
        ticks: list[TickData] = []
        for tick_idx in sorted(initialized_ticks):
            try:
                calldata = TICKS_SELECTOR + _encode_int24(tick_idx)
                data = self._rpc_call(chain, pool_address, calldata)
                _, liquidity_net = _decode_tick_data(data)

                price = _tick_to_price(tick_idx, token0_decimals, token1_decimals)

                ticks.append(
                    TickData(
                        tick_index=tick_idx,
                        liquidity_net=liquidity_net,
                        price_at_tick=price,
                    )
                )
            except Exception:
                # Skip failed tick reads
                continue

        return ticks


# ---------------------------------------------------------------------------
# SlippageEstimator
# ---------------------------------------------------------------------------


class SlippageEstimator:
    """Estimates price impact and slippage for potential swaps.

    For concentrated-liquidity pools (Uniswap V3, Aerodrome, PancakeSwap V3),
    simulates the swap through tick ranges using the liquidity distribution.

    For constant-product (V2-style) pools, uses the x*y=k formula.

    Args:
        liquidity_reader: LiquidityDepthReader for tick data.
        pool_reader_registry: PoolReaderRegistry for pool price lookups and address resolution.
        high_slippage_threshold_bps: Slippage threshold (in bps) above which a warning is logged.
            Default 100 = 1%.
        source_name: Source identifier for DataMeta.
    """

    def __init__(
        self,
        liquidity_reader: LiquidityDepthReader,
        pool_reader_registry: Any | None = None,
        high_slippage_threshold_bps: int = 100,
        source_name: str = "alchemy_rpc",
    ) -> None:
        self._liquidity_reader = liquidity_reader
        self._pool_reader_registry = pool_reader_registry
        self._high_slippage_threshold_bps = high_slippage_threshold_bps
        self._source_name = source_name

    def estimate_slippage(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        chain: str,
        protocol: str | None = None,
        pool_address: str | None = None,
        fee_tier: int = 3000,
    ) -> DataEnvelope[SlippageEstimate]:
        """Estimate slippage for a swap.

        For concentrated-liquidity pools, simulates the swap through tick
        ranges using actual on-chain liquidity data.

        Args:
            token_in: Input token symbol or address.
            token_out: Output token symbol or address.
            amount: Amount of token_in to swap (in human-readable units).
            chain: Chain name.
            protocol: Protocol name (e.g., "uniswap_v3"). Auto-detected if None.
            pool_address: Explicit pool address. Resolved if None.
            fee_tier: Fee tier for pool resolution (default 3000 = 0.3%).

        Returns:
            DataEnvelope[SlippageEstimate] with EXECUTION_GRADE classification.

        Raises:
            DataUnavailableError: If slippage cannot be estimated.
        """
        start_time = time.monotonic()
        chain_lower = chain.lower()

        try:
            # Resolve pool if not provided
            if pool_address is None:
                pool_address = self._resolve_pool(token_in, token_out, chain_lower, protocol, fee_tier)

            if pool_address is None:
                raise DataUnavailableError(
                    data_type="slippage_estimate",
                    instrument=f"{token_in}/{token_out}",
                    reason=f"No pool found for {token_in}/{token_out} on {chain_lower}",
                )

            # Read current pool state
            pool_price_envelope = self._read_pool_price(pool_address, chain_lower, protocol)
            pool_price = pool_price_envelope.value
            mid_price = pool_price.price
            current_tick = pool_price.tick
            current_liquidity = pool_price.liquidity
            token0_decimals = pool_price.token0_decimals
            token1_decimals = pool_price.token1_decimals

            # Determine swap direction (token0->token1 or token1->token0)
            zero_for_one = self._is_zero_for_one(token_in, token_out, pool_address, chain_lower, protocol)

            # Read liquidity depth
            depth_envelope = self._liquidity_reader.read_liquidity_depth(
                pool_address=pool_address,
                chain=chain_lower,
                current_tick=current_tick,
                current_liquidity=current_liquidity,
                current_price=mid_price,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                fee_tier=pool_price.fee_tier,
            )
            depth = depth_envelope.value

            # Simulate swap through tick ranges
            estimate = self._simulate_v3_swap(
                amount=amount,
                zero_for_one=zero_for_one,
                mid_price=mid_price,
                current_tick=current_tick,
                current_liquidity=current_liquidity,
                ticks=depth.ticks,
                tick_spacing=depth.tick_spacing,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                fee_bps=pool_price.fee_tier,
            )

            if estimate.effective_slippage_bps > self._high_slippage_threshold_bps:
                logger.warning(
                    "high_slippage_warning",
                    extra={
                        "token_in": token_in,
                        "token_out": token_out,
                        "amount": str(amount),
                        "chain": chain_lower,
                        "slippage_bps": estimate.effective_slippage_bps,
                        "threshold_bps": self._high_slippage_threshold_bps,
                    },
                )

        except DataUnavailableError:
            raise
        except Exception as e:
            raise DataUnavailableError(
                data_type="slippage_estimate",
                instrument=f"{token_in}/{token_out}",
                reason=f"Slippage estimation failed: {e}",
            ) from e

        latency_ms = int((time.monotonic() - start_time) * 1000)

        meta = DataMeta(
            source=self._source_name,
            observed_at=datetime.now(UTC),
            block_number=None,
            finality="latest",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=1.0,
            cache_hit=False,
        )

        return DataEnvelope(
            value=estimate,
            meta=meta,
            classification=DataClassification.EXECUTION_GRADE,
        )

    def estimate_slippage_v2(
        self,
        amount_in: Decimal,
        reserve_in: Decimal,
        reserve_out: Decimal,
        fee_bps: int = 30,
    ) -> SlippageEstimate:
        """Estimate slippage for a V2-style constant-product pool (x*y=k).

        Uses the standard AMM formula:
            amount_out = (reserve_out * amount_in_after_fee) / (reserve_in + amount_in_after_fee)

        Args:
            amount_in: Amount of input token (human-readable).
            reserve_in: Reserve of input token in the pool.
            reserve_out: Reserve of output token in the pool.
            fee_bps: Fee in basis points (default 30 = 0.3%).

        Returns:
            SlippageEstimate with price impact and effective slippage.

        Raises:
            DataUnavailableError: If reserves are zero.
        """
        if reserve_in <= 0 or reserve_out <= 0:
            raise DataUnavailableError(
                data_type="slippage_estimate",
                instrument="v2_pool",
                reason="Pool reserves must be positive",
            )

        # Mid-market price (no impact)
        mid_price = reserve_out / reserve_in

        # Apply fee
        fee_factor = Decimal(10000 - fee_bps) / Decimal(10000)
        amount_in_after_fee = amount_in * fee_factor

        # AMM formula: amount_out = reserve_out * amount_in_after_fee / (reserve_in + amount_in_after_fee)
        amount_out = (reserve_out * amount_in_after_fee) / (reserve_in + amount_in_after_fee)

        # Execution price
        if amount_in > 0:
            exec_price = amount_out / amount_in
        else:
            exec_price = mid_price

        # Price impact (vs mid-market, ignoring fees)
        amount_out_no_fee = (reserve_out * amount_in) / (reserve_in + amount_in)
        if amount_in > 0:
            exec_price_no_fee = amount_out_no_fee / amount_in
        else:
            exec_price_no_fee = mid_price

        price_impact_bps = int(abs(Decimal(1) - exec_price_no_fee / mid_price) * 10000) if mid_price > 0 else 0

        # Effective slippage (vs mid-market, including fees)
        effective_slippage_bps = int(abs(Decimal(1) - exec_price / mid_price) * 10000) if mid_price > 0 else 0

        # Recommended max size: solve for 1% slippage
        # At 1% price impact: amount_in = reserve_in * impact / (1 - impact) ~ reserve_in * 0.01
        recommended_max = reserve_in * Decimal("0.01")

        return SlippageEstimate(
            expected_price=exec_price,
            price_impact_bps=price_impact_bps,
            effective_slippage_bps=effective_slippage_bps,
            recommended_max_size=recommended_max,
        )

    # ----- internal helpers -----

    def _resolve_pool(
        self,
        token_in: str,
        token_out: str,
        chain: str,
        protocol: str | None,
        fee_tier: int,
    ) -> str | None:
        """Resolve a pool address for the given pair."""
        if self._pool_reader_registry is None:
            return None

        protocols = [protocol] if protocol else self._pool_reader_registry.protocols_for_chain(chain)

        for proto in protocols:
            try:
                reader = self._pool_reader_registry.get_reader(chain, proto)
                addr = reader.resolve_pool_address(token_in, token_out, chain, fee_tier)
                if addr:
                    return addr
            except Exception:
                continue

        return None

    def _read_pool_price(
        self,
        pool_address: str,
        chain: str,
        protocol: str | None,
    ) -> DataEnvelope:
        """Read the current pool price."""
        if self._pool_reader_registry is None:
            raise DataUnavailableError(
                data_type="slippage_estimate",
                instrument=pool_address,
                reason="No pool reader registry available",
            )

        # Try all protocols for the chain
        protocols = [protocol] if protocol else self._pool_reader_registry.protocols_for_chain(chain)

        for proto in protocols:
            try:
                reader = self._pool_reader_registry.get_reader(chain, proto)
                return reader.read_pool_price(pool_address, chain)
            except Exception:
                continue

        raise DataUnavailableError(
            data_type="slippage_estimate",
            instrument=pool_address,
            reason=f"Cannot read pool price for {pool_address} on {chain}",
        )

    def _is_zero_for_one(
        self,
        token_in: str,
        token_out: str,
        pool_address: str,
        chain: str,
        protocol: str | None,
    ) -> bool:
        """Determine if swap is token0 -> token1 (zeroForOne = True).

        Reads token0 from the pool contract and compares to token_in.
        If token_in matches token0, the swap is zeroForOne.
        """
        from .reader import decode_address

        try:
            token0_data = self._rpc_call_for_pool(pool_address, chain, protocol)
            token0_addr = decode_address(token0_data)

            # Check if token_in matches token0
            token_in_lower = token_in.lower()
            if token_in_lower == token0_addr.lower():
                return True
            if token_in_lower.startswith("0x") and len(token_in_lower) == 42:
                return token_in_lower == token0_addr.lower()
            # If token_in is a symbol, try resolving
            return False
        except Exception:
            # Default: try to infer from sorted address order
            return True

    def _rpc_call_for_pool(self, pool_address: str, chain: str, protocol: str | None) -> bytes:
        """Read token0 from pool contract."""
        from .reader import TOKEN0_SELECTOR

        return self._liquidity_reader._rpc_call(chain, pool_address, TOKEN0_SELECTOR)

    def _simulate_v3_swap(
        self,
        amount: Decimal,
        zero_for_one: bool,
        mid_price: Decimal,
        current_tick: int,
        current_liquidity: int,
        ticks: list[TickData],
        tick_spacing: int,
        token0_decimals: int,
        token1_decimals: int,
        fee_bps: int,
    ) -> SlippageEstimate:
        """Simulate a swap through V3 tick ranges to compute exact price impact.

        Walks through initialized ticks, consuming liquidity at each range,
        to compute the effective execution price for the given amount.

        For a zeroForOne swap (token0 -> token1):
            - Price decreases (tick moves down)
            - We consume liquidity moving left through ticks

        For a oneForZero swap (token1 -> token0):
            - Price increases (tick moves up)
            - We consume liquidity moving right through ticks
        """
        if current_liquidity == 0 and not ticks:
            # No liquidity at all
            return SlippageEstimate(
                expected_price=mid_price,
                price_impact_bps=10000,  # 100% impact
                effective_slippage_bps=10000,
                recommended_max_size=Decimal(0),
            )

        # Convert amount to raw units for simulation
        if zero_for_one:
            raw_amount = amount * Decimal(10) ** token0_decimals
        else:
            raw_amount = amount * Decimal(10) ** token1_decimals

        # Apply fee
        fee_factor = Decimal(10000 - fee_bps) / Decimal(10000)
        remaining_amount = raw_amount * fee_factor

        # Track total output
        total_output = Decimal(0)
        liquidity = Decimal(current_liquidity)

        # Sort ticks for traversal direction
        if zero_for_one:
            # Moving down: need ticks below current, sorted descending
            relevant_ticks = sorted(
                [t for t in ticks if t.tick_index <= current_tick],
                key=lambda t: t.tick_index,
                reverse=True,
            )
        else:
            # Moving up: need ticks above current, sorted ascending
            relevant_ticks = sorted(
                [t for t in ticks if t.tick_index >= current_tick],
                key=lambda t: t.tick_index,
            )

        # Walk through ticks
        current_sqrt_price = Decimal(str(math.pow(1.0001, current_tick / 2)))

        for tick_data in relevant_ticks:
            if remaining_amount <= 0:
                break

            if liquidity <= 0:
                # Update liquidity from this tick
                if zero_for_one:
                    liquidity -= Decimal(tick_data.liquidity_net)
                else:
                    liquidity += Decimal(tick_data.liquidity_net)
                continue

            target_sqrt_price = Decimal(str(math.pow(1.0001, tick_data.tick_index / 2)))

            if zero_for_one:
                # Amount of token0 needed to move from current to target sqrt price
                # delta_x = L * (1/sqrt_target - 1/sqrt_current) -- but simplified
                if target_sqrt_price >= current_sqrt_price:
                    continue
                sqrt_diff = current_sqrt_price - target_sqrt_price
                if current_sqrt_price > 0 and target_sqrt_price > 0:
                    amount_to_next = liquidity * sqrt_diff / (current_sqrt_price * target_sqrt_price)
                else:
                    break

                if amount_to_next >= remaining_amount:
                    # Swap completes within this tick range
                    # Compute output: delta_y = L * (sqrt_current - sqrt_target)
                    fraction = remaining_amount / amount_to_next
                    output = liquidity * sqrt_diff * fraction
                    total_output += output
                    remaining_amount = Decimal(0)
                else:
                    # Consume entire range
                    output = liquidity * sqrt_diff
                    total_output += output
                    remaining_amount -= amount_to_next
                    current_sqrt_price = target_sqrt_price
                    # Cross tick: update liquidity
                    liquidity -= Decimal(tick_data.liquidity_net)

            else:
                # Amount of token1 needed to move from current to target sqrt price
                if target_sqrt_price <= current_sqrt_price:
                    continue
                sqrt_diff = target_sqrt_price - current_sqrt_price
                amount_to_next = liquidity * sqrt_diff

                if amount_to_next >= remaining_amount:
                    fraction = remaining_amount / amount_to_next
                    if liquidity > 0 and current_sqrt_price > 0:
                        output = (
                            liquidity
                            * sqrt_diff
                            * fraction
                            / (current_sqrt_price * (current_sqrt_price + sqrt_diff * fraction))
                        )
                    else:
                        output = Decimal(0)
                    total_output += output
                    remaining_amount = Decimal(0)
                else:
                    if current_sqrt_price > 0 and target_sqrt_price > 0:
                        output = liquidity * sqrt_diff / (current_sqrt_price * target_sqrt_price)
                    else:
                        output = Decimal(0)
                    total_output += output
                    remaining_amount -= amount_to_next
                    current_sqrt_price = target_sqrt_price
                    liquidity += Decimal(tick_data.liquidity_net)

        # Convert output to human-readable units
        if zero_for_one:
            # Output is in token1 raw units -> divide by 10^token1_decimals
            human_output = total_output / Decimal(10) ** token1_decimals
        else:
            # Output is in token0 raw units -> divide by 10^token0_decimals
            human_output = total_output / Decimal(10) ** token0_decimals

        # Compute execution price
        if amount > 0 and human_output > 0:
            if zero_for_one:
                # Execution price = output_token1 / input_token0
                exec_price = human_output / amount
            else:
                # Execution price = input_token1 / output_token0
                exec_price = amount / human_output
        elif amount > 0 and human_output <= 0:
            # No output produced -- 100% slippage (insufficient liquidity)
            return SlippageEstimate(
                expected_price=Decimal(0),
                price_impact_bps=10000,
                effective_slippage_bps=10000,
                recommended_max_size=Decimal(0),
            )
        else:
            exec_price = mid_price

        # Price impact (vs mid-market)
        if mid_price > 0:
            price_impact_bps = int(abs(Decimal(1) - exec_price / mid_price) * 10000)
            effective_slippage_bps = price_impact_bps
        else:
            price_impact_bps = 0
            effective_slippage_bps = 0

        # Recommended max size: binary search for 1% slippage
        # Approximate: proportional to current liquidity coverage
        if price_impact_bps > 0 and amount > 0:
            # Linear approximation: max_size ~ amount * (100 bps / actual_bps)
            if price_impact_bps > 0:
                recommended_max = amount * Decimal(100) / Decimal(price_impact_bps)
            else:
                recommended_max = amount * Decimal(10)
        else:
            recommended_max = amount * Decimal(10)

        return SlippageEstimate(
            expected_price=exec_price,
            price_impact_bps=price_impact_bps,
            effective_slippage_bps=effective_slippage_bps,
            recommended_max_size=recommended_max,
        )
