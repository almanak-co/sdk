"""Uniswap V4 Receipt Parser.

Parses transaction receipts for V4 events emitted by PoolManager and
PositionManager:
- Swap events (PoolManager)
- ModifyLiquidity events (PoolManager)
- ERC-721 Transfer events (PositionManager, for position ID extraction)

V4 Swap event:
    event Swap(
        PoolId indexed id,
        address indexed sender,
        int128 amount0,
        int128 amount1,
        uint160 sqrtPriceX96,
        uint128 liquidity,
        int24 tick,
        uint24 fee
    )

V4 ModifyLiquidity event:
    event ModifyLiquidity(
        PoolId indexed id,
        address indexed sender,
        int24 tickLower,
        int24 tickUpper,
        int256 liquidityDelta,
        bytes32 salt
    )
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.connectors.base import HexDecoder

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import LPCloseData, SwapAmounts

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

# V4 PoolManager event topics
EVENT_TOPICS: dict[str, str] = {
    # Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1,
    #       uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)
    "Swap": "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f",
    # ModifyLiquidity(bytes32 indexed id, address indexed sender,
    #                  int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt)
    "ModifyLiquidity": "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec",
    # Transfer (ERC-20 standard)
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    # Approval (ERC-20 standard)
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

SWAP_EVENT_TOPIC = EVENT_TOPICS["Swap"]
MODIFY_LIQUIDITY_TOPIC = EVENT_TOPICS["ModifyLiquidity"]
TRANSFER_EVENT_TOPIC = EVENT_TOPICS["Transfer"]


# =============================================================================
# Enums
# =============================================================================


class UniswapV4EventType(Enum):
    """Uniswap V4 event types."""

    SWAP = "SWAP"
    MODIFY_LIQUIDITY = "MODIFY_LIQUIDITY"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    UNKNOWN = "UNKNOWN"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SwapEventData:
    """Decoded V4 Swap event data."""

    pool_id: str
    sender: str
    amount0: int
    amount1: int
    sqrt_price_x96: int
    liquidity: int
    tick: int
    fee: int


@dataclass
class ModifyLiquidityEventData:
    """Decoded V4 ModifyLiquidity event data."""

    pool_id: str
    sender: str
    tick_lower: int
    tick_upper: int
    liquidity_delta: int
    salt: str


@dataclass
class TransferEventData:
    """Decoded ERC-20/ERC-721 Transfer event."""

    token: str
    from_address: str
    to_address: str
    amount: int


@dataclass
class ParsedSwapResult:
    """High-level parsed swap result."""

    amount_in: int
    amount_out: int
    amount_in_decimal: Decimal = Decimal(0)
    amount_out_decimal: Decimal = Decimal(0)
    token_in: str | None = None
    token_out: str | None = None
    effective_price: Decimal | None = None
    price_impact_bps: int | None = None
    slippage_bps: int | None = None
    tick_after: int | None = None
    sqrt_price_x96_after: int | None = None


@dataclass
class ParseResult:
    """Full parse result from a V4 transaction receipt."""

    swap_events: list[SwapEventData] = field(default_factory=list)
    modify_liquidity_events: list[ModifyLiquidityEventData] = field(default_factory=list)
    transfer_events: list[TransferEventData] = field(default_factory=list)
    swap_result: ParsedSwapResult | None = None
    error: str | None = None


# =============================================================================
# UniswapV4ReceiptParser
# =============================================================================


class UniswapV4ReceiptParser:
    """Parse Uniswap V4 transaction receipts.

    Extracts swap amounts, effective prices, and balance deltas from
    V4 PoolManager events.

    Args:
        chain: Chain name for context.
        pool_manager_address: PoolManager address to filter events.
    """

    def __init__(
        self,
        chain: str = "ethereum",
        pool_manager_address: str | None = None,
        position_manager_address: str | None = None,
        token_resolver: Any | None = None,
    ) -> None:
        self.chain = chain.lower()
        self._token_resolver = token_resolver

        from almanak.core.contracts import UNISWAP_V4

        chain_addrs = UNISWAP_V4.get(self.chain, {})
        if pool_manager_address:
            self.pool_manager = pool_manager_address.lower()
        else:
            self.pool_manager = chain_addrs.get("pool_manager", "").lower()

        if position_manager_address:
            self.position_manager = position_manager_address.lower()
        else:
            self.position_manager = chain_addrs.get("position_manager", "").lower()

    def parse_receipt(
        self,
        receipt: dict[str, Any],
        quoted_amount_out: int | None = None,
    ) -> ParseResult:
        """Parse a transaction receipt for V4 events.

        Args:
            receipt: Transaction receipt dict with 'logs' field.
            quoted_amount_out: Expected output for slippage calculation.

        Returns:
            ParseResult with decoded events and swap summary.
        """
        result = ParseResult()
        logs = receipt.get("logs", [])

        for log in logs:
            topics = log.get("topics", [])
            if not topics:
                continue

            topic0 = topics[0].lower() if isinstance(topics[0], str) else hex(topics[0])

            if topic0 == SWAP_EVENT_TOPIC.lower():
                swap_event = self._decode_swap_event(log)
                if swap_event:
                    result.swap_events.append(swap_event)

            elif topic0 == MODIFY_LIQUIDITY_TOPIC.lower():
                ml_event = self._decode_modify_liquidity_event(log)
                if ml_event:
                    result.modify_liquidity_events.append(ml_event)

            elif topic0 == TRANSFER_EVENT_TOPIC.lower():
                transfer = self._decode_transfer_event(log)
                if transfer:
                    result.transfer_events.append(transfer)

        # Build high-level swap result from events
        if result.swap_events:
            result.swap_result = self._build_swap_result(
                result.swap_events,
                result.transfer_events,
                quoted_amount_out,
            )

        return result

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> SwapAmounts | None:
        """Extract swap amounts for ResultEnricher integration.

        Args:
            receipt: Transaction receipt dict.

        Returns:
            SwapAmounts or None if no swap event found.
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        parsed = self.parse_receipt(receipt)
        if not parsed.swap_result:
            return None

        sr = parsed.swap_result

        return SwapAmounts(
            amount_in=sr.amount_in,
            amount_out=sr.amount_out,
            amount_in_decimal=sr.amount_in_decimal,
            amount_out_decimal=sr.amount_out_decimal,
            effective_price=sr.effective_price or Decimal(0),
            slippage_bps=sr.slippage_bps,
            token_in=sr.token_in,
            token_out=sr.token_out,
        )

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:
        """Extract LP position NFT tokenId from ERC-721 Transfer event.

        Looks for a Transfer event emitted by the PositionManager contract
        where from_address is the zero address (indicating a mint).

        Called by ResultEnricher for LP_OPEN intents.

        Args:
            receipt: Transaction receipt dict.

        Returns:
            Position ID (tokenId) or None if not found.
        """
        logs = receipt.get("logs", [])
        for log in logs:
            topics = log.get("topics", [])
            if len(topics) < 4:
                continue

            topic0 = topics[0].lower() if isinstance(topics[0], str) else hex(topics[0])
            if topic0 != TRANSFER_EVENT_TOPIC.lower():
                continue

            # Check if emitted by PositionManager
            log_address = log.get("address", "").lower()
            if log_address != self.position_manager:
                continue

            # ERC-721 Transfer: topic[1]=from, topic[2]=to, topic[3]=tokenId
            from_addr = topics[1] if isinstance(topics[1], str) else hex(topics[1])
            # Mint: from = zero address
            if int(from_addr, 16) == 0:
                token_id_hex = topics[3] if isinstance(topics[3], str) else hex(topics[3])
                return int(token_id_hex, 16)

        return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract liquidity delta from ModifyLiquidity event.

        Called by ResultEnricher for LP_OPEN intents.

        Args:
            receipt: Transaction receipt dict.

        Returns:
            Liquidity amount or None if not found.
        """
        parsed = self.parse_receipt(receipt)
        if not parsed.modify_liquidity_events:
            return None

        # Return the first positive (mint) liquidity delta
        for event in parsed.modify_liquidity_events:
            if event.liquidity_delta > 0:
                return event.liquidity_delta

        return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> LPCloseData | None:
        """Extract LP close data from ModifyLiquidity and Transfer events.

        Called by ResultEnricher for LP_CLOSE intents.

        Args:
            receipt: Transaction receipt dict.

        Returns:
            LPCloseData with collected amounts, or None if not found.
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        parsed = self.parse_receipt(receipt)

        # Find the decrease event (negative liquidity delta)
        liquidity_removed = None
        for event in parsed.modify_liquidity_events:
            if event.liquidity_delta < 0:
                liquidity_removed = abs(event.liquidity_delta)
                break

        if liquidity_removed is None and not parsed.modify_liquidity_events:
            return None

        # Sum Transfer events FROM the pool manager TO the wallet (tokens collected)
        amount0_collected = 0
        amount1_collected = 0

        # Group transfers from PoolManager by token address
        collected_by_token: dict[str, int] = {}
        for transfer in parsed.transfer_events:
            if transfer.from_address.lower() == self.pool_manager:
                token = transfer.token.lower()
                collected_by_token[token] = collected_by_token.get(token, 0) + transfer.amount

        # Assign to amount0/amount1 by sorted token address order
        sorted_tokens = sorted(collected_by_token.keys())
        if len(sorted_tokens) >= 1:
            amount0_collected = collected_by_token[sorted_tokens[0]]
        if len(sorted_tokens) >= 2:
            amount1_collected = collected_by_token[sorted_tokens[1]]

        return LPCloseData(
            amount0_collected=amount0_collected,
            amount1_collected=amount1_collected,
            liquidity_removed=liquidity_removed,
        )

    # -- Decoding helpers -----------------------------------------------------

    def _decode_modify_liquidity_event(self, log: dict[str, Any]) -> ModifyLiquidityEventData | None:
        """Decode a V4 ModifyLiquidity event from a log entry."""
        topics = log.get("topics", [])
        data = log.get("data", "0x")

        if len(topics) < 3:
            return None

        try:
            pool_id = topics[1] if isinstance(topics[1], str) else hex(topics[1])
            sender = (
                HexDecoder.decode_address_from_data(topics[2][2:]) if isinstance(topics[2], str) else hex(topics[2])
            )

            # Data layout: int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt
            clean_data = data[2:] if data.startswith("0x") else data

            tick_lower = HexDecoder.decode_int24(clean_data[0:64])
            tick_upper = HexDecoder.decode_int24(clean_data[64:128])
            liquidity_delta = HexDecoder.decode_int256(clean_data[128:192])
            salt = "0x" + clean_data[192:256] if len(clean_data) >= 256 else "0x0"

            return ModifyLiquidityEventData(
                pool_id=pool_id,
                sender=sender,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                liquidity_delta=liquidity_delta,
                salt=salt,
            )
        except Exception as e:
            logger.warning("Failed to decode V4 ModifyLiquidity event: %s", e)
            return None

    def _decode_swap_event(self, log: dict[str, Any]) -> SwapEventData | None:
        """Decode a V4 Swap event from a log entry."""
        topics = log.get("topics", [])
        data = log.get("data", "0x")

        if len(topics) < 3:
            return None

        try:
            pool_id = topics[1] if isinstance(topics[1], str) else hex(topics[1])
            sender = (
                HexDecoder.decode_address_from_data(topics[2][2:]) if isinstance(topics[2], str) else hex(topics[2])
            )

            # Data layout: int128 amount0, int128 amount1, uint160 sqrtPriceX96,
            #              uint128 liquidity, int24 tick, uint24 fee
            # Each field is 32 bytes in ABI encoding
            clean_data = data[2:] if data.startswith("0x") else data

            amount0 = HexDecoder.decode_int256(clean_data[0:64])
            amount1 = HexDecoder.decode_int256(clean_data[64:128])
            sqrt_price_x96 = HexDecoder.decode_uint256(clean_data[128:192])
            liquidity = HexDecoder.decode_uint256(clean_data[192:256])
            tick = HexDecoder.decode_int24(clean_data[256:320])
            fee = HexDecoder.decode_uint256(clean_data[320:384])

            return SwapEventData(
                pool_id=pool_id,
                sender=sender,
                amount0=amount0,
                amount1=amount1,
                sqrt_price_x96=sqrt_price_x96,
                liquidity=liquidity,
                tick=tick,
                fee=fee,
            )
        except Exception as e:
            logger.warning("Failed to decode V4 Swap event: %s", e)
            return None

    def _decode_transfer_event(self, log: dict[str, Any]) -> TransferEventData | None:
        """Decode an ERC-20 Transfer event."""
        topics = log.get("topics", [])
        data = log.get("data", "0x")

        if len(topics) < 3:
            return None

        try:
            token = log.get("address", "").lower()
            from_addr = HexDecoder.decode_address_from_data(topics[1][2:]) if isinstance(topics[1], str) else ""
            to_addr = HexDecoder.decode_address_from_data(topics[2][2:]) if isinstance(topics[2], str) else ""

            clean_data = data[2:] if data.startswith("0x") else data
            amount = HexDecoder.decode_uint256(clean_data[0:64]) if clean_data else 0

            return TransferEventData(
                token=token,
                from_address=from_addr,
                to_address=to_addr,
                amount=amount,
            )
        except Exception as e:
            logger.warning("Failed to decode Transfer event: %s", e)
            return None

    def _build_swap_result(
        self,
        swap_events: list[SwapEventData],
        transfer_events: list[TransferEventData],
        quoted_amount_out: int | None,
    ) -> ParsedSwapResult:
        """Build a high-level swap result from decoded events."""
        # Use the first swap event (single-hop)
        swap = swap_events[0]

        # In V4 Swap events: positive = tokens entering the pool, negative = leaving
        # For exactInput: amount0 > 0 means token0 was input, amount1 < 0 means token1 was output
        if swap.amount0 > 0:
            amount_in = swap.amount0
            amount_out = abs(swap.amount1)
        else:
            amount_in = swap.amount1
            amount_out = abs(swap.amount0)

        # Calculate slippage vs quote
        slippage_bps = None
        if quoted_amount_out and quoted_amount_out > 0 and amount_out > 0:
            slippage = (quoted_amount_out - amount_out) / quoted_amount_out
            slippage_bps = int(slippage * 10000)

        # Resolve token decimals from Transfer events for proper decimal conversion
        token_in_addr = None
        token_out_addr = None
        token_in_decimals = None
        token_out_decimals = None

        # Identify token addresses from Transfer events
        # In a swap: one Transfer goes TO the pool (token_in), one comes FROM the pool (token_out)
        pool_manager = self.pool_manager
        for transfer in transfer_events:
            if transfer.to_address.lower() == pool_manager:
                token_in_addr = transfer.token
            elif transfer.from_address.lower() == pool_manager:
                token_out_addr = transfer.token

        # Fallback: V4 flash accounting routes tokens through UniversalRouter/Permit2,
        # so Transfers may not be directly to/from PoolManager. Match by amount instead.
        # Skip transfers for tokens already identified to avoid mismatches when
        # amount_in == amount_out (e.g. stablecoin-to-stablecoin swaps).
        if (token_in_addr is None or token_out_addr is None) and transfer_events:
            for transfer in transfer_events:
                # Skip transfers for tokens already assigned to the other side
                if token_in_addr is None and transfer.amount == amount_in and transfer.token != token_out_addr:
                    token_in_addr = transfer.token
                elif token_out_addr is None and transfer.amount == amount_out and transfer.token != token_in_addr:
                    token_out_addr = transfer.token

        # Resolve decimals via token_resolver (lazy-load if not injected)
        resolver = self._token_resolver
        if resolver is None:
            try:
                from almanak.framework.data.tokens import get_token_resolver

                resolver = get_token_resolver()
            except Exception:
                logger.debug("Could not load token_resolver for decimal conversion")

        if resolver and token_in_addr:
            try:
                resolved = resolver.resolve(token_in_addr, self.chain)
                token_in_decimals = resolved.decimals
            except Exception:
                logger.warning(
                    "Could not resolve decimals for token_in %s — decimal amounts will be zero", token_in_addr
                )
        if resolver and token_out_addr:
            try:
                resolved = resolver.resolve(token_out_addr, self.chain)
                token_out_decimals = resolved.decimals
            except Exception:
                logger.warning(
                    "Could not resolve decimals for token_out %s — decimal amounts will be zero", token_out_addr
                )

        # Compute human-readable decimal amounts
        # When decimals are not resolved, fall back to Decimal(0) rather than raw integer
        # amounts — a raw integer (e.g. 2000000000 for 2000 USDC) in a field documented as
        # "human-readable" would silently corrupt downstream financial logic.
        both_decimals_resolved = token_in_decimals is not None and token_out_decimals is not None
        if token_in_decimals is not None:
            amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
        else:
            amount_in_decimal = Decimal(0)
        if token_out_decimals is not None:
            amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**token_out_decimals)
        else:
            amount_out_decimal = Decimal(0)

        # Effective price: only compute when BOTH decimals are resolved to avoid
        # mixing raw integer amounts with human-readable decimals (would produce
        # prices off by orders of magnitude for cross-decimal pairs like USDC/WETH).
        effective_price = None
        if both_decimals_resolved and amount_in_decimal > 0 and amount_out_decimal > 0:
            effective_price = amount_out_decimal / amount_in_decimal

        return ParsedSwapResult(
            amount_in=amount_in,
            amount_out=amount_out,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            token_in=token_in_addr,
            token_out=token_out_addr,
            effective_price=effective_price,
            slippage_bps=slippage_bps,
            tick_after=swap.tick,
            sqrt_price_x96_after=swap.sqrt_price_x96,
        )


__all__ = [
    "EVENT_TOPICS",
    "ModifyLiquidityEventData",
    "ParsedSwapResult",
    "ParseResult",
    "SwapEventData",
    "TransferEventData",
    "UniswapV4EventType",
    "UniswapV4ReceiptParser",
]
