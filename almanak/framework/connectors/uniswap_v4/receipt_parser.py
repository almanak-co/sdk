"""Uniswap V4 Receipt Parser.

Parses transaction receipts for V4 swap events emitted by the PoolManager.
V4 uses a different Swap event signature than V3 since all swaps go through
the singleton PoolManager contract.

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
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.connectors.base import HexDecoder

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import SwapAmounts

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
class TransferEventData:
    """Decoded ERC-20 Transfer event."""

    token: str
    from_address: str
    to_address: str
    amount: int


@dataclass
class ParsedSwapResult:
    """High-level parsed swap result."""

    amount_in: int
    amount_out: int
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
    ) -> None:
        self.chain = chain.lower()
        if pool_manager_address:
            self.pool_manager = pool_manager_address.lower()
        else:
            from almanak.core.contracts import UNISWAP_V4

            self.pool_manager = UNISWAP_V4.get(self.chain, {}).get("pool_manager", "").lower()

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
            amount_in_decimal=Decimal(sr.amount_in),
            amount_out_decimal=Decimal(sr.amount_out),
            effective_price=sr.effective_price or Decimal(0),
            slippage_bps=sr.slippage_bps,
            token_in=sr.token_in,
            token_out=sr.token_out,
        )

    # -- Decoding helpers -----------------------------------------------------

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

        # Effective price
        effective_price = None
        if amount_in > 0 and amount_out > 0:
            effective_price = Decimal(amount_out) / Decimal(amount_in)

        return ParsedSwapResult(
            amount_in=amount_in,
            amount_out=amount_out,
            effective_price=effective_price,
            slippage_bps=slippage_bps,
            tick_after=swap.tick,
            sqrt_price_x96_after=swap.sqrt_price_x96,
        )


__all__ = [
    "EVENT_TOPICS",
    "ParsedSwapResult",
    "ParseResult",
    "SwapEventData",
    "TransferEventData",
    "UniswapV4EventType",
    "UniswapV4ReceiptParser",
]
