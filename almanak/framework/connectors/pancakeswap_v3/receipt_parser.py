"""PancakeSwap V3 Receipt Parser (Refactored).

Refactored to use base infrastructure while maintaining backward compatibility.
PancakeSwap V3 is a Uniswap V3 fork with identical event signatures.
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.connectors.base import BaseReceiptParser, EventRegistry, HexDecoder

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import LPCloseData, SwapAmounts

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures (same as Uniswap V3)
# =============================================================================

# PancakeSwap V3 uses a DIFFERENT Swap event signature than Uniswap V3!
# UniswapV3: Swap(address,address,int256,int256,uint160,uint128,int24) - 7 params
# PancakeSwapV3: Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128) - 9 params
# The extra 2 uint128 params are protocolFeesToken0 and protocolFeesToken1
EVENT_TOPICS: dict[str, str] = {
    "Swap": "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83",  # PancakeSwap V3 (9 params)
    "Mint": "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
    "Burn": "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
    "Collect": "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class PancakeSwapV3EventType(Enum):
    """PancakeSwap V3 event types."""

    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    COLLECT = "COLLECT"
    TRANSFER = "TRANSFER"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, PancakeSwapV3EventType] = {
    "Swap": PancakeSwapV3EventType.SWAP,
    "Mint": PancakeSwapV3EventType.MINT,
    "Burn": PancakeSwapV3EventType.BURN,
    "Collect": PancakeSwapV3EventType.COLLECT,
    "Transfer": PancakeSwapV3EventType.TRANSFER,
}

# PancakeSwap V3 NonfungiblePositionManager addresses
POSITION_MANAGER_ADDRESSES: dict[str, str] = {
    "bsc": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "ethereum": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "arbitrum": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "base": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
}

# Zero address for detecting mints
ZERO_ADDRESS_PADDED = "0x" + "0" * 64


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SwapEventData:
    """Parsed data from PancakeSwap V3 Swap event.

    Note: PancakeSwap V3 Swap event has 9 parameters (vs 7 for Uniswap V3):
    - sender, recipient (indexed)
    - amount0, amount1, sqrtPriceX96, liquidity, tick (same as UniV3)
    - protocolFeesToken0, protocolFeesToken1 (PancakeSwap V3 specific)
    """

    pool: str
    sender: str
    recipient: str
    amount0: Decimal
    amount1: Decimal
    sqrt_price_x96: int = 0
    liquidity: int = 0
    tick: int = 0
    protocol_fees_token0: int = 0  # PancakeSwap V3 specific
    protocol_fees_token1: int = 0  # PancakeSwap V3 specific

    @property
    def token0_in(self) -> bool:
        """Check if token0 is the input token."""
        return self.amount0 > 0

    @property
    def token1_in(self) -> bool:
        """Check if token1 is the input token."""
        return self.amount1 > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "pool": self.pool,
            "sender": self.sender,
            "recipient": self.recipient,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "sqrt_price_x96": str(self.sqrt_price_x96),
            "liquidity": str(self.liquidity),
            "tick": self.tick,
            "token0_in": self.token0_in,
            "token1_in": self.token1_in,
            "protocol_fees_token0": str(self.protocol_fees_token0),
            "protocol_fees_token1": str(self.protocol_fees_token1),
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    swaps: list[SwapEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "swaps": [s.to_dict() for s in self.swaps],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class PancakeSwapV3ReceiptParser(BaseReceiptParser[SwapEventData, ParseResult]):
    """Parser for PancakeSwap V3 transaction receipts.

    Uses base infrastructure for common parsing logic while handling
    PancakeSwap V3-specific event decoding.
    """

    def __init__(self, chain: str = "bsc", **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            chain: Blockchain network (for position manager address lookup)
        """
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        super().__init__(registry=registry)
        self.chain = chain.lower()

    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
        contract_address: str,
    ) -> dict[str, Any]:
        """Decode Swap event data.

        PancakeSwap V3 Swap event structure (9 params, different from UniV3's 7):
        - topic0: event signature
        - topic1: sender (indexed)
        - topic2: recipient (indexed)
        - data: amount0 (int256), amount1 (int256), sqrtPriceX96 (uint160),
                liquidity (uint128), tick (int24),
                protocolFeesToken0 (uint128), protocolFeesToken1 (uint128)
        """
        if event_name != "Swap":
            return {}

        # Parse indexed topics
        sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        recipient = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

        # Parse non-indexed data (each value is 32 bytes = 64 hex chars)
        # PancakeSwap V3 has 7 data fields vs UniV3's 5
        amount0 = HexDecoder.decode_int256(data, 0)
        amount1 = HexDecoder.decode_int256(data, 32)
        sqrt_price_x96 = HexDecoder.decode_uint160(data, 64)
        liquidity = HexDecoder.decode_uint128(data, 96)
        tick = HexDecoder.decode_int24(data, 128)
        # PancakeSwap V3 specific: protocol fees (offset 160 and 192)
        protocol_fees_token0 = HexDecoder.decode_uint128(data, 160)
        protocol_fees_token1 = HexDecoder.decode_uint128(data, 192)

        return {
            "pool_address": contract_address.lower(),
            "sender": sender,
            "recipient": recipient,
            "amount0": amount0,
            "amount1": amount1,
            "sqrt_price_x96": sqrt_price_x96,
            "liquidity": liquidity,
            "tick": tick,
            "protocol_fees_token0": protocol_fees_token0,
            "protocol_fees_token1": protocol_fees_token1,
        }

    def _create_event(
        self,
        event_name: str,
        log_index: int,
        tx_hash: str,
        block_number: int,
        contract_address: str,
        decoded_data: dict[str, Any],
        raw_topics: list[str],
        raw_data: str,
    ) -> SwapEventData | None:
        """Create SwapEventData from decoded data.

        Only creates events for Swap - returns None for other event types
        (Transfer, Mint, Burn, Collect) which are tracked but not returned.
        """
        _ = (log_index, tx_hash, block_number, raw_topics, raw_data)

        # Only create SwapEventData for actual Swap events
        if event_name != "Swap" or not decoded_data:
            return None

        return SwapEventData(
            pool=decoded_data.get("pool_address", contract_address),
            sender=decoded_data.get("sender", ""),
            recipient=decoded_data.get("recipient", ""),
            amount0=Decimal(decoded_data.get("amount0", 0)),
            amount1=Decimal(decoded_data.get("amount1", 0)),
            sqrt_price_x96=decoded_data.get("sqrt_price_x96", 0),
            liquidity=decoded_data.get("liquidity", 0),
            tick=decoded_data.get("tick", 0),
            protocol_fees_token0=decoded_data.get("protocol_fees_token0", 0),
            protocol_fees_token1=decoded_data.get("protocol_fees_token1", 0),
        )

    def _build_result(
        self,
        events: list[SwapEventData],
        receipt: dict[str, Any],
        tx_hash: str,
        block_number: int,
        tx_success: bool,
        **kwargs,
    ) -> ParseResult:
        """Build ParseResult from parsed events."""
        _ = receipt
        error = kwargs.get("error")

        if not tx_success or error:
            return ParseResult(
                success=False,
                error=error or "Transaction failed",
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        logger.info(f"Parsed PancakeSwap V3 receipt: tx={tx_hash[:10]}..., swaps={len(events)}")

        return ParseResult(
            success=True,
            swaps=events,
            transaction_hash=tx_hash,
            block_number=block_number,
        )

    # =============================================================================
    # Extraction Methods (for Result Enrichment)
    # =============================================================================

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> "SwapAmounts | None":
        """Extract swap amounts from a transaction receipt.

        Note: Decimal conversions assume 18 decimals. PancakeSwap pools often
        include tokens with different decimals (e.g., USDC with 6, WBTC with 8).
        The raw amount_in/amount_out fields are always accurate; use those with
        your own decimal scaling for precise calculations.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            SwapAmounts dataclass if swap event found, None otherwise
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            result = self.parse_receipt(receipt)
            if not result.swaps:
                return None

            swap = result.swaps[0]
            amount0 = int(swap.amount0)
            amount1 = int(swap.amount1)

            # Determine input/output based on signs
            if amount0 > 0:
                amount_in = amount0
                amount_out = abs(amount1)
            else:
                amount_in = amount1
                amount_out = abs(amount0)

            # Calculate effective price (assuming 18 decimals - see docstring for limitations)
            amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**18)
            amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**18)
            effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal(0)

            return SwapAmounts(
                amount_in=amount_in,
                amount_out=amount_out,
                amount_in_decimal=amount_in_decimal,
                amount_out_decimal=amount_out_decimal,
                effective_price=effective_price,
                slippage_bps=None,
                token_in=None,
                token_out=None,
            )

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:
        """Extract LP position ID (NFT tokenId) from a transaction receipt.

        Looks for ERC-721 Transfer events from the NonfungiblePositionManager
        where from=address(0), indicating a mint.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Position ID (tokenId) if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            position_manager = POSITION_MANAGER_ADDRESSES.get(self.chain, "").lower()
            if not position_manager:
                position_manager = "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364".lower()

            transfer_topic = EVENT_TOPICS["Transfer"].lower()

            for log in logs:
                topics = log.get("topics", [])
                address = log.get("address", "")

                if isinstance(address, bytes):
                    address = "0x" + address.hex()
                address = str(address).lower()

                if address != position_manager:
                    continue

                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != transfer_topic:
                    continue

                from_topic = topics[1]
                if isinstance(from_topic, bytes):
                    from_topic = "0x" + from_topic.hex()
                from_topic = str(from_topic).lower()

                if from_topic != ZERO_ADDRESS_PADDED:
                    continue

                token_id_topic = topics[3]
                if isinstance(token_id_topic, bytes):
                    token_id_topic = "0x" + token_id_topic.hex()
                token_id_topic = str(token_id_topic)

                try:
                    token_id = int(token_id_topic, 16)
                    logger.info(f"Extracted PancakeSwap V3 LP position ID: {token_id}")
                    return token_id
                except (ValueError, TypeError):
                    continue

            return None

        except Exception as e:
            logger.warning(f"Failed to extract position ID: {e}")
            return None

    def extract_tick_lower(self, receipt: dict[str, Any]) -> int | None:
        """Extract tick lower from LP mint transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Tick lower value if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            mint_topic = EVENT_TOPICS["Mint"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                tick_lower_topic = topics[2]
                if isinstance(tick_lower_topic, bytes):
                    tick_lower_topic = "0x" + tick_lower_topic.hex()
                tick_lower_topic = str(tick_lower_topic)

                tick_lower = HexDecoder.decode_int24(tick_lower_topic, 0)
                return tick_lower

            return None

        except Exception as e:
            logger.warning(f"Failed to extract tick_lower: {e}")
            return None

    def extract_tick_upper(self, receipt: dict[str, Any]) -> int | None:
        """Extract tick upper from LP mint transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Tick upper value if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            mint_topic = EVENT_TOPICS["Mint"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                tick_upper_topic = topics[3]
                if isinstance(tick_upper_topic, bytes):
                    tick_upper_topic = "0x" + tick_upper_topic.hex()
                tick_upper_topic = str(tick_upper_topic)

                tick_upper = HexDecoder.decode_int24(tick_upper_topic, 0)
                return tick_upper

            return None

        except Exception as e:
            logger.warning(f"Failed to extract tick_upper: {e}")
            return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract liquidity from LP mint transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Liquidity amount if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            mint_topic = EVENT_TOPICS["Mint"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                data = HexDecoder.normalize_hex(log.get("data", ""))
                if not data or data == "0x":
                    continue

                liquidity = HexDecoder.decode_uint128(data, 0)
                return liquidity

            return None

        except Exception as e:
            logger.warning(f"Failed to extract liquidity: {e}")
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> "LPCloseData | None":
        """Extract LP close data from transaction receipt.

        Looks for Collect events which indicate fees and principal being collected.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPCloseData dataclass if Collect event found, None otherwise
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            collect_topic = EVENT_TOPICS["Collect"].lower()
            burn_topic = EVENT_TOPICS["Burn"].lower()

            total_amount0 = 0
            total_amount1 = 0
            liquidity_removed = None

            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                data = HexDecoder.normalize_hex(log.get("data", ""))

                if first_topic == collect_topic and len(topics) >= 4:
                    amount0 = HexDecoder.decode_uint128(data, 0)
                    amount1 = HexDecoder.decode_uint128(data, 32)
                    total_amount0 += amount0
                    total_amount1 += amount1

                elif first_topic == burn_topic and len(topics) >= 4:
                    liquidity_removed = HexDecoder.decode_uint128(data, 0)

            if total_amount0 > 0 or total_amount1 > 0:
                return LPCloseData(
                    amount0_collected=total_amount0,
                    amount1_collected=total_amount1,
                    fees0=0,
                    fees1=0,
                    liquidity_removed=liquidity_removed,
                )

            return None

        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    # Backward compatibility methods
    def parse_swap(self, log: dict[str, Any]) -> SwapEventData | None:
        """Parse a Swap event from a single log entry.

        Backward compatibility method.
        """
        topics = log.get("topics", [])
        data = log.get("data", "")
        pool_address = log.get("address", "")

        if isinstance(data, bytes):
            data = "0x" + data.hex()
        if isinstance(pool_address, bytes):
            pool_address = "0x" + pool_address.hex()

        # Decode using internal method
        data_normalized = HexDecoder.normalize_hex(data)
        decoded_data = self._decode_log_data("Swap", topics, data_normalized, pool_address)

        if not decoded_data:
            return None

        return self._create_event(
            event_name="Swap",
            log_index=log.get("logIndex", 0),
            tx_hash="",
            block_number=0,
            contract_address=pool_address,
            decoded_data=decoded_data,
            raw_topics=[],
            raw_data="",
        )

    def is_pancakeswap_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known PancakeSwap V3 event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known PancakeSwap V3 event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic) if self.registry else False

    def get_event_type(self, topic: str | bytes) -> PancakeSwapV3EventType:
        """Get the event type for a topic.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            Event type or UNKNOWN
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        if self.registry is None:
            return PancakeSwapV3EventType.UNKNOWN
        return self.registry.get_event_type_from_topic(topic) or PancakeSwapV3EventType.UNKNOWN


__all__ = [
    "PancakeSwapV3ReceiptParser",
    "PancakeSwapV3EventType",
    "SwapEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "POSITION_MANAGER_ADDRESSES",
]
