"""Curve Finance Receipt Parser (Refactored).

Refactored to use base infrastructure utilities while maintaining backward compatibility.
Uses int128 for token indices and handles 2-pool and 3-pool variants.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.connectors.base import EventRegistry, HexDecoder

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import LPCloseData, SwapAmounts
from almanak.framework.utils.log_formatters import format_gas_cost, format_tx_hash

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    "TokenExchange": "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140",
    "TokenExchangeUnderlying": "0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b",
    "AddLiquidity2": "0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768",
    "AddLiquidity3": "0x423f6495a08fc652425cf4ed0d1f9e37e571d9b9529b1c1c23cce780b2e7df0d",
    "RemoveLiquidity2": "0x7c363854ccf79623411f8995b362bce5eddff18c927edc6f5dbbb5e05819a82c",
    "RemoveLiquidity3": "0xa49d4cf02656aebf8c771f5a8585638a2a15ee6c97cf7205d4208ed7c1df252d",
    "RemoveLiquidityOne": "0x5ad056f2e28a8cec232015406b843668c1e36cda598127ec3b8c59b8c72773a0",
    "RemoveLiquidityImbalance": "0x2b5508378d7e19e0d5fa338419034731416c4f5b219a10379956f764317fd47e",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

# Legacy exports
TOKEN_EXCHANGE_TOPIC = EVENT_TOPICS["TokenExchange"]
TOKEN_EXCHANGE_UNDERLYING_TOPIC = EVENT_TOPICS["TokenExchangeUnderlying"]


# =============================================================================
# Enums
# =============================================================================


class CurveEventType(Enum):
    """Curve event types."""

    TOKEN_EXCHANGE = "TOKEN_EXCHANGE"
    TOKEN_EXCHANGE_UNDERLYING = "TOKEN_EXCHANGE_UNDERLYING"
    ADD_LIQUIDITY = "ADD_LIQUIDITY"
    REMOVE_LIQUIDITY = "REMOVE_LIQUIDITY"
    REMOVE_LIQUIDITY_ONE = "REMOVE_LIQUIDITY_ONE"
    REMOVE_LIQUIDITY_IMBALANCE = "REMOVE_LIQUIDITY_IMBALANCE"
    TRANSFER = "TRANSFER"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, CurveEventType] = {
    "TokenExchange": CurveEventType.TOKEN_EXCHANGE,
    "TokenExchangeUnderlying": CurveEventType.TOKEN_EXCHANGE_UNDERLYING,
    "AddLiquidity2": CurveEventType.ADD_LIQUIDITY,
    "AddLiquidity3": CurveEventType.ADD_LIQUIDITY,
    "RemoveLiquidity2": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidity3": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidityOne": CurveEventType.REMOVE_LIQUIDITY_ONE,
    "RemoveLiquidityImbalance": CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
    "Transfer": CurveEventType.TRANSFER,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class CurveEvent:
    """Parsed Curve event."""

    event_type: CurveEventType
    event_name: str
    log_index: int
    transaction_hash: str
    block_number: int
    contract_address: str
    data: dict[str, Any]
    raw_topics: list[str] = field(default_factory=list)
    raw_data: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "event_type": self.event_type.value,
            "event_name": self.event_name,
            "log_index": self.log_index,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "contract_address": self.contract_address,
            "data": self.data,
            "raw_topics": self.raw_topics,
            "raw_data": self.raw_data,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class SwapEventData:
    """Parsed data from TokenExchange event."""

    buyer: str
    sold_id: int  # int128 token index
    tokens_sold: int
    bought_id: int  # int128 token index
    tokens_bought: int
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "buyer": self.buyer,
            "sold_id": self.sold_id,
            "tokens_sold": str(self.tokens_sold),
            "bought_id": self.bought_id,
            "tokens_bought": str(self.tokens_bought),
            "pool_address": self.pool_address,
        }


@dataclass
class AddLiquidityEventData:
    """Parsed data from AddLiquidity event."""

    provider: str
    token_amounts: list[int]
    fees: list[int]
    invariant: int
    token_supply: int
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "provider": self.provider,
            "token_amounts": [str(a) for a in self.token_amounts],
            "fees": [str(f) for f in self.fees],
            "invariant": str(self.invariant),
            "token_supply": str(self.token_supply),
            "pool_address": self.pool_address,
        }


@dataclass
class RemoveLiquidityEventData:
    """Parsed data from RemoveLiquidity event."""

    provider: str
    token_amounts: list[int]
    fees: list[int]
    token_supply: int
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "provider": self.provider,
            "token_amounts": [str(a) for a in self.token_amounts],
            "fees": [str(f) for f in self.fees],
            "token_supply": str(self.token_supply),
            "pool_address": self.pool_address,
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    events: list[CurveEvent] = field(default_factory=list)
    swap_events: list[SwapEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0
    transaction_success: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "swap_events": [s.to_dict() for s in self.swap_events],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "transaction_success": self.transaction_success,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class CurveReceiptParser:
    """Parser for Curve Finance transaction receipts.

    Refactored to use base infrastructure utilities for hex decoding
    and event registry management. Maintains full backward compatibility.
    """

    def __init__(self, chain: str = "ethereum", **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            chain: Blockchain network
            **kwargs: Additional arguments (ignored for compatibility)
        """
        self.chain = chain.lower()
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def parse_receipt(
        self,
        receipt: dict[str, Any],
    ) -> ParseResult:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dict

        Returns:
            ParseResult with extracted events
        """
        try:
            tx_hash = receipt.get("transactionHash", "")
            if isinstance(tx_hash, bytes):
                tx_hash = "0x" + tx_hash.hex()

            block_number = receipt.get("blockNumber", 0)
            logs = receipt.get("logs", [])
            status = receipt.get("status", 1)
            tx_success = status == 1

            if not logs:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=tx_success,
                )

            # Handle failed transactions
            if not tx_success:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=False,
                    error="Transaction reverted",
                )

            events: list[CurveEvent] = []
            swap_events: list[SwapEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data for swaps
                    if parsed_event.event_type in (
                        CurveEventType.TOKEN_EXCHANGE,
                        CurveEventType.TOKEN_EXCHANGE_UNDERLYING,
                    ):
                        swap_data = self._parse_swap_event(parsed_event)
                        if swap_data:
                            swap_events.append(swap_data)

            # Log parsed receipt
            gas_used = receipt.get("gasUsed", 0)
            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(gas_used)

            if swap_events:
                swap = swap_events[0]
                logger.info(
                    f"🔍 Parsed Curve swap: token{swap.sold_id} → token{swap.bought_id}, tx={tx_fmt}, {gas_fmt}"
                )
            else:
                logger.info(f"🔍 Parsed Curve receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                events=events,
                swap_events=swap_events,
                transaction_hash=tx_hash,
                block_number=block_number,
                transaction_success=tx_success,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(
                success=False,
                error=str(e),
            )

    def _parse_log(
        self,
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
    ) -> CurveEvent | None:
        """Parse a single log entry.

        Args:
            log: Log dict
            tx_hash: Transaction hash
            block_number: Block number

        Returns:
            Parsed event or None if not recognized
        """
        try:
            topics = log.get("topics", [])
            if not topics:
                return None

            # Normalize first topic
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            else:
                first_topic = str(first_topic)
            first_topic = first_topic.lower()

            # Check if known event
            event_name = self.registry.get_event_name(first_topic)
            if event_name is None:
                return None

            event_type = self.registry.get_event_type(event_name) or CurveEventType.UNKNOWN

            # Get raw data
            data = HexDecoder.normalize_hex(log.get("data", ""))

            # Normalize contract address
            contract_address = log.get("address", "")
            if isinstance(contract_address, bytes):
                contract_address = "0x" + contract_address.hex()

            # Convert topics to strings
            topics_str = []
            for topic in topics:
                if isinstance(topic, bytes):
                    topics_str.append("0x" + topic.hex())
                else:
                    topics_str.append(str(topic))

            # Parse log data
            parsed_data = self._decode_log_data(event_type, topics, data, contract_address)

            return CurveEvent(
                event_type=event_type,
                event_name=event_name,
                log_index=log.get("logIndex", 0),
                transaction_hash=tx_hash,
                block_number=block_number,
                contract_address=contract_address,
                data=parsed_data,
                raw_topics=topics_str,
                raw_data=data,
            )

        except Exception as e:
            logger.warning(f"Failed to parse log: {e}")
            return None

    def _decode_log_data(
        self,
        event_type: CurveEventType,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode log data based on event type.

        Args:
            event_type: Type of event
            topics: List of topics
            data: Hex-encoded event data
            address: Contract address

        Returns:
            Decoded event data dict
        """
        if event_type in (CurveEventType.TOKEN_EXCHANGE, CurveEventType.TOKEN_EXCHANGE_UNDERLYING):
            return self._decode_swap_data(topics, data, address)
        elif event_type == CurveEventType.ADD_LIQUIDITY:
            return self._decode_add_liquidity_data(topics, data, address)
        elif event_type == CurveEventType.REMOVE_LIQUIDITY:
            return self._decode_remove_liquidity_data(topics, data, address)
        else:
            return {"raw_data": data}

    def _decode_swap_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode TokenExchange event data.

        TokenExchange(address indexed buyer, int128 sold_id, uint256 tokens_sold,
                      int128 bought_id, uint256 tokens_bought)
        """
        try:
            # Indexed: buyer
            buyer = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""

            # Non-indexed: sold_id (int128), tokens_sold, bought_id (int128), tokens_bought
            sold_id = HexDecoder.decode_int128(data, 0)
            tokens_sold = HexDecoder.decode_uint256(data, 32)
            bought_id = HexDecoder.decode_int128(data, 64)
            tokens_bought = HexDecoder.decode_uint256(data, 96)

            pool_address = address.lower() if isinstance(address, str) else ""

            return {
                "buyer": buyer,
                "sold_id": sold_id,
                "tokens_sold": tokens_sold,
                "bought_id": bought_id,
                "tokens_bought": tokens_bought,
                "pool_address": pool_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode TokenExchange data: {e}")
            return {"raw_data": data}

    def _decode_add_liquidity_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode AddLiquidity event data."""
        try:
            # Indexed: provider
            provider = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""

            # Determine pool type (2-coin or 3-coin) based on data length
            # AddLiquidity: amounts + fees + invariant + supply
            # 2-coin: 2 + 2 + 1 + 1 = 6 fields × 64 = 384 hex chars
            # 3-coin: 3 + 3 + 1 + 1 = 8 fields × 64 = 512 hex chars
            data_len = len(data)
            if data_len >= 512:  # 8 * 64 for 3-coin
                n_coins = 3
            else:
                n_coins = 2

            # Parse token amounts
            token_amounts = []
            for i in range(n_coins):
                token_amounts.append(HexDecoder.decode_uint256(data, i * 32))

            # Parse fees
            fees = []
            for i in range(n_coins):
                fees.append(HexDecoder.decode_uint256(data, (n_coins + i) * 32))

            # Parse invariant and supply
            invariant = HexDecoder.decode_uint256(data, n_coins * 2 * 32)
            token_supply = HexDecoder.decode_uint256(data, (n_coins * 2 + 1) * 32)

            pool_address = address.lower() if isinstance(address, str) else ""

            return {
                "provider": provider,
                "token_amounts": token_amounts,
                "fees": fees,
                "invariant": invariant,
                "token_supply": token_supply,
                "pool_address": pool_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode AddLiquidity data: {e}")
            return {"raw_data": data}

    def _decode_remove_liquidity_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode RemoveLiquidity event data."""
        try:
            # Indexed: provider
            provider = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""

            # Determine pool type based on data length
            # RemoveLiquidity: amounts + fees + supply (no invariant)
            # 2-coin: 2 + 2 + 1 = 5 fields × 64 = 320 hex chars
            # 3-coin: 3 + 3 + 1 = 7 fields × 64 = 448 hex chars
            data_len = len(data)
            if data_len >= 448:  # 7 * 64 for 3-coin
                n_coins = 3
            else:
                n_coins = 2

            # Parse token amounts
            token_amounts = []
            for i in range(n_coins):
                token_amounts.append(HexDecoder.decode_uint256(data, i * 32))

            # Parse fees
            fees = []
            for i in range(n_coins):
                fees.append(HexDecoder.decode_uint256(data, (n_coins + i) * 32))

            # Parse supply
            token_supply = HexDecoder.decode_uint256(data, n_coins * 2 * 32)

            pool_address = address.lower() if isinstance(address, str) else ""

            return {
                "provider": provider,
                "token_amounts": token_amounts,
                "fees": fees,
                "token_supply": token_supply,
                "pool_address": pool_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode RemoveLiquidity data: {e}")
            return {"raw_data": data}

    def _parse_swap_event(self, event: CurveEvent) -> SwapEventData | None:
        """Parse a swap event into typed data."""
        try:
            data = event.data
            return SwapEventData(
                buyer=data.get("buyer", ""),
                sold_id=data.get("sold_id", 0),
                tokens_sold=data.get("tokens_sold", 0),
                bought_id=data.get("bought_id", 0),
                tokens_bought=data.get("tokens_bought", 0),
                pool_address=data.get("pool_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse SwapEventData: {e}")
            return None

    # =============================================================================
    # Extraction Methods (for Result Enrichment)
    # =============================================================================

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> "SwapAmounts | None":
        """Extract swap amounts from a transaction receipt.

        Note: Decimal conversions assume 18 decimals. Curve pools often use tokens
        with different decimals (e.g., USDC/USDT with 6, WBTC with 8). The raw
        amount_in/amount_out fields are always accurate; use those with your own
        decimal scaling for precise calculations.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            SwapAmounts dataclass if swap event found, None otherwise
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            result = self.parse_receipt(receipt)
            if not result.swap_events:
                return None

            swap = result.swap_events[0]
            amount_in = swap.tokens_sold
            amount_out = swap.tokens_bought

            # Calculate decimal amounts (assuming 18 decimals - see docstring for limitations)
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
                token_in=f"token{swap.sold_id}",
                token_out=f"token{swap.bought_id}",
            )

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    def extract_lp_tokens_received(self, receipt: dict[str, Any]) -> int | None:
        """Extract LP tokens received from AddLiquidity transaction.

        Looks for Transfer events from the pool address to the user.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LP token amount if found, None otherwise
        """
        try:
            # Look for Transfer events from zero address (mint)
            zero_addr = "0x0000000000000000000000000000000000000000"
            transfer_topic = EVENT_TOPICS["Transfer"].lower()

            logs = receipt.get("logs", [])
            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 3:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != transfer_topic:
                    continue

                from_addr = HexDecoder.topic_to_address(topics[1])
                if from_addr.lower() == zero_addr:
                    data = HexDecoder.normalize_hex(log.get("data", ""))
                    lp_amount = HexDecoder.decode_uint256(data, 0)
                    return lp_amount

            return None

        except Exception as e:
            logger.warning(f"Failed to extract LP tokens received: {e}")
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> "LPCloseData | None":
        """Extract LP close data from transaction receipt.

        Looks for RemoveLiquidity, RemoveLiquidityOne, or RemoveLiquidityImbalance events.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPCloseData dataclass if liquidity removal found, None otherwise
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            result = self.parse_receipt(receipt)

            # Look for removal events
            for event in result.events:
                if event.event_type in (
                    CurveEventType.REMOVE_LIQUIDITY,
                    CurveEventType.REMOVE_LIQUIDITY_ONE,
                    CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
                ):
                    token_amounts = event.data.get("token_amounts", [])

                    # Get amounts for token0 and token1
                    amount0 = token_amounts[0] if len(token_amounts) > 0 else 0
                    amount1 = token_amounts[1] if len(token_amounts) > 1 else 0

                    # Get fees if available
                    fees = event.data.get("fees", [])
                    fees0 = fees[0] if len(fees) > 0 else 0
                    fees1 = fees[1] if len(fees) > 1 else 0

                    return LPCloseData(
                        amount0_collected=amount0,
                        amount1_collected=amount1,
                        fees0=fees0,
                        fees1=fees1,
                        liquidity_removed=None,  # LP tokens burned
                    )

            return None

        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    # Backward compatibility methods
    def is_curve_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Curve event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known Curve event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes) -> CurveEventType:
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
        return self.registry.get_event_type_from_topic(topic) or CurveEventType.UNKNOWN


__all__ = [
    "CurveReceiptParser",
    "CurveEvent",
    "CurveEventType",
    "SwapEventData",
    "AddLiquidityEventData",
    "RemoveLiquidityEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "TOKEN_EXCHANGE_TOPIC",
    "TOKEN_EXCHANGE_UNDERLYING_TOPIC",
]
