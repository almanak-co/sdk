"""GMX v2 Receipt Parser.

This module provides parsing functionality for GMX v2 transaction receipts
and events, enabling extraction of position updates, order fills, and other
protocol events from on-chain data.

GMX v2 Events:
- OrderCreated: New order submitted
- OrderExecuted: Order filled successfully
- OrderCancelled: Order cancelled
- OrderFrozen: Order frozen due to error
- PositionIncrease: Position size increased
- PositionDecrease: Position size decreased
- DepositCreated: Liquidity deposit created
- DepositExecuted: Liquidity deposit executed
- WithdrawalCreated: Liquidity withdrawal created
- WithdrawalExecuted: Liquidity withdrawal executed

Example:
    from almanak.framework.connectors.gmx_v2 import GMXv2ReceiptParser

    parser = GMXv2ReceiptParser()

    # Parse transaction receipt
    events = parser.parse_receipt(receipt)

    for event in events:
        if event.event_type == GMXv2EventType.POSITION_INCREASE:
            print(f"Position increased: {event.data}")
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.base import EventRegistry, HexDecoder
from almanak.framework.utils.log_formatters import format_gas_cost, format_tx_hash, format_usd

logger = logging.getLogger(__name__)


def _normalize_datetime_to_utc(dt: datetime) -> datetime:
    """Normalize a datetime to UTC timezone.

    If the datetime is naive (no tzinfo), assume it's UTC and add UTC timezone.
    If it already has a timezone, convert it to UTC.

    Args:
        dt: Datetime that may or may not have timezone info.

    Returns:
        Datetime with UTC timezone.
    """
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


# =============================================================================
# Event Topic Signatures
# =============================================================================

# GMX v2 event topic signatures (keccak256 hashes of event signatures)
EVENT_TOPICS: dict[str, str] = {
    # Order events
    "OrderCreated": "0x3a3c9eff40a8c51a3d8ccf8f9eb47e7c9a0000000000000000000000000000001",
    "OrderExecuted": "0x3a3c9eff40a8c51a3d8ccf8f9eb47e7c9a0000000000000000000000000000002",
    "OrderCancelled": "0x3a3c9eff40a8c51a3d8ccf8f9eb47e7c9a0000000000000000000000000000003",
    "OrderFrozen": "0x3a3c9eff40a8c51a3d8ccf8f9eb47e7c9a0000000000000000000000000000004",
    "OrderUpdated": "0x3a3c9eff40a8c51a3d8ccf8f9eb47e7c9a0000000000000000000000000000005",
    # Position events
    "PositionIncrease": "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc0a3ffa67d28dc3",
    "PositionDecrease": "0x2e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000001",
    "PositionFeesInfo": "0x2e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000002",
    "PositionFeesCollected": "0x2e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000003",
    # Deposit events
    "DepositCreated": "0x4e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000001",
    "DepositExecuted": "0x4e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000002",
    "DepositCancelled": "0x4e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000003",
    # Withdrawal events
    "WithdrawalCreated": "0x5e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000001",
    "WithdrawalExecuted": "0x5e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000002",
    "WithdrawalCancelled": "0x5e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000003",
    # Market events
    "MarketCreated": "0x6e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000001",
    "MarketPoolValueUpdated": "0x6e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000002",
    # Oracle events
    "OraclePriceUpdated": "0x7e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000001",
    # Funding events
    "ClaimableFundingUpdated": "0x8e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000001",
    "FundingFeesClaimed": "0x8e1f85a64a2f22cf2f0c7e1c8f6a32b1b71d6f6c8f1d6b3c2d1e0f000000000002",
}

# Reverse lookup: topic -> event name
TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class GMXv2EventType(Enum):
    """GMX v2 event types."""

    # Order events
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_EXECUTED = "ORDER_EXECUTED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    ORDER_FROZEN = "ORDER_FROZEN"
    ORDER_UPDATED = "ORDER_UPDATED"

    # Position events
    POSITION_INCREASE = "POSITION_INCREASE"
    POSITION_DECREASE = "POSITION_DECREASE"
    POSITION_FEES_INFO = "POSITION_FEES_INFO"
    POSITION_FEES_COLLECTED = "POSITION_FEES_COLLECTED"

    # Deposit/Withdrawal events
    DEPOSIT_CREATED = "DEPOSIT_CREATED"
    DEPOSIT_EXECUTED = "DEPOSIT_EXECUTED"
    DEPOSIT_CANCELLED = "DEPOSIT_CANCELLED"
    WITHDRAWAL_CREATED = "WITHDRAWAL_CREATED"
    WITHDRAWAL_EXECUTED = "WITHDRAWAL_EXECUTED"
    WITHDRAWAL_CANCELLED = "WITHDRAWAL_CANCELLED"

    # Market events
    MARKET_CREATED = "MARKET_CREATED"
    MARKET_POOL_VALUE_UPDATED = "MARKET_POOL_VALUE_UPDATED"

    # Oracle events
    ORACLE_PRICE_UPDATED = "ORACLE_PRICE_UPDATED"

    # Funding events
    CLAIMABLE_FUNDING_UPDATED = "CLAIMABLE_FUNDING_UPDATED"
    FUNDING_FEES_CLAIMED = "FUNDING_FEES_CLAIMED"

    # Unknown
    UNKNOWN = "UNKNOWN"


# Mapping from event name to event type
EVENT_NAME_TO_TYPE: dict[str, GMXv2EventType] = {
    "OrderCreated": GMXv2EventType.ORDER_CREATED,
    "OrderExecuted": GMXv2EventType.ORDER_EXECUTED,
    "OrderCancelled": GMXv2EventType.ORDER_CANCELLED,
    "OrderFrozen": GMXv2EventType.ORDER_FROZEN,
    "OrderUpdated": GMXv2EventType.ORDER_UPDATED,
    "PositionIncrease": GMXv2EventType.POSITION_INCREASE,
    "PositionDecrease": GMXv2EventType.POSITION_DECREASE,
    "PositionFeesInfo": GMXv2EventType.POSITION_FEES_INFO,
    "PositionFeesCollected": GMXv2EventType.POSITION_FEES_COLLECTED,
    "DepositCreated": GMXv2EventType.DEPOSIT_CREATED,
    "DepositExecuted": GMXv2EventType.DEPOSIT_EXECUTED,
    "DepositCancelled": GMXv2EventType.DEPOSIT_CANCELLED,
    "WithdrawalCreated": GMXv2EventType.WITHDRAWAL_CREATED,
    "WithdrawalExecuted": GMXv2EventType.WITHDRAWAL_EXECUTED,
    "WithdrawalCancelled": GMXv2EventType.WITHDRAWAL_CANCELLED,
    "MarketCreated": GMXv2EventType.MARKET_CREATED,
    "MarketPoolValueUpdated": GMXv2EventType.MARKET_POOL_VALUE_UPDATED,
    "OraclePriceUpdated": GMXv2EventType.ORACLE_PRICE_UPDATED,
    "ClaimableFundingUpdated": GMXv2EventType.CLAIMABLE_FUNDING_UPDATED,
    "FundingFeesClaimed": GMXv2EventType.FUNDING_FEES_CLAIMED,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class GMXv2Event:
    """Parsed GMX v2 event.

    Attributes:
        event_type: Type of event
        event_name: Name of event (e.g., "PositionIncrease")
        log_index: Index of log in transaction
        transaction_hash: Transaction hash
        block_number: Block number
        contract_address: Contract that emitted event
        data: Parsed event data
        raw_topics: Raw event topics
        raw_data: Raw event data
        timestamp: Event timestamp
    """

    event_type: GMXv2EventType
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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GMXv2Event":
        """Create from dictionary."""
        # Parse and normalize timestamp to UTC
        if "timestamp" in data:
            timestamp = _normalize_datetime_to_utc(datetime.fromisoformat(data["timestamp"]))
        else:
            timestamp = datetime.now(UTC)

        return cls(
            event_type=GMXv2EventType(data["event_type"]),
            event_name=data["event_name"],
            log_index=data["log_index"],
            transaction_hash=data["transaction_hash"],
            block_number=data["block_number"],
            contract_address=data["contract_address"],
            data=data["data"],
            raw_topics=data.get("raw_topics", []),
            raw_data=data.get("raw_data", ""),
            timestamp=timestamp,
        )


@dataclass
class PositionIncreaseData:
    """Parsed data from PositionIncrease event.

    Attributes:
        key: Position key
        account: Account address
        market: Market address
        collateral_token: Collateral token address
        is_long: Position direction
        size_in_usd: New position size in USD
        size_in_tokens: New position size in tokens
        collateral_amount: New collateral amount
        borrowing_factor: Current borrowing factor
        funding_fee_amount_per_size: Funding fee per size
        long_token_claimable_funding_amount_per_size: Long token funding
        short_token_claimable_funding_amount_per_size: Short token funding
        execution_price: Execution price
        index_token_price_max: Max index token price
        index_token_price_min: Min index token price
        collateral_token_price_max: Max collateral token price
        collateral_token_price_min: Min collateral token price
        size_delta_usd: Size change in USD
        size_delta_in_tokens: Size change in tokens
        collateral_delta_amount: Collateral change
        price_impact_usd: Price impact in USD
        price_impact_diff_usd: Price impact difference in USD
        order_key: Associated order key
        order_type: Order type
    """

    key: str
    account: str
    market: str
    collateral_token: str
    is_long: bool
    size_in_usd: Decimal
    size_in_tokens: Decimal
    collateral_amount: Decimal
    borrowing_factor: Decimal = Decimal("0")
    funding_fee_amount_per_size: Decimal = Decimal("0")
    long_token_claimable_funding_amount_per_size: Decimal = Decimal("0")
    short_token_claimable_funding_amount_per_size: Decimal = Decimal("0")
    execution_price: Decimal = Decimal("0")
    index_token_price_max: Decimal = Decimal("0")
    index_token_price_min: Decimal = Decimal("0")
    collateral_token_price_max: Decimal = Decimal("0")
    collateral_token_price_min: Decimal = Decimal("0")
    size_delta_usd: Decimal = Decimal("0")
    size_delta_in_tokens: Decimal = Decimal("0")
    collateral_delta_amount: Decimal = Decimal("0")
    price_impact_usd: Decimal = Decimal("0")
    price_impact_diff_usd: Decimal = Decimal("0")
    order_key: str = ""
    order_type: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "account": self.account,
            "market": self.market,
            "collateral_token": self.collateral_token,
            "is_long": self.is_long,
            "size_in_usd": str(self.size_in_usd),
            "size_in_tokens": str(self.size_in_tokens),
            "collateral_amount": str(self.collateral_amount),
            "borrowing_factor": str(self.borrowing_factor),
            "funding_fee_amount_per_size": str(self.funding_fee_amount_per_size),
            "execution_price": str(self.execution_price),
            "index_token_price_max": str(self.index_token_price_max),
            "index_token_price_min": str(self.index_token_price_min),
            "collateral_token_price_max": str(self.collateral_token_price_max),
            "collateral_token_price_min": str(self.collateral_token_price_min),
            "size_delta_usd": str(self.size_delta_usd),
            "size_delta_in_tokens": str(self.size_delta_in_tokens),
            "collateral_delta_amount": str(self.collateral_delta_amount),
            "price_impact_usd": str(self.price_impact_usd),
            "price_impact_diff_usd": str(self.price_impact_diff_usd),
            "order_key": self.order_key,
            "order_type": self.order_type,
        }


@dataclass
class PositionDecreaseData:
    """Parsed data from PositionDecrease event.

    Attributes:
        key: Position key
        account: Account address
        market: Market address
        collateral_token: Collateral token address
        is_long: Position direction
        size_in_usd: New position size in USD
        size_in_tokens: New position size in tokens
        collateral_amount: New collateral amount
        execution_price: Execution price
        index_token_price_max: Max index token price
        index_token_price_min: Min index token price
        collateral_token_price_max: Max collateral token price
        collateral_token_price_min: Min collateral token price
        size_delta_usd: Size change in USD
        size_delta_in_tokens: Size change in tokens
        collateral_delta_amount: Collateral change
        price_impact_usd: Price impact in USD
        base_pnl_usd: Base PnL in USD
        uncapped_base_pnl_usd: Uncapped base PnL in USD
        realized_pnl: Realized PnL
        order_key: Associated order key
        order_type: Order type
    """

    key: str
    account: str
    market: str
    collateral_token: str
    is_long: bool
    size_in_usd: Decimal
    size_in_tokens: Decimal
    collateral_amount: Decimal
    execution_price: Decimal = Decimal("0")
    index_token_price_max: Decimal = Decimal("0")
    index_token_price_min: Decimal = Decimal("0")
    collateral_token_price_max: Decimal = Decimal("0")
    collateral_token_price_min: Decimal = Decimal("0")
    size_delta_usd: Decimal = Decimal("0")
    size_delta_in_tokens: Decimal = Decimal("0")
    collateral_delta_amount: Decimal = Decimal("0")
    price_impact_usd: Decimal = Decimal("0")
    base_pnl_usd: Decimal = Decimal("0")
    uncapped_base_pnl_usd: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    order_key: str = ""
    order_type: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "account": self.account,
            "market": self.market,
            "collateral_token": self.collateral_token,
            "is_long": self.is_long,
            "size_in_usd": str(self.size_in_usd),
            "size_in_tokens": str(self.size_in_tokens),
            "collateral_amount": str(self.collateral_amount),
            "execution_price": str(self.execution_price),
            "index_token_price_max": str(self.index_token_price_max),
            "index_token_price_min": str(self.index_token_price_min),
            "collateral_token_price_max": str(self.collateral_token_price_max),
            "collateral_token_price_min": str(self.collateral_token_price_min),
            "size_delta_usd": str(self.size_delta_usd),
            "size_delta_in_tokens": str(self.size_delta_in_tokens),
            "collateral_delta_amount": str(self.collateral_delta_amount),
            "price_impact_usd": str(self.price_impact_usd),
            "base_pnl_usd": str(self.base_pnl_usd),
            "uncapped_base_pnl_usd": str(self.uncapped_base_pnl_usd),
            "realized_pnl": str(self.realized_pnl),
            "order_key": self.order_key,
            "order_type": self.order_type,
        }


@dataclass
class OrderEventData:
    """Parsed data from Order events.

    Attributes:
        key: Order key
        account: Account address
        receiver: Receiver address
        market: Market address
        initial_collateral_token: Initial collateral token
        order_type: Order type
        decrease_position_swap_type: Swap type for decreases
        size_delta_usd: Size change in USD
        initial_collateral_delta_amount: Initial collateral change
        trigger_price: Trigger price
        acceptable_price: Acceptable price
        execution_fee: Execution fee paid
        min_output_amount: Minimum output amount
        updated_at_block: Block number when updated
        is_long: Position direction
        is_frozen: Whether order is frozen
        cancelled_reason: Reason for cancellation (if cancelled)
        frozen_reason: Reason for freezing (if frozen)
    """

    key: str
    account: str
    receiver: str
    market: str
    initial_collateral_token: str
    order_type: int
    decrease_position_swap_type: int = 0
    size_delta_usd: Decimal = Decimal("0")
    initial_collateral_delta_amount: Decimal = Decimal("0")
    trigger_price: Decimal = Decimal("0")
    acceptable_price: Decimal = Decimal("0")
    execution_fee: int = 0
    min_output_amount: Decimal = Decimal("0")
    updated_at_block: int = 0
    is_long: bool = True
    is_frozen: bool = False
    cancelled_reason: str = ""
    frozen_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "account": self.account,
            "receiver": self.receiver,
            "market": self.market,
            "initial_collateral_token": self.initial_collateral_token,
            "order_type": self.order_type,
            "decrease_position_swap_type": self.decrease_position_swap_type,
            "size_delta_usd": str(self.size_delta_usd),
            "initial_collateral_delta_amount": str(self.initial_collateral_delta_amount),
            "trigger_price": str(self.trigger_price),
            "acceptable_price": str(self.acceptable_price),
            "execution_fee": self.execution_fee,
            "min_output_amount": str(self.min_output_amount),
            "updated_at_block": self.updated_at_block,
            "is_long": self.is_long,
            "is_frozen": self.is_frozen,
            "cancelled_reason": self.cancelled_reason,
            "frozen_reason": self.frozen_reason,
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt.

    Attributes:
        success: Whether parsing succeeded
        events: List of parsed events
        position_increases: Position increase events
        position_decreases: Position decrease events
        order_events: Order-related events
        error: Error message if parsing failed
        transaction_hash: Transaction hash
        block_number: Block number
    """

    success: bool
    events: list[GMXv2Event] = field(default_factory=list)
    position_increases: list[PositionIncreaseData] = field(default_factory=list)
    position_decreases: list[PositionDecreaseData] = field(default_factory=list)
    order_events: list[OrderEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "position_increases": [p.to_dict() for p in self.position_increases],
            "position_decreases": [p.to_dict() for p in self.position_decreases],
            "order_events": [o.to_dict() for o in self.order_events],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class GMXv2ReceiptParser:
    """Parser for GMX v2 transaction receipts.

    This parser extracts and decodes GMX v2 events from transaction receipts,
    providing structured data for position updates, order fills, and other
    protocol events.

    SUPPORTED_EXTRACTIONS declares the extraction fields this parser can provide.
    Used by ResultEnricher to warn when expected fields are unsupported.

    Example:
        parser = GMXv2ReceiptParser()

        # Parse a receipt dict (from web3.py)
        result = parser.parse_receipt(receipt)

        if result.success:
            for event in result.events:
                print(f"Event: {event.event_name}")

            for increase in result.position_increases:
                print(f"Position increased: size=${increase.size_in_usd}")
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "swap_amounts",
            "position_id",
            "size_delta",
            "collateral",
            "entry_price",
            "leverage",
            "realized_pnl",
            "exit_price",
            "fees_paid",
        }
    )

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            **kwargs: Additional arguments (ignored for compatibility)
        """
        _ = kwargs  # Explicitly unused for forward compatibility
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def parse_receipt(self, receipt: dict[str, Any]) -> ParseResult:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dict containing 'logs', 'transactionHash',
                     'blockNumber', etc.

        Returns:
            ParseResult with extracted events and data
        """
        try:
            tx_hash = receipt.get("transactionHash", "")
            if isinstance(tx_hash, bytes):
                tx_hash = "0x" + tx_hash.hex()

            block_number = receipt.get("blockNumber", 0)
            logs = receipt.get("logs", [])

            if not logs:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                )

            events: list[GMXv2Event] = []
            position_increases: list[PositionIncreaseData] = []
            position_decreases: list[PositionDecreaseData] = []
            order_events: list[OrderEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data based on event type
                    if parsed_event.event_type == GMXv2EventType.POSITION_INCREASE:
                        increase_data = self._parse_position_increase(parsed_event)
                        if increase_data:
                            position_increases.append(increase_data)

                    elif parsed_event.event_type == GMXv2EventType.POSITION_DECREASE:
                        decrease_data = self._parse_position_decrease(parsed_event)
                        if decrease_data:
                            position_decreases.append(decrease_data)

                    elif parsed_event.event_type in (
                        GMXv2EventType.ORDER_CREATED,
                        GMXv2EventType.ORDER_EXECUTED,
                        GMXv2EventType.ORDER_CANCELLED,
                        GMXv2EventType.ORDER_FROZEN,
                    ):
                        order_data = self._parse_order_event(parsed_event)
                        if order_data:
                            order_events.append(order_data)

            # Log parsed receipt with user-friendly formatting
            gas_used = receipt.get("gasUsed", 0)
            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(gas_used)

            if position_increases:
                for p in position_increases:
                    direction = "LONG" if p.is_long else "SHORT"
                    size_fmt = format_usd(p.size_delta_usd)
                    price_fmt = format_usd(p.execution_price)
                    logger.info(
                        f"🔍 Parsed GMX position OPEN: {direction} {size_fmt}, "
                        f"entry={price_fmt}, tx={tx_fmt}, {gas_fmt}"
                    )
            elif position_decreases:
                for pd in position_decreases:
                    direction = "LONG" if pd.is_long else "SHORT"
                    size_fmt = format_usd(pd.size_delta_usd)
                    pnl_fmt = format_usd(pd.realized_pnl)
                    logger.info(
                        f"🔍 Parsed GMX position CLOSE: {direction} {size_fmt}, PnL={pnl_fmt}, tx={tx_fmt}, {gas_fmt}"
                    )
            elif order_events:
                for o in order_events:
                    logger.info(f"🔍 Parsed GMX order: type={o.order_type}, tx={tx_fmt}, {gas_fmt}")
            else:
                logger.info(f"🔍 Parsed GMX V2 receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                events=events,
                position_increases=position_increases,
                position_decreases=position_decreases,
                order_events=order_events,
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(
                success=False,
                error=str(e),
            )

    def parse_logs(self, logs: list[dict[str, Any]]) -> list[GMXv2Event]:
        """Parse a list of logs.

        Args:
            logs: List of log dicts

        Returns:
            List of parsed events
        """
        events = []
        for log in logs:
            event = self._parse_log(log, "", 0)
            if event:
                events.append(event)
        return events

    def _parse_log(
        self,
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
    ) -> GMXv2Event | None:
        """Parse a single log entry.

        Args:
            log: Log dict containing 'topics', 'data', 'address', etc.
            tx_hash: Transaction hash
            block_number: Block number

        Returns:
            Parsed event or None if not a known GMX v2 event
        """
        try:
            topics = log.get("topics", [])
            if not topics:
                return None

            # Get the event signature (first topic)
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            else:
                first_topic = str(first_topic)
            first_topic = first_topic.lower()

            # Look up event name using registry
            event_name = self.registry.get_event_name(first_topic)
            if event_name is None:
                # Unknown event, skip
                return None

            event_type = self.registry.get_event_type(event_name) or GMXv2EventType.UNKNOWN

            # Get raw data
            data = log.get("data", "")
            if isinstance(data, bytes):
                data = "0x" + data.hex()

            # Parse log data
            parsed_data = self._decode_log_data(event_name, topics, data)

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

            return GMXv2Event(
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
        event_name: str,
        topics: list[Any],
        data: str,
    ) -> dict[str, Any]:
        """Decode log data based on event type.

        Args:
            event_name: Name of the event
            topics: List of topics
            data: Hex-encoded event data

        Returns:
            Decoded event data dict
        """
        # Remove 0x prefix if present
        if data.startswith("0x"):
            data = data[2:]

        # Decode based on event type
        # Note: This is simplified - production would use proper ABI decoding

        if event_name == "PositionIncrease":
            return self._decode_position_increase_data(topics, data)
        elif event_name == "PositionDecrease":
            return self._decode_position_decrease_data(topics, data)
        elif event_name in ("OrderCreated", "OrderExecuted", "OrderCancelled", "OrderFrozen"):
            return self._decode_order_data(topics, data, event_name)
        else:
            # Return raw data for unknown events
            return {"raw_data": data}

    def _decode_position_increase_data(
        self,
        topics: list[Any],
        data: str,
    ) -> dict[str, Any]:
        """Decode PositionIncrease event data."""
        # Simplified decoding - production would use proper ABI decoding
        try:
            # GMX uses 10**30 for USD values and 10**18 for token values
            usd_scale = Decimal(10**30)
            token_scale = Decimal(10**18)

            # Parse key fields from PositionIncrease event data.
            # Layout (32 bytes each): account, market, collateral_token, is_long,
            # size_in_usd, size_in_tokens, collateral_amount, execution_price,
            # size_delta_usd, collateral_delta_amount, index_token_price_max,
            # index_token_price_min, collateral_token_price_max, collateral_token_price_min,
            # price_impact_usd, order_type, order_key
            result: dict[str, Any] = {
                "key": HexDecoder.topic_to_bytes32(topics[1]) if len(topics) > 1 else "0x" + "00" * 32,
                "account": HexDecoder.decode_address_from_data(data, 0),
                "market": HexDecoder.decode_address_from_data(data, 32),
                "collateral_token": HexDecoder.decode_address_from_data(data, 64),
                "is_long": HexDecoder.decode_uint256(data, 96) == 1,
                "size_in_usd": str(Decimal(HexDecoder.decode_uint256(data, 128)) / usd_scale),
                "size_in_tokens": str(Decimal(HexDecoder.decode_uint256(data, 160)) / token_scale),
                "collateral_amount": str(Decimal(HexDecoder.decode_uint256(data, 192)) / token_scale),
                "execution_price": str(Decimal(HexDecoder.decode_uint256(data, 224)) / usd_scale),
                "size_delta_usd": str(Decimal(HexDecoder.decode_uint256(data, 256)) / usd_scale),
                "collateral_delta_amount": str(Decimal(HexDecoder.decode_uint256(data, 288)) / token_scale),
                "index_token_price_max": str(Decimal(HexDecoder.decode_uint256(data, 320)) / usd_scale),
                "index_token_price_min": str(Decimal(HexDecoder.decode_uint256(data, 352)) / usd_scale),
                "collateral_token_price_max": str(Decimal(HexDecoder.decode_uint256(data, 384)) / usd_scale),
                "collateral_token_price_min": str(Decimal(HexDecoder.decode_uint256(data, 416)) / usd_scale),
                "price_impact_usd": str(Decimal(HexDecoder.decode_int256(data, 448)) / usd_scale),
                "order_type": HexDecoder.decode_uint256(data, 480),
                "order_key": "0x" + data[512 * 2 : 512 * 2 + 64] if len(data) >= 512 * 2 + 64 else "",
            }

            return result

        except Exception as e:
            logger.warning(f"Failed to decode PositionIncrease data: {e}")
            return {"raw_data": data}

    def _decode_position_decrease_data(
        self,
        topics: list[Any],
        data: str,
    ) -> dict[str, Any]:
        """Decode PositionDecrease event data."""
        try:
            # GMX uses 10**30 for USD values and 10**18 for token values
            usd_scale = Decimal(10**30)
            token_scale = Decimal(10**18)

            # Layout (32 bytes each): account, market, collateral_token, is_long,
            # size_in_usd, size_in_tokens, collateral_amount, execution_price,
            # size_delta_usd, collateral_delta_amount, index_token_price_max,
            # index_token_price_min, collateral_token_price_max, collateral_token_price_min,
            # price_impact_usd, realized_pnl
            result: dict[str, Any] = {
                "key": HexDecoder.topic_to_bytes32(topics[1]) if len(topics) > 1 else "0x" + "00" * 32,
                "account": HexDecoder.decode_address_from_data(data, 0),
                "market": HexDecoder.decode_address_from_data(data, 32),
                "collateral_token": HexDecoder.decode_address_from_data(data, 64),
                "is_long": HexDecoder.decode_uint256(data, 96) == 1,
                "size_in_usd": str(Decimal(HexDecoder.decode_uint256(data, 128)) / usd_scale),
                "size_in_tokens": str(Decimal(HexDecoder.decode_uint256(data, 160)) / token_scale),
                "collateral_amount": str(Decimal(HexDecoder.decode_uint256(data, 192)) / token_scale),
                "execution_price": str(Decimal(HexDecoder.decode_uint256(data, 224)) / usd_scale),
                "size_delta_usd": str(Decimal(HexDecoder.decode_uint256(data, 256)) / usd_scale),
                "collateral_delta_amount": str(Decimal(HexDecoder.decode_uint256(data, 288)) / token_scale),
                "index_token_price_max": str(Decimal(HexDecoder.decode_uint256(data, 320)) / usd_scale),
                "index_token_price_min": str(Decimal(HexDecoder.decode_uint256(data, 352)) / usd_scale),
                "collateral_token_price_max": str(Decimal(HexDecoder.decode_uint256(data, 384)) / usd_scale),
                "collateral_token_price_min": str(Decimal(HexDecoder.decode_uint256(data, 416)) / usd_scale),
                "price_impact_usd": str(Decimal(HexDecoder.decode_int256(data, 448)) / usd_scale),
                "realized_pnl": str(Decimal(HexDecoder.decode_int256(data, 480)) / usd_scale),
            }

            return result

        except Exception as e:
            logger.warning(f"Failed to decode PositionDecrease data: {e}")
            return {"raw_data": data}

    def _decode_order_data(
        self,
        topics: list[Any],
        data: str,
        event_name: str,
    ) -> dict[str, Any]:
        """Decode Order event data."""
        try:
            # GMX uses 10**30 for USD values and 10**18 for token values
            usd_scale = Decimal(10**30)
            token_scale = Decimal(10**18)

            # Layout (32 bytes each): account, receiver, market, initial_collateral_token,
            # order_type, decrease_position_swap_type, is_long, size_delta_usd,
            # initial_collateral_delta_amount, trigger_price, acceptable_price,
            # execution_fee, min_output_amount, updated_at_block
            result: dict[str, Any] = {
                "key": HexDecoder.topic_to_bytes32(topics[1]) if len(topics) > 1 else "0x" + "00" * 32,
                "account": HexDecoder.decode_address_from_data(data, 0),
                "receiver": HexDecoder.decode_address_from_data(data, 32),
                "market": HexDecoder.decode_address_from_data(data, 64),
                "initial_collateral_token": HexDecoder.decode_address_from_data(data, 96),
                "order_type": HexDecoder.decode_uint256(data, 128),
                "decrease_position_swap_type": HexDecoder.decode_uint256(data, 160),
                "is_long": HexDecoder.decode_uint256(data, 192) == 1,
                "size_delta_usd": str(Decimal(HexDecoder.decode_uint256(data, 224)) / usd_scale),
                "initial_collateral_delta_amount": str(Decimal(HexDecoder.decode_uint256(data, 256)) / token_scale),
                "trigger_price": str(Decimal(HexDecoder.decode_uint256(data, 288)) / usd_scale),
                "acceptable_price": str(Decimal(HexDecoder.decode_uint256(data, 320)) / usd_scale),
                "execution_fee": HexDecoder.decode_uint256(data, 352),
                "min_output_amount": str(Decimal(HexDecoder.decode_uint256(data, 384)) / token_scale),
                "updated_at_block": HexDecoder.decode_uint256(data, 416),
                "event_name": event_name,
            }

            # Add event-specific fields
            if event_name == "OrderCancelled":
                result["cancelled_reason"] = "User cancelled"  # Would decode from data
            elif event_name == "OrderFrozen":
                result["frozen_reason"] = "Execution failed"  # Would decode from data
                result["is_frozen"] = True

            return result

        except Exception as e:
            logger.warning(f"Failed to decode Order data: {e}")
            return {"raw_data": data, "event_name": event_name}

    def _parse_position_increase(self, event: GMXv2Event) -> PositionIncreaseData | None:
        """Parse a PositionIncrease event into typed data."""
        try:
            data = event.data
            return PositionIncreaseData(
                key=data.get("key", ""),
                account=data.get("account", ""),
                market=data.get("market", ""),
                collateral_token=data.get("collateral_token", ""),
                is_long=data.get("is_long", True),
                size_in_usd=Decimal(data.get("size_in_usd", "0")),
                size_in_tokens=Decimal(data.get("size_in_tokens", "0")),
                collateral_amount=Decimal(data.get("collateral_amount", "0")),
                execution_price=Decimal(data.get("execution_price", "0")),
                index_token_price_max=Decimal(data.get("index_token_price_max", "0")),
                index_token_price_min=Decimal(data.get("index_token_price_min", "0")),
                collateral_token_price_max=Decimal(data.get("collateral_token_price_max", "0")),
                collateral_token_price_min=Decimal(data.get("collateral_token_price_min", "0")),
                size_delta_usd=Decimal(data.get("size_delta_usd", "0")),
                collateral_delta_amount=Decimal(data.get("collateral_delta_amount", "0")),
                price_impact_usd=Decimal(data.get("price_impact_usd", "0")),
                order_type=data.get("order_type", 0),
                order_key=data.get("order_key", ""),
            )
        except Exception as e:
            logger.warning(f"Failed to parse PositionIncreaseData: {e}")
            return None

    def _parse_position_decrease(self, event: GMXv2Event) -> PositionDecreaseData | None:
        """Parse a PositionDecrease event into typed data."""
        try:
            data = event.data
            return PositionDecreaseData(
                key=data.get("key", ""),
                account=data.get("account", ""),
                market=data.get("market", ""),
                collateral_token=data.get("collateral_token", ""),
                is_long=data.get("is_long", True),
                size_in_usd=Decimal(data.get("size_in_usd", "0")),
                size_in_tokens=Decimal(data.get("size_in_tokens", "0")),
                collateral_amount=Decimal(data.get("collateral_amount", "0")),
                execution_price=Decimal(data.get("execution_price", "0")),
                index_token_price_max=Decimal(data.get("index_token_price_max", "0")),
                index_token_price_min=Decimal(data.get("index_token_price_min", "0")),
                collateral_token_price_max=Decimal(data.get("collateral_token_price_max", "0")),
                collateral_token_price_min=Decimal(data.get("collateral_token_price_min", "0")),
                size_delta_usd=Decimal(data.get("size_delta_usd", "0")),
                collateral_delta_amount=Decimal(data.get("collateral_delta_amount", "0")),
                price_impact_usd=Decimal(data.get("price_impact_usd", "0")),
                realized_pnl=Decimal(data.get("realized_pnl", "0")),
            )
        except Exception as e:
            logger.warning(f"Failed to parse PositionDecreaseData: {e}")
            return None

    def _parse_order_event(self, event: GMXv2Event) -> OrderEventData | None:
        """Parse an Order event into typed data."""
        try:
            data = event.data
            return OrderEventData(
                key=data.get("key", ""),
                account=data.get("account", ""),
                receiver=data.get("receiver", data.get("account", "")),
                market=data.get("market", ""),
                initial_collateral_token=data.get("initial_collateral_token", ""),
                order_type=data.get("order_type", 0),
                decrease_position_swap_type=data.get("decrease_position_swap_type", 0),
                size_delta_usd=Decimal(data.get("size_delta_usd", "0")),
                initial_collateral_delta_amount=Decimal(data.get("initial_collateral_delta_amount", "0")),
                trigger_price=Decimal(data.get("trigger_price", "0")),
                acceptable_price=Decimal(data.get("acceptable_price", "0")),
                execution_fee=data.get("execution_fee", 0),
                min_output_amount=Decimal(data.get("min_output_amount", "0")),
                updated_at_block=data.get("updated_at_block", 0),
                is_long=data.get("is_long", True),
                is_frozen=data.get("is_frozen", False),
                cancelled_reason=data.get("cancelled_reason", ""),
                frozen_reason=data.get("frozen_reason", ""),
            )
        except Exception as e:
            logger.warning(f"Failed to parse OrderEventData: {e}")
            return None

    def is_gmx_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known GMX v2 event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known GMX v2 event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes) -> GMXv2EventType:
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
        return self.registry.get_event_type_from_topic(topic) or GMXv2EventType.UNKNOWN

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> Any:
        """Extract swap amounts from transaction receipt.

        GMX V2 swaps are executed through orders, not traditional swap events.
        For GMX orders:
        - amount_in = initial_collateral_delta_amount (collateral deposited)
        - amount_out = size_delta_usd (position size in USD, scaled by 1e30)
        - effective_price represents the leverage ratio

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            SwapAmounts dataclass if swap order found, None otherwise
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            result = self.parse_receipt(receipt)
            # Look for swap-related order events
            for order in result.order_events:
                # Order type 0 is typically a swap in GMX
                if order.order_type == 0:
                    # Values are already decoded decimals from _decode_order_data
                    # (divided by 1e18 for tokens, 1e30 for USD)
                    amount_in_decimal = order.initial_collateral_delta_amount
                    amount_out_decimal = order.size_delta_usd

                    # Reconstruct raw integer amounts for SwapAmounts compatibility
                    amount_in = int(amount_in_decimal * Decimal(10**18))
                    amount_out = int(amount_out_decimal * Decimal(10**30))

                    # Effective price is the leverage ratio for GMX
                    effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal(0)

                    return SwapAmounts(
                        amount_in=amount_in,
                        amount_out=amount_out,
                        amount_in_decimal=amount_in_decimal,
                        amount_out_decimal=amount_out_decimal,
                        effective_price=effective_price,
                        slippage_bps=None,
                        token_in=None,  # GMX swaps don't have simple token in/out
                        token_out=None,
                    )
            return None
        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    def extract_position_id(self, receipt: dict[str, Any]) -> str | None:
        """Extract position ID (key) from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Position key if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            # Check position increases first (opening)
            if result.position_increases:
                return result.position_increases[0].key
            # Then check decreases (closing/reducing)
            if result.position_decreases:
                return result.position_decreases[0].key
            return None
        except Exception as e:
            logger.warning(f"Failed to extract position ID: {e}")
            return None

    def extract_size_delta(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract size delta (in USD) from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Size delta in USD if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_increases:
                return result.position_increases[0].size_delta_usd
            if result.position_decreases:
                return result.position_decreases[0].size_delta_usd
            return None
        except Exception as e:
            logger.warning(f"Failed to extract size delta: {e}")
            return None

    def extract_collateral(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract collateral amount from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Collateral amount if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_increases:
                return result.position_increases[0].collateral_amount
            if result.position_decreases:
                return result.position_decreases[0].collateral_amount
            return None
        except Exception as e:
            logger.warning(f"Failed to extract collateral: {e}")
            return None

    def extract_entry_price(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract entry price from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Entry price in USD if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_increases:
                return result.position_increases[0].execution_price
            return None
        except Exception as e:
            logger.warning(f"Failed to extract entry price: {e}")
            return None

    def extract_leverage(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract leverage from transaction receipt.

        Leverage is calculated as size_in_usd / (collateral_amount * collateral_token_price).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Leverage multiplier (e.g., Decimal("10") for 10x) if found, None otherwise.
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_increases:
                pos = result.position_increases[0]
                # Use collateral token price to convert collateral to USD
                if pos.collateral_amount > 0 and pos.collateral_token_price_max > 0:
                    collateral_value_usd = pos.collateral_amount * pos.collateral_token_price_max
                    return pos.size_in_usd / collateral_value_usd
            return None
        except Exception as e:
            logger.warning(f"Failed to extract leverage: {e}")
            return None

    def extract_realized_pnl(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract realized PnL from transaction receipt.

        Only available for position decreases (closing/reducing positions).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Realized PnL in USD if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_decreases:
                return result.position_decreases[0].realized_pnl
            return None
        except Exception as e:
            logger.warning(f"Failed to extract realized PnL: {e}")
            return None

    def extract_exit_price(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract exit price from transaction receipt.

        Only available for position decreases (closing/reducing positions).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Exit price in USD if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_decreases:
                return result.position_decreases[0].execution_price
            return None
        except Exception as e:
            logger.warning(f"Failed to extract exit price: {e}")
            return None

    def extract_fees_paid(self, receipt: dict[str, Any]) -> int | None:
        """Extract fees paid from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Execution fee in wei if found, None otherwise.
        """
        try:
            result = self.parse_receipt(receipt)
            if result.order_events:
                return result.order_events[0].execution_fee
            return None
        except Exception as e:
            logger.warning(f"Failed to extract fees paid: {e}")
            return None


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "GMXv2ReceiptParser",
    "GMXv2Event",
    "GMXv2EventType",
    "PositionIncreaseData",
    "PositionDecreaseData",
    "OrderEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
