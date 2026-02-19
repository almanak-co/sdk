"""Polymarket Receipt Parser.

This module provides parsing functionality for Polymarket CLOB responses
and CTF on-chain transaction receipts.

Polymarket uses a hybrid architecture:
- CLOB orders are matched off-chain (parsed from API responses)
- CTF operations are settled on-chain (parsed from transaction receipts)

CTF Events (ERC-1155):
- TransferSingle: Single token transfer
- TransferBatch: Multiple token transfers
- PayoutRedemption: Position redemption for USDC

CLOB Response Parsing:
- Order submission responses
- Fill notifications
- Order status updates

Example:
    from almanak.framework.connectors.polymarket import PolymarketReceiptParser

    parser = PolymarketReceiptParser()

    # Parse CLOB order response
    result = parser.parse_order_response(api_response)
    if result.success:
        print(f"Order {result.order_id}: {result.status}")
        print(f"Filled: {result.filled_size} @ {result.avg_price}")

    # Parse CTF redemption receipt
    ctf_result = parser.parse_ctf_receipt(tx_receipt)
    if ctf_result.success:
        print(f"Redeemed {ctf_result.amount_redeemed} USDC")
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.base import EventRegistry, HexDecoder

from .models import (
    CONDITIONAL_TOKENS,
    CTF_EXCHANGE,
    NEG_RISK_ADAPTER,
    NEG_RISK_EXCHANGE,
    USDC_POLYGON,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

# ERC-1155 event signatures (keccak256 hashes)
# TransferSingle(address,address,address,uint256,uint256)
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"

# TransferBatch(address,address,address,uint256[],uint256[])
TRANSFER_BATCH_TOPIC = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"

# PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)
# This is the CTF-specific redemption event from Conditional Tokens contract
PAYOUT_REDEMPTION_TOPIC = "0x2682012a4a4f1973119f1c9b90745f714c4c1e002c60c52b89896745d90ab678"

# ERC-20 Transfer event
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ApprovalForAll(address,address,bool) - ERC-1155
APPROVAL_FOR_ALL_TOPIC = "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31"

# Approval(address,address,uint256) - ERC-20
ERC20_APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"


# Event topic to name mapping
EVENT_TOPICS: dict[str, str] = {
    "TransferSingle": TRANSFER_SINGLE_TOPIC,
    "TransferBatch": TRANSFER_BATCH_TOPIC,
    "PayoutRedemption": PAYOUT_REDEMPTION_TOPIC,
    "ERC20Transfer": ERC20_TRANSFER_TOPIC,
    "ApprovalForAll": APPROVAL_FOR_ALL_TOPIC,
    "ERC20Approval": ERC20_APPROVAL_TOPIC,
}

# Reverse lookup: topic -> event name
TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Constants
# =============================================================================

# Polymarket uses 6 decimals for USDC
USDC_DECIMALS = 6
DECIMAL_SCALE = 10**USDC_DECIMALS

# Polymarket contract addresses (Polygon Mainnet)
# Used for filtering receipt logs to only process events from known contracts
POLYMARKET_CONTRACTS: frozenset[str] = frozenset(
    {
        CONDITIONAL_TOKENS.lower(),  # CTF ERC-1155 tokens
        CTF_EXCHANGE.lower(),  # CTF Exchange for trading
        NEG_RISK_EXCHANGE.lower(),  # Neg Risk Exchange
        NEG_RISK_ADAPTER.lower(),  # Neg Risk Adapter
        USDC_POLYGON.lower(),  # USDC on Polygon
    }
)


# =============================================================================
# Enums
# =============================================================================


class PolymarketEventType(Enum):
    """Polymarket/CTF event types."""

    TRANSFER_SINGLE = "TRANSFER_SINGLE"
    TRANSFER_BATCH = "TRANSFER_BATCH"
    PAYOUT_REDEMPTION = "PAYOUT_REDEMPTION"
    ERC20_TRANSFER = "ERC20_TRANSFER"
    APPROVAL_FOR_ALL = "APPROVAL_FOR_ALL"
    ERC20_APPROVAL = "ERC20_APPROVAL"
    UNKNOWN = "UNKNOWN"


# Mapping from event name to type
EVENT_NAME_TO_TYPE: dict[str, PolymarketEventType] = {
    "TransferSingle": PolymarketEventType.TRANSFER_SINGLE,
    "TransferBatch": PolymarketEventType.TRANSFER_BATCH,
    "PayoutRedemption": PolymarketEventType.PAYOUT_REDEMPTION,
    "ERC20Transfer": PolymarketEventType.ERC20_TRANSFER,
    "ApprovalForAll": PolymarketEventType.APPROVAL_FOR_ALL,
    "ERC20Approval": PolymarketEventType.ERC20_APPROVAL,
}


# =============================================================================
# Data Classes - CLOB Response Parsing
# =============================================================================


@dataclass
class TradeResult:
    """Result of parsing a CLOB order response or fill.

    Attributes:
        success: Whether parsing succeeded
        order_id: Order ID from CLOB
        status: Order status (LIVE, MATCHED, CANCELLED, etc.)
        filled_size: Amount filled (in shares)
        avg_price: Average fill price
        fee: Trading fee (in USDC)
        tx_hash: On-chain settlement tx hash (if settled)
        side: Order side (BUY or SELL)
        token_id: CLOB token ID
        timestamp: Order/fill timestamp
        error: Error message if parsing failed
    """

    success: bool
    order_id: str | None = None
    status: str | None = None
    filled_size: Decimal = field(default_factory=lambda: Decimal("0"))
    avg_price: Decimal = field(default_factory=lambda: Decimal("0"))
    fee: Decimal = field(default_factory=lambda: Decimal("0"))
    tx_hash: str | None = None
    side: str | None = None
    token_id: str | None = None
    timestamp: datetime | None = None
    error: str | None = None

    @property
    def is_filled(self) -> bool:
        """Check if order has been (at least partially) filled."""
        return self.filled_size > 0

    @property
    def is_complete(self) -> bool:
        """Check if order is completely filled or cancelled."""
        return self.status in ("MATCHED", "CANCELLED", "EXPIRED")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "order_id": self.order_id,
            "status": self.status,
            "filled_size": str(self.filled_size),
            "avg_price": str(self.avg_price),
            "fee": str(self.fee),
            "tx_hash": self.tx_hash,
            "side": self.side,
            "token_id": self.token_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "error": self.error,
        }


@dataclass
class RedemptionResult:
    """Result of parsing a CTF redemption transaction receipt.

    Attributes:
        success: Whether parsing succeeded
        tx_hash: Transaction hash
        amount_redeemed: Total USDC received from redemption
        condition_id: CTF condition ID
        index_sets: Which index sets were redeemed
        payout_amounts: Amount redeemed per index set
        redeemer: Address that received the payout
        gas_used: Gas used by the transaction
        error: Error message if parsing failed
    """

    success: bool
    tx_hash: str = ""
    amount_redeemed: Decimal = field(default_factory=lambda: Decimal("0"))
    condition_id: str | None = None
    index_sets: list[int] = field(default_factory=list)
    payout_amounts: list[Decimal] = field(default_factory=list)
    redeemer: str | None = None
    gas_used: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "tx_hash": self.tx_hash,
            "amount_redeemed": str(self.amount_redeemed),
            "condition_id": self.condition_id,
            "index_sets": self.index_sets,
            "payout_amounts": [str(p) for p in self.payout_amounts],
            "redeemer": self.redeemer,
            "gas_used": self.gas_used,
            "error": self.error,
        }


# =============================================================================
# Data Classes - CTF Event Parsing
# =============================================================================


@dataclass
class CtfEvent:
    """Parsed CTF/ERC-1155 event.

    Attributes:
        event_type: Type of event
        event_name: Name of event
        log_index: Index of log in transaction
        transaction_hash: Transaction hash
        block_number: Block number
        contract_address: Contract that emitted event
        data: Parsed event data
        raw_topics: Raw event topics
        raw_data: Raw event data
        timestamp: Event timestamp
    """

    event_type: PolymarketEventType
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
class TransferSingleData:
    """Parsed data from TransferSingle event.

    Event: TransferSingle(operator, from, to, id, value)

    Attributes:
        operator: Address that triggered the transfer
        from_addr: Sender address
        to_addr: Recipient address
        token_id: ERC-1155 token ID
        value: Amount transferred
        contract_address: Contract that emitted event
    """

    operator: str
    from_addr: str
    to_addr: str
    token_id: int
    value: int
    contract_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "operator": self.operator,
            "from_addr": self.from_addr,
            "to_addr": self.to_addr,
            "token_id": str(self.token_id),
            "value": str(self.value),
            "contract_address": self.contract_address,
        }


@dataclass
class TransferBatchData:
    """Parsed data from TransferBatch event.

    Event: TransferBatch(operator, from, to, ids[], values[])

    Attributes:
        operator: Address that triggered the transfer
        from_addr: Sender address
        to_addr: Recipient address
        token_ids: List of ERC-1155 token IDs
        values: List of amounts transferred
        contract_address: Contract that emitted event
    """

    operator: str
    from_addr: str
    to_addr: str
    token_ids: list[int]
    values: list[int]
    contract_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "operator": self.operator,
            "from_addr": self.from_addr,
            "to_addr": self.to_addr,
            "token_ids": [str(tid) for tid in self.token_ids],
            "values": [str(v) for v in self.values],
            "contract_address": self.contract_address,
        }


@dataclass
class PayoutRedemptionData:
    """Parsed data from PayoutRedemption event.

    Event: PayoutRedemption(redeemer, collateralToken, parentCollectionId, conditionId, indexSets[], payout)

    Attributes:
        redeemer: Address that received payout
        collateral_token: Collateral token address (USDC)
        parent_collection_id: Parent collection ID (usually 0x0)
        condition_id: CTF condition ID
        index_sets: Which outcomes were redeemed
        payout: Total payout amount
        contract_address: Contract that emitted event
    """

    redeemer: str
    collateral_token: str
    parent_collection_id: str
    condition_id: str
    index_sets: list[int]
    payout: int
    contract_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "redeemer": self.redeemer,
            "collateral_token": self.collateral_token,
            "parent_collection_id": self.parent_collection_id,
            "condition_id": self.condition_id,
            "index_sets": self.index_sets,
            "payout": str(self.payout),
            "contract_address": self.contract_address,
        }

    @property
    def payout_decimal(self) -> Decimal:
        """Get payout as decimal with 6 decimals."""
        return Decimal(self.payout) / Decimal(DECIMAL_SCALE)


@dataclass
class Erc20TransferData:
    """Parsed data from ERC-20 Transfer event.

    Event: Transfer(from, to, value)

    Attributes:
        from_addr: Sender address
        to_addr: Recipient address
        value: Amount transferred
        token_address: Token contract address
    """

    from_addr: str
    to_addr: str
    value: int
    token_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "from_addr": self.from_addr,
            "to_addr": self.to_addr,
            "value": str(self.value),
            "token_address": self.token_address,
        }

    @property
    def value_decimal(self) -> Decimal:
        """Get value as decimal (assumes 6 decimals for USDC)."""
        return Decimal(self.value) / Decimal(DECIMAL_SCALE)


@dataclass
class CtfParseResult:
    """Result of parsing a CTF transaction receipt.

    Attributes:
        success: Whether parsing succeeded
        events: List of all parsed events
        transfer_singles: TransferSingle events
        transfer_batches: TransferBatch events
        redemptions: PayoutRedemption events
        erc20_transfers: ERC-20 Transfer events
        redemption_result: High-level redemption result
        error: Error message if parsing failed
        transaction_hash: Transaction hash
        block_number: Block number
        transaction_success: Whether transaction succeeded
    """

    success: bool
    events: list[CtfEvent] = field(default_factory=list)
    transfer_singles: list[TransferSingleData] = field(default_factory=list)
    transfer_batches: list[TransferBatchData] = field(default_factory=list)
    redemptions: list[PayoutRedemptionData] = field(default_factory=list)
    erc20_transfers: list[Erc20TransferData] = field(default_factory=list)
    redemption_result: RedemptionResult | None = None
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0
    transaction_success: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "transfer_singles": [t.to_dict() for t in self.transfer_singles],
            "transfer_batches": [t.to_dict() for t in self.transfer_batches],
            "redemptions": [r.to_dict() for r in self.redemptions],
            "erc20_transfers": [t.to_dict() for t in self.erc20_transfers],
            "redemption_result": self.redemption_result.to_dict() if self.redemption_result else None,
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "transaction_success": self.transaction_success,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class PolymarketReceiptParser:
    """Parser for Polymarket CLOB responses and CTF transaction receipts.

    This parser handles two types of data:
    1. CLOB API responses - order submissions, fills, status updates
    2. CTF transaction receipts - on-chain token transfers and redemptions

    Example:
        parser = PolymarketReceiptParser()

        # Parse CLOB order submission response
        trade_result = parser.parse_order_response(api_response)
        if trade_result.success:
            print(f"Order {trade_result.order_id}: {trade_result.status}")

        # Parse CTF redemption transaction receipt
        ctf_result = parser.parse_ctf_receipt(tx_receipt)
        if ctf_result.success and ctf_result.redemption_result:
            print(f"Redeemed: {ctf_result.redemption_result.amount_redeemed} USDC")
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "outcome_tokens_received",
            "cost_basis",
            "market_id",
            "outcome_tokens_sold",
            "proceeds",
            "redemption_amount",
            "payout",
        }
    )

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the receipt parser.

        Args:
            **kwargs: Additional arguments (ignored for compatibility)
        """
        _ = kwargs  # Explicitly unused for forward compatibility
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        self._known_topics = set(EVENT_TOPICS.values())

    # =========================================================================
    # CLOB Response Parsing
    # =========================================================================

    def parse_order_response(self, response: dict[str, Any]) -> TradeResult:
        """Parse a CLOB order submission response.

        The response contains order details and initial status after submission.

        Args:
            response: CLOB API order response dict

        Returns:
            TradeResult with order details

        Example response:
            {
                "orderID": "0x1234...",
                "status": "LIVE",
                "owner": "0xYourAddress",
                "market": "19045189...",
                "side": "BUY",
                "price": "0.65",
                "size": "100",
                "filledSize": "0",
                "createdAt": "2025-01-15T10:30:00Z"
            }
        """
        try:
            order_id = response.get("orderID") or response.get("orderId") or response.get("id")
            status = response.get("status") or "UNKNOWN"

            # Handle None values gracefully
            filled_size_raw = response.get("filledSize")
            filled_size = Decimal(str(filled_size_raw)) if filled_size_raw is not None else Decimal("0")

            avg_price_raw = response.get("avgPrice")
            avg_price = Decimal(str(avg_price_raw)) if avg_price_raw is not None else Decimal("0")

            # If no avg_price, use order price
            if avg_price == 0 and response.get("price") is not None:
                avg_price = Decimal(str(response["price"]))

            fee_raw = response.get("fee")
            fee = Decimal(str(fee_raw)) if fee_raw is not None else Decimal("0")
            side = response.get("side", "").upper()
            token_id = response.get("market") or response.get("tokenId")

            # Parse timestamp
            timestamp = None
            created_at = response.get("createdAt")
            if created_at:
                try:
                    timestamp = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    pass

            logger.debug(
                "Parsed order response: order_id=%s, status=%s, filled=%s, price=%s",
                order_id,
                status,
                filled_size,
                avg_price,
            )

            return TradeResult(
                success=True,
                order_id=order_id,
                status=status,
                filled_size=filled_size,
                avg_price=avg_price,
                fee=fee,
                side=side,
                token_id=token_id,
                timestamp=timestamp,
            )

        except Exception as e:
            logger.exception("Failed to parse order response: %s", e)
            return TradeResult(success=False, error=str(e))

    def parse_fill_notification(self, notification: dict[str, Any]) -> TradeResult:
        """Parse a CLOB fill notification.

        Fill notifications are received when an order is matched.

        Args:
            notification: Fill notification from CLOB

        Returns:
            TradeResult with fill details

        Example notification:
            {
                "type": "fill",
                "orderId": "0x1234...",
                "matchId": "0x5678...",
                "fillSize": "50",
                "fillPrice": "0.65",
                "fee": "0",
                "timestamp": "2025-01-15T10:30:00Z"
            }
        """
        try:
            order_id = notification.get("orderId") or notification.get("orderID")
            filled_size = Decimal(str(notification.get("fillSize", "0")))
            fill_price = Decimal(str(notification.get("fillPrice", "0")))
            fee = Decimal(str(notification.get("fee", "0")))
            tx_hash = notification.get("txHash") or notification.get("matchId")

            # Parse timestamp
            timestamp = None
            ts = notification.get("timestamp")
            if ts:
                try:
                    timestamp = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    pass

            logger.debug(
                "Parsed fill notification: order_id=%s, filled=%s @ %s",
                order_id,
                filled_size,
                fill_price,
            )

            return TradeResult(
                success=True,
                order_id=order_id,
                status="MATCHED",
                filled_size=filled_size,
                avg_price=fill_price,
                fee=fee,
                tx_hash=tx_hash,
                timestamp=timestamp,
            )

        except Exception as e:
            logger.exception("Failed to parse fill notification: %s", e)
            return TradeResult(success=False, error=str(e))

    def parse_order_status(self, status_response: dict[str, Any]) -> TradeResult:
        """Parse a CLOB order status query response.

        Args:
            status_response: Order status response from CLOB

        Returns:
            TradeResult with current order status
        """
        return self.parse_order_response(status_response)

    # =========================================================================
    # CTF Receipt Parsing
    # =========================================================================

    def parse_ctf_receipt(
        self,
        receipt: dict[str, Any],
        filter_by_contract: bool = True,
    ) -> CtfParseResult:
        """Parse a CTF transaction receipt for on-chain events.

        Args:
            receipt: Transaction receipt dict with 'logs', 'transactionHash', etc.
            filter_by_contract: If True (default), only parse logs from known
                Polymarket contracts (CONDITIONAL_TOKENS, CTF_EXCHANGE,
                NEG_RISK_EXCHANGE, NEG_RISK_ADAPTER, USDC_POLYGON). Set to False
                to parse all matching event signatures regardless of contract
                address (useful for testing or analyzing multi-protocol transactions).

        Returns:
            CtfParseResult with extracted events and redemption data
        """
        try:
            tx_hash = receipt.get("transactionHash", "")
            if isinstance(tx_hash, bytes):
                tx_hash = "0x" + tx_hash.hex()

            block_number = receipt.get("blockNumber", 0)
            logs = receipt.get("logs", [])
            status = receipt.get("status", 1)
            tx_success = status == 1
            gas_used = receipt.get("gasUsed", 0)

            # Handle failed transactions
            if not tx_success:
                return CtfParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=False,
                    error="Transaction reverted",
                )

            if not logs:
                return CtfParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=tx_success,
                )

            events: list[CtfEvent] = []
            transfer_singles: list[TransferSingleData] = []
            transfer_batches: list[TransferBatchData] = []
            redemptions: list[PayoutRedemptionData] = []
            erc20_transfers: list[Erc20TransferData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number, filter_by_contract)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data based on event type
                    if parsed_event.event_type == PolymarketEventType.TRANSFER_SINGLE:
                        transfer_data = self._parse_transfer_single(parsed_event)
                        if transfer_data:
                            transfer_singles.append(transfer_data)

                    elif parsed_event.event_type == PolymarketEventType.TRANSFER_BATCH:
                        batch_data = self._parse_transfer_batch(parsed_event)
                        if batch_data:
                            transfer_batches.append(batch_data)

                    elif parsed_event.event_type == PolymarketEventType.PAYOUT_REDEMPTION:
                        redemption_data = self._parse_payout_redemption(parsed_event)
                        if redemption_data:
                            redemptions.append(redemption_data)

                    elif parsed_event.event_type == PolymarketEventType.ERC20_TRANSFER:
                        erc20_data = self._parse_erc20_transfer(parsed_event)
                        if erc20_data:
                            erc20_transfers.append(erc20_data)

            # Build high-level redemption result if we have redemption events
            redemption_result = None
            if redemptions:
                redemption_result = self._build_redemption_result(
                    redemptions,
                    erc20_transfers,
                    tx_hash,
                    gas_used,
                )

            logger.info(
                "Parsed CTF receipt: tx=%s, events=%d, redemptions=%d, transfers=%d",
                tx_hash[:10] + "..." if tx_hash else "N/A",
                len(events),
                len(redemptions),
                len(transfer_singles) + len(transfer_batches),
            )

            return CtfParseResult(
                success=True,
                events=events,
                transfer_singles=transfer_singles,
                transfer_batches=transfer_batches,
                redemptions=redemptions,
                erc20_transfers=erc20_transfers,
                redemption_result=redemption_result,
                transaction_hash=tx_hash,
                block_number=block_number,
                transaction_success=tx_success,
            )

        except Exception as e:
            logger.exception("Failed to parse CTF receipt: %s", e)
            return CtfParseResult(success=False, error=str(e))

    def _parse_log(
        self,
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
        filter_by_contract: bool = True,
    ) -> CtfEvent | None:
        """Parse a single log entry.

        Args:
            log: Log dict with 'topics', 'data', 'address', etc.
            tx_hash: Transaction hash
            block_number: Block number
            filter_by_contract: If True, skip logs from non-Polymarket contracts.
                Set to False to parse all matching event signatures regardless
                of contract address (useful for testing).

        Returns:
            Parsed event or None if not a known event or filtered out
        """
        try:
            # Get contract address first for filtering
            contract_address = log.get("address", "")
            if isinstance(contract_address, bytes):
                contract_address = "0x" + contract_address.hex()

            # Filter by contract address if enabled
            if filter_by_contract and contract_address:
                if contract_address.lower() not in POLYMARKET_CONTRACTS:
                    return None

            topics = log.get("topics", [])
            if not topics:
                return None

            # Get event signature (first topic)
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()

            # Look up event name
            event_name = TOPIC_TO_EVENT.get(first_topic)
            if event_name is None:
                return None

            event_type = EVENT_NAME_TO_TYPE.get(event_name, PolymarketEventType.UNKNOWN)

            # Get raw data
            data = log.get("data", "")
            if isinstance(data, bytes):
                data = "0x" + data.hex()

            # Get contract address
            contract_address = log.get("address", "")
            if isinstance(contract_address, bytes):
                contract_address = "0x" + contract_address.hex()

            # Parse log data
            parsed_data = self._decode_log_data(event_name, topics, data, contract_address)

            # Convert topics to strings
            topics_str = []
            for topic in topics:
                if isinstance(topic, bytes):
                    topics_str.append("0x" + topic.hex())
                else:
                    topics_str.append(str(topic))

            return CtfEvent(
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
            logger.warning("Failed to parse log: %s", e)
            return None

    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode log data based on event type.

        Args:
            event_name: Name of the event
            topics: List of topics
            data: Hex-encoded event data
            address: Contract address

        Returns:
            Decoded event data dict
        """
        # Remove 0x prefix if present
        if data.startswith("0x"):
            data = data[2:]

        if event_name == "TransferSingle":
            return self._decode_transfer_single_data(topics, data, address)
        elif event_name == "TransferBatch":
            return self._decode_transfer_batch_data(topics, data, address)
        elif event_name == "PayoutRedemption":
            return self._decode_payout_redemption_data(topics, data, address)
        elif event_name == "ERC20Transfer":
            return self._decode_erc20_transfer_data(topics, data, address)
        else:
            return {"raw_data": data}

    def _decode_transfer_single_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode TransferSingle event data.

        Event: TransferSingle(operator, from, to, id, value)
        - topic0: event signature
        - topic1: operator (indexed)
        - topic2: from (indexed)
        - topic3: to (indexed)
        - data: id (uint256), value (uint256)
        """
        try:
            operator = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            from_addr = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            to_addr = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

            # Parse data: id (32 bytes), value (32 bytes)
            token_id = HexDecoder.decode_uint256(data, 0)
            value = HexDecoder.decode_uint256(data, 32)

            return {
                "operator": operator,
                "from_addr": from_addr,
                "to_addr": to_addr,
                "token_id": token_id,
                "value": value,
                "contract_address": address.lower() if address else "",
            }

        except Exception as e:
            logger.warning("Failed to decode TransferSingle data: %s", e)
            return {"raw_data": data}

    def _decode_transfer_batch_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode TransferBatch event data.

        Event: TransferBatch(operator, from, to, ids[], values[])
        - topic0: event signature
        - topic1: operator (indexed)
        - topic2: from (indexed)
        - topic3: to (indexed)
        - data: offset to ids, offset to values, ids length, ids..., values length, values...
        """
        try:
            operator = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            from_addr = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            to_addr = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

            # Use HexDecoder to decode dynamic arrays
            token_ids = HexDecoder.decode_dynamic_array(data, offset=0)
            values = HexDecoder.decode_dynamic_array(data, offset=32)

            return {
                "operator": operator,
                "from_addr": from_addr,
                "to_addr": to_addr,
                "token_ids": token_ids,
                "values": values,
                "contract_address": address.lower() if address else "",
            }

        except Exception as e:
            logger.warning("Failed to decode TransferBatch data: %s", e)
            return {"raw_data": data, "token_ids": [], "values": []}

    def _decode_payout_redemption_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode PayoutRedemption event data.

        Event: PayoutRedemption(redeemer, collateralToken, parentCollectionId, conditionId, indexSets[], payout)
        - topic0: event signature
        - topic1: redeemer (indexed)
        - topic2: collateralToken (indexed)
        - topic3: conditionId (indexed)
        - data: parentCollectionId (bytes32), offset to indexSets, payout, indexSets...
        """
        try:
            redeemer = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            collateral_token = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            condition_id = HexDecoder.topic_to_bytes32(topics[3]) if len(topics) > 3 else ""

            # parentCollectionId (bytes32)
            parent_collection_id = HexDecoder.topic_to_bytes32("0x" + data[0:64] if len(data) >= 64 else "0x")

            # payout (uint256) - note: this comes AFTER the offset in the encoding
            payout = HexDecoder.decode_uint256(data, 64)

            # Parse indexSets dynamic array - the offset points to where the array starts
            index_sets = HexDecoder.decode_dynamic_array(data, offset=32)

            return {
                "redeemer": redeemer,
                "collateral_token": collateral_token,
                "parent_collection_id": parent_collection_id,
                "condition_id": condition_id,
                "index_sets": index_sets,
                "payout": payout,
                "contract_address": address.lower() if address else "",
            }

        except Exception as e:
            logger.warning("Failed to decode PayoutRedemption data: %s", e)
            return {"raw_data": data, "index_sets": [], "payout": 0}

    def _decode_erc20_transfer_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode ERC-20 Transfer event data.

        Event: Transfer(from, to, value)
        - topic0: event signature
        - topic1: from (indexed)
        - topic2: to (indexed)
        - data: value (uint256)
        """
        try:
            from_addr = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            to_addr = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            value = HexDecoder.decode_uint256(data, 0)

            return {
                "from_addr": from_addr,
                "to_addr": to_addr,
                "value": value,
                "token_address": address.lower() if address else "",
            }

        except Exception as e:
            logger.warning("Failed to decode ERC20 Transfer data: %s", e)
            return {"raw_data": data}

    # =========================================================================
    # Event Data Extraction
    # =========================================================================

    def _parse_transfer_single(self, event: CtfEvent) -> TransferSingleData | None:
        """Parse TransferSingle event into typed data."""
        try:
            data = event.data
            return TransferSingleData(
                operator=data.get("operator", ""),
                from_addr=data.get("from_addr", ""),
                to_addr=data.get("to_addr", ""),
                token_id=data.get("token_id", 0),
                value=data.get("value", 0),
                contract_address=data.get("contract_address", event.contract_address),
            )
        except Exception as e:
            logger.warning("Failed to parse TransferSingleData: %s", e)
            return None

    def _parse_transfer_batch(self, event: CtfEvent) -> TransferBatchData | None:
        """Parse TransferBatch event into typed data."""
        try:
            data = event.data
            return TransferBatchData(
                operator=data.get("operator", ""),
                from_addr=data.get("from_addr", ""),
                to_addr=data.get("to_addr", ""),
                token_ids=data.get("token_ids", []),
                values=data.get("values", []),
                contract_address=data.get("contract_address", event.contract_address),
            )
        except Exception as e:
            logger.warning("Failed to parse TransferBatchData: %s", e)
            return None

    def _parse_payout_redemption(self, event: CtfEvent) -> PayoutRedemptionData | None:
        """Parse PayoutRedemption event into typed data."""
        try:
            data = event.data
            return PayoutRedemptionData(
                redeemer=data.get("redeemer", ""),
                collateral_token=data.get("collateral_token", ""),
                parent_collection_id=data.get("parent_collection_id", ""),
                condition_id=data.get("condition_id", ""),
                index_sets=data.get("index_sets", []),
                payout=data.get("payout", 0),
                contract_address=data.get("contract_address", event.contract_address),
            )
        except Exception as e:
            logger.warning("Failed to parse PayoutRedemptionData: %s", e)
            return None

    def _parse_erc20_transfer(self, event: CtfEvent) -> Erc20TransferData | None:
        """Parse ERC-20 Transfer event into typed data."""
        try:
            data = event.data
            return Erc20TransferData(
                from_addr=data.get("from_addr", ""),
                to_addr=data.get("to_addr", ""),
                value=data.get("value", 0),
                token_address=data.get("token_address", event.contract_address),
            )
        except Exception as e:
            logger.warning("Failed to parse Erc20TransferData: %s", e)
            return None

    # =========================================================================
    # High-Level Result Building
    # =========================================================================

    def _build_redemption_result(
        self,
        redemptions: list[PayoutRedemptionData],
        erc20_transfers: list[Erc20TransferData],
        tx_hash: str,
        gas_used: int,
    ) -> RedemptionResult:
        """Build high-level redemption result from events.

        Args:
            redemptions: PayoutRedemption events
            erc20_transfers: ERC-20 Transfer events (for USDC payout)
            tx_hash: Transaction hash
            gas_used: Gas used by transaction

        Returns:
            RedemptionResult with aggregated data
        """
        if not redemptions:
            return RedemptionResult(success=False, error="No redemption events found")

        # Use first redemption (typically only one per tx)
        redemption = redemptions[0]

        # Sum all payouts if multiple redemptions
        total_payout = sum(r.payout for r in redemptions)
        amount_redeemed = Decimal(total_payout) / Decimal(DECIMAL_SCALE)

        # Collect all index sets and payout amounts
        all_index_sets = []
        payout_amounts = []
        for r in redemptions:
            all_index_sets.extend(r.index_sets)
            payout_amounts.append(r.payout_decimal)

        return RedemptionResult(
            success=True,
            tx_hash=tx_hash,
            amount_redeemed=amount_redeemed,
            condition_id=redemption.condition_id,
            index_sets=all_index_sets,
            payout_amounts=payout_amounts,
            redeemer=redemption.redeemer,
            gas_used=gas_used,
        )

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def is_polymarket_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Polymarket/CTF event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known event
        """
        # Normalize topic
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return topic in self._known_topics

    def is_polymarket_contract(self, address: str) -> bool:
        """Check if an address is a known Polymarket contract.

        Args:
            address: Contract address

        Returns:
            True if address is a known Polymarket contract
        """
        return address.lower() in POLYMARKET_CONTRACTS

    def get_event_type(self, topic: str | bytes) -> PolymarketEventType:
        """Get the event type for a topic.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            Event type or UNKNOWN
        """
        # Normalize topic
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()

        event_name = TOPIC_TO_EVENT.get(topic)
        if event_name:
            return EVENT_NAME_TO_TYPE.get(event_name, PolymarketEventType.UNKNOWN)
        return PolymarketEventType.UNKNOWN

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    def extract_outcome_tokens_received(self, receipt: dict[str, Any]) -> int | None:
        """Extract outcome tokens received from transaction receipt.

        Note: Without the user's address, we cannot determine transfer direction.
        This method sums all TransferSingle values as a proxy for tokens involved.
        For precise directional filtering, pass the user address to your own logic.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Total outcome tokens from all TransferSingle events if found, None otherwise
        """
        try:
            result = self.parse_ctf_receipt(receipt)
            if not result.transfer_singles:
                return None

            # Sum all token values from TransferSingle events
            # Note: Without user address context, we sum all transfer values
            total_received = sum(t.value for t in result.transfer_singles if t.value > 0)

            return total_received if total_received > 0 else None
        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract outcome tokens received: {e}")
            return None

    def extract_cost_basis(self, receipt: dict[str, Any]) -> int | None:
        """Extract cost basis (USDC involved) from transaction receipt.

        Note: Without the user's address, we cannot determine transfer direction.
        This method sums all USDC transfer values as a proxy for the transaction size.
        For precise directional filtering, pass the user address to your own logic.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Total USDC in base units (6 decimals) from transfers if found, None otherwise
        """
        try:
            result = self.parse_ctf_receipt(receipt)
            if not result.erc20_transfers:
                return None

            # Sum all USDC transfers (without user address, direction is unknown)
            total_spent = sum(
                transfer.value
                for transfer in result.erc20_transfers
                if transfer.token_address.lower() == USDC_POLYGON.lower()
            )

            return total_spent if total_spent > 0 else None
        except Exception as e:
            logger.warning(f"Failed to extract cost basis: {e}")
            return None

    def extract_market_id(self, receipt: dict[str, Any]) -> str | None:
        """Extract market ID (condition ID) from transaction receipt.

        For redemptions, extracts the condition ID directly from PayoutRedemption event.

        For trades, returns the raw token ID as hex. Note: CTF token IDs use a complex
        encoding where conditionId = keccak256(oracle, questionId, outcomeSlotCount) and
        tokenId = positionId derived from collateral + conditionId + indexSet. Decoding
        the actual conditionId from a tokenId requires reverse lookup via CTF contract.
        For simplified use cases, the raw tokenId serves as a unique market identifier.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Condition ID hex string for redemptions, raw token ID hex for trades,
            or None if not found.
        """
        try:
            result = self.parse_ctf_receipt(receipt)

            # Check redemption events first (direct condition ID)
            if result.redemptions:
                return result.redemptions[0].condition_id

            # For trades, would need to decode from token ID
            # Token ID encodes condition ID and outcome index
            # This is a simplified implementation
            if result.transfer_singles:
                # Token ID is a bytes32 that encodes market info
                token_id = result.transfer_singles[0].token_id
                # Return as hex string (full token ID contains market info)
                return hex(token_id)

            return None
        except Exception as e:
            logger.warning(f"Failed to extract market ID: {e}")
            return None

    def extract_outcome_tokens_sold(self, receipt: dict[str, Any]) -> int | None:
        """Extract outcome tokens sold from transaction receipt.

        Note: Polymarket sells are transfers to the exchange, not burns.
        Without the exchange contract address, we cannot reliably distinguish
        sells from other transfers. This method sums all TransferSingle values
        as a proxy for tokens involved in the transaction.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Total outcome tokens from all TransferSingle events if found, None otherwise
        """
        try:
            result = self.parse_ctf_receipt(receipt)
            if not result.transfer_singles:
                return None

            # Sum all token transfers (without exchange address, can't filter direction)
            total_sold = sum(t.value for t in result.transfer_singles if t.value > 0)

            return total_sold if total_sold > 0 else None
        except Exception as e:
            logger.warning(f"Failed to extract outcome tokens sold: {e}")
            return None

    def extract_proceeds(self, receipt: dict[str, Any]) -> int | None:
        """Extract proceeds (USDC involved) from transaction receipt.

        Note: Without the user's address, we cannot determine transfer direction.
        This method sums all USDC transfer values as a proxy for the transaction size.
        For precise directional filtering, pass the user address to your own logic.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Total USDC in base units (6 decimals) from transfers if found, None otherwise
        """
        try:
            result = self.parse_ctf_receipt(receipt)
            if not result.erc20_transfers:
                return None

            # Sum all USDC transfers (without user address, direction is unknown)
            total_received = sum(
                transfer.value
                for transfer in result.erc20_transfers
                if transfer.token_address.lower() == USDC_POLYGON.lower()
            )

            return total_received if total_received > 0 else None
        except Exception as e:
            logger.warning(f"Failed to extract proceeds: {e}")
            return None

    def extract_redemption_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract redemption amount (outcome tokens redeemed) from receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Total tokens redeemed if found, None otherwise
        """
        try:
            result = self.parse_ctf_receipt(receipt)
            if not result.transfer_singles:
                return None

            # Find transfers to zero address (burning tokens for redemption)
            zero_addr = "0x" + "0" * 40
            total_redeemed = 0
            for transfer in result.transfer_singles:
                if transfer.to_addr.lower() == zero_addr:
                    total_redeemed += transfer.value

            return total_redeemed if total_redeemed > 0 else None
        except Exception as e:
            logger.warning(f"Failed to extract redemption amount: {e}")
            return None

    def extract_payout(self, receipt: dict[str, Any]) -> int | None:
        """Extract payout amount from redemption transaction.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Payout amount in USDC base units if found, None otherwise
        """
        try:
            result = self.parse_ctf_receipt(receipt)
            if not result.redemptions:
                return None

            # Sum all payouts
            total_payout = sum(r.payout for r in result.redemptions)
            return total_payout if total_payout > 0 else None
        except Exception as e:
            logger.warning(f"Failed to extract payout: {e}")
            return None


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Parser
    "PolymarketReceiptParser",
    # CLOB Results
    "TradeResult",
    # CTF Results
    "RedemptionResult",
    "CtfParseResult",
    # Event Types
    "PolymarketEventType",
    "CtfEvent",
    # Event Data
    "TransferSingleData",
    "TransferBatchData",
    "PayoutRedemptionData",
    "Erc20TransferData",
    # Constants
    "POLYMARKET_CONTRACTS",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "TRANSFER_SINGLE_TOPIC",
    "TRANSFER_BATCH_TOPIC",
    "PAYOUT_REDEMPTION_TOPIC",
    "ERC20_TRANSFER_TOPIC",
    "USDC_DECIMALS",
]
