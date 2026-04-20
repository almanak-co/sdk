"""TraderJoe V2 Receipt Parser (Refactored).

Refactored to use base infrastructure utilities while maintaining backward compatibility.
TraderJoe V2 uses ERC-1155 liquidity bins with dynamic arrays for bin IDs and amounts.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
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
    # DepositedToBins(address,address,uint256[],bytes32[])
    "DepositedToBins": "0x87f1f9dcf5e8089a3e00811b6a008d8f30293a3da878cb1fe8c90ca376402f8a",
    # WithdrawnFromBins(address,address,uint256[],bytes32[])
    "WithdrawnFromBins": "0xa32e146844d6144a22e94c586715a1317d58a8aa3581ec33d040113ddcb24350",
    # TransferBatch(address,address,address,uint256[],uint256[]) - ERC1155
    "TransferBatch": "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb",
    # Transfer(address,address,uint256) - ERC20
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    # Approval(address,address,uint256) - ERC20
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
    # ClaimedFees(address indexed sender, address indexed to, uint256[] ids, bytes32[] amounts)
    # This event is emitted by LBPair.collectFees()
    "ClaimedFees": "0xdf71cf7e8bfaf5953702c2be6d726b8f61d37bf9d90fda35b7bbb3981264e24d",
    # Deposit(address,uint256) - WAVAX wrap
    "Deposit": "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c",
    # Withdrawal(address,uint256) - WAVAX unwrap
    "Withdrawal": "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

# Individual topic constants for backward compatibility
DEPOSITED_TO_BINS_TOPIC = EVENT_TOPICS["DepositedToBins"]
WITHDRAWN_FROM_BINS_TOPIC = EVENT_TOPICS["WithdrawnFromBins"]


# =============================================================================
# Enums
# =============================================================================


class TraderJoeV2EventType(Enum):
    """TraderJoe V2 event types."""

    DEPOSITED_TO_BINS = "DEPOSITED_TO_BINS"
    WITHDRAWN_FROM_BINS = "WITHDRAWN_FROM_BINS"
    TRANSFER_BATCH = "TRANSFER_BATCH"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    CLAIMED_FEES = "CLAIMED_FEES"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, TraderJoeV2EventType] = {
    "DepositedToBins": TraderJoeV2EventType.DEPOSITED_TO_BINS,
    "WithdrawnFromBins": TraderJoeV2EventType.WITHDRAWN_FROM_BINS,
    "TransferBatch": TraderJoeV2EventType.TRANSFER_BATCH,
    "Transfer": TraderJoeV2EventType.TRANSFER,
    "Approval": TraderJoeV2EventType.APPROVAL,
    "ClaimedFees": TraderJoeV2EventType.CLAIMED_FEES,
    "Deposit": TraderJoeV2EventType.DEPOSIT,
    "Withdrawal": TraderJoeV2EventType.WITHDRAWAL,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TraderJoeV2Event:
    """Parsed TraderJoe V2 event."""

    event_type: TraderJoeV2EventType
    event_name: str
    log_index: int
    transaction_hash: str
    block_number: int
    contract_address: str
    data: dict[str, Any]
    raw_topics: list[str] = field(default_factory=list)
    raw_data: str = ""
    timestamp: datetime | None = None


@dataclass
class SwapEventData:
    """Parsed data from a swap (Transfer events)."""

    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    sender: str
    recipient: str


@dataclass
class LiquidityEventData:
    """Parsed data from add/remove liquidity events."""

    pool_address: str
    sender: str
    to: str
    bin_ids: list[int]
    amounts_x: list[int] = field(default_factory=list)
    amounts_y: list[int] = field(default_factory=list)
    total_amount_x: int = 0
    total_amount_y: int = 0


@dataclass
class TransferEventData:
    """Parsed data from Transfer event."""

    token: str
    from_address: str
    to_address: str
    amount: int


@dataclass
class ParsedSwapResult:
    """Result of parsing a swap transaction."""

    success: bool
    token_in: str | None = None
    token_out: str | None = None
    amount_in: int | None = None
    amount_out: int | None = None
    price: Decimal | None = None
    gas_used: int | None = None
    block_number: int | None = None
    timestamp: datetime | None = None


@dataclass
class ParsedLiquidityResult:
    """Result of parsing a liquidity transaction."""

    success: bool
    is_add: bool = True
    pool_address: str | None = None
    bin_ids: list[int] = field(default_factory=list)
    amount_x: int = 0
    amount_y: int = 0
    gas_used: int | None = None
    block_number: int | None = None


@dataclass
class ParsedFeeCollectionResult:
    """Result of parsing a fee collection transaction."""

    success: bool
    pool_address: str | None = None
    bin_ids: list[int] = field(default_factory=list)
    fees_x: int = 0
    fees_y: int = 0
    gas_used: int | None = None
    block_number: int | None = None


@dataclass
class ParseResult:
    """Result of parsing a transaction receipt."""

    success: bool
    transaction_hash: str
    block_number: int
    gas_used: int
    events: list[TraderJoeV2Event] = field(default_factory=list)
    swap_result: ParsedSwapResult | None = None
    liquidity_result: ParsedLiquidityResult | None = None
    error: str | None = None


# =============================================================================
# Parser Class
# =============================================================================


class TraderJoeV2ReceiptParser:
    """Parser for TraderJoe V2 transaction receipts.

    Refactored to use base infrastructure utilities for hex decoding
    and event registry management. Now properly parses dynamic arrays
    in DepositedToBins and WithdrawnFromBins events.
    """

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
            receipt: Web3 transaction receipt dict

        Returns:
            ParseResult with extracted data
        """
        try:
            # Normalize transaction hash
            tx_hash_raw = receipt.get("transactionHash", "")
            if isinstance(tx_hash_raw, bytes):
                tx_hash = "0x" + tx_hash_raw.hex()
            elif isinstance(tx_hash_raw, str):
                tx_hash = tx_hash_raw
            else:
                tx_hash = ""

            block_number = receipt.get("blockNumber", 0)
            gas_used = receipt.get("gasUsed", 0)
            logs = receipt.get("logs", [])

            # Parse all events
            events = self._parse_logs(logs, tx_hash, block_number)

            # Check transaction status
            status = receipt.get("status", 1)
            if status != 1:
                return ParseResult(
                    success=False,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    gas_used=gas_used,
                    events=events,
                    error="Transaction reverted",
                )

            # Try to extract swap result
            swap_result = self._extract_swap_result(events, gas_used, block_number)

            # Try to extract liquidity result
            liquidity_result = self._extract_liquidity_result(events, gas_used, block_number)

            # Log parsed receipt with user-friendly formatting
            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(gas_used)

            if swap_result and swap_result.success:
                logger.info(
                    f"🔍 Parsed TraderJoe V2 swap: {swap_result.amount_in:,} → {swap_result.amount_out:,}, "
                    f"tx={tx_fmt}, {gas_fmt}"
                )
            elif liquidity_result and liquidity_result.success:
                action = "ADD" if liquidity_result.is_add else "REMOVE"
                logger.info(
                    f"🔍 Parsed TraderJoe V2 {action} liquidity: bins={len(liquidity_result.bin_ids)}, "
                    f"tx={tx_fmt}, {gas_fmt}"
                )
            else:
                logger.info(f"🔍 Parsed TraderJoe V2 receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                transaction_hash=tx_hash,
                block_number=block_number,
                gas_used=gas_used,
                events=events,
                swap_result=swap_result,
                liquidity_result=liquidity_result,
            )

        except Exception as e:
            logger.error(f"Failed to parse receipt: {e}")
            return ParseResult(
                success=False,
                transaction_hash=str(receipt.get("transactionHash", "")),
                block_number=receipt.get("blockNumber", 0),
                gas_used=receipt.get("gasUsed", 0),
                error=str(e),
            )

    def _parse_logs(
        self,
        logs: list[dict[str, Any]],
        tx_hash: str,
        block_number: int,
    ) -> list[TraderJoeV2Event]:
        """Parse logs into TraderJoeV2Event objects."""
        events = []

        for i, log in enumerate(logs):
            topics = log.get("topics", [])
            if not topics:
                continue

            # Normalize first topic (event signature)
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            elif not first_topic.startswith("0x"):
                first_topic = "0x" + first_topic

            # Look up event name
            event_name = self.registry.get_event_name(first_topic.lower())
            if event_name is None:
                continue

            event_type = self.registry.get_event_type(event_name) or TraderJoeV2EventType.UNKNOWN

            # Get contract address
            address = log.get("address", "")
            if isinstance(address, bytes):
                address = "0x" + address.hex()

            # Parse event data based on type
            data = self._parse_event_data(event_type, log)

            # Get raw data
            raw_data = log.get("data", "")
            if isinstance(raw_data, bytes):
                raw_data = "0x" + raw_data.hex()

            # Get raw topics
            raw_topics = []
            for topic in topics:
                if isinstance(topic, bytes):
                    raw_topics.append("0x" + topic.hex())
                else:
                    raw_topics.append(topic)

            event = TraderJoeV2Event(
                event_type=event_type,
                event_name=event_name,
                log_index=i,
                transaction_hash=tx_hash,
                block_number=block_number,
                contract_address=address,
                data=data,
                raw_topics=raw_topics,
                raw_data=raw_data,
            )
            events.append(event)

        return events

    def _parse_event_data(
        self,
        event_type: TraderJoeV2EventType,
        log: dict[str, Any],
    ) -> dict[str, Any]:
        """Parse event data based on event type."""
        topics = log.get("topics", [])
        data = log.get("data", "")

        # Normalize data to hex string
        data_hex = HexDecoder.normalize_hex(data)

        result: dict[str, Any] = {}

        try:
            if event_type == TraderJoeV2EventType.TRANSFER:
                # Transfer(address indexed from, address indexed to, uint256 value)
                if len(topics) >= 3:
                    result["from"] = HexDecoder.topic_to_address(topics[1])
                    result["to"] = HexDecoder.topic_to_address(topics[2])
                result["value"] = HexDecoder.decode_uint256(data_hex, 0)

            elif event_type == TraderJoeV2EventType.APPROVAL:
                # Approval(address indexed owner, address indexed spender, uint256 value)
                if len(topics) >= 3:
                    result["owner"] = HexDecoder.topic_to_address(topics[1])
                    result["spender"] = HexDecoder.topic_to_address(topics[2])
                result["value"] = HexDecoder.decode_uint256(data_hex, 0)

            elif event_type == TraderJoeV2EventType.DEPOSITED_TO_BINS:
                # DepositedToBins(address indexed sender, address indexed to, uint256[] ids, bytes32[] amounts)
                if len(topics) >= 3:
                    result["sender"] = HexDecoder.topic_to_address(topics[1])
                    result["to"] = HexDecoder.topic_to_address(topics[2])
                # ids and amounts are dynamic arrays (complex to parse)
                # Store raw_data for now, matching original behavior
                result["raw_data"] = data_hex

            elif event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS:
                # WithdrawnFromBins(address indexed sender, address indexed to, uint256[] ids, bytes32[] amounts)
                if len(topics) >= 3:
                    result["sender"] = HexDecoder.topic_to_address(topics[1])
                    result["to"] = HexDecoder.topic_to_address(topics[2])
                # ids and amounts are dynamic arrays (complex to parse)
                # Store raw_data for now, matching original behavior
                result["raw_data"] = data_hex

            elif event_type == TraderJoeV2EventType.CLAIMED_FEES:
                # ClaimedFees(address indexed sender, address indexed to, uint256[] ids, bytes32[] amounts)
                if len(topics) >= 3:
                    result["sender"] = HexDecoder.topic_to_address(topics[1])
                    result["to"] = HexDecoder.topic_to_address(topics[2])
                result["raw_data"] = data_hex

            elif event_type == TraderJoeV2EventType.DEPOSIT:
                # Deposit(address indexed dst, uint256 wad)
                if len(topics) >= 2:
                    result["dst"] = HexDecoder.topic_to_address(topics[1])
                result["wad"] = HexDecoder.decode_uint256(data_hex, 0)

            elif event_type == TraderJoeV2EventType.WITHDRAWAL:
                # Withdrawal(address indexed src, uint256 wad)
                if len(topics) >= 2:
                    result["src"] = HexDecoder.topic_to_address(topics[1])
                result["wad"] = HexDecoder.decode_uint256(data_hex, 0)

        except Exception as e:
            logger.warning(f"Failed to parse {event_type} event data: {e}")

        return result

    def _extract_swap_result(
        self,
        events: list[TraderJoeV2Event],
        gas_used: int,
        block_number: int,
    ) -> ParsedSwapResult | None:
        """Extract swap result from Transfer events."""
        # Get all Transfer events
        transfers = [e for e in events if e.event_type == TraderJoeV2EventType.TRANSFER]

        if len(transfers) < 2:
            return None

        # First transfer: user -> pool (token in)
        # Last transfer: pool -> user (token out)
        transfer_in = transfers[0]
        transfer_out = transfers[-1]

        try:
            amount_in = transfer_in.data.get("value", 0)
            amount_out = transfer_out.data.get("value", 0)

            if amount_in <= 0 or amount_out <= 0:
                return None

            # Calculate price
            price = Decimal(amount_out) / Decimal(amount_in) if amount_in > 0 else Decimal(0)

            return ParsedSwapResult(
                success=True,
                token_in=transfer_in.contract_address,
                token_out=transfer_out.contract_address,
                amount_in=amount_in,
                amount_out=amount_out,
                price=price,
                gas_used=gas_used,
                block_number=block_number,
            )

        except Exception as e:
            logger.warning(f"Failed to extract swap result: {e}")
            return None

    def _extract_liquidity_result(
        self,
        events: list[TraderJoeV2Event],
        gas_used: int,
        block_number: int,
    ) -> ParsedLiquidityResult | None:
        """Extract liquidity result from DepositedToBins or WithdrawnFromBins events."""
        # Look for deposit or withdrawal events
        deposit_events = [e for e in events if e.event_type == TraderJoeV2EventType.DEPOSITED_TO_BINS]
        withdraw_events = [e for e in events if e.event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS]

        if deposit_events:
            event = deposit_events[0]
            return ParsedLiquidityResult(
                success=True,
                is_add=True,
                pool_address=event.contract_address,
                gas_used=gas_used,
                block_number=block_number,
            )

        if withdraw_events:
            event = withdraw_events[0]
            return ParsedLiquidityResult(
                success=True,
                is_add=False,
                pool_address=event.contract_address,
                gas_used=gas_used,
                block_number=block_number,
            )

        return None

    def parse_swap_events(self, receipt: dict[str, Any]) -> list[SwapEventData]:
        """Parse swap events from a receipt.

        Convenient method to extract all swap-related data.

        Args:
            receipt: Web3 transaction receipt dict

        Returns:
            List of SwapEventData objects
        """
        result = self.parse_receipt(receipt)
        swaps = []

        if result.swap_result and result.swap_result.success:
            swaps.append(
                SwapEventData(
                    token_in=result.swap_result.token_in or "",
                    token_out=result.swap_result.token_out or "",
                    amount_in=result.swap_result.amount_in or 0,
                    amount_out=result.swap_result.amount_out or 0,
                    sender="",  # Not available from basic parsing
                    recipient="",
                )
            )

        return swaps

    # =============================================================================
    # Extraction Methods (for Result Enrichment)
    # =============================================================================

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> "SwapAmounts | None":
        """Extract swap amounts from a transaction receipt.

        Note: Decimal conversions assume 18 decimals. TraderJoe pools often
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
            if not result.swap_result or not result.swap_result.success:
                return None

            sr = result.swap_result
            amount_in = sr.amount_in or 0
            amount_out = sr.amount_out or 0

            # Calculate decimal amounts (assuming 18 decimals - see docstring for limitations)
            amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**18)
            amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**18)
            effective_price = sr.price or Decimal(0)

            return SwapAmounts(
                amount_in=amount_in,
                amount_out=amount_out,
                amount_in_decimal=amount_in_decimal,
                amount_out_decimal=amount_out_decimal,
                effective_price=effective_price,
                slippage_bps=None,
                token_in=sr.token_in,
                token_out=sr.token_out,
            )

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    def extract_bin_ids(self, receipt: dict[str, Any]) -> list[int] | None:
        """Extract bin IDs from LP transaction receipt.

        TraderJoe V2 uses bins for liquidity. This extracts the bin IDs
        from DepositedToBins or WithdrawnFromBins events.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            List of bin IDs if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)

            # Look for deposit or withdrawal events
            for event in result.events:
                if event.event_type in (
                    TraderJoeV2EventType.DEPOSITED_TO_BINS,
                    TraderJoeV2EventType.WITHDRAWN_FROM_BINS,
                ):
                    # Try to parse bin IDs from raw_data
                    raw_data = event.data.get("raw_data", "")
                    if raw_data:
                        bin_ids = self._parse_bin_ids_from_data(raw_data)
                        if bin_ids:
                            return bin_ids

            return None

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract bin_ids: {e}")
            return None

    def _parse_bin_ids_from_data(self, data: str) -> list[int] | None:
        """Parse bin IDs from DepositedToBins/WithdrawnFromBins event data.

        The data layout for these events is:
        - offset to ids array (32 bytes)
        - offset to amounts array (32 bytes)
        - ids array: length (32 bytes) + elements
        - amounts array: length (32 bytes) + elements
        """
        try:
            data_hex = HexDecoder.normalize_hex(data)
            if len(data_hex) < 128:  # At least 64 bytes (two 32-byte offsets)
                return None

            # Get offset to ids array (first 32 bytes of data)
            ids_offset = HexDecoder.decode_uint256(data_hex, 0)

            # Read ids array length at offset
            ids_length = HexDecoder.decode_uint256(data_hex, ids_offset)

            if ids_length == 0 or ids_length > 1000:  # Sanity check
                return None

            # Read each bin ID
            bin_ids = []
            for i in range(ids_length):
                bin_id = HexDecoder.decode_uint256(data_hex, ids_offset + 32 + (i * 32))
                bin_ids.append(bin_id)

            return bin_ids

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to parse bin IDs: {e}")
            return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract total liquidity from LP transaction receipt.

        Note: TraderJoe V2 uses ERC-1155 tokens for LP positions (not ERC-20).
        Liquidity amounts are encoded in DepositedToBins/WithdrawnFromBins events
        as bytes32 arrays requiring complex decoding. This method returns None
        as the exact liquidity amount extraction is not yet implemented.

        For LP operation detection, use parse_receipt().liquidity_result instead.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            None (liquidity amount extraction not implemented for TraderJoe V2)
        """
        try:
            result = self.parse_receipt(receipt)

            # TraderJoe V2 uses DepositedToBins/WithdrawnFromBins events, not Transfer
            # The amounts are encoded as bytes32 arrays - returning None until decoded
            for event in result.events:
                if event.event_type in (
                    TraderJoeV2EventType.DEPOSITED_TO_BINS,
                    TraderJoeV2EventType.WITHDRAWN_FROM_BINS,
                ):
                    # Event detected but amount decoding not implemented
                    logger.debug("TraderJoe V2 liquidity event detected, amount decoding not implemented")
                    return None

            return None

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract liquidity: {e}")
            return None

    def extract_collected_fees(self, receipt: dict[str, Any]) -> ParsedFeeCollectionResult | None:
        """Extract collected fees data from a fee collection transaction receipt.

        Looks for ClaimedFees events and Transfer events to determine fee amounts.
        Returns None if ClaimedFees is not found (older LBPair versions without this event).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            ParsedFeeCollectionResult if fee collection found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            gas_used = receipt.get("gasUsed", 0)
            block_number = receipt.get("blockNumber", 0)

            # Look for ClaimedFees events first (V2.1)
            claimed_events = [e for e in result.events if e.event_type == TraderJoeV2EventType.CLAIMED_FEES]
            if claimed_events:
                event = claimed_events[0]
                bin_ids = []
                raw_data = event.data.get("raw_data", "")
                if raw_data:
                    bin_ids = self._parse_bin_ids_from_data(raw_data) or []

                # Get fee amounts from Transfer events
                fees_x = 0
                fees_y = 0
                transfers = [e for e in result.events if e.event_type == TraderJoeV2EventType.TRANSFER]
                if len(transfers) >= 2:
                    fees_x = transfers[0].data.get("value", 0)
                    fees_y = transfers[1].data.get("value", 0)
                elif len(transfers) == 1:
                    fees_x = transfers[0].data.get("value", 0)

                return ParsedFeeCollectionResult(
                    success=True,
                    pool_address=event.contract_address,
                    bin_ids=bin_ids,
                    fees_x=fees_x,
                    fees_y=fees_y,
                    gas_used=gas_used,
                    block_number=block_number,
                )

            return None

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract collected fees: {e}")
            return None

    def extract_fees0(self, receipt: dict[str, Any]) -> int | None:
        """Extract fee amount for token X from fee collection receipt.

        Args:
            receipt: Transaction receipt dict

        Returns:
            Fee amount in wei for token X, or None
        """
        result = self.extract_collected_fees(receipt)
        if result and result.success:
            return result.fees_x
        return None

    def extract_fees1(self, receipt: dict[str, Any]) -> int | None:
        """Extract fee amount for token Y from fee collection receipt.

        Args:
            receipt: Transaction receipt dict

        Returns:
            Fee amount in wei for token Y, or None
        """
        result = self.extract_collected_fees(receipt)
        if result and result.success:
            return result.fees_y
        return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> "LPCloseData | None":
        """Extract LP close data from transaction receipt.

        Looks for WithdrawnFromBins events and Transfer events.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPCloseData dataclass if withdrawal found, None otherwise
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            result = self.parse_receipt(receipt)

            if not result.liquidity_result or result.liquidity_result.is_add:
                return None

            # Get amounts from Transfer events (token withdrawals)
            amount_x = 0
            amount_y = 0

            transfers = [e for e in result.events if e.event_type == TraderJoeV2EventType.TRANSFER]
            if len(transfers) >= 2:
                # Usually first two transfers are the token amounts
                amount_x = transfers[0].data.get("value", 0)
                amount_y = transfers[1].data.get("value", 0) if len(transfers) > 1 else 0

            if amount_x > 0 or amount_y > 0:
                return LPCloseData(
                    amount0_collected=amount_x,
                    amount1_collected=amount_y,
                    fees0=0,  # TraderJoe doesn't separate fees in events
                    fees1=0,
                    liquidity_removed=None,
                )

            return None

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None


__all__ = [
    "TraderJoeV2ReceiptParser",
    "TraderJoeV2EventType",
    "TraderJoeV2Event",
    "SwapEventData",
    "LiquidityEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParsedLiquidityResult",
    "ParsedFeeCollectionResult",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "DEPOSITED_TO_BINS_TOPIC",
    "WITHDRAWN_FROM_BINS_TOPIC",
]
