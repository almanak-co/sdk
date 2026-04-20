"""Aerodrome Receipt Parser (Refactored).

Refactored to use base infrastructure utilities while maintaining backward compatibility.
Uses unsigned integers (uint256, uint112) for all values.
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
from almanak.framework.execution.events import SwapResultPayload
from almanak.framework.utils.log_formatters import (
    format_gas_cost,
    format_slippage_bps,
    format_tx_hash,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    "Swap": "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
    "Mint": "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f",
    "Burn": "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496",
    "Sync": "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

# Legacy exports
SWAP_EVENT_TOPIC = EVENT_TOPICS["Swap"]
MINT_EVENT_TOPIC = EVENT_TOPICS["Mint"]
BURN_EVENT_TOPIC = EVENT_TOPICS["Burn"]


# =============================================================================
# Enums
# =============================================================================


class AerodromeEventType(Enum):
    """Aerodrome event types."""

    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    SYNC = "SYNC"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, AerodromeEventType] = {
    "Swap": AerodromeEventType.SWAP,
    "Mint": AerodromeEventType.MINT,
    "Burn": AerodromeEventType.BURN,
    "Sync": AerodromeEventType.SYNC,
    "Transfer": AerodromeEventType.TRANSFER,
    "Approval": AerodromeEventType.APPROVAL,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class AerodromeEvent:
    """Parsed Aerodrome event."""

    event_type: AerodromeEventType
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
    def from_dict(cls, data: dict[str, Any]) -> "AerodromeEvent":
        """Create from dictionary."""
        return cls(
            event_type=AerodromeEventType(data["event_type"]),
            event_name=data["event_name"],
            log_index=data["log_index"],
            transaction_hash=data["transaction_hash"],
            block_number=data["block_number"],
            contract_address=data["contract_address"],
            data=data["data"],
            raw_topics=data.get("raw_topics", []),
            raw_data=data.get("raw_data", ""),
            timestamp=datetime.fromisoformat(data["timestamp"]) if "timestamp" in data else datetime.now(UTC),
        )


@dataclass
class SwapEventData:
    """Parsed data from Swap event."""

    sender: str
    to: str
    amount0_in: int
    amount1_in: int
    amount0_out: int
    amount1_out: int
    pool_address: str

    @property
    def token0_is_input(self) -> bool:
        """Check if token0 is the input token."""
        return self.amount0_in > 0

    @property
    def token1_is_input(self) -> bool:
        """Check if token1 is the input token."""
        return self.amount1_in > 0

    @property
    def amount_in(self) -> int:
        """Get the input amount."""
        if self.amount0_in > 0:
            return self.amount0_in
        return self.amount1_in

    @property
    def amount_out(self) -> int:
        """Get the output amount."""
        if self.amount0_out > 0:
            return self.amount0_out
        return self.amount1_out

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sender": self.sender,
            "to": self.to,
            "amount0_in": str(self.amount0_in),
            "amount1_in": str(self.amount1_in),
            "amount0_out": str(self.amount0_out),
            "amount1_out": str(self.amount1_out),
            "pool_address": self.pool_address,
            "token0_is_input": self.token0_is_input,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
        }


@dataclass
class MintEventData:
    """Parsed data from Mint event."""

    sender: str
    amount0: int
    amount1: int
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sender": self.sender,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "pool_address": self.pool_address,
        }


@dataclass
class BurnEventData:
    """Parsed data from Burn event."""

    sender: str
    amount0: int
    amount1: int
    to: str
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sender": self.sender,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "to": self.to,
            "pool_address": self.pool_address,
        }


@dataclass
class TransferEventData:
    """Parsed data from Transfer event."""

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


@dataclass
class ParsedSwapResult:
    """High-level swap result extracted from receipt."""

    token_in: str
    token_out: str
    token_in_symbol: str
    token_out_symbol: str
    amount_in: int
    amount_out: int
    amount_in_decimal: Decimal
    amount_out_decimal: Decimal
    effective_price: Decimal
    slippage_bps: int
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "token_in_symbol": self.token_in_symbol,
            "token_out_symbol": self.token_out_symbol,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "amount_in_decimal": str(self.amount_in_decimal),
            "amount_out_decimal": str(self.amount_out_decimal),
            "effective_price": str(self.effective_price),
            "slippage_bps": self.slippage_bps,
            "pool_address": self.pool_address,
        }

    def to_swap_result_payload(self) -> SwapResultPayload:
        """Convert to SwapResultPayload for event emission."""
        return SwapResultPayload(
            token_in=self.token_in_symbol or self.token_in,
            token_out=self.token_out_symbol or self.token_out,
            amount_in=self.amount_in_decimal,
            amount_out=self.amount_out_decimal,
            effective_price=self.effective_price,
            slippage_bps=self.slippage_bps,
        )


@dataclass
class ParsedLiquidityResult:
    """High-level liquidity result extracted from receipt."""

    operation: str  # "add" or "remove"
    token0: str
    token1: str
    token0_symbol: str
    token1_symbol: str
    amount0: int
    amount1: int
    amount0_decimal: Decimal
    amount1_decimal: Decimal
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "operation": self.operation,
            "token0": self.token0,
            "token1": self.token1,
            "token0_symbol": self.token0_symbol,
            "token1_symbol": self.token1_symbol,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "amount0_decimal": str(self.amount0_decimal),
            "amount1_decimal": str(self.amount1_decimal),
            "pool_address": self.pool_address,
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    events: list[AerodromeEvent] = field(default_factory=list)
    swap_events: list[SwapEventData] = field(default_factory=list)
    mint_events: list[MintEventData] = field(default_factory=list)
    burn_events: list[BurnEventData] = field(default_factory=list)
    transfer_events: list[TransferEventData] = field(default_factory=list)
    swap_result: ParsedSwapResult | None = None
    liquidity_result: ParsedLiquidityResult | None = None
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
            "mint_events": [m.to_dict() for m in self.mint_events],
            "burn_events": [b.to_dict() for b in self.burn_events],
            "transfer_events": [t.to_dict() for t in self.transfer_events],
            "swap_result": self.swap_result.to_dict() if self.swap_result else None,
            "liquidity_result": self.liquidity_result.to_dict() if self.liquidity_result else None,
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "transaction_success": self.transaction_success,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class AerodromeReceiptParser:
    """Parser for Aerodrome transaction receipts.

    Refactored to use base infrastructure utilities for hex decoding
    and event registry management. Maintains full backward compatibility.
    """

    def __init__(
        self,
        chain: str = "base",
        token0_address: str | None = None,
        token1_address: str | None = None,
        token0_symbol: str | None = None,
        token1_symbol: str | None = None,
        token0_decimals: int = 18,
        token1_decimals: int = 18,
        quoted_price: Decimal | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the parser.

        Args:
            chain: Blockchain network (for token symbol resolution)
            token0_address: Address of token0 in the pool
            token1_address: Address of token1 in the pool
            token0_symbol: Symbol of token0
            token1_symbol: Symbol of token1
            token0_decimals: Decimals for token0
            token1_decimals: Decimals for token1
            quoted_price: Expected price for slippage calculation
        """
        self.chain = chain.lower()
        self.token0_address = token0_address.lower() if token0_address else None
        self.token1_address = token1_address.lower() if token1_address else None
        self.token0_symbol = token0_symbol
        self.token1_symbol = token1_symbol
        self.token0_decimals = token0_decimals
        self.token1_decimals = token1_decimals
        self.quoted_price = quoted_price

        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        # Try to resolve symbols and decimals from addresses via TokenResolver
        if self.token0_address and not self.token0_symbol:
            symbol, decimals = self._resolve_token_info(self.token0_address)
            if symbol:
                self.token0_symbol = symbol
            if decimals is not None:
                self.token0_decimals = decimals
        if self.token1_address and not self.token1_symbol:
            symbol, decimals = self._resolve_token_info(self.token1_address)
            if symbol:
                self.token1_symbol = symbol
            if decimals is not None:
                self.token1_decimals = decimals

        # If symbols were provided but decimals weren't, resolve decimals
        if self.token0_symbol and self.token0_decimals == 18:
            _, decimals = self._resolve_token_info(self.token0_symbol)
            if decimals is not None:
                self.token0_decimals = decimals
        if self.token1_symbol and self.token1_decimals == 18:
            _, decimals = self._resolve_token_info(self.token1_symbol)
            if decimals is not None:
                self.token1_decimals = decimals

    def _resolve_token_info(self, token: str) -> tuple[str, int | None]:
        """Resolve token symbol and decimals via TokenResolver.

        Args:
            token: Token address or symbol

        Returns:
            Tuple of (symbol, decimals) or ("", None) if not found
        """
        try:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            resolver = get_token_resolver()
            resolved = resolver.resolve(token, self.chain)
            return resolved.symbol, resolved.decimals
        except Exception:
            return "", None

    def parse_receipt(
        self,
        receipt: dict[str, Any],
        quoted_amount_out: int | None = None,
    ) -> ParseResult:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dict
            quoted_amount_out: Expected output amount for slippage calculation

        Returns:
            ParseResult with extracted events and swap data
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

            events: list[AerodromeEvent] = []
            swap_events: list[SwapEventData] = []
            mint_events: list[MintEventData] = []
            burn_events: list[BurnEventData] = []
            transfer_events: list[TransferEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data based on event type
                    if parsed_event.event_type == AerodromeEventType.SWAP:
                        swap_data = self._parse_swap_event(parsed_event)
                        if swap_data:
                            swap_events.append(swap_data)

                    elif parsed_event.event_type == AerodromeEventType.MINT:
                        mint_data = self._parse_mint_event(parsed_event)
                        if mint_data:
                            mint_events.append(mint_data)

                    elif parsed_event.event_type == AerodromeEventType.BURN:
                        burn_data = self._parse_burn_event(parsed_event)
                        if burn_data:
                            burn_events.append(burn_data)

                    elif parsed_event.event_type == AerodromeEventType.TRANSFER:
                        transfer_data = self._parse_transfer_event(parsed_event)
                        if transfer_data:
                            transfer_events.append(transfer_data)

            # Build high-level swap result
            swap_result = None
            if swap_events:
                swap_result = self._build_swap_result(
                    swap_events[0],  # Use first swap event
                    transfer_events,
                    quoted_amount_out,
                )

            # Build liquidity result
            liquidity_result = None
            if mint_events:
                liquidity_result = self._build_liquidity_result("add", mint_events[0])
            elif burn_events:
                liquidity_result = self._build_liquidity_result("remove", burn_events[0])

            # Log parsed receipt with user-friendly formatting
            gas_used = receipt.get("gasUsed", 0)
            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(gas_used)

            if swap_result:
                slippage_fmt = format_slippage_bps(swap_result.slippage_bps)
                logger.info(
                    f"🔍 Parsed Aerodrome swap: {swap_result.amount_in_decimal:.4f} {swap_result.token_in_symbol or 'token0'} "
                    f"→ {swap_result.amount_out_decimal:.4f} {swap_result.token_out_symbol or 'token1'}, "
                    f"slippage={slippage_fmt}, tx={tx_fmt}, {gas_fmt}"
                )
            elif liquidity_result:
                logger.info(
                    f"🔍 Parsed Aerodrome {liquidity_result.operation} liquidity: "
                    f"{liquidity_result.token0_symbol or 'token0'}/{liquidity_result.token1_symbol or 'token1'}, "
                    f"tx={tx_fmt}, {gas_fmt}"
                )
            else:
                logger.info(f"🔍 Parsed Aerodrome receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                events=events,
                swap_events=swap_events,
                mint_events=mint_events,
                burn_events=burn_events,
                transfer_events=transfer_events,
                swap_result=swap_result,
                liquidity_result=liquidity_result,
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
    ) -> AerodromeEvent | None:
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

            # Normalize first topic (event signature)
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

            event_type = self.registry.get_event_type(event_name) or AerodromeEventType.UNKNOWN

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
            parsed_data = self._decode_log_data(event_name, topics, data, contract_address)

            return AerodromeEvent(
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
        if event_name == "Swap":
            return self._decode_swap_data(topics, data, address)
        elif event_name == "Mint":
            return self._decode_mint_data(topics, data, address)
        elif event_name == "Burn":
            return self._decode_burn_data(topics, data, address)
        elif event_name == "Transfer":
            return self._decode_transfer_data(topics, data, address)
        else:
            return {"raw_data": data}

    def _decode_swap_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode Swap event data.

        Swap event structure:
        - topic1: sender (indexed)
        - topic2: to (indexed)
        - data: amount0In, amount1In, amount0Out, amount1Out (4x uint256)
        """
        try:
            # Indexed: sender, to
            sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            to = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            # Non-indexed: amount0In, amount1In, amount0Out, amount1Out
            amount0_in = HexDecoder.decode_uint256(data, 0)
            amount1_in = HexDecoder.decode_uint256(data, 32)
            amount0_out = HexDecoder.decode_uint256(data, 64)
            amount1_out = HexDecoder.decode_uint256(data, 96)

            pool_address = address.lower() if isinstance(address, str) else ""

            return {
                "sender": sender,
                "to": to,
                "amount0_in": amount0_in,
                "amount1_in": amount1_in,
                "amount0_out": amount0_out,
                "amount1_out": amount1_out,
                "pool_address": pool_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Swap data: {e}")
            return {"raw_data": data}

    def _decode_mint_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode Mint event data.

        Mint event structure:
        - topic1: sender (indexed)
        - data: amount0, amount1 (2x uint256)
        """
        try:
            # Indexed: sender
            sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""

            # Non-indexed: amount0, amount1
            amount0 = HexDecoder.decode_uint256(data, 0)
            amount1 = HexDecoder.decode_uint256(data, 32)

            pool_address = address.lower() if isinstance(address, str) else ""

            return {
                "sender": sender,
                "amount0": amount0,
                "amount1": amount1,
                "pool_address": pool_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Mint data: {e}")
            return {"raw_data": data}

    def _decode_burn_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode Burn event data.

        Burn event structure:
        - topic1: sender (indexed)
        - topic2: to (indexed)
        - data: amount0, amount1 (2x uint256)
        """
        try:
            # Indexed: sender, to
            sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            to = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            # Non-indexed: amount0, amount1
            amount0 = HexDecoder.decode_uint256(data, 0)
            amount1 = HexDecoder.decode_uint256(data, 32)

            pool_address = address.lower() if isinstance(address, str) else ""

            return {
                "sender": sender,
                "amount0": amount0,
                "amount1": amount1,
                "to": to,
                "pool_address": pool_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Burn data: {e}")
            return {"raw_data": data}

    def _decode_transfer_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode Transfer event data.

        Transfer event structure:
        - topic1: from (indexed)
        - topic2: to (indexed)
        - data: value (uint256)
        """
        try:
            from_addr = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            to_addr = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            value = HexDecoder.decode_uint256(data, 0)

            token_address = address.lower() if isinstance(address, str) else ""

            return {
                "from_addr": from_addr,
                "to_addr": to_addr,
                "value": value,
                "token_address": token_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Transfer data: {e}")
            return {"raw_data": data}

    def _parse_swap_event(self, event: AerodromeEvent) -> SwapEventData | None:
        """Parse a Swap event into typed data."""
        try:
            data = event.data
            return SwapEventData(
                sender=data.get("sender", ""),
                to=data.get("to", ""),
                amount0_in=data.get("amount0_in", 0),
                amount1_in=data.get("amount1_in", 0),
                amount0_out=data.get("amount0_out", 0),
                amount1_out=data.get("amount1_out", 0),
                pool_address=data.get("pool_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse SwapEventData: {e}")
            return None

    def _parse_mint_event(self, event: AerodromeEvent) -> MintEventData | None:
        """Parse a Mint event into typed data."""
        try:
            data = event.data
            return MintEventData(
                sender=data.get("sender", ""),
                amount0=data.get("amount0", 0),
                amount1=data.get("amount1", 0),
                pool_address=data.get("pool_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse MintEventData: {e}")
            return None

    def _parse_burn_event(self, event: AerodromeEvent) -> BurnEventData | None:
        """Parse a Burn event into typed data."""
        try:
            data = event.data
            return BurnEventData(
                sender=data.get("sender", ""),
                amount0=data.get("amount0", 0),
                amount1=data.get("amount1", 0),
                to=data.get("to", ""),
                pool_address=data.get("pool_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse BurnEventData: {e}")
            return None

    def _parse_transfer_event(self, event: AerodromeEvent) -> TransferEventData | None:
        """Parse a Transfer event into typed data."""
        try:
            data = event.data
            return TransferEventData(
                from_addr=data.get("from_addr", ""),
                to_addr=data.get("to_addr", ""),
                value=data.get("value", 0),
                token_address=data.get("token_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse TransferEventData: {e}")
            return None

    def _build_swap_result(
        self,
        swap_event: SwapEventData,
        transfer_events: list[TransferEventData],
        quoted_amount_out: int | None,
    ) -> ParsedSwapResult:
        """Build high-level swap result from events.

        Args:
            swap_event: The Swap event data
            transfer_events: List of Transfer events
            quoted_amount_out: Expected output for slippage calc

        Returns:
            ParsedSwapResult with full swap details
        """
        # Determine which token is in/out
        if swap_event.token0_is_input:
            token_in = self.token0_address or ""
            token_out = self.token1_address or ""
            token_in_symbol = self.token0_symbol or ""
            token_out_symbol = self.token1_symbol or ""
            token_in_decimals = self.token0_decimals
            token_out_decimals = self.token1_decimals
        else:
            token_in = self.token1_address or ""
            token_out = self.token0_address or ""
            token_in_symbol = self.token1_symbol or ""
            token_out_symbol = self.token0_symbol or ""
            token_in_decimals = self.token1_decimals
            token_out_decimals = self.token0_decimals

        amount_in = swap_event.amount_in
        amount_out = swap_event.amount_out

        # Convert to decimal with proper decimals
        amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
        amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**token_out_decimals)

        # Calculate effective price
        if amount_in_decimal > 0:
            effective_price = amount_out_decimal / amount_in_decimal
        else:
            effective_price = Decimal("0")

        # Calculate slippage
        slippage_bps = 0
        if quoted_amount_out and quoted_amount_out > 0:
            slippage_pct_float = (quoted_amount_out - amount_out) / quoted_amount_out
            slippage_bps = int(slippage_pct_float * 10000)
        elif self.quoted_price and self.quoted_price > 0:
            slippage_pct_decimal = (self.quoted_price - effective_price) / self.quoted_price
            slippage_bps = int(slippage_pct_decimal * 10000)

        return ParsedSwapResult(
            token_in=token_in,
            token_out=token_out,
            token_in_symbol=token_in_symbol,
            token_out_symbol=token_out_symbol,
            amount_in=amount_in,
            amount_out=amount_out,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            effective_price=effective_price,
            slippage_bps=slippage_bps,
            pool_address=swap_event.pool_address,
        )

    def _build_liquidity_result(
        self,
        operation: str,
        liquidity_event: MintEventData | BurnEventData,
    ) -> ParsedLiquidityResult:
        """Build high-level liquidity result from events.

        Args:
            operation: "add" or "remove"
            liquidity_event: Mint or Burn event data

        Returns:
            ParsedLiquidityResult with full liquidity details
        """
        token0 = self.token0_address or ""
        token1 = self.token1_address or ""
        token0_symbol = self.token0_symbol or ""
        token1_symbol = self.token1_symbol or ""

        amount0 = liquidity_event.amount0
        amount1 = liquidity_event.amount1

        # Convert to decimal
        amount0_decimal = Decimal(str(amount0)) / Decimal(10**self.token0_decimals)
        amount1_decimal = Decimal(str(amount1)) / Decimal(10**self.token1_decimals)

        return ParsedLiquidityResult(
            operation=operation,
            token0=token0,
            token1=token1,
            token0_symbol=token0_symbol,
            token1_symbol=token1_symbol,
            amount0=amount0,
            amount1=amount1,
            amount0_decimal=amount0_decimal,
            amount1_decimal=amount1_decimal,
            pool_address=liquidity_event.pool_address,
        )

    # =============================================================================
    # Extraction Methods (for Result Enrichment)
    # =============================================================================

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> "SwapAmounts | None":
        """Extract swap amounts from a transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            SwapAmounts dataclass if swap event found, None otherwise
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            result = self.parse_receipt(receipt)
            if not result.swap_result:
                return None

            sr = result.swap_result
            return SwapAmounts(
                amount_in=sr.amount_in,
                amount_out=sr.amount_out,
                amount_in_decimal=sr.amount_in_decimal,
                amount_out_decimal=sr.amount_out_decimal,
                effective_price=sr.effective_price,
                slippage_bps=sr.slippage_bps if sr.slippage_bps else None,
                token_in=sr.token_in_symbol or sr.token_in,
                token_out=sr.token_out_symbol or sr.token_out,
            )

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> "LPCloseData | None":
        """Extract LP close data from transaction receipt.

        Primary path: extracts from Burn events (amount0, amount1).
        Fallback path: extracts from Transfer events when Burn event is not
        detected (some Aerodrome pool variants may not emit a standard Burn event,
        but always emit Transfer events for the returned tokens).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPCloseData dataclass if token amounts found, None otherwise
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            result = self.parse_receipt(receipt)

            # Primary: use Burn events
            if result.burn_events:
                total_amount0 = sum(b.amount0 for b in result.burn_events)
                total_amount1 = sum(b.amount1 for b in result.burn_events)

                return LPCloseData(
                    amount0_collected=total_amount0,
                    amount1_collected=total_amount1,
                    fees0=0,  # Aerodrome V1 doesn't separate fees
                    fees1=0,
                    liquidity_removed=None,
                )

            # Fallback: use Transfer events for token returns.
            # In a removeLiquidity TX, the pool transfers token0 and token1
            # to the recipient. We identify these by:
            # 1. Filtering out LP token burns (transfers to/from zero address)
            # 2. Matching by known token0/token1 addresses if available
            # 3. Otherwise grouping by token_address (contract that emitted Transfer)
            if result.transfer_events:
                lp_close = self._extract_lp_close_from_transfers(result.transfer_events)
                if lp_close:
                    logger.info("Extracted lp_close_data via Transfer fallback (no Burn event detected)")
                    return lp_close

            return None

        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    def _extract_lp_close_from_transfers(self, transfer_events: list[TransferEventData]) -> "LPCloseData | None":
        """Extract LP close data from Transfer events (fallback path).

        In an Aerodrome removeLiquidity TX, the pool transfers token0 and
        token1 to the recipient. LP token burns go to the zero address.

        Strategy:
        - Path A: If token0/token1 addresses are known, find the final transfer
          of each token to a non-zero recipient. Uses only the last transfer per
          token to avoid double-counting from router hops.
        - Path B: Group transfers by recipient, then find a recipient who received
          2+ distinct tokens (the actual liquidity recipient). This avoids
          misidentifying LP token transfers or router intermediaries.

        Args:
            transfer_events: List of Transfer events from the receipt

        Returns:
            LPCloseData if token amounts found, None otherwise
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        zero_addr = "0x0000000000000000000000000000000000000000"

        # Path A: match by known token addresses, take last transfer per token
        if self.token0_address and self.token1_address:
            amount0 = 0
            amount1 = 0
            for t in transfer_events:
                addr = t.token_address.lower()
                to = t.to_addr.lower()
                frm = t.from_addr.lower()
                # Skip burns and mints (to/from zero)
                if to == zero_addr or frm == zero_addr:
                    continue
                if addr == self.token0_address:
                    amount0 = t.value  # last write wins, avoiding router double-count
                elif addr == self.token1_address:
                    amount1 = t.value
            if amount0 > 0 or amount1 > 0:
                return LPCloseData(
                    amount0_collected=amount0,
                    amount1_collected=amount1,
                    fees0=0,
                    fees1=0,
                    liquidity_removed=None,
                )

        # Path B: group by recipient, find one who received 2+ distinct tokens
        token_amounts_by_recipient: dict[str, dict[str, int]] = {}
        for t in transfer_events:
            to = t.to_addr.lower()
            frm = t.from_addr.lower()
            # Skip LP token burns (to zero) and mints (from zero)
            if to == zero_addr or frm == zero_addr:
                continue
            token_addr = t.token_address.lower()
            if to not in token_amounts_by_recipient:
                token_amounts_by_recipient[to] = {}
            token_amounts_by_recipient[to][token_addr] = token_amounts_by_recipient[to].get(token_addr, 0) + t.value

        # Find a recipient who received 2+ types of tokens (the LP closer)
        for token_amounts in token_amounts_by_recipient.values():
            if len(token_amounts) >= 2:
                amounts = sorted(token_amounts.values(), reverse=True)
                return LPCloseData(
                    amount0_collected=amounts[0],
                    amount1_collected=amounts[1],
                    fees0=0,
                    fees1=0,
                    liquidity_removed=None,
                )

        return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract liquidity from LP mint transaction receipt.

        For Aerodrome V1, this extracts the LP tokens minted from Transfer events.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LP token amount if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)

            # Look for LP token transfer from zero address (mint)
            zero_addr = "0x0000000000000000000000000000000000000000"
            for transfer in result.transfer_events:
                if transfer.from_addr.lower() == zero_addr:
                    return transfer.value

            return None

        except Exception as e:
            logger.warning(f"Failed to extract liquidity: {e}")
            return None

    # Backward compatibility methods
    def is_aerodrome_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Aerodrome event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known Aerodrome event
        """
        # Normalize topic to lowercase hex string with 0x prefix
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()

        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes) -> AerodromeEventType:
        """Get the event type for a topic.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            Event type or UNKNOWN
        """
        # Normalize topic to lowercase hex string with 0x prefix
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()

        return self.registry.get_event_type_from_topic(topic) or AerodromeEventType.UNKNOWN


__all__ = [
    "AerodromeReceiptParser",
    "AerodromeEvent",
    "AerodromeEventType",
    "SwapEventData",
    "MintEventData",
    "BurnEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParsedLiquidityResult",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "SWAP_EVENT_TOPIC",
    "MINT_EVENT_TOPIC",
    "BURN_EVENT_TOPIC",
]
