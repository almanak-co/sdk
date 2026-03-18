"""Uniswap V3 Receipt Parser (Refactored).

Refactored to use base infrastructure utilities while maintaining backward compatibility.
Handles signed integers (int256, int24) and various unsigned types (uint160, uint128).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.connectors.base import EventRegistry, HexDecoder
from almanak.framework.execution.events import SwapResultPayload

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import LPCloseData, SwapAmounts
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
    "Swap": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
    "Mint": "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
    "Burn": "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
    "Collect": "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0",
    "Flash": "0xbdbdb71d7860376ba52b25a5028beea23581364a40522f6bcfb86bb1f2dca633",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

# Uniswap V3 NonfungiblePositionManager addresses (varies by chain; forks use different addresses)
POSITION_MANAGER_ADDRESSES: dict[str, str] = {
    "ethereum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "arbitrum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "optimism": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "base": "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
    "polygon": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "avalanche": "0x655C406EBFa14EE2006250925e54ec43AD184f8B",
    "bnb": "0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613",
    "bsc": "0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613",
    "mantle": "0x218bf598D1453383e2F4AA7b14fFB9BfB102D637",  # Agni Finance fork
    "monad": "0x7197E214c0b767cFB76Fb734ab638E2c192F4E53",
}

# Zero address for detecting mints
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
ZERO_ADDRESS_PADDED = "0x" + "0" * 64

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

# Legacy exports
SWAP_EVENT_TOPIC = EVENT_TOPICS["Swap"]


# =============================================================================
# Enums
# =============================================================================


class UniswapV3EventType(Enum):
    """Uniswap V3 event types."""

    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    COLLECT = "COLLECT"
    FLASH = "FLASH"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, UniswapV3EventType] = {
    "Swap": UniswapV3EventType.SWAP,
    "Mint": UniswapV3EventType.MINT,
    "Burn": UniswapV3EventType.BURN,
    "Collect": UniswapV3EventType.COLLECT,
    "Flash": UniswapV3EventType.FLASH,
    "Transfer": UniswapV3EventType.TRANSFER,
    "Approval": UniswapV3EventType.APPROVAL,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class UniswapV3Event:
    """Parsed Uniswap V3 event."""

    event_type: UniswapV3EventType
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
    def from_dict(cls, data: dict[str, Any]) -> UniswapV3Event:
        """Create from dictionary."""
        return cls(
            event_type=UniswapV3EventType(data["event_type"]),
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
    recipient: str
    amount0: int
    amount1: int
    sqrt_price_x96: int
    liquidity: int
    tick: int
    pool_address: str

    @property
    def token0_is_input(self) -> bool:
        """Check if token0 is the input token."""
        return self.amount0 > 0

    @property
    def token1_is_input(self) -> bool:
        """Check if token1 is the input token."""
        return self.amount1 > 0

    @property
    def amount_in(self) -> int:
        """Get the absolute input amount."""
        if self.amount0 > 0:
            return self.amount0
        return self.amount1

    @property
    def amount_out(self) -> int:
        """Get the absolute output amount."""
        if self.amount0 < 0:
            return abs(self.amount0)
        return abs(self.amount1)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sender": self.sender,
            "recipient": self.recipient,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "sqrt_price_x96": str(self.sqrt_price_x96),
            "liquidity": str(self.liquidity),
            "tick": self.tick,
            "pool_address": self.pool_address,
            "token0_is_input": self.token0_is_input,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SwapEventData:
        """Create from dictionary."""
        return cls(
            sender=data["sender"],
            recipient=data["recipient"],
            amount0=int(data["amount0"]),
            amount1=int(data["amount1"]),
            sqrt_price_x96=int(data["sqrt_price_x96"]),
            liquidity=int(data["liquidity"]),
            tick=int(data["tick"]),
            pool_address=data["pool_address"],
        )


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
    sqrt_price_x96_after: int = 0
    tick_after: int = 0

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
            "sqrt_price_x96_after": str(self.sqrt_price_x96_after),
            "tick_after": self.tick_after,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ParsedSwapResult:
        """Create from dictionary."""
        return cls(
            token_in=data["token_in"],
            token_out=data["token_out"],
            token_in_symbol=data.get("token_in_symbol", ""),
            token_out_symbol=data.get("token_out_symbol", ""),
            amount_in=int(data["amount_in"]),
            amount_out=int(data["amount_out"]),
            amount_in_decimal=Decimal(data["amount_in_decimal"]),
            amount_out_decimal=Decimal(data["amount_out_decimal"]),
            effective_price=Decimal(data["effective_price"]),
            slippage_bps=data["slippage_bps"],
            pool_address=data["pool_address"],
            sqrt_price_x96_after=int(data.get("sqrt_price_x96_after", 0)),
            tick_after=int(data.get("tick_after", 0)),
        )

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
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    events: list[UniswapV3Event] = field(default_factory=list)
    swap_events: list[SwapEventData] = field(default_factory=list)
    transfer_events: list[TransferEventData] = field(default_factory=list)
    swap_result: ParsedSwapResult | None = None
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
            "transfer_events": [t.to_dict() for t in self.transfer_events],
            "swap_result": self.swap_result.to_dict() if self.swap_result else None,
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "transaction_success": self.transaction_success,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class UniswapV3ReceiptParser:
    """Parser for Uniswap V3 transaction receipts.

    Refactored to use base infrastructure utilities for hex decoding
    and event registry management. Maintains full backward compatibility.
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "position_id",
            "swap_amounts",
            "tick_lower",
            "tick_upper",
            "liquidity",
            "lp_close_data",
        }
    )

    def __init__(
        self,
        chain: str = "arbitrum",
        token0_address: str | None = None,
        token1_address: str | None = None,
        token0_symbol: str | None = None,
        token1_symbol: str | None = None,
        token0_decimals: int | None = None,
        token1_decimals: int | None = None,
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
        if self.token0_symbol and self.token0_decimals is None:
            _, decimals = self._resolve_token_info(self.token0_symbol)
            if decimals is not None:
                self.token0_decimals = decimals
        if self.token1_symbol and self.token1_decimals is None:
            _, decimals = self._resolve_token_info(self.token1_symbol)
            if decimals is not None:
                self.token1_decimals = decimals

        # Default to 18 decimals if still unresolved (needed for amount calculations)
        if self.token0_decimals is None:
            self.token0_decimals = 18
        if self.token1_decimals is None:
            self.token1_decimals = 18

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

            events: list[UniswapV3Event] = []
            swap_events: list[SwapEventData] = []
            transfer_events: list[TransferEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data based on event type
                    if parsed_event.event_type == UniswapV3EventType.SWAP:
                        swap_data = self._parse_swap_event(parsed_event)
                        if swap_data:
                            swap_events.append(swap_data)

                    elif parsed_event.event_type == UniswapV3EventType.TRANSFER:
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

            # Log parsed receipt with user-friendly formatting
            gas_used = receipt.get("gasUsed", 0)
            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(gas_used)
            if swap_result:
                slippage_fmt = format_slippage_bps(swap_result.slippage_bps) if swap_result.slippage_bps else "N/A"
                logger.info(
                    f"🔍 Parsed Uniswap V3 swap: {swap_result.amount_in_decimal:.4f} {swap_result.token_in_symbol or 'token0'} "
                    f"→ {swap_result.amount_out_decimal:.4f} {swap_result.token_out_symbol or 'token1'}, "
                    f"slippage={slippage_fmt}, tx={tx_fmt}, {gas_fmt}"
                )
            else:
                logger.info(f"🔍 Parsed Uniswap V3 receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                events=events,
                swap_events=swap_events,
                transfer_events=transfer_events,
                swap_result=swap_result,
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

    def parse_logs(self, logs: list[dict[str, Any]]) -> list[UniswapV3Event]:
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
    ) -> UniswapV3Event | None:
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

            event_type = self.registry.get_event_type(event_name) or UniswapV3EventType.UNKNOWN

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

            return UniswapV3Event(
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
        - topic2: recipient (indexed)
        - data: amount0 (int256), amount1 (int256), sqrtPriceX96 (uint160), liquidity (uint128), tick (int24)
        """
        try:
            # Indexed: sender, recipient
            sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            recipient = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            # Non-indexed: amount0, amount1, sqrtPriceX96, liquidity, tick
            amount0 = HexDecoder.decode_int256(data, 0)
            amount1 = HexDecoder.decode_int256(data, 32)
            sqrt_price_x96 = HexDecoder.decode_uint160(data, 64)
            liquidity = HexDecoder.decode_uint128(data, 96)
            tick = HexDecoder.decode_int24(data, 128)

            # Normalize address
            pool_address = address.lower() if isinstance(address, str) else ""
            if isinstance(address, bytes):
                pool_address = "0x" + address.hex()

            return {
                "sender": sender,
                "recipient": recipient,
                "amount0": amount0,
                "amount1": amount1,
                "sqrt_price_x96": sqrt_price_x96,
                "liquidity": liquidity,
                "tick": tick,
                "pool_address": pool_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Swap data: {e}")
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
            if isinstance(address, bytes):
                token_address = "0x" + address.hex()

            return {
                "from_addr": from_addr,
                "to_addr": to_addr,
                "value": value,
                "token_address": token_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Transfer data: {e}")
            return {"raw_data": data}

    def _parse_swap_event(self, event: UniswapV3Event) -> SwapEventData | None:
        """Parse a Swap event into typed data."""
        try:
            data = event.data
            return SwapEventData(
                sender=data.get("sender", ""),
                recipient=data.get("recipient", ""),
                amount0=data.get("amount0", 0),
                amount1=data.get("amount1", 0),
                sqrt_price_x96=data.get("sqrt_price_x96", 0),
                liquidity=data.get("liquidity", 0),
                tick=data.get("tick", 0),
                pool_address=data.get("pool_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse SwapEventData: {e}")
            return None

    def _parse_transfer_event(self, event: UniswapV3Event) -> TransferEventData | None:
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
        assert token_in_decimals is not None, "token_in_decimals must not be None"
        assert token_out_decimals is not None, "token_out_decimals must not be None"
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
            # Slippage = (expected - actual) / expected * 10000
            slippage_pct_float = (quoted_amount_out - amount_out) / quoted_amount_out
            slippage_bps = int(slippage_pct_float * 10000)
        elif self.quoted_price and self.quoted_price > 0:
            # Calculate from quoted price
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
            sqrt_price_x96_after=swap_event.sqrt_price_x96,
            tick_after=swap_event.tick,
        )

    # =============================================================================
    # Position ID Extraction
    # =============================================================================

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:
        """Extract LP position ID (NFT tokenId) from a transaction receipt.

        Looks for ERC-721 Transfer events from the NonfungiblePositionManager
        where from=address(0), indicating a mint (new position created).

        For ERC-721 Transfer events, the signature is:
            Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
        All parameters are indexed, so tokenId is in topics[3], not in data.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Position ID (tokenId) if found, None otherwise

        Example:
            >>> parser = UniswapV3ReceiptParser(chain="arbitrum")
            >>> position_id = parser.extract_position_id(receipt)
            >>> if position_id:
            ...     print(f"Opened position: {position_id}")
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            # Get position manager address for this chain
            position_manager = POSITION_MANAGER_ADDRESSES.get(self.chain, "").lower()
            if not position_manager:
                # Fall back to default (Ethereum/Arbitrum/Optimism address)
                position_manager = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88".lower()

            transfer_topic = EVENT_TOPICS["Transfer"].lower()

            for log in logs:
                # Handle both dict and object-style logs
                if hasattr(log, "get"):
                    topics = log.get("topics", [])
                    address = log.get("address", "")
                else:
                    topics = getattr(log, "topics", [])
                    address = getattr(log, "address", "")

                # Normalize address
                if isinstance(address, bytes):
                    address = "0x" + address.hex()
                address = str(address).lower()

                # Check if this is from the position manager
                if address != position_manager:
                    continue

                # Need at least 4 topics for ERC-721 Transfer
                if len(topics) < 4:
                    continue

                # Check if this is a Transfer event
                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()
                # Ensure 0x prefix for comparison
                if not first_topic.startswith("0x"):
                    first_topic = "0x" + first_topic

                if first_topic != transfer_topic:
                    continue

                # Check if from address is zero (minting)
                from_topic = topics[1]
                if isinstance(from_topic, bytes):
                    from_topic = "0x" + from_topic.hex()
                from_topic = str(from_topic).lower()
                # Ensure 0x prefix for comparison
                if not from_topic.startswith("0x"):
                    from_topic = "0x" + from_topic

                if from_topic != ZERO_ADDRESS_PADDED:
                    continue

                # Extract tokenId from topics[3] (ERC-721 has indexed tokenId)
                token_id_topic = topics[3]
                if isinstance(token_id_topic, bytes):
                    token_id_topic = "0x" + token_id_topic.hex()
                token_id_topic = str(token_id_topic)
                # Ensure 0x prefix for hex parsing
                if not token_id_topic.startswith("0x"):
                    token_id_topic = "0x" + token_id_topic

                try:
                    token_id = int(token_id_topic, 16)
                    logger.info(f"Extracted LP position ID from receipt: {token_id}")
                    return token_id
                except (ValueError, TypeError):
                    continue

            return None

        except Exception as e:
            logger.warning(f"Failed to extract position ID: {e}")
            return None

    @staticmethod
    def extract_position_id_from_logs(logs: list[dict[str, Any]], chain: str = "arbitrum") -> int | None:
        """Static method to extract position ID from logs without instantiating parser.

        Convenience method for cases where you just need to extract the position ID
        without parsing other events.

        Args:
            logs: List of log dicts from transaction receipt
            chain: Chain name for position manager address lookup

        Returns:
            Position ID (tokenId) if found, None otherwise

        Example:
            >>> position_id = UniswapV3ReceiptParser.extract_position_id_from_logs(
            ...     receipt["logs"], chain="arbitrum"
            ... )
        """
        parser = UniswapV3ReceiptParser(chain=chain)
        return parser.extract_position_id({"logs": logs})

    # =============================================================================
    # Swap Amounts Extraction (for Result Enrichment)
    # =============================================================================

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> SwapAmounts | None:
        """Extract swap amounts from a transaction receipt.

        This method is called by the ResultEnricher to automatically populate
        ExecutionResult.swap_amounts for SWAP intents.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            SwapAmounts dataclass if swap event found, None otherwise

        Example:
            >>> parser = UniswapV3ReceiptParser(chain="arbitrum")
            >>> swap_amounts = parser.extract_swap_amounts(receipt)
            >>> if swap_amounts:
            ...     print(f"Swapped: {swap_amounts.amount_in_decimal}")
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            # Parse the receipt to get swap result
            parse_result = self.parse_receipt(receipt)

            if not parse_result.swap_result:
                return None

            sr = parse_result.swap_result
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

    # =============================================================================
    # LP Extraction Methods (for Result Enrichment)
    # =============================================================================

    def extract_tick_lower(self, receipt: dict[str, Any]) -> int | None:
        """Extract tick lower from LP mint transaction receipt.

        Looks for Mint events from Uniswap V3 pools.
        tickLower is an indexed parameter in topics[2].

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

                # Check if this is a Mint event
                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                # tickLower is in topics[2] (indexed int24)
                tick_lower_topic = topics[2]
                if isinstance(tick_lower_topic, bytes):
                    tick_lower_topic = "0x" + tick_lower_topic.hex()
                tick_lower_topic = str(tick_lower_topic)

                # Decode as int24 (signed)
                tick_lower = HexDecoder.decode_int24(tick_lower_topic, 0)
                return tick_lower

            return None

        except Exception as e:
            logger.warning(f"Failed to extract tick_lower: {e}")
            return None

    def extract_tick_upper(self, receipt: dict[str, Any]) -> int | None:
        """Extract tick upper from LP mint transaction receipt.

        Looks for Mint events from Uniswap V3 pools.
        tickUpper is an indexed parameter in topics[3].

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

                # Check if this is a Mint event
                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                # tickUpper is in topics[3] (indexed int24)
                tick_upper_topic = topics[3]
                if isinstance(tick_upper_topic, bytes):
                    tick_upper_topic = "0x" + tick_upper_topic.hex()
                tick_upper_topic = str(tick_upper_topic)

                # Decode as int24 (signed)
                tick_upper = HexDecoder.decode_int24(tick_upper_topic, 0)
                return tick_upper

            return None

        except Exception as e:
            logger.warning(f"Failed to extract tick_upper: {e}")
            return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract liquidity from LP mint transaction receipt.

        Looks for Mint events from Uniswap V3 pools.
        Liquidity amount is in the data field.

        Mint event data layout:
        - amount (uint128): offset 0
        - amount0 (uint256): offset 16
        - amount1 (uint256): offset 48

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

                # Check if this is a Mint event
                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                # Extract liquidity from data field
                data = HexDecoder.normalize_hex(log.get("data", ""))
                if not data or data == "0x":
                    continue

                # Mint event: sender (address, 32 bytes), amount (uint128, 32 bytes), amount0, amount1
                # Actually the Mint event has: amount (uint128), amount0 (uint256), amount1 (uint256)
                # All packed at 32-byte boundaries
                liquidity = HexDecoder.decode_uint128(data, 0)
                return liquidity

            return None

        except Exception as e:
            logger.warning(f"Failed to extract liquidity: {e}")
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> LPCloseData | None:
        """Extract LP close data from transaction receipt.

        Looks for Collect events from Uniswap V3 pools which indicate
        fees and principal being collected when closing/reducing a position.

        Collect event: Collect(address indexed owner, int24 indexed tickLower,
                               int24 indexed tickUpper, uint128 amount0, uint128 amount1)

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

                # Check event type
                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                data = HexDecoder.normalize_hex(log.get("data", ""))

                if first_topic == collect_topic and len(topics) >= 4:
                    # Collect event - amounts collected
                    # data: amount0 (uint128), amount1 (uint128)
                    amount0 = HexDecoder.decode_uint128(data, 0)
                    amount1 = HexDecoder.decode_uint128(data, 32)
                    total_amount0 += amount0
                    total_amount1 += amount1

                elif first_topic == burn_topic and len(topics) >= 4:
                    # Burn event - liquidity being removed
                    # data: amount (uint128), amount0 (uint256), amount1 (uint256)
                    liquidity_removed = HexDecoder.decode_uint128(data, 0)

            if total_amount0 > 0 or total_amount1 > 0:
                return LPCloseData(
                    amount0_collected=total_amount0,
                    amount1_collected=total_amount1,
                    fees0=0,  # Uniswap V3 doesn't separate fees in events
                    fees1=0,
                    liquidity_removed=liquidity_removed,
                )

            return None

        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    # Backward compatibility methods
    def is_uniswap_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Uniswap V3 event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known Uniswap V3 event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes) -> UniswapV3EventType:
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
        return self.registry.get_event_type_from_topic(topic) or UniswapV3EventType.UNKNOWN


__all__ = [
    "UniswapV3ReceiptParser",
    "UniswapV3Event",
    "UniswapV3EventType",
    "SwapEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "SWAP_EVENT_TOPIC",
    "POSITION_MANAGER_ADDRESSES",
]
