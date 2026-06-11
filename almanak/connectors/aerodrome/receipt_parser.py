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

from almanak.connectors._strategy_base.base import EventRegistry, HexDecoder
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
    ExtractResult,
)

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import (
        LPCloseData,
        LPOpenData,
        ProtocolFees,
        SwapAmounts,
    )
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
    # Aerodrome / Velodrome V2 (Solidly fork) Pool emits:
    #   Swap(address indexed sender, address indexed to,
    #        uint256 amount0In, uint256 amount1In,
    #        uint256 amount0Out, uint256 amount1Out)
    # keccak256 of that signature is 0xb3e277... NOT 0xd78ad95f...
    # (the latter is Uniswap V2's Swap signature, which has `to` as the
    # last unindexed param). _decode_swap_data already assumes the Solidly
    # fork layout (sender + to both indexed); only the topic hash was off.
    # Fix surfaced by tests/intents/optimism/test_aerodrome_swap.py (VIB-4389).
    "Swap": "0xb3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b",
    "SwapCL": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
    "Mint": "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f",
    "Burn": "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496",
    "Sync": "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
    # Slipstream CL NFT events (NonfungiblePositionManager)
    "IncreaseLiquidity": "0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f",
    "DecreaseLiquidity": "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4",
    "CollectCL": "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01",
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
    "SwapCL": AerodromeEventType.SWAP,
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
class _SwapSeed:
    """Internal shared-state bundle for ``extract_swap_amounts`` phases.

    Not part of the public API — holds raw amounts + token hints carried
    between phase helpers so each helper stays small and individually
    testable. See ``AerodromeReceiptParser._seed_swap_fields``.
    """

    raw_in: int
    raw_out: int
    token_in_hint: str
    token_out_hint: str
    token_in_symbol: str
    token_out_symbol: str
    slippage_bps: int | None


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

    # Capability surface for the ResultEnricher SUPPORTED_EXTRACTIONS check
    # (VIB-4434 W2 — see audit doc §5). Each entry maps to a present
    # ``extract_<field>`` method on the class. Aerodrome V1 is a Solidly
    # fork with fungible LP — there is no NFT-position model and no
    # standalone tick extraction; the LP_OPEN fields ``lp_open_data`` /
    # ``tick_lower`` / ``tick_upper`` are intentionally absent and narrowed
    # via ``EXTRACTION_SPECS_REMOVE_BY_PROTOCOL["aerodrome"]["LP_OPEN"]``.
    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "swap_amounts",
            "position_id",  # returns the pool address (Solidly-fork LP id semantics)
            "liquidity",
            "lp_close_data",
            "protocol_fees",  # UNAVAILABLE-with-reason per VIB-3204 / VIB-3495
        }
    )

    def __init__(
        self,
        chain: str = "base",
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

        # Log warning if decimals remain unresolved — do NOT default to 18.
        if self.token0_decimals is None:
            logger.debug(f"token0 decimals unresolved for chain={self.chain} (address={self.token0_address})")
        if self.token1_decimals is None:
            logger.debug(f"token1 decimals unresolved for chain={self.chain} (address={self.token1_address})")

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

    def _resolve_decimals(self, token_address: str) -> int | None:
        """Resolve token decimals via the token resolver.

        Returns None if the resolver is unavailable or the token is unknown.
        """
        if not token_address:
            return None
        try:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            resolver = get_token_resolver()
            resolved = resolver.resolve(token_address, self.chain)
            return resolved.decimals
        except Exception:
            return None

    def parse_receipt(  # noqa: C901
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

            # Reverts must be reported before the empty-logs short-circuit,
            # otherwise an early-revert receipt (status=0, logs=[]) would be
            # silently surfaced as a successful empty receipt (issue #2064).
            if not tx_success:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=False,
                    error="Transaction reverted",
                )

            if not logs:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=tx_success,
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
                # ``amount_*_decimal`` may be None when token decimals could
                # not be resolved (Empty != zero invariant); fall back to "?"
                # rather than crashing the log line.
                in_fmt = f"{swap_result.amount_in_decimal:.4f}" if swap_result.amount_in_decimal is not None else "?"
                out_fmt = f"{swap_result.amount_out_decimal:.4f}" if swap_result.amount_out_decimal is not None else "?"
                logger.info(
                    f"🔍 Parsed Aerodrome swap: {in_fmt} {swap_result.token_in_symbol or 'token0'} "
                    f"→ {out_fmt} {swap_result.token_out_symbol or 'token1'}, "
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
        elif event_name == "SwapCL":
            return self._decode_cl_swap_data(topics, data, address)
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

    def _decode_cl_swap_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode Slipstream CL Swap event data (Uniswap V3-style).

        SwapCL event structure:
        - topic1: sender (indexed)
        - topic2: recipient (indexed)
        - data: amount0 (int256), amount1 (int256), sqrtPriceX96 (uint160),
                liquidity (uint128), tick (int24)

        Amount sign convention (pool perspective):
        - positive = tokens flowing INTO the pool (user pays)
        - negative = tokens flowing OUT of the pool (user receives)
        """
        sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        to = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

        amount0 = HexDecoder.decode_int256(data, 0)
        amount1 = HexDecoder.decode_int256(data, 32)

        pool_address = address.lower() if isinstance(address, str) else ""

        # Convert signed amounts to the V1-style amount_in/amount_out format:
        # positive = user pays (amount_in), negative = user receives (amount_out)
        amount0_in = amount0 if amount0 > 0 else 0
        amount1_in = amount1 if amount1 > 0 else 0
        amount0_out = abs(amount0) if amount0 < 0 else 0
        amount1_out = abs(amount1) if amount1 < 0 else 0

        return {
            "sender": sender,
            "to": to,
            "amount0_in": amount0_in,
            "amount1_in": amount1_in,
            "amount0_out": amount0_out,
            "amount1_out": amount1_out,
            "pool_address": pool_address,
        }

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
    ) -> ParsedSwapResult | None:
        """Build high-level swap result from events.

        Args:
            swap_event: The Swap event data
            transfer_events: List of Transfer events
            quoted_amount_out: Expected output for slippage calc

        Returns:
            ParsedSwapResult with full swap details, or None if decimals unresolved
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

        # Try to resolve decimals if not already known
        if token_in_decimals is None and token_in:
            token_in_decimals = self._resolve_decimals(token_in)
        if token_out_decimals is None and token_out:
            token_out_decimals = self._resolve_decimals(token_out)

        # Convert to decimal with proper decimals.
        # Return None when decimals are unresolved to avoid leaking fabricated
        # zero values to direct callers of parse_receipt().
        # extract_swap_amounts() handles this by falling back to raw swap_events.
        if token_in_decimals is not None and token_out_decimals is not None:
            amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
            amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**token_out_decimals)
        else:
            # DEBUG not WARNING: this is expected in the enrichment path where the
            # parser is constructed without token metadata.  extract_swap_amounts()
            # has its own fallback that resolves decimals from Transfer events.
            logger.debug(
                "Token decimals unresolved (in=%s, out=%s); omitting swap_result",
                token_in_decimals,
                token_out_decimals,
            )
            return None

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

        # Resolve decimals, falling back to resolver if constructor value is None
        t0_dec = self.token0_decimals if self.token0_decimals is not None else self._resolve_decimals(token0)
        t1_dec = self.token1_decimals if self.token1_decimals is not None else self._resolve_decimals(token1)

        # Convert to decimal (zero if decimals still unknown)
        amount0_decimal = Decimal(str(amount0)) / Decimal(10**t0_dec) if t0_dec is not None else Decimal(0)
        amount1_decimal = Decimal(str(amount1)) / Decimal(10**t1_dec) if t1_dec is not None else Decimal(0)

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

    def build_extract_kwargs(
        self,
        *,
        field: str,
        bundle_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Return Aerodrome-owned kwargs for ResultEnricher extraction calls.

        VIB-3164: the compiler records full token identity
        (``from_token`` / ``to_token`` dicts with address, symbol, decimals —
        see ``compile_swap_aerodrome``) in ``ActionBundle.metadata``.
        Threading it here lets ``_resolve_swap_decimals`` resolve decimals when
        the TokenResolver misses or Transfer events cannot be classified,
        instead of dropping the whole SwapAmounts row.

        Native-token entries are skipped: the receipt's Transfer events carry
        the wrapped token's address, so a native entry can never match by
        address, and its decimals (18) equal the fallback anyway.
        """
        if field != "swap_amounts":
            return {}
        meta: dict[str, dict[str, Any]] = {}
        for metadata_key, slot in (("from_token", "token_in"), ("to_token", "token_out")):
            raw = bundle_metadata.get(metadata_key)
            if not isinstance(raw, dict) or raw.get("is_native"):
                continue
            address = raw.get("address")
            decimals = raw.get("decimals")
            if not address or decimals is None:
                continue
            try:
                decimals_int = int(decimals)
            except (TypeError, ValueError):
                logger.debug("Could not coerce %s.decimals=%r to int; skipping hint", metadata_key, decimals)
                continue
            meta[slot] = {
                "address": str(address).lower(),
                "symbol": str(raw.get("symbol") or ""),
                "decimals": decimals_int,
            }
        return {"swap_token_meta": meta} if meta else {}

    @staticmethod
    def _build_hint_map(
        swap_token_meta: dict[str, dict[str, Any]] | None,
    ) -> dict[str, tuple[str, int]]:
        """Map compiler token metadata to ``{address: (symbol, decimals)}``."""
        hints: dict[str, tuple[str, int]] = {}
        if not swap_token_meta:
            return hints
        for slot in ("token_in", "token_out"):
            entry = swap_token_meta.get(slot)
            if not isinstance(entry, dict):
                continue
            address = entry.get("address")
            decimals = entry.get("decimals")
            if not address or decimals is None:
                continue
            try:
                hints[str(address).lower()] = (str(entry.get("symbol") or ""), int(decimals))
            except (TypeError, ValueError):
                logger.debug("Ignoring malformed token hint: %r", entry)
        return hints

    # ---- VIB-3159: tagged-variant wrappers ------------------------------------
    # See uniswap_v3/receipt_parser.py for the rationale. The raw methods
    # preserve their legacy return types so direct callers keep working.

    def _strict_parse(self, receipt: dict[str, Any]) -> ExtractResult[Any] | None:
        """Run ``parse_receipt`` and short-circuit with ``ExtractError`` if it
        reports a crash. See uniswap_v3 equivalent for rationale (VIB-3159)."""
        try:
            parsed = self.parse_receipt(receipt)
        except Exception as exc:  # noqa: BLE001 — malformed receipt shape
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if not parsed.success:
            return ExtractError(error=parsed.error or "parse_receipt reported failure")
        return None

    def extract_swap_amounts_result(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
    ) -> ExtractResult["SwapAmounts"]:
        """Fail-closed variant of :meth:`extract_swap_amounts` — see VIB-3159.

        VIB-3203: ``expected_out`` is forwarded to :meth:`extract_swap_amounts`
        for realized slippage_bps computation.

        VIB-3164: ``swap_token_meta`` is forwarded to :meth:`extract_swap_amounts`
        so compiler-supplied token hints can resolve decimals on resolver misses.
        **This is the method the ResultEnricher calls** — the kwarg MUST be here;
        omitting it would cause the enricher's TypeError fallback to silently drop
        ``expected_out`` too (see result_enricher.py §_invoke_extract).
        """
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = self.extract_swap_amounts(receipt, expected_out=expected_out, swap_token_meta=swap_token_meta)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no Swap event in receipt")
        return ExtractOk(value=value)

    def extract_lp_close_data_result(self, receipt: dict[str, Any]) -> ExtractResult["LPCloseData"]:
        """Fail-closed variant of :meth:`extract_lp_close_data` — see VIB-3159."""
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = self.extract_lp_close_data(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no Burn event in receipt")
        return ExtractOk(value=value)

    def extract_position_id_result(self, receipt: dict[str, Any]) -> ExtractResult[str]:
        """Fail-closed variant of :meth:`extract_position_id` — see VIB-3159."""
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = self.extract_position_id(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no LP position Transfer event")
        return ExtractOk(value=value)

    def extract_liquidity_result(self, receipt: dict[str, Any]) -> ExtractResult[int]:
        """Fail-closed variant of :meth:`extract_liquidity` — see VIB-3159."""
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = self.extract_liquidity(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no Mint event in receipt")
        return ExtractOk(value=value)

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
    ) -> "SwapAmounts | None":
        """Extract swap amounts from a transaction receipt.

        Resolves token decimals independently from ERC-20 Transfer events in the
        receipt, so it produces correct human-readable amounts even when the parser
        was constructed without token metadata (the enrichment path).

        Args:
            receipt: Transaction receipt dict with 'logs' and 'from' fields
            expected_out: VIB-3203 — pre-slippage-discount quote in human
                (Decimal) units from ``ActionBundle.metadata["expected_output_human"]``.
                When provided, realized ``slippage_bps`` is computed as
                ``(expected_out - amount_out_decimal) / expected_out * 10_000``.
            swap_token_meta: VIB-3164 — compiler-supplied token metadata threaded
                from ``build_extract_kwargs`` via the ResultEnricher hook.
                Shape: ``{"token_in": {"address": ..., "symbol": ..., "decimals": ...},
                "token_out": {...}}``. Hints win over the TokenResolver per address;
                a single-swap-gated direction fallback fills still-empty addresses;
                fail-closed semantics (return None on unresolved decimals) preserved.

        Returns:
            SwapAmounts dataclass if swap event found, None otherwise

        Implementation note:
            This method is intentionally thin — each logical phase is a
            dedicated helper so the control flow stays CC-bounded and
            individually testable (Phase 7.3 refactor). Preserves:
            (1) first-swap-event-wins multi-hop semantics,
            (2) sign conventions for amount0/amount1 (V1 + CL),
            (3) SwapAmounts field surface.
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            parse_result = self.parse_receipt(receipt)

            seed = self._seed_swap_fields(parse_result)
            if seed is None:
                return None

            hint_by_addr = self._build_hint_map(swap_token_meta)

            token_in_addr, token_out_addr = self._resolve_swap_token_addresses(
                receipt, parse_result, seed, swap_token_meta=swap_token_meta
            )

            decimals = self._resolve_swap_decimals(token_in_addr, token_out_addr, hint_by_addr=hint_by_addr)
            if decimals is None:
                return None
            in_decimals, out_decimals = decimals

            amount_in_decimal = Decimal(str(seed.raw_in)) / Decimal(10**in_decimals)
            amount_out_decimal = Decimal(str(seed.raw_out)) / Decimal(10**out_decimals)
            effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal("0")

            slippage_bps = self._apply_expected_out_slippage(seed.slippage_bps, expected_out, amount_out_decimal)

            return SwapAmounts(
                amount_in=seed.raw_in,
                amount_out=seed.raw_out,
                amount_in_decimal=amount_in_decimal,
                amount_out_decimal=amount_out_decimal,
                effective_price=effective_price,
                slippage_bps=slippage_bps,
                expected_out_decimal=expected_out,
                token_in=seed.token_in_symbol or token_in_addr or seed.token_in_hint,
                token_out=seed.token_out_symbol or token_out_addr or seed.token_out_hint,
            )

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    # ------------------------------------------------------------------
    # Extraction helpers — keep each CC small and individually testable.
    # ------------------------------------------------------------------

    def _seed_swap_fields(self, parse_result: ParseResult) -> "_SwapSeed | None":
        """Extract raw amounts + token hints from the parsed result.

        ``parse_result.swap_result`` is None when ``_build_swap_result`` fails
        closed on unresolved decimals, so fall back to the first raw Swap event.
        Returns None if no Swap event is present (multi-hop semantics still
        honoured: first swap event wins).
        """
        sr = parse_result.swap_result
        if sr:
            return _SwapSeed(
                raw_in=sr.amount_in,
                raw_out=sr.amount_out,
                token_in_hint=sr.token_in,
                token_out_hint=sr.token_out,
                token_in_symbol=sr.token_in_symbol,
                token_out_symbol=sr.token_out_symbol,
                slippage_bps=sr.slippage_bps if sr.slippage_bps else None,
            )
        if parse_result.swap_events:
            se = parse_result.swap_events[0]
            in_addr, out_addr, in_sym, out_sym = self._pick_token_hints(se.token0_is_input)
            return _SwapSeed(
                raw_in=se.amount_in,
                raw_out=se.amount_out,
                token_in_hint=in_addr,
                token_out_hint=out_addr,
                token_in_symbol=in_sym,
                token_out_symbol=out_sym,
                slippage_bps=None,
            )
        return None

    def _pick_token_hints(self, token0_is_input: bool) -> tuple[str, str, str, str]:
        """Pick ``(in_addr, out_addr, in_symbol, out_symbol)`` constructor hints.

        Returns empty strings for any unset field on the parser (constructor
        did not receive token metadata for that side).
        """
        if token0_is_input:
            return (
                self.token0_address or "",
                self.token1_address or "",
                self.token0_symbol or "",
                self.token1_symbol or "",
            )
        return (
            self.token1_address or "",
            self.token0_address or "",
            self.token1_symbol or "",
            self.token0_symbol or "",
        )

    def _resolve_swap_token_addresses(
        self,
        receipt: dict[str, Any],
        parse_result: ParseResult,
        seed: "_SwapSeed",
        *,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[str, str]:
        """Resolve ``(token_in_addr, token_out_addr)`` for the swap.

        Resolution order:
        1. ``_extract_swap_tokens_from_transfers`` — wallet<->pool Transfers.
        2. Pool fallback via ``_extract_tokens_by_pool`` — only when there is
           exactly ONE Swap event (multi-hop would pick the intermediate).
        2.5. VIB-3164 direction fallback: when a side is still empty AND there is
           exactly ONE Swap event AND the matching compiler-hint slot exists, take
           the hint address. Multi-hop receipts skip this (same gate as stage 2).
        3. Hints carried on the seed (token0/token1 metadata from constructor).
        """
        token_in_addr, token_out_addr, _, _ = self._extract_swap_tokens_from_transfers(receipt)

        single_swap = len(parse_result.swap_events) == 1

        if (not token_in_addr or not token_out_addr) and single_swap:
            pool_addr = parse_result.swap_events[0].pool_address
            if pool_addr:
                p_in, p_out = self._extract_tokens_by_pool(receipt, pool_addr)
                if not token_in_addr and p_in:
                    token_in_addr = p_in
                if not token_out_addr and p_out:
                    token_out_addr = p_out

        # Stage 2.5: direction fallback from compiler hints (single-swap only)
        if single_swap and swap_token_meta:
            if not token_in_addr:
                in_slot = swap_token_meta.get("token_in")
                if isinstance(in_slot, dict) and in_slot.get("address"):
                    token_in_addr = str(in_slot["address"]).lower()
            if not token_out_addr:
                out_slot = swap_token_meta.get("token_out")
                if isinstance(out_slot, dict) and out_slot.get("address"):
                    token_out_addr = str(out_slot["address"]).lower()

        if not token_in_addr:
            token_in_addr = seed.token_in_hint
        if not token_out_addr:
            token_out_addr = seed.token_out_hint

        return token_in_addr, token_out_addr

    def _resolve_swap_decimals(
        self,
        token_in_addr: str,
        token_out_addr: str,
        *,
        hint_by_addr: dict[str, tuple[str, int]] | None = None,
    ) -> tuple[int, int] | None:
        """Resolve ``(in_decimals, out_decimals)`` or return None when unknown.

        VIB-3164: ``hint_by_addr`` (address -> (symbol, decimals)) is consulted
        per side before the TokenResolver.  Compiler hints are intent-specific
        compile-time facts and win over the resolver for the same address.

        Fails closed with a WARNING log when either side can't be resolved —
        silent zero-decimal output would corrupt PnL calculations downstream.
        """
        addr_in = token_in_addr.lower() if token_in_addr else ""
        addr_out = token_out_addr.lower() if token_out_addr else ""

        if hint_by_addr and addr_in in hint_by_addr:
            in_decimals: int | None = hint_by_addr[addr_in][1]
        else:
            in_decimals = self._resolve_decimals(token_in_addr) if token_in_addr else None

        if hint_by_addr and addr_out in hint_by_addr:
            out_decimals: int | None = hint_by_addr[addr_out][1]
        else:
            out_decimals = self._resolve_decimals(token_out_addr) if token_out_addr else None

        if in_decimals is None or out_decimals is None:
            logger.warning(
                f"Cannot compute swap amounts: token decimals unknown "
                f"(in={token_in_addr}:{in_decimals}, out={token_out_addr}:{out_decimals})"
            )
            return None
        return in_decimals, out_decimals

    @staticmethod
    def _apply_expected_out_slippage(
        current_slippage_bps: int | None,
        expected_out: Decimal | None,
        amount_out_decimal: Decimal,
    ) -> int | None:
        """VIB-3203: override slippage_bps with realized slippage vs expected_out.

        Guards against expected_out <= 0 and amount_out_decimal <= 0 to avoid
        division by zero / negative-denominator math. When guards don't pass,
        preserves whatever slippage_bps the parser already produced.
        """
        if expected_out is not None and expected_out > 0 and amount_out_decimal > 0:
            realized_slippage = (expected_out - amount_out_decimal) / expected_out
            return int(realized_slippage * Decimal(10_000))
        return current_slippage_bps

    def _extract_swap_tokens_from_transfers(self, receipt: dict[str, Any]) -> tuple[str, str, int, int]:
        """Extract token addresses and amounts from ERC-20 Transfer events.

        Returns:
            Tuple of (token_in_addr, token_out_addr, amount_in, amount_out).
            Empty strings / 0 for fields that could not be determined.
        """
        raw_wallet = receipt.get("from") or receipt.get("from_address") or ""
        wallet = str(raw_wallet).lower() if raw_wallet else ""
        if not wallet:
            return "", "", 0, 0

        transfer_topic = EVENT_TOPICS["Transfer"].lower()
        transfers_from: list[tuple[str, int]] = []
        transfers_to: list[tuple[str, int]] = []

        for log in receipt.get("logs", []):
            topics = log.get("topics", [])
            if not topics or len(topics) < 3:
                continue

            topic0 = HexDecoder.normalize_hex(topics[0])
            if not topic0:
                continue
            if not topic0.startswith("0x"):
                topic0 = "0x" + topic0
            if topic0.lower() != transfer_topic:
                continue

            log_from = HexDecoder.topic_to_address(topics[1])
            log_to = HexDecoder.topic_to_address(topics[2])
            data = HexDecoder.normalize_hex(log.get("data", ""))
            if not data:
                continue
            try:
                amount = HexDecoder.decode_uint256(data, 0)
            except (ValueError, IndexError):
                continue

            raw_token = log.get("address") or ""
            token_address = str(raw_token).lower() if raw_token else ""
            if log_from == wallet:
                transfers_from.append((token_address, amount))
            if log_to == wallet:
                transfers_to.append((token_address, amount))

        token_in_addr, amount_in = transfers_from[0] if transfers_from else ("", 0)
        token_out_addr, amount_out = transfers_to[-1] if transfers_to else ("", 0)
        return token_in_addr, token_out_addr, amount_in, amount_out

    def _extract_tokens_by_pool(self, receipt: dict[str, Any], pool_address: str) -> tuple[str, str]:
        """Identify token_in and token_out from Transfer events using the pool address.

        In Solidly V2 swaps, the pool receives token_in and sends token_out.
        We find Transfers TO the pool (token_in) and FROM the pool (token_out).

        Args:
            receipt: Transaction receipt dict
            pool_address: Pool contract address (lowercase)

        Returns:
            Tuple of (token_in_addr, token_out_addr), empty strings if not found.
        """
        pool = pool_address.lower()
        transfer_topic = EVENT_TOPICS["Transfer"].lower()
        token_in = ""
        token_out = ""

        for log in receipt.get("logs", []):
            topics = log.get("topics", [])
            if not topics or len(topics) < 3:
                continue

            topic0 = HexDecoder.normalize_hex(topics[0])
            if not topic0:
                continue
            if not topic0.startswith("0x"):
                topic0 = "0x" + topic0
            if topic0.lower() != transfer_topic:
                continue

            log_to = HexDecoder.topic_to_address(topics[2])
            log_from = HexDecoder.topic_to_address(topics[1])

            raw_token = log.get("address") or ""
            token_address = str(raw_token).lower() if raw_token else ""

            if log_to == pool and not token_in:
                token_in = token_address
            if log_from == pool and not token_out:
                token_out = token_address

            if token_in and token_out:
                break

        return token_in, token_out

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
                    # VIB-4470 — Aerodrome V1 doesn't separate fees from
                    # principal; fees are unmeasured (Empty ≠ Zero).
                    fees0=None,
                    fees1=None,
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
                    # VIB-4470 — Transfer-event fallback doesn't separate
                    # fees from principal (Empty ≠ Zero).
                    fees0=None,
                    fees1=None,
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
                    # VIB-4470 — Transfer-event fallback doesn't separate fees
                    # from principal (Empty ≠ Zero).
                    fees0=None,
                    fees1=None,
                    liquidity_removed=None,
                )

        return None

    def extract_position_id(self, receipt: dict[str, Any]) -> str | None:
        """Extract position ID from LP mint transaction receipt.

        For Aerodrome (Solidly fork), LP tokens are fungible ERC-20s (not NFTs).
        The position ID is the pool address that emitted the Mint event, which
        is also the LP token contract address.

        Called by ResultEnricher for LP_OPEN intents.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Pool address (LP token address) as hex string, or None if not found
        """
        try:
            logs = receipt.get("logs", [])
            mint_topic = EVENT_TOPICS["Mint"]
            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue
                topic0 = topics[0]
                if isinstance(topic0, bytes | bytearray):
                    topic0 = "0x" + bytes(topic0).hex()
                else:
                    topic0 = str(topic0)
                if not topic0.startswith("0x"):
                    topic0 = "0x" + topic0
                if topic0.lower() == mint_topic.lower():
                    # The Mint event is emitted by the pool contract
                    pool_address = log.get("address", "")
                    if isinstance(pool_address, bytes | bytearray):
                        pool_address = "0x" + bytes(pool_address).hex()
                    else:
                        pool_address = str(pool_address)
                    if pool_address and not pool_address.startswith("0x"):
                        pool_address = "0x" + pool_address
                    if pool_address:
                        return str(pool_address).lower()
            return None
        except Exception as e:
            logger.warning(f"Failed to extract position_id: {e}")
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

    # =============================================================================
    # Protocol Fee Extraction (VIB-3204)
    # =============================================================================

    def extract_protocol_fees(self, _receipt: dict[str, Any]) -> "ProtocolFees":
        """VIB-3495: Aerodrome LP protocol fee coverage audit.

        Aerodrome V1 (Solidly fork) charges stable vs volatile pool-level
        fees (0.05% / 0.3%) on swaps. The fee is charged inside the pool's
        swap() function and is NOT emitted as a distinct on-chain event —
        neither the Swap event nor the Transfer events carry the fee amount
        separately from the net amounts. Resolving the USD fee amount would
        require (a) reading the pool's fee-rate slot and (b) a price oracle,
        neither of which is available at the receipt-parser layer.

        Returns a ProtocolFees with unavailable_reason so downstream
        attribution records "known-unknown" rather than "parser absent"
        (which was the old behaviour when this method returned None).
        """
        from almanak.framework.execution.extracted_data import ProtocolFees

        # VIB-3495: Aerodrome V1 fee rate is in pool storage (not the receipt).
        # The fee amount in token units is implicit in the Swap event's
        # amount_in vs amount_out gap, but USD conversion is unavailable here.
        return ProtocolFees(
            total_usd=None,
            unavailable_reason="protocol_fee_not_emitted_in_receipt",
        )

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


# =============================================================================
# Aerodrome Slipstream CL Receipt Parser
# =============================================================================

# Zero address constant for ERC-721 mint detection
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Slipstream CL NFT event topic constants
_INCREASE_LIQUIDITY_TOPIC = EVENT_TOPICS["IncreaseLiquidity"].lower()
_DECREASE_LIQUIDITY_TOPIC = EVENT_TOPICS["DecreaseLiquidity"].lower()
_COLLECT_CL_TOPIC = EVENT_TOPICS["CollectCL"].lower()
_ERC721_TRANSFER_TOPIC = EVENT_TOPICS["Transfer"].lower()
# Slipstream CL Pool emits the standard Uniswap V3-style Pool.Mint event.
# keccak256("Mint(address,address,int24,int24,uint128,uint256,uint256)")
_SLIPSTREAM_POOL_MINT_TOPIC = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
# Slipstream CL Pool emits the standard Uniswap V3-style Pool.Burn event on close.
# keccak256("Burn(address,int24,int24,uint128,uint256,uint256)")
# Distinct from EVENT_TOPICS["Burn"] (V2 AMM ``Burn(address,uint256,uint256,address)``)
# — Slipstream is a Uniswap V3 fork at the pool layer.
_SLIPSTREAM_POOL_BURN_TOPIC = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
# SwapCL event topic from the CL pool — carries post-swap current tick.
_SLIPSTREAM_SWAP_CL_TOPIC = EVENT_TOPICS["SwapCL"].lower()


# Aerodrome Slipstream NonfungiblePositionManager addresses, sourced from the
# canonical contracts registry (single source of truth — `AERODROME` in
# ``almanak/core/contracts.py``). Slipstream is a Base-only deployment today;
# adding a new chain is a one-line change in ``contracts.py`` and this dict
# rebuilds automatically.
def _build_slipstream_npm_addresses() -> dict[str, str]:
    from .addresses import AERODROME

    out: dict[str, str] = {}
    for chain, entry in AERODROME.items():
        cl_nft = entry.get("cl_nft")
        if cl_nft:
            out[chain.lower()] = cl_nft.lower()
    return out


_SLIPSTREAM_NPM_ADDRESSES: dict[str, str] = _build_slipstream_npm_addresses()


class AerodromeSlipstreamReceiptParser(AerodromeReceiptParser):
    """Receipt parser for Aerodrome Slipstream CL (concentrated liquidity) transactions.

    Extends AerodromeReceiptParser to handle NFT position events from the
    NonfungiblePositionManager contract:
    - IncreaseLiquidity: emitted on mint (LP_OPEN)
    - DecreaseLiquidity: emitted on decreaseLiquidity (LP_CLOSE)
    - ERC-721 Transfer: used to extract tokenId on mint

    Position ID is the NFT tokenId extracted from the ERC-721 Transfer (mint)
    event where from == zero_address.
    """

    # Compose with the V1 base set rather than restating it, so any addition
    # to the parent stays in lockstep with Slipstream's surface.
    #
    # Slipstream extends V1 with three V3-style fields:
    #   * ``lp_open_data`` — typed LPOpenData populated from IncreaseLiquidity
    #     + ERC-721 Transfer; ``tick_lower`` / ``tick_upper`` are emitted
    #     INSIDE this struct.
    #   * ``fees0`` / ``fees1`` — Slipstream-only standalone fee extractors.
    #
    # Standalone ``extract_tick_lower`` / ``extract_tick_upper`` deliberately
    # do NOT exist (ticks ship via ``lp_open_data``); both flat fields are
    # narrowed via ``EXTRACTION_SPECS_REMOVE_BY_PROTOCOL["aerodrome_slipstream"]
    # ["LP_OPEN"]`` so the SUPPORTED_EXTRACTIONS capability check does not
    # emit false info-warnings (VIB-4434 W2).
    SUPPORTED_EXTRACTIONS: frozenset[str] = AerodromeReceiptParser.SUPPORTED_EXTRACTIONS | frozenset(
        {
            "lp_open_data",
            "fees0",
            "fees1",
        }
    )

    def extract_position_id(self, receipt: dict[str, Any]) -> str | None:
        """Extract NFT tokenId from LP mint receipt.

        For Slipstream CL, minting creates an ERC-721 NFT. The tokenId is
        found in the Transfer event emitted by the NonfungiblePositionManager
        where ``from == zero_address`` (mint). ERC-721 Transfer events have
        4 topics: [Transfer_sig, from (indexed), to (indexed), tokenId (indexed)].

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            NFT tokenId as string (e.g. "12345"), or None if not found
        """
        try:
            logs = receipt.get("logs", [])
            for log in logs:
                topics = log.get("topics", [])
                # ERC-721 Transfer has exactly 4 topics
                if len(topics) != 4:
                    continue

                topic0 = topics[0]
                if isinstance(topic0, bytes | bytearray):
                    topic0 = "0x" + bytes(topic0).hex()
                else:
                    topic0 = str(topic0)
                if not topic0.startswith("0x"):
                    topic0 = "0x" + topic0
                if topic0.lower() != _ERC721_TRANSFER_TOPIC:
                    continue

                # Check if from == zero_address (mint)
                from_addr = HexDecoder.topic_to_address(topics[1])
                if from_addr.lower() != _ZERO_ADDRESS:
                    continue

                # tokenId is in topics[3] (indexed uint256)
                topic3 = topics[3]
                if isinstance(topic3, bytes | bytearray):
                    token_id_hex = bytes(topic3).hex()
                else:
                    token_id_hex = str(topic3).replace("0x", "").replace("0X", "")
                token_id = int(token_id_hex, 16)
                return str(token_id)

            return None
        except Exception as e:
            logger.warning(f"Failed to extract Slipstream CL position_id: {e}")
            return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract liquidity from CL mint receipt via IncreaseLiquidity event.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Liquidity amount if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue
                topic0 = topics[0]
                if isinstance(topic0, bytes | bytearray):
                    topic0 = "0x" + bytes(topic0).hex()
                else:
                    topic0 = str(topic0)
                if not topic0.startswith("0x"):
                    topic0 = "0x" + topic0
                if topic0.lower() != _INCREASE_LIQUIDITY_TOPIC:
                    continue

                # IncreaseLiquidity: topics[1]=tokenId(indexed), data=liquidity(uint128)+amount0(uint256)+amount1(uint256)
                data = HexDecoder.normalize_hex(log.get("data", ""))
                if len(data) >= 2 + 64:
                    # uint128 liquidity is the first 32-byte slot in data
                    liquidity = HexDecoder.decode_uint256(data, 0)
                    return liquidity

            return None
        except Exception as e:
            logger.warning(f"Failed to extract Slipstream CL liquidity: {e}")
            return None

    def extract_lp_open_data(self, receipt: dict[str, Any]) -> "LPOpenData | None":  # noqa: C901
        """Extract LP open data from a Slipstream CL mint receipt.

        Looks for ``IncreaseLiquidity`` events emitted by the Aerodrome
        Slipstream NonfungiblePositionManager when an LP position is opened
        or topped up. The event signature is::

            IncreaseLiquidity(
                uint256 indexed tokenId,
                uint128 liquidity,
                uint256 amount0,
                uint256 amount1,
            )

        Slipstream is a Uniswap V3 fork, so the surrounding receipt shape is
        identical: the CL pool emits a Uniswap-V3-style ``Mint`` event right
        before the NPM ``IncreaseLiquidity``; we track the most recent
        NPM-owned Pool Mint to recover tick bounds and pool address.

        Behaviour contract (matches the Uniswap V3 baseline parser):

        - Returns ``LPOpenData`` populated with the raw on-chain ints
          (``position_id``, ``liquidity``, ``amount0``, ``amount1``).
          The accounting handler is responsible for decimal-scaling.
        - Returns ``None`` when no ``IncreaseLiquidity`` log is present.
        - No outer ``try/except`` — the fail-closed variant
          ``extract_lp_open_data_result`` distinguishes parser crash vs.
          missing event per VIB-3159 / Blueprint 19.

        Args:
            receipt: Transaction receipt dict with 'logs' field.

        Returns:
            ``LPOpenData`` if an ``IncreaseLiquidity`` event is present,
            ``None`` otherwise.
        """
        from almanak.framework.execution.extracted_data import LPOpenData

        logs = receipt.get("logs") or []
        if not logs:
            return None

        chain_key = (self.chain or "").lower()
        npm_address = _SLIPSTREAM_NPM_ADDRESSES.get(chain_key)
        if not npm_address:
            # Fail loud on unsupported chains rather than defaulting to Base.
            # A silent fallback would mis-attribute logs once Slipstream ships
            # on a second chain — the parser's address-filter would reject
            # every IncreaseLiquidity from the real NPM, silently returning
            # ``LPOpenData = None`` and breaking LP accounting.
            logger.warning(
                "Slipstream NPM not registered for chain %r — extend "
                "almanak.core.contracts.AERODROME[<chain>]['cl_nft']",
                chain_key,
            )
            return None

        last_npm_mint: dict[str, Any] | None = None

        for log in logs:
            if hasattr(log, "get"):
                topics = log.get("topics", [])
                address = log.get("address", "")
                data = log.get("data", "")
            else:
                topics = getattr(log, "topics", [])
                address = getattr(log, "address", "")
                data = getattr(log, "data", "")

            if isinstance(address, bytes):
                address = "0x" + address.hex()
            address = str(address).lower()

            if not topics:
                continue

            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            first_topic = str(first_topic).lower()
            if not first_topic.startswith("0x"):
                first_topic = "0x" + first_topic

            # Track the most recent Pool Mint emitted with owner == NPM. The
            # next matching IncreaseLiquidity claims ITS ticks (and pool
            # address) — a multi-position bundle won't cross-contaminate.
            if first_topic == _SLIPSTREAM_POOL_MINT_TOPIC and len(topics) >= 4:
                if self._mint_owner_matches_npm(topics, npm_address):
                    last_npm_mint = log
                continue

            if address != npm_address:
                continue

            if len(topics) < 2:
                continue

            if first_topic != _INCREASE_LIQUIDITY_TOPIC:
                continue

            token_id_topic = topics[1]
            if isinstance(token_id_topic, bytes):
                token_id_topic = "0x" + token_id_topic.hex()
            token_id_topic = str(token_id_topic)
            if not token_id_topic.startswith("0x"):
                token_id_topic = "0x" + token_id_topic

            try:
                token_id = int(token_id_topic, 16)
            except (ValueError, TypeError):
                continue

            normalized = HexDecoder.normalize_hex(data)
            if not normalized or normalized == "0x":
                continue

            # IncreaseLiquidity data layout: liquidity (uint128, left-padded
            # to 32 bytes), amount0 (uint256), amount1 (uint256). Reading the
            # first slot as uint256 is equivalent to uint128 because the high
            # 16 bytes are zero — matches the Uniswap V3 baseline behaviour.
            # Decode failures here represent a malformed receipt (NPM emitted
            # a structurally-invalid IncreaseLiquidity log), NOT a missing
            # event. Propagate so ``extract_lp_open_data_result`` wraps as
            # ``ExtractError`` rather than ``ExtractMissing`` (VIB-3159 /
            # Blueprint 19 fail-closed disambiguation).
            try:
                liquidity = HexDecoder.decode_uint256(normalized, 0)
                amount0 = HexDecoder.decode_uint256(normalized, 32)
                amount1 = HexDecoder.decode_uint256(normalized, 64)
            except Exception as exc:
                raise ValueError(f"Malformed IncreaseLiquidity payload at offset 0-96: {exc}") from exc

            tick_lower, tick_upper = self._ticks_from_pool_mint(last_npm_mint)

            pool_address = ""
            if last_npm_mint is not None:
                addr_attr = (
                    last_npm_mint.get("address")
                    if hasattr(last_npm_mint, "get")
                    else getattr(last_npm_mint, "address", "")
                )
                if isinstance(addr_attr, bytes):
                    addr_attr = "0x" + addr_attr.hex()
                pool_address = str(addr_attr).lower()

            current_tick = self._current_tick_from_swap_cl(logs, pool_address)

            logger.info(
                f"Extracted Slipstream LP open data: tokenId={token_id} "
                f"liquidity={liquidity} amount0={amount0} amount1={amount1} "
                f"ticks=[{tick_lower}, {tick_upper}] current_tick={current_tick}"
            )
            return LPOpenData(
                position_id=token_id,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                liquidity=liquidity,
                amount0=amount0,
                amount1=amount1,
                current_tick=current_tick,
                pool_address=pool_address,
            )

        return None

    @staticmethod
    def _mint_owner_matches_npm(topics: list[Any], npm_address: str) -> bool:
        """Return True iff the Pool Mint event's ``owner`` indexed topic == NPM.

        Uses ``HexDecoder.topic_to_address`` for the indexed-address codec —
        the same helper the rest of this parser already uses (e.g. lines
        751, 794, 902 for ``_decode_*_data`` paths and ``from_addr``
        extraction). Single source of truth for "20-byte address inside a
        32-byte topic" decoding.
        """
        if len(topics) < 2:
            return False
        owner_addr = HexDecoder.topic_to_address(topics[1])
        if not owner_addr:
            return False
        return owner_addr.lower() == npm_address.lower()

    @staticmethod
    def _ticks_from_pool_mint(mint_log: dict[str, Any] | None) -> tuple[int | None, int | None]:
        """Decode (tickLower, tickUpper) from a Slipstream CL Pool Mint log.

        Mint(address sender, address indexed owner, int24 indexed tickLower,
             int24 indexed tickUpper, uint128 amount, uint256 amount0,
             uint256 amount1) — ticks live at topics[2] and topics[3] as
             indexed int24 values right-padded into 32-byte topics.
        """
        if mint_log is None:
            return (None, None)
        topics = mint_log.get("topics", []) if hasattr(mint_log, "get") else getattr(mint_log, "topics", [])
        if len(topics) < 4:
            return (None, None)

        def _decode_indexed_int24(topic: Any) -> int | None:
            if isinstance(topic, bytes):
                topic = "0x" + topic.hex()
            topic = str(topic).lower()
            if not topic.startswith("0x"):
                topic = "0x" + topic
            try:
                v = int(topic, 16)
            except (ValueError, TypeError):
                return None
            # Indexed int24 is stored as a 32-byte two's-complement value;
            # sign-extend by inspecting the top bit of the original 256-bit
            # word (negative numbers come back as huge unsigned values).
            if v >= 2**255:
                v -= 2**256
            return v

        return (_decode_indexed_int24(topics[2]), _decode_indexed_int24(topics[3]))

    @staticmethod
    def _current_tick_from_swap_cl(logs: list[Any], pool_address: str) -> int | None:
        """Decode post-swap current tick from a Slipstream SwapCL event.

        SwapCL data layout (Uniswap V3-compatible):
            amount0 (int256, 32B) + amount1 (int256, 32B)
            + sqrtPriceX96 (uint160, padded 32B)
            + liquidity (uint128, padded 32B)
            + tick (int24, sign-extended into 32B).
        Tick lives at byte offset 128 and is decoded via ``decode_int24``
        (clamps to int24 range, matching the Uniswap V3 baseline at
        ``uniswap_v3/receipt_parser.py:1779``).

        For multi-hop / bundled receipts with more than one SwapCL log on
        the same pool, we keep the LATEST tick — receipts come back from
        the RPC in logIndex order, so the final matching SwapCL carries
        the live post-swap tick the LP_OPEN sees. A single malformed log
        does not abort the scan; we ``continue`` and let later valid logs
        win.

        Returns None when no matching SwapCL event is present (e.g. a pure
        NPM.mint LP_OPEN with pre-balanced amounts).
        """
        if not pool_address:
            return None
        pool_addr_lower = pool_address.lower()
        latest_tick: int | None = None
        for log in logs:
            if hasattr(log, "get"):
                topics = log.get("topics", [])
                address = log.get("address", "")
                data = log.get("data", "")
            else:
                topics = getattr(log, "topics", [])
                address = getattr(log, "address", "")
                data = getattr(log, "data", "")

            if isinstance(address, bytes):
                address = "0x" + address.hex()
            if str(address).lower() != pool_addr_lower:
                continue
            if not topics:
                continue
            topic0 = topics[0]
            if isinstance(topic0, bytes):
                topic0 = "0x" + topic0.hex()
            topic0 = str(topic0).lower()
            if not topic0.startswith("0x"):
                topic0 = "0x" + topic0
            if topic0 != _SLIPSTREAM_SWAP_CL_TOPIC:
                continue

            normalized = HexDecoder.normalize_hex(data)
            # ``normalize_hex`` returns the payload WITHOUT a 0x prefix, so a
            # fully-formed SwapCL data field is exactly 5 × 64 = 320 hex chars
            # (amount0 + amount1 + sqrtPriceX96 + liquidity + tick). Skip
            # truncated logs but keep scanning for a later valid one.
            if not normalized or len(normalized) < 5 * 64:
                continue
            try:
                latest_tick = HexDecoder.decode_int24(normalized, 128)
            except Exception:
                # Malformed slot at offset 128 — skip this log, try next.
                continue
        return latest_tick

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> "LPCloseData | None":
        """Extract LP close data from Slipstream CL receipt.

        Reads the ``Collect`` event (actual amounts transferred to recipient) in
        preference to ``DecreaseLiquidity`` (amounts unlocked but not yet transferred).
        This ensures fees already owed before the close are included in the reported
        amounts, not just the principal removed in this transaction.

        Slipstream's close is a **two-transaction sequence**:
        ``decreaseLiquidity`` → ``collect``. The two events land in
        different receipts. Each call to this method sees ONE receipt, so it
        returns either:

        * a ``Collect``-sourced ``LPCloseData`` (``source="collect"``) — the
          principal + pre-existing fees actually transferred to the recipient;
        * a ``DecreaseLiquidity``-sourced fallback (``source="decrease_liquidity"``)
          — the principal unlocked into ``tokensOwed`` but not yet transferred.

        ``ResultEnricher`` aggregates across receipts and prefers the
        ``Collect``-sourced variant when both are present (VIB-4310). The
        ``source`` tag is the explicit signal it uses; do not strip it.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPCloseData if found, None otherwise
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            logs = receipt.get("logs", [])

            # Pass 1: prefer the Collect event — includes principal + pre-existing fees.
            # Collect: topics[0]=sig, topics[1]=tokenId(indexed),
            #          data = recipient(address,32b) + amount0Collected(uint256) + amount1Collected(uint256)
            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue
                topic0 = topics[0]
                if isinstance(topic0, bytes | bytearray):
                    topic0 = "0x" + bytes(topic0).hex()
                else:
                    topic0 = str(topic0)
                if not topic0.startswith("0x"):
                    topic0 = "0x" + topic0
                if topic0.lower() != _COLLECT_CL_TOPIC:
                    continue

                data = HexDecoder.normalize_hex(log.get("data", ""))
                # data = address(32b) + amount0(32b) + amount1(32b)
                if len(data) >= 2 + 96:
                    amount0 = HexDecoder.decode_uint256(data, 32)
                    amount1 = HexDecoder.decode_uint256(data, 64)
                    return LPCloseData(
                        amount0_collected=amount0,
                        amount1_collected=amount1,
                        # VIB-4470 — Slipstream's Collect event bundles
                        # principal+fees in ``amount0/amount1Collected``;
                        # fees are not separately observable here. Honest
                        # ``None`` (Empty ≠ Zero) vs the prior ``0`` lie.
                        fees0=None,
                        fees1=None,
                        source="collect",
                    )

            # Pass 2: fall back to DecreaseLiquidity (first tx receipt in two-tx close).
            # DecreaseLiquidity: topics[1]=tokenId(indexed),
            #                    data = liquidity(uint128) + amount0(uint256) + amount1(uint256)
            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue
                topic0 = topics[0]
                if isinstance(topic0, bytes | bytearray):
                    topic0 = "0x" + bytes(topic0).hex()
                else:
                    topic0 = str(topic0)
                if not topic0.startswith("0x"):
                    topic0 = "0x" + topic0
                if topic0.lower() != _DECREASE_LIQUIDITY_TOPIC:
                    continue

                data = HexDecoder.normalize_hex(log.get("data", ""))
                if len(data) >= 2 + 96:
                    liquidity = HexDecoder.decode_uint256(data, 0)
                    amount0 = HexDecoder.decode_uint256(data, 32)
                    amount1 = HexDecoder.decode_uint256(data, 64)
                    return LPCloseData(
                        amount0_collected=amount0,
                        amount1_collected=amount1,
                        # VIB-4470 — DecreaseLiquidity carries principal
                        # only; fees haven't been Collected yet so they are
                        # unmeasured (Empty ≠ Zero) rather than zero.
                        fees0=None,
                        fees1=None,
                        liquidity_removed=liquidity,
                        source="decrease_liquidity",
                    )

            return None
        except Exception as e:
            logger.warning(f"Failed to extract Slipstream CL lp_close_data: {e}")
            return None

    def _extract_collect_amounts(self, receipt: dict[str, Any]) -> tuple[int, int] | None:
        """Sum (amount0, amount1) over every Collect event in the receipt.

        Returns None when no Collect event is present **or when the receipt
        also carries a DecreaseLiquidity event** — the latter signals an
        LP_CLOSE bundle (decreaseLiquidity → collect) where the Collect
        amounts include unlocked principal in addition to any owed fees,
        making the value semantically incompatible with a pure-fees readout.

        ResultEnricher invokes both this extractor (via ``fees0``/``fees1``
        for LP_COLLECT_FEES) and ``extract_lp_close_data`` (for LP_CLOSE) on
        the same parser without protocol-level scoping; gating fees on
        absence of DecreaseLiquidity prevents an LP_CLOSE bundle from
        polluting ``extracted_data["fees0"]`` with principal+fees.
        """
        try:
            logs = receipt.get("logs", [])
            total0 = 0
            total1 = 0
            found = False
            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue
                topic0 = topics[0]
                if isinstance(topic0, bytes | bytearray):
                    topic0 = "0x" + bytes(topic0).hex()
                else:
                    topic0 = str(topic0)
                if not topic0.startswith("0x"):
                    topic0 = "0x" + topic0
                topic0_lower = topic0.lower()

                # Fees are not separable from principal in an LP_CLOSE bundle —
                # see docstring. Bail out as soon as we see DecreaseLiquidity.
                if topic0_lower == _DECREASE_LIQUIDITY_TOPIC:
                    return None

                if topic0_lower != _COLLECT_CL_TOPIC:
                    continue

                data = HexDecoder.normalize_hex(log.get("data", ""))
                # data layout: recipient(32b) + amount0(32b) + amount1(32b).
                # ``normalize_hex`` strips the ``0x`` prefix, so 96 bytes = 192
                # hex chars; reject any malformed payload that would cause
                # ``decode_uint256(data, 64)`` below to read past the end.
                if len(data) < 192:
                    continue
                total0 += HexDecoder.decode_uint256(data, 32)
                total1 += HexDecoder.decode_uint256(data, 64)
                found = True

            if not found:
                return None
            return total0, total1
        except Exception as e:
            logger.warning(f"Failed to extract Slipstream CL collect amounts: {e}")
            return None

    def extract_fees0(self, receipt: dict[str, Any]) -> int | None:
        """Extract token0 fees collected from a Slipstream Collect event.

        Used to enrich LP_COLLECT_FEES intents. The Collect event amount equals
        the realized fee in token0 only when liquidity is unchanged; if a
        DecreaseLiquidity event is also present we return None rather than
        misreport principal as fees (see ``_extract_collect_amounts``).
        """
        amounts = self._extract_collect_amounts(receipt)
        return None if amounts is None else amounts[0]

    def extract_fees1(self, receipt: dict[str, Any]) -> int | None:
        """Extract token1 fees collected from a Slipstream Collect event.

        See ``extract_fees0`` for semantics.
        """
        amounts = self._extract_collect_amounts(receipt)
        return None if amounts is None else amounts[1]

    def extract_lp_close_data_result(self, receipt: dict[str, Any]) -> ExtractResult["LPCloseData"]:
        """Fail-closed variant of extract_lp_close_data for Slipstream CL."""
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = self.extract_lp_close_data(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no Collect or DecreaseLiquidity event in receipt")
        return ExtractOk(value=value)

    def extract_lp_open_data_result(self, receipt: dict[str, Any]) -> ExtractResult["LPOpenData"]:
        """Fail-closed variant of extract_lp_open_data for Slipstream CL.

        Per VIB-3159 / Blueprint 19: callers that need to distinguish
        "parser crashed" from "no IncreaseLiquidity event present" use this
        variant. The bare ``extract_lp_open_data`` returns ``None`` on
        missing event and propagates exceptions unchanged.
        """
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = self.extract_lp_open_data(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no IncreaseLiquidity event in receipt")
        return ExtractOk(value=value)

    def extract_position_id_result(self, receipt: dict[str, Any]) -> ExtractResult[str]:
        """Fail-closed variant of extract_position_id for Slipstream CL."""
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = self.extract_position_id(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no ERC-721 mint Transfer event found")
        return ExtractOk(value=value)

    def extract_liquidity_result(self, receipt: dict[str, Any]) -> ExtractResult[int]:
        """Fail-closed variant of extract_liquidity for Slipstream CL."""
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = self.extract_liquidity(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no IncreaseLiquidity event in receipt")
        return ExtractOk(value=value)

    # =========================================================================
    # Registry-mode payload builders (VIB-4305 / T12 follow-up to PR #2241).
    # =========================================================================
    #
    # Strategy-runner ``_maybe_save_ledger_with_registry`` consumes these two
    # methods to compose ``position_registry.payload`` for LP_OPEN / LP_CLOSE
    # intents on Slipstream CL positions. They mirror the Uniswap V3 reference
    # implementation in
    # ``almanak/connectors/uniswap_v3/receipt_parser.py``
    # (PR #1869 / T08 / T12). The dict-shape helpers
    # (``_open_payload_disagrees`` / ``_build_close_receipt_payload`` /
    # ``_merge_open_payload_fields``) are reused from
    # ``UniswapV3ReceiptParser`` — they operate on plain dicts with no
    # UniV3-specific assumptions, and re-implementing them here would be
    # drift. The receipt-decoding (token_id from DecreaseLiquidity, pool from
    # Pool Burn, NPM address) is Slipstream-specific because the emitter
    # addresses and event signatures differ from canonical UniV3.

    def _nft_manager_address(self) -> str:
        """Return the canonical Slipstream NPM address for ``self.chain``.

        Sourced from ``_SLIPSTREAM_NPM_ADDRESSES`` (built at import time from
        ``almanak.core.contracts.AERODROME[<chain>]['cl_nft']``). The address
        is part of the ``physical_identity_hash`` input tuple (T08 invariant
        #1) and is a parser-side configuration constant — NEVER an off-chain
        RPC call — so the hash stays receipt-derivable from the receipt +
        parser config alone.

        Returns an empty string when no NPM is registered for the chain,
        matching the "Empty ≠ zero" contract: the caller (registry-payload
        builder) refuses to compose a payload on empty.
        """
        return _SLIPSTREAM_NPM_ADDRESSES.get((self.chain or "").lower(), "")

    def _decreaseliquidity_token_id(self, receipt: dict[str, Any]) -> int | None:
        """Recover ``tokenId`` from a ``DecreaseLiquidity`` log on the close
        receipt.

        Slipstream NPM emits ``DecreaseLiquidity(uint256 indexed tokenId, …)``
        on every ``decreaseLiquidity()`` call (identical ABI to UniV3). The
        indexed tokenId sits in ``topics[1]``. Returns ``None`` if no such
        log is in the receipt OR the emitting NPM doesn't match the
        configured chain — the close-side identity is then derivable only
        from strategy-supplied state, which is the legacy path the registry
        cutover is replacing. The caller treats ``None`` as
        "fall back to ``accounting_only``" with an ERROR log; no
        ``Decimal("0")`` substitution.

        Called by:

        - ``extract_registry_payload_close`` to anchor the close payload.
        - ``StrategyRunner._lookup_open_registry_payload`` when no
          token_id is supplied explicitly.
        """
        logs = receipt.get("logs") or []
        if not logs:
            return None
        npm_address = self._nft_manager_address()
        if not npm_address:
            return None

        for log in logs:
            if hasattr(log, "get"):
                topics = log.get("topics", [])
                address = log.get("address", "")
            else:
                topics = getattr(log, "topics", [])
                address = getattr(log, "address", "")

            if isinstance(address, bytes):
                address = "0x" + address.hex()
            address = str(address).lower()

            if not topics or len(topics) < 2:
                continue

            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            first_topic = str(first_topic).lower()
            if not first_topic.startswith("0x"):
                first_topic = "0x" + first_topic

            if first_topic != _DECREASE_LIQUIDITY_TOPIC:
                continue
            if address != npm_address:
                continue

            token_id_topic = topics[1]
            if isinstance(token_id_topic, bytes):
                token_id_topic = "0x" + token_id_topic.hex()
            token_id_topic = str(token_id_topic)
            if not token_id_topic.startswith("0x"):
                token_id_topic = "0x" + token_id_topic
            try:
                return int(token_id_topic, 16)
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _pool_address_from_burn(receipt: dict[str, Any]) -> str:
        """Recover the Slipstream pool address from a Pool ``Burn`` log emitter.

        Slipstream is a Uniswap V3 fork at the pool layer, so the close-side
        receipt carries a UV3-shape ``Burn(address indexed owner, int24
        indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256
        amount0, uint256 amount1)`` event. The pool itself emits the log, so
        ``log.address`` IS the pool address. We capture the FIRST burn we
        see (multicall closes targeting the same pool produce repeated burns
        with the same emitter).

        Returns lowercase hex address, or ``"" `` when no Pool Burn log is
        present. Empty string is the "Empty ≠ zero" signal — the caller
        refuses to compose the payload rather than collapsing to ``None`` /
        ``"0x000…"``.
        """
        logs = receipt.get("logs") or []
        for log in logs:
            if hasattr(log, "get"):
                topics = log.get("topics", [])
                address = log.get("address", "")
            else:
                topics = getattr(log, "topics", [])
                address = getattr(log, "address", "")

            if not topics:
                continue
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            first_topic = str(first_topic).lower()
            if not first_topic.startswith("0x"):
                first_topic = "0x" + first_topic
            if first_topic != _SLIPSTREAM_POOL_BURN_TOPIC:
                continue

            if isinstance(address, bytes):
                address = "0x" + address.hex()
            address = str(address).lower()
            if address and address != _ZERO_ADDRESS:
                return address
        return ""

    def extract_registry_payload_open(
        self,
        receipt: dict[str, Any],
        *,
        fee_tier: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the LP_OPEN ``position_registry.payload`` dict.

        Reads the existing :meth:`extract_lp_open_data` output for
        ``position_id`` / ``tick_lower`` / ``tick_upper`` / ``liquidity`` /
        ``amount0`` / ``amount1`` / ``pool_address`` and composes the
        canonical 8-key shape (plus optional ``fee_tier`` and the per-chain
        ``nft_manager_addr``). Returns ``None`` when any of the load-bearing
        identity fields are missing — the caller treats that as "fall back
        to ``accounting_only``", per CLAUDE.md "Empty ≠ zero" (a
        zero-substituted token_id would silently corrupt the
        ``physical_identity_hash``).

        Args:
            receipt: Transaction receipt dict with ``logs`` field.
            fee_tier: Optional pool fee tier (e.g. ``500`` for 0.05%);
                forwarded from the intent's compile-time metadata. ``None``
                when unknown — the payload key stays absent rather than
                substituting ``0`` (Empty ≠ zero).

        Returns:
            ``dict`` JSON-serializable with the 8 (or 9 with fee_tier) keys
            ratified by the PRD §Registry Data Shape and the T08 golden, OR
            ``None`` when the LP_OPEN data isn't extractable from the
            receipt.
        """
        lp_data = self.extract_lp_open_data(receipt)
        if lp_data is None:
            return None
        if lp_data.position_id is None or lp_data.position_id <= 0:
            # token_id is the identity anchor; a zero/negative value would
            # corrupt physical_identity_hash. Refuse to build the payload.
            return None
        if not lp_data.pool_address:
            # pool_address is the semantic_grouping_key anchor; missing it
            # would let two un-grouped rows in the same pool collide on
            # ix_registry_auto_mode. Refuse rather than emit a partial row.
            return None
        if lp_data.tick_lower is None or lp_data.tick_upper is None:
            # Range is part of the position's economic identity; missing
            # ticks would let teardown / rebalancing read malformed bounds.
            return None
        if lp_data.liquidity is None:
            return None
        nft_manager_addr = self._nft_manager_address()
        if not nft_manager_addr:
            # No NPM registered for this chain → refuse to emit a payload
            # with an empty identity component. ``_SLIPSTREAM_NPM_ADDRESSES``
            # is the single source of truth; extending Slipstream to a new
            # chain is a one-line ``AERODROME[<chain>]['cl_nft']`` change.
            return None

        payload: dict[str, Any] = {
            "token_id": str(lp_data.position_id),
            "pool_address": lp_data.pool_address.lower(),
            "tick_lower": lp_data.tick_lower,
            "tick_upper": lp_data.tick_upper,
            "liquidity": str(lp_data.liquidity),
            "amount0": str(lp_data.amount0) if lp_data.amount0 is not None else None,
            "amount1": str(lp_data.amount1) if lp_data.amount1 is not None else None,
            "nft_manager_addr": nft_manager_addr,
        }
        if fee_tier is not None and fee_tier > 0:
            payload["fee_tier"] = int(fee_tier)
        if self.token0_symbol:
            payload["_token0_label"] = self.token0_symbol
        if self.token1_symbol:
            payload["_token1_label"] = self.token1_symbol
        return payload

    def extract_registry_payload_close(
        self,
        receipt: dict[str, Any],
        *,
        open_payload: dict[str, Any] | None = None,
        fee_tier: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the LP_CLOSE ``position_registry.payload`` dict.

        Reads :meth:`extract_lp_close_data` (Burn / Collect amounts) and
        decodes the close-side ``DecreaseLiquidity`` event for the NFT
        ``token_id``, plus the Slipstream Pool Burn log for the pool
        emitter address. Then composes the close payload shape ratified by
        the T08 ``lp_close/expected_registry_row.json`` golden.

        Audit M1 (CodeRabbit): a real Slipstream LP_CLOSE proves itself
        with **both** ``DecreaseLiquidity`` on the NPM AND a Pool ``Burn``
        log carrying the pool address. A Collect-only receipt is NOT a
        close — it's a fee harvest. If we silently synthesized
        ``token_id`` / ``pool_address`` from ``open_payload`` here, a
        Collect-only receipt or a malformed close would produce a
        "successful" close payload with stale OPEN-side anchors, and the
        registry would mark a still-open NFT as closed (cutover spec
        D3.F6 silent-error class).

        The flow:

        1. Decode close-side amounts (``extract_lp_close_data``).
        2. Decode the DecreaseLiquidity log (``_decreaseliquidity_token_id``).
        3. Decode the Pool Burn log (``_pool_address_from_burn``).
        4. Verify receipt-derived identity anchors are present and non-zero.
        5. Cross-check against ``open_payload`` (``_open_payload_disagrees``)
           — refuse on disagreement.
        6. Compose the receipt-only payload
           (``_build_close_receipt_payload``).
        7. Merge OPEN-time fields (``_merge_open_payload_fields``) — ticks,
           OPEN-time amounts, original mint liquidity, fee tier, token
           labels (close receipt cannot re-derive these).
        8. Apply the ``fee_tier`` argument if ``open_payload`` didn't carry
           one (setdefault — OPEN-side wins).

        Helpers ``_open_payload_disagrees`` / ``_build_close_receipt_payload``
        / ``_merge_open_payload_fields`` are reused from
        :class:`UniswapV3ReceiptParser` — they operate on plain dicts with
        no UV3-specific assumptions and are the single source of truth for
        the merge / cross-check semantics.

        Returns ``None`` when the close-side identity anchors (token_id +
        pool_address) cannot be derived OR cross-checks fail. The caller
        treats that as "fall back to ``accounting_only``" with an ERROR
        log (no zero substitution).
        """
        # Local import keeps the module-load order independent — both
        # parsers import each other's helpers only in the registry-payload
        # path. The helpers are static / classmethod and require no UV3
        # parser state.
        from almanak.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        lp_close = self.extract_lp_close_data(receipt)
        if lp_close is None:
            return None
        token_id = self._decreaseliquidity_token_id(receipt)
        if token_id is None or token_id <= 0:
            return None
        # ``extract_lp_close_data`` does NOT populate ``pool_address`` for
        # Slipstream (the NPM Collect / DecreaseLiquidity events don't carry
        # the pool emitter — the Pool Burn does). Decode it explicitly.
        pool_address = self._pool_address_from_burn(receipt)
        if not pool_address:
            return None
        if UniswapV3ReceiptParser._open_payload_disagrees(
            open_payload=open_payload,
            token_id=token_id,
            pool_address=pool_address,
        ):
            return None

        nft_manager_addr = self._nft_manager_address()
        if not nft_manager_addr:
            return None

        payload = UniswapV3ReceiptParser._build_close_receipt_payload(
            token_id=token_id,
            pool_address=pool_address,
            lp_close=lp_close,
            nft_manager_addr=nft_manager_addr,
        )
        UniswapV3ReceiptParser._merge_open_payload_fields(payload, open_payload)
        if fee_tier is not None and fee_tier > 0:
            payload.setdefault("fee_tier", int(fee_tier))
        if self.token0_symbol:
            payload.setdefault("_token0_label", self.token0_symbol)
        if self.token1_symbol:
            payload.setdefault("_token1_label", self.token1_symbol)
        return payload


__all__ = [
    "AerodromeReceiptParser",
    "AerodromeSlipstreamReceiptParser",
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
