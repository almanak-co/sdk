"""SushiSwap V3 Receipt Parser.

This module provides receipt parsing for SushiSwap V3 transaction receipts.
Since SushiSwap V3 is a fork of Uniswap V3, it uses identical event signatures
and data layouts, making the parsing logic nearly identical.

Event Signatures:
- Swap: 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67
- Mint: 0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde
- Burn: 0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c
- Collect: 0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0
- Transfer: 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef

Key Data Structures:
- SwapEventData: Contains amount0, amount1 (signed), sqrtPriceX96, liquidity, tick
- MintEventData: Contains owner, tickLower, tickUpper, amount (liquidity), amount0, amount1
- BurnEventData: Contains owner, tickLower, tickUpper, amount (liquidity), amount0, amount1
- CollectEventData: Contains owner, tickLower, tickUpper, amount0, amount1

Example:
    from almanak.framework.connectors.sushiswap_v3.receipt_parser import (
        SushiSwapV3ReceiptParser,
    )

    parser = SushiSwapV3ReceiptParser(chain="arbitrum")
    result = parser.parse_receipt(receipt)

    if result.success and result.swap_result:
        print(f"Swapped: {result.swap_result.amount_in_decimal} -> {result.swap_result.amount_out_decimal}")
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
        SwapAmounts,
    )
from almanak.framework.utils.log_formatters import (
    format_gas_cost,
    format_slippage_bps,
    format_tx_hash,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures (identical to Uniswap V3)
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    "Swap": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
    "Mint": "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
    "Burn": "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
    "Collect": "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0",
    "Flash": "0xbdbdb71d7860376ba52b25a5028beea23581364a40522f6bcfb86bb1f2dca633",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
    # NonfungiblePositionManager.IncreaseLiquidity(uint256 indexed tokenId,
    # uint128 liquidity, uint256 amount0, uint256 amount1) — identical to the
    # Uniswap V3 NPM event (Sushi V3 is a fork). Used by ``extract_lp_open_data``
    # to recover (tokenId, liquidity, amount0, amount1) from an LP_OPEN receipt.
    "IncreaseLiquidity": "0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f",
    # NonfungiblePositionManager.DecreaseLiquidity — emitted on LP_CLOSE.
    # Kept alongside IncreaseLiquidity for parity with the Uniswap V3 baseline
    # (registry-mode close-side identity hash, VIB-4198/T12).
    "DecreaseLiquidity": "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4",
}


def _normalize_topic(topic: str | bytes) -> str:
    """Normalize an event topic to a lowercase '0x'-prefixed hex string.

    For string inputs, lowercases before checking the '0x' prefix so an
    uppercase '0X' isn't treated as missing-prefix and double-prefixed into
    a malformed '0x0x...' value. Bytes inputs go through ``bytes.hex()``,
    which is always lowercase, so the lowercase + prefix invariant holds
    without an explicit case fold.

    Structural normalization only -- does not validate that the body is hex.
    """
    if isinstance(topic, bytes):
        return "0x" + topic.hex()
    topic = str(topic).lower()
    if not topic.startswith("0x"):
        topic = "0x" + topic
    return topic


# SushiSwap V3 NonfungiblePositionManager addresses, sourced from the
# canonical contracts registry (single source of truth — ``SUSHISWAP_V3`` in
# ``almanak/core/contracts.py``). Adding a new chain is a one-line change in
# ``contracts.py`` and this dict rebuilds automatically at import time.
def _build_sushiswap_v3_npm_addresses() -> dict[str, str]:
    """Build {chain: position_manager} from the canonical SUSHISWAP_V3 registry.

    Mirrors the pattern Aerodrome Slipstream uses for its NPM dict (PR #2241)
    — keeps the parser in lock-step with the rest of the connector (SDK,
    adapter, compiler) and removes the risk of an address drift when a new
    chain is added to ``contracts.SUSHISWAP_V3`` but forgotten in the parser.

    The ``bsc`` entry in the canonical registry is aliased as ``bnb`` here
    because the framework uses ``bnb`` as the chain key on intent
    construction; both are accepted to avoid a silent miss on either spelling.
    """
    from almanak.core.contracts import SUSHISWAP_V3

    out: dict[str, str] = {}
    for chain, entry in SUSHISWAP_V3.items():
        pm = entry.get("position_manager")
        if pm:
            out[chain.lower()] = pm.lower()
    # Framework canonical chain key for Binance Smart Chain is ``bnb``; the
    # contracts registry uses ``bsc``. Alias the two so a receipt parsed with
    # either key resolves the same address.
    if "bsc" in out and "bnb" not in out:
        out["bnb"] = out["bsc"]
    return out


POSITION_MANAGER_ADDRESSES: dict[str, str] = _build_sushiswap_v3_npm_addresses()

# Zero address for detecting mints (ERC-721 Transfer from address(0))
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
ZERO_ADDRESS_PADDED = "0x" + "0" * 64

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

# Legacy exports
SWAP_EVENT_TOPIC = EVENT_TOPICS["Swap"]


# =============================================================================
# Enums
# =============================================================================


class SushiSwapV3EventType(Enum):
    """SushiSwap V3 event types."""

    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    COLLECT = "COLLECT"
    FLASH = "FLASH"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, SushiSwapV3EventType] = {
    "Swap": SushiSwapV3EventType.SWAP,
    "Mint": SushiSwapV3EventType.MINT,
    "Burn": SushiSwapV3EventType.BURN,
    "Collect": SushiSwapV3EventType.COLLECT,
    "Flash": SushiSwapV3EventType.FLASH,
    "Transfer": SushiSwapV3EventType.TRANSFER,
    "Approval": SushiSwapV3EventType.APPROVAL,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SushiSwapV3Event:
    """Parsed SushiSwap V3 event."""

    event_type: SushiSwapV3EventType
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
    def from_dict(cls, data: dict[str, Any]) -> SushiSwapV3Event:
        """Create from dictionary."""
        return cls(
            event_type=SushiSwapV3EventType(data["event_type"]),
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
    """Parsed data from Swap event.

    In V3 swaps:
    - Positive amount0/amount1 = tokens going INTO the pool (user spent)
    - Negative amount0/amount1 = tokens going OUT of the pool (user received)
    """

    sender: str
    recipient: str
    amount0: int  # Signed: positive = user paid, negative = user received
    amount1: int  # Signed: positive = user paid, negative = user received
    sqrt_price_x96: int
    liquidity: int
    tick: int
    pool_address: str

    @property
    def token0_is_input(self) -> bool:
        """Check if token0 is the input token (user paid token0)."""
        return self.amount0 > 0

    @property
    def token1_is_input(self) -> bool:
        """Check if token1 is the input token (user paid token1)."""
        return self.amount1 > 0

    @property
    def amount_in(self) -> int:
        """Get the absolute input amount (what user paid)."""
        if self.amount0 > 0:
            return self.amount0
        return self.amount1

    @property
    def amount_out(self) -> int:
        """Get the absolute output amount (what user received)."""
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
    events: list[SushiSwapV3Event] = field(default_factory=list)
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


class SushiSwapV3ReceiptParser:
    """Parser for SushiSwap V3 transaction receipts.

    Note: This parser uses HexDecoder and EventRegistry from the base module.
    A future refactor could inherit from BaseReceiptParser for full standardization.

    This parser handles:
    - Swap events (exact input and exact output)
    - Mint events (new LP positions)
    - Burn events (decrease liquidity)
    - Collect events (claim fees/tokens)
    - Transfer events (ERC-20 and ERC-721)

    Example:
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address="<token0-address>",  # e.g., WETH
            token1_address="<token1-address>",  # e.g., USDC
        )
        result = parser.parse_receipt(receipt)
    """

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
            # Normalize status to int (could be hex string like "0x1")
            status = receipt.get("status", 1)
            if isinstance(status, str):
                status = int(status, 16) if status.startswith("0x") else int(status)
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

            events: list[SushiSwapV3Event] = []
            swap_events: list[SwapEventData] = []
            transfer_events: list[TransferEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data based on event type
                    if parsed_event.event_type == SushiSwapV3EventType.SWAP:
                        swap_data = self._parse_swap_event(parsed_event)
                        if swap_data:
                            swap_events.append(swap_data)

                    elif parsed_event.event_type == SushiSwapV3EventType.TRANSFER:
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
                # ``amount_*_decimal`` may be None when token decimals could
                # not be resolved (Empty != zero invariant); fall back to "?"
                # rather than crashing the log line.
                in_fmt = f"{swap_result.amount_in_decimal:.4f}" if swap_result.amount_in_decimal is not None else "?"
                out_fmt = f"{swap_result.amount_out_decimal:.4f}" if swap_result.amount_out_decimal is not None else "?"
                logger.info(
                    f"Parsed SushiSwap V3 swap: {in_fmt} {swap_result.token_in_symbol or 'token0'} "
                    f"-> {out_fmt} {swap_result.token_out_symbol or 'token1'}, "
                    f"slippage={slippage_fmt}, tx={tx_fmt}, {gas_fmt}"
                )
            else:
                logger.info(f"Parsed SushiSwap V3 receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

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

    def parse_logs(self, logs: list[dict[str, Any]]) -> list[SushiSwapV3Event]:
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
    ) -> SushiSwapV3Event | None:
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

            event_type = self.registry.get_event_type(event_name) or SushiSwapV3EventType.UNKNOWN

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

            return SushiSwapV3Event(
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

    def _parse_swap_event(self, event: SushiSwapV3Event) -> SwapEventData | None:
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

    def _parse_transfer_event(self, event: SushiSwapV3Event) -> TransferEventData | None:
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
            logger.warning(
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
    # Position ID Extraction (for LP positions)
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
            >>> parser = SushiSwapV3ReceiptParser(chain="arbitrum")
            >>> position_id = parser.extract_position_id(receipt)
            >>> if position_id:
            ...     print(f"Opened position: {position_id}")
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            # Get position manager address for this chain. Fail loud when the
            # chain isn't registered rather than silently defaulting to a
            # different chain's NPM — a stale default would mis-attribute
            # every NFT mint and silently return None for the real position.
            chain_key = (self.chain or "").lower()
            position_manager = POSITION_MANAGER_ADDRESSES.get(chain_key)
            if not position_manager:
                logger.warning(
                    "SushiSwap V3 NPM not registered for chain %r — extend "
                    "almanak.core.contracts.SUSHISWAP_V3[<chain>]['position_manager']",
                    chain_key,
                )
                return None

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

                if first_topic != transfer_topic:
                    continue

                # Check if from address is zero (minting)
                from_topic = topics[1]
                if isinstance(from_topic, bytes):
                    from_topic = "0x" + from_topic.hex()
                from_topic = str(from_topic).lower()

                if from_topic != ZERO_ADDRESS_PADDED:
                    continue

                # Extract tokenId from topics[3] (ERC-721 has indexed tokenId)
                token_id_topic = topics[3]
                if isinstance(token_id_topic, bytes):
                    token_id_topic = "0x" + token_id_topic.hex()
                token_id_topic = str(token_id_topic)

                # Use HexDecoder for consistent uint256 decoding
                token_id = HexDecoder.decode_uint256(token_id_topic, 0)
                logger.info(f"Extracted LP position ID from receipt: {token_id}")
                return token_id

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
            >>> position_id = SushiSwapV3ReceiptParser.extract_position_id_from_logs(
            ...     receipt["logs"], chain="arbitrum"
            ... )
        """
        parser = SushiSwapV3ReceiptParser(chain=chain)
        return parser.extract_position_id({"logs": logs})

    # =============================================================================
    # Swap Amounts Extraction (for Result Enrichment)
    # =============================================================================

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
    ) -> SwapAmounts | None:
        """Extract swap amounts from a transaction receipt.

        This method is called by the ResultEnricher to automatically populate
        ExecutionResult.swap_amounts for SWAP intents.

        Resolves token decimals independently from ERC-20 Transfer events in the
        receipt, so it produces correct human-readable amounts even when the parser
        was constructed without token metadata (the enrichment path).

        Args:
            receipt: Transaction receipt dict with 'logs' and 'from' fields
            expected_out: VIB-3203 — pre-slippage-discount quote in human
                (Decimal) units from ``ActionBundle.metadata["expected_output_human"]``.
                When provided, realized ``slippage_bps`` is computed as
                ``(expected_out - amount_out_decimal) / expected_out * 10_000``.

        Returns:
            SwapAmounts dataclass if swap event found, None otherwise
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            parse_result = self.parse_receipt(receipt)

            # Extract raw amounts from swap_result if available, otherwise
            # fall back to raw swap_events (swap_result may be None when
            # _build_swap_result fails closed due to unresolved decimals).
            sr = parse_result.swap_result
            if sr:
                raw_in = sr.amount_in
                raw_out = sr.amount_out
                token_in_hint = sr.token_in
                token_out_hint = sr.token_out
                token_in_symbol = sr.token_in_symbol
                token_out_symbol = sr.token_out_symbol
                slippage_bps = sr.slippage_bps if sr.slippage_bps else None
            elif parse_result.swap_events:
                se = parse_result.swap_events[0]
                raw_in = se.amount_in
                raw_out = se.amount_out
                token_in_hint = (self.token0_address or "") if se.token0_is_input else (self.token1_address or "")
                token_out_hint = (self.token1_address or "") if se.token0_is_input else (self.token0_address or "")
                token_in_symbol = (self.token0_symbol or "") if se.token0_is_input else (self.token1_symbol or "")
                token_out_symbol = (self.token1_symbol or "") if se.token0_is_input else (self.token0_symbol or "")
                slippage_bps = None
            else:
                return None

            # Resolve decimals independently from Transfer events in the receipt.
            token_in_addr, token_out_addr, _, _ = self._extract_swap_tokens_from_transfers(receipt)

            if not token_in_addr:
                token_in_addr = token_in_hint
            if not token_out_addr:
                token_out_addr = token_out_hint

            in_decimals = self._resolve_decimals(token_in_addr) if token_in_addr else None
            out_decimals = self._resolve_decimals(token_out_addr) if token_out_addr else None

            if in_decimals is None or out_decimals is None:
                logger.warning(
                    f"Cannot compute swap amounts: token decimals unknown "
                    f"(in={token_in_addr}:{in_decimals}, out={token_out_addr}:{out_decimals})"
                )
                return None

            amount_in_decimal = Decimal(str(raw_in)) / Decimal(10**in_decimals)
            amount_out_decimal = Decimal(str(raw_out)) / Decimal(10**out_decimals)
            effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal("0")

            # VIB-3203: compute realized slippage from the compiler-supplied
            # pre-slippage-discount quote. Overrides parser-side slippage,
            # which is None on the enrichment path.
            if expected_out is not None and expected_out > 0 and amount_out_decimal > 0:
                realized_slippage = (expected_out - amount_out_decimal) / expected_out
                slippage_bps = int(realized_slippage * Decimal(10_000))

            return SwapAmounts(
                amount_in=raw_in,
                amount_out=raw_out,
                amount_in_decimal=amount_in_decimal,
                amount_out_decimal=amount_out_decimal,
                effective_price=effective_price,
                slippage_bps=slippage_bps,
                expected_out_decimal=expected_out,
                token_in=token_in_symbol or token_in_addr or token_in_hint,
                token_out=token_out_symbol or token_out_addr or token_out_hint,
            )

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

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

            if not topics[0]:
                continue
            topic0 = _normalize_topic(topics[0])
            if topic0 != transfer_topic:
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

    # =============================================================================
    # LP Extraction Methods (for Result Enrichment)
    # =============================================================================

    def extract_tick_lower(self, receipt: dict[str, Any]) -> int | None:
        """Extract tick lower from LP mint transaction receipt.

        Looks for Mint events from SushiSwap V3 pools.
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

        Looks for Mint events from SushiSwap V3 pools.
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

        Looks for Mint events from SushiSwap V3 pools.
        Liquidity amount is in the data field.

        Mint event data layout (non-indexed fields, 32-byte padded):
        - sender (address): offset 0
        - amount (uint128): offset 32
        - amount0 (uint256): offset 64
        - amount1 (uint256): offset 96

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

                # Mint event data layout (non-indexed fields):
                # - sender (address, padded to 32 bytes) - offset 0
                # - amount (uint128, padded to 32 bytes) - offset 32
                # - amount0 (uint256) - offset 64
                # - amount1 (uint256) - offset 96
                liquidity = HexDecoder.decode_uint128(data, 32)
                return liquidity

            return None

        except Exception as e:
            logger.warning(f"Failed to extract liquidity: {e}")
            return None

    def extract_lp_open_data(self, receipt: dict[str, Any]) -> LPOpenData | None:  # noqa: C901
        """Extract LP open data from a SushiSwap V3 mint receipt.

        Looks for ``IncreaseLiquidity`` events emitted by the SushiSwap V3
        NonfungiblePositionManager when an LP position is opened or topped up.
        The event signature is::

            IncreaseLiquidity(
                uint256 indexed tokenId,
                uint128 liquidity,
                uint256 amount0,
                uint256 amount1,
            )

        SushiSwap V3 is a clean Uniswap V3 fork, so the surrounding receipt
        shape is identical: the V3 pool emits a ``Mint`` event right before
        the NPM ``IncreaseLiquidity``; we track the most recent NPM-owned
        Pool Mint to recover tick bounds and pool address, then read the
        post-swap current tick from a Pool ``Swap`` event in the same receipt.

        Behaviour contract (matches the Uniswap V3 baseline parser):

        - Returns ``LPOpenData`` populated with the raw on-chain ints
          (``position_id``, ``liquidity``, ``amount0``, ``amount1``).
          The accounting handler is responsible for decimal-scaling.
        - Returns ``None`` (and emits a WARNING) when ``self.chain`` is not
          registered in ``SUSHISWAP_V3`` — does NOT silently default.
        - Returns ``None`` when no ``IncreaseLiquidity`` log is present in
          a receipt from a registered chain.
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

        # No outer ``try/except Exception`` here. The fail-closed variant
        # ``extract_lp_open_data_result`` is the documented entry point for
        # callers that need parser-crash vs. event-missing disambiguation
        # (VIB-3159 / Blueprint 19). A blanket-catch in this method would
        # collapse "parser crashed on a malformed receipt" into "no event
        # present" and re-introduce the ghost-position class of bug.
        logs = receipt.get("logs") or []
        if not logs:
            return None

        chain_key = (self.chain or "").lower()
        npm_address = POSITION_MANAGER_ADDRESSES.get(chain_key)
        if not npm_address:
            # Fail loud on unsupported chains rather than defaulting to any
            # specific chain. A silent fallback would mis-attribute logs once
            # SushiSwap V3 ships on a chain we haven't registered — the
            # parser's address-filter would reject every IncreaseLiquidity
            # from the real NPM, silently returning ``None`` and breaking
            # LP accounting end-to-end.
            logger.warning(
                "SushiSwap V3 NPM not registered for chain %r — extend "
                "almanak.core.contracts.SUSHISWAP_V3[<chain>]['position_manager']",
                chain_key,
            )
            return None

        increase_topic = EVENT_TOPICS["IncreaseLiquidity"].lower()
        mint_topic = EVENT_TOPICS["Mint"].lower()

        # Track the most recent Pool Mint whose ``owner`` indexed topic
        # equals the NPM. The next matching IncreaseLiquidity claims ITS
        # ticks (and pool address) — a multi-position bundle won't
        # cross-contaminate.
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

            # Track the most recent Pool Mint emitted with owner == NPM.
            if first_topic == mint_topic and len(topics) >= 4:
                if self._mint_owner_matches_npm(topics, npm_address):
                    last_npm_mint = log
                continue

            if address != npm_address:
                continue

            if len(topics) < 2:
                continue

            if first_topic != increase_topic:
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
                # Malformed indexed topic on an otherwise-matched event is
                # a known shape we want to skip-not-crash; the next log
                # might be a legitimate match.
                continue

            normalized = HexDecoder.normalize_hex(data)
            if not normalized or normalized == "0x":
                continue

            # IncreaseLiquidity data layout: liquidity (uint128, left-padded
            # to 32 bytes), amount0 (uint256), amount1 (uint256). Each field
            # starts on a 32-byte boundary.
            liquidity = HexDecoder.decode_uint128(normalized, 0)
            amount0 = HexDecoder.decode_uint256(normalized, 32)
            amount1 = HexDecoder.decode_uint256(normalized, 64)

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

            current_tick = self._current_tick_from_swap_event(logs, pool_address)

            logger.info(
                f"Extracted SushiSwap V3 LP open data: tokenId={token_id} "
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
                pool_address=pool_address,  # VIB-3893: framework slot0 fallback
            )

        return None

    @staticmethod
    def _mint_owner_matches_npm(topics: list[Any], npm_address: str) -> bool:
        """Return True iff the Pool Mint event's ``owner`` indexed topic == NPM.

        SushiSwap V3 Pool ``Mint`` event signature::

            Mint(address sender, address indexed owner, int24 indexed tickLower,
                 int24 indexed tickUpper, uint128 amount, uint256 amount0,
                 uint256 amount1)

        ``owner`` is the second topic (after topic0). Indexed addresses
        are right-aligned in a 32-byte topic; compare the low 40 hex chars.
        """
        if len(topics) < 2:
            return False
        owner_topic = topics[1]
        if isinstance(owner_topic, bytes):
            owner_topic = "0x" + owner_topic.hex()
        owner_topic = str(owner_topic).lower()
        try:
            owner_addr = "0x" + owner_topic.removeprefix("0x").rjust(64, "0")[-40:]
        except Exception:
            return False
        return owner_addr == npm_address.lower()

    @staticmethod
    def _ticks_from_pool_mint(mint_log: dict[str, Any] | None) -> tuple[int | None, int | None]:
        """Decode (tickLower, tickUpper) from a SushiSwap V3 Pool Mint log.

        ``Mint(address sender, address indexed owner, int24 indexed tickLower,
               int24 indexed tickUpper, uint128 amount, uint256 amount0,
               uint256 amount1)`` — ticks live at topics[2] and topics[3] as
        indexed int24 values right-padded into 32-byte topics.

        Returns ``(None, None)`` when the mint log is absent or has fewer than
        4 topics (defensive: malformed shapes do not crash the parser).
        """
        if mint_log is None:
            return (None, None)
        topics = mint_log.get("topics", []) if hasattr(mint_log, "get") else getattr(mint_log, "topics", [])
        if len(topics) < 4:
            return (None, None)

        def _decode(topic: Any) -> int | None:
            if isinstance(topic, bytes):
                topic = "0x" + topic.hex()
            try:
                # int24 sign-extension. HexDecoder.decode_int24 handles
                # two's-complement; raw int(...) would return the unsigned
                # value for negative ticks.
                return HexDecoder.decode_int24(str(topic), 0)
            except Exception:
                return None

        return (_decode(topics[2]), _decode(topics[3]))

    @staticmethod
    def _current_tick_from_swap_event(logs: list[Any], pool_address: str) -> int | None:
        """Find a Swap event from ``pool_address`` and decode its post-swap tick.

        VIB-3887 (Uniswap V3 baseline). The Uniswap V3 / SushiSwap V3 Pool
        Swap event signature is::

            event Swap(address indexed sender, address indexed recipient,
                       int256 amount0, int256 amount1, uint160 sqrtPriceX96,
                       uint128 liquidity, int24 tick)

        Layout: topics[0] = signature, topics[1] = sender, topics[2] =
        recipient. The non-indexed payload in ``data`` is
        ``amount0 (32) | amount1 (32) | sqrtPriceX96 (32) | liquidity (32)
        | tick (32, int24 right-aligned)``. The tick lives at byte offset 128.

        Returns ``None`` when no matching Swap is present (e.g. a pure
        NPM.mint LP_OPEN with pre-balanced inputs) — caller leaves
        ``current_tick=None`` and the framework's slot0 fallback kicks in.
        """
        if not pool_address:
            return None
        swap_topic = SWAP_EVENT_TOPIC.lower()
        pool_addr_lower = pool_address.lower()
        latest_swap_log: Any = None
        for log in logs:
            if hasattr(log, "get"):
                topics = log.get("topics", [])
                address = log.get("address", "")
            else:
                topics = getattr(log, "topics", [])
                address = getattr(log, "address", "")
            if isinstance(address, bytes):
                address = "0x" + address.hex()
            if str(address).lower() != pool_addr_lower:
                continue
            if not topics:
                continue
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            if str(first_topic).lower() != swap_topic:
                continue
            latest_swap_log = log  # later swaps override (post-swap tick is the live one)

        if latest_swap_log is None:
            return None
        data = (
            latest_swap_log.get("data", "") if hasattr(latest_swap_log, "get") else getattr(latest_swap_log, "data", "")
        )
        if isinstance(data, bytes):
            data = "0x" + data.hex()
        try:
            normalized = HexDecoder.normalize_hex(str(data))
            if not normalized or normalized == "0x":
                return None
            # tick is the 5th 32-byte slot in the data payload (offset 128).
            return HexDecoder.decode_int24(normalized, 128)
        except Exception:
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> LPCloseData | None:
        """Extract LP close data from transaction receipt.

        Looks for Collect and/or Burn events from SushiSwap V3 pools which
        indicate fees and principal being collected when closing or reducing
        a position. The compiler emits LP_CLOSE as a 3-TX bundle on Sushi V3
        (``lp_decrease_liquidity`` + ``lp_collect`` + ``lp_burn``), so a
        single receipt typically carries EITHER Burn (decrease tx) OR
        Collect (collect tx), not both — the runner's
        :meth:`_extract_receipt_from_result` picks one and asks the parser
        for whatever it can recover from it. Mirrors the Uniswap V3 baseline
        contract.

        Collect event: Collect(address indexed owner, int24 indexed tickLower,
                               int24 indexed tickUpper, uint128 amount0, uint128 amount1)

        Burn event: Burn(address indexed owner, int24 indexed tickLower,
                         int24 indexed tickUpper, uint128 amount,
                         uint256 amount0, uint256 amount1)

        Captures ``pool_address`` from the Burn event's emitter (the V3 pool)
        — required for the framework's slot0 fallback (VIB-3940) and for the
        registry-mode close payload (VIB-4198 / T12) whose
        ``physical_identity_hash`` / ``semantic_grouping_key`` derivation
        needs the pool address.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPCloseData dataclass if at least one Collect or Burn event is
            present, None otherwise. ``amount0_collected`` / ``amount1_collected``
            prefer Collect amounts when available; fall back to Burn's
            principal-only amounts when the receipt only carries a Burn (the
            ``lp_decrease_liquidity`` TX in a Sushi 3-TX close bundle).
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            collect_topic = EVENT_TOPICS["Collect"].lower()
            burn_topic = EVENT_TOPICS["Burn"].lower()

            collect_amount0 = 0
            collect_amount1 = 0
            burn_amount0 = 0
            burn_amount1 = 0
            burn_liquidity_total = 0
            saw_collect = False
            saw_burn = False
            # VIB-4198 / T12: capture pool address from the Burn event emitter
            # (the V3 pool itself emits Burn). Empty when no Burn is present
            # — bare ``collect()`` fee-harvests don't reveal the pool in the
            # current log shape. The registry-payload close-path treats an
            # empty pool_address as "fall back to save_ledger_entry", per
            # CLAUDE.md "Empty ≠ zero".
            pool_address = ""

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
                    # Collect(address indexed owner, address recipient, int24 indexed tickLower,
                    #         int24 indexed tickUpper, uint128 amount0, uint128 amount1)
                    # data layout: recipient (offset 0), amount0 (offset 32), amount1 (offset 64)
                    collect_amount0 += HexDecoder.decode_uint128(data, 32)
                    collect_amount1 += HexDecoder.decode_uint128(data, 64)
                    saw_collect = True

                elif first_topic == burn_topic and len(topics) >= 4:
                    # Burn event - liquidity being removed
                    # data: amount (uint128), amount0 (uint256), amount1 (uint256)
                    burn_liquidity_total += HexDecoder.decode_uint128(data, 0)
                    burn_amount0 += HexDecoder.decode_uint256(data, 32)
                    burn_amount1 += HexDecoder.decode_uint256(data, 64)
                    saw_burn = True
                    if not pool_address:
                        # Burn is emitted by the pool itself — its emitter
                        # address IS the pool. Capture once on the first
                        # Burn we see; subsequent Burns in a multicall close
                        # should be the same pool.
                        addr = log.get("address", "")
                        if isinstance(addr, bytes):
                            addr = "0x" + addr.hex()
                        if addr:
                            pool_address = str(addr).lower()

            if not (saw_collect or saw_burn):
                return None

            liquidity_removed = burn_liquidity_total if saw_burn else None

            # principal = burn amounts; fees = collect - burn when both are
            # in the same receipt (UV3 single-TX close path), clamped at
            # zero. On Sushi V3 the 3-TX bundle splits Burn and Collect into
            # separate receipts; a Burn-only receipt yields fees={0,0}, a
            # Collect-only receipt yields full collect as ``amount{0,1}_collected``
            # (we cannot disentangle without the burn).
            fees0 = max(collect_amount0 - burn_amount0, 0) if (saw_collect and saw_burn) else 0
            fees1 = max(collect_amount1 - burn_amount1, 0) if (saw_collect and saw_burn) else 0

            return LPCloseData(
                amount0_collected=collect_amount0 if saw_collect else burn_amount0,
                amount1_collected=collect_amount1 if saw_collect else burn_amount1,
                fees0=fees0,
                fees1=fees1,
                liquidity_removed=liquidity_removed,
                pool_address=pool_address,  # VIB-4198 / T12 — registry-mode close
            )

        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    # =============================================================================
    # Registry payload extraction — VIB-4198 (T12) SushiSwap V3 LP cutover
    # =============================================================================
    #
    # SushiSwap V3 is a clean Uniswap V3 fork. The registry payload shape is
    # identical to the UniV3 baseline (PRD §Registry Data Shape + T08 golden
    # contract), and so are the shape-only helpers
    # (``_open_payload_disagrees``, ``_build_close_receipt_payload``,
    # ``_merge_open_payload_fields``). We import-and-reuse those helpers from
    # ``almanak.framework.connectors.uniswap_v3.receipt_parser`` rather than
    # duplicating ~150 LoC of data-shape glue across forks — duplication
    # would let the two forks drift on Audit M1 cross-check semantics and on
    # the T08 contract.
    #
    # Sushi-specific bits (NPM address dict and DecreaseLiquidity emitter
    # filter) stay here because they depend on Sushi's NPM addresses, NOT
    # Uniswap's — the two forks deploy distinct NonfungiblePositionManagers
    # on every chain (e.g. Arbitrum: UV3 = 0xC36442b4..., Sushi V3 =
    # 0x2214A42d...).

    def _decreaseliquidity_token_id(self, receipt: dict[str, Any]) -> int | None:
        """Recover ``tokenId`` from a ``DecreaseLiquidity`` log on the close-side
        receipt.

        The SushiSwap V3 NPM emits ``DecreaseLiquidity(uint256 indexed
        tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)`` on
        every ``decreaseLiquidity()`` call (event signature is identical to
        Uniswap V3 — Sushi V3 is a clean fork). ``topics[1]`` is the
        indexed tokenId. Returns ``None`` if no such log is in the receipt
        or the emitter doesn't match the configured chain's Sushi NPM —
        the close-side identity is then derivable only from strategy-supplied
        state, which is the legacy path the registry cutover replaces.
        T12's caller treats ``None`` as "fall back to ``accounting_only``"
        with an INFO log; no ``Decimal("0")`` substitution.
        """
        logs = receipt.get("logs") or []
        if not logs:
            return None

        decrease_topic = EVENT_TOPICS["DecreaseLiquidity"].lower()
        position_manager = POSITION_MANAGER_ADDRESSES.get(self.chain, "").lower()
        if not position_manager:
            # Unknown chain — refuse rather than guess. Mirrors the
            # ``extract_lp_open_data`` fail-loud contract above.
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

            if first_topic != decrease_topic:
                continue
            if address != position_manager:
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

    def _nft_manager_address(self) -> str:
        """Return the canonical Sushi V3 NPM address for ``self.chain``, lowercased.

        The address is part of the ``physical_identity_hash`` input tuple
        (per T08 invariant #1). It is a parser-side configuration constant
        — NOT an off-chain RPC call — so the hash stays receipt-derivable
        from the receipt + parser config alone. Returns the empty string
        on unsupported chains rather than falling back to the Uniswap V3
        NPM (which is a different deployment); the caller treats empty as
        a path-applicability miss.
        """
        return POSITION_MANAGER_ADDRESSES.get(self.chain, "").lower()

    def extract_registry_payload_open(
        self,
        receipt: dict[str, Any],
        *,
        fee_tier: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the LP_OPEN ``position_registry.payload`` dict for Sushi V3.

        Reads :meth:`extract_lp_open_data` and composes the 8-key (or 11-key
        with ``fee_tier`` + token labels) shape ratified by PRD §Registry
        Data Shape and the T08 golden. Returns ``None`` when any of the
        load-bearing identity fields are missing — the caller treats that
        as "fall back to accounting_only", per CLAUDE.md "Empty ≠ zero" (a
        zero-substituted ``token_id`` would silently corrupt the
        ``physical_identity_hash``).

        Args:
            receipt: Transaction receipt dict with ``logs`` field.
            fee_tier: Optional pool fee tier (e.g. ``3000`` for 0.3%);
                forwarded from the intent's compile-time metadata. ``None``
                when unknown — the payload key stays absent rather than
                substituting ``0``.

        Returns:
            ``dict`` JSON-serializable with the canonical keys, OR
            ``None`` when the LP_OPEN data isn't extractable.
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
            # Sushi NPM not registered for this chain — refuse rather than
            # forge an identity tuple. The runner will fall back.
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
        """Build the LP_CLOSE ``position_registry.payload`` dict for Sushi V3.

        Mirrors the Uniswap V3 baseline (the shape contract is identical —
        Sushi V3 is a clean fork). Reads :meth:`extract_lp_close_data` and
        the close-side ``DecreaseLiquidity`` event for the NFT ``token_id``,
        then composes the 13-key shape that the T08 ``lp_close``
        ``expected_registry_row.json`` golden specifies.

        Audit M1 (CodeRabbit) contract: a real LP_CLOSE proves itself with
        DecreaseLiquidity on the receipt AND a Burn log carrying the pool
        address. A Collect-only receipt is NOT a close — it's a fee
        harvest. If we silently synthesized ``token_id`` / ``pool_address``
        from ``open_payload`` here, a malformed or fee-only close would
        produce a "successful" close payload with stale OPEN-side anchors,
        and the registry would mark a still-open NFT as closed (cutover
        spec D3.F6 silent-error class).

        The flow is:

        1. Decode close-side events (:meth:`extract_lp_close_data`) and the
           DecreaseLiquidity log (:meth:`_decreaseliquidity_token_id`).
        2. Verify the receipt-derived identity anchors are present and
           non-zero.
        3. Cross-check against ``open_payload`` if supplied — refuse on
           any disagreement (Uniswap V3's ``_open_payload_disagrees``).
        4. Compose the receipt-only payload (Uniswap V3's
           ``_build_close_receipt_payload``).
        5. Merge OPEN-time fields the close receipt cannot re-derive
           (Uniswap V3's ``_merge_open_payload_fields``) — ticks,
           OPEN-time amounts, original mint liquidity, fee tier, token
           labels.
        6. Apply the ``fee_tier`` argument if ``open_payload`` didn't
           carry one (``setdefault`` semantics — OPEN-side wins).

        Returns ``None`` when the close-side identity anchors (token_id +
        pool_address) cannot be derived OR cross-checks fail. The caller
        treats that as "fall back to accounting_only" with an INFO log
        (no zero substitution).
        """
        # Import the shape-only helpers from the Uniswap V3 baseline.
        # Sushi V3 is a clean fork — the data-shape contract is identical,
        # and duplicating these helpers would let Sushi drift from the
        # T08 golden and Audit M1 semantics on every Uniswap V3 update.
        from almanak.framework.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        lp_close = self.extract_lp_close_data(receipt)
        if lp_close is None:
            return None
        token_id = self._decreaseliquidity_token_id(receipt)
        if token_id is None or token_id <= 0:
            return None
        pool_address = (lp_close.pool_address or "").lower()
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
            # Sushi NPM not registered for this chain — but if we reached
            # this point ``_decreaseliquidity_token_id`` already required
            # the NPM to match an emitted log, so this is defensive only.
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
        return payload

    # =============================================================================
    # Fail-closed result variants (VIB-3159 / Blueprint 19)
    # =============================================================================

    def _strict_parse(self, receipt: dict[str, Any]) -> ExtractResult[Any] | None:
        """Run ``parse_receipt`` and short-circuit with ``ExtractError`` if it
        reports a crash.

        Returns ``None`` when parsing succeeded (caller should proceed), or an
        ``ExtractError`` variant when it did not. This is the strict
        counterpart to the legacy ``extract_*`` methods, which silently
        swallow exceptions and return ``None`` — making the "benign missing"
        and "crashed parsing" cases indistinguishable (VIB-3159).
        """
        try:
            parsed = self.parse_receipt(receipt)
        except Exception as exc:  # noqa: BLE001 — malformed receipt shape
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if not parsed.success:
            return ExtractError(error=parsed.error or "parse_receipt reported failure")
        return None

    def extract_lp_open_data_result(self, receipt: dict[str, Any]) -> ExtractResult[LPOpenData]:
        """Fail-closed variant of :meth:`extract_lp_open_data` — see VIB-3159.

        Distinguishes "no IncreaseLiquidity event" (benign — e.g. LP_OPEN
        that failed mid-bundle) from "parser crashed". Both are returned
        as ``None`` by the legacy method, which forces the enricher to
        treat genuine parse failures as missing data — exactly the
        ghost-position class of bug VIB-3159 addresses.
        """
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            logs = receipt.get("logs", [])
        except Exception as exc:  # noqa: BLE001 — malformed receipt shape
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if not logs:
            return ExtractMissing(reason="no logs in receipt")
        try:
            value = self.extract_lp_open_data(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no IncreaseLiquidity event from SushiSwap V3 position manager")
        return ExtractOk(value=value)

    # Backward compatibility methods
    def is_sushiswap_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known SushiSwap V3 event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known SushiSwap V3 event
        """
        return self.registry.is_known_event(_normalize_topic(topic))

    def get_event_type(self, topic: str | bytes) -> SushiSwapV3EventType:
        """Get the event type for a topic.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            Event type or UNKNOWN
        """
        return self.registry.get_event_type_from_topic(_normalize_topic(topic)) or SushiSwapV3EventType.UNKNOWN


__all__ = [
    "SushiSwapV3ReceiptParser",
    "SushiSwapV3Event",
    "SushiSwapV3EventType",
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
