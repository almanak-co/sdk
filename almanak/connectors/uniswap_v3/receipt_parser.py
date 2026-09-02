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

from almanak.connectors._strategy_base import v3_registry_payload
from almanak.connectors._strategy_base.base import EventRegistry, HexDecoder
from almanak.connectors._strategy_base.lp_leg_identity import (
    currencies_for_amounts,
    log_emitter_address,
    transfers_by_token,
)
from almanak.connectors._strategy_base.v3_fork_receipt_parser import (
    V3_STANDARD_LP_DATA_WORDS,
    V3_STANDARD_LP_TOPIC_COUNTS,
    V3_STANDARD_TRANSFER_LAYOUTS,
    V3ForkReceiptParser,
    V3ForkSpec,
)
from almanak.framework.execution.events import SwapResultPayload
from almanak.framework.execution.extract_result import ExtractResult

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import (
        LPCloseData,
        LPOpenData,
        ProtocolFees,
        SwapAmounts,
    )
from almanak.framework.utils.log_formatters import (
    format_gas_cost,
    format_slippage_bps,
    format_tx_hash,
)

logger = logging.getLogger(__name__)


EVENT_TOPICS: dict[str, str] = {
    "Swap": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
    "Mint": "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
    "Burn": "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
    "Collect": "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0",
    "Flash": "0xbdbdb71d7860376ba52b25a5028beea23581364a40522f6bcfb86bb1f2dca633",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
    "IncreaseLiquidity": "0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f",
    # Its indexed tokenId proves close identity; Collect alone may be fee-only.
    "DecreaseLiquidity": "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4",
}

# Fork-specific position managers differ from canonical Uniswap deployments.
POSITION_MANAGER_ADDRESSES: dict[str, str] = {
    "ethereum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "arbitrum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "optimism": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "base": "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
    "polygon": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "avalanche": "0x655C406EBFa14EE2006250925e54ec43AD184f8B",
    "bnb": "0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613",
    "bsc": "0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613",
    "mantle": "0x218bf598D1453383e2F4AA7b14fFB9BfB102D637",
    "monad": "0x7197E214c0b767cFB76Fb734ab638E2c192F4E53",
    "xlayer": "0x315e413A11AB0df498eF83873012430ca36638Ae",
    "zerog": "0x8F67A30Ed186e3E1f6504c6dE3239Ef43A2e0d72",
    "robinhood": "0x73991a25C818Bf1f1128dEAaB1492D45638DE0D3",
}

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
ZERO_ADDRESS_PADDED = "0x" + "0" * 64

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

SWAP_EVENT_TOPIC = EVENT_TOPICS["Swap"]


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

UNISWAP_V3_RECEIPT_SPEC = V3ForkSpec(
    protocol_name="Uniswap V3",
    event_topics=EVENT_TOPICS,
    event_name_to_type={
        **EVENT_NAME_TO_TYPE,
        "IncreaseLiquidity": UniswapV3EventType.UNKNOWN,
        "DecreaseLiquidity": UniswapV3EventType.UNKNOWN,
    },
    position_manager_addresses=POSITION_MANAGER_ADDRESSES,
    default_position_manager="0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    strict_decode_fields={
        "Swap": frozenset(
            {
                "amount0",
                "amount1",
                "sqrt_price_x96",
                "liquidity",
                "tick",
                "pool_address",
            }
        )
    },
    strict_topic_counts={"Swap": 3, **V3_STANDARD_LP_TOPIC_COUNTS},
    strict_data_words={"Swap": 5, **V3_STANDARD_LP_DATA_WORDS},
    strict_event_layouts=V3_STANDARD_TRANSFER_LAYOUTS,
)


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
    token_in_decimals_resolved: bool = True
    token_out_decimals_resolved: bool = True

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
            "token_in_decimals_resolved": self.token_in_decimals_resolved,
            "token_out_decimals_resolved": self.token_out_decimals_resolved,
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
            token_in_decimals_resolved=data.get("token_in_decimals_resolved", True),
            token_out_decimals_resolved=data.get("token_out_decimals_resolved", True),
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


class UniswapV3ReceiptParser(V3ForkReceiptParser):
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
            "lp_open_data",
            "lp_close_data",
            "protocol_fees",
        }
    )
    V3_FORK_SPEC = UNISWAP_V3_RECEIPT_SPEC

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

        self.registry = EventRegistry(
            dict(self.v3_fork_spec.event_topics),
            dict(self.v3_fork_spec.event_name_to_type),
        )

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

        if self.token0_symbol and self.token0_decimals is None:
            _, decimals = self._resolve_token_info(self.token0_symbol)
            if decimals is not None:
                self.token0_decimals = decimals
        if self.token1_symbol and self.token1_decimals is None:
            _, decimals = self._resolve_token_info(self.token1_symbol)
            if decimals is not None:
                self.token1_decimals = decimals

        # Fallback scaling remains explicitly unmeasured for accounting.
        self._token0_decimals_resolved = self.token0_decimals is not None
        self._token1_decimals_resolved = self.token1_decimals is not None
        if self.token0_decimals is None:
            self.token0_decimals = 18
        if self.token1_decimals is None:
            self.token1_decimals = 18

    def parse_receipt(
        self,
        receipt: dict[str, Any],
        quoted_amount_out: int | None = None,
        *,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
    ) -> ParseResult:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dict
            quoted_amount_out: Expected output amount for slippage calculation
            swap_token_meta: Optional compiler-supplied token metadata (VIB-3164).
                Forwarded to ``_build_swap_result`` so decimals resolve when the
                TokenResolver misses or Transfer events cannot be classified.
                Shape: ``{"token_in": {"address": ..., "symbol": ..., "decimals": ...},
                "token_out": {...}}``.

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

            # Reverted receipts commonly have no logs, so status must be checked first.
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

            events: list[UniswapV3Event] = []
            swap_events: list[SwapEventData] = []
            transfer_events: list[TransferEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    if parsed_event.event_type == UniswapV3EventType.SWAP:
                        swap_data = self._parse_swap_event(parsed_event)
                        if swap_data:
                            swap_events.append(swap_data)

                    elif parsed_event.event_type == UniswapV3EventType.TRANSFER:
                        transfer_data = self._parse_transfer_event(parsed_event)
                        if transfer_data:
                            transfer_events.append(transfer_data)

            swap_result = None
            if swap_events:
                swap_result = self._build_swap_result(
                    swap_events[0],
                    transfer_events,
                    quoted_amount_out,
                    swap_token_meta=swap_token_meta,
                    single_swap=len(swap_events) == 1,
                )

            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(receipt.get("gasUsed"))
            if swap_result:
                slippage_fmt = format_slippage_bps(swap_result.slippage_bps) if swap_result.slippage_bps else "N/A"
                in_fmt = f"{swap_result.amount_in_decimal:.4f}" if swap_result.amount_in_decimal is not None else "?"
                out_fmt = f"{swap_result.amount_out_decimal:.4f}" if swap_result.amount_out_decimal is not None else "?"
                logger.info(
                    f"🔍 Parsed Uniswap V3 swap: {in_fmt} {swap_result.token_in_symbol or 'token0'} "
                    f"→ {out_fmt} {swap_result.token_out_symbol or 'token1'}, "
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

    def _create_v3_event(
        self,
        *,
        event_type: UniswapV3EventType,
        event_name: str,
        log_index: int,
        tx_hash: str,
        block_number: int,
        contract_address: str,
        decoded_data: dict[str, Any],
        raw_topics: list[str],
        raw_data: str,
    ) -> UniswapV3Event:
        """Create a Uniswap event from the shared V3 log template."""
        return UniswapV3Event(
            event_type=event_type,
            event_name=event_name,
            log_index=log_index,
            transaction_hash=tx_hash,
            block_number=block_number,
            contract_address=contract_address,
            data=decoded_data,
            raw_topics=raw_topics,
            raw_data=raw_data,
        )

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

    def _resolve_tokens_from_transfers(
        self,
        transfer_events: list[TransferEventData],
        swap_event: SwapEventData,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
        single_swap: bool = True,
    ) -> dict[str, Any]:
        """Resolve token addresses and decimals from Transfer events.

        When the parser was constructed without token info (e.g., by the ResultEnricher),
        decimals default to 18 which is wrong for tokens like USDC (6 decimals).
        This method extracts token addresses from the Transfer events in the swap
        transaction and resolves their actual decimals via the TokenResolver.

        Returns a dict of per-receipt overrides (token0_address, token0_decimals, etc.)
        without mutating parser state, so cached parser instances stay clean across receipts.
        Branch 1 (addresses pre-set, just need decimals) writes to self because the correct
        decimals for those addresses are stable. Branch 2 (addresses inferred) only returns
        local overrides. Branch 3 (VIB-3164) falls back to compiler-supplied token metadata
        when Transfer classification produced nothing.

        Args:
            transfer_events: ERC-20 Transfer events from the receipt.
            swap_event: The Swap event whose pool address is used for classification.
            swap_token_meta: Optional compiler-supplied token metadata with shape
                ``{"token_in": {"address": ..., "symbol": ..., "decimals": ...},
                   "token_out": {...}}``. Either slot may be absent. Compiler hints
                take precedence over the TokenResolver in the ``resolved`` map
                (Branch 2b), and serve as a direction-based fallback when Transfer
                classification produces no addresses (Branch 3).
            single_swap: True when the receipt contains exactly one Swap event.
                Branch 3 is restricted to single-swap receipts: in a multi-hop
                receipt the first Swap event's output is an intermediate token,
                not the intent's to_token, so the compiler's to_token hint would
                map to the wrong pool slot.
        """
        # Inferred metadata stays receipt-local so cached parsers cannot leak token state.
        overrides: dict[str, Any] = {}

        needs_token0 = not self._token0_decimals_resolved
        needs_token1 = not self._token1_decimals_resolved
        if not needs_token0 and not needs_token1:
            return overrides

        hint_by_addr = self._build_hint_map(swap_token_meta)
        pool_address = swap_event.pool_address.lower() if swap_event.pool_address else ""

        input_addrs, output_addrs = self._classify_transfers_by_pool_direction(transfer_events, pool_address)
        resolved = self._build_resolved_token_map(set(input_addrs + output_addrs), hint_by_addr)
        needs_token0, needs_token1 = self._apply_decimals_to_known_slots(resolved, needs_token0, needs_token1)

        slot0_candidates, slot1_candidates = (
            (input_addrs, output_addrs) if swap_event.token0_is_input else (output_addrs, input_addrs)
        )

        if needs_token0 and not self.token0_address:
            entries = self._infer_slot_override(slot0_candidates, resolved, "token0")
            if entries:
                overrides.update(entries)
                needs_token0 = False
        if needs_token1 and not self.token1_address:
            entries = self._infer_slot_override(slot1_candidates, resolved, "token1")
            if entries:
                overrides.update(entries)
                needs_token1 = False

        overrides.update(
            self._meta_fallback_overrides(
                swap_token_meta, single_swap, swap_event.token0_is_input, needs_token0, needs_token1, overrides
            )
        )
        return overrides

    @staticmethod
    def _classify_transfers_by_pool_direction(
        transfer_events: list[TransferEventData],
        pool_address: str,
    ) -> tuple[list[str], list[str]]:
        """Returns (input_token_addrs, output_token_addrs) relative to the pool.

        In Uniswap V3: amount0 > 0 means token0 was sent TO the pool (input),
        amount1 < 0 means token1 was received FROM the pool (output), and vice versa.
        The corresponding ERC-20 Transfer events carry the actual token contract addresses.
        """
        input_token_addrs: list[str] = []
        output_token_addrs: list[str] = []
        if not pool_address:
            return input_token_addrs, output_token_addrs
        for transfer in transfer_events:
            addr = transfer.token_address.lower() if transfer.token_address else ""
            if not addr:
                continue
            if transfer.to_addr.lower() == pool_address:
                input_token_addrs.append(addr)
            elif transfer.from_addr.lower() == pool_address:
                output_token_addrs.append(addr)
        return input_token_addrs, output_token_addrs

    def _build_resolved_token_map(
        self,
        addrs: set[str],
        hint_by_addr: dict[str, tuple[str, int]],
    ) -> dict[str, tuple[str, int]]:
        """Address -> (symbol, decimals); compiler hints win over the TokenResolver.

        Compiler-supplied hints are intent-specific compile-time facts and cover
        resolver gaps. Addresses where decimals cannot be resolved are excluded.
        """
        resolved: dict[str, tuple[str, int]] = {}
        for addr in addrs:
            if addr in hint_by_addr:
                resolved[addr] = hint_by_addr[addr]
                continue
            symbol, decimals = self._resolve_token_info(addr)
            if decimals is not None:
                resolved[addr] = (symbol, decimals)
        return resolved

    def _apply_decimals_to_known_slots(
        self,
        resolved: dict[str, tuple[str, int]],
        needs_token0: bool,
        needs_token1: bool,
    ) -> tuple[bool, bool]:
        """Branch 1: write decimals to self for pre-set addresses; return updated needs flags.

        Safe to write to self because the token addresses are pre-set and stable.
        """
        for addr, (symbol, decimals) in resolved.items():
            if needs_token0 and self.token0_address and self.token0_address.lower() == addr:
                self.token0_decimals = decimals
                self._token0_decimals_resolved = True
                if symbol and not self.token0_symbol:
                    self.token0_symbol = symbol
                logger.debug(f"Resolved token0 decimals from Transfer: {symbol} = {decimals}")
                needs_token0 = False
            elif needs_token1 and self.token1_address and self.token1_address.lower() == addr:
                self.token1_decimals = decimals
                self._token1_decimals_resolved = True
                if symbol and not self.token1_symbol:
                    self.token1_symbol = symbol
                logger.debug(f"Resolved token1 decimals from Transfer: {symbol} = {decimals}")
                needs_token1 = False
        return needs_token0, needs_token1

    @staticmethod
    def _infer_slot_override(
        candidates: list[str],
        resolved: dict[str, tuple[str, int]],
        prefix: str,
    ) -> dict[str, Any]:
        """Branch 2: first candidate present in resolved wins; {} when none match.

        Returns local overrides instead of mutating self, so cached parsers stay
        clean. Uses the caller-supplied candidate list (ordered by transfer direction)
        to determine which address maps to the given pool slot.
        """
        for addr in candidates:
            if addr in resolved:
                symbol, decimals = resolved[addr]
                logger.debug(f"Inferred {prefix} from Transfer: {symbol} ({addr}) = {decimals}")
                return {f"{prefix}_address": addr, f"{prefix}_decimals": decimals, f"{prefix}_symbol": symbol or ""}
        return {}

    def _meta_fallback_overrides(
        self,
        swap_token_meta: dict[str, dict[str, Any]] | None,
        single_swap: bool,
        token0_is_input: bool,
        needs_token0: bool,
        needs_token1: bool,
        overrides: dict[str, Any],
    ) -> dict[str, Any]:
        """Branch 3: compiler-metadata direction fallback; returns new entries only.

        Fires only when Transfer classification produced nothing for a slot.
        The compiler's from_token IS the swap input and to_token IS the swap output,
        and ``token0_is_input`` maps them onto pool slots. Restricted to single-swap
        receipts: in a multi-hop receipt the first Swap event's output is an
        intermediate token, not the intent's to_token. Writes overrides only —
        never ``self``. Reads ``overrides`` for dedup checks but never mutates it.
        """
        # Intent direction cannot identify an intermediate pool in a multi-hop receipt.
        if not swap_token_meta or not single_swap or not (needs_token0 or needs_token1):
            return {}
        token_in_meta = swap_token_meta.get("token_in")
        token_out_meta = swap_token_meta.get("token_out")
        slot0_meta, slot1_meta = (token_in_meta, token_out_meta) if token0_is_input else (token_out_meta, token_in_meta)
        entries: dict[str, Any] = {}
        if needs_token0 and "token0_decimals" not in overrides:
            entries.update(self._meta_override_for_slot(slot0_meta, self.token0_address, "token0"))
        if needs_token1 and "token1_decimals" not in overrides:
            entries.update(self._meta_override_for_slot(slot1_meta, self.token1_address, "token1"))
        return entries

    @staticmethod
    def _meta_override_for_slot(
        meta: dict[str, Any] | None,
        existing_address: str | None,
        prefix: str,
    ) -> dict[str, Any]:
        """Build ``{prefix}_address/_decimals/_symbol`` overrides from one hint.

        Returns ``{}`` when the hint is missing/malformed, or when the parser
        already knows a DIFFERENT address for this slot (a mismatch means the
        hint does not describe this pool — do not apply it).
        """
        if not isinstance(meta, dict):
            return {}
        address = meta.get("address")
        decimals = meta.get("decimals")
        if not address or decimals is None:
            return {}
        addr = str(address).lower()
        if existing_address and existing_address.lower() != addr:
            logger.debug(
                "Compiler token hint %s does not match pre-set %s address %s; skipping",
                addr,
                prefix,
                existing_address,
            )
            return {}
        try:
            decimals_int = int(decimals)
        except (TypeError, ValueError):
            return {}
        return {
            f"{prefix}_address": addr,
            f"{prefix}_decimals": decimals_int,
            f"{prefix}_symbol": str(meta.get("symbol") or ""),
        }

    def _build_swap_result(
        self,
        swap_event: SwapEventData,
        transfer_events: list[TransferEventData],
        quoted_amount_out: int | None,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
        single_swap: bool = True,
    ) -> ParsedSwapResult:
        """Build high-level swap result from events.

        Args:
            swap_event: The Swap event data
            transfer_events: List of Transfer events
            quoted_amount_out: Expected output for slippage calc
            swap_token_meta: Optional compiler-supplied token metadata (VIB-3164).
                Shape: ``{"token_in": {"address": ..., "symbol": ..., "decimals": ...},
                "token_out": {...}}``. Threaded through to
                ``_resolve_tokens_from_transfers`` so decimals resolve when the
                TokenResolver misses or Transfer events cannot be classified.
            single_swap: True when the receipt contains exactly one Swap event;
                restricts Branch 3 of ``_resolve_tokens_from_transfers`` to
                single-hop receipts.

        Returns:
            ParsedSwapResult with full swap details
        """
        overrides = self._resolve_tokens_from_transfers(
            transfer_events, swap_event, swap_token_meta=swap_token_meta, single_swap=single_swap
        )

        t0_addr = overrides.get("token0_address", self.token0_address) or ""
        t0_symbol = overrides.get("token0_symbol", self.token0_symbol) or ""
        t0_decimals = overrides.get("token0_decimals", self.token0_decimals)
        t1_addr = overrides.get("token1_address", self.token1_address) or ""
        t1_symbol = overrides.get("token1_symbol", self.token1_symbol) or ""
        t1_decimals = overrides.get("token1_decimals", self.token1_decimals)

        # An 18-decimal fallback remains unresolved unless this receipt supplied the real value.
        t0_unresolved = not self._token0_decimals_resolved and "token0_decimals" not in overrides
        t1_unresolved = not self._token1_decimals_resolved and "token1_decimals" not in overrides
        if t0_unresolved or t1_unresolved:
            logger.warning(
                "Token decimals unresolved after Transfer analysis "
                f"(token0={'unresolved' if t0_unresolved else 'ok'}, "
                f"token1={'unresolved' if t1_unresolved else 'ok'}). "
                "Decimal amounts may be incorrect for non-18-decimal tokens "
                "(VIB-3164 deferred: see receipt_parser __init__ note)."
            )

        if swap_event.token0_is_input:
            token_in = t0_addr
            token_out = t1_addr
            token_in_symbol = t0_symbol
            token_out_symbol = t1_symbol
            token_in_decimals = t0_decimals
            token_out_decimals = t1_decimals
            token_in_decimals_resolved = not t0_unresolved
            token_out_decimals_resolved = not t1_unresolved
        else:
            token_in = t1_addr
            token_out = t0_addr
            token_in_symbol = t1_symbol
            token_out_symbol = t0_symbol
            token_in_decimals = t1_decimals
            token_out_decimals = t0_decimals
            token_in_decimals_resolved = not t1_unresolved
            token_out_decimals_resolved = not t0_unresolved

        amount_in = swap_event.amount_in
        amount_out = swap_event.amount_out

        assert token_in_decimals is not None, "token_in_decimals must not be None"
        assert token_out_decimals is not None, "token_out_decimals must not be None"
        amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
        amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**token_out_decimals)

        if amount_in_decimal > 0:
            effective_price = amount_out_decimal / amount_in_decimal
        else:
            effective_price = Decimal("0")

        # Basis points are (expected - actual) / expected * 10_000.
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
            sqrt_price_x96_after=swap_event.sqrt_price_x96,
            tick_after=swap_event.tick,
            token_in_decimals_resolved=token_in_decimals_resolved,
            token_out_decimals_resolved=token_out_decimals_resolved,
        )

    # Result variants keep parser failures distinct from missing events.
    def extract_position_id_result(self, receipt: dict[str, Any]) -> ExtractResult[int]:
        """Fail-closed variant of :meth:`extract_position_id` — see VIB-3159."""
        return self._wrap_extract(
            self.extract_position_id,
            receipt,
            "no Mint Transfer event from position manager",
        )

    def extract_swap_amounts_result(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
    ) -> ExtractResult[SwapAmounts]:
        """Fail-closed variant of :meth:`extract_swap_amounts` — see VIB-3159.

        VIB-3203: forwards ``expected_out`` so realized slippage is populated
        when the framework supplies the compiler's pre-slippage quote.
        VIB-3164: forwards ``swap_token_meta`` so compiler-supplied token
        decimals resolve when the TokenResolver misses.
        """
        return self._wrap_extract(
            self.extract_swap_amounts,
            receipt,
            "no Swap event in receipt",
            parse_kwargs={"swap_token_meta": swap_token_meta},
            expected_out=expected_out,
            swap_token_meta=swap_token_meta,
        )

    def extract_lp_close_data_result(self, receipt: dict[str, Any]) -> ExtractResult[LPCloseData]:
        """Fail-closed variant of :meth:`extract_lp_close_data` — see VIB-3159."""
        return self._wrap_extract(
            self.extract_lp_close_data,
            receipt,
            "no Collect/Burn event in receipt",
        )

    def extract_lp_open_data_result(self, receipt: dict[str, Any]) -> ExtractResult[LPOpenData]:
        """Fail-closed variant of :meth:`extract_lp_open_data` — see VIB-3159.

        Distinguishes "no IncreaseLiquidity event" (benign — e.g. LP_OPEN
        that failed mid-bundle) from "parser crashed". Both are returned
        as ``None`` by the legacy method, which forces the enricher to
        treat genuine parse failures as missing data — exactly the
        ghost-position class of bug VIB-3159 addresses.
        """
        return self._wrap_extract(
            self.extract_lp_open_data,
            receipt,
            "no IncreaseLiquidity event from position manager",
        )

    def extract_liquidity_result(self, receipt: dict[str, Any]) -> ExtractResult[int]:
        """Fail-closed variant of :meth:`extract_liquidity` — see VIB-3159."""
        return self._wrap_extract(
            self.extract_liquidity,
            receipt,
            "no Mint event in receipt",
        )

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:  # noqa: C901
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

            # Unknown chains retain the canonical NPM fallback used by this parser.
            position_manager = POSITION_MANAGER_ADDRESSES.get(self.chain, "").lower()
            if not position_manager:
                position_manager = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88".lower()

            transfer_topic = EVENT_TOPICS["Transfer"].lower()

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

                if address != position_manager:
                    continue

                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()
                if not first_topic.startswith("0x"):
                    first_topic = "0x" + first_topic

                if first_topic != transfer_topic:
                    continue

                from_topic = topics[1]
                if isinstance(from_topic, bytes):
                    from_topic = "0x" + from_topic.hex()
                from_topic = str(from_topic).lower()
                if not from_topic.startswith("0x"):
                    from_topic = "0x" + from_topic

                if from_topic != ZERO_ADDRESS_PADDED:
                    continue

                # ERC-721 tokenId is indexed in topics[3].
                token_id_topic = topics[3]
                if isinstance(token_id_topic, bytes):
                    token_id_topic = "0x" + token_id_topic.hex()
                token_id_topic = str(token_id_topic)
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

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
    ) -> SwapAmounts | None:
        """Extract swap amounts from a transaction receipt.

        This method is called by the ResultEnricher to automatically populate
        ExecutionResult.swap_amounts for SWAP intents.

        Args:
            receipt: Transaction receipt dict with 'logs' field
            expected_out: VIB-3203 — pre-slippage-discount quote in human
                (Decimal) units, sourced from ``ActionBundle.metadata["expected_output_human"]``.
                When provided, realized ``slippage_bps`` is computed as
                ``(expected_out - amount_out_decimal) / expected_out * 10_000``.
                When absent, ``slippage_bps`` stays ``None`` (legacy behavior).
            swap_token_meta: VIB-3164 — compiler-supplied token metadata.
                Shape: ``{"token_in": {"address": ..., "symbol": ..., "decimals": ...},
                "token_out": {...}}``. Forwarded to ``parse_receipt`` so the parser
                can resolve decimals when the TokenResolver misses or Transfer events
                cannot be classified against the pool address.

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
            parse_result = self.parse_receipt(receipt, swap_token_meta=swap_token_meta)

            if not parse_result.swap_result:
                num_events = len(parse_result.events)
                swap_count = len(parse_result.swap_events)
                logger.debug(
                    f"extract_swap_amounts: no swap_result (events={num_events}, "
                    f"swap_events={swap_count}, chain={self.chain})"
                )
                return None

            sr = parse_result.swap_result

            slippage_bps = sr.slippage_bps if sr.slippage_bps else None
            # expected_out and amount_out_decimal share human token units.
            if expected_out is not None and expected_out > 0 and sr.amount_out_decimal > 0:
                realized_slippage = (expected_out - sr.amount_out_decimal) / expected_out
                slippage_bps = int(realized_slippage * Decimal(10_000))

            from almanak.framework.execution.extracted_data import SlippageSource

            slippage_source = SlippageSource.RECEIPT_DECODED if slippage_bps is not None else SlippageSource.NONE

            # Ledger token identity must resolve to symbols rather than contract addresses.
            return SwapAmounts(
                amount_in=sr.amount_in,
                amount_out=sr.amount_out,
                amount_in_decimal=sr.amount_in_decimal,
                amount_out_decimal=sr.amount_out_decimal,
                effective_price=sr.effective_price,
                slippage_bps=slippage_bps,
                expected_out_decimal=expected_out,
                token_in=sr.token_in_symbol or sr.token_in,
                token_out=sr.token_out_symbol or sr.token_out,
                amount_in_decimal_resolved=getattr(sr, "token_in_decimals_resolved", True),
                amount_out_decimal_resolved=getattr(sr, "token_out_decimals_resolved", True),
                slippage_source=slippage_source,
            )

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

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

                # Indexed int24 values are two's-complement and sign-extended to 256 bits.
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
        """Extract liquidity from a Uniswap V3 Pool ``Mint`` event.

        Pool Mint signature (only the non-indexed fields land in ``data``)::

            event Mint(
                address sender,             // non-indexed → data[ 0..32 ]
                address indexed owner,
                int24   indexed tickLower,
                int24   indexed tickUpper,
                uint128 amount,             // non-indexed → data[32..64 ]  (the liquidity)
                uint256 amount0,            // non-indexed → data[64..96 ]
                uint256 amount1             // non-indexed → data[96..128]
            );

        Sister parsers (``sushiswap_v3``, ``pancakeswap_v3``) already read
        at offset 32 — this method used to read at offset 0 and surface the
        sender-address slot as "liquidity" (a ~50-digit garbage uint), which
        leaked into ``extracted_data['liquidity']`` for every LP_OPEN
        (VIB-4395). The fix here aligns with the ABI; the canonical
        per-position liquidity is also available via
        :meth:`extract_lp_open_data` (NFT manager ``IncreaseLiquidity``).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Liquidity amount if a Pool Mint event is found, ``None`` otherwise.
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

                # Pool Mint slots are sender@0, liquidity@32, amount0@64, amount1@96.
                liquidity = HexDecoder.decode_uint128(data, 32)
                return liquidity

            return None

        except Exception as e:
            logger.warning(f"Failed to extract liquidity: {e}")
            return None

    def extract_lp_open_data(self, receipt: dict[str, Any]) -> LPOpenData | None:  # noqa: C901
        """Extract LP open data from a transaction receipt.

        Looks for ``IncreaseLiquidity`` events emitted by the Uniswap V3
        NonfungiblePositionManager (and its forks listed in
        ``POSITION_MANAGER_ADDRESSES``) when an LP position is opened or
        topped up. The event signature is::

            IncreaseLiquidity(
                uint256 indexed tokenId,
                uint128 liquidity,
                uint256 amount0,
                uint256 amount1,
            )

        Layout (after VIB-3658 / April 30 audit item #4):

        - ``topics[0]``: keccak topic0 (constant)
        - ``topics[1]``: NFT ``tokenId`` (indexed uint256)
        - ``data``: ``liquidity`` (uint128, padded to 32 bytes)
            + ``amount0`` (uint256) + ``amount1`` (uint256)

        Behavior contract:

        - Returns ``LPOpenData`` populated with the raw on-chain ints
          (``position_id``, ``liquidity``, ``amount0``, ``amount1``).
          The accounting handler (``lp_handler.py``) is responsible for
          decimal-scaling and USD valuation — this parser stays raw so
          tests don't have to mock the token resolver and the gateway
          price oracle.
        - Returns ``None`` when no ``IncreaseLiquidity`` log is present
          (e.g. an LP_OPEN that failed mid-bundle, or a non-NPM contract
          path) — never raises.
        - The ``IncreaseLiquidity`` event is emitted by the NPM for
          ``mint()`` AND ``increaseLiquidity()`` calls. We accept either:
          a fresh-mint NPM emits the same event right after the ERC-721
          ``Transfer(0x0, owner, tokenId)``.

        Args:
            receipt: Transaction receipt dict with 'logs' field.

        Returns:
            ``LPOpenData`` if an ``IncreaseLiquidity`` event is present,
            ``None`` otherwise.
        """
        from almanak.framework.execution.extracted_data import LPOpenData

        # Let parse failures propagate so result variants can distinguish missing events.
        logs = receipt.get("logs") or []
        if not logs:
            return None

        increase_topic = EVENT_TOPICS["IncreaseLiquidity"].lower()

        # Unknown chains retain the canonical NPM fallback used by this parser.
        position_manager = POSITION_MANAGER_ADDRESSES.get(self.chain, "").lower()
        if not position_manager:
            position_manager = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88".lower()

        mint_topic = EVENT_TOPICS["Mint"].lower()
        # Pair IncreaseLiquidity with the latest preceding NPM-owned Pool Mint.
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

            if first_topic == mint_topic and len(topics) >= 4:
                if self._mint_owner_matches_npm(topics, position_manager):
                    last_npm_mint = log
                continue

            if address != position_manager:
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
                # A later matching log may still be valid.
                continue

            normalized = HexDecoder.normalize_hex(data)
            if not normalized or normalized == "0x":
                continue

            # IncreaseLiquidity slots are liquidity@0, amount0@32, amount1@64.
            liquidity = HexDecoder.decode_uint128(normalized, 0)
            amount0 = HexDecoder.decode_uint256(normalized, 32)
            amount1 = HexDecoder.decode_uint256(normalized, 64)

            tick_lower, tick_upper = self._ticks_from_mint(last_npm_mint)

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
            # Only a Swap from the paired pool can supply post-open state.
            current_tick = self._current_tick_from_swap_event(logs, pool_address)

            if current_tick is None:
                logger.debug(
                    f"Extracted LP open data: tokenId={token_id} liquidity={liquidity} "
                    f"amount0={amount0} amount1={amount1} ticks=[{tick_lower}, {tick_upper}] "
                    f"current_tick=None (no Swap event in receipt; framework slot0 fallback will resolve)"
                )
            else:
                logger.info(
                    f"Extracted LP open data: tokenId={token_id} liquidity={liquidity} "
                    f"amount0={amount0} amount1={amount1} ticks=[{tick_lower}, {tick_upper}] "
                    f"current_tick={current_tick}"
                )
            # Amounts use pool slot order, which may differ from user-facing labels.
            currency0, currency1 = currencies_for_amounts(
                transfers_by_token(logs, chain=self.chain, to_address=pool_address) if pool_address else {},
                amount0,
                amount1,
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
                position_hash=None,  # V3 lot identity is the NFT tokenId; hashes are V4-only.
                currency0=currency0,
                currency1=currency1,
            )

        return None

    @staticmethod
    def _mint_owner_matches_npm(topics: list[Any], npm_address: str) -> bool:
        """Return True iff the Mint event's ``owner`` indexed topic == NPM."""
        if len(topics) < 2:
            return False
        owner_topic = topics[1]
        if isinstance(owner_topic, bytes):
            owner_topic = "0x" + owner_topic.hex()
        owner_topic = str(owner_topic).lower()
        try:
            # Indexed addresses are right-aligned in 32-byte topics.
            owner_addr = "0x" + owner_topic.removeprefix("0x").rjust(64, "0")[-40:]
        except Exception:
            return False
        return owner_addr == npm_address.lower()

    @staticmethod
    def _ticks_from_mint(mint_log: dict[str, Any] | None) -> tuple[int | None, int | None]:
        """Decode (tickLower, tickUpper) from a Pool Mint log, or (None, None).

        Mint(address sender, address indexed owner, int24 indexed tickLower,
             int24 indexed tickUpper, uint128 amount, uint256 amount0,
             uint256 amount1) — ticks are at topics[2] and topics[3].
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
                # Indexed int24 values are two's-complement and sign-extended to 256 bits.
                return HexDecoder.decode_int24(str(topic), 0)
            except Exception:
                return None

        return (_decode(topics[2]), _decode(topics[3]))

    @staticmethod
    def _current_tick_from_swap_event(logs: list[Any], pool_address: str) -> int | None:
        """Find a Swap event from ``pool_address`` in ``logs`` and decode its tick.

        VIB-3887. The Uniswap V3 Pool Swap event signature is::

            event Swap(address indexed sender, address indexed recipient,
                       int256 amount0, int256 amount1, uint160 sqrtPriceX96,
                       uint128 liquidity, int24 tick)

        Layout: topics[0] = signature, topics[1] = sender, topics[2] =
        recipient. The non-indexed payload in ``data`` is
        ``amount0 (32) | amount1 (32) | sqrtPriceX96 (32) | liquidity (32)
        | tick (32, int24 right-aligned)``. We grab the last 32-byte slot.

        Returns ``None`` when no matching Swap is present (pure NPM mint
        with pre-balanced inputs) — caller leaves ``current_tick=None``.
        """
        if not pool_address:
            return None
        swap_topic = SWAP_EVENT_TOPIC.lower()
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
            if str(address).lower() != pool_address:
                continue
            if not topics:
                continue
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            if str(first_topic).lower() != swap_topic:
                continue
            latest_swap_log = log  # Latest Swap carries the receipt's final pool state.

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
            # Swap tick is the fifth 32-byte data slot.
            return HexDecoder.decode_int24(normalized, 128)
        except Exception:
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> LPCloseData | None:
        """Extract LP close data from transaction receipt.

        Decodes the Uniswap V3 LP close pattern: ``decreaseLiquidity`` emits
        ``Burn`` (which carries the principal amounts), and ``collect``
        emits ``Collect`` (which carries principal PLUS earned fees). The
        accrued fees are the difference between the two.

        Burn event: Burn(address indexed owner, int24 indexed tickLower,
                         int24 indexed tickUpper, uint128 amount,
                         uint256 amount0, uint256 amount1)
        - data layout: amount (uint128, left-padded to 32B)
                       ‖ amount0 (uint256) ‖ amount1 (uint256)

        Collect event: Collect(address indexed owner, address recipient,
                               int24 indexed tickLower, int24 indexed tickUpper,
                               uint128 amount0, uint128 amount1)
        - ``owner``, ``tickLower``, ``tickUpper`` are indexed (3 topics + topic0).
          ``recipient`` is **non-indexed** — it occupies the first 32-byte data
          slot, so amount0/amount1 start at offsets 32 and 64, not 0 and 32.
        - data layout: recipient (address, left-padded to 32B)
                       ‖ amount0 (uint128, left-padded to 32B)
                       ‖ amount1 (uint128, left-padded to 32B)

        For a fee-only ``collect()`` (no decreaseLiquidity in the same TX —
        e.g. an in-range fee harvest), there is no Burn event. We treat the
        full Collect amounts as fees, with principal = 0.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPCloseData if Burn or Collect event found, None otherwise.
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
            saw_burn = False
            saw_collect = False
            # Burn anchors registry identity; Collect retains a separate leg emitter.
            pool_address = ""
            collect_pool_address = ""

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
                    # Collect slots are recipient@0, amount0@32, amount1@64.
                    collect_amount0 += HexDecoder.decode_uint128(data, 32)
                    collect_amount1 += HexDecoder.decode_uint128(data, 64)
                    saw_collect = True
                    collect_pool_address = collect_pool_address or log_emitter_address(log, chain=self.chain)

                elif first_topic == burn_topic and len(topics) >= 4:
                    # Burn slots are liquidity@0, amount0@32, amount1@64.
                    burn_liquidity_total += HexDecoder.decode_uint128(data, 0)
                    burn_amount0 += HexDecoder.decode_uint256(data, 32)
                    burn_amount1 += HexDecoder.decode_uint256(data, 64)
                    saw_burn = True
                    if not pool_address:
                        addr = log.get("address", "")
                        if isinstance(addr, bytes):
                            addr = "0x" + addr.hex()
                        if addr:
                            pool_address = str(addr).lower()

            if not (saw_collect or saw_burn):
                return None

            liquidity_removed = burn_liquidity_total if saw_burn else None

            # Collect-only may be a fee harvest or one leg of a split close.
            if saw_collect:
                fees0: int | None = max(collect_amount0 - burn_amount0, 0)
                fees1: int | None = max(collect_amount1 - burn_amount1, 0)
            else:
                # Burn exposes principal but not fees.
                fees0 = None
                fees1 = None

            current_tick = self._current_tick_from_swap_event(logs, pool_address) if pool_address else None

            # Aggregation prefers Collect when reconstructing split closes.
            source = "collect" if saw_collect else "decrease_liquidity"

            amount0_collected = collect_amount0 if saw_collect else burn_amount0
            amount1_collected = collect_amount1 if saw_collect else burn_amount1

            # Burn-only receipts move no tokens, so their currencies remain unidentified.
            currency0, currency1 = currencies_for_amounts(
                transfers_by_token(logs, chain=self.chain, from_address=pool_address or collect_pool_address)
                if (pool_address or collect_pool_address)
                else {},
                amount0_collected,
                amount1_collected,
            )

            return LPCloseData(
                amount0_collected=amount0_collected,
                amount1_collected=amount1_collected,
                fees0=fees0,
                fees1=fees1,
                liquidity_removed=liquidity_removed,
                current_tick=current_tick,
                pool_address=pool_address,
                source=source,
                currency0=currency0,
                currency1=currency1,
            )

        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    def _decreaseliquidity_token_id(self, receipt: dict[str, Any]) -> int | None:
        """Recover ``tokenId`` from a ``DecreaseLiquidity`` log on the close-side
        receipt.

        The NPM emits ``DecreaseLiquidity(uint256 indexed tokenId, …)`` on
        every ``decreaseLiquidity()`` call. ``topics[1]`` is the indexed
        tokenId. Returns ``None`` if no such log is in the receipt or
        the NPM emitter doesn't match the configured chain — the close-side
        identity is then derivable only from strategy-supplied state, which
        is the legacy path the registry cutover is replacing. T12's caller
        treats ``None`` as "fall back to ``accounting_only`` for this intent"
        with an ERROR log; no ``Decimal("0")`` substitution.
        """
        logs = receipt.get("logs") or []
        if not logs:
            return None

        decrease_topic = EVENT_TOPICS["DecreaseLiquidity"].lower()
        position_manager = POSITION_MANAGER_ADDRESSES.get(self.chain, "").lower()
        if not position_manager:
            position_manager = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88".lower()

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

    def _resolve_fee_tier_from_pool_event(self, receipt: dict[str, Any]) -> int | None:
        """Best-effort fee-tier resolution from receipt-side hints.

        The fee tier is NOT carried on the LP_OPEN / LP_CLOSE event payload
        (UniV3's Mint / Burn / IncreaseLiquidity / DecreaseLiquidity events
        do not include it). The strategy-supplied compile-time metadata is
        the canonical source — but on Day-1 backfill we don't have that, and
        the goldens carry ``fee_tier=500`` as a shape constant.

        For T12 we return ``None`` here and let the caller (registry-payload
        builder) inject the value from the intent's ``fee_tier`` field when
        present. Returning ``None`` (not 0) honors CLAUDE.md "Empty ≠ zero".
        """
        _ = receipt
        return None

    def extract_registry_payload_open(
        self,
        receipt: dict[str, Any],
        *,
        fee_tier: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the LP_OPEN ``position_registry.payload`` dict.

        Reads the existing :meth:`extract_lp_open_data` output for
        ``token_id`` / ``tick_lower`` / ``tick_upper`` / ``liquidity`` /
        ``amount0`` / ``amount1`` / ``pool_address`` and composes the
        canonical 8-key shape (plus optional ``fee_tier`` and the
        per-chain ``nft_manager_addr``). Returns ``None`` when any of the
        load-bearing identity fields are missing — the caller treats that
        as "fall back to accounting_only", per CLAUDE.md "Empty ≠ zero"
        (a zero-substituted token_id would silently corrupt the
        physical_identity_hash).

        Args:
            receipt: Transaction receipt dict with ``logs`` field.
            fee_tier: Optional pool fee tier (e.g. ``500`` for 0.05%);
                forwarded from the intent's compile-time metadata. ``None``
                when unknown — the payload key stays absent rather than
                substituting ``0`` (Empty ≠ zero).

        Returns:
            ``dict`` JSON-serializable with the 8 (or 9 with fee_tier) keys
            ratified by PRD §Registry Data Shape and the T08 golden, OR
            ``None`` when the LP_OPEN data isn't extractable from the
            receipt.
        """
        # Refuse partial identity or range data rather than emit a corrupt registry row.
        lp_data = self.extract_lp_open_data(receipt)
        if lp_data is None:
            return None
        if lp_data.position_id is None or lp_data.position_id <= 0:
            return None
        if not lp_data.pool_address:
            return None
        if lp_data.tick_lower is None or lp_data.tick_upper is None:
            return None
        if lp_data.liquidity is None:
            return None

        payload: dict[str, Any] = {
            "token_id": str(lp_data.position_id),
            "pool_address": lp_data.pool_address.lower(),
            "tick_lower": lp_data.tick_lower,
            "tick_upper": lp_data.tick_upper,
            "liquidity": str(lp_data.liquidity),
            "amount0": str(lp_data.amount0) if lp_data.amount0 is not None else None,
            "amount1": str(lp_data.amount1) if lp_data.amount1 is not None else None,
            "nft_manager_addr": self._nft_manager_address(),
        }
        if fee_tier is not None and fee_tier > 0:
            payload["fee_tier"] = int(fee_tier)
        if self.token0_symbol:
            payload["_token0_label"] = self.token0_symbol
        if self.token1_symbol:
            payload["_token1_label"] = self.token1_symbol
        return payload

    @staticmethod
    def _open_payload_token_id_int(open_payload: dict[str, Any]) -> int | None:
        """Back-compat delegate - canonical implementation in
        ``almanak.connectors._strategy_base.v3_registry_payload``."""
        return v3_registry_payload.open_payload_token_id_int(open_payload)

    @classmethod
    def _open_payload_disagrees(
        cls,
        *,
        open_payload: dict[str, Any] | None,
        token_id: int,
        pool_address: str,
    ) -> bool:
        """Back-compat delegate - see ``v3_registry_payload.open_payload_disagrees``."""
        return v3_registry_payload.open_payload_disagrees(
            open_payload=open_payload, token_id=token_id, pool_address=pool_address
        )

    @staticmethod
    def _build_close_receipt_payload(
        *,
        token_id: int,
        pool_address: str,
        lp_close: Any,
        nft_manager_addr: str,
    ) -> dict[str, Any]:
        """Back-compat delegate - see ``v3_registry_payload.build_close_receipt_payload``."""
        return v3_registry_payload.build_close_receipt_payload(
            token_id=token_id,
            pool_address=pool_address,
            lp_close=lp_close,
            nft_manager_addr=nft_manager_addr,
        )

    @staticmethod
    def _merge_open_payload_fields(
        payload: dict[str, Any],
        open_payload: dict[str, Any] | None,
    ) -> None:
        """Back-compat delegate - see ``v3_registry_payload.merge_open_payload_fields``."""
        return v3_registry_payload.merge_open_payload_fields(payload, open_payload)

    def extract_registry_payload_close(
        self,
        receipt: dict[str, Any],
        *,
        open_payload: dict[str, Any] | None = None,
        fee_tier: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the LP_CLOSE ``position_registry.payload`` dict.

        Reads the existing :meth:`extract_lp_close_data` output (Burn /
        Collect amounts) and the close-side ``DecreaseLiquidity`` event
        for the NFT ``token_id``, then composes the 13-key shape that
        the T08 ``lp_close/expected_registry_row.json`` golden
        specifies.

        Audit M1 (CodeRabbit): a real UniV3 LP_CLOSE proves itself with
        DecreaseLiquidity on the receipt AND a Burn log carrying the
        pool address. A Collect-only receipt is NOT a close — it's a fee
        harvest. If we silently synthesized ``token_id`` /
        ``pool_address`` from ``open_payload`` here, a Collect-only
        receipt or a malformed close would produce a "successful" close
        payload with stale OPEN-side anchors, and the registry would
        mark a still-open NFT as closed (the cutover spec D3.F6
        silent-error class).

        The flow is:

        1. Decode close-side events (``extract_lp_close_data``) and the
           DecreaseLiquidity log (``_decreaseliquidity_token_id``).
        2. Verify the receipt-derived identity anchors are present and
           non-zero.
        3. Cross-check against ``open_payload`` if supplied — refuse on
           any disagreement (``_open_payload_disagrees``).
        4. Compose the receipt-only payload
           (``_build_close_receipt_payload``).
        5. Merge OPEN-time fields the close receipt cannot re-derive
           (``_merge_open_payload_fields``) — ticks, OPEN-time amounts,
           original mint liquidity, fee tier, token labels.
        6. Apply the ``fee_tier`` argument if open_payload didn't carry
           one.

        Returns ``None`` when the close-side identity anchors (token_id
        + pool_address) cannot be derived OR cross-checks fail. The
        caller treats that as "fall back to accounting_only" with an
        ERROR log (no zero substitution).
        """
        lp_close = self.extract_lp_close_data(receipt)
        if lp_close is None:
            return None
        token_id = self._decreaseliquidity_token_id(receipt)
        if token_id is None or token_id <= 0:
            return None
        pool_address = (lp_close.pool_address or "").lower()
        if not pool_address:
            return None
        # Close identity requires Burn and DecreaseLiquidity, not fee-only Collect.
        if self._open_payload_disagrees(
            open_payload=open_payload,
            token_id=token_id,
            pool_address=pool_address,
        ):
            return None

        payload = self._build_close_receipt_payload(
            token_id=token_id,
            pool_address=pool_address,
            lp_close=lp_close,
            nft_manager_addr=self._nft_manager_address(),
        )
        self._merge_open_payload_fields(payload, open_payload)
        if fee_tier is not None and fee_tier > 0:
            payload.setdefault("fee_tier", int(fee_tier))
        return payload

    def extract_protocol_fees(
        self,
        receipt: dict[str, Any],
        *,
        fee_tier_bps: int | None = None,
    ) -> ProtocolFees | None:
        """Extract DEX protocol fees from a swap receipt.

        Uniswap V3 swap fees are a pool-wide basis-points slice of
        ``amount_in``. The numeric fee tier is resolved at compile time and
        forwarded by the ResultEnricher via ``bundle_metadata["selected_fee_tier"]``
        so the parser does not need to re-read pool state.

        Fee amount (token-denominated):

            fee_amount_in = amount_in * fee_tier_bps / 1_000_000

        (``fee_tier_bps`` is Uniswap's pip-based tier, e.g. 500 = 0.05%, so
        the divisor is ``1_000_000`` — not ``10_000``.)

        VIB-3204 audit contract: until a price oracle is plumbed through
        to this layer, the parser returns ``None`` (unknown) — never a
        ``ProtocolFees(total_usd=Decimal(0))``, which would falsely
        advertise "measured to be zero" and cause PnL attribution to
        under-attribute swap costs. Callers that want the in-token fee
        can derive it from
        ``result.swap_amounts.amount_in_decimal * fee_tier_bps / 1_000_000``
        using values already on ``result.swap_amounts``.

        Args:
            receipt: Transaction receipt dict with 'logs' field.
            fee_tier_bps: Pool fee tier, forwarded from
                ``bundle_metadata["selected_fee_tier"]`` by the enricher.

        Returns:
            ``None`` — USD conversion not available at this layer. A
            future iteration with price-oracle access will return a
            populated ``ProtocolFees``. Also returns ``None`` when no
            Swap event is present OR when ``fee_tier_bps`` is missing.
        """

        # Missing fee metadata is unknown, not a measured zero.
        if fee_tier_bps is None or fee_tier_bps <= 0:
            return None

        try:
            parse_result = self.parse_receipt(receipt)
            if not parse_result.swap_result:
                return None

            # Token-denominated fees lack a USD price at this layer.
            return None

        except Exception as e:
            logger.warning(f"Failed to extract protocol_fees: {e}")
            return None

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
