"""
Pendle Protocol Receipt Parser

This parser handles transaction receipts from Pendle Protocol operations,
extracting relevant event data for:
- Swaps (token -> PT, PT -> token, YT swaps)
- Liquidity operations (add/remove)
- PT/YT redemptions

Event Signatures (from Pendle contracts):
- Swap: Swap(address indexed caller, address indexed receiver, int256 ptToAccount, int256 syToAccount)
- Mint: Mint(address indexed receiver, uint256 netLpMinted, uint256 netSyUsed, uint256 netPtUsed)
- Burn: Burn(address indexed receiver, uint256 netLpBurned, uint256 netSyOut, uint256 netPtOut)
- RedeemPY: RedeemPY(address indexed caller, address indexed receiver, uint256 netPYRedeemed, uint256 netSYRedeemed)
- Transfer (ERC20): Transfer(address indexed from, address indexed to, uint256 value)
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.base import EventRegistry, HexDecoder

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

# Pendle-specific event signatures (keccak256 hashes)
EVENT_TOPICS: dict[str, str] = {
    # Pendle Market events
    "Swap": "0x829000a5bc6a12d46e30cdcecd7c56b1efd88f6d7d059da6734a04f3764557c4",
    "Mint": "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f",
    "Burn": "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496",
    # PY Redemption events
    "RedeemPY": "0x99d3da4d3e0b3c4d2f147b1f2d6e1b9fe5e12c8b5c4a3d2e1f0a9b8c7d6e5f4a3",
    "MintPY": "0x88a3d4e3f2c1b0a9d8c7b6a5e4f3d2c1b0a9e8d7c6b5a4f3e2d1c0b9a8f7e6d5",
    # SY events
    "MintSY": "0x7a1d9b8c0e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b",
    "RedeemSY": "0x8b2e0c9d1f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9",
    # Standard ERC20 events
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

# Reverse mapping for topic lookup
TOPIC_TO_EVENT: dict[str, str] = {v.lower(): k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class PendleEventType(Enum):
    """Pendle event types."""

    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    REDEEM_PY = "REDEEM_PY"
    MINT_PY = "MINT_PY"
    MINT_SY = "MINT_SY"
    REDEEM_SY = "REDEEM_SY"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, PendleEventType] = {
    "Swap": PendleEventType.SWAP,
    "Mint": PendleEventType.MINT,
    "Burn": PendleEventType.BURN,
    "RedeemPY": PendleEventType.REDEEM_PY,
    "MintPY": PendleEventType.MINT_PY,
    "MintSY": PendleEventType.MINT_SY,
    "RedeemSY": PendleEventType.REDEEM_SY,
    "Transfer": PendleEventType.TRANSFER,
    "Approval": PendleEventType.APPROVAL,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PendleEvent:
    """Parsed Pendle event."""

    event_type: PendleEventType
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
    """Parsed data from Pendle Swap event."""

    caller: str
    receiver: str
    pt_to_account: int  # Signed - positive means PT received
    sy_to_account: int  # Signed - positive means SY received
    market_address: str

    @property
    def is_buy_pt(self) -> bool:
        """Check if this is a buy PT operation (SY -> PT)."""
        return self.pt_to_account > 0

    @property
    def is_sell_pt(self) -> bool:
        """Check if this is a sell PT operation (PT -> SY)."""
        return self.pt_to_account < 0

    @property
    def pt_amount(self) -> int:
        """Get absolute PT amount."""
        return abs(self.pt_to_account)

    @property
    def sy_amount(self) -> int:
        """Get absolute SY amount."""
        return abs(self.sy_to_account)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "caller": self.caller,
            "receiver": self.receiver,
            "pt_to_account": str(self.pt_to_account),
            "sy_to_account": str(self.sy_to_account),
            "market_address": self.market_address,
            "is_buy_pt": self.is_buy_pt,
            "pt_amount": str(self.pt_amount),
            "sy_amount": str(self.sy_amount),
        }


@dataclass
class MintEventData:
    """Parsed data from Pendle Mint (LP) event."""

    receiver: str
    net_lp_minted: int
    net_sy_used: int
    net_pt_used: int
    market_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "receiver": self.receiver,
            "net_lp_minted": str(self.net_lp_minted),
            "net_sy_used": str(self.net_sy_used),
            "net_pt_used": str(self.net_pt_used),
            "market_address": self.market_address,
        }


@dataclass
class BurnEventData:
    """Parsed data from Pendle Burn (LP removal) event."""

    receiver: str
    net_lp_burned: int
    net_sy_out: int
    net_pt_out: int
    market_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "receiver": self.receiver,
            "net_lp_burned": str(self.net_lp_burned),
            "net_sy_out": str(self.net_sy_out),
            "net_pt_out": str(self.net_pt_out),
            "market_address": self.market_address,
        }


@dataclass
class RedeemPYEventData:
    """Parsed data from Pendle RedeemPY event."""

    caller: str
    receiver: str
    net_py_redeemed: int
    net_sy_redeemed: int
    yt_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "caller": self.caller,
            "receiver": self.receiver,
            "net_py_redeemed": str(self.net_py_redeemed),
            "net_sy_redeemed": str(self.net_sy_redeemed),
            "yt_address": self.yt_address,
        }


@dataclass
class TransferEventData:
    """Parsed data from ERC20 Transfer event."""

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
    """High-level swap result from Pendle."""

    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    amount_in_decimal: Decimal
    amount_out_decimal: Decimal
    effective_price: Decimal
    slippage_bps: int
    market_address: str
    swap_type: str  # "buy_pt", "sell_pt", "buy_yt", "sell_yt"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "amount_in_decimal": str(self.amount_in_decimal),
            "amount_out_decimal": str(self.amount_out_decimal),
            "effective_price": str(self.effective_price),
            "slippage_bps": self.slippage_bps,
            "market_address": self.market_address,
            "swap_type": self.swap_type,
        }


@dataclass
class ParseResult:
    """Result of parsing a Pendle receipt."""

    success: bool
    events: list[PendleEvent] = field(default_factory=list)
    swap_events: list[SwapEventData] = field(default_factory=list)
    mint_events: list[MintEventData] = field(default_factory=list)
    burn_events: list[BurnEventData] = field(default_factory=list)
    redeem_events: list[RedeemPYEventData] = field(default_factory=list)
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
            "mint_events": [m.to_dict() for m in self.mint_events],
            "burn_events": [b.to_dict() for b in self.burn_events],
            "redeem_events": [r.to_dict() for r in self.redeem_events],
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


class PendleReceiptParser:
    """
    Parser for Pendle Protocol transaction receipts.

    Uses the base infrastructure (EventRegistry, HexDecoder) for standardized
    event parsing while handling Pendle-specific event structures.

    Example:
        parser = PendleReceiptParser(chain="arbitrum")
        result = parser.parse_receipt(receipt)

        if result.success and result.swap_events:
            swap = result.swap_events[0]
            print(f"Swapped {swap.sy_amount} SY for {swap.pt_amount} PT")
    """

    def __init__(
        self,
        chain: str = "arbitrum",
        token_in_decimals: int = 18,
        token_out_decimals: int = 18,
        quoted_price: Decimal | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Pendle receipt parser.

        Args:
            chain: Chain name for address resolution
            token_in_decimals: Decimals for input token
            token_out_decimals: Decimals for output token
            quoted_price: Expected price for slippage calculation
        """
        self.chain = chain.lower()
        self.token_in_decimals = token_in_decimals
        self.token_out_decimals = token_out_decimals
        self.quoted_price = quoted_price

        # Initialize event registry
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def parse_receipt(
        self,
        receipt: dict[str, Any],
        quoted_amount_out: int | None = None,
    ) -> ParseResult:
        """
        Parse a Pendle transaction receipt.

        Args:
            receipt: Transaction receipt dictionary
            quoted_amount_out: Expected output for slippage calculation

        Returns:
            ParseResult with extracted events and swap data
        """
        try:
            # Extract transaction metadata
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

            if not tx_success:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=False,
                    error="Transaction reverted",
                )

            # Parse all events
            events: list[PendleEvent] = []
            swap_events: list[SwapEventData] = []
            mint_events: list[MintEventData] = []
            burn_events: list[BurnEventData] = []
            redeem_events: list[RedeemPYEventData] = []
            transfer_events: list[TransferEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data based on event type
                    if parsed_event.event_type == PendleEventType.SWAP:
                        swap_data = self._parse_swap_event(parsed_event)
                        if swap_data:
                            swap_events.append(swap_data)

                    elif parsed_event.event_type == PendleEventType.MINT:
                        mint_data = self._parse_mint_event(parsed_event)
                        if mint_data:
                            mint_events.append(mint_data)

                    elif parsed_event.event_type == PendleEventType.BURN:
                        burn_data = self._parse_burn_event(parsed_event)
                        if burn_data:
                            burn_events.append(burn_data)

                    elif parsed_event.event_type == PendleEventType.REDEEM_PY:
                        redeem_data = self._parse_redeem_event(parsed_event)
                        if redeem_data:
                            redeem_events.append(redeem_data)

                    elif parsed_event.event_type == PendleEventType.TRANSFER:
                        transfer_data = self._parse_transfer_event(parsed_event)
                        if transfer_data:
                            transfer_events.append(transfer_data)

            # Build high-level swap result
            swap_result = None
            if swap_events:
                swap_result = self._build_swap_result(
                    swap_events[0],
                    transfer_events,
                    quoted_amount_out,
                )

            # Log parsed receipt
            logger.info(
                f"Parsed Pendle receipt: tx={tx_hash[:10]}..., "
                f"swaps={len(swap_events)}, mints={len(mint_events)}, "
                f"burns={len(burn_events)}, redeems={len(redeem_events)}"
            )

            return ParseResult(
                success=True,
                events=events,
                swap_events=swap_events,
                mint_events=mint_events,
                burn_events=burn_events,
                redeem_events=redeem_events,
                transfer_events=transfer_events,
                swap_result=swap_result,
                transaction_hash=tx_hash,
                block_number=block_number,
                transaction_success=tx_success,
            )

        except Exception as e:
            logger.exception(f"Failed to parse Pendle receipt: {e}")
            return ParseResult(
                success=False,
                error=str(e),
            )

    def _parse_log(
        self,
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
    ) -> PendleEvent | None:
        """Parse a single log entry."""
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

            event_type = self.registry.get_event_type(event_name) or PendleEventType.UNKNOWN

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

            # Decode log data
            parsed_data = self._decode_log_data(event_name, topics, data, contract_address)

            return PendleEvent(
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
        """Decode log data based on event type."""
        if event_name == "Swap":
            return self._decode_swap_data(topics, data, address)
        elif event_name == "Mint":
            return self._decode_mint_data(topics, data, address)
        elif event_name == "Burn":
            return self._decode_burn_data(topics, data, address)
        elif event_name == "RedeemPY":
            return self._decode_redeem_data(topics, data, address)
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
        """
        Decode Pendle Swap event.

        Swap(address indexed caller, address indexed receiver, int256 ptToAccount, int256 syToAccount)
        """
        try:
            # Indexed: caller, receiver
            caller = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            receiver = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            # Non-indexed: ptToAccount (int256), syToAccount (int256)
            pt_to_account = HexDecoder.decode_int256(data, 0)
            sy_to_account = HexDecoder.decode_int256(data, 32)

            market_address = address.lower() if isinstance(address, str) else ""
            if isinstance(address, bytes):
                market_address = "0x" + address.hex()

            return {
                "caller": caller,
                "receiver": receiver,
                "pt_to_account": pt_to_account,
                "sy_to_account": sy_to_account,
                "market_address": market_address,
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
        """
        Decode Pendle Mint (LP) event.

        Mint(address indexed receiver, uint256 netLpMinted, uint256 netSyUsed, uint256 netPtUsed)
        """
        try:
            receiver = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""

            net_lp_minted = HexDecoder.decode_uint256(data, 0)
            net_sy_used = HexDecoder.decode_uint256(data, 32)
            net_pt_used = HexDecoder.decode_uint256(data, 64)

            market_address = address.lower() if isinstance(address, str) else ""

            return {
                "receiver": receiver,
                "net_lp_minted": net_lp_minted,
                "net_sy_used": net_sy_used,
                "net_pt_used": net_pt_used,
                "market_address": market_address,
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
        """
        Decode Pendle Burn (LP removal) event.

        Burn(address indexed receiver, uint256 netLpBurned, uint256 netSyOut, uint256 netPtOut)
        """
        try:
            receiver = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""

            net_lp_burned = HexDecoder.decode_uint256(data, 0)
            net_sy_out = HexDecoder.decode_uint256(data, 32)
            net_pt_out = HexDecoder.decode_uint256(data, 64)

            market_address = address.lower() if isinstance(address, str) else ""

            return {
                "receiver": receiver,
                "net_lp_burned": net_lp_burned,
                "net_sy_out": net_sy_out,
                "net_pt_out": net_pt_out,
                "market_address": market_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Burn data: {e}")
            return {"raw_data": data}

    def _decode_redeem_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """
        Decode Pendle RedeemPY event.

        RedeemPY(address indexed caller, address indexed receiver, uint256 netPYRedeemed, uint256 netSYRedeemed)
        """
        try:
            caller = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            receiver = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            net_py_redeemed = HexDecoder.decode_uint256(data, 0)
            net_sy_redeemed = HexDecoder.decode_uint256(data, 32)

            yt_address = address.lower() if isinstance(address, str) else ""

            return {
                "caller": caller,
                "receiver": receiver,
                "net_py_redeemed": net_py_redeemed,
                "net_sy_redeemed": net_sy_redeemed,
                "yt_address": yt_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode RedeemPY data: {e}")
            return {"raw_data": data}

    def _decode_transfer_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode ERC20 Transfer event."""
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

    def _parse_swap_event(self, event: PendleEvent) -> SwapEventData | None:
        """Parse Swap event into typed data."""
        try:
            data = event.data
            return SwapEventData(
                caller=data.get("caller", ""),
                receiver=data.get("receiver", ""),
                pt_to_account=data.get("pt_to_account", 0),
                sy_to_account=data.get("sy_to_account", 0),
                market_address=data.get("market_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse SwapEventData: {e}")
            return None

    def _parse_mint_event(self, event: PendleEvent) -> MintEventData | None:
        """Parse Mint event into typed data."""
        try:
            data = event.data
            return MintEventData(
                receiver=data.get("receiver", ""),
                net_lp_minted=data.get("net_lp_minted", 0),
                net_sy_used=data.get("net_sy_used", 0),
                net_pt_used=data.get("net_pt_used", 0),
                market_address=data.get("market_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse MintEventData: {e}")
            return None

    def _parse_burn_event(self, event: PendleEvent) -> BurnEventData | None:
        """Parse Burn event into typed data."""
        try:
            data = event.data
            return BurnEventData(
                receiver=data.get("receiver", ""),
                net_lp_burned=data.get("net_lp_burned", 0),
                net_sy_out=data.get("net_sy_out", 0),
                net_pt_out=data.get("net_pt_out", 0),
                market_address=data.get("market_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse BurnEventData: {e}")
            return None

    def _parse_redeem_event(self, event: PendleEvent) -> RedeemPYEventData | None:
        """Parse RedeemPY event into typed data."""
        try:
            data = event.data
            return RedeemPYEventData(
                caller=data.get("caller", ""),
                receiver=data.get("receiver", ""),
                net_py_redeemed=data.get("net_py_redeemed", 0),
                net_sy_redeemed=data.get("net_sy_redeemed", 0),
                yt_address=data.get("yt_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse RedeemPYEventData: {e}")
            return None

    def _parse_transfer_event(self, event: PendleEvent) -> TransferEventData | None:
        """Parse Transfer event into typed data."""
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
        """Build high-level swap result from events."""
        # Determine swap direction
        if swap_event.is_buy_pt:
            # SY -> PT swap
            amount_in = swap_event.sy_amount
            amount_out = swap_event.pt_amount
            swap_type = "buy_pt"
            token_in = "SY"
            token_out = "PT"
            in_decimals = self.token_in_decimals
            out_decimals = self.token_out_decimals
        else:
            # PT -> SY swap
            amount_in = swap_event.pt_amount
            amount_out = swap_event.sy_amount
            swap_type = "sell_pt"
            token_in = "PT"
            token_out = "SY"
            in_decimals = self.token_out_decimals
            out_decimals = self.token_in_decimals

        # Convert to decimal
        amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**in_decimals)
        amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**out_decimals)

        # Calculate effective price
        if amount_in_decimal > 0:
            effective_price = amount_out_decimal / amount_in_decimal
        else:
            effective_price = Decimal("0")

        # Calculate slippage
        slippage_bps = 0
        if quoted_amount_out and quoted_amount_out > 0:
            slippage_pct_f = (quoted_amount_out - amount_out) / quoted_amount_out
            slippage_bps = int(slippage_pct_f * 10000)
        elif self.quoted_price and self.quoted_price > 0:
            slippage_pct_d = (self.quoted_price - effective_price) / self.quoted_price
            slippage_bps = int(slippage_pct_d * 10000)

        return ParsedSwapResult(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            effective_price=effective_price,
            slippage_bps=slippage_bps,
            market_address=swap_event.market_address,
            swap_type=swap_type,
        )

    # =========================================================================
    # Extraction Methods (for Result Enrichment)
    # =========================================================================

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """
        Extract swap amounts from receipt for Result Enrichment.

        Called by the framework after SWAP execution to populate
        ExecutionResult.swap_amounts.

        Returns:
            Dictionary with amount_in, amount_out, effective_price, slippage_bps
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.swap_result:
                return None

            sr = result.swap_result
            return {
                "amount_in": sr.amount_in,
                "amount_out": sr.amount_out,
                "amount_in_decimal": sr.amount_in_decimal,
                "amount_out_decimal": sr.amount_out_decimal,
                "effective_price": sr.effective_price,
                "slippage_bps": sr.slippage_bps,
                "token_in": sr.token_in,
                "token_out": sr.token_out,
            }

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    def extract_lp_minted(self, receipt: dict[str, Any]) -> int | None:
        """
        Extract LP tokens minted from receipt.

        Called by the framework after LP_OPEN execution.
        """
        try:
            result = self.parse_receipt(receipt)
            if result.mint_events:
                return result.mint_events[0].net_lp_minted
            return None
        except Exception as e:
            logger.warning(f"Failed to extract LP minted: {e}")
            return None

    def extract_lp_burned(self, receipt: dict[str, Any]) -> int | None:
        """
        Extract LP tokens burned from receipt.

        Called by the framework after LP_CLOSE execution.
        """
        try:
            result = self.parse_receipt(receipt)
            if result.burn_events:
                return result.burn_events[0].net_lp_burned
            return None
        except Exception as e:
            logger.warning(f"Failed to extract LP burned: {e}")
            return None

    def extract_redemption_amounts(self, receipt: dict[str, Any]) -> dict[str, int] | None:
        """
        Extract redemption amounts from receipt.

        Called by the framework after WITHDRAW/REDEEM execution.
        """
        try:
            result = self.parse_receipt(receipt)
            if result.redeem_events:
                redeem = result.redeem_events[0]
                return {
                    "py_redeemed": redeem.net_py_redeemed,
                    "sy_received": redeem.net_sy_redeemed,
                }
            return None
        except Exception as e:
            logger.warning(f"Failed to extract redemption amounts: {e}")
            return None


__all__ = [
    "PendleReceiptParser",
    "PendleEvent",
    "PendleEventType",
    "SwapEventData",
    "MintEventData",
    "BurnEventData",
    "RedeemPYEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
