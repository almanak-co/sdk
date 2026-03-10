"""BENQI Receipt Parser.

Parses transaction receipts for BENQI lending operations (Compound V2 architecture).
Uses base infrastructure utilities for hex decoding and event mapping.

Key events:
- Mint: User supplies underlying, receives qiTokens
- Redeem: User redeems qiTokens for underlying
- Borrow: User borrows underlying from the pool
- RepayBorrow: User repays borrowed underlying
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
# Event Topic Signatures (Compound V2 / BENQI)
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    # Core lending events
    "Mint": "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f",
    "Redeem": "0xe5b754fb1abb7f01b499791d0b820ae3b6af3424ac1c59768edb53f4ec31a929",
    "Borrow": "0x13ed6866d4e1ee6da46f845c46d7e54120883d75c5ea9a2dacc1c4ca8984ab80",
    "RepayBorrow": "0x1a2a22cb034d26d1854bdc6666a5b91fe25efbbb5dcad3b0355478d6f5c362a1",
    # Liquidation
    "LiquidateBorrow": "0x298637f684da70674f26509b10f07ec2fbc77a335ab1e7a6fff849315571e826",
    # ERC20 events
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class BenqiEventType(Enum):
    """BENQI event types."""

    MINT = "MINT"
    REDEEM = "REDEEM"
    BORROW = "BORROW"
    REPAY_BORROW = "REPAY_BORROW"
    LIQUIDATE_BORROW = "LIQUIDATE_BORROW"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, BenqiEventType] = {
    "Mint": BenqiEventType.MINT,
    "Redeem": BenqiEventType.REDEEM,
    "Borrow": BenqiEventType.BORROW,
    "RepayBorrow": BenqiEventType.REPAY_BORROW,
    "LiquidateBorrow": BenqiEventType.LIQUIDATE_BORROW,
    "Transfer": BenqiEventType.TRANSFER,
    "Approval": BenqiEventType.APPROVAL,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class BenqiEvent:
    """Parsed BENQI event."""

    event_type: BenqiEventType
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
    def from_dict(cls, data: dict[str, Any]) -> "BenqiEvent":
        """Create from dictionary."""
        return cls(
            event_type=BenqiEventType(data["event_type"]),
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
class ParseResult:
    """Result of parsing a transaction receipt."""

    success: bool
    events: list[BenqiEvent] = field(default_factory=list)
    supply_amount: Decimal = Decimal("0")
    withdraw_amount: Decimal = Decimal("0")
    borrow_amount: Decimal = Decimal("0")
    repay_amount: Decimal = Decimal("0")
    qi_tokens_minted: Decimal = Decimal("0")
    qi_tokens_redeemed: Decimal = Decimal("0")
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "supply_amount": str(self.supply_amount),
            "withdraw_amount": str(self.withdraw_amount),
            "borrow_amount": str(self.borrow_amount),
            "repay_amount": str(self.repay_amount),
            "qi_tokens_minted": str(self.qi_tokens_minted),
            "qi_tokens_redeemed": str(self.qi_tokens_redeemed),
            "error": self.error,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class BenqiReceiptParser:
    """Parse BENQI transaction receipts.

    Extracts supply, withdraw, borrow, and repay amounts from
    Compound V2-style events (Mint, Redeem, Borrow, RepayBorrow).
    """

    def __init__(self, underlying_decimals: int = 18, **kwargs: Any) -> None:
        self.underlying_decimals = underlying_decimals
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def parse_receipt(
        self,
        receipt: dict[str, Any],
        qi_token_address: str | None = None,
    ) -> ParseResult:
        """Parse a transaction receipt for BENQI events.

        Args:
            receipt: Transaction receipt dict with 'logs' field
            qi_token_address: Optional filter for specific qiToken

        Returns:
            ParseResult with extracted events and amounts
        """
        logs = receipt.get("logs", [])
        if not logs:
            return ParseResult(success=True)

        tx_hash = receipt.get("transactionHash", receipt.get("hash", ""))
        block_number = receipt.get("blockNumber", 0)
        if isinstance(block_number, str):
            block_number = int(block_number, 16) if block_number.startswith("0x") else int(block_number)

        events = self.parse_logs(logs, tx_hash=tx_hash, block_number=block_number)

        # Filter to specific qiToken if provided
        if qi_token_address:
            qi_lower = qi_token_address.lower()
            events = [e for e in events if e.contract_address.lower() == qi_lower]

        # Aggregate amounts
        result = ParseResult(success=True, events=events)
        for event in events:
            if event.event_type == BenqiEventType.MINT:
                result.supply_amount += Decimal(str(event.data.get("mint_amount", 0)))
                result.qi_tokens_minted += Decimal(str(event.data.get("mint_tokens", 0)))
            elif event.event_type == BenqiEventType.REDEEM:
                result.withdraw_amount += Decimal(str(event.data.get("redeem_amount", 0)))
                result.qi_tokens_redeemed += Decimal(str(event.data.get("redeem_tokens", 0)))
            elif event.event_type == BenqiEventType.BORROW:
                result.borrow_amount += Decimal(str(event.data.get("borrow_amount", 0)))
            elif event.event_type == BenqiEventType.REPAY_BORROW:
                result.repay_amount += Decimal(str(event.data.get("repay_amount", 0)))

        return result

    def parse_logs(
        self,
        logs: list[dict[str, Any]],
        tx_hash: str = "",
        block_number: int = 0,
    ) -> list[BenqiEvent]:
        """Parse a list of log entries into BENQI events."""
        events: list[BenqiEvent] = []

        for log in logs:
            topics = log.get("topics", [])
            if not topics:
                continue

            topic0 = topics[0]
            event_name = TOPIC_TO_EVENT.get(topic0)
            if not event_name:
                continue

            event_type = EVENT_NAME_TO_TYPE.get(event_name, BenqiEventType.UNKNOWN)
            raw_data = log.get("data", "0x")
            contract_address = log.get("address", "")
            log_index = log.get("logIndex", 0)
            if isinstance(log_index, str):
                log_index = int(log_index, 16) if log_index.startswith("0x") else int(log_index)

            # Decode event data (gracefully handle malformed data -- VIB-651)
            try:
                data = self._decode_event_data(event_name, topics, raw_data)
            except Exception as e:
                logger.warning("Failed to decode %s event data: %s", event_name, e)
                data = {}

            events.append(
                BenqiEvent(
                    event_type=event_type,
                    event_name=event_name,
                    log_index=log_index,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    contract_address=contract_address,
                    data=data,
                    raw_topics=topics,
                    raw_data=raw_data,
                )
            )

        return events

    def _decode_event_data(
        self,
        event_name: str,
        topics: list[str],
        raw_data: str,
    ) -> dict[str, Any]:
        """Decode event-specific data from topics and data fields.

        Compound V2 events encode most data in the non-indexed data field.
        """
        data: dict[str, Any] = {}
        hex_data = raw_data[2:] if raw_data.startswith("0x") else raw_data

        if event_name == "Mint":
            # Mint(address minter, uint256 mintAmount, uint256 mintTokens)
            # All in data field (non-indexed)
            if len(hex_data) >= 192:
                data["minter"] = HexDecoder.decode_address_from_data(hex_data[0:64])
                mint_amount_raw = HexDecoder.decode_uint256(hex_data[64:128])
                mint_tokens_raw = HexDecoder.decode_uint256(hex_data[128:192])
                data["mint_amount"] = str(Decimal(mint_amount_raw) / Decimal(10**self.underlying_decimals))
                data["mint_tokens"] = str(Decimal(mint_tokens_raw) / Decimal(10**8))  # qiTokens have 8 decimals

        elif event_name == "Redeem":
            # Redeem(address redeemer, uint256 redeemAmount, uint256 redeemTokens)
            if len(hex_data) >= 192:
                data["redeemer"] = HexDecoder.decode_address_from_data(hex_data[0:64])
                redeem_amount_raw = HexDecoder.decode_uint256(hex_data[64:128])
                redeem_tokens_raw = HexDecoder.decode_uint256(hex_data[128:192])
                data["redeem_amount"] = str(Decimal(redeem_amount_raw) / Decimal(10**self.underlying_decimals))
                data["redeem_tokens"] = str(Decimal(redeem_tokens_raw) / Decimal(10**8))

        elif event_name == "Borrow":
            # Borrow(address borrower, uint256 borrowAmount, uint256 accountBorrows, uint256 totalBorrows)
            if len(hex_data) >= 256:
                data["borrower"] = HexDecoder.decode_address_from_data(hex_data[0:64])
                borrow_amount_raw = HexDecoder.decode_uint256(hex_data[64:128])
                account_borrows_raw = HexDecoder.decode_uint256(hex_data[128:192])
                total_borrows_raw = HexDecoder.decode_uint256(hex_data[192:256])
                data["borrow_amount"] = str(Decimal(borrow_amount_raw) / Decimal(10**self.underlying_decimals))
                data["account_borrows"] = str(Decimal(account_borrows_raw) / Decimal(10**self.underlying_decimals))
                data["total_borrows"] = str(Decimal(total_borrows_raw) / Decimal(10**self.underlying_decimals))

        elif event_name == "RepayBorrow":
            # RepayBorrow(address payer, address borrower, uint256 repayAmount, uint256 accountBorrows, uint256 totalBorrows)
            if len(hex_data) >= 320:
                data["payer"] = HexDecoder.decode_address_from_data(hex_data[0:64])
                data["borrower"] = HexDecoder.decode_address_from_data(hex_data[64:128])
                repay_amount_raw = HexDecoder.decode_uint256(hex_data[128:192])
                account_borrows_raw = HexDecoder.decode_uint256(hex_data[192:256])
                total_borrows_raw = HexDecoder.decode_uint256(hex_data[256:320])
                data["repay_amount"] = str(Decimal(repay_amount_raw) / Decimal(10**self.underlying_decimals))
                data["account_borrows"] = str(Decimal(account_borrows_raw) / Decimal(10**self.underlying_decimals))
                data["total_borrows"] = str(Decimal(total_borrows_raw) / Decimal(10**self.underlying_decimals))

        elif event_name == "LiquidateBorrow":
            # LiquidateBorrow(address liquidator, address borrower, uint256 repayAmount,
            #                  address cTokenCollateral, uint256 seizeTokens)
            if len(hex_data) >= 320:
                data["liquidator"] = HexDecoder.decode_address_from_data(hex_data[0:64])
                data["borrower"] = HexDecoder.decode_address_from_data(hex_data[64:128])
                repay_amount_raw = HexDecoder.decode_uint256(hex_data[128:192])
                data["repay_amount"] = str(Decimal(repay_amount_raw) / Decimal(10**self.underlying_decimals))
                data["ctoken_collateral"] = HexDecoder.decode_address_from_data(hex_data[192:256])
                seize_tokens_raw = HexDecoder.decode_uint256(hex_data[256:320])
                data["seize_tokens"] = str(Decimal(seize_tokens_raw) / Decimal(10**8))  # qiTokens have 8 decimals

        elif event_name == "Transfer":
            # Transfer(address indexed from, address indexed to, uint256 value)
            if len(topics) >= 3:
                data["from"] = HexDecoder.decode_address_from_data(topics[1][2:])
                data["to"] = HexDecoder.decode_address_from_data(topics[2][2:])
            if len(hex_data) >= 64:
                data["value"] = str(HexDecoder.decode_uint256(hex_data[0:64]))

        elif event_name == "Approval":
            # Approval(address indexed owner, address indexed spender, uint256 value)
            if len(topics) >= 3:
                data["owner"] = HexDecoder.decode_address_from_data(topics[1][2:])
                data["spender"] = HexDecoder.decode_address_from_data(topics[2][2:])
            if len(hex_data) >= 64:
                data["value"] = str(HexDecoder.decode_uint256(hex_data[0:64]))

        return data

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    def extract_supply_data(self, result: dict[str, Any]) -> dict | None:
        """Extract supply data from a parsed transaction receipt.

        Called by ResultEnricher for SUPPLY intents.

        Args:
            result: Transaction receipt dict with 'logs' field

        Returns:
            Dict with supply_amount and qi_tokens_minted, or None if not found
        """
        try:
            parsed = self.parse_receipt(result)
            if not parsed.success or parsed.supply_amount == Decimal("0"):
                return None
            return {
                "supply_amount": str(parsed.supply_amount),
                "qi_tokens_minted": str(parsed.qi_tokens_minted),
            }
        except Exception as e:
            logger.warning(f"Failed to extract supply data: {e}")
            return None

    def extract_borrow_data(self, result: dict[str, Any]) -> dict | None:
        """Extract borrow data from a parsed transaction receipt.

        Called by ResultEnricher for BORROW intents.

        Args:
            result: Transaction receipt dict with 'logs' field

        Returns:
            Dict with borrow_amount, or None if not found
        """
        try:
            parsed = self.parse_receipt(result)
            if not parsed.success or parsed.borrow_amount == Decimal("0"):
                return None
            return {
                "borrow_amount": str(parsed.borrow_amount),
            }
        except Exception as e:
            logger.warning(f"Failed to extract borrow data: {e}")
            return None

    def extract_withdraw_data(self, result: dict[str, Any]) -> dict | None:
        """Extract withdraw data from a parsed transaction receipt.

        Called by ResultEnricher for WITHDRAW intents.

        Args:
            result: Transaction receipt dict with 'logs' field

        Returns:
            Dict with withdraw_amount, or None if not found
        """
        try:
            parsed = self.parse_receipt(result)
            if not parsed.success or parsed.withdraw_amount == Decimal("0"):
                return None
            return {
                "withdraw_amount": str(parsed.withdraw_amount),
            }
        except Exception as e:
            logger.warning(f"Failed to extract withdraw data: {e}")
            return None

    def extract_repay_data(self, result: dict[str, Any]) -> dict | None:
        """Extract repay data from a parsed transaction receipt.

        Called by ResultEnricher for REPAY intents.

        Args:
            result: Transaction receipt dict with 'logs' field

        Returns:
            Dict with repay_amount, or None if not found
        """
        try:
            parsed = self.parse_receipt(result)
            if not parsed.success or parsed.repay_amount == Decimal("0"):
                return None
            return {
                "repay_amount": str(parsed.repay_amount),
            }
        except Exception as e:
            logger.warning(f"Failed to extract repay data: {e}")
            return None
