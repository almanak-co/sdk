"""Spark Receipt Parser (Refactored).

Refactored to use base infrastructure utilities while maintaining backward compatibility.
Spark is an Aave V3 fork that uses the same event signatures with pool address filtering.
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.base import EventRegistry, HexDecoder

logger = logging.getLogger(__name__)


# =============================================================================
# Known Spark Pool Addresses
# =============================================================================

SPARK_POOL_ADDRESSES: set[str] = {
    # Ethereum Mainnet
    "0xc13e21b648a5ee794902342038ff3adab66be987",
}


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    # Core lending events (same as Aave V3)
    "Supply": "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
    "Withdraw": "0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7",
    "Borrow": "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
    "Repay": "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class SparkEventType(Enum):
    """Spark event types."""

    SUPPLY = "SUPPLY"
    WITHDRAW = "WITHDRAW"
    BORROW = "BORROW"
    REPAY = "REPAY"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, SparkEventType] = {
    "Supply": SparkEventType.SUPPLY,
    "Withdraw": SparkEventType.WITHDRAW,
    "Borrow": SparkEventType.BORROW,
    "Repay": SparkEventType.REPAY,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SupplyEventData:
    """Parsed data from Supply event."""

    reserve: str
    user: str
    on_behalf_of: str
    amount: Decimal
    referral_code: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "reserve": self.reserve,
            "user": self.user,
            "on_behalf_of": self.on_behalf_of,
            "amount": str(self.amount),
            "referral_code": self.referral_code,
        }


@dataclass
class WithdrawEventData:
    """Parsed data from Withdraw event."""

    reserve: str
    user: str
    to: str
    amount: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "reserve": self.reserve,
            "user": self.user,
            "to": self.to,
            "amount": str(self.amount),
        }


@dataclass
class BorrowEventData:
    """Parsed data from Borrow event."""

    reserve: str
    user: str
    on_behalf_of: str
    amount: Decimal
    interest_rate_mode: int
    borrow_rate: Decimal = Decimal("0")
    referral_code: int = 0

    @property
    def is_variable_rate(self) -> bool:
        """Check if borrow is variable rate."""
        return self.interest_rate_mode == 2

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "reserve": self.reserve,
            "user": self.user,
            "on_behalf_of": self.on_behalf_of,
            "amount": str(self.amount),
            "interest_rate_mode": self.interest_rate_mode,
            "is_variable_rate": self.is_variable_rate,
            "borrow_rate": str(self.borrow_rate),
            "referral_code": self.referral_code,
        }


@dataclass
class RepayEventData:
    """Parsed data from Repay event."""

    reserve: str
    user: str
    repayer: str
    amount: Decimal
    use_atokens: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "reserve": self.reserve,
            "user": self.user,
            "repayer": self.repayer,
            "amount": str(self.amount),
            "use_atokens": self.use_atokens,
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    supplies: list[SupplyEventData] = field(default_factory=list)
    withdraws: list[WithdrawEventData] = field(default_factory=list)
    borrows: list[BorrowEventData] = field(default_factory=list)
    repays: list[RepayEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "supplies": [s.to_dict() for s in self.supplies],
            "withdraws": [w.to_dict() for w in self.withdraws],
            "borrows": [b.to_dict() for b in self.borrows],
            "repays": [r.to_dict() for r in self.repays],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class SparkReceiptParser:
    """Parser for Spark transaction receipts.

    Refactored to use base infrastructure utilities for hex decoding
    and event registry management. Maintains pool address filtering
    to avoid false positives with Aave V3 events.
    """

    def __init__(
        self,
        pool_addresses: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the parser.

        Args:
            pool_addresses: Optional set of known Spark pool addresses to filter by.
                           If not provided, uses the default SPARK_POOL_ADDRESSES.
                           Addresses should be lowercase.
            **kwargs: Additional arguments (ignored for compatibility)
        """
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        self._pool_addresses = pool_addresses if pool_addresses is not None else SPARK_POOL_ADDRESSES

    def parse_receipt(self, receipt: dict[str, Any]) -> ParseResult:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dict

        Returns:
            ParseResult with extracted events
        """
        try:
            # Normalize transaction hash
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

            supplies: list[SupplyEventData] = []
            withdraws: list[WithdrawEventData] = []
            borrows: list[BorrowEventData] = []
            repays: list[RepayEventData] = []

            for log in logs:
                # Filter by contract address to avoid false positives
                # (e.g., Aave V3 events which use the same signatures)
                log_address = log.get("address", "")
                if isinstance(log_address, bytes):
                    log_address = "0x" + log_address.hex()
                log_address = log_address.lower()

                if log_address not in self._pool_addresses:
                    continue

                topics = log.get("topics", [])
                if not topics:
                    continue

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
                    continue

                # Get raw data
                data = HexDecoder.normalize_hex(log.get("data", ""))

                # Parse based on event type
                if event_name == "Supply":
                    supply_data = self._parse_supply_log(topics, data)
                    if supply_data:
                        supplies.append(supply_data)

                elif event_name == "Withdraw":
                    withdraw_data = self._parse_withdraw_log(topics, data)
                    if withdraw_data:
                        withdraws.append(withdraw_data)

                elif event_name == "Borrow":
                    borrow_data = self._parse_borrow_log(topics, data)
                    if borrow_data:
                        borrows.append(borrow_data)

                elif event_name == "Repay":
                    repay_data = self._parse_repay_log(topics, data)
                    if repay_data:
                        repays.append(repay_data)

            logger.info(
                f"Parsed Spark receipt: tx={tx_hash[:10]}..., "
                f"supplies={len(supplies)}, withdraws={len(withdraws)}, "
                f"borrows={len(borrows)}, repays={len(repays)}"
            )

            return ParseResult(
                success=True,
                supplies=supplies,
                withdraws=withdraws,
                borrows=borrows,
                repays=repays,
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(
                success=False,
                error=str(e),
            )

    def _parse_supply_log(self, topics: list[Any], data: str) -> SupplyEventData | None:
        """Parse Supply event data.

        Supply(address indexed reserve, address user, address indexed onBehalfOf,
               uint256 amount, uint16 referralCode)
        """
        try:
            # Indexed: reserve (topic 1), onBehalfOf (topic 2)
            reserve = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            on_behalf_of = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            # Non-indexed: user (address), amount (uint256), referralCode (uint16)
            user = HexDecoder.topic_to_address(data[:64])
            amount = HexDecoder.decode_uint256(data, 32)
            referral_code = HexDecoder.decode_uint256(data, 64)

            return SupplyEventData(
                reserve=reserve,
                user=user,
                on_behalf_of=on_behalf_of,
                amount=Decimal(amount),
                referral_code=referral_code,
            )

        except Exception as e:
            logger.warning(f"Failed to parse Supply event: {e}")
            return None

    def _parse_withdraw_log(self, topics: list[Any], data: str) -> WithdrawEventData | None:
        """Parse Withdraw event data.

        Withdraw(address indexed reserve, address indexed user, address indexed to,
                 uint256 amount)
        """
        try:
            # Indexed: reserve, user, to
            reserve = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            user = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            to = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

            # Non-indexed: amount
            amount = HexDecoder.decode_uint256(data, 0)

            return WithdrawEventData(
                reserve=reserve,
                user=user,
                to=to,
                amount=Decimal(amount),
            )

        except Exception as e:
            logger.warning(f"Failed to parse Withdraw event: {e}")
            return None

    def _parse_borrow_log(self, topics: list[Any], data: str) -> BorrowEventData | None:
        """Parse Borrow event data.

        Borrow(address indexed reserve, address user, address indexed onBehalfOf,
               uint256 amount, uint256 interestRateMode, uint256 borrowRate,
               uint16 referralCode)
        """
        try:
            # Indexed: reserve, onBehalfOf
            reserve = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            on_behalf_of = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            # Non-indexed: user, amount, interestRateMode, borrowRate, referralCode
            user = HexDecoder.topic_to_address(data[:64])
            amount = HexDecoder.decode_uint256(data, 32)
            interest_rate_mode = HexDecoder.decode_uint256(data, 64)
            borrow_rate_raw = HexDecoder.decode_uint256(data, 96)
            referral_code = HexDecoder.decode_uint256(data, 128)

            # Convert borrow rate from ray (1e27) to decimal
            borrow_rate = Decimal(borrow_rate_raw) / Decimal("1e27")

            return BorrowEventData(
                reserve=reserve,
                user=user,
                on_behalf_of=on_behalf_of,
                amount=Decimal(amount),
                interest_rate_mode=interest_rate_mode,
                borrow_rate=borrow_rate,
                referral_code=referral_code,
            )

        except Exception as e:
            logger.warning(f"Failed to parse Borrow event: {e}")
            return None

    def _parse_repay_log(self, topics: list[Any], data: str) -> RepayEventData | None:
        """Parse Repay event data.

        Repay(address indexed reserve, address indexed user, address indexed repayer,
              uint256 amount, bool useATokens)
        """
        try:
            # Indexed: reserve, user, repayer
            reserve = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            user = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            repayer = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

            # Non-indexed: amount, useATokens
            amount = HexDecoder.decode_uint256(data, 0)
            use_atokens = HexDecoder.decode_uint256(data, 32) == 1

            return RepayEventData(
                reserve=reserve,
                user=user,
                repayer=repayer,
                amount=Decimal(amount),
                use_atokens=use_atokens,
            )

        except Exception as e:
            logger.warning(f"Failed to parse Repay event: {e}")
            return None

    # Backward compatibility methods
    def parse_supply(self, log: dict[str, Any]) -> SupplyEventData | None:
        """Parse a Supply event from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))
        return self._parse_supply_log(topics, data)

    def parse_borrow(self, log: dict[str, Any]) -> BorrowEventData | None:
        """Parse a Borrow event from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))
        return self._parse_borrow_log(topics, data)

    def parse_repay(self, log: dict[str, Any]) -> RepayEventData | None:
        """Parse a Repay event from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))
        return self._parse_repay_log(topics, data)

    def parse_withdraw(self, log: dict[str, Any]) -> WithdrawEventData | None:
        """Parse a Withdraw event from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))
        return self._parse_withdraw_log(topics, data)

    def is_spark_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Spark event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known Spark event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes) -> SparkEventType:
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
        return self.registry.get_event_type_from_topic(topic) or SparkEventType.UNKNOWN

    def is_spark_pool(self, address: str) -> bool:
        """Check if an address is a known Spark pool."""
        return address.lower() in self._pool_addresses

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    def extract_supply_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract supply amount from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Supply amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.supplies:
                return None
            # Return the first supply amount as int
            return int(result.supplies[0].amount)
        except Exception as e:
            logger.warning(f"Failed to extract supply amount: {e}")
            return None

    def extract_withdraw_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract withdraw amount from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Withdraw amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.withdraws:
                return None
            return int(result.withdraws[0].amount)
        except Exception as e:
            logger.warning(f"Failed to extract withdraw amount: {e}")
            return None

    def extract_borrow_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract borrow amount from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Borrow amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.borrows:
                return None
            return int(result.borrows[0].amount)
        except Exception as e:
            logger.warning(f"Failed to extract borrow amount: {e}")
            return None

    def extract_repay_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract repay amount from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Repay amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.repays:
                return None
            return int(result.repays[0].amount)
        except Exception as e:
            logger.warning(f"Failed to extract repay amount: {e}")
            return None

    def extract_a_token_received(self, receipt: dict[str, Any]) -> int | None:
        """Extract spToken amount received from transaction receipt.

        Spark uses spTokens (similar to Aave's aTokens) to represent deposits.
        This extracts the amount from Transfer events (mint from zero address).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            spToken amount minted if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            # Transfer topic: keccak256("Transfer(address,address,uint256)")
            transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            zero_address = "0x0000000000000000000000000000000000000000"

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 3:
                    continue

                # Normalize first topic
                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != transfer_topic:
                    continue

                # Check if from address is zero (mint)
                from_addr = HexDecoder.topic_to_address(topics[1])
                if from_addr.lower() != zero_address:
                    continue

                # Extract amount from data
                data = HexDecoder.normalize_hex(log.get("data", ""))
                if len(data) < 64:
                    continue

                amount = HexDecoder.decode_uint256(data, 0)
                return amount

            return None
        except Exception as e:
            logger.warning(f"Failed to extract spToken received: {e}")
            return None

    def extract_borrow_rate(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract borrow rate from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Borrow rate as Decimal (already converted from ray) if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.borrows:
                return None
            return result.borrows[0].borrow_rate
        except Exception as e:
            logger.warning(f"Failed to extract borrow rate: {e}")
            return None


__all__ = [
    "SparkReceiptParser",
    "SparkEventType",
    "SupplyEventData",
    "WithdrawEventData",
    "BorrowEventData",
    "RepayEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "SPARK_POOL_ADDRESSES",
]
