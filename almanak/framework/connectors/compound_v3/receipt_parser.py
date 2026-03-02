"""Compound V3 (Comet) Receipt Parser (Refactored).

Refactored to use base infrastructure utilities while maintaining backward compatibility.
Compound V3 supports base asset lending and collateral management with liquidation events.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.base import EventRegistry, HexDecoder
from almanak.framework.utils.log_formatters import format_address, format_gas_cost, format_tx_hash

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    # Base asset events
    "Supply": "0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e",
    "Withdraw": "0x9b1bfa7fa9ee420a16e124f794c35ac9f90472acc99140eb2f6447c714cad8eb",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    # Collateral events
    "SupplyCollateral": "0xfa56f7b24f17183d81894d3ac2ee654e3c26388d17a28dbd9549b8114304e1f4",
    "WithdrawCollateral": "0xd6d480d5b3068db003533b170d67561494d72e3bf9fa40a266471351ebba9e16",
    "TransferCollateral": "0x29db89d45e1a802b4d55e202984fce9faf1d30aedf86503ff1ea0ed9ebb64201",
    # Liquidation events
    "AbsorbDebt": "0x1547a878dc89ad3c367b6338b4be6a65a5dd74fb77ae044da1e8747ef1f4f62f",
    "AbsorbCollateral": "0x9850ab1af75177e4a9201c65a2cf7976d5d28e40ef63494b44366f86b2f9412e",
    "BuyCollateral": "0xf891b2a411b0e66a5f0a6ff1368670fefa287a13f541eb633a386a1a9cc7046b",
    # Administrative events
    "PauseAction": "0x3be39979091ae7ca962aa1c44e645f2df3c221b79f324afa5f44aedc8d2f690d",
    "WithdrawReserves": "0xec4431f2ba1a9382f6b0c4352b888cba6f7db91667d9f776abe5ad8ddc5401b6",
    # ERC20 events
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class CompoundV3EventType(Enum):
    """Compound V3 event types."""

    # Base asset events
    SUPPLY = "SUPPLY"
    WITHDRAW = "WITHDRAW"
    TRANSFER = "TRANSFER"
    # Collateral events
    SUPPLY_COLLATERAL = "SUPPLY_COLLATERAL"
    WITHDRAW_COLLATERAL = "WITHDRAW_COLLATERAL"
    TRANSFER_COLLATERAL = "TRANSFER_COLLATERAL"
    # Liquidation events
    ABSORB_DEBT = "ABSORB_DEBT"
    ABSORB_COLLATERAL = "ABSORB_COLLATERAL"
    BUY_COLLATERAL = "BUY_COLLATERAL"
    # Administrative
    PAUSE_ACTION = "PAUSE_ACTION"
    WITHDRAW_RESERVES = "WITHDRAW_RESERVES"
    # Token events
    APPROVAL = "APPROVAL"
    # Unknown
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, CompoundV3EventType] = {
    "Supply": CompoundV3EventType.SUPPLY,
    "Withdraw": CompoundV3EventType.WITHDRAW,
    "Transfer": CompoundV3EventType.TRANSFER,
    "SupplyCollateral": CompoundV3EventType.SUPPLY_COLLATERAL,
    "WithdrawCollateral": CompoundV3EventType.WITHDRAW_COLLATERAL,
    "TransferCollateral": CompoundV3EventType.TRANSFER_COLLATERAL,
    "AbsorbDebt": CompoundV3EventType.ABSORB_DEBT,
    "AbsorbCollateral": CompoundV3EventType.ABSORB_COLLATERAL,
    "BuyCollateral": CompoundV3EventType.BUY_COLLATERAL,
    "PauseAction": CompoundV3EventType.PAUSE_ACTION,
    "WithdrawReserves": CompoundV3EventType.WITHDRAW_RESERVES,
    "Approval": CompoundV3EventType.APPROVAL,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class CompoundV3Event:
    """Parsed Compound V3 event."""

    event_type: CompoundV3EventType
    event_name: str
    log_index: int
    transaction_hash: str
    block_number: int
    contract_address: str
    data: dict[str, Any]
    raw_topics: list[str] = field(default_factory=list)
    raw_data: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=None))

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
    def from_dict(cls, data: dict[str, Any]) -> "CompoundV3Event":
        """Create from dictionary."""
        return cls(
            event_type=CompoundV3EventType(data["event_type"]),
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
    events: list[CompoundV3Event] = field(default_factory=list)
    supply_amount: Decimal = Decimal("0")
    withdraw_amount: Decimal = Decimal("0")
    collateral_supplied: dict[str, Decimal] = field(default_factory=dict)
    collateral_withdrawn: dict[str, Decimal] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "supply_amount": str(self.supply_amount),
            "withdraw_amount": str(self.withdraw_amount),
            "collateral_supplied": {k: str(v) for k, v in self.collateral_supplied.items()},
            "collateral_withdrawn": {k: str(v) for k, v in self.collateral_withdrawn.items()},
            "error": self.error,
        }


# =============================================================================
# Parser
# =============================================================================


class CompoundV3ReceiptParser:
    """Parser for Compound V3 transaction receipts.

    Refactored to use base infrastructure utilities for hex decoding
    and event registry management. Maintains all event parsing and
    aggregation logic.
    """

    def __init__(self, base_decimals: int = 6, **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            base_decimals: Decimals for the base token (default 6 for USDC)
            **kwargs: Additional arguments (ignored for compatibility)
        """
        self.base_decimals = base_decimals
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def parse_receipt(
        self,
        receipt: dict[str, Any],
        comet_address: str | None = None,
    ) -> ParseResult:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dictionary
            comet_address: Optional Comet contract address to filter events

        Returns:
            ParseResult with parsed events and aggregated data
        """
        try:
            logs = receipt.get("logs", [])
            tx_hash = receipt.get("transactionHash", "") or receipt.get("hash", "")
            if isinstance(tx_hash, bytes):
                tx_hash = "0x" + tx_hash.hex()
            block_number = receipt.get("blockNumber", 0)

            events: list[CompoundV3Event] = []
            supply_amount = Decimal("0")
            withdraw_amount = Decimal("0")
            collateral_supplied: dict[str, Decimal] = {}
            collateral_withdrawn: dict[str, Decimal] = {}

            for log in logs:
                # Filter by contract address if provided
                if comet_address:
                    log_address = log.get("address", "").lower()
                    if log_address != comet_address.lower():
                        continue

                event = self._parse_log(log, tx_hash, block_number)
                if event:
                    events.append(event)

                    # Aggregate amounts
                    if event.event_type == CompoundV3EventType.SUPPLY:
                        supply_amount += event.data.get("amount", Decimal("0"))
                    elif event.event_type == CompoundV3EventType.WITHDRAW:
                        withdraw_amount += event.data.get("amount", Decimal("0"))
                    elif event.event_type == CompoundV3EventType.SUPPLY_COLLATERAL:
                        asset = event.data.get("asset", "unknown")
                        amount = event.data.get("amount", Decimal("0"))
                        collateral_supplied[asset] = collateral_supplied.get(asset, Decimal("0")) + amount
                    elif event.event_type == CompoundV3EventType.WITHDRAW_COLLATERAL:
                        asset = event.data.get("asset", "unknown")
                        amount = event.data.get("amount", Decimal("0"))
                        collateral_withdrawn[asset] = collateral_withdrawn.get(asset, Decimal("0")) + amount

            # Log parsed receipt with user-friendly formatting
            gas_used = receipt.get("gasUsed", 0)
            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(gas_used)

            # Build summary of actions
            actions = []
            if supply_amount > 0:
                actions.append(f"SUPPLY {supply_amount:,.0f}")
            if withdraw_amount > 0:
                actions.append(f"WITHDRAW {withdraw_amount:,.0f}")
            for asset, amount in collateral_supplied.items():
                if amount > 0:
                    actions.append(f"SUPPLY_COLLATERAL {amount:,.0f} {format_address(asset)}")
            for asset, amount in collateral_withdrawn.items():
                if amount > 0:
                    actions.append(f"WITHDRAW_COLLATERAL {amount:,.0f} {format_address(asset)}")

            if actions:
                logger.info(f"🔍 Parsed Compound V3: {', '.join(actions)}, tx={tx_fmt}, {gas_fmt}")
            else:
                logger.info(f"🔍 Parsed Compound V3 receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                events=events,
                supply_amount=supply_amount,
                withdraw_amount=withdraw_amount,
                collateral_supplied=collateral_supplied,
                collateral_withdrawn=collateral_withdrawn,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(success=False, error=str(e))

    def parse_logs(
        self,
        logs: list[dict[str, Any]],
        tx_hash: str = "",
        block_number: int = 0,
    ) -> list[CompoundV3Event]:
        """Parse a list of logs."""
        events = []
        for log in logs:
            event = self._parse_log(log, tx_hash, block_number)
            if event:
                events.append(event)
        return events

    def _parse_log(
        self,
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
    ) -> CompoundV3Event | None:
        """Parse a single log entry."""
        topics = log.get("topics", [])
        if not topics:
            return None

        # Normalize first topic
        first_topic = topics[0]
        if isinstance(first_topic, bytes):
            first_topic = "0x" + first_topic.hex()
        elif not first_topic.startswith("0x"):
            first_topic = "0x" + first_topic
        first_topic = first_topic.lower()

        event_name = self.registry.get_event_name(first_topic)
        if not event_name:
            return None

        event_type = self.registry.get_event_type(event_name) or CompoundV3EventType.UNKNOWN
        log_index = log.get("logIndex", 0)
        contract_address = log.get("address", "")
        raw_data = log.get("data", "")

        # Parse event data based on type
        data = self._parse_event_data(event_name, topics, raw_data)

        return CompoundV3Event(
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

    def _parse_event_data(
        self,
        event_name: str,
        topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Parse event-specific data."""
        try:
            # Normalize data
            data_hex = HexDecoder.normalize_hex(data)

            if event_name == "Supply":
                return self._parse_supply_event(topics, data_hex)
            elif event_name == "Withdraw":
                return self._parse_withdraw_event(topics, data_hex)
            elif event_name == "SupplyCollateral":
                return self._parse_supply_collateral_event(topics, data_hex)
            elif event_name == "WithdrawCollateral":
                return self._parse_withdraw_collateral_event(topics, data_hex)
            elif event_name == "Transfer":
                return self._parse_transfer_event(topics, data_hex)
            elif event_name == "TransferCollateral":
                return self._parse_transfer_collateral_event(topics, data_hex)
            elif event_name == "AbsorbDebt":
                return self._parse_absorb_debt_event(topics, data_hex)
            elif event_name == "AbsorbCollateral":
                return self._parse_absorb_collateral_event(topics, data_hex)
            elif event_name == "BuyCollateral":
                return self._parse_buy_collateral_event(topics, data_hex)
            else:
                return {"raw": data}
        except Exception as e:
            logger.warning(f"Failed to parse {event_name} event data: {e}")
            return {"raw": data, "error": str(e)}

    def _parse_supply_event(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse Supply event.

        event Supply(address indexed from, address indexed dst, uint amount)
        """
        from_address = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        dst = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        amount = Decimal(HexDecoder.decode_uint256(data, 0)) if data else Decimal("0")

        return {
            "from_address": from_address,
            "dst": dst,
            "amount": amount,
        }

    def _parse_withdraw_event(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse Withdraw event.

        event Withdraw(address indexed src, address indexed to, uint amount)
        """
        src = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        to = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        amount = Decimal(HexDecoder.decode_uint256(data, 0)) if data else Decimal("0")

        return {
            "src": src,
            "to": to,
            "amount": amount,
        }

    def _parse_supply_collateral_event(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse SupplyCollateral event.

        event SupplyCollateral(address indexed from, address indexed dst, address indexed asset, uint amount)
        """
        from_address = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        dst = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        asset = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""
        amount = Decimal(HexDecoder.decode_uint256(data, 0)) if data else Decimal("0")

        return {
            "from_address": from_address,
            "dst": dst,
            "asset": asset,
            "amount": amount,
        }

    def _parse_withdraw_collateral_event(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse WithdrawCollateral event.

        event WithdrawCollateral(address indexed src, address indexed to, address indexed asset, uint amount)
        """
        src = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        to = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        asset = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""
        amount = Decimal(HexDecoder.decode_uint256(data, 0)) if data else Decimal("0")

        return {
            "src": src,
            "to": to,
            "asset": asset,
            "amount": amount,
        }

    def _parse_transfer_event(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse Transfer event (ERC20 standard).

        event Transfer(address indexed from, address indexed to, uint256 amount)
        """
        from_address = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        to = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        amount = Decimal(HexDecoder.decode_uint256(data, 0)) if data else Decimal("0")

        return {
            "from_address": from_address,
            "to": to,
            "amount": amount,
        }

    def _parse_transfer_collateral_event(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse TransferCollateral event.

        event TransferCollateral(address indexed from, address indexed to, address indexed asset, uint amount)
        """
        from_address = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        to = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        asset = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""
        amount = Decimal(HexDecoder.decode_uint256(data, 0)) if data else Decimal("0")

        return {
            "from_address": from_address,
            "to": to,
            "asset": asset,
            "amount": amount,
        }

    def _parse_absorb_debt_event(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse AbsorbDebt event.

        event AbsorbDebt(address indexed absorber, address indexed borrower, uint basePaidOut, uint usdValue)
        """
        absorber = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        borrower = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

        # Data contains basePaidOut and usdValue
        base_paid_out = Decimal(HexDecoder.decode_uint256(data, 0))
        usd_value = Decimal(HexDecoder.decode_uint256(data, 32))

        return {
            "absorber": absorber,
            "borrower": borrower,
            "base_paid_out": base_paid_out,
            "usd_value": usd_value,
        }

    def _parse_absorb_collateral_event(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse AbsorbCollateral event.

        event AbsorbCollateral(address indexed absorber, address indexed borrower, address indexed asset,
                               uint collateralAbsorbed, uint usdValue)
        """
        absorber = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        borrower = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        asset = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

        # Data contains collateralAbsorbed and usdValue
        collateral_absorbed = Decimal(HexDecoder.decode_uint256(data, 0))
        usd_value = Decimal(HexDecoder.decode_uint256(data, 32))

        return {
            "absorber": absorber,
            "borrower": borrower,
            "asset": asset,
            "collateral_absorbed": collateral_absorbed,
            "usd_value": usd_value,
        }

    def _parse_buy_collateral_event(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse BuyCollateral event.

        event BuyCollateral(address indexed buyer, address indexed asset, uint baseAmount, uint collateralAmount)
        """
        buyer = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        asset = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

        # Data contains baseAmount and collateralAmount
        base_amount = Decimal(HexDecoder.decode_uint256(data, 0))
        collateral_amount = Decimal(HexDecoder.decode_uint256(data, 32))

        return {
            "buyer": buyer,
            "asset": asset,
            "base_amount": base_amount,
            "collateral_amount": collateral_amount,
        }

    # Backward compatibility methods
    def is_compound_v3_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Compound V3 event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known Compound V3 event
        """
        # Normalize topic to lowercase hex string with 0x prefix
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        elif not str(topic).startswith("0x"):
            topic = "0x" + str(topic)
        else:
            topic = str(topic)
        topic = topic.lower()

        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes) -> CompoundV3EventType:
        """Get the event type for a topic.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            Event type enum or UNKNOWN if not recognized
        """
        # Normalize topic to lowercase hex string with 0x prefix
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        elif not str(topic).startswith("0x"):
            topic = "0x" + str(topic)
        else:
            topic = str(topic)
        topic = topic.lower()

        return self.registry.get_event_type_from_topic(topic) or CompoundV3EventType.UNKNOWN

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    def extract_supply_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract supply amount from transaction receipt.

        In Compound V3, supplying the base asset increases lending position
        and can also repay borrowed debt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Supply amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.supply_amount > 0:
                return int(result.supply_amount)
            return None
        except Exception as e:
            logger.warning(f"Failed to extract supply amount: {e}")
            return None

    def extract_withdraw_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract withdraw amount from transaction receipt.

        In Compound V3, withdrawing the base asset decreases lending position
        and can create a borrow if withdrawn amount exceeds supplied amount.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Withdraw amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.withdraw_amount > 0:
                return int(result.withdraw_amount)
            return None
        except Exception as e:
            logger.warning(f"Failed to extract withdraw amount: {e}")
            return None

    def extract_borrow_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract borrow amount from transaction receipt.

        In Compound V3, borrowing is done via the Withdraw event when the
        user withdraws more than their supplied balance. This method returns
        the withdraw amount which represents the borrowed amount in that case.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Borrow (withdraw) amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.withdraw_amount > 0:
                return int(result.withdraw_amount)
            return None
        except Exception as e:
            logger.warning(f"Failed to extract borrow amount: {e}")
            return None

    def extract_repay_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract repay amount from transaction receipt.

        In Compound V3, repaying is done via the Supply event when the user
        has outstanding debt. This method returns the supply amount which
        represents the repaid amount in that case.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Repay (supply) amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.supply_amount > 0:
                return int(result.supply_amount)
            return None
        except Exception as e:
            logger.warning(f"Failed to extract repay amount: {e}")
            return None


__all__ = [
    "CompoundV3ReceiptParser",
    "CompoundV3EventType",
    "CompoundV3Event",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
