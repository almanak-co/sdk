"""Morpho Blue Receipt Parser (Refactored).

This module provides parsing functionality for Morpho Blue transaction receipts
and events, enabling extraction of supply, borrow, repay, withdraw, and other
protocol events from on-chain data.

Morpho Blue Events:
- Supply: Assets supplied to a market (for lending)
- Withdraw: Assets withdrawn from a market
- Borrow: Assets borrowed from a market
- Repay: Debt repaid to a market
- SupplyCollateral: Collateral supplied to a market
- WithdrawCollateral: Collateral withdrawn from a market
- Liquidate: Position liquidated
- FlashLoan: Flash loan executed
- CreateMarket: New market created
- SetAuthorization: Authorization changed
- AccrueInterest: Interest accrued on a market

Example:
    from almanak.framework.connectors.morpho_blue import MorphoBlueReceiptParser

    parser = MorphoBlueReceiptParser()

    # Parse transaction receipt
    result = parser.parse_receipt(receipt)

    for event in result.events:
        if event.event_type == MorphoBlueEventType.SUPPLY:
            print(f"Supply: {event.data}")
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.base.hex_utils import HexDecoder
from almanak.framework.connectors.base.registry import EventRegistry
from almanak.framework.utils.log_formatters import format_gas_cost, format_tx_hash

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class MorphoBlueEventType(Enum):
    """Morpho Blue event types."""

    # Core lending events
    SUPPLY = "SUPPLY"
    WITHDRAW = "WITHDRAW"
    BORROW = "BORROW"
    REPAY = "REPAY"

    # Collateral events
    SUPPLY_COLLATERAL = "SUPPLY_COLLATERAL"
    WITHDRAW_COLLATERAL = "WITHDRAW_COLLATERAL"

    # Liquidation
    LIQUIDATE = "LIQUIDATE"

    # Flash loan
    FLASH_LOAN = "FLASH_LOAN"

    # Market events
    CREATE_MARKET = "CREATE_MARKET"

    # Authorization
    SET_AUTHORIZATION = "SET_AUTHORIZATION"

    # Interest accrual
    ACCRUE_INTEREST = "ACCRUE_INTEREST"

    # Token events
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"

    # Unknown
    UNKNOWN = "UNKNOWN"


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    "Supply": "0xedf8870433c83823eb071d3df1caa8d008f12f6440918c20d75a3602cda30fe0",
    "Withdraw": "0xa56fc0ad5702ec05ce63666221f796fb62437c32db1aa1aa075fc6484cf58fbf",
    "Borrow": "0x570954540bed6b1304a87dfe815a5eda4a648f7097a16240dcd85c9b5fd42a43",
    "Repay": "0x52acb05cebbd3cd39715469f22afbf5a17496295ef3bc9bb5944056c63ccaa09",
    "SupplyCollateral": "0xa3b9472a1399e17e123f3c2e6586c23e504184d504de59cdaa2b375e880c6184",
    "WithdrawCollateral": "0xe80ebd7cc9223d7382aab2e0d1d6155c65651f83d53c8b9b06901d167e321142",
    "Liquidate": "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41",
    "FlashLoan": "0xc76f1b4fe4396ac07a9fa55a415d4ca430e72651d37d3401f3bed7cb13fc4f12",
    "CreateMarket": "0xac4b2400f169220b0c0afdde7a0b32e775ba727ea1cb30b35f935cdaab8683ac",
    "SetAuthorization": "0xd5e969f01efe921d3f766bdebad25f0a05e3f237311f56482bf132d0326309c0",
    "AccrueInterest": "0x9d9bd501d0657d7dfe415f779a620a62b78bc508ddc0891fbbd8b7ac0f8fce87",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

EVENT_NAME_TO_TYPE: dict[str, MorphoBlueEventType] = {
    "Supply": MorphoBlueEventType.SUPPLY,
    "Withdraw": MorphoBlueEventType.WITHDRAW,
    "Borrow": MorphoBlueEventType.BORROW,
    "Repay": MorphoBlueEventType.REPAY,
    "SupplyCollateral": MorphoBlueEventType.SUPPLY_COLLATERAL,
    "WithdrawCollateral": MorphoBlueEventType.WITHDRAW_COLLATERAL,
    "Liquidate": MorphoBlueEventType.LIQUIDATE,
    "FlashLoan": MorphoBlueEventType.FLASH_LOAN,
    "CreateMarket": MorphoBlueEventType.CREATE_MARKET,
    "SetAuthorization": MorphoBlueEventType.SET_AUTHORIZATION,
    "AccrueInterest": MorphoBlueEventType.ACCRUE_INTEREST,
    "Transfer": MorphoBlueEventType.TRANSFER,
    "Approval": MorphoBlueEventType.APPROVAL,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class MorphoBlueEvent:
    """Parsed Morpho Blue event.

    Attributes:
        event_type: Type of event
        event_name: Name of event (e.g., "Supply")
        log_index: Index of log in transaction
        transaction_hash: Transaction hash
        block_number: Block number
        contract_address: Contract that emitted event
        data: Parsed event data
        raw_topics: Raw event topics
        raw_data: Raw event data
        timestamp: Event timestamp
    """

    event_type: MorphoBlueEventType
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
    def from_dict(cls, data: dict[str, Any]) -> "MorphoBlueEvent":
        """Create from dictionary."""
        return cls(
            event_type=MorphoBlueEventType(data["event_type"]),
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
class SupplyEventData:
    """Parsed data from Supply event.

    In Morpho Blue, Supply is for lending assets to earn interest.

    Attributes:
        market_id: Unique market identifier
        caller: Address that initiated the supply
        on_behalf_of: Address that received the supply shares
        assets: Amount of assets supplied
        shares: Amount of shares minted
    """

    market_id: str
    caller: str
    on_behalf_of: str
    assets: Decimal
    shares: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "caller": self.caller,
            "on_behalf_of": self.on_behalf_of,
            "assets": str(self.assets),
            "shares": str(self.shares),
        }


@dataclass
class WithdrawEventData:
    """Parsed data from Withdraw event.

    In Morpho Blue, Withdraw is for withdrawing supplied (lending) assets.

    Attributes:
        market_id: Unique market identifier
        caller: Address that initiated the withdrawal
        on_behalf_of: Address whose shares were burned
        receiver: Address that received the assets
        assets: Amount of assets withdrawn
        shares: Amount of shares burned
    """

    market_id: str
    caller: str
    on_behalf_of: str
    receiver: str
    assets: Decimal
    shares: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "caller": self.caller,
            "on_behalf_of": self.on_behalf_of,
            "receiver": self.receiver,
            "assets": str(self.assets),
            "shares": str(self.shares),
        }


@dataclass
class BorrowEventData:
    """Parsed data from Borrow event.

    Attributes:
        market_id: Unique market identifier
        caller: Address that initiated the borrow
        on_behalf_of: Address that received the debt
        receiver: Address that received the borrowed assets
        assets: Amount of assets borrowed
        shares: Amount of borrow shares minted
    """

    market_id: str
    caller: str
    on_behalf_of: str
    receiver: str
    assets: Decimal
    shares: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "caller": self.caller,
            "on_behalf_of": self.on_behalf_of,
            "receiver": self.receiver,
            "assets": str(self.assets),
            "shares": str(self.shares),
        }


@dataclass
class RepayEventData:
    """Parsed data from Repay event.

    Attributes:
        market_id: Unique market identifier
        caller: Address that made the repayment
        on_behalf_of: Address whose debt was repaid
        assets: Amount of assets repaid
        shares: Amount of borrow shares burned
    """

    market_id: str
    caller: str
    on_behalf_of: str
    assets: Decimal
    shares: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "caller": self.caller,
            "on_behalf_of": self.on_behalf_of,
            "assets": str(self.assets),
            "shares": str(self.shares),
        }


@dataclass
class SupplyCollateralEventData:
    """Parsed data from SupplyCollateral event.

    Attributes:
        market_id: Unique market identifier
        caller: Address that initiated the collateral supply
        on_behalf_of: Address that received the collateral credit
        assets: Amount of collateral supplied
    """

    market_id: str
    caller: str
    on_behalf_of: str
    assets: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "caller": self.caller,
            "on_behalf_of": self.on_behalf_of,
            "assets": str(self.assets),
        }


@dataclass
class WithdrawCollateralEventData:
    """Parsed data from WithdrawCollateral event.

    Attributes:
        market_id: Unique market identifier
        caller: Address that initiated the withdrawal
        on_behalf_of: Address whose collateral was withdrawn
        receiver: Address that received the collateral
        assets: Amount of collateral withdrawn
    """

    market_id: str
    caller: str
    on_behalf_of: str
    receiver: str
    assets: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "caller": self.caller,
            "on_behalf_of": self.on_behalf_of,
            "receiver": self.receiver,
            "assets": str(self.assets),
        }


@dataclass
class LiquidateEventData:
    """Parsed data from Liquidate event.

    Attributes:
        market_id: Unique market identifier
        caller: Address that initiated the liquidation
        borrower: Address of the borrower being liquidated
        repaid_assets: Amount of debt repaid
        repaid_shares: Amount of borrow shares burned
        seized_assets: Amount of collateral seized
        bad_debt_assets: Amount of bad debt (if any)
        bad_debt_shares: Amount of bad debt shares (if any)
    """

    market_id: str
    caller: str
    borrower: str
    repaid_assets: Decimal
    repaid_shares: Decimal
    seized_assets: Decimal
    bad_debt_assets: Decimal = Decimal("0")
    bad_debt_shares: Decimal = Decimal("0")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "caller": self.caller,
            "borrower": self.borrower,
            "repaid_assets": str(self.repaid_assets),
            "repaid_shares": str(self.repaid_shares),
            "seized_assets": str(self.seized_assets),
            "bad_debt_assets": str(self.bad_debt_assets),
            "bad_debt_shares": str(self.bad_debt_shares),
        }


@dataclass
class FlashLoanEventData:
    """Parsed data from FlashLoan event.

    Attributes:
        caller: Address that initiated the flash loan
        token: Address of the token borrowed
        assets: Amount borrowed
    """

    caller: str
    token: str
    assets: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "caller": self.caller,
            "token": self.token,
            "assets": str(self.assets),
        }


@dataclass
class CreateMarketEventData:
    """Parsed data from CreateMarket event.

    Attributes:
        market_id: Unique market identifier
        loan_token: Address of the loan token
        collateral_token: Address of the collateral token
        oracle: Address of the oracle
        irm: Address of the interest rate model
        lltv: Liquidation LTV (1e18 scale)
    """

    market_id: str
    loan_token: str
    collateral_token: str
    oracle: str
    irm: str
    lltv: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "loan_token": self.loan_token,
            "collateral_token": self.collateral_token,
            "oracle": self.oracle,
            "irm": self.irm,
            "lltv": self.lltv,
            "lltv_percent": self.lltv / 1e16,
        }


@dataclass
class SetAuthorizationEventData:
    """Parsed data from SetAuthorization event.

    Attributes:
        caller: Address that set the authorization
        authorizer: Address that granted the authorization
        authorized: Address that received the authorization
        is_authorized: Whether authorization was granted or revoked
    """

    caller: str
    authorizer: str
    authorized: str
    is_authorized: bool

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "caller": self.caller,
            "authorizer": self.authorizer,
            "authorized": self.authorized,
            "is_authorized": self.is_authorized,
        }


@dataclass
class AccrueInterestEventData:
    """Parsed data from AccrueInterest event.

    Attributes:
        market_id: Unique market identifier
        prev_borrow_rate: Previous borrow rate (per second, 1e18 scale)
        interest: Total interest accrued
        fee_shares: Shares minted as protocol fee
    """

    market_id: str
    prev_borrow_rate: Decimal
    interest: Decimal
    fee_shares: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "prev_borrow_rate": str(self.prev_borrow_rate),
            "interest": str(self.interest),
            "fee_shares": str(self.fee_shares),
        }


@dataclass
class TransferEventData:
    """Parsed data from ERC20 Transfer event.

    Attributes:
        from_address: Sender address
        to_address: Recipient address
        amount: Amount transferred
    """

    from_address: str
    to_address: str
    amount: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "from": self.from_address,
            "to": self.to_address,
            "amount": str(self.amount),
        }


@dataclass
class ParseResult:
    """Result of parsing a transaction receipt.

    Attributes:
        success: Whether parsing succeeded
        events: List of parsed events
        error: Error message if parsing failed
        transaction_hash: Hash of the parsed transaction
        block_number: Block number
    """

    success: bool
    events: list[MorphoBlueEvent] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class MorphoBlueReceiptParser:
    """Parser for Morpho Blue transaction receipts.

    This parser extracts and decodes events from Morpho Blue transactions,
    providing structured data for supply, borrow, repay, withdraw, liquidation,
    and other protocol operations.

    Example:
        parser = MorphoBlueReceiptParser()

        # Parse a receipt
        result = parser.parse_receipt(receipt)

        if result.success:
            for event in result.events:
                print(f"{event.event_name}: {event.data}")
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            **kwargs: Additional arguments (ignored for compatibility)
        """
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        logger.info("MorphoBlueReceiptParser initialized")

    def parse_receipt(
        self,
        receipt: dict[str, Any],
        timestamp: datetime | None = None,
    ) -> ParseResult:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dictionary with 'logs' field
            timestamp: Optional timestamp for events

        Returns:
            ParseResult with parsed events
        """
        try:
            logs = receipt.get("logs", [])
            transaction_hash = receipt.get("transactionHash", receipt.get("hash", ""))
            block_number = receipt.get("blockNumber", 0)

            # Normalize transaction hash
            if isinstance(transaction_hash, bytes):
                transaction_hash = "0x" + transaction_hash.hex()

            # Normalize block number
            if isinstance(block_number, str):
                block_number = int(block_number, 16) if block_number.startswith("0x") else int(block_number)

            events: list[MorphoBlueEvent] = []

            for i, log in enumerate(logs):
                event = self._parse_log(log, i, transaction_hash, block_number, timestamp)
                if event is not None:
                    events.append(event)

            # Log parsed receipt with user-friendly formatting
            gas_used = receipt.get("gasUsed", 0)
            tx_fmt = format_tx_hash(transaction_hash)
            gas_fmt = format_gas_cost(gas_used)

            # Count event types for summary
            event_types: dict[str, int] = {}
            for e in events:
                event_types[e.event_type.value] = event_types.get(e.event_type.value, 0) + 1

            # Build summary
            if event_types:
                summary = ", ".join(f"{k}={v}" for k, v in event_types.items() if k != "UNKNOWN")
                if summary:
                    logger.info(f"Parsed Morpho Blue: {summary}, tx={tx_fmt}, {gas_fmt}")
                else:
                    logger.info(f"Parsed Morpho Blue receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")
            else:
                logger.info(f"Parsed Morpho Blue receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                events=events,
                transaction_hash=transaction_hash,
                block_number=block_number,
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
        log_index: int,
        transaction_hash: str,
        block_number: int,
        timestamp: datetime | None,
    ) -> MorphoBlueEvent | None:
        """Parse a single log entry."""
        try:
            topics = log.get("topics", [])
            if not topics:
                return None

            # Normalize topic format (ensure 0x prefix and lowercase)
            topic0 = topics[0]
            if isinstance(topic0, bytes):
                topic0 = "0x" + topic0.hex()
            topic0 = topic0.lower()

            # Look up event name
            event_name = self.registry.get_event_name(topic0)
            if event_name is None:
                return None

            event_type = self.registry.get_event_type(event_name)
            if event_type is None:
                event_type = MorphoBlueEventType.UNKNOWN

            # Get contract address
            contract_address = log.get("address", "")
            if isinstance(contract_address, bytes):
                contract_address = "0x" + contract_address.hex()

            # Get raw data
            raw_data = log.get("data", "")
            if isinstance(raw_data, bytes):
                raw_data = "0x" + raw_data.hex()

            # Normalize topics (ensure 0x prefix and lowercase)
            raw_topics = []
            for t in topics:
                if isinstance(t, bytes):
                    raw_topics.append("0x" + t.hex())
                else:
                    raw_topics.append(t.lower() if t.startswith("0x") else "0x" + t.lower())

            # Parse event data
            parsed_data = self._parse_event_data(event_name, raw_topics, raw_data)

            return MorphoBlueEvent(
                event_type=event_type,
                event_name=event_name,
                log_index=log_index,
                transaction_hash=transaction_hash,
                block_number=block_number,
                contract_address=contract_address,
                data=parsed_data,
                raw_topics=raw_topics,
                raw_data=raw_data,
                timestamp=timestamp or datetime.now(tz=None),
            )

        except Exception as e:
            logger.warning(f"Failed to parse log at index {log_index}: {e}")
            return None

    def _parse_event_data(
        self,
        event_name: str,
        topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Parse event-specific data."""
        # Remove 0x prefix from data
        data_hex = data[2:] if data.startswith("0x") else data

        if event_name == "Supply":
            return self._parse_supply(topics, data_hex)
        elif event_name == "Withdraw":
            return self._parse_withdraw(topics, data_hex)
        elif event_name == "Borrow":
            return self._parse_borrow(topics, data_hex)
        elif event_name == "Repay":
            return self._parse_repay(topics, data_hex)
        elif event_name == "SupplyCollateral":
            return self._parse_supply_collateral(topics, data_hex)
        elif event_name == "WithdrawCollateral":
            return self._parse_withdraw_collateral(topics, data_hex)
        elif event_name == "Liquidate":
            return self._parse_liquidate(topics, data_hex)
        elif event_name == "FlashLoan":
            return self._parse_flash_loan(topics, data_hex)
        elif event_name == "CreateMarket":
            return self._parse_create_market(topics, data_hex)
        elif event_name == "SetAuthorization":
            return self._parse_set_authorization(topics, data_hex)
        elif event_name == "AccrueInterest":
            return self._parse_accrue_interest(topics, data_hex)
        elif event_name == "Transfer":
            return self._parse_transfer(topics, data_hex)
        elif event_name == "Approval":
            return self._parse_approval(topics, data_hex)
        else:
            return {"raw_data": data}

    def _parse_supply(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse Supply event.

        event Supply(Id indexed id, address indexed caller, address indexed onBehalfOf, uint256 assets, uint256 shares)
        """
        market_id = topics[1] if len(topics) > 1 else ""
        caller = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        on_behalf_of = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

        # Data: assets (uint256), shares (uint256)
        assets = HexDecoder.decode_uint256(data, 0)
        shares = HexDecoder.decode_uint256(data, 32)

        return SupplyEventData(
            market_id=market_id,
            caller=caller,
            on_behalf_of=on_behalf_of,
            assets=Decimal(assets),
            shares=Decimal(shares),
        ).to_dict()

    def _parse_withdraw(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse Withdraw event.

        event Withdraw(Id indexed id, address caller, address indexed onBehalfOf, address indexed receiver, uint256 assets, uint256 shares)
        """
        market_id = topics[1] if len(topics) > 1 else ""
        on_behalf_of = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        receiver = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

        # Data: caller (address), assets (uint256), shares (uint256)
        caller = "0x" + data[24:64].lower()
        assets = HexDecoder.decode_uint256(data, 32)
        shares = HexDecoder.decode_uint256(data, 64)

        return WithdrawEventData(
            market_id=market_id,
            caller=caller,
            on_behalf_of=on_behalf_of,
            receiver=receiver,
            assets=Decimal(assets),
            shares=Decimal(shares),
        ).to_dict()

    def _parse_borrow(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse Borrow event.

        event Borrow(Id indexed id, address caller, address indexed onBehalfOf, address indexed receiver, uint256 assets, uint256 shares)
        """
        market_id = topics[1] if len(topics) > 1 else ""
        on_behalf_of = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        receiver = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

        # Data: caller (address), assets (uint256), shares (uint256)
        caller = "0x" + data[24:64].lower()
        assets = HexDecoder.decode_uint256(data, 32)
        shares = HexDecoder.decode_uint256(data, 64)

        return BorrowEventData(
            market_id=market_id,
            caller=caller,
            on_behalf_of=on_behalf_of,
            receiver=receiver,
            assets=Decimal(assets),
            shares=Decimal(shares),
        ).to_dict()

    def _parse_repay(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse Repay event.

        event Repay(Id indexed id, address indexed caller, address indexed onBehalfOf, uint256 assets, uint256 shares)
        """
        market_id = topics[1] if len(topics) > 1 else ""
        caller = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        on_behalf_of = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

        # Data: assets (uint256), shares (uint256)
        assets = HexDecoder.decode_uint256(data, 0)
        shares = HexDecoder.decode_uint256(data, 32)

        return RepayEventData(
            market_id=market_id,
            caller=caller,
            on_behalf_of=on_behalf_of,
            assets=Decimal(assets),
            shares=Decimal(shares),
        ).to_dict()

    def _parse_supply_collateral(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse SupplyCollateral event.

        event SupplyCollateral(Id indexed id, address indexed caller, address indexed onBehalfOf, uint256 assets)
        """
        market_id = topics[1] if len(topics) > 1 else ""
        caller = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        on_behalf_of = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

        # Data: assets (uint256)
        assets = HexDecoder.decode_uint256(data, 0)

        return SupplyCollateralEventData(
            market_id=market_id,
            caller=caller,
            on_behalf_of=on_behalf_of,
            assets=Decimal(assets),
        ).to_dict()

    def _parse_withdraw_collateral(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse WithdrawCollateral event.

        event WithdrawCollateral(Id indexed id, address caller, address indexed onBehalfOf, address indexed receiver, uint256 assets)
        """
        market_id = topics[1] if len(topics) > 1 else ""
        on_behalf_of = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        receiver = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

        # Data: caller (address), assets (uint256)
        caller = "0x" + data[24:64].lower()
        assets = HexDecoder.decode_uint256(data, 32)

        return WithdrawCollateralEventData(
            market_id=market_id,
            caller=caller,
            on_behalf_of=on_behalf_of,
            receiver=receiver,
            assets=Decimal(assets),
        ).to_dict()

    def _parse_liquidate(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse Liquidate event.

        event Liquidate(Id indexed id, address indexed caller, address indexed borrower, uint256 repaidAssets, uint256 repaidShares, uint256 seizedAssets, uint256 badDebtAssets, uint256 badDebtShares)
        """
        market_id = topics[1] if len(topics) > 1 else ""
        caller = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        borrower = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

        # Data: repaidAssets, repaidShares, seizedAssets, badDebtAssets, badDebtShares
        repaid_assets = HexDecoder.decode_uint256(data, 0)
        repaid_shares = HexDecoder.decode_uint256(data, 32)
        seized_assets = HexDecoder.decode_uint256(data, 64)
        bad_debt_assets = HexDecoder.decode_uint256(data, 96) if len(data) >= 128 * 2 else 0
        bad_debt_shares = HexDecoder.decode_uint256(data, 128) if len(data) >= 160 * 2 else 0

        return LiquidateEventData(
            market_id=market_id,
            caller=caller,
            borrower=borrower,
            repaid_assets=Decimal(repaid_assets),
            repaid_shares=Decimal(repaid_shares),
            seized_assets=Decimal(seized_assets),
            bad_debt_assets=Decimal(bad_debt_assets),
            bad_debt_shares=Decimal(bad_debt_shares),
        ).to_dict()

    def _parse_flash_loan(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse FlashLoan event.

        event FlashLoan(address indexed caller, address indexed token, uint256 assets)
        """
        caller = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        token = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

        # Data: assets (uint256)
        assets = HexDecoder.decode_uint256(data, 0) if data else 0

        return FlashLoanEventData(
            caller=caller,
            token=token,
            assets=Decimal(assets),
        ).to_dict()

    def _parse_create_market(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse CreateMarket event.

        event CreateMarket(Id indexed id, MarketParams marketParams)
        """
        market_id = topics[1] if len(topics) > 1 else ""

        # Data: loanToken, collateralToken, oracle, irm, lltv (5 x 32 bytes)
        # Each address is in last 20 bytes (40 hex chars) of 32-byte word
        loan_token = "0x" + data[24:64].lower()
        collateral_token = "0x" + data[88:128].lower()  # 64 + 24 = 88
        oracle = "0x" + data[152:192].lower()  # 128 + 24 = 152
        irm = "0x" + data[216:256].lower()  # 192 + 24 = 216
        lltv = HexDecoder.decode_uint256(data, 128)

        return CreateMarketEventData(
            market_id=market_id,
            loan_token=loan_token,
            collateral_token=collateral_token,
            oracle=oracle,
            irm=irm,
            lltv=lltv,
        ).to_dict()

    def _parse_set_authorization(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse SetAuthorization event.

        event SetAuthorization(address indexed caller, address indexed authorizer, address indexed authorized, bool isAuthorized)
        """
        caller = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        authorizer = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        authorized = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

        # Data: isAuthorized (bool as uint256)
        is_authorized = HexDecoder.decode_uint256(data, 0) != 0 if data else False

        return SetAuthorizationEventData(
            caller=caller,
            authorizer=authorizer,
            authorized=authorized,
            is_authorized=is_authorized,
        ).to_dict()

    def _parse_accrue_interest(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse AccrueInterest event.

        event AccrueInterest(Id indexed id, uint256 prevBorrowRate, uint256 interest, uint256 feeShares)
        """
        market_id = topics[1] if len(topics) > 1 else ""

        # Data: prevBorrowRate, interest, feeShares
        prev_borrow_rate = HexDecoder.decode_uint256(data, 0)
        interest = HexDecoder.decode_uint256(data, 32)
        fee_shares = HexDecoder.decode_uint256(data, 64)

        return AccrueInterestEventData(
            market_id=market_id,
            prev_borrow_rate=Decimal(prev_borrow_rate),
            interest=Decimal(interest),
            fee_shares=Decimal(fee_shares),
        ).to_dict()

    def _parse_transfer(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse ERC20 Transfer event.

        event Transfer(address indexed from, address indexed to, uint256 value)
        """
        from_address = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        to_address = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        amount = HexDecoder.decode_uint256(data, 0) if data else 0

        return TransferEventData(
            from_address=from_address,
            to_address=to_address,
            amount=Decimal(amount),
        ).to_dict()

    def _parse_approval(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse ERC20 Approval event.

        event Approval(address indexed owner, address indexed spender, uint256 value)
        """
        owner = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        spender = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        amount = HexDecoder.decode_uint256(data, 0) if data else 0

        return {
            "owner": owner,
            "spender": spender,
            "amount": str(amount),
        }

    # =========================================================================
    # Backward Compatibility Methods
    # =========================================================================

    def is_morpho_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Morpho Blue event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if the topic is a known Morpho Blue event
        """
        # Normalize topic
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic)

    def get_event_type(self, topic_or_name: str | bytes) -> MorphoBlueEventType:
        """Get event type from topic hash or event name.

        Args:
            topic_or_name: Event topic (supports bytes, hex string with/without 0x, any case) or event name

        Returns:
            MorphoBlueEventType enum value
        """
        # Normalize if it's a topic (bytes or hex string starting with 0x or not)
        if isinstance(topic_or_name, bytes):
            topic_or_name = "0x" + topic_or_name.hex()
        else:
            topic_or_name = str(topic_or_name)

        # Try as topic first (has 0x prefix after normalization)
        if topic_or_name.startswith("0x"):
            # Ensure lowercase for registry lookup
            topic_or_name = topic_or_name.lower()
            event_name = self.registry.get_event_name(topic_or_name)
            if event_name:
                event_type = self.registry.get_event_type(event_name)
                return event_type if event_type else MorphoBlueEventType.UNKNOWN
            return MorphoBlueEventType.UNKNOWN
        # Try as event name (no 0x prefix, e.g., "Supply", "Withdraw")
        event_type = self.registry.get_event_type(topic_or_name)
        return event_type if event_type else MorphoBlueEventType.UNKNOWN

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    def extract_supply_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract supply amount (assets) from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Supply amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            for event in result.events:
                if event.event_type == MorphoBlueEventType.SUPPLY:
                    assets = event.data.get("assets")
                    if assets is not None:
                        return int(Decimal(assets))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract supply amount: {e}")
            return None

    def extract_withdraw_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract withdraw amount (assets) from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Withdraw amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            for event in result.events:
                if event.event_type == MorphoBlueEventType.WITHDRAW:
                    assets = event.data.get("assets")
                    if assets is not None:
                        return int(Decimal(assets))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract withdraw amount: {e}")
            return None

    def extract_borrow_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract borrow amount (assets) from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Borrow amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            for event in result.events:
                if event.event_type == MorphoBlueEventType.BORROW:
                    assets = event.data.get("assets")
                    if assets is not None:
                        return int(Decimal(assets))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract borrow amount: {e}")
            return None

    def extract_repay_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract repay amount (assets) from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Repay amount in token units if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            for event in result.events:
                if event.event_type == MorphoBlueEventType.REPAY:
                    assets = event.data.get("assets")
                    if assets is not None:
                        return int(Decimal(assets))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract repay amount: {e}")
            return None

    def extract_shares_received(self, receipt: dict[str, Any]) -> int | None:
        """Extract shares received from supply transaction.

        In Morpho Blue, supplying assets mints shares representing the deposit.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Shares minted if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            for event in result.events:
                if event.event_type == MorphoBlueEventType.SUPPLY:
                    shares = event.data.get("shares")
                    if shares is not None:
                        return int(Decimal(shares))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract shares received: {e}")
            return None

    def extract_shares_burned(self, receipt: dict[str, Any]) -> int | None:
        """Extract shares burned from withdraw or repay transaction.

        In Morpho Blue, withdrawing/repaying burns shares.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Shares burned if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            for event in result.events:
                if event.event_type in (MorphoBlueEventType.WITHDRAW, MorphoBlueEventType.REPAY):
                    shares = event.data.get("shares")
                    if shares is not None:
                        return int(Decimal(shares))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract shares burned: {e}")
            return None
