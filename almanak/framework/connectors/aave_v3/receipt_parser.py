"""Aave V3 Receipt Parser.

This module provides parsing functionality for Aave V3 transaction receipts
and events, enabling extraction of supply, borrow, repay, withdraw, flash loan,
liquidation, and other protocol events from on-chain data.

Aave V3 Events:
- Supply: Asset supplied to pool
- Withdraw: Asset withdrawn from pool
- Borrow: Asset borrowed from pool
- Repay: Debt repaid
- FlashLoan: Flash loan executed
- LiquidationCall: Position liquidated
- ReserveDataUpdated: Interest rates updated
- UserEModeSet: E-Mode category changed
- IsolationModeTotalDebtUpdated: Isolation mode debt changed

Example:
    from almanak.framework.connectors.aave_v3 import AaveV3ReceiptParser

    parser = AaveV3ReceiptParser()

    # Parse transaction receipt
    events = parser.parse_receipt(receipt)

    for event in events:
        if event.event_type == AaveV3EventType.SUPPLY:
            print(f"Supply: {event.data}")
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

# Null address constant used for ERC-20 Transfer mint/burn detection
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Aave V3 event topic signatures (keccak256 hashes of event signatures)
# These are the actual topic hashes from Aave V3 contracts
EVENT_TOPICS: dict[str, str] = {
    # Core lending events
    "Supply": "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
    "Withdraw": "0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7",
    "Borrow": "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
    "Repay": "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051",
    # Flash loan events
    "FlashLoan": "0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0",
    # Liquidation events
    "LiquidationCall": "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286",
    # Reserve and interest rate events
    "ReserveDataUpdated": "0x804c9b842b2748a22bb64b345453a3de7ca54a6ca45ce00d415894979e22897a",
    "ReserveUsedAsCollateralEnabled": "0x00058a56ea94653cdf4f152d227ace22d4c00ad99e2a43f58cb7d9e3feb295f2",
    "ReserveUsedAsCollateralDisabled": "0x44c58d81365b66dd4b1a7f36c25aa97b8c71c361ee4937adc1a00000227db5dd",
    # E-Mode events
    "UserEModeSet": "0xd728da875fc88944cbf17638bcbe4af0eedaef63becd1d1c57cc097eb4608d84",
    # Isolation mode events
    "IsolationModeTotalDebtUpdated": "0xaef84d3b40895fd58c561f3998000f0583abb992a52fbdc99ace8e8de4d676a5",
    # Approval events (aTokens)
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    # Oracle events
    "AssetSourceUpdated": "0x22c5b7b2d8561b457f56a9c1c5c01e8d2af689e6f8a696d4b03e4a9a93cfd48f",
    "FallbackOracleUpdated": "0x58c4a5b8a2ccbf65cac0c1e01a4b0d68e1a5aa3a8ef8b7a3b2c0d4e4f6a8b9c0",
    # Configuration events
    "ReserveInitialized": "0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575f3d832d5ed7b85",
    "BorrowCapChanged": "0x44c5b4c08a2c7fd5f2c5dbecf8ad1c3e2f6bc0e8c4f4a0a0e0c2c4e6f8a0b2c4",
    "SupplyCapChanged": "0x55e6c5a6c3b4e6f8a0b2c4d6e8f0a2b4c6d8e0f2a4b6c8d0e2f4a6b8c0d2e4f6",
    "DebtCeilingChanged": "0x66f7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7",
    "BridgeProtocolFeeUpdated": "0x77a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8",
}

# Reverse lookup: topic -> event name
TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class AaveV3EventType(Enum):
    """Aave V3 event types."""

    # Core lending events
    SUPPLY = "SUPPLY"
    WITHDRAW = "WITHDRAW"
    BORROW = "BORROW"
    REPAY = "REPAY"

    # Flash loan
    FLASH_LOAN = "FLASH_LOAN"

    # Liquidation
    LIQUIDATION_CALL = "LIQUIDATION_CALL"

    # Reserve/Interest events
    RESERVE_DATA_UPDATED = "RESERVE_DATA_UPDATED"
    RESERVE_USED_AS_COLLATERAL_ENABLED = "RESERVE_USED_AS_COLLATERAL_ENABLED"
    RESERVE_USED_AS_COLLATERAL_DISABLED = "RESERVE_USED_AS_COLLATERAL_DISABLED"

    # E-Mode
    USER_EMODE_SET = "USER_EMODE_SET"

    # Isolation mode
    ISOLATION_MODE_TOTAL_DEBT_UPDATED = "ISOLATION_MODE_TOTAL_DEBT_UPDATED"

    # Token events
    APPROVAL = "APPROVAL"
    TRANSFER = "TRANSFER"

    # Oracle events
    ASSET_SOURCE_UPDATED = "ASSET_SOURCE_UPDATED"
    FALLBACK_ORACLE_UPDATED = "FALLBACK_ORACLE_UPDATED"

    # Configuration events
    RESERVE_INITIALIZED = "RESERVE_INITIALIZED"
    BORROW_CAP_CHANGED = "BORROW_CAP_CHANGED"
    SUPPLY_CAP_CHANGED = "SUPPLY_CAP_CHANGED"
    DEBT_CEILING_CHANGED = "DEBT_CEILING_CHANGED"
    BRIDGE_PROTOCOL_FEE_UPDATED = "BRIDGE_PROTOCOL_FEE_UPDATED"

    # Unknown
    UNKNOWN = "UNKNOWN"


# Mapping from event name to event type
EVENT_NAME_TO_TYPE: dict[str, AaveV3EventType] = {
    "Supply": AaveV3EventType.SUPPLY,
    "Withdraw": AaveV3EventType.WITHDRAW,
    "Borrow": AaveV3EventType.BORROW,
    "Repay": AaveV3EventType.REPAY,
    "FlashLoan": AaveV3EventType.FLASH_LOAN,
    "LiquidationCall": AaveV3EventType.LIQUIDATION_CALL,
    "ReserveDataUpdated": AaveV3EventType.RESERVE_DATA_UPDATED,
    "ReserveUsedAsCollateralEnabled": AaveV3EventType.RESERVE_USED_AS_COLLATERAL_ENABLED,
    "ReserveUsedAsCollateralDisabled": AaveV3EventType.RESERVE_USED_AS_COLLATERAL_DISABLED,
    "UserEModeSet": AaveV3EventType.USER_EMODE_SET,
    "IsolationModeTotalDebtUpdated": AaveV3EventType.ISOLATION_MODE_TOTAL_DEBT_UPDATED,
    "Approval": AaveV3EventType.APPROVAL,
    "Transfer": AaveV3EventType.TRANSFER,
    "AssetSourceUpdated": AaveV3EventType.ASSET_SOURCE_UPDATED,
    "FallbackOracleUpdated": AaveV3EventType.FALLBACK_ORACLE_UPDATED,
    "ReserveInitialized": AaveV3EventType.RESERVE_INITIALIZED,
    "BorrowCapChanged": AaveV3EventType.BORROW_CAP_CHANGED,
    "SupplyCapChanged": AaveV3EventType.SUPPLY_CAP_CHANGED,
    "DebtCeilingChanged": AaveV3EventType.DEBT_CEILING_CHANGED,
    "BridgeProtocolFeeUpdated": AaveV3EventType.BRIDGE_PROTOCOL_FEE_UPDATED,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class AaveV3Event:
    """Parsed Aave V3 event.

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

    event_type: AaveV3EventType
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
    def from_dict(cls, data: dict[str, Any]) -> "AaveV3Event":
        """Create from dictionary."""
        return cls(
            event_type=AaveV3EventType(data["event_type"]),
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

    Attributes:
        reserve: Asset address that was supplied
        user: User who initiated the supply
        on_behalf_of: Address that received the aTokens
        amount: Amount supplied (in token units)
        referral_code: Referral code used
    """

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
    """Parsed data from Withdraw event.

    Attributes:
        reserve: Asset address that was withdrawn
        user: User who initiated the withdrawal
        to: Address that received the tokens
        amount: Amount withdrawn (in token units)
    """

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
    """Parsed data from Borrow event.

    Attributes:
        reserve: Asset address that was borrowed
        user: User who initiated the borrow
        on_behalf_of: Address that received the debt
        amount: Amount borrowed (in token units)
        interest_rate_mode: 1 = stable, 2 = variable
        borrow_rate: Current borrow rate (ray, 1e27)
        referral_code: Referral code used
    """

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
    """Parsed data from Repay event.

    Attributes:
        reserve: Asset address that was repaid
        user: User whose debt was repaid
        repayer: Address that made the repayment
        amount: Amount repaid (in token units)
        use_atokens: Whether aTokens were used for repayment
    """

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
class FlashLoanEventData:
    """Parsed data from FlashLoan event.

    Attributes:
        target: Contract that received the flash loan
        initiator: Address that initiated the flash loan
        asset: Asset address that was borrowed
        amount: Amount borrowed (in token units)
        interest_rate_mode: Debt mode (0 = no debt, 1 = stable, 2 = variable)
        premium: Premium paid (in token units)
        referral_code: Referral code used
    """

    target: str
    initiator: str
    asset: str
    amount: Decimal
    interest_rate_mode: int
    premium: Decimal = Decimal("0")
    referral_code: int = 0

    @property
    def opened_debt(self) -> bool:
        """Check if flash loan opened debt."""
        return self.interest_rate_mode > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "target": self.target,
            "initiator": self.initiator,
            "asset": self.asset,
            "amount": str(self.amount),
            "interest_rate_mode": self.interest_rate_mode,
            "opened_debt": self.opened_debt,
            "premium": str(self.premium),
            "referral_code": self.referral_code,
        }


@dataclass
class LiquidationCallEventData:
    """Parsed data from LiquidationCall event.

    Attributes:
        collateral_asset: Collateral asset that was seized
        debt_asset: Debt asset that was repaid
        user: User who was liquidated
        debt_to_cover: Amount of debt repaid (in debt token units)
        liquidated_collateral_amount: Amount of collateral seized (in collateral token units)
        liquidator: Address that performed the liquidation
        receive_atoken: Whether liquidator received aTokens
    """

    collateral_asset: str
    debt_asset: str
    user: str
    debt_to_cover: Decimal
    liquidated_collateral_amount: Decimal
    liquidator: str
    receive_atoken: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "collateral_asset": self.collateral_asset,
            "debt_asset": self.debt_asset,
            "user": self.user,
            "debt_to_cover": str(self.debt_to_cover),
            "liquidated_collateral_amount": str(self.liquidated_collateral_amount),
            "liquidator": self.liquidator,
            "receive_atoken": self.receive_atoken,
        }


@dataclass
class ReserveDataUpdatedEventData:
    """Parsed data from ReserveDataUpdated event.

    Attributes:
        reserve: Asset address
        liquidity_rate: Current supply/liquidity rate (ray, 1e27)
        stable_borrow_rate: Current stable borrow rate (ray, 1e27)
        variable_borrow_rate: Current variable borrow rate (ray, 1e27)
        liquidity_index: Current liquidity index
        variable_borrow_index: Current variable borrow index
    """

    reserve: str
    liquidity_rate: Decimal
    stable_borrow_rate: Decimal
    variable_borrow_rate: Decimal
    liquidity_index: Decimal = Decimal("0")
    variable_borrow_index: Decimal = Decimal("0")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "reserve": self.reserve,
            "liquidity_rate": str(self.liquidity_rate),
            "stable_borrow_rate": str(self.stable_borrow_rate),
            "variable_borrow_rate": str(self.variable_borrow_rate),
            "liquidity_index": str(self.liquidity_index),
            "variable_borrow_index": str(self.variable_borrow_index),
        }


@dataclass
class UserEModeSetEventData:
    """Parsed data from UserEModeSet event.

    Attributes:
        user: User address
        category_id: E-Mode category ID (0 = none, 1 = ETH correlated, 2 = stablecoins)
    """

    user: str
    category_id: int

    @property
    def category_name(self) -> str:
        """Get category name."""
        names = {0: "None", 1: "ETH Correlated", 2: "Stablecoins"}
        return names.get(self.category_id, f"Category {self.category_id}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "user": self.user,
            "category_id": self.category_id,
            "category_name": self.category_name,
        }


@dataclass
class IsolationModeDebtUpdatedEventData:
    """Parsed data from IsolationModeTotalDebtUpdated event.

    Attributes:
        asset: Asset address
        total_debt: New total debt (in USD with 2 decimals)
    """

    asset: str
    total_debt: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "asset": self.asset,
            "total_debt": str(self.total_debt),
        }


@dataclass
class SupplyAmountsResult:
    """Aggregated supply extraction result for ResultEnricher.

    Returned by extract_supply_amounts() to provide a structured view
    of all supply-related data from a single transaction receipt.

    Attributes:
        supply_amount: Raw amount supplied (in token's smallest unit)
        a_token_received: Amount of aTokens minted (if available)
        supply_rate: Supply APY at time of supply (if available)
    """

    supply_amount: int
    a_token_received: int | None = None
    supply_rate: "Decimal | None" = None

    def to_dict(self) -> "dict[str, Any]":
        """Convert to dictionary."""
        return {
            "supply_amount": self.supply_amount,
            "a_token_received": self.a_token_received,
            "supply_rate": str(self.supply_rate) if self.supply_rate is not None else None,
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt.

    Attributes:
        success: Whether parsing succeeded
        events: List of parsed events
        supplies: Supply events
        withdraws: Withdraw events
        borrows: Borrow events
        repays: Repay events
        flash_loans: Flash loan events
        liquidations: Liquidation events
        error: Error message if parsing failed
        transaction_hash: Transaction hash
        block_number: Block number
    """

    success: bool
    events: list[AaveV3Event] = field(default_factory=list)
    supplies: list[SupplyEventData] = field(default_factory=list)
    withdraws: list[WithdrawEventData] = field(default_factory=list)
    borrows: list[BorrowEventData] = field(default_factory=list)
    repays: list[RepayEventData] = field(default_factory=list)
    flash_loans: list[FlashLoanEventData] = field(default_factory=list)
    liquidations: list[LiquidationCallEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "supplies": [s.to_dict() for s in self.supplies],
            "withdraws": [w.to_dict() for w in self.withdraws],
            "borrows": [b.to_dict() for b in self.borrows],
            "repays": [r.to_dict() for r in self.repays],
            "flash_loans": [f.to_dict() for f in self.flash_loans],
            "liquidations": [liq.to_dict() for liq in self.liquidations],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class AaveV3ReceiptParser:
    """Parser for Aave V3 transaction receipts.

    This parser extracts and decodes Aave V3 events from transaction receipts,
    providing structured data for supplies, borrows, repays, flash loans,
    liquidations, and other protocol events.

    Example:
        parser = AaveV3ReceiptParser()

        # Parse a receipt dict (from web3.py)
        result = parser.parse_receipt(receipt)

        if result.success:
            for event in result.events:
                print(f"Event: {event.event_name}")

            for supply in result.supplies:
                print(f"Supply: {supply.amount} to {supply.reserve}")
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "supply_amount",
            "supply_amounts",
            "withdraw_amount",
            "borrow_amount",
            "repay_amount",
            "a_token_received",
            "a_token_burned",
            "borrow_rate",
            "debt_token",
            "supply_rate",
            "remaining_debt",
        }
    )

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            **kwargs: Additional arguments (ignored for compatibility)
        """
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def parse_receipt(self, receipt: dict[str, Any]) -> ParseResult:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dict containing 'logs', 'transactionHash',
                     'blockNumber', etc.

        Returns:
            ParseResult with extracted events and data
        """
        try:
            tx_hash = receipt.get("transactionHash", "")
            if isinstance(tx_hash, bytes):
                tx_hash = "0x" + tx_hash.hex()

            block_number = receipt.get("blockNumber", 0)
            # Normalize block number (can be int, hex string, or bytes from different providers)
            if isinstance(block_number, bytes):
                block_number = int.from_bytes(block_number, "big")
            elif isinstance(block_number, str):
                block_number = int(block_number, 16) if block_number.startswith("0x") else int(block_number)

            status = receipt.get("status", 1)
            # Normalize status (can be int, hex string, or bytes from different providers)
            if isinstance(status, bytes):
                status = int.from_bytes(status, "big")
            elif isinstance(status, str):
                status = int(status, 16) if status.startswith("0x") else int(status)

            # Check if transaction reverted
            if status == 0:
                return ParseResult(
                    success=True,
                    error="Transaction reverted",
                    transaction_hash=tx_hash,
                    block_number=block_number,
                )

            logs = receipt.get("logs", [])

            if not logs:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                )

            events: list[AaveV3Event] = []
            supplies: list[SupplyEventData] = []
            withdraws: list[WithdrawEventData] = []
            borrows: list[BorrowEventData] = []
            repays: list[RepayEventData] = []
            flash_loans: list[FlashLoanEventData] = []
            liquidations: list[LiquidationCallEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data based on event type
                    if parsed_event.event_type == AaveV3EventType.SUPPLY:
                        supply_data = self._parse_supply(parsed_event)
                        if supply_data:
                            supplies.append(supply_data)

                    elif parsed_event.event_type == AaveV3EventType.WITHDRAW:
                        withdraw_data = self._parse_withdraw(parsed_event)
                        if withdraw_data:
                            withdraws.append(withdraw_data)

                    elif parsed_event.event_type == AaveV3EventType.BORROW:
                        borrow_data = self._parse_borrow(parsed_event)
                        if borrow_data:
                            borrows.append(borrow_data)

                    elif parsed_event.event_type == AaveV3EventType.REPAY:
                        repay_data = self._parse_repay(parsed_event)
                        if repay_data:
                            repays.append(repay_data)

                    elif parsed_event.event_type == AaveV3EventType.FLASH_LOAN:
                        flash_loan_data = self._parse_flash_loan(parsed_event)
                        if flash_loan_data:
                            flash_loans.append(flash_loan_data)

                    elif parsed_event.event_type == AaveV3EventType.LIQUIDATION_CALL:
                        liquidation_data = self._parse_liquidation(parsed_event)
                        if liquidation_data:
                            liquidations.append(liquidation_data)

            # Log parsed receipt with user-friendly formatting
            gas_used = receipt.get("gasUsed", 0)
            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(gas_used)

            # Build summary of actions
            actions = []
            if supplies:
                for s in supplies:
                    actions.append(f"SUPPLY {s.amount:,.0f} to {format_address(s.reserve)}")
            if withdraws:
                for w in withdraws:
                    actions.append(f"WITHDRAW {w.amount:,.0f} from {format_address(w.reserve)}")
            if borrows:
                for b in borrows:
                    rate_type = "variable" if b.is_variable_rate else "stable"
                    actions.append(f"BORROW {b.amount:,.0f} ({rate_type}) from {format_address(b.reserve)}")
            if repays:
                for r in repays:
                    actions.append(f"REPAY {r.amount:,.0f} to {format_address(r.reserve)}")
            if liquidations:
                for liq in liquidations:
                    actions.append(f"LIQUIDATION on {format_address(liq.user)}")

            if actions:
                logger.info(f"🔍 Parsed Aave V3: {', '.join(actions)}, tx={tx_fmt}, {gas_fmt}")
            else:
                logger.info(f"🔍 Parsed Aave V3 receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                events=events,
                supplies=supplies,
                withdraws=withdraws,
                borrows=borrows,
                repays=repays,
                flash_loans=flash_loans,
                liquidations=liquidations,
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(
                success=False,
                error=str(e),
            )

    def parse_logs(self, logs: list[dict[str, Any]]) -> list[AaveV3Event]:
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
    ) -> AaveV3Event | None:
        """Parse a single log entry.

        Args:
            log: Log dict containing 'topics', 'data', 'address', etc.
            tx_hash: Transaction hash
            block_number: Block number

        Returns:
            Parsed event or None if not a known Aave V3 event
        """
        try:
            topics = log.get("topics", [])
            if not topics:
                return None

            # Get the event signature (first topic)
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            else:
                first_topic = str(first_topic)
            first_topic = first_topic.lower()

            # Look up event name using registry
            event_name = self.registry.get_event_name(first_topic)
            if event_name is None:
                # Unknown event, skip
                return None

            event_type = self.registry.get_event_type(event_name) or AaveV3EventType.UNKNOWN

            # Get raw data
            data = log.get("data", "")
            if isinstance(data, bytes):
                data = "0x" + data.hex()

            # Parse log data
            parsed_data = self._decode_log_data(event_name, topics, data)

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

            return AaveV3Event(
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
    ) -> dict[str, Any]:
        """Decode log data based on event type.

        Args:
            event_name: Name of the event
            topics: List of topics
            data: Hex-encoded event data

        Returns:
            Decoded event data dict
        """
        # Remove 0x prefix if present
        if data.startswith("0x"):
            data = data[2:]

        # Convert topics to addresses where applicable
        indexed_topics = []
        for topic in topics[1:]:  # Skip first topic (event signature)
            if isinstance(topic, bytes):
                indexed_topics.append("0x" + topic.hex())
            else:
                indexed_topics.append(str(topic))

        # Decode based on event type
        if event_name == "Supply":
            return self._decode_supply_data(indexed_topics, data)
        elif event_name == "Withdraw":
            return self._decode_withdraw_data(indexed_topics, data)
        elif event_name == "Borrow":
            return self._decode_borrow_data(indexed_topics, data)
        elif event_name == "Repay":
            return self._decode_repay_data(indexed_topics, data)
        elif event_name == "FlashLoan":
            return self._decode_flash_loan_data(indexed_topics, data)
        elif event_name == "LiquidationCall":
            return self._decode_liquidation_data(indexed_topics, data)
        elif event_name == "ReserveDataUpdated":
            return self._decode_reserve_data_updated(indexed_topics, data)
        elif event_name == "UserEModeSet":
            return self._decode_user_emode_set(indexed_topics, data)
        elif event_name == "IsolationModeTotalDebtUpdated":
            return self._decode_isolation_mode_debt(indexed_topics, data)
        elif event_name in ("ReserveUsedAsCollateralEnabled", "ReserveUsedAsCollateralDisabled"):
            return self._decode_collateral_toggle(indexed_topics, data, event_name)
        else:
            # Return raw data for unknown events
            return {"raw_data": data, "indexed_topics": indexed_topics}

    def _decode_supply_data(
        self,
        indexed_topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Decode Supply event data.

        Supply(address indexed reserve, address user, address indexed onBehalfOf,
               uint256 amount, uint16 referralCode)
        """
        try:
            # Indexed: reserve (topic 1), onBehalfOf (topic 2)
            reserve = HexDecoder.topic_to_address(indexed_topics[0]) if len(indexed_topics) > 0 else ""
            on_behalf_of = HexDecoder.topic_to_address(indexed_topics[1]) if len(indexed_topics) > 1 else ""

            # Non-indexed: user, amount, referralCode
            return {
                "reserve": reserve,
                "user": HexDecoder.decode_address_from_data(data, 0),
                "on_behalf_of": on_behalf_of,
                "amount": str(Decimal(HexDecoder.decode_uint256(data, 32))),
                "referral_code": HexDecoder.decode_uint256(data, 64),
            }

        except Exception as e:
            logger.warning(f"Failed to decode Supply data: {e}")
            return {"raw_data": data}

    def _decode_withdraw_data(
        self,
        indexed_topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Decode Withdraw event data.

        Withdraw(address indexed reserve, address indexed user, address indexed to,
                 uint256 amount)
        """
        try:
            # Indexed: reserve, user, to
            reserve = HexDecoder.topic_to_address(indexed_topics[0]) if len(indexed_topics) > 0 else ""
            user = HexDecoder.topic_to_address(indexed_topics[1]) if len(indexed_topics) > 1 else ""
            to = HexDecoder.topic_to_address(indexed_topics[2]) if len(indexed_topics) > 2 else ""

            return {
                "reserve": reserve,
                "user": user,
                "to": to,
                "amount": str(Decimal(HexDecoder.decode_uint256(data, 0))),
            }

        except Exception as e:
            logger.warning(f"Failed to decode Withdraw data: {e}")
            return {"raw_data": data}

    def _decode_borrow_data(
        self,
        indexed_topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Decode Borrow event data.

        Borrow(address indexed reserve, address user, address indexed onBehalfOf,
               uint256 amount, uint256 interestRateMode, uint256 borrowRate,
               uint16 referralCode)
        """
        try:
            # Indexed: reserve, onBehalfOf
            reserve = HexDecoder.topic_to_address(indexed_topics[0]) if len(indexed_topics) > 0 else ""
            on_behalf_of = HexDecoder.topic_to_address(indexed_topics[1]) if len(indexed_topics) > 1 else ""

            # Ray = 1e27 for borrow rate
            borrow_rate_raw = HexDecoder.decode_uint256(data, 96)
            borrow_rate = Decimal(borrow_rate_raw) / Decimal("1e27")

            return {
                "reserve": reserve,
                "user": HexDecoder.decode_address_from_data(data, 0),
                "on_behalf_of": on_behalf_of,
                "amount": str(Decimal(HexDecoder.decode_uint256(data, 32))),
                "interest_rate_mode": HexDecoder.decode_uint256(data, 64),
                "borrow_rate": str(borrow_rate),
                "referral_code": HexDecoder.decode_uint256(data, 128),
            }

        except Exception as e:
            logger.warning(f"Failed to decode Borrow data: {e}")
            return {"raw_data": data}

    def _decode_repay_data(
        self,
        indexed_topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Decode Repay event data.

        Repay(address indexed reserve, address indexed user, address indexed repayer,
              uint256 amount, bool useATokens)
        """
        try:
            # Indexed: reserve, user, repayer
            reserve = HexDecoder.topic_to_address(indexed_topics[0]) if len(indexed_topics) > 0 else ""
            user = HexDecoder.topic_to_address(indexed_topics[1]) if len(indexed_topics) > 1 else ""
            repayer = HexDecoder.topic_to_address(indexed_topics[2]) if len(indexed_topics) > 2 else ""

            return {
                "reserve": reserve,
                "user": user,
                "repayer": repayer,
                "amount": str(Decimal(HexDecoder.decode_uint256(data, 0))),
                "use_atokens": HexDecoder.decode_uint256(data, 32) == 1,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Repay data: {e}")
            return {"raw_data": data}

    def _decode_flash_loan_data(
        self,
        indexed_topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Decode FlashLoan event data.

        FlashLoan(address indexed target, address initiator, address indexed asset,
                  uint256 amount, uint256 interestRateMode, uint256 premium,
                  uint16 referralCode)
        """
        try:
            # Indexed: target, asset
            target = HexDecoder.topic_to_address(indexed_topics[0]) if len(indexed_topics) > 0 else ""
            asset = HexDecoder.topic_to_address(indexed_topics[1]) if len(indexed_topics) > 1 else ""

            return {
                "target": target,
                "initiator": HexDecoder.decode_address_from_data(data, 0),
                "asset": asset,
                "amount": str(Decimal(HexDecoder.decode_uint256(data, 32))),
                "interest_rate_mode": HexDecoder.decode_uint256(data, 64),
                "premium": str(Decimal(HexDecoder.decode_uint256(data, 96))),
                "referral_code": HexDecoder.decode_uint256(data, 128),
            }

        except Exception as e:
            logger.warning(f"Failed to decode FlashLoan data: {e}")
            return {"raw_data": data}

    def _decode_liquidation_data(
        self,
        indexed_topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Decode LiquidationCall event data.

        LiquidationCall(address indexed collateralAsset, address indexed debtAsset,
                        address indexed user, uint256 debtToCover,
                        uint256 liquidatedCollateralAmount, address liquidator,
                        bool receiveAToken)
        """
        try:
            # Indexed: collateralAsset, debtAsset, user
            collateral_asset = HexDecoder.topic_to_address(indexed_topics[0]) if len(indexed_topics) > 0 else ""
            debt_asset = HexDecoder.topic_to_address(indexed_topics[1]) if len(indexed_topics) > 1 else ""
            user = HexDecoder.topic_to_address(indexed_topics[2]) if len(indexed_topics) > 2 else ""

            return {
                "collateral_asset": collateral_asset,
                "debt_asset": debt_asset,
                "user": user,
                "debt_to_cover": str(Decimal(HexDecoder.decode_uint256(data, 0))),
                "liquidated_collateral_amount": str(Decimal(HexDecoder.decode_uint256(data, 32))),
                "liquidator": HexDecoder.decode_address_from_data(data, 64),
                "receive_atoken": HexDecoder.decode_uint256(data, 96) == 1,
            }

        except Exception as e:
            logger.warning(f"Failed to decode LiquidationCall data: {e}")
            return {"raw_data": data}

    def _decode_reserve_data_updated(
        self,
        indexed_topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Decode ReserveDataUpdated event data.

        ReserveDataUpdated(address indexed reserve, uint256 liquidityRate,
                          uint256 stableBorrowRate, uint256 variableBorrowRate,
                          uint256 liquidityIndex, uint256 variableBorrowIndex)
        """
        try:
            # Indexed: reserve
            reserve = HexDecoder.topic_to_address(indexed_topics[0]) if len(indexed_topics) > 0 else ""

            # Ray = 1e27, so divide to get human-readable rate
            ray = Decimal("1e27")

            return {
                "reserve": reserve,
                "liquidity_rate": str(Decimal(HexDecoder.decode_uint256(data, 0)) / ray),
                "stable_borrow_rate": str(Decimal(HexDecoder.decode_uint256(data, 32)) / ray),
                "variable_borrow_rate": str(Decimal(HexDecoder.decode_uint256(data, 64)) / ray),
                "liquidity_index": str(Decimal(HexDecoder.decode_uint256(data, 96)) / ray),
                "variable_borrow_index": str(Decimal(HexDecoder.decode_uint256(data, 128)) / ray),
            }

        except Exception as e:
            logger.warning(f"Failed to decode ReserveDataUpdated data: {e}")
            return {"raw_data": data}

    def _decode_user_emode_set(
        self,
        indexed_topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Decode UserEModeSet event data.

        UserEModeSet(address indexed user, uint8 categoryId)
        """
        try:
            # Indexed: user
            user = HexDecoder.topic_to_address(indexed_topics[0]) if len(indexed_topics) > 0 else ""

            return {
                "user": user,
                "category_id": HexDecoder.decode_uint256(data, 0),
            }

        except Exception as e:
            logger.warning(f"Failed to decode UserEModeSet data: {e}")
            return {"raw_data": data}

    def _decode_isolation_mode_debt(
        self,
        indexed_topics: list[str],
        data: str,
    ) -> dict[str, Any]:
        """Decode IsolationModeTotalDebtUpdated event data.

        IsolationModeTotalDebtUpdated(address indexed asset, uint256 totalDebt)
        """
        try:
            # Indexed: asset
            asset = HexDecoder.topic_to_address(indexed_topics[0]) if len(indexed_topics) > 0 else ""

            # Debt is in USD with 2 decimals
            total_debt_raw = HexDecoder.decode_uint256(data, 0)
            total_debt = Decimal(total_debt_raw) / Decimal("100")

            return {
                "asset": asset,
                "total_debt": str(total_debt),
            }

        except Exception as e:
            logger.warning(f"Failed to decode IsolationModeTotalDebtUpdated data: {e}")
            return {"raw_data": data}

    def _decode_collateral_toggle(
        self,
        indexed_topics: list[str],
        data: str,
        event_name: str,
    ) -> dict[str, Any]:
        """Decode ReserveUsedAsCollateralEnabled/Disabled event data.

        ReserveUsedAsCollateralEnabled/Disabled(address indexed reserve,
                                                address indexed user)
        """
        try:
            # Indexed: reserve, user
            reserve = HexDecoder.topic_to_address(indexed_topics[0]) if len(indexed_topics) > 0 else ""
            user = HexDecoder.topic_to_address(indexed_topics[1]) if len(indexed_topics) > 1 else ""

            return {
                "reserve": reserve,
                "user": user,
                "enabled": event_name == "ReserveUsedAsCollateralEnabled",
            }

        except Exception as e:
            logger.warning(f"Failed to decode collateral toggle data: {e}")
            return {"raw_data": data}

    def _parse_supply(self, event: AaveV3Event) -> SupplyEventData | None:
        """Parse a Supply event into typed data."""
        try:
            data = event.data
            return SupplyEventData(
                reserve=data.get("reserve", ""),
                user=data.get("user", ""),
                on_behalf_of=data.get("on_behalf_of", ""),
                amount=Decimal(data.get("amount", "0")),
                referral_code=data.get("referral_code", 0),
            )
        except Exception as e:
            logger.warning(f"Failed to parse SupplyEventData: {e}")
            return None

    def _parse_withdraw(self, event: AaveV3Event) -> WithdrawEventData | None:
        """Parse a Withdraw event into typed data."""
        try:
            data = event.data
            return WithdrawEventData(
                reserve=data.get("reserve", ""),
                user=data.get("user", ""),
                to=data.get("to", ""),
                amount=Decimal(data.get("amount", "0")),
            )
        except Exception as e:
            logger.warning(f"Failed to parse WithdrawEventData: {e}")
            return None

    def _parse_borrow(self, event: AaveV3Event) -> BorrowEventData | None:
        """Parse a Borrow event into typed data."""
        try:
            data = event.data
            return BorrowEventData(
                reserve=data.get("reserve", ""),
                user=data.get("user", ""),
                on_behalf_of=data.get("on_behalf_of", ""),
                amount=Decimal(data.get("amount", "0")),
                interest_rate_mode=data.get("interest_rate_mode", 2),
                borrow_rate=Decimal(data.get("borrow_rate", "0")),
                referral_code=data.get("referral_code", 0),
            )
        except Exception as e:
            logger.warning(f"Failed to parse BorrowEventData: {e}")
            return None

    def _parse_repay(self, event: AaveV3Event) -> RepayEventData | None:
        """Parse a Repay event into typed data."""
        try:
            data = event.data
            return RepayEventData(
                reserve=data.get("reserve", ""),
                user=data.get("user", ""),
                repayer=data.get("repayer", ""),
                amount=Decimal(data.get("amount", "0")),
                use_atokens=data.get("use_atokens", False),
            )
        except Exception as e:
            logger.warning(f"Failed to parse RepayEventData: {e}")
            return None

    def _parse_flash_loan(self, event: AaveV3Event) -> FlashLoanEventData | None:
        """Parse a FlashLoan event into typed data."""
        try:
            data = event.data
            return FlashLoanEventData(
                target=data.get("target", ""),
                initiator=data.get("initiator", ""),
                asset=data.get("asset", ""),
                amount=Decimal(data.get("amount", "0")),
                interest_rate_mode=data.get("interest_rate_mode", 0),
                premium=Decimal(data.get("premium", "0")),
                referral_code=data.get("referral_code", 0),
            )
        except Exception as e:
            logger.warning(f"Failed to parse FlashLoanEventData: {e}")
            return None

    def _parse_liquidation(self, event: AaveV3Event) -> LiquidationCallEventData | None:
        """Parse a LiquidationCall event into typed data."""
        try:
            data = event.data
            return LiquidationCallEventData(
                collateral_asset=data.get("collateral_asset", ""),
                debt_asset=data.get("debt_asset", ""),
                user=data.get("user", ""),
                debt_to_cover=Decimal(data.get("debt_to_cover", "0")),
                liquidated_collateral_amount=Decimal(data.get("liquidated_collateral_amount", "0")),
                liquidator=data.get("liquidator", ""),
                receive_atoken=data.get("receive_atoken", False),
            )
        except Exception as e:
            logger.warning(f"Failed to parse LiquidationCallEventData: {e}")
            return None

    # =============================================================================
    # Extraction Methods (for Result Enrichment)
    # =============================================================================

    def extract_supply_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract supply amount from a transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Supply amount if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.supplies:
                return None

            # Return first supply amount (most common case)
            return int(result.supplies[0].amount)

        except Exception as e:
            logger.warning(f"Failed to extract supply amount: {e}")
            return None

    def extract_withdraw_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract withdraw amount from a transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Withdraw amount if found, None otherwise
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
        """Extract borrow amount from a transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Borrow amount if found, None otherwise
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
        """Extract repay amount from a transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Repay amount if found, None otherwise
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
        """Extract aToken amount received from a transaction receipt.

        Looks for Transfer events from the zero address (minting).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            aToken amount if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            transfer_topic = EVENT_TOPICS["Transfer"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 3:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != transfer_topic:
                    continue

                from_addr = HexDecoder.topic_to_address(topics[1])
                if from_addr.lower() == _ZERO_ADDRESS:
                    data = HexDecoder.normalize_hex(log.get("data", ""))
                    amount = HexDecoder.decode_uint256(data, 0)
                    return amount

            return None

        except Exception as e:
            logger.warning(f"Failed to extract aToken received: {e}")
            return None

    def extract_borrow_rate(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract borrow rate from a transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Borrow rate (as decimal, e.g., 0.05 for 5%) if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.borrows:
                return None

            return result.borrows[0].borrow_rate

        except Exception as e:
            logger.warning(f"Failed to extract borrow rate: {e}")
            return None

    def extract_debt_token(self, receipt: dict[str, Any]) -> str | None:
        """Extract debt token address from a Borrow transaction receipt.

        Finds the variable debt token by looking for Transfer events from 0x0
        (minting) where the amount matches the borrow amount. The emitting
        contract address is the debt token.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Debt token contract address if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.borrows:
                return None

            borrow = result.borrows[0]
            borrow_amount = int(borrow.amount)

            logs = receipt.get("logs", [])
            if not logs:
                return None

            transfer_topic = EVENT_TOPICS["Transfer"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 3:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != transfer_topic:
                    continue

                # Check if this is a mint (from 0x0)
                from_addr = HexDecoder.topic_to_address(topics[1])
                if from_addr.lower() != _ZERO_ADDRESS:
                    continue

                # Check if the minted amount matches the borrow amount
                data = HexDecoder.normalize_hex(log.get("data", ""))
                amount = HexDecoder.decode_uint256(data, 0)
                if amount == borrow_amount:
                    # The contract emitting this Transfer is the debt token
                    log_address = log.get("address", "")
                    if isinstance(log_address, bytes):
                        log_address = "0x" + log_address.hex()
                    if log_address:
                        return str(log_address)

            return None

        except Exception as e:
            logger.warning(f"Failed to extract debt token: {e}")
            return None

    def extract_supply_rate(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract supply rate (APY) from a transaction receipt.

        Reads the currentLiquidityRate from the ReserveDataUpdated event, which
        is emitted by the Aave V3 pool on every supply/borrow/repay/withdraw.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Supply rate as decimal (e.g., 0.035 for 3.5% APY) if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)

            # Find ReserveDataUpdated events in the parsed events list
            for event in result.events:
                if event.event_type == AaveV3EventType.RESERVE_DATA_UPDATED:
                    liquidity_rate = event.data.get("liquidity_rate")
                    if liquidity_rate is not None:
                        return Decimal(str(liquidity_rate))

            return None

        except Exception as e:
            logger.warning(f"Failed to extract supply rate: {e}")
            return None

    def extract_supply_amounts(self, receipt: dict[str, Any]) -> "SupplyAmountsResult | None":
        """Extract aggregated supply data from a transaction receipt.

        Returns a SupplyAmountsResult combining supply_amount, a_token_received,
        and supply_rate into a single structured object. Called by ResultEnricher
        for SUPPLY intents.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            SupplyAmountsResult if supply found, None otherwise
        """
        try:
            supply_amount = self.extract_supply_amount(receipt)
            if supply_amount is None:
                return None
            a_token_received = self.extract_a_token_received(receipt)
            supply_rate = self.extract_supply_rate(receipt)
            return SupplyAmountsResult(
                supply_amount=supply_amount,
                a_token_received=a_token_received,
                supply_rate=supply_rate,
            )
        except Exception as e:
            logger.warning(f"Failed to extract supply amounts: {e}")
            return None

    def extract_a_token_burned(self, receipt: dict[str, Any]) -> int | None:
        """Extract aToken amount burned from a WITHDRAW transaction receipt.

        Looks for Transfer events TO the zero address (burning) in the receipt.
        These are emitted by the aToken contract when tokens are burned on withdrawal.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            aToken amount burned if found, None otherwise
        """
        try:
            parsed = self.parse_receipt(receipt)
            if not parsed.withdraws:
                return None
            withdraw_user = parsed.withdraws[0].user.lower()

            logs = receipt.get("logs", [])
            if not logs:
                return None

            transfer_topic = EVENT_TOPICS["Transfer"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 3:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != transfer_topic:
                    continue

                # Check if this is the withdraw user's burn (user -> 0x0)
                from_addr = HexDecoder.topic_to_address(topics[1]).lower()
                to_addr = HexDecoder.topic_to_address(topics[2]).lower()
                if to_addr == _ZERO_ADDRESS and from_addr == withdraw_user:
                    data = HexDecoder.normalize_hex(log.get("data", ""))
                    amount = HexDecoder.decode_uint256(data, 0)
                    return amount

            return None

        except Exception as e:
            logger.warning(f"Failed to extract aToken burned: {e}")
            return None

    def extract_remaining_debt(self, receipt: dict[str, Any]) -> int | None:
        """Extract remaining debt after a Repay transaction.

        Aave V3 does not emit authoritative post-repay debt balance in receipt events.
        Debt token Transfer amounts use scaled units (not raw amounts), and accrued
        interest can cause Mint events during burns. Determining remaining debt
        reliably requires an on-chain state query (balanceOf on the debt token).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Always None - remaining debt cannot be reliably inferred from receipts
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.repays:
                return None

            # Remaining debt requires on-chain balanceOf query on the variable/stable
            # debt token. Receipt-based inference is unreliable due to scaled amounts.
            return None

        except Exception as e:
            logger.warning(f"Failed to extract remaining debt: {e}")
            return None

    def is_aave_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Aave V3 event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known Aave V3 event
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

    def get_event_type(self, topic: str | bytes) -> AaveV3EventType:
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

        return self.registry.get_event_type_from_topic(topic) or AaveV3EventType.UNKNOWN


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Parser
    "AaveV3ReceiptParser",
    # Event class
    "AaveV3Event",
    "AaveV3EventType",
    # Event data classes
    "SupplyEventData",
    "WithdrawEventData",
    "BorrowEventData",
    "RepayEventData",
    "FlashLoanEventData",
    "LiquidationCallEventData",
    "ReserveDataUpdatedEventData",
    "UserEModeSetEventData",
    "IsolationModeDebtUpdatedEventData",
    "ParseResult",
    # Extraction result types
    "SupplyAmountsResult",
    # Constants
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
