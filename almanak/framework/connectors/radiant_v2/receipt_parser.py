"""Radiant V2 Receipt Parser.

Radiant V2 is an Aave V2 fork. Key ABI differences from Aave V3:
- `Deposit` event instead of `Supply` (same params, different topic hash)
- `Borrow` uses uint256 for borrowRateMode (V3 uses uint8 — different topic hash)
- `Repay` omits the useATokens bool parameter (V3 has it — different topic hash)
- `Withdraw` is the only event with an identical signature across V2 and V3

Extends BaseReceiptParser following the established framework pattern.
Uses pool address filtering to avoid false positives with Aave V3 events
on the same chain (Withdraw shares the topic hash across V2/V3).
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.base import BaseReceiptParser, EventRegistry, HexDecoder

logger = logging.getLogger(__name__)


# =============================================================================
# Known Radiant V2 Pool Addresses (EIP-55 checksummed)
# =============================================================================

RADIANT_V2_POOL_ADDRESSES: set[str] = {
    # Ethereum Mainnet
    "0xA950974f64aA33f27F6C5e017eEE93BF7588ED07",
    # Arbitrum (frozen post-October 2024 hack — kept for historical receipt parsing)
    "0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1",
}

# Lowercase version for runtime comparison (computed once at module load)
_POOL_ADDRESSES_LOWER: set[str] = {a.lower() for a in RADIANT_V2_POOL_ADDRESSES}


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    # Core lending events — Aave V2 ABI (NOT V3!)
    # Deposit(address indexed reserve, address user, address indexed onBehalfOf,
    #         uint256 amount, uint16 referralCode)
    "Deposit": "0xde6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951",
    # Withdraw has the same signature across V2 and V3
    # Withdraw(address indexed reserve, address indexed user, address indexed to, uint256 amount)
    "Withdraw": "0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7",
    # Borrow V2 uses uint256 for borrowRateMode (V3 uses uint8 — different hash!)
    # Borrow(address indexed reserve, address user, address indexed onBehalfOf,
    #        uint256 amount, uint256 borrowRateMode, uint256 borrowRate, uint16 referralCode)
    "Borrow": "0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b",
    # Repay V2 omits the useATokens bool (V3 has it — different hash!)
    # Repay(address indexed reserve, address indexed user, address indexed repayer, uint256 amount)
    "Repay": "0x4cdde6e09bb755c9a5589ebaec640bbfedff1362d4b255ebf8339782b9942faa",
    # Standard ERC-20 Transfer (for rToken mint/burn detection)
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    # Reserve data updated (for rate extraction)
    "ReserveDataUpdated": "0x804c9b842b2748a22bb64b345453a3de7ca54a6ca45ce00d415894979e22897a",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Minimum data lengths (in hex chars, excluding 0x prefix) per event type.
# Each ABI word is 32 bytes = 64 hex chars.
_MIN_DATA_WORDS: dict[str, int] = {
    "Deposit": 3,  # user + amount + referralCode
    "Withdraw": 1,  # amount
    "Borrow": 5,  # user + amount + borrowRateMode + borrowRate + referralCode
    "Repay": 1,  # amount only (V2 has no useATokens)
}


# =============================================================================
# Enums
# =============================================================================


class RadiantV2EventType(Enum):
    """Radiant V2 event types."""

    DEPOSIT = "DEPOSIT"  # V2 equivalent of V3's SUPPLY
    WITHDRAW = "WITHDRAW"
    BORROW = "BORROW"
    REPAY = "REPAY"
    TRANSFER = "TRANSFER"
    RESERVE_DATA_UPDATED = "RESERVE_DATA_UPDATED"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, RadiantV2EventType] = {
    "Deposit": RadiantV2EventType.DEPOSIT,
    "Withdraw": RadiantV2EventType.WITHDRAW,
    "Borrow": RadiantV2EventType.BORROW,
    "Repay": RadiantV2EventType.REPAY,
    "Transfer": RadiantV2EventType.TRANSFER,
    "ReserveDataUpdated": RadiantV2EventType.RESERVE_DATA_UPDATED,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class RadiantV2Event:
    """Parsed Radiant V2 event."""

    event_type: RadiantV2EventType
    event_name: str
    log_index: int
    transaction_hash: str
    block_number: int
    contract_address: str
    data: dict[str, Any]
    raw_topics: list[str] | None = None
    raw_data: str | None = None


@dataclass
class DepositEventData:
    """Parsed data from Deposit event (Aave V2 equivalent of Supply)."""

    reserve: str
    user: str
    on_behalf_of: str
    amount: Decimal
    referral_code: int = 0

    def to_dict(self) -> dict[str, Any]:
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
        return self.interest_rate_mode == 2

    def to_dict(self) -> dict[str, Any]:
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
        return {
            "reserve": self.reserve,
            "user": self.user,
            "repayer": self.repayer,
            "amount": str(self.amount),
            "use_atokens": self.use_atokens,
        }


@dataclass
class ParseResult:
    """Result of parsing a Radiant V2 receipt."""

    success: bool
    supplies: list[DepositEventData] = field(default_factory=list)
    withdraws: list[WithdrawEventData] = field(default_factory=list)
    borrows: list[BorrowEventData] = field(default_factory=list)
    repays: list[RepayEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0
    events: list[RadiantV2Event] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
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


class RadiantV2ReceiptParser(BaseReceiptParser[RadiantV2Event, ParseResult]):
    """Parser for Radiant V2 transaction receipts.

    Radiant V2 is an Aave V2 fork. This parser handles V2-specific event
    signatures (Deposit instead of Supply) while sharing the same parameter
    layouts for Withdraw/Borrow/Repay.

    Uses pool address filtering to avoid false positives with Aave V3 events
    on the same chain (Withdraw/Borrow/Repay share topic hashes).
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "supply_amount",
            "withdraw_amount",
            "borrow_amount",
            "repay_amount",
            "a_token_received",
            "borrow_rate",
            "supply_rate",
        }
    )

    def __init__(
        self,
        pool_addresses: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        super().__init__(registry=registry)
        self._pool_addresses = (
            {a.lower() for a in pool_addresses} if pool_addresses is not None else _POOL_ADDRESSES_LOWER
        )
        self._chain = kwargs.get("chain", "ethereum")

    # =========================================================================
    # Input Validation
    # =========================================================================

    @staticmethod
    def _has_min_data(data: str, min_words: int) -> bool:
        """Check if data has at least min_words 32-byte ABI words.

        Rejects truncated event payloads instead of decoding them to zeroes.
        """
        return len(data) >= min_words * 64

    @staticmethod
    def _has_min_topics(topics: list[Any], min_count: int) -> bool:
        """Check if topics list has minimum required indexed params."""
        return len(topics) >= min_count

    # =========================================================================
    # BaseReceiptParser Hook Methods
    # =========================================================================

    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
        contract_address: str,
    ) -> dict[str, Any]:
        """Decode Radiant V2 event data.

        Applies pool address filtering for events that share topic hashes
        with Aave V3 (Withdraw, Borrow, Repay). Deposit is unique to V2.
        Transfer and ReserveDataUpdated are not pool-filtered (emitted by
        token contracts and the pool respectively).
        """
        # Pool address filter for lending events (Deposit, Withdraw, Borrow, Repay)
        # prevents false positives from Aave V3 on the same chain
        pool_filtered_events = {"Deposit", "Withdraw", "Borrow", "Repay"}
        if event_name in pool_filtered_events:
            if contract_address.lower() not in self._pool_addresses:
                return {}

        if event_name == "Deposit":
            return self._decode_deposit(topics, data)
        elif event_name == "Withdraw":
            return self._decode_withdraw(topics, data)
        elif event_name == "Borrow":
            return self._decode_borrow(topics, data)
        elif event_name == "Repay":
            return self._decode_repay(topics, data)
        return {}

    def _create_event(
        self,
        event_name: str,
        log_index: int,
        tx_hash: str,
        block_number: int,
        contract_address: str,
        decoded_data: dict[str, Any],
        raw_topics: list[str],
        raw_data: str,
    ) -> RadiantV2Event | None:
        """Create RadiantV2Event from decoded data."""
        if not decoded_data:
            return None

        event_type = EVENT_NAME_TO_TYPE.get(event_name, RadiantV2EventType.UNKNOWN)
        return RadiantV2Event(
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

    def _build_result(
        self,
        events: list[RadiantV2Event],
        receipt: dict[str, Any],
        tx_hash: str,
        block_number: int,
        tx_success: bool,
        **kwargs,
    ) -> ParseResult:
        """Build ParseResult from parsed events."""
        error = kwargs.get("error")

        if not tx_success or error:
            return ParseResult(
                success=not error,
                error=error,
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        supplies: list[DepositEventData] = []
        withdraws: list[WithdrawEventData] = []
        borrows: list[BorrowEventData] = []
        repays: list[RepayEventData] = []

        for event in events:
            if event.event_type == RadiantV2EventType.DEPOSIT:
                deposit = self._to_deposit_data(event.data)
                if deposit:
                    supplies.append(deposit)
            elif event.event_type == RadiantV2EventType.WITHDRAW:
                withdraw = self._to_withdraw_data(event.data)
                if withdraw:
                    withdraws.append(withdraw)
            elif event.event_type == RadiantV2EventType.BORROW:
                borrow = self._to_borrow_data(event.data)
                if borrow:
                    borrows.append(borrow)
            elif event.event_type == RadiantV2EventType.REPAY:
                repay = self._to_repay_data(event.data)
                if repay:
                    repays.append(repay)

        if supplies or withdraws or borrows or repays:
            logger.info(
                f"Parsed Radiant V2 receipt: tx={tx_hash[:10]}..., "
                f"deposits={len(supplies)}, withdraws={len(withdraws)}, "
                f"borrows={len(borrows)}, repays={len(repays)}"
            )

        return ParseResult(
            success=True,
            supplies=supplies,
            withdraws=withdraws,
            borrows=borrows,
            repays=repays,
            events=events,
            transaction_hash=tx_hash,
            block_number=block_number,
        )

    # =========================================================================
    # Event Decoding (with input validation)
    # =========================================================================

    def _decode_deposit(self, topics: list[Any], data: str) -> dict[str, Any]:
        """Decode Deposit event.

        Deposit(address indexed reserve, address user, address indexed onBehalfOf,
                uint256 amount, uint16 referralCode)
        """
        if not self._has_min_topics(topics, 3) or not self._has_min_data(data, 3):
            logger.warning("Deposit event: insufficient topics or data")
            return {}

        return {
            "reserve": HexDecoder.topic_to_address(topics[1]),
            "on_behalf_of": HexDecoder.topic_to_address(topics[2]),
            "user": HexDecoder.decode_address_from_data(data, 0),
            "amount": str(Decimal(HexDecoder.decode_uint256(data, 32))),
            "referral_code": HexDecoder.decode_uint256(data, 64),
        }

    def _decode_withdraw(self, topics: list[Any], data: str) -> dict[str, Any]:
        """Decode Withdraw event.

        Withdraw(address indexed reserve, address indexed user, address indexed to,
                 uint256 amount)
        """
        if not self._has_min_topics(topics, 4) or not self._has_min_data(data, 1):
            logger.warning("Withdraw event: insufficient topics or data")
            return {}

        return {
            "reserve": HexDecoder.topic_to_address(topics[1]),
            "user": HexDecoder.topic_to_address(topics[2]),
            "to": HexDecoder.topic_to_address(topics[3]),
            "amount": str(Decimal(HexDecoder.decode_uint256(data, 0))),
        }

    def _decode_borrow(self, topics: list[Any], data: str) -> dict[str, Any]:
        """Decode Borrow event.

        Borrow(address indexed reserve, address user, address indexed onBehalfOf,
               uint256 amount, uint256 borrowRateMode, uint256 borrowRate,
               uint16 referralCode)
        """
        if not self._has_min_topics(topics, 3) or not self._has_min_data(data, 5):
            logger.warning("Borrow event: insufficient topics or data")
            return {}

        borrow_rate_raw = HexDecoder.decode_uint256(data, 96)
        borrow_rate = Decimal(borrow_rate_raw) / Decimal("1e27")  # ray to decimal

        return {
            "reserve": HexDecoder.topic_to_address(topics[1]),
            "on_behalf_of": HexDecoder.topic_to_address(topics[2]),
            "user": HexDecoder.decode_address_from_data(data, 0),
            "amount": str(Decimal(HexDecoder.decode_uint256(data, 32))),
            "interest_rate_mode": HexDecoder.decode_uint256(data, 64),
            "borrow_rate": str(borrow_rate),
            "referral_code": HexDecoder.decode_uint256(data, 128),
        }

    def _decode_repay(self, topics: list[Any], data: str) -> dict[str, Any]:
        """Decode Repay event.

        Aave V2 Repay — no useATokens param (that's V3 only):
        Repay(address indexed reserve, address indexed user, address indexed repayer,
              uint256 amount)
        """
        if not self._has_min_topics(topics, 4) or not self._has_min_data(data, 1):
            logger.warning("Repay event: insufficient topics or data")
            return {}

        return {
            "reserve": HexDecoder.topic_to_address(topics[1]),
            "user": HexDecoder.topic_to_address(topics[2]),
            "repayer": HexDecoder.topic_to_address(topics[3]),
            "amount": str(Decimal(HexDecoder.decode_uint256(data, 0))),
            "use_atokens": False,  # V2 does not have this field
        }

    # =========================================================================
    # Typed Data Conversion
    # =========================================================================

    @staticmethod
    def _to_deposit_data(data: dict[str, Any]) -> DepositEventData | None:
        try:
            return DepositEventData(
                reserve=data.get("reserve", ""),
                user=data.get("user", ""),
                on_behalf_of=data.get("on_behalf_of", ""),
                amount=Decimal(data.get("amount", "0")),
                referral_code=data.get("referral_code", 0),
            )
        except Exception as e:
            logger.warning(f"Failed to create DepositEventData: {e}")
            return None

    @staticmethod
    def _to_withdraw_data(data: dict[str, Any]) -> WithdrawEventData | None:
        try:
            return WithdrawEventData(
                reserve=data.get("reserve", ""),
                user=data.get("user", ""),
                to=data.get("to", ""),
                amount=Decimal(data.get("amount", "0")),
            )
        except Exception as e:
            logger.warning(f"Failed to create WithdrawEventData: {e}")
            return None

    @staticmethod
    def _to_borrow_data(data: dict[str, Any]) -> BorrowEventData | None:
        try:
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
            logger.warning(f"Failed to create BorrowEventData: {e}")
            return None

    @staticmethod
    def _to_repay_data(data: dict[str, Any]) -> RepayEventData | None:
        try:
            return RepayEventData(
                reserve=data.get("reserve", ""),
                user=data.get("user", ""),
                repayer=data.get("repayer", ""),
                amount=Decimal(data.get("amount", "0")),
                use_atokens=data.get("use_atokens", False),
            )
        except Exception as e:
            logger.warning(f"Failed to create RepayEventData: {e}")
            return None

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    def extract_supply_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract deposit (supply) amount from transaction receipt."""
        try:
            result = self.parse_receipt(receipt)
            if not result.supplies:
                return None
            return int(result.supplies[0].amount)
        except Exception as e:
            logger.warning(f"Failed to extract supply amount: {e}")
            return None

    def extract_withdraw_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract withdraw amount from transaction receipt."""
        try:
            result = self.parse_receipt(receipt)
            if not result.withdraws:
                return None
            return int(result.withdraws[0].amount)
        except Exception as e:
            logger.warning(f"Failed to extract withdraw amount: {e}")
            return None

    def extract_borrow_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract borrow amount from transaction receipt."""
        try:
            result = self.parse_receipt(receipt)
            if not result.borrows:
                return None
            return int(result.borrows[0].amount)
        except Exception as e:
            logger.warning(f"Failed to extract borrow amount: {e}")
            return None

    def extract_repay_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract repay amount from transaction receipt."""
        try:
            result = self.parse_receipt(receipt)
            if not result.repays:
                return None
            return int(result.repays[0].amount)
        except Exception as e:
            logger.warning(f"Failed to extract repay amount: {e}")
            return None

    def extract_a_token_received(self, receipt: dict[str, Any]) -> int | None:
        """Extract rToken (aToken equivalent) amount received.

        Looks for Transfer events from the zero address (minting).
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
                    if len(data) < 64:
                        continue
                    amount = HexDecoder.decode_uint256(data, 0)
                    return amount

            return None
        except Exception as e:
            logger.warning(f"Failed to extract rToken received: {e}")
            return None

    def extract_borrow_rate(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract borrow rate from transaction receipt."""
        try:
            result = self.parse_receipt(receipt)
            if not result.borrows:
                return None
            return result.borrows[0].borrow_rate
        except Exception as e:
            logger.warning(f"Failed to extract borrow rate: {e}")
            return None

    def extract_supply_rate(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract supply rate from ReserveDataUpdated event."""
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            reserve_updated_topic = EVENT_TOPICS["ReserveDataUpdated"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != reserve_updated_topic:
                    continue

                data = HexDecoder.normalize_hex(log.get("data", ""))
                if len(data) < 64:
                    continue
                liquidity_rate_raw = HexDecoder.decode_uint256(data, 0)
                return Decimal(liquidity_rate_raw) / Decimal("1e27")

            return None
        except Exception as e:
            logger.warning(f"Failed to extract supply rate: {e}")
            return None

    def extract_supply_amounts(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """Extract aggregated supply (deposit) data. Called by ResultEnricher."""
        try:
            supply_amount = self.extract_supply_amount(receipt)
            if supply_amount is None:
                return None
            return {
                "supply_amount": supply_amount,
                "a_token_received": self.extract_a_token_received(receipt),
                "supply_rate": self.extract_supply_rate(receipt),
            }
        except Exception as e:
            logger.warning(f"Failed to extract supply amounts: {e}")
            return None

    def extract_borrow_amounts(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """Extract aggregated borrow data. Called by ResultEnricher."""
        try:
            borrow_amount = self.extract_borrow_amount(receipt)
            if borrow_amount is None:
                return None
            return {
                "borrow_amount": borrow_amount,
                "borrow_rate": self.extract_borrow_rate(receipt),
            }
        except Exception as e:
            logger.warning(f"Failed to extract borrow amounts: {e}")
            return None

    def extract_withdraw_amounts(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """Extract aggregated withdraw data. Called by ResultEnricher."""
        try:
            withdraw_amount = self.extract_withdraw_amount(receipt)
            if withdraw_amount is None:
                return None
            return {"withdraw_amount": withdraw_amount}
        except Exception as e:
            logger.warning(f"Failed to extract withdraw amounts: {e}")
            return None

    def extract_repay_amounts(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """Extract aggregated repay data. Called by ResultEnricher."""
        try:
            repay_amount = self.extract_repay_amount(receipt)
            if repay_amount is None:
                return None
            return {"repay_amount": repay_amount}
        except Exception as e:
            logger.warning(f"Failed to extract repay amounts: {e}")
            return None


__all__ = [
    "RadiantV2ReceiptParser",
    "RadiantV2EventType",
    "RadiantV2Event",
    "DepositEventData",
    "WithdrawEventData",
    "BorrowEventData",
    "RepayEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "RADIANT_V2_POOL_ADDRESSES",
]
