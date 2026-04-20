"""Curvance receipt parser.

Parses transaction receipts emitted by Curvance cToken and BorrowableCToken
contracts into structured events for the framework's ``ResultEnricher``.

Events handled:
    - Deposit       (cToken)            — indexed: from, to; data: assets, shares
    - Withdraw      (cToken)            — indexed: sender, receiver, owner; data: assets, shares
    - Borrow        (BorrowableCToken)  — NOT indexed; data: assets, account
    - Repay         (BorrowableCToken)  — NOT indexed; data: assets, payer, account

Note on Borrow/Repay: neither event uses indexed parameters, so filtering on
``topic[0]`` only identifies the event type — the contract address on the log
is the authoritative source for "which market did this come from".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.base.hex_utils import HexDecoder

from .sdk import (
    EVENT_TOPIC_BORROW,
    EVENT_TOPIC_DEPOSIT,
    EVENT_TOPIC_REPAY,
    EVENT_TOPIC_WITHDRAW,
)

logger = logging.getLogger(__name__)


class CurvanceEventType(Enum):
    """Curvance event types recognised by this parser."""

    DEPOSIT = "DEPOSIT"
    WITHDRAW = "WITHDRAW"
    BORROW = "BORROW"
    REPAY = "REPAY"


@dataclass
class CurvanceEvent:
    """A single parsed Curvance event."""

    event_type: CurvanceEventType
    contract: str
    tx_hash: str
    block_number: int
    log_index: int
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "contract": self.contract,
            "tx_hash": self.tx_hash,
            "block_number": self.block_number,
            "log_index": self.log_index,
            "data": self.data,
        }


@dataclass
class CurvanceReceiptResult:
    """Structured view of a parsed Curvance receipt."""

    tx_hash: str
    success: bool
    events: list[CurvanceEvent] = field(default_factory=list)
    raw_receipt: dict[str, Any] | None = None

    @property
    def supply_events(self) -> list[CurvanceEvent]:
        return [e for e in self.events if e.event_type == CurvanceEventType.DEPOSIT]

    @property
    def withdraw_events(self) -> list[CurvanceEvent]:
        return [e for e in self.events if e.event_type == CurvanceEventType.WITHDRAW]

    @property
    def borrow_events(self) -> list[CurvanceEvent]:
        return [e for e in self.events if e.event_type == CurvanceEventType.BORROW]

    @property
    def repay_events(self) -> list[CurvanceEvent]:
        return [e for e in self.events if e.event_type == CurvanceEventType.REPAY]

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
        }


# -----------------------------------------------------------------------------
# Parser
# -----------------------------------------------------------------------------


def _topic_matches(log_topic: str | bytes, expected_topic_hex: str) -> bool:
    """Case-insensitive comparison of a log topic against a known event signature."""
    if isinstance(log_topic, bytes):
        log_topic = "0x" + log_topic.hex()
    return log_topic.lower() == expected_topic_hex.lower()


def _decode_address_topic(topic: str | bytes) -> str:
    """Decode an indexed address topic (32-byte padded) into a checksum hex string."""
    if isinstance(topic, bytes):
        hex_str = topic.hex()
    else:
        hex_str = topic[2:] if topic.startswith("0x") else topic
    return "0x" + hex_str[-40:]


class CurvanceReceiptParser:
    """Parse Curvance transaction receipts into structured events.

    The parser matches the minimal interface expected by ``ResultEnricher``:

        parser.parse_receipt(receipt) -> CurvanceReceiptResult
        parser.extract_supply_amount(receipt) -> int | None
        parser.extract_borrow_amount(receipt) -> int | None
        parser.extract_withdraw_amount(receipt) -> int | None
        parser.extract_repay_amount(receipt) -> int | None
    """

    # -------------------------------------------------------------------------
    # Core parser
    # -------------------------------------------------------------------------

    def parse_receipt(self, receipt: dict[str, Any]) -> CurvanceReceiptResult:
        """Parse a web3 tx receipt into a structured Curvance result."""
        tx_hash = receipt.get("transactionHash") or receipt.get("tx_hash", "")
        if isinstance(tx_hash, bytes):
            tx_hash = "0x" + tx_hash.hex()

        status = receipt.get("status")
        success = status in (1, "0x1", True) if status is not None else True

        events: list[CurvanceEvent] = []
        for log in receipt.get("logs", []):
            event = self._parse_log(log, tx_hash)
            if event is not None:
                events.append(event)

        return CurvanceReceiptResult(
            tx_hash=tx_hash,
            success=success,
            events=events,
            raw_receipt=receipt,
        )

    def _parse_log(self, log: dict[str, Any], tx_hash: str) -> CurvanceEvent | None:
        """Decode a single log entry if it matches a known Curvance event."""
        topics = log.get("topics") or []
        if not topics:
            return None

        topic0 = topics[0]
        contract = log.get("address", "")
        if isinstance(contract, bytes):
            contract = "0x" + contract.hex()

        block_number_raw = log.get("blockNumber", 0)
        if isinstance(block_number_raw, str):
            block_number = int(block_number_raw, 16)
        else:
            block_number = int(block_number_raw or 0)

        log_index_raw = log.get("logIndex", 0)
        if isinstance(log_index_raw, str):
            log_index = int(log_index_raw, 16)
        else:
            log_index = int(log_index_raw or 0)

        data_hex = log.get("data", "0x")
        if isinstance(data_hex, bytes):
            data_hex = "0x" + data_hex.hex()

        # Deposit(address indexed from, address indexed to, uint256 assets, uint256 shares)
        if _topic_matches(topic0, EVENT_TOPIC_DEPOSIT):
            if len(topics) < 3:
                return None
            assets = HexDecoder.decode_uint256(data_hex, offset=0)
            shares = HexDecoder.decode_uint256(data_hex, offset=32)
            return CurvanceEvent(
                event_type=CurvanceEventType.DEPOSIT,
                contract=contract,
                tx_hash=tx_hash,
                block_number=block_number,
                log_index=log_index,
                data={
                    "from": _decode_address_topic(topics[1]),
                    "to": _decode_address_topic(topics[2]),
                    "assets": assets,
                    "shares": shares,
                },
            )

        # Withdraw(address indexed sender, address indexed receiver, address indexed owner,
        #          uint256 assets, uint256 shares)
        if _topic_matches(topic0, EVENT_TOPIC_WITHDRAW):
            if len(topics) < 4:
                return None
            assets = HexDecoder.decode_uint256(data_hex, offset=0)
            shares = HexDecoder.decode_uint256(data_hex, offset=32)
            return CurvanceEvent(
                event_type=CurvanceEventType.WITHDRAW,
                contract=contract,
                tx_hash=tx_hash,
                block_number=block_number,
                log_index=log_index,
                data={
                    "sender": _decode_address_topic(topics[1]),
                    "receiver": _decode_address_topic(topics[2]),
                    "owner": _decode_address_topic(topics[3]),
                    "assets": assets,
                    "shares": shares,
                },
            )

        # Borrow(uint256 assets, uint256 debtAssetsOwed, address account) — 3 words, non-indexed
        if _topic_matches(topic0, EVENT_TOPIC_BORROW):
            words = self._decode_uint256_words(data_hex, count=3)
            if words is None:
                return None
            assets, debt_assets_owed, account_int = words
            return CurvanceEvent(
                event_type=CurvanceEventType.BORROW,
                contract=contract,
                tx_hash=tx_hash,
                block_number=block_number,
                log_index=log_index,
                data={
                    "assets": assets,
                    "debt_assets_owed": debt_assets_owed,
                    "account": "0x" + f"{account_int:040x}",
                },
            )

        # Repay(uint256 assets, uint256 debtAssetsOwed, address payer, address account) — 4 words
        if _topic_matches(topic0, EVENT_TOPIC_REPAY):
            words = self._decode_uint256_words(data_hex, count=4)
            if words is None:
                return None
            assets, debt_assets_owed, payer_int, account_int = words
            return CurvanceEvent(
                event_type=CurvanceEventType.REPAY,
                contract=contract,
                tx_hash=tx_hash,
                block_number=block_number,
                log_index=log_index,
                data={
                    "assets": assets,
                    "debt_assets_owed": debt_assets_owed,
                    "payer": "0x" + f"{payer_int:040x}",
                    "account": "0x" + f"{account_int:040x}",
                },
            )

        return None

    @staticmethod
    def _decode_uint256_words(data_hex: str, count: int) -> tuple[int, ...] | None:
        """Decode ``count`` 32-byte words (uint256 / address-as-uint256) from event data."""
        clean = data_hex[2:] if data_hex.startswith("0x") else data_hex
        if len(clean) < 64 * count:
            return None
        return tuple(int(clean[i * 64 : (i + 1) * 64], 16) for i in range(count))

    # -------------------------------------------------------------------------
    # Extractors for ResultEnricher
    # -------------------------------------------------------------------------

    def extract_supply_amount(self, receipt: dict[str, Any]) -> int | None:
        """Return the first Deposit event's ``assets`` amount (wei). None if absent."""
        result = self.parse_receipt(receipt)
        if result.supply_events:
            return int(result.supply_events[0].data["assets"])
        return None

    def extract_withdraw_amount(self, receipt: dict[str, Any]) -> int | None:
        """Return the first Withdraw event's ``assets`` amount (wei)."""
        result = self.parse_receipt(receipt)
        if result.withdraw_events:
            return int(result.withdraw_events[0].data["assets"])
        return None

    def extract_borrow_amount(self, receipt: dict[str, Any]) -> int | None:
        """Return the first Borrow event's ``assets`` amount (wei)."""
        result = self.parse_receipt(receipt)
        if result.borrow_events:
            return int(result.borrow_events[0].data["assets"])
        return None

    def extract_repay_amount(self, receipt: dict[str, Any]) -> int | None:
        """Return the first Repay event's ``assets`` amount (wei)."""
        result = self.parse_receipt(receipt)
        if result.repay_events:
            return int(result.repay_events[0].data["assets"])
        return None

    def extract_supply_amount_decimal(self, receipt: dict[str, Any], decimals: int) -> Decimal | None:
        """Human-units variant of ``extract_supply_amount``."""
        raw = self.extract_supply_amount(receipt)
        return Decimal(raw) / Decimal(10**decimals) if raw is not None else None

    # -------------------------------------------------------------------------
    # ResultEnricher generic hooks (no-ops for Curvance)
    # -------------------------------------------------------------------------
    # Curvance is a lending protocol, not LP/Swap. The framework's ResultEnricher
    # introspects parsers for these hooks generically; expose them as no-ops so
    # generic enrichment never breaks on a missing attribute.

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:  # noqa: ARG002
        return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:  # noqa: ARG002
        return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> Any | None:  # noqa: ARG002
        return None

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> Any | None:  # noqa: ARG002
        return None
