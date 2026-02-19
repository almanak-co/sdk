"""Lagoon Vault Receipt Parser.

Parses transaction receipts from Lagoon vault settlement operations (ERC-7540).
Handles three event types:
- SettleDeposit: Emitted when deposits are settled into the vault
- SettleRedeem: Emitted when redemptions are settled from the vault
- NewTotalAssets: Emitted when the valuator proposes a new total assets value

Uses base infrastructure utilities for hex decoding and event registry management.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from almanak.framework.connectors.base import EventRegistry, HexDecoder

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

# Verified keccak256 hashes from Lagoon v0.5.0 (hopperlabsxyz/lagoon-v0 src/v0.5.0/primitives/Events.sol)
EVENT_TOPICS: dict[str, str] = {
    # SettleDeposit(uint40 indexed epochId, uint40 indexed settledId,
    #              uint256 totalAssets, uint256 totalSupply,
    #              uint256 assetsDeposited, uint256 sharesMinted)
    "SettleDeposit": "0x26be8b1af887e484fec2868840869fd162e136268c24803bede886ab91aa29bc",
    # SettleRedeem(uint40 indexed epochId, uint40 indexed settledId,
    #             uint256 totalAssets, uint256 totalSupply,
    #             uint256 assetsWithdrawed, uint256 sharesBurned)
    "SettleRedeem": "0xa8fe241e26fead168e608ab85aa4e059a34552bad0fc6d98961122cb5a0abefd",
    # NewTotalAssetsUpdated(uint256 totalAssets)
    "NewTotalAssetsUpdated": "0x3809c8827d04fcf6537fe5af7a5e42a8ec939c3556096ca7107842a2d44efca5",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class LagoonEventType(Enum):
    """Lagoon vault event types."""

    SETTLE_DEPOSIT = "SETTLE_DEPOSIT"
    SETTLE_REDEEM = "SETTLE_REDEEM"
    UPDATE_VALUATION = "UPDATE_VALUATION"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, LagoonEventType] = {
    "SettleDeposit": LagoonEventType.SETTLE_DEPOSIT,
    "SettleRedeem": LagoonEventType.SETTLE_REDEEM,
    "NewTotalAssetsUpdated": LagoonEventType.UPDATE_VALUATION,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SettleDepositEventData:
    """Parsed data from SettleDeposit event."""

    epoch_id: int
    total_assets: int
    total_supply: int
    assets_deposited: int
    shares_minted: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "epoch_id": self.epoch_id,
            "total_assets": self.total_assets,
            "total_supply": self.total_supply,
            "assets_deposited": self.assets_deposited,
            "shares_minted": self.shares_minted,
        }


@dataclass
class SettleRedeemEventData:
    """Parsed data from SettleRedeem event."""

    epoch_id: int
    total_assets: int
    total_supply: int
    assets_withdrawn: int
    shares_burned: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "epoch_id": self.epoch_id,
            "total_assets": self.total_assets,
            "total_supply": self.total_supply,
            "assets_withdrawn": self.assets_withdrawn,
            "shares_burned": self.shares_burned,
        }


@dataclass
class NewTotalAssetsEventData:
    """Parsed data from NewTotalAssets event."""

    new_total_assets: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "new_total_assets": self.new_total_assets,
        }


@dataclass
class LagoonParseResult:
    """Result of parsing a Lagoon vault receipt."""

    success: bool
    settle_deposits: list[SettleDepositEventData] = field(default_factory=list)
    settle_redeems: list[SettleRedeemEventData] = field(default_factory=list)
    new_total_assets_events: list[NewTotalAssetsEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "settle_deposits": [e.to_dict() for e in self.settle_deposits],
            "settle_redeems": [e.to_dict() for e in self.settle_redeems],
            "new_total_assets_events": [e.to_dict() for e in self.new_total_assets_events],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class LagoonReceiptParser:
    """Parser for Lagoon vault transaction receipts.

    Handles three vault settlement event types:
    - SettleDeposit: deposit settlement with epoch, assets, and shares data
    - SettleRedeem: redemption settlement with epoch, assets, and shares data
    - NewTotalAssets: valuation update with new total assets value
    """

    def __init__(self, chain: str = "ethereum", **kwargs: Any) -> None:
        self.chain = chain
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def parse_receipt(self, receipt: dict[str, Any]) -> LagoonParseResult:
        """Parse a transaction receipt for Lagoon vault events.

        Args:
            receipt: Transaction receipt dict with 'logs', 'transactionHash', etc.

        Returns:
            LagoonParseResult with extracted events
        """
        try:
            tx_hash = receipt.get("transactionHash", "")
            if isinstance(tx_hash, bytes):
                tx_hash = "0x" + tx_hash.hex()

            block_number = receipt.get("blockNumber", 0)
            logs = receipt.get("logs", [])

            if not logs:
                return LagoonParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                )

            settle_deposits: list[SettleDepositEventData] = []
            settle_redeems: list[SettleRedeemEventData] = []
            new_total_assets_events: list[NewTotalAssetsEventData] = []

            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                else:
                    first_topic = str(first_topic)
                first_topic = first_topic.lower()

                event_name = self.registry.get_event_name(first_topic)
                if event_name is None:
                    continue

                data = HexDecoder.normalize_hex(log.get("data", ""))

                if event_name == "SettleDeposit":
                    deposit_event = self._parse_settle_deposit(topics, data)
                    if deposit_event:
                        settle_deposits.append(deposit_event)

                elif event_name == "SettleRedeem":
                    redeem_event = self._parse_settle_redeem(topics, data)
                    if redeem_event:
                        settle_redeems.append(redeem_event)

                elif event_name == "NewTotalAssetsUpdated":
                    assets_event = self._parse_new_total_assets(topics, data)
                    if assets_event:
                        new_total_assets_events.append(assets_event)

            logger.info(
                "Parsed Lagoon receipt: tx=%s..., settle_deposits=%d, settle_redeems=%d, new_total_assets=%d",
                tx_hash[:10],
                len(settle_deposits),
                len(settle_redeems),
                len(new_total_assets_events),
            )

            return LagoonParseResult(
                success=True,
                settle_deposits=settle_deposits,
                settle_redeems=settle_redeems,
                new_total_assets_events=new_total_assets_events,
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        except Exception as e:
            logger.exception("Failed to parse Lagoon receipt: %s", e)
            return LagoonParseResult(
                success=False,
                error=str(e),
            )

    def _parse_settle_deposit(self, topics: list[Any], data: str) -> SettleDepositEventData | None:
        """Parse SettleDeposit event data.

        SettleDeposit(uint40 indexed epochId, uint40 indexed settledId,
                     uint256 totalAssets, uint256 totalSupply,
                     uint256 assetsDeposited, uint256 sharesMinted)
        epochId and settledId are indexed (in topics[1], topics[2]).
        Remaining 4 fields are non-indexed (in data).
        """
        try:
            epoch_id = self._decode_indexed_topic(topics, 1)
            total_assets = HexDecoder.decode_uint256(data, 0)
            total_supply = HexDecoder.decode_uint256(data, 32)
            assets_deposited = HexDecoder.decode_uint256(data, 64)
            shares_minted = HexDecoder.decode_uint256(data, 96)

            return SettleDepositEventData(
                epoch_id=epoch_id,
                total_assets=total_assets,
                total_supply=total_supply,
                assets_deposited=assets_deposited,
                shares_minted=shares_minted,
            )
        except Exception as e:
            logger.warning("Failed to parse SettleDeposit event: %s", e)
            return None

    def _parse_settle_redeem(self, topics: list[Any], data: str) -> SettleRedeemEventData | None:
        """Parse SettleRedeem event data.

        SettleRedeem(uint40 indexed epochId, uint40 indexed settledId,
                    uint256 totalAssets, uint256 totalSupply,
                    uint256 assetsWithdrawed, uint256 sharesBurned)
        epochId and settledId are indexed (in topics[1], topics[2]).
        Remaining 4 fields are non-indexed (in data).
        """
        try:
            epoch_id = self._decode_indexed_topic(topics, 1)
            total_assets = HexDecoder.decode_uint256(data, 0)
            total_supply = HexDecoder.decode_uint256(data, 32)
            assets_withdrawn = HexDecoder.decode_uint256(data, 64)
            shares_burned = HexDecoder.decode_uint256(data, 96)

            return SettleRedeemEventData(
                epoch_id=epoch_id,
                total_assets=total_assets,
                total_supply=total_supply,
                assets_withdrawn=assets_withdrawn,
                shares_burned=shares_burned,
            )
        except Exception as e:
            logger.warning("Failed to parse SettleRedeem event: %s", e)
            return None

    def _parse_new_total_assets(self, topics: list[Any], data: str) -> NewTotalAssetsEventData | None:
        """Parse NewTotalAssetsUpdated event data.

        NewTotalAssetsUpdated(uint256 totalAssets)
        Single non-indexed field.
        """
        try:
            new_total_assets = HexDecoder.decode_uint256(data, 0)
            return NewTotalAssetsEventData(new_total_assets=new_total_assets)
        except Exception as e:
            logger.warning("Failed to parse NewTotalAssetsUpdated event: %s", e)
            return None

    @staticmethod
    def _decode_indexed_topic(topics: list[Any], index: int) -> int:
        """Decode an indexed topic value as uint256."""
        if index >= len(topics):
            return 0
        topic = topics[index]
        if isinstance(topic, bytes):
            return int.from_bytes(topic, "big")
        topic_str = str(topic).removeprefix("0x")
        return int(topic_str, 16) if topic_str else 0

    # =========================================================================
    # Single-log parsing methods (backward compatibility)
    # =========================================================================

    def parse_event(
        self, log: dict[str, Any]
    ) -> SettleDepositEventData | SettleRedeemEventData | NewTotalAssetsEventData | None:
        """Parse a single log entry for Lagoon vault events."""
        topics = log.get("topics", [])
        if not topics:
            return None

        first_topic = topics[0]
        if isinstance(first_topic, bytes):
            first_topic = "0x" + first_topic.hex()
        else:
            first_topic = str(first_topic)
        first_topic = first_topic.lower()

        event_name = self.registry.get_event_name(first_topic)
        if event_name is None:
            return None

        data = HexDecoder.normalize_hex(log.get("data", ""))

        if event_name == "SettleDeposit":
            return self._parse_settle_deposit(topics, data)
        elif event_name == "SettleRedeem":
            return self._parse_settle_redeem(topics, data)
        elif event_name == "NewTotalAssetsUpdated":
            return self._parse_new_total_assets(topics, data)
        return None


__all__ = [
    "EVENT_TOPICS",
    "LagoonEventType",
    "LagoonParseResult",
    "LagoonReceiptParser",
    "NewTotalAssetsEventData",
    "SettleDepositEventData",
    "SettleRedeemEventData",
    "TOPIC_TO_EVENT",
]
