"""MetaMorpho Vault Receipt Parser.

This module provides parsing functionality for MetaMorpho vault transaction receipts,
enabling extraction of ERC-4626 Deposit/Withdraw events and ERC-20 Transfer/Approval events.

Events parsed:
- Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
- Withdraw(address indexed sender, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)
- Transfer(address indexed from, address indexed to, uint256 amount)
- Approval(address indexed owner, address indexed spender, uint256 amount)

Example:
    from almanak.framework.connectors.morpho_vault import MetaMorphoReceiptParser

    parser = MetaMorphoReceiptParser()
    result = parser.parse_receipt(receipt)
    deposit_data = parser.extract_deposit_data(receipt)
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.base.hex_utils import HexDecoder
from almanak.framework.connectors.base.registry import EventRegistry

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class MetaMorphoEventType(Enum):
    """MetaMorpho event types."""

    DEPOSIT = "DEPOSIT"
    WITHDRAW = "WITHDRAW"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    UNKNOWN = "UNKNOWN"


# =============================================================================
# Event Topic Signatures (keccak256 of canonical Solidity signatures)
# =============================================================================

# ERC-4626 events (different from Morpho Blue's Supply/Withdraw topics)
EVENT_TOPICS: dict[str, str] = {
    # Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
    "Deposit": "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7",
    # Withdraw(address indexed sender, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)
    "Withdraw": "0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db",
    # ERC-20 Transfer(address indexed from, address indexed to, uint256 value)
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    # ERC-20 Approval(address indexed owner, address indexed spender, uint256 value)
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

EVENT_NAME_TO_TYPE: dict[str, MetaMorphoEventType] = {
    "Deposit": MetaMorphoEventType.DEPOSIT,
    "Withdraw": MetaMorphoEventType.WITHDRAW,
    "Transfer": MetaMorphoEventType.TRANSFER,
    "Approval": MetaMorphoEventType.APPROVAL,
}


# =============================================================================
# Event Data Classes
# =============================================================================


@dataclass
class VaultDepositEventData:
    """Parsed data from ERC-4626 Deposit event."""

    sender: str
    owner: str
    assets: Decimal
    shares: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "sender": self.sender,
            "owner": self.owner,
            "assets": str(self.assets),
            "shares": str(self.shares),
        }


@dataclass
class VaultWithdrawEventData:
    """Parsed data from ERC-4626 Withdraw event."""

    sender: str
    receiver: str
    owner: str
    assets: Decimal
    shares: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "sender": self.sender,
            "receiver": self.receiver,
            "owner": self.owner,
            "assets": str(self.assets),
            "shares": str(self.shares),
        }


@dataclass
class TransferEventData:
    """Parsed data from ERC-20 Transfer event."""

    from_address: str
    to_address: str
    amount: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "from": self.from_address,
            "to": self.to_address,
            "amount": str(self.amount),
        }


@dataclass
class MetaMorphoEvent:
    """Parsed MetaMorpho event."""

    event_type: MetaMorphoEventType
    event_name: str
    log_index: int
    transaction_hash: str
    block_number: int
    contract_address: str
    data: dict[str, Any]
    raw_topics: list[str] = field(default_factory=list)
    raw_data: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "event_name": self.event_name,
            "log_index": self.log_index,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "contract_address": self.contract_address,
            "data": self.data,
        }


@dataclass
class ParseResult:
    """Result of parsing a transaction receipt."""

    success: bool
    events: list[MetaMorphoEvent] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
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


class MetaMorphoReceiptParser:
    """Parser for MetaMorpho vault transaction receipts.

    Extracts ERC-4626 Deposit/Withdraw events and ERC-20 Transfer/Approval events.

    Example:
        parser = MetaMorphoReceiptParser()
        result = parser.parse_receipt(receipt)
        if result.success:
            for event in result.events:
                print(f"{event.event_name}: {event.data}")
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the parser."""
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

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

            if isinstance(transaction_hash, bytes):
                transaction_hash = "0x" + transaction_hash.hex()
            if isinstance(block_number, str):
                block_number = int(block_number, 16) if block_number.startswith("0x") else int(block_number)

            events: list[MetaMorphoEvent] = []
            for i, log in enumerate(logs):
                event = self._parse_log(log, i, transaction_hash, block_number)
                if event is not None:
                    events.append(event)

            return ParseResult(
                success=True,
                events=events,
                transaction_hash=transaction_hash,
                block_number=block_number,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(success=False, error=str(e))

    def _parse_log(
        self,
        log: dict[str, Any],
        log_index: int,
        transaction_hash: str,
        block_number: int,
    ) -> MetaMorphoEvent | None:
        """Parse a single log entry."""
        try:
            topics = log.get("topics", [])
            if not topics:
                return None

            topic0 = topics[0]
            if isinstance(topic0, bytes):
                topic0 = "0x" + topic0.hex()
            topic0 = topic0.lower()

            event_name = self.registry.get_event_name(topic0)
            if event_name is None:
                return None

            event_type = self.registry.get_event_type(event_name)
            if event_type is None:
                event_type = MetaMorphoEventType.UNKNOWN

            contract_address = log.get("address", "")
            if isinstance(contract_address, bytes):
                contract_address = "0x" + contract_address.hex()

            raw_data = log.get("data", "")
            if isinstance(raw_data, bytes):
                raw_data = "0x" + raw_data.hex()

            raw_topics = []
            for t in topics:
                if isinstance(t, bytes):
                    raw_topics.append("0x" + t.hex())
                else:
                    raw_topics.append(t.lower() if t.startswith("0x") else "0x" + t.lower())

            parsed_data = self._parse_event_data(event_name, raw_topics, raw_data)

            return MetaMorphoEvent(
                event_type=event_type,
                event_name=event_name,
                log_index=log_index,
                transaction_hash=transaction_hash,
                block_number=block_number,
                contract_address=contract_address,
                data=parsed_data,
                raw_topics=raw_topics,
                raw_data=raw_data,
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
        data_hex = data[2:] if data.startswith("0x") else data

        if event_name == "Deposit":
            return self._parse_deposit(topics, data_hex)
        elif event_name == "Withdraw":
            return self._parse_withdraw(topics, data_hex)
        elif event_name == "Transfer":
            return self._parse_transfer(topics, data_hex)
        elif event_name == "Approval":
            return self._parse_approval(topics, data_hex)
        else:
            return {"raw_data": data}

    def _parse_deposit(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse ERC-4626 Deposit event.

        Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
        """
        sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        owner = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        assets = HexDecoder.decode_uint256(data, 0)
        shares = HexDecoder.decode_uint256(data, 32)

        return VaultDepositEventData(
            sender=sender,
            owner=owner,
            assets=Decimal(assets),
            shares=Decimal(shares),
        ).to_dict()

    def _parse_withdraw(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse ERC-4626 Withdraw event.

        Withdraw(address indexed sender, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)
        """
        sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        receiver = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        owner = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""
        assets = HexDecoder.decode_uint256(data, 0)
        shares = HexDecoder.decode_uint256(data, 32)

        return VaultWithdrawEventData(
            sender=sender,
            receiver=receiver,
            owner=owner,
            assets=Decimal(assets),
            shares=Decimal(shares),
        ).to_dict()

    def _parse_transfer(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse ERC-20 Transfer event."""
        from_address = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        to_address = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        amount = HexDecoder.decode_uint256(data, 0) if data else 0

        return TransferEventData(
            from_address=from_address,
            to_address=to_address,
            amount=Decimal(amount),
        ).to_dict()

    def _parse_approval(self, topics: list[str], data: str) -> dict[str, Any]:
        """Parse ERC-20 Approval event."""
        owner = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        spender = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
        amount = HexDecoder.decode_uint256(data, 0) if data else 0

        return {
            "owner": owner,
            "spender": spender,
            "amount": str(amount),
        }

    # =========================================================================
    # Extraction Methods for ResultEnricher
    # =========================================================================

    def extract_deposit_data(self, receipt: dict[str, Any]) -> dict | None:
        """Extract deposit data from transaction receipt.

        Called by ResultEnricher for VAULT_DEPOSIT intents.

        Returns:
            Dict with {assets, shares, share_price_raw} if found, None otherwise.
            All values are in raw on-chain units (wei). share_price_raw is the
            ratio of raw assets to raw shares -- to get a human-readable price,
            normalize by asset and share decimals.
        """
        try:
            result = self.parse_receipt(receipt)
            for event in result.events:
                if event.event_type == MetaMorphoEventType.DEPOSIT:
                    assets = event.data.get("assets")
                    shares = event.data.get("shares")
                    if assets is not None and shares is not None:
                        assets_dec = Decimal(assets)
                        shares_dec = Decimal(shares)
                        share_price_raw = assets_dec / shares_dec if shares_dec > 0 else Decimal(0)
                        return {
                            "assets": int(assets_dec),
                            "shares": int(shares_dec),
                            "share_price_raw": str(share_price_raw),
                        }
            return None
        except Exception as e:
            logger.warning(f"Failed to extract deposit data: {e}")
            return None

    def extract_redeem_data(self, receipt: dict[str, Any]) -> dict | None:
        """Extract redeem data from transaction receipt.

        Called by ResultEnricher for VAULT_REDEEM intents.

        Returns:
            Dict with {shares_burned, assets_received} if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            for event in result.events:
                if event.event_type == MetaMorphoEventType.WITHDRAW:
                    assets = event.data.get("assets")
                    shares = event.data.get("shares")
                    if assets is not None and shares is not None:
                        return {
                            "shares_burned": int(Decimal(shares)),
                            "assets_received": int(Decimal(assets)),
                        }
            return None
        except Exception as e:
            logger.warning(f"Failed to extract redeem data: {e}")
            return None
