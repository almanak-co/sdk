"""Ethena Receipt Parser (Refactored).

Refactored to use base infrastructure utilities while maintaining backward compatibility.
Ethena uses standard ERC4626 events (Deposit/Withdraw) for staking operations.
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.base import EventRegistry, HexDecoder

logger = logging.getLogger(__name__)


# =============================================================================
# Contract Addresses
# =============================================================================

ETHENA_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "usde": "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
        "susde": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
    },
}


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    "Deposit": "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7",
    "Withdraw": "0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}
ETHENA_EVENT_SIGNATURES: dict[str, str] = EVENT_TOPICS  # Alias


# =============================================================================
# Enums
# =============================================================================


class EthenaEventType(Enum):
    """Ethena event types."""

    STAKE = "STAKE"
    UNSTAKE = "UNSTAKE"
    WITHDRAW = "WITHDRAW"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, EthenaEventType] = {
    "Deposit": EthenaEventType.STAKE,
    "Withdraw": EthenaEventType.WITHDRAW,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class StakeEventData:
    """Parsed data from Deposit event (stake operation)."""

    sender: str
    owner: str
    assets: Decimal
    shares: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sender": self.sender,
            "owner": self.owner,
            "assets": str(self.assets),
            "shares": str(self.shares),
        }


@dataclass
class WithdrawEventData:
    """Parsed data from Withdraw event (cooldown or final withdrawal)."""

    sender: str
    receiver: str
    owner: str
    assets: Decimal
    shares: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sender": self.sender,
            "receiver": self.receiver,
            "owner": self.owner,
            "assets": str(self.assets),
            "shares": str(self.shares),
        }


# Backward compatibility alias
UnstakeEventData = WithdrawEventData


@dataclass
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    stakes: list[StakeEventData] = field(default_factory=list)
    withdraws: list[WithdrawEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    @property
    def unstakes(self) -> list[WithdrawEventData]:
        """Backward compatibility alias for withdraws."""
        return self.withdraws

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "stakes": [s.to_dict() for s in self.stakes],
            "withdraws": [w.to_dict() for w in self.withdraws],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class EthenaReceiptParser:
    """Parser for Ethena transaction receipts.

    Refactored to use base infrastructure utilities for hex decoding
    and event registry management.
    """

    def __init__(self, chain: str = "ethereum", **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            chain: Blockchain network (ethereum)
            **kwargs: Additional arguments (ignored for compatibility)
        """
        self.chain = chain
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        # Get contract addresses for this chain
        chain_addresses = ETHENA_ADDRESSES.get(chain, {})
        self.usde_address = chain_addresses.get("usde", "").lower()
        self.susde_address = chain_addresses.get("susde", "").lower()

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

            stakes: list[StakeEventData] = []
            withdraws: list[WithdrawEventData] = []

            for log in logs:
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

                # Get contract address and check if it's sUSDe
                contract_address = log.get("address", "")
                if isinstance(contract_address, bytes):
                    contract_address = "0x" + contract_address.hex()
                contract_address = contract_address.lower()

                if contract_address != self.susde_address:
                    continue

                # Get raw data
                data = HexDecoder.normalize_hex(log.get("data", ""))

                # Parse based on event type
                if event_name == "Deposit":
                    stake_data = self._parse_deposit_log(topics, data)
                    if stake_data:
                        stakes.append(stake_data)

                elif event_name == "Withdraw":
                    withdraw_data = self._parse_withdraw_log(topics, data)
                    if withdraw_data:
                        withdraws.append(withdraw_data)

            logger.info(
                f"Parsed Ethena receipt: tx={tx_hash[:10]}..., stakes={len(stakes)}, withdraws={len(withdraws)}"
            )

            return ParseResult(
                success=True,
                stakes=stakes,
                withdraws=withdraws,
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(
                success=False,
                error=str(e),
            )

    def _parse_deposit_log(self, topics: list[Any], data: str) -> StakeEventData | None:
        """Parse Deposit event data.

        Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
        """
        try:
            # Indexed: sender (topic 1), owner (topic 2)
            sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            owner = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            # Non-indexed: assets, shares (both uint256)
            assets_wei = HexDecoder.decode_uint256(data, 0)
            shares_wei = HexDecoder.decode_uint256(data, 32)

            # Convert from wei to token units (18 decimals)
            assets = Decimal(assets_wei) / Decimal(10**18)
            shares = Decimal(shares_wei) / Decimal(10**18)

            return StakeEventData(
                sender=sender,
                owner=owner,
                assets=assets,
                shares=shares,
            )

        except Exception as e:
            logger.warning(f"Failed to parse Deposit event: {e}")
            return None

    def _parse_withdraw_log(self, topics: list[Any], data: str) -> WithdrawEventData | None:
        """Parse Withdraw event data.

        Withdraw(address indexed sender, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)
        """
        try:
            # Indexed: sender (topic 1), receiver (topic 2), owner (topic 3)
            sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            receiver = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            owner = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

            # Non-indexed: assets, shares (both uint256)
            assets_wei = HexDecoder.decode_uint256(data, 0)
            shares_wei = HexDecoder.decode_uint256(data, 32)

            # Convert from wei to token units (18 decimals)
            assets = Decimal(assets_wei) / Decimal(10**18)
            shares = Decimal(shares_wei) / Decimal(10**18)

            return WithdrawEventData(
                sender=sender,
                receiver=receiver,
                owner=owner,
                assets=assets,
                shares=shares,
            )

        except Exception as e:
            logger.warning(f"Failed to parse Withdraw event: {e}")
            return None

    # Backward compatibility methods
    def parse_stake(self, log: dict[str, Any]) -> StakeEventData | None:
        """Parse a Deposit (stake) event from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))
        return self._parse_deposit_log(topics, data)

    def parse_withdraw(self, log: dict[str, Any]) -> WithdrawEventData | None:
        """Parse a Withdraw event from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))
        return self._parse_withdraw_log(topics, data)

    def parse_unstake(self, log: dict[str, Any]) -> WithdrawEventData | None:
        """Backward compatibility alias for parse_withdraw."""
        return self.parse_withdraw(log)

    def is_ethena_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Ethena event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known Ethena event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes) -> EthenaEventType:
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
        return self.registry.get_event_type_from_topic(topic) or EthenaEventType.UNKNOWN

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    def extract_stake_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract stake amount (assets deposited) from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Stake amount in wei if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.stakes:
                # Return in wei (reverse the conversion done in parsing)
                return int(result.stakes[0].assets * Decimal(10**18))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract stake amount: {e}")
            return None

    def extract_shares_received(self, receipt: dict[str, Any]) -> int | None:
        """Extract sUSDe shares received from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Shares received in wei if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.stakes:
                # Return in wei (reverse the conversion done in parsing)
                return int(result.stakes[0].shares * Decimal(10**18))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract shares received: {e}")
            return None

    def extract_unstake_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract unstake amount (shares burned) from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Shares burned in wei if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.withdraws:
                # Return in wei (reverse the conversion done in parsing)
                return int(result.withdraws[0].shares * Decimal(10**18))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract unstake amount: {e}")
            return None

    def extract_underlying_received(self, receipt: dict[str, Any]) -> int | None:
        """Extract underlying USDe received from withdrawal.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Assets received in wei if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.withdraws:
                # Return in wei (reverse the conversion done in parsing)
                return int(result.withdraws[0].assets * Decimal(10**18))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract underlying received: {e}")
            return None


__all__ = [
    "EthenaReceiptParser",
    "EthenaEventType",
    "StakeEventData",
    "WithdrawEventData",
    "UnstakeEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "ETHENA_EVENT_SIGNATURES",
    "TOPIC_TO_EVENT",
]
