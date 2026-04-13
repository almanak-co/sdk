"""Gimo Finance Receipt Parser.

Parses transaction receipts from Gimo Finance liquid staking operations on 0G Chain.
Uses base infrastructure utilities for hex decoding and event registry management.

Events parsed:
    - Transfer (ERC-20): st0G minted (stake) or burned (unstake)
    - Staked: Custom event from StakePool (A0GI deposited)
    - Unstaked: Custom event from StakePool (st0G burned)
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

GIMO_ADDRESSES: dict[str, dict[str, str]] = {
    "zerog": {
        "st0g": "0x7bBC63D01CA42491c3E084C941c3E86e55951404",
        "stake_pool": "0x7bBC63D01CA42491c3E084C941c3E86e55951404",
    },
}


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    # Transfer(address indexed from, address indexed to, uint256 value)
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class GimoEventType(Enum):
    """Gimo event types."""

    STAKE = "STAKE"
    UNSTAKE = "UNSTAKE"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, GimoEventType] = {
    # Transfer is disambiguated by from/to address (mint = stake, burn = unstake)
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class StakeEventData:
    """Parsed data from a stake operation (Transfer mint from zero address)."""

    to_address: str
    amount: Decimal
    token: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "to_address": self.to_address,
            "amount": str(self.amount),
            "token": self.token,
        }


@dataclass
class UnstakeEventData:
    """Parsed data from an unstake operation (Transfer burn to zero address)."""

    from_address: str
    amount: Decimal
    token: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_address": self.from_address,
            "amount": str(self.amount),
            "token": self.token,
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    stakes: list[StakeEventData] = field(default_factory=list)
    unstakes: list[UnstakeEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "stakes": [s.to_dict() for s in self.stakes],
            "unstakes": [u.to_dict() for u in self.unstakes],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class GimoReceiptParser:
    """Parser for Gimo Finance transaction receipts.

    Handles st0G Transfer events to detect stake (mint) and unstake (burn)
    operations on 0G Chain.
    """

    def __init__(self, chain: str = "zerog", **kwargs: Any) -> None:
        self.chain = chain
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        chain_addresses = GIMO_ADDRESSES.get(chain, {})
        self.st0g_address = chain_addresses.get("st0g", "").lower()

    def parse_receipt(self, receipt: dict[str, Any]) -> ParseResult:
        """Parse a transaction receipt for Gimo events.

        Args:
            receipt: Transaction receipt dict

        Returns:
            ParseResult with extracted stake/unstake events
        """
        try:
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
            unstakes: list[UnstakeEventData] = []

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

                contract_address = log.get("address", "")
                if isinstance(contract_address, bytes):
                    contract_address = "0x" + contract_address.hex()
                contract_address = contract_address.lower()

                data = HexDecoder.normalize_hex(log.get("data", ""))

                if event_name == "Transfer" and contract_address == self.st0g_address:
                    transfer_data = self._parse_transfer_log(topics, data)
                    if transfer_data:
                        from_addr = transfer_data["from_address"].lower()
                        to_addr = transfer_data["to_address"].lower()
                        zero_addr = "0x" + "0" * 40

                        if from_addr == zero_addr:
                            # Mint = stake (user receives st0G)
                            stakes.append(
                                StakeEventData(
                                    to_address=transfer_data["to_address"],
                                    amount=transfer_data["amount"],
                                    token=contract_address,
                                )
                            )
                        elif to_addr == zero_addr:
                            # Burn = unstake (user burns st0G)
                            unstakes.append(
                                UnstakeEventData(
                                    from_address=transfer_data["from_address"],
                                    amount=transfer_data["amount"],
                                    token=contract_address,
                                )
                            )

            logger.info(f"Parsed Gimo receipt: tx={tx_hash[:10]}..., stakes={len(stakes)}, unstakes={len(unstakes)}")

            return ParseResult(
                success=True,
                stakes=stakes,
                unstakes=unstakes,
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(success=False, error=str(e))

    def _parse_transfer_log(self, topics: list[Any], data: str) -> dict[str, Any] | None:
        """Parse Transfer event data.

        Transfer(address indexed from, address indexed to, uint256 value)
        """
        try:
            from_addr = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            to_addr = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            amount_wei = HexDecoder.decode_uint256(data, 0)
            amount = Decimal(amount_wei) / Decimal(10**18)

            return {
                "from_address": from_addr,
                "to_address": to_addr,
                "amount": amount,
            }

        except Exception as e:
            logger.warning(f"Failed to parse Transfer event: {e}")
            return None

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    def extract_stake_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract stake amount (st0G received) from transaction receipt.

        Args:
            receipt: Transaction receipt dict

        Returns:
            st0G amount in wei if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.stakes:
                return int(result.stakes[0].amount * Decimal(10**18))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract stake amount: {e}")
            return None

    def extract_unstake_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract unstake amount (st0G burned) from transaction receipt.

        Args:
            receipt: Transaction receipt dict

        Returns:
            st0G amount in wei if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.unstakes:
                return int(result.unstakes[0].amount * Decimal(10**18))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract unstake amount: {e}")
            return None


__all__ = [
    "GimoReceiptParser",
    "GimoEventType",
    "StakeEventData",
    "UnstakeEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
]
