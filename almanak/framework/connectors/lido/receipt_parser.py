"""Lido Receipt Parser (Refactored).

Refactored to use base infrastructure utilities while maintaining backward compatibility.
Lido uses multiple contracts (stETH, wstETH, withdrawal queue) with different event types.
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

LIDO_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "steth": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
        "wsteth": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "withdrawal_queue": "0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1",
    },
    "arbitrum": {
        "wsteth": "0x5979D7b546E38E414F7E9822514be443A4800529",
    },
    "optimism": {
        "wsteth": "0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb",
    },
    "polygon": {
        "wsteth": "0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD",
    },
}


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    # Submitted(address indexed sender, uint256 amount, address referral)
    "Submitted": "0x96a25c8ce0baabc1fdefd93e9ed25d8e092a3332f3aa9a41722b5697231d1d1a",
    # Transfer(address indexed from, address indexed to, uint256 value)
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    # WithdrawalRequested(uint256 indexed requestId, address indexed requestor, address indexed owner, uint256 amountOfStETH, uint256 amountOfShares)
    "WithdrawalRequested": "0xf0cb471f23fb74ea44b8252eb1881a2dca546288d9f6e90d1a0e82fe0ed342ab",
    # WithdrawalClaimed(uint256 indexed requestId, address indexed owner, address indexed receiver, uint256 amountOfETH)
    "WithdrawalClaimed": "0x6ad26c5e238e7d002799f9a5db07e81ef14e37386ae03496d7a7ef04713e145b",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class LidoEventType(Enum):
    """Lido event types."""

    STAKE = "STAKE"
    WRAP = "WRAP"
    UNWRAP = "UNWRAP"
    WITHDRAWAL_REQUESTED = "WITHDRAWAL_REQUESTED"
    WITHDRAWAL_CLAIMED = "WITHDRAWAL_CLAIMED"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, LidoEventType] = {
    "Submitted": LidoEventType.STAKE,
    "WithdrawalRequested": LidoEventType.WITHDRAWAL_REQUESTED,
    "WithdrawalClaimed": LidoEventType.WITHDRAWAL_CLAIMED,
    # Transfer is ambiguous, determined by from/to addresses
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class StakeEventData:
    """Parsed data from Submitted event (stake operation)."""

    sender: str
    amount: Decimal
    referral: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sender": self.sender,
            "amount": str(self.amount),
            "referral": self.referral,
        }


@dataclass
class WrapEventData:
    """Parsed data from wrap operation (Transfer event on wstETH)."""

    from_address: str
    to_address: str
    amount: Decimal
    token: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "from_address": self.from_address,
            "to_address": self.to_address,
            "amount": str(self.amount),
            "token": self.token,
        }


@dataclass
class UnwrapEventData:
    """Parsed data from unwrap operation (Transfer event on wstETH)."""

    from_address: str
    to_address: str
    amount: Decimal
    token: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "from_address": self.from_address,
            "to_address": self.to_address,
            "amount": str(self.amount),
            "token": self.token,
        }


@dataclass
class WithdrawalRequestedEventData:
    """Parsed data from WithdrawalRequested event."""

    request_id: int
    requestor: str
    owner: str
    amount_of_steth: Decimal
    amount_of_shares: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "request_id": self.request_id,
            "requestor": self.requestor,
            "owner": self.owner,
            "amount_of_steth": str(self.amount_of_steth),
            "amount_of_shares": str(self.amount_of_shares),
        }


@dataclass
class WithdrawalClaimedEventData:
    """Parsed data from WithdrawalClaimed event."""

    request_id: int
    owner: str
    receiver: str
    amount_of_eth: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "request_id": self.request_id,
            "owner": self.owner,
            "receiver": self.receiver,
            "amount_of_eth": str(self.amount_of_eth),
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    stakes: list[StakeEventData] = field(default_factory=list)
    wraps: list[WrapEventData] = field(default_factory=list)
    unwraps: list[UnwrapEventData] = field(default_factory=list)
    withdrawal_requests: list[WithdrawalRequestedEventData] = field(default_factory=list)
    withdrawal_claims: list[WithdrawalClaimedEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "stakes": [s.to_dict() for s in self.stakes],
            "wraps": [w.to_dict() for w in self.wraps],
            "unwraps": [u.to_dict() for u in self.unwraps],
            "withdrawal_requests": [wr.to_dict() for wr in self.withdrawal_requests],
            "withdrawal_claims": [wc.to_dict() for wc in self.withdrawal_claims],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class LidoReceiptParser:
    """Parser for Lido transaction receipts.

    Refactored to use base infrastructure utilities for hex decoding
    and event registry management. Handles multiple contracts (stETH, wstETH,
    withdrawal queue) with different event types.
    """

    def __init__(self, chain: str = "ethereum", **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            chain: Blockchain network (ethereum, arbitrum, optimism, polygon)
            **kwargs: Additional arguments (ignored for compatibility)
        """
        self.chain = chain
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        # Get contract addresses for this chain
        chain_addresses = LIDO_ADDRESSES.get(chain, {})
        self.steth_address = chain_addresses.get("steth", "").lower()
        self.wsteth_address = chain_addresses.get("wsteth", "").lower()
        self.withdrawal_queue_address = chain_addresses.get("withdrawal_queue", "").lower()

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
            wraps: list[WrapEventData] = []
            unwraps: list[UnwrapEventData] = []
            withdrawal_requests: list[WithdrawalRequestedEventData] = []
            withdrawal_claims: list[WithdrawalClaimedEventData] = []

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

                # Get contract address and normalize
                contract_address = log.get("address", "")
                if isinstance(contract_address, bytes):
                    contract_address = "0x" + contract_address.hex()
                contract_address = contract_address.lower()

                # Get raw data
                data = HexDecoder.normalize_hex(log.get("data", ""))

                # Parse based on event type and contract
                if event_name == "Submitted":
                    # Submitted event from stETH contract (stake)
                    if contract_address == self.steth_address:
                        stake_data = self._parse_submitted_log(topics, data)
                        if stake_data:
                            stakes.append(stake_data)

                elif event_name == "Transfer":
                    # Transfer event - check if it's from wstETH contract
                    if contract_address == self.wsteth_address:
                        transfer_data = self._parse_transfer_log(topics, data, contract_address)
                        if transfer_data:
                            # Determine if wrap or unwrap based on from/to addresses
                            from_addr = transfer_data["from_address"].lower()
                            to_addr = transfer_data["to_address"].lower()
                            zero_addr = "0x" + "0" * 40

                            if from_addr == zero_addr:
                                # Mint = wrap (user receives wstETH)
                                wraps.append(
                                    WrapEventData(
                                        from_address=transfer_data["from_address"],
                                        to_address=transfer_data["to_address"],
                                        amount=transfer_data["amount"],
                                        token=contract_address,
                                    )
                                )
                            elif to_addr == zero_addr:
                                # Burn = unwrap (user burns wstETH)
                                unwraps.append(
                                    UnwrapEventData(
                                        from_address=transfer_data["from_address"],
                                        to_address=transfer_data["to_address"],
                                        amount=transfer_data["amount"],
                                        token=contract_address,
                                    )
                                )

                elif event_name == "WithdrawalRequested":
                    # WithdrawalRequested event from withdrawal queue
                    if contract_address == self.withdrawal_queue_address:
                        wr_data = self._parse_withdrawal_requested_log(topics, data)
                        if wr_data:
                            withdrawal_requests.append(wr_data)

                elif event_name == "WithdrawalClaimed":
                    # WithdrawalClaimed event from withdrawal queue
                    if contract_address == self.withdrawal_queue_address:
                        wc_data = self._parse_withdrawal_claimed_log(topics, data)
                        if wc_data:
                            withdrawal_claims.append(wc_data)

            logger.info(
                f"Parsed Lido receipt: tx={tx_hash[:10]}..., "
                f"stakes={len(stakes)}, wraps={len(wraps)}, unwraps={len(unwraps)}, "
                f"withdrawal_requests={len(withdrawal_requests)}, withdrawal_claims={len(withdrawal_claims)}"
            )

            return ParseResult(
                success=True,
                stakes=stakes,
                wraps=wraps,
                unwraps=unwraps,
                withdrawal_requests=withdrawal_requests,
                withdrawal_claims=withdrawal_claims,
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(
                success=False,
                error=str(e),
            )

    def _parse_submitted_log(self, topics: list[Any], data: str) -> StakeEventData | None:
        """Parse Submitted event data.

        Submitted(address indexed sender, uint256 amount, address referral)
        """
        try:
            # Indexed: sender (topic 1)
            sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""

            # Non-indexed: amount, referral
            amount_wei = HexDecoder.decode_uint256(data, 0)
            referral = HexDecoder.topic_to_address(data[64:128])  # Second 32-byte chunk

            # Convert from wei to ETH
            amount_eth = Decimal(amount_wei) / Decimal(10**18)

            return StakeEventData(
                sender=sender,
                amount=amount_eth,
                referral=referral,
            )

        except Exception as e:
            logger.warning(f"Failed to parse Submitted event: {e}")
            return None

    def _parse_transfer_log(self, topics: list[Any], data: str, contract_address: str) -> dict[str, Any] | None:
        """Parse Transfer event data.

        Transfer(address indexed from, address indexed to, uint256 value)
        """
        try:
            # Indexed: from (topic 1), to (topic 2)
            from_addr = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            to_addr = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            # Non-indexed: value
            amount_wei = HexDecoder.decode_uint256(data, 0)

            # Convert from wei to token units (18 decimals)
            amount = Decimal(amount_wei) / Decimal(10**18)

            return {
                "from_address": from_addr,
                "to_address": to_addr,
                "amount": amount,
                "token": contract_address,
            }

        except Exception as e:
            logger.warning(f"Failed to parse Transfer event: {e}")
            return None

    def _parse_withdrawal_requested_log(self, topics: list[Any], data: str) -> WithdrawalRequestedEventData | None:
        """Parse WithdrawalRequested event data.

        WithdrawalRequested(uint256 indexed requestId, address indexed requestor,
                           address indexed owner, uint256 amountOfStETH, uint256 amountOfShares)
        """
        try:
            # Indexed: requestId (topic 1), requestor (topic 2), owner (topic 3)
            request_id = HexDecoder.decode_uint256(topics[1], 0) if len(topics) > 1 else 0
            requestor = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            owner = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

            # Non-indexed: amountOfStETH, amountOfShares
            amount_of_steth_wei = HexDecoder.decode_uint256(data, 0)
            amount_of_shares_wei = HexDecoder.decode_uint256(data, 32)

            # Convert from wei to ETH (18 decimals)
            amount_of_steth = Decimal(amount_of_steth_wei) / Decimal(10**18)
            amount_of_shares = Decimal(amount_of_shares_wei) / Decimal(10**18)

            return WithdrawalRequestedEventData(
                request_id=request_id,
                requestor=requestor,
                owner=owner,
                amount_of_steth=amount_of_steth,
                amount_of_shares=amount_of_shares,
            )

        except Exception as e:
            logger.warning(f"Failed to parse WithdrawalRequested event: {e}")
            return None

    def _parse_withdrawal_claimed_log(self, topics: list[Any], data: str) -> WithdrawalClaimedEventData | None:
        """Parse WithdrawalClaimed event data.

        WithdrawalClaimed(uint256 indexed requestId, address indexed owner,
                         address indexed receiver, uint256 amountOfETH)
        """
        try:
            # Indexed: requestId (topic 1), owner (topic 2), receiver (topic 3)
            request_id = HexDecoder.decode_uint256(topics[1], 0) if len(topics) > 1 else 0
            owner = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            receiver = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

            # Non-indexed: amountOfETH
            amount_of_eth_wei = HexDecoder.decode_uint256(data, 0)

            # Convert from wei to ETH (18 decimals)
            amount_of_eth = Decimal(amount_of_eth_wei) / Decimal(10**18)

            return WithdrawalClaimedEventData(
                request_id=request_id,
                owner=owner,
                receiver=receiver,
                amount_of_eth=amount_of_eth,
            )

        except Exception as e:
            logger.warning(f"Failed to parse WithdrawalClaimed event: {e}")
            return None

    # Backward compatibility methods
    def parse_stake(self, log: dict[str, Any]) -> StakeEventData | None:
        """Parse a Submitted (stake) event from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))
        return self._parse_submitted_log(topics, data)

    def parse_wrap(self, log: dict[str, Any]) -> WrapEventData | None:
        """Parse a wrap event (Transfer from zero address) from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))

        contract_address = log.get("address", "")
        if isinstance(contract_address, bytes):
            contract_address = "0x" + contract_address.hex()

        transfer_data = self._parse_transfer_log(topics, data, contract_address)
        if transfer_data:
            from_addr = transfer_data["from_address"].lower()
            zero_addr = "0x" + "0" * 40

            if from_addr == zero_addr:
                return WrapEventData(
                    from_address=transfer_data["from_address"],
                    to_address=transfer_data["to_address"],
                    amount=transfer_data["amount"],
                    token=contract_address,
                )
        return None

    def parse_unwrap(self, log: dict[str, Any]) -> UnwrapEventData | None:
        """Parse an unwrap event (Transfer to zero address) from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))

        contract_address = log.get("address", "")
        if isinstance(contract_address, bytes):
            contract_address = "0x" + contract_address.hex()

        transfer_data = self._parse_transfer_log(topics, data, contract_address)
        if transfer_data:
            to_addr = transfer_data["to_address"].lower()
            zero_addr = "0x" + "0" * 40

            if to_addr == zero_addr:
                return UnwrapEventData(
                    from_address=transfer_data["from_address"],
                    to_address=transfer_data["to_address"],
                    amount=transfer_data["amount"],
                    token=contract_address,
                )
        return None

    def parse_withdrawal_requested(self, log: dict[str, Any]) -> WithdrawalRequestedEventData | None:
        """Parse a WithdrawalRequested event from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))
        return self._parse_withdrawal_requested_log(topics, data)

    def parse_withdrawals_claimed(self, log: dict[str, Any]) -> WithdrawalClaimedEventData | None:
        """Parse a WithdrawalClaimed event from a single log entry."""
        topics = log.get("topics", [])
        data = HexDecoder.normalize_hex(log.get("data", ""))
        return self._parse_withdrawal_claimed_log(topics, data)

    def is_lido_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Lido event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known Lido event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes, log: dict[str, Any] | None = None) -> LidoEventType:
        """Get the event type for a topic.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)
            log: Optional log dict for disambiguating Transfer events

        Returns:
            Event type or UNKNOWN
        """
        # Normalize topic
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()

        event_name = self.registry.get_event_name(topic)
        if event_name == "Submitted":
            return LidoEventType.STAKE
        elif event_name == "WithdrawalRequested":
            return LidoEventType.WITHDRAWAL_REQUESTED
        elif event_name == "WithdrawalClaimed":
            return LidoEventType.WITHDRAWAL_CLAIMED
        elif event_name == "Transfer" and log:
            # Need to check from/to addresses for wrap/unwrap
            topics = log.get("topics", [])
            if len(topics) >= 3:
                from_addr = HexDecoder.topic_to_address(topics[1])
                to_addr = HexDecoder.topic_to_address(topics[2])
                zero_addr = "0x" + "0" * 40

                if from_addr.lower() == zero_addr:
                    return LidoEventType.WRAP
                elif to_addr.lower() == zero_addr:
                    return LidoEventType.UNWRAP
        return LidoEventType.UNKNOWN

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    def extract_stake_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract stake amount from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Stake amount in wei if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.stakes:
                # Return in wei (reverse the conversion done in parsing)
                return int(result.stakes[0].amount * Decimal(10**18))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract stake amount: {e}")
            return None

    def extract_shares_received(self, receipt: dict[str, Any]) -> int | None:
        """Extract stETH/wstETH shares received from transaction receipt.

        When staking ETH, user receives stETH. When wrapping stETH, user receives wstETH.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Shares received in wei if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            # Check for wraps first (wstETH received)
            if result.wraps:
                return int(result.wraps[0].amount * Decimal(10**18))
            # Then check stakes (stETH received - same amount as staked)
            if result.stakes:
                return int(result.stakes[0].amount * Decimal(10**18))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract shares received: {e}")
            return None

    def extract_unstake_amount(self, receipt: dict[str, Any]) -> int | None:
        """Extract unstake amount from transaction receipt.

        This is the amount of stETH requested for withdrawal.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Unstake amount in wei if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.withdrawal_requests:
                # Return in wei (reverse the conversion done in parsing)
                return int(result.withdrawal_requests[0].amount_of_steth * Decimal(10**18))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract unstake amount: {e}")
            return None

    def extract_underlying_received(self, receipt: dict[str, Any]) -> int | None:
        """Extract underlying ETH received from withdrawal claim.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            ETH received in wei if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.withdrawal_claims:
                # Return in wei (reverse the conversion done in parsing)
                return int(result.withdrawal_claims[0].amount_of_eth * Decimal(10**18))
            return None
        except Exception as e:
            logger.warning(f"Failed to extract underlying received: {e}")
            return None


__all__ = [
    "LidoReceiptParser",
    "LidoEventType",
    "StakeEventData",
    "WrapEventData",
    "UnwrapEventData",
    "WithdrawalRequestedEventData",
    "WithdrawalClaimedEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
]
