"""Receipt parsing utilities for PnL backtesting.

This module provides functions to extract token transfer information from
transaction receipts, enabling the backtester to determine actual execution
amounts versus expected amounts.

The primary use case is for swap transactions where we want to compare:
- Expected token amounts (from the intent/quote)
- Actual token amounts (from the on-chain receipt)

Example:
    from almanak.framework.backtesting.pnl.receipt_utils import (
        parse_transfer_events,
        extract_token_flows,
    )

    # Parse a transaction receipt
    transfers = parse_transfer_events(receipt)

    # Extract token flows for a specific wallet
    flows = extract_token_flows(receipt, wallet_address="0x...")

    print(f"Tokens in: {flows.tokens_in}")
    print(f"Tokens out: {flows.tokens_out}")
"""

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Constants
# =============================================================================

# ERC-20 Transfer(address,address,uint256) event signature
# keccak256("Transfer(address,address,uint256)")
TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TransferEvent:
    """Parsed ERC-20 Transfer event.

    Attributes:
        token_address: Address of the token contract
        from_addr: Sender address
        to_addr: Recipient address
        value: Amount transferred (in wei/smallest unit)
        log_index: Index of this log in the transaction
    """

    token_address: str
    from_addr: str
    to_addr: str
    value: int
    log_index: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_address": self.token_address,
            "from_addr": self.from_addr,
            "to_addr": self.to_addr,
            "value": str(self.value),
            "log_index": self.log_index,
        }


@dataclass
class TokenFlow:
    """Token flow for a specific token.

    Attributes:
        token_address: Address of the token contract
        amount_in: Total amount received (to the wallet)
        amount_out: Total amount sent (from the wallet)
        net_amount: Net change (in - out, positive = received more)
    """

    token_address: str
    amount_in: int = 0
    amount_out: int = 0

    @property
    def net_amount(self) -> int:
        """Net change in token balance."""
        return self.amount_in - self.amount_out

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_address": self.token_address,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "net_amount": str(self.net_amount),
        }


@dataclass
class TokenFlows:
    """Aggregated token flows for a wallet address.

    Attributes:
        wallet_address: The wallet address these flows are for
        tokens_in: Dict of token_address -> amount received
        tokens_out: Dict of token_address -> amount sent
        flows: Dict of token_address -> TokenFlow (detailed per-token flows)
    """

    wallet_address: str
    tokens_in: dict[str, int] = field(default_factory=dict)
    tokens_out: dict[str, int] = field(default_factory=dict)
    flows: dict[str, TokenFlow] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "wallet_address": self.wallet_address,
            "tokens_in": {k: str(v) for k, v in self.tokens_in.items()},
            "tokens_out": {k: str(v) for k, v in self.tokens_out.items()},
            "flows": {k: v.to_dict() for k, v in self.flows.items()},
        }


# =============================================================================
# Parsing Functions
# =============================================================================


def _topic_to_address(topic: bytes | str) -> str:
    """Convert a topic to an address (last 20 bytes).

    Args:
        topic: Topic value (bytes or hex string)

    Returns:
        Checksummed address string
    """
    topic_hex = _hex_without_prefix(topic)
    if len(topic_hex) < 40:
        return ""
    return "0x" + topic_hex[-40:].lower()


def _hex_without_prefix(value: bytes | str) -> str:
    """Return lowercase hex without a leading 0x prefix."""
    if isinstance(value, bytes):
        return value.hex().lower()
    if isinstance(value, str):
        text = value.strip().lower()
        return text[2:] if text.startswith("0x") else text
    return ""


def _normalize_topic(topic: bytes | str) -> str:
    """Return a normalized 0x-prefixed topic string."""
    topic_hex = _hex_without_prefix(topic)
    return f"0x{topic_hex}" if topic_hex else ""


def _parse_int(value: Any) -> int | None:
    """Parse decimal, hex-string, or bytes integers from JSON-RPC fields."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, bytes):
        value_hex = value.hex()
        return int(value_hex, 16) if value_hex else 0
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        return int(text, 16) if text.lower().startswith("0x") else int(text)
    return None


def _receipt_succeeded(receipt: dict[str, Any]) -> bool:
    """Return whether a receipt status indicates success."""
    if "status" not in receipt:
        return True
    try:
        return _parse_int(receipt["status"]) == 1
    except ValueError:
        return False


def _is_transfer_topic(topics: list[Any] | tuple[Any, ...]) -> bool:
    """Return whether topics start with the ERC-20/721 Transfer signature."""
    return bool(topics) and _normalize_topic(topics[0]) == TRANSFER_EVENT_TOPIC


def _is_erc20_transfer_topics(topics: list[Any] | tuple[Any, ...]) -> bool:
    """Return whether topic shape matches ERC-20 Transfer(address,address,uint256)."""
    if not _is_transfer_topic(topics):
        return False
    if len(topics) < 3:
        logger.warning("Transfer event has fewer than 3 topics, skipping")
        return False
    if len(topics) != 3:
        logger.debug("Transfer event has non-ERC20 topic shape, skipping")
        return False
    return True


def _parse_transfer_value(data: Any) -> int | None:
    """Parse a Transfer data word; empty means unmeasured/malformed, not zero."""
    data_hex = _hex_without_prefix(data)
    if not data_hex:
        return None
    return int(data_hex, 16)


def _normalize_token_address(token_address: Any) -> str:
    """Normalize an emitting token contract address from a log."""
    if isinstance(token_address, bytes):
        return "0x" + token_address.hex().lower()
    if isinstance(token_address, str):
        text = token_address.strip().lower()
        return text if text.startswith("0x") else f"0x{text}"
    return ""


def _parse_log_index(log_index: Any) -> int:
    """Parse logIndex while preserving zero as the default for absent values."""
    try:
        parsed = _parse_int(log_index)
    except ValueError:
        return 0
    return parsed if parsed is not None else 0


def _parse_transfer_log(log: Any) -> TransferEvent | None:
    """Parse a single ERC-20 Transfer log, returning None when it is not usable."""
    if not isinstance(log, dict):
        return None

    topics = log.get("topics", [])
    if not isinstance(topics, list | tuple) or not _is_erc20_transfer_topics(topics):
        return None

    from_addr = _topic_to_address(topics[1])
    to_addr = _topic_to_address(topics[2])
    token_address = _normalize_token_address(log.get("address", ""))
    if not from_addr or not to_addr or not token_address:
        logger.warning("Transfer event has invalid address fields, skipping")
        return None

    value = _parse_transfer_value(log.get("data", ""))
    if value is None:
        logger.warning("Transfer event has empty data, skipping")
        return None

    return TransferEvent(
        token_address=token_address,
        from_addr=from_addr,
        to_addr=to_addr,
        value=value,
        log_index=_parse_log_index(log.get("logIndex", 0)),
    )


def parse_transfer_events(receipt: dict[str, Any] | None) -> list[TransferEvent]:
    """Parse ERC-20 Transfer events from a transaction receipt.

    This function extracts all Transfer events from a receipt's logs,
    returning structured TransferEvent objects with the sender, recipient,
    and amount for each transfer.

    Args:
        receipt: Transaction receipt dict containing 'logs', 'status', etc.
                 Can be None (returns empty list).

    Returns:
        List of TransferEvent objects, one for each Transfer event found.
        Returns empty list if receipt is None, has no logs, or failed (status != 1).

    Example:
        transfers = parse_transfer_events(receipt)
        for t in transfers:
            print(f"{t.from_addr} -> {t.to_addr}: {t.value} of {t.token_address}")
    """
    if receipt is None:
        return []

    if not _receipt_succeeded(receipt):
        logger.debug("Transaction failed (status != 1), no transfers to parse")
        return []

    logs = receipt.get("logs", [])
    if not logs:
        return []

    transfers: list[TransferEvent] = []

    for log in logs:
        try:
            transfer = _parse_transfer_log(log)
            if transfer is not None:
                transfers.append(transfer)

        except Exception as e:
            logger.warning(f"Failed to parse Transfer event: {e}")
            continue

    logger.debug(f"Parsed {len(transfers)} Transfer events from receipt")
    return transfers


def extract_token_flows(
    receipt: dict[str, Any] | None,
    wallet_address: str,
) -> TokenFlows:
    """Extract token flows for a specific wallet from a transaction receipt.

    This function aggregates all token transfers involving the specified wallet,
    calculating total tokens received (in) and sent (out) per token.

    Args:
        receipt: Transaction receipt dict containing 'logs', 'status', etc.
                 Can be None (returns empty TokenFlows).
        wallet_address: The wallet address to track flows for.

    Returns:
        TokenFlows object with aggregated in/out amounts per token.
        Returns TokenFlows with empty dicts if receipt is None or has no relevant transfers.

    Example:
        flows = extract_token_flows(receipt, wallet_address="0x123...")

        for token, amount in flows.tokens_in.items():
            print(f"Received {amount} of {token}")

        for token, amount in flows.tokens_out.items():
            print(f"Sent {amount} of {token}")
    """
    wallet_lower = wallet_address.lower()
    result = TokenFlows(wallet_address=wallet_lower)

    if receipt is None:
        return result

    transfers = parse_transfer_events(receipt)

    for transfer in transfers:
        token = transfer.token_address

        # Initialize flow tracking for this token if not seen
        if token not in result.flows:
            result.flows[token] = TokenFlow(token_address=token)

        # Check if wallet is recipient (token in)
        if transfer.to_addr == wallet_lower:
            result.tokens_in[token] = result.tokens_in.get(token, 0) + transfer.value
            result.flows[token].amount_in += transfer.value

        # Check if wallet is sender (token out)
        if transfer.from_addr == wallet_lower:
            result.tokens_out[token] = result.tokens_out.get(token, 0) + transfer.value
            result.flows[token].amount_out += transfer.value

    # Remove flows with no activity
    result.flows = {k: v for k, v in result.flows.items() if v.amount_in > 0 or v.amount_out > 0}

    logger.debug(
        f"Extracted token flows for {wallet_lower}: "
        f"{len(result.tokens_in)} tokens in, {len(result.tokens_out)} tokens out"
    )

    return result


# =============================================================================
# Discrepancy Calculation
# =============================================================================

# Default threshold for logging discrepancy warnings (1% = 0.01)
DEFAULT_DISCREPANCY_THRESHOLD = 0.01


@dataclass
class DiscrepancyResult:
    """Result of a discrepancy calculation between expected and actual amounts.

    Attributes:
        expected: The expected amount
        actual: The actual amount
        difference: Absolute difference (actual - expected)
        percentage: Percentage difference as a decimal (e.g., 0.05 = 5%)
        exceeds_threshold: Whether the discrepancy exceeds the threshold
        threshold: The threshold used for comparison
    """

    expected: int | float
    actual: int | float
    difference: int | float
    percentage: float
    exceeds_threshold: bool
    threshold: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "expected": str(self.expected),
            "actual": str(self.actual),
            "difference": str(self.difference),
            "percentage": self.percentage,
            "exceeds_threshold": self.exceeds_threshold,
            "threshold": self.threshold,
        }


def calculate_discrepancy(
    expected: int | float,
    actual: int | float,
    threshold: float = DEFAULT_DISCREPANCY_THRESHOLD,
    log_warning: bool = True,
    context: str | None = None,
) -> DiscrepancyResult:
    """Calculate the discrepancy between expected and actual amounts.

    This function computes the percentage difference between expected and actual
    execution amounts, optionally logging a warning when the discrepancy exceeds
    the specified threshold.

    Args:
        expected: The expected amount (from intent/quote)
        actual: The actual amount (from receipt/execution)
        threshold: Percentage threshold for warning (default 1% = 0.01)
        log_warning: Whether to log a warning if threshold is exceeded (default True)
        context: Optional context string to include in log messages (e.g., token symbol)

    Returns:
        DiscrepancyResult containing the expected, actual, difference,
        percentage, and whether it exceeds the threshold.

    Example:
        # Check if actual differs from expected by more than 1%
        result = calculate_discrepancy(expected=1000, actual=985)
        if result.exceeds_threshold:
            print(f"Discrepancy of {result.percentage:.2%} detected!")

        # Custom threshold with context
        result = calculate_discrepancy(
            expected=1000,
            actual=950,
            threshold=0.02,  # 2%
            context="USDC swap"
        )
    """
    # Calculate absolute difference
    difference = actual - expected

    # Calculate percentage difference (relative to expected)
    if expected == 0:
        # Avoid division by zero - if expected is 0 and actual is not, that's 100% discrepancy
        percentage = 1.0 if actual != 0 else 0.0
    else:
        percentage = abs(difference) / abs(expected)

    exceeds = percentage > threshold

    result = DiscrepancyResult(
        expected=expected,
        actual=actual,
        difference=difference,
        percentage=percentage,
        exceeds_threshold=exceeds,
        threshold=threshold,
    )

    # Log warning if threshold exceeded and logging is enabled
    if log_warning and exceeds:
        ctx_str = f" [{context}]" if context else ""
        logger.warning(
            f"Execution discrepancy{ctx_str}: "
            f"expected={expected}, actual={actual}, "
            f"difference={difference} ({percentage:.2%}), threshold={threshold:.2%}"
        )

    return result


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "TRANSFER_EVENT_TOPIC",
    "TransferEvent",
    "TokenFlow",
    "TokenFlows",
    "parse_transfer_events",
    "extract_token_flows",
    "DEFAULT_DISCREPANCY_THRESHOLD",
    "DiscrepancyResult",
    "calculate_discrepancy",
]
