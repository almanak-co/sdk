"""Enso Receipt Parser (Refactored).

This module provides functionality to parse transaction receipts
from Enso swap transactions and extract the actual amounts transferred.

Refactored to use base infrastructure utilities while maintaining
backward compatibility with the original API.
"""

import logging
from dataclasses import dataclass
from typing import Any

from almanak.framework.connectors.base.hex_utils import HexDecoder
from almanak.framework.utils.log_formatters import format_gas_cost, format_tx_hash

logger = logging.getLogger(__name__)


# Event signatures (keccak256 of event signature)
TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


@dataclass
class SwapResult:
    """Result of a swap transaction.

    Attributes:
        success: Whether the swap was successful
        token_in: Input token address
        token_out: Output token address
        amount_in: Actual input amount
        amount_out: Actual output amount
        tx_hash: Transaction hash
        gas_used: Gas used by the transaction
        effective_gas_price: Effective gas price
        error: Error message if failed
    """

    success: bool
    token_in: str | None = None
    token_out: str | None = None
    amount_in: int = 0
    amount_out: int = 0
    tx_hash: str | None = None
    gas_used: int = 0
    effective_gas_price: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "tx_hash": self.tx_hash,
            "gas_used": self.gas_used,
            "effective_gas_price": self.effective_gas_price,
            "error": self.error,
        }


class EnsoReceiptParser:
    """Parser for Enso transaction receipts.

    This parser extracts swap results from transaction receipts by:
    1. Checking transaction status
    2. Parsing Transfer event logs to find amounts
    3. Identifying the recipient's received amount

    Example:
        parser = EnsoReceiptParser()
        receipt = web3.eth.get_transaction_receipt(tx_hash)

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address="0x...",
            token_out="0x...",
        )
        print(f"Received: {result.amount_out}")
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize EnsoReceiptParser.

        Accepts and ignores keyword arguments (e.g. chain=) passed by
        the receipt_registry when instantiating parsers dynamically.
        """

    def parse_swap_receipt(
        self,
        receipt: dict[str, Any],
        wallet_address: str,
        token_out: str,
        token_in: str | None = None,
        expected_amount_out: int | None = None,
    ) -> SwapResult:
        """Parse a swap transaction receipt.

        Args:
            receipt: Transaction receipt from web3
            wallet_address: Address that received the output tokens
            token_out: Output token address
            token_in: Input token address (optional)
            expected_amount_out: Expected output amount for validation

        Returns:
            SwapResult with parsed data
        """
        # Normalize transaction hash
        tx_hash = self._normalize_tx_hash(receipt.get("transactionHash"))

        # Check transaction status
        status = receipt.get("status", 0)
        if status != 1:
            return SwapResult(
                success=False,
                tx_hash=tx_hash,
                error="Transaction reverted",
            )

        # Parse logs to find amounts
        logs = receipt.get("logs", [])
        amount_out = self._extract_transfer_amount(
            logs=logs,
            token_address=token_out,
            to_address=wallet_address,
        )

        amount_in = 0
        if token_in:
            amount_in = self._extract_transfer_amount(
                logs=logs,
                token_address=token_in,
                from_address=wallet_address,
            )

        # Use expected amount if we couldn't extract from logs
        if amount_out == 0 and expected_amount_out:
            logger.warning(f"Could not extract amount_out from logs, using expected: {expected_amount_out}")
            amount_out = expected_amount_out

        gas_used = receipt.get("gasUsed", 0)
        effective_gas_price = receipt.get("effectiveGasPrice", 0)

        # Log parsed receipt with user-friendly formatting
        tx_fmt = format_tx_hash(tx_hash)
        gas_fmt = format_gas_cost(gas_used)
        logger.info(f"🔍 Parsed Enso swap: tx={tx_fmt}, amount_out={amount_out:,}, {gas_fmt}")

        return SwapResult(
            success=True,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            tx_hash=tx_hash,
            gas_used=gas_used,
            effective_gas_price=effective_gas_price,
        )

    def _extract_transfer_amount(
        self,
        logs: list[dict[str, Any]],
        token_address: str,
        from_address: str | None = None,
        to_address: str | None = None,
    ) -> int:
        """Extract transfer amount from logs.

        Args:
            logs: Transaction logs
            token_address: Token contract address
            from_address: Filter by sender address
            to_address: Filter by recipient address

        Returns:
            Transfer amount (0 if not found)
        """
        token_address_lower = token_address.lower()

        for log in logs:
            # Get log address (normalize bytes/string)
            log_address = log.get("address", "")
            if isinstance(log_address, bytes):
                log_address = "0x" + log_address.hex()
            log_address = log_address.lower()

            # Check if this is from the token contract
            if log_address != token_address_lower:
                continue

            # Get topics
            topics = log.get("topics", [])
            if not topics:
                continue

            # Get first topic (event signature) - normalize bytes/string
            topic0 = topics[0]
            if isinstance(topic0, bytes):
                topic0 = "0x" + topic0.hex()
            topic0 = topic0.lower()

            # Check if this is a Transfer event
            if topic0 != TRANSFER_EVENT_SIGNATURE.lower():
                continue

            # For Transfer events: topics = [signature, from, to]
            if len(topics) < 3:
                continue

            # Extract from and to addresses from topics using HexDecoder
            log_from = HexDecoder.topic_to_address(topics[1])
            log_to = HexDecoder.topic_to_address(topics[2])

            # Filter by from/to address if specified
            if from_address and log_from != from_address.lower():
                continue
            if to_address and log_to != to_address.lower():
                continue

            # Extract amount from data using HexDecoder
            data = HexDecoder.normalize_hex(log.get("data", ""))
            if data:
                try:
                    amount = HexDecoder.decode_uint256(data, 0)
                    logger.debug(f"Found Transfer: from={log_from[:10]}..., to={log_to[:10]}..., amount={amount}")
                    return amount
                except (ValueError, IndexError):
                    continue

        return 0

    def parse_approval_receipt(
        self,
        receipt: dict[str, Any],
    ) -> dict[str, Any]:
        """Parse an approval transaction receipt.

        Args:
            receipt: Transaction receipt from web3

        Returns:
            Dict with approval result
        """
        tx_hash = self._normalize_tx_hash(receipt.get("transactionHash"))
        status = receipt.get("status", 0)

        return {
            "success": status == 1,
            "tx_hash": tx_hash,
            "gas_used": receipt.get("gasUsed", 0),
            "effective_gas_price": receipt.get("effectiveGasPrice", 0),
            "error": "Transaction reverted" if status != 1 else None,
        }

    @staticmethod
    def _normalize_tx_hash(tx_hash: Any) -> str:
        """Normalize transaction hash to hex string with 0x prefix."""
        if isinstance(tx_hash, bytes):
            result = tx_hash.hex()
            return result if result.startswith("0x") else "0x" + result
        return str(tx_hash) if tx_hash else ""


__all__ = ["EnsoReceiptParser", "SwapResult"]
