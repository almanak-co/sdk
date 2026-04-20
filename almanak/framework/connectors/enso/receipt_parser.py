"""Enso Receipt Parser (Refactored).

This module provides functionality to parse transaction receipts
from Enso swap transactions and extract the actual amounts transferred.

Refactored to use base infrastructure utilities while maintaining
backward compatibility with the original API.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.framework.connectors.base.hex_utils import HexDecoder
from almanak.framework.execution.extracted_data import SwapAmounts
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

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset({"swap_amounts"})

    def __init__(self, **kwargs: Any) -> None:
        """Initialize EnsoReceiptParser.

        Args:
            **kwargs: Keyword arguments passed by the receipt_registry.
                chain: Chain name for token decimal resolution.
        """
        self._chain: str | None = kwargs.get("chain")

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

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> SwapAmounts | None:
        """Extract swap amounts from an Enso swap receipt.

        Called by ResultEnricher for SWAP intents. Parses ERC-20 Transfer
        events to determine the input and output token amounts.

        The heuristic:
        - amount_in: first Transfer FROM the wallet (tx sender)
        - amount_out: last Transfer TO the wallet (final output after routing)

        Args:
            receipt: Transaction receipt dict with 'logs' and 'from' fields

        Returns:
            SwapAmounts if swap transfers found, None otherwise
        """
        wallet = self._normalize_address(receipt.get("from", ""))
        if not wallet:
            return None

        status = receipt.get("status", 0)
        if status != 1:
            return None

        logs = receipt.get("logs", [])
        transfers_from_wallet: list[tuple[str, int]] = []
        transfers_to_wallet: list[tuple[str, int]] = []

        for log in logs:
            topics = log.get("topics", [])
            if not topics or len(topics) < 3:
                continue

            topic0 = topics[0]
            if isinstance(topic0, bytes):
                topic0 = "0x" + topic0.hex()
            if topic0.lower() != TRANSFER_EVENT_SIGNATURE.lower():
                continue

            log_from = HexDecoder.topic_to_address(topics[1])
            log_to = HexDecoder.topic_to_address(topics[2])
            data = HexDecoder.normalize_hex(log.get("data", ""))
            if not data:
                continue
            try:
                amount = HexDecoder.decode_uint256(data, 0)
            except (ValueError, IndexError):
                continue

            token_address = self._normalize_address(log.get("address", ""))

            if log_from == wallet:
                transfers_from_wallet.append((token_address, amount))
            if log_to == wallet:
                transfers_to_wallet.append((token_address, amount))

        if not transfers_to_wallet:
            return None

        # Input: first transfer from wallet; Output: last transfer to wallet
        token_in_addr, amount_in_raw = transfers_from_wallet[0] if transfers_from_wallet else ("", 0)
        token_out_addr, amount_out_raw = transfers_to_wallet[-1]

        if amount_out_raw == 0:
            return None

        decimals_in = self._resolve_decimals(token_in_addr)
        decimals_out = self._resolve_decimals(token_out_addr)

        # If we can't resolve decimals for the output token, bail out rather than
        # returning wildly wrong amounts (e.g., 10^12x off for 6-decimal tokens).
        if decimals_out is None:
            logger.warning("Cannot compute swap amounts: output token decimals unknown")
            return None

        amount_in_decimal = (
            Decimal(amount_in_raw) / Decimal(10**decimals_in)
            if (amount_in_raw and decimals_in is not None)
            else Decimal(0)
        )
        amount_out_decimal = Decimal(amount_out_raw) / Decimal(10**decimals_out)

        effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal else Decimal(0)

        return SwapAmounts(
            amount_in=amount_in_raw,
            amount_out=amount_out_raw,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            effective_price=effective_price,
            token_in=token_in_addr or None,
            token_out=token_out_addr or None,
        )

    def _resolve_decimals(self, token_address: str) -> int | None:
        """Resolve token decimals via the token resolver.

        Returns None if the resolver is unavailable or the token is unknown,
        so callers can decide how to handle missing decimals rather than
        silently using a wrong default.

        Args:
            token_address: Checksummed or lowercase token address

        Returns:
            Token decimals (e.g. 6 for USDC, 18 for WETH), or None if unknown.
        """
        if not token_address:
            return None
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            token = resolver.resolve(token_address, self._chain or "ethereum")
            return token.decimals
        except Exception:
            logger.warning(f"Could not resolve decimals for {token_address}, swap amounts will be unavailable")
            return None

    @staticmethod
    def _normalize_address(address: Any) -> str:
        """Normalize an address to lowercase hex string."""
        if isinstance(address, bytes):
            address = "0x" + address.hex()
        return str(address).lower() if address else ""

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
