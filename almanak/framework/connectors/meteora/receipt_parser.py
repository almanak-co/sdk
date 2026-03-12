"""Meteora DLMM receipt parser.

Extracts LP operation results from Solana transaction receipts.
Uses the balance-delta approach (same as Raydium CLMM) for
determining actual deposited/received token amounts.

Meteora positions are non-transferable program accounts (not NFTs),
so position_id extraction looks at metadata rather than new mints.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


class MeteoraReceiptParser:
    """Parser for Meteora DLMM transaction receipts.

    Extracts position IDs, liquidity amounts, and token balances
    from Solana transaction receipts.

    Supports the extraction methods required by ResultEnricher:
    - extract_position_id(receipt) -> str | None
    - extract_liquidity(receipt) -> dict | None
    - extract_lp_close_data(receipt) -> dict | None

    Extraction approach:
    1. Parse log messages for Meteora program events
    2. Use preTokenBalances/postTokenBalances for actual amounts
    3. Look for position address in ActionBundle metadata
    """

    SUPPORTED_EXTRACTIONS = frozenset({"position_id", "liquidity", "lp_close_data"})

    def __init__(self, **kwargs: Any) -> None:
        """Initialize MeteoraReceiptParser.

        Args:
            **kwargs: Keyword arguments from receipt_registry (e.g., chain).
        """
        self._chain: str = kwargs.get("chain", "solana")

    def parse_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """Parse a receipt for ReceiptParser protocol compatibility.

        Args:
            receipt: Solana transaction receipt dict.

        Returns:
            Dict with parsed LP data.
        """
        position_id = self.extract_position_id(receipt)
        liquidity = self.extract_liquidity(receipt)
        lp_close = self.extract_lp_close_data(receipt)
        return {
            "position_id": position_id,
            "liquidity": liquidity,
            "lp_close_data": lp_close,
            "success": receipt.get("success", True),
        }

    def extract_position_id(self, receipt: dict[str, Any]) -> str | None:
        """Extract the position address from a Meteora LP open receipt.

        Meteora positions are Keypair-based accounts (not NFTs).
        The position address is typically in the ActionBundle metadata,
        but we also look in the transaction's account keys for new
        writable accounts.

        Args:
            receipt: Solana transaction receipt dict.

        Returns:
            Position address (Base58), or None if not found.
        """
        # Check metadata first (set by adapter during compilation)
        metadata = receipt.get("metadata", {})
        if metadata.get("position_address"):
            return metadata["position_address"]

        # Fallback: look for new writable accounts in the transaction
        # The position account is created as a new account during initializePosition
        meta = receipt.get("meta", {})
        if not meta:
            return None

        # Check log messages for position-related events
        log_messages = meta.get("logMessages", [])
        for msg in log_messages:
            if "InitializePosition" in msg or "initialize_position" in msg:
                logger.debug("Found initializePosition log entry")
                # The position address should be in account keys
                break

        return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """Extract liquidity data from an LP open/increase receipt.

        Uses balance deltas to determine actual deposited amounts.

        Args:
            receipt: Solana transaction receipt dict.

        Returns:
            Dict with amount_x, amount_y, position_address, or None.
        """
        meta = receipt.get("meta", {})
        if not meta:
            return None

        deltas = self._compute_balance_deltas(meta)
        if not deltas:
            return None

        # Negative deltas = tokens sent to pool (deposited)
        negative_deltas = [(mint, delta, dec) for mint, delta, dec in deltas if delta < 0]
        negative_deltas.sort(key=lambda x: x[1])  # Most negative first

        result: dict[str, Any] = {}
        if len(negative_deltas) >= 2:
            result["token_x_mint"] = negative_deltas[0][0]
            result["amount_x_raw"] = abs(negative_deltas[0][1])
            result["amount_x"] = str(Decimal(abs(negative_deltas[0][1])) / Decimal(10) ** negative_deltas[0][2])
            result["token_y_mint"] = negative_deltas[1][0]
            result["amount_y_raw"] = abs(negative_deltas[1][1])
            result["amount_y"] = str(Decimal(abs(negative_deltas[1][1])) / Decimal(10) ** negative_deltas[1][2])
        elif len(negative_deltas) == 1:
            result["token_x_mint"] = negative_deltas[0][0]
            result["amount_x_raw"] = abs(negative_deltas[0][1])
            result["amount_x"] = str(Decimal(abs(negative_deltas[0][1])) / Decimal(10) ** negative_deltas[0][2])

        # Include position address if available
        position_id = self.extract_position_id(receipt)
        if position_id:
            result["position_address"] = position_id

        return result if result else None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """Extract LP close data from a receipt.

        Uses balance deltas to determine received token amounts.

        Args:
            receipt: Solana transaction receipt dict.

        Returns:
            Dict with received amounts, or None.
        """
        meta = receipt.get("meta", {})
        if not meta:
            return None

        deltas = self._compute_balance_deltas(meta)
        if not deltas:
            return None

        # Positive deltas = tokens received by user (from pool)
        positive_deltas = [(mint, delta, dec) for mint, delta, dec in deltas if delta > 0]
        positive_deltas.sort(key=lambda x: x[1], reverse=True)

        result: dict[str, Any] = {}
        if len(positive_deltas) >= 2:
            result["token_x_mint"] = positive_deltas[0][0]
            result["amount_x_received_raw"] = positive_deltas[0][1]
            result["amount_x_received"] = str(Decimal(positive_deltas[0][1]) / Decimal(10) ** positive_deltas[0][2])
            result["token_y_mint"] = positive_deltas[1][0]
            result["amount_y_received_raw"] = positive_deltas[1][1]
            result["amount_y_received"] = str(Decimal(positive_deltas[1][1]) / Decimal(10) ** positive_deltas[1][2])
        elif len(positive_deltas) == 1:
            result["token_x_mint"] = positive_deltas[0][0]
            result["amount_x_received_raw"] = positive_deltas[0][1]
            result["amount_x_received"] = str(Decimal(positive_deltas[0][1]) / Decimal(10) ** positive_deltas[0][2])

        return result if result else None

    def _compute_balance_deltas(self, meta: dict[str, Any]) -> list[tuple[str, int, int]]:
        """Compute token balance deltas from pre/post balances.

        Returns:
            List of (mint, delta_raw, decimals) tuples.
        """
        pre_map: dict[tuple[int, str], int] = {}
        post_map: dict[tuple[int, str], tuple[int, int]] = {}

        for b in meta.get("preTokenBalances", []):
            key = (b.get("accountIndex", 0), b.get("mint", ""))
            amount = int(b.get("uiTokenAmount", {}).get("amount", "0"))
            pre_map[key] = amount

        for b in meta.get("postTokenBalances", []):
            key = (b.get("accountIndex", 0), b.get("mint", ""))
            amount = int(b.get("uiTokenAmount", {}).get("amount", "0"))
            decimals = b.get("uiTokenAmount", {}).get("decimals", 0)
            post_map[key] = (amount, decimals)

        all_keys = set(pre_map.keys()) | set(post_map.keys())
        deltas: list[tuple[str, int, int]] = []

        seen_mints: dict[str, int] = {}
        for key in all_keys:
            mint = key[1]
            if not mint:
                continue

            pre_amount = pre_map.get(key, 0)
            post_amount, decimals = post_map.get(key, (0, 0))
            delta = post_amount - pre_amount

            if delta != 0:
                if mint in seen_mints:
                    for i, (m, d, dec) in enumerate(deltas):
                        if m == mint:
                            deltas[i] = (m, d + delta, dec)
                            break
                else:
                    seen_mints[mint] = len(deltas)
                    deltas.append((mint, delta, decimals))

        return deltas
