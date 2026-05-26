"""Raydium CLMM receipt parser.

Extracts LP operation results from Solana transaction receipts.
Uses the balance-delta approach (same as Jupiter/Kamino) plus
log parsing for position NFT mints and liquidity events.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class RaydiumLPOpenResult:
    """Result from an LP open operation."""

    position_nft_mint: str
    pool_address: str
    amount_a: int
    amount_b: int
    amount_a_decimal: Decimal
    amount_b_decimal: Decimal
    liquidity: int
    tick_lower: int
    tick_upper: int


@dataclass
class RaydiumLPCloseResult:
    """Result from an LP close operation."""

    position_nft_mint: str
    amount_a_received: int
    amount_b_received: int
    amount_a_decimal: Decimal
    amount_b_decimal: Decimal
    fees_a: int
    fees_b: int


class RaydiumReceiptParser:
    """Parser for Raydium CLMM transaction receipts.

    Extracts position IDs, liquidity amounts, and token balances
    from Solana transaction receipts.

    Supports the extraction methods required by ResultEnricher:
    - extract_position_id(receipt) -> str | None
    - extract_liquidity(receipt) -> dict | None
    - extract_lp_close_data(receipt) -> dict | None

    Extraction approach:
    1. Parse log messages for Raydium program events
    2. Use preTokenBalances/postTokenBalances for actual amounts
    3. Look for NFT mint in innerInstructions
    """

    SUPPORTED_EXTRACTIONS = frozenset({"position_id", "liquidity", "lp_close_data"})

    def __init__(self, **kwargs: Any) -> None:
        """Initialize RaydiumReceiptParser.

        Args:
            **kwargs: Keyword arguments from receipt_registry (e.g., chain).
        """
        self._chain: str = kwargs.get("chain", "solana")

    def parse_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """Parse a receipt for ReceiptParser protocol compatibility.

        Args:
            receipt: Solana transaction receipt dict

        Returns:
            Dict with parsed LP data
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
        """Extract the position NFT mint from a Raydium LP open receipt.

        The position NFT is minted during openPosition. We find it by
        looking for a new token account with amount=1 in postTokenBalances
        that wasn't in preTokenBalances.

        Args:
            receipt: Solana transaction receipt dict.

        Returns:
            Position NFT mint address (Base58), or None if not found.
        """
        meta = receipt.get("meta", {})
        if not meta:
            return None

        pre_mints = {b.get("mint") for b in meta.get("preTokenBalances", []) if b.get("mint")}

        post_balances = meta.get("postTokenBalances", [])
        for balance in post_balances:
            mint = balance.get("mint", "")
            if mint and mint not in pre_mints:
                # New mint appeared — check if it's an NFT (amount = 1)
                amount = balance.get("uiTokenAmount", {}).get("amount", "0")
                if amount == "1":
                    logger.info(f"Found position NFT mint: {mint[:8]}...")
                    return mint

        # Fallback: parse logs for NFT mint
        return self._extract_nft_from_logs(meta)

    def extract_liquidity(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """Extract liquidity data from an LP open/increase receipt.

        Uses balance deltas to determine actual deposited amounts.

        Args:
            receipt: Solana transaction receipt dict.

        Returns:
            Dict with amount_a, amount_b, position_nft_mint, or None.
        """
        meta = receipt.get("meta", {})
        if not meta:
            return None

        deltas = self._compute_balance_deltas(meta)
        if not deltas:
            return None

        # The two largest negative deltas are the deposited tokens
        # (negative = user sent tokens to pool)
        negative_deltas = [(mint, delta, dec) for mint, delta, dec in deltas if delta < 0]
        negative_deltas.sort(key=lambda x: x[1])  # Most negative first

        result: dict[str, Any] = {}
        if len(negative_deltas) >= 2:
            result["token_a_mint"] = negative_deltas[0][0]
            result["amount_a_raw"] = abs(negative_deltas[0][1])
            result["amount_a"] = str(Decimal(abs(negative_deltas[0][1])) / Decimal(10) ** negative_deltas[0][2])
            result["token_b_mint"] = negative_deltas[1][0]
            result["amount_b_raw"] = abs(negative_deltas[1][1])
            result["amount_b"] = str(Decimal(abs(negative_deltas[1][1])) / Decimal(10) ** negative_deltas[1][2])
        elif len(negative_deltas) == 1:
            # Single-sided deposit
            result["token_a_mint"] = negative_deltas[0][0]
            result["amount_a_raw"] = abs(negative_deltas[0][1])
            result["amount_a"] = str(Decimal(abs(negative_deltas[0][1])) / Decimal(10) ** negative_deltas[0][2])

        # Include position NFT if found
        position_id = self.extract_position_id(receipt)
        if position_id:
            result["position_nft_mint"] = position_id

        return result if result else None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """Extract LP close data from a receipt.

        Uses balance deltas to determine received amounts.

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
        positive_deltas.sort(key=lambda x: x[1], reverse=True)  # Largest first

        result: dict[str, Any] = {}
        if len(positive_deltas) >= 2:
            result["token_a_mint"] = positive_deltas[0][0]
            result["amount_a_received_raw"] = positive_deltas[0][1]
            result["amount_a_received"] = str(Decimal(positive_deltas[0][1]) / Decimal(10) ** positive_deltas[0][2])
            result["token_b_mint"] = positive_deltas[1][0]
            result["amount_b_received_raw"] = positive_deltas[1][1]
            result["amount_b_received"] = str(Decimal(positive_deltas[1][1]) / Decimal(10) ** positive_deltas[1][2])
        elif len(positive_deltas) == 1:
            result["token_a_mint"] = positive_deltas[0][0]
            result["amount_a_received_raw"] = positive_deltas[0][1]
            result["amount_a_received"] = str(Decimal(positive_deltas[0][1]) / Decimal(10) ** positive_deltas[0][2])

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

        # Compute deltas for all keys
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
                # Accumulate deltas per mint (user may have multiple token accounts)
                if mint in seen_mints:
                    # Update existing delta
                    for i, (m, d, dec) in enumerate(deltas):
                        if m == mint:
                            deltas[i] = (m, d + delta, dec)
                            break
                else:
                    seen_mints[mint] = len(deltas)
                    deltas.append((mint, delta, decimals))

        return deltas

    def _extract_nft_from_logs(self, meta: dict[str, Any]) -> str | None:
        """Try to extract NFT mint from transaction log messages.

        Looks for MintTo or InitializeMint log entries.
        """
        log_messages = meta.get("logMessages", [])
        for msg in log_messages:
            # Look for Metaplex metadata creation log
            if "Create" in msg and "Metadata" in msg:
                # Try to extract mint from the message
                match = re.search(r"mint:\s*(\w{32,44})", msg)
                if match:
                    return match.group(1)
        return None
