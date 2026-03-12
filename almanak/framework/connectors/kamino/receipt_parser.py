"""Kamino Finance Lending Receipt Parser.

Extracts supply/borrow/repay/withdraw amounts from Solana transaction
receipts using the balance-delta approach (same pattern as Jupiter).

On Solana, transaction receipts include pre/post token balances, allowing
us to compute exact amounts transferred without parsing program-specific logs.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

# Kamino receipt parser supports these extraction types
SUPPORTED_EXTRACTIONS = frozenset({"supply_amounts", "borrow_amounts", "repay_amounts", "withdraw_amounts"})


@dataclass
class LendingAmounts:
    """Extracted lending operation amounts.

    Attributes:
        token: Token symbol
        amount: Amount in token units (human-readable)
        amount_raw: Amount in smallest units (lamports/etc.)
        action: The lending action (deposit, borrow, repay, withdraw)
    """

    token: str = ""
    amount: Decimal = Decimal("0")
    amount_raw: int = 0
    action: str = ""


class KaminoReceiptParser:
    """Receipt parser for Kamino Finance lending transactions.

    Uses balance-delta approach: compares pre/post token balances
    in the Solana transaction receipt to extract the actual amounts
    deposited, borrowed, repaid, or withdrawn.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize KaminoReceiptParser.

        Args:
            **kwargs: Keyword arguments from receipt_registry (e.g., chain).
        """
        self._chain: str = kwargs.get("chain", "solana")

    def parse_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """Parse a receipt for ReceiptParser protocol compatibility.

        Args:
            receipt: Solana transaction receipt dict

        Returns:
            Dict with parsed lending data
        """
        supply = self.extract_supply_amounts(receipt)
        borrow = self.extract_borrow_amounts(receipt)
        return {
            "supply_amounts": supply,
            "borrow_amounts": borrow,
            "success": receipt.get("success", True),
        }

    def extract_supply_amounts(self, receipt: dict[str, Any]) -> LendingAmounts | None:
        """Extract supply (deposit) amounts from receipt.

        Args:
            receipt: Solana transaction receipt dict

        Returns:
            LendingAmounts if extraction succeeds, None otherwise
        """
        return self._extract_balance_delta(receipt, action="deposit")

    def extract_borrow_amounts(self, receipt: dict[str, Any]) -> LendingAmounts | None:
        """Extract borrow amounts from receipt.

        Args:
            receipt: Solana transaction receipt dict

        Returns:
            LendingAmounts if extraction succeeds, None otherwise
        """
        return self._extract_balance_delta(receipt, action="borrow")

    def extract_repay_amounts(self, receipt: dict[str, Any]) -> LendingAmounts | None:
        """Extract repay amounts from receipt.

        Args:
            receipt: Solana transaction receipt dict

        Returns:
            LendingAmounts if extraction succeeds, None otherwise
        """
        return self._extract_balance_delta(receipt, action="repay")

    def extract_withdraw_amounts(self, receipt: dict[str, Any]) -> LendingAmounts | None:
        """Extract withdraw amounts from receipt.

        Args:
            receipt: Solana transaction receipt dict

        Returns:
            LendingAmounts if extraction succeeds, None otherwise
        """
        return self._extract_balance_delta(receipt, action="withdraw")

    def _extract_balance_delta(
        self,
        receipt: dict[str, Any],
        action: str = "",
    ) -> LendingAmounts | None:
        """Extract amounts by comparing pre/post token balances.

        Solana transactions include preTokenBalances and postTokenBalances
        arrays. By comparing these, we can determine the exact amount
        transferred for any token.

        Args:
            receipt: Solana transaction receipt dict
            action: The lending action for labeling

        Returns:
            LendingAmounts with the largest balance change, or None
        """
        meta = receipt.get("meta", {})
        if not meta:
            logger.debug("No meta in receipt, cannot extract balance delta")
            return None

        pre_balances = meta.get("preTokenBalances", [])
        post_balances = meta.get("postTokenBalances", [])

        if not pre_balances and not post_balances:
            logger.debug("No token balances in receipt")
            return None

        # Build balance maps: (account_index, mint) -> amount
        pre_map: dict[tuple[int, str], int] = {}
        for bal in pre_balances:
            key = (bal.get("accountIndex", 0), bal.get("mint", ""))
            amount_str = bal.get("uiTokenAmount", {}).get("amount", "0")
            pre_map[key] = int(amount_str)

        post_map: dict[tuple[int, str], int] = {}
        for bal in post_balances:
            key = (bal.get("accountIndex", 0), bal.get("mint", ""))
            amount_str = bal.get("uiTokenAmount", {}).get("amount", "0")
            post_map[key] = int(amount_str)

        # Find the largest absolute balance change
        all_keys = set(pre_map.keys()) | set(post_map.keys())
        max_delta = 0
        max_delta_mint = ""
        max_delta_decimals = 0

        for key in all_keys:
            pre_amount = pre_map.get(key, 0)
            post_amount = post_map.get(key, 0)
            delta = abs(post_amount - pre_amount)

            if delta > max_delta:
                max_delta = delta
                max_delta_mint = key[1]
                # Get decimals from post_balances or pre_balances
                for bal_list in (post_balances, pre_balances):
                    for bal in bal_list:
                        if bal.get("mint", "") == max_delta_mint:
                            max_delta_decimals = bal.get("uiTokenAmount", {}).get("decimals", 0)
                            break
                    if max_delta_decimals:
                        break

        if max_delta == 0:
            logger.debug("No balance changes detected in receipt")
            return None

        # Convert raw amount to human-readable
        if max_delta_decimals > 0:
            human_amount = Decimal(max_delta) / Decimal(10**max_delta_decimals)
        else:
            human_amount = Decimal(max_delta)

        return LendingAmounts(
            token=max_delta_mint,
            amount=human_amount,
            amount_raw=max_delta,
            action=action,
        )
