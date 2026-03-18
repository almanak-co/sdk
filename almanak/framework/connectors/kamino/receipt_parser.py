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
        repay = self.extract_repay_amounts(receipt)
        withdraw = self.extract_withdraw_amounts(receipt)
        return {
            "supply_amounts": supply,
            "borrow_amounts": borrow,
            "repay_amounts": repay,
            "withdraw_amounts": withdraw,
            "success": receipt.get("success", True),
        }

    def extract_supply_amounts(self, receipt: dict[str, Any], token_mint: str | None = None) -> LendingAmounts | None:
        """Extract supply (deposit) amounts from receipt.

        Supply = tokens leaving the wallet (pre > post).
        """
        return self._extract_balance_delta(receipt, action="deposit", direction="outflow", token_mint=token_mint)

    def extract_borrow_amounts(self, receipt: dict[str, Any], token_mint: str | None = None) -> LendingAmounts | None:
        """Extract borrow amounts from receipt.

        Borrow = tokens arriving at the wallet (post > pre).
        """
        return self._extract_balance_delta(receipt, action="borrow", direction="inflow", token_mint=token_mint)

    def extract_repay_amounts(self, receipt: dict[str, Any], token_mint: str | None = None) -> LendingAmounts | None:
        """Extract repay amounts from receipt.

        Repay = tokens leaving the wallet (pre > post).
        """
        return self._extract_balance_delta(receipt, action="repay", direction="outflow", token_mint=token_mint)

    def extract_withdraw_amounts(self, receipt: dict[str, Any], token_mint: str | None = None) -> LendingAmounts | None:
        """Extract withdraw amounts from receipt.

        Withdraw = tokens arriving at the wallet (post > pre).
        """
        return self._extract_balance_delta(receipt, action="withdraw", direction="inflow", token_mint=token_mint)

    def _extract_balance_delta(
        self,
        receipt: dict[str, Any],
        action: str = "",
        direction: str = "",
        token_mint: str | None = None,
    ) -> LendingAmounts | None:
        """Extract amounts by comparing pre/post token balances.

        Solana transactions include preTokenBalances and postTokenBalances
        arrays. By comparing these, we can determine the exact amount
        transferred for any token.

        Args:
            receipt: Solana transaction receipt dict
            action: The lending action for labeling
            direction: "inflow" for tokens arriving (post > pre),
                       "outflow" for tokens leaving (pre > post),
                       "" for largest absolute delta (legacy fallback)
            token_mint: Optional specific token mint to extract delta for

        Returns:
            LendingAmounts with the largest directional balance change, or None
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

        # Build decimals lookup: mint -> decimals
        decimals_map: dict[str, int] = {}
        for bal_list in (post_balances, pre_balances):
            for bal in bal_list:
                mint = bal.get("mint", "")
                if mint and mint not in decimals_map:
                    decimals_map[mint] = bal.get("uiTokenAmount", {}).get("decimals", 0)

        # Find the largest normalized balance change filtered by direction
        all_keys = set(pre_map.keys()) | set(post_map.keys())
        max_normalized_delta = Decimal(0)
        max_delta = 0
        max_delta_mint = ""
        max_delta_decimals = 0

        for key in all_keys:
            mint = key[1]

            # Filter by specific token mint if requested
            if token_mint and mint != token_mint:
                continue

            pre_amount = pre_map.get(key, 0)
            post_amount = post_map.get(key, 0)
            signed_delta = post_amount - pre_amount

            # Filter by direction
            if direction == "inflow" and signed_delta <= 0:
                continue
            if direction == "outflow" and signed_delta >= 0:
                continue

            delta = abs(signed_delta)
            if delta == 0:
                continue

            token_decimals = decimals_map.get(mint, 0)
            normalized = Decimal(delta) / Decimal(10**token_decimals) if token_decimals > 0 else Decimal(delta)

            if normalized > max_normalized_delta:
                max_normalized_delta = normalized
                max_delta = delta
                max_delta_mint = mint
                max_delta_decimals = token_decimals

        if max_delta == 0:
            logger.debug("No balance changes detected in receipt (direction=%s)", direction)
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
