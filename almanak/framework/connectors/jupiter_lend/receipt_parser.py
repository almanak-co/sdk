"""Jupiter Lend Receipt Parser.

Extracts supply/borrow/repay/withdraw amounts from Solana transaction
receipts using the balance-delta approach (same pattern as Kamino).

On Solana, transaction receipts include pre/post token balances, allowing
us to compute exact amounts transferred without parsing program-specific logs.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

# Jupiter Lend receipt parser supports these extraction types
SUPPORTED_EXTRACTIONS = frozenset({"supply_amounts", "borrow_amounts", "repay_amounts", "withdraw_amounts"})


@dataclass
class LendingAmounts:
    """Extracted lending operation amounts.

    Attributes:
        token: Token mint address
        amount: Amount in token units (human-readable)
        amount_raw: Amount in smallest units (lamports/etc.)
        action: The lending action (deposit, borrow, repay, withdraw)
    """

    token: str = ""
    amount: Decimal = Decimal("0")
    amount_raw: int = 0
    action: str = ""


class JupiterLendReceiptParser:
    """Receipt parser for Jupiter Lend transactions.

    Uses balance-delta approach: compares pre/post token balances
    in the Solana transaction receipt to extract the actual amounts
    deposited, borrowed, repaid, or withdrawn.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize JupiterLendReceiptParser.

        Args:
            **kwargs: Keyword arguments from receipt_registry (e.g., chain).
        """
        self._chain: str = kwargs.get("chain", "solana")

    def parse_receipt(self, receipt: dict[str, Any], action: str = "") -> dict[str, Any]:
        """Parse a receipt for ReceiptParser protocol compatibility.

        Args:
            receipt: Solana transaction receipt dict
            action: The lending action (deposit, borrow, repay, withdraw).
                    If provided, only that extraction is performed.

        Returns:
            Dict with parsed lending data
        """
        meta = receipt.get("meta") or {}
        success = receipt.get("success")
        if success is None:
            success = meta.get("err") is None

        result: dict[str, Any] = {"success": success}
        if action in ("deposit", "supply"):
            result["supply_amounts"] = self.extract_supply_amounts(receipt)
        if action == "borrow":
            result["borrow_amounts"] = self.extract_borrow_amounts(receipt)
        if action == "repay":
            result["repay_amounts"] = self.extract_repay_amounts(receipt)
        if action == "withdraw":
            result["withdraw_amounts"] = self.extract_withdraw_amounts(receipt)
        return result

    def extract_supply_amounts(self, receipt: dict[str, Any]) -> LendingAmounts | None:
        """Extract supply (deposit) amounts from receipt."""
        return self._extract_balance_delta(receipt, action="deposit")

    def extract_borrow_amounts(self, receipt: dict[str, Any]) -> LendingAmounts | None:
        """Extract borrow amounts from receipt."""
        return self._extract_balance_delta(receipt, action="borrow")

    def extract_repay_amounts(self, receipt: dict[str, Any]) -> LendingAmounts | None:
        """Extract repay amounts from receipt."""
        return self._extract_balance_delta(receipt, action="repay")

    def extract_withdraw_amounts(self, receipt: dict[str, Any]) -> LendingAmounts | None:
        """Extract withdraw amounts from receipt."""
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
        # Support both raw RPC receipts (meta.preTokenBalances) and
        # TransactionReceipt.to_dict() format (top-level pre_token_balances)
        meta = receipt.get("meta") or {}
        pre_balances = meta.get("preTokenBalances", []) or receipt.get("pre_token_balances", [])
        post_balances = meta.get("postTokenBalances", []) or receipt.get("post_token_balances", [])

        if not pre_balances and not post_balances:
            logger.debug("No token balances in receipt")
            return None

        # Build balance maps: (account_index, mint) -> amount
        pre_map: dict[tuple[int, str], int] = {}
        for bal in pre_balances:
            key = (bal.get("accountIndex", bal.get("account_index", 0)), bal.get("mint", ""))
            ui_amount = bal.get("uiTokenAmount", bal.get("ui_token_amount", {}))
            amount_str = ui_amount.get("amount", "0") if isinstance(ui_amount, dict) else "0"
            pre_map[key] = int(amount_str)

        post_map: dict[tuple[int, str], int] = {}
        for bal in post_balances:
            key = (bal.get("accountIndex", bal.get("account_index", 0)), bal.get("mint", ""))
            ui_amount = bal.get("uiTokenAmount", bal.get("ui_token_amount", {}))
            amount_str = ui_amount.get("amount", "0") if isinstance(ui_amount, dict) else "0"
            post_map[key] = int(amount_str)

        # Build decimals lookup: mint -> decimals
        decimals_map: dict[str, int] = {}
        for bal_list in (post_balances, pre_balances):
            for bal in bal_list:
                mint = bal.get("mint", "")
                if mint and mint not in decimals_map:
                    ui_amt = bal.get("uiTokenAmount", bal.get("ui_token_amount", {}))
                    if isinstance(ui_amt, dict):
                        decimals_map[mint] = ui_amt.get("decimals", 0)

        # Find the largest normalized (decimal-adjusted) balance change
        # Using normalized amounts avoids the raw-units bias where e.g.
        # 1 SOL (1e9 raw) would incorrectly "win" over 100 USDC (1e8 raw)
        all_keys = set(pre_map.keys()) | set(post_map.keys())
        max_normalized_delta = Decimal(0)
        max_delta = 0
        max_delta_mint = ""
        max_delta_decimals = 0

        for key in all_keys:
            pre_amount = pre_map.get(key, 0)
            post_amount = post_map.get(key, 0)
            delta = abs(post_amount - pre_amount)
            if delta == 0:
                continue

            mint = key[1]
            token_decimals = decimals_map.get(mint, 0)
            normalized = Decimal(delta) / Decimal(10**token_decimals) if token_decimals > 0 else Decimal(delta)

            if normalized > max_normalized_delta:
                max_normalized_delta = normalized
                max_delta = delta
                max_delta_mint = mint
                max_delta_decimals = token_decimals

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
