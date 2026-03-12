"""Jupiter Receipt Parser (Balance-Delta).

This module parses Solana transaction receipts from Jupiter swaps using
the balance-delta approach: compare pre_token_balances and post_token_balances
to determine what was swapped.

Unlike EVM receipt parsers that decode event logs, Solana provides pre/post
token balances directly in the transaction receipt, making extraction simpler.
"""

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.execution.extracted_data import SwapAmounts

logger = logging.getLogger(__name__)

# Native SOL wrapped mint address
WSOL_MINT = "So11111111111111111111111111111111111111112"


class JupiterReceiptParser:
    """Parser for Jupiter swap transaction receipts on Solana.

    Uses balance-delta extraction from pre/post token balances
    rather than event log parsing (as on EVM chains).

    Example:
        parser = JupiterReceiptParser(wallet_address="your-wallet-pubkey")
        receipt = {
            "signature": "...",
            "success": True,
            "pre_token_balances": [...],
            "post_token_balances": [...],
        }
        amounts = parser.extract_swap_amounts(receipt)
        if amounts:
            print(f"Swapped {amounts.amount_in_decimal} -> {amounts.amount_out_decimal}")
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset({"swap_amounts"})

    def __init__(self, **kwargs: Any) -> None:
        """Initialize JupiterReceiptParser.

        Args:
            **kwargs: Keyword arguments passed by the receipt_registry.
                wallet_address: Wallet public key for balance filtering.
                chain: Chain name (always "solana" for Jupiter).
        """
        self._wallet_address: str = kwargs.get("wallet_address", "")
        self._chain: str = kwargs.get("chain", "solana")

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> SwapAmounts | None:
        """Extract swap amounts from a Jupiter transaction receipt.

        Uses balance-delta approach:
        1. Index pre_token_balances by (owner, mint) -> amount
        2. Index post_token_balances by (owner, mint) -> amount
        3. For wallet owner: find mint where balance decreased (token_in)
           and increased (token_out)

        Args:
            receipt: Solana transaction receipt dict with:
                - pre_token_balances: list of token balance entries
                - post_token_balances: list of token balance entries
                - success: bool indicating transaction success
                - fee_payer or wallet_address: the wallet to track

        Returns:
            SwapAmounts if swap transfers found, None otherwise
        """
        if not receipt.get("success", True):
            return None

        wallet = self._resolve_wallet(receipt)
        if not wallet:
            logger.warning("No wallet address available for balance-delta extraction")
            return None

        pre_balances = receipt.get("pre_token_balances", [])
        post_balances = receipt.get("post_token_balances", [])

        if not pre_balances and not post_balances:
            return None

        # Build balance maps: (owner, mint) -> amount
        pre_map = self._build_balance_map(pre_balances)
        post_map = self._build_balance_map(post_balances)

        # Collect all (owner, mint) pairs
        all_keys = set(pre_map.keys()) | set(post_map.keys())

        # Find deltas for the wallet
        token_in_mint = None
        token_in_delta = 0
        token_out_mint = None
        token_out_delta = 0

        for owner, mint in all_keys:
            if owner != wallet:
                continue

            pre_amount = pre_map.get((owner, mint), 0)
            post_amount = post_map.get((owner, mint), 0)
            delta = post_amount - pre_amount

            if delta < 0 and abs(delta) > abs(token_in_delta):
                # Balance decreased — this is the input token
                token_in_mint = mint
                token_in_delta = delta
            elif delta > 0 and delta > token_out_delta:
                # Balance increased — this is the output token
                token_out_mint = mint
                token_out_delta = delta

        if token_out_mint is None or token_out_delta == 0:
            return None

        amount_in_raw = abs(token_in_delta) if token_in_delta else 0
        amount_out_raw = token_out_delta

        # Resolve decimals for human-readable amounts
        decimals_in = self._resolve_decimals(token_in_mint)
        decimals_out = self._resolve_decimals(token_out_mint)

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
            token_in=token_in_mint,
            token_out=token_out_mint,
        )

    def parse_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """Parse a receipt for ReceiptParser protocol compatibility.

        Args:
            receipt: Transaction receipt dict

        Returns:
            Dict with parsed data
        """
        swap_amounts = self.extract_swap_amounts(receipt)
        return {
            "swap_amounts": swap_amounts,
            "success": receipt.get("success", True),
            "signature": receipt.get("signature", ""),
        }

    def _resolve_wallet(self, receipt: dict[str, Any]) -> str:
        """Resolve the wallet address from receipt or instance config.

        Args:
            receipt: Transaction receipt

        Returns:
            Wallet public key string, or empty string if not found
        """
        # Try receipt-level wallet fields
        wallet = receipt.get("fee_payer", "") or receipt.get("wallet_address", "")
        if wallet:
            return wallet
        # Fall back to instance config
        return self._wallet_address

    @staticmethod
    def _build_balance_map(balances: list[dict[str, Any]]) -> dict[tuple[str, str], int]:
        """Build a (owner, mint) -> amount map from token balance entries.

        Handles both Solana RPC format and our simplified format:
        - RPC format: {"owner": "...", "mint": "...", "uiTokenAmount": {"amount": "..."}}
        - Simplified: {"owner": "...", "mint": "...", "amount": "..."}

        Args:
            balances: List of token balance dicts

        Returns:
            Dict mapping (owner, mint) to raw token amount
        """
        result: dict[tuple[str, str], int] = {}
        for entry in balances:
            owner = entry.get("owner", "")
            mint = entry.get("mint", "")
            if not owner or not mint:
                continue

            # Try uiTokenAmount.amount (Solana RPC format) first, then flat "amount"
            ui_amount = entry.get("uiTokenAmount")
            if isinstance(ui_amount, dict) and "amount" in ui_amount:
                amount_str = ui_amount["amount"]
            else:
                amount_str = entry.get("amount", "0")

            try:
                result[(owner, mint)] = int(amount_str)
            except (ValueError, TypeError):
                continue

        return result

    def _resolve_decimals(self, mint: str | None) -> int | None:
        """Resolve token decimals via the token resolver.

        Args:
            mint: Solana token mint address

        Returns:
            Token decimals, or None if unknown
        """
        if not mint:
            return None
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            token = resolver.resolve(mint, self._chain)
            return token.decimals
        except Exception:
            logger.warning(f"Could not resolve decimals for {mint}, swap amounts may be incomplete")
            return None


__all__ = ["JupiterReceiptParser", "WSOL_MINT"]
