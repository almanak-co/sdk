"""Drift Protocol Receipt Parser.

Parses Solana transaction receipts for Drift perp operations using
balance-delta approach (same pattern as Jupiter/Kamino).

Drift transactions don't emit EVM-style event logs. Instead, we parse:
- Pre/post token balances for collateral changes
- Transaction log messages for fill information
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


class DriftReceiptParser:
    """Receipt parser for Drift Protocol transactions.

    Uses balance-delta approach to extract execution information
    from Solana transaction receipts.
    """

    def parse_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """Parse a Drift transaction receipt.

        Args:
            receipt: Solana transaction receipt dict with 'meta' containing
                     preTokenBalances, postTokenBalances, and logMessages

        Returns:
            Parsed result with extracted data
        """
        result: dict[str, Any] = {
            "protocol": "drift",
            "success": False,
            "events": [],
        }

        meta = receipt.get("meta", {})
        if meta is None:
            return result

        # Check if transaction succeeded
        err = meta.get("err")
        if err is not None:
            result["error"] = str(err)
            return result

        result["success"] = True

        # Extract balance changes
        balance_changes = self._extract_balance_changes(meta)
        if balance_changes:
            result["balance_changes"] = balance_changes

        # Extract fill info from log messages
        fill_info = self._extract_fill_from_logs(meta.get("logMessages", []))
        if fill_info:
            result["fill"] = fill_info
            result["events"].append(fill_info)

        return result

    def extract_perp_fill(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """Extract perpetual fill data from a receipt.

        Parses Drift program logs for fill events that contain
        order execution details.

        Args:
            receipt: Transaction receipt

        Returns:
            Fill data dict or None
        """
        meta = receipt.get("meta", {})
        if meta is None:
            return None

        log_messages = meta.get("logMessages", [])
        return self._extract_fill_from_logs(log_messages)

    def _extract_balance_changes(self, meta: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract token balance changes from pre/post balances.

        Computes deltas between pre and post token balances to determine
        how much collateral was deposited/withdrawn.
        """
        pre_balances = meta.get("preTokenBalances", [])
        post_balances = meta.get("postTokenBalances", [])

        # Index pre-balances by (accountIndex, mint) — use Decimal for precision
        pre_map: dict[tuple[int, str], Decimal] = {}
        for bal in pre_balances:
            key = (bal.get("accountIndex", -1), bal.get("mint", ""))
            amount = bal.get("uiTokenAmount", {}).get("uiAmount", 0) or 0
            pre_map[key] = Decimal(str(amount))

        changes: list[dict[str, Any]] = []
        for bal in post_balances:
            key = (bal.get("accountIndex", -1), bal.get("mint", ""))
            post_amount = Decimal(str(bal.get("uiTokenAmount", {}).get("uiAmount", 0) or 0))
            pre_amount = pre_map.get(key, Decimal("0"))
            delta = post_amount - pre_amount

            if abs(delta) > Decimal("1e-9"):
                changes.append(
                    {
                        "mint": bal.get("mint", ""),
                        "owner": bal.get("owner", ""),
                        "pre_amount": str(pre_amount),
                        "post_amount": str(post_amount),
                        "delta": str(delta),
                        "decimals": bal.get("uiTokenAmount", {}).get("decimals", 0),
                    }
                )

        return changes

    def _extract_fill_from_logs(self, log_messages: list[str]) -> dict[str, Any] | None:
        """Extract fill information from Drift program log messages.

        Drift logs order fill events with messages like:
        "Program log: order_id=X, market_index=Y, fill_price=Z, ..."
        """
        fill_data: dict[str, Any] = {}

        for msg in log_messages:
            # Look for fill-related log messages
            if "fill" in msg.lower() or "order" in msg.lower():
                # Try to extract key-value pairs from log
                pairs = re.findall(r"(\w+)=([^,\s]+)", msg)
                for key, value in pairs:
                    fill_data[key] = value

        if fill_data:
            return {
                "type": "perp_fill",
                "data": fill_data,
            }

        return None
