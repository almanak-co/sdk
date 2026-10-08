"""Exact ERC-20 payout evidence; a successful transaction alone is insufficient."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _hex(value: Any) -> str:
    if isinstance(value, bytes):
        return "0x" + value.hex()
    return str(value).lower()


def payout_log(receipt: Any, *, token: str, receiver: str, net: Decimal, decimals: int) -> dict[str, Any]:
    raw = net * Decimal(10) ** decimals
    if not net.is_finite() or net <= 0 or raw != raw.to_integral_value():
        raise ValueError("payout net amount is not a positive exact token quantity")
    if int(receipt.get("status", 0)) != 1:
        raise ValueError("payout transaction did not succeed")
    matches = []
    for log in receipt.get("logs", []):
        topics = log.get("topics", [])
        if str(log.get("address", "")).lower() != token.lower() or len(topics) != 3:
            continue
        if _hex(topics[0]) != _TRANSFER or _hex(topics[2]) != "0x" + receiver.lower().removeprefix("0x").zfill(64):
            continue
        if int(_hex(log.get("data")), 16) == int(raw):
            matches.append(log)
    if len(matches) != 1:
        raise ValueError("payout receipt lacks one unique transfer of the exact token, receiver and net amount")
    log = matches[0]
    return {
        "log_index": int(log["logIndex"]),
        "block_number": int(receipt["blockNumber"]),
        "raw_amount": str(int(raw)),
    }
