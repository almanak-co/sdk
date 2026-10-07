"""Market-name and order-identity helpers for Aster Pro (pure, strategy-safe)."""

from __future__ import annotations

import re

# Aster Pro USDT-margined perps settle in USDT; it is the only margin asset the
# connector accepts.
MARGIN_ASSET = "USDT"

_BASE_PATTERN = re.compile(r"^[A-Z0-9]{2,15}$")
# Longest first so "USDT" is stripped before "USD".
_QUOTES = ("USDT", "USD")
_CLIENT_ORDER_ID_PATTERN = re.compile(r"^[.A-Z:/a-z0-9_-]{1,36}$")


def to_symbol(market: str) -> str:
    """Map an intent market (``ETH/USD``, ``ETH-USDT``, ``ETHUSDT``) to an Aster symbol."""
    normalized = market.strip().upper()
    parts = re.split(r"[/\-_]", normalized)
    if len(parts) == 2 and parts[1] in _QUOTES:
        base = parts[0]
    elif len(parts) == 1:
        base = next((normalized[: -len(q)] for q in _QUOTES if normalized.endswith(q)), normalized)
    else:
        base = ""
    if not _BASE_PATTERN.match(base):
        raise ValueError(f"Unrecognised Aster market {market!r}; use BASE/USD, e.g. 'ETH/USD'")
    return f"{base}{MARGIN_ASSET}"


def client_order_id(intent_id: str, *, leg: str) -> str:
    """Deterministic Aster ``newClientOrderId`` for one intent.

    Deterministic so a submission whose outcome is unknown can be reconciled by
    id instead of resubmitted. Aster caps the id at 36 chars of ``[.A-Za-z0-9:/_-]``.
    """
    compact = re.sub(r"[^A-Za-z0-9]", "", intent_id)
    if not compact:
        raise ValueError("intent_id must contain alphanumeric characters")
    value = f"alm{leg[0].lower()}{compact}"[:36]
    if not _CLIENT_ORDER_ID_PATTERN.match(value):
        raise ValueError(f"Cannot derive an Aster client order id from intent_id {intent_id!r}")
    return value


__all__ = ["MARGIN_ASSET", "client_order_id", "to_symbol"]
