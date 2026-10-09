"""Market-name and order-identity helpers for Aster Pro (pure, strategy-safe)."""

from __future__ import annotations

import re
from decimal import ROUND_CEILING, ROUND_DOWN, Decimal

# Aster Pro USDT-margined perps settle in USDT; it is the only margin asset the
# connector accepts.
MARGIN_ASSET = "USDT"

_BASE_PATTERN = re.compile(r"^[A-Z0-9]{2,15}$")
# Longest first so "USDT" is stripped before "USD".
_QUOTES = ("USDT", "USD")
_CLIENT_ORDER_ID_PATTERN = re.compile(r"^[.A-Z:/a-z0-9_-]{1,36}$")

# Markets served on the gateway funding lanes, in their ``BASE-USD`` key form.
FUNDING_MARKETS = ("BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD")


# Aster ETHUSDT order rules: quantity step and minimum order notional (USD). The venue
# rounds an order's quantity DOWN to the step and refuses it below the minimum.
ETH_QTY_STEP = Decimal("0.001")
MIN_ORDER_NOTIONAL_USD = Decimal("5")
# Clearance over the minimum, so a small move between the strategy's price read
# and the venue's mark at submission does not push the order under it.
_MIN_NOTIONAL_CLEARANCE = Decimal("1.02")
# Share of deposit x leverage an order may use, leaving room for the taker fee and
# an adverse move at open before the venue's initial-margin check binds.
MARGIN_USE_CAP = Decimal("0.9")


def venue_order_size(
    target_usd: Decimal,
    price: Decimal,
    *,
    max_notional_usd: Decimal,
    step: Decimal = ETH_QTY_STEP,
    min_notional_usd: Decimal = MIN_ORDER_NOTIONAL_USD,
) -> tuple[Decimal, Decimal]:
    """The order quantity for ``target_usd`` and the USD notional to send for it.

    The quantity is the target rounded down to the step (never above what was
    asked) unless that falls under the venue minimum, in which case it steps up
    only as far as the minimum requires. The notional sent is ``(quantity + half
    a step) * price``, capped at ``max_notional_usd`` (what the margin can carry),
    so the venue's own round-down lands on that quantity unless its mark moves
    by more than the headroom. Raises ``ValueError`` when the margin cannot carry
    the quantity at all.
    """
    if not all(v.is_finite() and v > 0 for v in (target_usd, price, step)):
        raise ValueError(
            f"order sizing needs positive finite inputs, got target={target_usd} price={price} step={step}"
        )
    floor_steps = (target_usd / price / step).to_integral_value(rounding=ROUND_DOWN)
    min_steps = (min_notional_usd * _MIN_NOTIONAL_CLEARANCE / price / step).to_integral_value(rounding=ROUND_CEILING)
    quantity = max(floor_steps, min_steps) * step
    cost = (quantity * price).quantize(Decimal("0.01"), rounding=ROUND_CEILING)
    if cost > max_notional_usd:
        raise ValueError(
            f"the smallest valid order ({quantity} at ~{price}, ${cost}) exceeds what the margin carries "
            f"(${max_notional_usd})"
        )
    notional = min(((quantity + step / 2) * price).quantize(Decimal("0.01"), rounding=ROUND_DOWN), max_notional_usd)
    return quantity, max(notional, cost)


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


__all__ = [
    "ETH_QTY_STEP",
    "FUNDING_MARKETS",
    "MARGIN_ASSET",
    "MARGIN_USE_CAP",
    "MIN_ORDER_NOTIONAL_USD",
    "client_order_id",
    "to_symbol",
    "venue_order_size",
]
