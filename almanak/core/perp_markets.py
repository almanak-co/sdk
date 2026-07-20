"""Canonical perp market-symbol parsing — the single canonicalization seam.

Perp market identifiers arrive in several spellings depending on the venue and
the author: ``"ETH/USD"`` (the SDK's documented GMX examples), ``"ETH-USD"``
(venue funding tables), ``"SOL-PERP"`` (Drift), ``"ETH_USD"``/``"ETH:USD"``
(seen in duck-typed intents), and bare coins (``"ETH"``, Hyperliquid).

Historically each lane parsed its own form: the backtest engine split on a
separator list, the gateway funding service split on ``"-"`` only, and the
connector funding tables were keyed by the dash form — so ``"ETH/USD"`` priced
a hedge at a $1 fallback in one lane (campaign-50 s42) and missed every
funding table in another (s38) while ``"ETH-USD"`` worked end to end.

Every consumer now normalizes through these two functions:

- :func:`perp_market_base` — the base asset symbol, for pricing lanes.
- :func:`perp_market_funding_key` — the canonical ``"<BASE>-USD"`` venue form,
  for funding-rate tables, gateway requests, and cache keys.

This module is intentionally pure stdlib (no framework/gateway imports) so the
backtest engine, the gateway servicers, and connector gateway providers can all
import it without cycles and without violating the gateway lean-import ratchet.
"""

from __future__ import annotations

#: Separators seen in perp market identifiers: "ETH/USD" (GMX docs), "ETH-USD",
#: "SOL-PERP" (Drift), "ETH_USD", "ETH:USD"; bare symbols ("ETH", Hyperliquid)
#: have no separator. The FIRST separator present wins, matching the engine's
#: historical parse.
PERP_MARKET_SEPARATORS: tuple[str, ...] = ("/", "-", ":", "_")


def perp_market_base(market: object) -> str | None:
    """Parse the base asset symbol from a perp market identifier.

    ``"ETH/USD"`` / ``"ETH-USD"`` / ``"ETH_USD"`` / ``"ETH:USD"`` / ``"ETH"``
    → ``"ETH"``; ``"SOL-PERP"`` → ``"SOL"``.

    Returns None for non-strings, empty strings, and address-style
    identifiers (0x...), which cannot be mapped to a priceable symbol
    without chain data.
    """
    if not isinstance(market, str):
        return None
    candidate = market.strip()
    if not candidate or candidate.lower().startswith("0x"):
        return None
    for separator in PERP_MARKET_SEPARATORS:
        if separator in candidate:
            candidate = candidate.split(separator)[0].strip()
            break
    if not candidate:
        return None
    return candidate.upper()


def perp_market_funding_key(market: object) -> str | None:
    """Canonical ``"<BASE>-USD"`` venue form for funding-rate lookups.

    ``"ETH/USD"`` / ``"ETH-USD"`` / ``"ETH"`` → ``"ETH-USD"``. Returns None
    when no base symbol is resolvable (see :func:`perp_market_base`).
    """
    base = perp_market_base(market)
    if base is None:
        return None
    return f"{base}-USD"


__all__ = [
    "PERP_MARKET_SEPARATORS",
    "perp_market_base",
    "perp_market_funding_key",
]
