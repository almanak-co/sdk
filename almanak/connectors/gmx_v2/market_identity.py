"""Pure GMX market-identifier normalisation."""

from __future__ import annotations

from almanak.core.perp_markets import perp_market_pair_key


def canonicalise_market(market: str) -> str:
    """Canonicalise venue labels while preserving raw address bytes/case."""
    candidate = market.strip()
    if candidate.lower().startswith("0x"):
        return candidate
    return perp_market_pair_key(candidate) or candidate.upper()


__all__ = ["canonicalise_market"]
