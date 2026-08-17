"""Backward-compatible import location for market-label normalisation.

ALM-3199 removed the hand-maintained collateral rule table. Runtime collateral
validity is derived from each gateway/on-chain-verified market record; offline
permission compilation uses the bounded generated seed. New code should import
``canonicalise_market`` from :mod:`market_identity` directly.
"""

from .market_identity import canonicalise_market

__all__ = ["canonicalise_market"]
