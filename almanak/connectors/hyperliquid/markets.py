"""Perp market resolution for Hyperliquid: symbol → (asset index, szDecimals).

A CoreWriter order encodes a perp by its **asset index** (a ``uint32`` — BTC is
0, ETH is 1, …) and needs the asset's **szDecimals** to round price/size to a
HyperCore-valid tick. A strategy names a market as a symbol (``"BTC"``,
``"BTC-USD"``), so something must map symbol → ``(index, szDecimals)``.

There is no pure on-chain symbol→index lookup: the ``perpAssetInfo(index)``
precompile answers "what is asset #7?", not "what index is BTC?". Resolution
therefore comes from Hyperliquid's perp *universe* (an ordered list from the
API). This module holds a **static seed** of that universe for the liquid
majors — verified live against ``api.hyperliquid.xyz`` (``type=meta``) on
2026-07-01 — and fails closed on any symbol it does not know.

Indices are **append-only** on Hyperliquid (a listed asset's index never
changes; new listings only get new indices), so a seeded major cannot silently
become the wrong asset. The seed's only staleness surface is coverage (new
listings absent) and the rare ``szDecimals`` change on a relist.

This is deliberately a **seam**: :func:`resolve_market` accepts an optional
``universe`` override so a future gateway-backed "perp universe" capability
(full ~230-market coverage, always current) can be layered in front of the seed
without touching the compiler — "try dynamic, fall back to seed".
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PerpMarket:
    """A resolved Hyperliquid perp market.

    Attributes:
        symbol: Canonical base symbol (e.g. ``"BTC"``).
        asset_index: HyperCore perp index (the ``uint32`` a CoreWriter order encodes).
        sz_decimals: Size decimal places — drives price/size tick rounding.
        max_leverage: Max leverage the venue allows for this asset (advisory).
    """

    symbol: str
    asset_index: int
    sz_decimals: int
    max_leverage: int


# Static seed of the liquid majors. Values verified live (index, szDecimals,
# maxLeverage) — NOT guessed. Note SOL is index 5 (not 2 — index 2 is ATOM); a
# from-memory seed like V1's ``SOL: 2`` would trade the wrong asset. Delisted
# markets (e.g. MATIC, index 3) are deliberately excluded.
_SEED_MARKETS: tuple[PerpMarket, ...] = (
    PerpMarket("BTC", 0, 5, 40),
    PerpMarket("ETH", 1, 4, 25),
    PerpMarket("ATOM", 2, 2, 5),
    PerpMarket("SOL", 5, 2, 20),
    PerpMarket("AVAX", 6, 2, 10),
    PerpMarket("BNB", 7, 3, 10),
    PerpMarket("OP", 9, 1, 5),
    PerpMarket("LTC", 10, 2, 10),
    PerpMarket("ARB", 11, 1, 10),
    PerpMarket("DOGE", 12, 0, 10),
    PerpMarket("INJ", 13, 1, 5),
    PerpMarket("SUI", 14, 1, 10),
    PerpMarket("CRV", 16, 1, 10),
    PerpMarket("LINK", 18, 1, 10),
    PerpMarket("XRP", 25, 0, 20),
    PerpMarket("APT", 27, 2, 10),
    PerpMarket("AAVE", 28, 2, 10),
    PerpMarket("WLD", 31, 1, 10),
    PerpMarket("SEI", 40, 0, 5),
    PerpMarket("TIA", 63, 1, 5),
    PerpMarket("ADA", 65, 0, 10),
    PerpMarket("PENDLE", 70, 0, 5),
    PerpMarket("NEAR", 74, 1, 10),
    PerpMarket("HYPE", 159, 2, 10),
)

_SEED_BY_SYMBOL: dict[str, PerpMarket] = {m.symbol: m for m in _SEED_MARKETS}

# Quote suffixes stripped during symbol normalisation (Hyperliquid perps are all
# USD-margined, so "BTC", "BTC-USD", "BTC/USD", "BTC-PERP" all mean asset BTC).
_QUOTE_SUFFIXES: tuple[str, ...] = ("-USD", "/USD", "-USDC", "/USDC", "-PERP", "/PERP", "-USDT", "/USDT")


def normalize_symbol(market: str) -> str:
    """Normalise a market string to a bare base symbol.

    ``"btc-usd"`` / ``"BTC/USD"`` / ``"BTC-PERP"`` / ``"BTC"`` → ``"BTC"``.
    Preserves Hyperliquid's ``k``-prefixed thousands symbols (``kPEPE``) by only
    upper-casing when no exact-case seed entry exists.
    """
    if not isinstance(market, str) or not market.strip():
        raise ValueError(f"market must be a non-empty string, got {market!r}")
    s = market.strip()
    up = s.upper()
    for suffix in _QUOTE_SUFFIXES:
        if up.endswith(suffix):
            s = s[: -len(suffix)]
            up = s.upper()
            break
    # Preserve exact case when it matches a known seed entry — Hyperliquid uses
    # case-sensitive k-prefixed thousands symbols (kPEPE, kBONK); upper-casing
    # kPEPE→KPEPE would never match a case-sensitive universe/seed entry. Honours
    # the documented contract; falls back to upper-case otherwise.
    return s if s in _SEED_BY_SYMBOL else up


def resolve_market(market: str, *, universe: dict[str, PerpMarket] | None = None) -> PerpMarket:
    """Resolve a market symbol to a :class:`PerpMarket`, fail-closed on unknown.

    Args:
        market: A market symbol (``"BTC"``, ``"BTC-USD"``, …). Resolution is by
            normalized symbol against the dynamic universe (when supplied) or the
            static seed; bare index strings are not parsed (fail-closed on unknown).
        universe: Optional symbol→:class:`PerpMarket` map from a dynamic source
            (the future gateway universe capability). Tried before the static
            seed; the seed is the fallback. When ``None`` (today), only the seed
            is consulted.

    Raises:
        ValueError: If the symbol is not resolvable — the connector never guesses
            an index (trading the wrong asset is the failure mode this prevents).
    """
    symbol = normalize_symbol(market)

    # Dynamic source first (future gateway universe), then the static seed.
    if universe is not None:
        hit = universe.get(symbol)
        if hit is not None:
            return hit

    seeded = _SEED_BY_SYMBOL.get(symbol)
    if seeded is not None:
        return seeded

    known = ", ".join(sorted(_SEED_BY_SYMBOL))
    raise ValueError(
        f"Hyperliquid perp market {market!r} (symbol {symbol!r}) is not in the "
        f"resolvable set. Seeded majors: {known}. New/thin markets need the "
        f"dynamic universe capability (not yet wired) or a seed addition."
    )


def seeded_symbols() -> frozenset[str]:
    """The set of symbols the static seed can resolve today."""
    return frozenset(_SEED_BY_SYMBOL)


__all__ = ["PerpMarket", "normalize_symbol", "resolve_market", "seeded_symbols"]
