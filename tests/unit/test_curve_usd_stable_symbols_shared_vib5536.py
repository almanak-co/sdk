"""VIB-5536 — the Curve USD-numeraire allowlist has ONE home, shared by identity.

``CURVE_USD_STABLE_SYMBOLS`` was relocated from
``framework/valuation/curve_lp_position_reader._USD_STABLE_SYMBOLS`` (valuation)
down to ``almanak.core.constants`` so BOTH the Curve NAV repricer (valuation)
and the Curve basis-peg (accounting LP handler) share a single source of truth
without accounting importing backward from valuation (the interim VIB-5429
lazy import). These tests pin that contract: same object by identity, byte-
identical membership, and a deliberately-stricter set than the broad
``STABLECOINS`` hint (which correctly includes yield-bearing dollar tokens that
trade above $1 and must NOT be pegged to $1 in a Curve LP mark).
"""

from __future__ import annotations

from almanak.core.constants import CURVE_USD_STABLE_SYMBOLS, STABLECOINS

# The exact membership the frozenset carried immediately before the VIB-5536
# relocation. A change here is a real change to a NAV-peg / basis-peg
# correctness claim ("this token is ~$1 on every supported chain"), never an
# incidental edit — update this literal only alongside that decision.
_EXPECTED_MEMBERSHIP = frozenset(
    {
        "USDC",
        "USDC.E",
        "USDT",
        "DAI",
        "FRAX",
        "CRVUSD",
        "USDD",
        "TUSD",
        "BUSD",
        "GUSD",
        "LUSD",
        "MIM",
        "SUSD",
        "USDP",
        "DOLA",
        "GHO",
        "PYUSD",
        "USDE",
        "USDBC",
        "AXLUSDC",
        # VIB-5551: frxUSD (Frax USD) — fully-reserved $1 numeraire, the FRAX v3
        # successor. Added with the Polygon frxUSD/USDT NG pool that replaces the
        # frozen-Aave am3pool as the Polygon Curve representative.
        "FRXUSD",
    }
)


def test_constant_importable_from_core_with_expected_membership() -> None:
    """The set is importable from its new lower-layer home, byte-identical."""
    assert CURVE_USD_STABLE_SYMBOLS == _EXPECTED_MEMBERSHIP
    assert isinstance(CURVE_USD_STABLE_SYMBOLS, frozenset)


def test_both_consumers_reference_the_same_object() -> None:
    """Valuation repricer and accounting LP handler share ONE object (no drift).

    Identity (``is``), not just equality — two equal-but-separate frozensets
    would silently drift the day one side is edited. This test fails loudly if a
    future change forks the list back into a duplicate.
    """
    from almanak.framework.accounting.category_handlers import lp_handler
    from almanak.framework.valuation.curve_lp_position_reader import _USD_STABLE_SYMBOLS

    # Valuation still exposes the historical module-local name (an alias).
    assert _USD_STABLE_SYMBOLS is CURVE_USD_STABLE_SYMBOLS
    # Accounting LP handler binds the same object at module scope (no backward
    # import from valuation, no lazy re-import).
    assert lp_handler.CURVE_USD_STABLE_SYMBOLS is CURVE_USD_STABLE_SYMBOLS


def test_stricter_than_broad_stablecoins_hint() -> None:
    """The Curve peg set is conservative and must NOT collapse into STABLECOINS.

    ``STABLECOINS`` is a looser "dollar-ish" hint that includes yield-bearing /
    rebasing dollar tokens (e.g. SDAI, SUSDE) which trade ABOVE $1. Pegging a
    Curve LP to $1 on those would mis-mark it, so the two sets are distinct.
    """
    assert CURVE_USD_STABLE_SYMBOLS != STABLECOINS
    # Yield-bearing dollar tokens that STABLECOINS carries must stay OUT of the
    # strict Curve peg set.
    assert "SDAI" in STABLECOINS
    assert "SUSDE" in STABLECOINS
    assert "SDAI" not in CURVE_USD_STABLE_SYMBOLS
    assert "SUSDE" not in CURVE_USD_STABLE_SYMBOLS
