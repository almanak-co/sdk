"""Equivalence pin for the synthetic-discovery membership sets (VIB-4928).

PR-1 of VIB-4928 folds the six hardcoded per-protocol membership frozensets in
``almanak/framework/permissions/synthetic_intents.py`` into connector-declared
data: each connector's ``permission_hints.py`` declares the intent types it
participates in for synthetic permission discovery
(:attr:`PermissionHints.synthetic_discovery_intents` and
:attr:`PermissionHints.supports_native_in_swap`), and the flash-loan registry
carries a per-provider :attr:`FlashLoanProviderRegistration.synthetic_discovery`
flag. ``synthetic_intents.py`` then DERIVES the six sets from those declarations
instead of hardcoding them.

This module pins the EXACT membership that existed on ``main`` immediately
before the fold (post-#2557 Radiant removal) as snapshot literals, and asserts
each derived set equals its snapshot verbatim. The point is to prove the
refactor preserved the *data* — no widening, no narrowing — independently of
the new derivation machinery. The registry's broader ``protocols_for_intent``
view (which adds enso/lifi/jupiter/uniswap_v4/fluid/aerodrome_slipstream-for-SWAP
/agni_finance, ~15 protocols) must NOT leak into these sets; the opt-in subset
is deliberate.

If a future connector legitimately joins or leaves a synthetic-discovery
category, update BOTH the connector's declaration AND the matching snapshot
below in the same change — the diff then documents the membership delta.
"""

from __future__ import annotations

import pytest

from almanak.framework.permissions.synthetic_intents import (
    _FLASH_LOAN_PROVIDERS,
    _LENDING_PROTOCOLS,
    _LP_PROTOCOLS,
    _NATIVE_IN_SWAP_PROTOCOLS,
    _PERP_PROTOCOLS,
    _SWAP_PROTOCOLS,
)

# ---------------------------------------------------------------------------
# Snapshot of the exact memberships on ``main`` before the VIB-4928 PR-1 fold.
# These are the literal contents of the (formerly hardcoded) frozensets at
# commit 830796305 (Radiant V2 removal, #2557). Do NOT edit these to make a
# test pass — edit them only alongside a deliberate, reviewed membership change.
# ---------------------------------------------------------------------------
_SNAPSHOT_SWAP_PROTOCOLS = frozenset(
    {
        "uniswap_v3",
        "pancakeswap_v3",
        "sushiswap_v3",
        "camelot",
        "fluid",
        "aerodrome",
        "traderjoe_v2",
        "pendle",
        "curve",
    }
)
_SNAPSHOT_NATIVE_IN_SWAP_PROTOCOLS = frozenset(
    {
        "uniswap_v3",
        "pancakeswap_v3",
        "sushiswap_v3",
    }
)
_SNAPSHOT_LP_PROTOCOLS = frozenset(
    {
        "uniswap_v3",
        "pancakeswap_v3",
        "sushiswap_v3",
        "aerodrome",
        "aerodrome_slipstream",
        "traderjoe_v2",
        "pendle",
    }
)
_SNAPSHOT_LENDING_PROTOCOLS = frozenset(
    {
        "aave_v3",
        "morpho_blue",
        "spark",
        "compound_v3",
        # VIB-5030: fluid fToken SUPPLY/WITHDRAW joins lending discovery.
        "fluid",
    }
)
_SNAPSHOT_PERP_PROTOCOLS = frozenset(
    {
        "gmx_v2",
        "aster_perps",
        "pancakeswap_perps",
    }
)
_SNAPSHOT_FLASH_LOAN_PROVIDERS = frozenset(
    {
        "aave",
        "balancer",
    }
)


def test_swap_protocols_equivalent() -> None:
    """Derived ``_SWAP_PROTOCOLS`` equals the pre-fold snapshot verbatim."""
    assert set(_SWAP_PROTOCOLS) == set(_SNAPSHOT_SWAP_PROTOCOLS)
    # The deliberately-excluded shared-compiler siblings must NOT leak in.
    assert "agni_finance" not in _SWAP_PROTOCOLS  # shares UniswapV3Compiler
    assert "aerodrome_slipstream" not in _SWAP_PROTOCOLS  # shares AerodromeCompiler
    assert "enso" not in _SWAP_PROTOCOLS  # in protocols_for_intent(SWAP), not opt-in
    assert "uniswap_v4" not in _SWAP_PROTOCOLS
    # "fluid" joined the SWAP set in Phase 1 (VIB-5029) — kill-switch removed.


def test_native_in_swap_protocols_equivalent() -> None:
    """Derived ``_NATIVE_IN_SWAP_PROTOCOLS`` equals the pre-fold snapshot."""
    assert set(_NATIVE_IN_SWAP_PROTOCOLS) == set(_SNAPSHOT_NATIVE_IN_SWAP_PROTOCOLS)
    # Native-in is a strict subset of the SWAP set.
    assert set(_NATIVE_IN_SWAP_PROTOCOLS) <= set(_SWAP_PROTOCOLS)


def test_lp_protocols_equivalent() -> None:
    """Derived ``_LP_PROTOCOLS`` equals the pre-fold snapshot verbatim."""
    assert set(_LP_PROTOCOLS) == set(_SNAPSHOT_LP_PROTOCOLS)
    # Slipstream IS an LP participant (unlike SWAP) — guard the asymmetry.
    assert "aerodrome_slipstream" in _LP_PROTOCOLS
    assert "agni_finance" not in _LP_PROTOCOLS  # shares UniswapV3Compiler


def test_lending_protocols_equivalent() -> None:
    """Derived ``_LENDING_PROTOCOLS`` equals the pre-fold snapshot verbatim."""
    assert set(_LENDING_PROTOCOLS) == set(_SNAPSHOT_LENDING_PROTOCOLS)
    # The ``morpho`` loader-key alias (→ MorphoBlueCompiler) must not leak;
    # the canonical lending slug is ``morpho_blue``.
    assert "morpho" not in _LENDING_PROTOCOLS


def test_perp_protocols_equivalent() -> None:
    """Derived ``_PERP_PROTOCOLS`` equals the pre-fold snapshot verbatim."""
    assert set(_PERP_PROTOCOLS) == set(_SNAPSHOT_PERP_PROTOCOLS)


def test_flash_loan_providers_equivalent() -> None:
    """Derived ``_FLASH_LOAN_PROVIDERS`` equals the pre-fold snapshot verbatim.

    The flash-loan registry knows ``aave``/``balancer``/``morpho``; only
    ``aave`` and ``balancer`` opt into synthetic discovery, so ``morpho`` must
    NOT appear here.
    """
    assert set(_FLASH_LOAN_PROVIDERS) == set(_SNAPSHOT_FLASH_LOAN_PROVIDERS)
    assert "morpho" not in _FLASH_LOAN_PROVIDERS


def test_derived_sets_are_frozensets() -> None:
    """All six derived sets are ``frozenset`` so a downstream ``in`` consumer
    cannot mutate-widen the membership (mirrors the ``UNIV3_LP_GROUPING_PROTOCOLS``
    contract)."""
    for derived in (
        _SWAP_PROTOCOLS,
        _NATIVE_IN_SWAP_PROTOCOLS,
        _LP_PROTOCOLS,
        _LENDING_PROTOCOLS,
        _PERP_PROTOCOLS,
        _FLASH_LOAN_PROVIDERS,
    ):
        assert isinstance(derived, frozenset)


def test_unknown_declared_intent_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A typo'd intent string (outside ``_VALID_SYNTHETIC_INTENTS``) must raise
    loudly rather than be silently ignored — silently dropping a connector from a
    membership set is the exact failure class VIB-4928 PR-1's CI break came from.
    """
    import almanak.framework.permissions.synthetic_intents as si
    from almanak.framework.permissions.hints import PermissionHints

    bad_hints = PermissionHints(synthetic_discovery_intents=frozenset({"L_OPEN"}))
    monkeypatch.setattr(si, "_all_connector_slugs", lambda: frozenset({"faketest"}))
    monkeypatch.setattr(si, "get_permission_hints", lambda _slug: bad_hints)

    # Exercise the REAL public path (``__getattr__`` -> cached ``_membership_sets``),
    # not the inner helper, so a regression in the lazy/cache layer is also caught.
    si._membership_sets.cache_clear()
    try:
        with pytest.raises(ValueError, match="L_OPEN"):
            _ = si._SWAP_PROTOCOLS
    finally:
        si._membership_sets.cache_clear()
