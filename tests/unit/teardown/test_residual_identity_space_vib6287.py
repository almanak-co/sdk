"""Residual space is NOT position space (VIB-6287 / VIB-5116).

A residual — a pending unfilled GMX order holding collateral in the OrderVault,
an unverified re-measurement sentinel — is surfaced as a ``PositionType.PERP``
row and can legitimately carry the SAME market, collateral and side as a real
open position. It is a DIFFERENT thing holding its own money.

The VIB-6287 identity seam makes two rows the same position when their alias
sets intersect, so if a residual were named in the same space as a position the
union would merge them and suppress one. **A suppressed residual is never
recovered** — nothing builds a closing intent for a row that is not in the
enumeration — which is the silent-strand failure, not the loud one.

This file is the census the seam needs: it asserts that residual-marked rows are
named by nobody, at every layer that could name them, and that the residual
union lane itself cannot collapse a residual into a position.

Scope note, stated rather than implied: this guards the ``_union_residuals``
bypass, it does not fix it. That lane keys on
``(chain, position_type, protocol, position_id)`` and never consults ``details``
at all, so a future connector emitting a real PERP *position* through residual
discovery would bypass the identity seam entirely. Deliberately out of scope for
VIB-6287; these tests fail if it starts to matter.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.perp_identity import is_residual_marked
from almanak.connectors.gmx_v2.perp_identity import gmx_v2_perp_identity
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary
from almanak.framework.teardown.perp_identity import venue_identity_tokens
from almanak.framework.teardown.registry_enumeration import (
    _IDENTITY_DEFAULTS,
    _dedupe_keys,
    _lp_default_identity,
    _perp_default_identity,
    _position_info_from_pendle_registry_row,
    _union_residuals,
)
from tests.support.gmx_v2 import GMX_V2_TOKENS
from tests.unit.connectors.gmx_v2.market_fixtures import market_address

CHAIN = "arbitrum"
MARKET = market_address(CHAIN, "ETH/USD")
USDC = GMX_V2_TOKENS[CHAIN]["USDC"]
WALLET = "0xafeB2f5c213b5e7F37c3Fc171dfCb6270d07e21a"

# Residual / non-position markers that exist in the tree today. The guards under
# test key on "any non-empty kind" rather than on this list, deliberately: a list
# would need extending by whoever adds the next residual kind, and the failure of
# forgetting is SILENT. The list is here only so the test names real cases; the
# `unknown_future_kind` row is what proves the guard is not list-shaped.
_KNOWN_KINDS = ("pending_order", "residual_unverified", "hypercore_cash", "swap_clamp_degraded")

# Blank-but-PRESENT markers. No producer emits these today — every writer that
# reaches a position's ``details`` uses a hardcoded literal, and the one
# payload-derived path (``_position_info_from_pendle_registry_row``) normalises
# and DROPS the row on an unrecognised kind. They are covered anyway because the
# guard's polarity has to be argued, not assumed: reading a blank marker as
# "not a residual" NAMES the row, which is the silent-strand direction. "Cannot
# happen in practice" is the reasoning that decays, and here it decays the
# wrong way.
_BLANK_KINDS = ("", "   ", "\t")


def _perp_row(*, position_id: str, kind: str | None = None, protocol: str = "gmx_v2") -> PositionInfo:
    """A perp row that is IDENTICAL to a real position except for its ``kind``."""
    details: dict = {"market": MARKET, "collateral_token": USDC, "is_long": True}
    if kind is not None:
        details["kind"] = kind
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=position_id,
        chain=CHAIN,
        protocol=protocol,
        value_usd=Decimal("0"),
        details=details,
    )


@pytest.mark.parametrize("kind", [*_KNOWN_KINDS, *_BLANK_KINDS, "unknown_future_kind"])
def test_the_venue_never_names_a_kind_marked_row(kind):
    """Layer 1: the connector hook declines to name a residual.

    Asserted through the framework seam as well as the hook directly — the hook
    could decline correctly while the seam still handed back something, and the
    seam is what the union actually calls.
    """
    row = _perp_row(position_id="0x" + "ab" * 32, kind=kind)
    assert gmx_v2_perp_identity(row, wallet_address=WALLET) == frozenset()
    assert venue_identity_tokens(row, WALLET) == frozenset()


@pytest.mark.parametrize("kind", [*_KNOWN_KINDS, *_BLANK_KINDS, "unknown_future_kind"])
def test_the_framework_default_never_names_a_kind_marked_row(kind):
    """Layer 2: the per-type default declines too.

    This is the layer that matters for the four perp venues with no identity
    hook (``aster_perps`` / ``drift`` / ``hyperliquid`` / ``pancakeswap_perps``).
    Without it the connector hook and the framework default would disagree about
    what a residual is, and the venues WITHOUT a hook would be the only place the
    hole stayed open — the worst possible distribution of a guard.
    """
    assert _perp_default_identity(_perp_row(position_id="resid-1", kind=kind)) == frozenset()


def test_guard_is_not_vacuous_the_same_row_without_kind_is_named():
    """Guard the guard. If the unmarked row were also unnamed, both tests above
    would pass for the trivial reason that nothing is ever named."""
    unmarked = _perp_row(position_id="pos-1")
    assert gmx_v2_perp_identity(unmarked, wallet_address=WALLET) != frozenset()
    assert _perp_default_identity(unmarked) != frozenset()


@pytest.mark.parametrize("kind", [*_KNOWN_KINDS, *_BLANK_KINDS, "unknown_future_kind"])
def test_a_residual_falls_back_to_its_own_raw_identity(kind):
    """Empty ≠ Zero: an unnamed row keeps its raw ``position_id`` key, so it stays
    distinct rather than becoming a wildcard that matches everything."""
    keys = _dedupe_keys(_perp_row(position_id="resid-1", kind=kind), wallet_for_chain=lambda _c: WALLET)
    assert keys == frozenset({(CHAIN, str(PositionType.PERP), "id", "resid-1")})


def test_a_residual_never_collapses_into_a_position_in_the_union():
    """End to end through the real residual lane: a pending order that shares
    market, collateral and side with a live position must survive as its own row.

    If this ever returns 1, the pending order's collateral is invisible to
    teardown and silently stranded in the OrderVault."""
    position = _perp_row(position_id="0x" + "aa" * 32)
    residual = _perp_row(position_id="0x" + "bb" * 32, kind="pending_order")
    summary = TeardownPositionSummary(
        deployment_id="deployment:test",
        timestamp=datetime.now(UTC),
        positions=[position],
    )
    merged = _union_residuals(summary, [residual])
    assert {p.position_id for p in merged.positions} == {position.position_id, residual.position_id}


def test_the_residual_union_lane_ignores_details_entirely():
    """Pin the ``_union_residuals`` contract this file guards rather than fixes.

    Its key is ``(chain, position_type, protocol, position_id)`` and never reads
    ``details``, so two residuals that differ ONLY in ``position_id`` both
    survive — correct for order keys, and the reason the identity seam does not
    reach this lane. If someone routes residuals through ``_dedupe_keys`` later,
    this test is where the change announces itself."""
    summary = TeardownPositionSummary(
        deployment_id="deployment:test",
        timestamp=datetime.now(UTC),
        positions=[],
    )
    a = _perp_row(position_id="0x" + "11" * 32, kind="pending_order")
    b = _perp_row(position_id="0x" + "22" * 32, kind="pending_order")
    merged = _union_residuals(summary, [a, b])
    assert len(merged.positions) == 2


def test_every_residual_discovery_publisher_is_covered_by_these_guards():
    """Census: the residual producers exist and are all perp venues these guards
    cover. A new publisher shows up here rather than silently inheriting a seam
    that was only ever reasoned about for GMX."""
    publishers = {c.name for c in CONNECTOR_REGISTRY.with_teardown_residual_discovery()}
    assert publishers, "no residual-discovery publishers found — this census would be vacuous"
    unexpected = publishers - {"gmx_v2"}
    assert not unexpected, (
        f"new teardown_residual_discovery publisher(s) {sorted(unexpected)}. Confirm their residual "
        "rows carry a details['kind'] marker — the VIB-6287 identity guards key on it, and a "
        "residual named in position space can be merged with a real position and silently "
        "suppressed. Then add them here."
    )


def test_none_kind_is_absent_not_blank():
    """``kind: None`` is the conventional "unset" value, not a marker.

    The polarity argument is about a producer that MEANT to say something and
    said it badly. ``None`` is what a dict carries when nobody set the key at
    all, so treating it as a residual would unname ordinary positions and
    over-split every one of them — loud, but wrong, and it would mask the real
    fix. Asserted so the presence check cannot quietly become ``"kind" in
    details`` alone."""
    row = _perp_row(position_id="pos-1")
    row.details["kind"] = None
    assert not is_residual_marked(row.details)
    assert gmx_v2_perp_identity(row, wallet_address=WALLET) != frozenset()
    assert _perp_default_identity(row) != frozenset()


def test_both_layers_share_one_predicate():
    """The connector hook and the framework default must not drift apart about
    what a residual is. A guard present in one layer and absent in the other is
    worse than no guard — it hides in exactly the venues nobody is looking at,
    which is what this commit found and fixed."""
    import almanak.connectors.gmx_v2.perp_identity as hook_mod
    import almanak.framework.teardown.registry_enumeration as fw_mod

    assert hook_mod.is_residual_marked is is_residual_marked
    assert fw_mod.is_residual_marked is is_residual_marked


# ---------------------------------------------------------------------------
# The predicate's SCOPE — `kind` is polysemous across primitives
# ---------------------------------------------------------------------------


def test_kind_is_polysemous_a_pendle_lp_row_is_a_real_position_carrying_kind():
    """Pin the counterexample that bounds ``is_residual_marked``.

    For a PERP row ``details["kind"]`` means "this is NOT a position". For a
    Pendle registry row it means "WHICH KIND of position this is":
    ``_position_info_from_pendle_registry_row`` writes ``kind="pt"|"lp"`` as a
    positional discriminator, and such a row is a real, closable position.

    One key name, two value spaces, and a consumer that assumes one of them —
    VIB-6287's own defect class, now latent inside its fix. Measured here rather
    than asserted in a comment, so the claim cannot rot.
    """
    row = _position_info_from_pendle_registry_row(
        {
            "chain": "arbitrum",
            "primitive": "pendle",
            "payload": {"protocol": "pendle", "kind": "lp", "market_id": "0xmarket", "pt_symbol": "PT-wstETH"},
        }
    )
    assert row is not None
    assert row.position_type is PositionType.LP, "fixture no longer drives the Pendle LP branch"
    assert row.details.get("kind") == "lp"
    # The predicate says True on a REAL POSITION. That is correct-in-scope and
    # dangerous out of it — hence the guard below.
    assert is_residual_marked(row.details)
    # ... and today nothing acts on that, because the LP default never consults it.
    assert _lp_default_identity(row) == frozenset({("lp", "0xmarket")})


def test_residual_marker_is_not_wired_into_a_positional_kind_default():
    """The guard that makes the scope note enforceable rather than advisory.

    ``is_residual_marked`` is valid only where ``kind`` is EXCLUSIVELY a residual
    marker — true for PERP, false for the primitives whose rows carry a
    positional ``kind``. Wiring it into one of those defaults, for symmetry and
    citing "a guard present in one layer and absent in the other is worse than
    no guard", would leave every Pendle LP registry row unnamed: a regression
    shipped in the name of the fix.

    Asserted by BEHAVIOUR — feed each such default a row carrying a positional
    ``kind`` and require it to still name the row — rather than by inspecting
    source, so it cannot be satisfied by moving the call somewhere this test
    does not read.
    """
    positional_kind_defaults = {
        PositionType.LP: ("lp", "0xmarket"),
    }
    for position_type, expected in positional_kind_defaults.items():
        default = _IDENTITY_DEFAULTS.get(position_type)
        assert default is not None, f"{position_type} lost its default; re-derive this guard"
        row = PositionInfo(
            position_type=position_type,
            position_id="0xmarket",
            chain=CHAIN,
            protocol="pendle",
            value_usd=Decimal("0"),
            details={"source": "position_registry", "kind": "lp", "market_id": "0xmarket"},
        )
        assert default(row) == frozenset({expected}), (
            f"the {position_type} default stopped naming a row carrying a POSITIONAL kind. "
            "is_residual_marked keys on presence and is valid only where `kind` is exclusively "
            "a residual marker (PERP). Pendle writes kind='pt'|'lp' to say WHICH KIND of position "
            "this is — unnaming those rows drops every Pendle LP registry row to raw-id identity."
        )
