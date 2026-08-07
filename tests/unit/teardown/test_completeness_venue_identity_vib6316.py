"""VIB-6316 — the completeness gate must resolve venue identity with the WALLET.

> **A teardown that closed every position must not report FAILED because the
> enumeration described one position twice.**

THE DEFECT, PROVEN ON MAINNET (R5, 2026-08-02, arbitrum, $0.55). A strategy whose
``get_open_positions()`` omits ``collateral_token`` emits no venue identity token,
so its HOT row cannot intersect the WARM ``position_registry`` row describing the
same physical position. The union stays at 2, one closing intent covers one of
them, and ``check_intent_coverage`` reports the other "would be stranded" — in the
same iteration that TD-08's post-teardown reconciliation chain-confirmed BOTH rows
CLOSED. VIB-5572's entry latch then bricks the strategy against re-entry on a
position that is provably flat.

``docs/internal/gmx-readiness/r5-20260802/`` holds the run; the shipped demo
``almanak/demo_strategies/gmx_v2_directional_perp`` is the subject.

WHY THIS FILE IS THE DELIVERABLE, NOT HARDENING. Every signature VIB-6316 touches
defaults ``wallet``/``wallet_for_chain`` to ``None`` so no existing caller breaks.
That is correct — ``None`` is UNMEASURED and falls back to the pre-VIB-6316
comparison — but it means **the entire fix is invisible to a test that does not
pass a wallet**. A PR shipping the production change with no tests here would be
green, would read as inert, and would be live. Measured, not assumed: the three
pre-existing strict xfails in
``test_venue_position_identity_census_vib6287.py`` do NOT flip under this change.

THE FIX IS A TRIPLE. Each control below fails on the revert of exactly one leg:

* **N1** ``_covers_perp`` gains the venue-alias arm (``raw OR alias``).
* **N2** ``_perp_carries_identity`` receives the wallet — the VIB-5494 Item-2
  disambiguation guard routes through it whenever ≥2 same-type positions exist,
  so the 2-row shape needs BOTH legs.
* **N3/N4** the wallet resolver is wired at BOTH production gate call sites.
  Wiring one is the named failure mode; an AST assertion is the only thing that
  catches the other, because a unit test calling the gate directly cannot see
  which arguments production passes.
* **N5** the intent probe stays wallet-FREE. Threading it too is the tempting
  "symmetric" completion and it over-credits.
* **N6** neither leg alone is sufficient.

ADDRESS-FIRST UPDATE. Symbol→address market resolution was deliberately removed
("start with the address"), so the symbol↔address pairing these controls were
written against no longer exists: registry rows and regenerated intents both
carry the market-token ADDRESS and agree via the RAW comparison, wallet or not.
The pairing that still EXERCISES the VIB-6316 triple is the adapter discovery
row — venue key id, catalog LABEL under ``details["market"]`` (display), address
under ``details["market_address"]`` — which only the wallet-corroborated venue
``sem`` token can match. Every N-control below runs against that row. A LEGACY
symbol-shaped row or intent now over-splits loudly (fail-safe); migrating such
state is the repair CLI's job
(``almanak/framework/cli/repair_position_references.py``), never a curated
table's (VIB-6155).
"""

from __future__ import annotations

import ast
import importlib.util
import inspect
import pathlib
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.teardown.completeness import (
    _covers,
    _perp_carries_identity,
    check_intent_coverage,
)
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary
from almanak.framework.teardown.registry_enumeration import reconcile_lp_with_registry

_REPO = pathlib.Path(__file__).resolve().parents[3]


def _census():
    """Load the VIB-6287 census module for its R3-faithful fixtures.

    Deliberately reused rather than re-declared: ``_registry_row()`` reproduces
    the real ``position_registry`` payload from R3's own SQLite field-for-field
    (``docs/internal/plans/vib6316-restart-identity-fix-design-20260802.md`` §1.2,
    "Finding 1 — the C3 fixture IS faithful"). A hand-rolled stand-in here would
    be a fixture asserting against a fixture: an invented venue key does not
    survive the GMX hook's keccak corroboration, which silently turns every
    negative control green for the wrong reason.
    """
    path = pathlib.Path(__file__).with_name("test_venue_position_identity_census_vib6287.py")
    spec = importlib.util.spec_from_file_location("_vib6287_census", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CENSUS = _census()
WALLET = CENSUS.WALLET
CHAIN = CENSUS.CHAIN

# An UNDER-DESCRIBED perp row: market + side, no ``collateral_token``.
#
# This was the shipped demo's literal shape until VIB-6316 also repaired the demo
# (``gmx_v2_directional_perp.get_open_positions`` now names its collateral). It is
# retained here as a SYNTHETIC fixture rather than deleted, because the gate's
# tolerance is not about one demo: any strategy may under-describe its own
# position, and nothing in the framework forces ``get_open_positions`` to name
# collateral. Fixing the producer removes this shape from OUR demo; it does not
# remove it from the world.
#
# Address-first: the market is the ADDRESS a migrated strategy writes. The
# under-description (missing collateral) is what splits the union — a property
# of the row's incompleteness, not of its value space.
UNDERDESCRIBED_DETAILS = {"market": CENSUS.MARKET, "side": "long", "size_usd": "8"}


def _resolver(_chain: str) -> str:
    return WALLET


def _hot_row(details: dict) -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="deployment:86f4562d5b6c",
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.PERP,
                position_id="gmx-v2-ETH/USD-long",
                chain=CHAIN,
                protocol="gmx_v2",
                value_usd=Decimal("10"),
                details=details,
            )
        ],
    )


def _union(details: dict, warm: PositionInfo | None = None):
    """The production enumeration: HOT strategy row ∪ one WARM row.

    ``warm`` defaults to the registry read row; the wallet-sensitive controls
    pass the adapter's catalog-labelled discovery row instead (see the module
    docstring's address-first update).
    """
    return reconcile_lp_with_registry(
        strategy_summary=_hot_row(details),
        registry_positions=[warm if warm is not None else CENSUS._registry_row()],
        registry_available=True,
        wallet_for_chain=_resolver,
    )


_DEFAULT_MARKET = object()  # sentinel: None is a meaningful "market-less intent"


def _close_intent(market=_DEFAULT_MARKET, **kw):
    """A regenerated close intent; defaults to the address-first market."""
    return CENSUS._CloseIntent(CENSUS.MARKET if market is _DEFAULT_MARKET else market, **kw)


# ---------------------------------------------------------------------------
# THE TARGET — the R5 mainnet shape, end to end
# ---------------------------------------------------------------------------


def test_the_r5_class_split_no_longer_false_fails():
    """THE HEADLINE — R5's failure class, expressed in the address-first world.

    R5 (mainnet arbitrum, 2026-08-02) paired an under-described HOT row with a
    WARM row for the same physical position; one full close covered one of them
    and the gate reported the other stranded. Address-first retires R5's literal
    symbol-vs-address pairing (both sides carry the address now, so the raw
    comparison agrees), but the CLASS survives: the WARM row here is the
    adapter's discovery row, whose ``market`` detail is the catalog display
    LABEL — only the wallet-corroborated venue token can match it.

    Union of 2 (one physical position), one address-space closing intent,
    wallet resolved. Without the resolver this is the R5 false-FAILED verbatim;
    with it the gate is complete. Both halves are asserted in ONE test so the
    pair can never be separated into "the fix" and "an unrelated regression".
    """
    union = _union(UNDERDESCRIBED_DETAILS, warm=CENSUS._adapter_display_row())
    assert len(union.positions) == 2, (
        "the union must still SPLIT — VIB-6316 makes the gate tolerant of the split, "
        "it does not collapse the enumeration. A union of 1 here means the subject "
        "stopped being able to express the defect and this test proves nothing."
    )

    intents = [_close_intent()]
    assert not check_intent_coverage(union, intents).complete, (
        "wallet-less must stay FAILING — this is the R5-class result and the "
        "inertness control. If this passes, the fix is not what makes the "
        "difference and every other assertion here is meaningless."
    )
    assert check_intent_coverage(union, intents, wallet_for_chain=_resolver).complete, (
        "VIB-6316: with the owning account resolved, the discovery row's venue key "
        "corroborates its display label against the address-space close"
    )


def _shipped_demo_row():
    """Build the row the SHIPPED demo actually emits, via its public method.

    This assertion used to be ``inspect.getsource(get_open_positions)`` plus a
    substring search for a collateral alias. That proxy was defeated the moment
    the row builder was extracted into a helper: the demo still emitted
    ``collateral_token``, the property held, and the test went red anyway.

    A source-text search answers "where does this string live", which is not the
    property. The property is "does the emitted row NAME its collateral", so this
    reads the row.

    It matters that this reads the real demo rather than a literal. The companion
    below (``test_the_repaired_demo_now_collapses_its_own_union``) proves the
    identity MECHANISM against a hand-built ``repaired`` dict — so if this test
    stopped touching the shipped module, the mechanism could stay green while the
    demo regressed, and nothing would connect the two. This is the only link
    between the mechanism and the code users copy.

    The venue probe has no market snapshot here, so it returns UNMEASURED and the
    demo falls back to its cached side — which is precisely the path that must
    still name collateral, since an unmeasured read is not a flat account.
    """
    import importlib.util
    import json
    from pathlib import Path
    from unittest.mock import patch

    seed = Path(__file__).resolve().parents[3] / "almanak" / "demo_strategies" / "gmx_v2_directional_perp"
    spec = importlib.util.spec_from_file_location("gmx_seed_row", seed / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cls = module.GmxV2DirectionalPerp
    cfg = json.loads((seed / "config.json").read_text(encoding="utf-8"))
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strat = cls.__new__(cls)
        strat._config = cfg
        strat.get_config = lambda k, d=None: cfg.get(k, d)
        cls.__init__(strat)
    strat._chain = "arbitrum"
    strat._position_side = "long"
    summary = strat.get_open_positions()
    assert len(summary.positions) == 1, (
        f"fixture precondition: a cached long must yield exactly one row, got {len(summary.positions)}"
    )
    return summary.positions[0]


def test_the_shipped_demo_now_names_its_own_position():
    """The producer-side repair, asserted against the shipped source.

    This test previously asserted the OPPOSITE — that the demo still omitted
    ``collateral_token`` — as a tripwire so the repair could not land silently.
    It has now landed, and this is the tripwire firing: the assertion is inverted
    rather than deleted, so the demo cannot regress back to the malformed shape
    without a red test.

    Why the repair belongs with the gate fix rather than after it: the gate change
    made teardown *complete* on the split, but the lifecycle counters still read
    ``positions_total=2, positions_closed=2`` for ONE physical position. Tolerating
    a malformed producer is correct; shipping one as the reference users copy is
    not.
    """
    from almanak.connectors.gmx_v2.perp_identity import _COLLATERAL_KEYS

    row = _shipped_demo_row()
    # VALUE, not membership: ``collateral_token=""`` or ``None`` is present but
    # derives ZERO identity tokens, so a membership check passes on a row the
    # identity hook cannot name — the same Empty ≠ Zero inversion this PR exists
    # to remove, hiding inside its own guard.
    written = [key for key in _COLLATERAL_KEYS if row.details.get(key)]
    assert written, (
        "gmx_v2_directional_perp.get_open_positions EMITS no collateral under any "
        f"alias the identity hook accepts ({list(_COLLATERAL_KEYS)}); "
        f"it emitted {sorted(row.details)}. "
        "Without it the row derives no venue key, falls through to its raw position_id, "
        "and one physical position enumerates twice again (mainnet R5: total=2, closed=2 "
        "for a single ETH/USD long)."
    )


def test_a_collateral_naming_address_row_collapses_its_own_union():
    """The repair's actual effect, measured — not inferred from the source string.

    SUCCESSOR of ``test_the_repaired_demo_now_collapses_its_own_union``: the
    row shape is the VIB-6316 producer repair carried into the address-first
    world. With collateral named and the market already an ADDRESS,
    ``gmx_v2_perp_identity`` DERIVEs the venue key (collateral still resolves
    through the token table; the market passes through) and it equals the
    bytes32 key the registry row ADOPTs — so the two rows intersect and the
    union is 1.

    That derived-equals-adopted equality is the whole mechanism. Asserting only
    ``len(union.positions) == 1`` would also pass if the rows collapsed for some
    unrelated reason, so the key identity is asserted directly.

    NOTE the shipped ``gmx_v2_directional_perp`` demo still CONFIGURES a symbol
    market (``config.json: "market": "ETH/USD"``); until its own address-first
    migration lands, its rows carry a symbol and over-split loudly — the
    fail-safe direction, pinned from the legacy side elsewhere in this file.
    """
    from almanak.framework.teardown.registry_enumeration import _dedupe_keys

    repaired = {**UNDERDESCRIBED_DETAILS, "collateral_token": "USDC"}

    union = _union(repaired)
    assert len(union.positions) == 1, (
        "the repaired demo row must collapse against the registry row — "
        f"got {len(union.positions)} rows for one physical position"
    )

    strategy_keys = _dedupe_keys(_hot_row(repaired).positions[0], wallet_for_chain=_resolver)
    registry_keys = _dedupe_keys(CENSUS._registry_row(), wallet_for_chain=_resolver)
    venue_key = f"gmx_v2:key:{CHAIN}:{CENSUS.VENUE_KEY}"
    assert (CHAIN, "PERP", "venue", venue_key) in strategy_keys, (
        "the repaired strategy row must DERIVE the venue key, not merely carry a symbol"
    )
    assert (CHAIN, "PERP", "venue", venue_key) in registry_keys
    assert strategy_keys & registry_keys, "derived and adopted keys must intersect"

    # And the collateral-less shape must STILL split — otherwise this test would
    # be passing because identity got looser for everyone, which is over-collapse.
    assert len(_union(UNDERDESCRIBED_DETAILS).positions) == 2, (
        "an under-described row must still split; the repair fixes the producer, "
        "it must not loosen identity for rows that genuinely cannot be named"
    )


# ---------------------------------------------------------------------------
# N1 / N2 — the two production legs, isolated
# ---------------------------------------------------------------------------


def test_n1_covers_perp_gained_the_venue_alias_arm():
    """N1 — revert the ``_covers_perp`` widening and this fails.

    ``_covers`` is the layer that rejected the pair one step before the identity
    check could ever run. Address-first, the pairing that still needs the alias
    arm is the adapter's discovery row: its ``market`` detail is the catalog
    display LABEL, so the raw comparison refuses the address-space intent and
    only the wallet-corroborated venue token can credit it. The registry row —
    address on both sides — must keep matching RAW, wallet or not (``raw OR
    alias``, never alias alone).
    """
    display_row = CENSUS._adapter_display_row()
    assert _covers(_close_intent(), display_row, WALLET), (
        "_covers_perp must accept a venue-corroborated alias match: the discovery row "
        "displays the catalog label, the regenerated intent names the ADDRESS"
    )
    assert not _covers(_close_intent(), display_row), (
        "and must be unchanged without a wallet — raw OR alias, never alias alone"
    )
    assert _covers(_close_intent(), CENSUS._registry_row()), (
        "the address↔address registry pairing must stay covered by the RAW clause alone"
    )


def test_n2_perp_carries_identity_receives_the_wallet():
    """N2 — revert the wallet threading and this fails.

    This is the leg the 2-row union needs: ``_position_is_covered`` consults
    ``_intent_carries_position_identity`` whenever ≥2 same-type positions exist
    (VIB-5494 Item 2), and that routes here. Address-first, the wallet is
    load-bearing for the catalog-labelled discovery row (see N1).
    """
    display_row = CENSUS._adapter_display_row()
    assert _perp_carries_identity(_close_intent(), display_row, WALLET)
    assert not _perp_carries_identity(_close_intent(), display_row), (
        "wallet-less must stay unmeasured — GMX cannot derive its own key without the account"
    )


def test_n6_neither_leg_alone_is_sufficient():
    """N6 — pins the TRIPLE so a future 'simplification' fails loudly.

    Each leg is disabled in turn against the live 2-row union. Address-first,
    the WARM row must be the catalog-labelled discovery row — the registry
    pairing matches raw and would leave every leg looking optional.
    """
    union = _union(UNDERDESCRIBED_DETAILS, warm=CENSUS._adapter_display_row())
    intents = [_close_intent()]

    # Leg A disabled: no wallet reaches the identity lane.
    assert not check_intent_coverage(union, intents).complete

    # Leg B disabled: the alias predicate itself is neutralised, wallet present.
    # Patching the SHARED predicate reverts both call sites at once, which is
    # exactly the point — it proves the alias arm is load-bearing rather than
    # decorative, in the presence of a fully resolved wallet.
    import almanak.framework.teardown.completeness as comp

    original = comp._perp_venue_alias_match
    try:
        comp._perp_venue_alias_match = lambda *_a, **_k: False
        assert not check_intent_coverage(union, intents, wallet_for_chain=_resolver).complete, (
            "with the alias arm neutralised the gate must fail again — if it still "
            "passes, something OTHER than this fix is carrying the result"
        )
    finally:
        comp._perp_venue_alias_match = original

    # Both present.
    assert check_intent_coverage(union, intents, wallet_for_chain=_resolver).complete


# ---------------------------------------------------------------------------
# N3 / N4 — the two production gate call sites
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("relpath", "why"),
    [
        (
            "almanak/framework/runner/_teardown_helpers.py",
            "G1 — the runner lane, whose positions come from the registry union",
        ),
        (
            "almanak/framework/teardown/teardown_manager.py",
            "G2 — the TWIN, reachable from the CLI `teardown execute --discover` lane with "
            "registry-shaped precomputed_positions. Wiring G1 alone is the named failure mode.",
        ),
    ],
)
def test_n3_n4_both_production_gates_pass_the_wallet_resolver(relpath: str, why: str):
    """N3/N4 — the ONLY controls that can see the production wiring.

    Every other test here calls ``check_intent_coverage`` directly and would stay
    green if neither call site passed a resolver. Asserted structurally rather
    than by string match so a reformat or a renamed lambda parameter does not
    silently void the check.
    """
    tree = ast.parse((_REPO / relpath).read_text())
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "check_intent_coverage"
    ]
    assert calls, f"no check_intent_coverage call found in {relpath} — did the gate move? ({why})"

    for call in calls:
        kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
        assert "wallet_for_chain" in kwargs, (
            f"{relpath} must pass wallet_for_chain to the completeness gate ({why}). "
            "Without it a registry-derived perp row cannot match a symbol-shaped closing "
            "intent and a successful teardown reports FAILED — VIB-6316, proven on mainnet."
        )
        # It must resolve through the SAME helper the enumeration uses. Two lanes on
        # two resolvers can disagree about who owns a position, which is this defect
        # one level up rather than a style preference.
        source = ast.dump(kwargs["wallet_for_chain"])
        assert "_teardown_wallet_for_chain" in source, (
            f"{relpath} must resolve the wallet through _teardown_wallet_for_chain — the same "
            f"resolver resolve_open_positions_with_registry threads into the enumeration. Got: {source}"
        )


# ---------------------------------------------------------------------------
# N5 + the adversarial matrix — nothing may get STRICTER, nothing over-credited
# ---------------------------------------------------------------------------


def test_n5_the_intent_probe_stays_wallet_free():
    """N5 — the over-credit case that killed the symmetric design.

    A registry row whose ``position_id`` is the ETH venue key but whose
    ``details['market']`` names BTC (the shape VIB-6155's catalogue defect
    produces) must NOT be credited by an ETH close. Threading the wallet into
    ``_perp_intent_venue_aliases`` too — the tempting completion — makes this
    return True, which reports a teardown complete over an open position.

    The position side is safe because GMX emits ``sem`` only after verifying by
    its own derivation that ``position_id`` and ``details`` name the same
    position; the intent probe carries no ``position_id`` to corroborate against.
    """
    mixed = CENSUS._registry_row(market=CENSUS.BTC_MARKET)
    assert not check_intent_coverage([mixed], [_close_intent("ETH/USD")], wallet_for_chain=_resolver).complete

    from almanak.framework.teardown.completeness import _perp_intent_venue_aliases

    params = inspect.signature(_perp_intent_venue_aliases).parameters
    assert "wallet" not in params and "wallet_address" not in params, (
        "the intent probe must stay wallet-free (VIB-6316 §4.1) — a derived-key match "
        "from a probe that carries no position_id is corroborated by nothing"
    )


@pytest.mark.parametrize(
    ("label", "positions", "intents", "expected"),
    [
        (
            "NEG different market",
            [CENSUS._registry_row(market=CENSUS.BTC_MARKET)],
            [_close_intent("ETH/USD")],
            False,
        ),
        ("NEG wrong side", [CENSUS._registry_row()], [_close_intent("ETH/USD", is_long=False)], False),
        (
            "NEG foreign protocol",
            [CENSUS._registry_row()],
            [_close_intent("ETH/USD", protocol="hyperliquid")],
            False,
        ),
        (
            "NEG different collateral",
            [CENSUS._registry_row()],
            [_close_intent("ETH/USD", collateral_token="WETH")],
            False,
        ),
    ],
)
def test_the_widening_never_over_credits(label, positions, intents, expected):
    """Over-crediting is the one direction that strands funds with no alarm."""
    assert check_intent_coverage(positions, intents, wallet_for_chain=_resolver).complete is expected, label


@pytest.mark.parametrize(
    ("label", "details", "intent_market"),
    [
        ("symbol row + symbol intent (raw path)", {"market": "ETH/USD", "is_long": True}, "ETH/USD"),
        ("market-less intent (lenient default)", {"market": "ETH/USD", "is_long": True}, None),
    ],
)
def test_the_widening_never_gets_stricter(label, details, intent_market):
    """The regression that would read as an improvement.

    A coverage check that starts REJECTING matches turns working teardowns into
    FAILED. These pass on ``main`` and must keep passing — with a wallet present,
    which is the configuration the fix introduces.
    """
    row = PositionInfo(
        position_type=PositionType.PERP,
        position_id="gmx-v2-ETH/USD-long",
        chain=CHAIN,
        protocol="gmx_v2",
        value_usd=Decimal("10"),
        details=details,
    )
    assert check_intent_coverage([row], [_close_intent(intent_market)], wallet_for_chain=_resolver).complete, label


def test_a_collateral_writing_strategy_is_completely_unaffected():
    """The control arm of the R3/R5 pair.

    ``strategies/accounting/perp`` writes ``collateral_token`` (and, migrated,
    the market ADDRESS), so its union already collapsed to 1 and its gate
    already passed. Neither VIB-6316 nor the address-first migration may change
    that in either direction.
    """
    union = _union({"market": CENSUS.MARKET, "collateral_token": "USDC", "side": "long", "size_usd": "8"})
    assert len(union.positions) == 1, "collateral-writing strategies must still collapse to one row"
    for resolver in (None, _resolver):
        assert check_intent_coverage(union, [_close_intent()], wallet_for_chain=resolver).complete


def test_an_unresolvable_wallet_degrades_to_the_old_behaviour():
    """Empty ≠ Zero, applied to the wallet.

    A resolver returning ``None`` (three documented GMX fallbacks) means
    UNMEASURED, not "no wallet": every comparison falls back to the raw
    pre-VIB-6316 path. Fail-SAFE, and deliberately not fixed here — the gate
    still reports the split, which is loud rather than silent. Address-first,
    the split that survives an unresolvable wallet is the catalog-labelled
    discovery row (the registry pairing matches raw regardless).
    """
    union = _union(UNDERDESCRIBED_DETAILS, warm=CENSUS._adapter_display_row())
    assert not check_intent_coverage(union, [_close_intent()], wallet_for_chain=lambda _c: None).complete

    # A raising resolver must never break teardown either.
    def _boom(_chain):
        raise RuntimeError("wallet backend down")

    assert not check_intent_coverage(union, [_close_intent()], wallet_for_chain=_boom).complete
