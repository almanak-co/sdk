"""Census + mechanism tests for the venue position identity seam (VIB-6287).

The epic's governing rule is that a fix may DELETE a list or ADD a census that
fails when a list is incomplete — it may not add a list, nor extend one, without
a test that would have caught the omission. Everything enumerated below is
derived from ``CONNECTOR_REGISTRY`` / ``PositionType`` at runtime; the only
literal lists here are WAIVERS, and each waiver is dated, ticketed, and guarded
by a test that fails when it expires or becomes wrong.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.perp_identity import (
    get_perp_identity_hook,
    has_perp_identity_hook,
    registered_perp_identity_protocols,
)
from almanak.connectors.gmx_v2.addresses import GMX_V2_MARKETS, GMX_V2_TOKENS
from almanak.connectors.gmx_v2.perp_identity import _resolve_address, gmx_v2_perp_identity
from almanak.framework.teardown.completeness import (
    _covers,
    _perp_carries_identity,
    check_intent_coverage,
)
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary
from almanak.framework.teardown.perp_identity import venue_identity_tokens
from almanak.framework.teardown.post_conditions import _connector_teardown_slugs
from almanak.framework.teardown.registry_enumeration import (
    _IDENTITY_DEFAULTS,
    _RAW_ID_TYPES,
    _dedupe_keys,
    reconcile_lp_with_registry,
)

CHAIN = "arbitrum"
MARKET = GMX_V2_MARKETS[CHAIN]["ETH/USD"]
BTC_MARKET = GMX_V2_MARKETS[CHAIN]["BTC/USD"]
USDC = GMX_V2_TOKENS[CHAIN]["USDC"]
USDT = GMX_V2_TOKENS[CHAIN]["USDT"]
WALLET = "0xafeB2f5c213b5e7F37c3Fc171dfCb6270d07e21a"
VENUE_KEY = "0xbf58e0307a44a17ea51e30850651f5269c9fc0f306990576c015e9a88ac9bafa"


def _perp(**kwargs) -> PositionInfo:
    base = {
        "position_type": PositionType.PERP,
        "position_id": "gmx-ETH/USD-arbitrum",
        "chain": CHAIN,
        "protocol": "gmx_v2",
        "value_usd": Decimal("6"),
        "details": {"market": MARKET, "collateral_token": USDC, "is_long": True},
    }
    base.update(kwargs)
    return PositionInfo(**base)


# ---------------------------------------------------------------------------
# Census 1 — every PositionType is a NAMED decision
# ---------------------------------------------------------------------------


def test_every_position_type_has_a_named_identity_decision():
    """No ``PositionType`` may fall through the identity seam by accident.

    A type is either given a framework default or explicitly parked in
    ``_RAW_ID_TYPES`` (raw ``position_id`` identity) with a documented reason.
    Adding a new type without deciding which it is fails here, rather than
    silently inheriting the fall-through — which is how a whole primitive ends
    up with an unowned identity rule that nobody notices until a teardown
    double-counts on mainnet.
    """
    decided = set(_IDENTITY_DEFAULTS) | set(_RAW_ID_TYPES)
    undecided = sorted(t.name for t in PositionType if t not in decided)
    assert not undecided, (
        f"PositionType(s) {undecided} have no identity decision — add a default to "
        "_IDENTITY_DEFAULTS or park them in _RAW_ID_TYPES with a reason"
    )
    overlap = sorted(t.name for t in PositionType if t in _IDENTITY_DEFAULTS and t in _RAW_ID_TYPES)
    assert not overlap, f"PositionType(s) {overlap} are in BOTH tables — the decision is ambiguous"


# ---------------------------------------------------------------------------
# Census 2 — every PERP venue declares a hook, or is waived
# ---------------------------------------------------------------------------

# Perp venues that do NOT yet publish an identity hook. VIB-6287 fixes
# ``gmx_v2`` ONLY, and this must not be recorded as a perp-wide fix. Each entry
# is (slug, ticket, until) — ``until`` is when the waiver expires and this
# census turns hard-RED, mirroring ``scripts/ci/demo-quarantine.yml``.
#
# These four are lower urgency than gmx_v2 for a MEASURED reason, not a guess:
# gmx_v2 is the only perp venue that writes a perp ``position_registry`` row
# today, so it is the only one whose HOT and WARM enumerations can currently
# disagree. The other four have nothing to collide with — yet. The day any of
# them gets a registry writer, it inherits the identical defect.
_PERP_IDENTITY_WAIVERS: dict[str, tuple[str, date]] = {
    "aster_perps": ("VIB-6287", date(2026, 10, 31)),
    "drift": ("VIB-6287", date(2026, 10, 31)),
    "hyperliquid": ("VIB-6287", date(2026, 10, 31)),
    "pancakeswap_perps": ("VIB-6287", date(2026, 10, 31)),
}


def _perp_connectors():
    return tuple(c for c in CONNECTOR_REGISTRY.all() if c.kind is ProtocolKind.PERP)


def test_the_perp_venue_universe_is_read_from_the_registry_not_a_literal():
    """Guard the guard: the census below must enumerate, not assert a hand list."""
    slugs = {c.name for c in _perp_connectors()}
    assert slugs, "no PERP connectors discovered — the census below would be vacuous"
    assert "gmx_v2" in slugs


@pytest.mark.parametrize("connector", _perp_connectors(), ids=lambda c: c.name)
def test_every_perp_venue_declares_an_identity_hook_or_is_waived(connector):
    """Every PERP venue either publishes an identity hook or holds a live waiver."""
    if connector.perp_identity is not None:
        assert has_perp_identity_hook(connector.name), (
            f"{connector.name} declares perp_identity on its manifest but no hook is "
            "registered — the manifest ref did not hydrate"
        )
        assert connector.name not in _PERP_IDENTITY_WAIVERS, (
            f"{connector.name} now publishes a hook; remove its waiver entry"
        )
        return
    waiver = _PERP_IDENTITY_WAIVERS.get(connector.name)
    assert waiver is not None, (
        f"perp venue {connector.name!r} publishes no identity hook and has no waiver. "
        "Its HOT and WARM enumerations cannot be reconciled, so a teardown can count "
        "one physical position twice (VIB-6287). Add a hook or a dated, ticketed waiver."
    )


def test_no_perp_identity_waiver_has_expired():
    """The waivers above are dated. This goes RED when one runs out of road.

    Deliberately a separate test from the census so an expiring waiver reports
    as "the deadline passed", not as "the venue is unsupported" — the two need
    different responses.
    """
    today = datetime.now(UTC).date()
    expired = sorted(
        f"{slug} ({ticket}, until {until})" for slug, (ticket, until) in _PERP_IDENTITY_WAIVERS.items() if until < today
    )
    assert not expired, f"perp identity waivers expired: {expired} — ship the hook or re-date with a reason"


def test_every_waived_venue_still_exists():
    """A waiver for a venue that no longer exists is dead weight that hides drift."""
    slugs = {c.name for c in _perp_connectors()}
    stale = sorted(set(_PERP_IDENTITY_WAIVERS) - slugs)
    assert not stale, f"waivers name non-existent PERP connectors: {stale}"


# ---------------------------------------------------------------------------
# Census 3 — the hook resolves under every slug the connector can emit
# ---------------------------------------------------------------------------


def test_gmx_hook_resolves_for_every_slug_the_connector_can_emit():
    """VIB-5573: registering by bare ``name`` silently no-ops for connectors whose
    positions carry a different protocol string. A hook that never resolves is
    indistinguishable from no fix at all, so assert every emittable slug."""
    connector = CONNECTOR_REGISTRY.get("gmx_v2")
    assert connector is not None
    slugs = _connector_teardown_slugs(connector)
    assert slugs, "gmx_v2 emits no teardown slugs — this test would be vacuous"
    unresolved = sorted(s for s in slugs if get_perp_identity_hook(s) is None)
    assert not unresolved, f"gmx_v2 identity hook does not resolve for slugs {unresolved}"


# ---------------------------------------------------------------------------
# The derivation
# ---------------------------------------------------------------------------


def test_derivation_reproduces_the_observed_mainnet_venue_key():
    """``keccak(abi.encode(account, market, collateral, isLong))``, checked against
    the key GMX actually used in the run of record."""
    tokens = gmx_v2_perp_identity(_perp(), wallet_address=WALLET)
    assert f"gmx_v2:key:{CHAIN}:{VENUE_KEY}" in tokens


def test_flipping_the_side_changes_the_derived_key():
    """A long and a short in the same market are DIFFERENT positions."""
    long_tokens = gmx_v2_perp_identity(_perp(), wallet_address=WALLET)
    short = _perp(details={"market": MARKET, "collateral_token": USDC, "is_long": False})
    short_tokens = gmx_v2_perp_identity(short, wallet_address=WALLET)
    assert long_tokens.isdisjoint(short_tokens)


def test_an_unmeasured_side_yields_no_token_rather_than_assuming_long():
    """Empty ≠ Zero. Guessing a side would merge a long and a short."""
    no_side = _perp(details={"market": MARKET, "collateral_token": USDC})
    assert gmx_v2_perp_identity(no_side, wallet_address=WALLET) == frozenset()


def test_an_unresolvable_symbol_yields_no_token():
    """A symbol absent from the chain's catalogue must produce NO token — never a
    degraded one. An under-specified token is the only way this design can
    over-collapse, and over-collapse strands funds silently."""
    unknown = _perp(details={"market": "NOTAMARKET/USD", "collateral_token": USDC, "is_long": True})
    assert gmx_v2_perp_identity(unknown, wallet_address=WALLET) == frozenset()


def test_a_residual_is_never_named_by_the_venue():
    """A pending unfilled order (VIB-5116) can name the same market, collateral
    and side as a real open position while BEING a different thing holding its
    own collateral. Naming it would let the union merge the two and suppress
    one — and a suppressed residual is never recovered."""
    residual = _perp(
        position_id="0x" + "cd" * 32,
        details={"market": MARKET, "collateral_token": USDC, "is_long": True, "kind": "pending_order"},
    )
    assert gmx_v2_perp_identity(residual, wallet_address=WALLET) == frozenset()
    # ... and therefore it does not collapse into the position in the union.
    summary = TeardownPositionSummary(
        deployment_id="deployment:test",
        timestamp=datetime.now(UTC),
        positions=[_perp()],
    )
    merged = reconcile_lp_with_registry(
        strategy_summary=summary,
        registry_positions=[residual],
        registry_available=True,
        wallet_for_chain=lambda _c: WALLET,
    )
    assert len(merged.positions) == 2


def test_adoption_needs_no_wallet_and_no_market():
    """The general mechanism: a row that already carries the venue key is named
    by it, even with no market data at all (the backfill folder writes
    ``market: None``) and no account."""
    backfill = _perp(position_id=VENUE_KEY, details={"collateral_token": "USDC", "direction": "long"})
    tokens = gmx_v2_perp_identity(backfill, wallet_address=None)
    assert f"gmx_v2:key:{CHAIN}:{VENUE_KEY}" in tokens


# ---------------------------------------------------------------------------
# Dispatch and opacity
# ---------------------------------------------------------------------------


def test_identity_dispatches_on_the_positions_own_protocol():
    """A hyperliquid row must NOT be named by GMX's hook.

    Calling a connector's hook directly instead of dispatching on
    ``position.protocol`` measures a COLLAPSE that real dispatch splits — the
    designer's own prototype made exactly this mistake, and the failure mode it
    produces is over-collapse, which strands funds silently.
    """
    gmx_row = _perp()
    hl_row = _perp(protocol="hyperliquid", position_id="hl-eth-long")

    assert venue_identity_tokens(gmx_row, WALLET), "gmx row should be named by its venue"
    assert venue_identity_tokens(hl_row, WALLET) == frozenset(), (
        "hyperliquid publishes no hook; naming it would mean GMX's hook was applied to another venue's row"
    )

    summary = TeardownPositionSummary(
        deployment_id="deployment:test",
        timestamp=datetime.now(UTC),
        positions=[gmx_row],
    )
    merged = reconcile_lp_with_registry(
        strategy_summary=summary,
        registry_positions=[hl_row],
        registry_available=True,
        wallet_for_chain=lambda _c: WALLET,
    )
    assert len(merged.positions) == 2


def test_the_framework_never_case_folds_a_venue_token(monkeypatch):
    """Tokens are OPAQUE — the framework compares them and does nothing else.

    ``drift`` keys on a base58 Solana pubkey where ``'B' != 'b'`` is a different
    byte rather than a formatting variant, while the EVM registry writers
    lowercase hex reflexively. No Solana perp path reaches this seam today, so
    this pins a latent trap for the next venue rather than a present bug —
    which is exactly why it needs a test that would fail if someone "tidied"
    a ``.lower()`` back in.
    """
    from almanak.connectors._strategy_base import perp_identity as seam

    mixed = "drift:acct:9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin:0:1"
    assert mixed != mixed.lower()
    monkeypatch.setitem(seam._REGISTRY, "drift", lambda _p, *, wallet_address=None: frozenset({mixed}))

    row = _perp(protocol="drift", chain="solana", position_id="drift-1")
    assert venue_identity_tokens(row, None) == frozenset({mixed})
    keys = _dedupe_keys(row)
    assert any(mixed in key for key in keys), f"token was altered in transit: {keys}"


# ---------------------------------------------------------------------------
# C3 — the completeness widening
# ---------------------------------------------------------------------------


class _CloseIntent:
    """Minimal ``PerpCloseIntent`` stand-in (``completeness._field`` reads attrs)."""

    def __init__(self, market, collateral_token="USDC", is_long=True, protocol="gmx_v2", chain=CHAIN):
        self.intent_type = "PERP_CLOSE"
        self.market = market
        self.collateral_token = collateral_token
        self.is_long = is_long
        self.protocol = protocol
        self.chain = chain


def _registry_row(market=None, collateral=None, position_id=VENUE_KEY) -> PositionInfo:
    """A row shaped exactly as the perp registry read produces on the restart path."""
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=position_id,
        chain=CHAIN,
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={
            "source": "position_registry",
            "market": (market or MARKET).lower(),
            "collateral_token": (collateral or USDC).lower(),
            "direction": "long",
        },
    )


@pytest.mark.xfail(
    strict=True,
    reason="completeness.py calls venue_identity_tokens(position) with NO wallet, so a keyed "
    "row now emits only its venue key and cannot match a symbol-space intent. The two lanes "
    "resolve identity by different rules; threading the wallet into the completeness lane is "
    "the follow-up. Inert today — _covers_perp rejects the pair one layer earlier.",
)
def test_c3_identity_layer_now_matches_across_value_spaces():
    """``_perp_carries_identity`` credits a SYMBOL intent against an ADDRESS
    position — the widening itself works at its own layer."""
    assert _perp_carries_identity(_CloseIntent("ETH/USD"), _registry_row())


def test_c3_identity_layer_is_strictly_wider_than_the_raw_compare(monkeypatch):
    """Disable the venue seam and the raw comparison must behave exactly as it
    did before. A coverage check that got STRICTER would turn working teardowns
    into FAILED — a regression that reads as an improvement."""
    from almanak.connectors._strategy_base import perp_identity as seam

    symbol_row = PositionInfo(
        position_type=PositionType.PERP,
        position_id="gmx-ETH/USD-arbitrum",
        chain=CHAIN,
        protocol="gmx_v2",
        value_usd=Decimal("6"),
        details={"market": "ETH/USD", "collateral_token": "USDC", "is_long": True},
    )
    intent = _CloseIntent("ETH/USD")
    assert _perp_carries_identity(intent, symbol_row)
    monkeypatch.setitem(seam._REGISTRY, "gmx_v2", lambda _p, *, wallet_address=None: frozenset())
    assert _perp_carries_identity(intent, symbol_row), "the raw market compare regressed"
    # ... and the widened path is genuinely what carries the cross-space case,
    # i.e. this is not vacuously green.
    assert not _perp_carries_identity(intent, _registry_row())


@pytest.mark.parametrize(
    ("label", "intent"),
    [
        ("different market", _CloseIntent("BTC/USD")),
        ("different collateral", _CloseIntent("ETH/USD", collateral_token="USDT")),
        ("opposite side", _CloseIntent("ETH/USD", is_long=False)),
        ("different protocol", _CloseIntent("ETH/USD", protocol="hyperliquid")),
    ],
)
def test_c3_never_credits_an_intent_that_does_not_name_this_position(label, intent):
    """The negative controls. Crediting an intent that does NOT close the position
    reports a teardown complete while money is still on-chain — the one direction
    this widening must never fail in, and the reason it is ``raw OR alias`` and
    never a replacement."""
    assert not _perp_carries_identity(intent, _registry_row()), f"falsely credited on: {label}"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "MEASURED, NOT SPECULATIVE (VIB-6287). C3's widening of "
        "_perp_carries_identity is INERT for the restart path, because the "
        "rejection happens one layer EARLIER:\n"
        "    _intent_type      -> PERP_CLOSE\n"
        "    _chain_compatible -> True\n"
        "    _covers           -> False   <-- rejects here\n"
        "    _perp_carries_id  -> True    <-- the widening works, never runs\n"
        "_position_is_covered computes `covering = [i for i in intents if "
        "_covers(i, position)]` and returns False when that list is empty, so "
        "_intent_carries_position_identity is never consulted. THE REJECTING "
        "LINE IS completeness.py:806-811 (_covers_perp), whose last clause is "
        "_lenient_identity_match(intent, position, 'market', ('market',)) — "
        "lenient only when a side OMITS its market; with BOTH sides naming one "
        "it requires equality. Here the registry row's details['market'] is "
        "'0x70d95587d40a2caf56bd97485ab3eec10bee6336' while the regenerated "
        "PerpCloseIntent.market is 'ETH/USD'. The identical symbol-vs-address "
        "polysemy as the enumeration union, one layer up.\n"
        "NOT FIXED HERE, DELIBERATELY (team-lead decision, option 2): widening "
        "_covers_perp means widening the gate that decides whether a teardown "
        "reports SUCCESS, on a money path, with no real-fork proof available — "
        "the mainnet A/B exercises the main path, not the restart path, and "
        "managed-Anvil cannot open a GMX position at all because its keeper is "
        "unreachable from the production lane (VIB-6288). Over-credit is the one "
        "direction with no alarm, and US-017 exists because fixture-green hid "
        "VIB-4983. This test is STRICT so it flips to a FAILURE the moment "
        "_covers_perp is widened — at that point delete the xfail, not the test."
    ),
)
def test_c3_restart_path_end_to_end_is_still_broken():
    """The end-to-end restart case C3 was scoped to fix. Still red — on purpose.

    Pinned as a strict xfail rather than deleted so the tree records that the
    restart path is NOT fixed. An inert fix that reads as green is the exact
    failure class US-017 exists to catch.
    """
    report = check_intent_coverage([_registry_row()], [_CloseIntent("ETH/USD")])
    assert report.complete


def test_covers_is_the_layer_that_rejects_the_restart_pair():
    """``_covers`` is what rejects the restart pair, one layer before identity.

    SPLIT OUT OF A STRICT XFAIL, because under it this claim was never measured.
    A strict xfail is satisfied by the FIRST failing assertion, and the identity
    assertion it used to sit behind fails today — so this line never executed
    while its docstring claimed it pinned the rejecting layer. An assertion that
    cannot run is not evidence (found by the #3534 panel).

    Runs and must stay green until ``_covers_perp`` is widened, at which point it
    flips and forces the change to be acknowledged rather than absorbed.
    """
    assert not _covers(_CloseIntent("ETH/USD"), _registry_row())


@pytest.mark.xfail(
    strict=True,
    reason="completeness.py calls venue_identity_tokens(position) with NO wallet, so a keyed "
    "row now emits only its venue key and cannot match a symbol-space intent. The two lanes "
    "resolve identity by different rules; threading the wallet into the completeness lane is "
    "the follow-up (VIB-6316).",
)
def test_the_identity_layer_would_agree_if_the_lanes_shared_a_wallet():
    """The identity-layer half of the old combined test, alone under the xfail.

    Records that the enumeration and completeness lanes disagree by construction:
    enumeration threads a wallet, completeness does not. Flips green the moment
    VIB-6316 makes them ask the venue the same question.
    """
    assert _perp_carries_identity(_CloseIntent("ETH/USD"), _registry_row())


# ---------------------------------------------------------------------------
# The catalogue defect this fix deliberately does NOT touch
# ---------------------------------------------------------------------------


def test_gmx_market_catalogue_is_injective_within_each_chain():
    """Derivation is chain-scoped, so a WITHIN-chain duplicate would be the thing
    that makes two distinct positions resolve to one identity. The known
    cross-chain duplicate (VIB-6155: ``arbitrum:AVAX/USD`` holds the Avalanche
    ETH/USD address) is deliberately NOT fixed here and cannot cause a collapse,
    because the chain is part of every key."""
    for chain, table in GMX_V2_MARKETS.items():
        addresses = [a.lower() for a in table.values()]
        assert len(addresses) == len(set(addresses)), f"{chain} market catalogue is no longer injective"


# ---------------------------------------------------------------------------
# The seam must validate the CONTAINER, not only the elements (#3534 panel)
# ---------------------------------------------------------------------------


def _probe_row(protocol: str) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id="probe-1",
        chain="arbitrum",
        protocol=protocol,
        value_usd=Decimal("0"),
        details={},
    )


@pytest.mark.parametrize(
    "returned",
    ["gmx_v2:key:arbitrum:0xdead", b"gmx_v2:key:arbitrum", 42, {"a": 1}],
    ids=["str", "bytes", "int", "dict"],
)
def test_a_hook_returning_a_non_collection_names_nothing(monkeypatch, returned):
    """A ``str`` is iterable, so element-wise validation alone is not enough.

    ``frozenset(t for t in "gmx_v2:key:...")`` yields SINGLE CHARACTERS, every one
    a non-empty ``str``, so an element-only check accepts them. That is the most
    likely authoring mistake against a ``-> frozenset[str]`` contract: returning
    the token instead of a set containing it.

    The consequence is the forbidden direction. Every position on that venue would
    share tokens like ``"x"``, ``":"``, ``"e"``, so the enumeration's disjointness
    test never succeeds and EVERY registry-derived position after the first is
    suppressed. Nothing builds a closing intent for a suppressed row, and nothing
    raises — teardown reports success over stranded funds.

    ``dict`` is included deliberately: it is a legal collection whose iteration
    yields KEYS, so accepting "any iterable" would still be wrong.
    """
    from almanak.connectors._strategy_base import perp_identity as registry

    monkeypatch.setitem(registry._REGISTRY, "probe_venue", lambda position, wallet_address=None: returned)

    assert venue_identity_tokens(_probe_row("probe_venue")) == frozenset(), (
        "a malformed hook return must name NOTHING (unmeasured -> raw position_id -> "
        "over-split, loud), never decompose into tokens that collapse every row"
    )


def test_a_conforming_hook_is_still_honoured():
    """Non-vacuity control for the guard above.

    Without this, tightening the container check to reject everything would pass
    the test above while silently disabling venue identity entirely — which is the
    over-split direction, loud, but still a total loss of the fix.
    """
    row = PositionInfo(
        position_type=PositionType.PERP,
        position_id="0x" + "ab" * 32,
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={
            "market": GMX_V2_MARKETS["arbitrum"]["ETH/USD"],
            "collateral_token": GMX_V2_TOKENS["arbitrum"]["USDC"],
            "is_long": True,
        },
    )
    tokens = venue_identity_tokens(row)
    assert tokens, "the real gmx_v2 hook must still name a conforming row"
    assert all(len(t) > 1 for t in tokens), "tokens must be whole strings, not characters"


# ---------------------------------------------------------------------------
# details["side"] is what SHIPPED producers write (#3534 panel)
# ---------------------------------------------------------------------------


def test_the_side_key_shipped_demos_actually_write_is_read():
    """`gmx_v2_directional_perp` and `hyperliquid_trailing_perp` both emit
    ``details = {"market", "side", "size_usd"}``. ``_side`` originally read
    ``is_long`` / ``position.direction`` / ``details["direction"]`` — not
    ``"side"`` — so the side was UNMEASURED for the very demos this fix serves,
    and no ``sem`` token was emitted.

    Pinned against the real demo's key name rather than a plausible one; that
    distinction is what this whole ticket is about.
    """
    row = PositionInfo(
        position_type=PositionType.PERP,
        position_id="gmx-v2-ETH/USD-long",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={
            "market": GMX_V2_MARKETS["arbitrum"]["ETH/USD"],
            "collateral_token": GMX_V2_TOKENS["arbitrum"]["USDC"],
            "side": "long",
        },
    )
    tokens = gmx_v2_perp_identity(row, wallet_address=None)
    assert any(":sem:" in t and t.endswith(":long") for t in tokens), (
        f"details['side'] was not read; got {sorted(tokens)}"
    )


def test_vib6155_the_catalogue_still_maps_an_arbitrum_market_to_an_avalanche_address():
    """Pin the constant defect itself — the only part provable from in-tree data.

    ``GMX_V2_MARKETS["arbitrum"]["AVAX/USD"]`` holds the **Avalanche ETH/USD**
    market address. Chain-scoped resolution means it can never route an Arbitrum
    row to an Avalanche identity, so OVER-COLLAPSE is impossible — verified
    separately by the injectivity test above.

    What it does break: a row in SYMBOL space resolves ``"AVAX/USD"`` through this
    wrong entry, while a row in ADDRESS space carries whatever address the chain
    actually reports for the Arbitrum AVAX market. Those differ, the ``sem`` tokens
    are disjoint, and one position enumerates as two — over-split, loud. The DERIVE
    key is computed from the wrong market too, naming a position that does not
    exist, which violates this module's emit-only-with-CERTAINTY contract.

    METHOD NOTE, because it cost a wrong test: the obvious way to write this is a
    symbol row plus an "address" row built from ``GMX_V2_MARKETS[...]["AVAX/USD"]``.
    That does NOT reproduce the defect — both sides then resolve through the same
    wrong entry and agree, which is exactly the (true, but irrelevant) claim the old
    module docstring made. Reproducing it needs the address the CHAIN reports, which
    is not in this tree. So this test asserts the constant, not the consequence, and
    says so rather than fabricating an address to make a stronger-looking assertion.

    Fails when VIB-6155 corrects the constant — at which point delete it.
    """
    arbitrum_avax = GMX_V2_MARKETS["arbitrum"]["AVAX/USD"].lower()
    avalanche_eth = GMX_V2_MARKETS["avalanche"]["ETH/USD"].lower()
    assert arbitrum_avax == avalanche_eth, (
        "VIB-6155 appears fixed — delete this test and the KNOWN CATALOGUE DEFECT "
        "paragraph in almanak/connectors/gmx_v2/perp_identity.py"
    )


# ---------------------------------------------------------------------------
# The identity hook must accept every market spelling the COMPILER accepts
# ---------------------------------------------------------------------------


def _usd_market_symbols(chain: str) -> list[str]:
    """Catalogue keys of the `<SYM>/USD` shape, which is what the aliases target."""
    return [s for s in GMX_V2_MARKETS[chain] if s.upper().endswith("/USD")]


@pytest.mark.parametrize("chain", sorted(GMX_V2_MARKETS))
def test_every_market_alias_the_compiler_accepts_also_resolves_for_identity(chain):
    """An identity hook that accepts FEWER spellings than the execution path is inert.

    `GMXV2SDK.get_market_address` resolves `ETH`, `WETH`, `BTC`, `WBTC` — and
    `AVAX`/`WAVAX` where wired — case-insensitively. A strategy writing
    ``details["market"] = "ETH"`` opens a real position, so if this hook only
    recognised ``ETH/USD`` the HOT row stayed unnamed while the registry row
    carried the resolved address: they never intersect and the duplicate
    enumeration survives. VIB-6287 would be fixed for `ETH/USD` strategies and
    silently inert for `ETH` ones.

    DERIVED from the catalogue, not enumerated: for every ``<SYM>/USD`` key the
    bare ``<SYM>`` and wrapped ``W<SYM>`` forms must resolve to the SAME address.
    A market added to the catalogue is covered automatically — there is no alias
    list here to fall out of step with the SDK's.
    """
    symbols = _usd_market_symbols(chain)
    assert symbols, f"{chain} has no <SYM>/USD markets — this census would be vacuous"

    for canonical in symbols:
        base = canonical.split("/")[0]
        expected = _resolve_address(GMX_V2_MARKETS, chain, canonical)
        assert expected, f"{chain}:{canonical} does not resolve at all"
        for alias in (base, base.lower(), f"W{base}", f"w{base.lower()}"):
            assert _resolve_address(GMX_V2_MARKETS, chain, alias) == expected, (
                f"{chain}: alias {alias!r} does not resolve to the same market as "
                f"{canonical!r}; the compiler accepts it but the identity hook does not, "
                "so positions opened with that spelling keep the VIB-6287 duplicate"
            )


def test_an_alias_that_names_no_market_still_yields_nothing():
    """Non-vacuity control: alias resolution must not become a guess.

    The alias fallback appends ``/USD`` and strips a leading ``W``. Neither may
    turn an unknown symbol into a market — emitting a token for a market the
    position is not in would violate the emit-only-with-CERTAINTY contract, and an
    under-specified token is the over-collapse direction.
    """
    catalogued = {sym.upper() for sym in GMX_V2_MARKETS["arbitrum"]}
    absent = ("ZZZZ", "WZZZZ", "NOTAMARKET", "W", "/USD", "")
    # Guard the guard: these must genuinely not be markets, or the control is vacuous.
    # (An earlier version used "DOGE", which IS an Arbitrum market — this assertion is
    # what caught that.)
    for symbol in absent:
        assert f"{symbol.upper()}/USD" not in catalogued and symbol.upper() not in catalogued, (
            f"{symbol!r} is a real market — pick a symbol that is not, or this test proves nothing"
        )

    for unknown in absent:
        assert _resolve_address(GMX_V2_MARKETS, "arbitrum", unknown) is None, unknown


def test_a_row_whose_id_and_details_name_different_positions_names_nothing():
    """One row may never name TWO positions (#3534 panel blocker).

    ADOPT reads the venue key off ``position_id``; DERIVE recomputes one from
    ``details``. When they disagree the row is internally inconsistent, and
    emitting both makes it a BRIDGE that merges two distinct positions under the
    transitive closure — suppressing one, silently.

    Refusing costs nothing real: the row falls through to its raw ``position_id``,
    i.e. over-split, loud, and no worse than `main`.
    """
    wallet = "0xafeB2f5c213b5e7F37c3Fc171dfCb6270d07e21a"
    chain = "arbitrum"
    # An id that is a well-formed venue key but NOT the one these details derive.
    unrelated_key = "0x" + "1e" * 32

    row = PositionInfo(
        position_type=PositionType.PERP,
        position_id=unrelated_key,
        chain=chain,
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={
            "market": GMX_V2_MARKETS[chain]["ETH/USD"],
            "collateral_token": GMX_V2_TOKENS[chain]["USDC"],
            "is_long": True,
        },
    )
    tokens = gmx_v2_perp_identity(row, wallet_address=wallet)
    assert tokens == frozenset({f"gmx_v2:key:{chain}:{unrelated_key}"}), sorted(tokens)
    assert not any(":sem:" in t for t in tokens), (
        "the semantic token is what would bridge this row to the position its details name"
    )


def test_a_consistent_row_is_still_named():
    """Non-vacuity control: refusing must be scoped to DISAGREEMENT.

    If the guard rejected whenever both mechanisms fired, every well-formed WARM
    row would go unnamed and the fix would be inert — over-split, loud, but a total
    loss. The real mainnet key is used so this cannot pass on a fabricated shape.
    """
    wallet = "0xafeB2f5c213b5e7F37c3Fc171dfCb6270d07e21a"
    chain = "arbitrum"
    real_key = "0xbf58e0307a44a17ea51e30850651f5269c9fc0f306990576c015e9a88ac9bafa"

    row = PositionInfo(
        position_type=PositionType.PERP,
        position_id=real_key,
        chain=chain,
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={
            "market": GMX_V2_MARKETS[chain]["ETH/USD"],
            "collateral_token": GMX_V2_TOKENS[chain]["USDC"],
            "is_long": True,
        },
    )
    tokens = gmx_v2_perp_identity(row, wallet_address=wallet)
    assert any(t.endswith(real_key) for t in tokens), sorted(tokens)
    assert any(":sem:" in t for t in tokens), sorted(tokens)


@pytest.mark.parametrize("chain", sorted(GMX_V2_MARKETS))
def test_the_w_prefix_alias_can_never_name_a_different_real_market(chain):
    """The alias fallback must fail to resolve, never resolve to something WRONG.

    Resolution tries the exact catalogue key, then ``<SYM>/USD``, then — for a
    ``W``-prefixed input — ``<SYM stripped of W>/USD``. That last step is a guess
    about wrapped-token spelling, and a guess that lands on a REAL market emits a
    token with false certainty for a position the row is not in. Under the
    transitive closure a wrong token is the over-collapse direction: the strand.

    The strip is safe only while no catalogue symbol is another catalogue symbol
    with a leading ``W``. Today nothing on either chain starts with ``W``, so the
    step is unreachable for catalogued names and only ever serves ``WETH``/``WBTC``/
    ``WAVAX``. This asserts that PROPERTY rather than the current symbol list, so
    adding a market like ``WIF/USD`` alongside ``IF/USD`` fails here instead of
    silently making ``WIF`` resolve to ``IF``.

    The exact-match-first ordering is a second line of defence — but only for the
    pair that exists; it does not save the ``WIF`` case if ``WIF/USD`` is absent.
    """
    symbols = {s.upper() for s in GMX_V2_MARKETS[chain]}
    for symbol in symbols:
        base = symbol.split("/")[0]
        if not base.startswith("W"):
            continue
        stripped = f"{base[1:]}/USD"
        assert stripped not in symbols, (
            f"{chain}: {base!r} and {stripped!r} both exist, so stripping the leading "
            "'W' is ambiguous and can name the wrong market — narrow the alias rule "
            "before adding this pair"
        )


def test_an_unverifiable_keyed_row_emits_only_its_venue_key():
    """A row may emit at most one identity family (#3534 panel blocker).

    The disagreement check needs a wallet to compute DERIVE. Without one, a row
    carrying an ADOPT key AND a ``sem`` token asserts that ``position_id`` and
    ``details`` name the same position with nothing having verified it — and if they
    disagree it BRIDGES two distinct positions under the transitive closure. The
    panel executed it: three registry rows describing two positions enumerate as ONE
    with no wallet, where `main` gives three.

    So the row emits only its adopted key: it names itself with the venue's own
    authoritative identity without vouching for unchecked attributes.

    NOT ``frozenset()`` — that was my first attempt and it is a different bug. A
    refused row does not fall through to the raw ``position_id``; ``_dedupe_keys`` is
    venue -> defaults -> raw, so it lands in the DEFAULT namespace where it can no
    longer intersect any venue-named row, manufacturing a duplicate `main` does not
    have.
    """
    chain = "arbitrum"
    key = "0x" + "1e" * 32
    row = PositionInfo(
        position_type=PositionType.PERP,
        position_id=key,
        chain=chain,
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={
            "market": GMX_V2_MARKETS[chain]["ETH/USD"],
            "collateral_token": GMX_V2_TOKENS[chain]["USDC"],
            "is_long": True,
        },
    )
    tokens = gmx_v2_perp_identity(row, wallet_address=None)
    assert tokens == frozenset({f"gmx_v2:key:{chain}:{key}"}), sorted(tokens)


def test_a_keyless_row_still_emits_its_semantic_token():
    """Non-vacuity control: the narrowing is scoped to rows that carry a venue key.

    A strategy row has no venue key, so there is nothing for its attributes to
    disagree WITH — it must still emit ``sem``, or the symbol side of the mainnet
    pair goes unnamed and VIB-6287 is unfixed.
    """
    chain = "arbitrum"
    row = PositionInfo(
        position_type=PositionType.PERP,
        position_id="eth-long",
        chain=chain,
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={"market": "ETH/USD", "collateral_token": "USDC", "is_long": True},
    )
    tokens = gmx_v2_perp_identity(row, wallet_address=None)
    assert any(":sem:" in t for t in tokens), sorted(tokens)


# ---------------------------------------------------------------------------
# The multi-alias emission contract is not enforceable by the framework.
# Until VIB-6329 lands, GMX is the only hook with a structural proof, and that
# is held by a CI gate rather than by a promise. Raised as a BLOCKER by the
# #3534 refresh panel (Grok B1).
# ---------------------------------------------------------------------------

# Venues whose identity hook has been shown to emit at most one identity FAMILY
# unless the venue itself verified that both name the same position. GMX proves
# it by re-deriving the venue key with keccak and refusing to emit `sem`
# alongside an unverified adopted key (`gmx_v2/perp_identity.py`).
#
# This is NOT a list to extend. It is the subject of an assertion that fails
# when a hook appears without such a proof — adding a slug here without the
# proof is the defect the test exists to catch.
_HOOKS_WITH_A_MULTI_ALIAS_SAFETY_PROOF = frozenset({"gmx_v2"})


def test_no_hook_may_ship_multi_alias_identity_without_a_safety_proof():
    """A second identity hook must not ship before VIB-6329 enforces the contract.

    THE HAZARD. ``_dedupe_keys`` treats two rows as the same position when their
    alias sets INTERSECT, and ``reconcile_lp_with_registry`` closes that relation
    TRANSITIVELY. So a single hook returning ``{key_A, sem_B}`` for one row — an
    unverified multi-family emission — bridges two physically distinct positions.
    Union-find merges their components, every registry row in the component but
    one is suppressed, nothing builds a closing intent for the suppressed row,
    and nothing raises. Silently stranded funds.

    WHY A TEST AND NOT A DOCSTRING. The emission contract in
    ``_strategy_base/perp_identity.py`` is prose, and ``venue_identity_tokens``
    validates only the container type and non-empty strings — it cannot check
    that two tokens name one position, because only the venue knows that. The
    contract was already violated three separate ways during VIB-6287's own
    review, by its author, while looking at it. Prose did not hold then and will
    not hold for the next author.

    WHAT THIS BUYS, STATED HONESTLY AND NARROWLY. It forces a DELIBERATE EDIT.
    Registering a second hook turns CI red until someone adds the slug below.

    WHAT IT DOES NOT BUY, because an earlier version of this docstring claimed it
    did: it is **not** "the only thing standing between a new hook and a silent
    strand", and it does not verify any safety property. Adding a slug below
    while shipping a hook that returns ``{key_A, sem_B}`` with no keccak check
    passes this gate and strands funds exactly as before. It is a speed bump with
    a written rationale, not a proof — the panel called this out as an
    honor-system allowlist and was right to.

    The mechanical guard is VIB-6329: reject ``|tokens| > 1`` at the framework
    seam unless the hook satisfies a verification protocol (e.g. ``derive(key) ==
    adopt``). Until that lands, a second hook needs a human reading this file, not
    a green check.

    DERIVED, NOT ENUMERATED. The live set comes from the hook registry, so a new
    connector is caught by registering at all; there is no list to keep in step.
    """
    registered = registered_perp_identity_protocols()
    unproven = set(registered) - _HOOKS_WITH_A_MULTI_ALIAS_SAFETY_PROOF
    assert not unproven, (
        f"perp identity hook(s) {sorted(unproven)} registered without a multi-alias safety proof.\n\n"
        "The teardown enumeration merges rows whose alias sets intersect, and closes that\n"
        "relation transitively. A hook that emits two tokens naming DIFFERENT physical\n"
        "positions therefore suppresses one of them with no alarm — stranded funds.\n\n"
        "Before adding a slug here, the hook must either:\n"
        "  (a) emit at most ONE token, or\n"
        "  (b) emit several only when the VENUE has verified they name one position\n"
        "      (GMX re-derives the venue key with keccak and refuses to emit `sem`\n"
        "      beside an unverified adopted key).\n\n"
        "VIB-6329 replaces this gate with a framework-side guard that CHECKS the property.\n"
        "This assertion does not: adding your slug above satisfies it whether or not the\n"
        "proof exists. It buys a deliberate edit and a human reading this message — that\n"
        "is all. If you cannot state which of (a) or (b) your hook satisfies, do not edit\n"
        "the set; land VIB-6329 first."
    )


def test_the_multi_alias_proof_gate_is_not_vacuous():
    """Guard the guard: the gate must fail on the very thing it exists to catch.

    A census that cannot fail is decoration. This registers a throwaway hook and
    asserts the gate above rejects it, then restores the registry — so the gate
    is proven to discriminate rather than merely to pass today.
    """
    from almanak.connectors._strategy_base import perp_identity as _pi

    slug = "vib6329_unproven_probe"
    assert slug not in registered_perp_identity_protocols()
    _pi._register_perp_identity(slug, lambda position, *, wallet_address: frozenset())
    try:
        assert slug in registered_perp_identity_protocols()
        with pytest.raises(AssertionError, match="without a multi-alias safety proof"):
            test_no_hook_may_ship_multi_alias_identity_without_a_safety_proof()
    finally:
        _pi._REGISTRY.pop(slug, None)
    assert slug not in registered_perp_identity_protocols()
