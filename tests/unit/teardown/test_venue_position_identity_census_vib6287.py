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
from almanak.connectors.gmx_v2.addresses import GMX_V2_TOKENS
from almanak.connectors.gmx_v2.perp_identity import _address_only, gmx_v2_perp_identity
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
from tests.unit.connectors.gmx_v2.market_fixtures import FIXTURE_MARKETS, market_address

CHAIN = "arbitrum"
MARKET = market_address(CHAIN, "ETH/USD")
BTC_MARKET = market_address(CHAIN, "BTC/USD")
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
    """A row shaped exactly as the perp registry read produces on the restart path.

    Address-first: the runtime writer persists ``intent.market`` verbatim, and
    strategies now supply the market-token ADDRESS on the intent, so the stored
    ``market`` is an address.
    """
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


def _adapter_display_row() -> PositionInfo:
    """The teardown-lane discovery row, built by the REAL adapter producer.

    ``GMXv2Adapter.get_positions_as_teardown_summary`` emits the one remaining
    cross-representation shape in the address-first world: ``position_id`` is
    the venue key, ``details["market"]`` holds the catalog LABEL (display only
    — the catalog is primed here exactly as a live compile's dynamic market
    resolution would have primed it), and ``details["market_address"]``
    carries the identity axis. Only the chain read is stubbed.
    """
    from datetime import UTC as _UTC
    from datetime import datetime as _datetime

    from almanak.connectors.gmx_v2.adapter import GMXv2Adapter, GMXv2Config, GMXv2Position
    from tests.unit.connectors.gmx_v2.market_fixtures import market_record, prime_catalog

    prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")
    adapter = GMXv2Adapter(GMXv2Config(chain=CHAIN, wallet_address=WALLET))
    onchain = GMXv2Position(
        position_key=VENUE_KEY,
        market=MARKET,
        collateral_token=USDC,
        size_in_usd=Decimal("6.0"),
        size_in_tokens=Decimal("0.003218709779491469"),
        collateral_amount=Decimal("2.9964"),
        entry_price=Decimal("1864.10"),
        is_long=True,
        leverage=Decimal("2"),
        last_updated=_datetime.now(_UTC),
    )
    adapter.get_positions_onchain = lambda **_kwargs: [onchain]  # type: ignore[method-assign]
    summary = adapter.get_positions_as_teardown_summary(deployment_id="deployment:test")
    row = summary.positions[0]
    assert row.details["market"] == "ETH/USD", "fixture precondition: catalog label must be the display name"
    return row


def test_c3_identity_layer_matches_the_regenerated_address_intent():
    """The C3 restart pairing is address ↔ address now — no wallet required.

    SUCCESSOR of ``test_c3_identity_layer_now_matches_across_value_spaces``
    (VIB-6316), rewritten for the address-first contract: symbol→address market
    resolution was deliberately removed ("start with the address"), so the
    restart pairing this layer was widened for — registry ADDRESS row vs
    regenerated SYMBOL intent — no longer exists. The regenerated intent
    carries the address the strategy supplied, and the raw market comparison
    credits it directly.

    A LEGACY symbol intent is refused even with the wallet: the market axis has
    no symbol resolution, so the split stays loud/fail-safe. Legacy
    symbol-shaped state is a repair-CLI migration case
    (``almanak/framework/cli/repair_position_references.py``).
    """
    assert _perp_carries_identity(_CloseIntent(MARKET), _registry_row(), WALLET)
    assert _perp_carries_identity(_CloseIntent(MARKET), _registry_row()), (
        "address ↔ address needs no wallet — the raw comparison already agrees"
    )
    assert not _perp_carries_identity(_CloseIntent("ETH/USD"), _registry_row(), WALLET), (
        "a legacy symbol intent must stay uncredited: over-split is loud, "
        "resolving through a curated table is the silent-disagreement hazard "
        "VIB-6155 proved"
    )


def test_c3_identity_layer_is_unmeasured_without_a_wallet():
    """A legacy SYMBOL intent is refused with or without a wallet (Empty ≠ Zero).

    Historically this pinned that the VIB-6316 wallet threading was
    load-bearing for the symbol↔address pairing. Address-first removed that
    pairing entirely: a symbol-shaped intent emits no venue probe token, so
    nothing credits it regardless of the wallet — the fail-safe over-split
    direction. Kept as the no-wallet control for the legacy refusal.
    """
    assert not _perp_carries_identity(_CloseIntent("ETH/USD"), _registry_row())


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
    never a replacement.

    Asserts BOTH paths: with ``WALLET`` (the production alias arm after VIB-6316)
    and without (the pre-VIB-6316 inertness baseline). Wallet-free alone is not a
    proof that the alias path rejects wrong market/side/protocol/collateral — a
    registry row without a wallet never emits the alias token, so every negative
    would vacuously pass.
    """
    assert not _perp_carries_identity(intent, _registry_row(), WALLET), f"alias path falsely credited on: {label}"
    assert not _perp_carries_identity(intent, _registry_row()), f"wallet-free path falsely credited on: {label}"


def test_c3_restart_path_end_to_end_is_fixed():
    """The end-to-end restart case C3 was scoped to fix, in address space.

    Fixed by VIB-6316 for the symbol↔address pairing (R5, mainnet arbitrum,
    2026-08-02, `docs/internal/gmx-readiness/r5-20260802/`); rewritten for the
    address-first contract, under which the restart pairing is address↔address:
    the regenerated close intent carries the market-token address the strategy
    supplied, and the gate credits the registry row without needing venue
    corroboration.

    The LEGACY half is asserted too: a symbol-shaped regenerated intent — the
    exact pre-migration shape — now leaves the registry row UNCOVERED even with
    the wallet resolved. That is deliberate fail-safe over-split (loud FAILED,
    never a silent strand); legacy symbol state is migrated by the repair CLI
    (``almanak/framework/cli/repair_position_references.py``), not resolved
    through a curated table (VIB-6155).
    """
    report = check_intent_coverage([_registry_row()], [_CloseIntent(MARKET)], wallet_for_chain=lambda _chain: WALLET)
    assert report.complete, f"uncovered after a full close: {report.uncovered}"
    legacy = check_intent_coverage([_registry_row()], [_CloseIntent("ETH/USD")], wallet_for_chain=lambda _chain: WALLET)
    assert not legacy.complete, (
        "a legacy symbol-space close must fail the gate LOUDLY — crediting it would "
        "require the symbol resolution this migration deleted"
    )


def test_c3_restart_path_without_a_wallet_still_reports_uncovered():
    """The no-wallet control for the legacy symbol-space lane.

    Not a defect: ``None`` is UNMEASURED, and falling back to the raw comparison
    is the fail-SAFE direction (a split reported loudly beats a strand reported
    as success). Under address-first the legacy symbol intent stays uncovered
    with a wallet too (see the end-to-end test above); this pins that removing
    the resolver never flips the answer.
    """
    assert not check_intent_coverage([_registry_row()], [_CloseIntent("ETH/USD")]).complete


def test_covers_is_the_layer_that_rejected_the_restart_pair():
    """``_covers`` is what rejected the restart pair, one layer before identity.

    SPLIT OUT OF A STRICT XFAIL, because under it this claim was never measured.
    A strict xfail is satisfied by the FIRST failing assertion, and the identity
    assertion it used to sit behind failed — so this line never executed while
    its docstring claimed it pinned the rejecting layer. An assertion that cannot
    run is not evidence (found by the #3534 panel).

    ITS OWN PREDICTION WAS WRONG, AND THE CORRECTION IS THE POINT. The docstring
    said this "must stay green until ``_covers_perp`` is widened, at which point
    it flips". ``_covers_perp`` IS widened (VIB-6316) and this is still green —
    because the widening's alias arm needs a WALLET, and this call passes none.
    So it did not flip, and nothing forced anyone to acknowledge the change. Left
    green deliberately as the no-wallet inertness control, with the prediction
    corrected rather than deleted: a tripwire whose trigger condition was
    mis-stated is worth recording, because the next one will be written the same
    way. The assertion that actually moves is
    ``test_c3_restart_path_end_to_end_is_fixed``.
    """
    assert not _covers(_CloseIntent("ETH/USD"), _registry_row())


def test_the_identity_layer_agrees_now_that_the_lanes_share_a_wallet():
    """The two lanes now ask the venue the SAME question.

    Recorded that the enumeration and completeness lanes disagreed *by
    construction* — enumeration threaded a wallet, completeness did not — which
    is the whole of VIB-6316. Both now resolve through
    ``_teardown_wallet_for_chain``; using two different resolvers would put the
    disagreement back one level up, which is why the production wiring is
    asserted structurally in
    ``test_completeness_venue_identity_vib6316.py::test_n3_n4_*``.

    Address-first update: the registry restart pairing is address↔address and
    no longer needs the wallet, so the pairing that still EXERCISES it is the
    adapter's discovery row — venue key id, catalog LABEL under ``market``
    (display), address under ``market_address``. Only with the wallet can the
    venue corroborate (derive == adopt) and emit the ``sem`` token the intent
    probe intersects; without it the row is key-only and the raw label-vs-
    address comparison correctly refuses.
    """
    row = _adapter_display_row()
    assert _perp_carries_identity(_CloseIntent(MARKET), row, WALLET)
    assert not _perp_carries_identity(_CloseIntent(MARKET), row), (
        "wallet-less must stay unmeasured for the label-display row — the venue "
        "cannot derive its own key without the account"
    )


# ---------------------------------------------------------------------------
# Identity collapse is a WITHIN-chain property
# ---------------------------------------------------------------------------


def test_gmx_market_catalogue_is_injective_within_each_chain():
    """Derivation is chain-scoped, so a WITHIN-chain duplicate would be the thing
    that makes two distinct positions resolve to one identity.

    This stays chain-scoped on purpose even though VIB-6155 has since corrected
    the catalogue. A cross-chain duplicate cannot collapse an identity here,
    because the chain is part of every key — and it is not inherently a defect
    either, since CREATE2 legitimately lands the same address on two chains.
    Whether each address IS the market it claims to be is a different question
    that no uniqueness assert can answer; it is settled on-chain against
    ``Reader.getMarket`` in ``tests/audit/test_gmx_v2_market_identity.py``."""
    for chain in sorted({c for c, _ in FIXTURE_MARKETS}):
        addresses = [r.market_token.lower() for c, r in FIXTURE_MARKETS if c == chain]
        assert len(addresses) == len(set(addresses)), f"{chain} fixture market snapshot is no longer injective"


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
            "market": market_address("arbitrum", "ETH/USD"),
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
            "market": market_address("arbitrum", "ETH/USD"),
            "collateral_token": GMX_V2_TOKENS["arbitrum"]["USDC"],
            "side": "long",
        },
    )
    tokens = gmx_v2_perp_identity(row, wallet_address=None)
    assert any(":sem:" in t and t.endswith(":long") for t in tokens), (
        f"details['side'] was not read; got {sorted(tokens)}"
    )


def test_vib6155_is_fixed_and_its_guard_is_deliberately_not_in_this_module():
    """VIB-6155 is corrected; this test records why no census test replaces it.

    The predecessor of this test asserted the defect itself — that
    ``market_address("arbitrum", "AVAX/USD")`` equalled the Avalanche ETH/USD
    address — and instructed its own deletion once VIB-6155 landed. It has landed
    (five wrong rows, not one), so the assertion is inverted here rather than
    dropped silently, and the reasoning that made it un-replaceable is kept.

    **Why this module cannot own the durable guard.** Reproducing the defect
    needs the address the CHAIN reports; the obvious in-tree reproduction — a
    symbol row plus an "address" row built from ``GMX_V2_MARKETS[...]`` — passes,
    because both sides resolve through the same wrong entry and agree. Any purely
    in-tree successor would be that same non-discriminating test.

    **And the tempting cheap invariant is the wrong one.** VIB-6155 originally
    proposed a cross-chain address-uniqueness assert. It would have caught one of
    the five: ``arbitrum:OP/USD`` held a real, live, *unique* Arbitrum address —
    the wstETH/WETH swap pool. Uniqueness is also wrong as a permanent rule, since
    identical CREATE2 addresses across chains are legitimately possible.

    The guard therefore lives on-chain in
    ``tests/audit/test_gmx_v2_market_identity.py``, which compares every declared
    market against that chain's ``Reader.getMarket()``. Its negative control was
    15 failures on the pre-fix tree.
    """
    assert market_address("arbitrum", "AVAX/USD").lower() != market_address("avalanche", "ETH/USD").lower(), (
        "VIB-6155 has regressed IN THE FIXTURE SNAPSHOT: the Arbitrum AVAX/USD row is holding the Avalanche "
        "ETH/USD address again. Run tests/audit/test_gmx_v2_market_identity.py -m audit "
        "for the full picture — this assertion only sees the one duplicate."
    )


# ---------------------------------------------------------------------------
# The identity hook must accept every market spelling the COMPILER accepts
# ---------------------------------------------------------------------------


def _usd_market_addresses(chain: str) -> list[str]:
    """Fixture market-token addresses — the only market vocabulary identity accepts."""
    return [r.market_token for c, r in FIXTURE_MARKETS if c == chain]


@pytest.mark.parametrize("chain", sorted({c for c, _ in FIXTURE_MARKETS}))
def test_every_address_spelling_resolves_and_no_symbol_spelling_does(chain):
    """Identity is address-first: address spellings resolve, symbol spellings never do.

    SUCCESSOR of ``test_every_market_alias_the_compiler_accepts_also_resolves_for_identity``.
    The alias census asserted that every label spelling the compiler accepted
    (``ETH``, ``WETH``, ``ETH/USD``) resolved through the curated market table —
    a table VIB-6155 proved can silently disagree with the chain, and which is
    now deleted. Producers write addresses; identity must accept every ADDRESS
    spelling case-insensitively and must treat EVERY symbol spelling as
    unmeasured (no token — over-split, loud; legacy symbol state is a
    repair-CLI migration case).
    """
    addresses = _usd_market_addresses(chain)
    assert addresses, f"{chain} has no fixture markets — this census would be vacuous"

    for address in addresses:
        expected = address.lower()
        for spelling in (address, address.lower(), "0x" + address[2:].upper()):
            assert _address_only(spelling) == expected, (
                f"{chain}: address spelling {spelling!r} does not resolve identically — "
                "case must never affect identity"
            )

    for chain_, record in FIXTURE_MARKETS:
        if chain_ != chain:
            continue
        base = record.label.split("/")[0]
        for symbol in (record.label, base, base.lower(), f"W{base}", f"w{base.lower()}"):
            assert _address_only(symbol) is None, (
                f"{chain}: symbol spelling {symbol!r} resolved — the market axis must "
                "not guess (address-first; VIB-6155)"
            )


def test_an_alias_that_names_no_market_still_yields_nothing():
    """Non-vacuity control kept from the alias era: junk never becomes a market."""
    for unknown in ("ZZZZ", "WZZZZ", "NOTAMARKET", "W", "/USD", ""):
        assert _address_only(unknown) is None, unknown


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
            "market": market_address(chain, "ETH/USD"),
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
            "market": market_address(chain, "ETH/USD"),
            "collateral_token": GMX_V2_TOKENS[chain]["USDC"],
            "is_long": True,
        },
    )
    tokens = gmx_v2_perp_identity(row, wallet_address=wallet)
    assert any(t.endswith(real_key) for t in tokens), sorted(tokens)
    assert any(":sem:" in t for t in tokens), sorted(tokens)


# RETIRED: ``test_the_w_prefix_alias_can_never_name_a_different_real_market``.
# The ``<SYM>``/``W<SYM>`` → ``<SYM>/USD`` alias fallback it guarded was deleted
# with the market table (address-first): ``_resolve_address`` now serves exact
# collateral symbols only and ``_address_only`` serves the market axis, so the
# W-strip ambiguity it defended against is unrepresentable. The successor
# property — no symbol spelling ever names a market — is
# ``test_every_address_spelling_resolves_and_no_symbol_spelling_does`` above.

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

    NOT ``frozenset()`` — even though VIB-6329 now routes an empty registered-hook
    result directly to raw ``position_id``, raw-id space cannot intersect the
    venue-key space used by another producer of the same row. Emitting the adopted
    key preserves that join and avoids manufacturing a duplicate.
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
            "market": market_address(chain, "ETH/USD"),
            "collateral_token": GMX_V2_TOKENS[chain]["USDC"],
            "is_long": True,
        },
    )
    tokens = gmx_v2_perp_identity(row, wallet_address=None)
    assert tokens == frozenset({f"gmx_v2:key:{chain}:{key}"}), sorted(tokens)


def test_a_keyless_row_still_emits_its_semantic_token():
    """Non-vacuity control: the narrowing is scoped to rows that carry a venue key.

    A strategy row has no venue key, so there is nothing for its attributes to
    disagree WITH — it must still emit ``sem``, or the strategy side of the
    mainnet pair goes unnamed and VIB-6287 is unfixed.

    Address-first: the MARKET must be an address for the row to be named at all.
    A legacy symbol-market row emits NOTHING — there is no symbol→address table
    left to resolve through (VIB-6155 is why there must not be), so it
    over-splits loudly instead; legacy symbol-shaped state is a repair-CLI
    migration case (``almanak/framework/cli/repair_position_references.py``).
    """
    chain = "arbitrum"

    def _keyless(market):
        return PositionInfo(
            position_type=PositionType.PERP,
            position_id="eth-long",
            chain=chain,
            protocol="gmx_v2",
            value_usd=Decimal("0"),
            details={"market": market, "collateral_token": "USDC", "is_long": True},
        )

    tokens = gmx_v2_perp_identity(_keyless(MARKET), wallet_address=None)
    assert any(":sem:" in t for t in tokens), sorted(tokens)
    assert gmx_v2_perp_identity(_keyless("ETH/USD"), wallet_address=None) == frozenset(), (
        "a symbol-shaped market must yield NO token — never a resolution through a "
        "curated table"
    )


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


# ---------------------------------------------------------------------------
# VIB-6329 — the structural rules the framework CAN enforce
#
# Each rung below has a NEGATIVE CONTROL (fails if the guard is reverted) and a
# NON-VACUITY TWIN (fails if the guard is tightened into rejecting everything,
# which would silently disable venue identity — over-split, loud, but a total
# loss of VIB-6287's fix).
# ---------------------------------------------------------------------------


@pytest.fixture
def probe_hook(monkeypatch):
    """Register a throwaway hook returning a fixed token set, and clean up."""
    from almanak.connectors._strategy_base import perp_identity as seam

    def _install(tokens, slug="probe_venue"):
        monkeypatch.setitem(seam._REGISTRY, slug, lambda _p, *, wallet_address=None: frozenset(tokens))
        return slug

    return _install


def _probe_perp(slug: str, ptype: PositionType = PositionType.PERP) -> PositionInfo:
    return PositionInfo(
        position_type=ptype,
        position_id="probe-1",
        chain=CHAIN,
        protocol=slug,
        value_usd=Decimal("0"),
        details={},
    )


def test_two_tokens_of_the_same_family_drop_that_family(probe_hook):
    """NEGATIVE CONTROL for the at-most-one-token-per-family rule (VIB-6329).

    Two different ``key`` tokens on ONE row means the row names two different
    positions in the same identity space. It is internally inconsistent by
    construction, and under the enumeration's transitive closure it BRIDGES two
    physically distinct positions — union-find merges their components, every
    registry row but one is suppressed, nothing builds a closing intent for the
    suppressed row, and nothing raises. Silently stranded funds.

    This is the half of ``derive(key) == adopt`` that survives the oracle problem:
    the framework cannot verify two tokens MEAN the same position (that needs the
    venue's derivation, and asking the connector for it checks the connector
    against itself), but it can see that two tokens of one family CANNOT.

    Fails if the family rule is reverted.
    """
    slug = probe_hook(
        {
            f"probe_venue:key:{CHAIN}:0x" + "aa" * 32,
            f"probe_venue:key:{CHAIN}:0x" + "bb" * 32,
            f"probe_venue:sem:{CHAIN}:eth:usdc:long",
        }
    )
    tokens = venue_identity_tokens(_probe_perp(slug))
    assert tokens == frozenset({f"probe_venue:sem:{CHAIN}:eth:usdc:long"}), sorted(tokens)


def test_a_fully_rejected_hook_emission_falls_to_raw_id_not_the_lossy_default(probe_hook):
    """Rejection must not move a row into the coarser PERP default namespace.

    Aster assigns one ``tradeHash`` per open call, so two distinct positions can
    share ``(market, collateral, side)``. Drift has the same shape across
    sub-accounts. If a future hook emits an invalid token set and the empty result
    reaches ``_perp_default_identity``, those distinct positions collapse and one
    receives no closing intent. Raw-id fallback is the only fund-safe direction.
    """
    slug = probe_hook(
        {
            f"probe_venue:key:{CHAIN}:trade-a",
            f"probe_venue:key:{CHAIN}:trade-b",
        }
    )
    shared_details = {"market": "ETH/USD", "collateral_token": "USDC", "is_long": True}
    first = _probe_perp(slug)
    first.details = dict(shared_details)
    second = _probe_perp(slug)
    second.position_id = "probe-2"
    second.details = dict(shared_details)

    first_keys = _dedupe_keys(first)
    second_keys = _dedupe_keys(second)

    assert first_keys == frozenset({(CHAIN, str(PositionType.PERP), "id", "probe-1")})
    assert second_keys == frozenset({(CHAIN, str(PositionType.PERP), "id", "probe-2")})
    assert first_keys.isdisjoint(second_keys), "fully rejected rows must never collapse through the PERP default"


def test_a_perp_venue_without_a_hook_still_uses_the_framework_default():
    """Non-vacuity twin: raw-id fallback is scoped to registered hooks only."""
    row = _probe_perp("venue_without_hook")
    row.details = {"market": "ETH/USD", "collateral_token": "USDC", "is_long": True}

    assert _dedupe_keys(row) == frozenset(
        {(CHAIN, str(PositionType.PERP), "perp", "venue_without_hook", "eth/usd", "usdc", "long")}
    )


def test_several_families_on_one_row_are_still_honoured(probe_hook):
    """NON-VACUITY TWIN — multi-FAMILY emission is legal and load-bearing.

    Measured on the shipped hook: 9 of 48 GMX row shapes emit more than one
    family. The key<->sem bridge is how a backfill row (key only) reaches a
    strategy row (sem only), which is the whole of VIB-6287. A rule that banned
    multi-alias outright would pass the control above and GUT the fix.
    """
    slug = probe_hook(
        {
            f"probe_venue:key:{CHAIN}:0x" + "aa" * 32,
            f"probe_venue:sem:{CHAIN}:eth:usdc:long",
        }
    )
    assert len(venue_identity_tokens(_probe_perp(slug))) == 2


def test_a_token_namespaced_for_a_venue_the_hook_does_not_own_is_dropped(probe_hook):
    """NEGATIVE CONTROL for the namespace rule (VIB-6329, #3534 panel Grok).

    ``_dedupe_keys`` builds ``(chain, position_type, "venue", <opaque token>)`` and
    deliberately omits the protocol, so cross-venue safety rests ENTIRELY on each
    connector namespacing its own tokens. Nothing checked that. A hook emitting a
    token in another venue's namespace, on the same chain and position type,
    over-collapses ACROSS protocols.

    Fails if the namespace check is reverted.
    """
    slug = probe_hook({f"gmx_v2:key:{CHAIN}:0x" + "aa" * 32, f"probe_venue:sem:{CHAIN}:eth:usdc:long"})
    tokens = venue_identity_tokens(_probe_perp(slug))
    assert tokens == frozenset({f"probe_venue:sem:{CHAIN}:eth:usdc:long"}), sorted(tokens)


def test_a_hook_registered_under_several_slugs_may_namespace_with_any_of_them():
    """NON-VACUITY TWIN for the namespace rule.

    One connector can be registered under several teardown slugs and may
    legitimately namespace ALL its tokens with just one of them. Holding it to the
    dispatch slug alone would reject conforming tokens and disable identity for
    every multi-slug connector — over-split, loud, but a total loss.
    """
    from almanak.connectors._strategy_base import perp_identity as seam

    slugs = frozenset({"probe_a", "probe_b"})
    token = f"probe_a:key:{CHAIN}:0x" + "aa" * 32
    for slug in slugs:
        seam._register_perp_identity(slug, lambda _p, *, wallet_address=None: frozenset({token}), namespaces=slugs)
    try:
        # Dispatched under probe_b, emitting in probe_a's namespace: legal.
        assert venue_identity_tokens(_probe_perp("probe_b")) == frozenset({token})
    finally:
        for slug in slugs:
            seam._REGISTRY.pop(slug, None)
            seam._NAMESPACES.pop(slug, None)


def test_a_malformed_token_without_a_family_segment_is_dropped(probe_hook):
    """NEGATIVE CONTROL for the grammar. A token with no ``<family>`` segment
    cannot be checked for the family rule at all, so accepting it would be a hole
    straight through the guard above."""
    slug = probe_hook({"probe_venue:justakey", "nocolonsatall", f"probe_venue:sem:{CHAIN}:eth:usdc:long"})
    tokens = venue_identity_tokens(_probe_perp(slug))
    assert tokens == frozenset({f"probe_venue:sem:{CHAIN}:eth:usdc:long"}), sorted(tokens)


def test_a_payload_containing_colons_survives_byte_identical(probe_hook):
    """NON-VACUITY TWIN for the grammar, and the case-folding rule restated.

    The payload may itself contain colons, and it is OPAQUE: ``drift``'s identity
    carries a base58 Solana pubkey where ``'B' != 'b'`` is a different byte. The
    framework folds the ``<slug>`` and ``<family>`` labels for COMPARISON ONLY and
    must return the token byte-identical. A grammar check that split naively, or
    folded the whole token, would corrupt exactly this venue.
    """
    token = "drift:acct:9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin:0:1"
    slug = probe_hook({token}, slug="drift")
    row = PositionInfo(
        position_type=PositionType.PERP,
        position_id="drift-1",
        chain="solana",
        protocol=slug,
        value_usd=Decimal("0"),
        details={},
    )
    assert venue_identity_tokens(row) == frozenset({token})


def test_a_non_perp_row_is_never_named_by_a_perp_identity_hook(probe_hook):
    """NEGATIVE CONTROL for PERP-scoped dispatch (VIB-6329, #3534 panel CodeRabbit).

    The registry is keyed by protocol ALONE, and no hook checks the row's type, so
    a non-perp row of a hook-publishing protocol would be handed a perp identity
    hook and skip its own per-type default.

    Unreachable today — every ``_dedupe_keys`` key is position-type-scoped, and
    ``gmx_v2`` declares perp intents only — so this pins the LAYERING, which is
    what makes the next connector's version of the mistake reachable.
    """
    slug = probe_hook({f"probe_venue:key:{CHAIN}:0x" + "aa" * 32})
    assert venue_identity_tokens(_probe_perp(slug, PositionType.LP)) == frozenset()


def test_a_perp_row_of_the_same_hook_is_still_named(probe_hook):
    """NON-VACUITY TWIN for PERP scoping: the identical row typed PERP is named.

    Without this, scoping the dispatch to a type nothing ever passes would satisfy
    the control above while disabling the seam entirely.
    """
    slug = probe_hook({f"probe_venue:key:{CHAIN}:0x" + "aa" * 32})
    assert venue_identity_tokens(_probe_perp(slug, PositionType.PERP))


def test_the_shipped_gmx_hook_never_emits_two_tokens_of_one_family():
    """The corpus check: GMX must satisfy the family rule UNCHANGED.

    A guard that forces edits to the one working implementation is suspicious, so
    this measures the shipped hook across the row shapes that actually occur —
    venue-key / synthetic / absent ids, address- and symbol-space details, a
    residual, an alias spelling, and the wallet present / absent / malformed —
    rather than asserting the property from the code.

    It also fails if a future GMX change starts emitting two ``key`` tokens, which
    is exactly the internally-inconsistent row the family rule exists to catch.
    """
    ids = (VENUE_KEY, "0x" + "1e" * 32, "gmx-ETH/USD-arbitrum", "")
    detail_shapes = (
        {"market": MARKET, "collateral_token": USDC, "is_long": True},
        {"market": "ETH/USD", "collateral_token": "USDC", "is_long": True},
        {"market": MARKET, "collateral_token": USDC},
        {"collateral_token": "USDC", "direction": "long"},
        {"market": MARKET, "collateral_token": USDC, "is_long": True, "kind": "pending_order"},
        {"market": "ETH", "collateral_token": "USDC", "side": "long"},
    )
    emitting = multi_family = 0
    for position_id in ids:
        for details in detail_shapes:
            for wallet in (WALLET, None, "0xdead"):
                tokens = gmx_v2_perp_identity(
                    _perp(position_id=position_id, details=dict(details)), wallet_address=wallet
                )
                if not tokens:
                    continue
                emitting += 1
                families: dict[str, set[str]] = {}
                for token in tokens:
                    families.setdefault(token.split(":")[1], set()).add(token)
                worst = max(families.values(), key=len)
                assert len(worst) == 1, (
                    f"gmx_v2 emitted {sorted(worst)} — two tokens of one family for one row, "
                    "which names two positions in one identity space"
                )
                multi_family += len(families) > 1
    assert emitting, "corpus emitted no tokens at all — this check would be vacuous"
    assert multi_family, (
        "no row emitted more than one FAMILY — the key<->sem bridge is what VIB-6287 fixes, "
        "so this corpus can no longer prove multi-alias emission is load-bearing"
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


# ---------------------------------------------------------------------------
# Shape A2 — the union split that is LIVE in a shipped demo, no restart needed.
#
# R3 (mainnet, 2026-08-02) passed its teardown because the strategy under test
# writes ``collateral_token`` into its position details, so its HOT row and the
# WARM registry row both emitted ``{key, sem}``, intersected, and COLLAPSED to one
# row. That collapse is what hid the registry row from the completeness gate.
#
# The collapse is CONDITIONAL. ``gmx_v2_directional_perp`` — a shipped demo —
# omits ``collateral_token`` from ``get_open_positions()`` while its own
# ``generate_teardown_intents()`` supplies it. Its row therefore emits no venue
# token at all, cannot intersect, and the union does NOT collapse: two rows for
# one physical position, which is exactly what VIB-6287 exists to prevent.
#
# Why this lives here rather than in an on-chain run: TEST-PLAN §4 measurement 1
# ("one physical position must enumerate as one") is the right check, but R3 ran
# it against the one strategy that STRUCTURALLY CANNOT FAIL IT. The measurement
# was sound; the subject was blind. This pins it for free.
# ---------------------------------------------------------------------------

# An UNDER-DESCRIBED perp row: market + side, no collateral under ANY alias.
#
# This was a hand-copy of ``gmx_v2_directional_perp.get_open_positions`` until
# VIB-6316 repaired that demo to name its collateral. It is kept as a SYNTHETIC
# fixture — the A2 split it produces is a property of under-described rows in
# general, not of one demo, and nothing in the framework compels a strategy to
# name its collateral. ``test_the_directional_perp_demo_now_names_its_collateral``
# below pins the repair from the other side, so this constant no longer claims to
# mirror anything and cannot rot into a fiction.
_UNDERDESCRIBED_PERP_DETAILS = {"market": "ETH/USD", "side": "long", "size_usd": "10"}

#: The address-first sibling: the market is the address a migrated strategy
#: writes, but collateral is still missing — the A2 split is a property of
#: UNDER-DESCRIPTION, not of the value space, so it must survive the migration.
_UNDERDESCRIBED_ADDRESS_DETAILS = {"market": MARKET, "side": "long", "size_usd": "10"}


def _hot_summary(details: dict) -> TeardownPositionSummary:
    """One strategy-reported (HOT) perp row carrying ``details``."""
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


def _union_size(details: dict) -> int:
    """Enumerated position count for a HOT row of ``details`` + R3's real WARM row."""
    merged = reconcile_lp_with_registry(
        strategy_summary=_hot_summary(details),
        registry_positions=[_registry_row()],
        registry_available=True,
        wallet_for_chain=lambda _chain: WALLET,
    )
    return len(merged.positions)


def test_a_perp_row_without_collateral_cannot_collapse_against_its_own_registry_row():
    """THE MECHANISM. Collateral in the HOT details is what makes the union collapse.

    Both rows describe the SAME physical position and the wallet is fully
    resolved. The two detail dicts below differ in more than one way (symbol vs
    address ``market``, ``side`` vs ``is_long``, ``size_usd``), but collateral is
    the axis that decides: isolating it,
    ``{market: 'ETH/USD', collateral_token: USDC, side: 'long', size_usd}``
    collapses to 1, while ``{market: <addr>, is_long: True}`` stays at 2. The
    GMX hook needs market AND collateral AND side to
    emit ``sem`` (``gmx_v2/perp_identity.py``), so a row missing collateral emits
    no venue token, cannot intersect the registry row's ``{key, sem}``, and
    survives as a second row.

    This is a live defect, not a restart edge case: no ``kill -9``, no wiped
    state, no unresolvable wallet, no synthetic input.
    """
    assert _union_size(_UNDERDESCRIBED_PERP_DETAILS) == 2, (
        "the shipped demo's row shape now collapses — if the demo was repaired, "
        "delete this test and its xfail sibling rather than weakening them"
    )
    assert _union_size({"market": MARKET, "collateral_token": USDC, "is_long": True}) == 1, (
        "a collateral-carrying row must still collapse to one; if this fails the "
        "regression is in _dedupe_keys, not in the demo"
    )


def _shipped_demo_row():
    """Build the row the SHIPPED demo actually emits, via its public method.

    The census fixtures below are deliberately synthetic; this is the one place
    that touches the real ``gmx_v2_directional_perp``. If this stopped reading the
    shipped module, every census assertion could stay green while the demo
    regressed to the collateral-less shape that made one physical position
    enumerate twice on mainnet.

    The venue probe has no market snapshot here, so it returns UNMEASURED and the
    demo falls back to its cached side — which is exactly the path that must still
    name collateral, because an unmeasured read is not a flat account.
    """
    import importlib.util
    import json
    from pathlib import Path
    from unittest.mock import patch

    seed = Path(__file__).resolve().parents[3] / "almanak" / "demo_strategies" / "gmx_v2_directional_perp"
    spec = importlib.util.spec_from_file_location("gmx_seed_census_row", seed / "strategy.py")
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


def test_the_directional_perp_demo_now_names_its_collateral():
    """The root-cause repair, pinned from the producer side (VIB-6316).

    This test previously asserted the OPPOSITE — that the demo still omitted
    collateral — as a tripwire so the repair could not land without re-evaluating
    the A2 fixtures. The repair has landed and the fixtures were re-evaluated
    (they are now explicitly synthetic), so the assertion is INVERTED rather than
    deleted: the demo must not regress to the shape that made one physical
    position enumerate twice.

    Checks EVERY name the hook accepts, not just ``collateral_token``. The hook
    reads ``_COLLATERAL_KEYS = ("collateral_address", "collateral_token")`` and the
    sibling demo ``gmx_perp_lifecycle`` writes the address form, so a
    ``collateral_token``-only assertion would go green on a demo that had been
    "repaired" into a form the hook cannot read. Imported from the hook rather than
    re-typed so a new alias cannot slip past.
    """
    from almanak.connectors.gmx_v2.perp_identity import _COLLATERAL_KEYS

    # Asserts on the row the demo EMITS, not on the source text of the method that
    # builds it. The earlier form was ``inspect.getsource(get_open_positions)`` plus
    # a substring search, which went red the moment the row builder was extracted
    # into a helper — while the demo still emitted ``collateral_token`` and the
    # property held. "Where does this string live" is not the property; "does the
    # emitted row name its collateral" is.
    row = _shipped_demo_row()
    # VALUE, not membership: an empty or None collateral is present but names
    # nothing, and the hook derives no venue key from it.
    written = [key for key in _COLLATERAL_KEYS if row.details.get(key)]
    assert written, (
        "gmx_v2_directional_perp.get_open_positions EMITS no collateral under any alias "
        f"the identity hook accepts ({list(_COLLATERAL_KEYS)}); it emitted "
        f"{sorted(row.details)}. Without it the row derives "
        "no venue key, falls through to its raw position_id, and one physical position "
        "enumerates twice — mainnet R5 reported positions_total=2 for a single ETH/USD long."
    )


def test_a2_union_split_is_covered_by_a_single_address_space_close():
    """END-TO-END for shape A2, in the address-first world (VIB-6316 successor).

    SUCCESSOR of ``test_a2_union_split_is_covered_by_a_single_symbol_space_close``:
    producers write ADDRESSES now, so the under-described HOT row and the close
    intent both carry the market-token address. One physical position still
    enumerates as two rows (collateral is missing, so the HOT row emits no venue
    token and cannot intersect), and ONE address-space ``PERP_CLOSE`` must
    credit both — the market comparison agrees in address space, no venue
    corroboration required.

    The LEGACY half: a symbol-space close against the same union leaves the
    registry row uncovered even with the wallet — the loud fail-safe FAILED,
    by design ("start with the address"). Legacy symbol state is a repair-CLI
    migration case (``almanak/framework/cli/repair_position_references.py``);
    this is the shape R5 reproduced on mainnet, retired rather than resolved
    through a curated table.
    """
    merged = reconcile_lp_with_registry(
        strategy_summary=_hot_summary(_UNDERDESCRIBED_ADDRESS_DETAILS),
        registry_positions=[_registry_row()],
        registry_available=True,
        wallet_for_chain=lambda _chain: WALLET,
    )
    assert len(merged.positions) == 2, "precondition: the A2 union must not collapse"

    report = check_intent_coverage(merged.positions, [_CloseIntent(MARKET)], wallet_for_chain=lambda _chain: WALLET)
    assert report.complete, f"uncovered after a full close: {report.uncovered}"

    legacy = check_intent_coverage(merged.positions, [_CloseIntent("ETH/USD")], wallet_for_chain=lambda _chain: WALLET)
    assert not legacy.complete, (
        "a legacy symbol-space close must fail the gate LOUDLY rather than resolve "
        "through a symbol table the migration deleted"
    )


def test_a2_union_split_without_a_wallet_is_the_r5_mainnet_failure():
    """The R5 mainnet result, pinned as a unit test.

    This exact configuration — the shipped demo's 2-row union and one full close,
    no wallet resolver — is what reported ``failed (closed=1, failed=1)`` on
    arbitrum on 2026-08-02 while TD-08 chain-confirmed 2/2 CLOSED. Kept so the
    test above is evidence about the fix rather than about the fixture.
    """
    merged = reconcile_lp_with_registry(
        strategy_summary=_hot_summary(_UNDERDESCRIBED_PERP_DETAILS),
        registry_positions=[_registry_row()],
        registry_available=True,
        wallet_for_chain=lambda _chain: WALLET,
    )
    assert not check_intent_coverage(merged.positions, [_CloseIntent("ETH/USD")]).complete
