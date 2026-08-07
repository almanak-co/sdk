"""One physical perp position must be closed ONCE per teardown plan (VIB-6341).

NEGATIVE CONTROL. Every ``must_flip`` test below FAILS on the unmodified parent
commit (two ``PERP_CLOSE`` intents reach the plan for one physical GMX position)
and passes after the guard lands. Every ``must_not_change`` test passes on BOTH
sides — they pin the collapse to positions the VENUE says are the same, so the
guard can never merge two genuinely distinct positions and strand one.

The reproduction is the shape VIB-6341 observed on the A3 Anvil run and the shape
a mainnet run reproduces whenever ``position_registry`` hydration lags: one
physical GMX perp named twice — under address-first, twice in ADDRESS space (the
strategy's own enumeration and a registry / settlement-derived row, distinct raw
ids). ``reconcile_lp_with_registry`` early-returns when the registry is empty, so
VIB-6287's alias union never runs; and even when it DOES run it is additive by
contract and never drops a strategy row. Either way the duplicate reaches
``full_close_intents``, which maps 1 row -> 1 intent with no de-duplication, and
the lanes then withhold the duplicate from DISPATCH while still showing it to the
completeness gate.

ADDRESS-FIRST UPDATE: the market axis has no symbol resolution any more, so a
LEGACY symbol-space row can no longer be collapsed against an address-space row —
that pair over-splits into TWO dispatched closes by design (loud, and exactly
why legacy symbol-shaped state must be migrated via
``almanak/framework/cli/repair_position_references.py`` rather than resolved
through a curated table — VIB-6155).
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors.gmx_v2.addresses import GMX_V2_TOKENS
from almanak.framework.intents import Intent
from almanak.framework.teardown.completeness import check_intent_coverage
from almanak.framework.teardown.full_close import full_close_intents
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.teardown.single_close_guard import collapse_duplicate_perp_closes
from tests.unit.connectors.gmx_v2.market_fixtures import market_address


def _dispatch(intents) -> list:
    return collapse_duplicate_perp_closes(intents).dispatch


CHAIN = "arbitrum"
MARKET = market_address(CHAIN, "ETH/USD")
BTC_MARKET = market_address(CHAIN, "BTC/USD")
USDC = GMX_V2_TOKENS[CHAIN]["USDC"]
WALLET = "0xafeB2f5c213b5e7F37c3Fc171dfCb6270d07e21a"
# The venue position key GMX derives for (WALLET, ETH/USD market, USDC, isLong)
# — reproduced byte-identically in ``gmx_v2/perp_identity.py``'s docstring.
VENUE_KEY = "0xbf58e0307a44a17ea51e30850651f5269c9fc0f306990576c015e9a88ac9bafa"


def _perp(position_id: str, **details) -> PositionInfo:
    base = {"market": MARKET, "collateral_token": USDC, "is_long": True}
    base.update(details)
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=position_id,
        chain=CHAIN,
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details=base,
    )


def _closes(intents) -> list:
    return [i for i in intents if str(getattr(i.intent_type, "value", i.intent_type)).upper().endswith("PERP_CLOSE")]


# ---------------------------------------------------------------------------
# must_flip — these FAIL on the parent commit
# ---------------------------------------------------------------------------


def test_two_address_space_rows_for_one_perp_build_one_close():
    """The VIB-6341 shape, address-first: one physical perp, two rows, ONE close.

    SUCCESSOR of ``test_symbol_and_address_rows_for_one_perp_build_one_close``:
    producers write addresses now, so the duplicate pair is two ADDRESS-space
    rows with distinct ids — Row A the strategy's own stub
    (``gmx-ETH/USD-arbitrum``, raw id), Row B the settlement-reconciler /
    registry row carrying the venue position key. GMX's identity hook resolves
    both closes to the identical ``sem`` token, so the venue itself says these
    are one position — and a teardown that submits two full closes against it
    is the over-close hazard the ticket names.
    """
    rows = [
        _perp("gmx-ETH/USD-arbitrum"),
        _perp(VENUE_KEY),
    ]
    assert len(_closes(_dispatch(full_close_intents(rows)))) == 1


def test_a_legacy_symbol_row_over_splits_into_two_closes():
    """A LEGACY symbol-space row is over-split BY DESIGN (VIB-6341, address-first).

    Pre-migration this pair collapsed: the hook resolved ``"ETH/USD"`` through
    the (since-deleted) curated market table into the same ``sem`` token as the
    address row. That table is deleted — a symbol row now has UNMEASURED venue
    identity, the guard never collapses on a guess (Empty ≠ Zero), and both
    closes dispatch. Over-split is the loud, fail-safe direction; the silent
    alternative is resolving through a table VIB-6155 proved can disagree with
    the chain. Legacy symbol-shaped state is a repair-CLI migration case
    (``almanak/framework/cli/repair_position_references.py``).
    """
    rows = [
        _perp("gmx-ETH/USD-arbitrum", market="ETH/USD", collateral_token="USDC"),
        _perp(VENUE_KEY),
    ]
    assert len(_closes(_dispatch(full_close_intents(rows)))) == 2


def test_duplicate_full_closes_collapse_regardless_of_row_order():
    """Order-independence: the registry read carries no ``ORDER BY``."""
    rows = [
        _perp(VENUE_KEY),
        _perp("gmx-ETH/USD-arbitrum"),
    ]
    assert len(_closes(_dispatch(full_close_intents(rows)))) == 1


def test_hand_rolled_duplicate_full_closes_collapse():
    """A strategy that hand-rolls its plan is guarded too.

    ``strategies/accounting/perp`` and every GMX demo build their own
    ``Intent.perp_close`` rather than delegating to ``full_close_intents``, so a
    guard that only lived in the framework builder would be inert for them.

    The two intents deliberately differ in SPELLING — lower-cased vs
    EIP-55-checksummed market address, symbol vs address collateral — so this
    also pins that the collapse is identity-based, not string equality.
    """
    plan = [
        Intent.perp_close(
            market=MARKET.lower(), collateral_token="USDC", is_long=True, size_usd=None, protocol="gmx_v2", chain=CHAIN
        ),
        Intent.perp_close(
            market=MARKET, collateral_token=USDC, is_long=True, size_usd=None, protocol="gmx_v2", chain=CHAIN
        ),
    ]
    assert len(_dispatch(plan)) == 1


# ---------------------------------------------------------------------------
# must_not_change — these pass on BOTH sides
# ---------------------------------------------------------------------------


def test_collapsing_never_turns_a_covered_enumeration_into_an_uncovered_one():
    """The risk this fix introduces, pinned.

    Withholding an intent from dispatch could make ``check_intent_coverage``
    (TD-11 / VIB-5469) report a KNOWN position with no closing intent — turning a
    passing teardown into a loud FAILED one.

    It does not, and the reason is NOT that the surviving close covers the other
    row: this guard's predicate (intent<->intent, ``sem`` space) is strictly
    stronger than coverage's (intent<->position), so it genuinely can collapse
    intents whose positions coverage would not match. The reason is that the gate
    is shown ``for_coverage`` — the plan as BUILT. See the C2b / C2c tests below,
    which are the shapes where the difference bites.
    """
    rows = [
        _perp("gmx-ETH/USD-arbitrum"),
        _perp(VENUE_KEY),
    ]
    plan = collapse_duplicate_perp_closes(full_close_intents(rows))
    assert len(_closes(plan.dispatch)) == 1
    report = check_intent_coverage(rows, plan.for_coverage, wallet_for_chain=lambda _chain: WALLET)
    assert report.complete, report.uncovered


def test_long_and_short_in_one_market_are_two_positions():
    """A long and a short are DIFFERENT venue keys — never collapse them."""
    rows = [_perp("long-row"), _perp("short-row", is_long=False)]
    assert len(_closes(_dispatch(full_close_intents(rows)))) == 2


def test_two_markets_are_two_positions():
    rows = [_perp("eth-row"), _perp("btc-row", market=BTC_MARKET)]
    assert len(_closes(_dispatch(full_close_intents(rows)))) == 2


def test_two_chains_are_two_positions():
    """Chain-scoped tokens: an Arbitrum perp must never suppress an Avalanche one."""
    avax_market = market_address("avalanche", "ETH/USD")
    avax_usdc = GMX_V2_TOKENS["avalanche"]["USDC"]
    rows = [
        _perp("arb-row"),
        PositionInfo(
            position_type=PositionType.PERP,
            position_id="avax-row",
            chain="avalanche",
            protocol="gmx_v2",
            value_usd=Decimal("0"),
            details={"market": avax_market, "collateral_token": avax_usdc, "is_long": True},
        ),
    ]
    assert len(_closes(_dispatch(full_close_intents(rows)))) == 2


def test_unmeasured_identity_never_collapses():
    """Empty ≠ Zero: a market the venue cannot resolve yields NO token.

    Two such rows stay two closes — over-split, loud, recoverable — because the
    only alternative is to merge on a guess and strand a live position.
    """
    rows = [
        _perp("row-a", market="NOT-A-REAL-MARKET/USD"),
        _perp("row-b", market="ALSO-NOT-REAL/USD"),
    ]
    assert len(_closes(_dispatch(full_close_intents(rows)))) == 2


def test_partial_closes_are_never_collapsed():
    """Only FULL closes (``size_usd=None``) are the same economic action twice.

    Two sized closes against one position may both be intended (a staged exit),
    so the guard leaves them alone.
    """
    plan = [
        Intent.perp_close(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            size_usd=Decimal("5"),
            protocol="gmx_v2",
            chain=CHAIN,
        ),
        Intent.perp_close(
            market=MARKET, collateral_token=USDC, is_long=True, size_usd=Decimal("5"), protocol="gmx_v2", chain=CHAIN
        ),
    ]
    assert len(_dispatch(plan)) == 2


def test_a_cancel_is_not_a_close():
    """A pending-order residual is CANCELLED, not closed — and never collapsed
    into the close of the position that eventually fills from it."""
    rows = [
        _perp("gmx-ETH/USD-arbitrum", market="ETH/USD", collateral_token="USDC"),
        PositionInfo(
            position_type=PositionType.PERP,
            position_id=VENUE_KEY,
            chain=CHAIN,
            protocol="gmx_v2",
            value_usd=Decimal("0"),
            details={
                "kind": "pending_order",
                "order_key": VENUE_KEY,
                "cancellable": True,
                "market": MARKET,
                "collateral_token": USDC,
                "is_long": True,
            },
        ),
    ]
    intents = full_close_intents(rows)
    assert len(_closes(intents)) == 1
    assert len(intents) == 2  # the cancel survives


def test_a_single_perp_plan_is_returned_unchanged():
    """The common path — one perp, one close — must be untouched."""
    plan = [
        Intent.perp_close(
            market=MARKET, collateral_token=USDC, is_long=True, size_usd=None, protocol="gmx_v2", chain=CHAIN
        )
    ]
    assert _dispatch(plan) == plan
    rows = [_perp("only-row")]
    assert len(_closes(_dispatch(full_close_intents(rows)))) == 1


def test_a_mixed_plan_keeps_every_non_perp_intent_in_order():
    """No-op for every non-PERP type, even alongside a collapsed perp pair."""
    repay = Intent.repay(protocol="aave_v3", token="USDC", amount=Decimal("0"), repay_full=True, chain=CHAIN)
    withdraw = Intent.withdraw(protocol="aave_v3", token="WETH", amount=Decimal("0"), withdraw_all=True, chain=CHAIN)
    close_a = Intent.perp_close(
        market=MARKET.lower(), collateral_token="USDC", is_long=True, size_usd=None, protocol="gmx_v2", chain=CHAIN
    )
    close_b = Intent.perp_close(
        market=MARKET, collateral_token=USDC, is_long=True, size_usd=None, protocol="gmx_v2", chain=CHAIN
    )
    assert _dispatch([close_a, repay, close_b, withdraw]) == [close_a, repay, withdraw]


def test_non_perp_plans_pass_through_unchanged():
    plan = [
        Intent.repay(protocol="aave_v3", token="USDC", amount=Decimal("0"), repay_full=True, chain=CHAIN),
        Intent.withdraw(protocol="aave_v3", token="WETH", amount=Decimal("0"), withdraw_all=True, chain=CHAIN),
    ]
    assert _dispatch(plan) == plan


def test_empty_and_none_plans_are_safe():
    assert _dispatch(None) == []
    assert _dispatch([]) == []


def test_the_guard_is_wired_only_dispatch_adjacent_and_never_in_the_builder():
    """CENSUS — the wiring rule that keeps the coverage gate honest.

    Every call site must sit in the same function as the dispatch it guards, so
    no lane can collapse a plan that a LATER function still has to
    coverage-check. ``full_close_intents`` is a builder whose output crosses a
    function boundary before ``check_intent_coverage`` runs, so it must NOT
    collapse — that arrangement is what stamped working teardowns FAILED.
    """
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[3]
    must_apply = {
        # (file, dispatch-adjacent gate it protects)
        "almanak/framework/teardown/teardown_manager.py": "G2 + resume",
        "almanak/framework/runner/_teardown_helpers.py": "G1",
        "almanak/framework/runner/runner_teardown.py": "inline lane (no gate)",
    }
    must_not_apply = [
        "almanak/framework/teardown/full_close.py",
        "almanak/framework/cli/teardown_helpers.py",
    ]
    for rel, why in must_apply.items():
        source = (repo_root / rel).read_text()
        assert "collapse_duplicate_perp_closes" in source, f"{rel} lost the VIB-6341 guard ({why})"
    for rel in must_not_apply:
        source = (repo_root / rel).read_text()
        assert "collapse_duplicate_perp_closes" not in source, (
            f"{rel} collapses the plan but its output crosses a function boundary before "
            "check_intent_coverage — that is the #3574 false-FAILED defect"
        )


def test_every_coverage_gate_is_fed_the_precollapse_plan():
    """CENSUS — the gate sites must consume ``for_coverage``, never the dispatch list.

    Passing the collapsed list to ``check_intent_coverage`` is the exact
    regression this PR shipped and then fixed. A future edit that swaps the
    argument back fails here.
    """
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[3]
    gates = {
        "almanak/framework/teardown/teardown_manager.py": "single_close.for_coverage",
        "almanak/framework/runner/_teardown_helpers.py": "_coverage_intents",
    }
    for rel, expected_arg in gates.items():
        source = (repo_root / rel).read_text()
        assert "check_intent_coverage(" in source, f"{rel} is no longer a coverage gate"
        assert expected_arg in source, f"{rel} must feed the PRE-COLLAPSE plan ({expected_arg}) to its gate"


def test_guard_is_idempotent():
    """It is applied at several lanes; applying it twice must be a no-op.

    The pair genuinely collapses (address spellings of one position), so the
    idempotence claim is exercised on a plan the guard actually changed.
    """
    plan = [
        Intent.perp_close(
            market=MARKET.lower(), collateral_token="USDC", is_long=True, size_usd=None, protocol="gmx_v2", chain=CHAIN
        ),
        Intent.perp_close(
            market=MARKET, collateral_token=USDC, is_long=True, size_usd=None, protocol="gmx_v2", chain=CHAIN
        ),
    ]
    once = _dispatch(plan)
    assert len(once) == 1, "precondition: the pair must actually collapse"
    assert _dispatch(once) == once


# ---------------------------------------------------------------------------
# #3574 audit — the collapse must never make a covered position look uncovered
# ---------------------------------------------------------------------------
#
# Both shapes below produce a position whose venue tokens are ``key``-only, with
# NO ``sem`` token, so it cannot intersect this guard's wallet-free ``sem`` probe.
# The withheld intent must still be shown to the completeness gate
# (``for_coverage``), which is what the #3574 fix established.
#
# ADDRESS-FIRST NOTE, recorded so the polarity flip below cannot read as an
# accident: the ORIGINAL reproduction paired a SYMBOL-space strategy row with an
# ADDRESS-space key row, and the dispatch-fed gate went ``complete=False``
# because the surviving intent lived in the other value space. That divergence
# needed the symbol axis, which this migration deleted — in address space every
# row of the pair carries the same market address, the raw comparison covers the
# key-only row directly, and even the (still forbidden) dispatch-fed gate can no
# longer be made to false-FAIL on this shape. The ``for_coverage`` discipline
# stays load-bearing as the structural rule — the wiring censuses above pin it —
# and these tests keep the key-only rows covered on both C2 branches.

# A venue key that does NOT derive from (WALLET, market, collateral, side).
NON_DERIVING_KEY = "0x" + "ab" * 32


def _duplicate_rows(venue_key: str) -> list[PositionInfo]:
    return [
        _perp("gmx-ETH/USD-arbitrum"),
        _perp(venue_key),
    ]


def test_c2b_no_wallet_key_only_row_stays_covered():
    """C2b — wallet UNMEASURED, so DERIVE cannot run and the row emits key-only.

    ``wallet is None`` is reachable through three deliberate fallbacks, not
    misconfiguration (``gmx_v2/perp_identity.py:328``). The collapse must still
    happen (the two INTENTS are sem-measurable without a wallet) and the
    key-only ROW must stay covered by the plan as built.
    """
    rows = _duplicate_rows(VENUE_KEY)
    plan = collapse_duplicate_perp_closes(full_close_intents(rows))
    assert len(_closes(plan.dispatch)) == 1, "still exactly one close is submitted"
    report = check_intent_coverage(rows, plan.for_coverage, wallet_for_chain=None)
    assert report.complete, f"withheld intent must still cover {[u.position_id for u in report.uncovered]}"

    # Address-first polarity flip (see the block comment above): the raw address
    # comparison now covers the key-only row even against the COLLAPSED list, so
    # the historical dispatch-fed false-FAIL is unreproducible on this shape.
    # Pinned positively: if this ever fails, a coverage path got STRICTER — the
    # regression that reads as an improvement.
    naive = check_intent_coverage(rows, plan.dispatch, wallet_for_chain=None)
    assert naive.complete, "the raw address comparison must keep the key-only row covered"


def test_c2c_disagreeing_key_row_stays_covered():
    """C2c — the row's adopted key disagrees with the derived key, so the venue
    refuses to vouch for its attributes and emits key-only
    (``gmx_v2/perp_identity.py:291``). That is the shape any keeper-tx
    mis-attribution produces; no misconfiguration required.
    """
    rows = _duplicate_rows(NON_DERIVING_KEY)
    plan = collapse_duplicate_perp_closes(full_close_intents(rows))
    assert len(_closes(plan.dispatch)) == 1, "still exactly one close is submitted"
    report = check_intent_coverage(rows, plan.for_coverage, wallet_for_chain=lambda _chain: WALLET)
    assert report.complete, f"withheld intent must still cover {[u.position_id for u in report.uncovered]}"

    # Same address-first polarity flip as C2b — see the block comment above.
    naive = check_intent_coverage(rows, plan.dispatch, wallet_for_chain=lambda _chain: WALLET)
    assert naive.complete, "the raw address comparison must keep the key-only row covered"


def test_for_coverage_is_the_plan_as_built():
    """The invariant the two gates rely on, asserted directly."""
    rows = _duplicate_rows(VENUE_KEY)
    built = full_close_intents(rows)
    plan = collapse_duplicate_perp_closes(built)
    assert plan.for_coverage == built
    assert len(plan.dispatch) == 1
    assert len(plan.dropped) == 1
    assert plan.collapsed is True


def test_an_uncollapsed_plan_reports_nothing_dropped():
    rows = [_perp("eth-row"), _perp("btc-row", market=BTC_MARKET)]
    plan = collapse_duplicate_perp_closes(full_close_intents(rows))
    assert plan.dropped == []
    assert plan.collapsed is False
    assert plan.dispatch == plan.for_coverage


def test_the_persisted_plan_is_the_dispatch_plan_not_the_plan_as_built():
    """VIB-6341: a resume must not replay a duplicate close.

    The in-memory collapse in ``execute_and_verify`` protects only the first
    attempt. ``_resolve_manager_execution_state`` persists the plan BEFORE that
    point, so a process restart deserialises ``pending_intents_json`` and the
    non-stale resume path re-executes it without the guard — the persisted
    duplicate survives every later resume and reaches the chain (#3574 audit).
    """
    import inspect

    from almanak.framework.runner import runner_teardown

    src = inspect.getsource(runner_teardown._resolve_manager_execution_state)
    persisted_arg = src.split("run_cancel_window_and_persist(", 1)[1]
    assert "collapse_duplicate_perp_closes(teardown_intents).dispatch" in persisted_arg, (
        "the plan handed to run_cancel_window_and_persist must be the COLLAPSED dispatch "
        "list; persisting the pre-collapse plan lets a resume submit the duplicate close"
    )


def test_the_guard_never_faults_the_teardown_lane(monkeypatch):
    """VIB-6341: "never raises" must hold for the WHOLE body, not just the read.

    Teardown's first job is to remove on-chain risk. A guard that faults the lane
    strands the position it was protecting -- strictly worse than the duplicate
    close it prevents. The identity read was guarded; the collapse itself was not
    (#3574 audit), so a fault in the union-find or in a connector-published
    identity hook could take the lane down.
    """
    from almanak.framework.teardown import single_close_guard as g

    def _boom(*_a, **_k):
        raise RuntimeError("collapse body exploded")

    # Patch a symbol reached ONLY AFTER the alias read. The pre-fix code already
    # guarded that read, so faulting it would exercise the branch that was always
    # protected and the test would pass on pre-fix code -- inert as a control.
    # ``_describe`` is called only on the drop branch, i.e. inside the union-find
    # body this fix newly covers (#3574 delta review).
    monkeypatch.setattr(g, "_describe", _boom)

    # Address-space pair so the drop branch — the only caller of ``_describe`` —
    # is actually reached; a non-collapsing pair would never fault and the
    # control would be vacuous.
    rows = [
        _perp(VENUE_KEY),
        _perp("gmx-ETH/USD-arbitrum"),
    ]
    plan = g.collapse_duplicate_perp_closes(full_close_intents(rows))

    # The plan survives unchanged rather than the lane dying.
    assert len(_closes(plan.dispatch)) == 2, "a faulted guard must leave the plan intact"
    assert plan.dropped == []
    assert plan.for_coverage == plan.dispatch
