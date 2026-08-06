"""Teardown enumeration must follow the VENUE, never a submission-time belief.

Three GMX strategies decided whether teardown emits a close from their own
cached bookkeeping, and each cached it at the wrong moment:

* ``rsi_martingale_short`` mutated the martingale ladder while BUILDING the
  intent and had no ``on_intent_executed`` at all, so a submission that never
  filled still counted. The dangerous direction is the close: the builder wiped
  the ladder before the close settled, so a failed close left the strategy
  believing it was flat while the short was live — ``get_open_positions()``
  returned nothing and teardown emitted no close for it.
* ``hedged_lp_weth_usdc`` keyed the hedge's EXISTENCE on ``_perp_entry_price``.
  GMX V2 settles in two steps, so a successful open legitimately reports no
  entry price, and the ``_pending_perp_price`` fallback was read but never
  assigned anywhere in that file. An open with no reported price therefore made
  a live short invisible to teardown.
* ``leverage_loop_cross_chain`` emitted perp_close + repay + withdraw
  UNCONDITIONALLY, disagreeing with its own carefully-gated preview: a teardown
  on an idle deployment previewed zero positions and fired three reverting
  intents.

Each test below is a negative control for one fix: revert that fix and the test
fails. They assert the direction that loses money — a position that exists must
be enumerable — rather than merely that a field is set.

Related: VIB-6498 (the class), VIB-6497 (false-success teardown), VIB-6159.
"""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _load(rel_dir: str, cls_name: str, module_tag: str):
    """Import a strategy module by path and build an instance without the base __init__."""
    seed_dir = _REPO_ROOT / rel_dir
    spec = importlib.util.spec_from_file_location(module_tag, seed_dir / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cfg = json.loads((seed_dir / "config.json").read_text(encoding="utf-8"))
    cls = getattr(module, cls_name)
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strat = cls.__new__(cls)
        strat._config = cfg
        strat.config = cfg
        strat.get_config = lambda k, d=None: cfg.get(k, d)
        # Set before __init__: some of these strategies resolve token decimals
        # off self.chain while constructing.
        strat._chain = "arbitrum"
        cls.__init__(strat)
    strat._deployment_id = "deployment:test"
    strat.create_market_snapshot = MagicMock(return_value=MagicMock())
    return module, strat


def _intent(type_name: str, **kw):
    return SimpleNamespace(intent_type=SimpleNamespace(value=type_name), **kw)


def _probe(state: str, *, is_long: bool = False, notional: Decimal | None = Decimal("100")):
    """Stand-in for a PerpProbe: OPEN / FLAT / UNMEASURED."""
    from almanak.framework.strategies import PerpProbe, PerpProbePosition, PerpProbeState

    if state == "OPEN":
        return PerpProbe(
            PerpProbeState.OPEN,
            positions=(
                PerpProbePosition(
                    is_long=is_long,
                    market="ETH/USD",
                    collateral_token="USDC",
                    notional_usd=notional,
                ),
            ),
        )
    return PerpProbe(getattr(PerpProbeState, state), reason="test")


# =============================================================================
# rsi_martingale_short — state is committed on settlement, never on submission
# =============================================================================


@pytest.fixture
def rsi():
    _, strat = _load(
        "strategies/incubating/rsi_martingale_short",
        "RSIMartingaleShortStrategy",
        "rsi_mart_test",
    )
    return strat


def test_failed_open_does_not_create_a_phantom_ladder(rsi):
    """A PERP_OPEN that fails must leave the ladder empty.

    Before the fix, _create_open_intent() appended to _entry_prices as a side
    effect of BUILDING the intent, so a rejected order still produced a
    position the strategy would then double down on.
    """
    rsi._create_open_intent(Decimal("3000"), level=0)
    assert rsi._has_position is False, "building an intent must not create a position"

    rsi.on_intent_executed(_intent("PERP_OPEN"), success=False, result=None)

    assert rsi._has_position is False
    assert rsi._total_position_size_usd == Decimal("0")
    assert rsi._trades_opened == 0


def test_successful_open_commits_the_ladder(rsi):
    """Liveness control: the commit path must actually fire, or the test above is vacuous."""
    rsi._create_open_intent(Decimal("3000"), level=0)
    rsi.on_intent_executed(_intent("PERP_OPEN"), success=True, result=None)

    assert rsi._has_position is True
    assert rsi._total_position_size_usd > Decimal("0")
    assert rsi._trades_opened == 1


def test_failed_close_retains_the_position(rsi):
    """The money case: a close that fails must NOT report the strategy flat.

    Before the fix, _create_close_intent() cleared _entry_prices on build, so a
    failed close left a live short with no cached evidence — teardown then
    emitted nothing and completed as a no-op.
    """
    rsi._create_open_intent(Decimal("3000"), level=0)
    rsi.on_intent_executed(_intent("PERP_OPEN"), success=True, result=None)
    size_before = rsi._total_position_size_usd

    rsi._create_close_intent(reason="take_profit")
    rsi.on_intent_executed(_intent("PERP_CLOSE"), success=False, result=None)

    assert rsi._has_position is True, "a failed close must not report the ladder flat"
    assert rsi._total_position_size_usd == size_before
    assert rsi._trades_closed == 0
    assert rsi._wins == 0, "a win must not be booked for a close that never settled"


def test_unmeasured_venue_read_is_not_a_flat_account(rsi):
    """UNMEASURED must fall back to the cache, never collapse to 'no positions' (VIB-6497)."""
    rsi._create_open_intent(Decimal("3000"), level=0)
    rsi.on_intent_executed(_intent("PERP_OPEN"), success=True, result=None)

    with patch.object(rsi, "_venue_probe", return_value=_probe("UNMEASURED")):
        summary = rsi.get_open_positions()
        intents = rsi.generate_teardown_intents(mode=None)

    assert len(summary.positions) == 1
    row = summary.positions[0]
    assert row.details["position_source"] == "strategy_cache_unverified"
    assert row.details["value_usd_unknown"] is True
    assert row.value_usd > Decimal("0.01"), "an unmeasured row must not be droppable as dust"
    assert len(intents) == 1, "an unmeasured read must still emit a close"


def test_venue_open_is_reported_even_when_the_ladder_is_empty(rsi):
    """The divergence the probe exists for: venue holds a position the cache never recorded."""
    assert rsi._has_position is False

    with patch.object(rsi, "_venue_probe", return_value=_probe("OPEN", notional=Decimal("250"))):
        summary = rsi.get_open_positions()
        intents = rsi.generate_teardown_intents(mode=None)

    assert len(summary.positions) == 1
    assert summary.positions[0].details["position_source"] == "venue"
    assert summary.positions[0].value_usd == Decimal("250")
    assert len(intents) == 1
    assert intents[0].size_usd is None, "a full close resolves its size live (VIB-5465)"


def test_venue_flat_retracts_a_stale_ladder(rsi):
    """A measured-flat venue must win over the cache, so no phantom residual is published."""
    rsi._create_open_intent(Decimal("3000"), level=0)
    rsi.on_intent_executed(_intent("PERP_OPEN"), success=True, result=None)

    with patch.object(rsi, "_venue_probe", return_value=_probe("FLAT")):
        assert rsi.get_open_positions().positions == []
        assert rsi.generate_teardown_intents(mode=None) == []


def test_unmeasured_24h_change_does_not_arm_an_entry(rsi):
    """Empty != Zero: an unavailable rally filter must veto, not silently pass."""
    market = MagicMock()
    market.price.return_value = Decimal("3000")
    market.rsi.return_value = SimpleNamespace(value=Decimal("90"))
    market.price_data.side_effect = ValueError("no 24h data")

    intent = rsi.decide(market)

    assert intent.intent_type.value == "HOLD"
    assert "UNMEASURED" in (intent.reason or "")
    assert rsi._pending_open is None


# =============================================================================
# hedged_lp_weth_usdc — hedge EXISTENCE is not its entry price
# =============================================================================


@pytest.fixture
def hedged():
    _, strat = _load(
        "strategies/incubating/hedged_lp_weth_usdc",
        "HedgedLpWethUsdcStrategy",
        "hedged_lp_test",
    )
    return strat


def test_open_without_entry_price_still_registers_the_hedge(hedged):
    """A successful PERP_OPEN reporting no entry_price must not hide the short.

    This is the exact GMX two-step case. Before the fix every teardown surface
    gated on _perp_entry_price, whose only fallback (_pending_perp_price) was
    never assigned, so the hedge became invisible.
    """
    hedged.on_intent_executed(
        _intent("PERP_OPEN"), success=True, result=SimpleNamespace(extracted_data={})
    )

    assert hedged._perp_open is True
    assert hedged._perp_entry_price is None, "price is genuinely unknown here"

    with patch.object(hedged, "_venue_probe", return_value=_probe("UNMEASURED")):
        summary = hedged.get_open_positions()
        intents = hedged.generate_teardown_intents(mode=None)

    perps = [p for p in summary.positions if p.protocol == "gmx_v2"]
    assert len(perps) == 1, "a live hedge with no entry price must still be enumerated"
    assert any(getattr(i, "intent_type", None) for i in intents)
    assert len(intents) == 1, "and teardown must emit a close for it"


def test_hedge_existence_survives_a_restart_without_an_entry_price(hedged):
    """_perp_open must round-trip through persistence; deriving it from the price was the bug."""
    hedged.on_intent_executed(
        _intent("PERP_OPEN"), success=True, result=SimpleNamespace(extracted_data={})
    )
    state = hedged.get_persistent_state()
    assert state["perp_open"] is True

    _, restored = _load(
        "strategies/incubating/hedged_lp_weth_usdc",
        "HedgedLpWethUsdcStrategy",
        "hedged_lp_test_restore",
    )
    restored.load_persistent_state(state)

    assert restored._perp_open is True, "restart must not lose the hedge"


def test_failed_close_leaves_the_machine_retryable(hedged):
    """A failed close must not park the state machine in 'closing' forever."""
    hedged._lp_position_id = 123
    hedged._perp_open = True
    hedged._state = "closing"

    hedged.on_intent_executed(_intent("PERP_CLOSE"), success=False, result=None)

    assert hedged._state != "closing", "state must leave 'closing' so the next tick can retry"


def test_orphaned_hedge_is_retried_rather_than_held_forever(hedged):
    """LP closed, perp close failed: the next tick must retry, not HOLD forever.

    The rebalance route nulls the LP tick bounds on LP_CLOSE. The failure
    handler then returns the machine to "hedged" because the hedge is still
    live — but the drift check requires _lp_tick_lower, so decide() fell through
    to HOLD on every later tick while a real short stayed on the venue.
    """
    hedged.force_action = ""
    hedged._state = "hedged"
    hedged._lp_position_id = None
    hedged._lp_tick_lower = None
    hedged._lp_tick_upper = None
    hedged._perp_open = True

    market = MagicMock()
    market.price.side_effect = lambda sym: (
        Decimal("3000") if sym == hedged.token0_symbol else Decimal("1")
    )

    result = hedged.decide(market)

    assert hedged._state == "closing", "an orphaned hedge must re-enter the closing path"
    emitted = [i.intent_type.value for i in getattr(result, "intents", [result])]
    assert emitted == ["PERP_CLOSE"], f"expected a retry of the orphaned leg, got {emitted}"


def test_teardown_emission_probes_fresh_never_a_stored_observation(hedged):
    """A stored probe can be arbitrarily stale — get_open_positions() is also
    called by heartbeat snapshots, registry enumeration, and CLI residual
    checks, with no paired generate_teardown_intents(). A stale FLAT consumed
    at teardown would SILENTLY omit the close. So the emission must observe the
    venue itself: an earlier FLAT enumeration must not suppress a close when
    the venue reports OPEN at emission time. (Two fresh probes can disagree
    within a pass; that fails the VIB-5469 completeness check LOUDLY, which is
    the safe direction. One pass-scoped observation is framework work,
    VIB-6585.)"""
    hedged._perp_open = True
    stale_then_fresh = MagicMock(side_effect=[_probe("FLAT"), _probe("OPEN")])
    with patch.object(hedged, "_venue_probe", stale_then_fresh):
        hedged.get_open_positions()  # heartbeat-style read observes FLAT
        intents = hedged.generate_teardown_intents(mode=None)

    assert stale_then_fresh.call_count == 2, "emission must take its own observation"
    assert len(intents) == 1, "the fresh OPEN observation must emit the close"


def test_rsi_teardown_emission_probes_fresh_never_a_stored_observation(rsi):
    """Same fresh-observation contract on the martingale ladder."""
    rsi._create_open_intent(Decimal("3000"), level=0)
    rsi.on_intent_executed(_intent("PERP_OPEN"), success=True, result=None)
    stale_then_fresh = MagicMock(side_effect=[_probe("FLAT"), _probe("OPEN")])
    with patch.object(rsi, "_venue_probe", stale_then_fresh):
        rsi.get_open_positions()
        intents = rsi.generate_teardown_intents(mode=None)

    assert stale_then_fresh.call_count == 2, "emission must take its own observation"
    assert len(intents) == 1


# =============================================================================
# hedged_lp_wbtc_usdt — idle is decided by EXISTENCE, not by an entry price
# =============================================================================


@pytest.fixture
def wbtc():
    _, strat = _load(
        "strategies/incubating/hedged_lp_wbtc_usdt",
        "HedgedLpWbtcUsdtStrategy",
        "hedged_wbtc_test",
    )
    return strat


def test_lp_close_does_not_go_idle_over_a_live_hedge(wbtc):
    """A live short with no reported entry price must not drop the machine to idle.

    GMX V2 settles in two steps, so _perp_entry_price is legitimately None on a
    successful open. Reading it as "the perp is closed" set state=idle over a
    live hedge, and decide() then opened a whole second position on top of it.
    """
    wbtc._state = "hedged"
    wbtc._lp_position_id = 123
    wbtc._perp_open = True
    wbtc._perp_entry_price = None

    wbtc.on_intent_executed(_intent("LP_CLOSE"), success=True, result=None)

    assert wbtc._state != "idle", "a live hedge must keep the machine out of idle"


def test_lp_close_goes_idle_when_the_hedge_is_actually_closed(wbtc):
    """Liveness control: with no hedge left, the idle transition must still fire."""
    wbtc._state = "closing"
    wbtc._lp_position_id = 123
    wbtc._perp_open = False
    wbtc._perp_entry_price = None

    wbtc.on_intent_executed(_intent("LP_CLOSE"), success=True, result=None)

    assert wbtc._state == "idle"


# =============================================================================
# gmx_dca_ladder — the teardown lane must not spend the ladder's budget
# =============================================================================


@pytest.fixture
def ladder():
    _, strat = _load(
        "strategies/experiments/gmx_dca_ladder_avax",
        "GMXDCALadderStrategy",
        "gmx_ladder_test",
    )
    return strat


def test_failed_teardown_close_does_not_spend_the_partial_budget(ladder):
    """A full close (size_usd=None) is the teardown lane, not the de-risk rung.

    Teardown is routed through this same callback, so counting its failures here
    exhausted max_rung_attempts and ABANDONED a partial de-risk the iteration
    lane had never even attempted.
    """
    for _ in range(ladder.max_rung_attempts + 1):
        ladder.on_intent_executed(
            _intent("PERP_CLOSE", size_usd=None), success=False, result=None
        )

    assert ladder._partial_attempts == 0, "the teardown lane must not spend the ladder budget"


def test_failed_partial_close_does_spend_the_partial_budget(ladder):
    """Liveness control: the sized partial close is what the cap exists for."""
    ladder.on_intent_executed(
        _intent("PERP_CLOSE", size_usd=Decimal("5")), success=False, result=None
    )

    assert ladder._partial_attempts == 1


# =============================================================================
# leverage_loop_cross_chain — teardown must agree with its own preview
# =============================================================================


@pytest.fixture
def loop():
    # This strategy takes an explicit typed config positional, unlike the
    # get_config()-style siblings above, so it needs its own construction.
    seed_dir = _REPO_ROOT / "strategies/incubating/leverage_loop_cross_chain"
    spec = importlib.util.spec_from_file_location("lev_loop_test", seed_dir / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strat = module.LeverageLoopStrategy.__new__(module.LeverageLoopStrategy)
        module.LeverageLoopStrategy.__init__(strat, module.LeverageLoopConfig())
    # `config` is normally set by the patched-out base __init__.
    strat.config = module.LeverageLoopConfig()
    strat._chain = "base"
    strat._deployment_id = "deployment:test"
    strat.create_market_snapshot = MagicMock(return_value=MagicMock())
    return module, strat


def test_teardown_emits_nothing_when_nothing_is_open(loop):
    """An idle deployment previewed zero positions but fired three reverting intents."""
    _, strat = loop
    strat.get_open_positions = MagicMock(
        return_value=SimpleNamespace(positions=[], deployment_id="d", timestamp=None)
    )

    assert strat.generate_teardown_intents(mode=None) == []


def test_teardown_closes_only_what_is_reported(loop):
    """Liveness control: a reported PERP leg must still produce exactly one close."""
    from almanak.framework.teardown import PositionInfo, PositionType

    _, strat = loop
    strat.get_open_positions = MagicMock(
        return_value=SimpleNamespace(
            positions=[
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id="leverage_loop_perp_0",
                    chain="arbitrum",
                    protocol="gmx_v2",
                    value_usd=Decimal("10"),
                    details={},
                )
            ],
            deployment_id="d",
            timestamp=None,
        )
    )

    intents = strat.generate_teardown_intents(mode=None)

    assert len(intents) == 1
    assert intents[0].intent_type.value == "PERP_CLOSE"


def test_venue_open_perp_is_closed_even_when_the_phase_row_was_lost(loop):
    """The venue holds an ETH long the cached phase knows nothing about.

    The perp leg used to be gated purely on ``_phase == POSITIONED``, and
    ``load_persistent_state`` defaults an unknown / missing / corrupt phase row
    to IDLE. A lost row therefore made a live GMX long invisible to teardown,
    which then completed as a no-op over real collateral.
    """
    module, strat = loop
    strat._phase = module.LeverageLoopPhase.IDLE

    # create=True so reverting the strategy fix fails this test on BEHAVIOUR
    # (no perp row) rather than on a missing-attribute error.
    with patch.object(strat, "_venue_probe", return_value=_probe("OPEN", is_long=True), create=True):
        summary = strat.get_open_positions()
        intents = strat.generate_teardown_intents(mode=None)

    perps = [p for p in summary.positions if p.protocol == "gmx_v2"]
    assert len(perps) == 1, "a venue-held long must be enumerated regardless of the phase"
    assert perps[0].details["position_source"] == "venue"
    assert [i.intent_type.value for i in intents] == ["PERP_CLOSE"]
    assert intents[0].size_usd is None, "a full close resolves its size live (VIB-5465)"


def test_unmeasured_aave_read_with_positioned_phase_still_emits_the_unwind(loop):
    """A provider outage is not a repaid loan.

    The Aave legs were emitted only when the health-factor probe returned a
    Decimal. An exception and "no position" landed in the same branch, so an
    UNMEASURED read over a live leveraged position emitted no repay/withdraw and
    teardown reported success with the debt still outstanding.
    """
    module, strat = loop
    strat._phase = module.LeverageLoopPhase.POSITIONED
    strat.create_market_snapshot = MagicMock(side_effect=RuntimeError("provider down"))

    # FLAT perp isolates this test on the Aave legs; the perp leg has its own.
    with patch.object(strat, "_venue_probe", return_value=_probe("FLAT"), create=True):
        summary = strat.get_open_positions()
        intents = strat.generate_teardown_intents(mode=None)

    by_type = {p.position_type.value: p for p in summary.positions}
    assert set(by_type) == {"SUPPLY", "BORROW"}, "an unreadable HF must not drop the legs"
    for row in by_type.values():
        assert row.details["aave_source"] == "strategy_cache_unverified"
        assert row.details["value_usd_unknown"] is True
        assert row.details["valuation_status"] == "no_path"

    assert [i.intent_type.value for i in intents] == ["REPAY", "WITHDRAW"]


def test_fresh_wallet_previews_no_phantom_legs(loop):
    """The property the old skip defended: nothing open means nothing emitted.

    No live health factor, phase IDLE, and a MEASURED-flat venue is the only
    combination that supports a negative claim on every leg.
    """
    module, strat = loop
    strat._phase = module.LeverageLoopPhase.IDLE

    with patch.object(strat, "_venue_probe", return_value=_probe("FLAT"), create=True):
        summary = strat.get_open_positions()
        intents = strat.generate_teardown_intents(mode=None)

    assert summary.positions == [], "a fresh wallet must not preview phantom legs"
    assert intents == []


def test_primary_sequence_is_the_bridge_alone(loop):
    """Steps 2-4 must not ride along with an asynchronous cross-chain swap.

    The Arbitrum tail used to execute the moment the Base tx confirmed, against
    WETH that had not yet settled, so Intent.supply(amount="all") supplied
    nothing. The tail now belongs solely to the post-bridge continuation.
    """
    _, strat = loop
    seq = strat._build_leverage_loop_sequence()

    assert len(seq.intents) == 1
    assert seq.intents[0].intent_type.value == "SWAP"
    assert seq.intents[0].destination_chain == "arbitrum"

    tail = strat._build_post_bridge_sequence()
    assert [i.intent_type.value for i in tail.intents] == ["SUPPLY", "BORROW", "PERP_OPEN"]
