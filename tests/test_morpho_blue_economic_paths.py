"""Morpho modeled liquidation, re-anchoring and evidence scopes on literal markets."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.types import UnixTime
from almanak.connectors.morpho_blue.domain import Position as FundPosition
from almanak.connectors.morpho_blue.economic_account import validate_ownership
from almanak.connectors.morpho_blue.economic_data import RiskSample, key
from almanak.connectors.morpho_blue.economic_market import committed, reanchor, seed
from almanak.connectors.morpho_blue.economic_positions import account_view
from almanak.connectors.morpho_blue.economic_state import (
    ActionResult,
    Event,
    MarketInputGap,
)
from almanak.models import AssetAmount
from almanak.strategy_api import SimulationEvidence
from tests.lending_economic_world import DEADLINE, MORPHO, START, at, world
from tests.test_morpho_blue_offline import BORROWER, LENDER, MARKET, SHARES, USDC, WETH, observation

# One WETH at 2000 USDC supports 1720 USDC of debt at the 86% LLTV.
UNDERWATER = 1800 * 10**6 * SHARES
HEALTHY = 1000 * 10**6 * SHARES
COLLATERAL = {"collateral": 10**18}


def borrowing(shares, **options):
    run = world(MORPHO, **options)
    return run, run.holding(collateral=COLLATERAL, loan={"borrow_shares": shares})


def blocking(state, fund=None):
    """A recorded event leaving the market (or one fund in it) without history."""
    gap = InputUnavailable(key(MARKET, "risk"), START, "synthetic gap").gap
    affected = SimpleNamespace(source=MARKET, fund=fund, gap=gap)
    result = SimpleNamespace(affected_markets=(affected,), unavailable=(gap,), market_changes=())
    return state.model_copy(
        update={"events": (SimpleNamespace(transition=SimpleNamespace(result=result)),)}
    )


# Market events


def test_an_owned_market_waits_for_its_next_prepared_boundary():
    run = world(MORPHO)
    assert run.model.next_event(run.state, DEADLINE) == START + 12
    assert run.model.next_event(run.state, START + 5) is None
    passed = run.state.model_copy(update={"event_cursor": UnixTime(START + 5)})
    assert run.model.next_event(passed, START + 5) is None
    assert run.model.external_event(run.state, UnixTime(START)) is None


def test_an_accounting_boundary_reanchors_the_shared_market():
    run = world(MORPHO)
    state = at(run.state, START + 48)
    assert run.model.next_event(state, DEADLINE) == START + 60
    transition = run.model.external_event(at(run.state, START + 60), UnixTime(START + 60))
    event = transition.result
    assert event.kind == "accounting" and event.liquidations == ()
    (change,) = event.market_changes
    assert (change.after.anchor_time, change.after.generation) == (START + 60, 1)
    # Unchanged external samples leave the hypothetical totals' net change at zero.
    assert change.after.accounting.supply_shares == change.before.accounting.supply_shares
    assert run.model.deliver_event(state, event) == state.memory


def test_an_underwater_fund_is_liquidated_at_the_first_eligible_second():
    run, state = borrowing(UNDERWATER)
    assert run.model.next_event(state, DEADLINE) == START
    transition = run.model.external_event(state, UnixTime(START))
    event = transition.result
    assert event.kind == "liquidation" and len(event.liquidations) == 1
    assert event.liquidations[0].market in event.market_changes
    collateral, loan = (c.after.payload for c in transition.effects.positions)
    assert collateral.seized_collateral > 0 and loan.borrow_shares == 0
    assert loan.liquidated_shares == UNDERWATER


def test_a_fund_that_cannot_be_liquidated_is_reported_beside_other_liquidations():
    run = world(MORPHO)
    # The lender owes without collateral: nothing can be seized, so the model refuses it.
    state = run.holding(
        collateral=COLLATERAL,
        loan={"borrow_shares": UNDERWATER},
        earn={"borrow_shares": 10 * SHARES},
    )
    event = run.model.external_event(state, UnixTime(START)).result
    assert event.kind == "liquidation" and len(event.liquidations) == 1
    (affected,) = event.affected_markets
    assert affected.fund == LENDER and affected.source == MARKET
    assert event.unavailable == (affected.gap,)


def test_missing_history_is_an_event_naming_the_affected_market():
    run, state = borrowing(HEALTHY)
    late = at(state, DEADLINE + 1)
    assert run.model.next_event(late, DEADLINE + 5) == DEADLINE + 1
    event = run.model.external_event(late, UnixTime(DEADLINE + 1)).result
    assert event.kind == "input_unavailable"
    (affected,) = event.affected_markets
    assert affected.source == MARKET and affected.fund is None


def test_blocked_markets_and_funds_are_not_evaluated_again():
    run, state = borrowing(UNDERWATER)
    blocked = blocking(state)
    assert run.model.next_event(blocked, DEADLINE) is None
    assert run.model.external_event(blocked, UnixTime(START)) is None
    # Only the borrower is blocked: the lender keeps the market, but no one is liquidated.
    borrower = blocking(state, BORROWER)
    assert run.model.external_event(borrower, UnixTime(START)) is None


# Re-anchoring


def test_reanchoring_refuses_another_market_or_out_of_order_samples():
    observed = observation()
    ledger = seed(observed, UnixTime(START))
    later = observed.model_copy(
        update={"block": observed.block.model_copy(update={"number": observed.block.number + 5})}
    )
    other = observed.model_copy(update={"source": MARKET.model_copy(update={"lltv": 10**17})})
    with pytest.raises(ValueError, match="require the same market identity"):
        reanchor(ledger, observed, other, UnixTime(START + 60))
    with pytest.raises(ValueError, match="sample boundary is out of order"):
        reanchor(ledger, observed, later, UnixTime(START))
    future = observed.model_copy(
        update={"block": observed.block.model_copy(update={"timestamp": START + 1})}
    )
    with pytest.raises(ValueError, match="sample boundary is out of order"):
        reanchor(ledger, future, later, UnixTime(START + 60))
    with pytest.raises(ValueError, match="cannot move backward in chain history"):
        reanchor(ledger, later, observed, UnixTime(START + 60))


# Ownership


def views(ledger, **positions):
    observed = observation()
    return {
        name: account_view((), observed, ledger, fund).model_copy(update={"position": position})
        for name, (fund, position) in positions.items()
    }


def test_ownership_counts_each_fund_once_within_market_totals():
    ledger = seed(observation(), UnixTime(START))
    half = ledger.accounting.supply_shares // 2
    owned = views(
        ledger,
        borrower=(BORROWER, FundPosition(supply_shares=half, borrow_shares=0, collateral=0)),
        lender=(LENDER, FundPosition(supply_shares=half, borrow_shares=0, collateral=0)),
    )
    # A fund counted twice through two allocations is still one balance.
    validate_ownership(ledger, (owned["borrower"], owned["borrower"], owned["lender"]))


def test_ownership_refuses_disagreeing_allocations_and_excess_shares():
    ledger = seed(observation(), UnixTime(START))
    totals = ledger.accounting
    owned = views(
        ledger,
        a=(BORROWER, FundPosition(supply_shares=1, borrow_shares=0, collateral=0)),
        b=(BORROWER, FundPosition(supply_shares=2, borrow_shares=0, collateral=0)),
        supply=(
            LENDER,
            FundPosition(supply_shares=totals.supply_shares, borrow_shares=0, collateral=0),
        ),
        borrow=(
            LENDER,
            FundPosition(supply_shares=0, borrow_shares=totals.borrow_shares, collateral=0),
        ),
        debt=(BORROWER, FundPosition(supply_shares=0, borrow_shares=1, collateral=0)),
    )
    with pytest.raises(ValueError, match="allocations disagree about their fund position"):
        validate_ownership(ledger, (owned["a"], owned["b"]))
    with pytest.raises(ValueError, match="aggregate owned shares exceed market totals"):
        validate_ownership(ledger, (owned["a"], owned["supply"]))
    with pytest.raises(ValueError, match="aggregate owned shares exceed market totals"):
        validate_ownership(ledger, (owned["debt"], owned["borrow"]))
    stale = committed(ledger, ledger).after
    with pytest.raises(ValueError, match="requires one current market ledger"):
        validate_ownership(stale, (owned["a"],))


# Risk samples


def test_risk_samples_require_one_complete_timestamp_group_and_its_following_block():
    observed = observation()
    block = observed.block
    following = block.model_copy(
        update={"number": block.number + 1, "timestamp": block.timestamp + 12}
    )
    earlier = block.model_copy(update={"number": block.number - 1})
    RiskSample(market=observed, block_group=(earlier, block), following_block=following)
    with pytest.raises(ValueError, match="requires the last block of its timestamp group"):
        RiskSample(market=observed, block_group=(earlier,), following_block=following)
    gap = block.model_copy(update={"number": block.number - 2})
    with pytest.raises(ValueError, match="timestamp group is incomplete or unordered"):
        RiskSample(market=observed, block_group=(gap, block), following_block=following)
    same_time = following.model_copy(update={"timestamp": block.timestamp})
    with pytest.raises(ValueError, match="immediate following time boundary"):
        RiskSample(market=observed, block_group=(block,), following_block=same_time)


# Evidence scopes


def revalidated(model, **changes):
    return type(model).model_validate({**dict(model), **changes})


def test_snapshots_refuse_another_target_time_payer_or_missing_evidence():
    run, state = borrowing(HEALTHY)
    observed = run.model.snapshot(state, state.portfolio.positions[1].payload)
    assert revalidated(observed) == observed
    assert observed.asset.identifier == USDC
    with pytest.raises(ValueError, match="differs from its fund/market target"):
        revalidated(observed, target=run.target("earn"))
    with pytest.raises(ValueError, match="differs from its accounting time"):
        revalidated(observed, effective_at=datetime.fromtimestamp(START + 1, UTC))
    with pytest.raises(ValueError, match="gas evidence differs from its payer scope"):
        revalidated(observed, native_balance=AssetAmount(asset=USDC, amount=0))
    with pytest.raises(ValueError, match="requires retained input evidence"):
        revalidated(observed, simulation=observed.simulation.model_copy(update={"inputs": ()}))
    collateral = run.model.snapshot(state, state.portfolio.positions[0].payload)
    assert collateral.asset.identifier == WETH


def action(**changes):
    run = world(MORPHO)
    ledger = seed(observation(), UnixTime(START))
    values = {
        "intent": run.config.supply("earn", 10**6),
        "success": True,
        "assets": AssetAmount(asset=USDC, amount=10**6),
        "market_change": committed(ledger, ledger),
        "effective_at": datetime.fromtimestamp(START, UTC),
        "simulation": SimulationEvidence(execution_id="x", model="m", inputs=("a" * 64,)),
    }
    return ActionResult.model_validate(values | changes)


def test_action_results_keep_effects_failures_and_evidence_consistent():
    assert action().status == "completed"
    assert (
        action(success=False, assets=None, market_change=None, error="refused").status == "failed"
    )
    cases = [
        ({"error": "refused"}, "failures and input gaps must remain visible"),
        ({"market_change": None}, "success requires its assets and market change"),
        (
            {"success": False, "market_change": None, "error": "refused"},
            "failed modeled Morpho action cannot retain an economic effect",
        ),
        ({"assets": AssetAmount(asset=WETH, amount=1)}, "differs from its target asset"),
        (
            {"effective_at": datetime.fromtimestamp(START + 1, UTC)},
            "differs from its market or logical time",
        ),
        (
            {"simulation": SimulationEvidence(execution_id="x", model="m", inputs=())},
            "requires retained evidence",
        ),
    ]
    for changes, text in cases:
        with pytest.raises(ValueError, match=text):
            action(**changes)


def test_events_keep_their_kind_gaps_liquidations_time_and_evidence_consistent():
    run, state = borrowing(UNDERWATER)
    event = run.model.external_event(state, UnixTime(START)).result
    assert revalidated(event) == event
    gap = InputUnavailable(key(MARKET, "risk"), START, "synthetic gap").gap
    affected = MarketInputGap(source=MARKET, gap=gap)
    (change,) = event.market_changes
    moved = change.model_copy(
        update={"after": change.after.model_copy(update={"effective_at": START + 1})}
    )
    cases = [
        ({"kind": "accounting"}, "event kind differs from its economic effects"),
        ({"unavailable": (gap,)}, "input gaps require their affected market scopes"),
        ({"market_changes": ()}, "liquidation lacks its shared market change"),
        (
            {"liquidations": (), "kind": "accounting", "market_changes": (moved,)},
            "differ from its logical time",
        ),
        (
            {"simulation": event.simulation.model_copy(update={"inputs": ()})},
            "market events require retained evidence",
        ),
    ]
    for changes, text in cases:
        with pytest.raises(ValueError, match=text):
            revalidated(event, **changes)
    unavailable = revalidated(
        event,
        kind="input_unavailable",
        liquidations=(),
        market_changes=(),
        unavailable=(gap,),
        affected_markets=(affected,),
    )
    assert isinstance(unavailable, Event)


def test_market_gaps_stay_within_their_market_and_chain():
    gap = InputUnavailable(key(MARKET, "accounting"), START, "synthetic gap").gap
    MarketInputGap(source=MARKET, gap=gap, fund=BORROWER)
    other = gap.model_copy(
        update={"key": key(MARKET, "accounting").model_copy(update={"source": "other"})}
    )
    with pytest.raises(ValueError, match="input gap differs from its affected market"):
        MarketInputGap(source=MARKET, gap=other)
    elsewhere = BORROWER.model_copy(
        update={"chain": BORROWER.chain.model_copy(update={"environment": "fork"})}
    )
    with pytest.raises(ValueError, match="fund has another chain scope"):
        MarketInputGap(source=MARKET, gap=gap, fund=elsewhere)
