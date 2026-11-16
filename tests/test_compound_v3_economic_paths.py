"""Comet economic absorption, evidence scopes and discovery refusals on literal markets."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from almanak.backtest.types import UnixTime
from almanak.caip import AssetType
from almanak.connectors.compound_v3.domain import ExtendedControls, Revision
from almanak.connectors.compound_v3.economic_acquisition import Discovery, validate_market
from almanak.connectors.compound_v3.economic_actions import check_utilization
from almanak.connectors.compound_v3.economic_liquidation import liquidate
from almanak.connectors.compound_v3.economic_state import ActionResult, Position, Snapshot
from almanak.connectors.compound_v3.liquidation import seizure
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount, AssetMetadata
from almanak.strategy_api import SimulationEvidence
from tests.lending_economic_cases import CASES
from tests.lending_economic_world import COMPOUND, DEADLINE, START, at, world
from tests.test_compound_v3_offline import (
    BASE_FEED,
    MARKET,
    USDC,
    WETH,
    WETH_FEED,
    economic_market,
    parameters,
)
from tests.test_compound_v3_risk import account, collateral_change

# One WETH at 2000 USD supports 1700 USDC before absorption (85% liquidate factor). The
# borrow index of 1.02 makes this principal a debt of 1836 USDC.
UNDERWATER = -1800 * 10**6
HEALTHY = -1000 * 10**6
COLLATERAL = {"collateral": 10**18, "collateral_enabled": True}


def borrowing(principal, **options):
    run = world(COMPOUND, **options)
    return run, run.holding(collateral=COLLATERAL, loan={"principal": principal})


def group(run, state, fund="borrower"):
    return tuple(p.payload for p in state.portfolio.positions if p.payload.target.account == fund)


# Absorption


def test_an_underwater_fund_is_absorbed_whole():
    run, state = borrowing(UNDERWATER)
    change = liquidate(run.config, group(run, state), economic_market())
    (effect,) = change.absorptions
    collateral, loan = change.positions
    assert (collateral.collateral, collateral.seized_collateral) == (0, 10**18)
    assert collateral.collateral_enabled is False
    assert loan.principal >= 0
    assert loan.absorption_credit == effect.absorption.collateral_credit.amount > 0
    assert effect.indices.borrow == economic_market().borrow_index


def test_a_healthy_fund_is_left_unchanged():
    run, state = borrowing(HEALTHY)
    positions = group(run, state)
    change = liquidate(run.config, positions, economic_market())
    assert change == type(change)(positions=positions, absorptions=())


def test_absorption_refuses_an_empty_group_or_mixed_funds():
    run, state = borrowing(UNDERWATER)
    with pytest.raises(ValueError, match="requires a configured fund group"):
        liquidate(run.config, (), economic_market())
    mixed = (*group(run, state), *group(run, state, "lender"))
    with pytest.raises(ValueError, match="requires one exact fund/market group"):
        liquidate(run.config, mixed, economic_market())


def test_absorption_is_unavailable_while_absorb_is_paused():
    run, state = borrowing(UNDERWATER)
    with pytest.raises(ValueError, match="unavailable while absorb is paused"):
        liquidate(run.config, group(run, state), economic_market(pause_flags=1 << 3))


@pytest.mark.parametrize(("offset", "expected"), [(0, (0, 0)), (1, (10**18, None))])
def test_seizure_takes_only_active_collateral(offset, expected):
    observed = account("base")
    amount, credit = seizure(observed, observed.market.collaterals[offset])
    assert amount == expected[0]
    if expected[1] is None:
        collateral = observed.market.collaterals[offset]
        value = amount * collateral.price.price // collateral.scale
        assert credit == value * collateral.liquidation_factor // 10**18 > 0
    else:
        assert credit == 0


def test_controlled_seizure_keeps_zero_factor_collateral_and_zero_price_entries():
    observed = account("base")
    assert observed.market.revision is Revision.CONTROLS
    kept = collateral_change(observed, 1, liquidation_factor=0)
    assert seizure(kept, kept.market.collaterals[1]) == (0, 0)
    unpriced = collateral_change(observed, 1, liquidate_factor=0)
    assert seizure(unpriced, unpriced.market.collaterals[1]) == (10**18, 0)


def test_standard_seizure_prices_collateral_whatever_its_liquidate_factor():
    observed = account("mainnet")
    assert observed.market.revision is Revision.STANDARD
    standard = collateral_change(observed, 2, liquidate_factor=0)
    amount, credit = seizure(standard, standard.market.collaterals[2])
    assert amount == 10**18 and credit > 0


# Market limits


def controlled(market):
    controls = ExtendedControls(
        pause_flags=0,
        collateral_supply_flags=0,
        collateral_withdraw_flags=0,
        collateral_transfer_flags=0,
        deactivated_collaterals=0,
    )
    observed = market.observed.model_copy(
        update={"controls": controls, "revision": Revision.CONTROLS}
    )
    return replace(market, sample=market.sample.model_copy(update={"market": observed}))


def test_utilization_is_checked_only_under_controls_and_includes_hypothetical_debt():
    config = parameters()
    market = economic_market()
    loan = config.target("loan")
    # 16 million more debt takes 21 million of 10 million supplied past the 200% ceiling.
    heavy = (Position(target=loan, principal=-16_000_000 * 10**6, collateral=0),)
    check_utilization(market, heavy)
    check_utilization(
        controlled(market), (Position(target=loan, principal=-(10**6), collateral=0),)
    )
    with pytest.raises(ValueError, match="exceeds the supported market utilization"):
        check_utilization(controlled(market), heavy)
    # Hypothetical supply raises the denominator: 10 million more supply admits the debt.
    lender = Position(target=config.target("earn"), principal=10_000_000 * 10**6, collateral=0)
    check_utilization(controlled(market), (*heavy, lender))


# Evidence scopes


def snapshot():
    run, state = borrowing(HEALTHY)
    return run.model.snapshot(state, state.portfolio.positions[1].payload)


def revalidated(model, **changes):
    return type(model).model_validate({**dict(model), **changes})


def test_snapshots_refuse_future_market_evidence_and_index_drift():
    observed = snapshot()
    assert revalidated(observed) == observed
    early = datetime.fromtimestamp(START - 1, UTC)
    with pytest.raises(ValueError, match="cannot use future market evidence"):
        revalidated(observed, effective_at=early)
    drifted = observed.account.model_copy(
        update={"borrowed_base": observed.account.borrowed_base + 1}
    )
    with pytest.raises(ValueError, match="balances differ from their modeled indices"):
        revalidated(observed, account=drifted)


def test_snapshots_require_payer_scoped_gas_and_input_evidence():
    observed = snapshot()
    with pytest.raises(ValueError, match="gas evidence differs from its payer scope"):
        revalidated(observed, native_balance=AssetAmount(asset=USDC, amount=0))
    with pytest.raises(ValueError, match="requires retained input evidence"):
        revalidated(observed, simulation=observed.simulation.model_copy(update={"inputs": ()}))
    assert isinstance(observed, Snapshot) and observed.asset.identifier == USDC


def action(**changes):
    config = parameters()
    values = {
        "intent": config.supply("collateral", 10**18),
        "success": True,
        "assets": AssetAmount(asset=WETH, amount=10**18),
        "principal_delta": 0,
        "collateral_delta": 10**18,
        "effective_at": datetime.fromtimestamp(START, UTC),
        "simulation": SimulationEvidence(execution_id="x", model="m", inputs=("a" * 64,)),
    }
    return ActionResult.model_validate(values | changes)


def test_action_results_keep_their_asset_failure_and_evidence_visible():
    assert action().status == "completed"
    assert action(success=False, assets=None, error="refused").status == "failed"
    with pytest.raises(ValueError, match="result has another asset"):
        action(assets=AssetAmount(asset=USDC, amount=1))
    with pytest.raises(ValueError, match="failure and missing-input evidence must stay visible"):
        action(error="refused")
    with pytest.raises(ValueError, match="failure and missing-input evidence must stay visible"):
        action(success=False, assets=None)
    with pytest.raises(ValueError, match="require retained input evidence"):
        action(simulation=SimulationEvidence(execution_id="x", model="m", inputs=()))


# Discovery


def discovery(**changes):
    block = economic_market().observed.block
    values = {
        "source": MARKET,
        "block": block,
        "implementation": "0x" + "b1" * 20,
        "extension": "0x" + "b2" * 20,
        "asset_list": "0x" + "b3" * 20,
        "collateral": (WETH,),
        "price_feeds": (WETH_FEED,),
    }
    return Discovery.model_validate(values | changes)


@pytest.mark.parametrize(
    ("changes", "text"),
    [
        ({"implementation": MARKET.comet}, "roles are absent, overlapping"),
        ({"extension": "0x" + "00" * 20}, "roles are absent, overlapping"),
        ({"price_feeds": ()}, "must remain correlated"),
        ({"collateral": (WETH,) * 25, "price_feeds": (WETH_FEED,) * 25}, "must remain correlated"),
        ({"collateral": (WETH, WETH), "price_feeds": (WETH_FEED, BASE_FEED)}, "aliases another"),
        ({"collateral": (USDC,)}, "aliases another asset"),
        (
            {"collateral": (AssetType("eip155:8453/erc20:0x" + "c1" * 20),)},
            "chain-scoped ERC-20 assets and feeds",
        ),
        ({"price_feeds": ("0x" + "00" * 20,)}, "chain-scoped ERC-20 assets and feeds"),
    ],
)
def test_discovery_refuses_unscoped_roles_and_collateral(changes, text):
    with pytest.raises(ValueError, match=text):
        discovery(**changes)


def test_discovery_admits_distinct_chain_scoped_roles():
    discovered = discovery()
    assert discovered.collateral == (WETH,)
    other_chain = discovered.block.model_copy(
        update={"chain": discovered.block.chain.model_copy(update={"environment": "fork"})}
    )
    with pytest.raises(ValueError, match="on another chain"):
        discovery(block=other_chain)


def test_historical_markets_must_carry_the_configured_collateral_feed_and_precision():
    config = parameters()
    observed = economic_market().observed
    validate_market(config, observed)
    moved = observed.collaterals[0].model_copy(update={"price_feed": BASE_FEED})
    with pytest.raises(ValueError, match="differs from its configured asset/feed"):
        validate_market(config, observed.model_copy(update={"collaterals": (moved,)}))
    with pytest.raises(ValueError, match="differs from its configured asset/feed"):
        validate_market(config, observed.model_copy(update={"collaterals": ()}))
    precise = config.model_copy(update={"asset_metadata": {WETH: AssetMetadata(decimals=8)}})
    with pytest.raises(DataError, match="configured decimals 8 differ from observed 18"):
        validate_market(precise, observed)


# Absorption events


def test_no_event_is_scheduled_without_debt():
    run = world(COMPOUND)
    assert run.model.next_event(run.state, DEADLINE) is None
    assert run.model.external_event(run.state, START) is None


def test_an_underwater_fund_is_absorbed_at_the_first_eligible_second():
    run, state = borrowing(UNDERWATER)
    assert run.model.next_event(state, DEADLINE) == START
    transition = run.model.external_event(state, UnixTime(START))
    event = transition.result
    assert event.kind == "liquidation" and len(event.absorptions) == 1
    assert event.unavailable == () and event.simulation.inputs
    (collateral, loan) = (c.after.payload for c in transition.effects.positions)
    assert collateral.seized_collateral == 10**18 and loan.principal >= 0


def test_a_healthy_debt_waits_for_the_next_prepared_boundary():
    run, state = borrowing(HEALTHY)
    assert run.model.next_event(state, DEADLINE) == START + 12
    # No boundary falls within this horizon, so nothing is scheduled.
    assert run.model.next_event(state, START + 5) is None
    # The event cursor has already passed the horizon.
    after = state.model_copy(update={"event_cursor": UnixTime(START + 5)})
    assert run.model.next_event(after, START + 5) is None
    assert run.model.external_event(state, UnixTime(START)) is None


def test_missing_history_is_an_event_naming_the_affected_fund():
    run, state = borrowing(HEALTHY)
    late = at(state, DEADLINE + 1)
    assert run.model.next_event(late, DEADLINE + 5) == DEADLINE + 1
    event = run.model.external_event(late, UnixTime(DEADLINE + 1)).result
    assert event.kind == "input_unavailable" and event.absorptions == ()
    (affected,) = event.affected_funds
    assert affected.fund == run.target("loan").fund
    assert event.unavailable == (affected.gap,)


def test_absorptions_beyond_the_event_limit_become_unavailable_input():
    run, state = borrowing(UNDERWATER)
    data = run.settings.data.model_copy(update={"max_liquidations_per_event": 1})
    run.model.settings = run.settings.model_copy(update={"data": data})
    # The lender also owes base with no collateral, so both funds are absorbable.
    both = run.holding(
        collateral=COLLATERAL, loan={"principal": UNDERWATER}, earn={"principal": -1}
    )
    event = run.model.external_event(both, UnixTime(START)).result
    assert event.kind == "liquidation" and len(event.absorptions) == 1
    (gap,) = event.unavailable
    assert gap.reason == "Comet absorption exceeds the explicit event limit"


def test_a_paused_absorb_is_reported_as_unavailable_history():
    case = CASES["compound_v3"]
    paused = economic_market(pause_flags=1 << 3).sample
    samples = (case.history(1)[0].model_copy(update={"market": paused.market}),)
    samples = (*samples, *case.history(11)[1:])
    run = world(COMPOUND, samples=samples)
    state = run.holding(collateral=COLLATERAL, loan={"principal": UNDERWATER})
    assert run.model.next_event(state, DEADLINE) == START
    event = run.model.external_event(state, UnixTime(START)).result
    assert event.kind == "input_unavailable"
    assert "absorb is paused" in event.unavailable[0].reason
