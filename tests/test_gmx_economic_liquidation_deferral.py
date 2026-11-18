"""Liquidations the protocol cannot execute stay visible and never abort the sweep.

Inputs are synthetic (``tests/gmx_perpetual_support.py``); these checks prove the
sweep's failure semantics and durable behavior, not historical coverage.
"""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import InputUnavailable
from almanak.connectors import dex_economic_model
from almanak.connectors.gmx_v2.economic_component import protocol_closed
from almanak.connectors.gmx_v2.economic_perpetual_requests import (
    liquidate,
    liquidation_opportunity,
    next_liquidation,
)
from almanak.connectors.gmx_v2.economic_perpetual_state import (
    EconomicLiquidationBatch,
    EconomicLiquidationDeferral,
)
from almanak.connectors.gmx_v2.economic_reporting import report
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.strategy_api import SimulationEvidence
from tests import gmx_perpetual_support as support
from tests.gmx_candidates import candidate_perpetual_model as candidate_perpetual_model
from tests.gmx_perpetual_support import START
from tests.test_gmx_economic_perpetual_requests import case as case
from tests.test_gmx_economic_perpetual_requests import chain as chain
from tests.test_gmx_economic_perpetual_requests import opened, risk_at
from tests.test_gmx_economic_perpetual_requests import side as side

HOLDING = (support.increase(), *([support.hold()] * 4))


@pytest.fixture(autouse=True)
def no_author_hook(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("the scripted perpetual lifecycle must not invoke an author hook")

    monkeypatch.setattr(dex_economic_model, "strategy_worker", forbidden)


def test_liquidation_that_the_pool_cannot_pay_is_refused_rather_than_hidden(case):
    held = opened(case)
    move = support.liquidation_prices(case.side)
    record = support.record_at(case.target, START + 9, long_price=move, thin=True)
    found = liquidation_opportunity(held, record, START + 9)
    assert found is not None and found[1] == "min collateral for leverage"
    with pytest.raises(SwapConstraint, match="insufficient physical market token balance"):
        liquidate(held, found[0])
    healthy = support.record_at(case.target, START + 9, long_price=move)
    after, _, _ = liquidate(held, liquidation_opportunity(held, healthy, START + 9)[0])
    assert after.owned.position is None


@pytest.mark.parametrize("side", ["short"], indirect=True)
def test_liquidation_needing_a_secondary_funding_payment_requires_the_holding_address(case):
    held = opened(case)
    risk = risk_at(case, START + 9).model_copy(update={"holding_address_available": False})
    record = support.record_at(
        case.target, START + 9, long_price=support.PROFIT_FOR_SHORT, risk=risk
    )
    opportunity = record.payload.opportunities[0]
    indexes = opportunity.funding_indexes
    surged = indexes.model_copy(
        update={
            "short_payments": indexes.short_payments.model_copy(
                update={"long": indexes.short_payments.long + 10**33}
            )
        }
    )
    opportunity = opportunity.model_copy(update={"funding_indexes": surged})
    record.payload = SimpleNamespace(risk=(risk,), opportunities=(opportunity,))
    found = liquidation_opportunity(held, record, START + 9)
    assert found is not None and found[1] == "min collateral"
    with pytest.raises(SwapConstraint, match="holding address"):
        liquidate(held, found[0])
    available = opportunity.model_copy(
        update={"risk": risk.model_copy(update={"holding_address_available": True})}
    )
    after, movements, settlement = liquidate(held, available)
    assert after.owned.position is None and movements == ()
    assert settlement.unpaid_usd_e30 > 0 and not settlement.fees_collected


def test_next_liquidation_starts_at_the_first_row_available_from_start(case):
    held = opened(case)
    move = support.liquidation_prices(case.side)
    frames = [
        support.frame(case.target, time, long_price=move if time >= START + 9 else None)
        for time in range(START, START + 12)
    ]
    timeline = support.SyntheticBook(case.settings, {case.target: frames}).timelines[case.target]
    assert next_liquidation(timeline, held, START + 9, START + 9) == START + 9
    assert next_liquidation(timeline, held, START + 12, START + 20) is None
    assert next_liquidation(timeline, held, START - 5, START + 8) is None


def test_liquidation_batch_accepts_deferrals_and_refuses_empty_or_repeated_entries(case):
    evidence = SimulationEvidence(execution_id="event", model="candidate", inputs=())
    at = datetime.fromtimestamp(START + 9, UTC)
    deferral = EconomicLiquidationDeferral(
        allocation="Main", effective_at=at, simulation=evidence, eligibility=None, reason="gap"
    )
    batch = EconomicLiquidationBatch(deferrals=(deferral,))
    assert EconomicLiquidationBatch.model_validate_json(batch.model_dump_json()) == batch
    assert batch.effective_at == at and batch.results == ()
    with pytest.raises(ValueError, match="at least one outcome"):
        EconomicLiquidationBatch()
    with pytest.raises(ValueError, match="repeat a position or allocation"):
        EconomicLiquidationBatch(deferrals=(deferral, deferral))


def setup(chain, candidate_perpetual_model, plan, **changes):
    config = support.parameters(chain)
    settings = support.settings(config)
    target = config.target(config.allocations[0].name)
    book = support.book(settings, target, **changes)
    model = support.driver(candidate_perpetual_model, config, settings, book, plan)
    return config, settings, model, support.seed(config, settings)


def test_unexecutable_liquidations_are_deferred_visibly_and_retried(
    tmp_path, chain, candidate_perpetual_model
):
    config, settings, model, seed = setup(
        chain, candidate_perpetual_model, HOLDING, crash_at=START + 9, thin=(START + 9, START + 10)
    )
    state = support.run(tmp_path / "run.sqlite", model, seed)
    assert state.mode == "finished"
    batches = [event.transition.result for event in state.events]
    assert [event.time for event in state.events] == [START + 9, START + 10, START + 11]
    assert [(len(batch.results), len(batch.deferrals)) for batch in batches] == [
        (0, 1),
        (0, 1),
        (1, 0),
    ]
    deferral = batches[0].deferrals[0]
    assert deferral.eligibility == "min collateral for leverage"
    assert "insufficient physical market token balance" in deferral.reason
    assert deferral.unavailable is None and deferral.simulation.inputs
    assert state.events[0].transition.effects.positions == ()
    assert batches[2].results[0].after.owned.position is None
    assert protocol_closed(state.portfolio.positions[0].payload)
    outcomes = [entry["outcome"] for entry in report(state, model)["liquidations"]]
    assert outcomes == ["deferred", "deferred", "liquidated"]
    replacement = support.driver(candidate_perpetual_model, config, settings, model.book, HOLDING)
    assert support.run(tmp_path / "again.sqlite", replacement, seed) == state


def test_two_targets_record_unavailable_input_without_aborting_the_sweep(
    tmp_path, chain, candidate_perpetual_model
):
    config = support.parameters(chain, allocations=2)
    settings = support.settings(config, end=START + 120, cleanup=60)
    main, second = (config.target(value.name) for value in config.allocations)
    book = support.SyntheticBook(
        settings,
        {
            main: support.frames_for(main, settings, crash_at=START + 85),
            # A coverage gap longer than the carried-input age hides the second target.
            second: support.frames_for(second, settings, skip=range(START + 21, START + 90)),
        },
    )
    with pytest.raises(InputUnavailable):
        book.at(second, START + 85)
    plan = (
        support.increase(name="Main"),
        support.increase(name="Second"),
        *([support.hold()] * 20),
    )
    model = support.driver(candidate_perpetual_model, config, settings, book, plan)
    state = support.run(tmp_path / "run.sqlite", model, support.seed(config, settings))
    assert state.mode == "finished"
    assert [event.time for event in state.events] == [START + 85]
    batch = state.events[0].transition.result
    assert [result.allocation for result in batch.results] == ["Main"]
    assert [value.allocation for value in batch.deferrals] == ["Second"]
    deferral = batch.deferrals[0]
    assert deferral.eligibility is None and deferral.unavailable is not None
    assert deferral.unavailable.time == START + 85
    assert all(protocol_closed(held.payload) for held in state.portfolio.positions)
    entries = report(state, model)["liquidations"]
    assert [(entry["outcome"], entry["allocation"]) for entry in entries] == [
        ("liquidated", "Main"),
        ("deferred", "Second"),
    ]
    assert entries[1]["unavailable"]["time"] == START + 85
