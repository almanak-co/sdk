"""A scripted perpetual lifecycle runs through the driver, the simulator and the report.

The inputs are synthetic (``tests/gmx_perpetual_support.py``) and the decisions
are scripted from durable memory instead of the sandboxed author hook, which
this environment cannot start. The checks prove driver wiring, durable resume
equality and accounting-only liquidation; they establish no historical coverage.
"""

import pytest

from almanak.connectors import dex_economic_model
from almanak.connectors.gmx_v2.economic_component import protocol_closed
from almanak.connectors.gmx_v2.economic_data import PERPETUAL_MODEL
from almanak.connectors.gmx_v2.economic_perpetual_state import (
    EconomicLiquidationBatch,
    EconomicPerpetualResult,
)
from almanak.connectors.gmx_v2.economic_reporting import report
from almanak.connectors.gmx_v2.economic_state import State
from almanak.strategy_api import Memory
from tests import gmx_perpetual_support as support
from tests.gmx_candidates import candidate_perpetual_model as candidate_perpetual_model
from tests.gmx_perpetual_support import COLLATERAL, SIZE, START

LIFECYCLE = (
    support.increase(COLLATERAL, SIZE),
    support.increase(COLLATERAL // 2, SIZE // 2),
    support.decrease(COLLATERAL // 10, SIZE // 2),
    support.close(),
    # A 0.001 collateral increase cannot support its notional: cancelled with refund.
    support.increase(10**15, SIZE),
)
HOLDING = (support.increase(COLLATERAL, SIZE), *([support.hold()] * 4))


@pytest.fixture(autouse=True)
def no_author_hook(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("the scripted perpetual lifecycle must not invoke an author hook")

    monkeypatch.setattr(dex_economic_model, "strategy_worker", forbidden)


@pytest.fixture(params=["arbitrum", "avalanche"])
def chain(request):
    return request.param


def setup(chain, candidate_perpetual_model, plan, **changes):
    config = support.parameters(chain)
    settings = support.settings(config)
    target = config.target(config.allocations[0].name)
    book = support.book(settings, target, **changes)
    model = support.driver(candidate_perpetual_model, config, settings, book, plan)
    return config, settings, model, support.seed(config, settings)


def phases(state):
    return [
        (value.intent.kind, value.phase)
        for completed in state.completed
        if isinstance(value := completed.transition.result, EconomicPerpetualResult)
        and value.phase != "waiting"
    ]


def test_scripted_lifecycle_opens_increases_decreases_closes_and_cleans_up(
    tmp_path, chain, candidate_perpetual_model
):
    config, settings, model, seed = setup(chain, candidate_perpetual_model, LIFECYCLE)
    assert model.model_name == PERPETUAL_MODEL
    state = support.run(tmp_path / "run.sqlite", model, seed)
    assert state.mode == "finished" and state.pending is None and state.events == ()
    assert phases(state) == [
        ("increase_position", "accepted"),
        ("increase_position", "settled"),
        ("increase_position", "accepted"),
        ("increase_position", "settled"),
        ("decrease_position", "accepted"),
        ("decrease_position", "settled"),
        ("close_position", "accepted"),
        ("close_position", "settled"),
        ("increase_position", "accepted"),
        ("increase_position", "cancelled"),
        ("revoke_position", "claimed"),
    ]
    results = [
        value
        for completed in state.completed
        if isinstance(value := completed.transition.result, EconomicPerpetualResult)
    ]
    assert all(value.success for value in results if value.phase != "cancelled")
    cancelled = next(value for value in results if value.phase == "cancelled")
    assert not cancelled.success and "leverage" in cancelled.reason
    assert cancelled.keeper_paid.amount == 10**15
    settled = [value for value in results if value.phase == "settled"]
    first, second, partial, closed = settled
    assert first.after.owned.position.size_usd_e30 == SIZE
    assert second.after.owned.position.size_usd_e30 == SIZE + SIZE // 2
    assert partial.after.owned.position.size_usd_e30 == SIZE
    assert closed.after.owned.position is None
    # Funding and borrowing accrue between settlements: accumulators advance each time.
    accumulators = [
        (
            value.after.owned.position.borrowing_factor,
            value.after.owned.position.funding_fee_amount_per_size,
        )
        for value in (first, second, partial)
    ]
    assert accumulators == sorted(accumulators) and len(set(accumulators)) == 3
    assert any(value.amount for value in partial.after.owned.claimable_funding)
    payload = state.portfolio.positions[0].payload
    assert protocol_closed(payload) and payload.owned.position is None
    fee = config.allocations[0].execution_fee.amount
    assert payload.spent[0].amount == COLLATERAL + COLLATERAL // 2 + 10**15 + 5 * fee
    assert payload.spent_notional_usd_e30 == SIZE + SIZE // 2 + SIZE
    assert not any(value.amount for value in payload.allowances)
    assert state.delivered == len(state.completed)
    assert State.model_validate_json(state.model_dump_json()) == state
    assert state.memory == Memory(values={"stage": len(LIFECYCLE)})


def test_report_includes_perpetual_gas_in_payer_totals_and_keeps_missing_measurements_visible(
    tmp_path, chain, candidate_perpetual_model
):
    config, settings, model, seed = setup(chain, candidate_perpetual_model, LIFECYCLE)
    state = support.run(tmp_path / "run.sqlite", model, seed)
    result = report(state, model)
    assert result["model"] == PERPETUAL_MODEL and result["closed"]
    steps = result["request_steps"]
    assert [step["phase"] for step in steps if step["phase"] != "waiting"] == [
        phase for _, phase in phases(state)
    ]
    gas = sum(int(step["gas_paid"]["amount"]) for step in steps)
    # Five acceptances and one claim at their own presets. Request-sized approvals leave no
    # residual allowance, so cleanup needs no revocation; the keeper-side cancellation is
    # paid from the escrowed execution fee, not by the submitter.
    expected = 5 * 3 * 10**14 + 5 * 10**13
    assert gas == expected
    payer = result["gas_payers"][0]
    assert payer["cost"]["amount"] == str(expected)
    assert payer["measurement_status"] == "assumed"
    assert result["liquidations"] == []
    assert any("liquidations are evaluated" in text for text in result["assumptions"])
    assert any("refuses to claim" in text for text in result["assumptions"])
    assert "claim" in result["cost_interpretation"]
    assert result["positions"][0]["measurement_status"] == "modeled"
    assert result["positions"][0]["remaining_obligations"] is False
    excluded = support.settings(config, costs="excluded")
    model = support.driver(candidate_perpetual_model, config, excluded, model.book, LIFECYCLE)
    state = support.run(tmp_path / "excluded.sqlite", model, support.seed(config, excluded))
    payer = report(state, model)["gas_payers"][0]
    assert payer["cost"]["amount"] is None and payer["measurement_status"] == "excluded"


@pytest.mark.parametrize("insolvent", [False, True])
def test_liquidation_batch_is_recorded_as_an_accounting_event_without_an_author_hook(
    tmp_path, chain, candidate_perpetual_model, insolvent
):
    config, settings, model, seed = setup(
        chain,
        candidate_perpetual_model,
        HOLDING,
        crash_at=START + 9,
        crash=support.liquidation_prices("long", insolvent=insolvent),
    )
    state = support.run(tmp_path / "run.sqlite", model, seed)
    assert state.mode == "finished"
    assert len(state.events) == 1 and state.events_delivered == 1
    event = state.events[0]
    assert event.time == START + 9 and isinstance(event.transition.result, EconomicLiquidationBatch)
    outcome = event.transition.result.results[0]
    assert outcome.before.owned.position is not None and outcome.after.owned.position is None
    assert outcome.reason == ("min collateral" if insolvent else "min collateral for leverage")
    assert outcome.simulation.model == PERPETUAL_MODEL and outcome.simulation.inputs
    if insolvent:
        assert outcome.unpaid_usd_e30 > 0 and not outcome.fees_collected and outcome.assets == ()
    else:
        assert outcome.unpaid_usd_e30 == 0 and outcome.fees_collected
        assert all(value.amount > 0 for value in outcome.assets)
    assert phases(state) == [
        ("increase_position", "accepted"),
        ("increase_position", "settled"),
        ("revoke_position", "claimed"),
    ]
    assert state.memory == Memory(values={"stage": len(HOLDING)})
    payload = state.portfolio.positions[0].payload
    assert protocol_closed(payload)
    result = report(state, model)
    assert len(result["liquidations"]) == 1
    liquidation = result["liquidations"][0]
    assert liquidation["reason"] == outcome.reason
    assert liquidation["unpaid_cost_usd_e30"] == str(outcome.unpaid_usd_e30)
    assert liquidation["keeper_cost"] == {"measurement_status": "unmeasured"}
    assert liquidation["fees_collected"] is outcome.fees_collected


@pytest.mark.parametrize(
    "plan,changes,stage,occurrence",
    [
        (LIFECYCLE, {}, "effects_commit", 1),
        (LIFECYCLE, {}, "effects_commit", 7),
        (LIFECYCLE, {}, "outcome", 9),
        (LIFECYCLE, {}, "decision_commit", 2),
        (HOLDING, {"crash_at": START + 9}, "event_effects", 1),
        (HOLDING, {"crash_at": START + 9}, "event_commit", 1),
        (HOLDING, {"crash_at": START + 9}, "event_delivery", 1),
    ],
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_interrupted_runs_resume_to_the_identical_final_state(
    tmp_path, chain, candidate_perpetual_model, plan, changes, stage, occurrence
):
    config, settings, model, seed = setup(chain, candidate_perpetual_model, plan, **changes)
    expected = support.run(tmp_path / "baseline.sqlite", model, seed)
    seen = {"count": 0}

    def interrupt(actual):
        if actual == stage:
            seen["count"] += 1
            if seen["count"] == occurrence:
                raise RuntimeError("simulated process interruption")

    with pytest.raises(RuntimeError, match="simulated process interruption"):
        support.run(tmp_path / "interrupted.sqlite", model, seed, fault=interrupt)
    replacement = support.driver(candidate_perpetual_model, config, settings, model.book, plan)
    resumed = support.run(tmp_path / "interrupted.sqlite", replacement, seed)
    assert resumed == expected
    assert resumed.mode == "finished"
