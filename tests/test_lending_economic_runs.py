"""Compound V3 and Morpho Blue economic runs share their state, entry and report rules.

Both connectors start from explicit inventory, keep ownership in configured order, admit
new exposure only while running without a failed effect in the same fund/market, and
report gas payers and unavailable history. These cases run each connector's real model
over literal observations.
"""

from types import SimpleNamespace

import pytest

from almanak.backtest.types import UnixTime
from almanak.effects import Portfolio
from tests.lending_economic_world import CONNECTORS, DEADLINE, NATIVE, at, world

ALL = sorted(CONNECTORS)


@pytest.fixture(params=ALL)
def run(request):
    return world(CONNECTORS[request.param])


def message(run, text):
    return f"{run.connector.label} {text}"


def step(intent, success=True):
    return SimpleNamespace(
        pending=SimpleNamespace(intent=intent),
        transition=SimpleNamespace(result=SimpleNamespace(success=success)),
    )


def test_initial_state_holds_empty_configured_positions_and_explicit_inventory(run):
    targets = [p.payload.target for p in run.state.portfolio.positions]
    assert targets == [run.target(n) for n in ("collateral", "loan", "earn")]
    assert all(p.payload.empty for p in run.state.portfolio.positions)
    assert run.state.time == run.settings.schedule.start
    # Every allocation's wallet exists, even with no starting balance.
    loan = run.connector.parameters.wallet(run.target("loan"))
    assert run.state.portfolio.balance(loan) == 0


@pytest.mark.parametrize("name", ALL)
def test_initial_state_requires_inventory_for_a_self_paying_fund(name):
    with pytest.raises(ValueError, match="observations require explicit payer inventory"):
        world(CONNECTORS[name], lender_native=None)


def test_initial_state_refuses_capital_beyond_the_fund_inventory(run):
    larger = tuple(
        a.model_copy(update={"capital": 2000 * 10**6}) if a.name == "earn" else a
        for a in run.config.allocations
    )
    config = run.config.model_copy(update={"allocations": larger})
    with pytest.raises(ValueError, match="inventory cannot fund the allocation capital"):
        run.connector.parameters.initial(run.settings, config, run.interval)


def test_state_must_keep_its_frozen_schedule_and_configured_ownership(run):
    run.model.validate_state(run.state)
    moved = run.state.model_copy(
        update={"schedule": run.state.schedule.model_copy(update={"end": UnixTime(DEADLINE)})}
    )
    with pytest.raises(ValueError, match=message(run, "state differs from its frozen schedule")):
        run.model.validate_state(moved)
    positions = run.state.portfolio.positions
    reordered = run.state.model_copy(
        update={
            "portfolio": Portfolio[run.connector.state.Position](
                balances=run.state.portfolio.balances, positions=positions[::-1]
            )
        }
    )
    with pytest.raises(ValueError, match=message(run, "ownership differs")):
        run.model.validate_state(reordered)
    other = positions[0].key.model_copy(update={"reference": "another-market/asset"})
    rekeyed = (positions[0].model_copy(update={"key": other}), *positions[1:])
    mislabeled = run.state.model_copy(
        update={
            "portfolio": Portfolio[run.connector.state.Position](
                balances=run.state.portfolio.balances, positions=rekeyed
            )
        }
    )
    with pytest.raises(ValueError, match=message(run, "position key differs")):
        run.model.validate_state(mislabeled)


def test_availability_names_every_allocation_without_an_observation(run):
    observed = run.model.observe(run.state)
    assert [a.status for a in run.model.availability(run.state, observed)] == ["ready"]
    (chain,) = run.model.availability(run.state, observed[1:2])
    assert chain.status == "unavailable"
    assert chain.fault.kind == "prepared_input_unavailable"
    assert (
        chain.fault.message == f"Missing prepared {run.connector.label} history: collateral, earn"
    )


def test_observations_past_prepared_coverage_are_omitted(run):
    assert run.model.observe(at(run.state, DEADLINE + 1)) == ()


def test_new_exposure_requires_running_mode_and_no_failed_prior_effect(run):
    supply = run.config.supply("collateral", 10**18)
    run.model.validate_entry(run.state, supply)
    # Exits are always admitted, even after a failure in teardown.
    failed = run.state.model_copy(
        update={"mode": "teardown", "completed": (step(supply, success=False),)}
    )
    run.model.validate_entry(failed, run.config.withdraw("collateral"))
    for state in (
        run.state.model_copy(update={"mode": "teardown"}),
        run.state.model_copy(update={"completed": (step(supply, success=False),)}),
    ):
        with pytest.raises(ValueError, match="requires running mode and successful prior"):
            run.model.validate_entry(state, supply)
    # A failure for another fund/market does not block this fund's entry.
    other = run.state.model_copy(
        update={"completed": (step(run.config.supply("earn", 1), success=False),)}
    )
    run.model.validate_entry(other, supply)


def test_new_exposure_requires_the_payer_native_minimum(run):
    raised = tuple(
        a.model_copy(update={"minimum_native_balance": 10**18 + 1}) for a in run.config.allocations
    )
    run.model.config = run.config.model_copy(update={"allocations": raised})
    with pytest.raises(ValueError, match="payer is below the configured native minimum"):
        run.model.validate_entry(run.state, run.config.supply("collateral", 10**18))


def test_position_reports_model_observed_positions_and_name_missing_history(run):
    report = run.connector.reporting.position_report
    position = run.state.portfolio.positions[1].payload
    modeled = report(run.state, run.model, position)
    assert modeled["measurement_status"] == "modeled"
    assert modeled["missing_input"] is None
    assert modeled["observation"]["target"] == position.target.model_dump(mode="json")
    assert modeled["effects"]["supplied"]["amount"] == "0"
    missing = report(at(run.state, DEADLINE + 1), run.model, position)
    assert missing["measurement_status"] == "unavailable"
    assert missing["observation"] is None
    assert missing["missing_input"]["reason"] == "requested time is outside prepared coverage"


def test_payer_reports_charge_successful_actions_to_their_gas_payer(run):
    connector = run.connector
    costs = {"kind": "fixed_native", "supply": 3, "withdraw": 5, "borrow": 7, "repay": 11}
    fixed = world(connector, costs=costs)
    completed = (
        step(fixed.config.supply("collateral", 10**18)),
        step(fixed.config.borrow("loan", 10**6)),
        step(fixed.config.borrow("loan", 10**6), success=False),
        step(fixed.config.supply("earn", 10**6)),
    )
    state = fixed.state.model_copy(update={"completed": completed})
    reports = {
        r["wallet"]["account"]: r for r in connector.reporting.payer_reports(state, fixed.model)
    }
    assert reports["gas"]["fund_accounts"] == ["borrower"]
    assert reports["gas"]["cost"]["amount"] == str(3 + 7)
    assert reports["lender"]["cost"]["amount"] == "3"
    assert {r["measurement_status"] for r in reports.values()} == {"assumed"}
    assert reports["gas"]["wallet"]["asset"] == str(NATIVE)
    excluded = connector.reporting.payer_reports(
        run.state.model_copy(update={"completed": completed}), run.model
    )
    assert [(r["cost"], r["measurement_status"]) for r in excluded] == [(None, "excluded")] * 2


def test_full_report_counts_positions_wallets_and_payers(run):
    report = run.connector.reporting.report(run.state, run.model)
    assert report["connector"] == run.connector.name
    assert report["closed"] is True
    assert len(report["positions"]) == 3 and len(report["gas_payers"]) == 2
    assert report["missing_inputs"] == []
