"""QA distinguishes Comet's base lending and collateral asset movements."""

import pytest

from almanak.connectors.identifiers import ConnectorId
from tests.compound_v3_helpers import candidate_compound_support as candidate_compound_support
from tests.compound_v3_helpers import recorded_compound as recorded_compound
from tests.qa.catalog import Case
from tests.qa.worker import assert_measurements, fork_actions

COMPOUND = ConnectorId("compound_v3")
ACTIONS = {"supply", "borrow", "repay", "repay_amount", "withdraw", "withdraw_amount"}


def test_qa_checks_base_and_collateral_lifecycles_from_actual_outcomes(recorded_compound):
    state = recorded_compound
    outcomes = [outcome.model_dump(mode="json") for outcome in state.outcomes]
    allocations = {a.name for a in state.config.allocations}
    assert ACTIONS <= fork_actions(outcomes, allocations, connector=COMPOUND)
    for allocation in state.config.allocations:
        target = state.config.target(allocation.name)
        observed = fork_actions(outcomes, {allocation.name}, connector=COMPOUND)
        if target.reserve.is_base and allocation.borrow:
            assert {"borrow", "repay", "repay_amount"} <= observed
        else:
            assert {"supply", "withdraw", "withdraw_amount"} <= observed
            assert not ({"borrow", "repay", "repay_amount"} & observed)
    for item in outcomes:
        if item["operation"]["kind"] == "withdraw" and not item["operation"]["full"]:
            item["success"] = False
    assert "withdraw_amount" not in fork_actions(outcomes, allocations, connector=COMPOUND)


def test_qa_rejects_missing_comet_economic_positions():
    case = Case(id="compound-model", lane="backtest", connector=COMPOUND, model_mode="economic")
    report = {
        "positions": [
            {
                "measurement_status": "modeled",
                "observation": {"retained": True},
                "missing_input": None,
            }
        ],
        "missing_inputs": [],
    }
    assert_measurements(case, report)
    report["positions"][0]["observation"] = None
    with pytest.raises(AssertionError):
        assert_measurements(case, report)
