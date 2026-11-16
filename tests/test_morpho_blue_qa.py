"""QA reads Morpho's serialized share repayment mode and collateral withdrawals."""

import pytest

from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.operations import RepayOperation, WithdrawOperation
from tests.morpho_blue_helpers import candidate_morpho_support as candidate_morpho_support
from tests.morpho_blue_helpers import recorded_morpho as recorded_morpho
from tests.qa.catalog import Case
from tests.qa.worker import assert_measurements, fork_actions, fork_operation_action

MORPHO = ConnectorId("morpho_blue")
ACTIONS = {"supply", "borrow", "repay", "repay_amount", "withdraw", "withdraw_amount"}


def test_qa_classifies_typed_captured_full_and_partial_repayment(recorded_morpho):
    variants = set()
    for outcome in recorded_morpho.outcomes:
        operation = outcome.operation
        if not isinstance(operation, RepayOperation):
            continue
        wire = operation.model_dump(mode="json")
        assert "full" not in wire
        assert type(wire["shares"]) is int
        assert operation.full == (wire["shares"] > 0)
        expected = "repay" if operation.full else "repay_amount"
        assert fork_operation_action(MORPHO, wire) == expected
        variants.add(expected)
    assert variants == {"repay", "repay_amount"}


def test_qa_covers_loan_and_collateral_from_successful_native_outcomes(recorded_morpho):
    state = recorded_morpho
    outcomes = [outcome.model_dump(mode="json") for outcome in state.outcomes]
    allocations = {allocation.name for allocation in state.config.allocations}
    assert ACTIONS <= fork_actions(outcomes, allocations, connector=MORPHO)
    for allocation in state.config.allocations:
        target = state.config.target(allocation.name)
        observed = fork_actions(outcomes, {allocation.name}, connector=MORPHO)
        if target.reserve.is_loan and allocation.borrow:
            assert {"borrow", "repay", "repay_amount"} <= observed
        else:
            assert {"supply", "withdraw", "withdraw_amount"} <= observed
            assert not ({"borrow", "repay", "repay_amount"} & observed)
    for outcome in state.outcomes:
        operation = outcome.operation
        if isinstance(operation, WithdrawOperation):
            wire = operation.model_dump(mode="json")
            assert type(wire["full"]) is bool
            if not operation.target.reserve.is_loan:
                assert wire["shares"] == 0
    for item in outcomes:
        operation = item["operation"]
        if operation["kind"] == "repay" and not operation["shares"]:
            item["success"] = False
    assert "repay_amount" not in fork_actions(outcomes, allocations, connector=MORPHO)


def test_qa_rejects_missing_morpho_economic_observation():
    case = Case(id="morpho-model", lane="backtest", connector=MORPHO, model_mode="economic")
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
