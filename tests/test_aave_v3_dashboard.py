"""The current author dashboard renders captured fund and submitter evidence."""

from pathlib import Path

import pytest

from almanak.connectors.aave_v3.dashboard import project
from almanak.dashboard.evaluate import evaluate
from almanak.dashboard.preview import render_html
from almanak.strategy import StrategyDefinition
from tests.aave_v3_helpers import recorded_v3 as recorded_v3


def test_dashboard_keeps_fund_positions_and_gas_payer_separate(recorded_v3):
    state = recorded_v3
    context, snapshot = project(state, captured_at=1788721500)
    assert not hasattr(context.config, "fork_bindings")
    assert not hasattr(context.config, "production")
    assert snapshot.values["loan-debt"].amount == "0"
    assert snapshot.values["collateral-supply"].amount == "0"
    cost = snapshot.values["fund-gas"]
    assert cost.amount == str(sum(o.gas_paid.amount for o in state.outcomes))
    assert cost.scope.fund != cost.scope.payer
    assert cost.scope.target is None
    assert context.datasets["fund_costs"].rows[0]["payer"] == cost.scope.payer.address


def test_current_dashboard_hook_uses_the_shared_sandbox_and_renderer(recorded_v3):
    source = Path(__file__).parents[1] / "strategies/aave_v3_ethereum_lending/strategy.py"
    # This checks the current dashboard hook against captured observations; it
    # does not relabel those observations as a new execution of the strategy.
    state = recorded_v3.model_copy(update={"strategy": StrategyDefinition.read(source)})
    response = evaluate(state)
    assert response.status == "ready"
    rendered = render_html(response)
    assert "Aave V3 lending" in rendered


def test_unobserved_and_invalidated_values_cannot_become_measured_zero(recorded_v3):
    state = recorded_v3.model_copy(
        update={
            "initialized": False,
            "baseline": (),
            "outcomes": (),
            "closure": (),
            "delivered": 0,
        }
    )
    _, snapshot = project(state)
    assert snapshot.values["loan-debt"].amount is None
    assert snapshot.values["collateral-supply"].amount is None
    assert snapshot.values["fund-gas"].amount is None
    with pytest.raises(ValueError, match="invalidated evidence"):
        project(state.model_copy(update={"integrity_error": "canonical receipt invalidated"}))
