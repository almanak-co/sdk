"""Comet dashboards retain fund ownership, separate payer costs and missing values."""

from pathlib import Path

import pytest

from almanak.connectors.compound_v3.dashboard import project
from almanak.dashboard.evaluate import evaluate
from almanak.dashboard.preview import render_html
from almanak.strategy import StrategyDefinition
from tests.compound_v3_helpers import candidate_compound_support as candidate_compound_support
from tests.compound_v3_helpers import recorded_compound as recorded_compound


def test_retained_principal_collateral_and_gas_keep_distinct_units_and_scopes(recorded_compound):
    state = recorded_compound
    context, snapshot = project(state)
    assert not hasattr(context.config, "fork_bindings") and not hasattr(
        context.config, "production"
    )
    assert snapshot.values["loan-debt"].amount == snapshot.values["collateral-supply"].amount == "0"
    assert snapshot.values["loan-ownership"].unit == "shares"
    assert snapshot.values["collateral-ownership"].unit == "base_units"
    assert sum(
        int(snapshot.values[b.name + "-gas"].amount) for b in state.config.accounts.bindings
    ) == sum(o.gas_paid.amount for o in state.outcomes)
    for binding in state.config.accounts.bindings:
        value = snapshot.values[binding.name + "-gas"]
        assert (
            value.scope.fund == binding.profile.fund
            and value.scope.payer == binding.profile.submitter
        )
    assert len(context.datasets["fund_costs"].rows) == 2


def test_current_dashboard_runs_in_shared_sandbox_and_renderer(recorded_compound):
    network = (
        "ethereum"
        if str(recorded_compound.config.positions[0].chain.chain_id) == "eip155:1"
        else "base"
    )
    source = Path(__file__).parents[1] / f"strategies/compound_v3_{network}_lending/strategy.py"
    state = recorded_compound.model_copy(update={"strategy": StrategyDefinition.read(source)})
    response = evaluate(state)
    assert response.status == "ready"
    rendered = render_html(response)
    assert "Compound V3 lending" in rendered and "Fund execution costs" in rendered


def test_unobserved_and_invalidated_financial_evidence_cannot_become_measured_zero(
    recorded_compound,
):
    state = recorded_compound.model_copy(
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
    assert snapshot.values["borrower-gas"].amount is None
    with pytest.raises(ValueError, match="invalidated"):
        project(state.model_copy(update={"integrity_error": "receipt invalidated"}))
