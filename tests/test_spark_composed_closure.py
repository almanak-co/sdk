"""A real post-reversal Spark checkpoint must retain debt and collateral cleanup."""

import json
from pathlib import Path

from almanak.connectors.composed.state import Checkpoint
from almanak.connectors.composition import connector_for
from tests.lending_captures import read_capture
from tests.reference_parameters import authored_values
from tests.test_new_reference_replay import module


def without_curve_ceilings(value):
    """Drop the Curve `maximums` spending ceilings that scope-only authority removed."""
    if isinstance(value, list):
        return [without_curve_ceilings(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {k: without_curve_ceilings(v) for k, v in value.items() if k != "maximums"}


def test_retained_post_reversal_spark_debt_requests_repayment():
    path = Path(__file__).with_name("fixtures") / "spark-composed-before-repay.json.gz"
    wire = without_curve_ceilings(json.loads(read_capture(path)))
    recorded = Checkpoint.model_validate_json(json.dumps(wire))
    # The capture predates strategy.parameters; the knobs are the reference's.
    knobs = authored_values("ethereum_spark_curve_borrow_and_trade")
    recorded = recorded.model_copy(
        update={"config": recorded.config.model_copy(update={"parameters": knobs})}
    )
    source = module("ethereum_spark_curve_borrow_and_trade")
    assert recorded.mode == "teardown" and not recorded.memory.values["traded"]
    assert recorded.owned[0].supply_scaled > 0 and recorded.owned[1].debt_scaled > 0
    context = connector_for(recorded.config).codec.decision_input(
        config=recorded.config,
        memory=source.State(phase=3, traded=False, exiting=True),
        owned=recorded.owned,
        observations=recorded.baseline,
        trigger=recorded.outcomes[-1].decision.trigger,
    )
    decision = source.decide(context)
    assert decision.intent.kind == "repay"
    assert decision.intent.target == recorded.config.target("loan")
    loan = recorded.owned[1].model_copy(update={"debt_scaled": 0})
    context = context.model_copy(update={"owned": (recorded.owned[0], loan, recorded.owned[2])})
    decision = source.decide(context)
    assert decision.intent.kind == "withdraw"
    assert decision.intent.target == recorded.config.target("collateral")
    collateral = recorded.owned[0].model_copy(
        update={"supply_scaled": 0, "collateral_enabled": True}
    )
    context = context.model_copy(update={"owned": (collateral, loan, recorded.owned[2])})
    assert source.decide(context).intent.kind == "withdraw"
    collateral = collateral.model_copy(update={"collateral_enabled": False})
    context = context.model_copy(update={"owned": (collateral, loan, recorded.owned[2])})
    assert source.decide(context).intent is None
