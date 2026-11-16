"""Trusted reference component checks, separate from sandbox and fork proofs."""

import importlib.util
from pathlib import Path

import pytest

from almanak.connectors.sparklend.state import DecisionInput, OutcomeInput
from tests.reference_parameters import authored_parameters
from tests.sparklend_helpers import candidate_spark_support as candidate_spark_support
from tests.sparklend_helpers import recorded_spark as recorded_spark
from tests.test_sparklend_execution import replay_states


@pytest.fixture
def reference_module():
    source = Path(__file__).parents[1] / "strategies/sparklend_ethereum_lending/strategy.py"
    spec = importlib.util.spec_from_file_location("sparklend_reference", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def current(config):
    """The captured config with the reference's knobs.

    The capture predates `borrow_quantity` moving from a strategy constant into
    `strategy.parameters`; the authored value (5) is the constant it ran with.
    """
    parameters = authored_parameters("sparklend_ethereum_lending")
    return config.model_copy(update={"parameters": parameters})


def test_current_reference_replays_captured_decisions_and_intermediate_outcomes(
    recorded_spark, reference_module
):
    module = reference_module
    module.validate_config(current(recorded_spark.config))
    assert module.capabilities(current(recorded_spark.config)) == recorded_spark.capabilities
    memory = module.State()
    for state, outcome in replay_states(recorded_spark):
        context = DecisionInput(
            config=current(state.config),
            memory=memory,
            owned=state.owned,
            observations=outcome.decision.observations,
            trigger=outcome.decision.trigger,
        )
        decision = module.decide(context)
        assert decision.intent == outcome.decision.intent
        assert decision.required_observations
        memory = module.on_outcome(
            OutcomeInput(
                config=current(state.config),
                memory=decision.memory,
                result=outcome.strategy_result(),
            )
        )
        if outcome.operation.kind == "borrow":
            holding = context.model_copy(update={"memory": memory})
            assert module.decide(holding).intent is None
            assert module.decide(holding).memory == memory
    assert (
        memory.model_dump(exclude={"version", "values", "schema_id"})
        == recorded_spark.memory.values
    )


def test_interrupted_entry_and_missing_observations_do_not_create_more_exposure(
    recorded_spark, reference_module
):
    outcome = next(o for o in recorded_spark.outcomes if o.decision.trigger.kind == "teardown")
    context = DecisionInput(
        config=recorded_spark.config,
        memory=reference_module.State(phase=1),
        owned=recorded_spark.owned,
        observations=outcome.decision.observations,
        trigger=outcome.decision.trigger,
    )
    decision = reference_module.decide(context)
    assert decision.intent.kind == "repay"
    assert decision.memory.exiting
    missing = context.model_copy(update={"observations": ()})
    assert reference_module.decide(missing).intent is None
