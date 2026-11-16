"""Trusted reference component checks, separate from sandbox and fork proofs."""

import importlib.util
from pathlib import Path

import pytest

from almanak.connectors.aave_v3.state import DecisionInput, OutcomeInput
from tests.aave_v3_helpers import recorded_v3 as recorded_v3
from tests.reference_parameters import authored_parameters
from tests.test_aave_v3_execution import replay_states


@pytest.fixture
def reference_module():
    source = Path(__file__).parents[1] / "strategies/aave_v3_ethereum_lending/strategy.py"
    spec = importlib.util.spec_from_file_location("aave_v3_reference", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def current(config):
    """The captured config with the reference's knobs.

    The capture predates `borrow_quantity` moving from a strategy constant into
    `strategy.parameters`; the authored value (5) is the constant it ran with.
    """
    parameters = authored_parameters("aave_v3_ethereum_lending")
    return config.model_copy(update={"parameters": parameters})


def test_current_reference_replays_captured_decisions_and_intermediate_outcomes(
    recorded_v3, reference_module
):
    module = reference_module
    module.validate_config(current(recorded_v3.config))
    assert module.capabilities(current(recorded_v3.config)) == recorded_v3.capabilities
    memory = module.State()
    for state, outcome in replay_states(recorded_v3):
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
        memory.model_dump(exclude={"version", "values", "schema_id"}) == recorded_v3.memory.values
    )


def test_interrupted_entry_and_missing_observations_do_not_create_more_exposure(
    recorded_v3, reference_module
):
    outcome = next(o for o in recorded_v3.outcomes if o.decision.trigger.kind == "teardown")
    context = DecisionInput(
        config=current(recorded_v3.config),
        memory=reference_module.State(phase=1),
        owned=recorded_v3.owned,
        observations=outcome.decision.observations,
        trigger=outcome.decision.trigger,
    )
    decision = reference_module.decide(context)
    assert decision.intent.kind == "repay"
    assert decision.memory.exiting
    missing = context.model_copy(update={"observations": ()})
    assert reference_module.decide(missing).intent is None
