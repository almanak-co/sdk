"""Current trusted reference component checks remain separate from process proofs."""

import importlib.util
from pathlib import Path

import pytest

from almanak.connectors.morpho_blue.state import DecisionInput, OutcomeInput
from tests.morpho_blue_helpers import candidate_morpho_support as candidate_morpho_support
from tests.morpho_blue_helpers import recorded_morpho as recorded_morpho
from tests.reference_parameters import TypedHooks
from tests.test_morpho_blue_execution import replay_states


@pytest.fixture
def reference_module(recorded_morpho):
    network = (
        "ethereum"
        if str(recorded_morpho.config.positions[0].chain.chain_id) == "eip155:1"
        else "base"
    )
    source = Path(__file__).parents[1] / f"strategies/morpho_blue_{network}_lending/strategy.py"
    spec = importlib.util.spec_from_file_location("morpho_reference", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return TypedHooks(module)


def test_current_reference_replays_all_recorded_decisions_and_intermediate_outcomes(
    recorded_morpho, reference_module
):
    module = reference_module
    module.validate_config(recorded_morpho.config)
    assert module.capabilities(recorded_morpho.config) == recorded_morpho.capabilities
    memory = module.State()
    for state, outcome in replay_states(recorded_morpho):
        context = DecisionInput(
            config=state.config,
            memory=memory,
            owned=state.owned,
            observations=outcome.decision.observations,
            trigger=outcome.decision.trigger,
        )
        decision = module.decide(context)
        assert decision.intent == outcome.decision.intent and decision.required_observations
        result = outcome.strategy_result()
        memory = module.on_outcome(
            OutcomeInput(config=state.config, memory=decision.memory, result=result)
        )
        if outcome.operation.kind == "borrow":
            hold = context.model_copy(update={"memory": memory})
            assert module.decide(hold).intent is None
            assert module.decide(hold).memory == memory
    assert memory.phase == 9 and not memory.exiting


def test_interrupted_entry_and_missing_borrower_preserve_independent_earning_exit(
    recorded_morpho, reference_module
):
    module = reference_module
    outcome = next(o for o in recorded_morpho.outcomes if o.decision.trigger.kind == "teardown")
    context = DecisionInput(
        config=recorded_morpho.config,
        memory=module.State(phase=1),
        owned=recorded_morpho.owned,
        observations=outcome.decision.observations,
        trigger=outcome.decision.trigger,
    )
    decision = module.decide(context)
    assert decision.intent.kind == "repay" and decision.memory.exiting
    missing = context.model_copy(
        update={"observations": tuple(o for o in context.observations if o.allocation == "earn")}
    )
    decision = module.decide(missing)
    assert decision.intent.target.allocation == "earn" and decision.intent.kind == "withdraw"
    assert module.decide(missing.model_copy(update={"observations": ()})).intent is None
    initial = context.model_copy(
        update={"observations": (), "trigger": recorded_morpho.outcomes[0].decision.trigger}
    )
    assert module.decide(initial).intent is None


def test_reference_refuses_wrong_fund_purposes_and_missing_allocations(
    recorded_morpho, reference_module
):
    config = recorded_morpho.config
    allocations = config.allocations
    with pytest.raises(ValueError, match="requires earn"):
        reference_module.validate_config(config.model_copy(update={"allocations": allocations[:1]}))
    loan = config.allocation("loan")
    changed = tuple(
        a.model_copy(update={"account": loan.account}) if a.name == "earn" else a
        for a in allocations
    )
    with pytest.raises(ValueError, match="separate funds"):
        reference_module.validate_config(config.model_copy(update={"allocations": changed}))
