"""Current trusted reference component checks remain separate from process proofs."""

import importlib.util
from pathlib import Path

import pytest

from almanak.connectors.compound_v3.state import DecisionInput, OutcomeInput
from tests.compound_v3_helpers import candidate_compound_support as candidate_compound_support
from tests.compound_v3_helpers import recorded_compound as recorded_compound
from tests.reference_parameters import TypedHooks
from tests.test_compound_v3_execution import replay_states


@pytest.fixture
def reference_module(recorded_compound):
    network = (
        "ethereum"
        if str(recorded_compound.config.positions[0].chain.chain_id) == "eip155:1"
        else "base"
    )
    source = Path(__file__).parents[1] / f"strategies/compound_v3_{network}_lending/strategy.py"
    spec = importlib.util.spec_from_file_location("compound_reference", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return TypedHooks(module)


def test_current_reference_replays_all_recorded_decisions_and_intermediate_outcomes(
    recorded_compound, reference_module
):
    module = reference_module
    module.validate_config(recorded_compound.config)
    assert module.capabilities(recorded_compound.config) == recorded_compound.capabilities
    memory = module.State()
    for state, outcome in replay_states(recorded_compound):
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
    recorded_compound, reference_module
):
    module = reference_module
    outcome = next(o for o in recorded_compound.outcomes if o.decision.trigger.kind == "teardown")
    context = DecisionInput(
        config=recorded_compound.config,
        memory=module.State(phase=1),
        owned=recorded_compound.owned,
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
        update={"observations": (), "trigger": recorded_compound.outcomes[0].decision.trigger}
    )
    assert module.decide(initial).intent is None


def test_surplus_credit_is_withdrawn_before_collateral_release(recorded_compound, reference_module):
    module = reference_module
    observed = list(recorded_compound.closure)
    index = next(i for i, o in enumerate(observed) if o.allocation == "loan")
    observed[index] = observed[index].model_copy(
        update={"account": observed[index].account.model_copy(update={"principal": 1})}
    )
    context = DecisionInput(
        config=recorded_compound.config,
        memory=module.State(exiting=True),
        owned=recorded_compound.owned,
        observations=tuple(observed),
        trigger=recorded_compound.outcomes[-1].decision.trigger,
    )
    assert module.decide(context).intent == recorded_compound.config.withdraw("loan")
    closed = context.model_copy(update={"observations": recorded_compound.closure})
    assert module.decide(closed).intent is None


def test_reference_refuses_wrong_fund_purposes_and_missing_allocations(
    recorded_compound, reference_module
):
    config = recorded_compound.config
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


@pytest.mark.parametrize(
    "name,change",
    [
        ("earn", {"capital": 9}),
        ("collateral", {"capital": 0}),
        ("loan", {"borrow": False}),
    ],
)
def test_reference_requires_supply_capital_and_one_borrowing_allocation(
    recorded_compound, reference_module, name, change
):
    config = recorded_compound.config
    changed = tuple(
        a.model_copy(update=change) if a.name == name else a for a in config.allocations
    )
    with pytest.raises(ValueError, match="separate supply capital and borrowing"):
        reference_module.validate_config(config.model_copy(update={"allocations": changed}))


def test_reference_accepts_an_optional_loan_asset_reserve(recorded_compound, reference_module):
    config = recorded_compound.config
    changed = tuple(
        a.model_copy(update={"capital": 10**6}) if a.name == "loan" else a
        for a in config.allocations
    )
    reference_module.validate_config(config.model_copy(update={"allocations": changed}))


def test_reference_borrows_its_fixed_amount_at_the_asset_precision(
    recorded_compound, reference_module
):
    config = recorded_compound.config
    decimals = config.asset_metadata[config.allocation("loan").reserve.asset].decimals
    typed = reference_module.typed_config(config)
    assert reference_module.borrow_amount(typed, "loan") == 200 * 10**decimals
    borrow = next(o for o in recorded_compound.outcomes if o.operation.kind == "borrow")
    assert borrow.operation.amount.amount == reference_module.borrow_amount(
        reference_module.typed_config(config), "loan"
    )
