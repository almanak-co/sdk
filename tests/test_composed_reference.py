"""Validate the reusable mixed reference through its actual sandboxed author hooks."""

from pathlib import Path

import pytest

from almanak.connectors.composition import checkpoint_from_json, connector_for
from almanak.memory_codec import freeze
from almanak.runtime import validate_strategy_config
from almanak.strategy import StrategyDefinition
from tests.composed_public import resolved_parameters
from tests.lending_captures import read_capture
from tests.reference_parameters import authored_parameters
from tests.test_composed_inventory import config as config
from tests.test_new_reference_replay import module
from tests.uniswap_support import scope_only_json


def test_author_inventory_and_market_contract_validate_in_sandbox(config):
    directory = Path(__file__).parents[1] / "strategies/base_borrow_and_trade"
    _, author = resolved_parameters(directory.name)
    definition = StrategyDefinition.read(directory / "strategy.py")
    validate_strategy_config(definition, author)
    loan = author.allocation("loan")
    source = module(directory.name)
    borrowed = source.borrow_amount(source.typed_config(author), "loan")
    assert loan.borrow and author.allocation("trade").capital.amount < borrowed


@pytest.mark.parametrize(
    ("reference", "capture"),
    [
        ("base_borrow_and_trade", "composed-base-safe-lifecycle.json.gz"),
        ("base_borrow_and_trade_v4", "composed-v4-closed-fork.json.gz"),
        ("arbitrum_aave_borrow_and_trade_v4", "composed-v4-arbitrum-closed-fork.json.gz"),
        ("optimism_aave_borrow_and_trade_v4", "composed-v4-optimism-closed-fork.json.gz"),
        ("ethereum_base_borrow_and_trade", "composed-multichain-safe-lifecycle.json.gz"),
    ],
)
def test_retained_safe_effects_replay_through_one_shared_inventory(config, reference, capture):
    path = Path(__file__).with_name("fixtures") / capture
    recorded = checkpoint_from_json(scope_only_json(read_capture(path)))
    # The capture predates strategy.parameters; the knobs are the reference's authored ones.
    knobs = authored_parameters(reference)
    recorded = recorded.model_copy(
        update={"config": recorded.config.model_copy(update={"parameters": knobs})}
    )
    source = module(reference)
    source.validate_config(recorded.config)
    assert source.capabilities(recorded.config) == recorded.capabilities
    binding = connector_for(recorded.config)
    current = binding.lifecycle.initial(
        recorded.config, recorded.strategy, recorded.capabilities, recorded.permissions
    )
    memory, decision_id = source.State(), None
    for outcome in recorded.outcomes:
        if outcome.decision.identity != decision_id:
            context = binding.codec.decision_input(
                config=recorded.config,
                memory=memory,
                owned=current.owned,
                observations=outcome.decision.observations,
                trigger=outcome.decision.trigger,
            )
            decision = source.decide(context)
            assert decision.intent == outcome.decision.intent
            memory, decision_id = decision.memory, outcome.decision.identity
        memory = source.on_outcome(
            binding.codec.outcome_input(
                config=recorded.config, memory=memory, result=outcome.strategy_result()
            )
        )
        current = current.model_copy(update=binding.lifecycle.ownership(current, outcome))
        current = checkpoint_from_json(current.model_dump_json())
    assert (current.owned, current.inventory) == (recorded.owned, recorded.inventory)
    assert freeze(vars(source._module), memory) == recorded.memory
    binding.lifecycle.validate_closure(current, recorded.closure)


def test_preflight_requires_shared_author_cash_without_prefunding_borrowed_trade(config):
    from almanak.connectors.composed.execution import preflight

    path = Path(__file__).with_name("fixtures") / "composed-base-safe-lifecycle.json.gz"
    recorded = checkpoint_from_json(scope_only_json(read_capture(path)))
    tokens, pristine = preflight(recorded.config, "fund", recorded.baseline)
    assert pristine and all(token["funded"] for token in tokens)
    assert len(tokens) == len(recorded.config.inventory) == 2
    loan = recorded.config.target("loan")
    cash = next(b.amount for b in recorded.config.inventory if b.key.asset == loan.asset)
    assert cash < recorded.config.allocation("trade").capital.amount
