"""Replay retained fund effects through the current reusable author references."""

import json
from pathlib import Path

import pytest

from almanak.connectors.composition import checkpoint_from_json, connector_for
from almanak.connectors.interfaces import Connector
from almanak.memory_codec import freeze
from almanak.reporting import report
from tests.lending_captures import read_capture
from tests.strategy_fork.lending_trade_assertions import (
    assert_closed,
    assert_fund_position,
    identity,
)
from tests.test_composed_lending_prepared import candidate_support
from tests.test_new_reference_replay import module
from tests.uniswap_support import scope_only_json

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/composed-lending-native"
)
CASES = tuple(json.loads((EVIDENCE / "manifest.json").read_text())["cases"])


@pytest.fixture(params=CASES, ids=lambda row: row["name"])
def recorded(request, monkeypatch):
    monkeypatch.setattr(Connector, "support_for", candidate_support)
    path = EVIDENCE / request.param["name"] / "checkpoint.json.gz"
    reference = request.param["reference"]
    return checkpoint_from_json(scope_only_json(read_capture(path, reference))), reference


def replay_states(recorded):
    binding = connector_for(recorded.config)
    current = binding.lifecycle.initial(
        recorded.config, recorded.strategy, recorded.capabilities, recorded.permissions
    )
    for outcome in recorded.outcomes:
        yield current, outcome
        current = current.model_copy(update=binding.lifecycle.ownership(current, outcome))
        current = checkpoint_from_json(current.model_dump_json())


def test_current_reference_replays_every_native_decision_without_duplicating_cash(recorded):
    state, reference = recorded
    source = module(reference)
    source.validate_config(state.config)
    assert source.capabilities(state.config) == state.capabilities
    binding = connector_for(state.config)
    memory, decision_id = source.State(), None
    for current, outcome in replay_states(state):
        if outcome.decision.identity != decision_id:
            context = binding.codec.decision_input(
                config=state.config,
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
                config=state.config, memory=memory, result=outcome.strategy_result()
            )
        )
    current = current.model_copy(update=binding.lifecycle.ownership(current, outcome))
    assert (current.owned, current.inventory) == (state.owned, state.inventory)
    assert freeze(vars(source._module), memory) == state.memory
    binding.lifecycle.validate_closure(current, state.closure)
    assert_closed(None, state)
    assert report(state)["transaction_accounting_complete"]


def test_native_holding_assertion_rejects_a_different_fund_debt(recorded):
    state, _ = recorded
    current, outcome = next(
        (current, outcome)
        for current, outcome in replay_states(state)
        if outcome.decision.intent.kind == "exit_exchange"
    )
    account = next(
        observation.account
        for observation in outcome.decision.observations
        if observation.allocation == "loan"
    )
    native = identity(state.config)
    assert_fund_position(current, account, native)
    if str(native) == "morpho_blue":
        account = account.model_copy(
            update={
                "position": account.position.model_copy(
                    update={"borrow_shares": account.position.borrow_shares + 1}
                )
            }
        )
    else:
        account = account.model_copy(update={"principal": account.principal + 1})
    with pytest.raises(AssertionError):
        assert_fund_position(current, account, native)
