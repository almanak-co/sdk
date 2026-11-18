"""Replay actual retained fork effects through each new author source, entirely offline."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.composition import checkpoint_from_json, connector_for
from almanak.reporting import report
from almanak.work import ObservationScope
from tests.dx_replay import assert_intent, memory_values
from tests.lending_captures import current_capture
from tests.reference_parameters import TypedHooks, authored_parameters
from tests.retained_scheduling import checkpoint_input, synthetic_event
from tests.uniswap_support import scope_only

ROOT = Path(__file__).parents[1]
NAMES = (
    "aave_v4_avalanche_borrowing",
    "aave_v4_multichain_borrowing",
    "uniswap_v4_avalanche_lp",
    "uniswap_v4_avalanche_trading",
)


def proof(name):
    raw = json.loads(
        gzip.decompress((ROOT / "tests/fixtures" / (name + "-lifecycle.json.gz")).read_bytes())
    )
    if name.startswith("uniswap_v4_"):
        raw = scope_only(raw)
    if name.startswith("aave_v4_"):
        raw = current_capture(raw)
    state = checkpoint_from_json(json.dumps(checkpoint_input(raw)))
    # The captures predate strategy.parameters: the knobs the strategy constants held
    # then are the reference's authored values now.
    config = state.config.model_copy(update={"parameters": authored_parameters(name)})
    return state.model_copy(update={"config": config})


def module(name):
    path = ROOT / "strategies" / name / "strategy.py"
    namespace = {}
    exec(compile(path.read_text(), str(path), "exec"), namespace)
    return TypedHooks(SimpleNamespace(**namespace))


def replay_decision(connector, source, state, memory, owned, outcome):
    observations = outcome.decision.observations
    targets = {o.target for o in observations}
    context = connector.codec.decision_input(
        config=state.config,
        memory=memory,
        owned=tuple(o for o in owned if o.target in targets),
        observations=observations,
        trigger=synthetic_event(),
    )
    decision = source.decide(context)
    assert_intent(state, decision.intent, outcome)
    assert set(decision.required_observations) == {
        ObservationScope(
            chain=o.target.fund.chain, account=o.target.account, allocation=o.target.allocation
        )
        for o in observations
    }
    return source.on_outcome(
        connector.codec.outcome_input(
            config=state.config, memory=memory, result=outcome.strategy_result()
        )
    )


@pytest.mark.parametrize("name", NAMES)
def test_real_strategy_replays_every_captured_economic_transition(name):
    state, source = proof(name), module(name)
    connector = connector_for(state.config)
    source.validate_config(state.config)
    assert source.capabilities(state.config) == state.capabilities
    initial = connector.lifecycle.initial(
        state.config, state.strategy, state.capabilities, state.permissions
    )
    memory, owned = source.State(), initial.owned
    for outcome in state.outcomes:
        memory = replay_decision(connector, source, state, memory, owned, outcome)
        owned = connector.lifecycle.commit_owned(owned, outcome)
    assert memory_values(memory) == state.memory.values and owned == state.owned
    connector.lifecycle.validate_closure(state, state.closure)
    assert report(state)["transaction_accounting_complete"]
    context = connector.codec.decision_input(
        config=state.config,
        memory=memory,
        owned=owned,
        observations=state.closure,
        trigger=synthetic_event(teardown=True),
    )
    assert source.decide(context).intent is None


@pytest.mark.parametrize("name", NAMES)
def test_missing_observations_never_reopen_closed_positions(name):
    state = proof(name)
    connector = connector_for(state.config)
    context = connector.codec.decision_input(
        config=state.config,
        memory=module(name).State(**state.memory.values),
        owned=(),
        observations=(),
        trigger=synthetic_event(),
    )
    result = module(name).decide(context)
    assert result.intent is None and memory_values(result.memory) == state.memory.values
