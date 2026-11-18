"""Replay captured admission and ownership transitions through V3 protocol checks."""

from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.connectors.aave_v3.execution import (
    validate_current,
    validate_operation,
    validate_simulation,
)
from almanak.connectors.aave_v3.lifecycle import AaveLifecycle
from almanak.connectors.aave_v3.planning import operation
from almanak.gateway.ethereum import DataError
from tests.aave_v3_helpers import recorded_v3 as recorded_v3


def replay_states(recorded):
    state = AaveLifecycle.initial(
        recorded.config, recorded.strategy, recorded.capabilities, recorded.permissions
    )
    AaveLifecycle.validate_initial(state, recorded.baseline)
    state = state.model_copy(update={"initialized": True, "baseline": recorded.baseline})
    for outcome in recorded.outcomes:
        state = state.model_copy(
            update={
                "mode": "teardown" if outcome.decision.trigger.kind == "teardown" else "running",
                "decision": outcome.decision,
                "memory": outcome.decision.memory,
            }
        )
        yield state, outcome
        state = state.model_copy(
            update={
                "owned": AaveLifecycle.commit_owned(state.owned, outcome),
                "outcomes": (*state.outcomes, outcome),
                "delivered": state.delivered + 1,
            }
        )
    AaveLifecycle.validate_closure(state, recorded.closure)


def test_captured_operations_reconcile_authority_capacity_and_owned_shares(recorded_v3):
    backend = SimpleNamespace(config=recorded_v3.config, guard=lambda: None)
    for state, outcome in replay_states(recorded_v3):
        actual = operation(state.config, state, outcome.decision.intent, outcome.before)
        assert actual == outcome.operation
        validate_operation(backend, state, actual, outcome.before)
        validate_current(backend, outcome, outcome.before)


@pytest.mark.parametrize("fault", ["shares", "allowance", "inventory", "bitmap"])
def test_changed_preparation_inputs_refuse_submission(recorded_v3, fault):
    outcome = next(o for o in recorded_v3.outcomes if o.operation.kind == "borrow")
    current = outcome.before
    if fault == "shares":
        current = current.model_copy(
            update={
                "position": current.position.model_copy(update={"debt_scaled": 1}),
            }
        )
    elif fault == "allowance":
        current = current.model_copy(update={"allowance": 1})
    elif fault == "inventory":
        current = current.model_copy(update={"wallet_balance": current.wallet_balance - 1})
    else:
        current = current.model_copy(
            update={
                "position": current.position.model_copy(update={"user_configuration": 0}),
            }
        )
    with pytest.raises(DataError, match="after preparation"):
        validate_current(SimpleNamespace(config=recorded_v3.config), outcome, current)


@pytest.mark.parametrize("kind", ["approve", "supply", "borrow", "collateral", "withdraw", "repay"])
def test_malformed_simulation_results_cannot_authorize_a_transaction(recorded_v3, kind):
    outcome = next(o for o in recorded_v3.outcomes if o.operation.kind == kind)
    raw = "0x" + encode(("uint256",), (0,)).hex()
    with pytest.raises(DataError, match="rejected|unexpected|differs"):
        validate_simulation(outcome.operation, raw, outcome.before)


def test_initialization_and_closure_do_not_adopt_foreign_balances_or_allowances(recorded_v3):
    empty = AaveLifecycle.initial(
        recorded_v3.config, recorded_v3.strategy, recorded_v3.capabilities, recorded_v3.permissions
    )
    foreign = recorded_v3.baseline[0].model_copy(update={"allowance": 1})
    with pytest.raises(DataError, match="cannot adopt"):
        AaveLifecycle.validate_initial(empty, (foreign, recorded_v3.baseline[1]))
    with pytest.raises(DataError, match="do not cover"):
        AaveLifecycle.validate_initial(empty, recorded_v3.baseline[:1])
    closure = recorded_v3.closure[0].model_copy(update={"allowance": 1})
    with pytest.raises(DataError, match="retains"):
        AaveLifecycle.validate_closure(recorded_v3, (closure, recorded_v3.closure[1]))
