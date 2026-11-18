"""Retained native outcomes verify Comet admission, receipt accounting and reporting."""

from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.connectors.compound_v3.execution import (
    CompoundExecution,
    validate_current,
    validate_operation,
    validate_simulation,
)
from almanak.connectors.compound_v3.lifecycle import CompoundLifecycle
from almanak.connectors.compound_v3.observations import owned
from almanak.connectors.compound_v3.planning import operation
from almanak.connectors.compound_v3.receipts import accounting_issues, movement, validate_effects
from almanak.connectors.compound_v3.reporting import report
from almanak.gateway.ethereum import DataError
from tests.compound_v3_helpers import candidate_compound_support as candidate_compound_support
from tests.compound_v3_helpers import recaptured_compound as recaptured_compound
from tests.compound_v3_helpers import recorded_compound as recorded_compound


def replay_states(recorded):
    state = CompoundLifecycle.initial(
        recorded.config, recorded.strategy, recorded.capabilities, recorded.permissions
    )
    CompoundLifecycle.validate_initial(state, recorded.baseline)
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
                "owned": CompoundLifecycle.commit_owned(state.owned, outcome),
                "outcomes": (*state.outcomes, outcome),
                "delivered": state.delivered + 1,
            }
        )
    CompoundLifecycle.validate_closure(state, recorded.closure)


# The 2026-09-07 repay approvals predate the held-balance approval of blueprint 25, so
# this replay reads the 2026-10-20 recapture of the same six lifecycles.
def test_recorded_safe_and_eoa_operations_keep_authority_inventory_and_receipt_effects(
    recaptured_compound,
):
    backend = SimpleNamespace(config=recaptured_compound.config, guard=lambda: None)
    for state, outcome in replay_states(recaptured_compound):
        actual = operation(state.config, state, outcome.decision.intent, outcome.before)
        assert actual == outcome.operation
        validate_operation(backend, state, actual, outcome.before)
        validate_current(backend, outcome, outcome.before)
        result = (
            "0x" + encode(("bool",), (True,)).hex()
            if actual.kind in ("approve", "revoke")
            else "0x"
        )
        validate_simulation(actual, result, outcome.before)
        assert CompoundExecution.call(state.config, actual) == outcome.call
        assert movement(outcome.receipt, actual) == outcome.assets
        assert validate_effects(outcome, outcome.after, outcome.assets, True) == (
            outcome.principal_delta,
            outcome.collateral_delta,
        )
        assert (
            accounting_issues(
                outcome.before, outcome.after, actual, outcome.assets, outcome.gas_paid.amount
            )
            == ()
        )
        assert owned(outcome.after).target == outcome.operation.target


@pytest.mark.parametrize("fault", ["principal", "allowance", "inventory", "bitmap", "collateral"])
def test_changed_inputs_cannot_authorize_prepared_submission(recorded_compound, fault):
    outcome = next(o for o in recorded_compound.outcomes if o.operation.kind == "borrow")
    current = outcome.before
    if fault == "principal":
        current = current.model_copy(
            update={"account": current.account.model_copy(update={"principal": -1})}
        )
    elif fault == "allowance":
        current = current.model_copy(update={"allowance": 1})
    elif fault == "inventory":
        current = current.model_copy(update={"wallet_balance": current.wallet_balance - 1})
    elif fault == "bitmap":
        current = current.model_copy(
            update={"account": current.account.model_copy(update={"assets_in": 0})}
        )
    else:
        balances = list(current.account.collaterals)
        balances[0] = balances[0].model_copy(update={"balance": balances[0].balance + 1})
        current = current.model_copy(
            update={"account": current.account.model_copy(update={"collaterals": tuple(balances)})}
        )
    with pytest.raises(DataError, match="after preparation"):
        validate_current(SimpleNamespace(config=recorded_compound.config), outcome, current)


@pytest.mark.parametrize("kind", ["approve", "supply", "borrow", "repay", "withdraw", "revoke"])
def test_simulation_rejects_unexpected_contract_returns(recorded_compound, kind):
    outcome = next(o for o in recorded_compound.outcomes if o.operation.kind == kind)
    with pytest.raises(DataError, match="rejected|unexpected"):
        validate_simulation(
            outcome.operation, "0x" + encode(("uint256",), (0,)).hex(), outcome.before
        )


def test_retained_reports_keep_financial_units_and_separate_gas_payers(recorded_compound):
    from tests.strategy_fork.compound_assertions import assert_closed

    assert_closed(None, recorded_compound)
    value = report(recorded_compound)
    assert value["transaction_accounting_complete"] and value["mode"] == "closed"
    assert len(value["positions"]) == 3 and not value["accounting_issues"]
    assert sum(int(p["gas_paid"]["amount"]) for p in value["gas_payers"]) == sum(
        o.gas_paid.amount for o in recorded_compound.outcomes
    )
    assert {p["ownership_unit"] for p in value["positions"]} == {
        "compound_signed_base_principal",
        "collateral_base_units",
    }
    assert all(p["measurement_status"] == "retained" for p in value["positions"])
    for binding in recorded_compound.config.accounts.bindings:
        rows, pristine = CompoundExecution.preflight(
            recorded_compound.config, binding.name, recorded_compound.baseline
        )
        assert pristine and all(row["funded"] for row in rows)
    CompoundExecution.validate_config(recorded_compound.config)


def test_retained_borrow_transition_meets_reference_hold_ownership_contract(recorded_compound):
    from tests.strategy_fork.compound_assertions import assert_entry_positions

    state, outcome = next(
        (s, o) for s, o in replay_states(recorded_compound) if o.operation.kind == "borrow"
    )
    memory = next(
        o.decision.memory
        for o in recorded_compound.outcomes
        if o.decision.memory.values["phase"] == 3
    )
    held = state.model_copy(
        update={
            "decision": None,
            "memory": memory,
            "owned": CompoundLifecycle.commit_owned(state.owned, outcome),
            "outcomes": (*state.outcomes, outcome),
            "delivered": len(state.outcomes) + 1,
        }
    )
    assert_entry_positions(held)
