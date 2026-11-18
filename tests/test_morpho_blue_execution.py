"""Retained native outcomes verify Morpho admission, receipt accounting and reporting."""

from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.connectors.morpho_blue.execution import (
    MorphoExecution,
    validate_current,
    validate_operation,
    validate_simulation,
)
from almanak.connectors.morpho_blue.lifecycle import MorphoLifecycle
from almanak.connectors.morpho_blue.observations import owned
from almanak.connectors.morpho_blue.planning import operation
from almanak.connectors.morpho_blue.receipts import accounting_issues, movement, validate_effects
from almanak.connectors.morpho_blue.reporting import report
from almanak.gateway.ethereum import DataError
from tests.morpho_blue_helpers import candidate_morpho_support as candidate_morpho_support
from tests.morpho_blue_helpers import recorded_morpho as recorded_morpho


def replay_states(recorded):
    state = MorphoLifecycle.initial(
        recorded.config, recorded.strategy, recorded.capabilities, recorded.permissions
    )
    MorphoLifecycle.validate_initial(state, recorded.baseline)
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
                "owned": MorphoLifecycle.commit_owned(state.owned, outcome),
                "outcomes": (*state.outcomes, outcome),
                "delivered": state.delivered + 1,
            }
        )
    MorphoLifecycle.validate_closure(state, recorded.closure)


def recaptured_full_repay_approval(actual, outcome):
    """Captures predate blueprint 25, whose full repay approves the held balance.

    The recorded approval took the old repayment ceiling instead, so today's plan for
    that one step differs and its recorded call and receipt cannot be replayed.
    """
    recorded = outcome.operation
    return (
        outcome.decision.intent.kind == "repay"
        and actual.kind == recorded.kind == "approve"
        and actual.purpose == recorded.purpose == "repay"
        and actual.target == recorded.target
        and actual.amount.amount == outcome.before.wallet_balance
    )


def test_recorded_safe_and_eoa_operations_keep_authority_inventory_and_receipt_effects(
    recorded_morpho,
):
    backend = SimpleNamespace(config=recorded_morpho.config, guard=lambda: None)
    for state, outcome in replay_states(recorded_morpho):
        actual = operation(state.config, state, outcome.decision.intent, outcome.before)
        if actual != outcome.operation:
            assert recaptured_full_repay_approval(actual, outcome)
            continue
        validate_operation(backend, state, actual, outcome.before)
        validate_current(backend, outcome, outcome.before)
        result = (
            "0x" + encode(("bool",), (True,)).hex()
            if actual.kind in ("approve", "revoke")
            else "0x"
            + encode(
                ("uint256", "uint256"), (outcome.assets, movement(outcome.receipt, actual).shares)
            ).hex()
            if actual.target.reserve.is_loan
            else "0x"
        )
        validate_simulation(actual, result, outcome.before)
        assert MorphoExecution.call(state.config, actual) == outcome.call
        result = movement(outcome.receipt, actual)
        assert result.assets == outcome.assets
        assert validate_effects(outcome, outcome.after, result, True) == (
            outcome.supply_shares_delta,
            outcome.borrow_shares_delta,
            outcome.collateral_delta,
        )
        assert (
            accounting_issues(
                outcome.before, outcome.after, actual, result, outcome.gas_paid.amount
            )
            == ()
        )
        assert owned(outcome.after).target == outcome.operation.target


def test_retained_reports_keep_share_units_and_separate_gas_payers(recorded_morpho):
    from tests.strategy_fork.morpho_assertions import assert_closed

    assert_closed(None, recorded_morpho)
    value = report(recorded_morpho)
    assert value["transaction_accounting_complete"] and value["mode"] == "closed"
    assert len(value["positions"]) == 3 and not value["accounting_issues"]
    assert sum(int(p["gas_paid"]["amount"]) for p in value["gas_payers"]) == sum(
        o.gas_paid.amount for o in recorded_morpho.outcomes
    )
    for position in value["positions"]:
        assert position["ownership_units"] == {
            "supply_shares": "morpho_market_supply_shares",
            "borrow_shares": "morpho_market_borrow_shares",
            "collateral": "collateral_base_units",
        }
        assert position["measurement_status"] == "retained"
    for binding in recorded_morpho.config.accounts.bindings:
        rows, pristine = MorphoExecution.preflight(
            recorded_morpho.config, binding.name, recorded_morpho.baseline
        )
        assert pristine and all(row["funded"] for row in rows)
    MorphoExecution.validate_config(recorded_morpho.config)


@pytest.mark.parametrize("fault", ["shares", "collateral", "allowance", "inventory"])
def test_changed_inputs_cannot_authorize_prepared_submission(recorded_morpho, fault):
    outcome = next(o for o in recorded_morpho.outcomes if o.operation.kind == "borrow")
    current = outcome.before
    if fault in ("shares", "collateral"):
        position = current.account.position
        key = "supply_shares" if fault == "shares" else "collateral"
        current = current.model_copy(
            update={
                "account": current.account.model_copy(
                    update={
                        "position": position.model_copy(update={key: getattr(position, key) + 1})
                    }
                )
            }
        )
    else:
        key = "allowance" if fault == "allowance" else "wallet_balance"
        current = current.model_copy(
            update={key: getattr(current, key) + (1 if fault == "allowance" else -1)}
        )
    with pytest.raises(DataError, match="after preparation"):
        validate_current(SimpleNamespace(config=recorded_morpho.config), outcome, current)


def test_retained_borrow_transition_meets_reference_hold_ownership_contract(recorded_morpho):
    from tests.strategy_fork.morpho_assertions import assert_entry_positions

    state, outcome = next(
        (s, o) for s, o in replay_states(recorded_morpho) if o.operation.kind == "borrow"
    )
    memory = next(
        o.decision.memory
        for o in recorded_morpho.outcomes
        if o.decision.memory.values["phase"] == 3
    )
    held = state.model_copy(
        update={
            "decision": None,
            "memory": memory,
            "owned": MorphoLifecycle.commit_owned(state.owned, outcome),
            "outcomes": (*state.outcomes, outcome),
            "delivered": len(state.outcomes) + 1,
        }
    )
    assert_entry_positions(held)
