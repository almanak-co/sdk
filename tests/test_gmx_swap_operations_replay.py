"""Native swap receipts reproduce gateway planning and reject expired admission."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import exchange_planning, exchange_reconciliation
from almanak.connectors.gmx_v2.api import Actions
from almanak.connectors.gmx_v2.exchange_validation import validate_current
from almanak.gateway.ethereum import DataError
from tests.gmx_native_runtime import current_exchange_completed as current_exchange_completed
from tests.gmx_retired import assert_request_sized, following_request, replayed_operation


def before_state(completed, index):
    outcome = completed.outcomes[index]
    return completed.model_copy(
        update={
            "mode": "running",
            "decision": outcome.decision,
            "outcomes": completed.outcomes[:index],
            "owned": (outcome.before.owned,),
            "requests": (),
        }
    )


def test_native_swap_plans_replay_with_request_sized_approvals(completed):
    for index, outcome in enumerate(completed.outcomes):
        if outcome.kind != "gmx_transaction_outcome":
            continue
        before = before_state(completed, index)
        approvals, operation = replayed_operation(
            exchange_planning.operation, completed.config, before, outcome
        )
        request = following_request(completed.outcomes, index)
        assert_request_sized(approvals, operation, outcome.operation, request)


def test_native_swap_operations_reproduce_transaction_effects(completed):
    for outcome in completed.outcomes:
        if outcome.kind != "gmx_transaction_outcome":
            continue
        pending = SimpleNamespace(
            operation=outcome.operation, before=outcome.before, decision=outcome.decision
        )
        request = exchange_reconciliation.completed_request(
            pending, outcome.receipt, outcome.after.block
        )
        assert request == outcome.request
        assets, owned = exchange_reconciliation.transaction_effects(
            pending, outcome.receipt, outcome.after, request, True
        )
        assert assets == outcome.assets and owned == outcome.after.owned
        assert exchange_reconciliation.transaction_effects(
            pending, outcome.receipt, outcome.before, None, False
        ) == ((), outcome.before.owned)


def test_repeated_swaps_need_only_owned_inventory(completed):
    """Scope-only authority: a swap repeats while the fund owns its inputs, with no budget."""
    first = next(
        o
        for o in completed.outcomes
        if o.kind == "gmx_transaction_outcome" and o.operation.kind == "gmx_create_swap"
    )
    exchange_planning.validate_inventory(first.operation, first.before)
    owned = first.before.owned
    short = owned.model_copy(
        update={
            "inventory": tuple(
                value.model_copy(update={"amount": 0})
                if value.asset == first.operation.amount.asset
                else value
                for value in owned.inventory
            )
        }
    )
    with pytest.raises(DataError, match="owned input inventory"):
        exchange_planning.validate_inventory(
            first.operation, first.before.model_copy(update={"owned": short})
        )


def test_swap_preflight_rejects_expired_deadline_and_changed_wallets(completed):
    outcome = completed.outcomes[1]
    plan = SimpleNamespace(
        operation=outcome.operation, decision=outcome.decision, before=outcome.before
    )
    validate_current(plan, outcome.before)
    late = outcome.before.model_copy(
        update={
            "block": outcome.before.block.model_copy(
                update={"timestamp": outcome.decision.intent.deadline}
            )
        }
    )
    with pytest.raises(DataError, match="deadline expired"):
        validate_current(plan, late)
    for field, values, message in (
        ("allowances", outcome.before.allowances, "allowances changed"),
        ("wallets", outcome.before.wallets, "no longer covers"),
    ):
        amount = 0 if field == "wallets" else values[0].amount + 1
        current = outcome.before.model_copy(
            update={field: (values[0].model_copy(update={"amount": amount}), *values[1:])}
        )
        with pytest.raises(DataError, match=message):
            validate_current(plan, current)
    for deadline in (outcome.before.block.timestamp, outcome.before.block.timestamp + 301):
        with pytest.raises(DataError, match="admission window"):
            exchange_planning.validate_deadline(deadline, outcome.before.block.timestamp, 300)


def test_exit_helper_uses_only_keeper_settled_proceeds_then_explicit_revocation(completed):
    holding = completed.outcomes[2].after
    context = SimpleNamespace(config=completed.config, require_observation=lambda name: holding)
    actions = Actions(context)
    result = actions.exit("Main")
    returned = [
        o.operation
        for o in completed.outcomes
        if o.kind == "gmx_transaction_outcome" and o.operation.kind == "gmx_create_swap"
    ][-1]
    assert result.amount == returned.amount
    assert result.minimum_output == completed.config.allocations[0].exit_minimum_output
    # Request-sized approvals leave nothing to revoke; a residual allowance must be revoked.
    closed = completed.closure[0]
    allowance = closed.allowances[0].model_copy(update={"amount": 1})
    residual = closed.model_copy(update={"allowances": (allowance, *closed.allowances[1:])})
    context.require_observation = lambda name: residual
    assert actions.exit("Main").kind == "revoke_exchange"
    context.require_observation = lambda name: closed
    assert actions.exit("Main") is None
