"""Actual native GM requests reproduce escrow, fungible ownership and keeper effects offline."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import (
    exchange_reconciliation,
    liquidity_planning,
    liquidity_reconciliation,
)
from almanak.gateway.ethereum import DataError
from tests.gmx_native_runtime import before_state
from tests.gmx_native_runtime import current_gm_completed as current_gm_completed
from tests.gmx_retired import (
    assert_request_sized,
    following_request,
    replayed_operation,
)


def test_actual_gm_plans_replay_with_request_sized_approvals(completed):
    for index, outcome in enumerate(completed.outcomes):
        if outcome.kind != "gmx_transaction_outcome":
            continue
        state = before_state(completed, index)
        approvals, operation = replayed_operation(
            liquidity_planning.operation, completed.config, state, outcome
        )
        request = following_request(completed.outcomes, index)
        assert_request_sized(approvals, operation, outcome.operation, request)


def test_actual_gm_creation_replays_requests_and_exact_transaction_effects(completed):
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


def test_actual_gm_keeper_replay_preserves_effects_and_never_attributes_keeper_gas(
    completed, monkeypatch
):
    for index, expected in enumerate(completed.outcomes):
        if expected.kind != "gmx_request_outcome":
            continue
        state = before_state(completed, index)
        request = completed.outcomes[index - 1].request
        state = state.model_copy(update={"requests": (request,)})
        monkeypatch.setattr(
            liquidity_reconciliation,
            "snapshot",
            lambda *args, expected=expected: expected.after.model_copy(update={"owned": args[-1]}),
        )
        for _ in range(2):
            result = exchange_reconciliation.reconcile_request(
                SimpleNamespace(), state, request, expected.request.events[-1]
            )
            assert result == expected and not hasattr(result, "gas_paid")
        orphan = state.model_copy(update={"outcomes": ()})
        with pytest.raises(ValueError, match="preceding request outcome"):
            exchange_reconciliation.reconcile_request(
                SimpleNamespace(), orphan, request, expected.request.events[-1]
            )


def test_actual_gm_keeper_rejects_unrelated_share_balance_or_allowance_changes(
    completed, monkeypatch
):
    index = next(i for i, o in enumerate(completed.outcomes) if o.kind == "gmx_request_outcome")
    expected = completed.outcomes[index]
    state = before_state(completed, index)
    request = completed.outcomes[index - 1].request
    bad_shares = expected.after.model_copy(
        update={
            "market_tokens": expected.after.market_tokens.model_copy(
                update={"amount": expected.after.market_tokens.amount + 1}
            )
        }
    )
    allowance = expected.after.allowances[0]
    bad_allowances = expected.after.model_copy(
        update={
            "allowances": (
                allowance.model_copy(update={"amount": allowance.amount + 1}),
                *expected.after.allowances[1:],
            )
        }
    )
    for after, message in ((bad_shares, "observed GM shares"), (bad_allowances, "allowances")):
        monkeypatch.setattr(liquidity_reconciliation, "snapshot", lambda *args, after=after: after)
        with pytest.raises(DataError, match=message):
            exchange_reconciliation.reconcile_request(
                SimpleNamespace(), state, request, expected.request.events[-1]
            )
