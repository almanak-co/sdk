"""Native user and keeper cancellations restore only their retained GM escrow."""

import gzip
from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import exchange_reconciliation, liquidity_reconciliation
from almanak.connectors.gmx_v2.liquidity_receipts import validate_request
from almanak.connectors.gmx_v2.state import Checkpoint
from almanak.gateway.ethereum import DataError
from tests.gmx_native_runtime import before_state
from tests.gmx_retired import current_json
from tests.retained_fixtures import retained


@pytest.fixture(params=["deposit", "withdrawal"])
def request_kind(request):
    return request.param


@pytest.fixture(params=["cancellation", "keeper-cancellation"])
def cancelled(request, request_kind):
    path = (
        retained("2026-09-09-gmx-gm")
        / f"arbitrum-safe-{request_kind}-{request.param}-runtime/checkpoint.json.gz"
    )
    return Checkpoint.model_validate_json(current_json(gzip.decompress(path.read_bytes())))


def cancellation(completed, request_kind):
    return next(
        (index, outcome)
        for index, outcome in enumerate(completed.outcomes)
        if outcome.request is not None
        and outcome.request.kind == request_kind
        and outcome.request.status == "cancelled"
    )


def test_native_gm_cancellation_replays_exact_refund_and_ownership(
    cancelled, request_kind, monkeypatch
):
    index, outcome = cancellation(cancelled, request_kind)
    if outcome.kind == "gmx_request_outcome":
        state = before_state(cancelled, index)
        request = cancelled.outcomes[index - 1].request
        state = state.model_copy(update={"requests": (request,)})
        monkeypatch.setattr(
            liquidity_reconciliation,
            "snapshot",
            lambda *args: outcome.after.model_copy(update={"owned": args[-1]}),
        )
        replayed = exchange_reconciliation.reconcile_request(
            SimpleNamespace(), state, request, outcome.request.events[-1]
        )
        assert replayed == outcome
    else:
        pending = SimpleNamespace(
            operation=outcome.operation, before=outcome.before, decision=outcome.decision
        )
        request = exchange_reconciliation.completed_request(
            pending, outcome.receipt, outcome.after.block
        )
        assert request == outcome.request
        assert exchange_reconciliation.transaction_effects(
            pending, outcome.receipt, outcome.after, request, True
        ) == (outcome.assets, outcome.after.owned)
    assert not outcome.accounting_issues


def test_cancelled_deposit_returns_both_currencies_or_withdrawal_returns_exact_shares(
    cancelled, request_kind
):
    _, outcome = cancellation(cancelled, request_kind)
    before, after = outcome.before.owned, outcome.after.owned
    assets = {value.key.asset: value.amount for value in outcome.assets}
    if request_kind == "deposit":
        assert before.market_tokens == after.market_tokens
        assert all(assets[value.asset] == value.amount for value in outcome.request.escrow)
    else:
        assert after.market_tokens.amount == (
            before.market_tokens.amount + outcome.request.escrow[0].amount
        )
        assert all(assets[asset] == 0 for asset in outcome.before.target.assets)


def test_retained_gm_request_cannot_change_escrow_fee_or_admission(cancelled, request_kind):
    creation = next(
        o for o in cancelled.outcomes if o.operation.kind == "gmx_create_" + request_kind
    )
    request = creation.request
    validate_request(creation.operation, request)
    fields = {
        "escrow": (
            request.escrow[0].model_copy(update={"amount": request.escrow[0].amount + 1}),
            *request.escrow[1:],
        ),
        "execution_fee": request.execution_fee.model_copy(
            update={"amount": request.execution_fee.amount + 1}
        ),
        "admission_deadline": request.admission_deadline + 1,
    }
    for field, value in fields.items():
        with pytest.raises(DataError, match="retained escrow, fee or admission"):
            validate_request(creation.operation, request.model_copy(update={field: value}))
