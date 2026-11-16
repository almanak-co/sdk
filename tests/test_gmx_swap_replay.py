"""Frozen native swap receipts prove keeper accounting and reject unrelated ownership."""

import gzip
from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import exchange_reconciliation as reconcile
from almanak.connectors.gmx_v2.state import Checkpoint
from almanak.gateway.ethereum import DataError
from tests.gmx_retired import current_json
from tests.retained_fixtures import retained


@pytest.fixture(params=["arbitrum-safe", "arbitrum-eoa", "avalanche-safe", "avalanche-eoa"])
def completed(request):
    path = retained("2026-09-09-gmx-exchange") / (
        request.param + "-authority-runtime/checkpoint.json.gz"
    )
    return Checkpoint.model_validate_json(current_json(gzip.decompress(path.read_bytes())))


def admitted(completed, index):
    prior = completed.outcomes[index - 1]
    return completed.model_copy(
        update={
            "decision": prior.decision,
            "outcomes": completed.outcomes[:index],
            "owned": (prior.after.owned,),
            "requests": (prior.request,),
        }
    )


@pytest.mark.parametrize("index", [2, 5])
def test_native_swap_keeper_replay_preserves_exact_effects(monkeypatch, completed, index):
    expected = completed.outcomes[index]
    state = admitted(completed, index)
    monkeypatch.setattr(
        reconcile, "snapshot", lambda *args: expected.after.model_copy(update={"owned": args[-1]})
    )
    for _ in range(2):
        actual = reconcile.reconcile_request(
            SimpleNamespace(), state, state.requests[0], expected.request.events[-1]
        )
        assert actual == expected
    assert not hasattr(actual, "gas_paid")
    assert actual.request.status == "executed"
    orphan = state.model_copy(update={"outcomes": ()})
    with pytest.raises(ValueError, match="preceding request outcome"):
        reconcile.reconcile_request(
            SimpleNamespace(), orphan, state.requests[0], expected.request.events[-1]
        )


def test_native_swap_keeper_rejects_foreign_shares_and_allowances(monkeypatch, completed):
    expected = completed.outcomes[2]
    state = admitted(completed, 2)
    wrong_shares = expected.after.market_tokens.model_copy(update={"amount": 1})
    bad_shares = expected.after.model_copy(update={"market_tokens": wrong_shares})
    allowance = expected.after.allowances[0]
    bad_allowances = expected.after.model_copy(
        update={
            "allowances": (
                allowance.model_copy(update={"amount": allowance.amount + 1}),
                *expected.after.allowances[1:],
            )
        }
    )
    for after, match in ((bad_shares, "unrelated GM shares"), (bad_allowances, "token allowances")):
        monkeypatch.setattr(reconcile, "snapshot", lambda *args, after=after: after)
        with pytest.raises(DataError, match=match):
            reconcile.reconcile_request(
                SimpleNamespace(), state, state.requests[0], expected.request.events[-1]
            )
