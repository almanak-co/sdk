"""Canonical lifecycle replay separates keeper settlement from submitter and request history."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import lifecycle, reconciliation
from almanak.connectors.gmx_v2.request_binding import active, merge
from almanak.gateway.ethereum import DataError
from tests.gmx_evidence import accepted, checkpoint, holding


def test_keeper_reconciliation_replays_exact_recorded_effects_without_double_accounting(
    monkeypatch,
):
    state = accepted()
    expected = checkpoint().outcomes[2]
    request = state.requests[0]
    calls = []

    def snapshot(backend, name, block, owned):
        calls.append((name, block, owned))
        assert block == expected.after.block
        return expected.after.model_copy(update={"owned": owned})

    monkeypatch.setattr(reconciliation, "snapshot", snapshot)
    backend = SimpleNamespace()
    for _ in range(2):
        actual = reconciliation.reconcile_request(
            backend, state, request, expected.request.events[-1]
        )
        assert actual == expected
        assert lifecycle.ownership(state, actual)["owned"] == (expected.after.owned,)
    assert calls[0] == calls[1]
    assert actual.assets[0].amount == 0
    assert not hasattr(actual, "gas_paid")
    orphan = state.model_copy(update={"outcomes": ()})
    with pytest.raises(ValueError, match="preceding request outcome"):
        reconciliation.reconcile_request(backend, orphan, request, expected.request.events[-1])


def test_request_scope_merge_cannot_erase_history_or_reassign_owned_request():
    before, after = accepted(), holding()
    chain = before.config.allocations[0].chain
    assert active(before) == before.requests[0]
    assert merge(before, after, chain) == {"requests": after.requests}
    assert merge(after, after, chain) == {"requests": after.requests}
    with pytest.raises(ValueError, match="erase or reorder"):
        merge(before, before.model_copy(update={"requests": ()}), chain)
    with pytest.raises(ValueError, match="another execution chain"):
        merge(before, after, chain.model_copy(update={"environment": "mainnet"}))
    with pytest.raises(ValueError, match="retained decision"):
        active(
            before.model_copy(
                update={"decision": before.decision.model_copy(update={"identity": "0" * 32})}
            )
        )
    with pytest.raises(ValueError, match="chain-local admitted"):
        active(after)


def test_request_commit_cannot_replace_receipts_and_closure_preserves_obligations():
    before, after = accepted(), holding()
    settled = after.requests[0]
    assert lifecycle.replace_request((), before.requests[0]) == before.requests
    with pytest.raises(ValueError, match="acceptance evidence"):
        lifecycle.replace_request((), settled)
    with pytest.raises(ValueError, match="erase or replace"):
        lifecycle.replace_request(after.requests, before.requests[0])
    forged = settled.model_copy(update={"allocation": "foreign"})
    with pytest.raises(ValueError, match="immutable identity"):
        lifecycle.replace_request(after.requests, forged)
    closed = checkpoint()
    lifecycle.validate_closure(closed, closed.closure)
    with pytest.raises(ValueError, match="unresolved"):
        lifecycle.validate_closure(
            closed.model_copy(update={"requests": before.requests}), closed.closure
        )
    with pytest.raises(ValueError, match="claimable funding"):
        observed = closed.closure[0]
        claim = observed.claimable_funding[0].model_copy(update={"amount": 1})
        lifecycle.validate_closure(
            closed, (observed.model_copy(update={"claimable_funding": (claim,)}),)
        )


def test_unexecuted_request_never_adopts_external_position_or_unexplained_wallets():
    before = accepted().outcomes[-1].after
    actual = checkpoint().outcomes[2].after
    with pytest.raises(DataError, match="unexecuted"):
        reconciliation.position_effect(accepted().requests[0], before, actual, {})
    state = holding()
    with pytest.raises(ValueError, match="observed position"):
        lifecycle.validate_owned(state, (before,))
    observed = actual.model_copy(update={"owned": state.owned[0]})
    poor = observed.wallets[0].model_copy(update={"amount": 0})
    with pytest.raises(ValueError, match="covers|cover"):
        lifecycle.validate_owned(
            state, (observed.model_copy(update={"wallets": (poor, *observed.wallets[1:])}),)
        )
