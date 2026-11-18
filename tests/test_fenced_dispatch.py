"""Fenced requests hold the journal lock only to commit, never across slow external calls."""

import threading
import time
from uuid import uuid4

import pytest

from almanak.gateway.ethereum import DataError
from tests.test_gateway import gateway
from tests.test_runtime_recovery import pending_for

served = gateway


class Gate:
    """A fake external call that blocks until the test releases it."""

    def __init__(self):
        self.entered, self.released = threading.Event(), threading.Event()

    def __call__(self, result):
        def call(*_):
            self.entered.set()
            assert self.released.wait(10)
            return result

        return call


def background(function):
    outcome = {}

    def run():
        try:
            outcome["result"] = function()
        except Exception as exc:  # noqa: BLE001 - asserted by the test
            outcome["error"] = exc

    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome


def lease(served, coordinator):
    state = served.client.load()
    served.client.request("acquire", coordinator=coordinator, deployment_id=state.deployment_id)
    return {"coordinator": coordinator, "deployment_id": state.deployment_id}


def expires(served):
    with served.service.journal.connect() as db:
        return db.execute("SELECT expires FROM coordinator").fetchone()[0]


def supersede(served):
    # Expiry models a dead coordinator; the new one acquires while the old request runs.
    with served.service.journal.connect() as db:
        db.execute("UPDATE coordinator SET expires=?", (time.time() - 1,))
    return lease(served, uuid4().hex)


def renew_during(gate, served, fields):
    assert gate.entered.wait(5)
    before = expires(served)
    started = time.monotonic()
    assert served.client.request("renew", **fields) is None
    # Waiting for the lock would take SQLite's 5s busy timeout and then fail with 503.
    assert time.monotonic() - started < 1
    assert expires(served) > before


def test_slow_signer_does_not_starve_lease_renewal(served):
    gate = Gate()
    served.service.backend.sign = gate({"signed": "slow"})
    fields = lease(served, uuid4().hex)
    signing, outcome = background(lambda: served.client.request("sign", **fields))
    try:
        renew_during(gate, served, fields)
    finally:
        gate.released.set()
        signing.join(10)
    assert outcome == {"result": {"signed": "slow"}}


def test_coordinator_superseded_while_signing_receives_no_signature(served):
    gate = Gate()
    served.service.backend.sign = gate({"signed": "stale"})
    fields = lease(served, uuid4().hex)
    signing, outcome = background(lambda: served.client.request("sign", **fields))
    try:
        assert gate.entered.wait(5)
        supersede(served)
    finally:
        gate.released.set()
        signing.join(10)
    assert "result" not in outcome
    assert outcome["error"].kind == "authority"
    assert "superseded" in str(outcome["error"])


@pytest.fixture
def pending(served, recorded):
    outcome = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    signed = pending_for(recorded.config, outcome)
    served.service.journal.update(lambda state: state.model_copy(update={"pending": signed}))
    return signed


@pytest.mark.parametrize("superseded", [False, True])
def test_broadcast_checks_run_unlocked_and_only_a_current_lease_sends(served, pending, superseded):
    gate, sent = Gate(), []
    backend = served.service.backend
    backend.prepare_submission = gate(None)
    backend.submit = lambda state: (sent.append(state.pending), (state, True))[1]
    fields = lease(served, uuid4().hex)
    broadcasting, outcome = background(lambda: served.client.request("broadcast", **fields))
    try:
        renew_during(gate, served, fields)
        if superseded:
            supersede(served)
    finally:
        gate.released.set()
        broadcasting.join(10)
    if superseded:
        assert outcome["error"].kind == "authority"
        assert sent == []
    else:
        assert outcome == {"result": True}
        assert sent == [pending]
    assert served.service.journal.load().pending == pending


def test_broadcast_refuses_a_pending_transaction_changed_during_its_checks(served, pending):
    gate, sent = Gate(), []
    backend = served.service.backend
    backend.prepare_submission = gate(None)
    backend.submit = lambda state: (sent.append(state.pending), (state, True))[1]
    fields = lease(served, uuid4().hex)
    broadcasting, outcome = background(lambda: served.client.request("broadcast", **fields))
    changed = pending.model_copy(update={"nonce": pending.nonce + 1})
    try:
        assert gate.entered.wait(5)
        served.service.journal.update(lambda state: state.model_copy(update={"pending": changed}))
    finally:
        gate.released.set()
        broadcasting.join(10)
    assert outcome["error"].kind == "conflict"
    assert sent == []


def test_teardown_during_slow_update_validation_is_preserved(served, monkeypatch):
    gate = Gate()
    monkeypatch.setattr(served.service, "validate_update", gate(None))
    fields = lease(served, uuid4().hex)
    before = served.client.load()
    recording, outcome = background(
        lambda: served.client.request("update", **fields, before=before, after=before)
    )
    try:
        assert gate.entered.wait(5)
        started = time.monotonic()
        assert served.client.teardown().mode == "teardown"
        assert time.monotonic() - started < 1
    finally:
        gate.released.set()
        recording.join(10)
    assert isinstance(outcome["error"], DataError)
    assert outcome["error"].kind == "conflict"
    assert served.service.journal.load().mode == "teardown"


@pytest.mark.parametrize("method", ["family", "sign"])
def test_invalidated_history_is_retained_by_locked_and_unlocked_requests(served, method):
    def invalidated(*_):
        raise DataError("reorganization", "recorded execution block is no longer canonical")

    served.service.backend.sign = invalidated
    served.service.backend.advance_family = invalidated
    fields = lease(served, uuid4().hex)
    family = {"family_id": uuid4().hex, "candidate_index": 0, "action": "inspect"}
    with pytest.raises(DataError, match="no longer canonical"):
        served.client.request(method, **fields, **(family if method == "family" else {}))
    assert "no longer canonical" in served.service.journal.load().integrity_error
