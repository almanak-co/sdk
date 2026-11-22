"""A send that outlasts the coordinator lease must not cost the coordinator its lease.

A broadcast sends while the gateway holds the journal's write lock, so no renewal can be
written until the send returns. These cases run the direct action's own path (the
in-process gateway client, the gateway service, the journal and the operator runtime)
over the captured Aave V4 lifecycle. Only the adapter is a double: its `submit`, the call
made under the lock, takes longer than the lease. The lease and the journal's lock wait
are shortened so the cases stay fast; nothing is signed and no network is used.
"""

import sqlite3
import threading
import time

import pytest

from almanak import ax_actions
from almanak.gateway.ethereum import DataError
from almanak.gateway.service import GatewayService
from almanak.operator_runtime import OperatorRuntime
from almanak.state import Journal
from tests.test_operator_runtime import Backend, Desk

# Short, but long enough that a loaded runner does not lose the lease before the send.
LEASE = 3
# Longer than the lease: every renewal due while the send runs finds the lock held.
HOLD = 4.0


@pytest.fixture(autouse=True)
def short_waits(tmp_path, monkeypatch):
    """Fund claims stay out of the operator's own state, and a locked journal answers
    BUSY well inside the shortened lease, as its 5 s wait does inside a 30 s lease."""
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "home/claims")
    monkeypatch.setattr("almanak.state.BUSY_TIMEOUT_MS", 100)
    monkeypatch.setattr(OperatorRuntime, "poll", 0.05)


class SlowSend(Backend):
    """The captured adapter; its first supply send is slow, and may then fail."""

    def __init__(self, journal, recorded, *, fail):
        super().__init__(journal, recorded)
        self.fail = fail
        self.slowed = False
        self.submissions = []

    def audit_history(self, state):
        pass

    def authorize_submission(self, state):
        pass

    def prepare_submission(self, state):
        pass

    def observe(self, state=None):
        return super().observe()

    def sign(self, state=None):
        return super().sign()

    def submit(self, state):
        pending = state.pending
        self.submissions.append(pending.operation.kind)
        if pending.operation.kind == "supply" and not self.slowed:
            self.slowed = True
            time.sleep(HOLD)
            if self.fail:
                raise DataError(
                    "transport", "eth_getTransactionCount: transport failed", transient=True
                )
        return state, self.broadcast(pending)


class Watched(ax_actions.LocalGateway):
    """The direct action's client, recording how each renewal ended."""

    def __init__(self, service):
        super().__init__(service)
        self.renewals = []

    def request(self, method, **fields):
        if method != "renew":
            return super().request(method, **fields)
        try:
            result = super().request(method, **fields)
        except Exception as exc:
            self.renewals.append(getattr(exc, "kind", type(exc).__name__))
            raise
        self.renewals.append("renewed")
        return result


def lease_row(journal):
    with journal.connect() as connection:
        return connection.execute("SELECT identity, expires FROM coordinator WHERE id=1").fetchone()


def supply(tmp_path, recorded, *, fail):
    backend = SlowSend(Journal(tmp_path / "state.sqlite"), recorded, fail=fail)
    service = GatewayService(
        recorded.config,
        tmp_path / "state.sqlite",
        backend,
        lease_seconds=LEASE,
        operator_actions=True,
    )
    client = Watched(service)
    runtime = OperatorRuntime(gateway=client, create=recorded.config)
    intent = next(o.decision.intent for o in recorded.outcomes if o.operation.kind == "supply")
    try:
        runtime.recover()
        runtime.submit(intent)
        runtime.execute(lambda: None)
        state = runtime.journal.load()
        identity, expires = lease_row(service.journal)
        return backend, client, state, (identity == client.coordinator, expires - time.time())
    finally:
        runtime.close()


@pytest.mark.parametrize("fail", [False, True], ids=["sent", "transport_failure"])
def test_a_send_slower_than_the_lease_keeps_the_lease_and_completes(tmp_path, recorded, fail):
    backend, client, state, (held, remaining) = supply(tmp_path, recorded, fail=fail)
    # The fault was exercised: renewals met the locked journal for longer than the lease.
    assert client.renewals.count("gateway_unavailable") >= 2
    assert client.renewals[-1] == "renewed"
    # The heartbeat outlived it, and the journal agrees the lease is this coordinator's.
    assert client.lease_error is None
    assert held and remaining > 0
    assert [item.operation.kind for item in state.outcomes] == ["approve", "supply"]
    assert state.pending is None
    # Each signed step reached the provider exactly once. A failed send is asked again
    # with the same recorded bytes, as before; nothing is planned or signed twice.
    assert len(backend.broadcasts) == len(set(backend.broadcasts)) == 2
    assert backend.submissions.count("supply") == (2 if fail else 1)
    assert backend.submissions.count("approve") == 1


def test_a_signed_step_is_in_the_journal_before_its_bytes_leave(tmp_path, recorded):
    """The record that precedes a send is untouched by the lease change."""
    seen = []

    class Recording(SlowSend):
        def submit(self, state):
            stored = self.journal.load().pending
            seen.append(stored == state.pending and bool(stored.raw_transaction))
            return super().submit(state)

    backend = Recording(Journal(tmp_path / "state.sqlite"), recorded, fail=False)
    backend.slowed = True
    service = GatewayService(
        recorded.config, backend.journal.path, backend, lease_seconds=LEASE, operator_actions=True
    )
    runtime = OperatorRuntime(gateway=ax_actions.LocalGateway(service), create=recorded.config)
    try:
        runtime.recover()
        runtime.submit(
            next(o.decision.intent for o in recorded.outcomes if o.operation.kind == "supply")
        )
        runtime.execute(lambda: None)
    finally:
        runtime.close()
    # Whenever the adapter was asked to send, the journal already held those signed bytes.
    assert len(seen) == 2 and all(seen)
    assert len(backend.broadcasts) == len(set(backend.broadcasts)) == 2


def test_a_fenced_request_that_raises_keeps_its_lease_and_loses_its_writes(tmp_path, recorded):
    desk = Desk(tmp_path, recorded)
    journal, before = desk.store, desk.store.body()
    journal.acquire("first", 0.3)
    with (
        pytest.raises(DataError, match="provider failed"),
        journal.authorized("first", 5) as (
            db,
            _,
        ),
    ):
        db.execute("UPDATE checkpoint SET body='{}' WHERE id=1")
        # Longer than the lease the request was admitted under.
        time.sleep(0.5)
        raise DataError("transport", "provider failed")
    identity, expires = lease_row(journal)
    assert identity == "first" and expires > time.time() + 4
    assert journal.body() == before
    assert journal.read_fenced("first") == before


def test_an_expired_or_superseded_lease_is_still_refused_at_the_lock(tmp_path, recorded):
    journal = Desk(tmp_path, recorded).store
    journal.acquire("first", 0.05)
    time.sleep(0.1)
    # Time under the lock does not count; time before it does.
    with pytest.raises(RuntimeError, match="absent, expired or superseded"):
        journal.renew("first", 5)
    assert lease_row(journal)[1] < time.time()
    journal.acquire("second", 5)
    with (
        pytest.raises(RuntimeError, match="absent, expired or superseded"),
        journal.authorized("first", 5),
    ):
        raise AssertionError("a superseded coordinator entered the fenced commit")
    assert lease_row(journal)[0] == "second"


def test_another_coordinator_is_not_admitted_while_a_send_holds_the_lock(tmp_path, recorded):
    journal = Desk(tmp_path, recorded).store
    journal.acquire("first", 0.2)
    entered, leave = threading.Event(), threading.Event()

    def send():
        with journal.authorized("first", 5):
            entered.set()
            leave.wait(5)

    thread = threading.Thread(target=send)
    thread.start()
    try:
        assert entered.wait(5)
        time.sleep(0.3)
        # The first lease's time has run out, but its holder still has the lock.
        with pytest.raises(sqlite3.OperationalError):
            journal.acquire("second", 5)
    finally:
        leave.set()
        thread.join()
    with pytest.raises(RuntimeError, match="another coordinator holds"):
        journal.acquire("second", 5)


def test_a_lost_lease_names_the_renewal_failure_that_lost_it(tmp_path, recorded):
    desk = Desk(tmp_path, recorded)
    client = ax_actions.LocalGateway(desk.service)
    client.acquire()

    def unavailable(method, request):
        raise sqlite3.OperationalError("database is locked")

    desk.service.dispatch = unavailable
    try:
        # The coordinator keeps asking, but a gateway that serves nothing answers nothing:
        # the renewal gives up once the lease has run out unrenewed, as it always did.
        deadline = time.monotonic() + 3 * LEASE
        with pytest.raises(RuntimeError) as refused:
            while time.monotonic() < deadline:
                with pytest.raises(DataError, match="gateway storage unavailable"):
                    client.scoped("observe")
                time.sleep(0.02)
    finally:
        del desk.service.dispatch
        client.close()
    message = str(refused.value)
    assert "heartbeat failed" in message and "expired" in message
    assert "DataError: gateway storage unavailable" in message
    assert "almanak ax recover" in message and "resume to recover" not in message
    assert refused.value.__cause__ is client.lease_error
    assert client.lease_error.kind == "gateway_unavailable"
