"""A send that outlasts the coordinator lease must not cost the coordinator its lease.

A broadcast sends while the gateway holds the journal's write lock, so no renewal can be
written until the send returns. These cases run the direct action's own path (the
in-process gateway client, the gateway service, the journal and the operator runtime)
over the captured Aave V4 lifecycle. Only the adapter is a double: its `submit`, the call
made under the lock, takes longer than the lease. The lease and the journal's lock wait
are shortened so the cases stay fast; nothing is signed and no network is used.
"""

import shlex
import sqlite3
import threading
import time
from types import SimpleNamespace

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
        # The command can end before the next renewal is due; give the heartbeat one.
        seen, patience = len(client.renewals), time.monotonic() + LEASE
        while "renewed" not in client.renewals[seen:] and time.monotonic() < patience:
            time.sleep(0.02)
        identity, expires = lease_row(service.journal)
        return backend, client, state, (identity == client.coordinator, expires - time.time())
    finally:
        runtime.close()


@pytest.mark.parametrize("fail", [False, True], ids=["sent", "transport_failure"])
def test_a_send_slower_than_the_lease_keeps_the_lease_and_completes(tmp_path, recorded, fail):
    backend, client, state, (held, remaining) = supply(tmp_path, recorded, fail=fail)
    # The fault was exercised: renewals met the locked journal for longer than the lease.
    assert client.renewals.count("gateway_unavailable") >= 2
    # A renewal was written after the send, and none failed after it.
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
    journal.acquire("first", 1)
    with (
        pytest.raises(DataError, match="provider failed"),
        journal.authorized("first", 5) as (
            db,
            _,
        ),
    ):
        db.execute("UPDATE checkpoint SET body='{}' WHERE id=1")
        # Longer than the lease the request was admitted under.
        time.sleep(1.2)
        raise DataError("transport", "provider failed")
    identity, expires = lease_row(journal)
    assert identity == "first" and expires > time.time() + 4
    assert journal.body() == before
    assert journal.read_fenced("first") == before


def test_storage_that_cannot_keep_the_lease_does_not_hide_the_request_failure(
    tmp_path, recorded, monkeypatch
):
    """The request's own failure surfaces, with nothing of the request committed. The
    lease is left as it was admitted, to run out by itself, and the lock is released."""
    journal = Desk(tmp_path, recorded).store
    before = journal.body()
    journal.acquire("first", 5)
    admitted = lease_row(journal)
    extensions = []

    def failing(connection, duration):
        extensions.append(duration)
        raise sqlite3.OperationalError("disk I/O error")

    with monkeypatch.context() as patch:
        patch.setattr("almanak.state.extend_lease", failing)
        with (
            pytest.raises(DataError, match="provider failed") as raised,
            journal.authorized("first", 60) as (
                db,
                _,
            ),
        ):
            db.execute("UPDATE checkpoint SET body='{}' WHERE id=1")
            raise DataError("transport", "provider failed")
    assert extensions == [60]
    assert raised.value.kind == "transport"
    assert lease_row(journal) == admitted
    assert journal.body() == before
    # Nothing is left open: the same coordinator writes again under its unchanged lease.
    journal.renew("first", 60)
    assert lease_row(journal)[1] > admitted[1] + 30


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
    journal.acquire("first", 1)
    entered, leave = threading.Event(), threading.Event()

    def send():
        with journal.authorized("first", 5):
            entered.set()
            leave.wait(5)

    thread = threading.Thread(target=send)
    thread.start()
    try:
        assert entered.wait(5)
        time.sleep(1.1)
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
    # The command that resolves a kept record, with this record's own path; without
    # `--record` it only lists them.
    assert f"`almanak ax recover --record {shlex.quote(str(desk.store.path.parent))}`" in message
    assert "resume to recover" not in message
    assert refused.value.__cause__ is client.lease_error
    assert client.lease_error.kind == "gateway_unavailable"


def test_the_named_record_is_quoted_so_the_command_pastes(tmp_path):
    record = tmp_path / "state dir"
    service = SimpleNamespace(journal=SimpleNamespace(path=record / "state.sqlite"))
    hint = ax_actions.LocalGateway(service).lease_recovery()
    assert f"`almanak ax recover --record '{record}'`" in hint


class Renewals(ax_actions.LocalGateway):
    """The direct action's client, with renewals the gateway cannot serve until healed."""

    def __init__(self, service):
        super().__init__(service)
        self.healed = threading.Event()
        self.renewed = threading.Event()
        self.failed = []

    def request(self, method, **fields):
        if method != "renew":
            return super().request(method, **fields)
        if not self.healed.is_set():
            self.failed.append(time.monotonic())
            raise DataError("gateway_unavailable", "synthetic storage failure")
        result = super().request(method, **fields)
        self.renewed.set()
        return result


def leased(tmp_path, recorded):
    desk = Desk(tmp_path, recorded)
    service = GatewayService(
        recorded.config, desk.store.path, desk.gateway, lease_seconds=LEASE, operator_actions=True
    )
    return service, Renewals(service)


def test_a_request_answered_while_the_renewal_waited_counts_at_its_next_failure(
    tmp_path, recorded, monkeypatch
):
    """A request that failed under the lock extended the lease in the journal, but is no
    confirmation the client can count from. Answered between two renewal attempts, it
    must still keep the renewal asking at the first failure past its own deadline."""
    service, client = leased(tmp_path, recorded)
    dispatch = service.dispatch

    def failing_send(method, request):
        if method != "audit":
            return dispatch(method, request)
        # A journal lease far longer than the client counts, so only the renewal's own
        # decision is timed here.
        with service.journal.authorized(request.coordinator, 60):
            raise DataError("transport", "provider failed")

    monkeypatch.setattr(service, "dispatch", failing_send)
    client.acquire()
    admitted = time.monotonic()
    # The renewal asks `progressed` only after it has renewed or when a failed attempt
    # finds the lease run out by its own count; nothing renews here, so a credit spent
    # is the renewal's failure past its deadline, wherever that fell on this clock. The
    # gateway heals there, in the renewal's own thread, before its next attempt; without
    # the credit that failure would have ended the renewal.
    spent = threading.Event()
    progressed = client.fenced.progressed

    def spending():
        result = progressed()
        if result:
            spent.set()
            client.healed.set()
        return result

    monkeypatch.setattr(client.fenced, "progressed", spending)
    try:
        # Inside the renewal's first wait.
        with pytest.raises(DataError, match="provider failed"):
            client.scoped("audit")
        assert not client.failed
        # The gateway stays unavailable until one renewal has failed past its deadline.
        while not spent.is_set():
            assert client.lease_error is None
            assert time.monotonic() < admitted + 3 * LEASE
            time.sleep(0.02)
        assert client.renewed.wait(10)
        assert client.lease_error is None
        identity, expires = lease_row(service.journal)
        assert identity == client.coordinator and expires > time.time()
    finally:
        client.close()


def test_a_send_slower_than_the_lease_confirms_the_lease_from_its_answer(
    tmp_path, recorded, monkeypatch
):
    """The lease runs from the release of the lock, so the renewal counts it from the
    answer: renewals that then fail for a while do not end a lease that still runs."""
    service, client = leased(tmp_path, recorded)
    dispatch = service.dispatch

    def slow_send(method, request):
        if method != "audit":
            return dispatch(method, request)
        with service.journal.authorized(request.coordinator, LEASE):
            time.sleep(HOLD)

    monkeypatch.setattr(service, "dispatch", slow_send)
    client.acquire()
    try:
        client.scoped("audit")
        answered = time.monotonic()
        # Renewals keep failing after the answer, past the deadline counted from the
        # start of the send, while the journal's lease still runs.
        while len([at for at in client.failed if at > answered]) < 2:
            assert client.lease_error is None
            assert time.monotonic() < answered + LEASE
            time.sleep(0.02)
        client.healed.set()
        assert client.renewed.wait(LEASE)
        assert client.lease_error is None
        identity, expires = lease_row(service.journal)
        assert identity == client.coordinator and expires > time.time()
    finally:
        client.close()


def test_a_storage_failure_while_a_signed_step_waits_is_held_and_asked_again(tmp_path, recorded):
    """The documented hold: a passing storage failure does not end a command whose signed
    step is unresolved, and the step is sent once the gateway serves again."""
    backend = SlowSend(Journal(tmp_path / "state.sqlite"), recorded, fail=False)
    backend.slowed = True
    # The direct action's own lease: this case is about the hold, not the lease.
    service = GatewayService(
        recorded.config, backend.journal.path, backend, lease_seconds=30, operator_actions=True
    )
    dispatch, refused = service.dispatch, []

    def storage_fails(method, request):
        pending = service.journal.load().pending if method == "broadcast" else None
        if pending is not None and pending.operation.kind == "supply" and len(refused) < 3:
            refused.append(method)
            raise sqlite3.OperationalError("database is locked")
        return dispatch(method, request)

    service.dispatch = storage_fails
    client = ax_actions.LocalGateway(service)
    runtime = OperatorRuntime(gateway=client, create=recorded.config)
    try:
        runtime.recover()
        runtime.submit(
            next(o.decision.intent for o in recorded.outcomes if o.operation.kind == "supply")
        )
        runtime.execute(lambda: None)
        state = runtime.journal.load()
    finally:
        runtime.close()
    assert refused == ["broadcast"] * 3
    assert client.lease_error is None
    assert [item.operation.kind for item in state.outcomes] == ["approve", "supply"]
    assert state.pending is None
    assert len(backend.broadcasts) == len(set(backend.broadcasts)) == 2
    assert backend.submissions.count("supply") == 1
