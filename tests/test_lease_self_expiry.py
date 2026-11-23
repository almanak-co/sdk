"""A coordinator that outlives its own lease keeps it until another one is admitted.

A process stall longer than the lease (a suspended machine, a frozen VM, a process that
is not scheduled) used to end the coordinator on wake-up, although the lease row still
named it and no successor had been admitted. The stall is simulated by an offset on the
clocks the journal and the gateway client read; the machine clock is not touched and no
test sleeps for a lease.

Component evidence against captured Aave V4 receipts, without signing or chain access:
the real journal, gateway service, in-process client with its renewal thread, operator
runtime and direct-action record. Only the chain adapter is a double. It is not fork or
process recovery evidence.
"""

import time as real_time

import pytest

from almanak import ax_actions
from almanak import state as durable
from almanak.gateway import client as gateway_client
from almanak.operator_runtime import OperatorRuntime
from almanak.state import Journal
from tests.test_operator_runtime import Backend, Desk

# A direct action's lease is 30 s; the live stalls were 90 to 176 s.
LONG = 150.0
SHORT = 20.0


class Clock:
    """The `time` module as one SDK module sees it, plus the simulated stall."""

    def __init__(self):
        self.wall = 0.0
        self.monotonic_offset = 0.0

    def __getattr__(self, name):
        return getattr(real_time, name)

    def time(self):
        return real_time.time() + self.wall

    def monotonic(self):
        return real_time.monotonic() + self.monotonic_offset

    def stall(self, seconds, clocks="wall"):
        """A suspended machine advances only the wall clock; a process that is not
        scheduled sees both clocks advance."""
        self.wall += seconds
        if clocks == "both":
            self.monotonic_offset += seconds


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    # The lease row is written and admitted on the wall clock; the renewal thread paces
    # itself on the monotonic clock.
    monkeypatch.setattr(durable, "time", clock)
    monkeypatch.setattr(gateway_client, "time", clock)
    return clock


@pytest.fixture(autouse=True)
def state_home(tmp_path, monkeypatch):
    """Records and fund claims of these tests stay out of the operator's own state."""
    claims = tmp_path / "home/claims"
    monkeypatch.setattr("almanak.state.claims_root", lambda: claims)


@pytest.fixture
def store(tmp_path, recorded):
    return Desk(tmp_path, recorded).store


def lease_row(journal):
    with journal.connect() as connection:
        return connection.execute("SELECT identity, expires FROM coordinator").fetchone()


def commit(journal, identity):
    """One fenced commit that writes; returns whether the write was kept."""
    marker = '{"marker": true}'
    with journal.authorized(identity, 30) as (connection, body):
        connection.execute("UPDATE checkpoint SET body=?", (marker,))
    with journal.connect() as connection:
        kept = connection.execute("SELECT body FROM checkpoint").fetchone()[0] == marker
        connection.execute("UPDATE checkpoint SET body=?", (body,))
    return kept


# The journal's rule.


def test_the_holder_reads_commits_and_renews_past_its_own_expiry(store, clock):
    store.acquire("holder", 30)
    before = store.body()
    clock.stall(LONG)
    assert lease_row(store)[1] < clock.time()
    # The row still names the holder: no other coordinator was admitted meanwhile.
    assert store.read_fenced("holder") == before
    assert lease_row(store)[1] < clock.time()
    # The first commit takes the lease again for its full duration.
    assert commit(store, "holder")
    assert lease_row(store) == ("holder", pytest.approx(clock.time() + 30, abs=5))
    clock.stall(LONG)
    store.renew("holder", 30)
    assert lease_row(store) == ("holder", pytest.approx(clock.time() + 30, abs=5))


def test_expiry_still_admits_a_successor_and_only_then(store, clock):
    store.acquire("holder", 30)
    clock.stall(SHORT)
    with pytest.raises(RuntimeError, match="another coordinator holds"):
        store.acquire("successor", 30)
    clock.stall(LONG)
    store.acquire("successor", 30)
    assert lease_row(store)[0] == "successor"


def test_a_superseded_holder_is_refused_at_its_read_commit_and_renewal(store, clock):
    store.acquire("holder", 30)
    before = store.body()
    clock.stall(LONG)
    store.acquire("successor", 30)
    with pytest.raises(RuntimeError, match="superseded"):
        store.read_fenced("holder")
    with pytest.raises(RuntimeError, match="superseded"):
        with store.authorized("holder", 30):
            pytest.fail("a superseded coordinator reached its commit")
    with pytest.raises(RuntimeError, match="superseded"):
        store.renew("holder", 30)
    # The refusals changed neither the checkpoint nor the successor's lease.
    assert store.body() == before
    assert lease_row(store)[0] == "successor"
    assert commit(store, "successor")


def test_a_successor_that_crashed_still_fences_the_former_holder(store, clock):
    store.acquire("holder", 30)
    clock.stall(LONG)
    store.acquire("successor", 30)
    # The successor's own lease runs out. The row names it, so the first holder, whose
    # identity it replaced, stays out however long both have been expired.
    clock.stall(LONG)
    with pytest.raises(RuntimeError, match="superseded"):
        store.read_fenced("holder")
    with pytest.raises(RuntimeError, match="superseded"):
        store.renew("holder", 30)
    store.renew("successor", 30)


def test_a_takeover_between_the_holders_read_and_its_commit_refuses_the_commit(store, clock):
    store.acquire("holder", 30)
    clock.stall(LONG)
    before = store.read_fenced("holder")
    store.acquire("successor", 30)
    with pytest.raises(RuntimeError, match="superseded"):
        with store.authorized("holder", 30):
            pytest.fail("a superseded coordinator reached its commit")
    assert store.body() == before


def test_a_released_or_never_taken_lease_is_absent(store, clock):
    with pytest.raises(RuntimeError, match="lease is absent"):
        store.read_fenced("holder")
    store.acquire("holder", 30)
    clock.stall(LONG)
    store.acquire("successor", 30)
    store.release("successor")
    # The successor is gone, yet the first holder was replaced and is not admitted again
    # by a read or a commit; only a new admission could.
    for identity in ("holder", "successor"):
        with pytest.raises(RuntimeError, match="lease is absent"):
            store.read_fenced(identity)
        with pytest.raises(RuntimeError, match="lease is absent"):
            store.renew(identity, 30)


def test_a_refusal_names_its_cause(store, clock):
    with pytest.raises(RuntimeError, match="absent: it was released or never taken"):
        store.renew("holder", 30)
    store.acquire("successor", 30)
    with pytest.raises(RuntimeError, match="superseded: another coordinator took it"):
        store.renew("holder", 30)


# The renewal thread, through the client a direct action uses (2 s lease).


def renewed_after(gateway, clock, moment, budget=5.0):
    """Wait, in real time, for a renewal the gateway wrote after `moment`."""
    deadline = real_time.monotonic() + budget
    while real_time.monotonic() < deadline and gateway.lease_error is None:
        if lease_row(gateway.service.journal)[1] > moment:
            return True
        real_time.sleep(0.02)
    return False


@pytest.mark.parametrize("clocks", ["wall", "both"])
def test_the_renewal_asks_the_gateway_after_a_stall_and_keeps_the_lease(
    tmp_path, recorded, clock, clocks
):
    gateway = ax_actions.LocalGateway(Desk(tmp_path, recorded).service)
    gateway.acquire()
    try:
        clock.stall(LONG, clocks)
        assert renewed_after(gateway, clock, clock.time())
        assert gateway.lease_error is None
        gateway.scoped("renew")
    finally:
        gateway.close()


@pytest.mark.parametrize("clocks", ["wall", "both"])
def test_the_renewal_ends_when_another_coordinator_took_the_lease(
    tmp_path, recorded, clock, clocks
):
    gateway = ax_actions.LocalGateway(Desk(tmp_path, recorded).service)
    gateway.acquire()
    try:
        clock.stall(LONG, clocks)
        gateway.service.journal.acquire("successor", 2)
        deadline = real_time.monotonic() + 5
        while gateway.lease_error is None and real_time.monotonic() < deadline:
            real_time.sleep(0.02)
        assert "superseded" in str(gateway.lease_error)
        with pytest.raises(RuntimeError, match="heartbeat failed"):
            gateway.scoped("observe")
        assert lease_row(gateway.service.journal)[0] == "successor"
    finally:
        gateway.close()


# A direct action: record, in-process gateway (30 s lease), operator runtime.


class Adapter(Backend):
    """The captured lifecycle as the chain adapter a direct action's service drives."""

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
        return state, self.broadcast(state.pending)


class Direct:
    """One direct supply over the capture, with a stall at a chosen point."""

    def __init__(self, recorded, clock, monkeypatch):
        self.recorded, self.clock = recorded, clock
        self.directory, self.lock = ax_actions.new_record()
        self.database = self.directory / "state.sqlite"
        self.backend = Adapter(Journal(self.database), recorded)
        self.stalled = False
        self.client = None
        # The capture's allocation is not named as a direct command names its own.
        monkeypatch.setattr(ax_actions, "position", lambda state: None)
        monkeypatch.setattr(OperatorRuntime, "poll", 0)

    def mined(self):
        return len(Journal(self.database).load().outcomes)

    def stall(self, seconds, clocks, successor):
        self.stalled = True
        assert lease_row(Journal(self.database))[0] == self.client.coordinator
        self.clock.stall(seconds, clocks)
        if successor:
            Journal(self.database).acquire("successor", 30)

    def run(self, *, at, seconds, clocks="wall", execute=True, successor=False):
        """`at` is `preview`, before the request is admitted with nothing signed, or the
        number of steps mined and recorded when the process stalls."""
        supply = next(
            item.decision.intent
            for item in self.recorded.outcomes
            if item.operation.kind == "supply"
        )
        scoped = ax_actions.LocalGateway.scoped
        direct = self

        class Client(ax_actions.LocalGateway):
            def scoped(self, method, **fields):
                due = isinstance(at, int) and method not in ("acquire", "renew")
                if due and not direct.stalled and direct.mined() >= at:
                    direct.stall(seconds, clocks, successor)
                return scoped(self, method, **fields)

        def act(service, check):
            self.client = Client(service)
            runtime = OperatorRuntime(gateway=self.client, create=self.recorded.config)
            try:
                runtime.recover()
                if at == "preview":
                    self.stall(seconds, clocks, successor)
                runtime.submit(supply)
                first = runtime.preview().operation.kind
                if not execute:
                    return {"first_step": first}
                return ax_actions.executed(runtime, 0, check)
            finally:
                runtime.close()

        return ax_actions.in_record(
            self.directory, self.lock, self.recorded.config, self.backend, act
        )

    def sent(self):
        return len(self.backend.broadcasts)


@pytest.fixture
def direct(recorded, clock, monkeypatch):
    return Direct(recorded, clock, monkeypatch)


def completed(direct, result, stalled=True):
    assert direct.stalled == stalled
    assert [item["operation"] for item in result["outcomes"]] == ["approve", "supply"]
    assert all(item["success"] for item in result["outcomes"])
    assert result["executed"] and result["completed"]
    # Each signed step reached the provider once; nothing is left for `ax recover`.
    assert direct.sent() == 2
    assert "record_kept" not in result and "unresolved" not in result
    assert not direct.directory.exists() and ax_actions.kept_records() == []


@pytest.mark.parametrize("clocks", ["wall", "both"])
def test_a_preview_that_stalled_before_its_request_still_plans(direct, clocks):
    result = direct.run(at="preview", seconds=LONG, clocks=clocks, execute=False)
    assert direct.stalled and result == {"first_step": "approve"}
    assert direct.sent() == 0 and not direct.directory.exists()


@pytest.mark.parametrize("clocks", ["wall", "both"])
@pytest.mark.parametrize("at", ["preview", 0, 1, 2])
def test_an_execution_that_stalled_longer_than_the_lease_completes(direct, at, clocks):
    """`at=2` is the live case: every step mined and recorded, then the stall."""
    completed(direct, direct.run(at=at, seconds=LONG, clocks=clocks))


@pytest.mark.parametrize("at", ["preview", 1, 2])
def test_a_stall_shorter_than_the_lease_completes(direct, at):
    completed(direct, direct.run(at=at, seconds=SHORT))


def test_an_execution_without_a_stall_completes(direct):
    completed(direct, direct.run(at="never", seconds=0), stalled=False)


@pytest.mark.parametrize("at", ["preview", 1, 2])
def test_a_stalled_execution_whose_lease_another_coordinator_took_is_refused(direct, at):
    with pytest.raises(RuntimeError, match="superseded"):
        direct.run(at=at, seconds=LONG, successor=True)
    # Nothing was signed or sent after the takeover.
    assert direct.stalled and direct.sent() == (0 if at == "preview" else at)
