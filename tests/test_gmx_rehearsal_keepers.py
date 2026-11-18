"""GMX keepers settle requests on the identified fork for Safe lifecycles and EOA rehearsals."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.models import Block, Chain
from tests import gmx_fork
from tests.strategy_fork import test_production_rehearsals as rehearsal

ARBITRUM = ChainId("eip155:42161")
INSTANCE = "0x" + "72" * 32
ORIGIN = Chain(chain_id=ARBITRUM, environment="mainnet")
FORK = Chain(chain_id=ARBITRUM, environment="anvil:" + INSTANCE)


class Node:
    """An identified fork: validated origin, Anvil instance and the chain ID it signs as."""

    def __init__(self, signing_chain):
        self.origin = Block(chain=ORIGIN, number=1, hash="0x" + "11" * 32, timestamp=1)
        self.metadata = {"instanceId": INSTANCE}
        self.rpc = Rpc(signing_chain)
        self.validated = 0

    def _validate_origin(self):
        self.validated += 1


class Rpc:
    def __init__(self, signing_chain):
        self.signing_chain = signing_chain
        self.calls = []

    def __call__(self, method, params=()):
        self.calls.append((method, list(params)))


def pending(monkeypatch, chain):
    request = SimpleNamespace(unresolved=True, fund=SimpleNamespace(chain=chain))
    state = SimpleNamespace(requests=(request,))
    waits = []

    def wait(journal, predicate, running, *, timeout):
        waits.append(timeout)
        assert predicate(state)
        return state

    monkeypatch.setattr(gmx_fork, "wait_state", wait)
    return state, request, waits


def test_fork_bound_request_settles_on_its_own_anvil_instance(monkeypatch):
    node = Node(ChainId("eip155:31337"))
    state, request, waits = pending(monkeypatch, FORK)
    assert gmx_fork.pending_request(node, None, None, 10) == (state, request)
    assert node.validated == 1 and waits == [180]
    assert node.rpc.calls == [("anvil_setBlockTimestampInterval", [10])]


def test_mainnet_bound_rehearsal_request_settles_only_while_the_fork_signs_as_origin(
    monkeypatch,
):
    rehearsal_node = Node(ARBITRUM)
    state, request, waits = pending(monkeypatch, ORIGIN)
    assert gmx_fork.pending_request(rehearsal_node, None, None, 0, 900) == (state, request)
    assert waits == [900]
    assert gmx_fork.identified_fork_chains(rehearsal_node) == (FORK, ORIGIN)
    # A fork that still signs as the local Anvil chain is not the rehearsal's
    # mainnet identity, so its mainnet-bound request is refused.
    pending(monkeypatch, ORIGIN)
    with pytest.raises(AssertionError):
        gmx_fork.pending_request(Node(ChainId("eip155:31337")), None, None)


@pytest.mark.parametrize(
    "chain",
    (
        Chain(chain_id=ARBITRUM, environment="anvil:" + "0x" + "99" * 32),
        Chain(chain_id=ChainId("eip155:43114"), environment="mainnet"),
        Chain(chain_id=ChainId("eip155:43114"), environment="anvil:" + INSTANCE),
    ),
    ids=("other_instance", "other_mainnet_chain", "other_chain_same_instance"),
)
def test_request_outside_the_identified_fork_is_refused(monkeypatch, chain):
    pending(monkeypatch, chain)
    with pytest.raises(AssertionError):
        gmx_fork.pending_request(Node(ARBITRUM), None, None)


def test_driver_threads_the_request_timeout(monkeypatch):
    seen = []

    def stop(node, journal, running, block_interval, timeout):
        seen.append((block_interval, timeout))
        raise LookupError

    monkeypatch.setattr(gmx_fork, "pending_request", stop)
    with pytest.raises(LookupError):
        gmx_fork.drive_external(None, None, None, None, block_interval=0, timeout=900)
    with pytest.raises(LookupError):
        gmx_fork.drive_external(None, None, None, None)
    assert seen == [(0, 900), (1, 180)]


def test_rehearsal_drives_keepers_at_entry_and_after_teardown(monkeypatch, tmp_path):
    events: list[object] = []
    node = object()
    closed = SimpleNamespace(mode="closed")
    assertions = SimpleNamespace(
        entered=lambda state: True,
        assert_closed=lambda n, state: events.append(("closed", n, state)),
    )

    class Process:
        def __init__(self, name):
            self.name = name

        def send_signal(self, signal):
            events.append(("signal", self.name))

        def wait(self, timeout):
            return 0

    @contextmanager
    def fork_set(author, env_file):
        yield SimpleNamespace(nodes={ORIGIN: node})

    @contextmanager
    def processes(temporary):
        def launch(arguments, ready=None):
            events.append(("launch", ready or arguments[1]))
            return Process(ready or arguments[1])

        yield launch

    def wait_state(journal, predicate, running, timeout):
        state = closed if events[-1] == ("external", "teardown") else SimpleNamespace(mode="hold")
        assert predicate(state)
        events.append(("wait", state.mode))
        return state

    def drive(given, fork, journal, running):
        assert (given, fork) == (assertions, node)
        events.append(("external", "teardown" if "teardown" in events else "entry"))

    monkeypatch.setattr(rehearsal, "load_assertions", lambda case: assertions)
    monkeypatch.setattr(rehearsal, "ForkSet", fork_set)
    monkeypatch.setattr(rehearsal, "processes", processes)
    monkeypatch.setattr(rehearsal, "prepare", lambda nodes, author, directory: None)
    monkeypatch.setattr(
        rehearsal,
        "launch_gateway",
        lambda directory, launch: (Process("gateway"), ["gateway"], ["--control"]),
    )
    monkeypatch.setattr(rehearsal, "Journal", lambda path: path)
    monkeypatch.setattr(rehearsal, "wait_state", wait_state)
    monkeypatch.setattr(rehearsal, "drive_external", drive)
    monkeypatch.setattr(rehearsal, "verify_hold", lambda *args: events.append("hold"))
    monkeypatch.setattr(
        rehearsal, "stop_runtime", lambda process, timeout=20: events.append("stop")
    )
    monkeypatch.setattr(rehearsal, "resume", lambda *args: Process("resumed"))
    monkeypatch.setattr(rehearsal, "request_teardown", lambda control: events.append("teardown"))
    monkeypatch.setattr(
        rehearsal, "assert_production_payers", lambda state: events.append("payers")
    )
    phases: list[str] = []
    case = SimpleNamespace(strategy=tmp_path / "strategy.py")
    rehearsal.exercise(case, object(), tmp_path / ".env", tmp_path, phases)

    runtime_started = events.index(("launch", "service"))
    assert events[runtime_started + 1 : runtime_started + 3] == [
        ("external", "entry"),
        ("wait", "hold"),
    ]
    teardown = events.index("teardown")
    assert events[teardown + 1 : teardown + 3] == [("external", "teardown"), ("wait", "closed")]
    assert [e for e in events if isinstance(e, tuple) and e[0] == "external"] == [
        ("external", "entry"),
        ("external", "teardown"),
    ]
    assert events[-2:] == [("closed", node, closed), "payers"]
    assert phases == [
        "production_eoa_entered",
        "runtime_resumed",
        "gateway_replaced",
        "canonical_receipts_and_zero_obligations",
    ]


FUND = "0x" + "93" * 20
KEEPER = "0x" + "dd" * 20
NATIVE = "eip155:42161/slip44:60"
RETAINED = object()


class Submitted:
    """A submitted transaction outcome: it carries the submitter's gas charge."""

    model_fields = {"gas_paid": None}

    def __init__(self, tx_hash, *, asset=NATIVE, profile="eoa"):
        self.tx_hash = tx_hash
        self.gas_paid = SimpleNamespace(asset=asset)
        self.operation = SimpleNamespace(account="fund")
        self.execution = SimpleNamespace(
            binding=SimpleNamespace(profile=SimpleNamespace(kind=profile))
        )


class Settled:
    """A keeper settlement: no submitter charge, the keeper's own transaction."""

    model_fields: dict[str, None] = {}

    def __init__(self, tx_hash, *, sender=KEEPER, request=RETAINED):
        self.tx_hash = tx_hash
        self.receipt = {"from": sender}
        self.request = request


def closed(*outcomes):
    policy = SimpleNamespace(asset=NATIVE)
    binding = SimpleNamespace(profile=SimpleNamespace(fund=SimpleNamespace(address=FUND)))
    config = SimpleNamespace(
        production=SimpleNamespace(policy=lambda account: policy),
        accounts=SimpleNamespace(bindings=(binding,)),
    )
    return SimpleNamespace(config=config, outcomes=outcomes, requests=(RETAINED,))


def test_production_payers_accept_keeper_settlements_of_retained_requests():
    rehearsal.assert_production_payers(closed(Submitted("0x01"), Settled("0x02")))


@pytest.mark.parametrize(
    "outcomes",
    (
        (Settled("0x02"),),
        (Submitted("0x01", asset="eip155:42161/erc20:" + "0" * 40), Settled("0x02")),
        (Submitted("0x01", profile="safe_roles"), Settled("0x02")),
        (Submitted("0x01"), Settled("0x02", sender=FUND.upper().replace("0X", "0x"))),
        (Submitted("0x01"), Settled("0x02", request=None)),
        (Submitted("0x01"), Settled("0x02", request=object())),
        (Submitted("0x01"), Settled("0x01")),
    ),
    ids=(
        "no_submitted_transaction",
        "payer_asset_outside_policy",
        "non_eoa_submitter",
        "bound_account_sent_settlement",
        "settlement_without_request",
        "settlement_of_unretained_request",
        "settlement_reuses_submitted_hash",
    ),
)
def test_production_payers_refuse_unattributed_charges(outcomes):
    with pytest.raises(AssertionError):
        rehearsal.assert_production_payers(closed(*outcomes))


def test_keeper_count_reads_only_gmx_request_outcomes_in_a_composed_state():
    lending = SimpleNamespace(decision=None)
    outcomes = (
        lending,
        SimpleNamespace(kind="gmx_transaction_outcome"),
        SimpleNamespace(kind="gmx_request_outcome"),
        lending,
        SimpleNamespace(kind="gmx_request_outcome"),
    )
    assert gmx_fork.keeper_count(SimpleNamespace(outcomes=outcomes)) == 2


EMITTER = "0x" + "ee" * 20
KEY = "0x" + "4b" * 32


def keeper_fixture(monkeypatch, *, status="0x1", key=KEY, market="gm"):
    """A pending order and a keeper receipt, with no retained evidence needed."""
    market_view = SimpleNamespace(
        share_asset="gm", contracts=SimpleNamespace(event_emitter=EMITTER)
    )
    target = SimpleNamespace(fund=FORK, market=market_view)
    request = SimpleNamespace(allocation="trade", fund=FORK, market=market, kind="order", key=KEY)
    state = SimpleNamespace(config=SimpleNamespace(target=lambda allocation: target))
    receipt = {"status": status, "logs": ["unrelated", "created", "executed"]}
    events = {
        "unrelated": None,
        "created": SimpleNamespace(name="OrderCreated", topics=(key,)),
        "executed": SimpleNamespace(name="OrderExecuted", topics=(key,)),
    }
    calls = []
    monkeypatch.setattr(gmx_fork, "pending_request", lambda *args: (state, request))
    monkeypatch.setattr(
        gmx_fork,
        "execute_keeper",
        lambda node, given, pending, source: (
            calls.append(("execute", given, pending, source)) or receipt
        ),
    )
    monkeypatch.setattr(
        gmx_fork,
        "retain_keeper",
        lambda node, journal, pending, source, given, *, block_interval: calls.append(
            ("retain", given, block_interval)
        ),
    )

    def decode(log, *, emitter):
        assert emitter == EMITTER
        return events[log]

    monkeypatch.setattr(gmx_fork, "decode_event", decode)
    return target, request, receipt, calls


def test_driver_executes_retains_and_matches_the_executed_event(monkeypatch):
    target, request, receipt, calls = keeper_fixture(monkeypatch)
    source = object()
    gmx_fork.drive_external(None, None, None, lambda pending: source, block_interval=0)
    assert calls == [("execute", target, request, source), ("retain", receipt, 0)]


@pytest.mark.parametrize(
    ("change", "error"),
    (
        ({"market": "other"}, AssertionError),
        ({"status": "0x0"}, AssertionError),
        ({"key": "0x" + "00" * 32}, AssertionError),
    ),
    ids=("target_market_mismatch", "reverted_keeper", "executed_event_for_another_key"),
)
def test_driver_refuses_a_mismatched_or_failed_settlement(monkeypatch, change, error):
    keeper_fixture(monkeypatch, **change)
    with pytest.raises(error):
        gmx_fork.drive_external(None, None, None, lambda pending: None)
