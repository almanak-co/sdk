"""The fork workspace's connector venue seam (`gateway/fork_venues.py`) and its supervised
host. The Hyperliquid venue itself is `tests/test_hyperliquid_fork_venue.py`."""

import json
from types import SimpleNamespace

import pytest

from almanak import supervisor_forks
from almanak.gateway import fork_venues
from almanak.gateway.ethereum import DataError
from almanak.supervisor_forks import SupervisedVenues, start_venues
from tests.test_hyperliquid_connector import FORK, MAINNET

ROUTE = {"chain": FORK.model_dump(mode="json"), "endpoint": "http://127.0.0.1:8545"}
VENUE = {"hypercore": {"model": "http://127.0.0.1:9", "agent_keys": "/keys.json"}}


def test_a_route_names_its_chain_endpoint_and_optional_venues():
    assert fork_venues.route_venues(ROUTE) == {}
    assert fork_venues.route_venues({**ROUTE, "venues": VENUE}) == VENUE
    assert fork_venues.chain_route({**ROUTE, "venues": VENUE}) == ROUTE
    for refused in ({**ROUTE, "extra": 1}, {"chain": ROUTE["chain"]}, [ROUTE]):
        with pytest.raises(ValueError, match="only their explicit chain and endpoint"):
            fork_venues.route_venues(refused)
    for refused in ({**ROUTE, "venues": {}}, {**ROUTE, "venues": [VENUE]}):
        with pytest.raises(ValueError, match="name each venue's descriptor"):
            fork_venues.route_venues(refused)


def test_routes_carry_each_chains_venues_and_nothing_else_changes():
    other = {**ROUTE, "chain": MAINNET.model_dump(mode="json")}
    assert fork_venues.routed([ROUTE, other], {FORK: VENUE}) == [
        {**ROUTE, "venues": VENUE},
        other,
    ]
    assert fork_venues.routed([{**ROUTE, "venues": VENUE}], {}) == [ROUTE]
    with pytest.raises(ValueError, match="no route for"):
        fork_venues.routed([other], {FORK: VENUE})


def test_the_adapter_is_the_connectors_own_only_when_it_runs_a_venue(monkeypatch, tmp_path):
    built = []
    monkeypatch.setattr(
        "almanak.gateway.fork_execution.ForkGateway",
        lambda *args, **fields: built.append(("evm", args, fields)) or "evm",
    )
    monkeypatch.setattr(fork_venues, "fork_binding", lambda config: SimpleNamespace())
    keys = tmp_path / "keys.json"
    assert fork_venues.fork_adapter("config", ROUTE["endpoint"], keys, chain=FORK) == "evm"
    assert built == [("evm", ("config", ROUTE["endpoint"], keys), {"chain": FORK})]
    with pytest.raises(ValueError, match="only a connector with a fork venue"):
        fork_venues.fork_adapter("config", ROUTE["endpoint"], keys, chain=FORK, venues=VENUE)
    assert fork_venues.start("config", None) == {}

    def adapter(config, endpoint, key_file, *, chain, venues):
        return ("venue", venues)

    binding = SimpleNamespace(fork_adapter=adapter, venues=lambda deployment, host: {FORK: VENUE})
    monkeypatch.setattr(fork_venues, "fork_binding", lambda config: binding)
    assert fork_venues.fork_adapter("config", "e", keys, venues=VENUE) == ("venue", VENUE)
    assert fork_venues.fork_adapter("config", "e", keys) == ("venue", {})
    assert fork_venues.start("config", None) == {FORK: VENUE}


def test_a_venue_process_runs_only_through_a_connector_that_serves_one(tmp_path):
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"connector": "aave_v4"}))
    with pytest.raises(ValueError, match="runs no fork venue"):
        fork_venues.serve(spec, None)


def test_a_venue_that_never_answers_fails_its_start(monkeypatch):
    monkeypatch.setattr(fork_venues, "READY_SECONDS", 0.2)
    with pytest.raises(RuntimeError, match="exited during start"):
        fork_venues.wait_ready("http://127.0.0.1:1", lambda: False)
    with pytest.raises(RuntimeError, match="did not answer"):
        fork_venues.wait_ready("http://127.0.0.1:1", lambda: True)


# The supervised host.


class Supervisor:
    def __init__(self, tmp_path, processes=None):
        self.root = tmp_path
        self.deployment = tmp_path / "private" / "deployment"
        self.state = {"processes": dict(processes or {})}

    def persist(self, **updates):
        self.state.update(updates)


@pytest.fixture
def spawned(monkeypatch):
    calls = []

    def spawn(root, name, arguments, *, listener):
        calls.append((name, arguments[:4], listener.getsockname()[1]))
        return {"pid": 7, "identity": {"started": "1"}}

    monkeypatch.setattr(supervisor_forks, "spawn", spawn)
    monkeypatch.setattr(fork_venues, "wait_ready", lambda endpoint, alive: None)
    return calls


def test_a_supervised_venue_is_recorded_reattached_and_restarted_at_its_endpoint(
    tmp_path, spawned, monkeypatch
):
    supervisor = Supervisor(tmp_path)
    host = SupervisedVenues(supervisor, SimpleNamespace(for_chain=lambda chain: chain), tmp_path)
    assert host.node(FORK) == FORK
    endpoint = host.launch("hypercore-model", tmp_path / "spec.json")
    record = supervisor.state["processes"]["venue-hypercore-model"]
    port = record["port"]
    assert endpoint == record["endpoint"] == f"http://127.0.0.1:{port}"
    assert spawned == [
        (
            "venue-hypercore-model",
            ["service", "fork-venue", "--spec", str(tmp_path / "spec.json")],
            port,
        )
    ]
    ready = []
    monkeypatch.setattr(fork_venues, "wait_ready", lambda endpoint, alive: ready.append(endpoint))
    monkeypatch.setattr(supervisor_forks, "running", lambda entry: True)
    assert host.launch("hypercore-model", tmp_path / "spec.json") == endpoint
    # A live process is reattached only once it answers.
    assert len(spawned) == 1 and ready == [endpoint]
    monkeypatch.setattr(supervisor_forks, "running", lambda entry: False)
    assert host.launch("hypercore-model", tmp_path / "spec.json") == endpoint
    assert spawned[-1][2] == port


def test_supervised_routes_name_the_venues_and_stay_unchanged_without_any(tmp_path, monkeypatch):
    supervisor = Supervisor(tmp_path)
    fixture = supervisor.deployment / "fixture"
    fixture.mkdir(parents=True)
    routes = fixture / "fork-routes.json"
    routes.write_text(json.dumps([ROUTE]))
    bundle = SimpleNamespace(configuration=lambda: "deployment")
    monkeypatch.setattr(fork_venues, "start", lambda deployment, host: {})
    start_venues(supervisor, bundle, None)
    assert json.loads(routes.read_text()) == [ROUTE]
    seen = []

    def start(deployment, host):
        seen.append((deployment, host.directory))
        return {FORK: VENUE}

    monkeypatch.setattr(fork_venues, "start", start)
    start_venues(supervisor, bundle, None)
    assert json.loads(routes.read_text()) == [{**ROUTE, "venues": VENUE}]
    assert seen == [("deployment", fixture)]
    start_venues(supervisor, bundle, None)
    assert json.loads(routes.read_text()) == [{**ROUTE, "venues": VENUE}]


def test_admission_restarts_a_stopped_venue_or_refuses_before_a_coordinator_starts(
    tmp_path, monkeypatch
):
    supervisor = Supervisor(tmp_path)
    bundle = SimpleNamespace(configuration=lambda: "deployment")
    supervisor.nodes = None
    monkeypatch.setattr(fork_venues, "start", lambda deployment, host: host.node(FORK))
    with pytest.raises(DataError, match="relaunch the workspace") as refused:
        supervisor_forks.require_venues(supervisor, bundle)
    assert refused.value.kind == "venue_unavailable"

    def unanswered(deployment, host):
        host.node(FORK)
        raise RuntimeError("fork venue at http://127.0.0.1:9 did not answer within 30 seconds")

    supervisor.nodes = SimpleNamespace(for_chain=lambda chain: chain)
    monkeypatch.setattr(fork_venues, "start", unanswered)
    with pytest.raises(DataError, match="did not answer") as refused:
        supervisor_forks.require_venues(supervisor, bundle)
    assert refused.value.kind == "venue_unavailable"

    def taken(deployment, host):
        raise OSError(98, "Address already in use")

    monkeypatch.setattr(fork_venues, "start", taken)
    with pytest.raises(DataError, match="already in use") as refused:
        supervisor_forks.require_venues(supervisor, bundle)
    assert refused.value.kind == "venue_unavailable"
    monkeypatch.setattr(fork_venues, "start", lambda deployment, host: {})
    supervisor_forks.require_venues(supervisor, bundle)
