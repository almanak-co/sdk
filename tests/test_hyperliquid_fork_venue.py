"""The Hyperliquid fork workspace's HyperCore venue, offline (blueprint 30, 9.1).

The generic venue seam (`gateway/fork_venues.py`), the `hypercore-model` process over a
fake HyperEVM fork, its state created once with the mainnet seed, the agent key file and
the owners' HyperCore HYPE, its keeper and control faces, and the supervised host. The
fork test (`tests/fork/test_hyperliquid_workspace.py`) drives the same process against
Anvil and the gateway adapter.
"""

import json
import threading
import time
from contextlib import ExitStack, contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from eth_account import Account as Key

from almanak.chains import HYPEREVM
from almanak.connectors.hyperliquid import fork_venue
from almanak.connectors.hyperliquid.assets import USDC
from almanak.connectors.hyperliquid.fork_model import REGISTER, HyperCoreModel, Keeper
from almanak.connectors.hyperliquid.fork_service import handler_for
from almanak.connectors.hyperliquid.fork_venue import (
    FIXTURE,
    HYPERCORE_HYPE,
    ModelKeeper,
    ModelProcess,
    fund_safe,
    prepare,
    require_market,
)
from almanak.connectors.hyperliquid.requests import Requests
from almanak.gateway import fork_venues
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import InfoResult
from almanak.gateway.operator_config import read_operator_config
from almanak.testflow_keepers import Forks
from tests.hypercore_trading_support import recorded, seed_model
from tests.test_hypercore_custody import SAFE, credit_log, hype_log
from tests.test_hyperliquid_connector import FORK, INSTANCE, ORIGIN, config, market

OWNER = Key.from_key("0x" + "0e" * 32)
# Another account's credit: the Safe's account, and so the owners' HYPE, stays untouched.
OUTSIDER = "0x" + "77" * 20
T = 1_793_866_000
HEAD = 100


class FakeFork:
    """A HyperEVM fork as the model process reads it: blocks with logs, balances, nonces
    and one sender's transaction, behind a loopback JSON-RPC endpoint the process can also
    reach."""

    def __init__(self, instance=INSTANCE):
        self.blocks = [{"timestamp": T, "logs": []} for _ in range(HEAD + 1)]
        self.balances: dict[str, int] = {}
        self.nonces: dict[str, int] = {}
        self.instance = instance
        self.endpoint = ""

    def mine(self, *logs, seconds=1, sender=None):
        """A block with these logs; with a sender, they are its transaction's, at its next
        nonce, and the transaction's hash is the block number."""
        number = len(self.blocks)
        digest = f"0x{number:064x}" if sender else "0x01"
        stamped = [
            {**entry, "blockNumber": hex(number), "logIndex": hex(i), "transactionHash": digest}
            for i, entry in enumerate(logs)
        ]
        block = {"timestamp": self.blocks[-1]["timestamp"] + seconds, "logs": stamped}
        if sender is not None:
            nonce = self.nonces.get(sender, 0)
            self.nonces[sender] = nonce + 1
            block["transactions"] = [{"hash": digest, "from": sender, "nonce": hex(nonce)}]
        self.blocks.append(block)
        return number

    def __call__(self, method, params=()):
        if method == "anvil_metadata":
            network = {"forkBlockNumber": ORIGIN.number, "forkBlockHash": ORIGIN.hash}
            return {"instanceId": self.instance, "forkedNetwork": network}
        if method == "eth_blockNumber":
            return hex(len(self.blocks) - 1)
        if method == "eth_getBlockByNumber":
            block = self.blocks[int(params[0], 16)]
            return {"timestamp": hex(block["timestamp"]), **self.transactions(block, params)}
        if method == "eth_getTransactionReceipt":
            return {"status": "0x1", "logs": self.blocks[int(params[0], 16)]["logs"]}
        if method == "eth_getLogs":
            number = int(params[0]["fromBlock"], 16)
            return [e for e in self.blocks[number]["logs"] if e["address"] in params[0]["address"]]
        return self.accounts(method, params)

    def transactions(self, block, params):
        return {"transactions": block.get("transactions", [])} if params[1] else {}

    def accounts(self, method, params):
        if method == "eth_getBalance":
            return hex(self.balances.get(params[0], 0))
        if method == "anvil_setBalance":
            self.balances[params[0]] = int(params[1], 16)
            return True
        assert method == "eth_getTransactionCount"
        return hex(self.nonces.get(params[0], 0))


@contextmanager
def served(fork):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            payload = {"jsonrpc": "2.0", "id": body["id"]}
            payload["result"] = fork(body["method"], body["params"])
            data = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    fork.endpoint = f"http://127.0.0.1:{server.server_port}"
    try:
        yield fork
    finally:
        server.shutdown()
        server.server_close()


def node_for(fork):
    fork.endpoint = fork.endpoint or "http://127.0.0.1:1"
    return SimpleNamespace(
        rpc=fork,
        origin=ORIGIN,
        metadata={"instanceId": INSTANCE},
        fixture_owners={"safe": OWNER},
    )


class RecordedInfo:
    """Mainnet `/info` as the 4 November 2026 recordings answered it."""

    def __init__(self):
        self.closed = False

    def fetch(self, request):
        label = {"metaAndAssetCtxs": "metaAndAssetCtxs", "l2Book": "l2Book.BTC"}
        found = recorded(label[request.info.type])
        return InfoResult.model_validate(
            {k: found[k] for k in ("request", "received_at", "result")}
        )

    def close(self):
        self.closed = True


@pytest.fixture
def seeded(monkeypatch):
    info = RecordedInfo()
    monkeypatch.setattr(fork_venue, "mainnet_info", lambda: info)
    return info


def model_process(tmp_path, fork):
    model = HyperCoreModel(tmp_path / "model.json", HEAD)
    seed_model(model)
    fund_safe(node_for(fork), model, SAFE)
    return ModelProcess(model, Keeper(fork, model), OWNER)


def owners_send(fork, sent):
    """The owners' HYPE send as the fork mines it: the HYPE system address's Received."""

    def send(rpc, owner, safe, to, data, value):
        sent.append((owner.address, safe, to, value))
        number = fork.mine(hype_log(safe, value), sender=owner.address)
        return {"transactionHash": f"0x{number:064x}", "blockNumber": hex(number)}

    return send


# The workspace state, created once.


def test_the_workspace_creates_the_model_keys_and_spec_once(tmp_path, seeded):
    fork = FakeFork()
    directory = tmp_path / "hypercore"
    spec = prepare(directory, config(), node_for(fork))
    values = json.loads(spec.read_text())
    keys = directory / "agent-keys.json"
    assert values["agent_keys"] == str(keys) and values["owner_key"] == OWNER.key.hex()
    assert (values["instance"], values["origin"]["number"]) == (INSTANCE, ORIGIN.number)
    for path in (spec, keys):
        assert path.stat().st_mode & 0o777 == 0o600
    assert directory.stat().st_mode & 0o777 == 0o700 and json.loads(keys.read_text()) == {}
    model = HyperCoreModel(directory / "hypercore-model.json", 0)
    source = model.doc["seed"]["source"]
    assert model.doc["block"] == HEAD and model.doc["accounts"] == {} and seeded.closed
    assert source["anchor"]["number"] == ORIGIN.number
    assert source["gap_seconds"] == source["metaAndAssetCtxs"] // 1000 - ORIGIN.timestamp
    assert model.doc["fixture"][FIXTURE] == {
        "user": SAFE,
        "amount": HYPERCORE_HYPE,
        "status": "waiting",
    }
    assert fork.balances[SAFE] == HYPERCORE_HYPE
    # Retained: nothing is read or funded again.
    assert prepare(directory, config(), None) == spec
    (directory / "spec.json").unlink()
    with pytest.raises(RuntimeError, match="interrupted"):
        prepare(directory, config(), node_for(fork))


def test_a_failed_seed_read_creates_nothing(tmp_path, monkeypatch):
    class Unavailable(RecordedInfo):
        def fetch(self, request):
            raise DataError("unavailable", "hyperliquid_info: request failed")

    monkeypatch.setattr(fork_venue, "mainnet_info", Unavailable)
    with pytest.raises(DataError):
        prepare(tmp_path / "hypercore", config(), node_for(FakeFork()))
    assert not (tmp_path / "hypercore").exists()


def test_the_seed_market_is_the_one_resolution_pinned():
    contexts = recorded("metaAndAssetCtxs")["result"]
    require_market(contexts, market())
    for changed in (market(max_leverage=20), market(asset=1), market(asset=9_999)):
        with pytest.raises(DataError, match="differs from resolution"):
            require_market(contexts, changed)


def test_the_venue_names_the_model_and_keys_in_its_chain_route(tmp_path, seeded):
    launched = []
    host = SimpleNamespace(
        directory=tmp_path,
        node=lambda chain: node_for(FakeFork()) if chain == FORK else None,
        launch=lambda name, spec: launched.append((name, spec)) or "http://127.0.0.1:9",
    )
    venues = fork_venue.start(config(), host)
    keys = str(tmp_path / "hypercore" / "agent-keys.json")
    assert venues == {FORK: {"hypercore": {"model": "http://127.0.0.1:9", "agent_keys": keys}}}
    assert launched == [("hypercore-model", tmp_path / "hypercore" / "spec.json")]


# The process: answers follow the fork head; the keeper pays and funds.


def test_answers_follow_the_fork_head_and_the_owners_hype_follows_activation(tmp_path, monkeypatch):
    fork, sent = FakeFork(), []
    monkeypatch.setattr(fork_venue, "owner_transaction", owners_send(fork, sent))
    process = model_process(tmp_path, fork)
    fork.mine()
    assert process.advance() == [] and not sent
    deposit = fork.mine(credit_log(SAFE, 30_000_000))
    with process.synced():
        assert process.model.doc["block"] == deposit and SAFE in process.model.doc["accounts"]
    rows = process.advance()
    fixture = process.model.doc["fixture"][FIXTURE]
    assert fixture["status"] == "sent" and fixture["block"] == deposit + 1
    assert rows == [{"fixture": FIXTURE, **fixture}, {"block": deposit + 1, "inputs": 1}]
    assert sent == [(OWNER.address, SAFE, "0x" + "22" * 20, HYPERCORE_HYPE)]
    assert process.model.doc["accounts"][SAFE]["spot"]["150"] == HYPERCORE_HYPE // 10**10
    assert process.advance() == [] and len(sent) == 1


def test_a_restarted_keeper_never_sends_the_owners_hype_twice(tmp_path, monkeypatch):
    fork, sent = FakeFork(), []
    monkeypatch.setattr(fork_venue, "owner_transaction", owners_send(fork, sent))
    process = model_process(tmp_path, fork)
    deposit = fork.mine(credit_log(SAFE, 30_000_000))
    with process.synced():
        fixture = process.model.doc["fixture"][FIXTURE]
    # Sent before a restart, at the retained nonce: recorded from its block, not sent again.
    process.record(fixture, status="sending", after=deposit, nonce=0)
    transfer = fork.mine(hype_log(SAFE, HYPERCORE_HYPE), sender=OWNER.address)
    (applied, row) = process.advance()
    assert applied == {"block": transfer, "inputs": 1} and row["status"] == "sent" and not sent
    assert (row["transaction"], row["block"]) == (f"0x{transfer:064x}", transfer)
    # Another owner transaction took the retained nonce: the transfer is sent again.
    process.record(fixture, status="sending", after=transfer, nonce=1)
    fork.mine(sender=OWNER.address)
    process.advance()
    assert fixture["status"] == "sent" and fixture["nonce"] == 2 and len(sent) == 1
    # An unused retained nonce is sent at.
    process.record(fixture, status="sending", after=len(fork.blocks) - 1, nonce=3)
    process.advance()
    assert fixture["status"] == "sent" and fixture["nonce"] == 3 and len(sent) == 2


def test_control_advances_returns_the_state_and_injects_faults(tmp_path):
    fork = FakeFork()
    process = model_process(tmp_path, fork)
    fork.mine(credit_log(OUTSIDER, 30_000_000))
    assert process.control({"op": "advance"}) == {"rows": [{"block": HEAD + 1, "inputs": 1}]}
    assert process.control({"op": "state"})["model"]["register"] == REGISTER
    process.control({"op": "withhold", "action": "order"})
    process.control({"op": "drop", "action": 7, "user": SAFE})
    assert process.model.doc["exchange_faults"] == [{"kind": "withhold", "action": "order"}]
    assert process.model.doc["faults"] == [{"kind": "drop", "action": 7, "user": SAFE}]
    with pytest.raises(ValueError, match="takes exactly action"):
        process.control({"op": "withhold"})
    with pytest.raises(KeyError):
        process.control({"op": "unknown"})


def test_the_service_holds_its_sync_through_each_answer_and_serves_its_control_face(
    tmp_path, monkeypatch
):
    fork = FakeFork()
    process = model_process(tmp_path, fork)
    events = []

    @contextmanager
    def synced():
        with process.synced():
            events.append("synced")
            yield
            events.append("released")

    for name in ("answer", "exchange"):
        method = getattr(process.model, name)
        monkeypatch.setattr(process.model, name, recording(events, name, method))
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), handler_for(process.model, synced, process.control)
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}"
    try:
        assert httpx.get(url + "/ready").json() == {"register": REGISTER}
        assert httpx.get(url + "/other").status_code == 404
        fork.mine(credit_log(OUTSIDER, 30_000_000))
        role = httpx.post(url + "/info", json={"type": "userRole", "user": OUTSIDER}).json()
        assert role == {"role": "user"} and released(events, 3) == ["synced", "answer", "released"]
        assert httpx.post(url + "/exchange", json={}).status_code == 422
        assert released(events, 6)[3:] == ["synced", "exchange", "released"]
        assert httpx.post(url + "/control", json={"op": "state"}).status_code == 200
        refused = httpx.post(url + "/control", json={"op": "withhold"})
        assert refused.status_code == 422 and "takes exactly" in refused.json()["error"]
        keeper = ModelKeeper(url)
        assert keeper.align(None, 60) == 60 and keeper.settle(None) == []
        with pytest.raises(RuntimeError, match="refused withhold"):
            keeper.control({"op": "withhold"})
    finally:
        server.shutdown()
        server.server_close()


def released(events, count):
    """The events once the handler has released its sync. It writes the answer while it
    holds the sync, so a client can read the answer before the release is recorded."""
    deadline = time.monotonic() + 5
    while len(events) < count and time.monotonic() < deadline:
        time.sleep(0.01)
    return events


def recording(events, name, method):
    """The model's answer, recorded between the boundary's entry and release."""

    def recorded(*args):
        events.append(name)
        return method(*args)

    return recorded


def test_the_process_serves_only_beside_its_identified_fork(tmp_path, seeded):
    with served(FakeFork()) as fork, ExitStack() as stack:
        spec = prepare(tmp_path / "hypercore", config(), node_for(fork))
        nodes = SimpleNamespace(for_chain=lambda chain: node_for(fork), stack=stack)
        host = fork_venues.LocalHost(nodes, tmp_path)
        endpoint = host.launch("hypercore-model", spec)
        fork.mine(credit_log(OUTSIDER, 30_000_000))
        rows = ModelKeeper(endpoint).settle(None)
        assert {"block": HEAD + 1, "inputs": 1, "advanced_seconds": 0} in rows
        values = json.loads(spec.read_text())
        values["instance"] = "0x" + "99" * 32
        other = tmp_path / "other.json"
        other.write_text(json.dumps(values))
        with pytest.raises(RuntimeError, match="exited during start"):
            host.launch("other-model", other)
        assert "identified Anvil fork" in (tmp_path / "other-model.log").read_text()


def test_the_process_opens_only_the_state_its_workspace_created(tmp_path):
    spec = {
        "rpc": "http://127.0.0.1:1",
        "instance": INSTANCE,
        "origin": ORIGIN.model_dump(mode="json"),
        "state": str(tmp_path / "missing.json"),
        "owner_key": OWNER.key.hex(),
    }
    fork = FakeFork()
    with pytest.raises(RuntimeError, match="created by its fork workspace"):
        fork_venue.open_process(spec, rpc=fork)
    fork_venue.require_fork(fork, spec)


# The run's fork keeper finds the process through the workspace's records.


def test_the_runs_fork_keeper_is_the_model_process(tmp_path):
    record = {"pid": 1, "endpoint": "http://127.0.0.1:9"}
    (tmp_path / "workspace.json").write_text(
        json.dumps({"processes": {"venue-hypercore-model": record}})
    )
    forks = Forks(tmp_path, tmp_path / ".env")
    assert forks.venue("hypercore-model") == record
    keeper = Requests.fork_keeper(forks, tmp_path / "keepers")
    assert isinstance(keeper, ModelKeeper) and keeper.endpoint == "http://127.0.0.1:9"
    with pytest.raises(ValueError, match="no other venue"):
        forks.venue("other")


def test_the_reference_operator_config_funds_a_safe_on_the_hyperevm_anchor():
    root = Path(__file__).parents[1]
    operator = read_operator_config(root / "examples/operators/hyperliquid_hyperevm_perp.fork.yaml")
    (anchor,) = operator.fork.anchors
    assert (anchor.chain, anchor.height) == (HYPEREVM.chain, ORIGIN.number)
    (fund,) = operator.fork.funds
    assert fund.kind == "safe_roles" and [a.asset for a in fund.assets] == [USDC]
    (payer,) = operator.fork.operators
    assert payer.native_balance.asset == HYPEREVM.native_asset
