"""Number pins and execution-context proof for chains whose providers misplace hash pins."""

import json
import threading

import httpx
import pytest
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.gateway import ethereum, observation_gateway
from almanak.gateway.ethereum import CONTEXT_PROBE, DataError, EthereumReader
from almanak.gateway.observation_gateway import GatewayRequest, ObservationGateway, gateway_reader
from almanak.gateway.service import http_server
from almanak.models import Chain

HYPEREVM = Chain(chain_id=ChainId("eip155:999"), environment="mainnet")
ETHEREUM = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
TOKEN = "0x5555555555555555555555555555555555555555"
HEAD = 40
DEPTH = evm_network(HYPEREVM.chain_id).observation_depth
# The widest window Alchemy ran number pins at head + 1 on HyperEVM (2026-10-21).
ALCHEMY_WINDOW = 192


class Node:
    """Linked headers 0..head, where `finalized` is the head. `runs_at_head` mimics a provider
    that ignores the block pin; `window` one that runs reads pinned within that many blocks of
    its head at head + 1 and serves older pins exactly, as Alchemy does on HyperEVM."""

    def __init__(
        self, chain_id: int, runs_at_head: bool = False, head: int = HEAD, window: int = -1
    ):
        self.chain_id = chain_id
        self.runs_at_head = runs_at_head
        self.head = head
        self.window = window
        self.requests: list[tuple[str, list]] = []
        self.headers: dict[int, dict] = {}
        parent = "0x" + "00" * 32
        for number in range(head + 1):
            block_hash = "0x" + keccak(text=f"block:{number}").hex()
            self.headers[number] = {
                "number": hex(number),
                "hash": block_hash,
                "parentHash": parent,
                "timestamp": hex(1_000 + number),
            }
            parent = block_hash
        self.by_hash = {header["hash"]: header for header in self.headers.values()}

    def executed(self, selector) -> dict:
        if isinstance(selector, dict):
            pinned = self.by_hash[selector["blockHash"]]
        else:
            pinned = self.headers[int(selector, 16)]
        if self.runs_at_head or self.head - int(pinned["number"], 16) <= self.window:
            # The next block's context: number and parent move with the head.
            head = self.headers[self.head]
            return {
                "number": hex(self.head + 1),
                "timestamp": hex(int(head["timestamp"], 16) + 1),
                "parentHash": head["hash"],
            }
        return pinned

    def result(self, method, params):
        if method == "eth_chainId":
            return hex(self.chain_id)
        if method == "eth_getBlockByNumber":
            tag = params[0]
            return self.headers[self.head if tag in ("latest", "finalized") else int(tag, 16)]
        if method == "eth_getBlockByHash":
            return self.by_hash.get(params[0])
        if method == "eth_getLogs":
            return []
        context = self.executed(params[-1])
        if method == "eth_call" and is_probe(params):
            words = (
                int(context["timestamp"], 16).to_bytes(32),
                int(context["number"], 16).to_bytes(32),
                bytes.fromhex(context["parentHash"][2:]),
            )
            return "0x" + b"".join(words).hex()
        # Every other read answers with the number of the block it ran at.
        return "0x" + int(context["number"], 16).to_bytes(32).hex()

    def handle(self, request):
        body = json.loads(request.content)
        rows = body if isinstance(body, list) else [body]
        answers = []
        for row in rows:
            self.requests.append((row["method"], row["params"]))
            answers.append(
                {
                    "jsonrpc": "2.0",
                    "id": row["id"],
                    "result": self.result(row["method"], row["params"]),
                }
            )
        return httpx.Response(200, json=answers if isinstance(body, list) else answers[0])


def reader_for(node: Node, chain: Chain = HYPEREVM, **options) -> EthereumReader:
    client = httpx.Client(transport=httpx.MockTransport(node.handle))
    return EthereumReader(chain, "https://rpc.invalid/key", client=client, **options)


def is_probe(params: list) -> bool:
    return isinstance(params[0], dict) and params[0].get("data") == CONTEXT_PROBE


def probes(node: Node) -> list[list]:
    return [p for m, p in node.requests if m == "eth_call" and is_probe(p)]


def reads(node: Node, method: str) -> list[list]:
    return [p for m, p in node.requests if m == method and not is_probe(p)]


def test_hyperevm_reads_pin_the_block_number_after_proving_its_context():
    node = Node(999)
    reader = reader_for(node)
    block = reader.pin_block(HEAD - 3)

    first = reader.call(TOKEN, "totalSupply()", block)
    second = reader.call(TOKEN, "totalSupply()", block)

    assert first == second == (HEAD - 3,)
    assert [p[-1] for p in reads(node, "eth_call")] == [hex(HEAD - 3)] * 2
    # One proof per block, sent with the same number pin the reads use.
    assert [p[-1] for p in probes(node)] == [hex(HEAD - 3)]


def test_raw_hash_selectors_and_batches_follow_the_number_pin():
    node = Node(999)
    reader = reader_for(node)
    block = reader.pin_block(HEAD - 1)
    selector = {"blockHash": block.hash, "requireCanonical": True}

    reader._rpc("eth_getBalance", [TOKEN, selector])
    reader.batch(
        [("eth_getStorageAt", [TOKEN, "0x0", selector]), ("eth_getCode", [TOKEN, selector])]
    )

    for method in ("eth_getBalance", "eth_getStorageAt", "eth_getCode"):
        assert node.requests[[m for m, _ in node.requests].index(method)][1][-1] == hex(HEAD - 1)
    assert len(probes(node)) == 1


def test_a_block_pinned_elsewhere_is_resolved_by_hash_before_its_number_pin():
    node = Node(999)
    reader = reader_for(node)
    other = node.headers[7]["hash"]

    reader._rpc("eth_getBalance", [TOKEN, {"blockHash": other, "requireCanonical": True}])

    assert ("eth_getBlockByHash", [other, False]) in node.requests
    assert reads(node, "eth_getBalance")[-1][-1] == hex(7)


def test_an_unknown_block_hash_is_refused_rather_than_read_at_head():
    node = Node(999)
    reader = reader_for(node)

    with pytest.raises(DataError) as refused:
        reader._rpc(
            "eth_getBalance", [TOKEN, {"blockHash": "0x" + "11" * 32, "requireCanonical": True}]
        )

    assert refused.value.kind == "block_mismatch"
    assert not reads(node, "eth_getBalance")


def test_a_provider_that_runs_pinned_reads_at_its_head_is_refused():
    node = Node(999, runs_at_head=True)
    reader = reader_for(node)
    block = reader.pin_block(HEAD - 5)

    with pytest.raises(DataError) as refused:
        reader.call(TOKEN, "totalSupply()", block)

    assert refused.value.kind == "provider_context"
    assert refused.value.details["pinned"][0] == HEAD - 5
    assert refused.value.details["executed"][0] == HEAD + 1
    # The read itself is never sent once the proof fails.
    assert not reads(node, "eth_call")


def test_log_filters_keep_their_block_hash():
    node = Node(999)
    reader = reader_for(node)
    block = reader.pin_block(HEAD)
    log_filter = {"blockHash": block.hash, "address": TOKEN}

    reader._rpc("eth_getLogs", [log_filter])

    assert ("eth_getLogs", [log_filter]) in node.requests
    assert not probes(node)


@pytest.mark.parametrize(
    ("chain", "options"),
    [
        (ETHEREUM, {}),
        (HYPEREVM, {"local_transport": True}),
        (Chain(chain_id=ChainId("eip155:999"), environment="anvil:hyperevm"), {}),
    ],
)
def test_other_networks_and_local_forks_keep_hash_pins_without_proof(chain, options):
    node = Node(
        31337 if chain.environment.startswith("anvil:") else int(str(chain.chain_id).split(":")[1])
    )
    reader = reader_for(node, chain, **options)
    block = reader.pin_block(HEAD - 2)

    reader.call(TOKEN, "totalSupply()", block)

    assert reads(node, "eth_call")[-1][-1] == {"blockHash": block.hash, "requireCanonical": True}
    assert not probes(node)


def test_header_and_proof_caches_stay_bounded_and_reprove_an_evicted_block(monkeypatch):
    monkeypatch.setattr(ethereum, "HEADER_CACHE", 2)
    node = Node(999)
    reader = reader_for(node)
    first = reader.pin_block(10)
    reader.call(TOKEN, "totalSupply()", first)
    for number in (11, 12):
        reader.call(TOKEN, "totalSupply()", reader.pin_block(number))

    assert len(reader._headers) == len(reader._proved) == 2
    reader.pin_block(10)
    reader.call(TOKEN, "totalSupply()", first)

    # Block 10 left both caches, so its next read proves it again.
    assert [p[-1] for p in probes(node)] == [hex(10), hex(11), hex(12), hex(10)]


def alchemy() -> Node:
    return Node(999, head=DEPTH + 64, window=ALCHEMY_WINDOW)


@pytest.mark.parametrize("tag", ["finalized", "latest"])
def test_an_observation_at_a_tag_pins_the_network_depth_behind_it(tag):
    node = alchemy()
    reader = reader_for(node, observation=True)
    block = reader.pin_block(tag)

    assert block.number == node.head - DEPTH
    assert reader.call(TOKEN, "totalSupply()", block) == (node.head - DEPTH,)
    assert [p[-1] for p in probes(node)] == [hex(node.head - DEPTH)]


def test_an_observation_still_refuses_a_provider_that_runs_its_pin_elsewhere():
    node = Node(999, head=DEPTH + 64, window=DEPTH)
    reader = reader_for(node, observation=True)
    block = reader.pin_block("finalized")

    with pytest.raises(DataError) as refused:
        reader.call(TOKEN, "totalSupply()", block)

    assert refused.value.kind == "provider_context"
    assert refused.value.details["pinned"][0] == node.head - DEPTH
    assert refused.value.details["executed"][0] == node.head + 1
    assert not reads(node, "eth_call")


def test_explicit_numbers_and_runtime_readers_keep_the_block_they_name():
    node = alchemy()
    assert reader_for(node, observation=True).pin_block(node.head - 3).number == node.head - 3
    runtime = reader_for(node)
    head = runtime.pin_block("latest")
    assert head.number == node.head

    # Near the head the provider runs the read elsewhere, and the proof refuses it.
    with pytest.raises(DataError) as refused:
        runtime.call(TOKEN, "totalSupply()", head)
    assert refused.value.kind == "provider_context"


@pytest.mark.parametrize(
    "chain",
    [ETHEREUM, Chain(chain_id=ChainId("eip155:999"), environment="anvil:hyperevm")],
)
def test_other_networks_and_forks_observe_at_the_tag(chain):
    node = Node(31337 if chain.environment.startswith("anvil:") else 1, head=DEPTH + 64)
    reader = reader_for(node, chain, observation=True)

    assert reader.pin_block("finalized").number == node.head


def test_a_gateway_observation_pins_behind_the_tag_and_the_gateway_proves_it(tmp_path, monkeypatch):
    node = alchemy()
    provider = reader_for(node, observation=True)
    monkeypatch.setattr(observation_gateway, "verified_reader", lambda chain, env_file: provider)
    token = tmp_path / "read-gateway.token"
    token.write_text("ab" * 32)
    token.chmod(0o600)
    gateway = ObservationGateway(tmp_path / "operator.env")
    server = http_server(gateway, token, 0, request_type=GatewayRequest, fields={"read"})
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = f"http://127.0.0.1:{server.server_port}"
        with gateway_reader(HYPEREVM.chain_id, endpoint, token) as reader:
            block = reader.pin_block("finalized")
            assert reader.call(TOKEN, "totalSupply()", block) == (node.head - DEPTH,)
    finally:
        server.shutdown()
        server.server_close()
        gateway.close()

    # The client picks the block; the gateway's reader proves it before the read.
    assert [p[-1] for p in probes(node)] == [hex(node.head - DEPTH)]
    assert [p[-1] for p in reads(node, "eth_call")] == [hex(node.head - DEPTH)]


def test_a_raw_gateway_read_at_the_tag_block_is_proved_and_refused(tmp_path, monkeypatch):
    node = alchemy()
    provider = reader_for(node, observation=True)
    monkeypatch.setattr(observation_gateway, "verified_reader", lambda chain, env_file: provider)
    gateway = ObservationGateway(tmp_path / "operator.env")

    def read(method, params):
        # As the server parses a request body: strict JSON mode, where arrays become tuples.
        body = GatewayRequest.model_validate_json(
            json.dumps({"chain": str(HYPEREVM.chain_id), "method": method, "params": params})
        )
        return gateway.dispatch("read", body)["result"]

    # The gateway answers the tag with the head, as the provider does; a client that skips
    # the observation depth and reads at that block gets a refusal, never another block's state.
    head = read("eth_getBlockByNumber", ["finalized", False])
    assert int(head["number"], 16) == node.head
    selector = {"blockHash": head["hash"], "requireCanonical": True}
    with pytest.raises(DataError) as refused:
        read("eth_call", [{"to": TOKEN, "data": "0x18160ddd"}, selector])

    assert refused.value.kind == "provider_context"
    assert not reads(node, "eth_call")
