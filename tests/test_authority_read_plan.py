"""Authority verification replays its previous reads in batches at each new head."""

import json

import httpx
import pytest

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.roles_storage import AuthorityReader, ReadPlan
from almanak.models import Chain

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
SAFE = "0x" + "5a" * 20
TOKEN = "0x" + "70" * 20


class Node:
    """Storage that changes between heads; `refused` slots answer with an RPC error."""

    def __init__(self):
        self.head = 10
        self.refused: set[str] = set()
        self.single: list[str] = []
        self.batched: list[list[str]] = []

    def block(self, number):
        return {"number": hex(number), "hash": "0x" + f"{number:064x}", "timestamp": hex(number)}

    def answer(self, body):
        method, params = body["method"], body["params"]
        row = {"jsonrpc": "2.0", "id": body["id"]}
        if method == "eth_chainId":
            row["result"] = "0x1"
        elif method == "eth_getBlockByNumber":
            row["result"] = self.block(self.head)
        elif method == "eth_getStorageAt" and params[1] in self.refused:
            row["error"] = {"code": -32000, "message": "refused"}
        elif method == "eth_getStorageAt":
            number = int(params[2]["blockHash"], 16)
            row["result"] = hex(int(params[1], 16) * 1000 + number)
        else:
            assert method == "eth_getCode"
            row["result"] = "0x60"
        return row

    def __call__(self, request):
        body = json.loads(request.content)
        if isinstance(body, list):
            self.batched.append([member["method"] for member in body])
            return httpx.Response(200, json=[self.answer(member) for member in body])
        if body["method"].startswith("eth_get") and body["method"] != "eth_getBlockByNumber":
            self.single.append(body["method"])
        return httpx.Response(200, json=self.answer(body))


@pytest.fixture
def node():
    node = Node()
    client = httpx.Client(transport=httpx.MockTransport(node))
    node.reader = EthereumReader(CHAIN, "https://fixture.invalid", client=client)
    yield node
    client.close()


def verification(node, plan, slots):
    authority = AuthorityReader(node.reader, node.reader.pin_block("latest"), plan)
    values = [authority.storage(SAFE, slot) for slot in slots]
    assert authority.code(TOKEN) == b"\x60"
    authority.retain_plan()
    return values


def test_next_head_replays_the_previous_reads_in_one_batch(node):
    plan = ReadPlan()
    assert verification(node, plan, [1, 2, 3]) == [1010, 2010, 3010]
    assert (len(node.single), node.batched) == (4, [])

    node.head, node.single = 11, []
    # Slot 3 is no longer read and slot 4 is new: each value is still read at head 11.
    assert verification(node, plan, [1, 2, 4]) == [1011, 2011, 4011]
    assert node.batched == [["eth_getStorageAt"] * 3 + ["eth_getCode"]]
    assert node.single == ["eth_getStorageAt"]
    assert [params[1] for method, params in plan.reads if method == "eth_getStorageAt"] == [
        "0x1",
        "0x2",
        "0x4",
    ]


def test_a_refused_replayed_read_is_read_again_on_its_own(node):
    plan = ReadPlan()
    verification(node, plan, [1, 2])
    node.head, node.single = 11, []
    node.refused.add("0x2")
    with pytest.raises(DataError):
        verification(node, plan, [1, 2])
    # The batch tolerated the refusal; the verification's own read raised it.
    assert node.single == ["eth_getStorageAt"]
    # A failed verification keeps the plan it started from.
    assert len(plan.reads) == 3


def test_reads_without_a_plan_are_not_recorded_or_replayed(node):
    authority = AuthorityReader(node.reader, node.reader.pin_block("latest"))
    assert authority.storage(SAFE, 1) == 1010
    authority.retain_plan()
    assert authority.replayable == {}
