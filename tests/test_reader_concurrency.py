"""Concurrent permission and runtime reads retain their own pinned observations."""

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import httpx
import pytest
from eth_abi import encode

from almanak.caip import AssetType, ChainId
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import Chain

TOKEN = AssetType("eip155:1/erc20:0x" + "12" * 20)


def respond(request):
    body = json.loads(request.content)
    if body["method"] == "eth_chainId":
        result = "0x1"
    elif body["method"] == "eth_getBlockByNumber":
        number = int(body["params"][0], 16)
        result = {"number": hex(number), "hash": "0x" + f"{number:064x}", "timestamp": "0x10"}
    else:
        assert body["method"] == "eth_call"
        assert body["params"][1]["requireCanonical"]
        result = "0x" + encode(("uint8",), (18,)).hex()
    return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": result})


def test_concurrent_requests_pin_independent_blocks_without_relaxing_validation():
    barrier = Barrier(2)
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        reader = EthereumReader(
            Chain(chain_id=ChainId("eip155:1"), environment="mainnet"),
            "https://fixture.invalid",
            client=client,
        )

        def inspect(number):
            block = reader.pin_block(number)
            barrier.wait(timeout=5)
            assert AuthorityReader(reader, block).block == block
            assert reader.asset(TOKEN, block).decimals == 18
            reader.pin_block(number + 10)
            with pytest.raises(DataError, match="pinned block"):
                reader.asset(TOKEN, block)
            return block.number

        with ThreadPoolExecutor(max_workers=2) as pool:
            assert sorted(pool.map(inspect, (1, 2))) == [1, 2]
        assert reader._pinned is None
