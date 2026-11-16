"""Captured canonical RPC inputs verify configured Curve runtime and quantity scope."""

import gzip
import json

import httpx
import pytest

from almanak.caip import ChainId
from almanak.connectors.curve.domain import CurvePool, PoolObservation
from almanak.connectors.curve.reader import quote, read_pool
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import AssetAmount, Chain
from tests.retained_fixtures import retained

CAPTURE = retained("2026-09-07-rollout/curve-initial-discovery") / "curve-reader-capture.json.gz"


def request_key(method, params):
    return json.dumps((method, params), sort_keys=True)


class CapturedReader(EthereumReader):
    def __init__(self, source, records):
        super().__init__(source.chain, "https://invalid.example")
        self.records = {request_key(row["method"], row["params"]): row["result"] for row in records}

    def _rpc(self, method, params):
        return self.records[request_key(method, params)]


@pytest.fixture
def captured(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("captured Curve reads cannot access HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    data = json.loads(gzip.decompress(CAPTURE.read_bytes()))
    source = CurvePool.model_validate_json(json.dumps(data["source"]))
    with CapturedReader(source, data["records"]) as reader:
        block = reader.pin_block(data["observation"]["block"]["number"])
        yield data, source, reader, block


def test_captured_reader_retains_runtime_identity_coin_order_and_quote_units(captured):
    data, source, reader, block = captured
    observed = read_pool(reader, source, block)
    assert observed == PoolObservation.model_validate_json(json.dumps(data["observation"]))
    amount = AssetAmount(asset=source.coins[1], amount=data["quote"]["amount"])
    assert quote(reader, source, block, amount, source.coins[0]) == data["quote"]["output"]
    assert PoolObservation.model_validate_json(observed.model_dump_json()) == observed


def test_other_chain_and_reordered_coins_cannot_reuse_the_captured_deployment(captured):
    _, source, reader, block = captured
    with pytest.raises(DataError, match="immutable factory or ordered coins"):
        read_pool(reader, source.model_copy(update={"coins": source.coins[::-1]}), block)
    foreign = source.model_copy(
        update={"chain": Chain(chain_id=ChainId("eip155:8453"), environment="mainnet")}
    )
    with pytest.raises(DataError, match="another execution scope"):
        read_pool(reader, foreign, block)


@pytest.mark.parametrize("role", ["factory", "pool", "implementation", "views"])
def test_changed_executable_bytes_cannot_pass_the_same_pool_configuration(captured, role):
    data, source, reader, block = captured
    address = (
        source.factory
        if role == "factory"
        else source.pool
        if role == "pool"
        else data["observation"][role]
    )
    key = request_key("eth_getCode", [address, {"blockHash": block.hash, "requireCanonical": True}])
    original = reader.records[key]
    first = int(original[2:4], 16) ^ 1
    reader.records[key] = "0x" + f"{first:02x}" + original[4:]
    with pytest.raises(DataError, match="compiled") as failure:
        read_pool(reader, source, block)
    assert failure.value.kind == "revision"
