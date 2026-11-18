"""Read-only revision comparisons; these are not lifecycle or model coverage proofs."""

import gzip
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.connectors.aave_v3.domain import PositionObservation, Reserve, Target
from almanak.connectors.aave_v3.economic_acquisition import discover
from almanak.connectors.aave_v3.math import (
    debt_assets,
    normalized_indices,
    supplied_assets,
)
from almanak.connectors.aave_v3.reader import read_position
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Account, Block, Chain

FIXTURE = Path(__file__).parent / "fixtures/aave-v3-ethereum-revision11-reads.json.gz"


def captured(network="ethereum"):
    path = FIXTURE.with_name(f"aave-v3-{network}-revision11-reads.json.gz")
    return json.loads(gzip.decompress(path.read_bytes()))


@contextmanager
def replay(raw):
    rows = {
        json.dumps([r["method"], r["params"]], sort_keys=True): r["result"] for r in raw["reads"]
    }

    def respond(request):
        body = json.loads(request.content)
        result = rows[json.dumps([body["method"], body["params"]], sort_keys=True)]
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": result})

    block = Block.model_validate_json(json.dumps(raw["block"]))
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with EthereumReader(block.chain, "https://archive.invalid", client=client) as reader:
            yield reader


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize(
    "network", ["ethereum", "base", "arbitrum", "optimism", "bnb", "polygon", "avalanche"]
)
def test_revision_11_reader_and_integer_indices_match_archival_contracts(index, network):
    raw = captured(network)
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][index]))
    comparison = raw["comparisons"][index]
    with replay(raw) as reader:
        block = reader.pin_block(expected.reserve.block.number)
        actual = read_position(reader, expected.reserve.source, expected.fund, block)
        assert actual == expected
        indices = normalized_indices(actual.reserve, block.timestamp)
        assert indices == tuple(comparison["normalized_indices"])
        assert comparison["elapsed_seconds"] > 0
        assert (
            supplied_assets(actual.reserve.total_scaled_supply, indices[0]),
            debt_assets(actual.reserve.total_scaled_debt, indices[1]),
        ) == tuple(comparison["total_balances"])


@pytest.mark.parametrize(
    "network", ["ethereum", "base", "arbitrum", "optimism", "bnb", "polygon", "avalanche"]
)
def test_changed_pool_implementation_cannot_claim_a_supported_revision(network):
    raw = captured(network)
    code_reads = [r for r in raw["reads"] if r["method"] == "eth_getCode"]
    implementation = code_reads[1]["params"][0]
    for row in code_reads:
        if row["params"][0] == implementation:
            row["result"] = "0x00"
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][0]))
    with replay(raw) as reader:
        block = reader.pin_block(expected.reserve.block.number)
        with pytest.raises(DataError, match="unverified revision"):
            read_position(reader, expected.reserve.source, expected.fund, block)


def test_historical_discovery_pins_the_selected_header_before_authority_reads():
    raw = captured()
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][0]))
    block = expected.reserve.block
    with replay(raw) as reader:
        history = SimpleNamespace(
            reader=reader,
            floor=lambda time: block,
            pin=lambda header: reader.pin_block(header.number),
        )
        assert reader._pinned is None
        result = discover(history, expected.reserve.source, block.timestamp)
        assert result.block == block
        assert result.a_token == expected.reserve.a_token


@pytest.mark.parametrize(
    "field,signature,result,error",
    [
        ("oracle", "getAssetPrice(address)", None, "unavailable"),
        ("oracle", "getAssetPrice(address)", "0x" + "00" * 32, "invalid_protocol_data"),
        ("risk", "getUserAccountData(address)", None, "unavailable"),
    ],
)
def test_missing_valuation_retains_balances_and_a_visible_reason(field, signature, result, error):
    raw = captured()
    selector = "0x" + keccak(text=signature)[:4].hex()
    for row in raw["reads"]:
        if row["method"] == "eth_call" and row["params"][0]["data"].startswith(selector):
            row["result"] = result
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][0]))
    with replay(raw) as reader:
        block = reader.pin_block(expected.reserve.block.number)
        actual = read_position(reader, expected.reserve.source, expected.fund, block)
    evidence = actual.reserve if field == "oracle" else actual
    assert getattr(evidence, field) is None
    assert getattr(evidence, field + "_error") == error
    assert (actual.supplied_scaled, actual.debt_scaled) == (
        expected.supplied_scaled,
        expected.debt_scaled,
    )


def test_reserve_and_fund_cannot_cross_environment_or_chain_scope():
    raw = captured()
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][0]))
    source = expected.reserve.source
    wrong = expected.fund.model_copy(
        update={"chain": source.chain.model_copy(update={"environment": "fixture"})}
    )
    with pytest.raises(ValueError, match="different scope"):
        read_position(None, source, wrong, expected.reserve.block)
    with pytest.raises(ValueError, match="chain/environment scope"):
        Target(allocation="supply", account="fund", fund=wrong, reserve=source)
    with pytest.raises(ValueError):
        Reserve(
            chain=Chain(chain_id=ChainId("eip155:8453"), environment="mainnet"),
            pool=source.pool,
            asset=source.asset,
        )
    assert Account.model_validate_json(json.dumps(raw["fund"])) == expected.fund


@pytest.mark.parametrize("offset", [0, -12], ids=["executable", "metadata_digest"])
def test_arbitrum_proxy_admission_keeps_its_entire_runtime_fingerprint(offset):
    raw = captured("arbitrum")
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][1]))
    address = expected.reserve.a_token.reference
    changed = 0
    for row in raw["reads"]:
        if row["method"] == "eth_getCode" and row["params"][0] == address:
            code = bytearray.fromhex(row["result"][2:])
            code[offset] ^= 1
            row["result"] = "0x" + code.hex()
            changed += 1
    assert changed
    with replay(raw) as reader, pytest.raises(DataError, match="unverified revision"):
        block = reader.pin_block(expected.reserve.block.number)
        read_position(reader, expected.reserve.source, expected.fund, block)
