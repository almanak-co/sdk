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
from almanak.connectors.aave_v3.reader import POOL_REVISIONS, read_position
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Account, Block, Chain

FIXTURE = Path(__file__).parent / "fixtures/aave-v3-ethereum-revision11-reads.json.gz"
POOL_REVISION = "0x" + keccak(text="POOL_REVISION()")[:4].hex()


NETWORKS = ["ethereum", "base", "arbitrum", "optimism", "bnb", "polygon", "avalanche"]
# The Pool revisions with captured reads on every network. The revision-12 blocks follow
# each Pool's upgrade; their comparison values are the contracts' own answers there.
REVISIONS = [11, 12]


def captured(network="ethereum", revision=11):
    path = FIXTURE.with_name(f"aave-v3-{network}-revision{revision}-reads.json.gz")
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
@pytest.mark.parametrize("network", NETWORKS)
@pytest.mark.parametrize("revision", REVISIONS)
def test_reader_and_integer_indices_match_archival_contracts(index, network, revision):
    raw = captured(network, revision)
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


def pool_identity(raw):
    """The Pool's proxy and implementation code hashes, from the recorded code itself."""
    proxy, implementation = [r for r in raw["reads"] if r["method"] == "eth_getCode"][:2]
    assert proxy["params"][0] == raw["positions"][0]["reserve"]["source"]["pool"]
    return tuple(
        "0x" + keccak(bytes.fromhex(row["result"][2:])).hex() for row in (proxy, implementation)
    )


@pytest.mark.parametrize("network", NETWORKS)
@pytest.mark.parametrize("revision", REVISIONS)
def test_each_pinned_pool_reports_the_revision_it_was_verified_as(network, revision):
    raw = captured(network, revision)
    reported = [
        int(r["result"], 16)
        for r in raw["reads"]
        if r["method"] == "eth_call" and r["params"][0]["data"] == POOL_REVISION
    ]
    assert set(reported) == {revision}
    chain = ChainId(raw["block"]["chain"]["chain_id"])
    assert POOL_REVISIONS[chain][pool_identity(raw)] == revision


def test_a_pool_pin_admits_its_code_on_its_own_chain_only(monkeypatch):
    raw = captured("base", 12)
    base, arbitrum = ChainId("eip155:8453"), ChainId("eip155:42161")
    moved = {**POOL_REVISIONS, arbitrum: POOL_REVISIONS[base], base: POOL_REVISIONS[arbitrum]}
    monkeypatch.setattr("almanak.connectors.aave_v3.reader.POOL_REVISIONS", moved)
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][0]))
    with replay(raw) as reader:
        block = reader.pin_block(expected.reserve.block.number)
        with pytest.raises(DataError, match="unverified revision"):
            read_position(reader, expected.reserve.source, expected.fund, block)


def test_the_revisions_of_one_network_share_a_pool_proxy_and_reserve_tokens():
    for network in NETWORKS:
        old, new = captured(network, 11), captured(network, 12)
        assert pool_identity(old)[0] == pool_identity(new)[0]
        assert pool_identity(old)[1] != pool_identity(new)[1]
        for before, after in zip(old["positions"], new["positions"], strict=True):
            for token in ("a_token", "variable_debt_token"):
                assert before["reserve"][token] == after["reserve"][token]


@pytest.mark.parametrize("revision", REVISIONS)
def test_pinned_pool_code_reporting_another_revision_is_refused(revision):
    raw = captured("base", revision)
    other = REVISIONS[1 - REVISIONS.index(revision)]
    for row in raw["reads"]:
        if row["method"] == "eth_call" and row["params"][0]["data"] == POOL_REVISION:
            row["result"] = "0x" + other.to_bytes(32, "big").hex()
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][0]))
    with replay(raw) as reader:
        block = reader.pin_block(expected.reserve.block.number)
        with pytest.raises(DataError, match="revision differs from its model"):
            read_position(reader, expected.reserve.source, expected.fund, block)


@pytest.mark.parametrize("network", NETWORKS)
@pytest.mark.parametrize("revision", REVISIONS)
def test_changed_pool_implementation_cannot_claim_a_supported_revision(network, revision):
    raw = captured(network, revision)
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


@pytest.mark.parametrize("revision", REVISIONS)
def test_historical_discovery_pins_the_selected_header_before_authority_reads(revision):
    raw = captured(revision=revision)
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
