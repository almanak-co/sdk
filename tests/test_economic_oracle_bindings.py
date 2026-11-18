"""Independent bytecode equivalence and per-reserve event selection evidence."""

import gzip
import hashlib
import json
from functools import cache
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_utils import keccak

from almanak.backtest.economic_oracles import contracts_for
from almanak.backtest.economic_prepare import Acquisition, topics
from almanak.caip import ChainId


@cache
def oracle_proof():
    return json.loads(
        gzip.decompress(
            (
                Path(__file__).parent / "fixtures/aave-v4-avalanche-oracle-semantics.json.gz"
            ).read_bytes()
        )
    )


def normalized(code, spans, metadata):
    code = bytearray.fromhex(code.removeprefix("0x"))
    for locations in spans.values():
        for location in locations:
            start, length = location["start"], location["length"]
            code[start : start + length] = bytes(length)
    if metadata:
        length = int.from_bytes(code[-2:])
        assert length == 51 and code[-length - 2 : -length + 6] == bytes.fromhex("a264697066735822")
        code = code[: -length - 2]
    return bytes(code)


@pytest.mark.parametrize("index", range(6))
def test_avalanche_oracle_runtime_matches_verified_source_semantics(index):
    proof = oracle_proof()
    row = proof["comparisons"][index]
    artifact = next(
        c for c in contracts_for(ChainId("eip155:43114")) if c["address"] == row["target"]
    )
    body = proof["verification_documents"][artifact["verification_sha256"]]
    assert hashlib.sha256(body.encode()).hexdigest() == artifact["verification_sha256"]
    spans, metadata = row["immutable_references"], row["metadata_excluded"]
    expected = normalized(row["reference_runtime"], spans, metadata)
    actual = normalized(row["captured_runtime"], spans, metadata)
    assert actual == expected
    assert "0x" + keccak(actual).hex() == row["normalized_keccak256"]
    assert "0x" + keccak(bytes.fromhex(row["captured_runtime"][2:])).hex() == artifact["code_hash"]
    if "recompiled_runtime" in row:
        assert normalized(row["recompiled_runtime"], spans, metadata) == expected


def test_oracle_binding_never_substitutes_another_chain():
    ethereum = contracts_for(ChainId("eip155:1"))
    avalanche = contracts_for(ChainId("eip155:43114"))
    assert not {c["address"] for c in ethereum} & {c["address"] for c in avalanche}
    with pytest.raises(ValueError, match="verified CAIP-2"):
        contracts_for(ChainId("eip155:8453"))


def test_only_stable_dependency_graphs_can_prune_unrelated_price_events():
    history = SimpleNamespace(floor=lambda at: at, pin=lambda block: block)
    session = SimpleNamespace(
        history=history,
        start=100,
        oracle=SimpleNamespace(price=lambda *_: SimpleNamespace(dependencies=("requested",))),
    )
    price_topic = topics(("AnswerUpdated(int256,uint256,uint256)",))[0]
    requested = SimpleNamespace(address="requested", topics=(price_topic,))
    unrelated = SimpleNamespace(address="other", topics=(price_topic,))
    events = (requested, unrelated)
    assert Acquisition.price_events(session, None, events) == (requested,)
    changed = SimpleNamespace(
        address="other", topics=topics(("AggregatorConfirmed(address,address)",))
    )
    assert Acquisition.price_events(session, None, (*events, changed)) == (*events, changed)
    upgraded = SimpleNamespace(address="spoke", topics=topics(("Upgraded(address)",)))
    assert Acquisition.price_events(session, None, (*events, upgraded)) == (*events, upgraded)
