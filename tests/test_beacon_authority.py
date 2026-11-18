"""Retained Arbitrum proxy evidence binds both beacon and implementation identity."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import decode
from eth_utils import keccak

from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import ZERO_ADDRESS, Block

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-arbitrum-token-discovery"
)


def captured(name):
    return json.loads(gzip.decompress((EVIDENCE / (name + ".json.gz")).read_bytes()))


def authority():
    tokens = captured("curve-arbitrum-tokens")
    implementations = captured("curve-arbitrum-token-implementations")
    records = tokens["records"] + implementations["records"]
    replies = {json.dumps([r["method"], r["params"]], sort_keys=True): r["result"] for r in records}
    block = Block.model_validate_json(json.dumps(tokens["block"]))

    def rpc(method, params):
        return replies[json.dumps([method, params], sort_keys=True)]

    def call(address, signature, observed, *, inputs, args, outputs):
        assert observed == block and inputs == args == ()
        raw = rpc(
            "eth_call",
            [
                {"to": address, "data": "0x" + keccak(text=signature)[:4].hex()},
                {"blockHash": block.hash, "requireCanonical": True},
            ],
        )
        return decode(outputs, bytes.fromhex(raw[2:]))

    reader = SimpleNamespace(_pinned=block, _rpc=rpc, call=call)
    return AuthorityReader(reader, block), implementations["tokens"]


@pytest.mark.parametrize("index", range(5))
def test_captured_token_identity_retains_resolved_implementation(index):
    reader, tokens = authority()
    token = tokens[index]
    identity = reader.identity(token["address"])
    assert identity.implementation == token["implementation"]
    assert (
        identity.implementation_hash
        == "0x" + keccak(bytes.fromhex(token["implementation_code"][2:])).hex()
    )
    assert identity.beacon == token["beacon"]
    assert ContractIdentity.model_validate_json(identity.model_dump_json()) == identity
    if token["beacon"]:
        assert identity.beacon_hash and identity.implementation_slot is None
    else:
        assert "beacon" not in identity.model_dump()


@pytest.mark.parametrize("mutation", ["missing_beacon", "beacon_code", "missing_code", "zero_impl"])
def test_invalid_beacon_authority_refuses_identity(monkeypatch, mutation):
    reader, tokens = authority()
    token = tokens[0]
    original_code = reader.code
    if mutation == "missing_beacon":
        monkeypatch.setattr(reader, "storage", lambda address, slot: 0)
    if mutation == "zero_impl":
        monkeypatch.setattr(reader, "call", lambda *_: (ZERO_ADDRESS,))
    changed = token["beacon"] if mutation == "beacon_code" else token["implementation"]
    monkeypatch.setattr(
        reader,
        "code",
        lambda address: b"" if address in (changed, ZERO_ADDRESS) else original_code(address),
    )
    with pytest.raises(DataError, match="beacon"):
        reader.identity(token["address"])


@pytest.mark.parametrize(
    "mutation", ["beacon_address", "implementation_address", "implementation_code"]
)
def test_changed_authority_cannot_equal_retained_identity(monkeypatch, mutation):
    reader, tokens = authority()
    token = tokens[0]
    retained = reader.identity(token["address"])
    original_code = reader.code
    alternative = "0x" + "12" * 20
    if mutation == "beacon_address":
        monkeypatch.setattr(reader, "storage", lambda address, slot: int(alternative, 16))
        monkeypatch.setattr(reader, "call", lambda *_: (token["implementation"],))
        monkeypatch.setattr(
            reader,
            "code",
            lambda address: original_code(token["beacon"] if address == alternative else address),
        )
    elif mutation == "implementation_address":
        monkeypatch.setattr(reader, "call", lambda *_: (alternative,))
        monkeypatch.setattr(
            reader,
            "code",
            lambda address: original_code(
                token["implementation"] if address == alternative else address
            ),
        )
    else:
        monkeypatch.setattr(
            reader,
            "code",
            lambda address: (
                b"new revision" if address == token["implementation"] else original_code(address)
            ),
        )
    assert reader.identity(token["address"]) != retained


def test_existing_plain_identity_wire_shape_is_unchanged():
    value = dict(
        address="0x" + "12" * 20,
        code_hash="0x" + "34" * 32,
        implementation=None,
        implementation_hash=None,
        implementation_slot=None,
    )
    assert ContractIdentity.model_validate(value).model_dump() == value


def test_usdc_fixture_identity_and_balance_slot_match_compiled_source():
    reader, tokens = authority()
    token = tokens[1]
    artifact = json.loads(
        (
            Path(__file__).parents[1]
            / "src/almanak/gateway/artifacts/arbitrum-502893237-authority.json"
        ).read_text()
    )
    assert artifact["contracts"] == [reader.identity(token["address"]).model_dump(mode="json")]
    source = captured("curve-arbitrum-implementation-" + token["implementation"])
    slot = next(
        item
        for item in source["storageLayout"]["storage"]
        if item["label"] == "balanceAndBlacklistStates"
    )
    assert slot["slot"] == "9" and slot["type"] == "t_mapping(t_address,t_uint256)"
