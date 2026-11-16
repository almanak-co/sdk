"""Polygon evidence binds native identity, bridged assets and actual proxy slots."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.gateway.permission_models import ContractIdentity
from almanak.gateway.roles_storage import AuthorityReader, mapping_slot
from almanak.gateway.safe_funding import fund_polygon_weth_storage, fund_wrapped
from almanak.models import Block

ROOT = Path(__file__).parents[1]
DIRECTORY = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/curve-polygon-token-discovery"
FUND = "0x" + "12" * 20


def captured(name):
    path = DIRECTORY / ("curve-polygon-agent-" + name + ".json.gz")
    return json.loads(gzip.decompress(path.read_bytes()))


def authority():
    evidence = captured("token-verification")
    block = Block.model_validate_json(json.dumps(evidence["block"]))
    replies = {
        json.dumps([r["method"], r["params"]], sort_keys=True): r["result"]
        for r in evidence["records"]
    }

    def rpc(method, params):
        return replies[json.dumps([method, params], sort_keys=True)]

    reader = SimpleNamespace(_pinned=block, _rpc=rpc)
    return AuthorityReader(reader, block), {row["label"]: row for row in evidence["tokens"]}


@pytest.mark.parametrize("label", ["usdc", "crvusd"])
def test_proxy_identity_uses_source_verified_implementation_slot(label):
    reader, tokens = authority()
    row = tokens[label]
    identity = reader.identity(row["address"])
    assert identity.implementation == row["implementation"]
    assert identity.implementation_slot == row["implementation_slot"]
    assert identity.implementation_hash == row["implementation_verification"]["runtime_keccak"]
    assert "beacon" not in identity.model_dump()
    assert ContractIdentity.model_validate_json(identity.model_dump_json()) == identity


@pytest.mark.parametrize("label", ["usdc", "crvusd"])
@pytest.mark.parametrize("mutation", ["implementation_address", "implementation_code"])
def test_proxy_changes_invalidate_retained_authority(monkeypatch, label, mutation):
    reader, tokens = authority()
    row = tokens[label]
    retained = reader.identity(row["address"])
    original = reader.code
    if mutation == "implementation_address":
        monkeypatch.setattr(reader, "storage", lambda *_: int(FUND, 16))
        monkeypatch.setattr(
            reader,
            "code",
            lambda address: original(row["implementation"] if address == FUND else address),
        )
    else:
        monkeypatch.setattr(
            reader,
            "code",
            lambda address: b"changed" if address == row["implementation"] else original(address),
        )
    assert reader.identity(row["address"]) != retained


def test_usdc_fixture_identity_and_slot_match_retained_compiler_layout():
    reader, tokens = authority()
    number = reader.block.number
    artifact = json.loads(
        (ROOT / f"src/almanak/gateway/artifacts/polygon-{number}-authority.json").read_text()
    )
    expected = next(
        row for row in artifact["contracts"] if row["address"] == tokens["usdc"]["address"]
    )
    assert expected == reader.identity(tokens["usdc"]["address"]).model_dump(mode="json")
    layout = captured("usdc-implementation")["storageLayout"]["storage"]
    slot = next(row for row in layout if row["label"] == "balanceAndBlacklistStates")
    assert slot["slot"] == "9" and slot["type"] == "t_mapping(t_address,t_uint256)"


def test_native_pol_is_distinct_from_bridged_weth_and_matches_pinned_genesis():
    evidence = captured("token-verification")
    network = evm_network(ChainId("eip155:137"))
    assert network.native_asset == AssetType("eip155:137/slip44:966")
    assert network.genesis == evidence["genesis"]["hash"]
    assert network.wrapped_native == AssetType(
        "eip155:137/erc20:0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
    )
    verified = captured("wpol-verification")
    assert verified["exact_runtime_from_official_node"]
    assert verified["compiled_runtime_equal_excluding_43_byte_solc_0_4_26_metadata"]


@pytest.fixture
def node():
    evidence = captured("token-verification")
    block = Block.model_validate_json(json.dumps(evidence["block"]))
    token = next(row for row in evidence["tokens"] if row["label"] == "weth")
    calls = []

    def rpc(method, params):
        calls.append((method, params))
        return token["code"]

    def validate():
        calls.append(("origin_verified", []))

    return SimpleNamespace(
        origin=block, rpc=rpc, _validate_origin=validate, calls=calls, token=token
    )


def test_weth_fixture_uses_verified_erc20_balance_slot_without_wrapping(node):
    token = node.token["address"]
    layout = captured("weth-source")["storageLayout"]["storage"]
    assert next(row for row in layout if row["label"] == "_balances")["slot"] == "0"
    fund_polygon_weth_storage(node, FUND, token, 10**16)
    assert node.calls == [
        ("origin_verified", []),
        ("eth_getCode", [token, "latest"]),
        (
            "anvil_setStorageAt",
            [
                token,
                "0x" + mapping_slot("address", FUND, 0).to_bytes(32, "big").hex(),
                "0x" + (10**16).to_bytes(32, "big").hex(),
            ],
        ),
    ]
    funding = SimpleNamespace(
        balance=SimpleNamespace(asset=AssetType("eip155:137/erc20:" + token), amount=1)
    )
    with pytest.raises(ValueError, match="wrapped funding"):
        fund_wrapped(node, None, FUND, funding)


@pytest.mark.parametrize("amount", [-1, True, 2**255])
def test_invalid_capital_cannot_edit_polygon_weth_storage(node, amount):
    with pytest.raises(ValueError, match="verified bridged"):
        fund_polygon_weth_storage(node, FUND, node.token["address"], amount)
    assert node.calls == [("origin_verified", [])]


@pytest.mark.parametrize("mutation", ["chain", "token", "code", "origin"])
def test_wrong_scope_or_runtime_refuses_polygon_weth_fixture(node, mutation):
    token = node.token["address"]
    if mutation == "chain":
        node.origin = SimpleNamespace(chain=SimpleNamespace(chain_id=ChainId("eip155:1")))
    elif mutation == "token":
        token = FUND
    elif mutation == "code":
        node.rpc = lambda *_: "0x6000"
    else:

        def refuse():
            raise ValueError("unidentified fork")

        node._validate_origin = refuse
    with pytest.raises(ValueError):
        fund_polygon_weth_storage(node, FUND, token, 1)
    assert not any(method == "anvil_setStorageAt" for method, _ in node.calls)


def test_polygon_native_currency_adapter_refuses_ethereum_native_identity():
    from almanak.gateway.evm import currency_address

    chain = ChainId("eip155:137")
    assert currency_address(AssetType("eip155:137/slip44:966"), chain) == "0x" + "00" * 20
    with pytest.raises(ValueError, match="ERC-20 adapter"):
        currency_address(AssetType("eip155:137/slip44:60"), chain)
