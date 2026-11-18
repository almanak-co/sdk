"""Stock dispatch pins the immutable beacon, registry, implementation and chain."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.gateway import robinhood_stock as stock
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import Block, Chain
from tests.test_curve_reader import CapturedReader, request_key

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-robinhood-stock-discovery"
)


def captured(name):
    return json.loads(gzip.decompress((EVIDENCE / (name + ".json.gz")).read_bytes()))


@pytest.fixture
def identity_reader():
    data = captured("stock-identities")
    block = Block.model_validate_json(json.dumps(data["block"]))
    with CapturedReader(SimpleNamespace(chain=block.chain), data["records"]) as reader:
        assert reader.pin_block(block.number) == block
        yield data, reader, AuthorityReader(reader, block)


@pytest.mark.parametrize("index", range(3))
def test_three_actual_stock_proxies_retain_the_complete_upgrade_identity(identity_reader, index):
    data, reader, authority = identity_reader
    expected = ContractIdentity.model_validate_json(json.dumps(data["tokens"][index]["identity"]))
    assert authority.identity(expected.address) == expected
    assert expected.beacon == stock.BEACON and expected.beacon_hash == stock.BEACON_HASH
    assert expected.implementation_hash == stock.STOCK_HASH
    assert expected.implementation_slot is None


@pytest.mark.parametrize(
    "change",
    [
        "chain",
        "stored_beacon",
        "beacon_code",
        "implementation_code",
        "missing_implementation",
        "proxy_hash",
    ],
)
def test_unverified_dispatch_or_dependency_fails_closed(identity_reader, change):
    data, reader, authority = identity_reader
    expected = data["tokens"][0]["identity"]
    address = expected["address"]
    if change == "chain":
        reader.chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    elif change == "proxy_hash":
        with pytest.raises(DataError, match="verified Robinhood"):
            stock.identity(authority, address, "0x" + "00" * 32)
        return
    elif change == "stored_beacon":
        key = request_key("eth_getStorageAt", [address, hex(stock.BEACON_SLOT), authority.tag])
        reader.records[key] = "0x" + "00" * 32
    else:
        target = stock.BEACON if change == "beacon_code" else expected["implementation"]
        key = request_key("eth_getCode", [target, authority.tag])
        reader.records[key] = "0x" if change == "missing_implementation" else "0x6000"
    with pytest.raises(DataError, match="Stock"):
        authority.identity(address)


@pytest.mark.parametrize(
    "name,expected_hash,site_count",
    [
        ("Stock", stock.STOCK_HASH, 17),
        ("BeaconProxy", stock.PROXY_HASH, 1),
        ("AccessControlsRegistry", stock.BEACON_HASH, 0),
    ],
)
def test_compiler_executable_equality_binds_only_declared_immutables(
    name, expected_hash, site_count
):
    from eth_utils import keccak

    match = captured("source/" + name + "-match")
    actual = bytes.fromhex(match["onchain_runtime"][2:])
    bound = bytes.fromhex(match["bound_runtime"])
    assert "0x" + keccak(actual).hex() == expected_hash
    assert actual[-11:] == bound[-11:] == bytes.fromhex("64736f6c63430008210033")
    assert int.from_bytes(actual[-2:], "big") == 51
    assert actual[:-53] == bound[:-53] and actual[-53:] != bound[-53:]
    output = next(iter(match["output"]["contracts"].values()))[name]["evm"]["deployedBytecode"]
    compiled = bytearray.fromhex(output["object"])
    sites = [site for rows in output["immutableReferences"].values() for site in rows]
    assert len(sites) == site_count
    for site in sites:
        assert site["length"] == 32
        value = int(stock.BEACON, 16).to_bytes(32, "big")
        assert actual[site["start"] : site["start"] + 32] == value
        compiled[site["start"] : site["start"] + 32] = value
    assert bytes(compiled) == bound
