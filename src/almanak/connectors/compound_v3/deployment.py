"""Comet proxy, implementation, extension and asset-list revision correlations."""

import json
from pathlib import Path

from eth_utils import keccak

from almanak.connectors.compound_v3.domain import Market, Revision
from almanak.connectors.evm_revision import read_code, read_matching_code
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS, Address, Model, address

REVISIONS = json.loads(Path(__file__).with_name("revisions.json").read_text())
IMPLEMENTATION_SLOT = int.from_bytes(keccak(text="eip1967.proxy.implementation")) - 1


class Deployment(Model):
    revision: Revision
    implementation: Address
    extension: Address
    asset_list: Address
    contracts: tuple[ContractIdentity, ...]
    configuration: dict[str, int]


def immutable_address(word):
    if type(word) is not int or not 0 < word < 2**160:
        raise DataError("revision", "Comet constructor address is missing or malformed")
    return address("0x" + word.to_bytes(20).hex())


def implementation_address(reader, market, block):
    raw = reader._rpc(
        "eth_getStorageAt",
        [
            market.comet,
            hex(IMPLEMENTATION_SLOT),
            {"blockHash": block.hash, "requireCanonical": True},
        ],
    )
    try:
        if len(raw) != 66 or not raw.startswith("0x"):
            raise ValueError("invalid storage word")
        return immutable_address(int(raw, 16))
    except (TypeError, ValueError, AttributeError):
        raise DataError("revision", "Comet proxy implementation slot is malformed") from None


def configured_market(market, values):
    if immutable_address(values["baseToken"]) != market.base_asset.reference:
        raise DataError("market", "Comet base token differs from the configured market")
    if immutable_address(values["baseTokenPriceFeed"]) != market.base_price_feed:
        raise DataError("market", "Comet base price feed differs from the configured source")
    if values["baseScale"] != 10 ** values["decimals"] or values["decimals"] > 18:
        raise DataError("revision", "Comet base precision differs from its constructor scale")
    if values["numAssets"] > 24:
        raise DataError("revision", "Comet asset list exceeds the verified implementation")


def read_deployment(reader, market: Market, block) -> Deployment:
    if reader.chain != market.chain or market.chain != block.chain or reader._pinned != block:
        raise DataError("block_mismatch", "Comet verification requires its configured pinned chain")
    _, _, proxy = read_matching_code(
        reader,
        market.comet,
        block,
        tuple((name, row["proxy"]) for name, row in REVISIONS.items()),
    )
    implementation = implementation_address(reader, market, block)
    name, values, implementation_identity = read_matching_code(
        reader,
        implementation,
        block,
        tuple((name, row["implementation"]) for name, row in REVISIONS.items()),
    )
    configured_market(market, values)
    extension = immutable_address(values["extensionDelegate"])
    asset_list = immutable_address(values["assetList"])
    if len({market.comet, implementation, extension, asset_list, ZERO_ADDRESS}) != 5:
        raise DataError("revision", "Comet deployment roles overlap or are absent")
    _, extension_identity = read_code(reader, extension, block, REVISIONS[name]["extension"])
    _, asset_list_identity = read_code(reader, asset_list, block, REVISIONS[name]["asset_list"])
    proxy = proxy.model_copy(
        update={
            "implementation": implementation,
            "implementation_hash": implementation_identity.code_hash,
            "implementation_slot": IMPLEMENTATION_SLOT,
        }
    )
    return Deployment(
        revision=Revision(name),
        implementation=implementation,
        extension=extension,
        asset_list=asset_list,
        configuration=values,
        contracts=(proxy, implementation_identity, extension_identity, asset_list_identity),
    )
