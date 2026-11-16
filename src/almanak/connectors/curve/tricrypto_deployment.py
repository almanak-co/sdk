"""Bind TriCrypto pool code, immutable assets and its active quote implementation."""

import json
from dataclasses import dataclass
from functools import cache
from pathlib import Path

from almanak.chains import evm_network
from almanak.connectors.curve.crypto_deployment import words
from almanak.connectors.evm_revision import read_matching_code
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.permission_models import ContractIdentity


@cache
def revision(filename="tricrypto-v2.0.0.json"):
    return json.loads((Path(__file__).with_name("artifacts") / filename).read_text())


def revision_files(implementation_revision=None):
    if implementation_revision == "tricrypto-ng-2024-01-04":
        return ("tricrypto-2024-01-04.json", "tricrypto-core-v2.0.0.json")
    return ("tricrypto-v2.0.0.json", "tricrypto-v2.0.0-l2.json", "tricrypto-v2.0.0-paris.json")


def verified_component(reader, address, block, component, implementation_revision=None):
    candidates = tuple(
        (filename, revision(filename)[component])
        for filename in revision_files(implementation_revision)
    )
    _, values, identity = read_matching_code(reader, address, block, candidates)
    return values, identity


@dataclass(frozen=True)
class Deployment:
    factory: ContractIdentity
    pool: ContractIdentity
    math: ContractIdentity
    views: ContractIdentity
    precisions: tuple[int, ...]
    wrapped_native: str | None

    @property
    def identities(self):
        return self.factory, self.pool, self.math, self.views


def verify_immutables(source, values):
    expected = tuple(int(coin.reference, 16) for coin in source.coins)
    if words(values["coins"], 3) != expected:
        raise DataError("identity", "TriCrypto immutable coin order differs from configuration")
    if values["CACHED_CHAIN_ID"] != chain_number(source.chain.chain_id):
        raise DataError(
            "identity", "TriCrypto immutable deployment chain differs from configuration"
        )
    if source.implementation_revision == "tricrypto-ng-2024-01-04":
        if values["factory"] != int(source.factory, 16):
            raise DataError("identity", "TriCrypto immutable factory differs from configuration")
        return None
    wrapped = evm_network(source.chain.chain_id).wrapped_native.reference
    if values["WETH20"] != int(wrapped, 16):
        raise DataError("identity", "TriCrypto native wrapper differs from the chain profile")
    return wrapped


def verify_pool(reader, source, block):
    if source.family != "tricrypto_ng" or len(source.coins) != 3:
        raise DataError("unsupported_pool", "this revision requires a TriCrypto NG pool")
    if reader.chain != source.chain or block.chain != source.chain:
        raise DataError("chain_mismatch", "TriCrypto pool belongs to another execution scope")
    selected = source.implementation_revision
    _, factory = verified_component(reader, source.factory, block, "factory", selected)
    values, pool = verified_component(reader, source.pool, block, "pool", selected)
    wrapped = verify_immutables(source, values)
    owner = reader.call(source.pool, "factory()", block, outputs=("address",))[0]
    if owner != source.factory:
        raise DataError("identity", "TriCrypto recorded factory differs from configuration")
    coins = reader.call(
        source.factory,
        "get_coins(address)",
        block,
        inputs=("address",),
        args=(source.pool,),
        outputs=("address[3]",),
    )[0]
    if tuple(coins) != tuple(coin.reference for coin in source.coins):
        raise DataError("identity", "TriCrypto factory registration has a different coin order")
    math_address = "0x" + values["MATH"].to_bytes(20, "big").hex()
    _, math = verified_component(reader, math_address, block, "math", selected)
    views_address = reader.call(
        source.factory, "views_implementation()", block, outputs=("address",)
    )[0]
    _, views = verified_component(reader, views_address, block, "views", selected)
    precisions = reader.call(source.pool, "precisions()", block, outputs=("uint256[3]",))[0]
    return Deployment(factory, pool, math, views, tuple(precisions), wrapped)
