"""TwoCrypto identities bind original deployment code and current quote code."""

import json
from dataclasses import dataclass
from functools import cache
from pathlib import Path

from eth_utils import keccak

from almanak.connectors.evm_revision import read_code, read_matching_code
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.permission_models import ContractIdentity


@cache
def revision(filename="twocrypto-v2.1.0.json"):
    return json.loads((Path(__file__).with_name("artifacts") / filename).read_text())


def words(value, count):
    encoded = value.to_bytes(count * 32, "big")
    return tuple(
        int.from_bytes(encoded[start : start + 32]) for start in range(0, len(encoded), 32)
    )


@dataclass(frozen=True)
class Deployment:
    factory: ContractIdentity
    blueprint: ContractIdentity
    pool: ContractIdentity
    math: ContractIdentity
    views: ContractIdentity
    precisions: tuple[int, ...]

    @property
    def identities(self):
        return self.factory, self.blueprint, self.pool, self.math, self.views


def storage(reader, address, slot, block):
    return int(
        reader._rpc(
            "eth_getStorageAt",
            [address, hex(slot), {"blockHash": block.hash, "requireCanonical": True}],
        ),
        16,
    )


def original_blueprint(reader, source, block, *, pool_data_slot=9):
    # Vyper 0.3.10 hashes the mapping slot before its address key. PoolArray's
    # implementation is the sixth word, after gauge, coins and decimals.
    encoded = pool_data_slot.to_bytes(32, "big") + int(source.pool, 16).to_bytes(32, "big")
    slot = int.from_bytes(keccak(encoded)) + 5
    value = storage(reader, source.factory, slot, block)
    if not 0 < value < 2**160:
        raise DataError("identity", "TwoCrypto factory lacks a valid original pool blueprint")
    return "0x" + value.to_bytes(20, "big").hex()


def verify_immutables(source, values):
    expected = tuple(int(coin.reference, 16) for coin in source.coins)
    if words(values["coins"], 2) != expected or values["factory"] != int(source.factory, 16):
        raise DataError(
            "identity", "TwoCrypto immutable factory or coin order differs from configuration"
        )
    if values["CACHED_CHAIN_ID"] != chain_number(source.chain.chain_id):
        raise DataError(
            "identity", "TwoCrypto immutable deployment chain differs from configuration"
        )
    return words(values["PRECISIONS"], 2)


def factory_revision(reader, source, block):
    filename, _, factory = read_matching_code(
        reader,
        source.factory,
        block,
        tuple(
            (name, revision(name)["factory"])
            for name in ("twocrypto-v2.1.0.json", "twocrypto-core-v2.1.0.json")
        ),
    )
    compiled = revision(filename)
    return compiled, factory


def blueprint_slot(compiled):
    # The deployed core factory removed one field before pool_data. Bind the
    # original implementation lookup to the same verified factory revision.
    if "factory_storage_layout" in compiled:
        return compiled["factory_storage_layout"]["pool_data"]["slot"]
    return 9


def verify_pool(reader, source, block):
    if source.family != "twocrypto_ng" or len(source.coins) != 2:
        raise DataError("unsupported_pool", "this Crypto revision requires a TwoCrypto NG pool")
    if reader.chain != source.chain or block.chain != source.chain:
        raise DataError("chain_mismatch", "TwoCrypto pool belongs to another execution scope")
    compiled, factory = factory_revision(reader, source, block)
    values, pool = read_code(reader, source.pool, block, compiled["pool"])
    precisions = verify_immutables(source, values)
    blueprint_address = original_blueprint(
        reader, source, block, pool_data_slot=blueprint_slot(compiled)
    )
    _, blueprint = read_code(reader, blueprint_address, block, compiled["blueprint"])
    math_address = "0x" + values["MATH"].to_bytes(20, "big").hex()
    _, math = read_code(reader, math_address, block, compiled["math"])
    views_address = reader.call(
        source.factory, "views_implementation()", block, outputs=("address",)
    )[0]
    _, views = read_code(reader, views_address, block, compiled["views"])
    coins = reader.call(
        source.factory,
        "get_coins(address)",
        block,
        inputs=("address",),
        args=(source.pool,),
        outputs=("address[2]",),
    )[0]
    if tuple(coins) != tuple(coin.reference for coin in source.coins):
        raise DataError("identity", "TwoCrypto factory registration has a different coin order")
    return Deployment(factory, blueprint, pool, math, views, precisions)
