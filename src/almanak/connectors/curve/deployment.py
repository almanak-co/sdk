"""Configured pools must match their factory, blueprint, immutable coins and views."""

import json
from dataclasses import dataclass
from functools import cache
from pathlib import Path

from almanak.connectors.curve.stable_math import DECEMBER, NOVEMBER, StableRevision
from almanak.connectors.evm_revision import read_code, read_matching_code
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.permission_models import ContractIdentity

CORE = "stableswap-core-v7.0.0"


@cache
def revision():
    return json.loads((Path(__file__).with_name("artifacts") / "stable-20231115.json").read_text())


@cache
def revisions():
    december = json.loads(
        (Path(__file__).with_name("artifacts") / "stable-20231212.json").read_text()
    )
    core = json.loads(
        (Path(__file__).with_name("artifacts") / "stable-core-v7.0.0.json").read_text()
    )
    return {DECEMBER: december, NOVEMBER: revision(), CORE: core}


def read_stable_code(reader, address, block, kind):
    return read_matching_code(
        reader, address, block, tuple((name, value[kind]) for name, value in revisions().items())
    )


def vector(value):
    encoded = value.to_bytes(288, "big")
    words = tuple(int.from_bytes(encoded[i : i + 32]) for i in range(0, 288, 32))
    if not 0 <= words[0] <= 8:
        raise DataError("identity", "Curve immutable vector length is invalid")
    return words[1 : words[0] + 1]


def scalar(reader, address, signature, block, *, outputs=("address",)):
    return reader.call(address, signature, block, outputs=outputs)[0]


def registered(reader, source, signature, block, outputs):
    return reader.call(
        source.factory,
        signature,
        block,
        inputs=("address",),
        args=(source.pool,),
        outputs=outputs,
    )[0]


@dataclass(frozen=True)
class Deployment:
    factory: ContractIdentity
    blueprint: ContractIdentity
    pool: ContractIdentity
    views: ContractIdentity
    rates: tuple[int, ...]
    asset_types: tuple[int, ...]
    revision: StableRevision

    @property
    def identities(self):
        return self.factory, self.blueprint, self.pool, self.views


def verify_immutables(source, values):
    expected = tuple(int(coin.reference, 16) for coin in source.coins)
    if values["factory"] != int(source.factory, 16) or vector(values["coins"]) != expected:
        raise DataError(
            "identity", "Curve immutable factory or ordered coins differ from configuration"
        )
    if (values["N_COINS"], values["N_COINS_128"]) != (len(expected), len(expected)):
        raise DataError("identity", "Curve immutable coin counts differ from configuration")
    if values["CACHED_CHAIN_ID"] != chain_number(source.chain.chain_id):
        raise DataError("identity", "Curve immutable deployment chain differs from configuration")
    types = vector(values["asset_types"])
    if types != (0,) * len(expected):
        raise DataError(
            "unsupported_asset_behavior", "this Curve revision requires standard non-rebasing coins"
        )
    if values.get("pool_contains_rebasing_tokens", 0):
        raise DataError("identity", "Curve rebasing flag differs from its standard asset types")
    return types, vector(values["rate_multipliers"])


def verify_pool(reader, source, block):
    if source.family == "tricrypto_ng":
        from almanak.connectors.curve.tricrypto_deployment import verify_pool as verify_tri

        return verify_tri(reader, source, block)
    if source.family == "twocrypto_ng":
        from almanak.connectors.curve.crypto_deployment import verify_pool as verify_crypto

        return verify_crypto(reader, source, block)
    if source.family != "stableswap_ng":
        raise DataError("unsupported_pool", "unsupported Curve pool family")
    return verify_stable_pool(reader, source, block)


def verify_stable_pool(reader, source, block):
    if reader.chain != source.chain or block.chain != source.chain:
        raise DataError("chain_mismatch", "Curve pool belongs to another execution scope")
    _, _, factory = read_stable_code(reader, source.factory, block, "factory")
    if registered(reader, source, "is_meta(address)", block, ("bool",)):
        raise DataError(
            "unsupported_pool", "Curve metapools and metapool routing are not supported"
        )
    implementation = registered(
        reader, source, "get_implementation_address(address)", block, ("address",)
    )
    pool_revision, values, pool = read_stable_code(reader, source.pool, block, "pool")
    _, blueprint = read_code(reader, implementation, block, revisions()[pool_revision]["blueprint"])
    types, rates = verify_immutables(source, values)
    coins = registered(reader, source, "get_coins(address)", block, ("address[]",))
    if tuple(coins) != tuple(coin.reference for coin in source.coins):
        raise DataError("identity", "Curve factory coin order differs from configuration")
    if (
        tuple(registered(reader, source, "get_pool_asset_types(address)", block, ("uint8[]",)))
        != types
    ):
        raise DataError("identity", "Curve factory asset types differ from pool immutables")
    address = scalar(reader, source.factory, "views_implementation()", block)
    _, _, views = read_stable_code(reader, address, block, "views")
    # Core v7 changes version metadata and compiler target; its economic source
    # and storage match the December model (retained compiled-source comparison).
    math_revision = DECEMBER if pool_revision == CORE else pool_revision
    return Deployment(factory, blueprint, pool, views, rates, types, math_revision)


def identities(config, account, reader):
    retained: dict[str, ContractIdentity] = {}
    for allocation in config.allocations:
        if allocation.account != account:
            continue
        deployment = verify_pool(reader.reader, allocation.pool, reader.block)
        tokens = tuple(reader.identity(coin.reference) for coin in allocation.pool.coins)
        for contract in (*deployment.identities, *tokens):
            if retained.setdefault(contract.address, contract) != contract:
                raise DataError("identity", "Curve contract identity differs across allocations")
    return tuple(retained[address] for address in sorted(retained))
