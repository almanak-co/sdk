"""Compiled Pancake roles and factory correlation, independent of sample markets."""

import json
from functools import cache
from pathlib import Path

from almanak.chains import evm_network
from almanak.connectors import evm_revision as compiled
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS


@cache
def revisions():
    return json.loads(
        (Path(__file__).with_name("artifacts") / "compiled-revisions.json").read_text()
    )


def verify_code(role, raw):
    return compiled.verify_code(raw, revisions()[role])


def read_code(reader, address, block, role):
    return compiled.read_code(reader, address, block, revisions()[role], role)


def match(values, expected):
    if any(values.get(name) != value for name, value in expected.items()):
        raise DataError("identity", "constructor immutables differ from configured Pancake roles")


def factory_deployer(reader, source, block):
    values, factory = read_code(reader, source.factory, block, "factory")
    if not 0 < values["poolDeployer"] < 2**160:
        raise DataError("identity", "Pancake factory has an invalid deployer address")
    deployer = "0x" + values["poolDeployer"].to_bytes(20, "big").hex()
    return deployer, factory


def verify_factory(reader, source, block):
    deployer, factory = factory_deployer(reader, source, block)
    _, identity = read_code(reader, deployer, block, "deployer")
    bound = reader.call(deployer, "factoryAddress()", block, outputs=("address",))[0]
    if bound != source.factory:
        raise DataError("identity", "Pancake pool deployer belongs to another factory")
    return factory, identity


def verify_pool(reader, source, block):
    if source.chain != block.chain:
        raise DataError("chain_mismatch", "Pancake pool belongs to another execution scope")
    factory, deployer = verify_factory(reader, source, block)
    pool = verify_pool_code(reader, source, block)
    registered = reader.call(
        source.factory,
        "getPool(address,address,uint24)",
        block,
        inputs=("address", "address", "uint24"),
        args=(source.currency0.reference, source.currency1.reference, source.fee),
        outputs=("address",),
    )[0]
    spacing = reader.call(
        source.factory,
        "feeAmountTickSpacing(uint24)",
        block,
        inputs=("uint24",),
        args=(source.fee,),
        outputs=("int24",),
    )[0]
    if registered != source.pool or spacing != source.tick_spacing:
        raise DataError("identity", "configured Pancake pool differs from factory registration")
    return factory, deployer, pool, *verify_callback(reader, source, block)


def verify_pool_code(reader, source, block):
    values, pool = read_code(reader, source.pool, block, "pool")
    count = 2 * (887272 // source.tick_spacing) + 1
    match(
        values,
        {
            "factory": int(source.factory, 16),
            "token0": int(source.currency0.reference, 16),
            "token1": int(source.currency1.reference, 16),
            "fee": source.fee,
            "tickSpacing": source.tick_spacing,
            "maxLiquidityPerTick": (2**128 - 1) // count,
        },
    )
    return pool


def verify_callback(reader, source, block):
    callback = reader.call(source.pool, "lmPool()", block, outputs=("address",))[0]
    if callback == ZERO_ADDRESS:
        return ()
    try:
        revision, values, identity = compiled.read_matching_code(
            reader,
            callback,
            block,
            tuple((role, revisions()[role]) for role in ("lm_v4", "lm_v1")),
        )
    except DataError as error:
        if error.kind != "revision":
            raise
        raise DataError(
            "revision", "Pancake liquidity-mining callback revision is unverified"
        ) from error
    match(values, {"pool": int(source.pool, 16)})
    if any(not 0 < value < 2**160 for value in values.values()):
        raise DataError("identity", "Pancake LM immutable dependency has an invalid address word")
    if (
        revision == "lm_v4"
        and not reader.call(callback, "initialization()", block, outputs=("bool",))[0]
    ):
        raise DataError("state", "Pancake LM callback migration is not initialized")
    # Verified swap entry points write only LM accounting. Calls to MasterChef
    # and predecessor LM contracts are static; unstaked NFTs receive no rewards.
    return (identity,)


def verify_periphery(reader, source, contracts, block):
    _, deployer = verify_factory(reader, source, block)
    expected = {
        "deployer": int(deployer.address, 16),
        "factory": int(source.factory, 16),
        "WETH9": int(evm_network(source.chain.chain_id).wrapped_native.reference, 16),
    }
    identities = []
    for role, address in contracts.model_dump().items():
        values, identity = read_code(reader, address, block, role)
        match(values, expected)
        identities.append(identity)
    return tuple(identities)


def identities(config, account, reader):
    retained: dict[str, ContractIdentity] = {}
    for allocation in config.allocations:
        if allocation.account != account:
            continue
        contracts = (
            *verify_pool(reader.reader, allocation.pool, reader.block),
            *verify_periphery(reader.reader, allocation.pool, allocation.contracts, reader.block),
            reader.identity(allocation.pool.currency0.reference),
            reader.identity(allocation.pool.currency1.reference),
        )
        for contract in contracts:
            retain_identity(retained, contract)
    return tuple(retained[address] for address in sorted(retained))


def retain_identity(retained, contract):
    if retained.setdefault(contract.address, contract) != contract:
        raise DataError("identity", "V3 contract identity differs across allocations")
