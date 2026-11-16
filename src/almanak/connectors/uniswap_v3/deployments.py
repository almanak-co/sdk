"""Compiled revision checks admit configured markets without pool or token allowlists."""

import json
from functools import cache
from pathlib import Path

from almanak.chains import evm_network
from almanak.connectors import evm_revision as compiled
from almanak.connectors.evm_revision import immutable_words as immutable_words
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import erc20_address
from almanak.gateway.permission_models import ContractIdentity


@cache
def revisions():
    path = Path(__file__).with_name("artifacts") / "compiled-revisions.json"
    return json.loads(path.read_text())


def verify_code(role, raw):
    return compiled.verify_code(raw, revisions()[role])


def read_code(reader, address, block, role):
    return compiled.read_code(reader, address, block, revisions()[role], role)


def match(values, expected):
    if any(values.get(name) != value for name, value in expected.items()):
        raise DataError("identity", "constructor immutables differ from configured V3 roles")


def verify_pool(reader, source, block):
    if source.chain != block.chain:
        raise DataError("chain_mismatch", "pool belongs to another execution scope")
    factory_values, factory = read_code(reader, source.factory, block, "factory")
    match(factory_values, {"original": int(source.factory, 16)})
    values, pool = read_code(reader, source.pool, block, "pool")
    count = 2 * (887272 // source.tick_spacing) + 1
    match(
        values,
        {
            "factory": int(source.factory, 16),
            "token0": int(erc20_address(source.currency0, source.chain.chain_id), 16),
            "token1": int(erc20_address(source.currency1, source.chain.chain_id), 16),
            "fee": source.fee,
            "tickSpacing": source.tick_spacing,
            "maxLiquidityPerTick": (2**128 - 1) // count,
            "original": int(source.pool, 16),
        },
    )
    (registered,) = reader.call(
        source.factory,
        "getPool(address,address,uint24)",
        block,
        inputs=("address", "address", "uint24"),
        args=(source.currency0.reference, source.currency1.reference, source.fee),
        outputs=("address",),
    )
    (spacing,) = reader.call(
        source.factory,
        "feeAmountTickSpacing(uint24)",
        block,
        inputs=("uint24",),
        args=(source.fee,),
        outputs=("int24",),
    )
    if registered != source.pool or spacing != source.tick_spacing:
        raise DataError("identity", "configured pool differs from its factory registration")
    return factory, pool


def verify_periphery(reader, source, contracts, block):
    expected = {
        "factory": int(source.factory, 16),
        "WETH9": int(
            erc20_address(evm_network(source.chain.chain_id).wrapped_native, source.chain.chain_id),
            16,
        ),
    }
    identities = []
    for role, address in contracts.model_dump().items():
        values, identity = read_code(reader, address, block, role)
        match(values, expected)
        if role == "router":
            match(values, {"positionManager": int(contracts.position_manager, 16)})
        identities.append(identity)
    # Descriptor and V2 factory immutables cannot affect the exact V3 execution
    # templates. Token metadata and V2 router methods receive no authority.
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
