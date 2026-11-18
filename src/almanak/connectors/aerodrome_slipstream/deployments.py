"""Closed compiled revisions, exact clones and configured factory correlations."""

import json
from functools import cache
from pathlib import Path

from eth_utils import keccak

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


def read_code(reader, address, block, role):
    return compiled.read_code(reader, address, block, revisions()[role], role)


def match(values, expected):
    if any(values.get(name) != value for name, value in expected.items()):
        raise DataError(
            "identity", "Slipstream constructor immutables differ from configured roles"
        )


def address_word(value):
    if not 0 < value < 2**160:
        raise DataError("identity", "Slipstream immutable dependency is not a nonzero address")
    return "0x" + value.to_bytes(20, "big").hex()


def verify_factory(reader, source, block):
    values, factory = read_code(reader, source.factory, block, "factory")
    for name in ("voter", "factoryRegistry"):
        address_word(values[name])
    implementation = address_word(values["poolImplementation"])
    _, identity = read_code(reader, implementation, block, "pool")
    return factory, identity


def fee_modules(reader, source, block):
    return {
        name: reader.call(source.factory, signature, block, outputs=("address",))[0]
        for name, signature in (
            ("swap_fee_module", "swapFeeModule()"),
            ("unstaked_fee_module", "unstakedFeeModule()"),
        )
    }


def verify_fee_modules(reader, source, block):
    identities = []
    for role, address in fee_modules(reader, source, block).items():
        if address == ZERO_ADDRESS:
            continue
        values, identity = read_code(reader, address, block, role)
        match(values, {"factory": int(source.factory, 16)})
        identities.append(identity)
    return tuple(identities)


def clone_revision(implementation):
    raw = bytes.fromhex(
        "363d3d373d3d3d363d73" + implementation[2:] + "5af43d82803e903d91602b57fd5bf3"
    )
    return {
        "runtime_length": len(raw),
        "normalized_code_hash": "0x" + keccak(raw).hex(),
        "immutables": {},
    }


def verify_pool(reader, source, block):
    if source.chain != block.chain:
        raise DataError("chain_mismatch", "Slipstream pool belongs to another execution scope")
    factory, implementation = verify_factory(reader, source, block)
    _, pool = compiled.read_code(reader, source.pool, block, clone_revision(implementation.address))
    verify_pool_storage(reader, source, block)
    registered = reader.call(
        source.factory,
        "getPool(address,address,int24)",
        block,
        inputs=("address", "address", "int24"),
        args=(source.currency0.reference, source.currency1.reference, source.tick_spacing),
        outputs=("address",),
    )[0]
    if registered != source.pool:
        raise DataError("identity", "configured Slipstream pool differs from factory registration")
    return factory, implementation, pool, *verify_fee_modules(reader, source, block)


def verify_pool_storage(reader, source, block):
    expected = {
        "factory": source.factory,
        "token0": source.currency0.reference,
        "token1": source.currency1.reference,
    }
    for name, address in expected.items():
        if reader.call(source.pool, name + "()", block, outputs=("address",))[0] != address:
            raise DataError("identity", "Slipstream pool storage differs from configured identity")
    spacing = reader.call(source.pool, "tickSpacing()", block, outputs=("int24",))[0]
    maximum = reader.call(source.pool, "maxLiquidityPerTick()", block, outputs=("uint128",))[0]
    count = 2 * (887272 // source.tick_spacing) + 1
    if spacing != source.tick_spacing or maximum != (2**128 - 1) // count:
        raise DataError("identity", "Slipstream pool spacing or maximum liquidity differs")


def verify_periphery(reader, source, contracts, block):
    expected = {
        "factory": int(source.factory, 16),
        "WETH9": int(evm_network(source.chain.chain_id).wrapped_native.reference, 16),
    }
    identities = []
    for role, address in contracts.model_dump().items():
        values, identity = read_code(reader, address, block, role)
        match(values, expected)
        identities.append(identity)
    manager = reader.call(source.pool, "nft()", block, outputs=("address",))[0]
    if manager not in (ZERO_ADDRESS, contracts.position_manager):
        raise DataError("identity", "Slipstream pool is associated with another NFT manager")
    return tuple(identities)


def require_no_discount(reader, source, block, submitter=ZERO_ADDRESS):
    module = fee_modules(reader, source, block)["swap_fee_module"]
    if module == ZERO_ADDRESS:
        return
    for address in sorted({ZERO_ADDRESS, submitter}):
        discount = reader.call(
            module,
            "discounted(address)",
            block,
            inputs=("address",),
            args=(address,),
            outputs=("uint24",),
        )[0]
        if discount:
            raise DataError(
                "unsupported_fee_context", "Slipstream submitter discounts are unsupported"
            )


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
