"""Closed runtime revisions and correlated factory, pool, fee and router roles."""

import json
from functools import cache
from pathlib import Path

from eth_abi import encode
from eth_utils import keccak

from almanak.chains import evm_network
from almanak.connectors import evm_revision as compiled
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.permission_models import ContractIdentity


@cache
def revisions():
    return json.loads(
        (Path(__file__).with_name("artifacts") / "compiled-revisions.json").read_text()
    )


def read_code(reader, address, block, role):
    return compiled.read_code(reader, address, block, revisions()[role], role)


def match(values, expected):
    if any(values.get(name) != value for name, value in expected.items()):
        raise DataError("identity", "Aerodrome constructor immutables differ from configured roles")


def address_word(value):
    if not 0 < value < 2**160:
        raise DataError("identity", "Aerodrome immutable dependency is not a nonzero address")
    return "0x" + value.to_bytes(20, "big").hex()


def implementation_domain(address, chain):
    hashed_name, hashed_version = keccak(b""), keccak(b"1")
    domain_type = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
    )
    chain_id = chain_number(chain.chain_id)
    separator = keccak(
        encode(
            ("bytes32", "bytes32", "bytes32", "uint256", "address"),
            (domain_type, hashed_name, hashed_version, chain_id, address),
        )
    )
    return {
        "_cachedDomainSeparator": int.from_bytes(separator),
        "_cachedChainId": chain_id,
        "_cachedThis": int(address, 16),
        "_name": 0,
        "_version": int.from_bytes(b"1" + b"\0" * 30 + b"\x01"),
        "_hashedName": int.from_bytes(hashed_name),
        "_hashedVersion": int.from_bytes(hashed_version),
    }


def verify_factory(reader, source, block):
    values, factory = read_code(reader, source.factory, block, "factory")
    implementation = address_word(values["implementation"])
    values, identity = read_code(reader, implementation, block, "implementation")
    match(values, implementation_domain(implementation, source.chain))
    return factory, identity


def clone_revision(implementation):
    raw = bytes.fromhex(
        "363d3d373d3d3d363d73" + implementation[2:] + "5af43d82803e903d91602b57fd5bf3"
    )
    return {
        "runtime_length": len(raw),
        "normalized_code_hash": "0x" + keccak(raw).hex(),
        "immutables": {},
    }


def verify_storage(reader, source, block):
    expected = {
        "factory": source.factory,
        "token0": source.currency0.reference,
        "token1": source.currency1.reference,
    }
    for name, address in expected.items():
        if reader.call(source.pool, name + "()", block, outputs=("address",))[0] != address:
            raise DataError("identity", "Aerodrome pool storage differs from configured identity")
    if reader.call(source.pool, "stable()", block, outputs=("bool",))[0] != source.stable:
        raise DataError("identity", "Aerodrome pool invariant differs from configured identity")
    registered = reader.call(
        source.factory,
        "getPool(address,address,bool)",
        block,
        inputs=("address", "address", "bool"),
        args=(source.currency0.reference, source.currency1.reference, source.stable),
        outputs=("address",),
    )[0]
    if registered != source.pool:
        raise DataError("identity", "Aerodrome pool differs from factory registration")


def verify_fees(reader, source, block):
    address = reader.call(source.pool, "poolFees()", block, outputs=("address",))[0]
    values, identity = read_code(reader, address, block, "pool_fees")
    match(
        values,
        {
            "pool": int(source.pool, 16),
            "token0": int(source.currency0.reference, 16),
            "token1": int(source.currency1.reference, 16),
        },
    )
    return identity


def verify_pool(reader, source, block):
    if source.chain != block.chain:
        raise DataError("chain_mismatch", "Aerodrome pool belongs to another execution scope")
    factory, implementation = verify_factory(reader, source, block)
    _, pool = compiled.read_code(reader, source.pool, block, clone_revision(implementation.address))
    verify_storage(reader, source, block)
    return factory, implementation, pool, verify_fees(reader, source, block)


def verify_router(reader, source, contracts, block):
    values, router = read_code(reader, contracts.router, block, "router")
    match(
        values,
        {
            "defaultFactory": int(source.factory, 16),
            "weth": int(evm_network(source.chain.chain_id).wrapped_native.reference, 16),
        },
    )
    address_word(values["_trustedForwarder"])
    address_word(values["voter"])
    registry = address_word(values["factoryRegistry"])
    values, identity = read_code(reader, registry, block, "factory_registry")
    match(values, {"fallbackPoolFactory": int(source.factory, 16)})
    approved = reader.call(
        registry,
        "isPoolFactoryApproved(address)",
        block,
        inputs=("address",),
        args=(source.factory,),
        outputs=("bool",),
    )[0]
    if not approved:
        raise DataError("identity", "Aerodrome router registry does not approve configured factory")
    return router, identity


def require_direct_fund(reader, target, block):
    values, _ = read_code(reader, target.contracts.router, block, "router")
    if int(target.fund.address, 16) == values["_trustedForwarder"]:
        raise DataError("unsupported_caller", "Aerodrome fund cannot act as the trusted forwarder")


def identities(config, account, reader):
    retained: dict[str, ContractIdentity] = {}
    for allocation in config.allocations:
        if allocation.account != account:
            continue
        target = config.target(allocation.name)
        require_direct_fund(reader.reader, target, reader.block)
        contracts = (
            *verify_pool(reader.reader, allocation.pool, reader.block),
            *verify_router(reader.reader, allocation.pool, allocation.contracts, reader.block),
            *(reader.identity(asset.reference) for asset in target.assets),
        )
        for contract in contracts:
            if retained.setdefault(contract.address, contract) != contract:
                raise DataError(
                    "identity", "Aerodrome contract identity differs across allocations"
                )
    return tuple(retained[address] for address in sorted(retained))
