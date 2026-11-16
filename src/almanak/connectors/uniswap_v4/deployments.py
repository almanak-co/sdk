"""Verified deployment identity, independent of credential and fork authority."""

import json
from pathlib import Path

from almanak.connectors.uniswap_v4.custody import native_index
from almanak.connectors.uniswap_v4.hooks import logical_chain
from almanak.connectors.uniswap_v4.pool import read_pool
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS

ARTIFACTS = Path(__file__).with_name("artifacts")
# Infrastructure roles are verified separately from author-selected pool keys/assets.
NETWORK_ROLES = {
    "eip155:137": {
        "pool_manager": "0x67366782805870060151383f4bbff9dab53e5cd6",
        "position_manager": "0x1ec2ebf4f37e7363fdfe3551602425af0b3ceef9",
        "quoter": "0xb3d5c3dfc3a7aebff71895a7191796bffc2c81b9",
        "state_view": "0x5ea1bd7974c8a611cbab0bdcafcb1d9cc9b3ba5a",
        "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",
        "router": "0x1095692a6237d83c6a72f3f5efedb9a670c49223",
    },
    "eip155:4663": {
        "pool_manager": "0x8366a39cc670b4001a1121b8f6a443a643e40951",
        "position_manager": "0x58daec3116aae6d93017baaea7749052e8a04fa7",
        "quoter": "0x8dc178efb8111bb0973dd9d722ebeff267c98f94",
        "state_view": "0xf3334192d15450cdd385c8b70e03f9a6bd9e673b",
        "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",
        "router": "0x8876789976decbfcbbbe364623c63652db8c0904",
    },
    "eip155:56": {
        "pool_manager": "0x28e2ea090877bf75740558f6bfb36a5ffee9e9df",
        "position_manager": "0x7a4a5c919ae2541aed11041a1aeee68f1287f95b",
        "quoter": "0x9f75dd27d6664c475b90e105573e550ff69437b0",
        "state_view": "0xd13dd3d6e93f276fafc9db9e6bb47c1180aee0c4",
        "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",
        "router": "0x1906c1d672b88cd1b9ac7593301ca990f94eae07",
    },
    "eip155:10": {
        "pool_manager": "0x9a13f98cb987694c9f086b1f5eb990eea8264ec3",
        "position_manager": "0x3c3ea4b57a46241e54610e5f022e5c45859a1017",
        "quoter": "0x1f3131a13296fb91c90870043742c3cdbff1a8d7",
        "state_view": "0xc18a3169788f4f75a170290584eca6395c75ecdb",
        "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",
        "router": "0x851116d9223fabed8e56c0e6b8ad0c31d98b3507",
    },
    "eip155:42161": {
        "pool_manager": "0x360e68faccca8ca495c1b759fd9eee466db9fb32",
        "position_manager": "0xd88f38f930b7952f2db2432cb002e7abbf3dd869",
        "quoter": "0x3972c00f7ed4885e145823eb7c655375d275a1c5",
        "state_view": "0x76fd297e2d437cd7f76d50f01afe6160f86e9990",
        "router": "0xa51afafe0263b40edaef0df8781ea9aa03e381a3",
        "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",
    },
    "eip155:8453": {
        "pool_manager": "0x498581ff718922c3f8e6a244956af099b2652b2b",
        "position_manager": "0x7c5f5a4bbd8fd63184577525326123b519429bdc",
        "quoter": "0x0d5e0f971ed27fbff6c2837bf31316121532048d",
        "state_view": "0xa3c0c9b65bad0b08107aa264b0f3db444b867a71",
        "router": "0x6ff5693b99212da76ad316178a184ab56d299b43",
        "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",
    },
    "eip155:1": {
        "router": "0x66a9893cc07d91d95644aedd05d03f95e1dba8af",
        "position_manager": "0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e",
        "quoter": "0x52f0e24d1c21c8a0cb1e5a5dd6198556bd9e1203",
        "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",
        "pool_manager": "0x000000000004444c5dc75cb358380d2e3de08a90",
        "state_view": "0x7ffe42c4a5deea5b0fec41c94c136cf115597227",
    },
    "eip155:43114": {
        "router": "0x94b75331ae8d42c1b61065089b7d48fe14aa73b7",
        "position_manager": "0xb74b1f14d2754acfcbbe1a221023a5cf50ab8acd",
        "quoter": "0xbe40675bb704506a3c2ccfb762dcfd1e979845c2",
        "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",
        "pool_manager": "0x06380c0e0912312b5150364b9dc4542ba0dbbc85",
        "state_view": "0xc3c9e198c735a4b97e3e683f391ccbdd60b69286",
    },
}


def infrastructure(target):
    roles = NETWORK_ROLES.get(network_key(target))
    actual = dict(target.contracts.model_dump())
    actual.update(pool_manager=target.pool.pool_manager, state_view=target.pool.state_view)
    if actual != roles:
        raise DataError("authority", "Uniswap contract roles differ from the verified deployment")
    return set(actual.values())


def target_addresses(target):
    return infrastructure(target) | {a.reference for a in target.assets}


def historical_addresses(target):
    """Archival reads also cover a reviewed hook: its pinned code and its fee views."""
    hooks = target.pool.hooks
    return target_addresses(target) | ({hooks} if hooks != ZERO_ADDRESS else set())


def pinned_identities():
    pins = json.loads((ARTIFACTS / "ethereum-25908472.json").read_text())
    return tuple(ContractIdentity.model_validate(i) for i in pins["contracts"])


def identities(config, account, reader):
    addresses = set()
    for allocation in config.allocations:
        if allocation.account != account:
            continue
        target = config.target(allocation.name)
        from almanak.gateway.environments import origin_chain

        if str(origin_chain(config, target.fund.chain).chain_id) != network_key(target):
            raise DataError("authority", "Uniswap origin differs from its protocol deployment")
        read_pool(reader.reader, target.pool, reader.block)
        addresses.update(target_addresses(target))
        for contract in (
            target.contracts.quoter,
            target.contracts.position_manager,
            target.pool.state_view,
        ):
            if reader.call(contract, "poolManager()")[0] != target.pool.pool_manager:
                raise DataError("authority", "Uniswap periphery has another PoolManager")
    observed = tuple(reader.identity(address) for address in sorted(addresses))
    for allocation in config.allocations:
        if allocation.account == account:
            verify_identities(config.target(allocation.name), observed)
    return observed


def verify_identities(target, observed):
    chain = network_key(target)
    filenames = {
        "eip155:1": "ethereum-protocol.json",
        "eip155:43114": "avalanche-protocol.json",
        "eip155:8453": "base-protocol.json",
        "eip155:42161": "arbitrum-protocol.json",
        "eip155:10": "optimism-protocol.json",
        "eip155:137": "polygon-protocol.json",
        "eip155:4663": "robinhood-protocol.json",
        "eip155:56": "bnb-protocol.json",
    }
    if chain not in filenames:
        raise DataError("authority", "Uniswap deployment has no verified runtime artifact")
    filename = filenames[chain]
    expected = json.loads((ARTIFACTS / filename).read_text())
    protocol = infrastructure(target)
    if native_index(target.pool) is not None:
        from almanak.connectors.uniswap_v4.native_router import wrapped_identity

        # The router wraps and unwraps through this token; a proxy's implementation is pinned.
        weth = wrapped_identity(target)
        protocol |= {weth.address}
        expected = {**expected, weth.address: weth.model_dump(mode="json")}
    for contract in observed:
        if contract.address in protocol and contract.model_dump(mode="json") != expected.get(
            contract.address
        ):
            raise DataError("authority", "Uniswap code differs from the verified implementation")


def network_key(target):
    return logical_chain(target.pool.chain.chain_id)
