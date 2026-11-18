"""Compiled GMX deployment closures and configured market identity, without sample-market lists."""

import json
from functools import cache
from pathlib import Path

from eth_utils import keccak

from almanak.connectors.evm_revision import read_code
from almanak.connectors.gmx_v2.exchange_domain import ExchangeContracts
from almanak.connectors.gmx_v2.oracle_reader import key, stored
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity

ROLES = {
    "exchange_router": "ExchangeRouter",
    "token_router": "Router",
    "reader": "Reader",
    "data_store": "DataStore",
    "event_emitter": "EventEmitter",
    "oracle": "Oracle",
    "order_handler": "OrderHandler",
    "order_vault": "OrderVault",
    "increase_order_executor": "IncreaseOrderExecutor",
    "decrease_order_executor": "DecreaseOrderExecutor",
    "swap_order_executor": "SwapOrderExecutor",
    "swap_handler": "SwapHandler",
    "deposit_handler": "DepositHandler",
    "deposit_vault": "DepositVault",
    "withdrawal_handler": "WithdrawalHandler",
    "withdrawal_vault": "WithdrawalVault",
}


@cache
def compiled():
    return json.loads(
        (Path(__file__).with_name("artifacts") / "compiled-deployments.json").read_text()
    )


def records(market):
    result = compiled()["chains"].get(str(market.chain.chain_id))
    if result is None:
        raise DataError("unsupported", "GMX chain lacks verified compiled deployment evidence")
    for field, address in market.contracts.model_dump().items():
        if result.get(address, {}).get("contract") != ROLES[field]:
            raise DataError("identity", "configured GMX role differs from its verified deployment")
    return result


def deployed(market, contract) -> str:
    """The reviewed address of a named GMX contract on the market's chain."""
    matches = [a for a, value in records(market).items() if value["contract"] == contract]
    if len(matches) != 1:
        raise DataError("identity", f"GMX deployment record lacks exactly one {contract}")
    return matches[0]


def verified_identity(reader, address, block, expected):
    raw = reader._rpc("eth_getCode", [address, {"blockHash": block.hash, "requireCanonical": True}])
    identity = ContractIdentity(
        address=address, code_hash="0x" + keccak(bytes.fromhex(raw[2:])).hex()
    )
    if identity.code_hash != expected["code_hash"]:
        raise DataError("revision", "GMX runtime differs from its compiled deployment closure")
    return identity


def verify_deployments(reader, market, block):
    if market.chain != block.chain or block.chain != reader.chain:
        raise DataError(
            "chain_mismatch", "GMX revision verification must retain its execution scope"
        )
    expected = records(market)
    identities = tuple(
        verified_identity(reader, address, block, value) for address, value in expected.items()
    )
    verify_roles(reader, block, expected)
    verify_order_executors(reader, market, block)
    market_identity = verify_market_token(reader, market, block, expected)
    wnt = stored(reader, block, market.contracts.data_store, "Address", "WNT")
    if wnt != market.wrapped_native.reference:
        raise DataError(
            "identity", "GMX configured fee asset differs from its wrapped-native binding"
        )
    return (*identities, market_identity)


def role_store(expected):
    return next(address for address, value in expected.items() if value["contract"] == "RoleStore")


def verify_roles(reader, block, expected):
    """Refuse a closure whose contracts no longer hold the roles recorded at its pin.

    GMX retires a deployment by revoking its RoleStore membership. The retired runtime
    and its getters are unchanged, so code hashes and handler selection still match while
    every request reverts.
    """
    store = role_store(expected)
    for address, value in expected.items():
        for role in value.get("roles", ()):
            held = reader.call(
                store,
                "hasRole(address,bytes32)",
                block,
                inputs=("address", "bytes32"),
                args=(address, key(role)),
                outputs=("bool",),
            )[0]
            if not held:
                raise DataError(
                    "identity",
                    f"GMX {value['contract']} {address} no longer holds {role} in its "
                    "RoleStore; the compiled deployment is retired",
                )


def verify_order_executors(reader, market, block):
    executors = [
        ("increaseOrderExecutor()", market.contracts.increase_order_executor),
        ("decreaseOrderExecutor()", market.contracts.decrease_order_executor),
    ]
    if isinstance(market.contracts, ExchangeContracts):
        executors.append(("swapOrderExecutor()", market.contracts.swap_order_executor))
        swap_handler = market.contracts.swap_handler
    else:
        swap_handler = next(
            address
            for address, value in records(market).items()
            if value["contract"] == "SwapHandler"
        )
    executors.append(("swapHandler()", swap_handler))
    for getter, expected in executors:
        selected = reader.call(market.contracts.order_handler, getter, block, outputs=("address",))[
            0
        ]
        if selected != expected:
            raise DataError("identity", "GMX order handler selects a different compiled executor")


def verify_market_token(reader, market, block, expected):
    values, identity = read_code(
        reader, market.share_asset.reference, block, compiled()["revisions"]["MarketToken"]
    )
    if values != {
        "dataStore": int(market.contracts.data_store, 16),
        "roleStore": int(role_store(expected), 16),
    }:
        raise DataError(
            "identity", "GMX market token constructor authority differs from configured deployment"
        )
    return identity


def identities(config, account, reader):
    candidates = (
        identity
        for allocation in config.allocations
        if allocation.account == account
        for identity in allocation_identities(allocation, reader)
    )
    return consistent_identities(candidates)


def allocation_identities(allocation, reader):
    return (
        *verify_deployments(reader.reader, allocation.market, reader.block),
        *(
            reader.identity(asset.reference)
            for asset in (
                allocation.market.index_asset,
                allocation.market.long_asset,
                allocation.market.short_asset,
                allocation.market.wrapped_native,
            )
        ),
    )


def consistent_identities(candidates):
    retained: dict[str, ContractIdentity] = {}
    for value in candidates:
        if retained.setdefault(value.address, value) != value:
            raise DataError("identity", "GMX contract identity differs across allocations")
    return tuple(retained[address] for address in sorted(retained))
