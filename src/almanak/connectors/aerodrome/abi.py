"""Exact single-pool router routes and fungible-share liquidity calls."""

from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.aerodrome.operations import Addition, Approval, Collect, Removal, Swap
from almanak.gateway.accounts import ContractCall


def calldata(signature, types, values):
    return keccak(text=signature)[:4] + encode(types, values)


def call(operation):
    target = operation.target
    destination = target.contracts.router
    if isinstance(operation, Approval):
        destination = operation.asset.reference
        data = calldata(
            "approve(address,uint256)",
            ("address", "uint256"),
            (operation.spender, operation.amount),
        )
    elif isinstance(operation, Swap):
        data = swap(operation)
    elif isinstance(operation, Addition):
        data = addition(operation)
    elif isinstance(operation, Removal):
        data = removal(operation)
    elif isinstance(operation, Collect):
        destination = target.pool.pool
        data = calldata("claimFees()", (), ())
    else:
        raise ValueError("unsupported Aerodrome protocol operation")
    return ContractCall(fund=target.fund, to=destination, value=0, data="0x" + data.hex())


def swap(operation):
    target = operation.target
    types = ("uint256", "uint256", "(address,address,bool,address)[]", "address", "uint256")
    route = (
        operation.amount.asset.reference,
        operation.minimum.asset.reference,
        target.pool.stable,
        target.pool.factory,
    )
    return calldata(
        "swapExactTokensForTokens(" + ",".join(types) + ")",
        types,
        (
            operation.amount.amount,
            operation.minimum.amount,
            (route,),
            target.fund.address,
            operation.deadline,
        ),
    )


def addition(operation):
    target = operation.target
    types = (
        "address",
        "address",
        "bool",
        "uint256",
        "uint256",
        "uint256",
        "uint256",
        "address",
        "uint256",
    )
    return calldata(
        "addLiquidity(" + ",".join(types) + ")",
        types,
        (
            target.pool.currency0.reference,
            target.pool.currency1.reference,
            target.pool.stable,
            operation.maximum0.amount,
            operation.maximum1.amount,
            operation.minimum0.amount,
            operation.minimum1.amount,
            target.fund.address,
            operation.deadline,
        ),
    )


def removal(operation):
    target = operation.target
    types = ("address", "address", "bool", "uint256", "uint256", "uint256", "address", "uint256")
    return calldata(
        "removeLiquidity(" + ",".join(types) + ")",
        types,
        (
            target.pool.currency0.reference,
            target.pool.currency1.reference,
            target.pool.stable,
            operation.shares.amount,
            operation.minimum0.amount,
            operation.minimum1.amount,
            target.fund.address,
            operation.deadline,
        ),
    )
