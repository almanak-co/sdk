"""Direct Curve calls pay the fund; pool-admin fee collection is not requested."""

from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.curve.operations import Addition, Approval, Removal, Swap
from almanak.connectors.curve.pool_abi import layout, signature
from almanak.gateway.accounts import ContractCall


def calldata(signature, types, values):
    return keccak(text=signature)[:4] + encode(types, values)


def call(operation):
    target = operation.target
    destination = target.pool.pool
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
        data = calldata(
            signature("add_liquidity", layout(target.pool).addition),
            layout(target.pool).addition,
            (
                tuple(value.amount for value in operation.amounts.root),
                operation.minimum_shares.amount,
            )
            + layout(target.pool).receiver(target.fund.address),
        )
    elif isinstance(operation, Removal):
        data = removal(operation)
    else:
        raise ValueError("unsupported Curve protocol operation")
    return ContractCall(fund=target.fund, to=destination, value=0, data="0x" + data.hex())


def swap(operation):
    target = operation.target
    return calldata(
        signature("exchange", layout(target.pool).exchange),
        layout(target.pool).exchange,
        (
            target.assets.index(operation.amount.asset),
            target.assets.index(operation.minimum.asset),
            operation.amount.amount,
            operation.minimum.amount,
        )
        + layout(target.pool).receiver(target.fund.address),
    )


def removal(operation):
    target = operation.target
    if operation.output_asset is not None:
        minimum = next(
            value.amount
            for value in operation.minimums.root
            if value.asset == operation.output_asset
        )
        return calldata(
            signature("remove_liquidity_one_coin", layout(target.pool).single_coin),
            layout(target.pool).single_coin,
            (
                operation.shares.amount,
                target.assets.index(operation.output_asset),
                minimum,
            )
            + layout(target.pool).receiver(target.fund.address),
        )
    return calldata(
        signature("remove_liquidity", layout(target.pool).proportional),
        layout(target.pool).proportional,
        (
            operation.shares.amount,
            tuple(value.amount for value in operation.minimums.root),
        )
        + layout(target.pool).receiver(target.fund.address)
        + ((False,) if layout(target.pool).admin_flag else ()),
    )
