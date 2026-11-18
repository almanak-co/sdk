"""TwoCrypto receipts distinguish fund settlement from pool-wide admin claims."""

from almanak.connectors.curve import receipts as shared
from almanak.connectors.curve.operations import Addition, Approval, Removal, Swap
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_logs import data, events, one_event, topic_address
from almanak.models import ZERO_ADDRESS

SWAP = shared.topic("TokenExchange(address,uint256,uint256,uint256,uint256,uint256,uint256)")
ADD = shared.topic("AddLiquidity(address,uint256[2],uint256,uint256,uint256)")
REMOVE = shared.topic("RemoveLiquidity(address,uint256[2],uint256)")
ONE = shared.topic("RemoveLiquidityOne(address,uint256,uint256,uint256,uint256,uint256)")
CLAIM = shared.topic("ClaimAdminFee(address,uint256[2])")


def sequence(receipt, operation):
    expected = None
    if isinstance(operation, Swap):
        expected = SWAP
    elif isinstance(operation, Addition):
        expected = ADD
    elif isinstance(operation, Removal):
        expected = REMOVE if operation.output_asset is None else ONE
    for candidate in (
        SWAP,
        ADD,
        REMOVE,
        ONE,
        shared.SWAP,
        shared.ADD,
        shared.REMOVE,
        shared.ONE,
        shared.IMBALANCE,
    ):
        if len(events(receipt, operation.target.pool.pool, candidate)) != int(
            candidate == expected
        ):
            raise DataError(
                "receipt", "TwoCrypto receipt contains another protocol action sequence"
            )


def swap_effect(receipt, operation):
    event = shared.pool_event(receipt, operation, SWAP)
    incoming, amount, outgoing, received, _, _ = data(event, ("uint256",) * 6)
    target = operation.target
    if (incoming, amount, outgoing) != (
        target.assets.index(operation.amount.asset),
        operation.amount.amount,
        target.assets.index(operation.minimum.asset),
    ) or received < operation.minimum.amount:
        raise DataError("receipt", "TwoCrypto swap violates its exact input or minimum output")
    amounts = [0] * len(target.assets)
    amounts[incoming], amounts[outgoing] = -amount, received
    return shared.effects(target, amounts)


def addition_effect(receipt, operation):
    event = shared.pool_event(receipt, operation, ADD)
    amounts, _, supply, scale = data(event, ("uint256[2]", "uint256", "uint256", "uint256"))
    if amounts != tuple(value.amount for value in operation.amounts.root):
        raise DataError("receipt", "TwoCrypto mint differs from its ordered exact coin inputs")
    target = operation.target
    changes = shared.transfers(receipt, target.pool.shares_asset)
    if len(changes) != 1 or changes[0][:2] != (ZERO_ADDRESS, target.fund.address):
        raise DataError("receipt", "TwoCrypto mint did not create only the fund's LP shares")
    minted = changes[0][2]
    if minted < operation.minimum_shares.amount or supply < minted or scale == 0:
        raise DataError("receipt", "TwoCrypto mint violates its retained LP share minimum")
    return shared.effects(target, tuple(-value for value in amounts), minted)


def removal_effect(receipt, operation):
    target = operation.target
    if operation.output_asset is None:
        event = shared.pool_event(receipt, operation, REMOVE)
        amounts, _ = data(event, ("uint256[2]", "uint256"))
    else:
        event = shared.pool_event(receipt, operation, ONE)
        burned, index, amount, _, _ = data(event, ("uint256",) * 5)
        if (
            index != target.assets.index(operation.output_asset)
            or burned != operation.shares.amount
        ):
            raise DataError("receipt", "TwoCrypto removal changed its selected coin or share burn")
        amounts = tuple(amount if coin == index else 0 for coin in range(2))
    if any(
        amount < minimum.amount
        for amount, minimum in zip(amounts, operation.minimums.root, strict=True)
    ):
        raise DataError("receipt", "TwoCrypto removal violates its ordered exit minima")
    return shared.effects(target, amounts, -operation.shares.amount)


def admin_transfers(receipt, operation, before):
    target = operation.target
    claims = events(receipt, target.pool.pool, CLAIM)
    if not claims:
        return {asset: [] for asset in target.assets}
    if not isinstance(operation, Removal) or operation.output_asset is None or before is None:
        raise DataError(
            "receipt", "unexpected TwoCrypto admin claim without retained withdrawal state"
        )
    event = one_event(receipt, target.pool.pool, CLAIM, 2)
    receiver = topic_address(event["topics"][1])
    if receiver != before.pool.accounting.receiver or receiver == target.fund.address:
        raise DataError(
            "receipt", "TwoCrypto admin receiver differs from retained independent fee scope"
        )
    (amounts,) = data(event, ("uint256[2]",))
    return {
        asset: [(target.pool.pool, receiver, amount)]
        for asset, amount in zip(target.assets, amounts, strict=True)
    }


def movement(receipt, operation, before):
    sequence(receipt, operation)
    if isinstance(operation, Approval):
        result = shared.approval_effect(receipt, operation)
    elif isinstance(operation, Swap):
        result = swap_effect(receipt, operation)
    elif isinstance(operation, Addition):
        result = addition_effect(receipt, operation)
    elif isinstance(operation, Removal):
        result = removal_effect(receipt, operation)
    else:
        raise ValueError("unsupported TwoCrypto protocol operation")
    shared.settlement(receipt, operation, result, admin_transfers(receipt, operation, before))
    return result
