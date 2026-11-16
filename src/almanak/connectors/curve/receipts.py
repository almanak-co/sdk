"""Attribute ordered coin movements and LP mint or burn to the exact fund."""

from eth_utils import keccak

from almanak.connectors.curve.operations import Addition, Approval, Removal, Swap
from almanak.effects import AssetDelta, WalletAsset
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_logs import data, events, one_event, topic_address
from almanak.models import ZERO_ADDRESS, Model


def topic(signature):
    return "0x" + keccak(text=signature).hex()


TRANSFER = topic("Transfer(address,address,uint256)")
APPROVAL = topic("Approval(address,address,uint256)")
SWAP = topic("TokenExchange(address,int128,uint256,int128,uint256)")
ADD = topic("AddLiquidity(address,uint256[],uint256[],uint256,uint256)")
REMOVE = topic("RemoveLiquidity(address,uint256[],uint256[],uint256)")
ONE = topic("RemoveLiquidityOne(address,int128,uint256,uint256,uint256)")
IMBALANCE = topic("RemoveLiquidityImbalance(address,uint256[],uint256[],uint256,uint256)")


class Effects(Model):
    asset_changes: tuple[AssetDelta, ...]
    share_change: AssetDelta

    def validate_target(self, target):
        expected = tuple(key(target, asset) for asset in target.assets)
        if tuple(value.key for value in self.asset_changes) != expected:
            raise ValueError("Curve effects must retain every coin in configured order")
        if self.share_change.key != key(target, target.pool.shares_asset):
            raise ValueError("Curve share effects must retain the pool and fund account")


def key(target, asset):
    return WalletAsset(account=target.account, chain=target.fund.chain, asset=asset)


def effects(target, amounts, shares=0):
    return Effects(
        asset_changes=tuple(
            AssetDelta(key=key(target, asset), amount=amount)
            for asset, amount in zip(target.assets, amounts, strict=True)
        ),
        share_change=AssetDelta(key=key(target, target.pool.shares_asset), amount=shares),
    )


def zero(target):
    return effects(target, (0,) * len(target.assets))


def transfers(receipt, asset):
    result = []
    for event in events(receipt, asset.reference, TRANSFER):
        if len(event["topics"]) != 3:
            raise DataError("receipt", "ERC-20 transfer has an unexpected topic layout")
        sender, receiver = (topic_address(t) for t in event["topics"][1:])
        (amount,) = data(event, ("uint256",))
        result.append((sender, receiver, amount))
    return result


def require_transfers(receipt, asset, expected):
    if sorted(transfers(receipt, asset)) != sorted(expected):
        raise DataError("receipt", "Curve transfers differ from exact fund settlement")


def pool_event(receipt, operation, signature):
    event = one_event(receipt, operation.target.pool.pool, signature, 2)
    if topic_address(event["topics"][1]) != operation.target.fund.address:
        raise DataError("receipt", "Curve event identifies another fund")
    return event


def sequence(receipt, operation):
    signature = None
    if isinstance(operation, Swap):
        signature = SWAP
    elif isinstance(operation, Addition):
        signature = ADD
    elif isinstance(operation, Removal):
        signature = REMOVE if operation.output_asset is None else ONE
    for candidate in (SWAP, ADD, REMOVE, ONE, IMBALANCE):
        if len(events(receipt, operation.target.pool.pool, candidate)) != int(
            candidate == signature
        ):
            raise DataError("receipt", "Curve receipt contains another protocol action sequence")


def swap_effect(receipt, operation):
    event = pool_event(receipt, operation, SWAP)
    incoming, amount, outgoing, received = data(event, ("int128", "uint256", "int128", "uint256"))
    target = operation.target
    if (incoming, amount, outgoing) != (
        target.assets.index(operation.amount.asset),
        operation.amount.amount,
        target.assets.index(operation.minimum.asset),
    ) or received < operation.minimum.amount:
        raise DataError("receipt", "Curve swap violates its exact input or minimum output")
    amounts = [0] * len(target.assets)
    amounts[incoming], amounts[outgoing] = -amount, received
    return effects(target, amounts)


def addition_effect(receipt, operation):
    event = pool_event(receipt, operation, ADD)
    amounts, fees, invariant, supply = data(event, ("uint256[]", "uint256[]", "uint256", "uint256"))
    if amounts != tuple(v.amount for v in operation.amounts.root) or len(fees) not in (
        0,
        len(amounts),
    ):
        raise DataError("receipt", "Curve mint differs from the ordered exact coin inputs")
    target = operation.target
    changes = transfers(receipt, target.pool.shares_asset)
    if len(changes) != 1 or changes[0][:2] != (ZERO_ADDRESS, target.fund.address):
        raise DataError("receipt", "Curve mint did not create only the fund's LP shares")
    minted = changes[0][2]
    if minted < operation.minimum_shares.amount or supply < minted or invariant == 0:
        raise DataError("receipt", "Curve mint violates its retained LP share minimum")
    return effects(target, tuple(-v for v in amounts), minted)


def removal_effect(receipt, operation):
    target = operation.target
    if operation.output_asset is None:
        event = pool_event(receipt, operation, REMOVE)
        amounts, fees, _ = data(event, ("uint256[]", "uint256[]", "uint256"))
        if fees:
            raise DataError("receipt", "proportional Curve removal reported unexpected fees")
    else:
        event = pool_event(receipt, operation, ONE)
        index, burned, amount, _ = data(event, ("int128", "uint256", "uint256", "uint256"))
        if (
            index != target.assets.index(operation.output_asset)
            or burned != operation.shares.amount
        ):
            raise DataError("receipt", "single-coin Curve removal changed its asset or share burn")
        amounts = tuple(amount if i == index else 0 for i in range(len(target.assets)))
    if len(amounts) != len(target.assets) or any(
        amount < minimum.amount
        for amount, minimum in zip(amounts, operation.minimums.root, strict=True)
    ):
        raise DataError("receipt", "Curve removal violates its ordered exit minima")
    return effects(target, amounts, -operation.shares.amount)


def approval_effect(receipt, operation):
    event = one_event(receipt, operation.asset.reference, APPROVAL, 3)
    if tuple(topic_address(t) for t in event["topics"][1:]) != (
        operation.target.fund.address,
        operation.spender,
    ) or data(event, ("uint256",)) != (operation.amount,):
        raise DataError("receipt", "Curve approval differs from owner, spender or amount")
    return zero(operation.target)


def settlement(receipt, operation, result, extra_asset_transfers=None, *, extra_share_transfers=()):
    target = operation.target
    for change in result.asset_changes:
        amount = change.amount
        expected = []
        if amount < 0:
            expected = [(target.fund.address, target.pool.pool, -amount)]
        elif amount > 0 or receives_zero(operation, change.key.asset):
            expected = [(target.pool.pool, target.fund.address, amount)]
        if extra_asset_transfers is not None:
            expected.extend(extra_asset_transfers[change.key.asset])
        require_transfers(receipt, change.key.asset, expected)
    shares = result.share_change.amount
    expected = []
    if shares > 0:
        expected = [(ZERO_ADDRESS, target.fund.address, shares)]
    elif shares < 0:
        expected = [(target.fund.address, ZERO_ADDRESS, -shares)]
    require_transfers(receipt, target.pool.shares_asset, [*expected, *extra_share_transfers])


def receives_zero(operation, asset):
    return isinstance(operation, Removal) and operation.output_asset in (None, asset)


def movement(receipt, operation, before=None):
    if operation.target.pool.family == "tricrypto_ng":
        from almanak.connectors.curve.tricrypto_receipts import movement as tri_movement

        return tri_movement(receipt, operation, before)
    if operation.target.pool.family == "twocrypto_ng":
        from almanak.connectors.curve.crypto_receipts import movement as crypto_movement

        return crypto_movement(receipt, operation, before)
    return stable_movement(receipt, operation)


def stable_movement(receipt, operation):
    sequence(receipt, operation)
    if isinstance(operation, Approval):
        result = approval_effect(receipt, operation)
    elif isinstance(operation, Swap):
        result = swap_effect(receipt, operation)
    elif isinstance(operation, Addition):
        result = addition_effect(receipt, operation)
    elif isinstance(operation, Removal):
        result = removal_effect(receipt, operation)
    else:
        raise ValueError("unsupported Curve protocol operation")
    settlement(receipt, operation, result)
    return result
