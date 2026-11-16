"""Attribute principal, LP shares and segregated fees to the exact fund."""

from eth_utils import keccak

from almanak.connectors.aerodrome.operations import Addition, Approval, Collect, Removal, Swap
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_logs import data, events, one_event, topic_address
from almanak.models import ZERO_ADDRESS, Model, UInt


def topic(signature):
    return "0x" + keccak(text=signature).hex()


TRANSFER = topic("Transfer(address,address,uint256)")
APPROVAL = topic("Approval(address,address,uint256)")
MINT = topic("Mint(address,uint256,uint256)")
BURN = topic("Burn(address,address,uint256,uint256)")
SWAP = topic("Swap(address,address,uint256,uint256,uint256,uint256)")
CLAIM = topic("Claim(address,address,uint256,uint256)")
FEES = topic("Fees(address,uint256,uint256)")


class Effects(Model):
    delta0: int
    delta1: int
    shares_delta: int = 0
    fees0: UInt = 0
    fees1: UInt = 0


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
        raise DataError("receipt", "Aerodrome transfers differ from exact fund settlement")


def pool_event(receipt, operation, signature, recipients):
    event = one_event(receipt, operation.target.pool.pool, signature, len(recipients) + 1)
    if tuple(topic_address(t) for t in event["topics"][1:]) != recipients:
        raise DataError("receipt", "Aerodrome event identifies another sender or fund")
    return event


def sequence(receipt, operation):
    expected = {
        "approve": (0, 0, 0),
        "swap": (1, 0, 0),
        "mint": (0, 1, 0),
        "increase": (0, 1, 0),
        "decrease": (0, 0, 1),
        "close": (0, 0, 1),
        "collect": (0, 0, 0),
    }[operation.kind]
    actual = tuple(len(events(receipt, operation.target.pool.pool, t)) for t in (SWAP, MINT, BURN))
    claims = len(events(receipt, operation.target.pool.pool, CLAIM))
    fees = len(events(receipt, operation.target.pool.pool, FEES))
    if (
        actual != expected
        or claims > int(isinstance(operation, Collect))
        or fees > int(isinstance(operation, Swap))
    ):
        raise DataError("receipt", "Aerodrome receipt contains another protocol action sequence")


def swap_effect(receipt, operation, fee_contract):
    target = operation.target
    event = pool_event(receipt, operation, SWAP, (target.contracts.router, target.fund.address))
    in0, in1, out0, out1 = data(event, ("uint256",) * 4)
    incoming, outgoing = (in0, in1), (out0, out1)
    index = target.assets.index(operation.amount.asset)
    if (
        incoming[index] != operation.amount.amount
        or incoming[1 - index]
        or outgoing[index]
        or outgoing[1 - index] < operation.minimum.amount
    ):
        raise DataError("receipt", "Aerodrome swap violates its exact input or minimum output")
    fees = swap_fees(receipt, operation, index, incoming)
    for i, asset in enumerate(target.assets):
        expected = []
        if incoming[i]:
            expected.append((target.fund.address, target.pool.pool, incoming[i]))
        if outgoing[i]:
            expected.append((target.pool.pool, target.fund.address, outgoing[i]))
        if fees[i]:
            expected.append((target.pool.pool, fee_contract, fees[i]))
        require_transfers(receipt, asset, expected)
    return Effects(delta0=out0 - in0, delta1=out1 - in1)


def swap_fees(receipt, operation, index, incoming):
    found = events(receipt, operation.target.pool.pool, FEES)
    if not found:
        return (0, 0)
    event = pool_event(receipt, operation, FEES, (operation.target.contracts.router,))
    fees = data(event, ("uint256", "uint256"))
    if fees[1 - index] or not 0 < fees[index] <= incoming[index]:
        raise DataError("receipt", "Aerodrome fee event identifies another input")
    return fees


def addition_effect(receipt, operation):
    target = operation.target
    event = pool_event(receipt, operation, MINT, (target.contracts.router,))
    amounts = data(event, ("uint256", "uint256"))
    share_transfers = transfers(receipt, target.pool.shares_asset)
    if len(share_transfers) != 1 or share_transfers[0][:2] != (ZERO_ADDRESS, target.fund.address):
        raise DataError("receipt", "Aerodrome mint did not create only the fund's LP shares")
    shares = share_transfers[0][2]
    if not shares:
        raise DataError("receipt", "Aerodrome mint did not create positive LP shares")
    for asset, amount, minimum, maximum in zip(
        target.assets,
        amounts,
        (operation.minimum0, operation.minimum1),
        (operation.maximum0, operation.maximum1),
        strict=True,
    ):
        if not minimum.amount <= amount <= maximum.amount:
            raise DataError("receipt", "Aerodrome mint violates its currency bounds")
        require_transfers(receipt, asset, [(target.fund.address, target.pool.pool, amount)])
    return Effects(delta0=-amounts[0], delta1=-amounts[1], shares_delta=shares)


def removal_effect(receipt, operation):
    target = operation.target
    event = pool_event(receipt, operation, BURN, (target.contracts.router, target.fund.address))
    amounts = data(event, ("uint256", "uint256"))
    for asset, amount, minimum in zip(
        target.assets, amounts, (operation.minimum0, operation.minimum1), strict=True
    ):
        if amount < minimum.amount:
            raise DataError("receipt", "Aerodrome burn violates its principal minimum")
        require_transfers(receipt, asset, [(target.pool.pool, target.fund.address, amount)])
    shares = operation.shares.amount
    require_transfers(
        receipt,
        target.pool.shares_asset,
        [(target.fund.address, target.pool.pool, shares), (target.pool.pool, ZERO_ADDRESS, shares)],
    )
    return Effects(delta0=amounts[0], delta1=amounts[1], shares_delta=-shares)


def collect_effect(receipt, operation, fee_contract):
    target = operation.target
    found = events(receipt, target.pool.pool, CLAIM)
    amounts = (0, 0)
    if found:
        event = pool_event(receipt, operation, CLAIM, (target.fund.address, target.fund.address))
        amounts = data(event, ("uint256", "uint256"))
    for asset, amount in zip(target.assets, amounts, strict=True):
        expected = [(fee_contract, target.fund.address, amount)] if amount else []
        require_transfers(receipt, asset, expected)
    return Effects(delta0=amounts[0], delta1=amounts[1], fees0=amounts[0], fees1=amounts[1])


def approval_effect(receipt, operation):
    event = one_event(receipt, operation.asset.reference, APPROVAL, 3)
    if tuple(topic_address(t) for t in event["topics"][1:]) != (
        operation.target.fund.address,
        operation.spender,
    ) or data(event, ("uint256",)) != (operation.amount,):
        raise DataError("receipt", "Aerodrome approval differs from owner, spender or amount")
    for asset in operation.target.assets:
        require_transfers(receipt, asset, [])
    return Effects(delta0=0, delta1=0)


def movement(receipt, operation, fee_contract):
    sequence(receipt, operation)
    if not isinstance(operation, (Addition, Removal)):
        require_transfers(receipt, operation.target.pool.shares_asset, [])
    if isinstance(operation, Approval):
        return approval_effect(receipt, operation)
    if isinstance(operation, Swap):
        return swap_effect(receipt, operation, fee_contract)
    if isinstance(operation, Addition):
        return addition_effect(receipt, operation)
    if isinstance(operation, Removal):
        return removal_effect(receipt, operation)
    if isinstance(operation, Collect):
        return collect_effect(receipt, operation, fee_contract)
    raise ValueError("unsupported Aerodrome protocol operation")
