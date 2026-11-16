"""V3 receipts establish fund cash from transfers and exact pool settlement events."""

from eth_utils import keccak

from almanak.caip import AssetId
from almanak.connectors.v3_pool_operations import (
    Approval,
    Close,
    Collect,
    Decrease,
    Increase,
    Mint,
    Swap,
    operation_range,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_logs import data, events, one_event, topic_address, topic_uint
from almanak.models import ZERO_ADDRESS, Model, UInt


def topic(signature):
    return "0x" + keccak(text=signature).hex()


TRANSFER = topic("Transfer(address,address,uint256)")
APPROVAL = topic("Approval(address,address,uint256)")
POOL_MINT = topic("Mint(address,address,int24,int24,uint128,uint256,uint256)")
POOL_BURN = topic("Burn(address,int24,int24,uint128,uint256,uint256)")
POOL_COLLECT = topic("Collect(address,address,int24,int24,uint128,uint128)")
INCREASE = topic("IncreaseLiquidity(uint256,uint128,uint256,uint256)")
DECREASE = topic("DecreaseLiquidity(uint256,uint128,uint256,uint256)")
COLLECT = topic("Collect(uint256,address,uint256,uint256)")


class Effects(Model):
    delta0: int
    delta1: int
    liquidity_delta: int = 0
    nft: AssetId | None = None
    principal0: UInt = 0
    principal1: UInt = 0


def token_movement(receipt, target, asset):
    delta = 0
    for event in events(receipt, asset.reference, TRANSFER):
        if len(event["topics"]) != 3:
            raise DataError("receipt", "ERC-20 transfer has an unexpected topic layout")
        sender, receiver = (topic_address(t) for t in event["topics"][1:])
        if target.fund.address not in (sender, receiver):
            raise DataError("receipt", "token transfer identifies another fund or recipient")
        counterparty = receiver if sender == target.fund.address else sender
        if counterparty != target.pool.pool:
            raise DataError("receipt", "fund transfer has another pool counterparty")
        (amount,) = data(event, ("uint256",))
        delta += amount if receiver == target.fund.address else -amount
    return delta


def nft_transition(receipt, operation):
    target = operation.target
    found = events(receipt, target.contracts.position_manager, TRANSFER)
    if not isinstance(operation, (Mint, Close)):
        if found:
            raise DataError("receipt", "unexpected V3 NFT ownership transition")
        return getattr(operation, "nft", None)
    event = one_event(receipt, target.contracts.position_manager, TRANSFER, 4)
    expected = (
        (ZERO_ADDRESS, target.fund.address)
        if isinstance(operation, Mint)
        else (target.fund.address, ZERO_ADDRESS)
    )
    if (
        tuple(topic_address(value) for value in event["topics"][1:3]) != expected
        or event["data"] != "0x"
    ):
        raise DataError("receipt", "NFT must mint to or burn from its retained fund")
    token = topic_uint(event["topics"][3])
    nft = AssetId(
        f"{target.fund.chain.chain_id}/erc721:{target.contracts.position_manager}/{token}"
    )
    if not token or isinstance(operation, Close) and nft != operation.nft:
        raise DataError("receipt", "NFT event differs from retained ownership")
    return nft


def swap_movement(receipt, operation, deltas, signature, types):
    target = operation.target
    event = one_event(receipt, target.pool.pool, signature, 3)
    if tuple(topic_address(value) for value in event["topics"][1:]) != (
        target.contracts.router,
        target.fund.address,
    ):
        raise DataError("receipt", "V3 swap identifies another router or recipient")
    values = data(event, types)
    index = target.assets.index(operation.amount.asset)
    if (
        tuple(-value for value in values[:2]) != deltas
        or deltas[index] != -operation.amount.amount
        or deltas[1 - index] < operation.minimum.amount
    ):
        raise DataError("receipt", "swap transfers differ from its exact input and minimum")


def manager_event(receipt, operation, signature, nft):
    event = one_event(receipt, operation.target.contracts.position_manager, signature, 2)
    if topic_uint(event["topics"][1]) != int(nft.token_id):
        raise DataError("receipt", "position-manager event identifies another NFT")
    return event


def pool_range(event, operation):
    target = operation.target
    if (
        len(event["topics"]) != 4
        or topic_address(event["topics"][1]) != target.contracts.position_manager
    ):
        raise DataError("receipt", "pool liquidity event identifies another position owner")
    ticks = tuple(data({"data": value}, ("int24",))[0] for value in event["topics"][2:])
    expected = operation_range(operation)
    if expected is None or ticks != (expected.lower, expected.upper):
        raise DataError("receipt", "pool liquidity event identifies another range")


def added(receipt, operation, deltas, nft):
    target = operation.target
    values = data(
        manager_event(receipt, operation, INCREASE, nft), ("uint128", "uint256", "uint256")
    )
    event = one_event(receipt, target.pool.pool, POOL_MINT, 4)
    pool_range(event, operation)
    sender, liquidity, amount0, amount1 = data(event, ("address", "uint128", "uint256", "uint256"))
    if (
        sender != target.contracts.position_manager
        or values != (liquidity, amount0, amount1)
        or deltas != (-amount0, -amount1)
    ):
        raise DataError("receipt", "liquidity addition events differ from fund transfers")
    if not liquidity or amount0 > operation.maximum0.amount or amount1 > operation.maximum1.amount:
        raise DataError("receipt", "liquidity addition exceeds its input ceilings")
    return Effects(delta0=deltas[0], delta1=deltas[1], liquidity_delta=liquidity, nft=nft)


def collected(receipt, operation, deltas, nft):
    target = operation.target
    recipient, requested0, requested1 = data(
        manager_event(receipt, operation, COLLECT, nft), ("address", "uint256", "uint256")
    )
    event = one_event(receipt, target.pool.pool, POOL_COLLECT, 4)
    pool_range(event, operation)
    receiver, actual0, actual1 = data(event, ("address", "uint128", "uint128"))
    if (
        recipient != target.fund.address
        or receiver != recipient
        or (actual0, actual1) != deltas
        or actual0 > requested0
        or actual1 > requested1
    ):
        raise DataError("receipt", "collection events differ from actual fund settlement")


def removed(receipt, operation, deltas, nft):
    collected(receipt, operation, deltas, nft)
    burns = events(receipt, operation.target.pool.pool, POOL_BURN)
    values = []
    for event in burns:
        pool_range(event, operation)
        values.append(data(event, ("uint128", "uint256", "uint256")))
    if isinstance(operation, Collect):
        if values != [(0, 0, 0)]:
            raise DataError("receipt", "fee collection changed pool liquidity")
        return Effects(delta0=deltas[0], delta1=deltas[1], nft=nft)
    decreased = data(
        manager_event(receipt, operation, DECREASE, nft), ("uint128", "uint256", "uint256")
    )
    expected = [decreased, (0, 0, 0)] if isinstance(operation, Decrease) else [decreased]
    if values != expected or decreased[0] != operation.liquidity:
        raise DataError("receipt", "liquidity removal differs from its retained amount")
    return Effects(
        delta0=deltas[0],
        delta1=deltas[1],
        liquidity_delta=-decreased[0],
        nft=nft,
        principal0=decreased[1],
        principal1=decreased[2],
    )


def approval_effect(receipt, operation, deltas):
    event = one_event(receipt, operation.asset.reference, APPROVAL, 3)
    if (
        tuple(topic_address(value) for value in event["topics"][1:])
        != (operation.target.fund.address, operation.spender)
        or data(event, ("uint256",)) != (operation.amount,)
        or deltas != (0, 0)
    ):
        raise DataError("receipt", "approval differs from retained owner, spender or amount")
    return Effects(delta0=0, delta1=0)


def movement(receipt, operation, signature, types):
    validate_sequence(receipt, operation, signature)
    target = operation.target
    deltas = tuple(token_movement(receipt, target, asset) for asset in target.assets)
    nft = nft_transition(receipt, operation)
    if isinstance(operation, Approval):
        return approval_effect(receipt, operation, deltas)
    if isinstance(operation, Swap):
        swap_movement(receipt, operation, deltas, signature, types)
        return Effects(delta0=deltas[0], delta1=deltas[1])
    if isinstance(operation, (Mint, Increase)):
        return added(receipt, operation, deltas, nft)
    return removed(receipt, operation, deltas, nft)


def validate_sequence(receipt, operation, swap_signature):
    expected = {
        "approve": (0, 0, 0, 0, 0, 0, 0),
        "swap": (1, 0, 0, 0, 0, 0, 0),
        "mint": (0, 1, 0, 0, 1, 0, 0),
        "increase": (0, 1, 0, 0, 1, 0, 0),
        "decrease": (0, 0, 2, 1, 0, 1, 1),
        "collect": (0, 0, 1, 1, 0, 0, 1),
        "close": (0, 0, 1, 1, 0, 1, 1),
    }[operation.kind]
    target = operation.target
    actual = tuple(
        len(events(receipt, target.pool.pool, signature))
        for signature in (swap_signature, POOL_MINT, POOL_BURN, POOL_COLLECT)
    )
    actual += tuple(
        len(events(receipt, target.contracts.position_manager, signature))
        for signature in (INCREASE, DECREASE, COLLECT)
    )
    if actual != expected:
        raise DataError("receipt", "receipt contains another protocol action sequence")
