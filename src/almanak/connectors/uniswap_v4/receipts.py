"""Correlate exact emitters, pool deltas and NFT transitions with admitted calls."""

from eth_abi import decode
from eth_utils import keccak

from almanak.caip import AssetId
from almanak.connectors.uniswap_v4.hooks import pool_hook
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.connectors.uniswap_v4.state import Approval, Burn, Collect, Mint, Swap
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_logs import events as events
from almanak.gateway.evm_logs import topic_address as topic_address
from almanak.models import ZERO_ADDRESS

TRANSFER = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
SWAP = "0x" + keccak(text="Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)").hex()
MODIFY = "0x" + keccak(text="ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)").hex()


def pool_event(receipt, operation, topic):
    target = operation.target
    expected = events(receipt, target.pool.pool_manager, topic)
    other = MODIFY if topic == SWAP else SWAP
    if len(expected) != 1 or events(receipt, target.pool.pool_manager, other):
        raise DataError("receipt", "receipt must contain exactly the admitted pool action")
    event = expected[0]
    sender = target.contracts.router if topic == SWAP else target.contracts.position_manager
    if (
        len(event["topics"]) != 3
        or event["topics"][1] != pool_id(target.pool)
        or topic_address(event["topics"][2]) != sender
    ):
        raise DataError("receipt", "pool event identifies another pool or periphery sender")
    return bytes.fromhex(event["data"][2:])


def transfer(event):
    if len(event["topics"]) != 3:
        raise DataError("receipt", "ERC-20 transfer has an unexpected topic layout")
    sender, receiver = (topic_address(t) for t in event["topics"][1:])
    return sender, receiver, decode(("uint256",), bytes.fromhex(event["data"][2:]))[0]


def taker(operation):
    """The hook that takes swap output from the PoolManager, for an output-haircut profile."""
    if not isinstance(operation, Swap):
        return None
    entry = pool_hook(operation.target.pool)
    if entry is None or entry.reviewed.model != "output_haircut":
        return None
    return operation.target.pool.hooks


def output_asset(operation):
    held = operation.target.assets
    return held[1] if operation.amount.asset == held[0] else held[0]


def hook_take(receipt, operation, asset):
    """Output an output-haircut hook took from the PoolManager in this swap receipt."""
    hook = taker(operation)
    if hook is None:
        return 0
    manager = operation.target.pool.pool_manager
    return sum(
        amount
        for sender, receiver, amount in map(transfer, events(receipt, asset.reference, TRANSFER))
        if (sender, receiver) == (manager, hook)
    )


def token_movement(receipt, target, asset, hook=None):
    delta = 0
    for event in events(receipt, asset.reference, TRANSFER):
        sender, receiver, amount = transfer(event)
        if hook is not None and (sender, receiver) == (target.pool.pool_manager, hook):
            continue
        if target.fund.address not in (sender, receiver):
            raise DataError("receipt", "token transfer has another recipient or fund")
        counterparty = receiver if sender == target.fund.address else sender
        if counterparty != target.pool.pool_manager:
            raise DataError("receipt", "fund transfer has an unauthorized counterparty")
        delta += amount if receiver == target.fund.address else -amount
    return delta


def nft_transition(receipt, operation):
    target = operation.target
    transitions = events(receipt, target.contracts.position_manager, TRANSFER)
    if not isinstance(operation, (Mint, Burn)):
        if transitions:
            raise DataError("receipt", "unexpected NFT ownership transition")
        return None
    if len(transitions) != 1:
        raise DataError("receipt", "mint or burn requires exactly one NFT transfer")
    event = transitions[0]
    if len(event["topics"]) != 4 or event["data"] != "0x":
        raise DataError("receipt", "NFT transfer has an unexpected ABI layout")
    sender, receiver = (topic_address(t) for t in event["topics"][1:3])
    expected = (
        (ZERO_ADDRESS, target.fund.address)
        if isinstance(operation, Mint)
        else (target.fund.address, ZERO_ADDRESS)
    )
    if (sender, receiver) != expected:
        raise DataError("receipt", "NFT transfer does not mint to or burn from the fund")
    token_id = int(event["topics"][3], 16)
    nft = AssetId(
        f"{target.fund.chain.chain_id}/erc721:{target.contracts.position_manager}/{token_id}"
    )
    if token_id == 0 or isinstance(operation, Burn) and nft != operation.nft:
        raise DataError("receipt", "burn does not identify the retained strategy NFT")
    return nft


def fund_movement(receipt, target):
    """The fund's own transfers with any counterparty: what a swap through hook code that
    no longer matches its reviewed model measurably moved."""
    fund = target.fund.address
    return tuple(
        sum(
            (receiver == fund) * amount - (sender == fund) * amount
            for sender, receiver, amount in map(transfer, events(receipt, a.reference, TRANSFER))
        )
        for a in target.assets
    )


def fund_spend(receipt, target):
    """What left the fund per asset, as negative deltas: what its spending allowances lost.

    A hook that refunds part of a swap's input changes the fund's net movement, not
    what the router pulled through Permit2.
    """
    fund = target.fund.address
    return tuple(
        -sum(
            amount
            for sender, _, amount in map(transfer, events(receipt, a.reference, TRANSFER))
            if sender == fund
        )
        for a in target.assets
    )


def swap_movement(receipt, operation, deltas):
    """(take, pool output before afterSwap) of an output-haircut swap; None otherwise."""
    raw = pool_event(receipt, operation, SWAP)
    delta0, delta1, _, _, _, _ = decode(
        ("int128", "int128", "uint160", "uint128", "int24", "uint24"), raw
    )
    index = 0 if operation.amount.asset == operation.target.assets[0] else 1
    # The Swap event carries the pool's deltas before afterSwap; an output-haircut
    # hook's take leaves the PoolManager to the hook, never through the fund.
    pool_deltas = [delta0, delta1]
    gross = pool_deltas[1 - index]
    take = hook_take(receipt, operation, operation.target.assets[1 - index])
    pool_deltas[1 - index] -= take
    if (
        tuple(pool_deltas) != deltas
        or deltas[index] != -operation.amount.amount
        or deltas[1 - index] < operation.minimum.amount
    ):
        raise DataError("receipt", "swap event and transfers differ from retained input or minimum")
    return None if taker(operation) is None else (take, gross)


def liquidity_movement(receipt, operation, deltas, nft):
    raw = pool_event(receipt, operation, MODIFY)
    lower, upper, liquidity, salt = decode(("int24", "int24", "int256", "bytes32"), raw)
    identity = nft if isinstance(operation, Mint) else operation.nft
    expected = (
        operation.liquidity
        if isinstance(operation, Mint)
        else -operation.liquidity
        if isinstance(operation, Burn)
        else 0
    )
    bounds = operation.range if operation.range is not None else operation.target.range
    if (
        bounds is None
        or (lower, upper) != (bounds.lower, bounds.upper)
        or liquidity != expected
        or int.from_bytes(salt, "big") != int(identity.token_id)
    ):
        raise DataError("receipt", "liquidity event differs from the retained NFT, range or amount")
    if isinstance(operation, Mint):
        if not (
            0 <= -deltas[0] <= operation.maximum0.amount
            and 0 <= -deltas[1] <= operation.maximum1.amount
        ):
            raise DataError("receipt", "mint transfers exceed the retained deposit ceilings")
    elif min(deltas) < 0:
        raise DataError("receipt", "liquidity exit unexpectedly spent fund assets")
    if isinstance(operation, Burn) and (
        deltas[0] < operation.minimum0.amount or deltas[1] < operation.minimum1.amount
    ):
        raise DataError("receipt", "burn transfers do not satisfy the retained minima")


def movement(receipt, operation):
    """Fund deltas, the minted NFT and an output-haircut swap's (take, gross output)."""
    target = operation.target
    if isinstance(operation, Swap) and target.native_index is not None:
        from almanak.connectors.uniswap_v4.native_receipts import native_swap_movement

        return native_swap_movement(receipt, operation)
    if isinstance(operation, (Mint, Collect, Burn)) and target.native_index is not None:
        from almanak.connectors.uniswap_v4.native_liquidity_receipts import (
            native_liquidity_movement,
        )

        return native_liquidity_movement(receipt, operation)
    hook = taker(operation)
    output = output_asset(operation) if hook is not None else None
    deltas = tuple(
        token_movement(receipt, target, a, hook if a == output else None) for a in target.assets
    )
    nft = nft_transition(receipt, operation)
    measured = None
    if isinstance(operation, Swap):
        measured = swap_movement(receipt, operation, deltas)
    elif isinstance(operation, (Mint, Collect, Burn)):
        liquidity_movement(receipt, operation, deltas, nft)
    elif isinstance(operation, Approval):
        if (
            deltas != (0, 0)
            or events(receipt, target.pool.pool_manager, SWAP)
            or events(receipt, target.pool.pool_manager, MODIFY)
        ):
            raise DataError("receipt", "approval unexpectedly changed protocol assets")
    return deltas, nft, measured
