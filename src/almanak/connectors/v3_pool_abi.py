"""Exact V3 periphery templates keep token recipients bound to the fund."""

from eth_abi import encode
from eth_utils import keccak

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
from almanak.gateway.accounts import ContractCall

MINT_PARAMS = "(address,address,uint24,int24,int24,uint256,uint256,uint256,uint256,address,uint256)"
INCREASE_PARAMS = "(uint256,uint256,uint256,uint256,uint256,uint256)"
DECREASE_PARAMS = "(uint256,uint128,uint256,uint256,uint256)"
COLLECT_PARAMS = "(uint256,address,uint128,uint128)"
MAX128 = 2**128 - 1


def calldata(signature, types, values):
    return keccak(text=signature)[:4] + encode(types, values)


def call(operation, encode_swap):
    target = operation.target
    if isinstance(operation, Approval):
        to = operation.asset.reference
        data = calldata(
            "approve(address,uint256)",
            ("address", "uint256"),
            (operation.spender, operation.amount),
        )
    elif isinstance(operation, Swap):
        to = target.contracts.router
        data = encode_swap(operation)
    else:
        to = target.contracts.position_manager
        data = liquidity(operation)
    return ContractCall(fund=target.fund, to=to, value=0, data="0x" + data.hex())


def liquidity(operation):
    if isinstance(operation, Mint):
        return mint(operation)
    if isinstance(operation, Increase):
        increase_params = (
            int(operation.nft.token_id),
            operation.maximum0.amount,
            operation.maximum1.amount,
            operation.minimum0.amount,
            operation.minimum1.amount,
            operation.deadline,
        )
        return calldata(
            f"increaseLiquidity({INCREASE_PARAMS})", (INCREASE_PARAMS,), (increase_params,)
        )
    collected = collect(operation)
    if isinstance(operation, Collect):
        return collected
    if not isinstance(operation, (Decrease, Close)):
        raise ValueError("unsupported V3 liquidity operation")
    params = (
        int(operation.nft.token_id),
        operation.liquidity,
        operation.minimum0.amount,
        operation.minimum1.amount,
        operation.deadline,
    )
    inner: tuple[bytes, ...] = (
        calldata(f"decreaseLiquidity({DECREASE_PARAMS})", (DECREASE_PARAMS,), (params,)),
        collected,
    )
    if isinstance(operation, Close):
        inner += (calldata("burn(uint256)", ("uint256",), (int(operation.nft.token_id),)),)
    return calldata("multicall(bytes[])", ("bytes[]",), (inner,))


def mint(operation):
    target = operation.target
    selected = operation_range(operation)
    params = (
        target.pool.currency0.reference,
        target.pool.currency1.reference,
        target.pool.fee,
        selected.lower,
        selected.upper,
        operation.maximum0.amount,
        operation.maximum1.amount,
        operation.minimum0.amount,
        operation.minimum1.amount,
        target.fund.address,
        operation.deadline,
    )
    return calldata(f"mint({MINT_PARAMS})", (MINT_PARAMS,), (params,))


def collect(operation):
    params = (int(operation.nft.token_id), operation.target.fund.address, MAX128, MAX128)
    return calldata(f"collect({COLLECT_PARAMS})", (COLLECT_PARAMS,), (params,))
