"""Slipstream uses signed spacing keys and forbids implicit pool initialization."""

from almanak.connectors import v3_pool_abi as family
from almanak.connectors.aerodrome_slipstream.operations import Mint
from almanak.connectors.v3_pool_abi import calldata
from almanak.gateway.accounts import ContractCall

SWAP_PARAMS = "(address,address,int24,address,uint256,uint256,uint256,uint160)"
MINT_PARAMS = (
    "(address,address,int24,int24,int24,uint256,uint256,uint256,uint256,address,uint256,uint160)"
)


def call(operation):
    if isinstance(operation, Mint):
        return ContractCall(
            fund=operation.target.fund,
            to=operation.target.contracts.position_manager,
            value=0,
            data="0x" + mint(operation).hex(),
        )
    return family.call(operation, swap)


def swap(operation):
    target = operation.target
    params = (
        operation.amount.asset.reference,
        operation.minimum.asset.reference,
        target.pool.tick_spacing,
        target.fund.address,
        operation.deadline,
        operation.amount.amount,
        operation.minimum.amount,
        0,
    )
    return calldata(f"exactInputSingle({SWAP_PARAMS})", (SWAP_PARAMS,), (params,))


def mint(operation):
    target = operation.target
    selected = family.operation_range(operation)
    params = (
        target.pool.currency0.reference,
        target.pool.currency1.reference,
        target.pool.tick_spacing,
        selected.lower,
        selected.upper,
        operation.maximum0.amount,
        operation.maximum1.amount,
        operation.minimum0.amount,
        operation.minimum1.amount,
        target.fund.address,
        operation.deadline,
        0,
    )
    return calldata(f"mint({MINT_PARAMS})", (MINT_PARAMS,), (params,))
