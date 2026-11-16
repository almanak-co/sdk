"""Connector-specific router encoding with shared verified V3 manager templates."""

from almanak.connectors import v3_pool_abi as family
from almanak.connectors.v3_pool_abi import (
    COLLECT_PARAMS as COLLECT_PARAMS,
)
from almanak.connectors.v3_pool_abi import (
    DECREASE_PARAMS as DECREASE_PARAMS,
)
from almanak.connectors.v3_pool_abi import (
    INCREASE_PARAMS as INCREASE_PARAMS,
)
from almanak.connectors.v3_pool_abi import (
    MAX128 as MAX128,
)
from almanak.connectors.v3_pool_abi import (
    MINT_PARAMS as MINT_PARAMS,
)
from almanak.connectors.v3_pool_abi import (
    calldata as calldata,
)
from almanak.connectors.v3_pool_abi import (
    collect as collect,
)
from almanak.connectors.v3_pool_abi import (
    liquidity as liquidity,
)
from almanak.connectors.v3_pool_abi import (
    mint as mint,
)


def call(operation):
    return family.call(operation, swap)


SWAP_PARAMS = "(address,address,uint24,address,uint256,uint256,uint160)"


def swap(operation):
    target = operation.target
    params = (
        operation.amount.asset.reference,
        operation.minimum.asset.reference,
        target.pool.fee,
        target.fund.address,
        operation.amount.amount,
        operation.minimum.amount,
        0,
    )
    inner = calldata(f"exactInputSingle({SWAP_PARAMS})", (SWAP_PARAMS,), (params,))
    return calldata(
        "multicall(uint256,bytes[])", ("uint256", "bytes[]"), (operation.deadline, (inner,))
    )
