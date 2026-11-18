"""One admitted GMX swap atomically escrows its input and wrapped-native keeper fee."""

from eth_abi import decode, encode

from almanak.connectors.erc20_approval import approval_succeeded
from almanak.connectors.gmx_v2.candidate_permissions import CREATE_ORDER, ORDER_PARAMS, calldata
from almanak.connectors.gmx_v2.exchange_operations import (
    CancelExchangeOrder,
    CreateSwap,
    ExchangeApproval,
)
from almanak.connectors.gmx_v2.liquidity_operations import (
    CancelLiquidityRequest,
    CreateLiquidity,
)
from almanak.gateway.accounts import ContractCall
from almanak.models import ZERO_ADDRESS


def swap_data(operation):
    target = operation.target
    params = (
        (
            target.fund.address,
            target.fund.address,
            ZERO_ADDRESS,
            ZERO_ADDRESS,
            ZERO_ADDRESS,
            operation.amount.asset.reference,
            (target.market.share_asset.reference,),
        ),
        (0, 0, 0, 0, operation.execution_fee.amount, 0, operation.minimum_output.amount, 0),
        0,
        0,
        False,
        False,
        False,
        bytes(32),
        (),
    )
    order = calldata(CREATE_ORDER, (ORDER_PARAMS,), (params,))
    transfers = tuple(
        calldata(
            "sendTokens(address,address,uint256)",
            ("address", "address", "uint256"),
            (value.asset.reference, target.market.contracts.order_vault, value.amount),
        ).ljust(len(order), b"\0")
        for value in operation.inputs
    )
    return calldata("multicall(bytes[])", ("bytes[]",), ((*transfers, order),))


def call(operation):
    target = operation.target
    if isinstance(operation, ExchangeApproval):
        to = operation.amount.asset.reference
        payload = calldata(
            "approve(address,uint256)",
            ("address", "uint256"),
            (target.market.contracts.token_router, operation.amount.amount),
        )
    else:
        to = target.market.contracts.exchange_router
        payload = _router_payload(operation)
    return ContractCall(fund=target.fund, to=to, value=0, data="0x" + payload.hex())


def _router_payload(operation):
    if isinstance(operation, CreateSwap):
        return swap_data(operation)
    if isinstance(operation, CreateLiquidity):
        from almanak.connectors.gmx_v2.liquidity_abi import create_data

        return create_data(operation)
    if isinstance(operation, CancelLiquidityRequest):
        from almanak.connectors.gmx_v2.liquidity_abi import cancel_data

        return cancel_data(operation)
    if isinstance(operation, CancelExchangeOrder):
        return calldata(
            "cancelOrder(bytes32)", ("bytes32",), (bytes.fromhex(operation.request_key[2:]),)
        )
    raise ValueError("unsupported GMX exchange operation")


def validate_simulation(operation, result):
    raw = bytes.fromhex(result[2:])
    if isinstance(operation, ExchangeApproval):
        if not approval_succeeded(raw):
            raise ValueError("GMX exchange token refused its simulated approval")
    elif isinstance(operation, (CreateSwap, CreateLiquidity)):
        values = decode(("bytes[]",), raw)[0]
        if (
            len(values) != len(operation.inputs) + 1
            or any(values[:-1])
            or len(values[-1]) != 32
            or not any(values[-1])
        ):
            raise ValueError("GMX swap simulation requires empty transfers and one request key")
        if encode(("bytes[]",), (values,)) != raw:
            raise ValueError("GMX swap simulation returned noncanonical ABI")
    elif raw:
        raise ValueError("GMX exchange cancellation returned unexpected data")
