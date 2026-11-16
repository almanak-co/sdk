"""GMX order creation atomically transfers exact escrow before storing a request."""

from eth_abi import decode, encode

from almanak.connectors.erc20_approval import approval_succeeded
from almanak.connectors.gmx_v2.candidate_permissions import CREATE_ORDER, ORDER_PARAMS, calldata
from almanak.connectors.gmx_v2.exchange_operations import ExchangeOperation
from almanak.connectors.gmx_v2.operations import Approval, CancelOrder, ClaimFunding, CreateOrder
from almanak.gateway.accounts import ContractCall
from almanak.models import ZERO_ADDRESS


def create_data(operation):
    target = operation.target
    if target.collateral_asset != target.market.wrapped_native:
        raise ValueError("GMX first request template requires wrapped-native collateral")
    params = (
        (
            target.fund.address,
            target.fund.address,
            ZERO_ADDRESS,
            ZERO_ADDRESS,
            target.market.share_asset.reference,
            target.collateral_asset.reference,
            (),
        ),
        (
            operation.size_delta_usd_e30,
            operation.collateral_delta.amount if operation.order_type == 4 else 0,
            0,
            operation.acceptable_price,
            operation.execution_fee.amount,
            0,
            0,
            0,
        ),
        operation.order_type,
        0,
        target.side == "long",
        False,
        False,
        bytes(32),
        (),
    )
    order = calldata(CREATE_ORDER, (ORDER_PARAMS,), (params,))
    transfer = calldata(
        "sendTokens(address,address,uint256)",
        ("address", "address", "uint256"),
        (
            target.market.wrapped_native.reference,
            target.market.contracts.order_vault,
            input_amount(operation),
        ),
    )
    transfer = transfer.ljust(len(order), b"\0")
    return calldata("multicall(bytes[])", ("bytes[]",), ((transfer, order),))


def input_amount(operation):
    return operation.execution_fee.amount + (
        operation.collateral_delta.amount if operation.order_type == 2 else 0
    )


def call(operation):
    if isinstance(operation, ExchangeOperation):
        from almanak.connectors.gmx_v2.exchange_abi import call as exchange_call

        return exchange_call(operation)
    target = operation.target
    if isinstance(operation, Approval):
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
    if isinstance(operation, CreateOrder):
        return create_data(operation)
    if isinstance(operation, ClaimFunding):
        return claim_data(operation.target)
    if isinstance(operation, CancelOrder):
        return calldata(
            "cancelOrder(bytes32)", ("bytes32",), (bytes.fromhex(operation.request_key[2:]),)
        )
    raise ValueError("unsupported GMX protocol operation")


def validate_simulation(operation, result, before):
    if isinstance(operation, ExchangeOperation):
        from almanak.connectors.gmx_v2.exchange_abi import (
            validate_simulation as validate_exchange,
        )

        validate_exchange(operation, result)
        return
    raw = bytes.fromhex(result[2:])
    _simulation_validator(operation)(operation, raw)


def _simulation_validator(operation):
    if isinstance(operation, Approval):
        return _validate_approval
    if isinstance(operation, CreateOrder):
        return _validate_creation
    if isinstance(operation, ClaimFunding):
        return _validate_claim
    return _validate_empty


def _validate_approval(operation, raw):
    if not approval_succeeded(raw):
        raise ValueError("GMX input token refused its simulated approval")


def _validate_creation(operation, raw):
    values = decode(("bytes[]",), raw)[0]
    _validate_creation_values(values)
    if encode(("bytes[]",), (values,)) != raw:
        raise ValueError("GMX simulation returned noncanonical ABI")


def _validate_creation_values(values):
    if len(values) != 2:
        raise ValueError("GMX simulated creation did not return exactly one request key")
    if values[0] != b"":
        raise ValueError("GMX simulated creation did not return exactly one request key")
    if len(values[1]) != 32:
        raise ValueError("GMX simulated creation did not return exactly one request key")
    if not any(values[1]):
        raise ValueError("GMX simulated creation did not return exactly one request key")


def _validate_claim(operation, raw):
    values = decode(("uint256[]",), raw)[0]
    if values != tuple(value.amount for value in operation.amounts):
        raise ValueError("GMX simulated funding claim differs from retained entitlements")
    if encode(("uint256[]",), (values,)) != raw:
        raise ValueError("GMX simulated funding claim returned noncanonical amounts")


def _validate_empty(operation, raw):
    if raw:
        raise ValueError("GMX cancellation returned unexpected data")


def claim_data(target):
    return calldata(
        "claimFundingFees(address[],address[],address)",
        ("address[]", "address[]", "address"),
        (
            (target.market.share_asset.reference,) * 2,
            (target.market.long_asset.reference, target.market.short_asset.reference),
            target.fund.address,
        ),
    )
