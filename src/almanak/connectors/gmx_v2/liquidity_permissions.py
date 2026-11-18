"""Generated GM authority correlates deposits and withdrawals with pinned assets and receivers."""

from eth_abi import decode, encode
from eth_utils import keccak

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.gmx_v2.candidate_permissions import word_conditions
from almanak.connectors.gmx_v2.exchange_models import input_assets
from almanak.connectors.gmx_v2.exchange_permissions import approvals
from almanak.connectors.gmx_v2.liquidity_abi import (
    CREATE_DEPOSIT,
    CREATE_WITHDRAWAL,
    create_data,
)
from almanak.connectors.gmx_v2.liquidity_operations import CreateDeposit, CreateWithdrawal
from almanak.gateway.roles_conditions import (
    FunctionGrant,
    any_amount,
    combine,
    comparison,
    either,
    equal,
    matches,
    nested_call,
    positive,
)
from almanak.models import AssetAmount

MARKERS = tuple(
    int.from_bytes(keccak(text=f"gmx-gm-variable-{index}")[:16], "big") for index in range(5)
)


def minimum_condition(amount):
    """At least `amount`: an author-stated floor, or any amount when the floor is zero."""
    at_least = comparison(17, encode(("uint256",), (max(amount - 1, 0),)))
    return at_least if amount else either(equal("uint256", 0), at_least)


def deposit_template(capability):
    target = capability.target
    return CreateDeposit(
        target=target,
        amounts=AssetAmounts(
            tuple(
                AssetAmount(asset=asset, amount=MARKERS[index])
                for index, asset in enumerate(target.assets)
            )
        ),
        minimum_market_tokens=capability.minimum_market_tokens,
        execution_fee=AssetAmount(
            asset=target.market.wrapped_native, amount=capability.maximum_execution_fee
        ),
        admission_deadline=1,
    )


def withdrawal_template(capability):
    target = capability.target
    return CreateWithdrawal(
        target=target,
        market_tokens=AssetAmount(asset=target.market.share_asset, amount=MARKERS[2]),
        minimum_outputs=AssetAmounts(
            tuple(
                AssetAmount(asset=asset, amount=MARKERS[index + 3])
                for index, asset in enumerate(target.assets)
            )
        ),
        execution_fee=AssetAmount(
            asset=target.market.wrapped_native, amount=capability.maximum_execution_fee
        ),
        admission_deadline=1,
    )


def transfer_condition(operation, value):
    """One escrow transfer: a principal of any amount, its fee, or the keeper fee alone.

    A deposit may leave either market currency at zero. When wrapped native is a deposited
    currency it travels with the keeper fee, so its transfer is at least that fee.
    """
    fee = operation.execution_fee
    economic = (
        {amount.asset for amount in operation.amounts.root}
        if isinstance(operation, CreateDeposit)
        else {operation.market_tokens.asset}
    )
    if value.asset not in economic:
        return equal("uint256", fee.amount)
    if value.asset == fee.asset:
        return minimum_condition(fee.amount)
    return any_amount() if isinstance(operation, CreateDeposit) else positive()


def branch(capability, operation):
    calls = decode(("bytes[]",), create_data(operation)[4:])[0]
    transfers = tuple(
        nested_call(
            "sendTokens(address,address,uint256)",
            *word_conditions(payload[4:], {value.amount: transfer_condition(operation, value)}),
        )
        for payload, value in zip(calls[:-1], operation.inputs, strict=True)
    )
    variables = (
        {}
        if isinstance(operation, CreateDeposit)
        else {
            MARKERS[index + 3]: minimum_condition(value.amount)
            for index, value in enumerate(capability.minimum_outputs.root)
        }
    )
    signature = CREATE_DEPOSIT if isinstance(operation, CreateDeposit) else CREATE_WITHDRAWAL
    request = nested_call(signature, *word_conditions(calls[-1][4:], variables))
    return matches(matches(*transfers, request, parameter=4))


def derive(capability):
    target = capability.target
    templates = (deposit_template(capability), withdrawal_template(capability))
    creation = FunctionGrant(
        target=target.market.contracts.exchange_router,
        signature="multicall(bytes[])",
        condition=either(*(branch(capability, operation) for operation in templates)),
        explanation=(
            "Atomic GM currency or share escrow and exact keeper fee precede one request.",
            "Fund recipients, market, GM minimum, empty routes and local behavior are fixed.",
            "Withdrawal currency floors are on-chain; exact requested minima "
            "and admission deadlines are gateway checks.",
        ),
    )
    allowed = approvals(
        target,
        (*input_assets(target.market), target.market.share_asset),
        "Configured input-token or GM-share allowance only to GMX Router, including zero cleanup.",
    )
    return combine([*allowed, creation])
