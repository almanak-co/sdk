"""Correlated one-hop GMX swap templates pin assets, market and Safe receivers only."""

from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.gmx_v2.candidate_permissions import CREATE_ORDER, word_conditions
from almanak.connectors.gmx_v2.exchange_abi import swap_data
from almanak.connectors.gmx_v2.exchange_models import input_assets
from almanak.connectors.gmx_v2.exchange_operations import CreateSwap
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

INPUT_MARKER, OUTPUT_MARKER = (
    int.from_bytes(keccak(text=f"gmx-swap-template-{name}")[:16], "big")
    for name in ("input", "minimum_output")
)


def above(amount):
    """Strictly more than `amount`: a wrapped-native input escrows its keeper fee with it."""
    return comparison(17, encode(("uint256",), (amount,)))


def approvals(target, assets, explanation):
    """Each escrowed asset may be approved only to GMX Router, with any amount or zero."""
    return [
        FunctionGrant(
            target=asset.reference,
            signature="approve(address,uint256)",
            condition=matches(equal("address", target.market.contracts.token_router), any_amount()),
            explanation=(explanation,),
        )
        for asset in assets
    ]


def swap_branch(capability, asset):
    target, fee = capability.target, capability.maximum_execution_fee
    output = next(value for value in target.assets if value != asset)
    amount = INPUT_MARKER - fee if asset == target.market.wrapped_native else INPUT_MARKER
    template = CreateSwap(
        target=target,
        amount=AssetAmount(asset=asset, amount=amount),
        minimum_output=AssetAmount(asset=output, amount=OUTPUT_MARKER),
        execution_fee=AssetAmount(asset=target.market.wrapped_native, amount=fee),
        admission_deadline=1,
    )
    calls = decode(("bytes[]",), swap_data(template)[4:])[0]
    transferred = []
    for payload, value in zip(calls[:-1], template.inputs, strict=True):
        if value.asset != asset:
            condition = equal("uint256", fee)
        elif asset == target.market.wrapped_native:
            condition = above(fee)
        else:
            condition = positive()
        words = word_conditions(payload[4:], {value.amount: condition})
        transferred.append(nested_call("sendTokens(address,address,uint256)", *words))
    words = word_conditions(calls[-1][4:], {OUTPUT_MARKER: positive()})
    return matches(matches(*transferred, nested_call(CREATE_ORDER, *words), parameter=4))


def derive(capability):
    target = capability.target
    creation = FunctionGrant(
        target=target.market.contracts.exchange_router,
        signature="multicall(bytes[])",
        condition=either(*(swap_branch(capability, asset) for asset in target.assets)),
        explanation=(
            "One configured input transfer and its exact fixed keeper fee precede one market swap.",
            "The one-hop market route and Safe recipients are fixed; "
            "callbacks, UI fees, native unwrap and extra calls are absent.",
            "Positive minimum output is on-chain; the exact requested minimum "
            "and admission deadline are independently enforced by the gateway.",
        ),
    )
    allowed = approvals(
        target,
        input_assets(target.market),
        "Configured input-token allowance only to GMX Router, including zero cleanup.",
    )
    return combine([*allowed, creation])
