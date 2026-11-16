"""Review-only GMX authority templates. This module does not advertise Safe support."""

from typing import Annotated, Literal, Self

from eth_abi import encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.gateway.evm import erc20_address
from almanak.gateway.roles_conditions import (
    FunctionGrant,
    any_amount,
    bounded,
    combine,
    comparison,
    either,
    equal,
    matches,
    nested_call,
    positive,
)
from almanak.models import ZERO_ADDRESS, Account, Address, Model, UInt

ORDER_PARAMS = (
    "((address,address,address,address,address,address,address[]),"
    "(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),"
    "uint8,uint8,bool,bool,bool,bytes32,bytes32[])"
)
CREATE_ORDER = f"createOrder({ORDER_PARAMS})"
Uint128 = Annotated[UInt, Field(gt=0, lt=2**128)]
MARKERS = tuple(
    int.from_bytes(keccak(text=f"gmx-review-variable-{i}")[:16], "big") for i in range(5)
)


class OrderScope(Model):
    fund: Account
    exchange_router: Address
    token_router: Address
    order_vault: Address
    market: AssetType
    wrapped_native: AssetType
    side: Literal["long", "short"]
    maximum_execution_fee: Uint128

    @model_validator(mode="after")
    def scope(self) -> Self:
        for asset in (self.market, self.wrapped_native):
            erc20_address(asset, self.fund.chain.chain_id)
        return self


def calldata(signature, types, args):
    return keccak(text=signature)[:4] + encode(types, args)


def order_data(scope, order_type, size, fee, price, *, receiver=None, collateral=0):
    fund = receiver or scope.fund.address
    addresses = (
        fund,
        fund,
        ZERO_ADDRESS,
        ZERO_ADDRESS,
        scope.market.reference,
        scope.wrapped_native.reference,
        (),
    )
    numbers = (size, collateral, 0, price, fee, 0, 0, 0)
    params = (addresses, numbers, order_type, 0, scope.side == "long", False, False, bytes(32), ())
    return calldata(CREATE_ORDER, (ORDER_PARAMS,), (params,))


def request_data(scope, order_type, amount, size, fee, price, *, receiver=None, collateral=0):
    order = order_data(
        scope, order_type, size, fee, price, receiver=receiver, collateral=collateral
    )
    transfer = calldata(
        "sendTokens(address,address,uint256)",
        ("address", "address", "uint256"),
        (scope.wrapped_native.reference, scope.order_vault, amount),
    )
    # Roles 2.1 decodes every array element using one shared type tree. Both calls
    # therefore expose the same number of static ABI words after their selectors.
    transfer = transfer.ljust(len(order), b"\0")
    return calldata("multicall(bytes[])", ("bytes[]",), ((transfer, order),))


def word_conditions(payload, variables):
    if len(payload) % 32:
        raise ValueError("GMX normalized argument bytes must contain whole ABI words")
    words = [int.from_bytes(payload[i : i + 32], "big") for i in range(0, len(payload), 32)]
    children = [equal("uint256", word) for word in words]
    for marker, condition in variables.items():
        indices = [i for i, word in enumerate(words) if word == marker]
        if len(indices) != 1:
            raise ValueError("GMX permission marker must identify exactly one ABI word")
        children[indices[0]] = condition
    return tuple(children)


def request_branch(scope, order_type):
    # Size, acceptable price and a decrease's collateral withdrawal take any amount: the
    # receivers, market, side and collateral token stay pinned, and GMX's own leverage
    # limits bound what the escrowed collateral can open.
    collateral = MARKERS[4] if order_type == 4 else 0
    order = order_data(scope, order_type, MARKERS[0], MARKERS[1], MARKERS[2], collateral=collateral)
    order_words = word_conditions(
        order[4:],
        {
            MARKERS[0]: any_amount(),
            MARKERS[1]: bounded(scope.maximum_execution_fee),
            MARKERS[2]: any_amount(),
            **({MARKERS[4]: any_amount()} if collateral else {}),
        },
    )
    transfer = calldata(
        "sendTokens(address,address,uint256)",
        ("address", "address", "uint256"),
        (scope.wrapped_native.reference, scope.order_vault, MARKERS[3]),
    )
    transfer = transfer.ljust(len(order), b"\0")
    # An increase escrows collateral and its keeper fee together in wrapped native. A
    # decrease escrows only the keeper fee, so its transfer keeps the author's fee bound.
    escrow = positive() if order_type == 2 else bounded(scope.maximum_execution_fee)
    transfer_words = word_conditions(transfer[4:], {MARKERS[3]: escrow})
    return matches(
        matches(
            nested_call("sendTokens(address,address,uint256)", *transfer_words),
            nested_call(CREATE_ORDER, *order_words),
            parameter=4,
        )
    )


def candidate_order_grants(scope: OrderScope):
    approval = FunctionGrant(
        target=scope.wrapped_native.reference,
        signature="approve(address,uint256)",
        condition=matches(equal("address", scope.token_router), any_amount()),
        explanation=("Wrapped-native allowance only to GMX Router, including zero cleanup.",),
    )
    creation = FunctionGrant(
        target=scope.exchange_router,
        signature="multicall(bytes[])",
        condition=either(*(request_branch(scope, kind) for kind in (2, 4))),
        explanation=(
            "One wrapped-native transfer to the order vault, "
            "atomically followed by one configured market order.",
            "Fixed fund receivers, side, market and collateral; "
            "no callbacks, swaps, UI fees or native value.",
            "The keeper fee is at most the configured execution fee.",
            "GMX produces request keys. These grants do not authorize cancellation.",
        ),
    )
    return combine([approval, creation])


def candidate_cancellation_grants(exchange_router: str):
    key = comparison(17, encode(("uint256",), (0,)))
    return tuple(
        FunctionGrant(
            target=exchange_router,
            signature=f"cancel{kind}(bytes32)",
            condition=matches(key),
            explanation=(
                "PROPOSAL: any nonzero request key, subject to GMX's existing fund-account check.",
                "This can cancel compatible Safe-owned requests outside this strategy or market.",
                "Existing receivers and callbacks apply; "
                "retained SDK requests fix them to the Safe and zero.",
                "Gateway admission must additionally require the retained unresolved request key.",
            ),
        )
        for kind in ("Order", "Deposit", "Withdrawal")
    )
