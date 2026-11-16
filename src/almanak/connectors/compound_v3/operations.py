"""Direct Comet calls keep economic intent separate from its multipurpose ABI."""

from typing import Annotated, Literal

from eth_abi import encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.connectors.compound_v3.domain import Target
from almanak.connectors.compound_v3.math import UINT256_MAX
from almanak.gateway.accounts import ContractCall
from almanak.lending_intents import validate_supply_amount
from almanak.models import AssetAmount, Model


class Operation(Model):
    target: Target

    @property
    def account(self):
        return self.target.account

    @property
    def allocation(self):
        return self.target.allocation


class FundedOperation(Operation):
    amount: AssetAmount

    @model_validator(mode="after")
    def scope(self):
        validate_supply_amount(self.target.asset, self.amount)
        if self.amount.amount == UINT256_MAX:
            raise ValueError("Compound explicit quantities cannot use a full-balance sentinel")
        return self


class ApproveOperation(FundedOperation):
    kind: Literal["approve"] = "approve"
    purpose: Literal["supply", "repay"]


class RevokeOperation(Operation):
    kind: Literal["revoke"] = "revoke"


class SupplyOperation(FundedOperation):
    kind: Literal["supply"] = "supply"


class BaseOperation(FundedOperation):
    @model_validator(mode="after")
    def base_only(self):
        if not self.target.reserve.is_base:
            raise ValueError("Compound borrowing and repayment require the market base asset")
        return self


class BorrowOperation(BaseOperation):
    kind: Literal["borrow"] = "borrow"


class RepayOperation(BaseOperation):
    kind: Literal["repay"] = "repay"
    # Full repayment uses the debt sentinel, which repays the debt at inclusion.
    # Its finite amount is the approved upper bound, not a protocol quantity.
    full: bool


class WithdrawOperation(FundedOperation):
    kind: Literal["withdraw"] = "withdraw"
    # Only base earns interest and supports the full-balance sentinel.
    # Collateral exits retain their exact non-interest-bearing owned quantity.
    full: bool


CompoundOperation = Annotated[
    ApproveOperation
    | RevokeOperation
    | SupplyOperation
    | BorrowOperation
    | RepayOperation
    | WithdrawOperation,
    Field(discriminator="kind"),
]


def call_arguments(operation: CompoundOperation):
    types = ("address", "uint256")
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        amount = operation.amount.amount if isinstance(operation, ApproveOperation) else 0
        return "approve", types, (operation.target.reserve.market.comet, amount)
    asset = operation.target.asset.reference
    if isinstance(operation, (SupplyOperation, RepayOperation)):
        amount = (
            UINT256_MAX
            if isinstance(operation, RepayOperation) and operation.full
            else operation.amount.amount
        )
        return "supply", types, (asset, amount)
    return withdrawal_arguments(operation, asset, types)


def withdrawal_arguments(operation, asset, types):
    if isinstance(operation, BorrowOperation):
        return "withdraw", types, (asset, operation.amount.amount)
    if isinstance(operation, WithdrawOperation):
        amount = (
            UINT256_MAX
            if operation.full and operation.target.reserve.is_base
            else operation.amount.amount
        )
        return "withdraw", types, (asset, amount)
    raise ValueError("unsupported Compound operation")


def plan_call(operation: CompoundOperation) -> ContractCall:
    name, types, values = call_arguments(operation)
    selector = keccak(text=f"{name}({','.join(types)})")[:4]
    token_call = isinstance(operation, (ApproveOperation, RevokeOperation))
    return ContractCall(
        fund=operation.target.fund,
        to=operation.target.asset.reference
        if token_call
        else operation.target.reserve.market.comet,
        value=0,
        data="0x" + (selector + encode(types, values)).hex(),
    )
