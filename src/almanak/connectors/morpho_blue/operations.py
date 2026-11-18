"""Morpho operations encode empty callbacks and bind all fund effects to the target."""

from typing import Annotated, Literal

from eth_abi import encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.connectors.morpho_blue.domain import PARAMS_ABI, Target, Uint128
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
        if self.amount.amount >= 2**128:
            raise ValueError("Morpho economic quantity exceeds uint128 market accounting")
        return self


class ApproveOperation(FundedOperation):
    kind: Literal["approve"] = "approve"
    purpose: Literal["supply", "repay"]


class RevokeOperation(Operation):
    kind: Literal["revoke"] = "revoke"


class SupplyOperation(FundedOperation):
    kind: Literal["supply"] = "supply"


class LoanOperation(FundedOperation):
    @model_validator(mode="after")
    def loan_only(self):
        if not self.target.reserve.is_loan:
            raise ValueError("Morpho borrowing and repayment require the loan asset")
        return self


class BorrowOperation(LoanOperation):
    kind: Literal["borrow"] = "borrow"


class RepayOperation(LoanOperation):
    kind: Literal["repay"] = "repay"
    # A nonzero retained share count selects exact full repayment. The amount
    # remains its bounded allowance, not a protocol sentinel or quoted payoff.
    shares: Uint128 = 0

    @property
    def full(self):
        return self.shares != 0


class WithdrawOperation(Operation):
    kind: Literal["withdraw"] = "withdraw"
    amount: AssetAmount
    full: bool
    shares: Uint128 = 0

    @model_validator(mode="after")
    def share_scope(self):
        if self.amount.asset != self.target.asset or self.amount.amount >= 2**128:
            raise ValueError("Morpho withdrawal requires its scoped uint128 asset quantity")
        if not (self.amount.amount or self.shares):
            raise ValueError("Morpho withdrawal requires an asset quantity or owned shares")
        if bool(self.shares) != (self.full and self.target.reserve.is_loan):
            raise ValueError("Morpho full lender exits require their retained supply shares")
        return self


MorphoOperation = Annotated[
    ApproveOperation
    | RevokeOperation
    | SupplyOperation
    | BorrowOperation
    | RepayOperation
    | WithdrawOperation,
    Field(discriminator="kind"),
]


def call_arguments(operation: MorphoOperation):
    market = operation.target.reserve.market
    fund = operation.target.fund.address
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        amount = operation.amount.amount if isinstance(operation, ApproveOperation) else 0
        return "approve", ("address", "uint256"), (market.core, amount)
    if not operation.target.reserve.is_loan:
        if isinstance(operation, SupplyOperation):
            return (
                "supplyCollateral",
                (PARAMS_ABI, "uint256", "address", "bytes"),
                (market.parameters, operation.amount.amount, fund, b""),
            )
        if isinstance(operation, WithdrawOperation):
            return (
                "withdrawCollateral",
                (PARAMS_ABI, "uint256", "address", "address"),
                (market.parameters, operation.amount.amount, fund, fund),
            )
        raise ValueError("Morpho collateral operation cannot borrow or repay")
    return loan_arguments(operation, market.parameters, fund)


def loan_arguments(operation, params, fund):
    callback = isinstance(operation, (SupplyOperation, RepayOperation))
    types = (PARAMS_ABI, "uint256", "uint256", "address", "bytes" if callback else "address")
    shares = operation.shares if isinstance(operation, (RepayOperation, WithdrawOperation)) else 0
    assets = 0 if shares else operation.amount.amount
    return operation.kind, types, (params, assets, shares, fund, b"" if callback else fund)


def plan_call(operation: MorphoOperation) -> ContractCall:
    name, types, values = call_arguments(operation)
    selector = keccak(text=f"{name}({','.join(types)})")[:4]
    token_call = isinstance(operation, (ApproveOperation, RevokeOperation))
    return ContractCall(
        fund=operation.target.fund,
        to=operation.target.asset.reference if token_call else operation.target.reserve.market.core,
        value=0,
        data="0x" + (selector + encode(types, values)).hex(),
    )
