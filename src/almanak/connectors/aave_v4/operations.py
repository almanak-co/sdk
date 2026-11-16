"""Validated Aave execution steps produced by the integration's planner."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from almanak.connectors.aave_v4.models import SupplyTarget
from almanak.lending_intents import validate_supply_amount
from almanak.models import AssetAmount, Model


class _AaveOperation(Model):
    target: SupplyTarget

    @property
    def account(self) -> str:
        return self.target.account

    @property
    def allocation(self) -> str:
        return self.target.allocation


class _FundedOperation(_AaveOperation):
    amount: AssetAmount

    @model_validator(mode="after")
    def quantities(self) -> Self:
        validate_supply_amount(self.target.asset, self.amount)
        return self


class ApproveOperation(_FundedOperation):
    kind: Literal["approve"] = "approve"


class SupplyOperation(_FundedOperation):
    kind: Literal["supply"] = "supply"


class BorrowOperation(_FundedOperation):
    kind: Literal["borrow"] = "borrow"


class RepayOperation(_FundedOperation):
    kind: Literal["repay"] = "repay"


class CollateralOperation(_AaveOperation):
    kind: Literal["collateral"] = "collateral"
    collateral: SupplyTarget
    enabled: bool
    amount: None = None

    @model_validator(mode="after")
    def scope(self) -> Self:
        if (self.target.fund, self.target.reserve.spoke) != (
            self.collateral.fund,
            self.collateral.reserve.spoke,
        ):
            raise ValueError("collateral operation must stay within its debt fund and Spoke")
        return self


class WithdrawOperation(_AaveOperation):
    kind: Literal["withdraw"] = "withdraw"
    amount: None = None


class RevokeOperation(_AaveOperation):
    kind: Literal["revoke"] = "revoke"
    amount: AssetAmount

    @model_validator(mode="after")
    def quantities(self) -> Self:
        if self.amount.asset != self.target.asset or self.amount.amount != 0:
            raise ValueError("revocation requires zero of the target reserve asset")
        return self


AaveOperation = Annotated[
    ApproveOperation
    | SupplyOperation
    | WithdrawOperation
    | RevokeOperation
    | BorrowOperation
    | RepayOperation
    | CollateralOperation,
    Field(discriminator="kind"),
]
