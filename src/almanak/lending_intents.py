"""Economic lending requests with explicitly bound connector target types."""

from abc import ABC, abstractmethod
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.models import Account, AssetAmount, Model

Name = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]
# The fund's owned cash of a lending target's asset, shared by every allocation on that
# wallet asset. Only composed decision input carries it; retained records leave it absent.
OwnedCash = Annotated[AssetAmount | None, Field(exclude_if=lambda value: value is None)]


class LendingTarget(Model, ABC):
    allocation: Name
    account: Name
    fund: Account

    @property
    @abstractmethod
    def asset(self) -> AssetType:
        """Underlying asset from the connector's complete market identity."""

    @model_validator(mode="after")
    def asset_scope(self) -> Self:
        if self.asset.chain != self.fund.chain.chain_id:
            raise ValueError("lending target asset has a different fund chain")
        return self


class _LendingIntent[Target: LendingTarget](Model):
    target: Target

    @model_validator(mode="after")
    def concrete_target(self) -> Self:
        # A generic bound or a base target class can discard connector identity.
        if type(self.target) is not type(self).model_fields["target"].annotation:
            raise ValueError("lending intents require their exact concrete target type")
        return self


class SupplyIntent[Target: LendingTarget](_LendingIntent[Target]):
    kind: Literal["supply"] = "supply"
    amount: AssetAmount

    @model_validator(mode="after")
    def quantities(self) -> Self:
        validate_supply_amount(self.target.asset, self.amount)
        return self


class WithdrawIntent[Target: LendingTarget](_LendingIntent[Target]):
    kind: Literal["withdraw"] = "withdraw"
    # Full closure has no requested quantity; null preserves the checkpoint contract.
    amount: None = None


type LendingIntent[Target: LendingTarget] = Annotated[
    SupplyIntent[Target] | WithdrawIntent[Target], Field(discriminator="kind")
]


class BorrowIntent[Target: LendingTarget](_LendingIntent[Target]):
    kind: Literal["borrow"] = "borrow"
    amount: AssetAmount

    @model_validator(mode="after")
    def quantities(self) -> Self:
        validate_supply_amount(self.target.asset, self.amount)
        return self


class WithdrawAmountIntent[Target: LendingTarget](_LendingIntent[Target]):
    kind: Literal["withdraw_amount"] = "withdraw_amount"
    amount: AssetAmount

    @model_validator(mode="after")
    def quantities(self) -> Self:
        validate_supply_amount(self.target.asset, self.amount)
        return self


class RepayIntent[Target: LendingTarget](_LendingIntent[Target]):
    kind: Literal["repay"] = "repay"
    amount: None = None


class RepayAmountIntent[Target: LendingTarget](_LendingIntent[Target]):
    kind: Literal["repay_amount"] = "repay_amount"
    amount: AssetAmount

    @model_validator(mode="after")
    def quantities(self) -> Self:
        validate_supply_amount(self.target.asset, self.amount)
        return self


class CollateralIntent[Target: LendingTarget](_LendingIntent[Target]):
    kind: Literal["collateral"] = "collateral"
    enabled: bool


type EconomicLendingIntent[Target: LendingTarget] = Annotated[
    SupplyIntent[Target]
    | WithdrawIntent[Target]
    | BorrowIntent[Target]
    | WithdrawAmountIntent[Target]
    | RepayIntent[Target]
    | RepayAmountIntent[Target]
    | CollateralIntent[Target],
    Field(discriminator="kind"),
]


def validate_supply_amount(asset: AssetType, amount: AssetAmount) -> None:
    if not 0 < amount.amount < 2**256:
        raise ValueError("supply requires a positive uint256 asset amount")
    if amount.asset != asset:
        raise ValueError("supply amount must identify the target reserve asset")
