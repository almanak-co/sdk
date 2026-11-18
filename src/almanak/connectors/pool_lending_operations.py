"""Compatible Aave V3 and SparkLend ABI calls with concrete connector targets."""

from typing import Annotated, Literal, Self

from eth_abi import encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.connectors.pool_lending_domain import PoolTarget
from almanak.connectors.pool_lending_math import UINT256_MAX
from almanak.gateway.accounts import ContractCall
from almanak.lending_intents import validate_supply_amount
from almanak.models import AssetAmount, Model


class Operation[Target: PoolTarget](Model):
    target: Target

    @model_validator(mode="after")
    def concrete_target(self) -> Self:
        if type(self.target) is not type(self).model_fields["target"].annotation:
            raise ValueError("Pool operations require their exact concrete connector target")
        return self

    @property
    def account(self) -> str:
        return self.target.account

    @property
    def allocation(self) -> str:
        return self.target.allocation


class FundedOperation[Target: PoolTarget](Operation[Target]):
    amount: AssetAmount

    @model_validator(mode="after")
    def quantities(self) -> Self:
        validate_supply_amount(self.target.asset, self.amount)
        if self.amount.amount == UINT256_MAX:
            raise ValueError("explicit quantities cannot use the Pool's full-balance sentinel")
        return self


class ApproveOperation[Target: PoolTarget](FundedOperation[Target]):
    kind: Literal["approve"] = "approve"
    purpose: Literal["supply", "repay"] = "supply"


class SupplyOperation[Target: PoolTarget](FundedOperation[Target]):
    kind: Literal["supply"] = "supply"


class BorrowOperation[Target: PoolTarget](FundedOperation[Target]):
    kind: Literal["borrow"] = "borrow"


class WithdrawOperation[Target: PoolTarget](FundedOperation[Target]):
    kind: Literal["withdraw"] = "withdraw"
    # The measured amount is evidence. Full closure uses the Pool sentinel so
    # accrued interest cannot leave dust between preparation and inclusion.
    full: bool


class RepayOperation[Target: PoolTarget](FundedOperation[Target]):
    kind: Literal["repay"] = "repay"
    # A full repayment sends the Pool sentinel, which repays the debt at inclusion;
    # its amount is the approved upper bound. A partial amount is exact.
    full: bool


class CollateralOperation[Target: PoolTarget](Operation[Target]):
    kind: Literal["collateral"] = "collateral"
    enabled: bool


class RevokeOperation[Target: PoolTarget](Operation[Target]):
    kind: Literal["revoke"] = "revoke"


type PoolOperation[Target: PoolTarget] = Annotated[
    ApproveOperation[Target]
    | SupplyOperation[Target]
    | BorrowOperation[Target]
    | WithdrawOperation[Target]
    | RepayOperation[Target]
    | CollateralOperation[Target]
    | RevokeOperation[Target],
    Field(discriminator="kind"),
]


def call_arguments(operation: PoolOperation) -> tuple[str, tuple, tuple]:
    asset = operation.target.asset.reference
    fund = operation.target.fund.address
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        amount = operation.amount.amount if isinstance(operation, ApproveOperation) else 0
        return "approve", ("address", "uint256"), (operation.target.reserve.pool, amount)
    if isinstance(operation, SupplyOperation):
        return (
            "supply",
            ("address", "uint256", "address", "uint16"),
            (asset, operation.amount.amount, fund, 0),
        )
    if isinstance(operation, BorrowOperation):
        return (
            "borrow",
            ("address", "uint256", "uint256", "uint16", "address"),
            (asset, operation.amount.amount, 2, 0, fund),
        )
    return exit_arguments(operation, asset, fund)


def exit_arguments(operation, asset, fund) -> tuple[str, tuple, tuple]:
    if isinstance(operation, WithdrawOperation):
        return (
            "withdraw",
            ("address", "uint256", "address"),
            (asset, UINT256_MAX if operation.full else operation.amount.amount, fund),
        )
    if isinstance(operation, RepayOperation):
        return (
            "repay",
            ("address", "uint256", "uint256", "address"),
            (asset, UINT256_MAX if operation.full else operation.amount.amount, 2, fund),
        )
    if isinstance(operation, CollateralOperation):
        return "setUserUseReserveAsCollateral", ("address", "bool"), (asset, operation.enabled)
    raise ValueError("unsupported Pool lending operation")


def plan_call(operation: PoolOperation) -> ContractCall:
    name, types, values = call_arguments(operation)
    selector = keccak(text=f"{name}({','.join(types)})")[:4]
    to = (
        operation.target.asset.reference
        if isinstance(operation, (ApproveOperation, RevokeOperation))
        else operation.target.reserve.pool
    )
    return ContractCall(
        fund=operation.target.fund,
        to=to,
        value=0,
        data="0x" + (selector + encode(types, values)).hex(),
    )
