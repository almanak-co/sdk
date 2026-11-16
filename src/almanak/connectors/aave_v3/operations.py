"""Aave operations bind the shared Pool ABI to this connector's exact target."""

from typing import Annotated

from pydantic import Field

from almanak.connectors import pool_lending_operations as shared
from almanak.connectors.aave_v3.domain import Target
from almanak.connectors.pool_lending_operations import call_arguments as call_arguments
from almanak.connectors.pool_lending_operations import exit_arguments as exit_arguments
from almanak.connectors.pool_lending_operations import plan_call as plan_call


class ApproveOperation(shared.ApproveOperation[Target]):
    pass


class SupplyOperation(shared.SupplyOperation[Target]):
    pass


class BorrowOperation(shared.BorrowOperation[Target]):
    pass


class WithdrawOperation(shared.WithdrawOperation[Target]):
    pass


class RepayOperation(shared.RepayOperation[Target]):
    pass


class CollateralOperation(shared.CollateralOperation[Target]):
    pass


class RevokeOperation(shared.RevokeOperation[Target]):
    pass


AaveOperation = Annotated[
    ApproveOperation
    | SupplyOperation
    | BorrowOperation
    | WithdrawOperation
    | RepayOperation
    | CollateralOperation
    | RevokeOperation,
    Field(discriminator="kind"),
]
