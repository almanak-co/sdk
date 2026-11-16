"""Aave V4 target identity and financial observations, independent of decisions."""

from datetime import UTC, datetime
from typing import Self

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.connectors.aave_v4.domain import AaveReserve, PositionObservation
from almanak.lending_intents import LendingTarget, Name
from almanak.models import Account, AssetAmount, Model, UInt


class SupplyTarget(LendingTarget):
    reserve: AaveReserve

    @property
    def asset(self) -> AssetType:
        return self.reserve.asset

    @model_validator(mode="after")
    def scope(self) -> Self:
        if self.fund.chain != self.reserve.chain:
            raise ValueError("supply target fund and reserve have different scope")
        return self

    @property
    def identity(self) -> tuple:
        # Hub/token metadata cannot make a second balance at the same Spoke reserve.
        return self.fund, self.reserve.spoke, self.reserve.reserve_id


class CollateralObservation(Model):
    target: SupplyTarget
    enabled: bool
    supplied_shares: UInt


class LendingSnapshot(Model):
    @property
    def effective_at(self) -> datetime:
        return datetime.fromtimestamp(self.position.reserve.block.timestamp, UTC)

    allocation: Name
    account: Name
    position: PositionObservation
    wallet_balance: UInt
    allowance: UInt
    native_balance: AssetAmount
    simulated: bool = Field(default=False, exclude_if=lambda value: not value)
    payer: Account | None = None
    collateral_enabled: bool = False
    drawn_shares: UInt = 0
    premium_shares: UInt = 0
    premium_offset_ray: int = 0
    health_factor: UInt | None = None
    health_error: str | None = None
    collateral: tuple[CollateralObservation, ...] = ()

    @model_validator(mode="after")
    def native_scope(self) -> Self:
        if self.native_balance.asset.chain != self.position.account.chain.chain_id:
            raise ValueError("native balance has a different account chain")
        return self

    @property
    def target(self) -> SupplyTarget:
        return SupplyTarget(
            allocation=self.allocation,
            account=self.account,
            fund=self.position.account,
            reserve=self.position.reserve.source,
        )

    @property
    def block(self):
        return self.position.reserve.block
