"""Hypothetical positions and keeper escrow carry simulation identity without on-chain keys."""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import InputGap
from almanak.backtest.types import UnixTime
from almanak.connectors.gmx_v2.abi import input_amount
from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.intents import (
    ClosePosition,
    DecreasePosition,
    IncreasePosition,
    RevokePosition,
)
from almanak.connectors.gmx_v2.operations import CreateOrder
from almanak.connectors.identifiers import Name
from almanak.effects import AssetDelta
from almanak.models import Account, AssetAmount, Block, Model, UInt
from almanak.strategy_api import SimulationEvidence
from almanak.triggers import UtcInstant

PositionIntent = Annotated[
    IncreasePosition | DecreasePosition | ClosePosition | RevokePosition,
    Field(discriminator="kind"),
]


class SimulatedPosition(Model):
    identity: Annotated[str, Field(min_length=1)]
    size_usd_e30: UInt
    index_amount: AssetAmount
    collateral: AssetAmount
    pending_impact_amount: int
    borrowing_factor: UInt
    funding_fee_amount_per_size: UInt
    long_claimable_funding_amount_per_size: UInt
    short_claimable_funding_amount_per_size: UInt
    increased_at: UnixTime
    decreased_at: UnixTime


class PerpetualOwnership(Model):
    target: Target
    inventory: tuple[AssetAmount, ...]
    position: SimulatedPosition | None
    claimable_funding: tuple[AssetAmount, ...]
    deferred_collateral: tuple[AssetAmount, ...]

    @model_validator(mode="after")
    def scope(self):
        market = self.target.market
        expected = (market.long_asset, market.short_asset)
        if tuple(value.asset for value in self.claimable_funding) != expected:
            raise ValueError("simulated GMX funding claims must identify both market assets")
        if tuple(value.asset for value in self.deferred_collateral) != expected:
            raise ValueError("simulated GMX deferred collateral must identify both market assets")
        assets = tuple(value.asset for value in self.inventory)
        if len(set(assets)) != len(assets) or any(
            asset.chain != self.target.fund.chain.chain_id for asset in assets
        ):
            raise ValueError("simulated GMX cash requires unique fund-chain assets")
        if self.position is not None and (
            self.position.index_amount.asset != market.index_asset
            or self.position.collateral.asset != self.target.collateral_asset
            or not self.position.size_usd_e30
            or not self.position.index_amount.amount
        ):
            raise ValueError("simulated GMX position requires its exact nonempty asset units")
        return self


class PerpetualEscrow(Model):
    identity: Annotated[str, Field(min_length=1)]
    accepted_at: UnixTime
    intent: PositionIntent
    request: CreateOrder

    @property
    def inputs(self):
        return (self.request.collateral_delta,) if self.request.order_type == 2 else ()

    @property
    def execution_fee(self):
        return self.request.execution_fee

    @model_validator(mode="after")
    def scope(self):
        if self.request.target != self.intent.target or isinstance(self.intent, RevokePosition):
            raise ValueError("simulated GMX escrow requires its exact admitted position action")
        if (self.request.order_type == 2) != isinstance(self.intent, IncreasePosition):
            raise ValueError("simulated GMX order direction differs from its admitted action")
        if isinstance(self.intent, (IncreasePosition, DecreasePosition)):
            collateral = (
                self.intent.collateral
                if isinstance(self.intent, IncreasePosition)
                else self.intent.collateral_withdrawal
            )
            if self.request.collateral_delta != collateral or (
                self.request.size_delta_usd_e30 != self.intent.size_delta_usd_e30
            ):
                raise ValueError("simulated GMX escrow changed its admitted economic amounts")
        return self


class EconomicPerpetualPosition(Model):
    target: Target
    owned: PerpetualOwnership
    spent: tuple[AssetAmount, ...]
    spent_notional_usd_e30: UInt
    allowances: tuple[AssetAmount, ...]
    escrow: PerpetualEscrow | None

    @model_validator(mode="after")
    def scope(self):
        if self.target != self.owned.target or (
            self.escrow is not None and self.escrow.intent.target != self.target
        ):
            raise ValueError("simulated GMX position, ownership and escrow require one target")
        expected = (self.target.market.wrapped_native,)
        if (
            tuple(value.asset for value in self.spent) != expected
            or tuple(value.asset for value in self.allowances) != expected
        ):
            raise ValueError("simulated GMX position authority requires wrapped-native units")
        if self.escrow is not None and input_amount(self.escrow.request) > self.spent[0].amount:
            raise ValueError("simulated GMX position escrow exceeds its retained spending")
        if (
            self.escrow is not None
            and self.escrow.request.order_type == 2
            and (self.escrow.request.size_delta_usd_e30 > self.spent_notional_usd_e30)
        ):
            raise ValueError("simulated GMX position escrow exceeds its retained notional spending")
        return self


class EconomicPerpetualSnapshot(Model):
    target: Target
    block: Block
    owned: PerpetualOwnership
    wallets: tuple[AssetAmount, ...]
    allowance: UInt
    native_balance: AssetAmount
    payer: Account
    simulation_time: UnixTime
    simulated: Literal[True] = True
    escrow: PerpetualEscrow | None

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def position(self):
        return self.owned.position

    @property
    def claimable_funding(self):
        return self.owned.claimable_funding

    @property
    def request_pending(self):
        return self.escrow is not None

    @property
    def needs_cleanup(self):
        return (
            self.request_pending
            or bool(self.allowance)
            or any(value.amount for value in self.claimable_funding)
            or any(value.amount for value in self.owned.deferred_collateral)
        )

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.simulation_time, UTC)

    @model_validator(mode="after")
    def scope(self):
        if (
            self.owned.target != self.target
            or self.block.chain != self.target.fund.chain
            or self.payer.chain != self.block.chain
            or self.block.timestamp > self.simulation_time
        ):
            raise ValueError("simulated GMX snapshot requires its canonical target and clock")
        if tuple(value.asset for value in self.wallets) != tuple(
            value.asset for value in self.owned.inventory
        ):
            raise ValueError("simulated GMX wallets must cover every owned cash asset in order")
        if self.escrow is not None and self.escrow.intent.target != self.target:
            raise ValueError("simulated GMX snapshot has another target's escrow")
        return self


class EconomicPerpetualResult(Model):
    kind: Literal["perpetual_action"] = "perpetual_action"
    intent: PositionIntent
    success: bool
    complete: bool
    phase: Literal["accepted", "waiting", "settled", "revoked", "cancelled", "claimed"]
    reason: str | None
    effective_at: UtcInstant
    simulation: SimulationEvidence
    before: EconomicPerpetualSnapshot
    after: EconomicPerpetualPosition
    assets: tuple[AssetDelta, ...]
    gas_paid: AssetAmount
    keeper_paid: AssetAmount
    fund_refund: AssetAmount

    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        if not self.success:
            return "failed"
        return "completed" if self.complete else "intermediate"

    @model_validator(mode="after")
    def scope(self):
        if self.before.target != self.intent.target or self.after.target != self.intent.target:
            raise ValueError("simulated perpetual result requires its admitted target")
        cleanup = isinstance(self.intent, RevokePosition)
        outstanding = self.after.escrow is not None or (cleanup and cleanup_outstanding(self.after))
        validate_completion(self.complete, self.phase, outstanding)
        validate_cancellation(self.success, self.phase, self.reason, cleanup)
        validate_native_units(
            self.before.native_balance.asset, (self.gas_paid, self.keeper_paid, self.fund_refund)
        )
        return self


def cleanup_outstanding(payload):
    owned = payload.owned
    return any(
        value.amount
        for value in (*payload.allowances, *owned.claimable_funding, *owned.deferred_collateral)
    )


def validate_completion(complete, phase, outstanding):
    if complete == outstanding or (complete and phase in ("accepted", "waiting")):
        raise ValueError("simulated perpetual completion differs from retained obligations")


def validate_cancellation(success, phase, reason, cleanup):
    cancelled = phase == "cancelled"
    if success != (not cancelled or cleanup) or (reason is None) == cancelled:
        raise ValueError("simulated perpetual cancellation must retain its failure reason")


def validate_native_units(native, amounts):
    if any(value.asset != native for value in amounts):
        raise ValueError("simulated perpetual costs must retain native fund/payer units")


class EconomicLiquidationResult(Model):
    kind: Literal["perpetual_liquidation"] = "perpetual_liquidation"
    allocation: Name
    success: Literal[True] = True
    complete: Literal[True] = True
    effective_at: UtcInstant
    simulation: SimulationEvidence
    before: EconomicPerpetualPosition
    after: EconomicPerpetualPosition
    assets: tuple[AssetDelta, ...]
    reason: str
    # Costs the exhausted collateral could not pay stay visible instead of vanishing.
    unpaid_usd_e30: UInt = 0
    fees_collected: bool = True

    @property
    def status(self) -> Literal["completed"]:
        return "completed"

    @model_validator(mode="after")
    def scope(self):
        if (
            self.before.target != self.after.target
            or self.allocation != self.before.target.allocation
            or self.before.owned.position is None
            or self.after.owned.position is not None
            or self.before.escrow != self.after.escrow
        ):
            raise ValueError(
                "simulated liquidation must close its position and preserve pending work"
            )
        return self


class EconomicPerpetualFailure(Model):
    kind: Literal["perpetual_failure"] = "perpetual_failure"
    intent: PositionIntent
    success: Literal[False] = False
    complete: bool
    effective_at: UtcInstant
    simulation: SimulationEvidence
    error: str
    unavailable: InputGap | None

    @property
    def status(self) -> Literal["failed"]:
        return "failed"


class EconomicLiquidationDeferral(Model):
    """A liquidatable position the protocol could not liquidate at this publication.

    The position stays open and the sweep continues; ``eligibility`` is the risk
    reason that selected it, ``reason`` why execution was refused, and
    ``unavailable`` the prepared-input gap when the position could not be read.
    """

    kind: Literal["perpetual_liquidation_deferred"] = "perpetual_liquidation_deferred"
    allocation: Name
    effective_at: UtcInstant
    simulation: SimulationEvidence
    eligibility: str | None
    reason: str
    unavailable: InputGap | None = None


class EconomicLiquidationBatch(Model):
    kind: Literal["perpetual_liquidations"] = "perpetual_liquidations"
    results: tuple[EconomicLiquidationResult, ...] = ()
    deferrals: tuple[EconomicLiquidationDeferral, ...] = ()

    @property
    def entries(self):
        return (*self.results, *self.deferrals)

    @property
    def effective_at(self):
        return self.entries[0].effective_at

    @model_validator(mode="after")
    def scope(self):
        if not self.entries:
            raise ValueError("simulated liquidation batch requires at least one outcome")
        identities = tuple(value.before.target.position_scope for value in self.results)
        allocations = tuple(value.allocation for value in self.entries)
        if len(set(identities)) != len(identities) or len(set(allocations)) != len(allocations):
            raise ValueError("simulated liquidation batch cannot repeat a position or allocation")
        if len({value.effective_at for value in self.entries}) != 1:
            raise ValueError("simulated liquidation batch requires one evaluation time")
        return self
