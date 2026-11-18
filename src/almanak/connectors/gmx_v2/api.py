"""Strategy helpers express economic GMX actions using retained observations."""

from almanak.connectors.gmx_v2.exchange_domain import (
    Close,
    Decrease,
    ExchangeTarget,
    Increase,
    Open,
    Revoke,
    Swap,
)
from almanak.connectors.gmx_v2.intents import (
    ClosePosition,
    DecreasePosition,
    IncreasePosition,
    RevokePosition,
)
from almanak.connectors.gmx_v2.liquidation_types import LiquidationResult
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation
from almanak.connectors.gmx_v2.state import Decision, DecisionInput, OutcomeInput
from almanak.models import AssetAmount

__all__ = ["Actions", "Decision", "DecisionInput", "OutcomeInput", "LiquidationResult"]


class Actions:
    def __init__(self, context):
        self.context = context

    def swap(self, name, *, amount: AssetAmount, minimum_output: AssetAmount):
        """Request an asynchronous keeper swap of `amount` with an explicit `minimum_output`."""
        observed = self.context.require_observation(name)
        allocation = self.context.config.allocation(name)
        return Swap(
            target=observed.target,
            amount=amount,
            minimum_output=minimum_output,
            deadline=observed.block.timestamp + allocation.deadline_seconds,
        )

    def open_liquidity(self, name, *, maximum0: AssetAmount, maximum1: AssetAmount):
        """Deposit up to `maximum0`/`maximum1` of the market currencies for GM tokens."""
        return self._deposit(Open, name, maximum0, maximum1)

    def increase_liquidity(self, name, *, maximum0: AssetAmount, maximum1: AssetAmount):
        """Deposit up to `maximum0`/`maximum1` more into the owned GM tokens."""
        return self._deposit(Increase, name, maximum0, maximum1)

    def _deposit(self, kind, name, maximum0, maximum1):
        observed = self.context.require_observation(name)
        return kind(
            target=observed.target,
            maximum0=maximum0,
            maximum1=maximum1,
            deadline=observed.block.timestamp
            + self.context.config.allocation(name).deadline_seconds,
        )

    def decrease_liquidity(
        self, name, *, shares: AssetAmount, minimum0: AssetAmount, minimum1: AssetAmount
    ):
        """Redeem `shares` GM market tokens for at least `minimum0`/`minimum1`."""
        observed = self.context.require_observation(name)
        if shares.asset != observed.target.market.share_asset:
            raise ValueError("GM partial withdrawal requires its exact configured share asset")
        return Decrease(
            target=observed.target,
            liquidity=shares.amount,
            minimum0=minimum0,
            minimum1=minimum1,
            deadline=observed.block.timestamp
            + self.context.config.allocation(name).deadline_seconds,
        )

    def close_liquidity(self, name, *, minimum0: AssetAmount, minimum1: AssetAmount):
        """Redeem all owned GM market tokens for at least `minimum0`/`minimum1`."""
        observed = self.context.require_observation(name)
        return Close(
            target=observed.target,
            minimum0=minimum0,
            minimum1=minimum1,
            deadline=observed.block.timestamp
            + self.context.config.allocation(name).deadline_seconds,
        )

    def increase(self, name, *, collateral_base_units, size_delta_usd_e30):
        """Open or grow the perpetual position by collateral and USD size.

        `collateral_base_units` is in the collateral asset; `size_delta_usd_e30` has 30 decimals.
        """
        observed = self.context.require_observation(name)
        return IncreasePosition(
            target=observed.target,
            collateral=AssetAmount(
                asset=observed.target.collateral_asset, amount=collateral_base_units
            ),
            size_delta_usd_e30=size_delta_usd_e30,
        )

    def decrease(self, name, *, collateral_base_units, size_delta_usd_e30):
        """Shrink the perpetual position by `size_delta_usd_e30` and withdraw collateral.

        `collateral_base_units` of the collateral asset are withdrawn alongside the size change.
        """
        observed = self.context.require_observation(name)
        return DecreasePosition(
            target=observed.target,
            collateral_withdrawal=AssetAmount(
                asset=observed.target.collateral_asset, amount=collateral_base_units
            ),
            size_delta_usd_e30=size_delta_usd_e30,
        )

    def close(self, name):
        """Close the whole perpetual position through a keeper order."""
        return ClosePosition(target=self.context.require_observation(name).target)

    def revoke(self, name):
        """Zero the allocation's remaining allowances (exchange or position authority)."""
        target = self.context.require_observation(name).target
        return (
            Revoke(target=target)
            if isinstance(target, ExchangeTarget)
            else RevokePosition(target=target)
        )

    def exit(self, name):
        """Next exit/teardown step, or None when nothing remains; call it every decision.

        Perpetuals close then revoke; swap allocations swap non-funding inventory to the
        funding asset at `exit_minimum_output`; GM liquidity redeems at `withdrawal_minimums`.
        """
        observed = self.context.require_observation(name)
        if isinstance(observed.target, ExchangeTarget):
            return self.exit_exchange(name, observed)
        if observed.owned.position is not None:
            return ClosePosition(target=observed.target)
        return RevokePosition(target=observed.target) if observed.needs_cleanup else None

    def exit_exchange(self, name, observed):
        if observed.request_pending:
            return Revoke(target=observed.target)
        if isinstance(self.context.config.allocation(name), LiquidityAllocation):
            return self.exit_liquidity(name, observed)
        if observed.owned.market_tokens.amount:
            raise ValueError("GMX swap exit cannot release unproven GM liquidity")
        other = next(
            value
            for value in observed.owned.inventory
            if value.asset in observed.target.assets
            and value.asset != observed.target.funding_asset
        )
        if other.amount:
            return self.swap(
                name,
                amount=other,
                minimum_output=self.context.config.allocation(name).exit_minimum_output,
            )
        return Revoke(target=observed.target) if observed.needs_cleanup else None

    def exit_liquidity(self, name, observed):
        if observed.owned.market_tokens.amount:
            minimum0, minimum1 = self.context.config.allocation(name).withdrawal_minimums.root
            return self.close_liquidity(name, minimum0=minimum0, minimum1=minimum1)
        return Revoke(target=observed.target) if observed.needs_cleanup else None
