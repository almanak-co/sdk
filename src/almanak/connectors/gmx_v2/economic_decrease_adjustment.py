"""Native market decreases can suppress withdrawal or close a subminimum remainder."""

from almanak.connectors.gmx_v2.economic_position_math import signed_div
from almanak.connectors.gmx_v2.economic_precision import apply_factor
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.models import Model, UInt


class DecreaseAdjustmentInput(Model):
    position_usd_e30: UInt
    collateral_amount: UInt
    collateral_minimum_price: UInt
    full_pnl_usd_e30: int
    requested_size_usd_e30: UInt
    requested_withdrawal: UInt
    next_minimum_collateral_factor: UInt
    minimum_collateral_usd_e30: UInt
    minimum_position_usd_e30: UInt


class DecreaseAdjustment(Model):
    size_usd_e30: UInt
    withdrawal: UInt


def adjust_decrease(state):
    if not state.position_usd_e30 or not state.collateral_minimum_price:
        raise ValueError("GMX market decrease requires a nonempty position and positive price")
    # Market decreases cap stale requested notional; limit-order behavior is different.
    size = min(state.requested_size_usd_e30, state.position_usd_e30)
    withdrawal = min(state.requested_withdrawal, state.collateral_amount)
    if size < state.position_usd_e30:
        withdrawal, size = adjust_partial(state, size, withdrawal)
    return DecreaseAdjustment(
        size_usd_e30=size,
        withdrawal=0 if size == state.position_usd_e30 else withdrawal,
    )


def adjust_partial(state, size, withdrawal):
    realized = signed_div(state.full_pnl_usd_e30 * size, state.position_usd_e30)
    remaining_pnl = state.full_pnl_usd_e30 - realized
    remaining_collateral = (
        state.collateral_amount - withdrawal
    ) * state.collateral_minimum_price + min(0, realized)
    required = apply_factor(state.position_usd_e30 - size, state.next_minimum_collateral_factor)
    if (
        remaining_collateral < required
        or remaining_collateral + remaining_pnl < state.minimum_collateral_usd_e30
    ):
        if not size:
            raise SwapConstraint("GMX collateral withdrawal would leave insufficient collateral")
        remaining_collateral += withdrawal * state.collateral_minimum_price
        withdrawal = 0
    if (
        remaining_collateral + remaining_pnl < state.minimum_collateral_usd_e30
        or state.position_usd_e30 - size < state.minimum_position_usd_e30
    ):
        size = state.position_usd_e30
    return withdrawal, size
