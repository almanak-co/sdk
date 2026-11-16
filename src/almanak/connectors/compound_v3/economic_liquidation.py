"""Immediate whole-fund Comet absorption preserves credit, deficits and excluded assets."""

from almanak.connectors.compound_v3.economic_state import (
    AbsorptionEffect,
    ModeledIndices,
    Position,
)
from almanak.connectors.compound_v3.economic_views import account_view, risk_account
from almanak.connectors.compound_v3.liquidation import absorb
from almanak.connectors.compound_v3.projection import validate_sources
from almanak.connectors.compound_v3.risk import is_liquidatable
from almanak.models import Model


class Change(Model):
    positions: tuple[Position, ...]
    absorptions: tuple[AbsorptionEffect, ...]


def liquidate(config, positions, market):
    if not positions:
        raise ValueError("economic Comet absorption requires a configured fund group")
    fund = positions[0].target.fund
    if any(
        p.target.fund != fund or p.target.reserve.market != market.observed.source
        for p in positions
    ):
        raise ValueError("economic Comet absorption requires one exact fund/market group")
    account = account_view(positions, market, fund)
    validate_sources(config, account)
    projected = risk_account(account, market)
    if not is_liquidatable(projected):
        return Change(positions=positions, absorptions=())
    if market.observed.accounting.pause_flags & (1 << 3):
        raise ValueError(
            "economic Comet immediate absorption is unavailable while absorb is paused"
        )
    result = absorb(projected)
    base = tuple(p for p in positions if p.target.reserve.is_base)
    if len(base) != 1:
        raise ValueError(
            "economic Comet absorption requires ownership of its signed base principal"
        )
    seized = {c.asset: c.balance for c in result.seized}
    remaining = {c.asset: c.balance for c in result.remaining}
    changed = []
    for position in positions:
        if position.target.reserve.is_base:
            updates = {
                "principal": result.principal,
                "absorption_credit": position.absorption_credit + result.collateral_credit.amount,
                "absorbed_deficit": position.absorbed_deficit + result.absorbed_deficit.amount,
            }
        else:
            updates = {
                "collateral": remaining[position.target.asset],
                "seized_collateral": position.seized_collateral + seized[position.target.asset],
                "collateral_enabled": False,
            }
        changed.append(
            Position.model_validate_json(position.model_copy(update=updates).model_dump_json())
        )
    return Change(
        positions=tuple(changed),
        absorptions=(
            AbsorptionEffect(
                absorption=result,
                indices=ModeledIndices(supply=market.supply_index, borrow=market.borrow_index),
            ),
        ),
    )
