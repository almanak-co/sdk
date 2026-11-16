"""SparkLend liquidation preserves unpaid variable debt when collateral is exhausted."""

from almanak.connectors.sparklend.economic_actions import reserve_inventory
from almanak.connectors.sparklend.economic_math import risk, validate_sources
from almanak.connectors.sparklend.economic_state import LiquidationEffect, Position
from almanak.connectors.sparklend.economic_views import bitmap, position_view
from almanak.connectors.sparklend.liquidation import LiquidationUnavailable, settle
from almanak.connectors.sparklend.math import UINT256_MAX, uint
from almanak.models import AssetAmount, Model


class Change(Model):
    positions: tuple[Position, ...]
    liquidations: tuple[LiquidationEffect, ...]
    blocked: tuple[str, ...]


def available(collateral, debt, markets):
    for index in (collateral, debt):
        market = markets[index]
        config = market.reserve.configuration
        if not config.active or config.paused:
            raise LiquidationUnavailable("liquidation reserve is inactive or paused")


def pair(positions, markets, collateral, debt, pool_positions):
    available(collateral, debt, markets)
    projected = risk(positions, markets).risk
    if projected.health_factor_wad >= 10**18:
        raise LiquidationUnavailable("Pool health is no longer liquidatable")
    c, d = positions[collateral], positions[debt]
    if not c.collateral_enabled or not c.supply_scaled or not d.debt_scaled:
        raise LiquidationUnavailable("liquidation requires owned collateral and debt")
    configuration = bitmap(positions, markets)
    value_c = position_view(c, markets[collateral], configuration, projected)
    value_d = position_view(d, markets[debt], configuration, projected)
    if (
        not value_c.reserve.configuration.liquidation_threshold_bps
        or value_c.reserve.configuration.liquidation_bonus_bps < 10_000
    ):
        raise ValueError("liquidation collateral bonus is unavailable")
    liquidity = value_c.reserve.available_underlying + reserve_inventory(
        pool_positions, c.target.reserve
    )
    if liquidity < 0:
        raise LiquidationUnavailable(
            "hypothetical reserve inventory conflicts with retained market liquidity"
        )
    value_c = value_c.model_copy(
        update={
            "reserve": value_c.reserve.model_copy(update={"available_underlying": uint(liquidity)})
        }
    )
    amounts = settle(
        value_c,
        value_d,
        projected,
        (markets[collateral].supply_index, markets[debt].debt_index),
        UINT256_MAX,
    )
    changed = apply_pair(positions, collateral, debt, amounts)
    effect = LiquidationEffect(
        collateral=c.target,
        debt=d.target,
        collateral_removed=AssetAmount(
            asset=c.target.asset, amount=amounts.collateral_to_liquidator + amounts.protocol_fee
        ),
        debt_restored=AssetAmount(asset=d.target.asset, amount=amounts.debt_repaid),
        protocol_fee=AssetAmount(asset=c.target.asset, amount=amounts.protocol_fee),
    )
    return changed, effect


def apply_pair(positions, collateral, debt, amounts):
    changed = list(positions)
    c = changed[collateral]
    remaining = c.supply_scaled - amounts.collateral_scaled - amounts.fee_scaled
    changed[collateral] = c.model_copy(
        update={
            "supply_scaled": remaining,
            "collateral_enabled": c.collateral_enabled
            and bool(remaining)
            and not amounts.collateral_exhausted,
            "liquidated_collateral": c.liquidated_collateral
            + amounts.collateral_to_liquidator
            + amounts.protocol_fee,
            "liquidation_fees": c.liquidation_fees + amounts.protocol_fee,
        }
    )
    # Debt and collateral can be the same reserve. Apply changes to the updated
    # position so debt bookkeeping cannot restore already transferred collateral.
    d = changed[debt]
    changed[debt] = d.model_copy(
        update={
            "debt_scaled": d.debt_scaled - amounts.debt_scaled,
            "liquidated_debt": d.liquidated_debt + amounts.debt_repaid,
        }
    )
    return tuple(changed)


def candidates(positions):
    return tuple(
        (c, d)
        for c, collateral in enumerate(positions)
        for d, debt in enumerate(positions)
        if collateral.collateral_enabled and collateral.supply_scaled and debt.debt_scaled
    )


def liquidate(config, positions, markets, pool_positions, limit):
    validate_sources(config, positions, markets)
    liquidations, blocked = [], []
    for _ in range(limit):
        if risk(positions, markets).risk.health_factor_wad >= 10**18:
            break
        changed = None
        pairs = candidates(positions)
        if not pairs:
            blocked.append("outstanding debt has no liquidation-eligible collateral")
        for collateral, debt in pairs:
            try:
                changed, effect = pair(positions, markets, collateral, debt, pool_positions)
                break
            except LiquidationUnavailable as exc:
                blocked.append(str(exc))
        if changed is None:
            break
        replacements = {p.target: p for p in changed}
        pool_positions = tuple(replacements.get(p.target, p) for p in pool_positions)
        positions = changed
        liquidations.append(effect)
        blocked.clear()
    else:
        if risk(positions, markets).risk.health_factor_wad < 10**18:
            raise ValueError("liquidation event exceeds its explicit step limit")
    return Change(
        positions=positions,
        liquidations=tuple(liquidations),
        blocked=tuple(dict.fromkeys(blocked)),
    )
