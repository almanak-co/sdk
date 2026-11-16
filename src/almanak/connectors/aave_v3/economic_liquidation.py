"""Deterministic V3 liquidation and bad-debt effects over hypothetical Pool ownership."""

from almanak.connectors.aave_v3.economic_actions import reserve_inventory
from almanak.connectors.aave_v3.economic_math import risk, validate_sources
from almanak.connectors.aave_v3.economic_state import LiquidationEffect, Position
from almanak.connectors.aave_v3.economic_views import bitmap, position_view
from almanak.connectors.aave_v3.liquidation import LiquidationUnavailable, settle
from almanak.connectors.aave_v3.math import UINT256_MAX
from almanak.models import AssetAmount, Model


class Change(Model):
    positions: tuple[Position, ...]
    liquidations: tuple[LiquidationEffect, ...]
    deficits: tuple[AssetAmount, ...]
    blocked: tuple[str, ...]


def available(collateral, debt, markets):
    for index in (collateral, debt):
        market = markets[index]
        config = market.reserve.configuration
        if not config.active or config.paused:
            raise LiquidationUnavailable("liquidation reserve is inactive or paused")
        if market.sample.liquidation_grace_until >= market.time:
            raise LiquidationUnavailable("liquidation is inside the reserve grace period")


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
    if value_c.reserve.configuration.liquidation_bonus_bps < 10_000:
        raise ValueError("liquidation collateral bonus is unavailable")
    amounts = settle(
        value_c,
        value_d,
        projected,
        (markets[collateral].supply_index, markets[debt].debt_index),
        UINT256_MAX,
    )
    liquidity = min(
        value_c.reserve.available_underlying, value_c.reserve.virtual_underlying_balance
    )
    if amounts.collateral_to_liquidator > liquidity + reserve_inventory(
        pool_positions, c.target.reserve
    ):
        raise LiquidationUnavailable(
            "reserve cannot settle underlying collateral to the liquidator"
        )
    changed, deficits = apply_pair(positions, markets, collateral, debt, amounts)
    effect = LiquidationEffect(
        collateral=c.target,
        debt=d.target,
        collateral_removed=AssetAmount(
            asset=c.target.asset, amount=amounts.collateral_to_liquidator + amounts.protocol_fee
        ),
        debt_restored=AssetAmount(asset=d.target.asset, amount=amounts.debt_repaid),
        protocol_fee=AssetAmount(asset=c.target.asset, amount=amounts.protocol_fee),
    )
    return changed, effect, deficits


def apply_pair(positions, markets, collateral, debt, amounts):
    changed = list(positions)
    c = changed[collateral]
    remaining = c.supply_scaled - amounts.collateral_scaled - amounts.fee_scaled
    changed[collateral] = c.model_copy(
        update={
            "supply_scaled": remaining,
            "collateral_enabled": c.collateral_enabled
            and bool(remaining)
            and not amounts.no_collateral_left,
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
            "deficit": d.deficit + amounts.deficit,
        }
    )
    deficits = (
        (AssetAmount(asset=d.target.asset, amount=amounts.deficit),) if amounts.deficit else ()
    )
    if amounts.no_collateral_left:
        changed, additional = bad_debt(tuple(changed), markets)
        return tuple(changed), (*deficits, *additional)
    return tuple(changed), deficits


def bad_debt(positions, markets):
    changed, deficits = [], []
    for position, market in zip(positions, markets, strict=True):
        if not position.debt_scaled or not market.reserve.configuration.active:
            changed.append(position)
            continue
        amount = market.borrowed(position.debt_scaled)
        deficits.append(AssetAmount(asset=position.target.asset, amount=amount))
        changed.append(
            position.model_copy(update={"debt_scaled": 0, "deficit": position.deficit + amount})
        )
    return tuple(changed), tuple(deficits)


def candidates(positions):
    return tuple(
        (c, d)
        for c, collateral in enumerate(positions)
        for d, debt in enumerate(positions)
        if collateral.collateral_enabled and collateral.supply_scaled and debt.debt_scaled
    )


def liquidate(config, positions, markets, pool_positions, limit):
    validate_sources(config, positions, markets)
    liquidations, deficits, blocked = [], [], []
    for _ in range(limit):
        if risk(positions, markets).risk.health_factor_wad >= 10**18:
            break
        changed = None
        for collateral, debt in candidates(positions):
            try:
                changed, effect, losses = pair(positions, markets, collateral, debt, pool_positions)
                break
            except LiquidationUnavailable as exc:
                blocked.append(str(exc))
        if changed is None:
            break
        replacements = {p.target: p for p in changed}
        pool_positions = tuple(replacements.get(p.target, p) for p in pool_positions)
        positions = changed
        liquidations.append(effect)
        deficits.extend(losses)
        blocked.clear()
    else:
        if risk(positions, markets).risk.health_factor_wad < 10**18:
            raise ValueError("liquidation event exceeds its explicit step limit")
    return Change(
        positions=positions,
        liquidations=tuple(liquidations),
        deficits=tuple(deficits),
        blocked=tuple(dict.fromkeys(blocked)),
    )
