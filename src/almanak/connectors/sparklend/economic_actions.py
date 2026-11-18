"""Economic SparkLend transitions change shared inventory only by settled asset effects."""

from almanak.connectors.lending_health import health_floor
from almanak.connectors.sparklend.authority import admit, declared
from almanak.connectors.sparklend.economic_math import require_group, risk, validate_sources
from almanak.connectors.sparklend.economic_state import Position
from almanak.connectors.sparklend.math import (
    borrow_shares,
    debt_assets,
    normalized_indices,
    percent_div,
    repay_shares,
    supplied_assets,
    supply_shares,
    treasury_shares,
    uint,
    withdraw_shares,
)
from almanak.connectors.sparklend.planning import reserve_available
from almanak.lending_intents import (
    BorrowIntent,
    CollateralIntent,
    RepayAmountIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawAmountIntent,
    WithdrawIntent,
)
from almanak.models import Model, UInt


class Change(Model):
    positions: tuple[Position, ...]
    inventory_delta: int
    assets: UInt
    supply_delta: int
    debt_delta: int


def apply(config, intent, positions, markets, inventory, pool_positions):
    require_group(positions, markets)
    admit(declared(config), intent)
    index = next((i for i, p in enumerate(positions) if p.target == intent.target), None)
    if index is None or inventory < 0:
        raise ValueError("economic action requires its configured position and inventory")
    before, market = positions[index], markets[index]
    allocation = config.allocation(intent.target.allocation)
    after, delta = position_action(intent, before, market, allocation)
    if inventory + delta < 0:
        raise ValueError("economic action cannot spend missing fund inventory")
    check_capacity(intent, before, market, pool_positions, abs(delta))
    changed = (*positions[:index], after, *positions[index + 1 :])
    check_risk(config, intent, before, changed, markets)
    return Change(
        positions=changed,
        inventory_delta=delta,
        assets=abs(delta),
        supply_delta=after.supply_scaled - before.supply_scaled,
        debt_delta=after.debt_scaled - before.debt_scaled,
    )


def position_action(intent, before, market, allocation):
    if isinstance(intent, SupplyIntent):
        return supply(before, market, allocation, intent.amount.amount)
    if isinstance(intent, BorrowIntent):
        return borrow(before, market, allocation, intent.amount.amount)
    if isinstance(intent, (WithdrawIntent, WithdrawAmountIntent)):
        return withdraw(
            before, market, None if isinstance(intent, WithdrawIntent) else intent.amount.amount
        )
    if isinstance(intent, (RepayIntent, RepayAmountIntent)):
        return repay(
            before, market, None if isinstance(intent, RepayIntent) else intent.amount.amount
        )
    if isinstance(intent, CollateralIntent):
        reserve_available(market.reserve)
        if not before.supply_scaled or before.collateral_enabled == intent.enabled:
            raise ValueError("collateral control requires a nonempty changing position")
        if intent.enabled and (
            not allocation.collateral or not market.reserve.configuration.ltv_bps
        ):
            raise ValueError("economic collateral enablement is unavailable or unauthorized")
        return before.model_copy(update={"collateral_enabled": intent.enabled}), 0
    raise ValueError("unsupported economic SparkLend action")


def supply(before, market, allocation, amount):
    reserve_available(market.reserve, entry=True)
    scaled = supply_shares(amount, market.supply_index)
    if not scaled:
        raise ValueError("supply requires nonzero shares")
    collateral = before.collateral_enabled or bool(
        not before.supply_scaled and market.reserve.configuration.first_supply_enables_collateral
    )
    if collateral and not allocation.collateral:
        raise ValueError("automatic collateral enablement requires explicit authority")
    return before.model_copy(
        update={
            "supply_scaled": uint(before.supply_scaled + scaled, 128),
            "supplied": before.supplied + amount,
            "collateral_enabled": collateral,
        }
    ), -amount


def borrow(before, market, allocation, amount):
    reserve_available(market.reserve, entry=True)
    if not allocation.borrow or not market.reserve.configuration.borrowing:
        raise ValueError("borrowing is unavailable or outside the allocation's authority")
    scaled = borrow_shares(amount, market.debt_index)
    if not scaled:
        raise ValueError("borrowing would mint zero variable-debt shares")
    return before.model_copy(
        update={
            "debt_scaled": uint(before.debt_scaled + scaled, 128),
            "borrowed": before.borrowed + amount,
        }
    ), amount


def withdraw(before, market, amount):
    reserve_available(market.reserve)
    assets = market.supplied(before.supply_scaled) if amount is None else amount
    scaled = (
        before.supply_scaled if amount is None else withdraw_shares(assets, market.supply_index)
    )
    if not scaled or scaled > before.supply_scaled:
        raise ValueError("withdrawal exceeds the owned reserve shares")
    return before.model_copy(
        update={
            "supply_scaled": before.supply_scaled - scaled,
            "withdrawn": before.withdrawn + assets,
            "collateral_enabled": before.collateral_enabled and scaled != before.supply_scaled,
        }
    ), assets


def repay(before, market, amount):
    reserve_available(market.reserve)
    debt = market.borrowed(before.debt_scaled)
    if not debt or (amount is not None and amount > debt):
        raise ValueError("repayment requires owned debt and at most that debt")
    assets = debt if amount is None else amount
    scaled = repay_shares(assets, market.debt_index)
    if not scaled:
        raise ValueError("repayment would burn zero debt shares")
    return before.model_copy(
        update={
            "debt_scaled": before.debt_scaled - scaled,
            "repaid": before.repaid + assets,
        }
    ), -assets


def reserve_inventory(positions, source):
    return sum(
        p.supplied
        + p.repaid
        + p.liquidated_debt
        - p.withdrawn
        - p.borrowed
        - p.liquidated_collateral
        + p.liquidation_fees
        for p in positions
        if p.target.reserve == source
    )


def check_capacity(intent, before, market, positions, amount):
    reserve = market.reserve
    affected = tuple(p for p in positions if p.target.reserve == reserve.source)
    if before not in affected:
        raise ValueError("reserve capacity must include the action's retained position")
    config = reserve.configuration
    supply_index, debt_index = normalized_indices(reserve, market.time)
    if isinstance(intent, SupplyIntent) and config.supply_cap:
        external = supplied_assets(
            reserve.total_scaled_supply + treasury_shares(reserve, market.time), supply_index
        )
        owned = market.supplied(sum(p.supply_scaled for p in affected))
        if uint(external + owned + amount) > config.supply_cap:
            raise ValueError("economic supply exceeds the reserve cap")
    if isinstance(intent, BorrowIntent) and config.borrow_cap:
        external = debt_assets(reserve.total_scaled_debt, debt_index) + reserve.total_stable_debt
        owned = market.borrowed(sum(p.debt_scaled for p in affected))
        if uint(external + owned + amount) > config.borrow_cap:
            raise ValueError("economic borrowing exceeds the reserve cap")
    if isinstance(intent, (BorrowIntent, WithdrawIntent, WithdrawAmountIntent)):
        available = reserve.available_underlying
        if amount > available + reserve_inventory(positions, reserve.source):
            raise ValueError("economic operation exceeds the available reserve inventory")


def check_risk(config, intent, before, positions, markets):
    release = isinstance(intent, (WithdrawIntent, WithdrawAmountIntent)) or (
        isinstance(intent, CollateralIntent) and not intent.enabled
    )
    needs = (
        isinstance(intent, (SupplyIntent, BorrowIntent))
        or (isinstance(intent, CollateralIntent) and intent.enabled)
        or (release and before.collateral_enabled and any(p.debt_scaled for p in positions))
    )
    if not needs:
        return
    validate_sources(config, positions, markets)
    projected = risk(positions, markets)
    if projected.risk.health_factor_wad < health_floor(
        config.allocation(intent.target.allocation).minimum_health_factor
    ):
        raise ValueError("economic Pool health is below the configured minimum")
    if isinstance(intent, BorrowIntent):
        validate_borrow_ltv(intent, before, positions, markets)
    own = next(m for m in markets if m.reserve.source == before.target.reserve)
    if release and projected.has_zero_ltv_collateral and own.reserve.configuration.ltv_bps:
        raise ValueError("zero-LTV collateral must be released first")


def validate_borrow_ltv(intent, before, positions, markets):
    prior = tuple(before if p.target == before.target else p for p in positions)
    current = risk(prior, markets).risk
    market = next(m for m in markets if m.reserve.source == before.target.reserve)
    price = market.reserve.oracle
    if price is None:
        raise ValueError("economic borrowing requires its configured oracle quote")
    amount_base = uint(price.price * intent.amount.amount) // 10**market.reserve.asset.decimals
    if (
        not current.ltv_bps
        or current.health_factor_wad <= 10**18
        or current.total_collateral_base
        < percent_div(uint(current.total_debt_base + amount_base), current.ltv_bps)
    ):
        raise ValueError("economic collateral cannot cover the requested borrowing at current LTV")
