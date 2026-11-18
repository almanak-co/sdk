"""Project owned shares and Pool risk without substituting financial observations."""

from almanak.connectors.lending_health import health_floor
from almanak.connectors.sparklend.math import (
    borrow_shares,
    debt_assets,
    percent_div,
    repay_shares,
    supplied_assets,
    supply_shares,
    transition_indices,
    uint,
    withdraw_shares,
)
from almanak.connectors.sparklend.operations import (
    BorrowOperation,
    CollateralOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.sparklend.risk import RiskInputs, project
from almanak.gateway.ethereum import DataError

BORROW_FLAGS = int("55" * 32, 16)


def share_transition(position, operation):
    supply_index, debt_index = transition_indices(
        position.reserve, position.reserve.block.timestamp
    )
    supply, debt = position.supplied_scaled, position.debt_scaled
    collateral = position.collateral_enabled
    if isinstance(operation, SupplyOperation):
        supply += supply_shares(operation.amount.amount, supply_index)
        if (
            not position.supplied_scaled
            and position.reserve.configuration.first_supply_enables_collateral
        ):
            collateral = True
    elif isinstance(operation, BorrowOperation):
        debt += borrow_shares(operation.amount.amount, debt_index)
    elif isinstance(operation, WithdrawOperation):
        supply = (
            0 if operation.full else supply - withdraw_shares(operation.amount.amount, supply_index)
        )
        if not supply:
            collateral = False
    elif isinstance(operation, RepayOperation):
        paid = min(operation.amount.amount, debt_assets(debt, debt_index))
        debt -= repay_shares(paid, debt_index)
    elif isinstance(operation, CollateralOperation):
        collateral = operation.enabled
    else:
        raise ValueError("risk projection requires an economic Pool operation")
    if min(supply, debt) < 0:
        raise DataError("ownership_mismatch", "projected operation exceeds owned shares")
    return supply, debt, collateral, supply_index, debt_index


def after_position(position, operation):
    supply, debt, collateral, supply_index, debt_index = share_transition(position, operation)
    reserve = position.reserve.model_copy(
        update={
            "liquidity_index_ray": supply_index,
            "variable_borrow_index_ray": debt_index,
            "last_update_timestamp": position.reserve.block.timestamp,
        }
    )
    return position.model_copy(
        update={
            "reserve": reserve,
            "supplied_scaled": supply,
            "debt_scaled": debt,
            "supplied_assets": supplied_assets(supply, supply_index),
            "debt_assets": debt_assets(debt, debt_index),
            "collateral_enabled": collateral,
        }
    )


def projected_inputs(before, operation):
    inputs = before.pool_risk
    if inputs is None:
        raise DataError("risk_unavailable", "complete Pool risk inputs are unavailable")
    after = after_position(before.position, operation)
    shift = 2 * after.reserve.reserve_id
    bitmap = (inputs.user_configuration & ~(3 << shift)) | (
        (int(bool(after.debt_scaled)) + 2 * int(after.collateral_enabled)) << shift
    )
    positions = tuple(
        (after if position == before.position else position).model_copy(
            update={"user_configuration": bitmap}
        )
        for position in inputs.positions
    )
    return RiskInputs(
        positions=positions, user_configuration=bitmap, emode_category=inputs.emode_category
    )


def validate_sources(config, inputs):
    for position in inputs.positions:
        if not (position.collateral_enabled or position.debt_scaled):
            continue
        allocation = next(
            a
            for a in config.positions
            if (config.target(a.name).fund, a.reserve) == (position.fund, position.reserve.source)
        )
        price = position.reserve.oracle
        if price is None or (price.oracle, price.feed) != (
            allocation.oracle,
            allocation.price_source,
        ):
            raise DataError(
                "oracle_changed", "Pool price evidence differs from its configured source"
            )


def requires_risk(before, operation):
    if isinstance(operation, (SupplyOperation, BorrowOperation)):
        return True
    if isinstance(operation, CollateralOperation) and operation.enabled:
        return True
    return bool(before.position.user_configuration & BORROW_FLAGS) and (
        isinstance(operation, CollateralOperation)
        or isinstance(operation, WithdrawOperation)
        and before.position.collateral_enabled
    )


def validate_projection(config, before, operation):
    if not requires_risk(before, operation):
        return None
    inputs = projected_inputs(before, operation)
    validate_sources(config, inputs)
    projection = project(inputs)
    allocation = config.allocation(operation.allocation)
    if projection.risk.health_factor_wad < health_floor(allocation.minimum_health_factor):
        raise DataError("health_factor", "projected Pool health is below the configured minimum")
    if isinstance(operation, BorrowOperation):
        validate_borrow_ltv(before, operation)
    release = isinstance(operation, WithdrawOperation) or (
        isinstance(operation, CollateralOperation) and not operation.enabled
    )
    if release and projection.has_zero_ltv_collateral:
        if before.position.reserve.configuration.ltv_bps:
            raise DataError("zero_ltv_collateral", "zero-LTV collateral must be released first")
    return projection


def validate_borrow_ltv(before, operation):
    risk = project(before.pool_risk).risk
    price = before.position.reserve.oracle
    if price is None:
        raise DataError("risk_unavailable", "borrowing requires the configured oracle quote")
    amount_base = uint(price.price * operation.amount.amount) // (
        10**before.position.reserve.asset.decimals
    )
    if (
        not risk.ltv_bps
        or risk.health_factor_wad <= 10**18
        or risk.total_collateral_base
        < percent_div(uint(risk.total_debt_base + amount_base), risk.ltv_bps)
    ):
        raise DataError("ltv", "projected collateral cannot cover the borrowed debt at current LTV")
