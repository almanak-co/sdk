"""Exchange hooks receive retained scoped inventory, without external reads."""

from almanak.connectors.concentrated_math import accrued
from almanak.connectors.dex_dashboard import (
    concentrated,
    inventory_dataset,
    payer_values,
    several_accounts,
    symbol_of,
    transactions_dataset,
)
from almanak.dashboard.models import Context, ExchangePosition, Scope, Value
from almanak.dashboard.projection import snapshot


def project(state, *, captured_at=None):
    if state.integrity_error:
        raise ValueError("invalidated execution evidence cannot produce a financial dashboard")
    if len(state.owned) > 16:
        raise ValueError("dashboard supports at most 16 allocations")
    positions = tuple(position(state, owned) for owned in state.owned)
    values = {v.id: v for p in positions for v in p.inventory}
    datasets = {
        "exchange_inventory": inventory_dataset(positions, keep=several_accounts(state)),
        "payer_costs": payer_values(state, values),
        "transactions": transactions_dataset(state, values),
    }
    context = Context(
        config=state.config,
        memory=state.memory,
        positions=positions,
        values=values,
        datasets=datasets,
    )
    return context, snapshot(state, values, captured_at)


def position(state, owned):
    target = owned.target
    observed = [o for o in state.baseline if o.target == target]
    observed.extend(o.after for o in state.outcomes if o.operation.target == target)
    observed.extend(o for o in state.closure if o.target == target)
    latest = observed[-1] if observed else None
    scope = Scope(account=target.account, fund=target.fund, target=target)
    values = tuple(
        Value(
            id=f"{target.allocation}-inventory-{index}",
            amount=str(amount.amount) if latest else None,
            unit="base_units",
            asset=amount.asset,
            decimals=state.config.asset_metadata[amount.asset].decimals
            if amount.asset in state.config.asset_metadata
            else None,
            symbol=symbol_of(state, amount.asset),
            scope=scope,
            block=latest.block if latest else None,
            evidence="recorded",
            reason="Retained strategy-owned inventory; excludes LP principal and fees."
            if latest
            else "No retained observation.",
        )
        for index, amount in enumerate((owned.inventory0, owned.inventory1))
    )
    return ExchangePosition(allocation=target.allocation, target=target, inventory=values)


def readings(observation, start=None):
    """The concentrated-liquidity readings (`dex_dashboard.readings`) of a V4 observation, with
    the position's uncollected `fees0`/`fees1` at this block counted in `value`. Fees are absent
    when the observation could not read fee growth."""
    position = observation.owned.position
    if position is None:
        return concentrated(observation, start=start)
    shape = position.range or observation.target.range
    liquidity = observation.position_liquidity or position.liquidity
    fees = None
    if observation.fee_growth is not None and observation.position_growth is not None:
        fees = tuple(
            accrued(liquidity, old, new)
            for old, new in zip(observation.position_growth, observation.fee_growth, strict=True)
        )
    return concentrated(observation, liquidity, shape, start, fees)


def history_row(state, observed):
    from almanak.connectors.dex_dashboard import history_row as family

    return family(state, observed, readings)
