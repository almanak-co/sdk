"""Read-only scoped values for the shared strategy dashboard renderer."""

from typing import Literal

from almanak.connectors.lending_tables import fund_costs, lending_transactions
from almanak.connectors.sparklend.models import Parameters
from almanak.connectors.sparklend.reporting import observations
from almanak.dashboard.models import Column, Context, Position, Ref, Scope, Value
from almanak.dashboard.projection import snapshot
from almanak.dashboard.repayment import repayment_datasets
from almanak.dashboard.tables import accounts_dataset, several_accounts, symbol_of


def position_values(state, owned, values):
    target = owned.target
    history = observations(state, target)
    latest = history[-1] if history else None
    block = latest.block if latest else None
    decimals = latest.position.reserve.asset.decimals if latest else None
    scope = Scope(account=target.account, fund=target.fund, target=target)
    metrics: dict[str, tuple[int | None, Literal["base_units", "shares"], str]] = {
        "supply": (
            latest.position.supplied_assets if latest else None,
            "base_units",
            "Last observed supplied assets.",
        ),
        "debt": (
            latest.position.debt_assets if latest else None,
            "base_units",
            "Last observed variable debt.",
        ),
        "supply-shares": (owned.supply_scaled, "shares", "Runtime-owned aToken scaled units."),
        "debt-shares": (owned.debt_scaled, "shares", "Runtime-owned variable-debt scaled units."),
    }
    for name, (amount, unit, reason) in metrics.items():
        key = f"{target.allocation}-{name}"
        values[key] = Value(
            id=key,
            amount=str(amount) if amount is not None else None,
            unit=unit,
            asset=target.asset,
            decimals=decimals if unit == "base_units" else None,
            symbol=symbol_of(state, target.asset) if unit == "base_units" else None,
            scope=scope,
            evidence="recorded",
            block=block,
            reason=reason if amount is not None else "No retained protocol observation.",
        )
    return Position(
        allocation=target.allocation,
        target=target,
        owned_shares=values[f"{target.allocation}-supply-shares"],
    )


def project(state, *, captured_at=None):
    if state.integrity_error:
        raise ValueError("invalidated evidence cannot produce a financial dashboard")
    if len(state.owned) > 16 or len(state.config.accounts.bindings) > 16:
        raise ValueError("dashboard preview supports at most 16 reserves and funds")
    values: dict[str, Value] = {}
    positions = tuple(position_values(state, owned, values) for owned in state.owned)
    rows = tuple(
        {
            "allocation": o.target.allocation,
            "account": o.target.account,
            "supply": Ref(ref=f"{o.target.allocation}-supply"),
            "debt": Ref(ref=f"{o.target.allocation}-debt"),
            "collateral": "enabled" if o.collateral_enabled else "disabled",
        }
        for o in state.owned
    )
    dataset = accounts_dataset(
        keep=several_accounts(state),
        columns=(
            Column(key="allocation", label="Position"),
            Column(key="account", label="Fund account"),
            Column(key="supply", label="Supplied assets", kind="value"),
            Column(key="debt", label="Variable debt", kind="value"),
            Column(key="collateral", label="Collateral"),
        ),
        rows=rows,
        coverage="Last retained transaction observations. Token balances and debt "
        "are separate; fiat PnL is unavailable.",
    )
    context = Context(
        config=Parameters.model_validate(
            {name: getattr(state.config, name) for name in Parameters.model_fields}
        ),
        memory=state.memory,
        positions=positions,
        values=values,
        datasets={
            "lending_positions": dataset,
            "fund_costs": fund_costs(state, values),
            "transactions": lending_transactions(state, values),
            **repayment_datasets(state, values),
        },
    )
    return context, snapshot(state, values, captured_at)
