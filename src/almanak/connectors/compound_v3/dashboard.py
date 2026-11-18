"""Read-only Comet principal and asset quantities from retained snapshots."""

from almanak.connectors.compound_v3.models import Parameters
from almanak.connectors.compound_v3.observations import owned as observed_ownership
from almanak.connectors.compound_v3.reporting import observations
from almanak.connectors.lending_tables import fund_costs, lending_transactions
from almanak.dashboard.models import Column, Context, Position, Ref, Scope, Value
from almanak.dashboard.projection import snapshot
from almanak.dashboard.repayment import repayment_datasets
from almanak.dashboard.tables import accounts_dataset, several_accounts, symbol_of


def position_values(state, retained, values):
    target = retained.target
    history = observations(state, target)
    latest = history[-1] if history else None
    base = target.reserve.is_base
    supply = (
        None
        if latest is None
        else latest.account.supplied_base
        if base
        else observed_ownership(latest).collateral
    )
    debt = None if latest is None else latest.account.borrowed_base if base else 0
    scope = Scope(account=target.account, fund=target.fund, target=target)
    for name, amount in (("supply", supply), ("debt", debt)):
        key = f"{target.allocation}-{name}"
        values[key] = Value(
            id=key,
            amount=str(amount) if amount is not None else None,
            unit="base_units",
            asset=target.asset,
            decimals=latest.asset.decimals if latest else None,
            symbol=symbol_of(state, target.asset),
            scope=scope,
            evidence="recorded",
            block=latest.block if latest else None,
            reason="Last retained Comet asset balance."
            if latest
            else "No retained Comet observation.",
        )
    key = f"{target.allocation}-ownership"
    values[key] = Value(
        id=key,
        amount=str(retained.principal if base else retained.collateral),
        unit="shares" if base else "base_units",
        asset=target.asset,
        decimals=None if base or latest is None else latest.asset.decimals,
        symbol=None if base else symbol_of(state, target.asset),
        scope=scope,
        evidence="recorded",
        block=latest.block if latest else None,
        reason="Signed Comet principal; convert with the supply or borrow index scaled by 10^15."
        if base
        else "Runtime-owned non-interest-bearing collateral base units.",
    )
    return Position(allocation=target.allocation, target=target, owned_shares=values[key])


def project(state, *, captured_at=None):
    if state.integrity_error:
        raise ValueError("invalidated Comet evidence cannot produce a financial dashboard")
    if len(state.owned) > 16 or len(state.config.accounts.bindings) > 16:
        raise ValueError("Comet dashboard supports at most 16 allocations and funds")
    values: dict[str, Value] = {}
    positions = tuple(position_values(state, retained, values) for retained in state.owned)
    dataset = accounts_dataset(
        keep=several_accounts(state),
        columns=(
            Column(key="allocation", label="Position"),
            Column(key="account", label="Fund account"),
            Column(key="supply", label="Supplied assets", kind="value"),
            Column(key="debt", label="Base debt", kind="value"),
            Column(key="ownership", label="Principal / collateral units", kind="value"),
        ),
        rows=tuple(
            {
                "allocation": p.target.allocation,
                "account": p.target.account,
                "supply": Ref(ref=f"{p.target.allocation}-supply"),
                "debt": Ref(ref=f"{p.target.allocation}-debt"),
                "ownership": Ref(ref=f"{p.target.allocation}-ownership"),
            }
            for p in state.owned
        ),
        coverage="Last retained transaction observations. Base principal and collateral units "
        "are distinct; fiat PnL is unavailable.",
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
