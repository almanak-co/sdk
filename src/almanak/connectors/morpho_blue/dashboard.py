"""Read-only Morpho principal and asset quantities from retained snapshots."""

from almanak.connectors.lending_tables import fund_costs, lending_transactions
from almanak.connectors.morpho_blue.models import Parameters
from almanak.connectors.morpho_blue.reporting import observations
from almanak.connectors.morpho_blue.risk import borrowed_assets, supplied_assets
from almanak.dashboard.models import Column, Context, Position, Ref, Scope, Value
from almanak.dashboard.projection import snapshot
from almanak.dashboard.repayment import repayment_datasets
from almanak.dashboard.tables import accounts_dataset, several_accounts, symbol_of


def position_values(state, retained, values):
    target = retained.target
    history = observations(state, target)
    latest = history[-1] if history else None
    supply, debt = asset_quantities(latest)
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
            reason="Asset conversion from retained Morpho accounting at its observed block."
            if latest
            else "No retained Morpho observation.",
        )
    loan = target.reserve.is_loan
    for name, amount in (
        ("ownership", retained.supply_shares if loan else retained.collateral),
        ("debt-shares", retained.borrow_shares if loan else None),
    ):
        key = f"{target.allocation}-{name}"
        is_shares = loan or name == "debt-shares"
        values[key] = Value(
            id=key,
            amount=str(amount) if amount is not None else None,
            unit="shares" if is_shares else "base_units",
            asset=target.asset,
            decimals=None if is_shares or latest is None else latest.asset.decimals,
            symbol=None if is_shares else symbol_of(state, target.asset),
            scope=scope,
            evidence="recorded",
            block=latest.block if latest else None,
            reason="Runtime-owned Morpho supply and borrow shares have separate conversion totals."
            if loan
            else "Non-interest-bearing collateral has no loan share balance.",
        )
    return Position(
        allocation=target.allocation,
        target=target,
        owned_shares=values[f"{target.allocation}-ownership"],
    )


def asset_quantities(latest):
    if latest is None:
        return None, None
    if latest.target.reserve.is_loan:
        return supplied_assets(latest.account), borrowed_assets(latest.account)
    return latest.account.position.collateral, 0


def project(state, *, captured_at=None):
    if state.integrity_error:
        raise ValueError("invalidated Morpho evidence cannot produce a financial dashboard")
    if len(state.owned) > 16 or len(state.config.accounts.bindings) > 16:
        raise ValueError("Morpho dashboard supports at most 16 allocations and funds")
    values: dict[str, Value] = {}
    positions = tuple(position_values(state, retained, values) for retained in state.owned)
    dataset = accounts_dataset(
        keep=several_accounts(state),
        columns=(
            Column(key="allocation", label="Position"),
            Column(key="account", label="Fund account"),
            Column(key="supply", label="Supplied assets", kind="value"),
            Column(key="debt", label="Loan debt", kind="value"),
            Column(key="ownership", label="Supply shares / collateral units", kind="value"),
            Column(key="debt_shares", label="Borrow shares", kind="value"),
        ),
        rows=tuple(
            {
                "allocation": p.target.allocation,
                "account": p.target.account,
                "supply": Ref(ref=f"{p.target.allocation}-supply"),
                "debt": Ref(ref=f"{p.target.allocation}-debt"),
                "ownership": Ref(ref=f"{p.target.allocation}-ownership"),
                "debt_shares": Ref(ref=f"{p.target.allocation}-debt-shares"),
            }
            for p in state.owned
        ),
        coverage="Last retained transaction observations. Supply shares, borrow shares and "
        "collateral units are distinct; fiat PnL is unavailable.",
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
