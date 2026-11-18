"""Debt and collateral use the shared renderer's typed values and evidence scopes."""

from almanak.connectors.aave_v4.reporting import debt_report
from almanak.dashboard.models import Column, Ref, Scope, Value
from almanak.dashboard.tables import accounts_dataset, several_accounts, symbol_of


def debt_dataset(state, values):
    rows = []
    for debt in state.config.debts:
        rows.append(debt_row(state, debt, values))
    return accounts_dataset(
        keep=several_accounts(state),
        columns=(
            Column(key="allocation", label="Position"),
            Column(key="account", label="Fund account"),
            Column(key="borrowed", label="Borrowed", kind="value"),
            Column(key="repaid", label="Repaid", kind="value"),
            Column(key="debt", label="Last observed liability", kind="value"),
            Column(key="cost", label="Realized borrowing cost", kind="value"),
            Column(key="drawn", label="Owned drawn shares", kind="value"),
            Column(key="premium", label="Owned premium shares", kind="value"),
            Column(key="collateral", label="Collateral"),
        ),
        rows=tuple(rows),
        coverage="Retained debt and collateral evidence. Borrowed cash is matched by a liability; "
        "repayment principal is not a loss.",
    )


def debt_row(state, debt, values):
    record = debt_report(state, debt)
    target = state.config.target(debt.name)
    observed = [o for o in state.baseline if o.target == target]
    observed.extend(o.after for o in state.outcomes if o.operation.target == target)
    observed.extend(o for o in state.closure if o.target == target)
    latest = observed[-1] if observed else None
    owned = next(o for o in state.owned if o.target == target)
    cost = record["realized_borrowing_cost"]
    amounts = {
        "borrowed": record["borrowed"]["amount"],
        "repaid": record["repaid"]["amount"],
        "cost": cost["amount"] if cost else None,
        "debt": str(latest.position.total_debt) if latest else None,
        "drawn": str(owned.drawn_shares),
        "premium": str(owned.premium_shares),
    }
    for key, amount in amounts.items():
        shares = key in ("drawn", "premium")
        identifier = f"{debt.name}-{key}"
        values[identifier] = Value(
            id=identifier,
            amount=amount,
            unit="shares" if shares else "base_units",
            asset=target.asset,
            scope=Scope(account=debt.account, fund=target.fund, target=target),
            decimals=None if shares or latest is None else latest.position.reserve.asset.decimals,
            symbol=None if shares else symbol_of(state, target.asset),
            evidence="recorded",
            block=latest.position.reserve.block if latest else None,
            reason="Retained position evidence; cost is unavailable until verified full repayment.",
        )
    collateral = []
    for name in debt.collateral:
        owned_collateral = next(o for o in state.owned if o.target.allocation == name)
        collateral.append(
            f"{name}: {'enabled' if owned_collateral.collateral_enabled else 'disabled'}"
        )
    return {
        "allocation": debt.name,
        "account": debt.account,
        "collateral": "; ".join(collateral),
        **{field: Ref(ref=f"{debt.name}-{field}") for field in amounts},
    }
