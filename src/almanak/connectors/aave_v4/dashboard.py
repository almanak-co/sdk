"""A bounded display projection over retained Aave checkpoint evidence."""

from almanak.connectors.aave_v4.state import Checkpoint
from almanak.connectors.lending_tables import fund_costs, lending_transactions
from almanak.dashboard.models import (
    Column,
    Context,
    Position,
    Ref,
    Scope,
    Snapshot,
    Value,
)
from almanak.dashboard.projection import snapshot
from almanak.dashboard.repayment import repayment_datasets
from almanak.dashboard.tables import accounts_dataset, several_accounts, symbol_of


def observations(state, target):
    rows = [item for item in state.baseline if item.target == target]
    rows.extend(item.after for item in state.outcomes if item.operation.target == target)
    rows.extend(item for item in state.closure if item.target == target)
    return rows


def project(state: Checkpoint, *, captured_at: int | None = None) -> tuple[Context, Snapshot]:
    if state.integrity_error:
        raise ValueError("invalidated execution evidence cannot produce a financial dashboard")
    if len(state.config.allocations) > 16 or len(state.config.accounts.bindings) > 16:
        raise ValueError("dashboard preview supports at most 16 allocations and funds")
    values: dict[str, Value] = {}
    positions = tuple(position_values(state, item, values) for item in state.config.allocations)
    datasets = {
        "supply_positions": positions_dataset(state, values),
        "fund_costs": fund_costs(state, values, ledger_required=False),
        "transactions": transactions_dataset(state, values),
    }
    if state.config.debts:
        from almanak.dashboard.debt_projection import debt_dataset

        datasets["debt_positions"] = debt_dataset(state, values)
    datasets.update(repayment_datasets(state, values))
    values["position-count"] = Value(
        id="position-count",
        amount=str(sum(owned.shares > 0 for owned in state.owned)),
        unit="count",
        evidence="recorded",
        reason="Positions with retained owned shares.",
    )
    context = Context(
        config=state.config,
        memory=state.memory,
        positions=positions,
        values=values,
        datasets=datasets,
    )
    return context, snapshot(state, values, captured_at)


def position_values(state, allocation, values):
    target = state.config.target(allocation.name)
    scope = Scope(account=allocation.account, fund=target.fund, target=target)
    observed = observations(state, target)
    latest = observed[-1] if observed else None
    block = latest.position.reserve.block if latest else None
    decimals = latest.position.reserve.asset.decimals if latest else None
    outcomes = [item for item in state.outcomes if item.operation.target == target]
    supplied = sum(o.assets for o in outcomes if o.success and o.operation.kind == "supply")
    withdrawn = sum(o.assets for o in outcomes if o.success and o.operation.kind == "withdraw")
    shares = next(item.shares for item in state.owned if item.target == target)
    amounts = {
        "configured": (allocation.capital, "configured", "Configured starting capital."),
        "supplied": (supplied, "recorded", "Sum of confirmed supply effects."),
        "withdrawn": (withdrawn, "recorded", "Sum of confirmed withdrawal effects."),
        "observed": (
            latest.position.supplied_assets if latest else None,
            "recorded",
            "Last retained protocol balance; not a live balance.",
        ),
        "delta": (
            withdrawn - supplied if supplied and shares == 0 and withdrawn else None,
            "recorded",
            "Token withdrawal less supply after closure; excludes gas. "
            "Unavailable until closure and a recorded supply.",
        ),
    }
    for field, (amount, evidence, reason) in amounts.items():
        key = f"{allocation.name}-{field}"
        values[key] = Value(
            id=key,
            amount=str(amount) if amount is not None else None,
            unit="base_units",
            asset=allocation.reserve.asset,
            decimals=decimals,
            symbol=symbol_of(state, allocation.reserve.asset),
            scope=scope,
            evidence="configured" if evidence == "configured" else "recorded",
            block=block,
            reason=reason,
        )
    key = f"{allocation.name}-shares"
    values[key] = Value(
        id=key,
        amount=str(shares),
        unit="shares",
        asset=allocation.reserve.asset,
        scope=scope,
        evidence="recorded",
        block=block,
        reason="Runtime-owned shares; independent of strategy memory.",
    )
    return Position(allocation=allocation.name, target=target, owned_shares=values[key])


def positions_dataset(state, values):
    columns = (
        Column(key="allocation", label="Position"),
        Column(key="account", label="Fund account"),
        Column(key="status", label="Recorded position"),
        Column(key="supplied", label="Supplied", kind="value"),
        Column(key="withdrawn", label="Withdrawn", kind="value"),
        Column(key="observed", label="Last observed supply", kind="value"),
        Column(key="shares", label="Owned shares", kind="value"),
        Column(key="hold", label="Holding rule"),
    )
    rows = tuple(position_row(state, allocation, values) for allocation in state.config.allocations)
    return accounts_dataset(
        keep=several_accounts(state),
        columns=columns,
        rows=rows,
        coverage="Recorded effects and last balances. No continuous balance or yield feed.",
    )


def position_row(state, allocation, values):
    prefix = allocation.name
    shares = values[f"{prefix}-shares"].amount
    status = "Owned shares" if shares != "0" else "No owned shares"
    if not state.initialized:
        status = "Awaiting initial observation"
    hold = "Until teardown" if allocation.hold_seconds is None else f"{allocation.hold_seconds} s"
    started = state.memory.values.get("supplied_at", {})
    if isinstance(started, dict) and prefix in started:
        hold += f"; strategy supply time {started[prefix]}"
    return {
        "allocation": prefix,
        "account": allocation.account,
        "status": status,
        "hold": hold,
        **{
            field: Ref(ref=f"{prefix}-{field}")
            for field in (
                "supplied",
                "withdrawn",
                "observed",
                "shares",
            )
        },
    }


def asset_effect(state, outcome, values):
    """The recorded asset effect of an Aave transaction, not its requested amount."""
    key = "effect-" + outcome.tx_hash.removeprefix("0x")
    values[key] = Value(
        id=key,
        amount=str(outcome.assets),
        unit="base_units",
        asset=outcome.operation.target.asset,
        decimals=outcome.after.position.reserve.asset.decimals,
        symbol=symbol_of(state, outcome.operation.target.asset),
        evidence="recorded",
        scope=Scope(
            account=outcome.operation.account,
            fund=outcome.operation.target.fund,
            target=outcome.operation.target,
        ),
        block=outcome.after.position.reserve.block,
        reason="Recorded asset effect of this transaction, not a requested amount.",
    )
    return Ref(ref=key)


def transactions_dataset(state, values):
    return lending_transactions(
        state,
        values,
        extra=(
            (
                Column(key="assets", label="Asset effect", kind="value"),
                lambda outcome, values: asset_effect(state, outcome, values),
            ),
        ),
    )
