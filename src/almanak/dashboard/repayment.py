"""Repayment coverage rows shared by the lending dashboards.

Values come from the coverage the runtime retains at each decision while debt is
open. The projection is a model output and its reason says so; it is never
presented as a measurement.
"""

from almanak.dashboard.models import Column, Dataset, Ref, Scope, Value
from almanak.dashboard.tables import symbol_of

COLUMNS = (
    Column(key="allocation", label="Debt allocation"),
    Column(key="debt", label="Debt", kind="value"),
    Column(key="available", label="Available to repay", kind="value"),
    Column(key="shortfall", label="Repayment shortfall", kind="value"),
    Column(key="projection", label="Projected time to shortfall", kind="value"),
)
REASONS = {
    "debt": "Debt observed at the latest decision.",
    "available": "Loan asset available for full repayment: the fund's wallet balance.",
    "shortfall": "Debt minus the available amount at the same block; zero when covered. "
    "Sending the shortfall to the fund lets the repayment proceed.",
}


def projection_value(key, row, scope):
    projection = row.projection
    if projection is None:
        reason = "No projection: the borrow rate is not observed for this market."
        amount = None
    elif projection.seconds_until_shortfall is None:
        reason = "Model output: the debt does not grow at the observed borrow rate."
        amount = None
    else:
        reason = (
            "Model output, not a measurement: the observed borrow rate of "
            f"{projection.borrow_rate_per_second_wad} wad per second held constant."
        )
        amount = str(projection.seconds_until_shortfall)
    return Value(
        id=key,
        amount=amount,
        unit="seconds",
        evidence="recorded",
        block=row.block,
        reason=reason,
        inputs=(f"{row.allocation}-repayment-debt", f"{row.allocation}-repayment-available"),
    )


def coverage_values(row, scope, values, symbol=None):
    for name in ("debt", "available", "shortfall"):
        amount = getattr(row, name)
        key = f"{row.allocation}-repayment-{name}"
        values[key] = Value(
            id=key,
            amount=None if amount is None else str(amount),
            unit="base_units",
            asset=row.asset,
            symbol=symbol,
            scope=scope,
            evidence="recorded",
            block=row.block,
            reason=REASONS[name] if amount is not None else "Not observed at the latest decision.",
        )
    key = f"{row.allocation}-repayment-projection"
    values[key] = projection_value(key, row, scope)


def repayment_datasets(state, values) -> dict[str, Dataset]:
    """A `repayment_coverage` dataset while any debt is open; nothing otherwise."""
    if not state.repayment:
        return {}
    targets = {owned.target.allocation: owned.target for owned in state.owned}
    rows = []
    for row in state.repayment:
        target = targets[row.allocation]
        scope = Scope(account=target.account, fund=target.fund, target=target)
        coverage_values(row, scope, values, symbol_of(state, row.asset))
        rows.append(
            {
                "allocation": row.allocation,
                **{
                    name: Ref(ref=f"{row.allocation}-repayment-{name}")
                    for name in ("debt", "available", "shortfall", "projection")
                },
            }
        )
    return {
        "repayment_coverage": Dataset(
            columns=COLUMNS,
            rows=tuple(rows),
            coverage="Latest decision observation while debt is open. A shortfall is "
            "expected while borrowed proceeds are deployed; it is the amount the exit must "
            "recover, not an error.",
        )
    }
