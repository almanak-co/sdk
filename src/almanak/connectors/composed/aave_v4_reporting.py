"""Native Aave V4 supply and debt evidence within the composed fund report."""

from almanak.connectors.aave_v4.dashboard import position_values as supply_values
from almanak.connectors.aave_v4.models import DebtAllocation
from almanak.connectors.aave_v4.reporting import debt_report
from almanak.connectors.aave_v4.reporting import position_report as supply_report
from almanak.dashboard.debt_projection import debt_row
from almanak.dashboard.models import Position


def position_report(state, owned):
    allocation = state.config.allocation(owned.target.allocation)
    project = debt_report if isinstance(allocation, DebtAllocation) else supply_report
    return project(state, allocation)


def position_values(state, owned, values):
    allocation = state.config.allocation(owned.target.allocation)
    if not isinstance(allocation, DebtAllocation):
        return supply_values(state, allocation, values)
    debt_row(state, allocation, values)
    return Position(
        allocation=allocation.name,
        target=owned.target,
        owned_shares=values[f"{allocation.name}-drawn"],
    )
