"""Report fungible pool ownership and separately calculated fee claims."""

from almanak.connectors.aerodrome.ownership import at_indices
from almanak.connectors.dex_reporting import observations, quantity
from almanak.connectors.dex_reporting import position_report as exchange_position
from almanak.connectors.dex_reporting import report as exchange_report


def report(state):
    result = exchange_report(state)
    result["positions"] = [position_report(state, owned) for owned in state.owned]
    return result


def position_report(state, owned):
    item = exchange_position(state, owned)
    history = observations(state, owned.target)
    latest = history[-1] if history else None
    position = at_indices(owned.position, latest.pool) if latest else None
    item["uncollected_fees"] = [
        quantity(
            asset,
            getattr(position, f"fees{index}").amount if position else (0 if latest else None),
        )
        for index, asset in enumerate(owned.target.assets)
    ]
    item["fee_measurement"] = (
        "Calculated at retained pool indices; stored fund checkpoints remain unchanged."
    )
    return item
