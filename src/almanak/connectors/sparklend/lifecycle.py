"""V3 ownership transitions driven by the existing deployment coordinator."""

from uuid import uuid4

from almanak.connectors.ownership import protocol_ownership
from almanak.connectors.pool_lending_lifecycle import adopt as adopt
from almanak.connectors.pool_lending_lifecycle import commit_owned as commit_owned
from almanak.connectors.pool_lending_lifecycle import validate_closure as validate_closure
from almanak.connectors.pool_lending_lifecycle import validate_initial as validate_initial
from almanak.connectors.pool_lending_lifecycle import validate_owned as validate_owned
from almanak.connectors.repayment import annual_ray_per_second_wad, coverage
from almanak.connectors.sparklend.observations import Ownership
from almanak.lending_intents import BorrowIntent, CollateralIntent, SupplyIntent
from almanak.triggers import DecisionSchedule
from almanak.work import initial_work


def repayment_coverage(state, observed):
    """Coverage of each borrowing allocation whose observed debt is open."""
    config = state.config
    rows = []
    for observation in observed:
        allocation = config.allocation(observation.allocation)
        debt = observation.position.debt_assets
        if not allocation.borrow or not debt:
            continue
        reserve = observation.position.reserve
        rows.append(
            coverage(
                observation.allocation,
                observation.target.asset,
                observation.block,
                debt,
                observation.wallet_balance,
                annual_ray_per_second_wad(reserve.variable_borrow_rate_ray),
            )
        )
    return tuple(rows)


class SparkLifecycle:
    repayment_coverage = staticmethod(repayment_coverage)
    ownership = staticmethod(protocol_ownership)
    validate_initial = staticmethod(validate_initial)
    validate_owned = staticmethod(validate_owned)
    commit_owned = staticmethod(commit_owned)
    validate_closure = staticmethod(validate_closure)
    adopt = staticmethod(adopt)

    @staticmethod
    def increases_exposure(intent):
        return isinstance(intent, (SupplyIntent, BorrowIntent)) or (
            isinstance(intent, CollateralIntent) and intent.enabled
        )

    @staticmethod
    def initial(config, strategy, capabilities, permissions):
        from almanak.connectors.sparklend.state import Checkpoint, Work

        return Checkpoint(
            schedule=DecisionSchedule(),
            work=initial_work(config, Work),
            deployment_id=uuid4().hex,
            config=config,
            strategy=strategy,
            operator=strategy is None,
            capabilities=capabilities,
            permissions=permissions,
            mode="running",
            initialized=False,
            owned=tuple(
                Ownership(
                    target=config.target(a.name),
                    supply_scaled=0,
                    debt_scaled=0,
                    collateral_enabled=False,
                )
                for a in config.positions
            ),
        )
