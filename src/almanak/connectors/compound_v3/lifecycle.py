"""Complete Comet ownership and cleanup through the shared deployment coordinator."""

from uuid import uuid4

from almanak.connectors.closure_obligations import obligation, require_closed
from almanak.connectors.compound_v3.accounting import rates
from almanak.connectors.compound_v3.domain import Market, MarketObservation
from almanak.connectors.compound_v3.observations import Ownership, owned
from almanak.connectors.compound_v3.planning import validate_ownership
from almanak.connectors.ownership import protocol_ownership
from almanak.connectors.repayment import coverage
from almanak.gateway.ethereum import DataError
from almanak.lending_intents import BorrowIntent, SupplyIntent
from almanak.triggers import DecisionSchedule
from almanak.work import initial_work


def validate_market_ownership(state, observation):
    account = observation.account
    market = account.market.source
    positions = {
        item.target.asset: item
        for item in state.owned
        if item.target.fund == account.fund and item.target.reserve.market == market
    }
    base = positions.get(market.base_asset)
    if account.principal != (base.principal if base is not None else 0):
        raise DataError(
            "ownership_mismatch", "Comet base principal is not owned by this deployment"
        )
    for collateral in account.collaterals:
        position = positions.get(collateral.asset)
        if collateral.balance != (position.collateral if position is not None else 0):
            raise DataError(
                "ownership_mismatch", "Comet collateral is not owned by this deployment"
            )


def validate_owned(state, observations):
    if tuple(o.target for o in observations) != tuple(o.target for o in state.owned):
        raise DataError(
            "ownership_mismatch", "observations must cover every configured Comet position"
        )
    markets: dict[Market, MarketObservation] = {}
    for retained, observation in zip(state.owned, observations, strict=True):
        validate_ownership(retained, observation)
        validate_market_ownership(state, observation)
        market = observation.account.market
        if markets.setdefault(market.source, market) != market:
            raise DataError(
                "inconsistent", "Comet allocations have inconsistent market observations"
            )


def validate_initial(state, observations):
    validate_owned(state, observations)
    if any(not item.empty for item in state.owned) or any(o.allowance for o in observations):
        raise DataError(
            "existing_exposure", "new deployment cannot adopt Comet positions or authority"
        )


def commit_owned(retained, outcome):
    if not outcome.success:
        return retained
    replacement = owned(outcome.after)
    return tuple(replacement if item.target == replacement.target else item for item in retained)


def validate_closure(state, observations):
    validate_owned(state, observations)
    require_closed(
        "Comet teardown retains positions or allowance",
        (
            obligation(
                observed,
                {
                    "principal": owned.principal,
                    "collateral": owned.collateral,
                    "allowance": observed.allowance,
                },
                {
                    "supply": observed.account.supplied_base,
                    "debt": observed.account.borrowed_base,
                }
                if observed.target.reserve.is_base
                else None,
            )
            for owned, observed in zip(state.owned, observations, strict=True)
        ),
    )


def repayment_coverage(state, observed):
    """Coverage of each borrowing allocation whose observed base debt is open."""
    config = state.config
    rows = []
    for observation in observed:
        allocation = config.allocation(observation.allocation)
        debt = observation.account.borrowed_base
        if not allocation.borrow or not debt:
            continue
        rows.append(
            coverage(
                observation.allocation,
                observation.target.asset,
                observation.block,
                debt,
                observation.wallet_balance,
                # Comet rates are per second with 18 decimals.
                rates(observation.account.market)[1],
            )
        )
    return tuple(rows)


class CompoundLifecycle:
    repayment_coverage = staticmethod(repayment_coverage)
    ownership = staticmethod(protocol_ownership)
    validate_initial = staticmethod(validate_initial)
    validate_owned = staticmethod(validate_owned)
    commit_owned = staticmethod(commit_owned)
    validate_closure = staticmethod(validate_closure)

    @staticmethod
    def increases_exposure(intent):
        return isinstance(intent, (SupplyIntent, BorrowIntent))

    @staticmethod
    def initial(config, strategy, capabilities, permissions):
        from almanak.connectors.compound_v3.state import Checkpoint, Work

        return Checkpoint(
            schedule=DecisionSchedule(),
            work=initial_work(config, Work),
            deployment_id=uuid4().hex,
            config=config,
            strategy=strategy,
            capabilities=capabilities,
            permissions=permissions,
            mode="running",
            initialized=False,
            owned=tuple(
                Ownership(target=config.target(a.name), principal=0, collateral=0)
                for a in config.positions
            ),
        )
