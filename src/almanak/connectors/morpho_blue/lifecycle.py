"""Morpho ownership and closure reuse the shared durable deployment coordinator."""

from uuid import uuid4

from almanak.connectors.closure_obligations import obligation, require_closed
from almanak.connectors.morpho_blue.domain import Market, MarketObservation
from almanak.connectors.morpho_blue.observations import Ownership, owned
from almanak.connectors.morpho_blue.planning import validate_ownership
from almanak.connectors.morpho_blue.risk import borrowed_assets
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
    loan, collateral = positions.get(market.loan_asset), positions.get(market.collateral_asset)
    expected = (
        loan.supply_shares if loan is not None else 0,
        loan.borrow_shares if loan is not None else 0,
        collateral.collateral if collateral is not None else 0,
    )
    actual = (
        account.position.supply_shares,
        account.position.borrow_shares,
        account.position.collateral,
    )
    if actual != expected:
        raise DataError(
            "ownership_mismatch",
            "Morpho market shares or collateral are not owned by this deployment",
        )


def validate_owned(state, observations):
    if tuple(observation.target for observation in observations) != tuple(
        item.target for item in state.owned
    ):
        raise DataError(
            "ownership_mismatch", "observations must cover every configured Morpho allocation"
        )
    markets: dict[Market, MarketObservation] = {}
    for retained, observation in zip(state.owned, observations, strict=True):
        validate_ownership(retained, observation)
        validate_market_ownership(state, observation)
        market = observation.account.market
        if markets.setdefault(market.source, market) != market:
            raise DataError(
                "inconsistent", "Morpho allocations have inconsistent market observations"
            )


def validate_initial(state, observations):
    validate_owned(state, observations)
    if any(not item.empty for item in state.owned) or any(
        observation.allowance for observation in observations
    ):
        raise DataError(
            "existing_exposure", "new deployment cannot adopt Morpho positions or authority"
        )


def adopt_ownership(retained, observations):
    """Ownership of each position as its observation states it, for an operator deployment."""
    if tuple(observation.target for observation in observations) != tuple(
        item.target for item in retained
    ):
        raise DataError(
            "ownership_mismatch", "observations must cover every configured Morpho allocation"
        )
    return tuple(owned(observation) for observation in observations)


def commit_owned(retained, outcome):
    if not outcome.success:
        return retained
    replacement = owned(outcome.after)
    return tuple(replacement if item.target == replacement.target else item for item in retained)


def validate_closure(state, observations):
    validate_owned(state, observations)
    require_closed(
        "Morpho teardown retains positions or allowance",
        (
            obligation(
                observed,
                {
                    "supply_shares": owned.supply_shares,
                    "borrow_shares": owned.borrow_shares,
                    "collateral": owned.collateral,
                    "allowance": observed.allowance,
                },
                {"debt": borrowed_assets(observed.account, observed.block.timestamp)}
                if owned.borrow_shares
                else None,
            )
            for owned, observed in zip(state.owned, observations, strict=True)
        ),
    )


def repayment_coverage(state, observed):
    """Coverage of each borrowing allocation whose observed debt is open."""
    config = state.config
    rows = []
    for observation in observed:
        allocation = config.allocation(observation.allocation)
        debt = borrowed_assets(observation.account, observation.block.timestamp)
        if not allocation.borrow or not debt:
            continue
        rows.append(
            coverage(
                observation.allocation,
                observation.target.asset,
                observation.block,
                debt,
                observation.wallet_balance,
                # The observed IRM borrow rate is per second with 18 decimals.
                observation.account.market.borrow_rate,
            )
        )
    return tuple(rows)


class MorphoLifecycle:
    repayment_coverage = staticmethod(repayment_coverage)
    ownership = staticmethod(protocol_ownership)
    validate_initial = staticmethod(validate_initial)
    validate_owned = staticmethod(validate_owned)
    commit_owned = staticmethod(commit_owned)
    validate_closure = staticmethod(validate_closure)
    adopt = staticmethod(adopt_ownership)

    @staticmethod
    def increases_exposure(intent):
        return isinstance(intent, (SupplyIntent, BorrowIntent))

    @staticmethod
    def initial(config, strategy, capabilities, permissions):
        from almanak.connectors.morpho_blue.state import Checkpoint, Work

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
                    target=config.target(allocation.name),
                    supply_shares=0,
                    borrow_shares=0,
                    collateral=0,
                )
                for allocation in config.positions
            ),
        )
