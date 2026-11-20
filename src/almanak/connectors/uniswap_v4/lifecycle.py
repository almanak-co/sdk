"""Exchange ownership and verified teardown for shared runtime orchestration."""

from uuid import uuid4

from almanak.connectors.ownership import protocol_ownership
from almanak.connectors.uniswap_v4.observation_models import counter_settled
from almanak.connectors.uniswap_v4.observations import initial_owned
from almanak.connectors.uniswap_v4.state import (
    Checkpoint,
    LiquidityPosition,
    Ownership,
    Work,
)
from almanak.connectors.uniswap_v4.validation import validate_owned
from almanak.dex_intents import OpenLiquidity, SwapExactInput
from almanak.gateway.ethereum import DataError
from almanak.work import initial_work


def increases_exposure(intent):
    return isinstance(intent, OpenLiquidity) or (
        isinstance(intent, SwapExactInput) and intent.amount.asset == intent.target.funding_currency
    )


def validate_observed(state, observed):
    if tuple(o.target for o in observed) != tuple(o.target for o in state.owned):
        raise ValueError("observations must cover each retained inventory exactly once")
    for owned, observation in zip(state.owned, observed, strict=True):
        validate_owned(owned, observation)


def validate_initial(state, observed):
    validate_observed(state, observed)
    if any(o.needs_cleanup for o in observed):
        raise ValueError("new deployment cannot adopt pre-existing spending authority")


def adopt_owned(owned, observed):
    """Ownership of an operator deployment at its start: what its allocation states.

    The stated inventory is attributed only when the wallet covers it; an operator is
    refused here, before anything is planned, when it does not. An NFT the allocation
    names becomes the owned position with the liquidity and the fee checkpoint the
    observation read from the chain, which verified its owner, pool key and range.
    Nothing else of the wallet is adopted.
    """
    return tuple(adopted(item, seen) for item, seen in zip(owned, observed, strict=True))


def adopted(item, seen):
    for held, attributed in zip(
        (seen.wallet0, seen.wallet1), (item.inventory0, item.inventory1), strict=True
    ):
        if held.amount < attributed.amount:
            raise DataError(
                "inventory",
                f"the wallet holds {held.amount} base units of {held.asset}, "
                f"less than the {attributed.amount} the action needs",
            )
    if item.adopts is None:
        return item
    if seen.owned != item or not seen.position_liquidity:
        raise DataError("ownership", "the observation does not hold the NFT to adopt")
    # The position was opened before this deployment, so its entry is already made. Its
    # fee checkpoint is the PoolManager's own, so the fees it has earned and not yet
    # collected are measured from it; None when that read failed, which later reports
    # the fees as unmeasured.
    return Ownership(
        target=item.target,
        inventory0=item.inventory0,
        inventory1=item.inventory1,
        position=LiquidityPosition(
            nft=item.adopts, liquidity=seen.position_liquidity, fee_growth=seen.position_growth
        ),
        entry_complete=True,
    )


def commit_owned(owned, outcome):
    if not outcome.success:
        return owned
    return tuple(outcome.after.owned if o.target == outcome.operation.target else o for o in owned)


def validate_protocol_closure(state, observed):
    validate_observed(state, observed)
    if any(o.position is not None for o in state.owned):
        raise ValueError("closure requires no LP obligation")
    if any(o.needs_cleanup for o in observed):
        raise ValueError("closure requires verified ERC-20 and Permit2 cleanup")


def validate_closure(state, observed):
    """Closed positions may retain counter inventory only as the residual measured here."""
    validate_protocol_closure(state, observed)
    if any(
        o.owned.position is not None or not counter_settled(o.owned, o.exit_residual)
        for o in observed
    ):
        raise ValueError("closure requires no LP obligation or unmeasured non-funding inventory")


class UniswapLifecycle:
    ownership = staticmethod(protocol_ownership)
    increases_exposure = staticmethod(increases_exposure)
    validate_initial = staticmethod(validate_initial)
    validate_owned = staticmethod(validate_observed)
    commit_owned = staticmethod(commit_owned)
    adopt = staticmethod(adopt_owned)
    validate_closure = staticmethod(validate_closure)

    @staticmethod
    def initial(config, strategy, capabilities, permissions):
        from almanak.triggers import DecisionSchedule

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
            owned=tuple(initial_owned(config, a.name) for a in config.allocations),
        )
