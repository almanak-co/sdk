"""Exchange ownership and verified teardown for shared runtime orchestration."""

from collections.abc import Callable
from typing import ClassVar
from uuid import uuid4

from almanak.connectors.dex_residual import counter_settled
from almanak.connectors.ownership import protocol_ownership
from almanak.connectors.v3_pool_validation import validate_owned
from almanak.dex_intents import IncreaseLiquidity, OpenLiquidity, SwapExactInput
from almanak.gateway.ethereum import DataError
from almanak.work import initial_work


def increases_exposure(intent):
    return isinstance(intent, (OpenLiquidity, IncreaseLiquidity)) or (
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
    names becomes the owned position exactly as the observation read it from the chain,
    which verified its owner, pool and range. Nothing else of the wallet is adopted.
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
    if seen.position is None or seen.position.nft != item.adopts:
        raise DataError("ownership", "the observation does not hold the NFT to adopt")
    # The position was opened before this deployment, so its entry is already made.
    return type(item)(
        target=item.target,
        inventory0=item.inventory0,
        inventory1=item.inventory1,
        position=seen.position,
        entry_complete=True,
    )


def commit_owned(owned, outcome):
    if not outcome.success:
        return owned
    return tuple(outcome.after.owned if o.target == outcome.operation.target else o for o in owned)


def validate_closure(state, observed):
    """Closed positions may retain counter inventory only as the residual measured here."""
    validate_protocol_closure(state, observed)
    if any(has_obligations(o.owned, o.exit_residual) for o in observed):
        raise ValueError("closure requires no LP obligation or unmeasured non-funding inventory")


def validate_protocol_closure(state, observed):
    validate_observed(state, observed)
    if any(o.position is not None for o in state.owned):
        raise ValueError("closure requires no LP obligation")
    if any(o.needs_cleanup for o in observed):
        raise ValueError("closure requires verified ERC-20 cleanup")


def has_obligations(owned, residual=None):
    return owned.position is not None or not counter_settled(owned, residual)


class Lifecycle:
    checkpoint_type: ClassVar[Callable]
    work_type: ClassVar[Callable]
    initial_owned: ClassVar[Callable]

    ownership = staticmethod(protocol_ownership)
    increases_exposure = staticmethod(increases_exposure)
    validate_initial = staticmethod(validate_initial)
    validate_owned = staticmethod(validate_observed)
    commit_owned = staticmethod(commit_owned)
    adopt = staticmethod(adopt_owned)
    validate_closure = staticmethod(validate_closure)

    @classmethod
    def initial(cls, config, strategy, capabilities, permissions):
        from almanak.triggers import DecisionSchedule

        if strategy is not None and any(a.adopt is not None for a in config.allocations):
            raise ValueError("only an operator deployment adopts inventory or a position")
        return cls.checkpoint_type(
            schedule=DecisionSchedule(),
            work=initial_work(config, cls.work_type),
            deployment_id=uuid4().hex,
            config=config,
            strategy=strategy,
            operator=strategy is None,
            capabilities=capabilities,
            permissions=permissions,
            mode="running",
            initialized=False,
            owned=tuple(cls.initial_owned(config, a.name) for a in config.allocations),
        )
