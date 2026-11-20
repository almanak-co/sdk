"""Aerodrome closure retains LP shares, unpaid fees and spending obligations."""

from collections.abc import Callable
from typing import ClassVar
from uuid import uuid4

from almanak.connectors.aerodrome.observations import initial_owned
from almanak.connectors.aerodrome.ownership import validate_initial as validate_initial_snapshot
from almanak.connectors.aerodrome.ownership import validate_owned
from almanak.connectors.aerodrome.state import Checkpoint, Work
from almanak.connectors.dex_residual import counter_settled
from almanak.connectors.ownership import protocol_ownership
from almanak.connectors.v3_pool_lifecycle import adopt_owned as adopt_inventory
from almanak.dex_intents import IncreaseLiquidity, OpenLiquidity, SwapExactInput
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
    for value in observed:
        validate_initial_snapshot(value)
    if any(o.needs_cleanup for o in observed):
        raise ValueError("new deployment cannot adopt pre-existing spending authority")


def adopt_owned(owned, observed):
    """Ownership of an operator deployment at its start: the inventory its allocation states.

    An operator deployment is trading-only, so it adopts no pool shares and no fee
    claims. A wallet that holds either is refused when it is observed
    (`observations.position`), before this is reached. Shares in the pool's own
    balance are not checked: a swap never burns them. The stated inventory is
    attributed only when the wallet covers it; an operator is refused here, before
    anything is planned, when it does not.
    """
    if any(item.target.liquidity_enabled for item in owned):
        raise ValueError("an Aerodrome operator deployment is trading-only")
    return adopt_inventory(owned, observed)


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


class AerodromeLifecycle:
    checkpoint_type: ClassVar[Callable] = Checkpoint
    work_type: ClassVar[Callable] = Work
    initial_owned: ClassVar[Callable] = staticmethod(initial_owned)

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
