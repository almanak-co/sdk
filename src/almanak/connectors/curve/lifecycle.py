"""Curve closure retains LP shares, residual coin inventory and spending obligations."""

from collections.abc import Callable
from typing import ClassVar
from uuid import uuid4

from almanak.connectors.curve.observations import initial_owned, unsettled
from almanak.connectors.curve.ownership import validate_initial as validate_initial_snapshot
from almanak.connectors.curve.ownership import validate_owned
from almanak.connectors.curve.state import Checkpoint, Work
from almanak.connectors.ownership import protocol_ownership
from almanak.dex_intents import IncreasePoolLiquidity, OpenPoolLiquidity, SwapExactInput
from almanak.gateway.ethereum import DataError
from almanak.work import initial_work


def increases_exposure(intent):
    return isinstance(intent, (OpenPoolLiquidity, IncreasePoolLiquidity)) or (
        isinstance(intent, SwapExactInput) and intent.amount.asset == intent.target.funding_asset
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

    Nothing is read into ownership. The stated coin amounts are attributed only when
    the wallet covers each of them. LP shares are never adopted: a wallet that holds
    this pool's LP token is refused here, before anything is planned, because every
    later ownership check requires the observed shares to equal the owned ones.
    """
    for item, seen in zip(owned, observed, strict=True):
        if seen.position is not None:
            raise DataError(
                "ownership",
                f"the wallet holds {seen.position.shares.amount} base units of this pool's "
                f"LP token {seen.position.shares.asset}; a direct action adopts no LP shares",
            )
        for held, attributed in zip(seen.wallets.root, item.inventory.root, strict=True):
            if held.amount < attributed.amount:
                raise DataError(
                    "inventory",
                    f"the wallet holds {held.amount} base units of {held.asset}, "
                    f"less than the {attributed.amount} the action needs",
                )
    return owned


def commit_owned(owned, outcome):
    if not outcome.success:
        return owned
    return tuple(outcome.after.owned if o.target == outcome.operation.target else o for o in owned)


def validate_closure(state, observed):
    """Closed positions may retain non-funding inventory only as residuals measured here."""
    validate_protocol_closure(state, observed)
    if any(has_obligations(o.owned, o.exit_residuals) for o in observed):
        raise ValueError("closure requires no LP obligation or unmeasured non-funding inventory")


def validate_protocol_closure(state, observed):
    validate_observed(state, observed)
    if any(o.position is not None for o in state.owned):
        raise ValueError("closure requires no LP obligation")
    if any(o.needs_cleanup for o in observed):
        raise ValueError("closure requires verified ERC-20 cleanup")


def has_obligations(owned, residuals=()):
    return owned.position is not None or bool(unsettled(owned, residuals))


class CurveLifecycle:
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
