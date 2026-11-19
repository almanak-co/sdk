"""Curve closure retains LP shares, residual coin inventory and spending obligations."""

from collections.abc import Callable
from typing import ClassVar
from uuid import uuid4

from almanak.connectors.curve.observations import initial_owned, unsettled
from almanak.connectors.curve.ownership import refuse_unowned_shares, validate_owned
from almanak.connectors.curve.ownership import validate_initial as validate_initial_snapshot
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
    """Ownership of an operator deployment at its start.

    The coin amounts its allocation states are attributed only when the wallet covers
    each of them. A target with liquidity authority also takes the fund's whole balance
    of the pool's LP token as its position, exactly as the observation read it: LP
    tokens are fungible, so there is no part of the balance the wallet does not own.
    Beside a trading-only target the same balance is not owned, and the wallet is
    refused before anything is planned.
    """
    return tuple(adopted(item, seen) for item, seen in zip(owned, observed, strict=True))


def adopted(item, seen):
    refuse_unowned_shares(seen)
    for held, attributed in zip(seen.wallets.root, item.inventory.root, strict=True):
        if held.amount < attributed.amount:
            raise DataError(
                "inventory",
                f"the wallet holds {held.amount} base units of {held.asset}, "
                f"less than the {attributed.amount} the action needs",
            )
    if seen.position is None:
        return item
    # The shares were minted before this deployment, so its entry is already made.
    return type(item)(
        target=item.target, inventory=item.inventory, position=seen.position, entry_complete=True
    )


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

        if strategy is not None and any(a.adopt for a in config.allocations):
            raise ValueError("only an operator deployment adopts the fund's LP shares")
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
