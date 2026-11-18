"""Exchange ownership and verified teardown for shared runtime orchestration."""

from uuid import uuid4

from almanak.connectors.ownership import protocol_ownership
from almanak.connectors.uniswap_v4.observation_models import counter_settled
from almanak.connectors.uniswap_v4.observations import initial_owned
from almanak.connectors.uniswap_v4.state import Checkpoint, Work
from almanak.connectors.uniswap_v4.validation import validate_owned
from almanak.dex_intents import OpenLiquidity, SwapExactInput
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
            capabilities=capabilities,
            permissions=permissions,
            mode="running",
            initialized=False,
            owned=tuple(initial_owned(config, a.name) for a in config.allocations),
        )
