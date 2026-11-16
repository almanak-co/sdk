"""GMX inventory and request history commit together through ordinary runtime ownership."""

from uuid import uuid4

from almanak.connectors.gmx_v2.exchange_observations import ExchangeOwnership, ExchangeSnapshot
from almanak.connectors.gmx_v2.intents import IncreasePosition
from almanak.connectors.gmx_v2.liquidation_types import LiquidationOutcome
from almanak.connectors.gmx_v2.observations import initial_owned
from almanak.connectors.gmx_v2.positions import Ownership
from almanak.connectors.gmx_v2.state import Checkpoint, Work
from almanak.dex_intents import LiquidityBudget, SwapExactInput
from almanak.triggers import DecisionSchedule
from almanak.work import initial_work


def replace_request(requests, updated):
    if updated is None:
        return requests
    matches = [r for r in requests if (r.fund.chain, r.key) == (updated.fund.chain, updated.key)]
    if not matches:
        if len(updated.events) != 1 or updated.status != "accepted":
            raise ValueError("new GMX request requires exactly its acceptance evidence")
        return (*requests, updated)
    previous = matches[0]
    if updated.events[: len(previous.events)] != previous.events:
        raise ValueError("GMX request history cannot erase or replace prior evidence")
    expected = previous
    for event in updated.events[len(previous.events) :]:
        expected = expected.append(event)
    if expected != updated:
        raise ValueError("GMX request update changed its retained immutable identity")
    return tuple(updated if r == previous else r for r in requests)


def ownership(state, outcome=None):
    if outcome is None:
        return {"owned": state.owned, "requests": state.requests}
    return {
        "owned": commit_owned(state.owned, outcome),
        "requests": state.requests
        if isinstance(outcome, LiquidationOutcome)
        else replace_request(state.requests, outcome.request),
    }


def commit_owned(owned, outcome):
    return tuple(outcome.after.owned if o.target == outcome.after.target else o for o in owned)


def validate_owned(state, observations):
    if tuple(o.target for o in observations) != tuple(o.target for o in state.owned):
        raise ValueError("GMX observations must cover every retained target exactly once")
    for owned, observed in zip(state.owned, observations, strict=True):
        if observed.owned != owned:
            raise ValueError("GMX observed position differs from retained ownership")
        validate_position(owned, observed)
        if any(
            wallet.amount < balance.amount
            for wallet, balance in zip(observed.wallets, owned.inventory, strict=True)
        ):
            raise ValueError("GMX fund wallet no longer covers retained owned inventory")


def validate_position(owned, observed):
    if isinstance(owned, ExchangeOwnership):
        if (
            not isinstance(observed, ExchangeSnapshot)
            or observed.market_tokens != owned.market_tokens
        ):
            raise ValueError("GMX observed GM shares differ from retained ownership")
    elif observed.position != owned.position:
        raise ValueError("GMX observed position differs from retained ownership")


def has_position(observed):
    if isinstance(observed, ExchangeSnapshot):
        return observed.market_tokens.amount != 0
    return observed.position is not None


def validate_initial(state, observations):
    validate_owned(state, observations)
    if any(has_position(o) or o.needs_cleanup for o in observations):
        raise ValueError("GMX initialization cannot adopt existing positions, allowance or funding")


def validate_closure(state, observations):
    validate_owned(state, observations)
    if any(r.unresolved for r in state.requests):
        raise ValueError("GMX closure cannot discard unresolved protocol requests")
    if any(has_position(o) or o.needs_cleanup for o in observations):
        raise ValueError("GMX closure requires no position, token allowance or claimable funding")
    if any(a.amount for o in state.owned if isinstance(o, Ownership) for a in o.claimable_funding):
        raise ValueError("GMX closure requires settlement of remaining claimable funding")


class Lifecycle:
    ownership = staticmethod(ownership)
    commit_owned = staticmethod(commit_owned)
    validate_owned = staticmethod(validate_owned)
    validate_initial = staticmethod(validate_initial)
    validate_closure = staticmethod(validate_closure)

    @staticmethod
    def increases_exposure(intent):
        if isinstance(intent, SwapExactInput):
            return intent.minimum_output.asset != intent.target.funding_asset
        return isinstance(intent, (IncreasePosition, LiquidityBudget))

    @staticmethod
    def initial(config, strategy, capabilities, permissions):
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
