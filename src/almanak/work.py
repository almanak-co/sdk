"""Atomic chain-scoped work within one deployment, configuration and strategy memory."""

from typing import Literal

from pydantic import Field

from almanak.chains import configured_chains
from almanak.models import Chain, Model


class ChainFault(Model):
    kind: str
    message: str
    invalidated: bool = False


class ChainWork[Decision, Plan, Pending](Model):
    chain: Chain
    initialized: bool = False
    decision: Decision | None = None
    plan: Plan | None = None
    pending: Pending | None = None
    released: tuple[Pending, ...] = Field(default=(), exclude_if=lambda value: not value)
    closed: bool = False
    fault: ChainFault | None = None


AvailabilityStatus = Literal["unobserved", "ready", "unavailable", "invalidated", "closed"]


class ChainAvailability(Model):
    chain: Chain
    status: AvailabilityStatus
    fault: ChainFault | None = None


class ObservationScope(Model):
    chain: Chain
    account: str = Field(min_length=1, pattern=r"^\S+$")
    allocation: str = Field(min_length=1, pattern=r"^\S+$")


def local(items, chain):
    return tuple(item for item in items if item.target.fund.chain == chain)


def outcome_chain(outcome):
    from almanak.external_effects import ExternalEffect

    if isinstance(outcome, ExternalEffect):
        return outcome.identity.scope.chain
    return outcome.operation.target.fund.chain


def work_for(state, chain):
    return next(item for item in state.work if item.chain == chain)


def project(state, chain):
    """Ephemeral view only. Its full configuration remains the authoritative instance."""
    from almanak.external_effects import scoped_fields

    if not state.work:
        if configured_chains(state.config) != (chain,):
            raise ValueError("checkpoint does not belong to this execution scope")
        return state
    work = work_for(state, chain)
    outcomes = tuple(o for o in state.outcomes if outcome_chain(o) == chain)
    delivered = sum(outcome_chain(o) == chain for o in state.outcomes[: state.delivered])
    return state.model_copy(
        update={
            **project_inventory(state, chain),
            **request_fields(state, chain),
            **scoped_fields(state, chain),
            **transfer_fields(state, chain),
            **inbound_fields(state, chain),
            "work": (),
            "scheduler_cursor": 0,
            "initialized": work.initialized,
            "decision": work.decision,
            "plan": work.plan,
            "pending": work.pending,
            **released_field(state, work.released),
            "integrity_error": work.fault.message
            if work.fault and work.fault.invalidated
            else None,
            "owned": local(state.owned, chain),
            "baseline": local(state.baseline, chain),
            "closure": local(state.closure, chain),
            "outcomes": outcomes,
            "delivered": delivered,
            "mode": "closed" if work.closed else state.mode,
        }
    )


def released_field(state, released):
    # Only connectors that can release expired signed work declare the field.
    return {"released": released} if "released" in type(state).model_fields else {}


def replace_scoped(original, changed, chain):
    by_target = {item.target: item for item in changed}
    retained = tuple(
        by_target.pop(item.target) if item.target.fund.chain == chain else item for item in original
    )
    return (*retained, *by_target.values())


def project_inventory(state, chain):
    if not hasattr(state, "inventory"):
        return {}
    return {"inventory": tuple(b for b in state.inventory if b.key.chain == chain)}


def merge_inventory(state, changed, chain):
    if not hasattr(state, "inventory"):
        return {}
    expected = {b.key for b in state.inventory if b.key.chain == chain}
    amounts = {b.key: b for b in changed.inventory}
    if set(amounts) != expected or len(amounts) != len(changed.inventory):
        raise ValueError("scoped inventory must retain exactly its chain's wallet assets")
    return {"inventory": tuple(amounts.get(b.key, b) for b in state.inventory)}


def transfer_fields(state, chain, changed=None):
    # Only checkpoints that retain value in flight between chains declare the field. A
    # transfer is shown to its source and its destination chain, which each resolve it.
    if "transfers" not in type(state).model_fields:
        return {}
    from almanak.transfers import merged, visible

    if changed is None:
        return {"transfers": visible(state.transfers, chain)}
    return {"transfers": merged(state.transfers, changed.transfers, chain)}


def inbound_fields(state, chain):
    """Wallet assets of this chain that another chain's unrecorded bridge work delivers into.

    A deposit another chain has planned or signed can be mined, and filled here, before
    its outcome opens the transfer. Until then this chain's wallet may hold tokens no
    record names yet. The field exists only in a chain's view and is never retained.
    """
    if "inbound" not in type(state).model_fields:
        return {}
    from almanak.bridge_intents import arrival_wallet

    keys = (
        arrival_wallet(getattr(getattr(attempt, "operation", None), "target", None))
        for work in state.work
        if work.chain != chain
        for attempt in (work.plan, work.pending)
        if attempt is not None
    )
    return {"inbound": tuple(dict.fromkeys(k for k in keys if k and k.chain == chain))}


def reopened(works, transfers):
    """A chain an open transfer touches is not closed: it still owes the arrival or the refund.

    A chain with nothing left can close before another chain's signed work opens a
    transfer towards it. Opening the transfer reopens that chain.
    """
    waiting = {c for t in transfers if t.open for c in (t.source.chain, t.destination.chain)}
    return tuple(
        work.model_copy(update={"closed": False}) if work.closed and work.chain in waiting else work
        for work in works
    )


def merge(state, chain, changed):
    from almanak.external_effects import scoped_fields

    if not state.work:
        return changed
    before = project(state, chain)
    if changed.outcomes[: len(before.outcomes)] != before.outcomes:
        raise ValueError("scoped changes cannot erase execution outcomes")
    work = work_for(state, chain).model_copy(
        update={
            "initialized": changed.initialized,
            "decision": changed.decision,
            "plan": changed.plan,
            "pending": changed.pending,
            "released": getattr(changed, "released", ()),
            "closed": changed.mode == "closed",
        }
    )
    transfers = transfer_fields(state, chain, changed)
    works = reopened(
        tuple(work if item.chain == chain else item for item in state.work),
        transfers.get("transfers", ()),
    )
    return state.model_copy(
        update={
            **merge_inventory(state, changed, chain),
            **request_fields(state, chain, changed),
            **scoped_fields(state, chain, changed),
            **transfers,
            "work": works,
            "memory": changed.memory,
            "schedule": changed.schedule,
            "owned": replace_scoped(state.owned, changed.owned, chain),
            "baseline": replace_scoped(state.baseline, changed.baseline, chain),
            "closure": replace_scoped(state.closure, changed.closure, chain),
            "outcomes": (*state.outcomes, *changed.outcomes[len(before.outcomes) :]),
            "mode": "closed" if all(item.closed for item in works) else state.mode,
        }
    )


def initial_work(config, work_type):
    chains = configured_chains(config)
    return () if len(chains) == 1 else tuple(work_type(chain=c) for c in chains)


def validate_work(state):
    if not state.work:
        if len(configured_chains(state.config)) > 1:
            raise ValueError("multi-chain checkpoints require durable work for every chain")
        return
    if tuple(w.chain for w in state.work) != configured_chains(state.config):
        raise ValueError("work must cover every configured chain exactly once")
    if (
        state.initialized
        or state.decision
        or state.plan
        or state.pending
        or getattr(state, "released", ())
        or state.integrity_error
    ):
        raise ValueError("chain work cannot coexist with a scalar execution slot")
    if state.mode == "closed" and not all(w.closed for w in state.work):
        raise ValueError("deployment closure requires every chain to close")
    for work in state.work:
        validate_local_work(work)


def validate_local_work(work):
    for field in (work.decision, work.plan, work.pending, *work.released):
        if field is None:
            continue
        target = field.intent.target if field is work.decision else field.operation.target
        if target.fund.chain != work.chain:
            raise ValueError("work contains an intent or attempt routed to another chain")
    for attempt in (work.plan, work.pending):
        if attempt is not None and attempt.decision != work.decision:
            raise ValueError("attempt must retain its chain decision")
    if work.plan is not None and work.pending is not None:
        raise ValueError("each chain permits only one active transaction attempt")
    if work.closed and (work.decision or work.plan or work.pending):
        raise ValueError("chain closure cannot discard unresolved work")


def availability(state):
    if not state.work:
        status: AvailabilityStatus = (
            "invalidated"
            if state.integrity_error
            else (
                "closed"
                if state.mode == "closed"
                else "ready"
                if state.initialized
                else "unobserved"
            )
        )
        return tuple(
            ChainAvailability(chain=chain, status=status)
            for chain in configured_chains(state.config)
        )
    return tuple(
        ChainAvailability(chain=w.chain, status=work_status(w), fault=w.fault) for w in state.work
    )


def work_status(work):
    if work.closed:
        return "closed"
    if work.fault:
        return "invalidated" if work.fault.invalidated else "unavailable"
    return "ready" if work.initialized else "unobserved"


def retain_fault(state, chain, error):
    fault = ChainFault(
        kind=error.kind,
        message=str(error),
        invalidated=error.kind in ("reorganization", "integrity"),
    )
    work = work_for(state, chain)
    if work.fault and work.fault.invalidated:
        return state
    changed = work.model_copy(update={"fault": fault})
    return state.model_copy(
        update={"work": tuple(changed if w.chain == chain else w for w in state.work)}
    )


def clear_fault(state, chain):
    work = work_for(state, chain)
    if work.fault is None or work.fault.invalidated:
        return state
    changed = work.model_copy(update={"fault": None})
    return state.model_copy(
        update={"work": tuple(changed if w.chain == chain else w for w in state.work)}
    )


def validate_dependencies(decision):
    observed = {
        ObservationScope(
            chain=o.target.fund.chain, account=o.target.account, allocation=o.target.allocation
        )
        for o in decision.observations
    }
    if not set(decision.required_observations).issubset(observed):
        raise ValueError("decision dependencies lack required observations")


def request_fields(state, chain, changed=None):
    from almanak.connectors.composition import connector_for

    binding = connector_for(state.config).requests
    if binding is None:
        return {}
    return (
        binding.project(state, chain) if changed is None else binding.merge(state, changed, chain)
    )
