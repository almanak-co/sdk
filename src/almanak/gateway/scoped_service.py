"""Independent admission of chain-scoped mutations, committed under the shared journal lock."""

from copy import copy

from almanak.gateway.ethereum import DataError
from almanak.gateway.routing import backend_for
from almanak.work import clear_fault, merge, project

ROUTED = frozenset(
    (
        "observe",
        "audit",
        "plan",
        "sign",
        "receipt",
        "broadcast",
        "reconcile",
        "family",
        "update",
        "request-reconcile",
        "request-cancel-plan",
        "external-reconcile",
    )
)


def dispatch_chain(service, method, request, db, state):
    chain = request.chain
    if chain is None:
        raise ValueError("chain operations require an explicit execution scope")
    local = project(state, chain)
    scoped = copy(service)
    scoped.route_chain = chain
    scoped.backend = backend_for(service.backend, chain)
    scoped.check_execution_authority(method, local)
    if method == "update":
        return record_chain_update(scoped, request, db, state)
    if method in ("family", "broadcast"):
        return scoped.dispatch_admission(method, request, db, state, local)
    result = scoped.dispatch_authorized(method, request, db, local)
    if method == "audit" and state.work:
        check_chain_risk(local)
        db.apply(lambda current: clear_audited(current, chain, local))
    return result


def clear_audited(current, chain, audited):
    if project(current, chain).outcomes != audited.outcomes:
        raise DataError("conflict", "chain history changed during its audit")
    return clear_fault(current, chain)


def check_chain_risk(state):
    from almanak.external_effects import ExternalEffect

    if state.mode != "running":
        return
    if any(
        not outcome.success for outcome in state.outcomes if not isinstance(outcome, ExternalEffect)
    ):
        raise DataError(
            "execution_failed", "recorded execution failure blocks this chain's new exposure"
        )
    if any(outcome.accounting_issues for outcome in state.outcomes):
        raise DataError("accounting", "unexplained fund movements block this chain's new exposure")


def record_chain_update(service, request, db, state):
    if request.before != state:
        raise DataError("conflict", "checkpoint changed; reload before recording")
    after = request.after
    if after is None:
        raise ValueError("updated checkpoint is required")
    chain = request.chain
    before_local, after_local = project(state, chain), project(after, chain)
    if merge(state, chain, after_local) != after:
        raise ValueError("scoped update changes another chain or shared deployment identity")
    service.validate_update(before_local, after_local)
    db.replace(after)
    return after


def validate_global_update(service, before, after):
    mutable = {"memory", "delivered", "scheduler_cursor"}
    if before.model_dump(exclude=mutable) != after.model_dump(exclude=mutable):
        raise ValueError("execution changes require an explicit chain route")
    if after.delivered not in (before.delivered, before.delivered + 1):
        raise ValueError("outcomes must be delivered once in retained order")
    if after.scheduler_cursor >= len(after.work):
        raise ValueError("scheduler cursor exceeds the configured chain collection")
    service.validate_recorded_evidence(before, after)
