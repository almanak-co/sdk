"""Connector-owned request progress uses the shared trusted runtime and gateway seam."""

from almanak.connectors.gmx_v2.exchange_operations import CancelExchangeOrder
from almanak.connectors.gmx_v2.lifecycle import replace_request
from almanak.connectors.gmx_v2.liquidity_operations import CancelLiquidityRequest
from almanak.connectors.gmx_v2.liquidity_reader import pending, read_pending
from almanak.connectors.gmx_v2.observations import snapshot
from almanak.connectors.gmx_v2.operations import CancelOrder
from almanak.connectors.gmx_v2.request_reader import audit_request, next_event


def active(state):
    requests = [r for r in state.requests if r.unresolved]
    if len(requests) != 1 or state.decision is None:
        raise ValueError("GMX request progression requires one chain-local admitted request")
    request = requests[0]
    if request.decision_identity != state.decision.identity:
        raise ValueError("GMX unresolved request differs from its retained decision")
    return request


def next_evidence(backend, state):
    request = active(state)
    allocation = state.config.allocation(request.allocation)
    head = backend.reader.pin_block("latest")
    return next_event(
        backend.reader, request, head, maximum_block_span=allocation.maximum_event_block_span
    )


def cancellation_plan(backend, state, attempt_id, *, reference=None):
    before = cancellation_reference(backend, state, reference=reference)
    if before is None:
        return None
    return backend.prepare(state, state.decision.intent, before, attempt_id)


def cancellation_reference(backend, state, *, reference=None):
    """The snapshot a teardown cancellation of the active request is prepared from.

    None while the request has newer keeper evidence, or is gone or not yet cancellable.
    """
    if state.mode != "teardown":
        raise ValueError("automatic GMX request cancellation requires teardown")
    request = active(state)
    if next_evidence(backend, state) is not None:
        return None
    current = backend.reader.pin_block("latest")
    order = read_pending(backend.reader, state.config.target(request.allocation), request, current)
    if order is None or not order.cancellable:
        return None
    before = reference or current_snapshot(backend, state, request, current)
    require_cancellable(before, request)
    return before


def current_snapshot(backend, state, request, block):
    owned = next(o for o in state.owned if o.target.allocation == request.allocation)
    return snapshot(backend, request.allocation, block, owned, request=request)


def require_cancellable(before, request):
    order = pending(before)
    if before.request != request or order is None or not order.cancellable:
        raise ValueError("GMX cancellation reference lacks its exact eligible retained request")


def is_cancellation(state, operation):
    if not isinstance(operation, (CancelOrder, CancelExchangeOrder, CancelLiquidityRequest)):
        return False
    request = active(state)
    if isinstance(operation, CancelLiquidityRequest) and operation.request_kind != request.kind:
        return False
    return (operation.target, operation.request_key) == (
        state.config.target(request.allocation),
        request.key,
    )


def merge(state, changed, chain):
    if any(r.fund.chain != chain for r in changed.requests):
        raise ValueError("GMX scoped request merge cannot affect another execution chain")
    existing = tuple(r for r in state.requests if r.fund.chain == chain)
    if tuple(r.key for r in changed.requests[: len(existing)]) != tuple(r.key for r in existing):
        raise ValueError("GMX scoped request merge cannot erase or reorder retained identities")
    result = state.requests
    for request in changed.requests:
        result = replace_request(result, request)
    return {"requests": result}


class Requests:
    cancellation_plan = staticmethod(cancellation_plan)
    cancellation_reference = staticmethod(cancellation_reference)
    is_cancellation = staticmethod(is_cancellation)
    merge = staticmethod(merge)

    @staticmethod
    def fork_keeper(forks, output):
        """Fork tests only: replays authentic keeper reports for accepted requests."""
        from almanak.connectors.gmx_v2.fork_keeper import Keeper

        return Keeper(forks, output)

    @staticmethod
    def unresolved(state):
        return any(r.unresolved for r in state.requests)

    @staticmethod
    def reconcile(backend, state):
        from almanak.connectors.gmx_v2.reconciliation import reconcile_request

        event = next_evidence(backend, state)
        return None if event is None else reconcile_request(backend, state, active(state), event)

    @staticmethod
    def project(state, chain):
        return {"requests": tuple(r for r in state.requests if r.fund.chain == chain)}

    @staticmethod
    def audit(backend, state):
        for request in state.requests:
            if request.fund.chain == backend.reader.chain:
                audit_request(backend.reader, request)
