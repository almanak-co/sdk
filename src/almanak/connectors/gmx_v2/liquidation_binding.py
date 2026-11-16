"""Canonical external liquidation scanning is independent of admitted request authority."""

from almanak.connectors.gmx_v2 import liquidation_reader
from almanak.connectors.gmx_v2.liquidation_types import Scan
from almanak.connectors.gmx_v2.positions import Ownership
from almanak.gateway.ethereum import DataError


def reconcile(backend, state, *, reference=None):
    targets = [o.target for o in state.owned if isinstance(o, Ownership) and o.position is not None]
    if reference is None and state.external_scans:
        last = state.external_scans[-1].target
        if last in targets:
            index = targets.index(last) + 1
            targets = targets[index:] + targets[:index]
    if reference is not None:
        if reference.target not in targets:
            raise DataError("ownership", "GMX external page does not concern an owned position")
        targets = [reference.target]
    for target in targets:
        result = liquidation_reader.page(backend, state, target, reference)
        if result is not None:
            return result
    return None


def audit(backend, state):
    reader = backend.reader
    for page in state.external_scans:
        if page.scope.chain != reader.chain:
            continue
        if (
            reader.pin_block(page.start.number) != page.start
            or reader.pin_block(page.end.number) != page.end
        ):
            raise DataError(
                "reorganization", "GMX retained external scan bounds are no longer canonical"
            )
        if liquidation_reader.logs(reader, page.target, page.start, page.end) != page.events:
            raise DataError(
                "reorganization", "GMX retained external scan events are no longer canonical"
            )
        if page.outcome is not None:
            audit_outcome(reader, page.outcome)


def audit_outcome(reader, outcome):
    receipt = reader._rpc("eth_getTransactionReceipt", [outcome.tx_hash], allow_none=True)
    trace = reader._rpc("debug_traceTransaction", [outcome.tx_hash, {"tracer": "callTracer"}])
    if receipt != outcome.receipt or trace != outcome.trace:
        raise DataError("reorganization", "GMX retained liquidation receipt or trace has changed")


class ExternalEffects:
    page_type = Scan
    reconcile = staticmethod(reconcile)
    audit = staticmethod(audit)
