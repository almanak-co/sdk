"""A bridge allocation owns no venue position; a mined deposit opens a transfer."""

from uuid import uuid4

from almanak.bridge_intents import BridgeIntent
from almanak.connectors.across.observations import Ownership
from almanak.connectors.across.settlement_types import CONNECTOR, ArrivalOutcome, RefundOutcome
from almanak.connectors.closure_obligations import obligation, require_closed
from almanak.gateway.ethereum import DataError
from almanak.transfers import Arrived, Refunded, opened, proven_unfilled, resolved
from almanak.triggers import DecisionSchedule
from almanak.work import initial_work


def validate_owned(state, observations):
    if tuple(o.target for o in observations) != tuple(o.target for o in state.owned):
        raise DataError(
            "ownership_mismatch", "observations must cover every configured Across route"
        )


def validate_initial(state, observations):
    validate_owned(state, observations)
    if any(o.allowance for o in observations):
        raise DataError("existing_exposure", "a new deployment cannot adopt a SpokePool allowance")


def adopt_owned(owned, observed):
    """Ownership of an operator deployment at its start. A bridge owns nothing in the
    venue, and an allowance the wallet already gave the SpokePool is cleared by the
    request's own first step, so nothing is taken over."""
    if tuple(o.target for o in observed) != tuple(o.target for o in owned):
        raise DataError(
            "ownership_mismatch", "observations must cover every configured Across route"
        )
    return owned


def commit_owned(retained, outcome):
    return retained


def settled(state, outcome):
    """The transfers after an arrival or a refund, each resolved by its measured amount."""
    source = outcome.target.fund.chain
    transfers = state.transfers
    if isinstance(outcome, ArrivalOutcome):
        identity = (CONNECTOR, source, outcome.transfer)
        return resolved(transfers, identity, Arrived(received=outcome.received))
    paid = [
        t
        for t in transfers
        if (t.connector, t.reference) in {(CONNECTOR, r) for r in outcome.transfers}
    ]
    if sum(t.sent.amount for t in paid) != outcome.returned.amount:
        raise ValueError("an Across refund must return exactly what its transfers sent")
    for reference in outcome.transfers:
        identity = (CONNECTOR, source, reference)
        sent = next(t.sent for t in transfers if t.identity == identity)
        # A refund returns each transfer's whole sent amount; the page proved their sum.
        transfers = resolved(transfers, identity, Refunded(returned=sent))
    return transfers


def unfilled(state, transfers):
    """Mark each open transfer whose arrival scan, retained in this chain's view, ended
    past the fill deadline without a fill. Only the destination chain holds such a page."""
    for page in getattr(state, "external_scans", ()):
        if not page.expired:
            continue
        identity = (CONNECTOR, page.target.fund.chain, page.transfer)
        current = next((t for t in transfers if t.identity == identity), None)
        if current is not None and current.open and not current.unfilled:
            transfers = proven_unfilled(transfers, identity)
    return transfers


def ownership(state, outcome=None):
    """The owned fields after `outcome`: a mined deposit opens its transfer, an arrival
    or a refund resolves the transfers it settles, and an expired arrival scan proves
    its transfer unfilled."""
    if isinstance(outcome, (ArrivalOutcome, RefundOutcome)):
        return {"owned": state.owned, "transfers": settled(state, outcome)}
    transfer = getattr(outcome, "transfer", None)
    transfers = state.transfers if transfer is None else opened(state.transfers, transfer)
    return {"owned": state.owned, "transfers": unfilled(state, transfers)}


def validate_closure(state, observations):
    validate_owned(state, observations)
    require_closed(
        "Across teardown retains a SpokePool allowance",
        (obligation(observed, {"allowance": observed.allowance}) for observed in observations),
    )
    waiting = [t.reference for t in state.transfers if t.resolution is None]
    if waiting:
        # Teardown does not bridge back and cannot cancel: it waits until every transfer
        # is arrived or refunded. One proven unfilled still waits for its refund.
        raise DataError(
            "transfer_in_flight",
            "Across teardown waits for transfers in flight: " + ", ".join(waiting),
        )


class AcrossLifecycle:
    ownership = staticmethod(ownership)
    validate_initial = staticmethod(validate_initial)
    validate_owned = staticmethod(validate_owned)
    commit_owned = staticmethod(commit_owned)
    adopt = staticmethod(adopt_owned)
    validate_closure = staticmethod(validate_closure)

    @staticmethod
    def increases_exposure(intent):
        return isinstance(intent, BridgeIntent)

    @staticmethod
    def initial(config, strategy, capabilities, permissions):
        from almanak.connectors.across.state import Checkpoint, Work

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
            operator=strategy is None,
            owned=tuple(Ownership(target=config.target(a.name)) for a in config.positions),
        )
