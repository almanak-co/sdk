"""A bridge allocation owns no venue position; a mined deposit opens a transfer."""

from uuid import uuid4

from almanak.bridge_intents import BridgeIntent
from almanak.connectors.across.observations import Ownership
from almanak.connectors.across.settlement_types import CONNECTOR, ArrivalOutcome, RefundOutcome
from almanak.connectors.closure_obligations import obligation, require_closed
from almanak.gateway.ethereum import DataError
from almanak.transfers import Arrived, Refunded, opened, resolved
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


def ownership(state, outcome=None):
    """The owned fields after `outcome`: a mined deposit opens its transfer, and an
    arrival or a refund resolves the transfers it settles."""
    if isinstance(outcome, (ArrivalOutcome, RefundOutcome)):
        return {"owned": state.owned, "transfers": settled(state, outcome)}
    transfer = getattr(outcome, "transfer", None)
    transfers = state.transfers if transfer is None else opened(state.transfers, transfer)
    return {"owned": state.owned, "transfers": transfers}


def validate_closure(state, observations):
    validate_owned(state, observations)
    require_closed(
        "Across teardown retains a SpokePool allowance",
        (obligation(observed, {"allowance": observed.allowance}) for observed in observations),
    )
    waiting = [t.reference for t in state.transfers if t.open]
    if waiting:
        # Teardown does not bridge back and cannot cancel: it waits for arrival or refund.
        raise DataError(
            "transfer_in_flight",
            "Across teardown waits for transfers in flight: " + ", ".join(waiting),
        )


class AcrossLifecycle:
    ownership = staticmethod(ownership)
    validate_initial = staticmethod(validate_initial)
    validate_owned = staticmethod(validate_owned)
    commit_owned = staticmethod(commit_owned)
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
            owned=tuple(Ownership(target=config.target(a.name)) for a in config.positions),
        )
