"""Runtime progression of one durable transaction family."""

from almanak.gateway.ethereum import DataError

ABANDON = {"stale_plan", "elapsed_limit", "candidate_limit", "clock_reversed", "teardown"}


def progress(runtime, pending):
    family = pending.family
    index = len(family.candidates) - 1
    if family.proposal is not None:
        action = "abandon" if family.blocked in ABANDON else "sign"
        advance(runtime, pending, action, index + 1)
        return False
    if family.candidates[-1].submission is None:
        advance(runtime, pending, "attempt", index)
        return False
    try:
        if runtime.gateway.broadcast(pending):
            runtime.fault("broadcast", pending)
    except DataError as exc:
        if exc.kind in ("inconsistent", "reorganization", "integrity", "gateway_unavailable"):
            raise
        # A send rejection is not evidence that the signed operation can be forgotten.
        # The normal interval may still become eligible after an endpoint rejection.
        # The rejection itself never creates a new fee proposal.
    state = runtime.gateway.family(pending, "inspect", index)
    if state.pending is not None and state.pending.family.blocked is None:
        advance(runtime, state.pending, "reserve", index + 1)
    return False


def advance(runtime, pending, action, index):
    state = runtime.gateway.family(pending, action, index)
    if state.pending is not None and state.pending.family.blocked is None:
        runtime.fault("replacement_" + action, state.pending)
