"""Progress admitted asynchronous requests without another strategy decision."""

from almanak.connectors.composition import connector_for


def unresolved(state):
    binding = connector_for(state.config).requests
    return binding is not None and binding.unresolved(state)


def is_cancellation(state, operation):
    binding = connector_for(state.config).requests
    return binding is not None and binding.is_cancellation(state, operation)


def discard_plan(state):
    return state.model_copy(
        update={"plan": None, "decision": state.decision if unresolved(state) else None}
    )


def release_pending(state):
    """Release a signed attempt that can never take effect; the next decision plans afresh.

    An attempt that may have been sent stays recorded in `released` until an attempt at
    its nonce is reconciled, so a copy that lands first stops the deployment with its
    identity retained. Bytes never sent cannot land, so a never-sent attempt is not
    retained, and repeated refusals at one nonce do not grow the checkpoint.
    """
    from almanak.transaction_family import is_unsent

    if "released" not in type(state).model_fields:
        raise ValueError("this connector cannot release signed work")
    retained = state.released if is_unsent(state.pending) else (*state.released, state.pending)
    return state.model_copy(
        update={
            "pending": None,
            "released": retained,
            "decision": state.decision if unresolved(state) else None,
        }
    )


def drop_superseded(state):
    """Drop a signed attempt that a final transaction superseded; nothing is retained.

    The gateway proved the attempt can never be mined, so no copy of it can land and its
    identity is not needed to reconcile its nonce. The request it belonged to ends, as
    after any release.
    """
    return state.model_copy(
        update={"pending": None, "decision": state.decision if unresolved(state) else None}
    )


def settle_released(state, pending):
    """Drop released attempts at a reconciled attempt's nonce; they can no longer land."""
    if not getattr(state, "released", ()):
        return {}
    from almanak.gateway.execution import submission_slot

    slot = submission_slot(pending)
    return {"released": tuple(r for r in state.released if submission_slot(r) != slot)}


def commit_request(current, before, outcome):
    if current != before:
        raise RuntimeError("request state changed during reconciliation")
    if (
        current.pending is not None
        or current.plan is not None
        or current.decision is None
        or current.delivered != len(current.outcomes)
        or not unresolved(current)
        or outcome.decision != current.decision
    ):
        raise ValueError("request outcome requires an unresolved durably admitted request")
    connector = connector_for(current.config)
    ownership = connector.lifecycle.ownership(current, outcome)
    changed = current.model_copy(update={"outcomes": (*current.outcomes, outcome), **ownership})
    # A stepwise binding's intent continues with its next step once an intermediate request
    # resolves, as a transaction's intermediate outcome keeps its decision. Any other
    # binding's intermediate outcome must leave its request unresolved.
    stepwise = getattr(connector.requests, "stepwise", False)
    if not outcome.strategy_result().complete and not unresolved(changed) and not stepwise:
        raise ValueError("intermediate request outcome cannot erase unresolved work")
    if outcome.strategy_result().complete and not unresolved(changed):
        changed = changed.model_copy(update={"decision": None})
    return changed


def progress(runtime, state):
    outcome = runtime.gateway.request_reconcile()
    if outcome is not None:
        runtime.journal.update(lambda current: commit_request(current, state, outcome))
        return False
    if state.mode == "teardown":
        plan = runtime.gateway.request_cancel_plan()
        if plan is not None:
            runtime.journal.update(
                lambda current: (
                    current.model_copy(update={"plan": plan}) if current == state else current
                )
            )
    return False
