"""Independent gateway verification of execution ownership and financial evidence."""

from almanak.gateway.ethereum import DataError


def validate_execution_update(backend, before, after):
    from almanak.external_effects import changed

    if changed(before, after):
        from almanak.gateway.external_effects import verified_commit

        verified_commit(backend, before, after)
    elif before.outcomes != after.outcomes:
        validate_outcome(backend, before, after)
    else:
        from almanak.connectors.composition import connector_for

        ownership = connector_for(before.config).lifecycle.ownership(before)
        if any(getattr(after, field) != value for field, value in ownership.items()):
            raise ValueError("ownership can change only through verified execution evidence")
        from almanak.request_runtime import unresolved

        if unresolved(before) and before.decision != after.decision:
            raise ValueError("unresolved requests must retain their admitted decision")
    if before.initialized != after.initialized:
        validate_initialization(backend, before, after)
    elif before.baseline != after.baseline:
        raise ValueError("recorded baseline is immutable")
    if after.mode == "closed" and before.mode != "closed":
        verify_closure(backend, after)
    elif after.closure != before.closure:
        raise ValueError("closure evidence can be recorded only at verified closure")


def validate_outcome(backend, before, after):
    from almanak.runtime import commit_outcome

    if before.pending is None:
        from almanak.gateway.requests import verified_commit

        verified_commit(backend, before, after)
        return
    if len(after.outcomes) != len(before.outcomes) + 1:
        raise ValueError("outcomes require one durably admitted pending transaction")
    receipt = backend.receipt(before.pending)
    if receipt is None:
        raise DataError("unavailable", "outcome admission requires a canonical mined receipt")
    outcome = backend.reconcile(before.pending, receipt)
    expected = commit_outcome(before, before.pending, outcome)
    from almanak.connectors.composition import connector_for

    fields = set(connector_for(before.config).lifecycle.ownership(before, outcome))
    fields.update(("outcomes", "owned", "plan", "pending", "decision", "memory", "delivered"))
    for field in fields:
        if getattr(after, field) != getattr(expected, field):
            raise ValueError("recorded outcome differs from independently verified execution")


def verified_observations(backend, observations):
    return tuple(backend.snapshot(item.allocation, item.block, item) for item in observations)


def validate_initialization(backend, before, after):
    from almanak.runtime import validate_initial_balances

    if before.initialized or not after.initialized:
        raise ValueError("initialization cannot be undone")
    if verified_observations(backend, after.baseline) != after.baseline:
        raise ValueError("baseline differs from verified chain observations")
    validate_initial_balances(before, after.baseline)
    require_empty_venue(backend, after)


def verify_closure(backend, state):
    from almanak.request_runtime import unresolved
    from almanak.runtime import validate_closure, validate_observed_ownership

    if (
        state.pending is not None
        or state.plan is not None
        or state.decision is not None
        or unresolved(state)
    ):
        raise ValueError("closure cannot discard unresolved execution")
    for released in getattr(state, "released", ()):
        backend.require_nonce_unused(released)
    if verified_observations(backend, state.closure) != state.closure:
        raise ValueError("closure differs from verified chain observations")
    validate_observed_ownership(state, state.closure)
    validate_closure(state, state.closure)
    require_empty_venue(backend, state)


def require_empty_venue(backend, state):
    """An adapter whose positions live at an off-chain venue proves there, with its own
    reads, that the deployment's account holds nothing before it starts and after it
    closes; block-pinned snapshots cannot show venue state (blueprint 30, 13.1)."""
    verify = getattr(backend, "require_empty_venue", None)
    if verify is not None:
        verify(state)
