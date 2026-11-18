"""Independent admission of connector-owned asynchronous request evidence."""

from almanak.connectors.composition import connector_for
from almanak.request_runtime import commit_request


def ready(state):
    binding = connector_for(state.config).requests
    if (
        binding is None
        or not binding.unresolved(state)
        or state.decision is None
        or state.pending is not None
        or state.plan is not None
        or state.delivered != len(state.outcomes)
        or state.mode == "closed"
    ):
        raise ValueError("request progression requires durably admitted, delivered unresolved work")
    return binding


def dispatch(backend, state, method, attempt_id=None):
    binding = ready(state)
    if method == "request-reconcile":
        return binding.reconcile(backend, state)
    if method != "request-cancel-plan" or state.mode != "teardown" or not attempt_id:
        raise ValueError("request cancellation planning requires teardown and attempt identity")
    if binding.reconcile(backend, state) is not None:
        return None
    plan = binding.cancellation_plan(backend, state, attempt_id)
    if plan is not None and (
        plan.decision != state.decision or not binding.is_cancellation(state, plan.operation)
    ):
        raise ValueError("cancellation must retain the admitted request and decision")
    return plan


def verified_commit(backend, before, after):
    binding = ready(before)
    if len(after.outcomes) != len(before.outcomes) + 1:
        raise ValueError("request reconciliation appends exactly one outcome")
    backend.audit_history(before)
    outcome = binding.reconcile(backend, before)
    if outcome is None or outcome != after.outcomes[-1]:
        raise ValueError("request outcome differs from independently reconciled evidence")
    expected = commit_request(before, before, outcome)
    fields = set(connector_for(before.config).lifecycle.ownership(before, outcome))
    fields.update(("outcomes", "owned", "plan", "pending", "decision", "memory", "delivered"))
    if any(getattr(after, field) != getattr(expected, field) for field in fields):
        raise ValueError("request commit differs from independently verified ownership")
    return expected


def validate_cancellation_plan(backend, state, plan, observed):
    binding = ready(state)
    if state.mode != "teardown" or not binding.is_cancellation(state, plan.operation):
        raise ValueError("unresolved requests permit only their admitted cancellation")
    if binding.reconcile(backend, state) is not None:
        raise ValueError("reconcile the request event before cancellation admission")
    expected = binding.cancellation_plan(backend, state, plan.attempt_id, reference=observed)
    if expected is None or expected != plan:
        raise ValueError("cancellation differs from independently verified preparation")
