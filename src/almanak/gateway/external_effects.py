"""Verify the exact proposed canonical scan page independently before recording effects."""

from almanak import external_effects


def ready(state):
    binding = external_effects.binding(state)
    if (
        binding is None
        or not state.initialized
        or state.mode == "closed"
        or state.delivered != len(state.outcomes)
    ):
        raise ValueError("external scanning requires initialized, delivered, open work")
    return binding


def reconcile(backend, state):
    return ready(state).reconcile(backend, state)


def verified_commit(backend, before, after):
    binding = ready(before)
    if (
        after.external_scans[: len(before.external_scans)] != before.external_scans
        or len(after.external_scans) != len(before.external_scans) + 1
    ):
        raise ValueError("external recording appends exactly one bounded scan page")
    page = after.external_scans[-1]
    backend.audit_history(before)
    verified = binding.reconcile(backend, before, reference=page)
    if verified != page:
        raise ValueError("external page differs from independently verified canonical evidence")
    expected = external_effects.commit(before, before, page)
    if after != expected:
        raise ValueError("external commit changed pending work or differs from verified ownership")
    return expected
