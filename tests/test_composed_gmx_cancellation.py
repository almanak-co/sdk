"""A composed GMX cancellation reads through GMX's view and is prepared by the composed backend."""

from types import SimpleNamespace

import pytest

from almanak.connectors.composed import requests as composed_requests
from almanak.connectors.composed.requests import GMX, ComposedRequests
from almanak.connectors.gmx_v2 import request_binding


class Backend:
    """A gateway adapter stand-in that records what it prepares."""

    def __init__(self, config):
        self.config, self.prepared = config, []

    def prepare(self, state, intent, before, attempt_id):
        self.prepared.append((state, intent, before, attempt_id))
        return "plan"


def composed(monkeypatch, reference):
    seen = []
    monkeypatch.setattr(composed_requests, "component_state", lambda state, identity: "gmx state")

    def cancellation_reference(view, state, *, reference=None):
        seen.append((view, state, reference))
        return reference_result

    reference_result = reference
    monkeypatch.setattr(
        composed_requests.Requests, "cancellation_reference", staticmethod(cancellation_reference)
    )
    config = SimpleNamespace(component=lambda identity: f"{identity} config")
    state = SimpleNamespace(decision=SimpleNamespace(intent="swap"))
    return Backend(config), state, seen


def test_the_composed_backend_prepares_against_the_composed_configuration(monkeypatch):
    backend, state, seen = composed(monkeypatch, "snapshot")
    assert ComposedRequests.cancellation_plan(backend, state, "attempt", reference="ref") == "plan"
    [(view, gmx_state, reference)] = seen
    # GMX reads its pending order and snapshot through its own configuration.
    assert view.config == f"{GMX} config" and gmx_state == "gmx state" and reference == "ref"
    assert view is not backend and backend.config.component(GMX) == view.config
    # The plan is prepared with the composed state on the composed backend, whose
    # configuration the permission manifest belongs to.
    assert backend.prepared == [(state, "swap", "snapshot", "attempt")]


def test_no_reference_prepares_no_cancellation(monkeypatch):
    backend, state, seen = composed(monkeypatch, None)
    assert ComposedRequests.cancellation_plan(backend, state, "attempt") is None
    assert backend.prepared == [] and len(seen) == 1


def test_a_gmx_deployment_prepares_its_cancellation_from_the_same_reference(monkeypatch):
    backend = Backend(config="gmx config")
    state = SimpleNamespace(decision=SimpleNamespace(intent="increase"))
    monkeypatch.setattr(request_binding, "cancellation_reference", lambda b, s, reference: None)
    assert request_binding.cancellation_plan(backend, state, "attempt") is None
    monkeypatch.setattr(
        request_binding, "cancellation_reference", lambda b, s, reference: (b, s, reference)
    )
    assert request_binding.cancellation_plan(backend, state, "attempt", reference="r") == "plan"
    assert backend.prepared == [(state, "increase", (backend, state, "r"), "attempt")]


def request(allocation="trade"):
    return SimpleNamespace(allocation=allocation, unresolved=True, decision_identity="d")


def gmx_state(active, mode="teardown"):
    owned = (SimpleNamespace(target=SimpleNamespace(allocation="trade")),)
    return SimpleNamespace(
        mode=mode,
        requests=(active,),
        decision=SimpleNamespace(identity="d", intent="swap"),
        config=SimpleNamespace(target=lambda name: f"{name} target"),
        owned=owned,
    )


def cancellable(order=True):
    return SimpleNamespace(cancellable=order)


@pytest.fixture
def reads(monkeypatch):
    calls = {"evidence": None, "order": cancellable(), "pending": cancellable(), "snapshots": []}
    monkeypatch.setattr(request_binding, "next_evidence", lambda backend, state: calls["evidence"])
    monkeypatch.setattr(
        request_binding, "read_pending", lambda reader, target, request, block: calls["order"]
    )
    monkeypatch.setattr(request_binding, "pending", lambda before: calls["pending"])

    def snapshot(backend, allocation, block, owned, *, request):
        calls["snapshots"].append((allocation, block, owned))
        return SimpleNamespace(request=request)

    monkeypatch.setattr(request_binding, "snapshot", snapshot)
    return calls


def reader_backend():
    return SimpleNamespace(reader=SimpleNamespace(pin_block=lambda tag: "latest block"))


def test_a_cancellable_request_is_referenced_from_a_current_snapshot(reads):
    active = request()
    state = gmx_state(active)
    before = request_binding.cancellation_reference(reader_backend(), state)
    assert before.request is active
    assert reads["snapshots"] == [("trade", "latest block", state.owned[0])]
    retained = SimpleNamespace(request=active)
    assert (
        request_binding.cancellation_reference(reader_backend(), state, reference=retained)
        is retained
    )
    assert len(reads["snapshots"]) == 1


@pytest.mark.parametrize(
    "change", [{"evidence": "executed"}, {"order": None}, {"order": cancellable(False)}]
)
def test_settled_missing_or_not_yet_cancellable_requests_are_not_referenced(reads, change):
    reads.update(change)
    assert request_binding.cancellation_reference(reader_backend(), gmx_state(request())) is None


def test_only_teardown_cancels_and_only_the_exact_eligible_request(reads):
    active = request()
    with pytest.raises(ValueError, match="requires teardown"):
        request_binding.cancellation_reference(reader_backend(), gmx_state(active, "running"))
    other = SimpleNamespace(request=request("other"))
    with pytest.raises(ValueError, match="exact eligible retained"):
        request_binding.cancellation_reference(reader_backend(), gmx_state(active), reference=other)
    reads["pending"] = cancellable(False)
    with pytest.raises(ValueError, match="exact eligible retained"):
        request_binding.cancellation_reference(reader_backend(), gmx_state(active))
    reads["pending"] = None
    with pytest.raises(ValueError, match="exact eligible retained"):
        request_binding.cancellation_reference(reader_backend(), gmx_state(active))
