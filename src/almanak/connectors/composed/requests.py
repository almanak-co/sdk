"""Compose the existing GMX request and external-event bindings under one coordinator."""

from copy import copy

from almanak.connectors.composed.lifecycle import component_state
from almanak.connectors.gmx_v2.liquidation_binding import ExternalEffects
from almanak.connectors.gmx_v2.request_binding import Requests
from almanak.connectors.identifiers import ConnectorId

GMX = ConnectorId("gmx_v2")


def backend_view(backend):
    view = copy(backend)
    view.config = backend.config.component(GMX)
    return view


class ComposedRequests:
    unresolved = staticmethod(Requests.unresolved)
    project = staticmethod(Requests.project)
    merge = staticmethod(Requests.merge)

    @staticmethod
    def is_cancellation(state, operation):
        return Requests.unresolved(state) and Requests.is_cancellation(state, operation)

    @staticmethod
    def reconcile(backend, state):
        return Requests.reconcile(backend_view(backend), component_state(state, GMX))

    @staticmethod
    def fork_keeper(forks, output):
        keeper = Requests.fork_keeper(forks, output)
        keeper.project = lambda state: (
            component_state(state, GMX) if GMX in state.config.parameter_components else None
        )
        return keeper

    @staticmethod
    def cancellation_plan(backend, state, attempt_id, *, reference=None):
        """GMX reads its request and snapshot through its own configuration; the composed
        backend prepares the cancellation, so it is checked against the permission manifest
        and inventory of the configuration that installed them."""
        before = Requests.cancellation_reference(
            backend_view(backend), component_state(state, GMX), reference=reference
        )
        if before is None:
            return None
        return backend.prepare(state, state.decision.intent, before, attempt_id)

    @staticmethod
    def audit(backend, state):
        if GMX in state.config.parameter_components:
            Requests.audit(backend_view(backend), component_state(state, GMX))


class ComposedExternalEffects:
    page_type = ExternalEffects.page_type

    @staticmethod
    def reconcile(backend, state, *, reference=None):
        if GMX not in state.config.parameter_components:
            if reference is not None:
                raise ValueError("external page requires its configured connector")
            return None
        return ExternalEffects.reconcile(
            backend_view(backend), component_state(state, GMX), reference=reference
        )

    @staticmethod
    def audit(backend, state):
        if GMX in state.config.parameter_components:
            ExternalEffects.audit(backend_view(backend), component_state(state, GMX))
