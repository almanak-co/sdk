"""Compose the existing GMX request and external-event bindings under one coordinator."""

from copy import copy

from pydantic import TypeAdapter

from almanak.connectors.across.settlement_types import Scan as AcrossScan
from almanak.connectors.composed.lifecycle import component_state
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.gmx_v2.liquidation_types import Scan as GmxScan
from almanak.connectors.gmx_v2.request_binding import Requests
from almanak.connectors.identifiers import ConnectorId

GMX = ConnectorId("gmx_v2")
ACROSS = ConnectorId("across")


def backend_view(backend):
    view = copy(backend)
    view.config = backend.config.component(GMX)
    return view


class ForkKeepers:
    """Fork tests only: the GMX keeper for accepted requests and the Across settler for
    transfers in flight, each given only its own component's records."""

    def __init__(self, forks, output):
        from almanak.connectors.across.settlement import ExternalEffects as AcrossEffects

        self.gmx = Requests.fork_keeper(forks, output)
        self.across = AcrossEffects.fork_keeper(forks, output)

    def parts(self, state):
        components = state.config.parameter_components
        if GMX in components:
            yield self.gmx, component_state(state, GMX)
        if ACROSS in components:
            yield self.across, component_state(state, ACROSS)

    def align(self, state, seconds: int) -> int:
        return max(
            (keeper.align(view, seconds) for keeper, view in self.parts(state)), default=seconds
        )

    def settle(self, state) -> tuple[dict, ...]:
        return tuple(row for keeper, view in self.parts(state) for row in keeper.settle(view))


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
        return ForkKeepers(forks, output)

    @staticmethod
    def fork_unsettled(state) -> bool:
        """Fork tests only: whether a keeper report or a transfer's settlement is owed."""
        return Requests.unresolved(state) or any(t.open for t in state.transfers)

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


class ScanPage:
    """Decodes a retained page as the scan type of the connector that read it."""

    adapter: TypeAdapter = TypeAdapter(GmxScan | AcrossScan)

    @classmethod
    def model_validate_json(cls, value):
        return cls.adapter.validate_json(value)


def scanning(state):
    """The configured components that read external pages, each with its binding."""
    result = {}
    for identity in state.config.parameter_components:
        binding = connector_from_wire({"connector": str(identity)}).external_effects
        if binding is not None:
            result[identity] = binding
    return result


def rotation(state, identities):
    """Start after the connector of the last committed page, so one component's
    steady pages never keep another's from being read."""
    order = list(identities)
    last = state.external_scans[-1].connector if state.external_scans else None
    if last in order:
        index = order.index(last) + 1
        order = order[index:] + order[:index]
    return order


def component_view(backend, identity):
    view = copy(backend)
    view.config = backend.config.component(identity)
    return view


class ComposedExternalEffects:
    """Each component reads, verifies and audits its own pages through its own view."""

    page_type = ScanPage

    @staticmethod
    def reconcile(backend, state, *, reference=None):
        bindings = scanning(state)
        if reference is not None:
            if reference.connector not in bindings:
                raise ValueError("external page requires its configured connector")
            order = [reference.connector]
        else:
            order = rotation(state, bindings)
        for identity in order:
            page = bindings[identity].reconcile(
                component_view(backend, identity),
                component_state(state, identity),
                reference=reference,
            )
            if page is not None:
                return page
        return None

    @staticmethod
    def audit(backend, state):
        for identity, binding in scanning(state).items():
            binding.audit(component_view(backend, identity), component_state(state, identity))
