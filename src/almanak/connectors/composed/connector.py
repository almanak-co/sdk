"""A deployment binding composes reviewed protocols without inventing a connector identity."""

from almanak.connectors.composed.codec import codec
from almanak.connectors.composed.execution import ComposedExecution
from almanak.connectors.composed.fork import ComposedFork
from almanak.connectors.composed.lifecycle import ComposedLifecycle
from almanak.connectors.composed.permissions import ComposedPermissions
from almanak.connectors.composed.requests import ComposedExternalEffects, ComposedRequests
from almanak.connectors.interfaces import Connector
from almanak.connectors.target_routing import connector_for_target


def effects(outcome):
    binding = connector_for_target(outcome.after.target)
    if binding.execution_effects is None:
        raise ValueError("connector has no verified execution cash effects")
    return binding.execution_effects(outcome)


class ComposedProjection:
    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.composed.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.composed.dashboard_history import history_row

        return history_row(state, observed)

    @staticmethod
    def report(state):
        from almanak.connectors.composed.reporting import report

        return report(state)


def connector():
    return Connector(
        codec=codec(),
        lifecycle=ComposedLifecycle(),
        execution=ComposedExecution(),
        permissions=ComposedPermissions(),
        projection=ComposedProjection(),
        fork=ComposedFork(),
        execution_effects=effects,
        requests=ComposedRequests(),
        external_effects=ComposedExternalEffects(),
        economic="almanak.connectors.composed.economic_binding:ComposedEconomic",
    )
