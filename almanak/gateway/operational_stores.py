"""Explicit operational-store ownership for one gateway server."""

import logging
from dataclasses import dataclass
from typing import Any

from almanak.core.lifecycle import LifecycleCommand, LifecycleState
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.lifecycle import create_lifecycle_store
from almanak.gateway.lifecycle.store import AgentCommand, AgentState, LifecycleStore
from almanak.gateway.registry.store import InstanceRegistry
from almanak.gateway.store_lifetime import StoreLifetime, store_operation
from almanak.gateway.timeline.store import TimelineStore

logger = logging.getLogger(__name__)


def timeline_store_options(settings: GatewaySettings) -> dict[str, Any]:
    """Resolve the storage backend and hosted history scope once at boot."""
    if settings.database_url:
        from almanak.framework.deployment.mode import deployment_id, is_hosted

        return {
            "database_url": settings.database_url,
            "scope_deployment_id": deployment_id() if is_hosted() else None,
            "startup_load_limit": settings.timeline_startup_load_limit,
        }
    path = settings.timeline_db_path
    return {"db_path": path if path and path.strip() else settings.gateway_db_path}


class OwnedLifecycleStore:
    """Retain the plugin protocol while preventing use after its owner's close."""

    def __init__(self, backend: LifecycleStore) -> None:
        self._backend = backend
        self._lifetime = StoreLifetime()

    @store_operation
    def initialize(self) -> None:
        self._backend.initialize()

    def close(self) -> None:
        self._lifetime.close(self._backend.close)

    @store_operation
    def write_state(
        self,
        deployment_id: str,
        state: LifecycleState,
        error_message: str | None = None,
        running_almanak_version: str | None = None,
    ) -> None:
        self._backend.write_state(deployment_id, state, error_message, running_almanak_version)

    @store_operation
    def read_state(self, deployment_id: str) -> AgentState | None:
        return self._backend.read_state(deployment_id)

    @store_operation
    def heartbeat(self, deployment_id: str) -> None:
        self._backend.heartbeat(deployment_id)

    @store_operation
    def read_pending_command(self, deployment_id: str) -> AgentCommand | None:
        return self._backend.read_pending_command(deployment_id)

    @store_operation
    def ack_command(self, command_id: int) -> None:
        self._backend.ack_command(command_id)

    @store_operation
    def write_command(self, deployment_id: str, command: LifecycleCommand, issued_by: str) -> None:
        self._backend.write_command(deployment_id, command, issued_by)


@dataclass
class OperationalStores:
    registry: InstanceRegistry
    timeline: TimelineStore
    lifecycle: OwnedLifecycleStore

    @classmethod
    def create(cls, settings: GatewaySettings) -> "OperationalStores":
        registry = InstanceRegistry(db_path=settings.gateway_db_path)
        timeline = TimelineStore(**timeline_store_options(settings))
        lifecycle = OwnedLifecycleStore(
            create_lifecycle_store(
                database_url=settings.database_url,
                sqlite_path=settings.gateway_db_path,
            )
        )
        owner = cls(registry, timeline, lifecycle)
        try:
            timeline.initialize()
            registry.initialize()
            stale_count = registry.reconcile_stale_on_startup()
            if stale_count:
                logger.warning("Gateway startup: reconciled %d ghost RUNNING instance(s) -> STALE", stale_count)
            lifecycle.initialize()
        except BaseException as initialization_error:
            try:
                owner.close()
            except BaseException as close_error:
                raise BaseExceptionGroup(
                    "Gateway operational-store startup and cleanup failed",
                    [initialization_error, close_error],
                ) from initialization_error
            raise
        return owner

    def close(self) -> None:
        failures: list[tuple[str, Exception]] = []
        for name, resource in (("lifecycle", self.lifecycle), ("registry", self.registry), ("timeline", self.timeline)):
            try:
                resource.close()
            except Exception as exc:
                logger.exception("Failed to close gateway-owned %s store", name)
                failures.append((name, exc))
        if failures:
            raise ExceptionGroup(
                "Gateway operational-store close failed",
                [exc for _, exc in failures],
            )
