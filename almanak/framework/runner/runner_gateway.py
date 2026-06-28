"""Gateway and lifecycle integration methods for StrategyRunner.

Extracted from strategy_runner.py for maintainability. Each function takes
``runner`` (a StrategyRunner instance) as its first argument and is called
via a thin delegation stub in StrategyRunner.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .runner_models import StrategyProtocol

# Use the original strategy_runner logger so existing log-capture tests and
# log-filtering rules continue to work after the extraction.
logger = logging.getLogger("almanak.framework.runner.strategy_runner")


# -------------------------------------------------------------------------
# Gateway client discovery
# -------------------------------------------------------------------------


def get_gateway_client(runner: Any) -> Any | None:
    """Get the gateway gRPC client from the execution orchestrator.

    Checks GatewayExecutionOrchestrator directly, gateway-backed
    MultiChainOrchestrator, and legacy per-chain executors.

    Returns:
        GatewayClient instance or None if not gateway-backed.
    """
    # Prefer explicitly set client
    if runner._gateway_client is not None:
        return runner._gateway_client

    from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator

    if isinstance(runner.execution_orchestrator, GatewayExecutionOrchestrator):
        return runner.execution_orchestrator._client

    # Gateway-backed MultiChainOrchestrator stores gateway client directly.
    # Read from the instance dict so a generic MagicMock orchestrator does not
    # fabricate a gateway client via dynamic attribute access.
    client = getattr(runner.execution_orchestrator, "__dict__", {}).get("_gateway_client")
    if client is not None:
        return client

    # Legacy multi-chain mode: check per-chain executors for a gateway client
    if runner._is_multi_chain and hasattr(runner.execution_orchestrator, "_executors"):
        for executor in runner.execution_orchestrator._executors.values():
            orch = getattr(executor, "orchestrator", None)
            if isinstance(orch, GatewayExecutionOrchestrator):
                return orch._client

    return None


# -------------------------------------------------------------------------
# Instance registration
# -------------------------------------------------------------------------


def _strategy_display_name(strategy: StrategyProtocol) -> str:
    config = getattr(strategy, "config", None)
    metadata = getattr(strategy, "STRATEGY_METADATA", None)
    metadata_name = getattr(metadata, "name", "")
    if not metadata_name and isinstance(metadata, dict):
        metadata_name = metadata.get("name") or metadata.get("canonical_name", "")

    return (
        getattr(strategy, "strategy_display_name", "")
        or getattr(config, "strategy_display_name", "")
        or getattr(strategy, "STRATEGY_NAME", "")
        or metadata_name
        or type(strategy).__name__
    )


def register_with_gateway(runner: Any, strategy: StrategyProtocol) -> None:
    """Register this strategy instance with the gateway's instance registry.

    Non-fatal: catches all exceptions so the strategy continues running
    even if registration fails.
    """
    client = runner._get_gateway_client()
    if client is None:
        return

    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.RegisterInstanceRequest(
            deployment_id=strategy.deployment_id,
            strategy_name=_strategy_display_name(strategy),
            template_name=type(strategy).__name__,
            chain=getattr(strategy, "chain", ""),
            protocol=getattr(strategy, "protocol", ""),
            wallet_address=getattr(strategy, "wallet_address", ""),
            config_json="",
            version="",
        )
        response = client.dashboard.RegisterStrategyInstance(request)
        if response.success:
            verb = "Re-registered" if response.already_existed else "Registered"
            logger.info(f"{verb} strategy instance with gateway: {strategy.deployment_id}")
        else:
            logger.warning(f"Failed to register with gateway: {response.error}")
    except Exception as e:
        logger.debug(f"Failed to register with gateway (non-fatal): {e}")


def deregister_from_gateway(runner: Any, deployment_id: str) -> None:
    """Mark this strategy instance as INACTIVE in the gateway registry.

    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return

    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.UpdateInstanceStatusRequest(
            deployment_id=deployment_id,
            status="INACTIVE",
            reason="Strategy runner stopped",
        )
        client.dashboard.UpdateStrategyInstanceStatus(request)
        logger.debug(f"Deregistered strategy instance from gateway: {deployment_id}")
    except Exception as e:
        logger.debug(f"Failed to deregister from gateway (non-fatal): {e}")


# -------------------------------------------------------------------------
# Gateway status / heartbeat
# -------------------------------------------------------------------------


def gateway_update_status(runner: Any, deployment_id: str, status: str) -> None:
    """Update instance status in the gateway registry (non-heartbeat).

    Used to flip status (e.g. INACTIVE on shutdown) so that
    strat list / strat status reflects the correct state.
    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return
    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.UpdateInstanceStatusRequest(
            deployment_id=deployment_id,
            status=status,
            heartbeat_only=False,
        )
        response = client.dashboard.UpdateStrategyInstanceStatus(request, timeout=5.0)
        if not response.success:
            logger.warning(
                "Gateway rejected status update to %s for %s: %s",
                status,
                deployment_id,
                response.error,
            )
    except Exception as e:
        logger.debug(f"Failed to update gateway status to {status} (non-fatal): {e}")


def gateway_heartbeat(runner: Any, deployment_id: str, positions: list | None = None) -> None:
    """Send a heartbeat to the gateway for this strategy instance.

    Args:
        runner: StrategyRunner instance.
        deployment_id: Strategy instance ID.
        positions: Optional list of StrategyPosition protos to cache in the dashboard.

    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return

    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.UpdateInstanceStatusRequest(
            deployment_id=deployment_id,
            heartbeat_only=True,
        )
        if positions:
            request.positions.extend(positions)
        client.dashboard.UpdateStrategyInstanceStatus(request)
    except Exception as e:
        logger.debug(f"Failed to send heartbeat to gateway (non-fatal): {e}")


_POSITION_OPTIONAL_FIELDS = (
    "entry_price",
    "current_price",
    "unrealized_pnl_usd",
    "unrealized_pnl_pct",
    "direction",
    "size_usd",
    "collateral_usd",
    "leverage",
)


def _position_value(value: Any) -> str:
    return str(value.value) if hasattr(value, "value") else str(value)


def _can_collect_position_snapshot(runner: Any, strategy: StrategyProtocol) -> bool:
    # VIB-5474 (TD-16): position snapshotting is OBSERVABILITY, not teardown
    # eligibility — keep the two decoupled. A strategy that opts OUT of framework
    # teardown (``supports_teardown() == False``, e.g. a V3-DEX LP the connector
    # cannot unwind, VIB-572) still holds positions the operator must monitor and
    # recover manually, so the dashboard MUST keep reporting them. Gating the
    # snapshot on the teardown opt-in would blind the operator to exactly the
    # positions they need to see. Teardown *eligibility* is decided solely at the
    # runner teardown trigger (``strategy_runner._check_teardown_requested``).
    #
    # The old ``hasattr(strategy, "get_open_positions")`` sniff is removed: it was
    # always True (the method is abstract on ``IntentStrategy``) so it never gated
    # anything, and ``collect_position_snapshot`` already degrades non-fatally if
    # positions cannot be read. Gate only on gateway-client presence.
    del strategy  # observability gate: intentionally independent of the strategy
    return runner._get_gateway_client() is not None


def _summary_positions(strategy: StrategyProtocol) -> Any | None:
    summary = strategy.get_open_positions()
    if summary is None or not hasattr(summary, "positions") or not summary.positions:
        return None
    return summary.positions


def _copy_position_details(proto: Any, position: Any) -> None:
    if not position.details:
        return
    for key, value in position.details.items():
        proto.details[str(key)] = str(value)


def _copy_optional_position_field(proto: Any, position: Any, field_name: str) -> None:
    value = getattr(position, field_name)
    if value is not None:
        setattr(proto, field_name, str(value))


def _copy_optional_position_fields(proto: Any, position: Any) -> None:
    if position.health_factor is not None:
        proto.health_factor = str(position.health_factor)
    _copy_position_details(proto, position)
    for field_name in _POSITION_OPTIONAL_FIELDS:
        _copy_optional_position_field(proto, position, field_name)


def _build_strategy_position_proto(gateway_pb2: Any, position: Any) -> Any:
    proto = gateway_pb2.StrategyPosition(
        position_type=_position_value(position.position_type),
        position_id=str(position.position_id),
        chain=_position_value(position.chain),
        protocol=str(position.protocol),
        value_usd=str(position.value_usd),
        liquidation_risk=bool(position.liquidation_risk),
    )
    _copy_optional_position_fields(proto, position)
    return proto


def collect_position_snapshot(runner: Any, strategy: StrategyProtocol) -> list | None:
    """Call strategy.get_open_positions() and convert to proto messages.

    Non-fatal: returns None on any error so heartbeat still fires.
    """
    if not _can_collect_position_snapshot(runner, strategy):
        return None

    try:
        positions = _summary_positions(strategy)
        if positions is None:
            return None

        from almanak.gateway.proto import gateway_pb2

        return [_build_strategy_position_proto(gateway_pb2, position) for position in positions]
    except Exception as e:
        logger.debug(f"Failed to collect position snapshot (non-fatal): {e}")
        return None


# -------------------------------------------------------------------------
# Lifecycle helpers
# -------------------------------------------------------------------------

_REPORTED_ALMANAK_VERSION: str | None

try:
    from importlib.metadata import PackageNotFoundError, version

    try:
        _REPORTED_ALMANAK_VERSION = version("almanak")
    except PackageNotFoundError:
        _REPORTED_ALMANAK_VERSION = None
except Exception:
    _REPORTED_ALMANAK_VERSION = None

# Process-lifetime cache: hosted V2 runs one strategy per process, so reporting
# once per deployment avoids rewriting the same package version on every RUNNING write.
_RUNNING_VERSION_REPORTED_DEPLOYMENT_IDS: set[str] = set()


def lifecycle_write_state(runner: Any, deployment_id: str, state: str, error_message: str | None = None) -> None:
    """Write deployment lifecycle state via gateway.

    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return
    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.WriteAgentStateRequest(
            deployment_id=deployment_id,
            state=state,
            error_message=error_message or "",
        )
        reported_version = _REPORTED_ALMANAK_VERSION
        reported_running_version = False
        if (
            state == "RUNNING"
            and reported_version is not None
            and deployment_id not in _RUNNING_VERSION_REPORTED_DEPLOYMENT_IDS
        ):
            request.running_almanak_version = reported_version
            reported_running_version = True
        client.lifecycle.WriteState(request)
        if reported_running_version:
            _RUNNING_VERSION_REPORTED_DEPLOYMENT_IDS.add(deployment_id)
    except Exception as e:
        logger.debug(f"Failed to write lifecycle state (non-fatal): {e}")


def lifecycle_heartbeat(runner: Any, deployment_id: str) -> None:
    """Send lifecycle heartbeat via gateway.

    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return
    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.HeartbeatRequest(deployment_id=deployment_id)
        client.lifecycle.Heartbeat(request)
    except Exception as e:
        logger.debug(f"Failed to send lifecycle heartbeat (non-fatal): {e}")


def lifecycle_poll_command(runner: Any, deployment_id: str) -> str | None:
    """Poll for pending command from LifecycleStore.

    Returns command string (STOP) or None.
    The command is acknowledged only after it is returned so that callers
    can apply side-effects before the ack.  If the process crashes between
    read and ack the command will be re-delivered on the next poll.
    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return None
    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.ReadAgentCommandRequest(deployment_id=deployment_id)
        response = client.lifecycle.ReadCommand(request)
        if response.found:
            command = response.command
            logger.info("Received lifecycle command: %s (from %s)", command, response.issued_by)
            # Acknowledge after reading so the command is re-delivered if we crash
            try:
                ack_request = gateway_pb2.AckAgentCommandRequest(command_id=response.command_id)
                client.lifecycle.AckCommand(ack_request)
            except Exception:
                logger.warning("Failed to ack lifecycle command %s (will be re-delivered)", response.command_id)
            return command
        return None
    except Exception as e:
        logger.debug("Failed to poll lifecycle command (non-fatal): %s", e)
        return None


def lifecycle_handle_stop(runner: Any, deployment_id: str, strategy: Any) -> None:
    """Handle STOP command: bridge into teardown.

    Shared by both the normal STOP path and the STOP-while-paused path.

    Local mode keeps writing to the strategy SQLite DB. Hosted mode routes the
    same request through the gateway so the strategy container never receives
    database credentials. The next iteration's ``_check_teardown_request``
    picks it up, runs teardown intents, then shuts the runner down.
    """
    from almanak.framework.teardown import (
        TeardownMode,
        TeardownRequest,
        get_teardown_state_manager_for_runtime,
    )

    runner._lifecycle_write_state(deployment_id, "STOPPING")

    try:
        manager = get_teardown_state_manager_for_runtime(gateway_client=runner._get_gateway_client())
        teardown_request = TeardownRequest(
            deployment_id=deployment_id,
            mode=TeardownMode.SOFT,
            reason="Lifecycle STOP command",
            requested_by="lifecycle",
        )
        manager.create_request(teardown_request)
        logger.info("Created teardown request for %s from STOP command", deployment_id)
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to create teardown request for %s: %s; hard-stopping", deployment_id, e)
        runner._shutdown_requested = True
    # Don't break -- let the next iteration pick up the teardown request
    # via _check_teardown_request(), which will execute teardown intents
    # and then call request_shutdown()


# -------------------------------------------------------------------------
# Public gateway integration setup/teardown
# -------------------------------------------------------------------------


def set_gateway_client(runner: Any, client: Any) -> None:
    """Explicitly set the gateway client for instance registration.

    Use this when the gateway client can't be discovered from the
    execution orchestrator (e.g. multi-chain mode).
    """
    runner._gateway_client = client


def setup_gateway_integration(runner: Any, strategy: StrategyProtocol) -> None:
    """Set up gateway dual-write and instance registration.

    Call this before run_iteration() when running outside run_loop()
    (e.g. --once mode) so that single-iteration runs also appear
    in the instance registry and emit gateway timeline events.
    """
    gateway_client = runner._get_gateway_client()
    if gateway_client is not None:
        from ..api.timeline import set_event_gateway_client

        set_event_gateway_client(gateway_client)
        logger.debug("Enabled gateway dual-write for timeline events")

    runner._register_with_gateway(strategy)


def teardown_gateway_integration(runner: Any, deployment_id: str) -> None:
    """Mark instance as INACTIVE and clear gateway dual-write.

    Call this after run_iteration() when running outside run_loop().
    """
    runner._deregister_from_gateway(deployment_id)
