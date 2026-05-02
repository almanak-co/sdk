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

    # Gateway-backed MultiChainOrchestrator stores gateway client directly
    if hasattr(runner.execution_orchestrator, "_gateway_client"):
        client = runner.execution_orchestrator._gateway_client
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
            strategy_id=strategy.strategy_id,
            strategy_name=getattr(
                strategy,
                "strategy_display_name",
                getattr(getattr(strategy, "config", None), "strategy_display_name", strategy.strategy_id),
            ),
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
            logger.info(f"{verb} strategy instance with gateway: {strategy.strategy_id}")
        else:
            logger.warning(f"Failed to register with gateway: {response.error}")
    except Exception as e:
        logger.debug(f"Failed to register with gateway (non-fatal): {e}")


def deregister_from_gateway(runner: Any, strategy_id: str) -> None:
    """Mark this strategy instance as INACTIVE in the gateway registry.

    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return

    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.UpdateInstanceStatusRequest(
            strategy_id=strategy_id,
            status="INACTIVE",
            reason="Strategy runner stopped",
        )
        client.dashboard.UpdateStrategyInstanceStatus(request)
        logger.debug(f"Deregistered strategy instance from gateway: {strategy_id}")
    except Exception as e:
        logger.debug(f"Failed to deregister from gateway (non-fatal): {e}")


# -------------------------------------------------------------------------
# Gateway status / heartbeat
# -------------------------------------------------------------------------


def gateway_update_status(runner: Any, strategy_id: str, status: str) -> None:
    """Update instance status in the gateway registry (non-heartbeat).

    Used to flip status to PAUSED/RUNNING on pause/resume so that
    strat list / strat status reflects the correct state.
    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return
    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.UpdateInstanceStatusRequest(
            strategy_id=strategy_id,
            status=status,
            heartbeat_only=False,
        )
        response = client.dashboard.UpdateStrategyInstanceStatus(request, timeout=5.0)
        if not response.success:
            logger.warning(
                "Gateway rejected status update to %s for %s: %s",
                status,
                strategy_id,
                response.error,
            )
    except Exception as e:
        logger.debug(f"Failed to update gateway status to {status} (non-fatal): {e}")


def gateway_heartbeat(runner: Any, strategy_id: str, positions: list | None = None) -> None:
    """Send a heartbeat to the gateway for this strategy instance.

    Args:
        runner: StrategyRunner instance.
        strategy_id: Strategy instance ID.
        positions: Optional list of StrategyPosition protos to cache in the dashboard.

    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return

    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.UpdateInstanceStatusRequest(
            strategy_id=strategy_id,
            heartbeat_only=True,
        )
        if positions:
            request.positions.extend(positions)
        client.dashboard.UpdateStrategyInstanceStatus(request)
    except Exception as e:
        logger.debug(f"Failed to send heartbeat to gateway (non-fatal): {e}")


def collect_position_snapshot(runner: Any, strategy: StrategyProtocol) -> list | None:
    """Call strategy.get_open_positions() and convert to proto messages.

    Non-fatal: returns None on any error so heartbeat still fires.
    """
    if runner._get_gateway_client() is None:
        return None
    if not hasattr(strategy, "get_open_positions"):
        return None

    try:
        summary = strategy.get_open_positions()
        if summary is None or not hasattr(summary, "positions") or not summary.positions:
            return None

        from almanak.gateway.proto import gateway_pb2

        protos = []
        for pos in summary.positions:
            sp = gateway_pb2.StrategyPosition(
                position_type=str(pos.position_type.value)
                if hasattr(pos.position_type, "value")
                else str(pos.position_type),
                position_id=str(pos.position_id),
                chain=str(pos.chain.value) if hasattr(pos.chain, "value") else str(pos.chain),
                protocol=str(pos.protocol),
                value_usd=str(pos.value_usd),
                liquidation_risk=bool(pos.liquidation_risk),
            )
            if pos.health_factor is not None:
                sp.health_factor = str(pos.health_factor)
            if pos.details:
                for k, v in pos.details.items():
                    sp.details[str(k)] = str(v)
            # Optional monitoring fields
            if pos.entry_price is not None:
                sp.entry_price = str(pos.entry_price)
            if pos.current_price is not None:
                sp.current_price = str(pos.current_price)
            if pos.unrealized_pnl_usd is not None:
                sp.unrealized_pnl_usd = str(pos.unrealized_pnl_usd)
            if pos.unrealized_pnl_pct is not None:
                sp.unrealized_pnl_pct = str(pos.unrealized_pnl_pct)
            if pos.direction is not None:
                sp.direction = str(pos.direction)
            if pos.size_usd is not None:
                sp.size_usd = str(pos.size_usd)
            if pos.collateral_usd is not None:
                sp.collateral_usd = str(pos.collateral_usd)
            if pos.leverage is not None:
                sp.leverage = str(pos.leverage)
            protos.append(sp)
        return protos
    except Exception as e:
        logger.debug(f"Failed to collect position snapshot (non-fatal): {e}")
        return None


# -------------------------------------------------------------------------
# Lifecycle helpers
# -------------------------------------------------------------------------


def lifecycle_write_state(runner: Any, agent_id: str, state: str, error_message: str | None = None) -> None:
    """Write agent state to LifecycleStore via gateway.

    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return
    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.WriteAgentStateRequest(
            agent_id=agent_id,
            state=state,
            error_message=error_message or "",
        )
        client.lifecycle.WriteState(request)
    except Exception as e:
        logger.debug(f"Failed to write lifecycle state (non-fatal): {e}")


def lifecycle_heartbeat(runner: Any, agent_id: str) -> None:
    """Send lifecycle heartbeat via gateway.

    Non-fatal: catches all exceptions.
    """
    client = runner._get_gateway_client()
    if client is None:
        return
    try:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.HeartbeatRequest(agent_id=agent_id)
        client.lifecycle.Heartbeat(request)
    except Exception as e:
        logger.debug(f"Failed to send lifecycle heartbeat (non-fatal): {e}")


def lifecycle_poll_command(runner: Any, agent_id: str) -> str | None:
    """Poll for pending command from LifecycleStore.

    Returns command string (PAUSE, RESUME, STOP) or None.
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

        request = gateway_pb2.ReadAgentCommandRequest(agent_id=agent_id)
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


def lifecycle_handle_stop(runner: Any, strategy_id: str, strategy: Any) -> None:
    """Handle STOP command: bridge into teardown or hard-stop.

    Shared by both the normal STOP path and the STOP-while-paused path.

    Local mode: writes a teardown request to the SQLite approval channel
    so the next iteration's ``_check_teardown_requested()`` picks it up,
    runs teardown intents, then shuts the runner down.

    Hosted mode (``AGENT_ID`` set): the SQLite approval channel doesn't
    exist (gateway/Postgres owns the teardown channel; the runner-side
    gateway lookup is the planned path under VIB-3777). We can't write a
    cross-process request, so STOP becomes a direct shutdown signal:
    write ``STOPPING`` lifecycle state, set ``_shutdown_requested`` so the
    run loop exits cleanly on its next sleep wake-up. Position unwind in
    hosted mode is a separate channel — operators issue teardown via the
    dashboard's hosted teardown endpoint, not via STOP. STOP semantics
    here are "stop the runner loop", not "unwind positions".
    """
    from almanak.framework.deployment import is_hosted

    runner._lifecycle_write_state(strategy_id, "STOPPING")

    if is_hosted():
        logger.info(
            "Hosted-mode STOP for %s: requesting clean runner shutdown. "
            "Position unwind (if any) is a separate hosted teardown channel "
            "(VIB-3777) — STOP does not implicitly unwind positions in hosted mode.",
            strategy_id,
        )
        runner._shutdown_requested = True
        return

    from almanak.framework.teardown import TeardownMode, TeardownRequest, get_teardown_state_manager

    try:
        manager = get_teardown_state_manager()
        teardown_request = TeardownRequest(
            strategy_id=strategy_id,
            mode=TeardownMode.SOFT,
            reason="Lifecycle STOP command",
            requested_by="lifecycle",
        )
        manager.create_request(teardown_request)
        logger.info("Created teardown request for %s from STOP command", strategy_id)
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to create teardown request for %s: %s; hard-stopping", strategy_id, e)
        runner._shutdown_requested = True
    # Don't break -- let the next iteration pick up the teardown request
    # via _check_teardown_requested(), which will execute teardown intents
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


def teardown_gateway_integration(runner: Any, strategy_id: str) -> None:
    """Mark instance as INACTIVE and clear gateway dual-write.

    Call this after run_iteration() when running outside run_loop().
    """
    runner._deregister_from_gateway(strategy_id)
