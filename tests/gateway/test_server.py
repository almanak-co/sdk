"""Tests for gateway gRPC server."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest
import pytest_asyncio
from grpc_health.v1 import health_pb2, health_pb2_grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.server import _AIOHTTP_SHUTDOWN_GRACE_SECONDS, GatewayServer


@pytest_asyncio.fixture
async def gateway_server():
    """Start a gateway server for testing."""
    # Use different port for tests to avoid conflicts
    # Disable metrics and audit to avoid port conflicts and log spam
    # Enable allow_insecure for testing without auth token
    settings = GatewaySettings(grpc_port=50052, metrics_enabled=False, audit_enabled=False, allow_insecure=True)
    server = GatewayServer(settings)
    await server.start()
    yield server
    await server.stop()


@pytest.mark.asyncio
async def test_server_starts_and_stops():
    """Server can start and stop cleanly."""
    settings = GatewaySettings(grpc_port=50053, metrics_enabled=False, audit_enabled=False, allow_insecure=True)
    server = GatewayServer(settings)

    await server.start()
    assert server.server is not None

    await server.stop()
    # Server should be stopped (but server object still exists)


@pytest.mark.asyncio
async def test_server_health_check(gateway_server):
    """Server responds to health checks."""
    # Give server a moment to fully start
    await asyncio.sleep(0.1)

    # Use async gRPC channel to avoid blocking the event loop
    channel = grpc.aio.insecure_channel("localhost:50052")
    stub = health_pb2_grpc.HealthStub(channel)

    response = await stub.Check(health_pb2.HealthCheckRequest(service=""))

    assert response.status == health_pb2.HealthCheckResponse.SERVING
    await channel.close()


@pytest.mark.asyncio
async def test_server_health_check_after_stop():
    """Server health check fails after stop."""
    settings = GatewaySettings(grpc_port=50054, metrics_enabled=False, audit_enabled=False, allow_insecure=True)
    server = GatewayServer(settings)
    await server.start()
    await asyncio.sleep(0.1)

    # Stop the server
    await server.stop()
    await asyncio.sleep(0.1)

    # Use async gRPC channel to avoid blocking the event loop
    channel = grpc.aio.insecure_channel("localhost:50054")
    stub = health_pb2_grpc.HealthStub(channel)

    with pytest.raises(grpc.aio.AioRpcError):
        await stub.Check(health_pb2.HealthCheckRequest(service=""))

    await channel.close()


@pytest.mark.asyncio
async def test_allow_insecure_disables_auth_even_with_configured_token():
    """allow_insecure=True must win for local test harnesses on test networks."""
    settings = GatewaySettings(
        grpc_port=50059,
        metrics_enabled=False,
        audit_enabled=False,
        allow_insecure=True,
        auth_token="ambient-token",  # noqa: S106 - test fixture, not a real credential
        network="anvil",
    )
    server = GatewayServer(settings)

    # Patch AuthInterceptor so we can prove it was never constructed.
    # Health.Check is auth-exempt, so asserting the interceptor was not
    # instantiated is the reliable way to prove auth was skipped.
    # Patched at the helper module where it is now imported (Phase 8.3d).
    with patch("almanak.gateway._server_start_helpers.AuthInterceptor") as auth_interceptor_cls:
        await server.start()
        await asyncio.sleep(0.1)
        assert auth_interceptor_cls.call_count == 0, (
            "AuthInterceptor should not be constructed when allow_insecure=True"
        )

        channel = grpc.aio.insecure_channel("localhost:50059")
        stub = health_pb2_grpc.HealthStub(channel)
        response = await stub.Check(health_pb2.HealthCheckRequest(service=""))
        assert response.status == health_pb2.HealthCheckResponse.SERVING
        await channel.close()

    await server.stop()


@pytest.mark.asyncio
async def test_allow_insecure_with_auth_token_on_mainnet_is_rejected():
    """Contradictory config on a production network must hard-fail at start()."""
    settings = GatewaySettings(
        grpc_port=50060,
        metrics_enabled=False,
        audit_enabled=False,
        allow_insecure=True,
        auth_token="ambient-token",  # noqa: S106 - test fixture, not a real credential
        network="mainnet",
    )
    server = GatewayServer(settings)

    with pytest.raises(RuntimeError, match="conflicting configuration"):
        await server.start()


@pytest.mark.asyncio
async def test_heartbeat_ttl_task_lifecycle():
    """GatewayServer starts the heartbeat TTL task on start() and cancels it on stop() (VIB-1280)."""
    settings = GatewaySettings(grpc_port=50056, metrics_enabled=False, audit_enabled=False, allow_insecure=True)
    server = GatewayServer(settings)

    assert server._heartbeat_ttl_task is None

    await server.start()
    assert server._heartbeat_ttl_task is not None
    assert not server._heartbeat_ttl_task.done()

    await server.stop()
    assert server._heartbeat_ttl_task.done()


@pytest.mark.asyncio
async def test_stop_awaits_aiohttp_grace_period():
    """stop() sleeps for _AIOHTTP_SHUTDOWN_GRACE_SECONDS to let aiohttp connectors drain (VIB-1832)."""
    settings = GatewaySettings(grpc_port=50057, metrics_enabled=False, audit_enabled=False, allow_insecure=True)
    server = GatewayServer(settings)
    await server.start()

    original_sleep = asyncio.sleep
    sleep_calls: list[float] = []

    async def tracking_sleep(delay, *args, **kwargs):
        sleep_calls.append(delay)
        await original_sleep(delay, *args, **kwargs)

    with patch("almanak.gateway.server.asyncio.sleep", side_effect=tracking_sleep):
        await server.stop()

    assert _AIOHTTP_SHUTDOWN_GRACE_SECONDS in sleep_calls, (
        f"Expected asyncio.sleep({_AIOHTTP_SHUTDOWN_GRACE_SECONDS}) during stop(), got calls: {sleep_calls}"
    )


@pytest.mark.asyncio
async def test_serving_deferred_until_after_warmup():
    """SERVING health status is set only AFTER warmup completes (VIB-2413).

    Previously, the gateway marked itself SERVING before warmup, causing
    clients to call balance/price endpoints before providers were initialized.
    """
    settings = GatewaySettings(
        grpc_port=50058,
        metrics_enabled=False,
        audit_enabled=False,
        allow_insecure=True,
        chains=["arbitrum"],
    )
    server = GatewayServer(settings)

    # Track the order of operations: warmup vs SERVING
    call_order: list[str] = []

    original_set = server._health_servicer.set

    async def tracking_health_set(service, status):
        if status == health_pb2.HealthCheckResponse.NOT_SERVING:
            call_order.append("NOT_SERVING")
        elif status == health_pb2.HealthCheckResponse.SERVING:
            call_order.append("SERVING")
        await original_set(service, status)

    async def tracking_warmup(*args, **kwargs):
        call_order.append("warmup_start")
        call_order.append("warmup_end")

    async def tracking_prewarm(*args, **kwargs):
        call_order.append("prewarm_start")
        call_order.append("prewarm_end")

    server._health_servicer.set = tracking_health_set

    with (
        patch(
            "almanak.gateway.services.market_service.MarketServiceServicer.warmup",
            new=AsyncMock(side_effect=tracking_warmup),
        ),
        patch.object(server, "_prewarm_chains", side_effect=tracking_prewarm),
    ):
        await server.start()

    # NOT_SERVING must be set before warmup; SERVING must come after
    assert "NOT_SERVING" in call_order, f"NOT_SERVING was never set: {call_order}"
    assert "SERVING" in call_order, f"SERVING was never set: {call_order}"
    serving_idx = call_order.index("SERVING")
    not_serving_idx = call_order.index("NOT_SERVING")
    assert not_serving_idx < serving_idx, (
        f"NOT_SERVING must precede SERVING: {call_order}"
    )
    assert "warmup_end" in call_order, f"Warmup was not executed: {call_order}"
    assert "prewarm_end" in call_order, f"Prewarm was not executed: {call_order}"
    assert call_order.index("warmup_end") < serving_idx, (
        f"SERVING set before warmup completed: {call_order}"
    )
    assert call_order.index("prewarm_end") < serving_idx, (
        f"SERVING set before prewarm completed: {call_order}"
    )

    await server.stop()


# ---------------------------------------------------------------------------
# _announce_initializing — ALM-2732 follow-up
#
# Both pods of a hosted V2 agent (strategy + dashboard) ship the same gateway
# image with the same AGENT_ID. Only the strategy-pod gateway is configured
# with ``lifecycle_writer=True`` and should write INITIALIZING; the dashboard
# pod must stay read-only for lifecycle state so it doesn't clobber RUNNING.
# ---------------------------------------------------------------------------


def _make_state_row(state: str):
    """Build a minimal stand-in for an ``AgentState`` row returned by read_state."""
    row = MagicMock()
    row.state = state
    return row


@pytest.mark.asyncio
async def test_announce_initializing_writes_when_hosted_and_writer(monkeypatch):
    """Strategy-pod gateway writes INITIALIZING on first boot (no existing row)."""
    monkeypatch.setenv("AGENT_ID", "agent-abc")
    settings = GatewaySettings(metrics_enabled=False, audit_enabled=False, lifecycle_writer=True)
    server = GatewayServer(settings)
    store = MagicMock()
    store.read_state.return_value = None  # fresh deploy

    await server._announce_initializing(store)

    store.write_state.assert_called_once_with("agent-abc", "INITIALIZING")


@pytest.mark.asyncio
async def test_announce_initializing_writes_when_row_is_platform_state(monkeypatch):
    """V2_DEPLOYING / V2_PREPARING are platform-owned — gateway advances them."""
    monkeypatch.setenv("AGENT_ID", "agent-abc")
    settings = GatewaySettings(metrics_enabled=False, audit_enabled=False, lifecycle_writer=True)
    server = GatewayServer(settings)
    store = MagicMock()
    store.read_state.return_value = _make_state_row("V2_DEPLOYING")

    await server._announce_initializing(store)

    store.write_state.assert_called_once_with("agent-abc", "INITIALIZING")


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_state", ["RUNNING", "STOPPING", "TEARING_DOWN", "TERMINATED", "ERROR", "INITIALIZING"])
async def test_announce_initializing_skips_when_sdk_owned_state(monkeypatch, existing_state):
    """Gateway sidecar restart while strategy is healthy must NOT regress the row.

    Codex P2 regression guard: a K8s native sidecar can restart on its own while
    the main container keeps running, and the SDK runner only writes RUNNING at
    process startup. Without this skip, a sidecar-only restart would clobber
    RUNNING → INITIALIZING and the reconciler would escalate to V2_DEPLOY_FAILED.
    """
    monkeypatch.setenv("AGENT_ID", "agent-abc")
    settings = GatewaySettings(metrics_enabled=False, audit_enabled=False, lifecycle_writer=True)
    server = GatewayServer(settings)
    store = MagicMock()
    store.read_state.return_value = _make_state_row(existing_state)

    await server._announce_initializing(store)

    store.write_state.assert_not_called()


@pytest.mark.asyncio
async def test_announce_initializing_noop_when_not_writer(monkeypatch):
    """Dashboard-pod gateway must not write — flag defaults to False."""
    monkeypatch.setenv("AGENT_ID", "agent-abc")
    settings = GatewaySettings(metrics_enabled=False, audit_enabled=False)
    server = GatewayServer(settings)
    store = MagicMock()

    await server._announce_initializing(store)

    store.read_state.assert_not_called()
    store.write_state.assert_not_called()


@pytest.mark.asyncio
async def test_announce_initializing_noop_when_local(monkeypatch):
    """Local mode (AGENT_ID unset) never writes — local SDK owns its own state."""
    monkeypatch.delenv("AGENT_ID", raising=False)
    settings = GatewaySettings(metrics_enabled=False, audit_enabled=False, lifecycle_writer=True)
    server = GatewayServer(settings)
    store = MagicMock()

    await server._announce_initializing(store)

    store.read_state.assert_not_called()
    store.write_state.assert_not_called()


@pytest.mark.asyncio
async def test_announce_initializing_swallows_write_errors(monkeypatch):
    """A failed write must not raise — the SDK's later RUNNING write supersedes."""
    monkeypatch.setenv("AGENT_ID", "agent-abc")
    settings = GatewaySettings(metrics_enabled=False, audit_enabled=False, lifecycle_writer=True)
    server = GatewayServer(settings)
    store = MagicMock()
    store.read_state.return_value = None
    store.write_state.side_effect = RuntimeError("db down")

    await server._announce_initializing(store)  # must not raise

    store.write_state.assert_called_once_with("agent-abc", "INITIALIZING")


@pytest.mark.asyncio
async def test_announce_initializing_swallows_read_errors(monkeypatch):
    """A read failure short-circuits cleanly without raising."""
    monkeypatch.setenv("AGENT_ID", "agent-abc")
    settings = GatewaySettings(metrics_enabled=False, audit_enabled=False, lifecycle_writer=True)
    server = GatewayServer(settings)
    store = MagicMock()
    store.read_state.side_effect = RuntimeError("db down")

    await server._announce_initializing(store)  # must not raise

    store.write_state.assert_not_called()
