"""Tests for gateway gRPC server."""

import asyncio
from unittest.mock import AsyncMock, patch

import grpc
import pytest
import pytest_asyncio
from grpc_health.v1 import health_pb2, health_pb2_grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.server import GatewayServer, _AIOHTTP_SHUTDOWN_GRACE_SECONDS


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
async def test_server_settings_from_environment(monkeypatch):
    """Server loads settings from environment variables."""
    monkeypatch.setenv("ALMANAK_GATEWAY_GRPC_PORT", "50055")
    monkeypatch.setenv("ALMANAK_GATEWAY_GRPC_MAX_WORKERS", "5")

    # Clear the lru_cache to pick up new env vars
    from almanak.gateway.core.settings import get_settings

    get_settings.cache_clear()

    settings = get_settings()
    assert settings.grpc_port == 50055
    assert settings.grpc_max_workers == 5

    # Clean up
    get_settings.cache_clear()


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
