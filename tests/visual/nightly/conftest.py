"""Runtime fixtures for nightly MarketSnapshot contract tests.

Ensures `uv run pytest tests/visual/nightly` is self-contained by auto-starting
managed gateway + Anvil when no gateway is already running.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from dataclasses import dataclass

import pytest

from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.managed import ManagedGateway, find_available_gateway_port


@dataclass(frozen=True)
class NightlyGatewayRuntime:
    """Resolved gateway runtime details for nightly tests."""

    host: str
    port: int
    chain: str
    started_by_fixture: bool


def _is_gateway_ready(host: str, port: int, timeout: float = 5.0) -> bool:
    """Return True when gateway is reachable and healthy."""
    client = GatewayClient(GatewayClientConfig(host=host, port=port, timeout=timeout))
    try:
        client.connect()
        return client.health_check()
    except Exception:  # noqa: BLE001
        return False
    finally:
        client.disconnect()


def _wait_for_gateway_ready(host: str, port: int, wait_timeout: float) -> bool:
    """Wait until gateway reports ready status."""
    client = GatewayClient(GatewayClientConfig(host=host, port=port, timeout=10.0))
    try:
        client.connect()
        return client.wait_for_ready(timeout=wait_timeout, interval=2.0)
    finally:
        client.disconnect()


@pytest.fixture(scope="session", autouse=True)
def nightly_gateway_runtime() -> Generator[NightlyGatewayRuntime, None, None]:
    """Provide a ready gateway endpoint for nightly tests.

    Behavior:
    - Reuse an already-running gateway at GATEWAY_HOST/GATEWAY_PORT when healthy.
    - Otherwise start ManagedGateway in `anvil` mode for MARKET_CONTRACT_CHAIN.
    """

    chain = os.getenv("MARKET_CONTRACT_CHAIN", "arbitrum").lower()
    host = os.getenv("GATEWAY_HOST", "127.0.0.1")
    default_port = int(os.getenv("GATEWAY_PORT", "50051"))

    if _is_gateway_ready(host, default_port):
        runtime = NightlyGatewayRuntime(
            host=host,
            port=default_port,
            chain=chain,
            started_by_fixture=False,
        )
        yield runtime
        return

    selected_port = find_available_gateway_port(host=host, preferred_port=default_port, max_attempts=50)
    startup_timeout = float(os.getenv("MARKET_CONTRACT_GATEWAY_STARTUP_TIMEOUT", "120"))
    ready_timeout = float(os.getenv("MARKET_CONTRACT_GATEWAY_READY_TIMEOUT", "180"))

    settings = GatewaySettings(
        grpc_host=host,
        grpc_port=selected_port,
        network="anvil",
        chains=[chain],
        metrics_enabled=False,
        audit_enabled=False,
        allow_insecure=True,
        log_level="warning",
    )
    gateway = ManagedGateway(settings=settings, anvil_chains=[chain])
    gateway.start(timeout=startup_timeout)

    if not _wait_for_gateway_ready(host=host, port=selected_port, wait_timeout=ready_timeout):
        gateway.stop(timeout=10.0)
        raise RuntimeError(f"Managed gateway failed readiness at {host}:{selected_port}")

    old_host = os.environ.get("GATEWAY_HOST")
    old_port = os.environ.get("GATEWAY_PORT")
    os.environ["GATEWAY_HOST"] = host
    os.environ["GATEWAY_PORT"] = str(selected_port)

    try:
        runtime = NightlyGatewayRuntime(
            host=host,
            port=selected_port,
            chain=chain,
            started_by_fixture=True,
        )
        yield runtime
    finally:
        gateway.stop(timeout=10.0)
        if old_host is None:
            os.environ.pop("GATEWAY_HOST", None)
        else:
            os.environ["GATEWAY_HOST"] = old_host
        if old_port is None:
            os.environ.pop("GATEWAY_PORT", None)
        else:
            os.environ["GATEWAY_PORT"] = old_port
