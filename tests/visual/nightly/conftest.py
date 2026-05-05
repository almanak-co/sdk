"""Runtime fixtures for nightly MarketSnapshot contract tests.

Ensures `uv run pytest tests/visual/nightly` is self-contained by auto-starting
managed gateway + Anvil when no gateway is already running.
"""

from __future__ import annotations

import os
import shutil
import tempfile
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

    # VIB-3996 / VIB-3835: ManagedGateway.start() acquires a flock on the
    # local SQLite DB resolved by `local_paths._resolve_db_path(strict=True)`.
    # That resolver requires either ALMANAK_STATE_DB, ALMANAK_STRATEGY_FOLDER,
    # or a strategy folder in cwd (config.json / strategy.py). The nightly
    # Market Data API Contract harness is not a strategy — it only queries
    # gateway market data — and runs inside `tests/visual/nightly/`, which
    # is intentionally not a strategy folder. Without an explicit ALMANAK_STATE_DB,
    # gateway startup raises LocalPathError before the test body ever runs.
    #
    # Provide a per-session tmp DB path. This preserves the folder-scoped
    # invariant (the flock still enforces 1:1 against this explicit path) and
    # is opt-in via env var — we don't weaken `_resolve_db_path(strict=True)`.
    nightly_db_dir = tempfile.mkdtemp(prefix="market_data_api_contract_")
    nightly_db_path = os.path.join(nightly_db_dir, "almanak_state.db")
    old_state_db = os.environ.get("ALMANAK_STATE_DB")
    old_host = os.environ.get("GATEWAY_HOST")
    old_port = os.environ.get("GATEWAY_PORT")
    os.environ["ALMANAK_STATE_DB"] = nightly_db_path

    gateway: ManagedGateway | None = None
    host_set = False
    port_set = False
    try:
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
            raise RuntimeError(f"Managed gateway failed readiness at {host}:{selected_port}")

        os.environ["GATEWAY_HOST"] = host
        host_set = True
        os.environ["GATEWAY_PORT"] = str(selected_port)
        port_set = True

        runtime = NightlyGatewayRuntime(
            host=host,
            port=selected_port,
            chain=chain,
            started_by_fixture=True,
        )
        yield runtime
    finally:
        if gateway is not None:
            try:
                gateway.stop(timeout=10.0)
            except Exception:  # noqa: BLE001 - cleanup must continue even if stop fails
                pass
        if host_set:
            if old_host is None:
                os.environ.pop("GATEWAY_HOST", None)
            else:
                os.environ["GATEWAY_HOST"] = old_host
        if port_set:
            if old_port is None:
                os.environ.pop("GATEWAY_PORT", None)
            else:
                os.environ["GATEWAY_PORT"] = old_port
        if old_state_db is None:
            os.environ.pop("ALMANAK_STATE_DB", None)
        else:
            os.environ["ALMANAK_STATE_DB"] = old_state_db
        shutil.rmtree(nightly_db_dir, ignore_errors=True)
