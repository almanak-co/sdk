"""Regression pin: unit tests stay hermetic against a gateway on the default port.

A standalone gateway (``almanak gateway --standalone``) listens on
``localhost:50051`` — the same address ``GatewayClientConfig.from_env()``
resolves when nothing is configured. Before the unit-suite hermeticity guard
(``tests/unit/conftest.py::_hermetic_ambient_gateway``), a ``MarketSnapshot``
constructed WITHOUT a rate monitor or gateway client resolved real lending
rates through that ambient gateway: the lazy ``RateMonitor`` lane fell back
to the default-port ``get_gateway_client()`` singleton, flipping
"no monitor -> raises" tests to DID NOT RAISE whenever anything was
listening on the developer's machine.

This file binds a REAL in-process gRPC server that serves
``RateHistoryService.GetLendingRateCurrent`` on the default port, then
asserts the no-injection paths still raise. If the conftest pin is ever
removed, the lazy lane connects to this server, returns a rate, and the
pin tests below fail — the negative control proves the server would indeed
have answered.

This file is a sanctioned exception to the tests/unit "no real network
calls" rule (maintainer decision on PR #3665; see the tests/unit path
instructions in .coderabbit.yaml): the listener is loopback-only and
test-owned, and mocking the gateway surface here would make the pin inert —
it must exercise the real resolution path to prove the guard works.
"""

import asyncio
import time
from concurrent import futures
from decimal import Decimal

import grpc
import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.rates.monitor import RateMonitor
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.framework.market import MarketSnapshot
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc

_DEFAULT_GATEWAY_ADDR = "127.0.0.1:50051"


class _FakeRateHistoryService(gateway_pb2_grpc.RateHistoryServiceServicer):
    """Serves a plausible live rate for every GetLendingRateCurrent request."""

    def GetLendingRateCurrent(self, request, context):  # noqa: N802 - gRPC servicer API
        return gateway_pb2.LendingRatePointResponse(
            protocol=request.protocol,
            chain=request.chain,
            asset_symbol=request.asset_symbol,
            side=request.side,
            point=gateway_pb2.LendingRatePoint(
                timestamp=int(time.time()),
                supply_apy_pct="4.20",
                borrow_apy_pct="6.10",
                utilization_pct="55.5",
            ),
            source="on_chain",
            is_live_data=True,
            success=True,
        )


@pytest.fixture(scope="module")
def ambient_gateway_server():
    """A rate-serving gRPC server on the default gateway port, if bindable.

    Yields the server when this module owns the port, or ``None`` when the
    port is already occupied (a real gateway, or a sibling xdist worker) —
    in that case something IS listening at the default address, which is the
    exact hazard the pin tests assert against, so they proceed either way.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    gateway_pb2_grpc.add_RateHistoryServiceServicer_to_server(_FakeRateHistoryService(), server)
    bound_port = server.add_insecure_port(_DEFAULT_GATEWAY_ADDR)
    if bound_port == 0:
        yield None
        return
    server.start()
    try:
        yield server
    finally:
        server.stop(grace=None)


@pytest.fixture
def snapshot_without_providers():
    """A MarketSnapshot with no rate monitor and no gateway client injected."""
    return MarketSnapshot(chain="arbitrum", wallet_address="0xtest")


def test_negative_control_fake_gateway_serves_rates(ambient_gateway_server):
    """An EXPLICITLY-dialled client gets a rate from the fake server.

    This is what makes the pin tests below meaningful: it proves the server
    on the default port would have answered the lazy lane with a real rate,
    so "raises" there demonstrates the lane never dialled it — not that the
    server was broken.
    """
    if ambient_gateway_server is None:
        pytest.skip("default gateway port already occupied by another process")

    host, port = _DEFAULT_GATEWAY_ADDR.split(":")
    client = GatewayClient(GatewayClientConfig(host=host, port=int(port), timeout=5.0))
    client.connect()
    try:
        monitor = RateMonitor(chain="arbitrum", gateway_client=client, _internal=True)
        rate = asyncio.run(monitor._fetch_lending_rate_via_gateway("aave_v3", "USDC", "supply", None))
    finally:
        client.disconnect()

    assert rate.apy_percent == Decimal("4.20")


def test_lending_rate_without_monitor_raises_despite_ambient_gateway(
    ambient_gateway_server, snapshot_without_providers
):
    """The no-monitor lending_rate path raises even with a live default-port gateway."""
    with pytest.raises(ValueError, match="No rate monitor configured"):
        snapshot_without_providers.lending_rate("aave_v3", "USDC")


def test_best_lending_rate_without_monitor_raises_despite_ambient_gateway(
    ambient_gateway_server, snapshot_without_providers
):
    """The no-monitor best_lending_rate path raises even with a live default-port gateway."""
    with pytest.raises(ValueError, match="No rate monitor configured"):
        snapshot_without_providers.best_lending_rate("USDC", "supply")


def test_hermeticity_guard_does_not_signal_gateway_presence():
    """The guard must not flip the product's gateway-presence detection.

    ``ALMANAK_GATEWAY_HOST``'s mere PRESENCE is the signal
    ``gateway_backtest_configured()`` consumes — an env-var pin would move
    every unit test onto gateway-transport branches (CoinGecko / Subgraph
    providers growing gateway transports instead of their default
    direct/offline behaviour). The conftest guard therefore pins
    ``GatewayClientConfig.from_env()`` instead of exporting env vars, and
    scrubs inherited gateway host/port vars so a developer's shell exports
    cannot flip the signal either.
    """
    from almanak.framework.backtesting.pnl.providers.gateway_transport import (
        gateway_backtest_configured,
    )

    assert not gateway_backtest_configured()


def test_uninjected_rate_monitor_gateway_lane_is_unmeasured_despite_ambient_gateway(
    ambient_gateway_server,
):
    """A RateMonitor with no injected client cannot reach the ambient gateway.

    The singleton fallback (``_monitor_get_connected_gateway_client``) is the
    seam every ambient lane shares; with the unit-suite pin it must resolve
    to an unroutable address and surface honest-unmeasured
    (``DataSourceUnavailable``), never the ambient server's rate.
    """
    monitor = RateMonitor(chain="arbitrum", _internal=True)
    with pytest.raises(DataSourceUnavailable):
        asyncio.run(monitor._fetch_lending_rate_via_gateway("aave_v3", "USDC", "supply", None))
