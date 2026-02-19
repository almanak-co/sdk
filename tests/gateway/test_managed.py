"""Tests for managed gateway (background thread gateway for strat run)."""

import socket
import threading
import time

import grpc
import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.managed import (
    GatewayPortUnavailableError,
    ManagedGateway,
    find_available_gateway_port,
    is_port_in_use,
)


class TestIsPortInUse:
    """Tests for the is_port_in_use utility."""

    def test_free_port_returns_false(self):
        """A port that nothing is listening on returns False."""
        # Dynamically find a free port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            free_port = s.getsockname()[1]
        assert is_port_in_use("127.0.0.1", free_port) is False

    def test_occupied_port_returns_true(self):
        """A port with a listening socket returns True."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 59998))
        sock.listen(1)
        try:
            assert is_port_in_use("127.0.0.1", 59998) is True
        finally:
            sock.close()


class TestFindAvailableGatewayPort:
    """Tests for the find_available_gateway_port utility."""

    def test_returns_preferred_port_when_free(self):
        """Returns the preferred port immediately if it's available."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            free_port = s.getsockname()[1]
        result = find_available_gateway_port("127.0.0.1", free_port)
        assert result == free_port

    def test_skips_occupied_port(self):
        """Skips an occupied port and returns a free one."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        occupied_port = sock.getsockname()[1]
        try:
            result = find_available_gateway_port("127.0.0.1", occupied_port)
            assert result != occupied_port
            assert not is_port_in_use("127.0.0.1", result)
        finally:
            sock.close()

    def test_raises_when_all_ports_occupied(self):
        """Raises GatewayPortUnavailableError when all candidate ports are occupied."""
        sockets = []
        base_port = None
        # Dynamically find 3 consecutive bindable ports
        for _ in range(50):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.bind(("127.0.0.1", 0))
                candidate = probe.getsockname()[1]
            bound = []
            try:
                for i in range(3):
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind(("127.0.0.1", candidate + i))
                    s.listen(1)
                    bound.append(s)
                base_port = candidate
                sockets = bound
                break
            except OSError:
                for s in bound:
                    s.close()
                continue

        if base_port is None:
            pytest.skip("Could not reserve 3 consecutive ports")

        try:
            with pytest.raises(GatewayPortUnavailableError, match="No available port found"):
                find_available_gateway_port("127.0.0.1", base_port, max_attempts=3)
        finally:
            for s in sockets:
                s.close()


class TestManagedGateway:
    """Tests for ManagedGateway lifecycle."""

    def _make_settings(self, port: int) -> GatewaySettings:
        return GatewaySettings(
            grpc_port=port,
            metrics_enabled=False,
            audit_enabled=False,
            allow_insecure=True,
        )

    def test_start_and_stop(self):
        """ManagedGateway starts in a background thread and stops cleanly."""
        gw = ManagedGateway(self._make_settings(50060))
        gw.start()
        try:
            assert gw._thread is not None
            assert gw._thread.is_alive()
            assert gw._thread.daemon is True
            assert gw._thread.name == "managed-gateway"
        finally:
            gw.stop()

        # Thread should be dead after stop
        assert not gw._thread.is_alive()

    def test_health_check_after_start(self):
        """Gateway responds to gRPC health checks after start."""
        gw = ManagedGateway(self._make_settings(50061))
        gw.start()
        try:
            channel = grpc.insecure_channel("127.0.0.1:50061")
            stub = health_pb2_grpc.HealthStub(channel)
            response = stub.Check(health_pb2.HealthCheckRequest(service=""))
            assert response.status == health_pb2.HealthCheckResponse.SERVING
            channel.close()
        finally:
            gw.stop()

    def test_context_manager(self):
        """ManagedGateway works as a context manager."""
        with ManagedGateway(self._make_settings(50062)) as gw:
            assert gw._thread.is_alive()
            channel = grpc.insecure_channel("127.0.0.1:50062")
            stub = health_pb2_grpc.HealthStub(channel)
            response = stub.Check(health_pb2.HealthCheckRequest(service=""))
            assert response.status == health_pb2.HealthCheckResponse.SERVING
            channel.close()

        # After exiting context, thread should be stopped
        assert not gw._thread.is_alive()

    def test_host_and_port_properties(self):
        """Host and port properties reflect settings."""
        settings = self._make_settings(50063)
        gw = ManagedGateway(settings)
        assert gw.host == "127.0.0.1"
        assert gw.port == 50063

    def test_stop_is_idempotent(self):
        """Calling stop multiple times does not raise."""
        gw = ManagedGateway(self._make_settings(50064))
        gw.start()
        gw.stop()
        gw.stop()  # Should not raise
        assert not gw._thread.is_alive()
