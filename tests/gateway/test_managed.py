"""Tests for managed gateway (background thread gateway for strat run)."""

import os
import socket
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

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


class TestManagedGatewayExternalAnvil:
    """Tests for Bring Your Own Anvil (BYOA) support."""

    def _make_settings(self, port: int, network: str = "anvil") -> GatewaySettings:
        return GatewaySettings(
            grpc_port=port,
            metrics_enabled=False,
            audit_enabled=False,
            allow_insecure=True,
            network=network,
        )

    def test_external_anvil_ports_stored(self):
        """External anvil ports are stored on construction."""
        settings = self._make_settings(50070)
        gw = ManagedGateway(settings, anvil_chains=["arbitrum"], external_anvil_ports={"arbitrum": 8545})
        assert gw._external_anvil_ports == {"arbitrum": 8545}

    def test_defaults_to_empty_dict(self):
        """External anvil ports default to empty dict."""
        settings = self._make_settings(50070)
        gw = ManagedGateway(settings)
        assert gw._external_anvil_ports == {}

    @pytest.mark.asyncio
    async def test_start_anvil_forks_skips_external_chains(self):
        """_start_anvil_forks sets env var for external chains without starting RollingForkManager."""
        settings = self._make_settings(50071)
        gw = ManagedGateway(
            settings,
            anvil_chains=["arbitrum"],
            external_anvil_ports={"arbitrum": 9999},
        )
        with (
            patch("almanak.gateway.managed.is_port_in_use", return_value=True),
            patch("shutil.which", return_value="/usr/bin/anvil"),
        ):
            await gw._start_anvil_forks()

        # Should NOT have created a RollingForkManager
        assert "arbitrum" not in gw._anvil_managers
        # Should have set the env var
        assert os.environ.get("ANVIL_ARBITRUM_PORT") == "9999"

        # Cleanup
        await gw._stop_anvil_forks()

    @pytest.mark.asyncio
    async def test_external_anvil_unreachable_raises(self):
        """Raises RuntimeError if external Anvil instance is not reachable."""
        settings = self._make_settings(50072)
        gw = ManagedGateway(
            settings,
            anvil_chains=["arbitrum"],
            external_anvil_ports={"arbitrum": 9999},
        )
        with (
            patch("almanak.gateway.managed.is_port_in_use", return_value=False),
            patch("shutil.which", return_value="/usr/bin/anvil"),
        ):
            with pytest.raises(RuntimeError, match="not reachable"):
                await gw._start_anvil_forks()

    @pytest.mark.asyncio
    async def test_mixed_external_and_managed(self):
        """External chains skip fork startup, remaining chains start managed forks."""
        settings = self._make_settings(50073)
        gw = ManagedGateway(
            settings,
            anvil_chains=["arbitrum", "base"],
            external_anvil_ports={"arbitrum": 9999},
        )
        mock_manager = AsyncMock()
        mock_manager.start.return_value = True
        mock_manager.anvil_port = 12345

        with (
            patch("almanak.gateway.managed.is_port_in_use", return_value=True),
            patch("almanak.framework.backtesting.paper.fork_manager.RollingForkManager", return_value=mock_manager),
            patch("almanak.gateway.utils.rpc_provider.get_rpc_url", return_value="http://mock"),
            patch("shutil.which", return_value="/usr/bin/anvil"),
        ):
            await gw._start_anvil_forks()

        # arbitrum: external, no manager
        assert "arbitrum" not in gw._anvil_managers
        assert os.environ.get("ANVIL_ARBITRUM_PORT") == "9999"
        # base: managed, has manager
        assert "base" in gw._anvil_managers

        # Cleanup
        gw._keep_anvil = False
        await gw._stop_anvil_forks()

    @pytest.mark.asyncio
    async def test_external_env_var_restored_on_stop(self):
        """Env vars for external chains are restored after stop."""
        settings = self._make_settings(50074)
        gw = ManagedGateway(
            settings,
            anvil_chains=["arbitrum"],
            external_anvil_ports={"arbitrum": 9999},
        )
        original_val = os.environ.get("ANVIL_ARBITRUM_PORT")
        with (
            patch("almanak.gateway.managed.is_port_in_use", return_value=True),
            patch("shutil.which", return_value="/usr/bin/anvil"),
        ):
            await gw._start_anvil_forks()

        assert os.environ.get("ANVIL_ARBITRUM_PORT") == "9999"
        await gw._stop_anvil_forks()

        # Should be restored to original (likely None/removed)
        assert os.environ.get("ANVIL_ARBITRUM_PORT") == original_val


class TestManagedGatewayKeepAlive:
    """Tests for --keep-anvil behavior."""

    def _make_settings(self, port: int, network: str = "anvil") -> GatewaySettings:
        return GatewaySettings(
            grpc_port=port,
            metrics_enabled=False,
            audit_enabled=False,
            allow_insecure=True,
            network=network,
        )

    def test_keep_anvil_flag_stored(self):
        """keep_anvil flag is stored on construction."""
        settings = self._make_settings(50075)
        gw = ManagedGateway(settings, keep_anvil=True)
        assert gw._keep_anvil is True

    def test_keep_anvil_defaults_false(self):
        """keep_anvil defaults to False."""
        settings = self._make_settings(50075)
        gw = ManagedGateway(settings)
        assert gw._keep_anvil is False

    @pytest.mark.asyncio
    async def test_stop_anvil_forks_skips_when_keep_alive(self):
        """_stop_anvil_forks does NOT call manager.stop() when keep_anvil=True."""
        settings = self._make_settings(50076)
        gw = ManagedGateway(settings, anvil_chains=["arbitrum"], keep_anvil=True)

        mock_manager = AsyncMock()
        mock_manager.anvil_port = 12345
        mock_manager._process = MagicMock()
        mock_manager._process.pid = 99999
        gw._anvil_managers["arbitrum"] = mock_manager

        await gw._stop_anvil_forks()

        # manager.stop() should NOT have been called
        mock_manager.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_anvil_forks_stops_when_not_keep_alive(self):
        """_stop_anvil_forks calls manager.stop() when keep_anvil=False (default)."""
        settings = self._make_settings(50077)
        gw = ManagedGateway(settings, anvil_chains=["arbitrum"], keep_anvil=False)

        mock_manager = AsyncMock()
        gw._anvil_managers["arbitrum"] = mock_manager

        await gw._stop_anvil_forks()

        # manager.stop() SHOULD have been called
        mock_manager.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_keep_alive_restores_external_env_vars(self):
        """When keep_anvil=True, external Anvil env vars are still restored."""
        settings = self._make_settings(50078)
        gw = ManagedGateway(
            settings,
            anvil_chains=["arbitrum", "base"],
            external_anvil_ports={"base": 8888},
            keep_anvil=True,
        )

        # Simulate managed fork for arbitrum
        mock_manager = AsyncMock()
        mock_manager.anvil_port = 12345
        mock_manager._process = MagicMock()
        mock_manager._process.pid = 11111
        gw._anvil_managers["arbitrum"] = mock_manager

        # Simulate env vars set during _start_anvil_forks
        gw._original_env["ANVIL_ARBITRUM_PORT"] = None
        gw._original_env["ANVIL_BASE_PORT"] = None
        os.environ["ANVIL_ARBITRUM_PORT"] = "12345"
        os.environ["ANVIL_BASE_PORT"] = "8888"

        await gw._stop_anvil_forks()

        # Managed fork env var should be preserved (keep alive)
        assert os.environ.get("ANVIL_ARBITRUM_PORT") == "12345"
        # External fork env var should be restored (removed, since original was None)
        assert os.environ.get("ANVIL_BASE_PORT") is None
        # Manager should NOT have been stopped
        mock_manager.stop.assert_not_called()

        # Manual cleanup
        os.environ.pop("ANVIL_ARBITRUM_PORT", None)

    @pytest.mark.asyncio
    async def test_force_stop_overrides_keep_alive(self):
        """_stop_anvil_forks(force=True) stops managers even when keep_anvil=True."""
        settings = self._make_settings(50079)
        gw = ManagedGateway(settings, anvil_chains=["arbitrum"], keep_anvil=True)

        mock_manager = AsyncMock()
        mock_manager.anvil_port = 12345
        mock_manager._process = MagicMock()
        mock_manager._process.pid = 99999
        gw._anvil_managers["arbitrum"] = mock_manager

        await gw._stop_anvil_forks(force=True)

        # manager.stop() SHOULD have been called despite keep_anvil=True
        mock_manager.stop.assert_called_once()
