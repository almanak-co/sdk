"""Tests for managed gateway (background thread gateway for strat run)."""

import os
import socket
import threading
from decimal import Decimal
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
        sock.bind(("127.0.0.1", 0))
        occupied_port = sock.getsockname()[1]
        sock.listen(1)
        try:
            assert is_port_in_use("127.0.0.1", occupied_port) is True
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
            patch("almanak.framework.anvil.fork_manager.RollingForkManager", return_value=mock_manager),
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


class TestAnvilWatchdog:
    """Tests for the Anvil process watchdog."""

    def _make_settings(self, port: int, network: str = "anvil") -> GatewaySettings:
        return GatewaySettings(
            grpc_port=port,
            metrics_enabled=False,
            audit_enabled=False,
            allow_insecure=True,
            network=network,
        )

    @pytest.mark.asyncio
    async def test_watchdog_restarts_crashed_process(self):
        """Watchdog detects a dead Anvil process and restarts it."""
        import asyncio

        settings = self._make_settings(50080)
        gw = ManagedGateway(settings, anvil_chains=["arbitrum"])
        gw._stop_requested = threading.Event()

        # First call: not running (crashed). Second call: running (restarted).
        mock_manager = AsyncMock()
        mock_manager.is_running = False
        mock_manager.reset_to_latest = AsyncMock(return_value=True)
        gw._anvil_managers["arbitrum"] = mock_manager
        gw._wallet_address = None  # no funding needed

        # Use a short interval so the watchdog triggers immediately
        gw._watchdog_interval = 0.05

        # Run watchdog for just long enough for one cycle, then stop
        async def run_briefly():
            task = asyncio.ensure_future(gw._anvil_watchdog())
            await asyncio.sleep(0.15)
            gw._stop_requested.set()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await run_briefly()

        mock_manager.reset_to_latest.assert_called()

    @pytest.mark.asyncio
    async def test_watchdog_skips_healthy_process(self):
        """Watchdog does not restart a healthy Anvil process."""
        import asyncio

        settings = self._make_settings(50081)
        gw = ManagedGateway(settings, anvil_chains=["base"])
        gw._stop_requested = threading.Event()

        mock_manager = AsyncMock()
        mock_manager.is_running = True  # healthy
        mock_manager.reset_to_latest = AsyncMock(return_value=True)
        gw._anvil_managers["base"] = mock_manager

        gw._watchdog_interval = 0.05

        async def run_briefly():
            task = asyncio.ensure_future(gw._anvil_watchdog())
            await asyncio.sleep(0.15)
            gw._stop_requested.set()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await run_briefly()

        mock_manager.reset_to_latest.assert_not_called()

    @pytest.mark.asyncio
    async def test_watchdog_handles_restart_failure_gracefully(self):
        """Watchdog logs an error but does not raise when restart fails."""
        import asyncio

        settings = self._make_settings(50082)
        gw = ManagedGateway(settings, anvil_chains=["arbitrum"])
        gw._stop_requested = threading.Event()

        mock_manager = AsyncMock()
        mock_manager.is_running = False
        mock_manager.reset_to_latest = AsyncMock(return_value=False)  # restart fails
        gw._anvil_managers["arbitrum"] = mock_manager

        gw._watchdog_interval = 0.05

        async def run_briefly():
            task = asyncio.ensure_future(gw._anvil_watchdog())
            await asyncio.sleep(0.15)
            gw._stop_requested.set()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Should not raise
        await run_briefly()
        mock_manager.reset_to_latest.assert_called()

    @pytest.mark.asyncio
    async def test_watchdog_exits_on_stop_requested(self):
        """Watchdog exits cleanly when stop is requested before first check."""
        import asyncio

        settings = self._make_settings(50083)
        gw = ManagedGateway(settings, anvil_chains=["arbitrum"])
        gw._stop_requested = threading.Event()
        gw._stop_requested.set()  # stop immediately

        mock_manager = AsyncMock()
        mock_manager.is_running = False
        gw._anvil_managers["arbitrum"] = mock_manager

        gw._watchdog_interval = 0.05

        task = asyncio.ensure_future(gw._anvil_watchdog())
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Should not have tried to restart since stop was requested immediately
        mock_manager.reset_to_latest.assert_not_called()


class TestAnvilPublicRpcFallback:
    """Tests for public RPC fallback when primary Alchemy/private RPC fails."""

    def _make_settings(self, port: int, network: str = "anvil") -> GatewaySettings:
        return GatewaySettings(
            grpc_port=port,
            metrics_enabled=False,
            audit_enabled=False,
            allow_insecure=True,
            network=network,
        )

    @pytest.mark.asyncio
    async def test_falls_back_to_public_rpc_on_primary_failure(self):
        """When primary RPC start fails, retries with public fallback URL."""
        settings = self._make_settings(50090)
        gw = ManagedGateway(settings, anvil_chains=["xlayer"])

        # First call fails (primary RPC), second call succeeds (public fallback)
        mock_manager_fail = AsyncMock()
        mock_manager_fail.start.return_value = False
        mock_manager_fail.anvil_port = 8557

        mock_manager_ok = AsyncMock()
        mock_manager_ok.start.return_value = True
        mock_manager_ok.anvil_port = 8558

        manager_instances = [mock_manager_fail, mock_manager_ok]

        with (
            patch(
                "almanak.framework.anvil.fork_manager.RollingForkManager",
                side_effect=manager_instances,
            ),
            patch(
                "almanak.gateway.utils.rpc_provider.get_rpc_url",
                return_value="https://xlayer-mainnet.g.alchemy.com/v2/FAKE_KEY",
            ),
            patch(
                "almanak.gateway.utils.rpc_provider.PUBLIC_RPC_URLS",
                {"xlayer": "https://rpc.xlayer.tech"},
            ),
            patch("almanak.gateway.managed.find_free_port", return_value=8558),
            patch("shutil.which", return_value="/usr/bin/anvil"),
        ):
            await gw._start_anvil_forks()

        # Should have ended up with the successful (public) manager
        assert "xlayer" in gw._anvil_managers
        assert gw._anvil_managers["xlayer"] is mock_manager_ok

        # Cleanup
        gw._keep_anvil = False
        await gw._stop_anvil_forks()

    @pytest.mark.asyncio
    async def test_raises_when_both_rpc_fail(self):
        """Raises RuntimeError when both primary and public RPC start fail."""
        settings = self._make_settings(50091)
        gw = ManagedGateway(settings, anvil_chains=["xlayer"])

        mock_manager_fail = AsyncMock()
        mock_manager_fail.start.return_value = False
        mock_manager_fail.anvil_port = 8557

        mock_manager_fail2 = AsyncMock()
        mock_manager_fail2.start.return_value = False
        mock_manager_fail2.anvil_port = 8558

        with (
            patch(
                "almanak.framework.anvil.fork_manager.RollingForkManager",
                side_effect=[mock_manager_fail, mock_manager_fail2],
            ),
            patch(
                "almanak.gateway.utils.rpc_provider.get_rpc_url",
                return_value="https://xlayer-mainnet.g.alchemy.com/v2/FAKE_KEY",
            ),
            patch(
                "almanak.gateway.utils.rpc_provider.PUBLIC_RPC_URLS",
                {"xlayer": "https://rpc.xlayer.tech"},
            ),
            patch("almanak.gateway.managed.find_free_port", return_value=8558),
            patch("shutil.which", return_value="/usr/bin/anvil"),
        ):
            with pytest.raises(RuntimeError, match="Failed to start Anvil fork"):
                await gw._start_anvil_forks()


class TestAnvilForkBlockPinning:
    """Tests for ANVIL_FORK_BLOCK_{CHAIN} env var passthrough to RollingForkManager."""

    def _make_settings(self, port: int) -> GatewaySettings:
        return GatewaySettings(
            grpc_port=port,
            metrics_enabled=False,
            audit_enabled=False,
            allow_insecure=True,
            network="anvil",
        )

    @pytest.mark.asyncio
    async def test_fork_block_env_var_passed_to_manager(self):
        """When ANVIL_FORK_BLOCK_{CHAIN} is set, fork_block_number is passed to RollingForkManager."""
        settings = self._make_settings(50095)
        gw = ManagedGateway(settings, anvil_chains=["arbitrum"])

        mock_manager = AsyncMock()
        mock_manager.start.return_value = True
        mock_manager.anvil_port = 8560

        env_patch = patch.dict(os.environ, {"ANVIL_FORK_BLOCK_ARBITRUM": "449499053"})

        with (
            env_patch,
            patch(
                "almanak.framework.anvil.fork_manager.RollingForkManager",
                return_value=mock_manager,
            ) as mock_cls,
            patch(
                "almanak.gateway.utils.rpc_provider.get_rpc_url",
                return_value="https://arb-mainnet.g.alchemy.com/v2/FAKE",
            ),
            patch("almanak.gateway.managed.find_free_port", return_value=8560),
            patch("shutil.which", return_value="/usr/bin/anvil"),
        ):
            await gw._start_anvil_forks()

        # Verify fork_block_number was passed
        mock_cls.assert_called_once()
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["fork_block_number"] == 449499053

        # Cleanup
        gw._keep_anvil = False
        await gw._stop_anvil_forks()

    @pytest.mark.asyncio
    async def test_no_fork_block_env_var_defaults_to_none(self):
        """When ANVIL_FORK_BLOCK_{CHAIN} is not set, fork_block_number is None."""
        settings = self._make_settings(50096)
        gw = ManagedGateway(settings, anvil_chains=["base"])

        mock_manager = AsyncMock()
        mock_manager.start.return_value = True
        mock_manager.anvil_port = 8561

        # Ensure the env var is NOT set
        env_patch = patch.dict(os.environ, {}, clear=False)

        with (
            env_patch,
            patch(
                "almanak.framework.anvil.fork_manager.RollingForkManager",
                return_value=mock_manager,
            ) as mock_cls,
            patch(
                "almanak.gateway.utils.rpc_provider.get_rpc_url",
                return_value="https://base-mainnet.g.alchemy.com/v2/FAKE",
            ),
            patch("almanak.gateway.managed.find_free_port", return_value=8561),
            patch("shutil.which", return_value="/usr/bin/anvil"),
        ):
            # Remove env var if it happens to exist
            os.environ.pop("ANVIL_FORK_BLOCK_BASE", None)
            await gw._start_anvil_forks()

        mock_cls.assert_called_once()
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["fork_block_number"] is None

        # Cleanup
        gw._keep_anvil = False
        await gw._stop_anvil_forks()


class TestAnvilFundingNativeTokenWarning:
    """VIB-1579: warn when anvil_funding uses 'ETH' on non-ETH chains."""

    def _make_gateway(self, chain: str, anvil_funding: dict) -> "ManagedGateway":
        from almanak.gateway.core.settings import GatewaySettings
        from almanak.gateway.managed import ManagedGateway

        settings = GatewaySettings(
            host="127.0.0.1",
            port=59990,
            rpc_url="http://localhost:8545",
            chain=chain,
        )
        gw = ManagedGateway(settings, anvil_chains=[chain], anvil_funding=anvil_funding)
        gw._wallet_address = "0x" + "a" * 40
        return gw

    @pytest.mark.asyncio
    async def test_eth_key_on_bsc_emits_warning(self):
        """Using 'ETH' in anvil_funding on BSC should log a warning."""
        gw = self._make_gateway("bsc", {"ETH": 10, "USDC": 1000})

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["bsc"] = mock_manager

        with patch("almanak.gateway.managed.logger") as mock_logger:
            await gw._fund_anvil_wallets()

        # Should have warned about ETH on BSC
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("ETH" in w and "BNB" in w for w in warning_calls), (
            f"Expected warning about ETH on BSC, got: {warning_calls}"
        )

    @pytest.mark.asyncio
    async def test_bnb_key_on_bsc_no_warning(self):
        """Using 'BNB' in anvil_funding on BSC should not warn."""
        gw = self._make_gateway("bsc", {"BNB": 10, "USDC": 1000})

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["bsc"] = mock_manager

        with patch("almanak.gateway.managed.logger") as mock_logger:
            await gw._fund_anvil_wallets()

        # Should NOT have warned about native token mismatch
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        assert not any("BNB" in w and "ETH" in w for w in warning_calls), (
            f"Unexpected native token mismatch warning: {warning_calls}"
        )

    @pytest.mark.asyncio
    async def test_eth_key_on_arbitrum_no_warning(self):
        """Using 'ETH' in anvil_funding on Arbitrum (ETH-native) should not warn."""
        gw = self._make_gateway("arbitrum", {"ETH": 1, "USDC": 1000})

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["arbitrum"] = mock_manager

        with patch("almanak.gateway.managed.logger") as mock_logger:
            await gw._fund_anvil_wallets()

        # Arbitrum is ETH-native -- no warning expected
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        native_mismatch = [w for w in warning_calls if "Did you mean" in w]
        assert not native_mismatch, f"Unexpected mismatch warning: {native_mismatch}"


class TestAnvilFundingDefaultNativeGas:
    """VIB-3752: ensure native gas is funded even when anvil_funding is empty/missing.

    The QA April 29 batch found ``velodrome_swap_optimism`` failing on Optimism
    Anvil with ``Insufficient funds for gas * price + value`` because the
    incubating strategy had no ``config.json`` (and therefore no
    ``anvil_funding``). The previous behaviour skipped funding entirely in that
    case, leaving the wallet at 0 native balance.
    """

    def _make_gateway(self, chain: str, anvil_funding: dict | None) -> "ManagedGateway":
        settings = GatewaySettings(
            host="127.0.0.1",
            port=59990,
            rpc_url="http://localhost:8545",
            chain=chain,
        )
        gw = ManagedGateway(settings, anvil_chains=[chain], anvil_funding=anvil_funding)
        gw._wallet_address = "0x" + "a" * 40
        return gw

    @pytest.mark.asyncio
    async def test_empty_funding_still_funds_native_gas(self):
        """Empty anvil_funding -> default native gas amount is still applied."""
        gw = self._make_gateway("optimism", anvil_funding=None)

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["optimism"] = mock_manager

        await gw._fund_anvil_wallets()

        # The default native gas top-up MUST have been applied — without this,
        # strategies with no anvil_funding boot with 0 ETH and revert.
        mock_manager.fund_wallet.assert_awaited_once()
        call_args = mock_manager.fund_wallet.await_args
        assert call_args.args[0] == gw._wallet_address
        assert call_args.args[1] == ManagedGateway.DEFAULT_ANVIL_NATIVE_GAS_AMOUNT
        # No ERC20 funding when the dict is empty.
        mock_manager.fund_tokens.assert_not_called()

    @pytest.mark.asyncio
    async def test_partial_funding_below_default_is_topped_up(self):
        """anvil_funding with a small native amount -> topped up to default."""
        gw = self._make_gateway("optimism", anvil_funding={"ETH": "0.5"})

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["optimism"] = mock_manager

        await gw._fund_anvil_wallets()

        mock_manager.fund_wallet.assert_awaited_once()
        funded_amount = mock_manager.fund_wallet.await_args.args[1]
        assert funded_amount == ManagedGateway.DEFAULT_ANVIL_NATIVE_GAS_AMOUNT, (
            "0.5 ETH < default 100 ETH; helper must top up so gas is never the bottleneck"
        )

    @pytest.mark.asyncio
    async def test_funding_above_default_is_respected(self):
        """anvil_funding with a generous native amount -> not reduced."""
        gw = self._make_gateway("optimism", anvil_funding={"ETH": 1000, "USDC": 5000})

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["optimism"] = mock_manager

        await gw._fund_anvil_wallets()

        funded_amount = mock_manager.fund_wallet.await_args.args[1]
        assert funded_amount == Decimal("1000"), "User-specified amount must not be reduced"
        mock_manager.fund_tokens.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_default_uses_chain_native_token_not_eth(self):
        """On non-ETH chains, the default tops up the chain's native token (e.g. AVAX)."""
        gw = self._make_gateway("avalanche", anvil_funding=None)

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["avalanche"] = mock_manager

        await gw._fund_anvil_wallets()

        # fund_wallet always denominates in the chain's native token; the
        # top-up must therefore go through (not be filtered for ETH-only).
        mock_manager.fund_wallet.assert_awaited_once()
        funded_amount = mock_manager.fund_wallet.await_args.args[1]
        assert funded_amount == ManagedGateway.DEFAULT_ANVIL_NATIVE_GAS_AMOUNT

    @pytest.mark.asyncio
    async def test_no_wallet_address_skips_funding(self):
        """Without a derivable wallet, funding is skipped — no exception."""
        gw = self._make_gateway("optimism", anvil_funding=None)
        gw._wallet_address = None

        mock_manager = AsyncMock()
        gw._anvil_managers["optimism"] = mock_manager

        # No ALMANAK_PRIVATE_KEY in env (env-isolation via patch)
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ALMANAK_PRIVATE_KEY", None)
            await gw._fund_anvil_wallets()

        mock_manager.fund_wallet.assert_not_called()

    @pytest.mark.asyncio
    async def test_default_funding_applies_when_chain_not_in_native_symbol_map(self):
        """CodeRabbit feedback (PR #1981): even when the chain isn't yet in
        ``CHAIN_NATIVE_SYMBOL``, the baseline native gas top-up must still
        fire — ``manager.fund_wallet`` uses ``anvil_setBalance`` which is
        symbol-agnostic, and skipping the top-up here would silently
        regress every newly-added Anvil chain back to the original
        ``Insufficient funds for gas`` failure mode that VIB-3752 fixes.
        """
        # "newchain" is intentionally NOT in CHAIN_NATIVE_SYMBOL.
        assert "newchain" not in ManagedGateway.CHAIN_NATIVE_SYMBOL
        gw = self._make_gateway("newchain", anvil_funding=None)

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["newchain"] = mock_manager

        await gw._fund_anvil_wallets()

        # Baseline must apply regardless of CHAIN_NATIVE_SYMBOL coverage.
        mock_manager.fund_wallet.assert_awaited_once()
        funded_amount = mock_manager.fund_wallet.await_args.args[1]
        assert funded_amount == ManagedGateway.DEFAULT_ANVIL_NATIVE_GAS_AMOUNT


class TestAnvilFundingPrivateKeyResolution:
    """Wallet-address resolution in ``_fund_anvil_wallets``: env > settings >
    skip. Regression: ``almanak strat test`` plumbs ANVIL_DEFAULT_PRIVATE_KEY
    via ``GatewaySettings.private_key`` (#1975 path through #2100), but the
    funding code originally only read ``os.environ`` — so the test-only key
    reached the signer but not the funder, and every test run silently
    skipped Anvil funding.
    """

    ANVIL_DEFAULT_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"  # gitleaks:allow
    ANVIL_DEFAULT_ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    OTHER_KEY = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"  # gitleaks:allow
    OTHER_ADDRESS = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

    def _make_gateway(self, *, settings_private_key: str | None) -> "ManagedGateway":
        settings = GatewaySettings(
            host="127.0.0.1",
            port=59990,
            rpc_url="http://localhost:8545",
            chain="base",
            private_key=settings_private_key,
        )
        # No explicit ``wallet_address`` — exercise the fallback path.
        return ManagedGateway(settings, anvil_chains=["base"], anvil_funding={"ETH": 10})

    @pytest.mark.asyncio
    async def test_settings_private_key_used_when_env_unset(self, monkeypatch):
        """``strat test`` path: env empty, settings carries the Anvil default."""
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        gw = self._make_gateway(settings_private_key=self.ANVIL_DEFAULT_KEY)

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["base"] = mock_manager

        await gw._fund_anvil_wallets()

        # Funding must have run against the Anvil-default address derived from
        # ``settings.private_key``. Without the settings consult, this test
        # would observe ``fund_wallet`` never awaited and a "skipping Anvil
        # funding" warning instead.
        mock_manager.fund_wallet.assert_awaited_once()
        assert mock_manager.fund_wallet.await_args.args[0] == self.ANVIL_DEFAULT_ADDRESS

    @pytest.mark.asyncio
    async def test_env_wins_over_settings(self, monkeypatch):
        """Explicit env export at funding time overrides the cached settings
        value — preserves the historical env-wins-at-funding semantic so a
        human override remains a hard override."""
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", self.OTHER_KEY)
        gw = self._make_gateway(settings_private_key=self.ANVIL_DEFAULT_KEY)

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["base"] = mock_manager

        await gw._fund_anvil_wallets()

        mock_manager.fund_wallet.assert_awaited_once()
        assert mock_manager.fund_wallet.await_args.args[0] == self.OTHER_ADDRESS

    @pytest.mark.asyncio
    async def test_skip_when_neither_env_nor_settings_has_key(self, monkeypatch):
        """Neither env nor settings → keep the existing skip-with-warning
        behaviour. No silent funding of an arbitrary address."""
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        gw = self._make_gateway(settings_private_key=None)

        mock_manager = AsyncMock()
        mock_manager.fund_wallet = AsyncMock()
        mock_manager.fund_tokens = AsyncMock()
        gw._anvil_managers["base"] = mock_manager

        with patch("almanak.gateway.managed.logger") as mock_logger:
            await gw._fund_anvil_wallets()

        mock_manager.fund_wallet.assert_not_called()
        mock_manager.fund_tokens.assert_not_called()
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("skipping Anvil funding" in w for w in warning_calls), (
            f"Expected the skip-funding warning, got: {warning_calls}"
        )


class TestStartAnvilForksBranches:
    """Branch coverage for ManagedGateway._start_anvil_forks.

    All process/network seams are mocked: ``shutil.which``, the archive-RPC
    gate, ``RollingForkManager``, ``get_rpc_url``/``PUBLIC_RPC_URLS``, and the
    managed-module env seam (``env_value``/``set_env_value``) so no real
    ``os.environ`` mutation or subprocess launch happens.
    """

    def _make_gateway(self, chains, external_ports=None):
        settings = GatewaySettings(
            grpc_port=50097,
            metrics_enabled=False,
            audit_enabled=False,
            allow_insecure=True,
            network="anvil",
        )
        return ManagedGateway(settings, anvil_chains=chains, external_anvil_ports=external_ports)

    @staticmethod
    def _manager(start_ok=True):
        manager = AsyncMock()
        manager.start.return_value = start_ok
        return manager

    def _patches(
        self,
        fake_env,
        *,
        managers,
        free_ports,
        rpc_url="https://eth-mainnet.g.alchemy.com/v2/TOPSECRETKEY",
        public_rpc_urls=None,
        fork_block=None,
        port_in_use=True,
        which="/usr/bin/anvil",
    ):
        """Build the full patch stack for a _start_anvil_forks run."""
        rfm_cls = MagicMock(side_effect=managers)
        return (
            rfm_cls,
            (
                patch("shutil.which", return_value=which),
                patch("almanak.framework.anvil.fork_manager.RollingForkManager", rfm_cls),
                patch("almanak.gateway.utils.rpc_provider.get_rpc_url", return_value=rpc_url),
                patch("almanak.gateway.utils.rpc_provider.PUBLIC_RPC_URLS", public_rpc_urls or {}),
                patch("almanak.gateway.managed.find_free_port", side_effect=free_ports),
                patch("almanak.gateway.managed.is_port_in_use", return_value=port_in_use),
                patch("almanak.gateway.managed.anvil_fork_block_for_chain", return_value=fork_block),
                patch("almanak.gateway.managed.env_value", side_effect=fake_env.get),
                patch(
                    "almanak.gateway.managed.set_env_value",
                    side_effect=lambda name, value: fake_env.__setitem__(name, value),
                ),
            ),
        )

    @pytest.mark.asyncio
    async def test_missing_anvil_binary_raises_install_error(self):
        """No anvil on PATH -> actionable install error before any fork work."""
        gw = self._make_gateway(["arbitrum"])
        fake_env: dict[str, str] = {}
        rfm_cls, patches = self._patches(fake_env, managers=[], free_ports=[], which=None)
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            gate = stack.enter_context(patch.object(gw, "_check_archive_rpc_availability"))
            with pytest.raises(RuntimeError, match="Anvil is not installed"):
                await gw._start_anvil_forks()

        gate.assert_not_called()
        rfm_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_managed_happy_path_starts_fork_sets_env_and_redacts_url(self, caplog):
        """Managed chain: fork started, env routed, archive gate run, URL redacted."""
        import logging

        gw = self._make_gateway(["arbitrum"])
        fake_env: dict[str, str] = {}
        manager = self._manager(start_ok=True)
        rfm_cls, patches = self._patches(fake_env, managers=[manager], free_ports=[7101])
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            gate = stack.enter_context(patch.object(gw, "_check_archive_rpc_availability"))
            with caplog.at_level(logging.INFO, logger="almanak.gateway.managed"):
                await gw._start_anvil_forks()

        gate.assert_called_once()
        manager.start.assert_awaited_once()
        assert gw._anvil_managers["arbitrum"] is manager
        assert fake_env["ANVIL_ARBITRUM_PORT"] == "7101"
        assert gw._original_env == {"ANVIL_ARBITRUM_PORT": None}
        kwargs = rfm_cls.call_args.kwargs
        assert kwargs["chain"] == "arbitrum"
        assert kwargs["anvil_port"] == 7101
        assert kwargs["fork_block_number"] is None
        assert kwargs["startup_timeout_seconds"] == 30.0  # arbitrum is not cold-start slow
        assert kwargs["keep_alive_detached"] is False
        # API key must never leak into logs; path tail is redacted to /***
        assert "TOPSECRETKEY" not in caplog.text
        assert "/***" in caplog.text

    @pytest.mark.asyncio
    async def test_cold_start_slow_chain_gets_extended_timeout_and_fork_pin(self):
        """Ethereum: 90s startup budget and the pinned fork block is threaded through."""
        gw = self._make_gateway(["ethereum"])
        fake_env = {"ANVIL_ETHEREUM_PORT": "1111"}  # pre-existing value must be preserved
        manager = self._manager(start_ok=True)
        rfm_cls, patches = self._patches(
            fake_env, managers=[manager], free_ports=[7102], fork_block=123_456
        )
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch.object(gw, "_check_archive_rpc_availability"))
            await gw._start_anvil_forks()

        kwargs = rfm_cls.call_args.kwargs
        assert kwargs["startup_timeout_seconds"] == 90.0
        assert kwargs["fork_block_number"] == 123_456
        # Original env value snapshotted for restore-on-stop
        assert gw._original_env == {"ANVIL_ETHEREUM_PORT": "1111"}
        assert fake_env["ANVIL_ETHEREUM_PORT"] == "7102"

    @pytest.mark.asyncio
    async def test_external_chain_sets_env_without_starting_manager(self):
        """External Anvil: env var routed to the external port, no fork process."""
        gw = self._make_gateway(["arbitrum"], external_ports={"arbitrum": 9999})
        fake_env: dict[str, str] = {}
        rfm_cls, patches = self._patches(fake_env, managers=[], free_ports=[], port_in_use=True)
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch.object(gw, "_check_archive_rpc_availability"))
            await gw._start_anvil_forks()

        rfm_cls.assert_not_called()
        assert gw._anvil_managers == {}
        assert fake_env["ANVIL_ARBITRUM_PORT"] == "9999"
        assert gw._original_env == {"ANVIL_ARBITRUM_PORT": None}

    @pytest.mark.asyncio
    async def test_external_chain_unreachable_raises_and_cleans_up(self):
        """Unreachable external Anvil: RuntimeError + forced fork cleanup."""
        gw = self._make_gateway(["arbitrum"], external_ports={"arbitrum": 9999})
        fake_env: dict[str, str] = {}
        _, patches = self._patches(fake_env, managers=[], free_ports=[], port_in_use=False)
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch.object(gw, "_check_archive_rpc_availability"))
            stop = stack.enter_context(patch.object(gw, "_stop_anvil_forks", AsyncMock()))
            with pytest.raises(RuntimeError, match="not reachable on port 9999"):
                await gw._start_anvil_forks()

        stop.assert_awaited_once_with(force=True)

    @pytest.mark.asyncio
    async def test_primary_failure_falls_back_to_public_rpc(self):
        """Primary RPC start failure retries once against the public endpoint."""
        gw = self._make_gateway(["arbitrum"])
        fake_env: dict[str, str] = {}
        failed = self._manager(start_ok=False)
        ok = self._manager(start_ok=True)
        rfm_cls, patches = self._patches(
            fake_env,
            managers=[failed, ok],
            free_ports=[7101, 7102],
            public_rpc_urls={"arbitrum": "https://arb-public.example/rpc"},
        )
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch.object(gw, "_check_archive_rpc_availability"))
            await gw._start_anvil_forks()

        assert rfm_cls.call_count == 2
        retry_kwargs = rfm_cls.call_args_list[1].kwargs
        assert retry_kwargs["rpc_url"] == "https://arb-public.example/rpc"
        assert retry_kwargs["anvil_port"] == 7102
        assert gw._anvil_managers["arbitrum"] is ok
        assert fake_env["ANVIL_ARBITRUM_PORT"] == "7102"

    @pytest.mark.asyncio
    async def test_primary_failure_without_public_fallback_raises(self):
        """No public fallback URL for the chain: fail fast and clean up."""
        gw = self._make_gateway(["arbitrum"])
        fake_env: dict[str, str] = {}
        failed = self._manager(start_ok=False)
        rfm_cls, patches = self._patches(
            fake_env, managers=[failed], free_ports=[7101], public_rpc_urls={}
        )
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch.object(gw, "_check_archive_rpc_availability"))
            stop = stack.enter_context(patch.object(gw, "_stop_anvil_forks", AsyncMock()))
            with pytest.raises(RuntimeError, match="Failed to start Anvil fork for arbitrum"):
                await gw._start_anvil_forks()

        assert rfm_cls.call_count == 1  # no retry without a distinct public URL
        stop.assert_awaited_once_with(force=True)

    @pytest.mark.asyncio
    async def test_primary_failure_public_same_as_primary_raises_without_retry(self):
        """Public URL identical to the failed primary: retry is pointless, raise."""
        gw = self._make_gateway(["arbitrum"])
        fake_env: dict[str, str] = {}
        failed = self._manager(start_ok=False)
        rfm_cls, patches = self._patches(
            fake_env,
            managers=[failed],
            free_ports=[7101],
            rpc_url="https://arb-public.example/rpc",
            public_rpc_urls={"arbitrum": "https://arb-public.example/rpc"},
        )
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch.object(gw, "_check_archive_rpc_availability"))
            stack.enter_context(patch.object(gw, "_stop_anvil_forks", AsyncMock()))
            with pytest.raises(RuntimeError, match="Failed to start Anvil fork"):
                await gw._start_anvil_forks()

        assert rfm_cls.call_count == 1

    @pytest.mark.asyncio
    async def test_fallback_also_failing_raises(self):
        """Both primary and public fallback fail to start: RuntimeError."""
        gw = self._make_gateway(["arbitrum"])
        fake_env: dict[str, str] = {}
        rfm_cls, patches = self._patches(
            fake_env,
            managers=[self._manager(start_ok=False), self._manager(start_ok=False)],
            free_ports=[7101, 7102],
            public_rpc_urls={"arbitrum": "https://arb-public.example/rpc"},
        )
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch.object(gw, "_check_archive_rpc_availability"))
            stop = stack.enter_context(patch.object(gw, "_stop_anvil_forks", AsyncMock()))
            with pytest.raises(RuntimeError, match="Failed to start Anvil fork"):
                await gw._start_anvil_forks()

        assert rfm_cls.call_count == 2
        stop.assert_awaited_once_with(force=True)

    @pytest.mark.asyncio
    async def test_failure_on_second_chain_triggers_forced_cleanup(self):
        """A later chain failing must force-stop forks already started."""
        gw = self._make_gateway(["arbitrum", "base"])
        fake_env: dict[str, str] = {}
        first_ok = self._manager(start_ok=True)
        second_fail = self._manager(start_ok=False)
        rfm_cls, patches = self._patches(
            fake_env,
            managers=[first_ok, second_fail],
            free_ports=[7101, 7102],
            public_rpc_urls={},
        )
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch.object(gw, "_check_archive_rpc_availability"))
            stop = stack.enter_context(patch.object(gw, "_stop_anvil_forks", AsyncMock()))
            with pytest.raises(RuntimeError, match="Failed to start Anvil fork for base"):
                await gw._start_anvil_forks()

        # First chain had already registered its manager before the failure
        assert gw._anvil_managers == {"arbitrum": first_ok}
        stop.assert_awaited_once_with(force=True)


class TestResetAnvilForks:
    """Branch coverage for ManagedGateway.reset_anvil_forks (thread-safe reset)."""

    def _make_gateway(self):
        settings = GatewaySettings(
            grpc_port=50098,
            metrics_enabled=False,
            audit_enabled=False,
            allow_insecure=True,
            network="anvil",
        )
        return ManagedGateway(settings, anvil_chains=["arbitrum"])

    @pytest.fixture
    def bg_loop(self):
        """A real event loop running in a background thread (mirrors serve())."""
        import asyncio

        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        yield loop
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()

    def test_no_managers_returns_false(self):
        gw = self._make_gateway()
        assert gw.reset_anvil_forks() is False

    def test_missing_loop_returns_false(self):
        gw = self._make_gateway()
        gw._anvil_managers["arbitrum"] = AsyncMock()
        gw._loop = None
        assert gw.reset_anvil_forks() is False

    def test_closed_loop_returns_false(self):
        import asyncio

        gw = self._make_gateway()
        gw._anvil_managers["arbitrum"] = AsyncMock()
        loop = asyncio.new_event_loop()
        loop.close()
        gw._loop = loop
        assert gw.reset_anvil_forks() is False

    def test_all_resets_succeed_returns_true_and_refunds(self, bg_loop):
        gw = self._make_gateway()
        m1 = AsyncMock()
        m1.reset_to_latest.return_value = True
        m2 = AsyncMock()
        m2.reset_to_latest.return_value = True
        gw._anvil_managers = {"arbitrum": m1, "base": m2}
        gw._loop = bg_loop
        with patch.object(gw, "_fund_anvil_wallets", AsyncMock()) as fund:
            assert gw.reset_anvil_forks() is True
        m1.reset_to_latest.assert_awaited_once()
        m2.reset_to_latest.assert_awaited_once()
        fund.assert_awaited_once()
        # Watchdog guard set must be restored after the reset completes
        assert gw._resetting_chains == set()

    def test_single_reset_failure_returns_false_and_skips_funding(self, bg_loop):
        gw = self._make_gateway()
        failing = AsyncMock()
        failing.reset_to_latest.return_value = False
        gw._anvil_managers = {"arbitrum": failing}
        gw._loop = bg_loop
        with patch.object(gw, "_fund_anvil_wallets", AsyncMock()) as fund:
            assert gw.reset_anvil_forks() is False
        fund.assert_not_awaited()
        assert gw._resetting_chains == set()

    def test_reset_exception_returns_false(self, bg_loop):
        gw = self._make_gateway()
        exploding = AsyncMock()
        exploding.reset_to_latest.side_effect = RuntimeError("anvil gone")
        gw._anvil_managers = {"arbitrum": exploding}
        gw._loop = bg_loop
        with patch.object(gw, "_fund_anvil_wallets", AsyncMock()) as fund:
            assert gw.reset_anvil_forks() is False
        fund.assert_not_awaited()
        assert gw._resetting_chains == set()

    def test_timeout_returns_false(self, bg_loop):
        import concurrent.futures

        gw = self._make_gateway()
        gw._anvil_managers = {"arbitrum": AsyncMock()}
        gw._loop = bg_loop

        class _NeverDoneFuture:
            def result(self, timeout=None):
                raise concurrent.futures.TimeoutError()

        def fake_run_coroutine_threadsafe(coro, loop):
            coro.close()  # avoid "coroutine never awaited" warnings
            return _NeverDoneFuture()

        with patch(
            "almanak.gateway.managed.asyncio.run_coroutine_threadsafe",
            side_effect=fake_run_coroutine_threadsafe,
        ):
            assert gw.reset_anvil_forks() is False
