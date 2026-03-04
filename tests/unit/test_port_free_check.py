"""Tests for ForkManager._wait_for_port_free with SO_REUSEADDR (VIB-432).

Verifies that the port-free check uses SO_REUSEADDR to avoid false negatives
from TCP TIME_WAIT connections after Anvil process termination.
"""

import asyncio
import socket
from unittest.mock import patch

import pytest

from almanak.framework.anvil.fork_manager import RollingForkManager


def _make_fork_manager(port: int = 18545) -> RollingForkManager:
    """Create a RollingForkManager with a test port."""
    return RollingForkManager(
        rpc_url="https://arb1.arbitrum.io/rpc",
        chain="arbitrum",
        anvil_port=port,
    )


class TestWaitForPortFree:
    """Tests for _wait_for_port_free SO_REUSEADDR behavior."""

    @pytest.mark.asyncio
    async def test_port_free_returns_immediately(self):
        """When port is free, _wait_for_port_free returns without delay."""
        # Find a real free port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            free_port = s.getsockname()[1]
        fm = _make_fork_manager(port=free_port)
        # Should return quickly without warning
        await fm._wait_for_port_free(timeout=1.0)

    @pytest.mark.asyncio
    async def test_port_in_use_waits_then_logs_debug(self):
        """When port is in use, waits until timeout then logs at debug level."""
        # Bind a blocker socket to hold a port
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        held_port = blocker.getsockname()[1]
        fm = _make_fork_manager(port=held_port)
        try:
            with patch("almanak.framework.anvil.fork_manager.logger") as mock_logger:
                await fm._wait_for_port_free(timeout=0.3)
                # Should log at debug level, NOT warning
                mock_logger.debug.assert_called_once()
                mock_logger.warning.assert_not_called()
                assert str(fm.anvil_port) in mock_logger.debug.call_args[0][0]
        finally:
            blocker.close()

    @pytest.mark.asyncio
    async def test_port_freed_mid_wait(self):
        """Port freed during wait returns before timeout."""
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        held_port = blocker.getsockname()[1]
        fm = _make_fork_manager(port=held_port)

        async def release_port():
            await asyncio.sleep(0.2)
            blocker.close()

        # Release port after 0.2s; timeout is 2s
        asyncio.create_task(release_port())
        await fm._wait_for_port_free(timeout=2.0)
        # If we get here without timeout, port was freed mid-wait

    @pytest.mark.asyncio
    async def test_check_socket_uses_so_reuseaddr(self):
        """Verify the check socket sets SO_REUSEADDR before bind."""
        fm = _make_fork_manager()
        original_socket = socket.socket

        so_reuseaddr_set = []

        class MockSocket:
            """Track SO_REUSEADDR usage on the check socket."""

            def __init__(self, *args, **kwargs):
                self._sock = original_socket(*args, **kwargs)

            def setsockopt(self, level, optname, value):
                if level == socket.SOL_SOCKET and optname == socket.SO_REUSEADDR:
                    so_reuseaddr_set.append(True)
                return self._sock.setsockopt(level, optname, value)

            def bind(self, addr):
                return self._sock.bind(addr)

            def close(self):
                return self._sock.close()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.close()

        with patch("almanak.framework.anvil.fork_manager.socket.socket", MockSocket):
            await fm._wait_for_port_free(timeout=0.5)

        assert len(so_reuseaddr_set) >= 1, "SO_REUSEADDR should be set on the check socket"
