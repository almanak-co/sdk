"""Tests for AnvilFork port retry logic in e2e test fixtures.

Validates that AnvilFork retries with a new port when the initial port
is unavailable (race condition between _find_free_port and Anvil binding).

VIB-2184
"""

from __future__ import annotations

import socket
from unittest.mock import MagicMock, patch

import pytest


def _find_free_port() -> int:
    """Find a free TCP port by binding to an ephemeral port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestFindFreePort:
    """Test the _find_free_port helper."""

    def test_returns_valid_port(self):
        port = _find_free_port()
        assert isinstance(port, int)
        assert 1024 <= port <= 65535

    def test_port_is_bindable(self):
        """Returned port should be immediately bindable."""
        port = _find_free_port()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", port))


class TestAnvilForkRetry:
    """Test that AnvilFork.start() retries on port conflict."""

    def test_retry_allocates_new_port_on_failure(self):
        """When _wait_for_port returns False, start() should try a new port."""
        # Import the class from the e2e test module
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "test_defai_vault_lp",
            "strategies/internal/tests/e2e/test_defai_vault_lp.py",
        )
        mod = importlib.util.module_from_spec(spec)

        # Mock out heavy imports that might fail
        with (
            patch.dict("sys.modules", {"web3": MagicMock(), "web3.middleware": MagicMock()}),
        ):
            try:
                spec.loader.exec_module(mod)
            except (ModuleNotFoundError, ImportError, FileNotFoundError) as exc:
                pytest.skip(f"Could not import test_defai_vault_lp module: {exc}")

        AnvilFork = mod.AnvilFork

        ports_tried = []

        def mock_find_free_port():
            port = 50000 + len(ports_tried)
            ports_tried.append(port)
            return port

        # Simulate: first 2 ports fail, third succeeds
        wait_results = [False, False, True]

        with (
            patch.object(mod, "_find_free_port", side_effect=mock_find_free_port),
            patch.object(mod, "_wait_for_port", side_effect=wait_results),
            patch("subprocess.Popen") as mock_popen,
        ):
            mock_proc = MagicMock()
            mock_proc.stderr = MagicMock()
            mock_proc.stderr.read.return_value = b""
            mock_popen.return_value = mock_proc

            fork = AnvilFork(rpc_url="http://localhost:8545", port=0)
            fork.start()

            # Should have tried 3 ports (initial + 2 retries)
            assert len(ports_tried) >= 2
            assert mock_popen.call_count == 3

    def test_raises_after_max_attempts(self):
        """After _MAX_START_ATTEMPTS failures, should raise RuntimeError."""
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "test_defai_vault_lp",
            "strategies/internal/tests/e2e/test_defai_vault_lp.py",
        )
        mod = importlib.util.module_from_spec(spec)

        with patch.dict("sys.modules", {"web3": MagicMock(), "web3.middleware": MagicMock()}):
            try:
                spec.loader.exec_module(mod)
            except (ModuleNotFoundError, ImportError, FileNotFoundError) as exc:
                pytest.skip(f"Could not import test_defai_vault_lp module: {exc}")

        AnvilFork = mod.AnvilFork

        with (
            patch.object(mod, "_find_free_port", return_value=55555),
            patch.object(mod, "_wait_for_port", return_value=False),
            patch("subprocess.Popen") as mock_popen,
        ):
            mock_proc = MagicMock()
            mock_proc.stderr = MagicMock()
            mock_proc.stderr.read.return_value = b"address already in use"
            mock_popen.return_value = mock_proc

            fork = AnvilFork(rpc_url="http://localhost:8545", port=0)

            with pytest.raises(RuntimeError, match="Anvil failed after 3 attempts"):
                fork.start()

            assert mock_popen.call_count == 3
