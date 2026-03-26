"""Unit tests for RollingForkManager Anvil version detection and command building."""

from unittest.mock import patch

import pytest

import almanak.framework.anvil.fork_manager as fm
from almanak.framework.anvil.fork_manager import (
    RollingForkManager,
    _anvil_supports_no_gas_cap,
    _get_anvil_version,
)


def _clear_version_cache():
    """Reset the module-level version cache between tests."""
    fm._cached_anvil_version = None
    fm._anvil_version_detected = False


class TestGetAnvilVersion:
    """Tests for _get_anvil_version()."""

    def setup_method(self):
        _clear_version_cache()

    def teardown_method(self):
        _clear_version_cache()

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_parses_standard_format(self, mock_run):
        mock_run.return_value.stdout = "anvil 0.3.0 (5a8bd89 2024-12-19)"
        assert _get_anvil_version() == (0, 3, 0)

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_parses_newer_version(self, mock_run):
        mock_run.return_value.stdout = "anvil 0.4.1 (abc1234 2025-06-01)"
        assert _get_anvil_version() == (0, 4, 1)

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_parses_major_version(self, mock_run):
        mock_run.return_value.stdout = "anvil 1.0.0"
        assert _get_anvil_version() == (1, 0, 0)

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_returns_none_on_unexpected_format(self, mock_run):
        mock_run.return_value.stdout = "some unexpected output"
        assert _get_anvil_version() is None

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_returns_none_on_empty_output(self, mock_run):
        mock_run.return_value.stdout = ""
        assert _get_anvil_version() is None

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_returns_none_on_exception(self, mock_run):
        mock_run.side_effect = FileNotFoundError("anvil not found")
        assert _get_anvil_version() is None

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_returns_none_on_timeout(self, mock_run):
        import subprocess

        mock_run.side_effect = subprocess.TimeoutExpired(cmd="anvil", timeout=5)
        assert _get_anvil_version() is None

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_transient_failure_not_cached(self, mock_run):
        """Transient failures should NOT be cached — retried on next call."""
        mock_run.side_effect = FileNotFoundError("anvil not found")
        assert _get_anvil_version() is None

        # Second call after anvil becomes available should succeed
        mock_run.side_effect = None
        mock_run.return_value.stdout = "anvil 0.4.0"
        assert _get_anvil_version() == (0, 4, 0)

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_successful_detection_is_cached(self, mock_run):
        """Successful detections should be cached (no repeated subprocess calls)."""
        mock_run.return_value.stdout = "anvil 0.4.0"
        assert _get_anvil_version() == (0, 4, 0)
        assert mock_run.call_count == 1

        # Second call should use cache, not call subprocess again
        assert _get_anvil_version() == (0, 4, 0)
        assert mock_run.call_count == 1


class TestAnvilSupportsNoGasCap:
    """Tests for _anvil_supports_no_gas_cap()."""

    def setup_method(self):
        _clear_version_cache()

    def teardown_method(self):
        _clear_version_cache()

    @patch("almanak.framework.anvil.fork_manager._get_anvil_version")
    def test_returns_false_for_0_3_0(self, mock_ver):
        mock_ver.return_value = (0, 3, 0)
        assert _anvil_supports_no_gas_cap() is False

    @patch("almanak.framework.anvil.fork_manager._get_anvil_version")
    def test_returns_true_for_0_4_0(self, mock_ver):
        mock_ver.return_value = (0, 4, 0)
        assert _anvil_supports_no_gas_cap() is True

    @patch("almanak.framework.anvil.fork_manager._get_anvil_version")
    def test_returns_true_for_1_0_0(self, mock_ver):
        mock_ver.return_value = (1, 0, 0)
        assert _anvil_supports_no_gas_cap() is True

    @patch("almanak.framework.anvil.fork_manager._get_anvil_version")
    def test_returns_false_when_detection_fails(self, mock_ver):
        mock_ver.return_value = None
        assert _anvil_supports_no_gas_cap() is False


class TestBuildAnvilCommand:
    """Tests for _build_anvil_command() gas cap flag."""

    def _make_manager(self) -> RollingForkManager:
        return RollingForkManager(
            rpc_url="https://eth-mainnet.example.com",
            chain="ethereum",
            anvil_port=8545,
        )

    @patch("almanak.framework.anvil.fork_manager._anvil_supports_no_gas_cap")
    def test_includes_no_gas_cap_when_supported(self, mock_supports):
        mock_supports.return_value = True
        mgr = self._make_manager()
        cmd = mgr._build_anvil_command()
        assert "--no-gas-cap" in cmd

    @patch("almanak.framework.anvil.fork_manager._anvil_supports_no_gas_cap")
    def test_excludes_no_gas_cap_when_unsupported(self, mock_supports):
        mock_supports.return_value = False
        mgr = self._make_manager()
        cmd = mgr._build_anvil_command()
        assert "--no-gas-cap" not in cmd

    @patch("almanak.framework.anvil.fork_manager._anvil_supports_no_gas_cap")
    def test_always_includes_timeout_and_retries(self, mock_supports):
        mock_supports.return_value = False
        mgr = self._make_manager()
        cmd = mgr._build_anvil_command()
        assert "--timeout" in cmd
        assert "--retries" in cmd
        assert "--silent" in cmd


class TestGetTokenBalance:
    """Fix #2: _get_token_balance must handle empty hex '0x' responses."""

    @pytest.fixture()
    def manager(self):
        _clear_version_cache()
        with patch("almanak.framework.anvil.fork_manager._get_anvil_version", return_value=(0, 2, 0)):
            mgr = RollingForkManager(rpc_url="http://rpc.test", chain="arbitrum", anvil_port=9999)
        return mgr

    @pytest.mark.asyncio()
    async def test_empty_hex_0x_returns_zero(self, manager):
        """'0x' (empty hex) from eth_call must return 0, not crash."""
        with patch.object(manager, "_rpc_call", return_value="0x"):
            result = await manager._get_token_balance("0x" + "a" * 40, "0x" + "b" * 40)
            assert result == 0

    @pytest.mark.asyncio()
    async def test_none_returns_zero(self, manager):
        """None from eth_call must return 0."""
        with patch.object(manager, "_rpc_call", return_value=None):
            result = await manager._get_token_balance("0x" + "a" * 40, "0x" + "b" * 40)
            assert result == 0

    @pytest.mark.asyncio()
    async def test_valid_hex_returns_int(self, manager):
        """Valid hex response must be parsed correctly."""
        with patch.object(manager, "_rpc_call", return_value="0x64"):
            result = await manager._get_token_balance("0x" + "a" * 40, "0x" + "b" * 40)
            assert result == 100

    @pytest.mark.asyncio()
    async def test_zero_balance_hex(self, manager):
        """0x0 (zero balance) must return 0."""
        with patch.object(manager, "_rpc_call", return_value="0x0"):
            result = await manager._get_token_balance("0x" + "a" * 40, "0x" + "b" * 40)
            assert result == 0
