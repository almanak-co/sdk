"""Unit tests for RollingForkManager Anvil version detection and command building."""

from unittest.mock import patch

import pytest

import almanak.framework.anvil.fork_manager as fm
from almanak.framework.anvil.fork_manager import (
    RollingForkManager,
    _anvil_supports_no_gas_cap,
    _get_anvil_supported_flags,
    _get_anvil_version,
)


def _clear_version_cache():
    """Reset the module-level version cache between tests."""
    fm._cached_anvil_version = None
    fm._anvil_version_detected = False
    fm._cached_anvil_flags = None
    fm._anvil_flags_detected = False


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
    def test_parses_foundry_stable_suffix(self, mock_run):
        """Foundry 1.5.1-stable should parse as (1, 5, 1)."""
        mock_run.return_value.stdout = "anvil 1.5.1-stable (abc1234 2026-03-15)"
        assert _get_anvil_version() == (1, 5, 1)

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


class TestGetAnvilSupportedFlags:
    """Tests for _get_anvil_supported_flags()."""

    def setup_method(self):
        _clear_version_cache()

    def teardown_method(self):
        _clear_version_cache()

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_parses_flags_from_help(self, mock_run):
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = (
            "Usage: anvil [OPTIONS]\n\n"
            "Options:\n"
            "  --fork-url <URL>   Fork from URL\n"
            "  --port <PORT>      Listen on port\n"
            "  --no-gas-cap       Disable gas cap\n"
            "  --silent           Silent mode\n"
        )
        flags = _get_anvil_supported_flags()
        assert "--no-gas-cap" in flags
        assert "--fork-url" in flags
        assert "--silent" in flags

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_returns_empty_on_failure(self, mock_run):
        mock_run.side_effect = FileNotFoundError("anvil not found")
        assert _get_anvil_supported_flags() == set()

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_help_without_no_gas_cap(self, mock_run):
        """Newer Foundry (1.x) that removed --no-gas-cap."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = (
            "Usage: anvil [OPTIONS]\n\n"
            "Options:\n"
            "  --fork-url <URL>   Fork from URL\n"
            "  --port <PORT>      Listen on port\n"
            "  --silent           Silent mode\n"
        )
        flags = _get_anvil_supported_flags()
        assert "--no-gas-cap" not in flags
        assert "--fork-url" in flags

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_non_zero_returncode_not_cached(self, mock_run):
        """Non-zero returncode should not cache and should return empty set."""
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = "error"
        assert _get_anvil_supported_flags() == set()
        # Should retry on next call (not cached)
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "--fork-url --no-gas-cap"
        flags = _get_anvil_supported_flags()
        assert "--no-gas-cap" in flags

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_caches_successful_detection(self, mock_run):
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "--fork-url --silent"
        flags1 = _get_anvil_supported_flags()
        assert mock_run.call_count == 1
        flags2 = _get_anvil_supported_flags()
        assert mock_run.call_count == 1
        assert flags1 == flags2

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_transient_failure_not_cached(self, mock_run):
        mock_run.side_effect = FileNotFoundError("anvil not found")
        assert _get_anvil_supported_flags() == set()

        mock_run.side_effect = None
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "--fork-url --no-gas-cap"
        flags = _get_anvil_supported_flags()
        assert "--no-gas-cap" in flags


class TestAnvilSupportsNoGasCap:
    """Tests for _anvil_supports_no_gas_cap()."""

    def setup_method(self):
        _clear_version_cache()

    def teardown_method(self):
        _clear_version_cache()

    @patch("almanak.framework.anvil.fork_manager._get_anvil_supported_flags")
    def test_returns_true_when_flag_in_help(self, mock_flags):
        mock_flags.return_value = {"--fork-url", "--no-gas-cap", "--silent"}
        assert _anvil_supports_no_gas_cap() is True

    @patch("almanak.framework.anvil.fork_manager._get_anvil_supported_flags")
    def test_returns_false_when_flag_not_in_help(self, mock_flags):
        """Foundry 1.5.x removed the flag — should return False."""
        mock_flags.return_value = {"--fork-url", "--silent"}
        assert _anvil_supports_no_gas_cap() is False

    @patch("almanak.framework.anvil.fork_manager._get_anvil_version")
    @patch("almanak.framework.anvil.fork_manager._get_anvil_supported_flags")
    def test_falls_back_to_version_when_help_fails(self, mock_flags, mock_ver):
        """When help probe fails (empty set), fall back to version check."""
        mock_flags.return_value = set()  # help probe failed
        mock_ver.return_value = (0, 4, 0)
        assert _anvil_supports_no_gas_cap() is True

    @patch("almanak.framework.anvil.fork_manager._get_anvil_version")
    @patch("almanak.framework.anvil.fork_manager._get_anvil_supported_flags")
    def test_version_fallback_old_version(self, mock_flags, mock_ver):
        """Help probe fails + old version = False."""
        mock_flags.return_value = set()
        mock_ver.return_value = (0, 3, 0)
        assert _anvil_supports_no_gas_cap() is False

    @patch("almanak.framework.anvil.fork_manager._get_anvil_version")
    @patch("almanak.framework.anvil.fork_manager._get_anvil_supported_flags")
    def test_version_fallback_foundry_1x_returns_false(self, mock_flags, mock_ver):
        """Foundry 1.x removed --no-gas-cap; version fallback must reject 1.x."""
        mock_flags.return_value = set()  # help probe failed
        mock_ver.return_value = (1, 5, 1)
        assert _anvil_supports_no_gas_cap() is False

    @patch("almanak.framework.anvil.fork_manager._get_anvil_version")
    @patch("almanak.framework.anvil.fork_manager._get_anvil_supported_flags")
    def test_version_fallback_foundry_1_0_0_returns_false(self, mock_flags, mock_ver):
        """Boundary: 1.0.0 is outside the 0.4.x range."""
        mock_flags.return_value = set()
        mock_ver.return_value = (1, 0, 0)
        assert _anvil_supports_no_gas_cap() is False

    @patch("almanak.framework.anvil.fork_manager._get_anvil_version")
    @patch("almanak.framework.anvil.fork_manager._get_anvil_supported_flags")
    def test_both_fail_returns_false(self, mock_flags, mock_ver):
        """Both help probe and version detection fail = fail-safe False."""
        mock_flags.return_value = set()
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
