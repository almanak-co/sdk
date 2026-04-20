"""Unit tests for RollingForkManager Anvil flag detection and command building."""

from unittest.mock import patch

import pytest

import almanak.framework.anvil.fork_manager as fm
from almanak.framework.anvil.fork_manager import (
    RollingForkManager,
    _get_anvil_supported_flags,
)


def _clear_flags_cache():
    """Reset the module-level flags cache between tests."""
    fm._cached_anvil_flags = None
    fm._anvil_flags_detected = False


class TestGetAnvilSupportedFlags:
    """Tests for _get_anvil_supported_flags()."""

    def setup_method(self):
        _clear_flags_cache()

    def teardown_method(self):
        _clear_flags_cache()

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_parses_flags_from_help(self, mock_run):
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = (
            "Usage: anvil [OPTIONS]\n\n"
            "Options:\n"
            "  --fork-url <URL>   Fork from URL\n"
            "  --port <PORT>      Listen on port\n"
            "  --cache-path <P>   Cache path\n"
            "  --silent           Silent mode\n"
        )
        flags = _get_anvil_supported_flags()
        assert "--cache-path" in flags
        assert "--fork-url" in flags
        assert "--silent" in flags

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_returns_empty_on_failure(self, mock_run):
        mock_run.side_effect = FileNotFoundError("anvil not found")
        assert _get_anvil_supported_flags() == set()

    @patch("almanak.framework.anvil.fork_manager.subprocess.run")
    def test_non_zero_returncode_not_cached(self, mock_run):
        """Non-zero returncode should not cache and should return empty set."""
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = "error"
        assert _get_anvil_supported_flags() == set()
        # Should retry on next call (not cached)
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "--fork-url --cache-path"
        flags = _get_anvil_supported_flags()
        assert "--cache-path" in flags

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
        mock_run.return_value.stdout = "--fork-url --cache-path"
        flags = _get_anvil_supported_flags()
        assert "--cache-path" in flags


class TestBuildAnvilCommand:
    """Tests for _build_anvil_command() base fee and gas flags."""

    def _make_manager(self) -> RollingForkManager:
        return RollingForkManager(
            rpc_url="https://eth-mainnet.example.com",
            chain="ethereum",
            anvil_port=8545,
        )

    def test_always_includes_block_base_fee_per_gas_0(self):
        """--block-base-fee-per-gas 0 must always be present regardless of Anvil version."""
        mgr = self._make_manager()
        cmd = mgr._build_anvil_command()
        assert "--block-base-fee-per-gas" in cmd
        idx = cmd.index("--block-base-fee-per-gas")
        assert cmd[idx + 1] == "0"

    def test_never_includes_no_gas_cap(self):
        """--no-gas-cap must never appear — it's version-specific and has been removed."""
        mgr = self._make_manager()
        cmd = mgr._build_anvil_command()
        assert "--no-gas-cap" not in cmd

    def test_always_includes_timeout_and_retries(self):
        mgr = self._make_manager()
        cmd = mgr._build_anvil_command()
        assert "--timeout" in cmd
        assert "--retries" in cmd
        assert "--silent" in cmd


class TestGetTokenBalance:
    """Fix #2: _get_token_balance must handle empty hex '0x' responses."""

    @pytest.fixture()
    def manager(self):
        _clear_flags_cache()
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
