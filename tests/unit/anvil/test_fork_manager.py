"""Unit tests for RollingForkManager Anvil flag detection and command building."""

from decimal import Decimal
from unittest.mock import AsyncMock, patch

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
        # cache_path=None keeps these tests env-independent. The default
        # picks up ANVIL_FORK_CACHE_PATH which would route through the
        # subprocess probe (--cache-path support detection) and turn this
        # into a non-unit test.
        return RollingForkManager(
            rpc_url="https://eth-mainnet.example.com",
            chain="ethereum",
            anvil_port=8545,
            cache_path=None,
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

    def test_gas_limit_included_for_mantle(self):
        """Mantle gets --gas-limit + --disable-block-gas-limit unconditionally
        (VIB-3666 / VIB-3746 / #2103).

        ``--gas-limit`` has been a stable Foundry/Anvil flag since 1.0; the
        previous ``_get_anvil_supported_flags()`` gate masked CI environments
        where the help-output probe returned an empty set, silently dropping
        the override and re-introducing the Mantle "intrinsic gas too high"
        failure on Foundry 1.7.0 in CI (issue #2103).

        Belt-and-suspenders: ``--disable-block-gas-limit`` removes Anvil's
        ``tx.gas_limit <= block.gas_limit`` check outright, which is the
        defense against Mantle's L1-calldata-included gas accounting
        producing tx-level estimates that exceed even a 1B block override.
        """
        _clear_flags_cache()
        mgr = RollingForkManager(
            rpc_url="https://mantle.example.com",
            chain="mantle",
            anvil_port=8545,
            cache_path=None,
        )
        cmd = mgr._build_anvil_command()
        assert "--gas-limit" in cmd
        idx = cmd.index("--gas-limit")
        assert cmd[idx + 1] == "1000000000"
        assert "--disable-block-gas-limit" in cmd

    def test_block_gas_limit_legacy_flag_never_used(self):
        """Sanity: legacy ``--block-gas-limit`` flag must not appear (VIB-3746).

        Anvil does not expose ``--block-gas-limit``; passing it would crash older
        builds and is silently dropped on newer ones. The override is now wired to
        ``--gas-limit`` instead.
        """
        _clear_flags_cache()
        mgr = RollingForkManager(
            rpc_url="https://mantle.example.com",
            chain="mantle",
            anvil_port=8545,
            cache_path=None,
        )
        cmd = mgr._build_anvil_command()
        assert "--block-gas-limit" not in cmd
        assert "--gas-limit" in cmd

    def test_gas_limit_not_included_for_non_override_chains(self):
        """Ethereum (no entry in _CHAIN_BLOCK_GAS_LIMITS) never gets --gas-limit
        or --disable-block-gas-limit — both are reserved for chains with
        non-standard gas accounting."""
        mgr = self._make_manager()  # chain="ethereum"
        cmd = mgr._build_anvil_command()
        assert "--gas-limit" not in cmd
        assert "--block-gas-limit" not in cmd
        assert "--disable-block-gas-limit" not in cmd


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


class TestFundTokensWrappedNativeFallback:
    """Test that fund_tokens falls back to storage-slot when deposit() fails.

    VIB-2690: WAVAX on Avalanche (and any other wrapped native) must fall back
    to known storage-slot / anvil_deal when the deposit() path fails silently
    (e.g., transient Alchemy RPC outage causes Anvil to use a public fallback
    RPC that doesn't support impersonation, or wallet balance exactly equals
    the deposit amount leaving nothing for gas).
    """

    WAVAX_ADDRESS = "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"
    WALLET = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"

    @pytest.fixture()
    def manager(self):
        _clear_flags_cache()
        mgr = RollingForkManager(
            rpc_url="http://rpc.test",
            chain="avalanche",
            anvil_port=9999,
        )
        # Pretend the fork is running: _is_running=True + process that poll()=None (alive)
        mgr._is_running = True
        patcher = patch("subprocess.Popen")
        mock_popen = patcher.start()
        mock_popen.poll.return_value = None  # process alive
        mgr._process = mock_popen
        yield mgr
        patcher.stop()

    @pytest.mark.asyncio()
    async def test_deposit_success_skips_slot(self, manager):
        """When deposit() succeeds, storage-slot path must NOT be called."""
        with (
            patch.object(manager, "_fund_wrapped_native_via_deposit", new_callable=AsyncMock, return_value=True),
            patch.object(manager, "_set_balance_at_slot", new_callable=AsyncMock) as mock_slot,
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(True, None)),
        ):
            result = await manager.fund_tokens(self.WALLET, {"WAVAX": Decimal("10")})
        assert result is True
        mock_slot.assert_not_called()

    @pytest.mark.asyncio()
    async def test_deposit_failure_falls_back_to_slot(self, manager):
        """When deposit() fails, fund_tokens must fall back to known storage slot 3."""
        with (
            patch.object(manager, "_fund_wrapped_native_via_deposit", new_callable=AsyncMock, return_value=False),
            patch.object(manager, "_set_balance_at_slot", new_callable=AsyncMock, return_value=True) as mock_slot,
            # anvil_deal not needed since slot succeeds; but mock to avoid real RPC calls
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(False, None)),
        ):
            result = await manager.fund_tokens(self.WALLET, {"WAVAX": Decimal("10")})
        assert result is True
        # Slot 3 is WAVAX's known slot on Avalanche — must have been called
        mock_slot.assert_called_once()
        call_args = mock_slot.call_args
        # _set_balance_at_slot(wallet_address, token_address, amount_hex, slot, symbol)
        # slot is the 4th positional arg (index 3)
        assert call_args[0][3] == 3, f"Expected slot 3 for WAVAX, got {call_args[0][3]}"

    @pytest.mark.asyncio()
    async def test_deposit_failure_falls_back_to_anvil_deal(self, manager):
        """When deposit() and slot both fail, anvil_deal must be tried."""
        with (
            patch.object(manager, "_fund_wrapped_native_via_deposit", new_callable=AsyncMock, return_value=False),
            patch.object(manager, "_set_balance_at_slot", new_callable=AsyncMock, return_value=False),
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(True, None)) as mock_rpc,
        ):
            result = await manager.fund_tokens(self.WALLET, {"WAVAX": Decimal("10")})
        assert result is True
        # anvil_deal should have been called (returns True = success)
        deal_calls = [c for c in mock_rpc.call_args_list if c[0][0] == "anvil_deal"]
        assert len(deal_calls) == 1, "anvil_deal must be called as fallback"
