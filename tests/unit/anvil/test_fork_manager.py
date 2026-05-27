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
        """Mantle gets ``--gas-limit 3B`` (VIB-3666 / VIB-3746 / #2103).

        The numeric ceiling must be high enough to admit the lp_mint per-tx
        gas_limit (1B compiler estimate × 1.5x framework gas buffer = 1.5B);
        3B leaves comfortable headroom. ``--disable-block-gas-limit`` is not
        used because Anvil 1.7.x rejects combining it with ``--gas-limit``
        and using it alone showed receipt-not-mined hangs in CI.
        """
        mgr = RollingForkManager(
            rpc_url="https://mantle.example.com",
            chain="mantle",
            anvil_port=8545,
            cache_path=None,
        )
        cmd = mgr._build_anvil_command()
        assert "--disable-block-gas-limit" not in cmd
        assert "--gas-limit" in cmd
        idx = cmd.index("--gas-limit")
        assert cmd[idx + 1] == "3000000000"

    def test_block_gas_limit_legacy_flag_never_used(self):
        """Sanity: legacy ``--block-gas-limit`` flag must not appear (VIB-3746).

        Anvil does not expose ``--block-gas-limit``; passing it would crash older
        builds and is silently dropped on newer ones. The override is wired to
        ``--gas-limit`` instead.
        """
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


# =============================================================================
# Regression guards for the FiatToken-proxy funding bug (e.g. cbBTC on Base):
# brute-force slot probing would write to slot 3 (the `blacklisted` mapping in
# Circle's FiatTokenV2_2 storage layout) before landing on slot 9 (`_balances`),
# blacklisting the wallet so every subsequent approve/transfer reverted with
# "Blacklistable: account is blacklisted". The fixes below are tested here.
# =============================================================================


def _make_rpc_dispatcher(handlers: dict):
    """Build an AsyncMock side_effect for `_rpc_call_raw` that dispatches by method.

    Each handler value is either:
      - a callable `fn(params) -> (success, result)` invoked per call
      - a static `(success, result)` tuple returned every time
    Unknown methods raise AssertionError so the test fails loudly.
    """

    async def _dispatch(method, params):
        if method not in handlers:
            raise AssertionError(f"Unexpected RPC call: {method} {params!r}")
        h = handlers[method]
        return h(params) if callable(h) else h

    return AsyncMock(side_effect=_dispatch)


class TestFundTokenViaStorageSnapshotRevert:
    """Wrong-slot writes must be reverted before the next probe attempt.

    Without snapshot/revert, slot probing on Coinbase-style FiatToken proxies
    would leave the wallet blacklisted (slot 3 = blacklisted mapping) before
    reaching the right balance slot. These tests guard the snapshot/revert
    wrapper around each iteration of `_fund_token_via_storage`.
    """

    WALLET = "0x" + "a" * 40
    TOKEN = "0x" + "b" * 40
    AMOUNT_HEX = "0x" + (1_000_000).to_bytes(32, "big").hex()  # 1e6 token units

    @pytest.fixture()
    def manager(self):
        _clear_flags_cache()
        mgr = RollingForkManager(rpc_url="http://rpc.test", chain="base", anvil_port=9999)
        mgr._is_running = True
        return mgr

    @pytest.mark.asyncio()
    async def test_wrong_slot_writes_are_reverted(self, manager):
        """For each non-matching slot probed, evm_revert MUST be called before the next snapshot."""
        # Make slot 9 the "correct" one: balanceOf only returns the expected
        # value on the 7th call (slot 9 is index 6 in [0,1,2,3,4,5,9,51,52]).
        handlers = {
            "evm_snapshot": (True, "0xsnap"),
            "anvil_setStorageAt": (True, None),
            "evm_mine": (True, None),
            "evm_revert": (True, True),
        }
        rpc_mock = _make_rpc_dispatcher(handlers)

        balance_call_count = 0

        async def fake_balance(_token, _wallet):
            nonlocal balance_call_count
            balance_call_count += 1
            return 1_000_000 if balance_call_count == 7 else 0

        with (
            patch.object(manager, "_rpc_call_raw", rpc_mock),
            patch.object(manager, "_get_token_balance", side_effect=fake_balance),
        ):
            result = await manager._fund_token_via_storage(self.WALLET, self.TOKEN, self.AMOUNT_HEX, "TEST")

        assert result is True
        # Exactly 6 evm_revert calls — one per wrong slot (0,1,2,3,4,5). The
        # matching slot 9 keeps its snapshot uncommitted (no revert).
        revert_calls = [c for c in rpc_mock.call_args_list if c[0][0] == "evm_revert"]
        assert len(revert_calls) == 6, f"Expected 6 reverts, got {len(revert_calls)}"
        # And exactly 7 snapshots (one per attempted slot up to and including slot 9)
        snap_calls = [c for c in rpc_mock.call_args_list if c[0][0] == "evm_snapshot"]
        assert len(snap_calls) == 7
        # Ordering: each wrong-slot snapshot must be reverted BEFORE the next
        # iteration takes its snapshot. A regression that batches reverts at
        # the end would still satisfy the counts above but leave wrong-slot
        # writes visible to subsequent probes — defeating the snapshot fix.
        methods = [c[0][0] for c in rpc_mock.call_args_list]
        snapshot_positions = [i for i, m in enumerate(methods) if m == "evm_snapshot"]
        revert_positions = [i for i, m in enumerate(methods) if m == "evm_revert"]
        for k, rev_pos in enumerate(revert_positions):
            next_snap_pos = snapshot_positions[k + 1]
            assert rev_pos < next_snap_pos, (
                f"Revert for iteration {k} (pos {rev_pos}) must come before next snapshot (pos {next_snap_pos})"
            )

    @pytest.mark.asyncio()
    async def test_aborts_when_snapshot_unsupported(self, manager):
        """If evm_snapshot returns (False, _), probing must abort without writes."""
        handlers = {
            "evm_snapshot": (False, None),
            # If anvil_setStorageAt or evm_revert get called, the dispatcher
            # asserts — that itself would fail the test.
        }
        rpc_mock = _make_rpc_dispatcher(handlers)
        with (
            patch.object(manager, "_rpc_call_raw", rpc_mock),
            patch.object(manager, "_get_token_balance", AsyncMock(return_value=0)),
        ):
            result = await manager._fund_token_via_storage(self.WALLET, self.TOKEN, self.AMOUNT_HEX, "TEST")

        assert result is False
        set_storage_calls = [c for c in rpc_mock.call_args_list if c[0][0] == "anvil_setStorageAt"]
        assert len(set_storage_calls) == 0, "Must NOT write storage when snapshot is unavailable"

class TestFundTokenViaWhaleGasFunding:
    """Whale impersonation must work even when the whale is a contract with 0 ETH.

    Many realistic whales (Aave aTokens, Morpho vaults) hold large token reserves
    but carry no native gas, so eth_sendTransaction would fail. The fix tops up
    the whale conditionally (only when it has < 0.1 ETH) and restores the
    original balance on exit so the fork's observable state is unchanged.
    """

    WALLET = "0x" + "a" * 40
    TOKEN = "0x" + "b" * 40
    WHALE = "0x" + "c" * 40
    AMOUNT_HEX = "0x" + (1_000_000).to_bytes(32, "big").hex()

    @pytest.fixture()
    def manager(self):
        _clear_flags_cache()
        mgr = RollingForkManager(rpc_url="http://rpc.test", chain="base", anvil_port=9999)
        mgr._is_running = True
        return mgr

    @pytest.mark.asyncio()
    async def test_tops_up_whale_when_balance_low(self, manager):
        """anvil_setBalance must be called when the whale has < 0.1 ETH."""
        handlers = {
            "anvil_impersonateAccount": (True, None),
            "eth_getBalance": (True, "0x0"),  # 0 ETH — needs top-up
            "anvil_setBalance": (True, None),
            "eth_sendTransaction": (True, "0xtxhash"),
            "evm_mine": (True, None),
            "anvil_stopImpersonatingAccount": (True, None),
        }
        rpc_mock = _make_rpc_dispatcher(handlers)
        with (
            patch.object(manager, "_rpc_call_raw", rpc_mock),
            patch.object(manager, "_get_token_balance", AsyncMock(return_value=1_000_000)),
        ):
            result = await manager._fund_token_via_whale(self.WALLET, self.TOKEN, self.AMOUNT_HEX, self.WHALE, "TEST")

        assert result is True
        setbalance_calls = [c for c in rpc_mock.call_args_list if c[0][0] == "anvil_setBalance"]
        # Two setBalance calls expected: top-up to 1 ETH, then restore to 0x0
        assert len(setbalance_calls) == 2, f"Expected 2 setBalance calls (topup + restore), got {len(setbalance_calls)}"
        topup_args = setbalance_calls[0][0][1]
        assert topup_args[0] == self.WHALE
        assert int(topup_args[1], 16) == 10**18, "Top-up must be 1 ETH"

    @pytest.mark.asyncio()
    async def test_skips_topup_when_whale_has_enough_eth(self, manager):
        """When whale has >= 0.1 ETH, anvil_setBalance must NOT be called."""
        existing_balance_hex = "0x" + format(5 * 10**17, "x")  # 0.5 ETH
        handlers = {
            "anvil_impersonateAccount": (True, None),
            "eth_getBalance": (True, existing_balance_hex),
            # setBalance MUST NOT be called — would raise via dispatcher
            "eth_sendTransaction": (True, "0xtxhash"),
            "evm_mine": (True, None),
            "anvil_stopImpersonatingAccount": (True, None),
        }
        rpc_mock = _make_rpc_dispatcher(handlers)
        with (
            patch.object(manager, "_rpc_call_raw", rpc_mock),
            patch.object(manager, "_get_token_balance", AsyncMock(return_value=1_000_000)),
        ):
            result = await manager._fund_token_via_whale(self.WALLET, self.TOKEN, self.AMOUNT_HEX, self.WHALE, "TEST")

        assert result is True
        setbalance_calls = [c for c in rpc_mock.call_args_list if c[0][0] == "anvil_setBalance"]
        assert len(setbalance_calls) == 0, "Whale with sufficient ETH must not be perturbed"

    @pytest.mark.asyncio()
    async def test_restores_original_balance_on_exit(self, manager):
        """After topping up, the whale's original balance must be restored."""
        original_balance_hex = "0x1234"
        handlers = {
            "anvil_impersonateAccount": (True, None),
            "eth_getBalance": (True, original_balance_hex),
            "anvil_setBalance": (True, None),
            "eth_sendTransaction": (True, "0xtxhash"),
            "evm_mine": (True, None),
            "anvil_stopImpersonatingAccount": (True, None),
        }
        rpc_mock = _make_rpc_dispatcher(handlers)
        with (
            patch.object(manager, "_rpc_call_raw", rpc_mock),
            patch.object(manager, "_get_token_balance", AsyncMock(return_value=1_000_000)),
        ):
            await manager._fund_token_via_whale(self.WALLET, self.TOKEN, self.AMOUNT_HEX, self.WHALE, "TEST")

        setbalance_calls = [c for c in rpc_mock.call_args_list if c[0][0] == "anvil_setBalance"]
        assert len(setbalance_calls) == 2
        # Second call (the restore) must reference the ORIGINAL balance hex.
        restore_args = setbalance_calls[1][0][1]
        assert restore_args[0] == self.WHALE
        assert restore_args[1] == original_balance_hex, "Restore must use the original balance"

def test_cbbtc_base_whale_entry_present():
    """cbBTC on Base must be in the whale list — guards against accidental deletion
    of the entry that prevents storage probing from corrupting FiatTokenV2_2 state.
    """
    assert "base" in fm.WHALE_FUNDED_TOKENS
    assert "CBBTC" in fm.WHALE_FUNDED_TOKENS["base"]
