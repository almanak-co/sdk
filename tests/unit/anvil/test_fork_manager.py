"""Unit tests for RollingForkManager Anvil flag detection and command building."""

from decimal import Decimal
from unittest.mock import AsyncMock, PropertyMock, patch

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


class TestKeepAliveDetached:
    """VIB-5063: a `--keep-anvil` fork must be spawned in its own session so it
    survives the runner's exit / process-group signals for a post-teardown audit.
    Off by default so a normal run's Anvil stays in the runner's group."""

    async def _start_and_capture_popen(self, *, keep_alive_detached: bool):
        _clear_flags_cache()
        mgr = RollingForkManager(
            rpc_url="http://rpc.test",
            chain="avalanche",
            anvil_port=9999,
            keep_alive_detached=keep_alive_detached,
        )
        with (
            patch.object(mgr, "_validate_source_chain_id", new_callable=AsyncMock),
            patch.object(mgr, "_wait_for_ready", new_callable=AsyncMock, return_value=True),
            patch.object(mgr, "_rpc_call_raw", new_callable=AsyncMock, return_value=(True, None)),
            patch("almanak.framework.anvil.fork_manager.subprocess.Popen") as mock_popen,
        ):
            ok = await mgr.start()
        assert ok is True
        return mock_popen.call_args.kwargs

    @pytest.mark.asyncio()
    async def test_detached_when_keep_alive(self):
        kwargs = await self._start_and_capture_popen(keep_alive_detached=True)
        assert kwargs.get("start_new_session") is True

    @pytest.mark.asyncio()
    async def test_grouped_by_default(self):
        kwargs = await self._start_and_capture_popen(keep_alive_detached=False)
        assert kwargs.get("start_new_session") is False


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
    to known storage-slot / anvil_dealERC20 when the deposit() path fails silently
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
            result = await manager.fund_tokens(self.WALLET, {self.WAVAX_ADDRESS: Decimal("10")})
        assert result is True
        mock_slot.assert_not_called()

    @pytest.mark.asyncio()
    async def test_deposit_failure_falls_back_to_slot(self, manager):
        """When deposit() fails, fund_tokens must fall back to known storage slot 3."""
        with (
            patch.object(manager, "_fund_wrapped_native_via_deposit", new_callable=AsyncMock, return_value=False),
            patch.object(manager, "_set_balance_at_slot", new_callable=AsyncMock, return_value=True) as mock_slot,
            # anvil_dealERC20 not needed since slot succeeds; but mock to avoid real RPC calls
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(False, None)),
        ):
            result = await manager.fund_tokens(self.WALLET, {self.WAVAX_ADDRESS: Decimal("10")})
        assert result is True
        # Slot 3 is WAVAX's known slot on Avalanche — must have been called
        mock_slot.assert_called_once()
        call_args = mock_slot.call_args
        # _set_balance_at_slot(wallet_address, token_address, amount_hex, slot, symbol)
        # slot is the 4th positional arg (index 3)
        assert call_args[0][3] == 3, f"Expected slot 3 for WAVAX, got {call_args[0][3]}"

    @pytest.mark.asyncio()
    async def test_deposit_failure_falls_back_to_anvil_deal_erc20(self, manager):
        """When deposit() and slot both fail, anvil_dealERC20 must be tried.

        Regression (2026-08-14 funding probe): this tier used to call
        "anvil_deal", which has never been a Foundry RPC method — every call
        returned -32601 and the tier was silently dead. The assertions below
        pin both the real method name and its (account, token, balance) param
        order so neither can regress unnoticed.

        The tier order is asserted too: the cheap known-slot write must be
        attempted BEFORE the node-side slot search, otherwise every funding call
        would pay for a trace it usually does not need.
        """
        events: list[str] = []

        async def record_slot(*_args, **_kwargs):
            events.append("slot")
            return False

        async def record_rpc(method, *_args, **_kwargs):
            events.append(f"rpc:{method}")
            return (True, None)

        with (
            patch.object(manager, "_fund_wrapped_native_via_deposit", new_callable=AsyncMock, return_value=False),
            patch.object(manager, "_set_balance_at_slot", side_effect=record_slot) as mock_slot,
            patch.object(manager, "_rpc_call_raw", side_effect=record_rpc) as mock_rpc,
        ):
            result = await manager.fund_tokens(self.WALLET, {self.WAVAX_ADDRESS: Decimal("10")})
        assert result is True
        deal_calls = [c for c in mock_rpc.call_args_list if c[0][0] == "anvil_dealERC20"]
        assert len(deal_calls) == 1, "anvil_dealERC20 must be called as fallback"
        account, token, amount_hex = deal_calls[0][0][1]
        assert account == self.WALLET, "first param must be the funded account"
        assert token == self.WAVAX_ADDRESS, "second param must be the token contract"
        assert amount_hex == hex(10 * 10**18), "third param must be the balance in token units"
        legacy_calls = [c for c in mock_rpc.call_args_list if c[0][0] == "anvil_deal"]
        assert not legacy_calls, "the nonexistent anvil_deal method must never be called"
        mock_slot.assert_awaited()
        assert events.index("slot") < events.index("rpc:anvil_dealERC20"), (
            f"known-slot must be attempted before anvil_dealERC20; got {events}"
        )

    @pytest.mark.asyncio()
    async def test_deal_erc20_timeout_aborts_batch_and_blames_only_unfunded(self, manager):
        """A dealERC20 timeout must abort the batch instead of poisoning it.

        The slot search runs inside the node, so a client-side timeout leaves it
        running and every later token fails for reasons that have nothing to do
        with that token (measured: an unrelated whale-funded USDC went from 1.6s
        to a hard failure after one such call). The tier must therefore stop the
        batch, report the untouched remainder as failed, and never re-enter the
        deal tier for them.
        """
        second_token = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"

        async def timing_out_rpc(method, *_args, **_kwargs):
            if method == "anvil_dealERC20":
                raise TimeoutError("slot search still running server-side")
            return (True, None)

        with (
            patch.object(manager, "_fund_wrapped_native_via_deposit", new_callable=AsyncMock, return_value=False),
            patch.object(manager, "_set_balance_at_slot", new_callable=AsyncMock, return_value=False),
            patch.object(manager, "_fund_token_via_storage", new_callable=AsyncMock, return_value=True) as brute,
            patch.object(manager, "_rpc_call_raw", side_effect=timing_out_rpc) as mock_rpc,
        ):
            failed = await manager.fund_tokens_report(
                self.WALLET, {self.WAVAX_ADDRESS: Decimal("10"), second_token: Decimal("5")}
            )

        assert failed == [self.WAVAX_ADDRESS, second_token], (
            "the timed-out token and every unprocessed token must be reported failed"
        )
        deal_calls = [c for c in mock_rpc.call_args_list if c[0][0] == "anvil_dealERC20"]
        assert len(deal_calls) == 1, "the degraded fork must not be asked to deal a second token"
        brute.assert_not_awaited()

    @pytest.mark.asyncio()
    async def test_deal_erc20_call_is_time_bounded(self, manager):
        """The deal tier must cap its own timeout, not inherit an unbounded budget."""
        from almanak.framework.anvil.fork_manager import _DEAL_ERC20_TIMEOUT_SECONDS

        with (
            patch.object(manager, "_fund_wrapped_native_via_deposit", new_callable=AsyncMock, return_value=False),
            patch.object(manager, "_set_balance_at_slot", new_callable=AsyncMock, return_value=False),
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(True, None)) as mock_rpc,
        ):
            await manager.fund_tokens(self.WALLET, {self.WAVAX_ADDRESS: Decimal("10")})

        deal_call = next(c for c in mock_rpc.call_args_list if c[0][0] == "anvil_dealERC20")
        assert deal_call.kwargs["timeout_override"] == _DEAL_ERC20_TIMEOUT_SECONDS, (
            "the deal tier must use its own budget, not the general rpc_timeout_seconds: "
            "tying them together lets a latency knob abort funding batches"
        )
        assert deal_call.kwargs["raise_on_timeout"] is True, "timeouts must be distinguishable from errors"


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

    async def _dispatch(method, params, timeout_override=None):
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
    async def test_openzeppelin_erc7201_root_is_probed_after_legacy_slots(self, manager, caplog):
        """The canonical OZ namespace is the tenth probe and keeps its successful write."""
        caplog.set_level("INFO", logger=fm.__name__)
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
            return 1_000_000 if balance_call_count == 10 else 0

        with (
            patch.object(manager, "_rpc_call_raw", rpc_mock),
            patch.object(manager, "_get_token_balance", side_effect=fake_balance),
        ):
            result = await manager._fund_token_via_storage(self.WALLET, self.TOKEN, self.AMOUNT_HEX, "TEST")

        assert result is True
        snapshot_calls = [call for call in rpc_mock.call_args_list if call.args[0] == "evm_snapshot"]
        revert_calls = [call for call in rpc_mock.call_args_list if call.args[0] == "evm_revert"]
        storage_calls = [call for call in rpc_mock.call_args_list if call.args[0] == "anvil_setStorageAt"]
        assert len(snapshot_calls) == 10
        assert len(revert_calls) == 9
        assert len(storage_calls) == 10

        expected_probe_slots = (0, 1, 2, 3, 4, 5, 9, 51, 52, fm._OPENZEPPELIN_ERC20_STORAGE_LOCATION)
        assert [call.args[1][1] for call in storage_calls] == [
            manager._calculate_mapping_slot(self.WALLET, slot) for slot in expected_probe_slots
        ]

        # Independently calculated keccak256(pad32(wallet) || pad32(OZ root)).
        expected_root_key = "0x00234118d1a2e51ba1b4fbb0a262d354b2c35d7510ec11bc5fd2c912bffd86ec"
        assert storage_calls[-1].args[1][1] == expected_root_key
        assert storage_calls[-1].args[1][2] == manager._pad_hex_to_32_bytes(self.AMOUNT_HEX)
        assert f"slot {hex(fm._OPENZEPPELIN_ERC20_STORAGE_LOCATION)}" in caplog.text

        methods = [call.args[0] for call in rpc_mock.call_args_list]
        assert methods[-2:] == ["anvil_setStorageAt", "evm_mine"]

    @pytest.mark.asyncio()
    async def test_all_failed_probes_are_reverted_including_erc7201_root(self, manager):
        """An unsuccessful ERC-7201 probe must not leak its storage write."""
        handlers = {
            "evm_snapshot": (True, "0xsnap"),
            "anvil_setStorageAt": (True, None),
            "evm_mine": (True, None),
            "evm_revert": (True, True),
        }
        rpc_mock = _make_rpc_dispatcher(handlers)

        with (
            patch.object(manager, "_rpc_call_raw", rpc_mock),
            patch.object(manager, "_get_token_balance", AsyncMock(return_value=0)),
        ):
            result = await manager._fund_token_via_storage(self.WALLET, self.TOKEN, self.AMOUNT_HEX, "TEST")

        assert result is False
        snapshot_calls = [call for call in rpc_mock.call_args_list if call.args[0] == "evm_snapshot"]
        revert_calls = [call for call in rpc_mock.call_args_list if call.args[0] == "evm_revert"]
        storage_calls = [call for call in rpc_mock.call_args_list if call.args[0] == "anvil_setStorageAt"]
        assert len(snapshot_calls) == 10
        assert len(revert_calls) == 10
        assert len(storage_calls) == 10

        root_storage_key = manager._calculate_mapping_slot(
            self.WALLET,
            fm._OPENZEPPELIN_ERC20_STORAGE_LOCATION,
        )
        assert storage_calls[-1].args[1][1] == root_storage_key
        assert rpc_mock.call_args_list[-1].args == ("evm_revert", ["0xsnap"])

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
    assert "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf" in fm.WHALE_FUNDED_TOKENS["base"]


class TestUsdfWhaleFunding:
    """ALM-3254: exact Ethereum USDf uses a real transfer, not proxy-slot probing."""

    USDF_ADDRESS = "0xfa2b947eec368f42195f24f36d2af29f7c24cec2"
    PASSIVE_HOLDER = "0x77134cbC06cB00b66F4c7e623D5fdBF6777635EC"
    WALLET = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"

    def test_exact_usdf_address_has_pinned_whale_recipe(self) -> None:
        assert fm.WHALE_FUNDED_TOKENS["ethereum"][self.USDF_ADDRESS] == self.PASSIVE_HOLDER

    @pytest.mark.asyncio()
    async def test_exact_usdf_address_selects_whale_and_never_storage(self) -> None:
        manager = _make_running_manager(chain="ethereum")
        with (
            patch.object(manager, "_fund_token_via_whale", new_callable=AsyncMock, return_value=True) as whale,
            patch.object(manager, "_set_balance_at_slot", new_callable=AsyncMock) as known_slot,
            patch.object(manager, "_fund_token_via_storage", new_callable=AsyncMock) as brute_force,
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock) as rpc,
        ):
            result = await manager.fund_tokens(
                self.WALLET,
                {self.USDF_ADDRESS.upper().replace("0X", "0x"): Decimal("1")},
            )

        assert result is True
        whale.assert_awaited_once_with(
            self.WALLET,
            self.USDF_ADDRESS,
            hex(10**18),
            self.PASSIVE_HOLDER,
            "USDF",
        )
        known_slot.assert_not_awaited()
        brute_force.assert_not_awaited()
        rpc.assert_not_awaited()

    @pytest.mark.asyncio()
    async def test_failed_usdf_whale_transfer_fails_loud_without_anvil_deal_erc20(self) -> None:
        manager = _make_running_manager(chain="ethereum")
        with (
            patch.object(manager, "_fund_token_via_whale", new_callable=AsyncMock, return_value=False),
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock) as rpc,
            patch.object(manager, "_fund_token_via_storage", new_callable=AsyncMock) as brute_force,
        ):
            result = await manager.fund_tokens(self.WALLET, {self.USDF_ADDRESS: Decimal("1")})

        assert result is False
        rpc.assert_not_awaited()
        brute_force.assert_not_awaited()


@pytest.mark.asyncio
async def test_symbol_funding_key_is_rejected_before_any_rpc() -> None:
    """ALM-3255: a valid token symbol must never select an ERC-20 contract."""
    manager = _make_running_manager(chain="base")
    with patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock) as rpc:
        result = await manager.fund_tokens(
            "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
            {"JitoSOL": Decimal("10")},
        )
    assert result is False
    rpc.assert_not_awaited()


class TestAddressKeyedBalanceSeedFunding:
    """ALM-3255: JitoSOL funding uses its exact Base address and Solady seed."""

    JITOSOL_ADDRESS = "0x97be14dd8f994a5364573bc035d85309e7cb34de"
    WALLET = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
    SEED = 0x87A211A2

    def test_calculates_solady_balance_storage_key(self) -> None:
        manager = RollingForkManager(rpc_url="http://rpc.test", chain="base", anvil_port=9999)
        assert manager._calculate_seeded_balance_slot(self.WALLET, self.SEED) == (
            "0x523a6896f5f8911768b8b7bb6464df9b5b7fefd3b0494de3368a2aaa2375e6c3"
        )

    @pytest.mark.asyncio()
    async def test_exact_jitosol_address_selects_seed_recipe(self) -> None:
        manager = _make_running_manager(chain="base")
        with (
            patch.object(manager, "_set_balance_at_seed", new_callable=AsyncMock, return_value=True) as set_seed,
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock) as rpc,
        ):
            result = await manager.fund_tokens(self.WALLET, {self.JITOSOL_ADDRESS: Decimal("1")})

        assert result is True
        set_seed.assert_awaited_once_with(
            self.WALLET,
            self.JITOSOL_ADDRESS,
            hex(10**9),
            self.SEED,
            "JITOSOL",
        )
        rpc.assert_not_awaited()


def _make_running_manager(chain: str = "arbitrum") -> RollingForkManager:
    """Manager that reports is_running=True without a real subprocess."""
    _clear_flags_cache()
    mgr = RollingForkManager(rpc_url="http://rpc.test", chain=chain, anvil_port=9999)
    mgr._is_running = True
    process = AsyncMock()  # container only; poll is a plain MagicMock attr
    process.poll = lambda: None  # alive
    mgr._process = process
    return mgr


class TestResetToLatest:
    """reset_to_latest: in-place anvil_reset vs stop/start fallback paths."""

    @pytest.mark.asyncio()
    async def test_in_place_reset_success_updates_block_and_clears_pin(self):
        mgr = _make_running_manager()
        mgr.fork_block_number = 12345
        rpc_mock = _make_rpc_dispatcher(
            {
                "anvil_reset": (True, None),
                "eth_blockNumber": (True, "0x10"),
            }
        )
        with (
            patch.object(mgr, "_rpc_call_raw", rpc_mock),
            patch.object(mgr, "_assert_chain_id_after_reset", new_callable=AsyncMock) as mock_assert,
            patch.object(mgr, "stop", new_callable=AsyncMock) as mock_stop,
            patch.object(mgr, "start", new_callable=AsyncMock) as mock_start,
        ):
            result = await mgr.reset_to_latest()

        assert result is True
        # Pinned block cleared so the next auto-restart forks latest too
        assert mgr.fork_block_number is None
        assert mgr._current_block == 16
        # VIB-2552 chain-id integrity assertion runs after the in-place reset
        mock_assert.assert_awaited_once()
        # No process restart on the fast path
        mock_stop.assert_not_called()
        mock_start.assert_not_called()
        reset_calls = [c for c in rpc_mock.call_args_list if c[0][0] == "anvil_reset"]
        assert reset_calls[0][0][1] == [{"forking": {"jsonRpcUrl": "http://rpc.test"}}]

    @pytest.mark.asyncio()
    async def test_in_place_reset_tolerates_missing_block_number(self):
        """A failed eth_blockNumber read must not fail the reset."""
        mgr = _make_running_manager()
        rpc_mock = _make_rpc_dispatcher(
            {
                "anvil_reset": (True, None),
                "eth_blockNumber": (False, None),
            }
        )
        with (
            patch.object(mgr, "_rpc_call_raw", rpc_mock),
            patch.object(mgr, "_assert_chain_id_after_reset", new_callable=AsyncMock),
        ):
            result = await mgr.reset_to_latest()

        assert result is True
        assert mgr._current_block is None

    @pytest.mark.asyncio()
    async def test_anvil_reset_failure_falls_back_to_stop_start(self):
        mgr = _make_running_manager()
        rpc_mock = _make_rpc_dispatcher({"anvil_reset": (False, None)})
        with (
            patch.object(mgr, "_rpc_call_raw", rpc_mock),
            patch.object(mgr, "_assert_chain_id_after_reset", new_callable=AsyncMock) as mock_assert,
            patch.object(mgr, "stop", new_callable=AsyncMock) as mock_stop,
            patch.object(mgr, "start", new_callable=AsyncMock, return_value=True) as mock_start,
        ):
            result = await mgr.reset_to_latest()

        assert result is True
        mock_stop.assert_awaited_once()
        mock_start.assert_awaited_once()
        # Chain-id assertion also runs after the stop/start fallback
        mock_assert.assert_awaited_once()
        assert mgr.fork_block_number is None

    @pytest.mark.asyncio()
    async def test_anvil_reset_exception_falls_back_to_stop_start(self):
        mgr = _make_running_manager()
        with (
            patch.object(mgr, "_rpc_call_raw", AsyncMock(side_effect=RuntimeError("rpc boom"))),
            patch.object(mgr, "_assert_chain_id_after_reset", new_callable=AsyncMock),
            patch.object(mgr, "stop", new_callable=AsyncMock) as mock_stop,
            patch.object(mgr, "start", new_callable=AsyncMock, return_value=True),
        ):
            result = await mgr.reset_to_latest()

        assert result is True
        mock_stop.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_not_running_skips_in_place_reset(self):
        _clear_flags_cache()
        mgr = RollingForkManager(rpc_url="http://rpc.test", chain="arbitrum", anvil_port=9999)
        rpc_mock = AsyncMock()
        with (
            patch.object(mgr, "_rpc_call_raw", rpc_mock),
            patch.object(mgr, "_assert_chain_id_after_reset", new_callable=AsyncMock),
            patch.object(mgr, "stop", new_callable=AsyncMock) as mock_stop,
            patch.object(mgr, "start", new_callable=AsyncMock, return_value=True),
        ):
            result = await mgr.reset_to_latest()

        assert result is True
        rpc_mock.assert_not_called()
        mock_stop.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_fallback_start_failure_restores_pinned_block(self):
        mgr = _make_running_manager()
        mgr.fork_block_number = 777
        rpc_mock = _make_rpc_dispatcher({"anvil_reset": (False, None)})
        with (
            patch.object(mgr, "_rpc_call_raw", rpc_mock),
            patch.object(mgr, "_assert_chain_id_after_reset", new_callable=AsyncMock) as mock_assert,
            patch.object(mgr, "stop", new_callable=AsyncMock),
            patch.object(mgr, "start", new_callable=AsyncMock, return_value=False),
        ):
            result = await mgr.reset_to_latest()

        assert result is False
        assert mgr.fork_block_number == 777
        mock_assert.assert_not_called()

    @pytest.mark.asyncio()
    async def test_fallback_stop_exception_restores_pinned_block(self):
        mgr = _make_running_manager()
        mgr.fork_block_number = 888
        rpc_mock = _make_rpc_dispatcher({"anvil_reset": (False, None)})
        with (
            patch.object(mgr, "_rpc_call_raw", rpc_mock),
            patch.object(mgr, "_assert_chain_id_after_reset", new_callable=AsyncMock),
            patch.object(mgr, "stop", AsyncMock(side_effect=OSError("kill failed"))),
            patch.object(mgr, "start", new_callable=AsyncMock) as mock_start,
        ):
            result = await mgr.reset_to_latest()

        assert result is False
        assert mgr.fork_block_number == 888
        mock_start.assert_not_called()


class TestAdvanceTime:
    """advance_time: evm_increaseTime + evm_mine + block refresh."""

    @pytest.mark.asyncio()
    async def test_not_running_returns_false_without_rpc(self):
        _clear_flags_cache()
        mgr = RollingForkManager(rpc_url="http://rpc.test", chain="arbitrum", anvil_port=9999)
        rpc_mock = AsyncMock()
        with patch.object(mgr, "_rpc_call_raw", rpc_mock):
            assert await mgr.advance_time(60) is False
        rpc_mock.assert_not_called()

    @pytest.mark.asyncio()
    async def test_success_advances_and_refreshes_block(self):
        mgr = _make_running_manager()
        rpc_mock = _make_rpc_dispatcher(
            {
                "evm_increaseTime": (True, None),
                "evm_mine": (True, None),
                "eth_blockNumber": (True, "0x2a"),
            }
        )
        with patch.object(mgr, "_rpc_call_raw", rpc_mock):
            assert await mgr.advance_time(3600) is True

        assert mgr._current_block == 42
        methods = [c[0][0] for c in rpc_mock.call_args_list]
        assert methods == ["evm_increaseTime", "evm_mine", "eth_blockNumber"]
        assert rpc_mock.call_args_list[0][0][1] == [3600]

    @pytest.mark.asyncio()
    async def test_success_without_block_number_still_true(self):
        mgr = _make_running_manager()
        rpc_mock = _make_rpc_dispatcher(
            {
                "evm_increaseTime": (True, None),
                "evm_mine": (True, None),
                "eth_blockNumber": (False, None),
            }
        )
        with patch.object(mgr, "_rpc_call_raw", rpc_mock):
            assert await mgr.advance_time(10) is True
        assert mgr._current_block is None

    @pytest.mark.asyncio()
    async def test_increase_time_failure_stops_before_mine(self):
        mgr = _make_running_manager()
        rpc_mock = _make_rpc_dispatcher({"evm_increaseTime": (False, None)})
        with patch.object(mgr, "_rpc_call_raw", rpc_mock):
            assert await mgr.advance_time(60) is False
        methods = [c[0][0] for c in rpc_mock.call_args_list]
        assert methods == ["evm_increaseTime"]

    @pytest.mark.asyncio()
    async def test_mine_failure_returns_false(self):
        mgr = _make_running_manager()
        rpc_mock = _make_rpc_dispatcher(
            {
                "evm_increaseTime": (True, None),
                "evm_mine": (False, None),
            }
        )
        with patch.object(mgr, "_rpc_call_raw", rpc_mock):
            assert await mgr.advance_time(60) is False
        methods = [c[0][0] for c in rpc_mock.call_args_list]
        assert methods == ["evm_increaseTime", "evm_mine"]

    @pytest.mark.asyncio()
    async def test_rpc_exception_returns_false(self):
        mgr = _make_running_manager()
        with patch.object(mgr, "_rpc_call_raw", AsyncMock(side_effect=RuntimeError("boom"))):
            assert await mgr.advance_time(60) is False


class TestAssertChainIdAfterReset:
    """VIB-2552: chain-id integrity check + anvil_setChainId repair."""

    @pytest.mark.asyncio()
    async def test_unknown_chain_skips_assertion(self):
        mgr = _make_running_manager()
        mgr.chain = "not-a-chain"  # post-init: bypasses __post_init__ validation
        rpc_mock = AsyncMock()
        with patch.object(mgr, "_rpc_call_raw", rpc_mock):
            await mgr._assert_chain_id_after_reset()
        rpc_mock.assert_not_called()

    @pytest.mark.asyncio()
    async def test_unreadable_chain_id_returns_without_fix(self):
        mgr = _make_running_manager()
        rpc_mock = _make_rpc_dispatcher({"eth_chainId": (False, None)})
        with patch.object(mgr, "_rpc_call_raw", rpc_mock):
            await mgr._assert_chain_id_after_reset()
        methods = [c[0][0] for c in rpc_mock.call_args_list]
        assert "anvil_setChainId" not in methods

    @pytest.mark.asyncio()
    async def test_matching_chain_id_needs_no_fix(self):
        mgr = _make_running_manager(chain="arbitrum")
        rpc_mock = _make_rpc_dispatcher({"eth_chainId": (True, hex(42161))})
        with patch.object(mgr, "_rpc_call_raw", rpc_mock):
            await mgr._assert_chain_id_after_reset()
        methods = [c[0][0] for c in rpc_mock.call_args_list]
        assert methods == ["eth_chainId"]

    @pytest.mark.asyncio()
    async def test_mismatch_fixed_via_set_chain_id(self):
        mgr = _make_running_manager(chain="arbitrum")
        rpc_mock = _make_rpc_dispatcher(
            {
                "eth_chainId": (True, hex(31337)),  # Anvil default leaked through
                "anvil_setChainId": (True, None),
            }
        )
        with patch.object(mgr, "_rpc_call_raw", rpc_mock):
            await mgr._assert_chain_id_after_reset()
        fix_calls = [c for c in rpc_mock.call_args_list if c[0][0] == "anvil_setChainId"]
        assert len(fix_calls) == 1
        assert fix_calls[0][0][1] == [42161]

    @pytest.mark.asyncio()
    async def test_mismatch_fix_failure_is_swallowed(self):
        """A failed anvil_setChainId logs but must not raise."""
        mgr = _make_running_manager(chain="arbitrum")
        rpc_mock = _make_rpc_dispatcher(
            {
                "eth_chainId": (True, hex(31337)),
                "anvil_setChainId": (False, None),
            }
        )
        with patch.object(mgr, "_rpc_call_raw", rpc_mock):
            await mgr._assert_chain_id_after_reset()  # must not raise

    @pytest.mark.asyncio()
    async def test_mismatch_fix_exception_is_swallowed(self):
        mgr = _make_running_manager(chain="arbitrum")

        call_count = 0

        async def _dispatch(method, params, timeout_override=None):
            nonlocal call_count
            call_count += 1
            if method == "eth_chainId":
                return (True, hex(31337))
            raise RuntimeError("setChainId transport error")

        with patch.object(mgr, "_rpc_call_raw", AsyncMock(side_effect=_dispatch)):
            await mgr._assert_chain_id_after_reset()  # must not raise
        assert call_count == 2


class TestFundTokensReport:
    """ALM-3264: funding failures must be attributed per token — naming the
    whole batch sent debuggers chasing tokens that funded fine."""

    def _manager(self) -> RollingForkManager:
        return RollingForkManager(rpc_url="http://rpc.test", chain="arbitrum", anvil_port=9999)

    @pytest.mark.asyncio()
    async def test_not_running_reports_every_requested_key(self):
        mgr = self._manager()
        tokens = {
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831": Decimal("1"),
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": Decimal("2"),
        }
        failed = await mgr.fund_tokens_report("0x" + "1" * 40, tokens)
        assert failed == list(tokens)

    @pytest.mark.parametrize(("failed", "expected"), [([], True), (["0x" + "a" * 40], False)])
    @pytest.mark.asyncio()
    async def test_bool_wrapper_preserves_all_or_nothing_contract(self, failed, expected):
        mgr = self._manager()
        address = "0x" + "1" * 40
        tokens = {"0x" + "a" * 40: Decimal("1")}

        with patch.object(mgr, "fund_tokens_report", new=AsyncMock(return_value=failed)) as report:
            result = await mgr.fund_tokens(address, tokens)

        assert result is expected
        report.assert_awaited_once_with(address, tokens)

    @pytest.mark.asyncio()
    async def test_non_address_key_lands_in_failed_list_verbatim(self):
        """Symbol keys are rejected per-token; the report must name the bad
        key, not just flip a global bool."""
        mgr = self._manager()
        with patch.object(RollingForkManager, "is_running", new_callable=PropertyMock, return_value=True):
            failed = await mgr.fund_tokens_report("0x" + "1" * 40, {"USDC": Decimal("1")})
        assert failed == ["USDC"]

    @pytest.mark.asyncio()
    async def test_unknown_decimals_token_reported_alone(self):
        """A token whose decimals cannot be discovered fails alone — siblings
        that fund fine must not appear in the report."""
        good = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        bad = "0x80f3d493ebce97e343c53d29a137942416b4ffc0"
        mgr = self._manager()

        with (
            patch.object(RollingForkManager, "is_running", new_callable=PropertyMock, return_value=True),
            patch.object(mgr, "_fetch_decimals_onchain", new=AsyncMock(return_value=None)),
            patch.object(mgr, "_fund_token_via_storage", new=AsyncMock(return_value=True)),
            patch.object(mgr, "_rpc_call_raw", new=AsyncMock(return_value=(False, None))),
            patch("almanak.framework.data.tokens.get_token_resolver") as resolver_factory,
        ):
            from almanak.framework.data.tokens.exceptions import TokenNotFoundError

            resolver = resolver_factory.return_value

            def _resolve(address, chain, **kwargs):
                if address == good:
                    resolved = type("R", (), {"decimals": 6, "symbol": "USDC"})()
                    return resolved
                raise TokenNotFoundError(token=address, chain=chain)

            resolver.resolve.side_effect = _resolve
            failed = await mgr.fund_tokens_report(
                "0x" + "1" * 40,
                {good: Decimal("1"), bad: Decimal("1")},
            )

        assert failed == [bad]
