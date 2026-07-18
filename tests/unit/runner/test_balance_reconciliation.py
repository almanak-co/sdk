"""Tests for post-execution balance reconciliation (VIB-1257).

Verifies that:
- Successful execution includes balance reconciliation data
- Token extraction works for various intent types (swap, LP, supply)
- Reconciliation warnings raised for zero/negative swap amounts
- Balance query failures are non-fatal
- HoldIntent skips reconciliation (no tokens)
- balance_reconciliation field is None when reconciliation can't be performed
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import HoldIntent, SwapIntent
from almanak.framework.runner.reconciliation import BalanceSnapshot
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)

# =============================================================================
# Helpers
# =============================================================================

_PAUSE_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._is_strategy_paused"
_TEARDOWN_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._check_teardown_requested"


def _make_strategy(decide_return=None):
    """Create a mock strategy."""
    strategy = MagicMock()
    strategy.deployment_id = "test_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.create_market_snapshot.return_value.has_critical_data_failures.return_value = False
    strategy.generate_teardown_intents.side_effect = NotImplementedError

    if decide_return is None:
        decide_return = HoldIntent(reason="Test hold")
    strategy.decide.return_value = decide_return

    return strategy


def _make_runner(balance_provider=None):
    """Create a StrategyRunner."""
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=balance_provider or MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


# =============================================================================
# Tests: _extract_intent_tokens
# =============================================================================


class TestExtractIntentTokens:
    def test_swap_intent_extracts_from_and_to(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert "USDC" in tokens
        assert "ETH" in tokens

    def test_hold_intent_returns_empty(self):
        intent = HoldIntent(reason="waiting")
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert tokens == []

    def test_lp_intent_extracts_token0_and_token1(self):
        intent = MagicMock()
        intent.token0 = "WETH"
        intent.token1 = "USDC"
        # Remove swap-like attributes
        del intent.from_token
        del intent.to_token
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert "WETH" in tokens
        assert "USDC" in tokens

    def test_supply_intent_extracts_token(self):
        intent = MagicMock()
        intent.token = "USDC"
        del intent.from_token
        del intent.to_token
        del intent.token0
        del intent.token1
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert tokens == ["USDC"]

    def test_lp_open_intent_parses_pool_with_fee_tier(self):
        """``LPOpenIntent`` shaped like ``pool="WETH/USDC/500"`` — the third
        segment is the fee tier, not a token. The parser must keep the first
        two and drop the rest. Without this, mainnet balance snapshots and
        price oracles for LP intents were empty (Mainnet 2026-05-01)."""
        intent = MagicMock()
        intent.pool = "WETH/USDC/500"
        # Force the LP-pool branch by removing every earlier-priority attr.
        del intent.from_token
        del intent.to_token
        del intent.token0
        del intent.token1
        del intent.token
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert tokens == ["WETH", "USDC"]

    def test_lp_open_intent_parses_two_segment_pool(self):
        """TraderJoe V2 style — ``"TOKEN0/TOKEN1"`` with bin_step in
        ``protocol_params`` rather than the pool string."""
        intent = MagicMock()
        intent.pool = "AVAX/USDC"
        del intent.from_token
        del intent.to_token
        del intent.token0
        del intent.token1
        del intent.token
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert tokens == ["AVAX", "USDC"]

    def test_malformed_pool_string_returns_empty(self):
        """Single-segment / empty / leading-slash pool strings must NOT be
        treated as tokens — fail closed rather than fabricating one."""
        for bad_pool in ("", "TOKENONLY", "/USDC", "  ", "/"):
            intent = MagicMock()
            intent.pool = bad_pool
            del intent.from_token
            del intent.to_token
            del intent.token0
            del intent.token1
            del intent.token
            tokens = StrategyRunner._extract_intent_tokens(intent)
            assert tokens == [], f"unexpected tokens for pool={bad_pool!r}: {tokens}"

    def test_pool_string_strips_whitespace_segments(self):
        """``"WETH / USDC / 500"`` (operator-typed, whitespace creep) must
        round-trip cleanly to canonical symbols."""
        intent = MagicMock()
        intent.pool = "WETH / USDC / 500"
        del intent.from_token
        del intent.to_token
        del intent.token0
        del intent.token1
        del intent.token
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert tokens == ["WETH", "USDC"]

    def test_borrow_intent_extracts_collateral_and_borrow(self):
        """VIB-3350: ``BorrowIntent`` has no ``token`` attr — it carries
        ``collateral_token`` + ``borrow_token``. The prior ``token``-only
        fallback returned [], so reconciliation read neither leg. Both legs
        move the wallet (collateral out, borrowed token in)."""
        from almanak.framework.intents.lending_intents import BorrowIntent

        # Token-extraction fixture on a pre-built bundled borrow -- model_construct
        # bypasses the bundled-collateral guard so both legs (collateral_token +
        # borrow_token) still exist to reconcile; extraction is under test, not validation.
        intent = BorrowIntent.model_construct(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
        )
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert tokens == ["WETH", "USDC"]

    def test_perp_open_intent_extracts_collateral(self):
        """VIB-3350: ``PerpOpenIntent`` carries ``collateral_token`` (no
        ``token``/``borrow_token``). ``size_usd`` is notional, not a wallet
        token, so only the collateral leg is reconciled."""
        from almanak.framework.intents.perp_intents import PerpOpenIntent

        intent = PerpOpenIntent(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("500"),
            size_usd=Decimal("1000"),
            is_long=True,
            leverage=Decimal("2"),
        )
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert tokens == ["USDC"]

    def test_perp_close_intent_extracts_collateral(self):
        """VIB-3350: ``PerpCloseIntent`` settles PnL + returns collateral in
        the collateral token, so that is the leg to reconcile."""
        from almanak.framework.intents.perp_intents import PerpCloseIntent

        intent = PerpCloseIntent(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
        )
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert tokens == ["USDC"]

    def test_supply_intent_real_class_still_single_token(self):
        """Regression guard: the multi-leg branches must not steal SupplyIntent
        (which has a single ``token`` and no ``collateral_token``)."""
        from almanak.framework.intents.lending_intents import SupplyIntent

        intent = SupplyIntent(protocol="aave_v3", token="USDC", amount=Decimal("100"))
        tokens = StrategyRunner._extract_intent_tokens(intent)
        assert tokens == ["USDC"]


# =============================================================================
# Tests: _reconcile_post_execution_balances
# =============================================================================


class TestReconcileBalances:
    @pytest.mark.asyncio
    async def test_returns_none_for_hold_intent(self):
        """No reconciliation for HoldIntent (no tokens)."""
        runner = _make_runner()
        strategy = MagicMock()
        strategy.deployment_id = "test"
        intent = HoldIntent(reason="waiting")

        result = await runner._reconcile_post_execution_balances(strategy, intent, None)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_balance_data_for_swap(self):
        """Reconciliation should include post balances for swap tokens."""
        bp = MagicMock()
        usdc_bal = MagicMock()
        usdc_bal.balance = Decimal("900")
        eth_bal = MagicMock()
        eth_bal.balance = Decimal("0.5")
        bp.get_balance = AsyncMock(side_effect=lambda t: usdc_bal if t == "USDC" else eth_bal)

        runner = _make_runner(balance_provider=bp)
        strategy = MagicMock()
        strategy.deployment_id = "test"

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        recon = await runner._reconcile_post_execution_balances(strategy, intent, None)

        assert recon is not None
        assert "USDC" in recon["tokens_checked"]
        assert "ETH" in recon["tokens_checked"]
        assert recon["post_balances"]["USDC"] == "900"
        assert recon["post_balances"]["ETH"] == "0.5"

    @pytest.mark.asyncio
    async def test_warns_on_zero_swap_output(self):
        """Warning should be raised if swap output amount is zero."""
        bp = MagicMock()
        bal = MagicMock()
        bal.balance = Decimal("100")
        bp.get_balance = AsyncMock(return_value=bal)

        runner = _make_runner(balance_provider=bp)
        strategy = MagicMock()
        strategy.deployment_id = "test"

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))

        exec_result = MagicMock()
        exec_result.swap_amounts = MagicMock()
        exec_result.swap_amounts.amount_out_decimal = Decimal("0")
        exec_result.swap_amounts.amount_in_decimal = Decimal("100")

        recon = await runner._reconcile_post_execution_balances(strategy, intent, exec_result)

        assert recon is not None
        assert len(recon["warnings"]) > 0
        assert "zero or negative" in recon["warnings"][0]

    @pytest.mark.asyncio
    async def test_balance_query_failure_is_nonfatal(self):
        """If balance_provider.get_balance raises, reconciliation should still return."""
        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=Exception("RPC down"))

        runner = _make_runner(balance_provider=bp)
        strategy = MagicMock()
        strategy.deployment_id = "test"

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        recon = await runner._reconcile_post_execution_balances(strategy, intent, None)

        # Returns None because no balances could be fetched
        assert recon is None

    @pytest.mark.asyncio
    async def test_partial_balance_query_still_returns(self):
        """If one token balance fails, reconciliation should still include the other."""
        bp = MagicMock()
        usdc_bal = MagicMock()
        usdc_bal.balance = Decimal("900")

        async def get_bal(token):
            if token == "USDC":
                return usdc_bal
            raise Exception("ETH balance unavailable")

        bp.get_balance = AsyncMock(side_effect=get_bal)

        runner = _make_runner(balance_provider=bp)
        strategy = MagicMock()
        strategy.deployment_id = "test"

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        recon = await runner._reconcile_post_execution_balances(strategy, intent, None)

        assert recon is not None
        assert "USDC" in recon["tokens_checked"]
        assert "ETH" not in recon["tokens_checked"]

    @pytest.mark.asyncio
    async def test_real_delta_post_reads_force_fresh_balances(self):
        """Post-execution reconciliation must bypass gateway-side balance cache."""
        bp = MagicMock()
        usdc_bal = MagicMock()
        usdc_bal.balance = Decimal("96")
        eth_bal = MagicMock()
        eth_bal.balance = Decimal("4")
        bp.get_balance = AsyncMock(side_effect=lambda t, *, force_refresh=False: usdc_bal if t == "USDC" else eth_bal)

        runner = _make_runner(balance_provider=bp)
        strategy = MagicMock()
        strategy.deployment_id = "test"
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"))
        pre_snapshot = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

        recon = await runner._reconcile_post_execution_balances(strategy, intent, None, pre_snapshot=pre_snapshot)

        assert recon is not None
        assert bp.get_balance.await_args_list[0].kwargs["force_refresh"] is True
        assert bp.get_balance.await_args_list[1].kwargs["force_refresh"] is True

    @pytest.mark.asyncio
    async def test_real_delta_post_falls_back_when_provider_rejects_force_refresh(self):
        """Legacy provider that doesn't accept force_refresh still reconciles cleanly."""
        usdc_bal = MagicMock()
        usdc_bal.balance = Decimal("96")
        eth_bal = MagicMock()
        eth_bal.balance = Decimal("4")

        async def legacy_get_balance(token):  # no force_refresh kwarg supported
            return usdc_bal if token == "USDC" else eth_bal

        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=legacy_get_balance)

        runner = _make_runner(balance_provider=bp)
        strategy = MagicMock()
        strategy.deployment_id = "test"
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"))
        pre_snapshot = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

        recon = await runner._reconcile_post_execution_balances(strategy, intent, None, pre_snapshot=pre_snapshot)

        assert recon is not None
        # Each token is read twice: first attempt with kwarg fails -> fall back without kwarg.
        kwargs_seen = [c.kwargs for c in bp.get_balance.await_args_list]
        assert any("force_refresh" in k for k in kwargs_seen), "should have tried kwarg first"
        assert any(k == {} for k in kwargs_seen), "should have fallen back without kwarg"

    @pytest.mark.asyncio
    async def test_real_delta_post_no_kwarg_when_no_pre_snapshot(self):
        """Without pre_snapshot, force_refresh stays off and kwarg is never sent."""
        bp = MagicMock()
        usdc_bal = MagicMock()
        usdc_bal.balance = Decimal("96")
        bp.get_balance = AsyncMock(return_value=usdc_bal)

        runner = _make_runner(balance_provider=bp)
        strategy = MagicMock()
        strategy.deployment_id = "test"
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"))

        recon = await runner._reconcile_post_execution_balances(strategy, intent, None, pre_snapshot=None)

        # Legacy/post-only path should still reconcile and never pass force_refresh.
        assert recon is not None
        assert bp.get_balance.await_count == 2
        for call in bp.get_balance.await_args_list:
            assert "force_refresh" not in call.kwargs

    @pytest.mark.asyncio
    async def test_real_delta_post_reraises_unrelated_type_error(self):
        """A TypeError unrelated to force_refresh must NOT be swallowed."""

        async def buggy_provider(token, *, force_refresh=False):
            raise TypeError("bad return type from underlying client")

        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=buggy_provider)

        runner = _make_runner(balance_provider=bp)
        strategy = MagicMock()
        strategy.deployment_id = "test"
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"))
        pre_snapshot = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

        # The reconciliation loop swallows per-token exceptions and logs them;
        # the assertion is that no second call was issued for the same token
        # (i.e., the fallback path did NOT fire on an unrelated TypeError).
        await runner._reconcile_post_execution_balances(strategy, intent, None, pre_snapshot=pre_snapshot)
        per_token_calls = [c for c in bp.get_balance.await_args_list if c.args == ("USDC",)]
        # One kwarg attempt; no follow-up no-kwarg fallback.
        assert len(per_token_calls) == 1
        assert per_token_calls[0].kwargs.get("force_refresh") is True


# =============================================================================
# Tests: Integration - reconciliation on successful run_iteration
# =============================================================================


class TestReconciliationInRunIteration:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_hold_result_has_no_reconciliation(self, _mock_pause, _mock_teardown):
        """HOLD iterations should not have reconciliation data."""
        runner = _make_runner()
        strategy = _make_strategy(decide_return=HoldIntent(reason="waiting"))

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.HOLD
        assert result.balance_reconciliation is None

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_error_result_has_no_reconciliation(self, _mock_pause, _mock_teardown):
        """Failed iterations should not have reconciliation data."""
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.decide.side_effect = RuntimeError("bug")

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert result.balance_reconciliation is None


# =============================================================================
# Tests: snapshot_balances_for_intent native-gas symmetry (VIB-4979)
# =============================================================================


class TestSnapshotNativeGasSymmetry:
    """The pre-state snapshot (transaction_ledger.pre_state_json, the data
    source for the dashboard 'Wallet deployed' anchor) must cover the SAME
    token universe as NAV — including the chain's native gas token. Without
    this, lifetime_pnl = NAV − Deployed inherits the native-gas balance as
    phantom profit (VIB-4979)."""

    @pytest.mark.asyncio
    async def test_native_gas_captured_for_non_native_swap(self):
        """USDC->WETH on Arbitrum: pre-state must also carry ETH (native)."""
        balances = {"USDC": Decimal("100"), "WETH": Decimal("0.05"), "ETH": Decimal("0.002")}

        async def get_bal(token):
            bal = MagicMock()
            bal.balance = balances.get(token, Decimal("0"))
            return bal

        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=get_bal)
        runner = _make_runner(balance_provider=bp)

        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")
        snap = await runner._snapshot_balances_for_intent(intent)

        assert snap is not None
        # Native ETH appears alongside the intent tokens.
        assert "ETH" in snap.balances
        assert snap.balances["ETH"] == Decimal("0.002")
        assert "USDC" in snap.balances
        assert "WETH" in snap.balances

    @pytest.mark.asyncio
    async def test_native_not_added_when_intent_has_no_chain(self):
        """Pre-fix behaviour preserved: no chain → native symbol unresolved →
        snapshot stays intent-token-only (the existing reconciliation tests
        rely on this)."""
        balances = {"USDC": Decimal("100"), "ETH": Decimal("0.5")}

        async def get_bal(token):
            bal = MagicMock()
            bal.balance = balances.get(token, Decimal("0"))
            return bal

        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=get_bal)
        runner = _make_runner(balance_provider=bp)

        # SwapIntent.chain defaults to None.
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"))
        assert getattr(intent, "chain", None) is None
        snap = await runner._snapshot_balances_for_intent(intent)

        assert snap is not None
        assert set(snap.balances.keys()) == {"USDC", "WETH"}

    @pytest.mark.asyncio
    async def test_native_from_swap_does_not_double_fetch_native(self):
        """ETH->USDC on Arbitrum: native is already an intent token, so it is
        not added a second time (case-insensitive dedupe)."""
        seen: list[str] = []

        async def get_bal(token):
            seen.append(token)
            bal = MagicMock()
            bal.balance = Decimal("1")
            return bal

        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=get_bal)
        runner = _make_runner(balance_provider=bp)

        intent = SwapIntent(from_token="ETH", to_token="USDC", amount=Decimal("1"), chain="arbitrum")
        snap = await runner._snapshot_balances_for_intent(intent)

        assert snap is not None
        # ETH fetched exactly once (intent token), never duplicated.
        assert seen.count("ETH") == 1
        assert set(snap.balances.keys()) == {"ETH", "USDC"}


# =============================================================================
# Tests: snapshot_balances_for_intent tracked-token universe (VIB-5866 leg A)
# =============================================================================


class TestSnapshotTrackedTokenUniverse:
    """Generalizes VIB-4979: the pre-state anchor must cover the strategy's FULL
    tracked-token universe (the same set NAV values), not just the first intent's
    tokens + native. A held token the first intent doesn't name must be captured
    so it stops booking as phantom PnL (NAV counts it, Deployed didn't)."""

    @pytest.mark.asyncio
    async def test_tracked_token_not_in_intent_is_captured(self):
        """USDC->WETH swap, wallet also holds ARB (a tracked token the intent
        never names): ARB must appear in the pre-state anchor."""
        balances = {
            "USDC": Decimal("100"),
            "WETH": Decimal("0.05"),
            "ETH": Decimal("0.002"),
            "ARB": Decimal("42"),
        }

        async def get_bal(token):
            bal = MagicMock()
            bal.balance = balances.get(token, Decimal("0"))
            return bal

        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=get_bal)
        runner = _make_runner(balance_provider=bp)
        runner._current_tracked_tokens = ["USDC", "WETH", "ARB"]

        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")
        snap = await runner._snapshot_balances_for_intent(intent)

        assert snap is not None
        # The tracked-but-not-in-intent token is now anchored.
        assert "ARB" in snap.balances
        assert snap.balances["ARB"] == Decimal("42")
        # Intent tokens + native still present.
        assert {"USDC", "WETH", "ETH"} <= set(snap.balances)

    @pytest.mark.asyncio
    async def test_tracked_tokens_deduped_against_intent_and_native(self):
        """Tracked set overlapping the intent tokens / native must not double-fetch."""
        seen: list[str] = []

        async def get_bal(token):
            seen.append(token)
            bal = MagicMock()
            bal.balance = Decimal("1")
            return bal

        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=get_bal)
        runner = _make_runner(balance_provider=bp)
        # Mixed case + overlap with intent (USDC/WETH) and native (ETH).
        runner._current_tracked_tokens = ["usdc", "WETH", "eth", "ARB"]

        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")
        snap = await runner._snapshot_balances_for_intent(intent)

        assert snap is not None
        # Each token fetched exactly once despite case-mismatched overlap.
        assert seen.count("USDC") == 1
        assert seen.count("WETH") == 1
        assert seen.count("ETH") == 1
        assert "ARB" in snap.balances

    @pytest.mark.asyncio
    async def test_mixed_case_tracked_token_stored_uppercase(self):
        """A canonical mixed-case tracked asset (wstETH) is stored under its
        UPPERCASE key so it matches the uppercase price_inputs the anchor's
        exact `prices.get()` reads (Codex P1) — else it would never be valued."""
        balances = {
            "USDC": Decimal("100"),
            "WETH": Decimal("0.05"),
            "ETH": Decimal("0.002"),
            "WSTETH": Decimal("3"),
        }

        async def get_bal(token):
            bal = MagicMock()
            bal.balance = balances.get(token, Decimal("0"))
            return bal

        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=get_bal)
        runner = _make_runner(balance_provider=bp)
        runner._current_tracked_tokens = ["wstETH"]  # canonical mixed-case

        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")
        snap = await runner._snapshot_balances_for_intent(intent)

        assert snap is not None
        assert "WSTETH" in snap.balances  # uppercased to match uppercase price keys
        assert "wstETH" not in snap.balances
        assert snap.balances["WSTETH"] == Decimal("3")

    @pytest.mark.asyncio
    async def test_tracked_captured_when_intent_names_no_tokens(self):
        """First action names no extractor-recognized tokens (vault deposit /
        stake / prediction, here stood in by HOLD → extract returns []): the
        tracked-token anchor is STILL written (Codex P2 — previously the early
        `if not tokens: return None` skipped it, leaving no Deployed anchor)."""
        balances = {"ARB": Decimal("42")}

        async def get_bal(token):
            bal = MagicMock()
            bal.balance = balances.get(token, Decimal("0"))
            return bal

        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=get_bal)
        runner = _make_runner(balance_provider=bp)
        runner._current_tracked_tokens = ["ARB"]

        intent = HoldIntent(reason="no tokens")  # extract_intent_tokens -> []
        snap = await runner._snapshot_balances_for_intent(intent)

        assert snap is not None
        assert snap.balances == {"ARB": Decimal("42")}

    @pytest.mark.asyncio
    async def test_no_tokens_and_no_tracked_still_returns_none(self):
        """Empty intent + empty tracked set → None (legacy post-only fallback),
        unchanged: the moved guard only fires when there is truly nothing."""
        bp = MagicMock()
        bp.get_balance = AsyncMock(return_value=MagicMock(balance=Decimal("0")))
        runner = _make_runner(balance_provider=bp)
        assert runner._current_tracked_tokens == []

        snap = await runner._snapshot_balances_for_intent(HoldIntent(reason="x"))
        assert snap is None

    @pytest.mark.asyncio
    async def test_no_tracked_tokens_preserves_prefix_behaviour(self):
        """Empty tracked set (default / teardown / legacy): capture is byte-for-byte
        the pre-VIB-5866 intent-tokens + native shape."""
        balances = {"USDC": Decimal("100"), "WETH": Decimal("0.05"), "ETH": Decimal("0.002")}

        async def get_bal(token):
            bal = MagicMock()
            bal.balance = balances.get(token, Decimal("0"))
            return bal

        bp = MagicMock()
        bp.get_balance = AsyncMock(side_effect=get_bal)
        runner = _make_runner(balance_provider=bp)
        assert runner._current_tracked_tokens == []  # __init__ default

        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")
        snap = await runner._snapshot_balances_for_intent(intent)

        assert snap is not None
        assert set(snap.balances.keys()) == {"USDC", "WETH", "ETH"}


class TestResolveTrackedTokens:
    """`resolve_tracked_tokens` is best-effort and never raises (VIB-5866)."""

    def test_returns_strategy_tracked_tokens(self):
        from almanak.framework.runner.runner_state import resolve_tracked_tokens

        strategy = MagicMock()
        strategy._get_tracked_tokens.return_value = ["USDC", "WETH", "ARB"]
        assert resolve_tracked_tokens(strategy) == ["USDC", "WETH", "ARB"]

    def test_missing_hook_returns_empty(self):
        from almanak.framework.runner.runner_state import resolve_tracked_tokens

        strategy = object()  # no _get_tracked_tokens
        assert resolve_tracked_tokens(strategy) == []

    def test_raising_hook_returns_empty(self):
        from almanak.framework.runner.runner_state import resolve_tracked_tokens

        strategy = MagicMock()
        strategy._get_tracked_tokens.side_effect = RuntimeError("boom")
        assert resolve_tracked_tokens(strategy) == []

    def test_filters_non_string_and_empty(self):
        from almanak.framework.runner.runner_state import resolve_tracked_tokens

        strategy = MagicMock()
        strategy._get_tracked_tokens.return_value = ["USDC", "", None, 123, "ARB"]
        assert resolve_tracked_tokens(strategy) == ["USDC", "ARB"]


class _StatefulMarket:
    """Minimal market: price(token) marks it priced; get_price_oracle_dict()
    returns the priced set. Lets the refresh helper's top-off be observed."""

    def __init__(self, chain="arbitrum", initially_priceable=None):
        self.chain = chain
        # Tokens the market CAN price (price() only succeeds for these).
        self._priceable = {t.upper() for t in (initially_priceable or [])}
        self._priced: dict[str, Decimal] = {}

    def price(self, token):
        key = str(token).upper()
        if key not in self._priceable:
            raise ValueError(f"no price for {token}")
        self._priced[key] = Decimal("1")
        return Decimal("1")

    def get_price_oracle_dict(self, with_sources=False):
        return dict(self._priced)


class TestRefreshPriceOracleExtraTokens:
    """`_refresh_price_oracle_for_ledger` prices the tracked-token top-off so
    price_inputs_json can value the anchor (VIB-5866 leg A)."""

    def test_extra_tokens_priced_into_oracle(self):
        # Intent tokens + native + ARB are all priceable → non-empty oracle,
        # so the tracked-token ARB is topped off and lands in the result.
        market = _StatefulMarket(chain="arbitrum", initially_priceable=["USDC", "WETH", "ETH", "ARB"])
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")

        oracle = StrategyRunner._refresh_price_oracle_for_ledger(market, intent, extra_tokens=["ARB"])

        assert oracle is not None
        assert "ARB" in oracle  # the tracked-but-not-in-intent token got priced

    def test_extra_tokens_skipped_when_oracle_empty(self):
        # Nothing is priceable → oracle stays empty after the intent loop, so the
        # non-empty guard must skip the ARB top-off (never flip placeholder→priced).
        market = _StatefulMarket(chain="arbitrum", initially_priceable=[])
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")

        oracle = StrategyRunner._refresh_price_oracle_for_ledger(market, intent, extra_tokens=["ARB"])

        # Empty oracle → None (unpriced path); ARB was never priced.
        assert not oracle
        assert "ARB" not in market._priced

    def test_none_extra_tokens_is_prefix_behaviour(self):
        # extra_tokens=None → behaves exactly as before (intent + native only).
        market = _StatefulMarket(chain="arbitrum", initially_priceable=["USDC", "WETH", "ETH"])
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")

        oracle = StrategyRunner._refresh_price_oracle_for_ledger(market, intent, extra_tokens=None)

        assert oracle is not None
        assert set(oracle) == {"USDC", "WETH", "ETH"}

    def test_none_market_returns_none(self):
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")
        assert StrategyRunner._refresh_price_oracle_for_ledger(None, intent, extra_tokens=["ARB"]) is None

    def test_market_without_oracle_method_returns_none(self):
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")
        market = object()  # no get_price_oracle_dict
        assert StrategyRunner._refresh_price_oracle_for_ledger(market, intent, extra_tokens=["ARB"]) is None

    def test_oracle_fetch_exception_is_swallowed(self):
        """Best-effort: a raising market yields None, never propagates."""

        class _Boom:
            chain = "arbitrum"

            def get_price_oracle_dict(self, with_sources=False):
                raise RuntimeError("oracle down")

            def price(self, token):
                raise RuntimeError("oracle down")

        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"), chain="arbitrum")
        assert StrategyRunner._refresh_price_oracle_for_ledger(_Boom(), intent, extra_tokens=["ARB"]) is None
