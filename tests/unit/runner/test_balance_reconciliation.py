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
    strategy.strategy_id = "test_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.supports_teardown.return_value = False
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


# =============================================================================
# Tests: _reconcile_post_execution_balances
# =============================================================================


class TestReconcileBalances:
    @pytest.mark.asyncio
    async def test_returns_none_for_hold_intent(self):
        """No reconciliation for HoldIntent (no tokens)."""
        runner = _make_runner()
        strategy = MagicMock()
        strategy.strategy_id = "test"
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
        strategy.strategy_id = "test"

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
        strategy.strategy_id = "test"

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
        strategy.strategy_id = "test"

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
        strategy.strategy_id = "test"

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        recon = await runner._reconcile_post_execution_balances(strategy, intent, None)

        assert recon is not None
        assert "USDC" in recon["tokens_checked"]
        assert "ETH" not in recon["tokens_checked"]


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
