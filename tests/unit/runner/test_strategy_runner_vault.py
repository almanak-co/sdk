"""Tests for vault lifecycle integration in StrategyRunner."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import IterationStatus, StrategyRunner
from almanak.framework.vault.config import SettlementResult, VaultAction


def _make_runner(vault_lifecycle=None):
    """Create a StrategyRunner with mocked dependencies."""
    state_manager = MagicMock()
    state_manager.load_state = AsyncMock(
        return_value=SimpleNamespace(state={"is_paused": False})
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_manager,
        vault_lifecycle=vault_lifecycle,
    )


def _make_strategy():
    """Create a mock strategy that returns HoldIntent."""
    from almanak.framework.intents.vocabulary import HoldIntent

    strategy = MagicMock()
    strategy.strategy_id = "test_vault_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0xWALLET"
    strategy.decide.return_value = HoldIntent(reason="No action")
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.should_teardown.return_value = False
    return strategy


class TestVaultLifecycleHookCalledBeforeDecide:
    """Vault hook is called before decide() when vault_lifecycle is configured."""

    @pytest.mark.asyncio
    async def test_pre_decide_hook_called(self):
        """pre_decide_hook is called with the strategy before decide()."""
        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.return_value = VaultAction.HOLD
        runner = _make_runner(vault_lifecycle=vault_lifecycle)
        strategy = _make_strategy()

        await runner.run_iteration(strategy)

        vault_lifecycle.pre_decide_hook.assert_called_once_with(strategy)

    @pytest.mark.asyncio
    async def test_decide_still_called_after_hold(self):
        """When vault hook returns HOLD, decide() proceeds normally."""
        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.return_value = VaultAction.HOLD
        runner = _make_runner(vault_lifecycle=vault_lifecycle)
        strategy = _make_strategy()

        result = await runner.run_iteration(strategy)

        strategy.decide.assert_called_once()
        assert result.status == IterationStatus.HOLD


class TestSettlementRunsWhenDue:
    """Settlement runs when pre_decide_hook returns SETTLE or RESUME_SETTLE."""

    @pytest.mark.asyncio
    async def test_settlement_runs_on_settle(self):
        """run_settlement_cycle is called when hook returns SETTLE."""
        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.return_value = VaultAction.SETTLE
        vault_lifecycle.run_settlement_cycle = AsyncMock(
            return_value=SettlementResult(success=True, new_total_assets=1000000, epoch_id=1)
        )
        runner = _make_runner(vault_lifecycle=vault_lifecycle)
        strategy = _make_strategy()

        result = await runner.run_iteration(strategy)

        vault_lifecycle.run_settlement_cycle.assert_called_once_with(strategy)
        # decide() should still be called after settlement
        strategy.decide.assert_called_once()
        assert result.status == IterationStatus.HOLD

    @pytest.mark.asyncio
    async def test_settlement_runs_on_resume_settle(self):
        """run_settlement_cycle is called when hook returns RESUME_SETTLE."""
        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.return_value = VaultAction.RESUME_SETTLE
        vault_lifecycle.run_settlement_cycle = AsyncMock(
            return_value=SettlementResult(success=True, new_total_assets=500000, epoch_id=2)
        )
        runner = _make_runner(vault_lifecycle=vault_lifecycle)
        strategy = _make_strategy()

        result = await runner.run_iteration(strategy)

        vault_lifecycle.run_settlement_cycle.assert_called_once_with(strategy)
        strategy.decide.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_vault_settled_called_after_settlement(self):
        """strategy.on_vault_settled() is called with the SettlementResult."""
        settlement_result = SettlementResult(success=True, new_total_assets=1000000, epoch_id=1)
        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.return_value = VaultAction.SETTLE
        vault_lifecycle.run_settlement_cycle = AsyncMock(return_value=settlement_result)
        runner = _make_runner(vault_lifecycle=vault_lifecycle)
        strategy = _make_strategy()

        await runner.run_iteration(strategy)

        strategy.on_vault_settled.assert_called_once_with(settlement_result)

    @pytest.mark.asyncio
    async def test_on_vault_settled_skipped_when_not_present(self):
        """If strategy has no on_vault_settled, no error occurs."""
        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.return_value = VaultAction.SETTLE
        vault_lifecycle.run_settlement_cycle = AsyncMock(
            return_value=SettlementResult(success=True, epoch_id=1)
        )
        runner = _make_runner(vault_lifecycle=vault_lifecycle)
        strategy = _make_strategy()
        del strategy.on_vault_settled  # Remove the attribute

        # Should not raise
        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.HOLD


class TestSettlementSkippedWhenNotDue:
    """Settlement does not run when pre_decide_hook returns HOLD."""

    @pytest.mark.asyncio
    async def test_no_settlement_on_hold(self):
        """run_settlement_cycle is NOT called when hook returns HOLD."""
        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.return_value = VaultAction.HOLD
        vault_lifecycle.run_settlement_cycle = AsyncMock()
        runner = _make_runner(vault_lifecycle=vault_lifecycle)
        strategy = _make_strategy()

        await runner.run_iteration(strategy)

        vault_lifecycle.run_settlement_cycle.assert_not_called()


class TestSettlementErrorHandling:
    """Settlement errors are caught and logged, do not crash the iteration."""

    @pytest.mark.asyncio
    async def test_settlement_error_does_not_crash(self):
        """If settlement raises an exception, the iteration continues."""
        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.return_value = VaultAction.SETTLE
        vault_lifecycle.run_settlement_cycle = AsyncMock(side_effect=RuntimeError("RPC failed"))
        runner = _make_runner(vault_lifecycle=vault_lifecycle)
        strategy = _make_strategy()

        result = await runner.run_iteration(strategy)

        # Iteration should continue to decide() even after settlement error
        strategy.decide.assert_called_once()
        assert result.status == IterationStatus.HOLD

    @pytest.mark.asyncio
    async def test_pre_decide_hook_error_does_not_crash(self):
        """If pre_decide_hook raises, the iteration continues."""
        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.side_effect = RuntimeError("State load failed")
        runner = _make_runner(vault_lifecycle=vault_lifecycle)
        strategy = _make_strategy()

        result = await runner.run_iteration(strategy)

        strategy.decide.assert_called_once()
        assert result.status == IterationStatus.HOLD

    @pytest.mark.asyncio
    async def test_failed_settlement_continues_to_decide(self):
        """When settlement returns success=False, decide() still runs."""
        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.return_value = VaultAction.SETTLE
        vault_lifecycle.run_settlement_cycle = AsyncMock(
            return_value=SettlementResult(success=False)
        )
        runner = _make_runner(vault_lifecycle=vault_lifecycle)
        strategy = _make_strategy()

        result = await runner.run_iteration(strategy)

        strategy.decide.assert_called_once()
        assert result.status == IterationStatus.HOLD


class TestNonVaultStrategyUnaffected:
    """Strategies without vault_lifecycle are completely unaffected."""

    @pytest.mark.asyncio
    async def test_no_vault_lifecycle_no_hook(self):
        """Without vault_lifecycle, no vault logic runs."""
        runner = _make_runner(vault_lifecycle=None)
        strategy = _make_strategy()

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.HOLD
        strategy.decide.assert_called_once()

    @pytest.mark.asyncio
    async def test_runner_constructor_default_none(self):
        """StrategyRunner defaults vault_lifecycle to None."""
        runner = StrategyRunner(
            price_oracle=MagicMock(),
            balance_provider=MagicMock(),
            execution_orchestrator=MagicMock(),
            state_manager=MagicMock(),
        )
        assert runner._vault_lifecycle is None
