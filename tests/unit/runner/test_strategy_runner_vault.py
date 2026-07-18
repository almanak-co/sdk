"""Tests for vault lifecycle integration in StrategyRunner."""

from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import IterationStatus, StrategyRunner
from almanak.framework.vault.config import SettlementResult, VaultAction


def _make_runner(vault_lifecycle=None):
    """Create a StrategyRunner with mocked dependencies."""
    from almanak.framework.state.state_manager import StateData

    state_manager = MagicMock()
    state_manager.load_state = AsyncMock(
        return_value=StateData(
            deployment_id="test_vault_strategy", version=1, state={"is_paused": False}
        )
    )
    state_manager.save_state = AsyncMock()
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
    strategy.deployment_id = "test_vault_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0xWALLET"
    strategy.decide.return_value = HoldIntent(reason="No action")
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.create_market_snapshot.return_value.has_critical_data_failures.return_value = False
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

        # The runner threads its bound settlement-commit callable (VIB-5666) so
        # every confirmed settlement leg books ledger + accounting rows.
        vault_lifecycle.run_settlement_cycle.assert_called_once_with(strategy, settlement_commit=ANY)
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

        # The runner threads its bound settlement-commit callable (VIB-5666) so
        # every confirmed settlement leg books ledger + accounting rows.
        vault_lifecycle.run_settlement_cycle.assert_called_once_with(strategy, settlement_commit=ANY)
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


# --- VIB-5667 vault-safe teardown: execute_vault_release wiring ---


class TestExecuteVaultRelease:
    """The teardown vault-release step (runner_teardown.execute_vault_release)."""

    def test_noop_for_plain_strategy(self):
        """A strategy with no vault lifecycle -> release is a no-op (None)."""
        import asyncio

        from almanak.framework.runner.runner_teardown import execute_vault_release

        runner = MagicMock()
        runner._vault_lifecycle = None
        strategy = MagicMock()
        strategy.deployment_id = "plain"
        strategy.chain = "base"

        result = asyncio.run(
            execute_vault_release(runner, strategy, MagicMock(), teardown_cycle_id="teardown-x")
        )
        assert result is None

    def test_invokes_release_with_bound_commit(self):
        """For a vault strategy, release_on_teardown is called with a commit callback."""
        import asyncio

        from almanak.framework.runner.runner_teardown import execute_vault_release
        from almanak.framework.vault.config import ReleaseResult

        runner = MagicMock()
        vault_lifecycle = MagicMock()
        vault_lifecycle._config.vault_address = "0xVAULT"
        vault_lifecycle.release_on_teardown = AsyncMock(
            return_value=ReleaseResult(released=True, final_state="Closed", final_nav=1000)
        )
        runner._vault_lifecycle = vault_lifecycle
        runner.commit_teardown_intent = AsyncMock()

        strategy = MagicMock()
        strategy.deployment_id = "vaulted"
        strategy.chain = "base"

        result = asyncio.run(
            execute_vault_release(runner, strategy, MagicMock(), teardown_cycle_id="teardown-y")
        )
        assert result.released is True
        vault_lifecycle.release_on_teardown.assert_awaited_once()
        # A commit callback was bound and threaded in.
        _, kwargs = vault_lifecycle.release_on_teardown.await_args
        assert callable(kwargs["commit"])

    def test_bound_commit_drives_commit_teardown_intent(self):
        """The bound commit callback routes each leg through commit_teardown_intent."""
        import asyncio

        from almanak.framework.runner.runner_teardown import execute_vault_release
        from almanak.framework.vault.config import ReleaseResult

        captured = {}

        async def _fake_release(strategy, market, *, commit):
            # Simulate one successful release leg driving the commit callback.
            await commit(
                action_type="CLOSE_VAULT",
                bundle=MagicMock(metadata={"vault_address": "0xVAULT"}),
                execution_result=MagicMock(success=True),
                signer="0xWALLET",
            )
            return ReleaseResult(released=True)

        runner = MagicMock()
        vault_lifecycle = MagicMock()
        vault_lifecycle._config.vault_address = "0xVAULT"
        vault_lifecycle.release_on_teardown = _fake_release
        runner._vault_lifecycle = vault_lifecycle

        async def _capture_commit(strategy, intent, **kwargs):
            captured["intent_type"] = intent.intent_type.value
            captured["cycle_id"] = kwargs["teardown_cycle_id"]

        runner.commit_teardown_intent = _capture_commit

        strategy = MagicMock()
        strategy.deployment_id = "vaulted"
        strategy.chain = "base"

        asyncio.run(execute_vault_release(runner, strategy, MagicMock(), teardown_cycle_id="teardown-z"))
        assert captured["intent_type"] == "CLOSE_VAULT"
        assert captured["cycle_id"] == "teardown-z"


class TestMaybeReleaseVaultAfterTeardown:
    """The fold helper only releases on a successful teardown + present vault."""

    def _result(self, success=True):
        r = MagicMock()
        r.success = success
        r.accounting_degraded = False
        r.accounting_degraded_count = 0
        return r

    def test_skips_when_no_vault_lifecycle(self):
        import asyncio

        from almanak.framework.runner.runner_teardown import _maybe_release_vault_after_teardown

        runner = MagicMock()
        runner._vault_lifecycle = None
        runner._execute_vault_release = AsyncMock()
        tr = self._result(success=True)
        asyncio.run(_maybe_release_vault_after_teardown(runner, MagicMock(), MagicMock(), "cyc", tr, "dep"))
        runner._execute_vault_release.assert_not_awaited()

    def test_skips_when_teardown_failed(self):
        import asyncio

        from almanak.framework.runner.runner_teardown import _maybe_release_vault_after_teardown

        runner = MagicMock()
        runner._vault_lifecycle = MagicMock()
        runner._execute_vault_release = AsyncMock()
        tr = self._result(success=False)
        asyncio.run(_maybe_release_vault_after_teardown(runner, MagicMock(), MagicMock(), "cyc", tr, "dep"))
        runner._execute_vault_release.assert_not_awaited()

    def test_degraded_release_flags_accounting_degraded(self):
        import asyncio

        from almanak.framework.runner.runner_teardown import _maybe_release_vault_after_teardown
        from almanak.framework.vault.config import ReleaseResult

        runner = MagicMock()
        runner._vault_lifecycle = MagicMock()
        runner._teardown_recovery_incomplete = False
        runner._execute_vault_release = AsyncMock(
            return_value=ReleaseResult(degraded=True, reason="single-signer mismatch")
        )
        tr = self._result(success=True)
        asyncio.run(_maybe_release_vault_after_teardown(runner, MagicMock(), MagicMock(), "cyc", tr, "dep"))
        runner._execute_vault_release.assert_awaited_once()
        assert tr.accounting_degraded is True
        assert tr.accounting_degraded_count == 1

    def test_skips_when_recovery_incomplete(self):
        """VIB-5667 audit #1: a deployment-owned orphan may still be open when
        recovery is incomplete — refuse the irreversible vault close, flag degraded,
        and leave the vault Open (never release around an un-unwound position)."""
        import asyncio

        from almanak.framework.runner.runner_teardown import _maybe_release_vault_after_teardown

        runner = MagicMock()
        runner._vault_lifecycle = MagicMock()
        runner._teardown_recovery_incomplete = True
        runner._execute_vault_release = AsyncMock()
        tr = self._result(success=True)
        asyncio.run(_maybe_release_vault_after_teardown(runner, MagicMock(), MagicMock(), "cyc", tr, "dep"))
        runner._execute_vault_release.assert_not_awaited()
        assert tr.accounting_degraded is True
        assert tr.accounting_degraded_count == 1

    def test_release_exception_flags_degraded_never_faults_teardown(self):
        """A raising _execute_vault_release must NOT propagate into the teardown lane —
        it is caught, logged, and folded into accounting_degraded (VIB-5667)."""
        import asyncio

        from almanak.framework.runner.runner_teardown import _maybe_release_vault_after_teardown

        runner = MagicMock()
        runner._vault_lifecycle = MagicMock()
        runner._teardown_recovery_incomplete = False
        runner._execute_vault_release = AsyncMock(side_effect=RuntimeError("release rpc exploded"))
        tr = self._result(success=True)
        # Must not raise despite the release blowing up.
        asyncio.run(_maybe_release_vault_after_teardown(runner, MagicMock(), MagicMock(), "cyc", tr, "dep"))
        runner._execute_vault_release.assert_awaited_once()
        assert tr.accounting_degraded is True
        assert tr.accounting_degraded_count == 1
