"""Unit tests for StrategyRunner._write_outbox_and_fire_processor (VIB-3614).

Six behaviors are verified:
1. write_outbox_entry returns a valid outbox_id → asyncio.Task for drain_one is
   created and tracked in _pending_drain_tasks (core VIB-3652 fix).
2. write_outbox_entry returns None in non-live mode → warning logged, no asyncio
   task created, _pending_drain_tasks unchanged.
3. write_outbox_entry returns None in live mode → AccountingPersistenceError
   raised with write_kind == AccountingWriteKind.ACCOUNTING.
4. write_outbox_entry raises NotImplementedError (VIB-3482: stale gateway without
   outbox RPC) → warning logged in non-live mode; AccountingPersistenceError in
   live mode (runner wraps all non-APE exceptions in live mode).
5. Unexpected Exception raised + _is_live_mode() True → AccountingPersistenceError
   raised with write_kind == AccountingWriteKind.ACCOUNTING.
6. Unexpected Exception raised + _is_live_mode() False → warning logged, no
   exception raised, no drain task.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import (
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.state.exceptions import (
    AccountingPersistenceError,
    AccountingWriteKind,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_runner() -> StrategyRunner:
    """Build a StrategyRunner with side-effects stubbed out."""
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    state_mgr = AsyncMock()
    state_mgr.get_accounting_events_sync = MagicMock(return_value=[])
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_mgr,
        alert_manager=MagicMock(),
        config=config,
    )
    # Stub accounting processor — drain_one must be an AsyncMock so
    # asyncio.create_task can wrap it without complaining.
    runner._accounting_processor = MagicMock()
    runner._accounting_processor.drain_one = AsyncMock()
    runner._accounting_processor._deployment_id = ""
    return runner


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.strategy_id = "test-strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0xabc"
    strategy.deployment_id = "dep-1"
    return strategy


def _make_intent() -> MagicMock:
    intent = MagicMock()
    intent.intent_type = MagicMock()
    intent.intent_type.value = "SWAP"
    return intent


# =============================================================================
# Tests
# =============================================================================


class TestWriteOutboxAndFireProcessor:
    """_write_outbox_and_fire_processor unit tests."""

    @pytest.mark.asyncio
    async def test_happy_path_schedules_drain_one(self):
        """Core VIB-3652 fix: outbox_id returned → drain_one task created and tracked.

        This is the primary regression guard for the fix: before VIB-3652
        the outbox write raised NotImplementedError which was caught and silently
        returned, never scheduling drain_one.  This test verifies the full
        happy-path: write succeeds → drain_one is scheduled as an asyncio.Task
        → task is tracked in _pending_drain_tasks.
        """
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()
        assert len(runner._pending_drain_tasks) == 0

        with (
            patch(
                "almanak.framework.accounting.processor.write_outbox_entry",
                new=AsyncMock(return_value="outbox-id-abc"),
            ),
            patch(
                "almanak.framework.observability.context.get_cycle_id",
                return_value="cycle-happy",
            ),
            patch.object(runner, "_is_live_mode", return_value=True),
        ):
            await runner._write_outbox_and_fire_processor(strategy, intent, "ledger-happy-001")

        # drain_one must have been scheduled — it creates an asyncio.Task
        runner._accounting_processor.drain_one.assert_called_once_with("ledger-happy-001")
        assert len(runner._pending_drain_tasks) == 1

    @pytest.mark.asyncio
    async def test_none_return_non_live_logs_warning_no_task(self, caplog):
        """write_outbox_entry returning None in non-live mode → warning, no drain task."""
        import logging

        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()
        initial_task_count = len(runner._pending_drain_tasks)

        with (
            patch(
                "almanak.framework.accounting.processor.write_outbox_entry",
                new=AsyncMock(return_value=None),
            ),
            patch(
                "almanak.framework.observability.context.get_cycle_id",
                return_value="cycle-abc",
            ),
            patch.object(runner, "_is_live_mode", return_value=False),
            caplog.at_level(logging.WARNING),
        ):
            await runner._write_outbox_and_fire_processor(
                strategy, intent, "ledger-id-001"
            )

        assert len(runner._pending_drain_tasks) == initial_task_count
        assert any(
            "outbox write returned None" in record.message or "drain skipped" in record.message
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_none_return_live_mode_raises_accounting_error(self):
        """write_outbox_entry returning None in live mode → AccountingPersistenceError."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()

        with (
            patch(
                "almanak.framework.accounting.processor.write_outbox_entry",
                new=AsyncMock(return_value=None),
            ),
            patch(
                "almanak.framework.observability.context.get_cycle_id",
                return_value="cycle-abc",
            ),
            patch.object(runner, "_is_live_mode", return_value=True),
        ):
            with pytest.raises(AccountingPersistenceError) as exc_info:
                await runner._write_outbox_and_fire_processor(
                    strategy, intent, "ledger-id-001-live"
                )

        assert exc_info.value.write_kind == AccountingWriteKind.ACCOUNTING

    @pytest.mark.asyncio
    async def test_not_implemented_error_is_non_fatal_in_non_live_mode(self, caplog):
        """NotImplementedError (stale gateway without outbox RPC) → warning, no raise in non-live."""
        import logging

        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()

        caplog.clear()
        with (
            patch(
                "almanak.framework.accounting.processor.write_outbox_entry",
                new=AsyncMock(side_effect=NotImplementedError("save_outbox_entry not deployed")),
            ),
            patch(
                "almanak.framework.observability.context.get_cycle_id",
                return_value="cycle-nie",
            ),
            patch.object(runner, "_is_live_mode", return_value=False),
            caplog.at_level(logging.WARNING),
        ):
            # Must not raise in non-live mode
            await runner._write_outbox_and_fire_processor(strategy, intent, "ledger-nie-false")

        assert any(
            "failed" in r.message.lower() or "non-blocking" in r.message.lower()
            for r in caplog.records
        ), f"expected a warning log; got: {[r.message for r in caplog.records]}"
        assert len(runner._pending_drain_tasks) == 0

    @pytest.mark.asyncio
    async def test_not_implemented_error_raises_in_live_mode(self):
        """NotImplementedError (stale gateway without outbox RPC) → AccountingPersistenceError in live.

        GatewayStateManager.save_outbox_entry no longer raises NotImplementedError normally.
        If it does (old gateway version deployed), live mode must treat it as fatal.
        """
        from almanak.framework.state.exceptions import AccountingPersistenceError

        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()

        with (
            patch(
                "almanak.framework.accounting.processor.write_outbox_entry",
                new=AsyncMock(side_effect=NotImplementedError("save_outbox_entry not deployed")),
            ),
            patch(
                "almanak.framework.observability.context.get_cycle_id",
                return_value="cycle-nie-live",
            ),
            patch.object(runner, "_is_live_mode", return_value=True),
        ):
            with pytest.raises(AccountingPersistenceError):
                await runner._write_outbox_and_fire_processor(strategy, intent, "ledger-nie-live")

    @pytest.mark.asyncio
    async def test_exception_live_mode_raises_accounting_error(self):
        """Unexpected exception + live mode → AccountingPersistenceError raised."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()

        with (
            patch(
                "almanak.framework.accounting.processor.write_outbox_entry",
                new=AsyncMock(side_effect=RuntimeError("backend exploded")),
            ),
            patch(
                "almanak.framework.observability.context.get_cycle_id",
                return_value="cycle-xyz",
            ),
            patch.object(runner, "_is_live_mode", return_value=True),
        ):
            with pytest.raises(AccountingPersistenceError) as exc_info:
                await runner._write_outbox_and_fire_processor(
                    strategy, intent, "ledger-id-002"
                )

        assert exc_info.value.write_kind == AccountingWriteKind.ACCOUNTING

    @pytest.mark.asyncio
    async def test_exception_non_live_mode_logs_warning_no_raise(self, caplog):
        """Unexpected exception + non-live mode → warning logged, no exception."""
        import logging

        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()

        with (
            patch(
                "almanak.framework.accounting.processor.write_outbox_entry",
                new=AsyncMock(side_effect=RuntimeError("backend exploded")),
            ),
            patch(
                "almanak.framework.observability.context.get_cycle_id",
                return_value="cycle-xyz",
            ),
            patch.object(runner, "_is_live_mode", return_value=False),
            caplog.at_level(logging.WARNING),
        ):
            # Must not raise
            await runner._write_outbox_and_fire_processor(
                strategy, intent, "ledger-id-003"
            )

        # No drain tasks created
        assert len(runner._pending_drain_tasks) == 0
        # Warning must have been emitted
        assert any(
            "non-blocking" in record.message or "_write_outbox_and_fire_processor" in record.message
            for record in caplog.records
        )
