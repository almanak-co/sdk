"""Unit tests for StrategyRunner._write_outbox_and_fire_processor (VIB-3614).

Five behaviors are verified:
1. write_outbox_entry returns None in non-live mode → warning logged, no asyncio
   task created, _pending_drain_tasks unchanged.
2. write_outbox_entry returns None in live mode → AccountingPersistenceError
   raised with write_kind == AccountingWriteKind.ACCOUNTING.
3. write_outbox_entry raises NotImplementedError (VIB-3482: backend not deployed)
   → warning logged, strategy continues, no exception in any mode.
4. Unexpected Exception raised + _is_live_mode() True → AccountingPersistenceError
   raised with write_kind == AccountingWriteKind.ACCOUNTING.
5. Unexpected Exception raised + _is_live_mode() False → warning logged, no
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
    async def test_not_implemented_error_is_always_non_fatal(self, caplog):
        """NotImplementedError (VIB-3482: gateway not deployed) → warning, no raise in any mode."""
        import logging

        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()

        for live_mode in (True, False):
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
                patch.object(runner, "_is_live_mode", return_value=live_mode),
                caplog.at_level(logging.WARNING),
            ):
                # Must not raise regardless of live/non-live
                await runner._write_outbox_and_fire_processor(
                    strategy, intent, f"ledger-nie-{live_mode}"
                )

            assert any(
                "gateway outbox not yet available" in r.message or "VIB-3482" in r.message
                for r in caplog.records
            ), f"expected VIB-3482 warning in live_mode={live_mode}"
            assert len(runner._pending_drain_tasks) == 0

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
