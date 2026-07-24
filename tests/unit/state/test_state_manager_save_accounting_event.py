"""Tests for StateManager.save_accounting_event delegation and failure modes.

The method is a thin warm-backend delegator with observability: it must
return False (not raise) when the backend lacks accounting-event support,
report backend soft-failures in metrics, and RE-RAISE hard failures (the
live accounting lane relies on the exception to halt, per blueprint 27).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.state.state_manager import StateManager, StateTier


def _bare_manager(warm) -> StateManager:
    """Build a StateManager without running __init__ (unit seam).

    Only the attributes save_accounting_event reads are set.
    """
    manager = object.__new__(StateManager)
    manager._initialized = True
    manager._warm = warm
    manager._record_metrics = MagicMock()
    return manager


class _WarmWithoutAccounting:
    """Warm backend predating accounting events (no save_accounting_event)."""


class TestBackendUnsupported:
    @pytest.mark.asyncio
    async def test_no_warm_backend_returns_false(self):
        manager = _bare_manager(warm=None)

        assert await manager.save_accounting_event({"event_type": "OPEN"}) is False
        manager._record_metrics.assert_not_called()

    @pytest.mark.asyncio
    async def test_backend_without_method_returns_false(self):
        manager = _bare_manager(warm=_WarmWithoutAccounting())

        assert await manager.save_accounting_event({"event_type": "OPEN"}) is False
        manager._record_metrics.assert_not_called()


class TestDelegation:
    @pytest.mark.asyncio
    async def test_success_returns_true_and_records_metrics(self):
        warm = MagicMock()
        warm.save_accounting_event = AsyncMock(return_value=True)
        manager = _bare_manager(warm)
        event = {"event_type": "OPEN"}

        assert await manager.save_accounting_event(event) is True

        warm.save_accounting_event.assert_awaited_once_with(event)
        tier, op, _latency, ok, error = manager._record_metrics.call_args[0]
        assert tier == StateTier.WARM
        assert op == "save_accounting_event"
        assert ok is True
        assert error is None

    @pytest.mark.asyncio
    async def test_backend_soft_no_op_returns_false_with_reason(self):
        """A backend returning False is a silent no-op — metrics must record
        it as a failure, not a success."""
        warm = MagicMock()
        warm.save_accounting_event = AsyncMock(return_value=False)
        manager = _bare_manager(warm)

        assert await manager.save_accounting_event({"event_type": "OPEN"}) is False

        _tier, _op, _latency, ok, error = manager._record_metrics.call_args[0]
        assert ok is False
        assert error == "backend_returned_false"

    @pytest.mark.asyncio
    async def test_backend_exception_is_recorded_and_reraised(self):
        warm = MagicMock()
        warm.save_accounting_event = AsyncMock(side_effect=RuntimeError("disk full"))
        manager = _bare_manager(warm)

        with pytest.raises(RuntimeError, match="disk full"):
            await manager.save_accounting_event({"event_type": "OPEN"})

        _tier, op, _latency, ok, error = manager._record_metrics.call_args[0]
        assert op == "save_accounting_event"
        assert ok is False
        assert "disk full" in error

    @pytest.mark.asyncio
    async def test_uninitialized_manager_initializes_first(self):
        warm = MagicMock()
        warm.save_accounting_event = AsyncMock(return_value=True)
        manager = _bare_manager(warm)
        manager._initialized = False
        manager.initialize = AsyncMock()

        assert await manager.save_accounting_event({"event_type": "OPEN"}) is True
        manager.initialize.assert_awaited_once()
