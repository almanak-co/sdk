"""Unit tests for the portfolio metrics baseline logic in runner_state.py.

Covers the VIB-3614 fallback: when total_value_usd == 0 on the first snapshot
(strategy hasn't deployed capital yet), the baseline should use available_cash_usd
so subsequent PnL calculations are meaningful.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.runner_state import _build_metrics_for_snapshot


def _make_snapshot(total_value_usd: str, available_cash_usd: str) -> MagicMock:
    snap = MagicMock()
    snap.deployment_id = "test-strategy"
    snap.timestamp = MagicMock()
    snap.total_value_usd = Decimal(total_value_usd)
    snap.available_cash_usd = Decimal(available_cash_usd)
    snap.chain = "arbitrum"
    # Required: function skips if snapshot.error is truthy or confidence is UNAVAILABLE
    snap.error = None
    snap.value_confidence = MagicMock()
    snap.value_confidence.__eq__ = lambda self, other: False  # not UNAVAILABLE
    return snap


def _make_runner() -> MagicMock:
    from decimal import Decimal

    runner = MagicMock()
    runner.state_manager = MagicMock()
    runner.state_manager.get_portfolio_metrics = AsyncMock(return_value=None)
    runner.state_manager.save_portfolio_metrics = AsyncMock()
    # VIB-4225 ACC-02: the runner-side metrics builder calls sum_ledger_gas_usd
    # to populate gas_spent_usd. The bare MagicMock attribute can't be
    # awaited, so we explicitly wire it as an AsyncMock returning 0
    # (these baseline tests don't assert on the gas trail).
    runner.state_manager.sum_ledger_gas_usd = AsyncMock(return_value=Decimal("0"))
    runner.deployment_id = "dep-test"
    runner._last_cycle_id = "cycle-1"
    return runner


class TestPortfolioBaselineFallback:
    """Baseline uses available_cash_usd when total_value_usd is zero."""

    @pytest.mark.asyncio
    async def test_non_zero_positions_use_total_value(self):
        """Baseline = total_value_usd when strategy has open positions."""
        snapshot = _make_snapshot(total_value_usd="1000", available_cash_usd="200")
        runner = _make_runner()

        with patch(
            "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
            return_value="live",
        ):
            metrics = await _build_metrics_for_snapshot(
                runner=runner,
                snapshot=snapshot,
                deployment_id="test-strategy",
            )

        assert metrics is not None
        assert metrics.initial_value_usd == Decimal("1000")

    @pytest.mark.asyncio
    async def test_zero_positions_falls_back_to_cash(self):
        """Baseline = available_cash_usd when total_value_usd is zero (pre-deployment)."""
        snapshot = _make_snapshot(total_value_usd="0", available_cash_usd="5000")
        runner = _make_runner()

        with patch(
            "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
            return_value="live",
        ):
            metrics = await _build_metrics_for_snapshot(
                runner=runner,
                snapshot=snapshot,
                deployment_id="test-strategy",
            )

        assert metrics is not None
        assert metrics.initial_value_usd == Decimal("5000"), (
            "Baseline should use wallet cash when no positions are open, "
            "not zero (which would make all future PnL calculations return zero)"
        )

    @pytest.mark.asyncio
    async def test_both_zero_emits_warning(self):
        """When both are zero, a warning is logged and baseline stays zero."""
        snapshot = _make_snapshot(total_value_usd="0", available_cash_usd="0")
        runner = _make_runner()

        # runner_state uses structlog, not stdlib logging, so caplog doesn't capture it.
        # Patch the logger directly to assert the warning call.
        mock_logger = MagicMock()
        with (
            patch(
                "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
                return_value="paper",
            ),
            patch("almanak.framework.runner.runner_state.logger", mock_logger),
        ):
            metrics = await _build_metrics_for_snapshot(
                runner=runner,
                snapshot=snapshot,
                deployment_id="test-strategy",
            )

        assert metrics is not None
        assert metrics.initial_value_usd == Decimal("0")
        warning_calls = mock_logger.warning.call_args_list
        assert any(
            "baseline is zero" in str(call).lower() or "zero" in str(call).lower()
            for call in warning_calls
        ), f"Expected baseline-zero warning; warning calls: {warning_calls}"

    @pytest.mark.asyncio
    async def test_existing_metrics_not_overwritten(self):
        """If metrics already exist, the initial_value_usd is not touched."""
        snapshot = _make_snapshot(total_value_usd="500", available_cash_usd="100")
        runner = _make_runner()

        existing = MagicMock()
        existing.initial_value_usd = Decimal("999")
        existing.timestamp = MagicMock()
        existing.total_value_usd = Decimal("999")
        runner.state_manager.get_portfolio_metrics = AsyncMock(return_value=existing)

        with patch(
            "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
            return_value="live",
        ):
            metrics = await _build_metrics_for_snapshot(
                runner=runner,
                snapshot=snapshot,
                deployment_id="test-strategy",
            )

        # initial_value_usd must stay at the original baseline, not be re-set
        assert metrics.initial_value_usd == Decimal("999")
