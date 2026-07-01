"""Unit tests for `_validate_and_build_context` guards in `pnl_backtest`.

Covers regression tests for:
- Issue #1700: runtime guard now raises `click.Abort` with a stderr line
  instead of relying on `assert` (which is a no-op under `python -O`).
"""

from __future__ import annotations

from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import click
import pytest

from almanak.framework.backtesting import PnLBacktestConfig
from almanak.framework.cli.backtest.pnl import _validate_and_build_context


def _make_pnl_config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 2, 1, tzinfo=UTC),
        interval_seconds=3600,
        token_funding=_pnl_token_funding(Decimal("10000"), chain="arbitrum"),
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        gas_price_gwei=Decimal("30"),
        include_gas_costs=True,
    )


class TestValidateAndBuildContextConfigGuard:
    """Issue #1700: None `pnl_config` must raise `click.Abort` (not assert)."""

    def test_builds_fresh_config_when_not_loaded(self) -> None:
        """Happy path: a fresh config is built from CLI args."""
        with patch(
            "almanak.framework.cli.backtest.pnl.validate_strategy_is_registered"
        ):
            ctx = _validate_and_build_context(
                strategy="demo",
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
                chain="arbitrum",
                tokens="WETH,USDC",
                gas_price=30.0,
                output=None,
                loaded_from_result=False,
                pnl_config=None,
            )
        assert ctx.pnl_config is not None
        assert ctx.strategy == "demo"

    def test_reuses_loaded_config_when_loaded_from_result(self) -> None:
        """When loaded_from_result=True the existing config is reused."""
        existing = _make_pnl_config()
        with patch(
            "almanak.framework.cli.backtest.pnl.validate_strategy_is_registered"
        ):
            ctx = _validate_and_build_context(
                strategy="demo",
                start=None,
                end=None,
                interval=3600,
                chain="arbitrum",
                tokens="WETH,USDC",
                gas_price=30.0,
                output=None,
                loaded_from_result=True,
                pnl_config=existing,
            )
        assert ctx.pnl_config is existing

    def test_raises_click_abort_when_config_is_none_after_load_path(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Regression for #1700.

        When `loaded_from_result=True` but `pnl_config` is somehow None, we
        must abort with a clear stderr message rather than triggering a bare
        `AssertionError` (which is a no-op under `python -O`).
        """
        with patch(
            "almanak.framework.cli.backtest.pnl.validate_strategy_is_registered"
        ):
            with pytest.raises(click.Abort):
                _validate_and_build_context(
                    strategy="demo",
                    start=None,
                    end=None,
                    interval=3600,
                    chain="arbitrum",
                    tokens="WETH,USDC",
                    gas_price=30.0,
                    output=None,
                    loaded_from_result=True,
                    pnl_config=None,
                )

        captured = capsys.readouterr()
        assert "internal error" in captured.err
        assert "PnL backtest config was not constructed" in captured.err
