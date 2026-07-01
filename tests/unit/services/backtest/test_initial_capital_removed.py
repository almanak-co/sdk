from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.services.backtest.models import BacktestRequest, QuickBacktestRequest, StrategySpec, TimeframeSpec


def test_backtest_request_rejects_initial_capital_usd() -> None:
    with pytest.raises(ValidationError) as exc_info:
        BacktestRequest(
            strategy_spec=StrategySpec(
                protocol="uniswap_v3",
                chain="base",
                action="swap",
                parameters={"token_funding": []},
            ),
            timeframe=TimeframeSpec(start=date(2026, 6, 1), end=date(2026, 6, 2)),
            initial_capital_usd=Decimal("10000"),
        )

    assert "initial_capital_usd" in str(exc_info.value)


def test_quick_backtest_request_rejects_initial_capital_usd() -> None:
    with pytest.raises(ValidationError) as exc_info:
        QuickBacktestRequest(
            strategy_spec=StrategySpec(
                protocol="uniswap_v3",
                chain="base",
                action="swap",
                parameters={"token_funding": []},
            ),
            initial_capital_usd=Decimal("10000"),
        )

    assert "initial_capital_usd" in str(exc_info.value)
