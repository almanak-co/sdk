from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics, BacktestResult
from almanak.framework.backtesting.scenarios.crisis import CrisisScenario
from almanak.framework.backtesting.scenarios.crisis_runner import (
    CrisisBacktestConfig,
    run_crisis_backtest,
    run_crisis_backtest_sync,
    run_multiple_crisis_backtests,
    run_multiple_crisis_backtests_sync,
)

TOKEN_FUNDING = [
    {
        "symbol": "USDC",
        "address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "chain": "arbitrum",
        "amount": "10000",
        "amount_type": "usd",
    }
]


def _scenario(name: str = "recent_stress") -> CrisisScenario:
    return CrisisScenario(
        name=name,
        start_date=datetime(2026, 6, 1, tzinfo=UTC),
        end_date=datetime(2026, 6, 2, tzinfo=UTC),
        description="Recent synthetic stress window",
        warmup_days=0,
    )


def _result(config: Any) -> BacktestResult:
    return BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="test_crisis",
        start_time=config.start_time,
        end_time=config.end_time,
        metrics=BacktestMetrics(),
        initial_portfolio_value_usd=Decimal("10000"),
        final_capital_usd=Decimal("10000"),
    )


class RecordingBacktester:
    def __init__(self) -> None:
        self.data_provider = object()
        self.configs: list[Any] = []

    async def backtest(self, strategy: object, config: Any) -> BacktestResult:
        self.configs.append(config)
        return _result(config)


def test_crisis_config_to_pnl_config_forwards_token_funding() -> None:
    config = CrisisBacktestConfig(scenario=_scenario(), token_funding=TOKEN_FUNDING)

    pnl_config = config.to_pnl_config()

    assert pnl_config.token_funding == TOKEN_FUNDING


def test_crisis_config_from_dict_rejects_legacy_initial_capital() -> None:
    data = CrisisBacktestConfig(scenario=_scenario(), token_funding=TOKEN_FUNDING).to_dict()
    data["initial_capital_usd"] = "10000"

    with pytest.raises(ValueError, match="initial_capital_usd"):
        CrisisBacktestConfig.from_dict(data)


def test_crisis_config_rejects_legacy_initial_capital_in_extra_config() -> None:
    config = CrisisBacktestConfig(
        scenario=_scenario(),
        token_funding=TOKEN_FUNDING,
        extra_config={"initial_capital_usd": Decimal("10000")},
    )

    with pytest.raises(ValueError, match="initial_capital_usd"):
        config.to_pnl_config()


@pytest.mark.asyncio
async def test_run_crisis_backtest_forwards_token_funding() -> None:
    backtester = RecordingBacktester()

    await run_crisis_backtest(
        strategy=object(),
        scenario=_scenario(),
        backtester=backtester,  # type: ignore[arg-type]
        token_funding=TOKEN_FUNDING,
    )

    assert backtester.configs[0].token_funding == TOKEN_FUNDING


def test_run_crisis_backtest_sync_forwards_token_funding() -> None:
    backtester = RecordingBacktester()

    run_crisis_backtest_sync(
        strategy=object(),
        scenario=_scenario(),
        backtester=backtester,  # type: ignore[arg-type]
        token_funding=TOKEN_FUNDING,
    )

    assert backtester.configs[0].token_funding == TOKEN_FUNDING


@pytest.mark.asyncio
async def test_run_multiple_crisis_backtests_forwards_token_funding() -> None:
    backtester = RecordingBacktester()

    await run_multiple_crisis_backtests(
        strategy=object(),
        scenarios=[_scenario("stress_one"), _scenario("stress_two")],
        backtester=backtester,  # type: ignore[arg-type]
        token_funding=TOKEN_FUNDING,
    )

    assert [config.token_funding for config in backtester.configs] == [TOKEN_FUNDING, TOKEN_FUNDING]


def test_run_multiple_crisis_backtests_sync_forwards_token_funding() -> None:
    backtester = RecordingBacktester()

    run_multiple_crisis_backtests_sync(
        strategy=object(),
        scenarios=[_scenario("stress_one"), _scenario("stress_two")],
        backtester=backtester,  # type: ignore[arg-type]
        token_funding=TOKEN_FUNDING,
    )

    assert [config.token_funding for config in backtester.configs] == [TOKEN_FUNDING, TOKEN_FUNDING]
