from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import HistoricalDataCapability, MarketState, token_ref_display
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from tests.backtesting_funding import pnl_token_funding


class _Provider:
    provider_name = "readiness_fixture"
    historical_capability = HistoricalDataCapability.FULL

    def __init__(self, *, omit_second_weth: bool = False) -> None:
        self.omit_second_weth = omit_second_weth

    async def get_price(self, token: Any, timestamp: datetime) -> Decimal:
        return Decimal("1") if "USDC" in token_ref_display(token).upper() else Decimal("2000")

    async def iterate(self, config: Any):
        for index in range(2):
            timestamp = config.start_time + timedelta(hours=index)
            prices = {
                token: Decimal("1") if "USDC" in token_ref_display(token).upper() else Decimal("2000")
                for token in config.tokens
                if not (self.omit_second_weth and index == 1 and token_ref_display(token).upper() == "WETH")
            }
            yield timestamp, MarketState(timestamp=timestamp, prices=prices, chain="arbitrum")


class _Strategy:
    deployment_id = "readiness_probe"
    config = {
        "chain": "arbitrum",
        "token_funding": pnl_token_funding(Decimal("100"), chain="arbitrum"),
    }

    def __init__(self) -> None:
        self.decide_calls = 0

    def decide(self, market: Any) -> None:
        self.decide_calls += 1
        return None


def _config() -> PnLBacktestConfig:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=1),
        interval_seconds=3600,
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        token_funding=_Strategy.config["token_funding"],
        include_gas_costs=False,
        preflight_validation=False,
    )


def _backtester(provider: _Provider) -> PnLBacktester:
    return PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


@pytest.mark.asyncio
async def test_readiness_checks_full_price_grid_without_calling_strategy() -> None:
    strategy = _Strategy()
    result = await _backtester(_Provider()).check_readiness(strategy, _config())

    assert result.ready
    assert result.observations_checked > 0
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_readiness_fails_closed_on_later_missing_price_without_running_strategy() -> None:
    strategy = _Strategy()
    result = await _backtester(_Provider(omit_second_weth=True)).check_readiness(strategy, _config())

    assert result.status == "not_ready"
    assert result.blockers[0]["code"] == "ValueError"
    assert "No historical USD price" in result.blockers[0]["message"]
    assert "WETH" in result.blockers[0]["message"]
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_readiness_requires_complete_declared_funding_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    require_complete_values: list[bool] = []

    async def prewarm(_source, _strategy, _strategy_config, *, require_complete: bool = False) -> None:
        require_complete_values.append(require_complete)

    monkeypatch.setattr(_engine_helpers, "_prewarm_declared_funding_history", prewarm)

    result = await _backtester(_Provider()).check_readiness(_Strategy(), _config())

    assert result.ready
    assert require_complete_values == [True]
