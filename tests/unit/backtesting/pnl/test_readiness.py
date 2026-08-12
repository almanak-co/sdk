from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import HistoricalDataCapability, MarketState, token_ref_display
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger
from almanak.framework.data.timeframes import OHLCVTimeframe
from tests.backtesting_funding import pnl_token_funding

_ARBITRUM_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_ARBITRUM_WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


class _Provider:
    provider_name = "readiness_fixture"
    historical_capability = HistoricalDataCapability.FULL

    def __init__(self, *, omit_second_weth: bool = False, omit_usdc: bool = False) -> None:
        self.omit_second_weth = omit_second_weth
        self.omit_usdc = omit_usdc

    @staticmethod
    def _is_usdc(token: Any) -> bool:
        token_label = token_ref_display(token).lower()
        return "usdc" in token_label or _ARBITRUM_USDC in token_label

    async def get_price(self, token: Any, timestamp: datetime) -> Decimal:
        return Decimal("1") if self._is_usdc(token) else Decimal("2000")

    async def iterate(self, config: Any):
        for index in range(2):
            timestamp = config.start_time + timedelta(hours=index)
            prices = {
                token: Decimal("1") if self._is_usdc(token) else Decimal("2000")
                for token in config.tokens
                if not (self.omit_second_weth and index == 1 and token_ref_display(token).upper() == "WETH")
                and not (self.omit_usdc and self._is_usdc(token))
            }
            yield timestamp, MarketState(timestamp=timestamp, prices=prices, chain="arbitrum")


class _AddressKeyedProvider(_Provider):
    _token_addresses = {"WETH": ("arbitrum", _ARBITRUM_WETH)}

    async def iterate(self, config: Any):
        for index in range(2):
            timestamp = config.start_time + timedelta(hours=index)
            yield (
                timestamp,
                MarketState(
                    timestamp=timestamp,
                    prices={("arbitrum", _ARBITRUM_WETH): Decimal("2000")},
                    chain="arbitrum",
                ),
            )


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


def _config(*, tokens: list[str] | None = None) -> PnLBacktestConfig:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(hours=1),
        interval_seconds=3600,
        chain="arbitrum",
        tokens=tokens or ["WETH", "USDC"],
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


@pytest.mark.parametrize("cash_token", ["USDC", _ARBITRUM_USDC])
@pytest.mark.asyncio
async def test_readiness_skips_missing_cash_equivalent_prices(cash_token: str) -> None:
    strategy = _Strategy()
    result = await _backtester(_Provider(omit_usdc=True)).check_readiness(
        strategy,
        _config(tokens=["WETH", cash_token]),
    )

    assert result.ready
    assert result.blockers == ()
    assert result.observations_checked > 0
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_readiness_uses_registered_address_for_symbol_keyed_tokens() -> None:
    strategy = _Strategy()
    result = await _backtester(_AddressKeyedProvider()).check_readiness(strategy, _config())

    assert result.ready
    assert result.blockers == ()
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


@pytest.mark.asyncio
async def test_strict_runner_repeats_funding_coverage_before_decide(monkeypatch: pytest.MonkeyPatch) -> None:
    """The execution path repeats readiness's strict funding check before tick 1."""
    strategy = _Strategy()
    strategy.config = {
        **strategy.config,
        "protocol": "gmx_v2",
        "funding_market": "XMR-USD",
        "market_address": "0x7c54d547fad72f8afbf6e5b04403a0168b654c6f",
    }
    backtester = _backtester(_Provider())
    backtester.data_config = BacktestDataConfig(
        use_historical_funding=True,
        strict_historical_mode=True,
    )
    config = _config()
    logger = BacktestLogger(backtest_id="strict-funding-runner", json_format=False)
    state = _engine_helpers.initialize_backtest(
        backtester=backtester,
        strategy=strategy,
        config=config,
        bt_logger=logger,
    )
    require_complete_values: list[bool] = []

    async def unavailable(_source, _strategy, _strategy_config, *, require_complete: bool = False) -> None:
        require_complete_values.append(require_complete)
        raise RuntimeError("declared GMX funding coverage unavailable")

    monkeypatch.setattr(_engine_helpers, "_prewarm_declared_funding_history", unavailable)

    with pytest.raises(RuntimeError, match="declared GMX funding coverage unavailable"):
        await _engine_helpers.execute_iteration_loop(
            backtester=backtester,
            strategy=strategy,
            config=config,
            bt_logger=logger,
            state=state,
        )

    assert require_complete_values == [True]
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_execution_loop_uses_strategy_data_granularity_for_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real loop must wire strategy config into the snapshot default."""
    strategy = _Strategy()
    strategy.config = {**strategy.config, "data_granularity": "4h"}
    backtester = _backtester(_Provider())
    config = _config()
    logger = BacktestLogger(backtest_id="configured-granularity-runner", json_format=False)
    state = _engine_helpers.initialize_backtest(
        backtester=backtester,
        strategy=strategy,
        config=config,
        bt_logger=logger,
    )
    captured_timeframes: list[OHLCVTimeframe] = []

    class SnapshotCaptured(RuntimeError):
        pass

    def capture_snapshot(**kwargs: Any) -> None:
        captured_timeframes.append(kwargs["default_timeframe"])
        raise SnapshotCaptured

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.engine.create_market_snapshot_from_state",
        capture_snapshot,
    )

    with pytest.raises(SnapshotCaptured):
        await _engine_helpers.execute_iteration_loop(
            backtester=backtester,
            strategy=strategy,
            config=config,
            bt_logger=logger,
            state=state,
        )

    assert captured_timeframes == [OHLCVTimeframe.FOUR_HOURS]
    assert strategy.decide_calls == 0
