"""Historical depth capability refusal cannot become a retried entry guard."""

from datetime import timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.error_handling import classify_error
from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger
from almanak.framework.market.errors import HistoricalLiquidityDepthUnavailableError, LiquidityDepthUnavailableError
from tests.unit.backtesting.pnl.test_historical_depth_contract import POOL, _snapshot
from tests.unit.backtesting.pnl.test_pending_intent_execution import MockSwapIntent
from tests.unit.backtesting.pnl.test_readiness import _backtester, _config, _Provider, _Strategy


def test_only_stamped_historical_depth_is_fatal_regardless_of_message():
    snapshot = _snapshot()
    snapshot._liquidity_depth_refusal_detail = "provider unavailable: timeout; retry later"
    with pytest.raises(HistoricalLiquidityDepthUnavailableError) as caught:
        snapshot.liquidity_depth(POOL)
    assert classify_error(caught.value).is_fatal
    assert not caught.value.retryable
    assert classify_error(LiquidityDepthUnavailableError(POOL, "provider unavailable")).is_recoverable


@pytest.mark.asyncio
@pytest.mark.parametrize("structural_classification", [True, False])
async def test_actual_engine_mixed_fills_require_structural_depth_classification(
    monkeypatch, structural_classification
):
    if not structural_classification:
        from almanak.framework.backtesting.pnl import error_handling

        # Reproduce the keyword-only classification while keeping actual fills and decision errors.
        monkeypatch.setattr(
            error_handling,
            "_FAIL_LOUD_DATA_ERRORS",
            tuple(
                kind
                for kind in error_handling._FAIL_LOUD_DATA_ERRORS
                if kind is not HistoricalLiquidityDepthUnavailableError
            ),
        )

    class Provider(_Provider):
        async def iterate(self, config):
            for index in range(20):
                timestamp = config.start_time + timedelta(hours=index)
                yield (
                    timestamp,
                    MarketState(
                        timestamp=timestamp,
                        prices={
                            token: Decimal(1) if self._is_usdc(token) else Decimal(2000) for token in config.tokens
                        },
                        chain="arbitrum",
                    ),
                )

    class Strategy(_Strategy):
        def decide(self, market):
            self.decide_calls += 1
            if self.decide_calls % 3 == 0:
                market.liquidity_depth(POOL)
            if self.decide_calls % 3 == 2:
                return None
            return MockSwapIntent(from_token="USDC", to_token="WETH", amount=Decimal(1))

    config = _config()
    config.end_time = config.start_time + timedelta(hours=19)
    strategy = Strategy()
    backtester = _backtester(Provider())
    logger = BacktestLogger(backtest_id="mixed-fill-depth")
    state = _engine_helpers.initialize_backtest(backtester, strategy, config, logger)
    if structural_classification:
        with pytest.raises(RuntimeError, match="Fatal error in strategy.decide") as caught:
            await _engine_helpers.execute_iteration_loop(backtester, strategy, config, logger, state)
        assert isinstance(caught.value.__cause__, HistoricalLiquidityDepthUnavailableError)
        assert strategy.decide_calls == 3
        assert len(state.portfolio.trades) == 1
        assert state.portfolio.trades[0].success
    else:
        await _engine_helpers.execute_iteration_loop(backtester, strategy, config, logger, state)
        assert strategy.decide_calls == 20
        assert len(state.portfolio.trades) >= 6
        assert all(trade.success for trade in state.portfolio.trades)
