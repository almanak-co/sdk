"""Readiness claims follow the exact dependency evidence gathered by the runner."""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.dependencies import HistoricalDataDependency, declared_dependencies
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger
from almanak.framework.backtesting.pnl.providers.pool_history_fallback import DailyPoolHistory
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.market_snapshot import LiquidityDepthUnavailableError
from almanak.framework.market.builders import MarketSnapshotBuilder
from tests.unit.backtesting.pnl.test_readiness import _backtester, _config, _Provider, _Strategy

POOL = "0x" + "12" * 20


def dependency(config, **changes):
    value = HistoricalDataDependency(
        "volume",
        "pool_analytics",
        config.chain,
        POOL,
        "curve",
        config.start_time,
        config.end_time,
        "volume_24h_usd",
    )
    return replace(value, **changes)


@pytest.mark.asyncio
async def test_unreached_dependency_does_not_claim_missing_history(monkeypatch):
    queried = []

    class Strategy(_Strategy):
        def backtest_data_dependencies(self, config):
            return (dependency(config), dependency(config, dependency_id="second", pool_address="0x" + "34" * 20))

    class History:
        def daily_history(self, **kwargs):
            queried.append(kwargs["pool_address"])
            return None

    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", lambda: History())
    result = await _backtester(_Provider()).check_readiness(Strategy(), _config())
    assert not result.ready
    assert set(queried) == {POOL}
    assert result.dependency_coverage[0]["state"] == "incomplete_historical_coverage"
    assert result.dependency_coverage[1]["state"] is None
    assert "not yet been validated" in result.dependency_coverage[1]["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["readiness", "backtest"])
async def test_declarations_are_resolved_once_per_operation(monkeypatch, surface):
    class Strategy(_Strategy):
        calls = 0

        def backtest_data_dependencies(self, config):
            self.calls += 1
            if self.calls % 2 == 0:
                raise AssertionError("declaration was evaluated twice in one operation")
            return (dependency(config),)

    class History:
        def daily_history(self, **kwargs):
            return DailyPoolHistory(tvl=None, tvl_source="", volume_24h=Decimal(123), volume_source="fixture")

    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", lambda: History())
    strategy = Strategy()
    backtester = _backtester(_Provider())
    if surface == "readiness":
        result = await backtester.check_readiness(strategy, _config())
        assert result.ready, result.blockers
    else:
        result = await backtester.backtest(strategy, _config())
        assert result.error is None
    assert strategy.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"pool_address": "0xnothex"}, {"required_fidelity": "volume_24hr_usd"}])
async def test_malformed_declaration_is_not_an_unsupported_provider(changes):
    class Strategy(_Strategy):
        def backtest_data_dependencies(self, config):
            return (dependency(config, **changes),)

    with pytest.raises(PreflightValidationError) as caught:
        await _engine_helpers.run_preflight(
            _backtester(_Provider()),
            _config(),
            BacktestLogger(backtest_id="invalid-declaration"),
            Strategy(),
        )
    assert caught.value.code == "HISTORICAL_DATA_DECLARATION"
    assert "declaration is invalid" in str(caught.value)


@pytest.mark.parametrize("chain", ["arb", " Arbitrum "])
def test_dependency_chain_alias_is_same_run_identity(chain):
    class Strategy:
        def backtest_data_dependencies(self, config):
            return (dependency(config, chain=chain),)

    values = declared_dependencies(Strategy(), _config())
    assert values[0].chain == chain


def test_nonhistorical_provider_error_does_not_add_new_critical_record():
    class Reader:
        def read_liquidity_depth(self, **kwargs):
            raise DataSourceUnavailable("liquidity_depth", "provider unavailable", transport=True)

    snapshot = MarketSnapshotBuilder.for_strategy_runner(
        chain="arbitrum", strategy=SimpleNamespace(liquidity_depth_reader=Reader())
    )
    with pytest.raises(LiquidityDepthUnavailableError):
        snapshot.liquidity_depth(POOL)
    assert not snapshot._critical_data_failures


@pytest.mark.asyncio
async def test_unclassified_provider_failure_does_not_assert_missing_or_transient_history(monkeypatch):
    from almanak.framework.backtesting.pnl.providers.pool_history_fallback import PoolHistoryProviderUnclassified

    class Strategy(_Strategy):
        def backtest_data_dependencies(self, config):
            return (dependency(config),)

    class History:
        def daily_history(self, **kwargs):
            raise PoolHistoryProviderUnclassified("pool_history", "gateway could not serve the request")

    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", lambda: History())
    result = await _backtester(_Provider()).check_readiness(Strategy(), _config())
    assert not result.ready
    assert result.dependency_coverage[0]["state"] is None
    assert "without a classified cause" in result.dependency_coverage[0]["detail"]
