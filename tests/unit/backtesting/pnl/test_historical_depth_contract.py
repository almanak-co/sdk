from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.dependencies import HistoricalDataDependency, HistoricalDependencyError
from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state
from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger
from almanak.framework.data.market_snapshot import LiquidityDepthUnavailableError
from almanak.framework.data.null_readers import NullLiquidityDepthReader
from almanak.framework.market.builders import MarketSnapshotBuilder
from tests.unit.backtesting.pnl.test_readiness import _backtester, _config, _Provider, _Strategy

POOL = "0x" + "12" * 20


def _snapshot():
    return create_market_snapshot_from_state(
        MarketState(timestamp=datetime(2026, 1, 1, tzinfo=UTC), prices={"USDC": Decimal(1)}, chain="arbitrum"),
        chain="arbitrum",
    )


@pytest.mark.parametrize("null_reader", [False, True])
def test_factory_refuses_depth_before_reading(null_reader):
    snapshot = _snapshot()
    if null_reader:
        snapshot._liquidity_depth_reader = NullLiquidityDepthReader()
    with pytest.raises(LiquidityDepthUnavailableError, match="historical tick-level"):
        snapshot.liquidity_depth(POOL)
    assert ("liquidity_depth", "backtest_no_historical_plane") in snapshot._critical_data_failures
    assert ("liquidity_depth", "unconfigured") not in snapshot._critical_data_failures


def test_live_unconfigured_and_unstamped_null_reader_are_distinct():
    snapshot = MarketSnapshotBuilder.for_strategy_runner(chain="arbitrum", strategy=SimpleNamespace())
    with pytest.raises(ValueError, match="No liquidity depth"):
        snapshot.liquidity_depth(POOL)
    assert ("liquidity_depth", "unconfigured") in snapshot._critical_data_failures
    snapshot = MarketSnapshotBuilder.for_pnl_backtest_state(
        chain="arbitrum", wallet_address="", state=SimpleNamespace()
    )
    with pytest.raises(LiquidityDepthUnavailableError):
        snapshot.liquidity_depth(POOL)
    assert ("liquidity_depth", "backtest_no_historical_plane") in snapshot._critical_data_failures


class _DepthStrategy(_Strategy):
    def backtest_data_dependencies(self, config):
        return (
            HistoricalDataDependency(
                dependency_id="entry_depth",
                lane="liquidity_depth",
                chain=config.chain,
                pool_address=POOL,
                protocol="uniswap_v3",
                start_time=config.start_time,
                end_time=config.end_time,
                required_fidelity="initialized_ticks",
            ),
        )


@pytest.mark.asyncio
async def test_readiness_blocks_declared_depth_before_decide():
    strategy = _DepthStrategy()
    result = await _backtester(_Provider()).check_readiness(strategy, _config())
    assert not result.ready
    assert result.blockers[0]["code"] == "HISTORICAL_DATA_DEPENDENCY"
    assert result.blockers[0]["details"]["dependencies"][0]["state"] == "unsupported_capability"
    assert result.blockers[0]["details"]["dependencies"][0]["dependency"]["pool_address"] == POOL
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_optional_preflight_flags_cannot_disable_dependency_gate():
    config = _config()
    config.preflight_validation = False
    config.fail_on_preflight_error = False
    with pytest.raises(HistoricalDependencyError):
        await _engine_helpers.run_preflight(
            _backtester(_Provider()),
            config,
            BacktestLogger(backtest_id="depth"),
            _DepthStrategy(),
        )


@pytest.mark.asyncio
async def test_explicit_altered_guard_is_labeled_and_round_trips():
    config = _config()
    original_hash = config.calculate_config_hash()
    config.altered_backtest_guards = {"entry_depth": "Evaluate entry signal without the live depth guard"}
    result = await _backtester(_Provider()).check_readiness(_DepthStrategy(), config)
    assert result.ready
    assert result.to_dict()["strategy_comparison"] == "altered_guards_not_live_equivalent"
    assert result.to_dict()["altered_guards"] == config.altered_backtest_guards
    encoded = config.to_dict()
    assert encoded["strategy_comparison"] == "altered_guards_not_live_equivalent"
    assert PnLBacktestConfig.from_dict(encoded).altered_backtest_guards == config.altered_backtest_guards
    assert config.calculate_config_hash() != original_hash
    snapshot = _snapshot()
    snapshot._altered_backtest_guards = frozenset(config.altered_backtest_guards)
    assert not snapshot.backtest_guard_enabled("entry_depth")
    assert snapshot.backtest_guard_enabled("another_guard")
    assert MarketSnapshotBuilder.for_strategy_runner(
        chain="arbitrum", strategy=SimpleNamespace()
    ).backtest_guard_enabled("entry_depth")
    with pytest.raises(LiquidityDepthUnavailableError):
        snapshot.liquidity_depth(POOL)


@pytest.mark.asyncio
async def test_unknown_override_does_not_silently_bypass_guard():
    config = _config()
    config.altered_backtest_guards = {"typo": "Diagnostic variant"}
    result = await _backtester(_Provider()).check_readiness(_DepthStrategy(), config)
    assert not result.ready
    assert "must name declared dependencies" in result.blockers[0]["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("altered", [False, True])
async def test_real_iteration_loop_wires_refusal_and_explicit_guard_branch(altered):
    snapshots = []

    class Strategy(_DepthStrategy):
        def decide(self, market):
            snapshots.append(market)
            if market.backtest_guard_enabled("entry_depth"):
                with pytest.raises(LiquidityDepthUnavailableError):
                    market.liquidity_depth(POOL)
            return None

    strategy = Strategy()
    config = _config()
    if altered:
        config.altered_backtest_guards = {"entry_depth": "Evaluate changed entry guard"}
    backtester = _backtester(_Provider())
    logger = BacktestLogger(backtest_id="depth-loop")
    state = _engine_helpers.initialize_backtest(backtester, strategy, config, logger)
    await _engine_helpers.execute_iteration_loop(backtester, strategy, config, logger, state)
    assert snapshots
    for snapshot in snapshots:
        assert snapshot.backtest_guard_enabled("entry_depth") is not altered
        assert (("liquidity_depth", "backtest_no_historical_plane") in snapshot._critical_data_failures) is not altered


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome", ["verified_coverage", "incomplete_historical_coverage", "transient_provider_failure"]
)
async def test_declared_analytics_uses_real_serving_source_and_classifies_coverage(monkeypatch, outcome):
    from almanak.framework.backtesting.pnl.providers.pool_history_fallback import DailyPoolHistory
    from almanak.framework.data.interfaces import DataSourceUnavailable

    class Strategy(_Strategy):
        def backtest_data_dependencies(self, config):
            return (
                HistoricalDataDependency(
                    "volume_guard",
                    "pool_analytics",
                    config.chain,
                    POOL,
                    "curve",
                    config.start_time,
                    config.end_time,
                    "volume_24h_usd",
                ),
            )

    class History:
        def daily_history(self, **kwargs):
            if outcome == "transient_provider_failure":
                raise DataSourceUnavailable("pool_history", "gateway connection unavailable", transport=True)
            if outcome == "incomplete_historical_coverage":
                return None
            return DailyPoolHistory(tvl=None, tvl_source="", volume_24h=Decimal(123), volume_source="fixture")

    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", lambda: History())
    strategy = Strategy()
    result = await _backtester(_Provider()).check_readiness(strategy, _config())
    assert result.ready is (outcome == "verified_coverage")
    assert result.dependency_coverage[0]["state"] == outcome
    assert result.dependency_coverage[0]["dependency"]["pool_address"] == POOL
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_empty_price_grid_cannot_certify_declared_coverage(monkeypatch):
    class EmptyProvider(_Provider):
        async def iterate(self, config):
            return
            yield

    config = _config()
    config.altered_backtest_guards = {"entry_depth": "Explicit diagnostic variant"}
    result = await _backtester(EmptyProvider()).check_readiness(_DepthStrategy(), config)
    assert not result.ready
    assert "coverage incomplete" in result.blockers[0]["message"]


@pytest.mark.asyncio
async def test_missing_final_tick_cannot_certify_run_window():
    class TruncatedProvider(_Provider):
        async def iterate(self, config):
            async for timestamp, state in super().iterate(config):
                if timestamp < config.end_time:
                    yield timestamp, state

    config = _config()
    config.altered_backtest_guards = {"entry_depth": "Explicit diagnostic variant"}
    result = await _backtester(TruncatedProvider()).check_readiness(_DepthStrategy(), config)
    assert not result.ready
    assert "coverage incomplete" in result.blockers[0]["message"]


@pytest.mark.asyncio
async def test_full_backtest_persists_altered_configuration():
    config = _config()
    config.altered_backtest_guards = {"entry_depth": "Evaluate an explicitly different strategy"}
    result = await _backtester(_Provider()).backtest(_DepthStrategy(), config)
    assert result.config["strategy_comparison"] == "altered_guards_not_live_equivalent"
    assert result.config["altered_backtest_guards"] == config.altered_backtest_guards
    assert result.to_dict()["config"]["strategy_comparison"] == "altered_guards_not_live_equivalent"


@pytest.mark.asyncio
async def test_complete_run_grid_cannot_certify_dependency_warmup_window():
    class Strategy(_Strategy):
        def backtest_data_dependencies(self, config):
            return (
                HistoricalDataDependency(
                    "volume_guard",
                    "pool_analytics",
                    config.chain,
                    POOL,
                    "curve",
                    config.start_time - timedelta(days=7),
                    config.end_time,
                    "volume_24h_usd",
                ),
            )

    result = await _backtester(_Provider()).check_readiness(Strategy(), _config())
    assert not result.ready
    coverage = result.dependency_coverage[0]
    assert coverage["state"] == "unsupported_capability"
    assert "declared dependency window" in coverage["detail"]
    assert coverage["dependency"]["start_time"] == (_config().start_time - timedelta(days=7)).isoformat()


@pytest.mark.asyncio
async def test_measured_volume_does_not_certify_a_different_required_pool_field(monkeypatch):
    from almanak.framework.backtesting.pnl.providers.pool_history_fallback import DailyPoolHistory

    class Strategy(_Strategy):
        def backtest_data_dependencies(self, config):
            return (
                HistoricalDataDependency(
                    "fee_guard",
                    "pool_analytics",
                    config.chain,
                    POOL,
                    "curve",
                    config.start_time,
                    config.end_time,
                    "fee_apy",
                ),
            )

    class History:
        def daily_history(self, **kwargs):
            return DailyPoolHistory(tvl=None, tvl_source="", volume_24h=Decimal(123), volume_source="fixture")

    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", lambda: History())
    result = await _backtester(_Provider()).check_readiness(Strategy(), _config())
    assert not result.ready
    assert result.dependency_coverage[0]["state"] == "incomplete_historical_coverage"
    assert "fee_apy" in result.blockers[0]["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", [False, True])
@pytest.mark.parametrize("protocol", ["curve", "uniswap-v3", " Curve "])
@pytest.mark.parametrize("fidelity", ["volume_24h_usd", " volume_24h_usd "])
async def test_real_pool_history_fallback_preserves_gateway_failure_kind(monkeypatch, transport, protocol, fidelity):
    from almanak.framework.backtesting.pnl.providers.pool_history_fallback import PoolHistoryFallback
    from almanak.framework.data.interfaces import DataSourceUnavailable

    class Strategy(_Strategy):
        def backtest_data_dependencies(self, config):
            return (
                HistoricalDataDependency(
                    "volume_guard",
                    "pool_analytics",
                    config.chain,
                    POOL,
                    protocol,
                    config.start_time,
                    config.end_time,
                    fidelity,
                ),
            )

    def unavailable():
        raise DataSourceUnavailable(
            "pool_history",
            "gateway unavailable" if transport else "unsupported protocol",
            transport=transport,
        )

    fallback = PoolHistoryFallback()
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp._gateway_history.get_connected_gateway_client",
        unavailable,
    )
    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", lambda: fallback)
    result = await _backtester(_Provider()).check_readiness(Strategy(), _config())
    assert not result.ready
    assert result.dependency_coverage[0]["state"] == (
        "transient_provider_failure" if transport else "unsupported_capability"
    )
    assert (
        fallback.daily_history(
            pool_address=POOL,
            chain="arbitrum",
            protocol="curve",
            day=_config().start_time.date(),
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["volume_24h_usd", "volume_7d_usd"])
async def test_guard_requires_only_its_declared_history_window(monkeypatch, field):
    from almanak.framework.backtesting.pnl.providers.pool_history_fallback import (
        DailyPoolHistory,
        DailyPoolHistoryOutcome,
        PoolHistoryFallback,
    )

    class Strategy(_Strategy):
        def backtest_data_dependencies(self, config):
            return (
                HistoricalDataDependency(
                    "volume_guard",
                    "pool_analytics",
                    config.chain,
                    POOL,
                    "curve",
                    config.start_time,
                    config.end_time,
                    field,
                ),
            )

    fallback = PoolHistoryFallback()
    newest = (_config().start_time - timedelta(days=1)).date()

    def outcome(**kwargs):
        if kwargs["day"] == newest:
            return DailyPoolHistoryOutcome(
                DailyPoolHistory(tvl=None, tvl_source="", volume_24h=Decimal(123), volume_source="fixture"),
                True,
            )
        return DailyPoolHistoryOutcome(None, False, "transient_provider_failure")

    monkeypatch.setattr(fallback, "daily_history_outcome", outcome)
    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", lambda: fallback)
    result = await _backtester(_Provider()).check_readiness(Strategy(), _config())
    assert result.ready is (field == "volume_24h_usd")
    assert result.dependency_coverage[0]["state"] == (
        "verified_coverage" if field == "volume_24h_usd" else "transient_provider_failure"
    )
