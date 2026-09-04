from __future__ import annotations

import asyncio
import gc
import weakref
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import HistoricalDataCapability, MarketState, token_ref_display
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger
from almanak.framework.backtesting.pnl.providers.pool_history_fallback import DailyPoolHistory
from almanak.framework.backtesting.pnl.providers.snapshot_pool_analytics import HistoricalPoolAnalyticsTarget
from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import HistoricalPoolTVL
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.timeframes import OHLCVTimeframe
from almanak.framework.market.errors import PoolPriceUnavailableError
from tests.backtesting_funding import pnl_token_funding

_ARBITRUM_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_ARBITRUM_WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
_ARBITRUM_POOL = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
_CURVE_POOL = "0x186cf879186986a20aadfb7ead50e3c20cb26cec"
_ARBITRUM_WBTC = "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f"
_ARBITRUM_TBTC = "0x6c84a8f1c29108f47a79964b5fe888d4f4d0de40"


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


def test_curve_permission_binding_builds_execution_descriptor_without_archive_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from almanak.connectors.curve import pool_binding
    from almanak.connectors.curve.pool_binding import CurvePoolPermissionBinding
    from almanak.framework.backtesting.pnl.providers.perp import _gateway_history

    binding = CurvePoolPermissionBinding(
        chain="arbitrum",
        pool_address=_CURVE_POOL,
        coin_symbols=("WBTC", "TBTC"),
        coin_addresses=(_ARBITRUM_WBTC, _ARBITRUM_TBTC),
        coin_decimals=(8, 18),
        lp_token=_CURVE_POOL,
        n_coins=2,
        pool_type="stableswap",
        abi_families=("stableswap_legacy", "stableswap_ng"),
        is_metapool=False,
    )
    client = SimpleNamespace(is_connected=True)
    monkeypatch.setattr(_gateway_history, "get_connected_gateway_client", lambda: (client, object()))
    monkeypatch.setattr(
        pool_binding,
        "resolve_configured_pool_bindings",
        lambda **kwargs: (binding,) if kwargs["gateway_client"] is client else (),
    )
    strategy_config = {
        "permission_bindings": [
            {
                "protocol": "curve",
                "resource_type": "pool",
                "chain": "arbitrum",
                "address": _CURVE_POOL,
                "coin_addresses": [_ARBITRUM_WBTC, _ARBITRUM_TBTC],
            }
        ]
    }

    descriptors = _engine_helpers._configured_pool_descriptors(strategy_config, chain="arbitrum")

    assert len(descriptors) == 1
    assert descriptors[0].key == ("arbitrum", "curve", _CURVE_POOL)
    assert descriptors[0].fee_rate is None


def test_strategy_without_curve_binding_does_not_open_descriptor_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.framework.backtesting.pnl.providers.perp import _gateway_history

    monkeypatch.setattr(
        _gateway_history,
        "get_connected_gateway_client",
        lambda: pytest.fail("non-Curve strategy must not connect descriptor transport"),
    )

    assert _engine_helpers._configured_pool_descriptors({"protocol": "gmx_v2"}, chain="arbitrum") == ()


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

    async def prewarm(
        _source, _strategy, _strategy_config, *, require_complete: bool = False, prepared_targets=()
    ) -> None:
        require_complete_values.append(require_complete)

    monkeypatch.setattr(_engine_helpers, "_prewarm_declared_funding_history", prewarm)

    result = await _backtester(_Provider()).check_readiness(_Strategy(), _config())

    assert result.ready
    assert require_complete_values == [True]


@pytest.mark.asyncio
async def test_readiness_fails_before_decide_when_required_pool_analytics_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy = _Strategy()
    strategy.backtest_pool_analytics_targets = (
        HistoricalPoolAnalyticsTarget("arbitrum", "uniswap_v3", _ARBITRUM_POOL),
    )

    class _UnavailableView:
        def read_pool_tvl_usd(self, **_kwargs: Any) -> None:
            raise ValueError("no historical USD price for either pool token")

    class _PoolStateSource:
        def view_at(self, _timestamp: datetime) -> _UnavailableView:
            return _UnavailableView()

    class _NoPoolHistory:
        def daily_history(self, **_kwargs: Any) -> None:
            return None

    async def prepare_pool_state(*_args: Any, **_kwargs: Any) -> _PoolStateSource:
        return _PoolStateSource()

    monkeypatch.setattr(_engine_helpers, "_prepare_declared_historical_pool_state", prepare_pool_state)
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.data_broker.pool_history_provider",
        lambda: _NoPoolHistory(),
    )

    result = await _backtester(_Provider()).check_readiness(strategy, _config())

    assert result.status == "not_ready"
    assert result.blockers[0]["failed_checks"] == ["historical_pool_analytics"]
    assert "no historical USD price for either pool token" in result.blockers[0]["message"]
    assert strategy.decide_calls == 0


def test_pool_analytics_preflight_wraps_pool_price_unavailable() -> None:
    target = HistoricalPoolAnalyticsTarget("arbitrum", "uniswap_v3", _ARBITRUM_POOL)

    class _UnavailableReader:
        def get_pool_analytics(self, **kwargs: Any) -> None:
            raise PoolPriceUnavailableError(kwargs["pool_address"], "archive state unavailable")

    with pytest.raises(PreflightValidationError) as caught:
        _engine_helpers._validate_declared_historical_pool_analytics(
            _UnavailableReader(),
            (target,),
            datetime(2026, 1, 1, tzinfo=UTC),
        )

    assert caught.value.failed_checks == ["historical_pool_analytics"]
    assert isinstance(caught.value.__cause__, PoolPriceUnavailableError)


@pytest.mark.asyncio
async def test_readiness_accepts_required_archive_valued_pool_tvl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy = _Strategy()
    strategy.backtest_pool_analytics_targets = (
        HistoricalPoolAnalyticsTarget("arbitrum", "uniswap_v3", _ARBITRUM_POOL),
    )

    class _AvailableView:
        def read_pool_tvl_usd(self, **kwargs: Any) -> DataEnvelope[HistoricalPoolTVL]:
            tick = kwargs["market_state"].timestamp
            return DataEnvelope(
                value=HistoricalPoolTVL(
                    tvl_usd=Decimal("1000000"),
                    token0_value_usd=Decimal("500000"),
                    token1_value_usd=Decimal("500000"),
                    token0_weight=0.5,
                    token1_weight=0.5,
                ),
                meta=DataMeta(
                    source="historical:on_chain_archive+historical_price:fixture",
                    observed_at=tick,
                    block_number=123,
                    freshness_reference_at=tick,
                ),
                classification=DataClassification.INFORMATIONAL,
            )

    class _PoolStateSource:
        def view_at(self, _timestamp: datetime) -> _AvailableView:
            return _AvailableView()

    class _NoPoolHistory:
        def daily_history(self, **_kwargs: Any) -> None:
            return None

    async def prepare_pool_state(*_args: Any, **_kwargs: Any) -> _PoolStateSource:
        return _PoolStateSource()

    monkeypatch.setattr(_engine_helpers, "_prepare_declared_historical_pool_state", prepare_pool_state)
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.data_broker.pool_history_provider",
        lambda: _NoPoolHistory(),
    )

    result = await _backtester(_Provider()).check_readiness(strategy, _config())

    assert result.ready
    assert result.observations_checked >= 2
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_readiness_accepts_volume_only_analytics_without_pool_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy = _Strategy()
    strategy.backtest_pool_analytics_targets = (
        HistoricalPoolAnalyticsTarget(
            "arbitrum",
            "curve",
            _ARBITRUM_POOL,
            frozenset({"volume_24h_usd"}),
        ),
    )

    def reject_pool_state_construction(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("volume-only analytics must not construct an exact pool-state source")

    class _MeasuredVolumeHistory:
        def daily_history(self, **_kwargs: Any) -> DailyPoolHistory:
            return DailyPoolHistory(
                tvl=None,
                tvl_source="",
                volume_24h=Decimal("12345"),
                volume_source="fixture",
            )

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.snapshot_pool_state.SnapshotPoolStateSource",
        reject_pool_state_construction,
    )
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.data_broker.pool_history_provider",
        lambda: _MeasuredVolumeHistory(),
    )

    result = await _backtester(_Provider()).check_readiness(strategy, _config())

    assert result.ready
    assert result.blockers == ()
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_readiness_accepts_required_curve_fee_apy_without_archive_pool_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy = _Strategy()
    strategy.backtest_pool_analytics_targets = (
        HistoricalPoolAnalyticsTarget(
            "arbitrum",
            "curve",
            _ARBITRUM_POOL,
            frozenset({"fee_apy"}),
        ),
    )

    class _FeeApyHistory:
        def daily_history(self, **_kwargs: Any) -> DailyPoolHistory:
            return DailyPoolHistory(
                tvl=None,
                tvl_source="",
                volume_24h=None,
                volume_source="",
                fee_apy=Decimal("1.37"),
                fee_apy_source="defillama",
            )

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.data_broker.pool_history_provider",
        lambda: _FeeApyHistory(),
    )

    result = await _backtester(_Provider()).check_readiness(strategy, _config())

    assert result.ready
    assert result.observations_checked >= 2
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_runner_validates_full_pool_analytics_grid_before_decide(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy = _Strategy()
    strategy.backtest_pool_analytics_targets = (
        HistoricalPoolAnalyticsTarget("arbitrum", "uniswap_v3", _ARBITRUM_POOL),
    )

    class _CountingProvider(_Provider):
        def __init__(self) -> None:
            super().__init__()
            self.iterate_calls = 0

        async def iterate(self, config: Any):
            self.iterate_calls += 1
            async for item in super().iterate(config):
                yield item

    class _LateGapView:
        def read_pool_tvl_usd(self, **kwargs: Any) -> DataEnvelope[HistoricalPoolTVL]:
            tick = kwargs["market_state"].timestamp
            if tick == config.end_time:
                raise ValueError("late historical pool analytics gap")
            return DataEnvelope(
                value=HistoricalPoolTVL(
                    tvl_usd=Decimal("1000000"),
                    token0_value_usd=Decimal("500000"),
                    token1_value_usd=Decimal("500000"),
                    token0_weight=0.5,
                    token1_weight=0.5,
                ),
                meta=DataMeta(
                    source="historical:on_chain_archive+historical_price:fixture",
                    observed_at=tick,
                    block_number=123,
                    freshness_reference_at=tick,
                ),
                classification=DataClassification.INFORMATIONAL,
            )

    class _PoolStateSource:
        def view_at(self, _timestamp: datetime) -> _LateGapView:
            return _LateGapView()

        def descriptors(self) -> tuple[Any, ...]:
            return ()

    class _NoPoolHistory:
        def daily_history(self, **_kwargs: Any) -> None:
            return None

    async def prepare_pool_state(*_args: Any, **_kwargs: Any) -> _PoolStateSource:
        return _PoolStateSource()

    provider = _CountingProvider()
    backtester = _backtester(provider)
    config = _config()
    logger = BacktestLogger(backtest_id="analytics-grid-runner", json_format=False)
    state = _engine_helpers.initialize_backtest(
        backtester=backtester,
        strategy=strategy,
        config=config,
        bt_logger=logger,
    )
    monkeypatch.setattr(_engine_helpers, "_prepare_declared_historical_pool_state", prepare_pool_state)
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.data_broker.pool_history_provider",
        lambda: _NoPoolHistory(),
    )

    with pytest.raises(_engine_helpers.PreflightValidationError, match="late historical pool analytics gap"):
        await _engine_helpers.execute_iteration_loop(
            backtester=backtester,
            strategy=strategy,
            config=config,
            bt_logger=logger,
            state=state,
        )

    assert provider.iterate_calls == 1
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_runner_spools_large_analytics_grid_with_bounded_state_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy = _Strategy()
    strategy.backtest_pool_analytics_targets = (
        HistoricalPoolAnalyticsTarget(
            "arbitrum",
            "curve",
            _ARBITRUM_POOL,
            frozenset({"volume_24h_usd"}),
        ),
    )

    class _LargeReplayProvider(_Provider):
        def __init__(self) -> None:
            super().__init__()
            self.iterate_calls = 0
            self.first_pass_refs: list[weakref.ReferenceType[MarketState]] = []

        async def iterate(self, config: Any):
            self.iterate_calls += 1
            if self.iterate_calls > 1:
                raise AssertionError("validated provider stream must not be requested twice")
            timestamp = config.start_time
            while timestamp <= config.end_time:
                state = MarketState(
                    timestamp=timestamp,
                    prices={
                        token: Decimal("1") if self._is_usdc(token) else Decimal("2000") for token in config.tokens
                    },
                    chain="arbitrum",
                )
                self.first_pass_refs.append(weakref.ref(state))
                yield timestamp, state
                timestamp += timedelta(hours=1)

    class _MeasuredVolumeHistory:
        def daily_history(self, **_kwargs: Any) -> DailyPoolHistory:
            return DailyPoolHistory(
                tvl=None,
                tvl_source="",
                volume_24h=Decimal("12345"),
                volume_source="fixture",
            )

    class StopAfterReplay(RuntimeError):
        pass

    provider = _LargeReplayProvider()
    backtester = _backtester(provider)
    config = _config()
    config.end_time = config.start_time + timedelta(hours=999)
    logger = BacktestLogger(backtest_id="analytics-grid-bounded-replay", json_format=False)
    created_spools: list[Any] = []
    real_temporary_file = _engine_helpers.tempfile.TemporaryFile

    def tracked_temporary_file(*args: Any, **kwargs: Any) -> Any:
        spool = real_temporary_file(*args, **kwargs)
        created_spools.append(spool)
        return spool

    state = _engine_helpers.initialize_backtest(
        backtester=backtester,
        strategy=strategy,
        config=config,
        bt_logger=logger,
    )
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.data_broker.pool_history_provider",
        lambda: _MeasuredVolumeHistory(),
    )
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.engine.create_market_snapshot_from_state",
        lambda **_kwargs: (_ for _ in ()).throw(StopAfterReplay()),
    )
    monkeypatch.setattr(_engine_helpers.tempfile, "TemporaryFile", tracked_temporary_file)

    with pytest.raises(StopAfterReplay):
        await _engine_helpers.execute_iteration_loop(
            backtester=backtester,
            strategy=strategy,
            config=config,
            bt_logger=logger,
            state=state,
        )

    gc.collect()
    assert provider.iterate_calls == 1
    assert len(provider.first_pass_refs) == 1_000
    assert sum(ref() is not None for ref in provider.first_pass_refs) == 0
    assert len(created_spools) == 1
    assert created_spools[0].closed
    assert strategy.decide_calls == 0


@pytest.mark.parametrize("exit_error", [None, RuntimeError, asyncio.CancelledError])
def test_spooled_market_state_scope_closes_for_every_exit(exit_error: type[BaseException] | None) -> None:
    spool = _engine_helpers.tempfile.TemporaryFile(mode="w+b")
    iterator = _engine_helpers._SpooledMarketStateIterator(spool)

    if exit_error is None:
        with _engine_helpers._market_state_iterator_scope(iterator):
            pass
    else:
        with pytest.raises(exit_error), _engine_helpers._market_state_iterator_scope(iterator):
            raise exit_error

    assert spool.closed


@pytest.mark.asyncio
async def test_readiness_rejects_curve_when_required_fee_apy_coverage_disappears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy = _Strategy()
    strategy.backtest_pool_analytics_targets = (
        HistoricalPoolAnalyticsTarget(
            "arbitrum",
            "curve",
            _ARBITRUM_POOL,
            frozenset({"fee_apy"}),
        ),
    )

    class _MissingHistory:
        def daily_history(self, **_kwargs: Any) -> None:
            return None

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.data_broker.pool_history_provider",
        lambda: _MissingHistory(),
    )

    result = await _backtester(_Provider()).check_readiness(strategy, _config())

    assert result.status == "not_ready"
    assert result.blockers[0]["failed_checks"] == ["historical_pool_analytics"]
    assert "measured no data" in result.blockers[0]["message"]
    assert strategy.decide_calls == 0


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

    async def unavailable(
        _source, _strategy, _strategy_config, *, require_complete: bool = False, prepared_targets=()
    ) -> None:
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
