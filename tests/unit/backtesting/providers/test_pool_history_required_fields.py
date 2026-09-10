from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.pnl.data_broker import BacktestDataBroker
from almanak.framework.backtesting.pnl.dependencies import HistoricalDataDependency
from almanak.framework.backtesting.pnl.providers.pool_history_fallback import PoolHistoryFallback
from almanak.framework.data.interfaces import DataSourceUnavailable
from tests.unit.backtesting.pnl.test_readiness import _backtester, _config, _Provider, _Strategy
from tests.unit.backtesting.providers.test_pool_history_fallback import _snap

POOL = "0x" + "34" * 20


class _DeclaredStrategy(_Strategy):
    def __init__(self, field="volume_24h_usd"):
        super().__init__()
        self.field = field

    def backtest_data_dependencies(self, config):
        return (
            HistoricalDataDependency(
                "pool_guard",
                "pool_analytics",
                config.chain,
                POOL,
                "curve",
                config.start_time,
                config.end_time,
                self.field,
            ),
        )


def _gateway_reader(monkeypatch, read):
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.perp._gateway_history.get_connected_gateway_client",
        lambda: (object(), object()),
    )
    monkeypatch.setattr(
        "almanak.framework.data.pools.history.PoolHistoryReader",
        lambda **kwargs: SimpleNamespace(get_pool_history=read),
    )


def _envelope(rows):
    return SimpleNamespace(value=rows, meta=SimpleNamespace(source="fixture"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,hourly_failure,expected",
    [
        ("volume_24h_usd", True, "transient_provider_failure"),
        ("fee_apy", True, "verified_coverage"),
        ("volume_24h_usd", False, "incomplete_historical_coverage"),
    ],
)
async def test_partial_row_is_classified_for_the_required_field(monkeypatch, field, hourly_failure, expected):
    def read(**kwargs):
        if kwargs["resolution"] == "1d":
            return _envelope([_snap(kwargs["start_date"], tvl=Decimal(100), fee_apy=Decimal(4))])
        if hourly_failure:
            raise DataSourceUnavailable("pool_history", "hourly gateway failed", transport=True)
        return _envelope([])

    _gateway_reader(monkeypatch, read)
    result = await _backtester(_Provider()).check_readiness(_DeclaredStrategy(field), _config())
    assert result.ready is (expected == "verified_coverage")
    assert result.dependency_coverage[0]["state"] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,expected",
    [
        ("volume_24h_usd", "transient_provider_failure"),
        ("fee_apy", "transient_provider_failure"),
    ],
)
async def test_daily_failure_prevents_missing_volume_claim_after_empty_hourly_response(monkeypatch, field, expected):
    def read(**kwargs):
        if kwargs["resolution"] == "1d":
            raise DataSourceUnavailable("pool_history", "daily gateway failed", transport=True)
        return _envelope([])

    _gateway_reader(monkeypatch, read)
    result = await _backtester(_Provider()).check_readiness(_DeclaredStrategy(field), _config())
    assert not result.ready
    assert result.dependency_coverage[0]["state"] == expected


@pytest.mark.asyncio
async def test_daily_volume_recovers_after_outage_and_empty_hourly_response(monkeypatch):
    healthy = False

    def read(**kwargs):
        if kwargs["resolution"] == "1h":
            return _envelope([])
        if not healthy:
            raise DataSourceUnavailable("pool_history", "daily gateway failed", transport=True)
        return _envelope([_snap(kwargs["start_date"], volume=Decimal(30))])

    _gateway_reader(monkeypatch, read)
    fallback = PoolHistoryFallback()
    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", lambda: fallback)
    backtester = _backtester(_Provider())
    first = await backtester.check_readiness(_DeclaredStrategy(), _config())
    assert first.dependency_coverage[0]["state"] == "transient_provider_failure"
    healthy = True
    recovered = await backtester.check_readiness(_DeclaredStrategy(), _config())
    assert recovered.ready
    assert recovered.dependency_coverage[0]["state"] == "verified_coverage"


@pytest.mark.parametrize(
    "daily,hourly,expected",
    [
        (None, None, None),
        ("transient_provider_failure", None, "transient_provider_failure"),
        (None, "transient_provider_failure", "transient_provider_failure"),
        ("unknown_provider_failure", "transient_provider_failure", "unknown_provider_failure"),
        ("transient_provider_failure", "unknown_provider_failure", "unknown_provider_failure"),
        ("unsupported_capability", "transient_provider_failure", "transient_provider_failure"),
        ("transient_provider_failure", "unsupported_capability", "transient_provider_failure"),
        ("unsupported_capability", "unsupported_capability", "unsupported_capability"),
        ("unsupported_capability", None, None),
        (None, "unsupported_capability", None),
    ],
)
def test_unmeasured_volume_considers_both_candidate_source_outcomes(monkeypatch, daily, hourly, expected):
    def read(**kwargs):
        failure = daily if kwargs["resolution"] == "1d" else hourly
        if failure is None:
            return _envelope([])
        reason = "unsupported protocol" if failure == "unsupported_capability" else "provider failed"
        raise DataSourceUnavailable("pool_history", reason, transport=failure == "transient_provider_failure")

    _gateway_reader(monkeypatch, read)
    fallback = PoolHistoryFallback()
    kwargs = {
        "pool_address": POOL,
        "chain": "arbitrum",
        "protocol": "curve",
        "day": _config().start_time.date(),
        "required_fields": frozenset({"volume_24h"}),
    }
    if expected is None:
        assert fallback.required_daily_history(**kwargs) is None
    else:
        with pytest.raises(DataSourceUnavailable) as caught:
            fallback.required_daily_history(**kwargs)
        assert caught.value.code == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("unsupported", [False, True])
async def test_measured_hourly_volume_survives_daily_source_failure(monkeypatch, unsupported):
    def read(**kwargs):
        if kwargs["resolution"] == "1d":
            raise DataSourceUnavailable(
                "pool_history", "unsupported protocol" if unsupported else "daily failed", transport=not unsupported
            )
        return _envelope([_snap(kwargs["start_date"] + timedelta(hours=hour), volume=Decimal(1)) for hour in range(24)])

    _gateway_reader(monkeypatch, read)
    result = await _backtester(_Provider()).check_readiness(_DeclaredStrategy(), _config())
    assert result.ready
    assert result.dependency_coverage[0]["state"] == "verified_coverage"


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["unsupported protocol", "pool history service not yet enabled"])
async def test_unsupported_daily_source_does_not_fence_hourly_recovery_on_same_provider(monkeypatch, reason):
    hourly_healthy = False

    def read(**kwargs):
        if kwargs["resolution"] == "1d":
            raise DataSourceUnavailable("pool_history", reason)
        if not hourly_healthy:
            raise DataSourceUnavailable("pool_history", "hourly connection failed", transport=True)
        return _envelope([_snap(kwargs["start_date"] + timedelta(hours=hour), volume=Decimal(1)) for hour in range(24)])

    _gateway_reader(monkeypatch, read)
    fallback = PoolHistoryFallback()
    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", lambda: fallback)
    backtester = _backtester(_Provider())
    first = await backtester.check_readiness(_DeclaredStrategy(), _config())
    assert first.dependency_coverage[0]["state"] == "transient_provider_failure"
    hourly_healthy = True
    recovered = await backtester.check_readiness(_DeclaredStrategy(), _config())
    assert recovered.ready
    assert recovered.dependency_coverage[0]["state"] == "verified_coverage"


def test_provisional_volume_is_incomplete_without_a_provider_failure(monkeypatch):
    def read(**kwargs):
        return _envelope([_snap(kwargs["start_date"], tvl=Decimal(100), volume=Decimal(30))])

    _gateway_reader(monkeypatch, read)
    fallback = PoolHistoryFallback()
    day = datetime.now(UTC).date()
    outcome = fallback.daily_history_outcome(pool_address=POOL, chain="arbitrum", protocol="curve", day=day)
    assert not outcome.cacheable
    assert outcome.unavailable_kind is None
    assert outcome.history is not None and outcome.history.volume_24h is None
    row = fallback.required_daily_history(
        pool_address=POOL,
        chain="arbitrum",
        protocol="curve",
        day=day,
        required_fields=frozenset({"volume_24h"}),
    )
    assert row is not None and row.volume_24h is None


@pytest.mark.asyncio
async def test_next_readiness_run_recovers_after_gateway_service_is_enabled(monkeypatch):
    enabled = False

    def read(**kwargs):
        if not enabled:
            raise DataSourceUnavailable("pool_history", "pool history service not yet enabled")
        return _envelope([_snap(kwargs["start_date"], tvl=Decimal(100), volume=Decimal(30))])

    _gateway_reader(monkeypatch, read)
    backtester = _backtester(_Provider())
    first = await backtester.check_readiness(_DeclaredStrategy(), _config())
    assert not first.ready
    assert first.dependency_coverage[0]["state"] == "unsupported_capability"
    enabled = True
    second = await backtester.check_readiness(_DeclaredStrategy(), _config())
    assert second.ready
    assert second.dependency_coverage[0]["state"] == "verified_coverage"


def test_broker_reuses_provider_within_run_and_isolates_another_run():
    first = BacktestDataBroker()
    second = BacktestDataBroker()
    assert first.pool_history() is first.pool_history()
    assert first.pool_history() is not second.pool_history()


def test_unclassified_gateway_rejection_does_not_claim_missing_or_transient_history(monkeypatch):
    from almanak.framework.backtesting.pnl.providers.pool_history_fallback import PoolHistoryProviderUnclassified

    def read(**kwargs):
        raise DataSourceUnavailable("pool_history", "request failed without structured failure metadata")

    _gateway_reader(monkeypatch, read)
    fallback = PoolHistoryFallback()
    with pytest.raises(PoolHistoryProviderUnclassified) as caught:
        fallback.required_daily_history(
            pool_address=POOL,
            chain="arbitrum",
            protocol="curve",
            day=_config().start_time.date(),
            required_fields=frozenset({"volume_24h"}),
        )
    assert caught.value.code == "unknown_provider_failure"
    assert not caught.value.transport


@pytest.mark.asyncio
async def test_legacy_multi_field_target_only_requires_fee_apy_on_newest_day(monkeypatch):
    from almanak.framework.backtesting.pnl.providers.snapshot_pool_analytics import HistoricalPoolAnalyticsTarget

    newest = (_config().start_time - timedelta(days=1)).date()

    def read(**kwargs):
        start = kwargs["start_date"]
        if kwargs["resolution"] == "1d":
            if start.date() == newest:
                return _envelope([_snap(start, volume=Decimal(30), fee_apy=Decimal(4))])
            raise DataSourceUnavailable("pool_history", "older daily gateway failed", transport=True)
        return _envelope([_snap(start + timedelta(hours=hour), volume=Decimal(1)) for hour in range(24)])

    _gateway_reader(monkeypatch, read)
    strategy = _Strategy()
    strategy.backtest_pool_analytics_targets = (
        HistoricalPoolAnalyticsTarget("arbitrum", "curve", POOL, frozenset({"volume_7d_usd", "fee_apy"})),
    )
    result = await _backtester(_Provider()).check_readiness(strategy, _config())
    assert result.ready
    assert result.blockers == ()
