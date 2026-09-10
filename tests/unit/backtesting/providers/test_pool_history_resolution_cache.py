from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.data_broker import BacktestDataBroker
from almanak.framework.backtesting.pnl.providers.pool_history_fallback import PoolHistoryFallback
from almanak.framework.data.interfaces import DataSourceUnavailable
from tests.unit.backtesting.providers.test_pool_history_fallback import _DAY, _DAY_START, _POOL, _snap
from tests.unit.backtesting.providers.test_pool_history_required_fields import _envelope, _gateway_reader


def _daily(provider):
    return provider.daily_history(pool_address=_POOL, chain="arbitrum", protocol="curve", day=_DAY)


def _raw(provider, *, start=_DAY_START, end=None, pool=_POOL):
    return provider._get_history(
        pool_address=pool,
        chain="arbitrum",
        protocol="curve",
        start=start,
        end=end or start + timedelta(days=1),
        resolution="1h",
    )


@pytest.mark.parametrize("reason", ["unsupported protocol", "pool history service not yet enabled"])
def test_mixed_support_reuses_measurements_and_new_broker_rechecks_capability(monkeypatch, reason):
    calls = []
    daily_enabled = False

    def read(**kwargs):
        calls.append(kwargs["resolution"])
        if kwargs["resolution"] == "1d":
            if not daily_enabled:
                raise DataSourceUnavailable("pool_history", reason)
            return _envelope([_snap(kwargs["start_date"], tvl=Decimal(50), volume=Decimal(9))])
        return _envelope([_snap(kwargs["start_date"] + timedelta(hours=h), volume=Decimal(0)) for h in range(24)])

    _gateway_reader(monkeypatch, read)
    provider = BacktestDataBroker().pool_history()
    first = _daily(provider)
    assert first.volume_24h == Decimal(0)
    assert first.volume_source == "fixture"
    assert first.tvl is None
    assert _daily(provider) == first
    assert calls == ["1d", "1h"]
    daily_enabled = True
    recovered = _daily(BacktestDataBroker().pool_history())
    assert recovered.tvl == Decimal(50)
    assert recovered.volume_24h == Decimal(9)
    assert calls == ["1d", "1h", "1d"]


@pytest.mark.parametrize("transport", [True, False])
def test_failed_resolution_retries_without_refetching_served_daily_measurement(monkeypatch, transport):
    calls = []
    healthy = False

    def read(**kwargs):
        calls.append(kwargs["resolution"])
        if kwargs["resolution"] == "1d":
            return _envelope([_snap(kwargs["start_date"], tvl=Decimal(0))])
        if not healthy:
            raise DataSourceUnavailable("pool_history", "provider temporarily failed", transport=transport)
        return _envelope([_snap(kwargs["start_date"] + timedelta(hours=h), volume=Decimal(1)) for h in range(24)])

    _gateway_reader(monkeypatch, read)
    provider = PoolHistoryFallback()
    first = _daily(provider)
    assert first.tvl == Decimal(0)
    assert first.volume_24h is None
    healthy = True
    recovered = _daily(provider)
    assert recovered.tvl == Decimal(0)
    assert recovered.volume_24h == Decimal(24)
    assert calls == ["1d", "1h", "1h"]


def test_resolution_cache_keeps_sources_and_caller_list_mutation_isolated(monkeypatch):
    calls = []

    def read(**kwargs):
        calls.append(kwargs["pool_address"])
        return _envelope([_snap(kwargs["start_date"], volume=Decimal(0))])

    _gateway_reader(monkeypatch, read)
    provider = PoolHistoryFallback()
    rows, source, failure = _raw(provider)
    assert source == "fixture" and failure is None
    rows.clear()
    cached, source, failure = _raw(provider)
    assert len(cached) == 1 and cached[0].volume_24h == Decimal(0)
    assert source == "fixture" and failure is None
    _raw(provider, pool="0x" + "56" * 20)
    assert len(calls) == 2


@pytest.mark.parametrize("window", ["partial", "provisional"])
def test_incomplete_windows_are_never_reused(monkeypatch, window):
    calls = []

    def read(**kwargs):
        calls.append(kwargs)
        return _envelope([_snap(kwargs["start_date"], volume=Decimal(len(calls)))])

    _gateway_reader(monkeypatch, read)
    provider = PoolHistoryFallback()
    start = _DAY_START if window == "partial" else datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=2) if window == "partial" else start + timedelta(days=1)
    first, _, _ = _raw(provider, start=start, end=end)
    second, _, _ = _raw(provider, start=start, end=end)
    assert first[0].volume_24h == Decimal(1)
    assert second[0].volume_24h == Decimal(2)
    assert len(calls) == 2


def test_completed_served_absence_remains_unmeasured_and_is_run_local(monkeypatch):
    calls = []

    def read(**kwargs):
        calls.append(kwargs)
        return _envelope([])

    _gateway_reader(monkeypatch, read)
    provider = PoolHistoryFallback()
    assert _raw(provider) == ([], "fixture", None)
    assert _raw(provider) == ([], "fixture", None)
    assert len(calls) == 1
    assert _raw(PoolHistoryFallback()) == ([], "fixture", None)
    assert len(calls) == 2


def test_resolution_measurement_cache_evicts_least_recent_window(monkeypatch):
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.providers.pool_history_fallback._MAX_RESOLUTION_CACHE_ENTRIES", 2
    )
    calls = []

    def read(**kwargs):
        calls.append(kwargs["start_date"])
        return _envelope([_snap(kwargs["start_date"], volume=Decimal(1))])

    _gateway_reader(monkeypatch, read)
    provider = PoolHistoryFallback()
    first, second, third = [_DAY_START - timedelta(days=d) for d in range(3)]
    for start in (first, second, first, third, second):
        _raw(provider, start=start)
    assert calls == [first, second, third, second]


def test_transport_pause_preserves_cached_measurement_and_retries_after_expiry(monkeypatch):
    calls = []
    daily_healthy = False

    def read(**kwargs):
        calls.append(kwargs["resolution"])
        if kwargs["resolution"] == "1d":
            if not daily_healthy:
                raise DataSourceUnavailable("pool_history", "daily connection failed", transport=True)
            return _envelope([_snap(kwargs["start_date"], tvl=Decimal(5))])
        return _envelope([_snap(kwargs["start_date"] + timedelta(hours=h), volume=Decimal(1)) for h in range(24)])

    _gateway_reader(monkeypatch, read)
    provider = PoolHistoryFallback()
    for _ in range(4):
        result = _daily(provider)
        assert result.volume_24h == Decimal(24)
        assert result.tvl is None
    assert calls == ["1d", "1h", "1d", "1d"]
    assert provider._transport_disabled_until is not None
    daily_healthy = True
    provider._transport_disabled_until = datetime.now(UTC) - timedelta(seconds=1)
    recovered = _daily(provider)
    assert recovered.tvl == Decimal(5)
    assert recovered.volume_24h == Decimal(24)
    assert calls == ["1d", "1h", "1d", "1d", "1d"]


def test_disabled_service_diagnostic_is_once_per_provider_across_resolutions(monkeypatch, caplog):
    calls = []

    def read(**kwargs):
        calls.append(kwargs["resolution"])
        raise DataSourceUnavailable("pool_history", "pool history service not yet enabled")

    _gateway_reader(monkeypatch, read)
    caplog.set_level("INFO", logger="almanak.framework.backtesting.pnl.providers.pool_history_fallback")
    provider = PoolHistoryFallback()
    assert _daily(provider) is None
    assert calls == ["1d", "1h"]
    assert _daily(provider) is None
    diagnostics = [record.message for record in caplog.records if "PoolHistoryService is disabled" in record.message]
    assert len(diagnostics) == 1
    assert "ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true" in diagnostics[0]
    assert "on the gateway" in diagnostics[0]
    assert _daily(PoolHistoryFallback()) is None
    assert len([record for record in caplog.records if "PoolHistoryService is disabled" in record.message]) == 2
