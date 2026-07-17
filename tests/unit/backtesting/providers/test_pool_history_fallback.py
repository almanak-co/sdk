"""Unit tests for the measured pool-history ladder fallback (ALM-2940).

Pins the field routing (1d TVL, 1d-else-1h-summed volume with the coverage
floor), the miss/disable taxonomy (service-disabled memo, unsupported-pair
memo, transport streak), the per-pool-day cache, and the two consumer hooks
(LP-adapter volume rescue, liquidity-depth TVL rescue).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.backtesting.pnl.providers.pool_history_fallback import (
    _MIN_HOURLY_BARS_FOR_DAILY_VOLUME,
    DailyPoolHistory,
    DailyPoolHistoryOutcome,
    PoolHistoryFallback,
)
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.pools.history import PoolSnapshot

_DAY = date(2026, 7, 1)
_DAY_START = datetime(2026, 7, 1, tzinfo=UTC)
_POOL = "0xcdac0d6c6c59727a65f871236188350531885c43"


def _snap(ts: datetime, *, tvl: Decimal | None = None, volume: Decimal | None = None) -> PoolSnapshot:
    unmeasured = frozenset(
        name
        for name, value in (
            ("tvl", tvl),
            ("volume_24h", volume),
            ("fee_revenue_24h", None),
            ("token0_reserve", None),
            ("token1_reserve", None),
        )
        if value is None
    )
    return PoolSnapshot(
        timestamp=ts,
        tvl=tvl,
        volume_24h=volume,
        fee_revenue_24h=None,
        token0_reserve=None,
        token1_reserve=None,
        unmeasured_fields=unmeasured,
    )


def _fallback_with_history(by_resolution: dict[str, tuple[list[PoolSnapshot] | None, str]]) -> PoolHistoryFallback:
    fallback = PoolHistoryFallback()
    fallback._calls: list[str] = []  # type: ignore[attr-defined]

    def fake_get_history(*, resolution: str, **_kwargs):
        fallback._calls.append(resolution)  # type: ignore[attr-defined]
        return by_resolution.get(resolution, (None, ""))

    fallback._get_history = fake_get_history  # type: ignore[method-assign]
    return fallback


# =============================================================================
# Field routing
# =============================================================================


def test_daily_row_serves_both_fields_without_hourly_call():
    fallback = _fallback_with_history(
        {"1d": ([_snap(_DAY_START, tvl=Decimal("100"), volume=Decimal("42"))], "the_graph")}
    )
    history = fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert history == DailyPoolHistory(
        tvl=Decimal("100"), tvl_source="the_graph", volume_24h=Decimal("42"), volume_source="the_graph"
    )
    assert fallback._calls == ["1d"]  # type: ignore[attr-defined]


def test_unmeasured_daily_volume_falls_to_hourly_sum():
    """DefiLlama-shaped 1d rows (TVL only) route volume through the 1h sum."""
    hourly = [_snap(datetime(2026, 7, 1, h, tzinfo=UTC), volume=Decimal("10")) for h in range(24)]
    fallback = _fallback_with_history(
        {
            "1d": ([_snap(_DAY_START, tvl=Decimal("7583793"))], "defillama"),
            "1h": (hourly, "geckoterminal"),
        }
    )
    history = fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert history is not None
    assert history.tvl == Decimal("7583793")
    assert history.tvl_source == "defillama"
    assert history.volume_24h == Decimal("240")
    assert history.volume_source == "geckoterminal"
    assert fallback._calls == ["1d", "1h"]  # type: ignore[attr-defined]


def test_hourly_sum_below_coverage_floor_is_a_miss_not_a_floor_estimate():
    bars = _MIN_HOURLY_BARS_FOR_DAILY_VOLUME - 1
    hourly = [_snap(datetime(2026, 7, 1, h, tzinfo=UTC), volume=Decimal("10")) for h in range(bars)]
    fallback = _fallback_with_history(
        {
            "1d": ([_snap(_DAY_START, tvl=Decimal("50"))], "defillama"),
            "1h": (hourly, "geckoterminal"),
        }
    )
    history = fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert history is not None
    assert history.tvl == Decimal("50")
    assert history.volume_24h is None  # partial coverage never masquerades as a day's volume
    assert history.volume_source == ""


def test_nothing_measured_returns_none():
    fallback = _fallback_with_history({"1d": ([], "defillama"), "1h": ([], "geckoterminal")})
    assert fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY) is None


def test_future_day_short_circuits_without_rpc():
    fallback = _fallback_with_history({})
    future = date(2100, 1, 1)
    assert fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=future) is None
    assert fallback._calls == []  # type: ignore[attr-defined]


def test_daily_result_and_miss_are_cached():
    fallback = _fallback_with_history({"1d": ([_snap(_DAY_START, tvl=Decimal("1"), volume=Decimal("2"))], "the_graph")})
    first = fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    second = fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert first == second
    assert fallback._calls == ["1d"]  # type: ignore[attr-defined]  # cache hit — one fetch

    miss = _fallback_with_history({"1d": ([], ""), "1h": ([], "")})
    assert miss.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY) is None
    assert miss.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY) is None
    assert miss._calls == ["1d", "1h"]  # type: ignore[attr-defined]  # miss cached too


# =============================================================================
# Miss / disable taxonomy
# =============================================================================


def test_service_disabled_memoizes_process_wide():
    fallback = PoolHistoryFallback()
    fallback._classify_miss(
        "gateway error: PoolHistoryService not yet enabled - see VIB-4728", chain="base", protocol="aerodrome"
    )
    assert fallback._disabled_reason == "service disabled"
    fallback._calls = []  # type: ignore[attr-defined]
    fallback._get_history = lambda **_: pytest.fail("disabled fallback must not fetch")  # type: ignore[method-assign]
    assert fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY) is None


def test_unsupported_pair_memoizes_per_protocol_chain():
    fallback = PoolHistoryFallback()
    fallback._classify_miss(
        "gateway error: unsupported (chain, protocol) pair: ('optimism', 'curve')", chain="optimism", protocol="curve"
    )
    assert ("curve", "optimism") in fallback._unsupported_pairs
    assert fallback._disabled_reason is None  # pair memo never disables globally
    fallback._get_history = lambda **_: pytest.fail("memoized pair must not fetch")  # type: ignore[method-assign]
    assert fallback.daily_history(pool_address=_POOL, chain="optimism", protocol="curve", day=_DAY) is None


def test_transport_streak_disables_after_two_and_resets_on_success():
    fallback = PoolHistoryFallback()
    fallback._classify_miss(
        "gateway error: StatusCode.UNAVAILABLE: connect refused", chain="base", protocol="aerodrome"
    )
    assert fallback._disabled_reason is None

    # An interleaved SUCCESSFUL _get_history call must reset the streak —
    # exercised through the real reader path, not by mutating the counter
    # (CodeRabbit PR review, #3283).
    envelope = SimpleNamespace(
        value=[_snap(_DAY_START, tvl=Decimal("1"))],
        meta=SimpleNamespace(source="defillama"),
    )
    reader = SimpleNamespace(get_pool_history=lambda **_kwargs: envelope)
    with (
        patch(
            "almanak.framework.backtesting.pnl.providers.perp._gateway_history.get_connected_gateway_client",
            return_value=(object(), object()),
        ),
        patch("almanak.framework.data.pools.history.PoolHistoryReader", return_value=reader),
    ):
        rows, source = fallback._get_history(
            pool_address=_POOL, chain="base", protocol="aerodrome", start=_DAY_START, end=_DAY_START, resolution="1d"
        )
    assert rows is not None and source == "defillama"
    assert fallback._transport_failure_streak == 0  # success reset

    fallback._classify_miss(
        "gateway error: StatusCode.UNAVAILABLE: connect refused", chain="base", protocol="aerodrome"
    )
    assert fallback._transport_disabled_until is None  # streak restarted — one strike again

    fallback._classify_miss("gateway client not connected: channel is None", chain="base", protocol="aerodrome")
    # Transient breaker PAUSES (a deadline is set) rather than permanently
    # disabling — the singleton must not poison every later backtest.
    assert fallback._transport_disabled_until is not None
    assert fallback._disabled_reason is None  # never the permanent config-disable


def test_structured_transport_flag_counts_toward_streak():
    """DataSourceUnavailable.transport=True must strike even when the text has no marker."""
    fallback = PoolHistoryFallback()
    fallback._classify_miss(
        "some new failure shape without markers", chain="base", protocol="aerodrome", transport=True
    )
    fallback._classify_miss(
        "some new failure shape without markers", chain="base", protocol="aerodrome", transport=True
    )
    assert fallback._transport_disabled_until is not None


def test_transport_pause_self_heals_after_cooldown():
    """The transport breaker is transient: once the cooldown elapses the next
    lookup re-arms with a fresh streak and retries (a permanent disable would
    poison every later backtest sharing this process-wide singleton)."""
    from datetime import timedelta

    fallback = _fallback_with_history({"1d": ([_snap(_DAY_START, tvl=Decimal("1"), volume=Decimal("2"))], "the_graph")})
    # Trip the breaker (2 strikes) — the ladder pauses.
    for _ in range(2):
        fallback._classify_miss("gateway client not connected: channel is None", chain="base", protocol="aerodrome")
    assert fallback._transport_disabled_until is not None
    assert fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY) is None
    assert fallback._calls == []  # type: ignore[attr-defined]  # paused: no fetch

    # Force the cooldown into the past → the next lookup re-arms and fetches.
    fallback._transport_disabled_until = datetime.now(UTC) - timedelta(seconds=1)
    result = fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert result is not None and result.tvl == Decimal("1")
    assert fallback._transport_disabled_until is None  # cleared
    assert fallback._transport_failure_streak == 0  # fresh streak
    assert fallback._calls == ["1d"]  # type: ignore[attr-defined]  # retried after cooldown


def test_served_upstream_miss_is_not_transport():
    """A served success=False (providers exhausted) must not count toward the streak."""
    fallback = PoolHistoryFallback()
    fallback._classify_miss(
        "the_graph: not found; defillama: not found; geckoterminal: HTTP 503 Service Unavailable",
        chain="base",
        protocol="aerodrome",
    )
    assert fallback._transport_failure_streak == 0
    assert fallback._disabled_reason is None


def test_served_non_transport_response_resets_transport_streak():
    """A SERVED (non-transport) response breaks a transport streak — two
    NON-consecutive transport failures must not trip the 2-strike breaker
    (CodeRabbit #3283)."""
    transport_err = "gateway client not connected: channel is None"
    # Data-level miss between two transport failures.
    fallback = PoolHistoryFallback()
    fallback._classify_miss(transport_err, chain="base", protocol="aerodrome")
    assert fallback._transport_failure_streak == 1
    fallback._classify_miss("the_graph: not found; defillama: not found", chain="base", protocol="aerodrome")
    assert fallback._transport_failure_streak == 0  # served miss reset the streak
    fallback._classify_miss(transport_err, chain="base", protocol="aerodrome")
    assert fallback._transport_failure_streak == 1  # strike 1 again, not 2
    assert fallback._transport_disabled_until is None  # breaker NOT tripped

    # An unsupported-pair response also resets the streak.
    fallback2 = PoolHistoryFallback()
    fallback2._classify_miss(transport_err, chain="base", protocol="aerodrome")
    fallback2._classify_miss(
        "gateway error: unsupported (chain, protocol) pair: ('base', 'aerodrome')",
        chain="base",
        protocol="aerodrome",
    )
    assert fallback2._transport_failure_streak == 0


# =============================================================================
# Retryable partials are never memoized (Codex PR review, #3283)
# =============================================================================


def test_tvl_only_partial_from_failed_hourly_leg_is_retried_not_cached():
    """A TVL-only day whose 1h leg FAILED must not memoize — the next lookup
    retries and can recover measured volume once the failure clears."""
    hourly_ok = [_snap(datetime(2026, 7, 1, h, tzinfo=UTC), volume=Decimal("10")) for h in range(24)]
    state = {"hourly_fails": True}
    fallback = PoolHistoryFallback()
    fallback._calls = []  # type: ignore[attr-defined]

    def fake_get_history(*, resolution: str, **_kwargs):
        fallback._calls.append(resolution)  # type: ignore[attr-defined]
        if resolution == "1d":
            return [_snap(_DAY_START, tvl=Decimal("50"))], "defillama"
        return (None, "") if state["hourly_fails"] else (hourly_ok, "geckoterminal")

    fallback._get_history = fake_get_history  # type: ignore[method-assign]

    first = fallback.daily_history_outcome(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert first.history is not None
    assert first.history.tvl == Decimal("50") and first.history.volume_24h is None
    assert first.cacheable is False

    state["hourly_fails"] = False
    second = fallback.daily_history_outcome(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert second.history is not None and second.history.volume_24h == Decimal("240")  # retried, recovered
    assert second.cacheable is True
    assert fallback._calls == ["1d", "1h", "1d", "1h"]  # type: ignore[attr-defined]


def test_served_below_floor_partial_is_definitive_and_cached():
    """A SERVED hourly response below the coverage floor is a definitive
    no-volume day — cached, no per-tick re-dialing."""
    sparse = [_snap(datetime(2026, 7, 1, h, tzinfo=UTC), volume=Decimal("10")) for h in range(3)]
    fallback = _fallback_with_history(
        {
            "1d": ([_snap(_DAY_START, tvl=Decimal("50"))], "defillama"),
            "1h": (sparse, "geckoterminal"),
        }
    )
    fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert fallback._calls == ["1d", "1h"]  # type: ignore[attr-defined]  # second call = cache hit


def test_current_day_partial_is_served_but_not_cached():
    """The in-progress (current) day is provisional — more volume accrues, so a
    served reading must NOT memoize; a later lookup re-fetches the completed
    day. A fully-elapsed past day (above) caches; today does not (Codex, #3283)."""
    today = datetime.now(UTC).date()
    today_start = datetime.combine(today, datetime.min.time(), tzinfo=UTC)
    fallback = _fallback_with_history(
        {"1d": ([_snap(today_start, tvl=Decimal("50"), volume=Decimal("7"))], "the_graph")}
    )
    first = fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=today)
    # TVL (a point-in-time snapshot) is still served; the incomplete current-day
    # VOLUME is nulled to unmeasured rather than served as a low partial.
    assert first is not None and first.tvl == Decimal("50")
    assert first.volume_24h is None and first.volume_source == ""
    fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=today)
    # NOT cached: both lookups fetched. A fully-elapsed day would show ["1d"] only.
    assert fallback._calls == ["1d", "1d"]  # type: ignore[attr-defined]


def test_current_day_with_only_volume_is_a_full_miss():
    """When the in-progress day has no TVL and only a partial volume, nulling
    the incomplete volume leaves nothing measured — a retryable miss, never a
    cached low reading."""
    today = datetime.now(UTC).date()
    today_start = datetime.combine(today, datetime.min.time(), tzinfo=UTC)
    fallback = _fallback_with_history({"1d": ([_snap(today_start, volume=Decimal("7"))], "the_graph")})
    assert fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=today) is None
    # Retryable: the second lookup fetches again (not memoized as a definitive miss).
    assert fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=today) is None
    assert fallback._calls.count("1d") == 2  # type: ignore[attr-defined]


def test_failed_daily_leg_is_retried_not_cached():
    state = {"daily_fails": True}
    fallback = PoolHistoryFallback()
    fallback._calls = []  # type: ignore[attr-defined]

    def fake_get_history(*, resolution: str, **_kwargs):
        fallback._calls.append(resolution)  # type: ignore[attr-defined]
        if resolution == "1d":
            return (
                (None, "")
                if state["daily_fails"]
                else ([_snap(_DAY_START, tvl=Decimal("7"), volume=Decimal("9"))], "the_graph")
            )
        return [], ""

    fallback._get_history = fake_get_history  # type: ignore[method-assign]
    assert fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY) is None
    state["daily_fails"] = False
    recovered = fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert recovered is not None and recovered.tvl == Decimal("7")


# =============================================================================
# Consumer hooks
# =============================================================================


def _stub_singleton(history: DailyPoolHistory | None, *, cacheable: bool = True):
    stub = PoolHistoryFallback()
    outcome = DailyPoolHistoryOutcome(history=history, cacheable=cacheable)
    stub.daily_history = lambda **_kwargs: history  # type: ignore[method-assign]
    stub.daily_history_outcome = lambda **_kwargs: outcome  # type: ignore[method-assign]

    async def daily_history_async(**_kwargs):
        return history

    async def daily_history_outcome_async(**_kwargs):
        return outcome

    stub.daily_history_async = daily_history_async  # type: ignore[method-assign]
    stub.daily_history_outcome_async = daily_history_outcome_async  # type: ignore[method-assign]
    return patch(
        "almanak.framework.backtesting.pnl.providers.pool_history_fallback.get_pool_history_fallback",
        return_value=stub,
    )


def test_lp_adapter_volume_hook_serves_medium_confidence():
    from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter

    adapter = LPBacktestAdapter()
    history = DailyPoolHistory(tvl=None, tvl_source="", volume_24h=Decimal("605043"), volume_source="geckoterminal")
    with _stub_singleton(history):
        result = adapter._pool_history_ladder_volume(_POOL, "base", "aerodrome", _DAY, (_POOL, _DAY))
    assert result == (Decimal("605043"), DataConfidence.MEDIUM)
    assert adapter._volume_cache[(_POOL, _DAY)] == result

    with _stub_singleton(None):
        assert (
            adapter._pool_history_ladder_volume(_POOL, "base", "aerodrome", date(2026, 7, 2), (_POOL, date(2026, 7, 2)))
            is None
        )
    # Protocol-less lookups can't route through the gateway validator.
    assert adapter._pool_history_ladder_volume(_POOL, "base", None, _DAY, (_POOL, _DAY)) is None


def _make_adapter(rows=None, exc: Exception | None = None):
    """LPBacktestAdapter with a fake primary volume provider installed."""
    from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter

    adapter = LPBacktestAdapter()

    class _FakeVolumeProvider:
        async def get_volume(self, **_kwargs):
            if exc is not None:
                raise exc
            return rows or []

    adapter._volume_provider = _FakeVolumeProvider()
    adapter._volume_provider_initialized = True
    return adapter


def _volume_row(value: str, confidence: DataConfidence, *, day: date = _DAY):
    from almanak.framework.backtesting.pnl.types import DataSourceInfo, VolumeResult

    return VolumeResult(
        value=Decimal(value),
        source_info=DataSourceInfo(
            source="test_subgraph",
            confidence=confidence,
            timestamp=datetime.combine(day, datetime.min.time(), tzinfo=UTC),
        ),
    )


_LADDER_HISTORY = DailyPoolHistory(tvl=None, tvl_source="", volume_24h=Decimal("777"), volume_source="geckoterminal")


def test_fetch_and_cache_volume_routes_empty_low_and_exception_to_ladder():
    """The sync fetch path must consult the ladder on empty results,
    LOW-confidence placeholder rows, and provider exceptions (CodeRabbit
    PR review, #3283)."""
    cases = {
        "empty": _make_adapter(rows=[]),
        "low_row": _make_adapter(rows=[_volume_row("0", DataConfidence.LOW)]),
        "exception": _make_adapter(exc=RuntimeError("lane down")),
    }
    for label, adapter in cases.items():
        cache_key = (_POOL, _DAY)
        with _stub_singleton(_LADDER_HISTORY):
            result = adapter._fetch_and_cache_volume(
                adapter._volume_provider, _POOL, _DAY_START, _DAY, "base", "aerodrome", cache_key
            )
        assert result == (Decimal("777"), DataConfidence.MEDIUM), label
        assert adapter._volume_cache[cache_key] == result, label


def test_fetch_and_cache_volume_high_row_bypasses_ladder_and_low_row_survives_ladder_miss():
    high = _make_adapter(rows=[_volume_row("1234", DataConfidence.HIGH)])
    with _stub_singleton(_LADDER_HISTORY):
        result = high._fetch_and_cache_volume(
            high._volume_provider, _POOL, _DAY_START, _DAY, "base", "aerodrome", (_POOL, _DAY)
        )
    assert result == (Decimal("1234"), DataConfidence.HIGH)  # primary wins; ladder not consulted

    low = _make_adapter(rows=[_volume_row("0", DataConfidence.LOW)])
    with _stub_singleton(None):
        result = low._fetch_and_cache_volume(
            low._volume_provider, _POOL, _DAY_START, _DAY, "base", "aerodrome", (_POOL, _DAY)
        )
    # Ladder missed too — the pre-ladder placeholder behaviour is preserved.
    assert result == (Decimal("0"), DataConfidence.LOW)


def test_adapter_does_not_cache_retryable_ladder_miss_and_recovers():
    """A retryable ladder miss must survive the adapter boundary uncached."""
    adapter = _make_adapter(rows=[])
    cache_key = (_POOL, _DAY)

    with _stub_singleton(None, cacheable=False):
        first = adapter._fetch_and_cache_volume(
            adapter._volume_provider, _POOL, _DAY_START, _DAY, "base", "aerodrome", cache_key
        )
    assert first == (None, DataConfidence.LOW)
    assert cache_key not in adapter._volume_cache

    with _stub_singleton(_LADDER_HISTORY):
        recovered = adapter._fetch_and_cache_volume(
            adapter._volume_provider, _POOL, _DAY_START, _DAY, "base", "aerodrome", cache_key
        )
    assert recovered == (Decimal("777"), DataConfidence.MEDIUM)
    assert adapter._volume_cache[cache_key] == recovered


def test_low_primary_row_waits_for_definitive_ladder_miss_before_caching():
    adapter = _make_adapter(rows=[_volume_row("0", DataConfidence.LOW)])
    cache_key = (_POOL, _DAY)
    with _stub_singleton(None, cacheable=False):
        result = adapter._fetch_and_cache_volume(
            adapter._volume_provider, _POOL, _DAY_START, _DAY, "base", "aerodrome", cache_key
        )
    assert result == (None, DataConfidence.LOW)
    assert cache_key not in adapter._volume_cache


def test_prewarm_batches_bounded_contiguous_ranges_and_populates_each_day():
    from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter

    days = [_DAY + timedelta(days=offset) for offset in range(32)]
    calls: list[tuple[date, date]] = []

    class _RangeProvider:
        async def get_volume(self, *, start_date, end_date, **_kwargs):
            calls.append((start_date, end_date))
            count = (end_date - start_date).days + 1
            return [
                _volume_row(str(offset + 1), DataConfidence.HIGH, day=start_date + timedelta(days=offset))
                for offset in range(count)
            ]

    adapter = LPBacktestAdapter()
    adapter._volume_provider = _RangeProvider()
    adapter._volume_provider_initialized = True

    with _stub_singleton(None):
        asyncio.run(adapter._prewarm_volume_lane(_POOL, "aerodrome", "base", days))

    assert calls == [(_DAY, days[29]), (days[30], days[31])]
    assert all(adapter._volume_cache[(_POOL, day)][1] == DataConfidence.HIGH for day in days)


def test_prewarm_routes_misses_through_ladder_and_reports_true_gaps():
    days = [_DAY, date(2026, 7, 2)]

    # Primary lane raises -> both days served by the ladder, no unwarmed days.
    adapter = _make_adapter(exc=RuntimeError("subgraph dead"))
    with _stub_singleton(_LADDER_HISTORY):
        asyncio.run(adapter._prewarm_volume_lane(_POOL, "aerodrome", "base", days))
    for day in days:
        assert adapter._volume_cache[(_POOL, day)] == (Decimal("777"), DataConfidence.MEDIUM)

    # LOW placeholder rows -> ladder first; ladder miss keeps the placeholder.
    adapter = _make_adapter(rows=[_volume_row("0", DataConfidence.LOW)])
    with _stub_singleton(None):
        asyncio.run(adapter._prewarm_volume_lane(_POOL, "aerodrome", "base", [_DAY]))
    cached_value, cached_confidence = adapter._volume_cache[(_POOL, _DAY)]
    assert (cached_value, cached_confidence) == (Decimal("0"), DataConfidence.LOW)

    # Both lanes miss entirely -> day stays unwarmed (accrual gap is honest).
    adapter = _make_adapter(rows=[])
    with _stub_singleton(None):
        asyncio.run(adapter._prewarm_volume_lane(_POOL, "aerodrome", "base", [_DAY]))
    assert (_POOL, _DAY) not in adapter._volume_cache


def test_ladder_volume_provenance_reaches_fee_resolution():
    """Durable fee provenance must name the ladder provider, not multi_dex
    (CodeRabbit PR review, #3283)."""
    adapter = _make_adapter(rows=[])
    with _stub_singleton(_LADDER_HISTORY):
        asyncio.run(adapter._prewarm_volume_lane(_POOL, "aerodrome", "base", [_DAY]))
    assert adapter._volume_source_labels[(_POOL, _DAY)] == "gateway_pool_history:geckoterminal"

    resolution = adapter._historical_pool_volume_resolution(_DAY_START, _POOL, "aerodrome")
    assert resolution is not None
    assert resolution.volume_usd == Decimal("777")
    assert resolution.data_source_label == "gateway_pool_history:geckoterminal"

    # A primary-lane day carries no override -> legacy multi_dex label path.
    high = _make_adapter(rows=[_volume_row("1234", DataConfidence.HIGH)])
    with _stub_singleton(None):
        asyncio.run(high._prewarm_volume_lane(_POOL, "aerodrome", "base", [_DAY]))
    resolution = high._historical_pool_volume_resolution(_DAY_START, _POOL, "aerodrome")
    assert resolution is not None and resolution.data_source_label is None


def test_prewarm_volume_lane_retries_primary_after_a_single_error():
    """ALM-2953: a single transient primary-lane error must NOT disable the
    primary for the rest of the window — that takes TWO consecutive errors. Day
    1 errors (the ladder covers it, MEDIUM); day 2 must still dial the primary
    and succeed (HIGH) rather than being stuck on the ladder."""
    from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter

    day2 = date(2026, 7, 2)
    days = [_DAY, day2]
    calls: list[tuple[date, date]] = []

    class _FlakyProvider:
        async def get_volume(self, *, start_date, end_date, **_kwargs):
            calls.append((start_date, end_date))
            if start_date != end_date:
                raise RuntimeError("range request transient")
            if start_date == _DAY:
                raise RuntimeError("transient")
            return [_volume_row("500", DataConfidence.HIGH, day=start_date)]

    adapter = LPBacktestAdapter()
    adapter._volume_provider = _FlakyProvider()
    adapter._volume_provider_initialized = True

    with _stub_singleton(_LADDER_HISTORY):  # ladder covers the day-1 miss
        asyncio.run(adapter._prewarm_volume_lane(_POOL, "aerodrome", "base", days))

    assert calls == [(_DAY, day2), (_DAY, _DAY), (day2, day2)]
    assert adapter._volume_cache[(_POOL, _DAY)] == (Decimal("777"), DataConfidence.MEDIUM)  # day1 -> ladder
    assert adapter._volume_cache[(_POOL, day2)][1] == DataConfidence.HIGH  # day2 -> primary recovered


def test_prewarm_volume_lane_uses_ladder_when_primary_provider_absent():
    """CodeRabbit #3283: the pool-history ladder is INDEPENDENT of the primary
    DEX-volume provider — when the provider can't be built, the prewarm must
    still iterate so the ladder warms the window (not return empty)."""
    from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter

    adapter = LPBacktestAdapter()  # use_historical_volume defaults True
    adapter._volume_provider = None  # primary provider unavailable
    adapter._volume_provider_initialized = True

    with _stub_singleton(_LADDER_HISTORY):
        asyncio.run(adapter._prewarm_volume_lane(_POOL, "aerodrome", "base", [_DAY]))

    assert adapter._volume_cache[(_POOL, _DAY)] == (Decimal("777"), DataConfidence.MEDIUM)

    # The warmed entry must be CONSUMED by the accrual read path even though the
    # primary provider is still None (cache check precedes the provider check —
    # CodeRabbit #3283); otherwise the fix is inert.
    resolution = adapter._historical_pool_volume_resolution(_DAY_START, _POOL, "aerodrome")
    assert resolution is not None
    assert resolution.volume_usd == Decimal("777")
    assert resolution.data_source_label == "gateway_pool_history:geckoterminal"


def test_failed_daily_leg_does_not_suppress_hourly_volume():
    """CodeRabbit #3283: a failed 1d fetch must not block the independent 1h
    lane. Volume is recovered from the hourly sum, but the day stays
    non-cacheable because its TVL leg is still retryable."""
    hourly = [_snap(datetime(2026, 7, 1, h, tzinfo=UTC), volume=Decimal("10")) for h in range(24)]
    fallback = _fallback_with_history({"1h": (hourly, "geckoterminal")})  # no "1d" -> 1d fetch returns None

    history = fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert history is not None
    assert history.volume_24h == Decimal("240")  # recovered from the 1h lane despite the 1d failure
    assert history.tvl is None
    # Non-cacheable (1d retryable): a second lookup re-fetches.
    fallback.daily_history(pool_address=_POOL, chain="base", protocol="aerodrome", day=_DAY)
    assert fallback._calls.count("1d") == 2  # type: ignore[attr-defined]


def test_liquidity_depth_hook_serves_medium_confidence_and_falls_back_on_miss():
    from almanak.framework.backtesting.pnl.providers.liquidity_depth import (
        DATA_SOURCE_FALLBACK,
        LiquidityDepthProvider,
    )
    from almanak.framework.backtesting.pnl.providers.subgraph_client import (
        SubgraphClient,
        SubgraphClientConfig,
    )

    client = SubgraphClient(config=SubgraphClientConfig(api_key="test-key"))
    client.query = AsyncMock(return_value={})  # family query parses to no data -> miss
    provider = LiquidityDepthProvider(client=client, fallback_depth=Decimal("0"))

    history = DailyPoolHistory(tvl=Decimal("7583793"), tvl_source="defillama", volume_24h=None, volume_source="")
    with _stub_singleton(history):
        result = asyncio.run(
            provider.get_liquidity_depth(pool_address=_POOL, chain="base", timestamp=_DAY_START, protocol="aerodrome")
        )
    assert result.depth == Decimal("7583793")
    assert result.source_info.confidence == DataConfidence.MEDIUM
    assert result.source_info.source == "gateway_pool_history:defillama"

    with _stub_singleton(None):
        result = asyncio.run(
            provider.get_liquidity_depth(pool_address=_POOL, chain="base", timestamp=_DAY_START, protocol="aerodrome")
        )
    assert result.source_info.source == DATA_SOURCE_FALLBACK
    assert result.source_info.confidence == DataConfidence.LOW
