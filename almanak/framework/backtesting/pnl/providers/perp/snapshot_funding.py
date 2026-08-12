"""Strategy-facing funding-rate lane for PnL-backtest MarketSnapshots.

``MarketSnapshot.funding_rate(...)`` / ``funding_rate_spread(...)`` delegate to
an async provider object; the live runner wires ``GatewayFundingRateProvider``.
The PnL engine used to wire nothing, so every strategy-facing funding read in
``decide()`` raised ``ValueError("No funding rate provider configured for
MarketSnapshot")`` and funding-gated perp strategies could never enter — every
backtest ended with 0 trades regardless of window.

This module is the backtest counterpart of the live provider:
:class:`SnapshotFundingRateSource` is the per-run funding-history plane and
:class:`SnapshotFundingRateView` is the per-tick, timestamp-bound provider the
engine binds into each snapshot (``create_market_snapshot_from_state``). The
engine also binds the same source into the perp adapter, so strategy decisions
and position accrual resolve from one immutable measured series. Declared
``(venue, market, market_address)`` targets are prewarmed before data iteration;
dynamically
selected targets materialize on first read. Each load covers
``[run_start - 24h, run_end]`` through the gateway in bounded chunks. Every
later read is in-memory and resolves with no look-ahead: the rate at tick ``T``
is the latest measured point at or before ``T``, provided it is no more than
24 hours old.

``BacktestDataConfig`` gates the lane exactly like the adapter lane:

- ``use_historical_funding=False`` (or no ``data_config``): serve
  ``funding_fallback_rate`` as a fixed rate — zero network. Note the perp
  adapter's own fixed lane charges ``PerpBacktestConfig.default_funding_rate``;
  both knobs default to the same 0.00001/h scalar, so the default fixed run is
  coherent end to end.
- ``use_historical_funding=True``: gateway-backed history; hours without a
  measured point fall back to ``funding_fallback_rate`` (the adapter's
  historical-fallback knob) unless ``strict_historical_mode`` is set, in which
  case the read raises ``FundingRateUnavailableError`` — strategies treat that
  as "funding unavailable, hold" rather than gating on a fabricated number.

GMX observations additionally carry signed long/short payment rates (positive
receives, negative pays); the adapter accrues the position's actual side while
the public scalar retains its legacy positive-longs-pay convention. Every
served :class:`FundingRate` carries ``is_live_data=False`` and an hour-normalized
timestamp (funding cadence is hourly on every supported venue).
Expected data-source failures and unavailable full-series data are sticky for
the run: strict mode refuses every affected read, while non-strict mode serves
the configured fallback deterministically instead of mixing measured and
fabricated rates after transient per-tick failures. Unexpected exceptions
propagate and remain uncached so programming defects fail loudly.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from bisect import bisect_right
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
from almanak.core.perp_markets import perp_market_funding_key
from almanak.framework.backtesting.config import DEFAULT_FUNDING_FALLBACK_RATE
from almanak.framework.backtesting.pnl.data_manifest import (
    CONSUMER_STRATEGY_DECISION,
    LANE_FUNDING,
    OUTCOME_DEGRADED,
    OUTCOME_REFUSED,
    OUTCOME_SERVED,
)
from almanak.framework.backtesting.pnl.data_provider import is_address_like
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
    FundingHistoryPoint,
    fetch_funding_points,
    run_sync_gateway_call,
)
from almanak.framework.data.funding import (
    HOURS_PER_YEAR,
    FundingRate,
    FundingRateSpread,
    FundingRateUnavailableError,
    Venue,
)
from almanak.framework.data.interfaces import DataSourceError

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig
    from almanak.framework.backtesting.pnl.data_manifest import RunDataManifest

logger = logging.getLogger(__name__)

#: Fixed-lane rate when no ``BacktestDataConfig`` is provided. The same scalar
#: as ``BacktestDataConfig.funding_fallback_rate``,
#: ``providers.funding_rates.DEFAULT_FUNDING_RATE``, and the perp adapter's
#: ``PerpBacktestConfig.default_funding_rate`` defaults, so what ``decide()``
#: gates on matches what the position pays on a default run.
DEFAULT_FALLBACK_RATE = DEFAULT_FUNDING_FALLBACK_RATE

_HOURS_PER_8H = Decimal("8")
_POINT_LOOKBACK = timedelta(hours=24)
_POINT_LOOKBACK_SECONDS = int(_POINT_LOOKBACK.total_seconds())


def _hour_utc(timestamp: datetime) -> datetime:
    """Hour-floored UTC timestamp (naive input is UTC by provider contract).

    Aware values are converted with ``astimezone(UTC)`` BEFORE flooring:
    flooring in the value's own offset would shift the query boundary for
    odd-offset zones (a +05:30 tick at 07:00 UTC would query through 06:30
    UTC) and could miss the latest measured point.
    """
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def _unix_seconds(timestamp: datetime) -> int:
    """UTC unix seconds for an aware or naive-UTC timestamp."""
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return int(timestamp.astimezone(UTC).timestamp())


@dataclass(frozen=True, slots=True)
class BacktestFundingObservation:
    """One rate served by the shared funding plane."""

    rate: Decimal
    confidence: str
    source: str
    degraded: bool
    reason: str = ""
    long_rate_hourly: Decimal | None = None
    short_rate_hourly: Decimal | None = None

    def payment_rate(self, *, is_long: bool) -> Decimal:
        """Signed payment rate for one position side (positive receives)."""
        side_rate = self.long_rate_hourly if is_long else self.short_rate_hourly
        if side_rate is not None:
            return side_rate
        return -self.rate if is_long else self.rate


@dataclass(frozen=True, slots=True)
class _FundingSeries:
    """Immutable materialized history for one canonical venue and market."""

    points: tuple[FundingHistoryPoint, ...]
    timestamps: tuple[int, ...]
    failure_reason: str | None = None
    failure_source: str | None = None


@dataclass(frozen=True, slots=True)
class _FundingResolution:
    """Cached value plus the provenance emitted for every consumer read."""

    rate: Decimal | None
    source: str
    outcome: str
    detail: str = ""
    error: FundingRateUnavailableError | None = None
    long_rate_hourly: Decimal | None = None
    short_rate_hourly: Decimal | None = None


class SnapshotFundingRateSource:
    """Per-backtest-run funding plane shared by decisions and accrual.

    One instance per engine run (built in ``execute_iteration_loop``); each
    tick binds it to the tick's simulated timestamp via :meth:`view_at`. The
    instance owns the materialized series and per-hour resolution cache, so
    parallel sweeps never share mutable state or an event-loop-bound provider.
    """

    def __init__(
        self,
        *,
        chain: str,
        start_time: datetime,
        end_time: datetime,
        data_config: BacktestDataConfig | None = None,
        manifest: RunDataManifest | None = None,
    ) -> None:
        run_start = _hour_utc(start_time)
        run_end = _hour_utc(end_time)
        if run_end < run_start:
            raise ValueError("end_time must be greater than or equal to start_time")
        self._chain = chain.strip().lower()
        self._run_start = run_start
        self._run_end = run_end
        self._history_start = run_start - _POINT_LOOKBACK
        self._history_end = run_end
        self._use_historical = bool(data_config is not None and data_config.use_historical_funding)
        self._strict = bool(data_config is not None and data_config.strict_historical_mode)
        self._fallback_rate = data_config.funding_fallback_rate if data_config is not None else DEFAULT_FALLBACK_RATE
        self._manifest = manifest
        self._state_lock = threading.Lock()
        self._series: dict[tuple[str, str, str], _FundingSeries] = {}
        self._inflight: dict[tuple[str, str, str], Future[_FundingSeries]] = {}
        self._market_addresses: dict[tuple[str, str], set[str]] = {}
        self._markets_by_address: dict[tuple[str, str], set[str]] = {}
        # Cached resolution per (venue, market, address, hour), including provenance and
        # strict-mode unavailability. Every consumer read is recorded even
        # when the resolution itself comes from this cache.
        self._cache: dict[tuple[str, str, str, datetime], _FundingResolution] = {}
        self._degraded_points: set[tuple[str, str, str, datetime]] = set()
        self._degraded_reasons: dict[tuple[str, str, str, datetime], str] = {}
        self._warned_failures: set[tuple[str, str, str, str]] = set()

    def view_at(self, timestamp: datetime) -> SnapshotFundingRateView:
        """Provider view bound to one tick's simulated timestamp."""
        return SnapshotFundingRateView(self, timestamp)

    def point_was_degraded(
        self,
        venue: Venue | str,
        market: str,
        timestamp: datetime,
        market_address: str = "",
    ) -> bool:
        """True when the hour covering ``timestamp`` resolved from the fallback."""
        venue_value = str(venue).lower()
        try:
            market_upper, resolved_address = self._resolve_market_identity(venue_value, market, market_address)
        except FundingRateUnavailableError:
            # An address-less ambiguous label cannot prove that any exact
            # series was measured. Treat it as degraded so history callers
            # fail closed while preserving this predicate's boolean contract.
            return True
        with self._state_lock:
            return (venue_value, market_upper, resolved_address, _hour_utc(timestamp)) in self._degraded_points

    @property
    def history_capable(self) -> bool:
        """True when the run resolves measured historical rates.

        A fallback-mode run answers every hour with one configured constant —
        a constant series labeled "history" would be fabrication, so the
        history accessor refuses unless this is True.
        """
        return self._use_historical

    async def funding_rate_at(
        self,
        venue: Venue | str,
        market: str,
        timestamp: datetime,
        market_address: str = "",
    ) -> FundingRate:
        """The funding rate in effect at simulated time ``timestamp``.

        Raises:
            FundingRateUnavailableError: In ``strict_historical_mode`` when no
                measured point covers the hour (never fabricates under strict).
        """
        venue_value = str(venue).lower()
        # Canonicalize the market spelling ("ETH/USD" == "ETH-USD" == the
        # venue-form "ETH-USD") so slash-form strategy reads hit the same
        # cache entries and gateway funding tables as dash-form ones
        # (campaign-50 s38). Unparseable identifiers pass through upper-cased
        # and fail downstream with the venue's own unsupported-market error.
        market_upper, resolved_address = self._resolve_market_identity(venue_value, market, market_address)
        hour = _hour_utc(timestamp)
        key = (venue_value, market_upper, resolved_address, hour)
        with self._state_lock:
            resolution = self._cache.get(key)
        if resolution is None:
            if self._use_historical:
                resolution = await self._historical_rate(venue_value, market_upper, resolved_address, hour)
            else:
                resolution = _FundingResolution(
                    rate=self._fallback_rate,
                    source="fixed:configured",
                    outcome=OUTCOME_DEGRADED,
                    detail=f"historical funding disabled; funding_rate_hourly={self._fallback_rate}",
                )
            with self._state_lock:
                resolution = self._cache.setdefault(key, resolution)

        self._record_resolution(market_upper, hour, resolution)
        if resolution.error is not None:
            raise resolution.error
        assert resolution.rate is not None

        return FundingRate(
            venue=venue_value,
            market=market_upper,
            rate_hourly=resolution.rate,
            rate_8h=resolution.rate * _HOURS_PER_8H,
            rate_annualized=resolution.rate * HOURS_PER_YEAR,
            long_rate_hourly=resolution.long_rate_hourly,
            short_rate_hourly=resolution.short_rate_hourly,
            market_address=resolved_address or None,
            timestamp=hour,
            is_live_data=False,
        )

    async def _historical_rate(
        self,
        venue: str,
        market: str,
        market_address: str,
        hour: datetime,
    ) -> _FundingResolution:
        """Latest measured hourly rate at or before ``hour`` (no look-ahead)."""
        series = await self._ensure_series(venue, market, market_address)
        return self._resolution_from_series(venue, market, market_address, hour, series)

    async def materialize_history(self, venue: Venue | str, market: str, market_address: str = "") -> int:
        """Load one run-wide measured series, returning its point count."""
        if not self._use_historical:
            return 0
        venue_value = str(venue).lower()
        market_upper = perp_market_funding_key(market) or market.upper()
        resolved_address = self._register_market_address(venue_value, market_upper, market_address)
        series = await self._ensure_series(venue_value, market_upper, resolved_address)
        return len(series.points)

    async def require_complete_history(self, venue: Venue | str, market: str, market_address: str = "") -> int:
        """Fail unless measured history can serve every hour in the run window."""
        if not self._use_historical:
            return 0
        venue_value = str(venue).lower()
        market_upper, resolved_address = self._resolve_market_identity(venue_value, market, market_address)
        series = await self._ensure_series(venue_value, market_upper, resolved_address)
        checked_hours = 0
        hour = self._run_start
        while hour <= self._run_end:
            reason = self._series_unavailability_reason(series, hour)
            if reason is not None:
                raise FundingRateUnavailableError(venue=venue_value, market=market_upper, reason=reason)
            checked_hours += 1
            hour += timedelta(hours=1)
        return checked_hours

    def observation_at(
        self,
        venue: Venue | str,
        market: str,
        timestamp: datetime,
        market_address: str = "",
    ) -> BacktestFundingObservation:
        """Resolve an already-materialized series for synchronous accrual."""
        venue_value = str(venue).lower()
        market_upper, resolved_address = self._resolve_market_identity(venue_value, market, market_address)
        hour = _hour_utc(timestamp)
        if not self._use_historical:
            return BacktestFundingObservation(
                rate=self._fallback_rate,
                confidence="low",
                source="fixed",
                degraded=False,
            )
        series_key = self._series_key(venue_value, market_upper, resolved_address)
        with self._state_lock:
            series = self._series.get(series_key)
        if series is None:
            resolution = self._degraded(
                venue_value,
                market_upper,
                "run-wide funding history was not materialized before accrual",
                hour=hour,
                source="fallback:provider_unavailable",
                market_address=resolved_address,
            )
        else:
            resolution = self._resolution_from_series(venue_value, market_upper, resolved_address, hour, series)
        if resolution.error is not None:
            raise resolution.error
        assert resolution.rate is not None
        degraded = resolution.outcome == OUTCOME_DEGRADED
        reason = ""
        if degraded:
            with self._state_lock:
                reason = self._degraded_reasons.get((venue_value, market_upper, resolved_address, hour), "")
        return BacktestFundingObservation(
            rate=resolution.rate,
            confidence="low" if degraded else "high",
            source="fallback:funding_history" if degraded else resolution.source,
            degraded=degraded,
            reason=reason,
            long_rate_hourly=resolution.long_rate_hourly,
            short_rate_hourly=resolution.short_rate_hourly,
        )

    async def _ensure_series(self, venue: str, market: str, market_address: str) -> _FundingSeries:
        """Return one shared series, caching only completed or expected-failure results.

        ``_fetch_series`` converts typed data-source failures into immutable
        failed series. Unexpected exceptions propagate without populating the
        cache so programming defects cannot silently become fallback data.
        """
        series_key = self._series_key(venue, market, market_address)
        with self._state_lock:
            cached = self._series.get(series_key)
            if cached is not None:
                return cached
            future = self._inflight.get(series_key)
            if future is None:
                future = Future()
                self._inflight[series_key] = future
                lead = True
            else:
                lead = False

        if not lead:
            return await asyncio.wrap_future(future)

        try:
            series = await self._fetch_series(venue, market, market_address)
            with self._state_lock:
                self._series[series_key] = series
            future.set_result(series)
            return series
        except asyncio.CancelledError:
            future.cancel()
            raise
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            with self._state_lock:
                self._inflight.pop(series_key, None)

    async def _fetch_series(self, venue: str, market: str, market_address: str) -> _FundingSeries:
        canonical = FundingHistoryRegistry.canonical(venue)
        if canonical is None:
            return self._failed_series(
                f"no funding-history connector declares venue '{venue}'",
                source="fallback:provider_unavailable",
            )

        declared = FundingHistoryRegistry.declared_chains(canonical)
        if declared and self._chain not in declared:
            return self._failed_series(
                f"venue declares no funding data for chain '{self._chain}'",
                source="fallback:unsupported_chain",
            )

        gateway_venue = FundingHistoryRegistry.venue_for(canonical)
        if gateway_venue is None:
            return self._failed_series(
                f"venue '{venue}' has no gateway funding-history dispatch key",
                source="fallback:provider_unavailable",
            )

        chain = self._chain if declared else ""
        try:
            points = await run_sync_gateway_call(
                fetch_funding_points,
                venue=gateway_venue,
                market=market,
                market_address=market_address,
                chain=chain,
                start_ts=_unix_seconds(self._history_start),
                end_ts=_unix_seconds(self._history_end),
            )
        except DataSourceError as exc:
            return self._failed_series(str(exc), source="fallback:error")

        if not points:
            return self._failed_series(
                "gateway returned no measured funding points for the run window",
                source="fallback:no_data",
            )

        ordered = tuple(sorted(points, key=lambda point: point.timestamp))
        return _FundingSeries(
            points=ordered,
            timestamps=tuple(point.timestamp for point in ordered),
        )

    @staticmethod
    def _failed_series(reason: str, *, source: str) -> _FundingSeries:
        return _FundingSeries(points=(), timestamps=(), failure_reason=reason, failure_source=source)

    @staticmethod
    def _series_key(venue: str, market: str, market_address: str = "") -> tuple[str, str, str]:
        canonical = FundingHistoryRegistry.canonical(venue)
        return (canonical or venue.lower(), market.upper(), market_address.strip().lower())

    def _register_market_address(self, venue: str, market: str, market_address: str) -> str:
        address = market_address.strip().lower()
        if not address:
            return self._resolve_market_address(venue, market, "")
        canonical_venue = FundingHistoryRegistry.canonical(venue) or venue.lower()
        market_upper = market.upper()
        key = (canonical_venue, market_upper)
        with self._state_lock:
            self._market_addresses.setdefault(key, set()).add(address)
            self._markets_by_address.setdefault((canonical_venue, address), set()).add(market_upper)
        return address

    def _resolve_market_identity(self, venue: str, market: str, market_address: str) -> tuple[str, str]:
        """Resolve pair/address and legacy address-as-market spellings to one series key."""
        raw_market = market.strip()
        explicit_address = market_address.strip().lower()
        if is_address_like(raw_market):
            address = raw_market.lower()
            if explicit_address and explicit_address != address:
                raise FundingRateUnavailableError(
                    venue=venue,
                    market=market,
                    reason="market and market_address identify different GMX market contracts",
                )
            canonical_venue = FundingHistoryRegistry.canonical(venue) or venue.lower()
            with self._state_lock:
                labels = tuple(sorted(self._markets_by_address.get((canonical_venue, address), ())))
            if len(labels) > 1:
                raise FundingRateUnavailableError(
                    venue=venue,
                    market=market,
                    reason="the exact GMX market address was registered under multiple pair labels",
                )
            # Declared strategies prewarm pair + address before tick 1. Reuse
            # that canonical series for the established address-as-market API;
            # an undeclared dynamic read remains address-first end to end.
            return (labels[0] if labels else raw_market.upper(), address)

        market_upper = perp_market_funding_key(raw_market) or raw_market.upper()
        return market_upper, self._resolve_market_address(venue, market_upper, explicit_address)

    def _resolve_market_address(self, venue: str, market: str, market_address: str) -> str:
        address = market_address.strip().lower()
        if address:
            return address
        key = (FundingHistoryRegistry.canonical(venue) or venue.lower(), market.upper())
        with self._state_lock:
            addresses = tuple(sorted(self._market_addresses.get(key, ())))
        if len(addresses) == 1:
            return addresses[0]
        if len(addresses) > 1:
            raise FundingRateUnavailableError(
                venue=venue,
                market=market,
                reason="multiple exact GMX market addresses share this label; supply market_address",
            )
        return ""

    def _resolution_from_series(
        self,
        venue: str,
        market: str,
        market_address: str,
        hour: datetime,
        series: _FundingSeries,
    ) -> _FundingResolution:
        reason = self._series_unavailability_reason(series, hour)
        if reason is not None:
            return self._degraded(
                venue,
                market,
                reason,
                hour=hour,
                source=series.failure_source or "fallback:no_data",
                market_address=market_address,
            )

        hour_ts = _unix_seconds(hour)
        point_index = bisect_right(series.timestamps, hour_ts) - 1
        point = series.points[point_index]
        return _FundingResolution(
            rate=point.rate_hourly,
            source=f"historical:{point.source}",
            outcome=OUTCOME_SERVED,
            long_rate_hourly=point.long_rate_hourly,
            short_rate_hourly=point.short_rate_hourly,
        )

    @staticmethod
    def _series_unavailability_reason(series: _FundingSeries, hour: datetime) -> str | None:
        if series.failure_reason is not None:
            return series.failure_reason
        hour_ts = _unix_seconds(hour)
        point_index = bisect_right(series.timestamps, hour_ts) - 1
        if point_index < 0:
            return "no measured funding point at or before the tick"
        if hour_ts - series.points[point_index].timestamp > _POINT_LOOKBACK_SECONDS:
            return "latest measured funding point is more than 24 hours old"
        return None

    def _degraded(
        self,
        venue: str,
        market: str,
        reason: str,
        *,
        hour: datetime,
        source: str,
        market_address: str = "",
    ) -> _FundingResolution:
        if self._strict:
            return _FundingResolution(
                rate=None,
                source="",
                outcome=OUTCOME_REFUSED,
                detail=reason,
                error=FundingRateUnavailableError(venue=venue, market=market, reason=reason),
            )
        should_log = False
        with self._state_lock:
            self._degraded_points.add((venue, market, market_address, hour))
            self._degraded_reasons[(venue, market, market_address, hour)] = reason
            warning_key = (venue, market, market_address, reason)
            if warning_key not in self._warned_failures:
                self._warned_failures.add(warning_key)
                should_log = True
        if should_log:
            logger.warning(
                "Historical funding unavailable for %s/%s (%s); using fallback rate %s/h",
                venue,
                market,
                reason,
                self._fallback_rate,
            )
        return _FundingResolution(
            rate=self._fallback_rate,
            source=source,
            outcome=OUTCOME_DEGRADED,
            detail=f"{reason}; funding_rate_hourly={self._fallback_rate}",
        )

    def _record_resolution(self, market: str, hour: datetime, resolution: _FundingResolution) -> None:
        """Record every strategy-decision funding read, including cache hits."""
        if self._manifest is None:
            return
        self._manifest.record(
            lane=LANE_FUNDING,
            key=market,
            consumer=CONSUMER_STRATEGY_DECISION,
            source=resolution.source,
            outcome=resolution.outcome,
            at=hour,
            detail=resolution.detail,
        )


class SnapshotFundingRateView:
    """Timestamp-bound async provider bound into one tick's MarketSnapshot.

    Implements the provider protocol ``MarketSnapshot.funding_rate`` /
    ``funding_rate_spread`` expect (``get_funding_rate`` /
    ``get_funding_rate_spread`` coroutines). The bound timestamp is the
    snapshot's simulated tick time, so a strategy reading funding "now" reads
    the rate in effect at that simulated instant, never a later one.
    """

    def __init__(self, source: SnapshotFundingRateSource, timestamp: datetime) -> None:
        self._source = source
        self._timestamp = timestamp

    async def get_funding_rate(self, venue: Venue | str, market: str, market_address: str = "") -> FundingRate:
        return await self._source.funding_rate_at(venue, market, self._timestamp, market_address)

    async def get_funding_rate_spread(
        self,
        market: str,
        venue_a: Venue | str,
        venue_b: Venue | str,
        market_address: str = "",
    ) -> FundingRateSpread:
        address_a = market_address if FundingHistoryRegistry.discovers_markets(str(venue_a)) else ""
        address_b = market_address if FundingHistoryRegistry.discovers_markets(str(venue_b)) else ""
        rate_a = await self.get_funding_rate(venue_a, market, address_a)
        rate_b = await self.get_funding_rate(venue_b, market, address_b)
        return FundingRateSpread(
            market=perp_market_funding_key(market) or market.upper(),
            venue_a=rate_a.venue,
            venue_b=rate_b.venue,
            rate_a=rate_a,
            rate_b=rate_b,
            spread_8h=rate_a.rate_8h - rate_b.rate_8h,
            spread_annualized=rate_a.rate_annualized - rate_b.rate_annualized,
            timestamp=_hour_utc(self._timestamp),
        )


__all__ = [
    "BacktestFundingObservation",
    "DEFAULT_FALLBACK_RATE",
    "SnapshotFundingRateSource",
    "SnapshotFundingRateView",
]
