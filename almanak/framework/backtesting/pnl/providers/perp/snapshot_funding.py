"""Strategy-facing funding-rate lane for PnL-backtest MarketSnapshots.

``MarketSnapshot.funding_rate(...)`` / ``funding_rate_spread(...)`` delegate to
an async provider object; the live runner wires ``GatewayFundingRateProvider``.
The PnL engine used to wire nothing, so every strategy-facing funding read in
``decide()`` raised ``ValueError("No funding rate provider configured for
MarketSnapshot")`` and funding-gated perp strategies could never enter — every
backtest ended with 0 trades regardless of window.

This module is the backtest counterpart of the live provider:
:class:`SnapshotFundingRateSource` is the per-run source and
:class:`SnapshotFundingRateView` is the per-tick, timestamp-bound provider the
engine binds into each snapshot (``create_market_snapshot_from_state``). Rates
come from the SAME connector-owned historical lane the perp adapter uses for
position evolution (``FundingRateProvider`` -> ``FundingHistoryRegistry`` ->
gateway ``GetFundingRateHistory``), resolved with no look-ahead: the rate at
tick ``T`` is the latest measured point at or before ``T``.

``BacktestDataConfig`` gates the lane exactly like the adapter lane:

- ``use_historical_funding=False`` (or no ``data_config``): serve
  ``funding_fallback_rate`` as a fixed rate — zero network. Note the perp
  adapter's own fixed lane charges ``PerpBacktestConfig.default_funding_rate``;
  both knobs default to the same 0.0001/h scalar, so the default fixed run is
  coherent end to end.
- ``use_historical_funding=True``: gateway-backed history; hours without a
  measured point fall back to ``funding_fallback_rate`` (the adapter's
  historical-fallback knob) unless ``strict_historical_mode`` is set, in which
  case the read raises ``FundingRateUnavailableError`` — strategies treat that
  as "funding unavailable, hold" rather than gating on a fabricated number.

Every served :class:`FundingRate` carries ``is_live_data=False`` and an
hour-normalized timestamp (funding cadence is hourly on every supported venue).
Resolutions are cached per ``(venue, market, hour)`` — including strict-mode
unavailability, which re-raises from cache — so a fetch failure is retried at
most once per simulated hour, not once per tick.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
from almanak.core.perp_markets import perp_market_funding_key
from almanak.framework.data.funding import (
    HOURS_PER_YEAR,
    FundingRate,
    FundingRateSpread,
    FundingRateUnavailableError,
    Venue,
)

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig
    from almanak.framework.backtesting.pnl.providers.funding_rates import FundingRateProvider

logger = logging.getLogger(__name__)

#: Fixed-lane rate when no ``BacktestDataConfig`` is provided. The same scalar
#: as ``BacktestDataConfig.funding_fallback_rate``,
#: ``providers.funding_rates.DEFAULT_FUNDING_RATE``, and the perp adapter's
#: ``PerpBacktestConfig.default_funding_rate`` defaults, so what ``decide()``
#: gates on matches what the position pays on a default run.
DEFAULT_FALLBACK_RATE = Decimal("0.0001")  # 0.01% per hour

_HOURS_PER_8H = Decimal("8")


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


class SnapshotFundingRateSource:
    """Per-backtest-run source behind strategy-facing funding reads.

    One instance per engine run (built in ``execute_iteration_loop``); each
    tick binds it to the tick's simulated timestamp via :meth:`view_at`. The
    instance owns the per-hour rate cache and the lazily-constructed
    ``FundingRateProvider``, so parallel sweeps never share mutable state.
    """

    def __init__(self, *, chain: str, data_config: BacktestDataConfig | None = None) -> None:
        self._chain = chain.strip().lower()
        self._use_historical = bool(data_config is not None and data_config.use_historical_funding)
        self._strict = bool(data_config is not None and data_config.strict_historical_mode)
        self._fallback_rate = data_config.funding_fallback_rate if data_config is not None else DEFAULT_FALLBACK_RATE
        self._provider: FundingRateProvider | None = None
        self._provider_init_done = False
        # Cached resolution per (venue, market, hour): the served FundingRate,
        # or the strict-mode FundingRateUnavailableError — unavailability is as
        # deterministic per simulated hour as a resolved rate, and caching it
        # keeps strict runs at one gateway attempt per hour like non-strict.
        self._cache: dict[tuple[str, str, datetime], FundingRate | FundingRateUnavailableError] = {}

    def view_at(self, timestamp: datetime) -> SnapshotFundingRateView:
        """Provider view bound to one tick's simulated timestamp."""
        return SnapshotFundingRateView(self, timestamp)

    async def funding_rate_at(self, venue: Venue | str, market: str, timestamp: datetime) -> FundingRate:
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
        market_upper = perp_market_funding_key(market) or market.upper()
        hour = _hour_utc(timestamp)
        key = (venue_value, market_upper, hour)
        cached = self._cache.get(key)
        if isinstance(cached, FundingRateUnavailableError):
            raise cached
        if cached is not None:
            return cached

        if self._use_historical:
            try:
                rate = await self._historical_rate(venue_value, market_upper, hour)
            except FundingRateUnavailableError as exc:
                self._cache[key] = exc
                raise
        else:
            rate = self._fallback_rate

        result = FundingRate(
            venue=venue_value,
            market=market_upper,
            rate_hourly=rate,
            rate_8h=rate * _HOURS_PER_8H,
            rate_annualized=rate * HOURS_PER_YEAR,
            timestamp=hour,
            is_live_data=False,
        )
        self._cache[key] = result
        return result

    async def _historical_rate(self, venue: str, market: str, hour: datetime) -> Decimal:
        """Latest measured hourly rate at or before ``hour`` (no look-ahead)."""
        declared = FundingHistoryRegistry.declared_chains(venue)
        if declared and self._chain not in declared:
            return self._degraded(venue, market, f"venue declares no funding data for chain '{self._chain}'")

        provider = self._ensure_provider()
        if provider is None:
            return self._degraded(venue, market, "no funding-history connector declares this run's chain")

        from almanak.framework.backtesting.pnl.providers.funding_rates import FundingRateError

        try:
            data = await provider.get_historical_funding_rate(protocol=venue, market=market, timestamp=hour)
        except FundingRateError as exc:
            return self._degraded(venue, market, str(exc))
        if data.source == "fallback":
            # The gateway had no measured point in the lookback window (or was
            # unreachable) — substitute the engine-configured fallback, not the
            # provider's module default, so one knob governs both funding lanes.
            return self._degraded(venue, market, "no measured funding point at or before the tick")
        return data.rate

    def _ensure_provider(self) -> FundingRateProvider | None:
        """Lazily build the shared gateway-history client (once per run).

        ``FundingRateProvider`` rejects chains no funding connector declares;
        venue/chain compatibility is already enforced per-read in
        ``_historical_rate`` and chain-agnostic venues ignore the constructor
        chain entirely, so any declared chain is a safe stand-in when this
        run's chain has no on-chain funding venue.
        """
        if not self._provider_init_done:
            self._provider_init_done = True
            from almanak.framework.backtesting.pnl.providers.funding_rates import FundingRateProvider

            declared = FundingHistoryRegistry.all_declared_chains()
            chain = self._chain if self._chain in declared else next(iter(sorted(declared)), None)
            if chain is not None:
                self._provider = FundingRateProvider(chain=chain)
        return self._provider

    def _degraded(self, venue: str, market: str, reason: str) -> Decimal:
        if self._strict:
            raise FundingRateUnavailableError(venue, market, reason)
        logger.warning(
            "Historical funding unavailable for %s/%s (%s); using fallback rate %s/h",
            venue,
            market,
            reason,
            self._fallback_rate,
        )
        return self._fallback_rate


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

    async def get_funding_rate(self, venue: Venue | str, market: str) -> FundingRate:
        return await self._source.funding_rate_at(venue, market, self._timestamp)

    async def get_funding_rate_spread(
        self,
        market: str,
        venue_a: Venue | str,
        venue_b: Venue | str,
    ) -> FundingRateSpread:
        rate_a = await self.get_funding_rate(venue_a, market)
        rate_b = await self.get_funding_rate(venue_b, market)
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
    "DEFAULT_FALLBACK_RATE",
    "SnapshotFundingRateSource",
    "SnapshotFundingRateView",
]
