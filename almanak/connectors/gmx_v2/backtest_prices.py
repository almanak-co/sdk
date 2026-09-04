"""Gateway-backed GMX oracle-candle provider for historical backtests.

The strategy process performs no provider HTTP.  It asks the gateway for a
verified market-scoped page; the GMX connector resolves
``market -> market token -> index token -> index symbol`` and owns the API call.
CoinGecko remains the fallback only for *other* tracked assets.  The GMX index
series is never stitched with another provider.
"""

from __future__ import annotations

import asyncio
import bisect
import logging
import statistics
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.framework.backtesting.pnl.data_provider import (
    OHLCV,
    HistoricalDataCapability,
    HistoricalDataConfig,
    HistoricalPriceObservation,
    MarketState,
    TokenRef,
    is_address_like,
    is_token_key,
    normalize_token_key,
    normalize_token_ref,
    token_ref_display,
    token_ref_provider_symbol,
)
from almanak.framework.backtesting.pnl.providers.coingecko import OHLCVCache
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.ohlcv.venue_native_provider import market_labels_agree
from almanak.framework.data.timeframes import CANONICAL_OHLCV_TIMEFRAMES, parse_ohlcv_timeframe

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

logger = logging.getLogger(__name__)

_GMX_TIMEFRAMES: tuple[str, ...] = tuple(timeframe.value for timeframe in CANONICAL_OHLCV_TIMEFRAMES)
_PAGE_LIMIT = 10_000
_MAX_PAGE_REQUESTS = 256


class GMXPriceHistoryCoverageError(ValueError):
    """The exact requested GMX candle plane cannot cover the whole window."""


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class _GMXOracleMarketSource:
    """One verified GMX market's provider-exact historical series."""

    def __init__(self, *, chain: str, market: str, venue: str) -> None:
        self.chain = chain.lower()
        self.requested_market = market.strip()
        self.resolved_market: str | None = None
        self.venue = venue.lower()
        self.market_token: str | None = None
        self.index_token: str | None = None
        self.index_symbol: str | None = None
        self.timeframe: str | None = None
        self._series: list[OHLCV] = []
        self._series_timestamps: list[datetime] = []
        self._coverage: tuple[datetime, datetime] | None = None
        self._alias_symbol: str | None = None
        self._alias_symbol_resolved = False

    @staticmethod
    def _gateway() -> tuple[Any, Any]:
        from almanak.framework.gateway_client import get_gateway_client
        from almanak.gateway.proto import gateway_pb2

        client = get_gateway_client()
        if not client.is_connected:
            client.connect()
        return client, gateway_pb2

    async def _fetch_page(self, *, timeframe: str, before_ts: int, limit: int = _PAGE_LIMIT) -> Any:
        client, gateway_pb2 = self._gateway()
        request = gateway_pb2.GetPerpPriceCandlesRequest(
            venue=self.venue,
            chain=self.chain,
            market=self.requested_market,
            timeframe=timeframe,
            before_ts=before_ts,
            limit=limit,
        )
        try:
            response = await asyncio.to_thread(
                client.rate_history.GetPerpPriceCandles,
                request,
                timeout=client.config.timeout,
            )
        except Exception as exc:
            raise DataSourceUnavailable(
                source="gateway",
                reason=f"GetPerpPriceCandles RPC failed: {exc}",
                transport=True,
            ) from exc
        if not response.success:
            raise DataSourceUnavailable(
                source="gmx_oracle",
                reason=response.error or "GMX oracle candles unavailable",
            )
        return response

    def _accept_identity(self, response: Any, timeframe: str) -> None:
        identity = (
            response.market,
            response.market_token.lower(),
            response.index_token.lower(),
            response.index_symbol.upper(),
        )
        if not all(identity) or response.timeframe != timeframe:
            raise DataSourceUnavailable(
                source="gmx_oracle",
                reason="gateway returned incomplete GMX market provenance",
            )
        # Provenance that is merely present and self-consistent proves
        # nothing about *which* market it is for. Page 1 is latched as ground
        # truth for the whole series, so a page about a different market (a
        # gateway dispatch bug, a mis-associated response, a connector that
        # resolved the wrong catalogue entry) would silently price the backtest
        # off another instrument. Bind the page to the request first. Label
        # agreement uses the live lane's helper so both guards mean the same
        # thing by "same market"; an address request cannot be compared as a
        # label, so it binds on the served market token instead.
        self._require_requested_market(response)
        current = (self.resolved_market, self.market_token, self.index_token, self.index_symbol)
        if current != (None, None, None, None) and current != identity:
            raise DataSourceUnavailable(
                source="gmx_oracle",
                reason="verified GMX market identity changed while paging history",
            )
        self.resolved_market, self.market_token, self.index_token, self.index_symbol = identity

    def _require_requested_market(self, response: Any) -> None:
        """Refuse a candle page whose served market is not the requested one.

        ``response.venue`` and ``response.chain`` are not compared: the gateway
        echoes the request's own values into them
        (``rate_history_service._perp_price_success_response``), so they
        attest nothing about the page. ``market`` / ``market_token`` come from
        the connector's verified catalogue record and are the binding fields.
        """
        requested = self.requested_market
        served_market = str(response.market)
        # Anything 0x-prefixed binds as an address: the label helper waves
        # every 0x request through, so a malformed address must reach the
        # token comparison, where it never matches and is refused.
        if requested.lower().startswith("0x"):
            served_token = str(response.market_token).lower()
            if served_token == requested.lower():
                return
            raise DataSourceUnavailable(
                source="gmx_oracle",
                reason=(
                    f"gateway served market token {served_token!r} ({served_market!r}) on a request for "
                    f"{requested!r} ({self.chain}); refusing a candle page that is not about the requested market"
                ),
            )
        if market_labels_agree(served_market, requested):
            return
        raise DataSourceUnavailable(
            source="gmx_oracle",
            reason=(
                f"gateway answered for market {served_market!r} on a request for {requested!r} "
                f"({self.chain}); refusing a candle page that is not about the requested market"
            ),
        )

    @staticmethod
    def _decode_candle(raw: Any, timeframe: str) -> OHLCV:
        try:
            values = tuple(Decimal(value) for value in (raw.open, raw.high, raw.low, raw.close))
        except (ArithmeticError, ValueError) as exc:
            raise DataSourceUnavailable(source="gmx_oracle", reason="gateway returned malformed candle data") from exc
        if raw.timestamp <= 0 or any(not value.is_finite() or value <= 0 for value in values):
            raise DataSourceUnavailable(source="gmx_oracle", reason="gateway returned invalid candle data")
        # GMX timestamps candle *open* time and includes the currently forming
        # candle.  A close is observable only after the native interval ends;
        # shifting to close time prevents look-ahead at the first tick.
        close_timestamp = raw.timestamp + parse_ohlcv_timeframe(timeframe, field_name="GMX timeframe").seconds
        return OHLCV(
            timestamp=datetime.fromtimestamp(close_timestamp, tz=UTC),
            open=values[0],
            high=values[1],
            low=values[2],
            close=values[3],
            volume=None,
        )

    async def _fetch_complete(self, *, timeframe: str, start: datetime, end: datetime) -> list[OHLCV]:
        seconds = parse_ohlcv_timeframe(timeframe, field_name="GMX timeframe").seconds
        start_utc, end_utc = _utc(start), _utc(end)
        start_ts, end_ts = int(start_utc.timestamp()), int(end_utc.timestamp())
        before_ts = end_ts + seconds
        expected_first_ts = start_ts - (start_ts % seconds)
        # GMX pages by candle-open timestamp while the provider exposes the
        # candle only at close. Request one raw candle before the first close
        # we need and one currently-forming candle after the last close. A
        # fixed 10k page can extend years before a short backtest and make an
        # unrelated malformed upstream row abort otherwise valid coverage.
        earliest_open_ts = expected_first_ts - seconds
        by_timestamp: dict[int, OHLCV] = {}

        for _request_number in range(_MAX_PAGE_REQUESTS):
            latest_open_ts = (before_ts // seconds) * seconds - seconds
            remaining_points = ((latest_open_ts - earliest_open_ts) // seconds) + 1
            if remaining_points <= 0:
                break
            response = await self._fetch_page(
                timeframe=timeframe,
                before_ts=before_ts,
                limit=min(_PAGE_LIMIT, remaining_points),
            )
            self._accept_identity(response, timeframe)
            page = [self._decode_candle(raw, timeframe) for raw in response.candles]
            if not page:
                break
            for candle in page:
                timestamp = int(candle.timestamp.timestamp())
                if timestamp <= end_ts:
                    by_timestamp[timestamp] = candle
            oldest_close_ts = min(int(candle.timestamp.timestamp()) for candle in page)
            if oldest_close_ts <= start_ts:
                break
            # Pagination is expressed in GMX's raw candle-open timestamps;
            # using our shifted close timestamps here would repeat one page.
            oldest_open_ts = min(int(raw.timestamp) for raw in response.candles)
            if oldest_open_ts >= before_ts:
                raise DataSourceUnavailable(
                    source="gmx_oracle",
                    reason="GMX candle pagination did not make backward progress",
                )
            before_ts = oldest_open_ts
        else:
            raise DataSourceUnavailable(
                source="gmx_oracle",
                reason="GMX candle pagination exceeded its request bound",
            )

        series = [by_timestamp[key] for key in sorted(by_timestamp)]
        if not series:
            raise GMXPriceHistoryCoverageError(
                f"GMX {timeframe} candles contain no observations for {self.requested_market} in the requested window"
            )
        # A complete native plane has one close on every UTC-aligned boundary
        # spanning the requested window.  Checking only oldest/newest would
        # accept internal outages and forward-fill across them.
        expected_last_ts = end_ts - (end_ts % seconds)
        series = [
            candle for candle in series if expected_first_ts <= int(candle.timestamp.timestamp()) <= expected_last_ts
        ]
        oldest = series[0].timestamp if series else None
        newest = series[-1].timestamp if series else None
        missing_boundary = (
            oldest is None
            or newest is None
            or int(oldest.timestamp()) != expected_first_ts
            or int(newest.timestamp()) != expected_last_ts
        )
        gap = next(
            (
                (left.timestamp, right.timestamp)
                for left, right in zip(series, series[1:], strict=False)
                if int((right.timestamp - left.timestamp).total_seconds()) != seconds
            ),
            None,
        )
        if missing_boundary or gap is not None:
            observed = (
                "no aligned observations"
                if oldest is None or newest is None
                else f"{oldest.isoformat()}..{newest.isoformat()}"
            )
            gap_detail = "" if gap is None else f" with an internal gap at {gap[0].isoformat()}..{gap[1].isoformat()}"
            raise GMXPriceHistoryCoverageError(
                f"GMX {timeframe} coverage for {self.requested_market} is {observed}{gap_detail}, which does not provide "
                f"every native candle across {start_utc.isoformat()}..{end_utc.isoformat()}"
            )
        return series

    async def prepare(self, *, requested: str | None, start: datetime, end: datetime) -> str:
        """Resolve exact or auto cadence and materialize one complete series."""
        if requested == "auto":
            cadence_errors: list[str] = []
            for timeframe in _GMX_TIMEFRAMES:
                try:
                    series = await self._fetch_complete(timeframe=timeframe, start=start, end=end)
                except GMXPriceHistoryCoverageError as exc:
                    cadence_errors.append(f"{timeframe}: {exc}")
                    continue
                except DataSourceUnavailable as exc:
                    # A connection/RPC failure says nothing about a cadence's
                    # coverage and must abort immediately.  A valid gateway
                    # response whose data is malformed or unavailable is a
                    # cadence-level rejection, so auto may still probe the
                    # next native plane while retaining the diagnostic.
                    if exc.transport:
                        raise
                    cadence_errors.append(f"{timeframe}: {exc}")
                    continue
                self.install_series(timeframe=timeframe, series=series, start=start, end=end)
                return timeframe
            raise GMXPriceHistoryCoverageError(
                "No native GMX candle timeframe covers the requested window: " + "; ".join(cadence_errors)
            )

        if requested is None:
            raise ValueError("An explicit GMX timeframe is required when auto resolution is disabled")
        if requested not in _GMX_TIMEFRAMES:
            raise ValueError(
                f"GMX does not support explicit timeframe {requested!r}; choose one of {_GMX_TIMEFRAMES} or 'auto'"
            )
        series = await self._fetch_complete(timeframe=requested, start=start, end=end)
        self.install_series(timeframe=requested, series=series, start=start, end=end)
        return requested

    def install_series(
        self,
        *,
        timeframe: str,
        series: list[OHLCV],
        start: datetime,
        end: datetime,
    ) -> None:
        """Commit one fully validated candidate cadence to this source."""
        self.timeframe = timeframe
        self._series = series
        self._series_timestamps = [candle.timestamp for candle in series]
        self._coverage = (_utc(start), _utc(end))

    def remember_verified_market(self) -> None:
        """Publish this run's venue-verified market identity to the perps-read catalog.

        The live path populates ``market_catalog`` from the compiler (open /
        close compilation); a backtest never compiles, so the address-form
        fill-pricing and close-matching lanes
        (``PerpsReadRegistry.market_metadata`` → ``market_catalog``) could not
        resolve the declared market's index asset and rejected every PERP_OPEN
        as unpriceable — while THIS source held the verified identity for
        exactly that market. Resolve once through the same venue-verified
        surface the compiler uses (``GetPerpMarket``, VIB-6561) — the warm
        step the live runner performs in
        ``inner_runner._resolve_perp_index_symbol`` — and remember the
        result. It never substitutes a guessed symbol.

        Fail-open on an unavailable/unknowing dynamic surface: the candle
        plane still serves, and the fill lane keeps its NAMED per-intent
        rejection — never a guessed identity. Fail-closed on identity
        disagreement with the candle provenance already accepted for this run,
        and a no-op before that provenance exists (nothing to cross-check).
        """
        if self.market_token is None:
            return
        from . import market_catalog
        from .market_metadata import resolve_market_via_gateway

        try:
            client, _ = self._gateway()
            metadata = resolve_market_via_gateway(client, chain=self.chain, market=self.requested_market)
        except Exception as exc:  # noqa: BLE001 - enrichment only; the fill lane stays loud without it
            logger.warning(
                "GMX market metadata unavailable for %s on %s: %s — address-form PERP_OPEN "
                "fill pricing will reject until the market can be verified",
                self.requested_market,
                self.chain,
                exc,
            )
            return
        metadata_identity = (
            metadata.market_token.lower(),
            metadata.index_token.lower(),
            metadata.index_symbol.upper(),
        )
        candle_identity = (self.market_token, self.index_token, self.index_symbol)
        if metadata_identity != candle_identity:
            logger.warning(
                "GMX market metadata identity for %s on %s disagrees with the verified candle "
                "provenance (%s != %s); refusing to remember it",
                self.requested_market,
                self.chain,
                metadata_identity,
                candle_identity,
            )
            return
        market_catalog.remember(self.chain, metadata)

    def owns(self, token: TokenRef) -> bool:
        if self.index_symbol is None or self.index_token is None:
            return False
        # Normalize first so every spelling of the index identity — tuple key,
        # bare address, "chain:address" display key — resolves to ownership.
        normalized = normalize_token_ref(token, self.chain)
        if is_token_key(normalized):
            chain, address = normalized
            return chain == self.chain and address == self.index_token
        assert isinstance(normalized, str)
        labels = {self.index_symbol, self.index_token.upper()}
        # The published read alias is owned vocabulary too: on a native-label
        # collision the alias is the index token's registry symbol (WETH), and
        # a strategy tracking that spelling must route to the venue series —
        # sending it to the fallback would fetch the SAME (chain, address)
        # cache key from another provider.
        alias = self.alias_symbol()
        if alias is not None:
            labels.add(alias.upper())
        return normalized.upper() in labels

    def alias_symbol(self) -> str | None:
        """Symbol under which the verified index identity may publish a read alias.

        GMX labels its wrapped-native-indexed markets with the bare gas symbol
        (index symbol "ETH", index token WETH), but on the run chain that
        symbol's address-native identity is the native sentinel (ALM-3067), so
        publishing the venue label would make one symbol claim two identities
        and trip the engine's fail-closed ambiguity guard at tick 1. On such a
        collision the alias is the index token's registry symbol (the wrapped
        ERC-20, e.g. WETH); native-symbol reads then resolve through the chain
        registry's native<->wrapped 1:1 plane. A collision the registry cannot
        name outside the native family publishes no alias at all — the series
        stays reachable by its ``(chain, address)`` identity, never under a
        symbol two assets claim.
        """
        if self.index_symbol is None or self.index_token is None:
            return None
        if self._alias_symbol_resolved:
            return self._alias_symbol
        self._alias_symbol = self._resolve_alias_symbol(self.index_symbol, self.index_token)
        self._alias_symbol_resolved = True
        return self._alias_symbol

    def _resolve_alias_symbol(self, index_symbol: str, index_token: str) -> str | None:
        from almanak.core.chains._helpers import native_symbols_for
        from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

        index_key = normalize_token_key(self.chain, index_token)
        if index_symbol.upper() not in native_symbols_for(self.chain):
            return index_symbol
        if index_key == normalize_token_key(self.chain, NATIVE_SENTINEL):
            # The venue's claim IS the chain-native identity; the label agrees
            # with the engine's own native registration.
            return index_symbol
        registry_symbol = token_ref_provider_symbol(index_key, self.chain)
        if (
            is_address_like(registry_symbol)
            or ":" in registry_symbol
            or registry_symbol.upper() in native_symbols_for(self.chain)
        ):
            return None
        return registry_symbol

    def series(self, *, start: datetime, end: datetime, interval_seconds: int) -> list[OHLCV]:
        timeframe, coverage = self._prepared_series_contract()
        self._validate_series_interval(timeframe, interval_seconds)
        self._validate_series_coverage(coverage, start=start, end=end)
        return list(self._series)

    def _prepared_series_contract(self) -> tuple[str, tuple[datetime, datetime]]:
        """Return the immutable preparation contract or fail before data access."""
        if self.timeframe is None or self._coverage is None:
            raise RuntimeError("GMX oracle history was not prepared before use")
        return self.timeframe, self._coverage

    @staticmethod
    def _validate_series_interval(timeframe: str, interval_seconds: int) -> None:
        if parse_ohlcv_timeframe(timeframe, field_name="resolved GMX timeframe").seconds != interval_seconds:
            raise ValueError(
                f"Explicit {interval_seconds}s data request does not match resolved GMX timeframe {timeframe}"
            )

    @staticmethod
    def _validate_series_coverage(
        coverage: tuple[datetime, datetime],
        *,
        start: datetime,
        end: datetime,
    ) -> None:
        coverage_start, coverage_end = coverage
        if _utc(start) < coverage_start or _utc(end) > coverage_end:
            raise ValueError("GMX oracle request falls outside the prepared backtest window")

    def price_at(self, timestamp: datetime) -> Decimal:
        return self.candle_at(timestamp).close

    def candle_at(self, timestamp: datetime) -> Any:
        """The candle in force at ``timestamp`` -- the latest at or before it.

        Callers that publish an observation need the candle's OWN timestamp:
        when the venue cadence is coarser than the simulation tick, stamping
        the tick would make an older mark look freshly measured.
        """
        target = _utc(timestamp)
        index = bisect.bisect_right(self._series_timestamps, target) - 1
        if index < 0:
            raise ValueError(f"No GMX oracle price for {self.index_symbol} at {target.isoformat()}")
        return self._series[index]

    @property
    def provenance(self) -> dict[str, str | None]:
        return {
            "source": "gmx_oracle_candles",
            "venue": self.venue,
            "chain": self.chain,
            "market": self.resolved_market or self.requested_market,
            "requested_market": self.requested_market,
            "market_token": self.market_token,
            "index_token": self.index_token,
            "index_symbol": self.index_symbol,
            "timeframe": self.timeframe,
            "fidelity": "index_mark_ohlc_without_execution_spread_or_price_impact",
        }


class GMXOracleDataProvider:
    """Route verified GMX index assets to GMX and all other assets to fallback."""

    resolution_based_availability: ClassVar[bool] = True
    historical_capability: ClassVar[HistoricalDataCapability] = HistoricalDataCapability.FULL

    def __init__(
        self,
        *,
        fallback: Any,
        chain: str,
        market: str | None = None,
        markets: tuple[str, ...] | None = None,
        venue: str = "gmx_v2",
    ) -> None:
        requested_markets = markets if markets is not None else ((market,) if market is not None else ())
        if not requested_markets:
            raise ValueError("GMX price-history provider requires at least one market")
        if market is not None and markets is not None:
            raise ValueError("Declare GMX price-history market or markets, not both")
        if any(not isinstance(item, str) or not item.strip() for item in requested_markets):
            raise ValueError("Every GMX price-history market must be a non-empty string")
        normalized_markets = tuple(item.strip() for item in requested_markets)
        if len({item.lower() for item in normalized_markets}) != len(normalized_markets):
            raise ValueError("GMX price-history markets must be unique")
        self._fallback = fallback
        self._sources = tuple(
            _GMXOracleMarketSource(chain=chain, market=item, venue=venue) for item in normalized_markets
        )
        # Singular compatibility for callers and tests that inspect the
        # original one-market provider internals.
        self._source = self._sources[0]
        self._chain = chain.lower()
        self._cache: OHLCVCache | None = None
        self._price_sources: dict[str, str] = {}
        self.measured_granularity_seconds: int | None = None

    @classmethod
    def for_backtest(cls, *, fallback: Any, chain: str, market: str, venue: str) -> GMXOracleDataProvider:
        return cls(fallback=fallback, chain=chain, market=market, venue=venue)

    @classmethod
    def for_backtest_many(
        cls,
        *,
        fallback: Any,
        chain: str,
        markets: tuple[str, ...],
        venue: str,
    ) -> GMXOracleDataProvider:
        return cls(fallback=fallback, chain=chain, markets=markets, venue=venue)

    @staticmethod
    def _canonical_timeframe_for_interval(interval_seconds: int) -> str:
        requested = next(
            (timeframe.value for timeframe in CANONICAL_OHLCV_TIMEFRAMES if timeframe.seconds == interval_seconds),
            None,
        )
        if requested is None:
            raise ValueError(
                f"GMX oracle candles cannot serve explicit {interval_seconds}s ticks exactly; "
                "choose a canonical OHLCV interval or timeframe='auto'"
            )
        return requested

    @classmethod
    def _requested_timeframe(cls, config: PnLBacktestConfig) -> str:
        # A serialized result carries the previously resolved cadence. Replays
        # pin and revalidate that exact plane instead of re-running auto against
        # a provider whose retention boundary may have moved.
        requested = getattr(config, "resolved_timeframe", None) or getattr(config, "timeframe", None)
        return requested or cls._canonical_timeframe_for_interval(int(config.interval_seconds))

    async def prepare_backtest(self, config: PnLBacktestConfig) -> str:
        requested = self._requested_timeframe(config)
        if len(self._sources) == 1:
            resolved = await self._source.prepare(requested=requested, start=config.start_time, end=config.end_time)
        else:
            resolved = await self._prepare_all_sources(
                requested=requested,
                start=config.start_time,
                end=config.end_time,
            )
        resolved_seconds = parse_ohlcv_timeframe(resolved, field_name="resolved GMX timeframe").seconds
        self.measured_granularity_seconds = resolved_seconds
        config.apply_resolved_timeframe(resolved, resolved_seconds)
        # Sole priming path for the process perps-read catalog, which only the
        # (never-run-in-backtest) compiler otherwise populates. Deliberately
        # AFTER prepare(): the resolved metadata is cross-checked against the
        # accepted candle provenance, and an unconditional pre-prepare prime
        # would remember records that check can never veto. A miss is
        # non-fatal — the fill-pricing and close-matching lanes keep their
        # fail-closed named rejections.
        for source in self._sources:
            await asyncio.to_thread(source.remember_verified_market)
        return resolved

    async def _prepare_all_sources(self, *, requested: str, start: datetime, end: datetime) -> str:
        """Commit one native cadence only when it completely covers every target."""
        if requested != "auto" and requested not in _GMX_TIMEFRAMES:
            raise ValueError(
                f"GMX does not support explicit timeframe {requested!r}; choose one of {_GMX_TIMEFRAMES} or 'auto'"
            )
        candidates = _GMX_TIMEFRAMES if requested == "auto" else (requested,)
        cadence_errors: list[str] = []
        for timeframe in candidates:
            prepared: list[tuple[_GMXOracleMarketSource, list[OHLCV]]] = []
            candidate_error: str | None = None
            for source in self._sources:
                try:
                    series = await source._fetch_complete(timeframe=timeframe, start=start, end=end)
                except GMXPriceHistoryCoverageError as exc:
                    candidate_error = f"{source.requested_market}: {exc}"
                    break
                except DataSourceUnavailable as exc:
                    if exc.transport:
                        raise
                    candidate_error = f"{source.requested_market}: {exc}"
                    break
                prepared.append((source, series))
            if candidate_error is not None:
                if requested != "auto":
                    raise GMXPriceHistoryCoverageError(
                        f"GMX {timeframe} does not cover every basket target; {candidate_error}"
                    )
                cadence_errors.append(f"{timeframe}: {candidate_error}")
                continue

            # Every source accepted this exact candidate. Validate the
            # identities before publishing any series as the run's plane.
            self._validate_source_identities()
            for source, series in prepared:
                source.install_series(timeframe=timeframe, series=series, start=start, end=end)
            return timeframe

        raise GMXPriceHistoryCoverageError(
            "No single native GMX candle timeframe covers every basket target: " + "; ".join(cadence_errors)
        )

    def _validate_source_identities(self) -> None:
        """Reject duplicate assets and aliases before any market state is served."""
        owners: dict[tuple[str, str], str] = {}
        labels: dict[str, tuple[str, str]] = {}
        for source in self._sources:
            if source.index_token is None or source.index_symbol is None:
                raise DataSourceUnavailable(
                    source="gmx_oracle",
                    reason=f"GMX market {source.requested_market!r} has no verified index identity",
                )
            index_key = normalize_token_key(self._chain, source.index_token)
            previous_market = owners.get(index_key)
            if previous_market is not None and previous_market != source.requested_market:
                raise DataSourceUnavailable(
                    source="gmx_oracle",
                    reason=(
                        f"GMX markets {previous_market!r} and {source.requested_market!r} resolve to "
                        f"the same index token {token_ref_display(index_key)}"
                    ),
                )
            owners[index_key] = source.requested_market
            for label in (source.index_symbol, source.alias_symbol()):
                if label is None:
                    continue
                normalized_label = label.upper()
                prior_key = labels.get(normalized_label)
                if prior_key is not None and prior_key != index_key:
                    raise DataSourceUnavailable(
                        source="gmx_oracle",
                        reason=(
                            f"GMX basket alias {normalized_label!r} resolves to both "
                            f"{token_ref_display(prior_key)} and {token_ref_display(index_key)}"
                        ),
                    )
                labels[normalized_label] = index_key

    def _source_for(self, token: TokenRef) -> _GMXOracleMarketSource | None:
        owners = [source for source in self._sources if source.owns(token)]
        if len(owners) > 1:
            raise RuntimeError(f"GMX basket token {token_ref_display(token)!r} has multiple price-history owners")
        return owners[0] if owners else None

    def register_token_addresses(self, addresses: dict[str, tuple[str, str]]) -> None:
        register = getattr(self._fallback, "register_token_addresses", None)
        if callable(register):
            register(addresses)

    def _cache_key(self, token: TokenRef) -> TokenRef:
        source = self._source_for(token)
        if source is not None:
            if source.index_token is None:
                raise RuntimeError("GMX oracle market identity was not prepared before cache-key resolution")
            # Provider-owned prices are always address-native.  The verified
            # symbol is published separately as a read alias on MarketState;
            # replacing the key with the symbol would break address-native
            # strategy configs, reporting, and data-quality accounting.
            return normalize_token_key(self._chain, source.index_token)
        key_fn = getattr(self._fallback, "_market_cache_key", None)
        if callable(key_fn):
            return key_fn(token, self._chain)
        if is_token_key(token):
            return normalize_token_key(token[0], token[1])
        return token_ref_display(token).upper()

    async def get_price(self, token: TokenRef, timestamp: datetime) -> Decimal:
        source = self._source_for(token)
        if source is not None:
            return source.price_at(timestamp)
        return await self._fallback.get_price(token, timestamp)

    async def get_ohlcv(
        self,
        token: TokenRef,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        source = self._source_for(token)
        if source is not None:
            return source.series(start=start, end=end, interval_seconds=interval_seconds)
        return await self._fallback.get_ohlcv(token, start, end, interval_seconds)

    async def _prefetch(self, config: HistoricalDataConfig) -> OHLCVCache:
        data: dict[TokenRef, list[OHLCV]] = {}
        sources: dict[str, str] = {}
        fallback_tokens: list[TokenRef] = []
        owns_any = False
        for token in config.tokens:
            source = self._source_for(token)
            if source is None:
                fallback_tokens.append(token)
                continue
            owns_any = True
            key = self._cache_key(token)
            if key in data:
                continue
            native_interval = self._native_interval_seconds()
            candles = source.series(
                start=config.start_time,
                end=config.end_time,
                interval_seconds=native_interval,
            )
            data[key] = sorted(candles, key=lambda candle: candle.timestamp)
            sources[token_ref_display(key)] = "gmx_oracle_candles"

        fallback_data = await self._prefetch_fallback(config, fallback_tokens)
        # The venue-owned series is never replaced (blueprint 31: the GMX
        # index asset never stitches or falls back). ``owns()`` routes every
        # known spelling of the index identity here, but a user-registered
        # custom symbol for the index token still normalizes onto the owned
        # (chain, address) cache key in the fallback — dropping it loudly
        # keeps that key venue-served; the dropped symbol becomes an honest
        # per-tick miss instead of plausible cross-provider prices.
        for key in [key for key in fallback_data if key in data]:
            fallback_data.pop(key)
            logger.warning(
                "Discarding fallback price series for %s: its identity is the venue-owned "
                "GMX index series, which is never replaced by another provider",
                token_ref_display(key),
            )
        data.update(fallback_data)
        fallback_source = getattr(self._fallback, "provider_name", "fallback")
        sources.update({token_ref_display(key): fallback_source for key in fallback_data})

        self._price_sources = sources
        measured = self._measure_granularity(data)
        measured_native_interval = self._native_interval_seconds() if owns_any else None
        granularities = [value for value in (measured, measured_native_interval) if value is not None]
        self.measured_granularity_seconds = max(granularities) if granularities else None
        return OHLCVCache(data=data, fetched_at=datetime.now(UTC), default_chain=self._chain)

    def _native_interval_seconds(self) -> int:
        timeframes = {source.timeframe for source in self._sources}
        if None in timeframes:
            raise RuntimeError("GMX oracle history was not prepared before prefetch")
        resolved_timeframes = {timeframe for timeframe in timeframes if timeframe is not None}
        if len(resolved_timeframes) != 1:
            raise RuntimeError(f"GMX basket sources do not share one native timeframe: {sorted(resolved_timeframes)!r}")
        timeframe = next(iter(resolved_timeframes))
        return parse_ohlcv_timeframe(timeframe, field_name="resolved GMX timeframe").seconds

    async def _prefetch_fallback(
        self,
        config: HistoricalDataConfig,
        tokens: list[TokenRef],
    ) -> dict[TokenRef, list[OHLCV]]:
        """Prefetch non-owned assets through the fallback's complete policy."""
        if not tokens:
            return {}
        fallback_config = replace(config, tokens=tokens)
        prefetch = getattr(self._fallback, "prefetch_ohlcv_data", None)
        if callable(prefetch):
            cache = await prefetch(fallback_config)
            if not isinstance(cache, OHLCVCache):
                raise TypeError("Fallback prefetch_ohlcv_data() must return OHLCVCache")
            return dict(cache.data)

        # Compatibility path for third-party providers without the public
        # composition hook.  It deliberately mirrors CoinGecko's two crucial
        # invariants: one bad token degrades only that token, and first-tick
        # seeding searches the prior 24 hours rather than one tick.
        data: dict[TokenRef, list[OHLCV]] = {}
        start, end = _utc(config.start_time), _utc(config.end_time)
        for token in tokens:
            key = self._cache_key(token)
            try:
                candles = await self._fallback.get_ohlcv(token, start, end, config.interval_seconds)
            except ValueError:
                data[key] = []
                continue
            candles = sorted(candles, key=lambda candle: candle.timestamp)
            if candles and candles[0].timestamp > start:
                try:
                    prior = await self._fallback.get_ohlcv(
                        token,
                        start - timedelta(days=1),
                        start,
                        config.interval_seconds,
                    )
                except ValueError:
                    prior = []
                candidates = [candle for candle in prior if candle.timestamp <= start]
                if candidates and candidates[-1].timestamp < candles[0].timestamp:
                    candles = [candidates[-1], *candles]
            data[key] = candles
        return data

    @staticmethod
    def _measure_granularity(data: dict[TokenRef, list[OHLCV]]) -> int | None:
        per_token: list[int] = []
        for candles in data.values():
            if len(candles) < 2:
                continue
            spacings = [
                int((right.timestamp - left.timestamp).total_seconds())
                for left, right in zip(candles, candles[1:], strict=False)
            ]
            per_token.append(int(statistics.median(spacings)))
        return max(per_token) if per_token else None

    @staticmethod
    def _advance_cursor(candles: list[OHLCV], cursor: int, current: datetime) -> int:
        while cursor + 1 < len(candles) and candles[cursor + 1].timestamp <= current:
            cursor += 1
        return cursor

    def _market_state_at(
        self,
        *,
        current: datetime,
        config: HistoricalDataConfig,
        cursors: dict[TokenRef, int],
    ) -> MarketState:
        assert self._cache is not None
        prices: dict[TokenRef, Decimal] = {}
        ohlcv: dict[TokenRef, OHLCV] = {}
        price_observations: dict[TokenRef, HistoricalPriceObservation] = {}
        for token in config.tokens:
            key = self._cache_key(token)
            candles = self._cache.data.get(key, [])
            cursor = self._advance_cursor(candles, cursors.get(key, -1), current)
            cursors[key] = cursor
            if cursor < 0:
                continue
            candle = candles[cursor]
            prices[key] = candle.close
            source = self._price_sources.get(token_ref_display(key), "")
            if source:
                price_observations[key] = HistoricalPriceObservation(
                    price=candle.close,
                    timestamp=candle.timestamp,
                    source=source,
                    confidence=self._confidence_for_source(source),
                )
            if config.include_ohlcv:
                ohlcv[key] = candle
        metadata: dict[str, Any] = {"price_sources": dict(self._price_sources)}
        if len(self._sources) == 1:
            metadata["gmx_oracle"] = self._source.provenance
        else:
            metadata["gmx_oracles"] = [source.provenance for source in self._sources]
        state = MarketState(
            timestamp=current,
            prices=prices,
            ohlcv=ohlcv,
            chain=self._chain,
            block_number=None,
            gas_price_gwei=None,
            price_observations=price_observations,
            metadata=metadata,
        )
        aliases = {
            alias_symbol: normalize_token_key(self._chain, source.index_token)
            for source in self._sources
            if source.index_token is not None and (alias_symbol := source.alias_symbol()) is not None
        }
        if aliases:
            state.register_symbol_aliases(aliases)
        return state

    @staticmethod
    def _confidence_for_source(source: str) -> DataConfidence:
        """Map known price providers without promoting an unknown source."""
        source_lower = source.lower()
        if "gmx_oracle" in source_lower or "chainlink" in source_lower or "twap" in source_lower:
            return DataConfidence.HIGH
        if "coingecko" in source_lower:
            return DataConfidence.MEDIUM
        return DataConfidence.LOW

    def tick_close_history_before(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        interval_seconds: int,
    ) -> dict[str, list[Decimal]]:
        """Return venue closes on the simulation grid strictly before ``end_time``.

        This is the lazy-route counterpart of ``iterate()``. It deliberately
        samples each already-prepared native candle with ``candle_at`` so a 4h
        series under 1h ticks has the same flat-within-period shape as a market
        declared before tick 1. No observation at or after the active tick is
        exposed, preserving the normal loop's no-lookahead boundary.
        """
        if interval_seconds <= 0:
            raise ValueError("simulation interval_seconds must be positive")
        history: dict[str, list[Decimal]] = {}
        current = _utc(start_time)
        boundary = _utc(end_time)
        interval = timedelta(seconds=interval_seconds)
        while current < boundary:
            for source in self._sources:
                if source.index_token is None:
                    continue
                key = token_ref_display(normalize_token_key(self._chain, source.index_token))
                try:
                    candle = source.candle_at(current)
                except ValueError:
                    continue
                history.setdefault(key, []).append(candle.close)
            current += interval
        return history

    def overlay_market_state(self, state: MarketState) -> tuple[TokenRef, ...]:
        """Merge this provider's venue-owned index prices into an existing tick state.

        First-use discovery (an undeclared perp market named by an intent
        mid-run) cannot re-wrap the run's data provider, whose ``iterate()`` is
        already streaming. The engine instead keeps the lazily prepared
        provider aside and asks it to overlay each tick: the same
        address-native key, provenance and read alias ``_market_state_at``
        publishes. The venue-owned index series is authoritative — it replaces
        a spot-provider price for the same identity exactly as the declared
        path's ``_prefetch`` discards the fallback series for an owned key
        (blueprint 31: the GMX index asset never stitches or falls back) — so a
        market discovered at first use marks identically to one declared up
        front.
        """
        aliases: dict[str, Any] = {}
        provenance: list[dict[str, str | None]] = []
        overlaid: list[TokenRef] = []
        for source in self._sources:
            if source.index_token is None or source.timeframe is None:
                continue
            key = normalize_token_key(self._chain, source.index_token)
            try:
                candle = source.candle_at(state.timestamp)
            except ValueError:
                continue
            state.prices[key] = candle.close
            state.price_observations[key] = HistoricalPriceObservation(
                price=candle.close,
                timestamp=candle.timestamp,
                source="gmx_oracle_candles",
                confidence=DataConfidence.HIGH,
            )
            overlaid.append(key)
            alias_symbol = source.alias_symbol()
            if alias_symbol is not None:
                aliases[alias_symbol] = key
            provenance.append(source.provenance)
        if aliases:
            state.register_symbol_aliases(aliases)
        if provenance:
            state.metadata.setdefault("gmx_oracles_first_use", []).extend(provenance)
        return tuple(dict.fromkeys(overlaid))

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        self._cache = await self._prefetch(config)
        current = _utc(config.start_time)
        end = _utc(config.end_time)
        interval = timedelta(seconds=config.interval_seconds)
        cursors: dict[TokenRef, int] = dict.fromkeys(self._cache.data, -1)
        while current <= end:
            yield current, self._market_state_at(current=current, config=config, cursors=cursors)
            current += interval

    async def close(self) -> None:
        close = getattr(self._fallback, "close", None)
        if callable(close):
            result = close()
            if asyncio.iscoroutine(result):
                await result

    async def close_backtest_overlay(self) -> None:
        """GMX wrappers own no resources independently of their fallback."""

    @property
    def provider_name(self) -> str:
        return "gmx_oracle_routed"

    @property
    def supported_tokens(self) -> list[str]:
        tokens = set(getattr(self._fallback, "supported_tokens", []))
        for source in self._sources:
            if source.index_symbol:
                tokens.add(source.index_symbol)
            if source.index_token:
                tokens.add(source.index_token)
            alias = source.alias_symbol()
            if alias is not None:
                tokens.add(alias)
        return sorted(tokens)

    @property
    def supported_chains(self) -> list[str]:
        return [self._chain]

    @property
    def price_provenance(self) -> dict[str, Any]:
        if len(self._sources) == 1:
            return self._source.provenance
        return {
            "source": "gmx_oracle_candles",
            "venue": self._source.venue,
            "chain": self._chain,
            "timeframe": self._source.timeframe,
            "fidelity": "index_mark_ohlc_without_execution_spread_or_price_impact",
            "targets": [source.provenance for source in self._sources],
        }

    @property
    def price_history_target(self) -> tuple[str, str, str]:
        return self._source.venue, self._chain, self._source.requested_market

    @property
    def price_history_targets(self) -> tuple[tuple[str, str, str], ...]:
        return tuple((source.venue, self._chain, source.requested_market) for source in self._sources)

    @property
    def resolved_price_history_markets(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (source.resolved_market or source.requested_market, source.market_token or "") for source in self._sources
        )

    @property
    def required_price_tokens(self) -> tuple[TokenRef, ...]:
        """Verified dynamic index identity the engine must include in its data plane."""
        return tuple(
            dict.fromkeys(
                normalize_token_key(self._chain, source.index_token)
                for source in self._sources
                if source.index_token is not None
            )
        )


__all__ = ["GMXOracleDataProvider", "GMXPriceHistoryCoverageError"]
