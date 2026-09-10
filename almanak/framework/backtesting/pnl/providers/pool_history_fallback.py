"""Measured pool-history fallback for the backtest volume + liquidity lanes (ALM-2940).

When a backtest's PRIMARY data lane fails — the DEX-volume subgraph lane
(``GetDexVolumeHistory``) for fee accrual, or the liquidity subgraph lane
for TVL/slippage — the engine historically dropped straight to fabricated
values (the opt-in ``volume_multiplier`` heuristic, the zero-depth
liquidity fallback), degrading the whole backtest. This module inserts a
MEASURED middle rung: the gateway's ``PoolHistoryService`` ladder
(TheGraph -> DefiLlama -> CoinGecko Onchain), which serves real aggregator
data for pools whose native subgraphs are dead (aerodrome classic) or
stale (traderjoe_v2).

Field routing (one daily snapshot per pool per day):

* ``tvl`` comes from the 1d series (DefiLlama daily TVL when the pool's
  TheGraph deployment is dead — same ``tvlUSD`` semantics as the measured
  liquidity lane's subgraph queries).
* ``fee_apy`` comes from the 1d series when the provider measured base fee
  yield (DefiLlama ``apyBase``). Reward / incentive APY is never substituted.
* ``volume_24h`` comes from the 1d row when the serving provider measured
  it; otherwise from the 1h series summed over the UTC day (the 1d
  dispatch order serves DefiLlama first, which carries TVL but no
  historical volume — the 1h order skips the daily-only DefiLlama and
  reaches CoinGecko Onchain OHLCV directly). A partially-measured day
  (fewer than ``_MIN_HOURLY_BARS_FOR_DAILY_VOLUME`` bars) is a MISS, not a
  floor estimate — summing 3 bars and calling it a day's volume would
  understate fees while carrying a measured-data label.

Callers label this data MEDIUM confidence with per-field source strings
(``gateway_pool_history:<provider>``). Empty != Zero holds throughout:
``None`` means unmeasured, never zero.

The gateway ships with ``PoolHistoryService`` DISABLED
(``ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true`` enables it). Capabilities are
memoized per ``(protocol, chain, resolution)``. Served complete UTC-day windows
retain their rows and source in a bounded cache, so one failed resolution
retries without refetching the successful resolution. Both caches belong to
one data-broker run; later runs recheck gateway capabilities. Repeated transport
failures (2 consecutive) pause the fallback for a bounded cooldown — mirrors
the perp funding providers' streak memo without permanently poisoning a
long-lived runner after a transient outage.
"""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

from almanak.framework.data.interfaces import DataSourceUnavailable

if TYPE_CHECKING:
    from almanak.framework.data.pools.history import PoolSnapshot

logger = logging.getLogger(__name__)

#: Minimum measured hourly bars for an hourly-summed daily volume. A full
#: past UTC day has 24; below this floor the sum is a partial-coverage
#: floor, not a day's volume, and the lookup reports a miss instead.
_MIN_HOURLY_BARS_FOR_DAILY_VOLUME = 18

#: Consecutive transport-failure ceiling before the fallback PAUSES
#: (mirrors the perp funding providers' 2-strike memo; reset on any success).
_MAX_TRANSPORT_FAILURE_STREAK = 2

#: A transport pause expires so a recovered gateway can be observed without
#: restarting the provider. Continued failures establish a fresh pause.
_TRANSPORT_DISABLE_COOLDOWN = timedelta(seconds=60)

_MAX_RESOLUTION_CACHE_ENTRIES = 2048

#: Source-label prefix; callers compose ``gateway_pool_history:<provider>``.
POOL_HISTORY_SOURCE_PREFIX = "gateway_pool_history"
ProviderFailureKind = Literal["unsupported_capability", "transient_provider_failure", "unknown_provider_failure"]


@dataclass(frozen=True)
class DailyPoolHistory:
    """One pool-day of measured ladder data with per-field provenance.

    ``None`` fields are unmeasured (Empty != Zero). Per-field source values
    name the serving gateway provider (e.g.
    ``"defillama"``, ``"coingecko_onchain"``) and are ``""`` when the field is
    unmeasured.
    """

    tvl: Decimal | None
    tvl_source: str
    volume_24h: Decimal | None
    volume_source: str
    fee_apy: Decimal | None = None
    fee_apy_source: str = ""


@dataclass(frozen=True)
class DailyPoolHistoryOutcome:
    """A ladder lookup plus whether a miss is safe to memoize.

    ``cacheable=False`` means at least one required fetch failed or the day is
    still provisional. Consumers must not turn such a miss into a permanent
    negative cache entry: a later lookup may recover additional measurements.
    """

    history: DailyPoolHistory | None
    cacheable: bool
    unavailable_kind: ProviderFailureKind | None = None
    field_unavailability: tuple[tuple[str, ProviderFailureKind | None], ...] = ()


class PoolHistoryCapabilityUnsupported(DataSourceUnavailable):
    code = "unsupported_capability"


class PoolHistoryProviderTransient(DataSourceUnavailable):
    code = "transient_provider_failure"


class PoolHistoryProviderUnclassified(DataSourceUnavailable):
    code = "unknown_provider_failure"


class PoolHistoryFallback:
    """Shared helper over the gateway ``GetPoolHistory`` RPC.

    Thread-safe; one instance serves both the LP adapter's (sync) volume
    path and the liquidity provider's (async, via
    ``run_sync_gateway_call``) TVL path. Definitive lookups cache per
    ``(pool, chain, protocol, day)`` including definitive misses. Retryable or
    provisional misses remain uncached so a later lookup can recover.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._daily_cache: dict[tuple[str, str, str, date], DailyPoolHistory | None] = {}
        # Single-flight (ALM-2956): concurrent callers for one pool-day share
        # one fetch. The volume (sync) and liquidity (async-executor) paths
        # can race on the same day; without coalescing both spend the fetch.
        # Each flight is {"event", "outcome"}; waiters keep a local reference,
        # so popping the flight leaves nothing retained per pool-day.
        self._inflight: dict[tuple[str, str, str, date], dict[str, Any]] = {}
        self._transport_failure_streak = 0
        #: Wall-clock deadline until which the transport breaker keeps the
        #: ladder paused; ``None`` when not paused. An expired pause rechecks
        #: the channel instead of permanently disabling the provider.
        self._transport_disabled_until: datetime | None = None
        self._unsupported_pairs: set[tuple[str, str]] = set()
        self._unsupported_resolutions: set[tuple[str, str, str]] = set()
        self._disabled_service_logged = False
        self._resolution_cache: OrderedDict[
            tuple[str, str, str, datetime, datetime, str], tuple[tuple[PoolSnapshot, ...], str]
        ] = OrderedDict()

    # -- Public API ---------------------------------------------------------

    def daily_history(
        self,
        *,
        pool_address: str,
        chain: str,
        protocol: str,
        day: date,
    ) -> DailyPoolHistory | None:
        """Fetch one pool-day of measured ladder data (blocking).

        Returns ``None`` on any miss — service disabled, unsupported pair,
        gateway unreachable, or no provider measured any tracked field for the
        day. Never raises: the fallback rescues a lane that has ALREADY
        failed, so its own failures must degrade to the caller's existing
        (heuristic / zero-depth) path, not replace one error with another.
        """
        return self.daily_history_outcome(
            pool_address=pool_address,
            chain=chain,
            protocol=protocol,
            day=day,
        ).history

    def daily_history_outcome(
        self,
        *,
        pool_address: str,
        chain: str,
        protocol: str,
        day: date,
    ) -> DailyPoolHistoryOutcome:
        """Fetch a pool-day and preserve definitive-vs-retryable miss state."""
        # Cache keys preserve the caller's address casing: the gateway
        # validator normalizes chain-aware (EVM lowercases, Solana-family
        # base58 preserves case), and lowercasing here would corrupt a
        # Solana address. Mixed-case EVM callers at worst duplicate a
        # cache entry.
        pool_key = (pool_address, chain, protocol, day)
        while True:
            with self._lock:
                if (protocol, chain) in self._unsupported_pairs:
                    return DailyPoolHistoryOutcome(
                        history=None, cacheable=True, unavailable_kind="unsupported_capability"
                    )
                if pool_key in self._daily_cache:
                    return DailyPoolHistoryOutcome(history=self._daily_cache[pool_key], cacheable=True)
                flight = self._inflight.get(pool_key)
                if flight is None:
                    flight = {"event": threading.Event(), "outcome": None}
                    self._inflight[pool_key] = flight
                    break  # this caller owns the fetch
            # Another caller is fetching this exact pool-day: share its
            # outcome instead of double-spending the fetch (ALM-2956).
            flight["event"].wait()
            shared = flight["outcome"]
            if shared is not None:
                return shared
            # Owner died without recording (process-level error): loop and
            # re-evaluate gates/cache; at worst this caller fetches itself.

        try:
            outcome = self._fetch_daily(pool_address=pool_address, chain=chain, protocol=protocol, day=day)
            if outcome.cacheable:
                # Only DEFINITIVE outcomes are memoized: a served day with these
                # exact measurements (or a served both-legs miss). A day whose
                # fetch FAILED (gateway blip, empty rate bucket) stays uncached
                # so a later lookup retries once the condition clears —
                # permanently caching a TVL-only partial would strand measured
                # volume for the rest of the run (Codex PR review, #3283).
                with self._lock:
                    self._daily_cache[pool_key] = outcome.history
            flight["outcome"] = outcome
            return outcome
        finally:
            with self._lock:
                self._inflight.pop(pool_key, None)
            flight["event"].set()

    def required_daily_history(
        self,
        *,
        pool_address: str,
        chain: str,
        protocol: str,
        day: date,
        required_fields: frozenset[str] = frozenset({"tvl", "volume_24h", "fee_apy"}),
    ) -> DailyPoolHistory | None:
        """Preserve capability and retryable failures for mandatory guard reads."""
        unknown = required_fields - {"tvl", "volume_24h", "fee_apy"}
        if unknown:
            raise ValueError(f"Unknown required pool-history fields: {sorted(unknown)}")
        outcome = self.daily_history_outcome(pool_address=pool_address, chain=chain, protocol=protocol, day=day)
        failures = dict(outcome.field_unavailability)
        for field_name in sorted(required_fields):
            if outcome.history is not None and getattr(outcome.history, field_name) is not None:
                continue
            failure = failures.get(field_name, outcome.unavailable_kind)
            if failure == "unsupported_capability":
                raise PoolHistoryCapabilityUnsupported(
                    "pool_history", "Historical pool-history capability is unavailable"
                )
            if failure == "transient_provider_failure":
                raise PoolHistoryProviderTransient(
                    "pool_history", f"Historical pool-history provider failed while reading {field_name}"
                )
            if failure == "unknown_provider_failure":
                raise PoolHistoryProviderUnclassified(
                    "pool_history", f"Historical pool-history read of {field_name} failed without a classified cause"
                )
        return outcome.history

    async def daily_history_async(
        self,
        *,
        pool_address: str,
        chain: str,
        protocol: str,
        day: date,
    ) -> DailyPoolHistory | None:
        """Async form of :meth:`daily_history` (runs on the gateway executor)."""
        from .perp._gateway_history import run_sync_gateway_call

        return await run_sync_gateway_call(
            self.daily_history,
            pool_address=pool_address,
            chain=chain,
            protocol=protocol,
            day=day,
        )

    async def daily_history_outcome_async(
        self,
        *,
        pool_address: str,
        chain: str,
        protocol: str,
        day: date,
    ) -> DailyPoolHistoryOutcome:
        """Async form of :meth:`daily_history_outcome`."""
        from .perp._gateway_history import run_sync_gateway_call

        return await run_sync_gateway_call(
            self.daily_history_outcome,
            pool_address=pool_address,
            chain=chain,
            protocol=protocol,
            day=day,
        )

    # -- Fetch + error taxonomy ----------------------------------------------

    def _fetch_daily(
        self,
        *,
        pool_address: str,
        chain: str,
        protocol: str,
        day: date,
    ) -> DailyPoolHistoryOutcome:
        """Fetch one pool-day, retaining failures for each independent field.

        An outcome is cacheable only when every leg that produced a miss was
        actually SERVED (a definitive no-data answer). A failed fetch —
        gateway blip, empty rate bucket — is retryable and must not memoize.
        """
        day_start = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
        now = datetime.now(UTC)
        full_day_end = day_start + timedelta(days=1)
        day_end = min(full_day_end, now)
        # A day is DEFINITIVELY measurable only once it is fully elapsed. The
        # current (in-progress) day and any future day are provisional: more
        # hourly bars / a higher daily volume will appear, so a miss OR a
        # served-but-partial reading must stay retryable, never permanently
        # cached — else a later job (or a later tick, for a window ending
        # "today") can never discover the completed day (Codex PR review,
        # #3283). Past complete days keep definitive caching.
        day_fully_elapsed = now >= full_day_end
        if day_end <= day_start:
            return DailyPoolHistoryOutcome(history=None, cacheable=False)

        daily_rows, daily_source, daily_failure = self._get_history(
            pool_address=pool_address,
            chain=chain,
            protocol=protocol,
            start=day_start,
            end=day_end,
            resolution="1d",
        )
        # A failed 1d fetch must NOT suppress the independent 1h volume lane
        # (CodeRabbit #3283): continue with no daily rows so the hourly sum can
        # still recover measured volume, but keep the day non-cacheable because
        # its TVL leg stays retryable.
        daily_definitive = daily_rows is not None
        if daily_rows is None:
            daily_rows = []

        tvl: Decimal | None = None
        tvl_source = ""
        volume: Decimal | None = None
        volume_source = ""
        fee_apy: Decimal | None = None
        fee_apy_source = ""
        for snap in daily_rows:
            # Date comparison rather than exact epoch equality: the reader
            # yields tz-aware UTC rows aligned to the 1d grid, and matching
            # on the UTC date is immune to any future naive/aware or
            # alignment drift (gemini PR review, #3283).
            if snap.timestamp.date() != day:
                continue
            if snap.tvl is not None:
                tvl = snap.tvl
                tvl_source = daily_source
            if snap.volume_24h is not None:
                volume = snap.volume_24h
                volume_source = daily_source
            if snap.fee_apy is not None:
                fee_apy = snap.fee_apy
                fee_apy_source = daily_source
            break

        volume_definitive = volume is not None
        volume_failure: ProviderFailureKind | None = None
        if volume is None:
            volume, volume_source, volume_definitive, volume_failure = self._hourly_volume_sum(
                pool_address=pool_address,
                chain=chain,
                protocol=protocol,
                start=day_start,
                end=day_end,
            )
            if volume is None:
                volume_failure = self._merge_volume_failure(daily_failure, volume_failure)
                with self._lock:
                    if daily_failure == volume_failure == "unsupported_capability":
                        self._unsupported_pairs.add((protocol, chain))
                    else:
                        self._unsupported_pairs.discard((protocol, chain))

        # The current (in-progress) day's volume is inherently incomplete: both
        # the daily bar and the hourly sum cover only the hours elapsed so far.
        # Serving that partial as the day's MEASURED volume would understate
        # fees for a window ending "today" (Codex PR review, #3283). TVL is a
        # point-in-time reserve snapshot and stays valid; only volume is nulled
        # to unmeasured, so accrual falls to its own fallback for the in-progress
        # day instead of trusting a low partial. (Not-yet-cacheable either way —
        # day_fully_elapsed already gates the memo below.)
        if not day_fully_elapsed:
            volume, volume_source, volume_definitive = None, "", False

        history = None
        if tvl is not None or volume is not None or fee_apy is not None:
            history = DailyPoolHistory(
                tvl=tvl,
                tvl_source=tvl_source,
                volume_24h=volume,
                volume_source=volume_source,
                fee_apy=fee_apy,
                fee_apy_source=fee_apy_source,
            )
        return DailyPoolHistoryOutcome(
            history=history,
            cacheable=daily_definitive and (volume is not None or volume_definitive) and day_fully_elapsed,
            unavailable_kind=daily_failure or volume_failure,
            field_unavailability=(("tvl", daily_failure), ("fee_apy", daily_failure), ("volume_24h", volume_failure)),
        )

    @staticmethod
    def _merge_volume_failure(
        daily_failure: ProviderFailureKind | None,
        hourly_failure: ProviderFailureKind | None,
    ) -> ProviderFailureKind | None:
        """Both sources can measure volume, so a served miss cannot hide a failed source.

        An unclassified failure prevents a more specific verdict. A retryable
        source remains a recovery route even if the other is unsupported. Only
        two unsupported sources establish that neither route is available.
        """
        failures = (daily_failure, hourly_failure)
        if "unknown_provider_failure" in failures:
            return "unknown_provider_failure"
        if "transient_provider_failure" in failures:
            return "transient_provider_failure"
        if daily_failure == hourly_failure == "unsupported_capability":
            return "unsupported_capability"
        return None

    def _hourly_volume_sum(
        self,
        *,
        pool_address: str,
        chain: str,
        protocol: str,
        start: datetime,
        end: datetime,
    ) -> tuple[Decimal | None, str, bool, ProviderFailureKind | None]:
        """Sum measured 1h bars over the day; miss below the coverage floor.

        The third element is whether the answer is DEFINITIVE (the fetch was
        served) — a failed fetch returns ``(None, "", False)`` so the caller
        treats the pool-day as retryable instead of memoizing the miss.
        The fourth element preserves the failed fetch's classification.
        """
        hourly_rows, hourly_source, failure = self._get_history(
            pool_address=pool_address,
            chain=chain,
            protocol=protocol,
            start=start,
            end=end,
            resolution="1h",
        )
        if hourly_rows is None:
            return None, "", False, failure
        measured = [snap.volume_24h for snap in hourly_rows if snap.volume_24h is not None]
        if len(measured) < _MIN_HOURLY_BARS_FOR_DAILY_VOLUME:
            if measured:
                logger.debug(
                    "Pool-history fallback: only %d/24 measured hourly bars for %s/%s on %s — "
                    "below the %d-bar floor, reporting volume as unmeasured",
                    len(measured),
                    chain,
                    pool_address[:10],
                    start.date(),
                    _MIN_HOURLY_BARS_FOR_DAILY_VOLUME,
                )
            return None, "", True, None
        return sum(measured, Decimal("0")), hourly_source, True, None

    def _get_history(
        self,
        *,
        pool_address: str,
        chain: str,
        protocol: str,
        start: datetime,
        end: datetime,
        resolution: str,
    ) -> tuple[list[PoolSnapshot] | None, str, ProviderFailureKind | None]:
        """Reuse served complete-day windows while failed resolutions stay retryable."""
        from almanak.framework.data.interfaces import DataSourceUnavailable

        capability_key = (protocol, chain, resolution)
        window_key = (pool_address, chain, protocol, start, end, resolution)
        with self._lock:
            cached = self._resolution_cache.get(window_key)
            if cached is not None:
                self._resolution_cache.move_to_end(window_key)
                rows, source = cached
                return list(rows), source, None
            if capability_key in self._unsupported_resolutions:
                return None, "", "unsupported_capability"
            if self._transport_disabled_until is not None:
                if datetime.now(UTC) < self._transport_disabled_until:
                    return None, "", "transient_provider_failure"
                self._transport_disabled_until = None
                self._transport_failure_streak = 0

        try:
            from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
                get_connected_gateway_client,
            )
            from almanak.framework.data.pools.history import PoolHistoryReader

            client, _ = get_connected_gateway_client()
            reader = PoolHistoryReader(gateway_client=client)
            envelope = reader.get_pool_history(
                pool_address=pool_address,
                chain=chain,
                start_date=start,
                end_date=end,
                resolution=resolution,
                protocol=protocol,
            )
        except DataSourceUnavailable as exc:
            failure = self._classify_miss(str(exc), chain=chain, protocol=protocol, transport=bool(exc.transport))
            if failure == "unsupported_capability":
                with self._lock:
                    self._unsupported_resolutions.add(capability_key)
            return None, "", failure
        except Exception as exc:  # noqa: BLE001 — the fallback must never out-fail the failed primary lane
            logger.debug(
                "Pool-history fallback: unexpected error for %s/%s (%s): %s",
                chain,
                pool_address[:10],
                resolution,
                exc,
            )
            return None, "", "unknown_provider_failure"

        rows = tuple(envelope.value)
        source = envelope.meta.source or "gateway"
        utc_start = start.astimezone(UTC)
        complete_day = (
            start.tzinfo is not None
            and end.tzinfo is not None
            and utc_start == datetime.combine(utc_start.date(), datetime.min.time(), tzinfo=UTC)
            and end - start == timedelta(days=1)
            and end <= datetime.now(UTC)
        )
        with self._lock:
            self._transport_failure_streak = 0
            # A served resolution contradicts a pair-wide unsupported memo.
            self._unsupported_pairs.discard((protocol, chain))
            self._unsupported_resolutions.discard(capability_key)
            if complete_day:
                self._resolution_cache[window_key] = (rows, source)
                self._resolution_cache.move_to_end(window_key)
                while len(self._resolution_cache) > _MAX_RESOLUTION_CACHE_ENTRIES:
                    self._resolution_cache.popitem(last=False)
        return list(rows), source, None

    def _classify_miss(self, reason: str, *, chain: str, protocol: str, transport: bool = False) -> ProviderFailureKind:
        """Route a reader failure into the right memo (see module docstring).

        ``transport`` carries the exception's STRUCTURED classification
        (``DataSourceUnavailable.transport`` — set by
        ``get_connected_gateway_client`` for import/connect failures); the
        string markers below remain as an OR because the reader's own
        RpcError mapping does not populate the attribute.
        """
        lowered = reason.lower()
        if any(
            marker in lowered for marker in ("not yet enabled", "unsupported protocol", "unsupported (chain, protocol)")
        ):
            with self._lock:
                self._transport_failure_streak = 0
                log_disabled = "not yet enabled" in lowered and not self._disabled_service_logged
                if log_disabled:
                    self._disabled_service_logged = True
            if log_disabled:
                logger.info(
                    "Gateway PoolHistoryService is disabled. Set ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true "
                    "on the gateway to enable measured pool history, then start a new backtest run."
                )
            return "unsupported_capability"
        # Transport = the gateway CHANNEL failed, not a served miss. Covers the
        # reader's failure shapes: get_connected_gateway_client's import /
        # connect failures, the not-connected RuntimeError mapping, and
        # RpcError text carrying UNAVAILABLE / DEADLINE_EXCEEDED. A served
        # success=False envelope (e.g. an upstream provider's HTTP 503 quoted
        # in response.error) deliberately does NOT count — the gateway is
        # reachable, so keep asking for other pools/days.
        transport = transport or (
            "gateway client not connected" in lowered
            or "gateway connect failed" in lowered
            or "gateway client unavailable" in lowered
            or ("gateway error:" in lowered and ("unavailable" in lowered or "deadline" in lowered))
        )
        if transport:
            with self._lock:
                self._transport_failure_streak += 1
                streak = self._transport_failure_streak
                if streak >= _MAX_TRANSPORT_FAILURE_STREAK and self._transport_disabled_until is None:
                    self._transport_disabled_until = datetime.now(UTC) + _TRANSPORT_DISABLE_COOLDOWN
                    disable = True
                else:
                    disable = False
            if disable:
                logger.info(
                    "Pool-history fallback paused for %ds after %d consecutive gateway "
                    "transport failures (last: %s); it retries once the cooldown elapses",
                    int(_TRANSPORT_DISABLE_COOLDOWN.total_seconds()),
                    streak,
                    reason,
                )
            return "transient_provider_failure"
        # A served data-level miss (providers exhausted, not a channel failure)
        # also breaks a transport streak — the gateway is reachable.
        with self._lock:
            self._transport_failure_streak = 0
        logger.debug("Pool-history fallback miss: %s", reason)
        return "unknown_provider_failure"


_SINGLETON_LOCK = threading.Lock()
_SINGLETON: PoolHistoryFallback | None = None


def get_pool_history_fallback() -> PoolHistoryFallback:
    """Return the process-wide :class:`PoolHistoryFallback` instance."""
    global _SINGLETON
    with _SINGLETON_LOCK:
        if _SINGLETON is None:
            _SINGLETON = PoolHistoryFallback()
        return _SINGLETON


__all__ = [
    "POOL_HISTORY_SOURCE_PREFIX",
    "DailyPoolHistory",
    "DailyPoolHistoryOutcome",
    "PoolHistoryFallback",
    "get_pool_history_fallback",
]
