"""Aster Pro funding-rate reads for the gateway funding lanes (gateway-side only).

Aster Pro speaks the Binance futures dialect over public, unsigned endpoints:

* ``GET /fapi/v3/premiumIndex?symbol=`` — ``lastFundingRate`` is the rate that
  will settle at ``nextFundingTime`` (it moves with the premium until then), so
  it is the live rate, expressed per funding interval.
* ``GET /fapi/v3/fundingInfo?symbol=`` — ``fundingIntervalHours`` for the
  symbol. Intervals differ per symbol (1, 2, 4 or 8 hours) and Aster can change
  them, so the interval is read on every request rather than assumed.
* ``GET /fapi/v3/fundingRate?symbol=&startTime=&endTime=&limit=`` — settled
  rates, one row per settlement, each per funding interval.

The gateway funding interface is per hour (``rate_hourly``), so every rate is
divided by its interval in hours. Binance-dialect funding is symmetric: a
positive rate means longs pay shorts exactly that rate, so the signed per-side
payment rates are ``long = -rate`` and ``short = +rate``.

Every failure — transport, non-200, unknown symbol, malformed or non-finite
field — raises; nothing here substitutes a default rate.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp

from almanak.connectors.aster_perps.gateway.api_client import DEFAULT_BASE_URL
from almanak.connectors.aster_perps.markets import to_symbol

logger = logging.getLogger(__name__)

VENUE = "aster_perps"
PREMIUM_INDEX_PATH = "/fapi/v3/premiumIndex"
FUNDING_INFO_PATH = "/fapi/v3/fundingInfo"
FUNDING_HISTORY_PATH = "/fapi/v3/fundingRate"

# Aster serves at most 1000 settlement rows per request.
HISTORY_PAGE_LIMIT = 1000
# 1000 rows x 1h interval is ~41 days per page; 40 pages covers >4 years even
# for hourly symbols and bounds a runaway pagination loop.
HISTORY_MAX_PAGES = 40
# Intervals Aster has published; a settlement spacing outside this set is a
# missing row or a schedule change, not an interval.
KNOWN_INTERVAL_HOURS = frozenset({1, 2, 4, 8})
_HOURS_PER_YEAR = Decimal(8760)
_MS_PER_HOUR = 3_600_000


class AsterFundingUnavailable(Exception):
    """The Aster funding read failed; the rate is unknown (never zero)."""


class AsterFundingRateLimited(AsterFundingUnavailable):
    """Aster answered HTTP 429."""

    def __init__(self, message: str, retry_after: float | None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class AsterUnknownMarket(ValueError):
    """The market does not map to a listed Aster Pro symbol."""


def base_url_from_settings(settings: Any) -> str:
    """The Aster base URL the trading client uses (``ALMANAK_GATEWAY_ASTER_PERPS_BASE_URL``)."""
    configured = getattr(settings, "aster_perps_base_url", None)
    return (configured or DEFAULT_BASE_URL).rstrip("/")


def resolve_symbol(market: str) -> str:
    """Aster symbol for a funding market key (``ETH-USD`` -> ``ETHUSDT``)."""
    try:
        return to_symbol(market)
    except ValueError as exc:
        raise AsterUnknownMarket(str(exc)) from exc


def _decimal(payload: dict[str, Any], field: str, *, positive: bool = False) -> Decimal:
    raw = payload.get(field)
    if raw is None or raw == "" or isinstance(raw, bool):
        raise AsterFundingUnavailable(f"Aster response is missing {field!r}")
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise AsterFundingUnavailable(f"Aster {field!r} is not a number: {raw!r}") from exc
    if not value.is_finite():
        raise AsterFundingUnavailable(f"Aster {field!r} is not finite: {raw!r}")
    if positive and value <= 0:
        raise AsterFundingUnavailable(f"Aster {field!r} must be positive, got {raw!r}")
    return value


def _epoch_ms(payload: dict[str, Any], field: str) -> int:
    raw = payload.get(field)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise AsterFundingUnavailable(f"Aster {field!r} must be a positive epoch-ms integer, got {raw!r}")
    return raw


def parse_interval_hours(payload: Any, symbol: str) -> int:
    """``fundingIntervalHours`` for ``symbol`` from a ``fundingInfo`` response."""
    rows = payload if isinstance(payload, list) else [payload]
    row = next((r for r in rows if isinstance(r, dict) and r.get("symbol") == symbol), None)
    if row is None:
        raise AsterUnknownMarket(f"Aster fundingInfo has no entry for {symbol}")
    hours = row.get("fundingIntervalHours")
    if isinstance(hours, bool) or not isinstance(hours, int) or hours <= 0:
        raise AsterFundingUnavailable(f"Aster fundingIntervalHours for {symbol} is invalid: {hours!r}")
    return hours


def parse_premium_index(payload: Any, symbol: str, interval_hours: int, market: str) -> Any:
    """Build the gateway ``FundingRateData`` from a ``premiumIndex`` response."""
    from almanak.gateway.services.funding_rate_service import FundingRateData

    if not isinstance(payload, dict) or payload.get("symbol") != symbol:
        raise AsterFundingUnavailable(f"Aster premiumIndex did not return {symbol}: {str(payload)[:200]}")
    rate_per_interval = _decimal(payload, "lastFundingRate")
    rate_hourly = rate_per_interval / Decimal(interval_hours)
    return FundingRateData(
        venue=VENUE,
        market=market,
        rate_hourly=rate_hourly,
        long_rate_hourly=-rate_hourly,
        short_rate_hourly=rate_hourly,
        open_interest_long=None,
        open_interest_short=None,
        mark_price=_decimal(payload, "markPrice", positive=True),
        index_price=_decimal(payload, "indexPrice", positive=True),
        next_funding_time=datetime.fromtimestamp(_epoch_ms(payload, "nextFundingTime") / 1000, tz=UTC),
        is_live_data=True,
        observed_at=datetime.fromtimestamp(_epoch_ms(payload, "time") / 1000, tz=UTC),
    )


def _retry_after(response: aiohttp.ClientResponse) -> float | None:
    raw = response.headers.get("Retry-After") if response.headers else None
    try:
        value = float(str(raw).strip()) if raw else None
    except ValueError:
        return None
    return value if value is not None and value >= 0 else None


Throttle = Callable[[], Awaitable[Any]] | None


async def get_json(
    session: aiohttp.ClientSession, base_url: str, path: str, params: dict[str, str], throttle: Throttle = None
) -> Any:
    """GET a public Aster endpoint; any non-200 or undecodable body raises.

    ``throttle`` is awaited before every request, so a caller's budget counts
    each upstream call (interval lookup, every page), not each operation.
    """
    if throttle is not None:
        await throttle()
    try:
        async with session.get(f"{base_url}{path}", params=params) as response:
            text = await response.text()
            if response.status == 429:
                raise AsterFundingRateLimited(f"Aster {path}: HTTP 429 {text[:200]}", _retry_after(response))
            if response.status != 200:
                if '"code":-1121' in text.replace(" ", ""):
                    raise AsterUnknownMarket(f"Aster {path}: invalid symbol {params.get('symbol')!r}")
                raise AsterFundingUnavailable(f"Aster {path}: HTTP {response.status} {text[:200]}")
            try:
                return await response.json(content_type=None)
            except ValueError as exc:
                raise AsterFundingUnavailable(f"Aster {path}: non-JSON body {text[:200]}") from exc
    except (TimeoutError, aiohttp.ClientError) as exc:
        raise AsterFundingUnavailable(f"Aster {path}: transport failure ({exc})") from exc


async def fetch_interval_hours(
    session: aiohttp.ClientSession, base_url: str, symbol: str, throttle: Throttle = None
) -> int:
    payload = await get_json(session, base_url, FUNDING_INFO_PATH, {"symbol": symbol}, throttle)
    return parse_interval_hours(payload, symbol)


async def fetch_live_funding_rate(
    session: aiohttp.ClientSession, base_url: str, market: str, throttle: Throttle = None
) -> Any:
    """Live Aster funding for ``market`` as gateway ``FundingRateData`` (per hour)."""
    symbol = resolve_symbol(market)
    premium, interval_hours = await asyncio.gather(
        get_json(session, base_url, PREMIUM_INDEX_PATH, {"symbol": symbol}, throttle),
        fetch_interval_hours(session, base_url, symbol, throttle),
    )
    return parse_premium_index(premium, symbol, interval_hours, market)


def history_points(
    rows: list[Any],
    *,
    current_interval_hours: int,
    start_ts: int,
    end_ts: int,
) -> list[Any]:
    """Settlement rows -> ascending ``FundingRatePoint`` rows (per hour).

    A settled rate covers the spacing since the previous settlement when that
    spacing is one of Aster's published intervals. A missing row doubles the
    spacing over the gap, which can collide with a published interval, so a
    spacing is taken only when it agrees with the spacing before it (or there
    is none) or does not exceed the spacing after it (the newest row compares
    with the symbol's current interval): a schedule change passes, a gap does
    not. A gap row takes the spacing after it; anything else falls back to the
    symbol's current interval. Rows outside
    ``[start_ts, end_ts]`` are dropped only after they have served as a
    predecessor. A malformed row raises rather than being skipped: a silent gap
    would mis-scale its successor.
    """
    from almanak.gateway.services.rate_history_service import FundingRatePoint

    parsed: list[tuple[int, Decimal]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise AsterFundingUnavailable(f"Aster fundingRate row is not an object: {row!r}")
        parsed.append((_epoch_ms(row, "fundingTime"), _decimal(row, "fundingRate")))
    parsed.sort(key=lambda item: item[0])

    spacings = [
        round((later - earlier) / _MS_PER_HOUR) for (earlier, _), (later, _) in zip(parsed, parsed[1:], strict=False)
    ]
    points: list[Any] = []
    for index, (funding_ms, rate) in enumerate(parsed):
        before = spacings[index - 1] if index >= 1 else None
        earlier = spacings[index - 2] if index >= 2 else None
        after = spacings[index] if index < len(spacings) else current_interval_hours
        if before not in KNOWN_INTERVAL_HOURS:
            interval = current_interval_hours
        elif earlier is None or before == earlier or before <= after:
            interval = before
        else:
            interval = after if after in KNOWN_INTERVAL_HOURS else current_interval_hours
        timestamp = funding_ms // 1000
        if not start_ts <= timestamp <= end_ts:
            continue
        hourly = rate / Decimal(interval)
        points.append(
            FundingRatePoint(
                timestamp=timestamp,
                rate_hourly=hourly,
                rate_annualized=hourly * _HOURS_PER_YEAR,
                long_rate_hourly=-hourly,
                short_rate_hourly=hourly,
            )
        )
    return points


async def fetch_settlement_rows(
    session: aiohttp.ClientSession,
    base_url: str,
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
    throttle: Throttle = None,
) -> list[Any]:
    """Every settlement row in ``[start_ms, end_ms]``, following Aster's 1000-row pages."""
    rows: list[Any] = []
    cursor = start_ms
    for _ in range(HISTORY_MAX_PAGES):
        page = await get_json(
            session,
            base_url,
            FUNDING_HISTORY_PATH,
            {"symbol": symbol, "startTime": str(cursor), "endTime": str(end_ms), "limit": str(HISTORY_PAGE_LIMIT)},
            throttle,
        )
        if not isinstance(page, list):
            raise AsterFundingUnavailable(f"Aster fundingRate returned a non-list body for {symbol}")
        rows.extend(page)
        if len(page) < HISTORY_PAGE_LIMIT:
            return rows
        last = page[-1].get("fundingTime") if isinstance(page[-1], dict) else None
        if isinstance(last, bool) or not isinstance(last, int) or last < cursor:
            raise AsterFundingUnavailable(f"Aster fundingRate pagination did not advance for {symbol}")
        cursor = last + 1
    raise AsterFundingUnavailable(f"Aster fundingRate history for {symbol} exceeds {HISTORY_MAX_PAGES} pages")


async def fetch_funding_history(
    session: aiohttp.ClientSession,
    base_url: str,
    market: str,
    *,
    start_ts: int,
    end_ts: int,
    throttle: Throttle = None,
) -> list[Any]:
    """Settled Aster funding in ``[start_ts, end_ts]`` as per-hour ``FundingRatePoint`` rows."""
    symbol = resolve_symbol(market)
    current_interval = await fetch_interval_hours(session, base_url, symbol, throttle)
    # Start one maximal interval early so the first in-window settlement has a
    # predecessor to measure its spacing from.
    lookback_ms = max(KNOWN_INTERVAL_HOURS) * _MS_PER_HOUR
    rows = await fetch_settlement_rows(
        session,
        base_url,
        symbol,
        start_ms=max(0, start_ts * 1000 - lookback_ms),
        end_ms=end_ts * 1000,
        throttle=throttle,
    )
    points = history_points(rows, current_interval_hours=current_interval, start_ts=start_ts, end_ts=end_ts)
    if not points:
        raise AsterFundingUnavailable(f"Aster has no {symbol} funding settlements in [{start_ts}, {end_ts}]")
    return points


__all__ = [
    "AsterFundingRateLimited",
    "AsterFundingUnavailable",
    "AsterUnknownMarket",
    "VENUE",
    "base_url_from_settings",
    "fetch_funding_history",
    "fetch_live_funding_rate",
    "history_points",
    "parse_interval_hours",
    "parse_premium_index",
    "resolve_symbol",
]
