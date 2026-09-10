"""Exchange-session status from published calendars — shared by gateway and framework.

Session state is pure date arithmetic over the calendars bundled with
``pandas_market_calendars`` (regular session, holidays, early closes, DST,
scheduled lunch breaks and interruptions). No oracle, RPC, or network access is
involved, so the same evaluator is safe on both sides of the gateway boundary:
the gateway attaches a session status to reference prices, and
``MarketSnapshot.market_session`` exposes it to strategies evaluated at the
snapshot timestamp (deterministic on live chain, Anvil, and backtest ticks).

This module imports neither gateway internals nor framework models. Strategies
never embed their own calendar and never infer an open market from oracle
freshness. Unknown exchanges, empty schedules and calendar failures never
report OPEN (fail closed).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

logger = logging.getLogger(__name__)


class ReferenceMarketStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class MarketHoursObservation:
    status: ReferenceMarketStatus
    as_of: datetime
    source: str
    calendar: str = ""


def _normalize(as_of: datetime | None) -> datetime:
    now = as_of or datetime.now(UTC)
    return now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)


def _resolve_calendar_name(requested: str) -> str | None:
    """Match a requested exchange name against the library's registry, case-insensitively."""
    # Lazy import keeps the substantial pandas calendar registry off the
    # normal startup path; only session/reference users pay the load.
    import pandas_market_calendars as mcal

    wanted = requested.strip()
    if not wanted:
        return None
    names = mcal.get_calendar_names()
    if wanted in names:
        return wanted
    lowered = wanted.lower()
    return next((name for name in names if name.lower() == lowered), None)


def _is_in_session(calendar: object, schedule: object, now: datetime) -> bool:
    """Open-inclusive, close-exclusive, and closed during scheduled breaks/interruptions.

    Delegates to the library's ``open_at_time`` so lunch breaks (``break_start``
    / ``break_end``) and ``interruption_*`` columns are honoured without a second
    calendar implementation. The helper raises for an empty schedule and for
    timestamps outside the schedule's outer bounds; both are "no session" here.
    """
    import pandas as pd

    frame = pd.DataFrame(schedule)
    if frame.empty:
        return False
    stamp = pd.Timestamp(now)
    if stamp < frame["market_open"].iat[0] or stamp >= frame["market_close"].iat[-1]:
        return False
    return bool(calendar.open_at_time(frame, stamp, include_close=False, only_rth=True))  # type: ignore[attr-defined]


def exchange_market_status(exchange: str, *, as_of: datetime | None = None) -> MarketHoursObservation:
    """Return a fail-closed session status for any exchange calendar the library publishes.

    ``exchange`` is a ``pandas_market_calendars`` name such as ``"NYSE"``,
    ``"NASDAQ"``, ``"HKEX"`` or ``"CMEGlobex_Gold"`` (matched case-insensitively).
    Scheduled lunch breaks and interruptions count as CLOSED. Unknown names and
    calendar failures return ``UNKNOWN``; callers must treat that as closed for
    any decision that should only run inside the regular session.
    """
    now = _normalize(as_of)
    try:
        calendar_name = _resolve_calendar_name(exchange)
    except Exception as exc:  # noqa: BLE001 - UNKNOWN is the fail-closed contract
        logger.warning("Exchange calendar registry unavailable for %s: %s", exchange, type(exc).__name__)
        return MarketHoursObservation(ReferenceMarketStatus.UNKNOWN, now, "unsupported")
    if calendar_name is None:
        return MarketHoursObservation(ReferenceMarketStatus.UNKNOWN, now, "unsupported")

    source = f"pandas_market_calendars:{calendar_name}"
    try:
        import pandas_market_calendars as mcal

        calendar = mcal.get_calendar(calendar_name)
        # ``interruptions=True`` is required for declared halts to appear as
        # ``interruption_*`` columns; ``open_at_time`` only honours what the
        # schedule carries, and the default schedule omits them.
        schedule = calendar.schedule(
            start_date=(now - timedelta(days=2)).date(),
            end_date=(now + timedelta(days=1)).date(),
            interruptions=True,
        )
        is_open = _is_in_session(calendar, schedule, now)
    except Exception as exc:  # noqa: BLE001 - UNKNOWN is the fail-closed contract
        logger.warning("Exchange calendar failed for %s (%s): %s", exchange, calendar_name, type(exc).__name__)
        return MarketHoursObservation(ReferenceMarketStatus.UNKNOWN, now, source, calendar_name)

    status = ReferenceMarketStatus.OPEN if is_open else ReferenceMarketStatus.CLOSED
    return MarketHoursObservation(status, now, source, calendar_name)
