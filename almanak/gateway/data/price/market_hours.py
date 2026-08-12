"""Gateway-owned reference-market session status.

Market hours belong at the gateway perimeter: strategies receive a typed status
and never embed a local calendar or infer an open market from oracle freshness.
Unknown instruments and calendar failures return UNKNOWN (fail closed).
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


# Chainlink classifies XAU/USD as Precious_Metals. CME Globex Gold provides the
# conservative regular/holiday/early-close calendar for that reference market.
_CALENDAR_BY_PAIR = {"XAU/USD": "CMEGlobex_Gold"}


def reference_market_status(pair: str, *, as_of: datetime | None = None) -> MarketHoursObservation:
    """Return a fail-closed session status for a supported reference pair."""
    now = as_of or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    else:
        now = now.astimezone(UTC)

    calendar_name = _CALENDAR_BY_PAIR.get(pair.strip().upper())
    if calendar_name is None:
        return MarketHoursObservation(ReferenceMarketStatus.UNKNOWN, now, "unsupported")

    source = f"pandas_market_calendars:{calendar_name}"
    try:
        # Lazy import keeps the substantial pandas calendar registry off the
        # normal gateway startup path; only reference-price users pay the load.
        import pandas_market_calendars as mcal

        calendar = mcal.get_calendar(calendar_name)
        schedule = calendar.schedule(
            start_date=(now - timedelta(days=2)).date(),
            end_date=(now + timedelta(days=1)).date(),
        )
        is_open = any(row.market_open <= now < row.market_close for row in schedule.itertuples())
    except Exception as exc:  # noqa: BLE001 - UNKNOWN is the fail-closed contract
        logger.warning("Reference-market calendar failed for %s (%s): %s", pair, calendar_name, type(exc).__name__)
        return MarketHoursObservation(ReferenceMarketStatus.UNKNOWN, now, source)

    return MarketHoursObservation(ReferenceMarketStatus.OPEN if is_open else ReferenceMarketStatus.CLOSED, now, source)
