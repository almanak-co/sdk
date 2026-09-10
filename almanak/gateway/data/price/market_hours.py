"""Gateway-owned reference-market session status.

Market hours belong at the gateway perimeter: strategies receive a typed status
and never embed a local calendar or infer an open market from oracle freshness.
The calendar evaluator itself lives in :mod:`almanak.core.market_sessions` so
that ``MarketSnapshot.market_session`` can evaluate the same calendars at the
snapshot timestamp without importing gateway internals. This module keeps the
gateway-only reference-pair → calendar mapping and delegates to that evaluator.
Unknown instruments and calendar failures return UNKNOWN (fail closed).
"""

from __future__ import annotations

from datetime import datetime

from almanak.core.market_sessions import (
    MarketHoursObservation,
    ReferenceMarketStatus,
    _normalize,
    exchange_market_status,
)

__all__ = [
    "MarketHoursObservation",
    "ReferenceMarketStatus",
    "exchange_market_status",
    "reference_market_status",
]

# Chainlink classifies XAU/USD as Precious_Metals. CME Globex Gold provides the
# conservative regular/holiday/early-close calendar for that reference market.
# Equity calendars follow the provider directory's regular-session convention.
_CALENDAR_BY_PAIR = {"XAU/USD": "CMEGlobex_Gold", "GOOGL/USD": "NYSE", "TSLA/USD": "NYSE"}


def reference_market_status(pair: str, *, as_of: datetime | None = None) -> MarketHoursObservation:
    """Return a fail-closed session status for a supported reference pair."""
    now = _normalize(as_of)
    calendar_name = _CALENDAR_BY_PAIR.get(pair.strip().upper())
    if calendar_name is None:
        return MarketHoursObservation(ReferenceMarketStatus.UNKNOWN, now, "unsupported")
    return exchange_market_status(calendar_name, as_of=now)
