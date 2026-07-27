"""Funding-history decode contract for the Hyperliquid gateway provider.

Malformed or non-finite rates are SKIPPED, never decoded as a measured 0
(same contract as the shared decoder in ``almanak.framework.data.rates``).
"""

from decimal import Decimal

import pytest

from almanak.connectors.hyperliquid.gateway.provider import (
    _hyperliquid_parse_funding_entries,
    _hyperliquid_read_funding_response,
)
from almanak.gateway.services.rate_history_service import RateHistoryRateLimited


def _entry(time_ms: int, rate: object) -> dict:
    return {"time": time_ms, "fundingRate": rate}


def test_malformed_and_nonfinite_rates_are_skipped_never_zeroed() -> None:
    start_ts, end_ts = 1_700_000_000, 1_700_100_000
    entries = [
        _entry(1_700_000_000_000, "0.0000125"),  # valid
        _entry(1_700_003_600_000, "garbage"),  # malformed: skip
        _entry(1_700_007_200_000, "NaN"),  # non-finite: skip
        _entry(1_700_010_800_000, None),  # unmeasured: skip
        _entry(1_700_014_400_000, ""),  # unmeasured: skip
    ]

    points = _hyperliquid_parse_funding_entries(entries, start_ts=start_ts, end_ts=end_ts)

    assert [p.rate_hourly for p in points] == [Decimal("0.0000125")]
    assert points[0].rate_annualized == Decimal("0.0000125") * 8760


@pytest.mark.asyncio
async def test_http_429_captures_retry_after() -> None:
    class _Response:
        status = 429
        headers = {"Retry-After": "7.5"}

        async def text(self) -> str:
            return "rate limited"

    with pytest.raises(RateHistoryRateLimited) as exc_info:
        await _hyperliquid_read_funding_response(_Response())

    assert exc_info.value.retry_after == 7.5
