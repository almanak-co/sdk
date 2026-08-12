"""Fail-closed precious-metals market-hours tests for ALM-3223."""

from datetime import UTC, datetime
from unittest.mock import patch

from almanak.gateway.data.price.market_hours import ReferenceMarketStatus, reference_market_status


def test_xau_market_is_open_during_regular_globex_session():
    result = reference_market_status("XAU/USD", as_of=datetime(2026, 8, 10, 18, tzinfo=UTC))
    assert result.status is ReferenceMarketStatus.OPEN


def test_xau_market_is_closed_on_weekend():
    result = reference_market_status("XAU/USD", as_of=datetime(2026, 8, 8, 18, tzinfo=UTC))
    assert result.status is ReferenceMarketStatus.CLOSED


def test_unknown_reference_pair_is_never_assumed_open():
    result = reference_market_status("XPT/USD", as_of=datetime(2026, 8, 10, 18, tzinfo=UTC))
    assert result.status is ReferenceMarketStatus.UNKNOWN
    assert result.source == "unsupported"


def test_calendar_failure_is_observable_and_fails_closed(caplog):
    with patch("pandas_market_calendars.get_calendar", side_effect=RuntimeError("calendar unavailable")):
        result = reference_market_status("XAU/USD", as_of=datetime(2026, 8, 10, 18, tzinfo=UTC))

    assert result.status is ReferenceMarketStatus.UNKNOWN
    assert result.source == "pandas_market_calendars:CMEGlobex_Gold"
    assert "RuntimeError" in caplog.text
    assert "calendar unavailable" not in caplog.text
