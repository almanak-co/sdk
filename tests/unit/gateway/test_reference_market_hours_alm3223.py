"""Fail-closed precious-metals market-hours tests for ALM-3223."""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

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


@pytest.mark.parametrize("pair", ["GOOGL/USD", "TSLA/USD"])
@pytest.mark.parametrize(
    ("time", "expected"),
    [
        ("2026-09-08T13:29:59", ReferenceMarketStatus.CLOSED),
        ("2026-09-08T13:30:00", ReferenceMarketStatus.OPEN),
        ("2026-09-08T20:00:00", ReferenceMarketStatus.CLOSED),
        ("2026-09-05T15:00:00", ReferenceMarketStatus.CLOSED),
        ("2026-09-07T15:00:00", ReferenceMarketStatus.CLOSED),
        ("2026-07-03T15:00:00", ReferenceMarketStatus.CLOSED),
        ("2026-11-27T17:30:00", ReferenceMarketStatus.OPEN),
        ("2026-11-27T18:00:00", ReferenceMarketStatus.CLOSED),
        ("2026-03-06T14:00:00", ReferenceMarketStatus.CLOSED),
        ("2026-03-09T14:00:00", ReferenceMarketStatus.OPEN),
    ],
)
def test_equity_regular_sessions_include_holidays_early_closes_and_dst(pair, time, expected):
    as_of = datetime.fromisoformat(time).replace(tzinfo=UTC)
    result = reference_market_status(pair, as_of=as_of)
    assert result.status is expected
    assert result.as_of == as_of
    assert result.source == "pandas_market_calendars:NYSE"


@pytest.mark.parametrize(
    "pair", ["GOOGLB/USD", "TSLAB/USD", "GOOGLX/USD", "GOOGLON/USD", "TSLAX/USD", "TSLAON/USD", "GOOG/USD"]
)
def test_wrapper_or_different_share_class_does_not_inherit_equity_calendar(pair):
    result = reference_market_status(pair, as_of=datetime(2026, 9, 8, 15, tzinfo=UTC))
    assert result.status is ReferenceMarketStatus.UNKNOWN


def test_every_bsc_reference_feed_has_a_measured_session_calendar():
    from almanak.integrations.chainlink.catalog import CATALOG, FeedKind

    for pair in CATALOG.feeds("bsc", kind=FeedKind.REFERENCE):
        result = reference_market_status(pair, as_of=datetime(2026, 9, 8, 15, tzinfo=UTC))
        assert result.status is not ReferenceMarketStatus.UNKNOWN, pair
