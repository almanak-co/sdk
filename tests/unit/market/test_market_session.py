"""Strategy-facing exchange-session gate (`MarketSnapshot.market_session`).

Tokenized-equity strategies need to know whether the listing exchange is open.
That answer is a calendar, not a price: it must not depend on an oracle round,
a gateway RPC, or a chain read, and it must be identical on live chain, an
Anvil fork, and a historical backtest tick. It is keyed by exchange, never by
token or ticker symbol.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from almanak import MarketSessionData as PublicMarketSessionData
from almanak.framework import MarketSessionData as FrameworkMarketSessionData
from almanak.framework.market import MarketSessionData, MarketSnapshotBuilder
from almanak.framework.market.models import ReferenceMarketStatus

NYSE_OPEN_MONDAY = datetime(2026, 8, 10, 18, tzinfo=UTC)  # 14:00 ET, regular session
NYSE_SATURDAY = datetime(2026, 8, 8, 18, tzinfo=UTC)
NYSE_PRE_MARKET = datetime(2026, 8, 10, 12, tzinfo=UTC)  # 08:00 ET, before the bell
NYSE_HOLIDAY = datetime(2026, 7, 3, 16, tzinfo=UTC)  # Independence Day observed (Fri), 12:00 ET
NYSE_EARLY_CLOSE_AFTERNOON = datetime(2026, 11, 27, 19, tzinfo=UTC)  # day after Thanksgiving, 14:00 ET


def _snapshot(as_of: datetime):
    return MarketSnapshotBuilder.seeded(chain="bsc", timestamp=as_of)


def test_market_session_is_exported_from_every_public_surface():
    assert PublicMarketSessionData is MarketSessionData
    assert FrameworkMarketSessionData is MarketSessionData


@pytest.mark.parametrize(
    ("as_of", "expected"),
    [
        (NYSE_OPEN_MONDAY, ReferenceMarketStatus.OPEN),
        (NYSE_SATURDAY, ReferenceMarketStatus.CLOSED),
        (NYSE_PRE_MARKET, ReferenceMarketStatus.CLOSED),
        (NYSE_HOLIDAY, ReferenceMarketStatus.CLOSED),
        (NYSE_EARLY_CLOSE_AFTERNOON, ReferenceMarketStatus.CLOSED),
    ],
)
def test_session_follows_the_published_calendar_at_the_snapshot_timestamp(as_of, expected):
    session = _snapshot(as_of).market_session("NYSE")

    assert session.status is expected
    assert session.is_open is (expected is ReferenceMarketStatus.OPEN)
    assert session.as_of == as_of
    assert session.exchange == "NYSE"
    assert session.source == "pandas_market_calendars:NYSE"


def test_exchange_name_is_matched_case_insensitively():
    session = _snapshot(NYSE_OPEN_MONDAY).market_session("nasdaq")

    assert session.exchange == "NASDAQ"
    assert session.status is ReferenceMarketStatus.OPEN


@pytest.mark.parametrize(
    ("as_of", "expected"),
    [
        (datetime(2026, 9, 10, 1, 29, tzinfo=UTC), ReferenceMarketStatus.CLOSED),  # before the open
        (datetime(2026, 9, 10, 1, 30, tzinfo=UTC), ReferenceMarketStatus.OPEN),  # open is inclusive
        (datetime(2026, 9, 10, 4, 0, tzinfo=UTC), ReferenceMarketStatus.CLOSED),  # lunch closure begins
        (datetime(2026, 9, 10, 4, 30, tzinfo=UTC), ReferenceMarketStatus.CLOSED),  # inside the lunch closure
        (datetime(2026, 9, 10, 5, 0, tzinfo=UTC), ReferenceMarketStatus.OPEN),  # afternoon session resumes
        (datetime(2026, 9, 10, 7, 59, tzinfo=UTC), ReferenceMarketStatus.OPEN),
        (datetime(2026, 9, 10, 8, 0, tzinfo=UTC), ReferenceMarketStatus.CLOSED),  # close is exclusive
    ],
)
def test_scheduled_lunch_break_counts_as_closed(as_of, expected):
    """A gate that says OPEN during HKEX's lunch break would trade inside the closure it exists to prevent."""
    session = _snapshot(as_of).market_session("HKEX")

    assert session.status is expected
    assert session.exchange == "HKEX"


@pytest.mark.parametrize(
    ("as_of", "expected"),
    [
        (datetime(2026, 8, 10, 17, 59, tzinfo=UTC), ReferenceMarketStatus.OPEN),  # 13:59 ET, before the halt
        (datetime(2026, 8, 10, 18, 0, tzinfo=UTC), ReferenceMarketStatus.CLOSED),  # 14:00 ET, halt begins
        (datetime(2026, 8, 10, 18, 15, tzinfo=UTC), ReferenceMarketStatus.CLOSED),  # inside the halt
        (datetime(2026, 8, 10, 18, 30, tzinfo=UTC), ReferenceMarketStatus.OPEN),  # 14:30 ET, trading resumes
    ],
)
def test_declared_interruption_counts_as_closed(as_of, expected):
    """A declared intraday halt must read CLOSED; the schedule has to be built with interruptions included.

    No shipped calendar declares an interruption today, so this temporarily
    declares one on the NYSE calendar class for 2026-08-10 14:00-14:30 ET.
    Subclassing is avoided on purpose: the library's metaclass registers
    subclasses under the parent's aliases, which would leak into other tests.
    """
    from datetime import time

    import pandas_market_calendars as mcal

    nyse_class = type(mcal.get_calendar("NYSE"))
    halt = [("2026-08-10", time(14, 0), time(14, 30))]
    with patch.object(nyse_class, "interruptions", new_callable=PropertyMock, return_value=halt):
        session = _snapshot(as_of).market_session("NYSE")

    assert session.status is expected
    assert session.exchange == "NYSE"


def test_commodity_calendar_is_available_by_name():
    session = _snapshot(NYSE_OPEN_MONDAY).market_session("CMEGlobex_Gold")

    assert session.exchange == "CMEGlobex_Gold"
    assert session.status is ReferenceMarketStatus.OPEN


@pytest.mark.parametrize("name", ["NOT_AN_EXCHANGE", "GOOGL", "GOOGLB", "NVDA", ""])
def test_symbols_and_unknown_names_are_unknown_and_never_open(name):
    """Session hours belong to an exchange; a ticker or token symbol is not accepted as one."""
    session = _snapshot(NYSE_OPEN_MONDAY).market_session(name)

    assert session.status is ReferenceMarketStatus.UNKNOWN
    assert session.is_open is False
    assert session.source == "unsupported"
    assert session.exchange == ""


def test_market_session_uses_snapshot_time_not_wall_clock():
    """A backtest tick stamped on a weekend must read as closed even if run on a weekday."""
    snapshot = _snapshot(NYSE_SATURDAY)

    with patch("almanak.core.market_sessions.datetime") as fake_datetime:
        fake_datetime.now.return_value = NYSE_OPEN_MONDAY
        session = snapshot.market_session("NYSE")

    assert session.status is ReferenceMarketStatus.CLOSED
    assert session.as_of == NYSE_SATURDAY
    fake_datetime.now.assert_not_called()


def test_market_session_never_touches_the_gateway():
    """The whole point: no oracle, no RPC, so Anvil and live behave the same."""
    client = MagicMock()
    client.is_connected = True
    snapshot = MarketSnapshotBuilder.for_strategy_runner(
        strategy=SimpleNamespace(chain="bsc", wallet_address="0x1"),
        gateway_client=client,
        runtime_surface="unit_test",
    )

    session = snapshot.market_session("NYSE")

    assert session.status in (ReferenceMarketStatus.OPEN, ReferenceMarketStatus.CLOSED)
    assert client.market.method_calls == []
    assert not snapshot.has_critical_data_failures()


def test_calendar_failure_fails_closed():
    with patch("pandas_market_calendars.get_calendar", side_effect=RuntimeError("calendar unavailable")):
        session = _snapshot(NYSE_OPEN_MONDAY).market_session("NYSE")

    assert session.status is ReferenceMarketStatus.UNKNOWN
    assert session.is_open is False
    assert session.source == "pandas_market_calendars:NYSE"


def test_market_session_data_is_immutable():
    session = _snapshot(NYSE_OPEN_MONDAY).market_session("NYSE")

    with pytest.raises(AttributeError):
        session.status = ReferenceMarketStatus.OPEN  # type: ignore[misc]
