"""market.pool_history() served from the run's pool-history lane.

Parity contract: the engine already consumed this daily ladder internally
(LP fee accrual) while the strategy-facing accessor refused. The backtest
reader closes that gap with three honesty rules pinned here:

- daily bars only ("1d"); finer resolutions refuse with guidance,
- completed days only — the tick's own day-bar would include trades from
  the tick's future,
- Empty != Zero per-row unmeasured fields; deterministic meta (observed_at
  is the bound tick, never wall clock).
"""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    BacktestPoolHistoryReader,
    create_market_snapshot_from_state,
)
from almanak.framework.backtesting.pnl.providers.pool_history_fallback import DailyPoolHistory

TICK = datetime(2026, 4, 21, 14, 0, tzinfo=UTC)
POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"


class _FakeProvider:
    """Deterministic stand-in for the pool-history ladder."""

    def __init__(self, rows: dict[date, DailyPoolHistory | None]) -> None:
        self.rows = rows
        self.calls: list[date] = []

    def daily_history(self, *, pool_address: str, chain: str, protocol: str, day: date):
        self.calls.append(day)
        return self.rows.get(day)


def _row(tvl: str | None, volume: str | None, tvl_src: str = "defillama", vol_src: str = "geckoterminal"):
    return DailyPoolHistory(
        tvl=Decimal(tvl) if tvl is not None else None,
        tvl_source=tvl_src if tvl is not None else "",
        volume_24h=Decimal(volume) if volume is not None else None,
        volume_source=vol_src if volume is not None else "",
    )


def _reader(rows: dict[date, DailyPoolHistory | None]) -> BacktestPoolHistoryReader:
    reader = BacktestPoolHistoryReader(_FakeProvider(rows), "ethereum")
    reader.bind(TICK)
    return reader


def _window(reader: BacktestPoolHistoryReader, start: datetime, end: datetime, resolution: str = "1d"):
    return reader.get_pool_history(
        pool_address=POOL,
        chain="ethereum",
        start_date=start,
        end_date=end,
        resolution=resolution,
        protocol="uniswap_v3",
    )


class TestServeContract:
    def test_serves_completed_days_only(self):
        rows = {date(2026, 4, d): _row("1000000", "50000") for d in range(17, 22)}
        reader = _reader(rows)

        envelope = _window(reader, datetime(2026, 4, 18, tzinfo=UTC), TICK)

        served_days = [snap.timestamp.date() for snap in envelope.value]
        # Window end is the tick (Apr 21 14:00): the newest servable bar is
        # Apr 20 — the tick's own day-bar would include the tick's future.
        assert served_days == [date(2026, 4, 18), date(2026, 4, 19), date(2026, 4, 20)]
        assert envelope.value[0].tvl == Decimal("1000000")
        assert envelope.value[0].volume_24h == Decimal("50000")

    def test_finer_resolution_refuses_with_guidance(self):
        reader = _reader({})
        with pytest.raises(ValueError, match="pass resolution='1d'"):
            _window(reader, datetime(2026, 4, 18, tzinfo=UTC), TICK, resolution="1h")

    def test_unbound_reader_refuses(self):
        reader = BacktestPoolHistoryReader(_FakeProvider({}), "ethereum")
        with pytest.raises(ValueError, match="not bound"):
            _window(reader, datetime(2026, 4, 18, tzinfo=UTC), TICK)

    def test_window_with_no_completed_days_refuses(self):
        reader = _reader({})
        with pytest.raises(ValueError, match="no completed days"):
            _window(reader, datetime(2026, 4, 21, tzinfo=UTC), datetime(2026, 4, 22, tzinfo=UTC))

    def test_no_coverage_refuses(self):
        reader = _reader({})  # provider misses every day
        with pytest.raises(ValueError, match="measured no days"):
            _window(reader, datetime(2026, 4, 18, tzinfo=UTC), TICK)

    def test_partial_misses_skip_and_unmeasured_fields_stay_none(self):
        rows = {
            date(2026, 4, 18): _row("1000000", None),  # volume unmeasured
            date(2026, 4, 19): None,  # whole day missed
            date(2026, 4, 20): _row(None, "70000"),  # tvl unmeasured
        }
        reader = _reader(rows)

        envelope = _window(reader, datetime(2026, 4, 18, tzinfo=UTC), TICK)

        assert [s.timestamp.date() for s in envelope.value] == [date(2026, 4, 18), date(2026, 4, 20)]
        first, second = envelope.value
        assert first.volume_24h is None and "volume_24h" in first.unmeasured_fields
        assert second.tvl is None and "tvl" in second.unmeasured_fields
        # Fields this ladder never measures are always unmeasured, never zero.
        assert "fee_revenue_24h" in first.unmeasured_fields
        assert "token0_reserve" in first.unmeasured_fields

    def test_meta_is_deterministic_and_source_labeled(self):
        rows = {date(2026, 4, 20): _row("1000000", "50000")}
        reader = _reader(rows)

        envelope = _window(reader, datetime(2026, 4, 20, tzinfo=UTC), TICK)

        assert envelope.meta.observed_at == TICK  # the bound tick, not wall clock
        assert envelope.meta.source.startswith("backtest_pool_history:")
        assert "defillama" in envelope.meta.source and "geckoterminal" in envelope.meta.source


class TestSnapshotWiring:
    def _state(self) -> MarketState:
        return MarketState(timestamp=TICK, prices={"WETH": Decimal("3000")}, chain="ethereum", block_number=1)

    def test_snapshot_serves_through_reader(self):
        rows = {date(2026, 4, 20): _row("1000000", "50000")}
        reader = _reader(rows)
        snapshot = create_market_snapshot_from_state(
            market_state=self._state(), chain="ethereum", pool_history_reader=reader
        )

        envelope = snapshot.pool_history(
            POOL, start_date=datetime(2026, 4, 20, tzinfo=UTC), resolution="1d", protocol="uniswap_v3"
        )

        assert len(envelope.value) == 1
        assert not snapshot._critical_data_failures

    def test_snapshot_without_reader_keeps_refusal(self):
        snapshot = create_market_snapshot_from_state(market_state=self._state(), chain="ethereum")
        with pytest.raises(ValueError):
            snapshot.pool_history(
                POOL, start_date=datetime(2026, 4, 20, tzinfo=UTC), resolution="1d", protocol="uniswap_v3"
            )
        assert ("pool_history", "unconfigured") in snapshot._critical_data_failures
