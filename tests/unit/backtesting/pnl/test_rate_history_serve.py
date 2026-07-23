"""market.funding_rate_history() served from the run's funding lane.

Pinned honesty gates:
- serves an ascending hourly series ending at the tick's hour, resolved
  through the same no-look-ahead per-hour lane the engine's funding accrual
  uses;
- refuses in fallback-funding mode (a constant series labeled "history"
  would be fabrication);
- lending_rate_history refuses (the run's lending plane is connector-default
  constants — same objection).
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.engine import BacktestRateHistoryReader

TICK = datetime(2026, 4, 21, 14, 30, tzinfo=UTC)


class _FakeSource:
    """Historical-capable stand-in: rate varies by hour so a served series is
    visibly a measurement, not a constant. ``degraded_hours`` marks hours
    that "resolved" from the fallback constant (the non-strict degrade)."""

    def __init__(self, history_capable: bool = True, degraded_hours: set[datetime] | None = None) -> None:
        self.history_capable = history_capable
        self.asked: list[datetime] = []
        self.degraded_hours = degraded_hours or set()

    async def funding_rate_at(self, venue: str, market: str, timestamp: datetime):
        from types import SimpleNamespace

        hour = timestamp.replace(minute=0, second=0, microsecond=0)
        self.asked.append(hour)
        rate = Decimal("0.0001") * (1 + hour.hour % 3)
        return SimpleNamespace(
            venue=venue,
            market=market,
            rate_hourly=rate,
            rate_annualized=rate * 8760,
            timestamp=hour,
        )

    def point_was_degraded(self, venue: str, market: str, timestamp: datetime) -> bool:
        return timestamp.replace(minute=0, second=0, microsecond=0) in self.degraded_hours


def _reader(source: _FakeSource) -> BacktestRateHistoryReader:
    reader = BacktestRateHistoryReader(source, "arbitrum")
    reader.bind(TICK)
    return reader


class TestFundingHistory:
    def test_serves_ascending_hourly_series_ending_at_tick_hour(self):
        source = _FakeSource()
        envelope = _reader(source).get_funding_rate_history(venue="gmx_v2", market_symbol="ETH-USD", hours=6)

        snaps = envelope.value
        tick_hour = TICK.replace(minute=0, second=0, microsecond=0)
        expected_hours = [tick_hour - timedelta(hours=offset) for offset in range(5, -1, -1)]
        assert len(snaps) == 6
        # The exact contiguous requested-hour sequence, and the source was
        # asked those hours — not later points relabeled with old timestamps.
        assert [s.timestamp for s in snaps] == expected_hours
        assert source.asked == expected_hours
        # Rates vary hour to hour — a measurement series, not a constant.
        assert len({s.rate for s in snaps}) > 1
        assert envelope.meta.observed_at == TICK  # deterministic meta

    def test_fallback_mode_refuses(self):
        source = _FakeSource(history_capable=False)
        with pytest.raises(ValueError, match="fabrication"):
            _reader(source).get_funding_rate_history(venue="gmx_v2", market_symbol="ETH-USD", hours=6)

    def test_unbound_reader_refuses(self):
        reader = BacktestRateHistoryReader(_FakeSource(), "arbitrum")
        with pytest.raises(ValueError, match="not bound"):
            reader.get_funding_rate_history(venue="gmx_v2", market_symbol="ETH-USD", hours=6)

    def test_any_degraded_point_refuses_the_whole_series(self):
        """A window where SOME hours resolved from the fallback constant must
        refuse — a partially fabricated series is not history. Point reads
        may tolerate the degrade; this accessor may not. The degraded hour
        sits mid-window so a final-point-only check cannot pass."""
        degraded_hour = TICK.replace(minute=0, second=0, microsecond=0) - timedelta(hours=3)
        source = _FakeSource(degraded_hours={degraded_hour})
        with pytest.raises(ValueError, match="partially fabricated"):
            _reader(source).get_funding_rate_history(venue="gmx_v2", market_symbol="ETH-USD", hours=6)

    def test_bridge_worker_is_reused_across_calls(self):
        """One long-lived worker per reader, not a per-call pool."""
        source = _FakeSource()
        reader = _reader(source)
        reader.get_funding_rate_history(venue="gmx_v2", market_symbol="ETH-USD", hours=2)
        first = reader._bridge_executor
        reader.get_funding_rate_history(venue="gmx_v2", market_symbol="ETH-USD", hours=2)
        assert reader._bridge_executor is first
        assert first is None or first._max_workers == 1

    def test_zero_hours_refuses(self):
        with pytest.raises(ValueError, match="hours >= 1"):
            _reader(_FakeSource()).get_funding_rate_history(venue="gmx_v2", market_symbol="ETH-USD", hours=0)

    def test_orphaned_timeout_worker_poisons_reads_until_it_finishes(self):
        """A timed-out bridge worker keeps running (cancel() can't stop it)
        and keeps mutating the shared funding source — reads refuse while it
        lives and resume once it is done."""

        class _Orphan:
            done_now = False

            def done(self) -> bool:
                return self.done_now

        source = _FakeSource()
        reader = _reader(source)
        orphan = _Orphan()
        reader._orphaned_future = orphan

        with pytest.raises(ValueError, match="still resolving in the background"):
            reader.get_funding_rate_history(venue="gmx_v2", market_symbol="ETH-USD", hours=2)
        assert source.asked == []  # refused before touching the shared lane

        orphan.done_now = True
        envelope = reader.get_funding_rate_history(venue="gmx_v2", market_symbol="ETH-USD", hours=2)
        assert len(envelope.value) == 2
        assert reader._orphaned_future is None


class TestLendingHistoryRefusal:
    def test_lending_rate_history_refuses_with_reason(self):
        with pytest.raises(ValueError, match="connector-default constant"):
            _reader(_FakeSource()).get_lending_rate_history(protocol="aave_v3", token="USDC")
