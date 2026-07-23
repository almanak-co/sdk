"""market.pool_analytics() served from the run's pool-history lane.

Serve shape pinned per the PoolAnalytics contract: TVL and 24h volume from
the newest COMPLETED pool-day, 7d volume only when all seven days measured,
fee_apr/fee_apy honestly unmeasured (placeholder zeros declared in
``unmeasured_fields``, confidence decayed), and best_pool keeps refusing —
live best_pool is itself deferred to a gateway RPC, so refusal IS parity.
"""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    BacktestPoolAnalyticsReader,
    create_market_snapshot_from_state,
)
from almanak.framework.backtesting.pnl.providers.pool_history_fallback import DailyPoolHistory

TICK = datetime(2026, 4, 21, 14, 0, tzinfo=UTC)
NEWEST_COMPLETE = date(2026, 4, 20)
POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"


class _FakeProvider:
    def __init__(self, rows: dict[date, DailyPoolHistory | None]) -> None:
        self.rows = rows

    def daily_history(self, *, pool_address: str, chain: str, protocol: str, day: date):
        return self.rows.get(day)


def _row(tvl: str | None, volume: str | None):
    return DailyPoolHistory(
        tvl=Decimal(tvl) if tvl is not None else None,
        tvl_source="defillama" if tvl is not None else "",
        volume_24h=Decimal(volume) if volume is not None else None,
        volume_source="geckoterminal" if volume is not None else "",
    )


def _reader(rows: dict[date, DailyPoolHistory | None]) -> BacktestPoolAnalyticsReader:
    reader = BacktestPoolAnalyticsReader(_FakeProvider(rows), "ethereum")
    reader.bind(TICK)
    return reader


def _full_week(tvl: str = "1000000", volume: str = "50000") -> dict[date, DailyPoolHistory]:
    return {NEWEST_COMPLETE - timedelta(days=offset): _row(tvl, volume) for offset in range(7)}


class TestServeContract:
    def test_serves_from_newest_completed_day(self):
        envelope = _reader(_full_week()).get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")

        analytics = envelope.value
        assert analytics.tvl_usd == Decimal("1000000")
        assert analytics.volume_24h_usd == Decimal("50000")
        assert analytics.volume_7d_usd == Decimal("350000")
        assert "tvl_usd" not in analytics.unmeasured_fields
        assert envelope.meta.observed_at == TICK  # deterministic, not wall clock

    def test_fees_are_declared_unmeasured_and_confidence_decays(self):
        envelope = _reader(_full_week()).get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")

        analytics = envelope.value
        assert {"fee_apr", "fee_apy"} <= analytics.unmeasured_fields
        assert analytics.fee_apr == 0.0  # placeholder per the model contract
        # Baseline 0.85 minus 0.15 per unmeasured money field (2 here).
        assert envelope.meta.confidence == pytest.approx(0.55)

    def test_incomplete_week_makes_7d_volume_unmeasured(self):
        rows = _full_week()
        del rows[NEWEST_COMPLETE - timedelta(days=6)]
        envelope = _reader(rows).get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")

        analytics = envelope.value
        assert "volume_7d_usd" in analytics.unmeasured_fields
        assert analytics.volume_24h_usd == Decimal("50000")  # newest day still serves

    def test_uncovered_pool_refuses(self):
        with pytest.raises(ValueError, match="measured no data"):
            _reader({}).get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")

    def test_missing_protocol_hint_refuses_with_guidance(self):
        with pytest.raises(ValueError, match="pass protocol="):
            _reader(_full_week()).get_pool_analytics(POOL, "ethereum")

    def test_best_pool_keeps_live_parity_refusal(self):
        with pytest.raises(ValueError, match="VIB-4729"):
            _reader(_full_week()).best_pool("WETH", "USDC", "ethereum")


class TestSnapshotWiring:
    def test_snapshot_serves_pool_analytics(self):
        state = MarketState(timestamp=TICK, prices={"WETH": Decimal("3000")}, chain="ethereum", block_number=1)
        snapshot = create_market_snapshot_from_state(
            market_state=state, chain="ethereum", pool_analytics_reader=_reader(_full_week())
        )

        envelope = snapshot.pool_analytics(POOL, protocol="uniswap_v3")

        assert envelope.value.tvl_usd == Decimal("1000000")
        assert not snapshot._critical_data_failures
