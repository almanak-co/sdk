"""market.pool_analytics() served from exact state plus pool history.

Serve shape pinned per the PoolAnalytics contract: TVL, 24h volume, and base
fee APY from the newest COMPLETED pool-day, 7d volume only when all seven days
measured, unavailable fee fields honestly declared in ``unmeasured_fields``,
and best_pool keeps refusing —
live best_pool is itself deferred to a gateway RPC, so refusal IS parity.
"""

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.pnl.data_provider import HistoricalPriceObservation, MarketState
from almanak.framework.backtesting.pnl.engine import (
    BacktestPoolAnalyticsReader,
    create_market_snapshot_from_state,
)
from almanak.framework.backtesting.pnl.providers.pool_history_fallback import DailyPoolHistory
from almanak.framework.backtesting.pnl.providers.snapshot_pool_analytics import (
    HistoricalPoolAnalyticsTarget,
    validate_historical_pool_analytics,
)
from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import (
    HistoricalPoolStatePoint,
    HistoricalPoolStateTarget,
    HistoricalPoolTVL,
    SnapshotPoolStateSource,
)
from almanak.framework.backtesting.pnl.providers.snapshot_twap import HistoricalTWAPTarget, SnapshotTWAPSource
from almanak.framework.backtesting.pnl.providers.twap import HistoricalTWAPPoint
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.market.errors import PoolPriceUnavailableError

TICK = datetime(2026, 4, 21, 14, 0, tzinfo=UTC)
NEWEST_COMPLETE = date(2026, 4, 20)
POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
SPCXB = "0xbe9d156892e55e7154bcd3cb0fea677f9d3103e1"
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
BSC_POOL = "0x66faad27cf481f82d0089ec8156b3aa3636010c7"


class _FakeProvider:
    def __init__(self, rows: dict[date, DailyPoolHistory | None]) -> None:
        self.rows = rows

    def daily_history(self, *, pool_address: str, chain: str, protocol: str, day: date):
        return self.rows.get(day)


def _row(tvl: str | None, volume: str | None, fee_apy: str | None = None):
    return DailyPoolHistory(
        tvl=Decimal(tvl) if tvl is not None else None,
        tvl_source="defillama" if tvl is not None else "",
        volume_24h=Decimal(volume) if volume is not None else None,
        volume_source="coingecko_onchain" if volume is not None else "",
        fee_apy=Decimal(fee_apy) if fee_apy is not None else None,
        fee_apy_source="defillama" if fee_apy is not None else "",
    )


def _reader(rows: dict[date, DailyPoolHistory | None]) -> BacktestPoolAnalyticsReader:
    reader = BacktestPoolAnalyticsReader(_FakeProvider(rows), "ethereum")
    reader.bind(TICK)
    return reader


class _ExactStateView:
    def read_pool_tvl_usd(self, **_kwargs):
        observed_at = TICK - timedelta(seconds=5)
        return DataEnvelope(
            value=HistoricalPoolTVL(
                tvl_usd=Decimal("1250000"),
                token0_value_usd=Decimal("500000"),
                token1_value_usd=Decimal("750000"),
                token0_weight=0.4,
                token1_weight=0.6,
            ),
            meta=DataMeta(
                source="historical:on_chain_archive+historical_price:coingecko",
                observed_at=observed_at,
                block_number=123,
                staleness_ms=5_000,
                freshness_reference_at=TICK,
            ),
            classification=DataClassification.INFORMATIONAL,
        )


class _SelectiveExactStateView(_ExactStateView):
    def __init__(
        self,
        declared_pool: str,
        *,
        failure_reason: str = "exact pool was not declared and prewarmed",
    ) -> None:
        self.declared_pool = declared_pool.lower()
        self.failure_reason = failure_reason

    def read_pool_tvl_usd(self, *, pool_address: str, **kwargs):
        del kwargs
        if pool_address.lower() != self.declared_pool:
            raise PoolPriceUnavailableError(pool_address, self.failure_reason)
        return super().read_pool_tvl_usd()


def _full_week(tvl: str = "1000000", volume: str = "50000") -> dict[date, DailyPoolHistory]:
    return {NEWEST_COMPLETE - timedelta(days=offset): _row(tvl, volume) for offset in range(7)}


class TestServeContract:
    def test_unbound_reader_refuses(self):
        reader = BacktestPoolAnalyticsReader(_FakeProvider(_full_week()), "ethereum")

        with pytest.raises(ValueError, match="not bound to a tick"):
            reader.get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")

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

    def test_measured_base_fee_apy_is_served_without_incentive_inference(self):
        rows = _full_week()
        rows[NEWEST_COMPLETE] = _row("1000000", "50000", "1.92")

        envelope = _reader(rows).get_pool_analytics(POOL, "ethereum", protocol="curve")

        assert envelope.value.fee_apy == pytest.approx(1.92)
        assert "fee_apy" not in envelope.value.unmeasured_fields
        assert "fee_apr" in envelope.value.unmeasured_fields
        assert envelope.meta.source == "backtest_pool_analytics:coingecko_onchain+defillama"
        assert envelope.meta.confidence == pytest.approx(0.7)

    def test_fee_apy_only_row_counts_as_measured_pool_data(self):
        envelope = _reader({NEWEST_COMPLETE: _row(None, None, "0")}).get_pool_analytics(
            POOL,
            "ethereum",
            protocol="curve",
        )

        assert envelope.value.fee_apy == 0.0
        assert "fee_apy" not in envelope.value.unmeasured_fields

    def test_incomplete_week_makes_7d_volume_unmeasured(self):
        rows = _full_week()
        del rows[NEWEST_COMPLETE - timedelta(days=6)]
        envelope = _reader(rows).get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")

        analytics = envelope.value
        assert "volume_7d_usd" in analytics.unmeasured_fields
        assert analytics.volume_24h_usd == Decimal("50000")  # newest day still serves

    def test_newest_volume_without_tvl_is_measured_pool_data(self):
        envelope = _reader({NEWEST_COMPLETE: _row(None, "50000")}).get_pool_analytics(
            POOL,
            "ethereum",
            protocol="uniswap_v3",
        )

        assert envelope.value.volume_24h_usd == Decimal("50000")
        assert "tvl_usd" in envelope.value.unmeasured_fields

    def test_uncovered_pool_refuses(self):
        with pytest.raises(ValueError, match="measured no data"):
            _reader({}).get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")

    def test_missing_protocol_hint_refuses_with_guidance(self):
        with pytest.raises(ValueError, match="pass protocol="):
            _reader(_full_week()).get_pool_analytics(POOL, "ethereum")

    def test_best_pool_keeps_live_parity_refusal(self):
        with pytest.raises(ValueError, match="VIB-4729"):
            _reader(_full_week()).best_pool("WETH", "USDC", "ethereum")

    def test_exact_state_tvl_serves_when_daily_pool_history_is_absent(self):
        reader = BacktestPoolAnalyticsReader(_FakeProvider({}), "ethereum")
        market_state = MarketState(timestamp=TICK, prices={}, chain="ethereum")
        reader.bind(TICK, market_state=market_state, pool_state_view=_ExactStateView())

        envelope = reader.get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")

        assert envelope.value.tvl_usd == Decimal("1250000")
        assert "tvl_usd" not in envelope.value.unmeasured_fields
        assert {"volume_24h_usd", "volume_7d_usd"} <= envelope.value.unmeasured_fields
        assert envelope.value.token0_weight == pytest.approx(0.4)
        assert envelope.meta.block_number == 123
        assert envelope.is_fresh
        assert envelope.meta.source.startswith("backtest_pool_analytics:")

    def test_exact_state_reader_requires_bound_market_state(self):
        reader = BacktestPoolAnalyticsReader(_FakeProvider({}), "ethereum")
        reader.bind(TICK, pool_state_view=_ExactStateView())

        with pytest.raises(ValueError, match="missing the bound historical market state"):
            reader.get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")

    def test_undeclared_exact_pool_uses_completed_day_history_fallback(self):
        reader = BacktestPoolAnalyticsReader(_FakeProvider(_full_week()), "ethereum")
        reader.bind(
            TICK,
            market_state=MarketState(timestamp=TICK, prices={}, chain="ethereum"),
            pool_state_view=_SelectiveExactStateView(BSC_POOL),
        )

        envelope = reader.get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")

        assert envelope.value.tvl_usd == Decimal("1000000")
        assert envelope.meta.source == "backtest_pool_analytics:coingecko_onchain+defillama"

    def test_declared_exact_pool_state_failure_is_not_hidden_by_history_fallback(self):
        reader = BacktestPoolAnalyticsReader(_FakeProvider(_full_week()), "ethereum")
        reader.bind(
            TICK,
            market_state=MarketState(timestamp=TICK, prices={}, chain="ethereum"),
            pool_state_view=_SelectiveExactStateView(
                BSC_POOL,
                failure_reason="no historical pool state exists at this tick",
            ),
        )

        with pytest.raises(PoolPriceUnavailableError, match="no historical pool state exists at this tick"):
            reader.get_pool_analytics(POOL, "ethereum", protocol="uniswap_v3")


class TestHistoricalAnalyticsDeclarations:
    def test_required_field_validation_accepts_archive_tvl_without_volume(self):
        reader = BacktestPoolAnalyticsReader(_FakeProvider({}), "ethereum")
        reader.bind(
            TICK,
            market_state=MarketState(timestamp=TICK, prices={}, chain="ethereum"),
            pool_state_view=_ExactStateView(),
        )
        target = HistoricalPoolAnalyticsTarget("ethereum", "uniswap_v3", POOL, frozenset({"tvl_usd"}), 60)

        assert validate_historical_pool_analytics(reader, (target,), TICK) == 1

    def test_required_unmeasured_field_fails_closed(self):
        reader = BacktestPoolAnalyticsReader(_FakeProvider({}), "ethereum")
        reader.bind(
            TICK,
            market_state=MarketState(timestamp=TICK, prices={}, chain="ethereum"),
            pool_state_view=_ExactStateView(),
        )
        target = HistoricalPoolAnalyticsTarget(
            "ethereum",
            "uniswap_v3",
            POOL,
            frozenset({"volume_24h_usd"}),
        )

        with pytest.raises(ValueError, match="required fields are unmeasured"):
            validate_historical_pool_analytics(reader, (target,), TICK)

    def test_volume_only_freshness_limit_fails_without_measured_observation(self):
        reader = _reader(_full_week())
        target = HistoricalPoolAnalyticsTarget(
            "ethereum",
            "uniswap_v3",
            POOL,
            frozenset({"volume_24h_usd"}),
            60,
        )

        with pytest.raises(ValueError, match="freshness is unmeasured"):
            validate_historical_pool_analytics(reader, (target,), TICK)

    def test_required_measured_fee_apy_succeeds_without_exact_state(self):
        reader = _reader({NEWEST_COMPLETE: _row(None, None, "1.37")})
        target = HistoricalPoolAnalyticsTarget(
            "ethereum",
            "curve",
            POOL,
            frozenset({"fee_apy"}),
        )

        assert validate_historical_pool_analytics(reader, (target,), TICK) == 1

    @pytest.mark.parametrize(
        ("field_name", "value"),
        (("tvl_usd", Decimal("NaN")), ("fee_apr", float("inf"))),
    )
    def test_required_nonfinite_numeric_field_fails_closed(self, field_name: str, value: object):
        reader = SimpleNamespace(
            get_pool_analytics=lambda **_kwargs: SimpleNamespace(
                value=SimpleNamespace(unmeasured_fields=frozenset(), **{field_name: value}),
                meta=SimpleNamespace(staleness_ms=0),
            )
        )
        target = HistoricalPoolAnalyticsTarget(
            "ethereum",
            "uniswap_v3",
            POOL,
            frozenset({field_name}),
        )

        with pytest.raises(ValueError, match=f"{field_name}="):
            validate_historical_pool_analytics(reader, (target,), TICK)


class TestSnapshotWiring:
    def test_snapshot_serves_pool_analytics(self):
        state = MarketState(timestamp=TICK, prices={"WETH": Decimal("3000")}, chain="ethereum", block_number=1)
        snapshot = create_market_snapshot_from_state(
            market_state=state, chain="ethereum", pool_analytics_reader=_reader(_full_week())
        )

        envelope = snapshot.pool_analytics(POOL, protocol="uniswap_v3")

        assert envelope.value.tvl_usd == Decimal("1000000")
        assert not snapshot._critical_data_failures

    def test_exact_pancakeswap_signal_inputs_are_measured_and_fresh(self):
        """Pin the three fail-closed reads used by the reported strategy."""
        tick_ts = int(TICK.timestamp())
        state_point = HistoricalPoolStatePoint(
            timestamp=tick_ts - 5,
            block_number=56_000_000,
            sqrt_price_x96=2**96,
            tick=0,
            liquidity=10**18,
            token0=WBNB,
            token1=SPCXB,
            token0_decimals=18,
            token1_decimals=18,
            fee_tier=2500,
            reserve0_raw=1_000 * 10**18,
            reserve1_raw=1_000 * 10**18,
            source="on_chain_archive",
        )
        pool_source = SnapshotPoolStateSource(
            start_time=TICK,
            end_time=TICK,
            sample_interval_seconds=3600,
            fetcher=lambda **_kwargs: [state_point],
        )
        asyncio.run(
            pool_source.materialize_history(
                HistoricalPoolStateTarget(
                    "bsc",
                    "pancakeswap_v3",
                    BSC_POOL,
                    (WBNB, SPCXB),
                    2500,
                )
            )
        )
        twap_source = SnapshotTWAPSource(
            start_time=TICK,
            end_time=TICK,
            sample_interval_seconds=3600,
            fetcher=lambda **_kwargs: [
                HistoricalTWAPPoint(tick_ts - 5, Decimal("1"), 2, "archive_observe", 56_000_000)
            ],
        )
        asyncio.run(twap_source.materialize_history(HistoricalTWAPTarget("bsc", "pancakeswap_v3", BSC_POOL, 900)))

        market_state = MarketState(
            timestamp=TICK,
            prices={
                ("bsc", WBNB): Decimal("600"),
            },
            price_observations={
                ("bsc", WBNB): HistoricalPriceObservation(
                    price=Decimal("600"),
                    timestamp=TICK,
                    source="coingecko",
                    confidence=DataConfidence.MEDIUM,
                )
            },
            chain="bsc",
            block_number=56_000_000,
        )
        pool_view = pool_source.view_at(TICK)
        analytics_reader = BacktestPoolAnalyticsReader(_FakeProvider({}), "bsc")
        analytics_reader.bind(TICK, market_state=market_state, pool_state_view=pool_view)
        snapshot = create_market_snapshot_from_state(
            market_state=market_state,
            chain="bsc",
            token_addresses={"SPCXB": ("bsc", SPCXB), "WBNB": ("bsc", WBNB)},
            pool_price_view=pool_view,
            pool_reader=pool_view,
            pool_analytics_reader=analytics_reader,
            price_aggregator=twap_source.view_at(TICK),
        )

        analytics = snapshot.pool_analytics(BSC_POOL, chain="bsc", protocol="pancakeswap_v3")
        spot = snapshot.pool_price(BSC_POOL, chain="bsc")
        twap = snapshot.twap(
            "SPCXB/WBNB",
            chain="bsc",
            window_seconds=900,
            pool_address=BSC_POOL,
            protocol="pancakeswap_v3",
        )

        assert analytics.value.tvl_usd == Decimal("1200000")
        assert analytics.value.tvl_usd >= Decimal("500000")
        assert analytics.is_fresh
        assert spot.is_fresh
        assert twap.is_fresh
        assert not snapshot._critical_data_failures
