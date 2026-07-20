"""market.ohlcv() served from the run's own price series (ALM-2962).

A live-traded momentum strategy produced 0 trades in backtest because the
OHLCV accessor was unconfigured; its defensive except turned every read into
HOLD for 2,161 ticks (staging `6d501f2f`). These tests pin the fix: close-only
honest bars, no look-ahead, resampling through the indicator engine, refusals
recorded on the decision-input ledger, and the reproducing strategy shape
actually reading candles end-to-end.
"""

import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.engine import BacktestOHLCVView, create_market_snapshot_from_state
from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine

BOUND_TS = datetime(2026, 4, 20, 12, tzinfo=UTC)
BASE_WETH = ("base", "0x4200000000000000000000000000000000000006")


def _engine_with_series(token: str, closes: list[float]) -> BacktestIndicatorEngine:
    engine = BacktestIndicatorEngine(required_indicators={"rsi"})
    for close in closes:
        engine.append_price(token, Decimal(str(close)))
    return engine


def _view(engine: BacktestIndicatorEngine, token_addresses: dict | None = None) -> BacktestOHLCVView:
    view = BacktestOHLCVView(engine, 3600, token_addresses)
    view.bind(BOUND_TS)
    return view


class TestBacktestOHLCVView:
    def test_close_only_honest_frame(self):
        closes = [3000.0 + i for i in range(30)]
        view = _view(_engine_with_series("WETH", closes))

        df = view.get_ohlcv("WETH", timeframe="1h", limit=10)

        assert list(df.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
        assert len(df) == 10
        # Close-only honesty: no fabricated spread, no fabricated volume.
        assert (df["open"] == df["close"]).all()
        assert (df["high"] == df["close"]).all()
        assert (df["low"] == df["close"]).all()
        assert df["volume"].apply(math.isnan).all()
        assert df.attrs["source"] == "backtest_price_series:close_only"
        # Newest close is the current tick's.
        assert df["close"].iloc[-1] == closes[-1]

    def test_no_look_ahead_timestamps(self):
        view = _view(_engine_with_series("WETH", [1.0] * 24))

        df = view.get_ohlcv("WETH", timeframe="1h", limit=100)

        assert df["timestamp"].iloc[-1] == BOUND_TS
        assert (df["timestamp"] <= BOUND_TS).all()
        deltas = df["timestamp"].diff().dropna().unique()
        assert list(deltas) == [timedelta(hours=1)]

    def test_resamples_whole_multiple_timeframes(self):
        # 48 hourly closes -> 4h buckets, bucket close = last close.
        closes = [float(i) for i in range(48)]
        view = _view(_engine_with_series("WETH", closes))

        df = view.get_ohlcv("WETH", timeframe="4h", limit=100)

        assert df["close"].iloc[-1] == 47.0
        assert df["close"].iloc[-2] == 43.0
        assert df["timestamp"].diff().dropna().unique().tolist() == [timedelta(hours=4)]

    def test_non_multiple_timeframe_refuses(self):
        view = _view(_engine_with_series("WETH", [1.0] * 24))
        with pytest.raises(ValueError, match="derivable"):
            view.get_ohlcv("WETH", timeframe="90m")

    def test_jittered_hourly_cadence_serves_hourly_candles(self):
        # CoinGecko hourly points are sometimes spaced 3601s apart; on short
        # windows the measured cadence reads 3601s. That 1s jitter must not
        # masquerade as coarser data — 1h candles still serve.
        engine = _engine_with_series("WETH", [3000.0 + i for i in range(30)])
        engine.set_data_granularity(3601, 3600)
        view = _view(engine)

        df = view.get_ohlcv("WETH", timeframe="1h", limit=10)
        assert len(df) == 10

    def test_hourly_cadence_still_refuses_finer_candles(self):
        # Jitter tolerance must not loosen the upsampling refusal (ALM-2957):
        # genuinely hourly data on a 15m tick grid never serves 15m candles.
        engine = _engine_with_series("WETH", [3000.0 + i for i in range(80)])
        engine.set_data_granularity(3600, 900)
        view = BacktestOHLCVView(engine, 900, None)
        view.bind(BOUND_TS)

        with pytest.raises(ValueError, match="ALM-2957"):
            view.get_ohlcv("WETH", timeframe="15m")
        assert len(view.get_ohlcv("WETH", timeframe="1h", limit=10)) == 10

    def test_symbol_resolves_through_registered_addresses(self):
        # The engine buffers are keyed address-native in real runs; a
        # strategy-facing "WETH" read must find them via the run's map.
        key = f"{BASE_WETH[0]}:{BASE_WETH[1]}"
        engine = _engine_with_series(key, [3000.0] * 20)
        view = _view(engine, {"WETH": BASE_WETH})

        df = view.get_ohlcv("WETH", limit=5)
        assert len(df) == 5

    def test_pair_string_uses_base_leg(self):
        view = _view(_engine_with_series("WETH", [3000.0] * 20))
        assert len(view.get_ohlcv("WETH/USDC", limit=5)) == 5

    def test_unknown_token_refuses(self):
        view = _view(_engine_with_series("WETH", [1.0] * 20))
        with pytest.raises(ValueError, match="no backtest price series"):
            view.get_ohlcv("DOGE")

    def test_non_usd_quote_refuses(self):
        view = _view(_engine_with_series("WETH", [1.0] * 20))
        with pytest.raises(ValueError, match="USD-quoted"):
            view.get_ohlcv("WETH", quote="BTC")


class TestSnapshotIntegration:
    def _snapshot(self, view: Any):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        state = MarketState(
            timestamp=BOUND_TS, prices={"WETH": Decimal("3000"), "USDC": Decimal("1")}, chain="base", block_number=1
        )
        return create_market_snapshot_from_state(market_state=state, chain="base", ohlcv_module=view)

    def test_accessor_serves_through_the_view(self):
        view = _view(_engine_with_series("WETH", [3000.0 + i for i in range(30)]))
        snapshot = self._snapshot(view)

        df = snapshot.ohlcv("WETH", timeframe="1h", limit=10)
        assert len(df) == 10
        assert (df["open"] == df["close"]).all()

    def test_refusals_are_recorded_on_the_ledger(self):
        view = _view(_engine_with_series("WETH", [1.0] * 20))
        snapshot = self._snapshot(view)

        with pytest.raises(ValueError):
            snapshot.ohlcv("DOGE")

        failures = getattr(snapshot, "_critical_data_failures", {})
        assert any(source == "ohlcv" for (source, _key) in failures), failures

    def test_unknown_pool_scoped_reads_still_refuse_loudly(self):
        # A pool the registry cannot resolve to a pair keeps the recorded
        # refusal; only registry-known pools get the pair proxy.
        view = _view(_engine_with_series("WETH", [1.0] * 20))
        snapshot = self._snapshot(view)

        with pytest.raises(ValueError, match="not a registry-known pool"):
            snapshot.ohlcv("WETH", pool_address="0x" + "d" * 40)
        assert snapshot._critical_data_failures


class TestReviewRound:
    def test_zero_and_negative_limit_return_empty_frames(self):
        view = _view(_engine_with_series("WETH", [1.0] * 24))
        assert len(view.get_ohlcv("WETH", limit=0)) == 0
        assert len(view.get_ohlcv("WETH", limit=-5)) == 0
        # Columns survive the empty case.
        assert list(view.get_ohlcv("WETH", limit=0).columns) == ["timestamp", "open", "high", "low", "close", "volume"]

    def test_capacity_truncation_is_marked_and_warned_once(self, caplog):
        import logging

        # A FULL buffer resampled to 4h cannot serve the requested depth:
        # the frame must say so instead of posing as complete history.
        engine = BacktestIndicatorEngine(required_indicators={"rsi"}, max_history=200)
        for i in range(400):
            engine.append_price("WETH", Decimal(str(3000 + (i % 7))))
        view = BacktestOHLCVView(engine, 3600, None)
        view.bind(BOUND_TS)

        with caplog.at_level(logging.WARNING):
            df = view.get_ohlcv("WETH", timeframe="4h", limit=100)
            view.get_ohlcv("WETH", timeframe="4h", limit=100)  # second call: no re-warn

        assert len(df) == 50  # 200 retained ticks -> 50 4h bars
        assert df.attrs["capacity_truncated"] is True
        truncation_warnings = [r for r in caplog.records if "served only" in r.message]
        assert len(truncation_warnings) == 1

    def test_sufficient_depth_is_not_marked_truncated(self):
        view = _view(_engine_with_series("WETH", [1.0] * 100))
        df = view.get_ohlcv("WETH", timeframe="1h", limit=50)
        assert df.attrs["capacity_truncated"] is False


class TestPoolPairProxy:
    """Pool-scoped requests served as the pool pair's price series, labeled."""

    BASE_WETH_USDC_POOL = "0xd0b53D9277642d899DF5C87A3966A349A798F224"

    def test_known_pool_serves_pair_proxy_with_provenance(self):
        engine = _engine_with_series("WETH", [3000.0 + i for i in range(30)])
        view = _view(engine)

        df = view.get_pool_ohlcv(self.BASE_WETH_USDC_POOL, chain="base", timeframe="1h", limit=10)

        assert len(df) == 10
        assert df.attrs["source"].endswith(":pool_pair_proxy")
        assert df.attrs["pool_address"] == self.BASE_WETH_USDC_POOL
        assert df.attrs["base"] == "WETH"
        assert df.attrs["quote"] == "USD"  # USDC leg is cash-equivalent

    ETH_USDC_WETH_POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"  # registry key order (USDC, WETH)

    def test_requested_orientation_wins_over_registry_key_order(self):
        engine = _engine_with_series("WETH", [3000.0 + i for i in range(30)])
        view = _view(engine)

        df = view.get_pool_ohlcv(
            self.ETH_USDC_WETH_POOL, chain="ethereum", timeframe="1h", limit=10, requested_symbol="WETH/USDC"
        )

        assert df.attrs["base"] == "WETH"  # requested orientation, not the registry's USDC-first key
        assert float(df["close"].iloc[-1]) > 1  # ~3000, never the inverted ~1/3000

    def test_single_token_request_orients_to_that_base(self):
        engine = _engine_with_series("WETH", [3000.0 + i for i in range(30)])
        view = _view(engine)

        df = view.get_pool_ohlcv(
            self.ETH_USDC_WETH_POOL, chain="ethereum", timeframe="1h", limit=10, requested_symbol="WETH"
        )

        assert df.attrs["base"] == "WETH"

    def test_mismatched_pair_pin_refuses(self):
        view = _view(_engine_with_series("WETH", [3000.0] * 10))
        with pytest.raises(ValueError, match="check the pool pin"):
            view.get_pool_ohlcv(
                self.ETH_USDC_WETH_POOL, chain="ethereum", timeframe="1h", requested_symbol="WBTC/USDC"
            )

    def test_unknown_pool_still_refuses(self):
        view = _view(_engine_with_series("WETH", [3000.0] * 10))
        with pytest.raises(ValueError, match="not a registry-known pool"):
            view.get_pool_ohlcv("0x" + "9" * 40, chain="base")

    def test_snapshot_pool_scoped_call_serves_via_capability(self):
        engine = _engine_with_series("WETH", [3000.0 + i for i in range(30)])
        view = _view(engine)
        snapshot = TestSnapshotIntegration()._snapshot(view)

        df = snapshot.ohlcv("WETH", timeframe="1h", limit=5, pool_address=self.BASE_WETH_USDC_POOL)

        assert len(df) == 5
        assert df.attrs["source"].endswith(":pool_pair_proxy")
        # Served, not a decision-input failure.
        assert not snapshot._critical_data_failures


class TestPairRatioCandles:
    def test_non_stable_quote_serves_ratio(self):
        engine = _engine_with_series("WETH", [3000.0] * 20)
        for value in [0.05] * 20:
            engine.append_price("CBBTC", Decimal(str(value)))
        view = _view(engine)

        df = view.get_ohlcv("WETH/CBBTC", timeframe="1h", limit=5)

        assert df.attrs["quote"] == "CBBTC"
        assert df.attrs["source"].endswith(":pair_ratio")
        assert all(abs(c - 3000.0 / 0.05) < 1e-6 for c in df["close"])

    def test_stable_quote_keeps_usd_series(self):
        view = _view(_engine_with_series("WETH", [3000.0] * 20))
        df = view.get_ohlcv("WETH/USDC", timeframe="1h", limit=5)
        assert df.attrs["quote"] == "USD"
        assert all(c == 3000.0 for c in df["close"])
