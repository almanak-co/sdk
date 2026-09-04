"""Overlay semantics for a GMX market prepared at first use.

A market an intent names mid-run cannot be re-wrapped into the streaming
provider, so the connector's prepared source is merged onto each tick's
``MarketState`` instead. Two properties decide whether that overlay is
equivalent to a market declared before the run: the venue series must WIN for
the index token (a spot price for the same identity is not the venue's mark),
and each published observation must carry the timestamp of the candle it came
from, not the tick it was read on.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.connectors.gmx_v2.backtest_prices import GMXOracleDataProvider, _GMXOracleMarketSource
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import OHLCV, MarketState, normalize_token_key, token_ref_display
from almanak.framework.backtesting.pnl.engine import PnLBacktester
from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine

CHAIN = "arbitrum"
MARKET_TOKEN = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
INDEX_TOKEN = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
START = datetime(2025, 1, 1, tzinfo=UTC)


def _hourly_source() -> _GMXOracleMarketSource:
    source = _GMXOracleMarketSource(chain=CHAIN, market="ETH/USD", venue="gmx_v2")
    source.market_token = MARKET_TOKEN
    source.index_token = INDEX_TOKEN
    source.index_symbol = "ETH"
    source.resolved_market = "ETH/USD"
    candles = [
        OHLCV(
            timestamp=START + timedelta(hours=hour),
            open=Decimal("2000"),
            high=Decimal("2000"),
            low=Decimal("2000"),
            close=Decimal("2000") + Decimal(hour),
            volume=Decimal("0"),
        )
        for hour in range(3)
    ]
    source.install_series(timeframe="1h", series=candles, start=START, end=START + timedelta(hours=2))
    return source


def _provider_with(source: _GMXOracleMarketSource) -> GMXOracleDataProvider:
    provider = GMXOracleDataProvider.__new__(GMXOracleDataProvider)
    provider._chain = CHAIN  # noqa: SLF001 — constructing the prepared shape without the gateway
    provider._sources = [source]  # noqa: SLF001
    return provider


def _state(timestamp: datetime, prices: dict[str, Decimal] | None = None) -> MarketState:
    return MarketState(timestamp=timestamp, prices=dict(prices or {}), chain=CHAIN)


def test_overlay_publishes_the_venue_price_for_the_index_token() -> None:
    source = _hourly_source()
    key = normalize_token_key(CHAIN, INDEX_TOKEN)
    state = _state(START + timedelta(hours=1), {key: Decimal("1234")})

    _provider_with(source).overlay_market_state(state)

    assert state.prices[key] == Decimal("2001")
    observation = state.price_observations[key]
    assert observation.source == "gmx_oracle_candles"
    assert observation.price == Decimal("2001")


def test_overlay_stamps_the_candle_timestamp_not_the_tick() -> None:
    """A tick between candles must not make an older mark look freshly measured."""
    source = _hourly_source()
    key = normalize_token_key(CHAIN, INDEX_TOKEN)
    tick = START + timedelta(hours=1, minutes=30)
    state = _state(tick)

    _provider_with(source).overlay_market_state(state)

    assert state.price_observations[key].timestamp == START + timedelta(hours=1)
    assert state.price_observations[key].timestamp != tick


def test_overlay_leaves_a_tick_before_the_series_untouched() -> None:
    source = _hourly_source()
    key = normalize_token_key(CHAIN, INDEX_TOKEN)
    state = _state(START - timedelta(hours=1), {key: Decimal("1234")})

    _provider_with(source).overlay_market_state(state)

    assert state.prices[key] == Decimal("1234")
    assert key not in state.price_observations


def test_overlay_records_provenance_for_the_run_manifest() -> None:
    source = _hourly_source()
    state = _state(START + timedelta(hours=2))

    _provider_with(source).overlay_market_state(state)

    entries = state.metadata["gmx_oracles_first_use"]
    assert [entry["source"] for entry in entries] == ["gmx_oracle_candles"]
    assert entries[0]["market_token"] == MARKET_TOKEN


@pytest.mark.parametrize("hour", [0, 1, 2])
def test_overlay_tracks_the_candle_in_force_at_each_tick(hour: int) -> None:
    source = _hourly_source()
    key = normalize_token_key(CHAIN, INDEX_TOKEN)
    state = _state(START + timedelta(hours=hour))

    _provider_with(source).overlay_market_state(state)

    assert state.prices[key] == Decimal("2000") + Decimal(hour)


def test_lazy_history_matches_declared_tick_sampling_without_current_tick_lookahead() -> None:
    provider = _provider_with(_hourly_source())
    key = token_ref_display(normalize_token_key(CHAIN, INDEX_TOKEN))

    history = provider.tick_close_history_before(
        start_time=START,
        end_time=START + timedelta(hours=2),
        interval_seconds=1800,
    )

    # 00:00, 00:30, 01:00, 01:30. The 02:00 close belongs to the active
    # tick and is intentionally left for the normal loop to append once.
    assert history == {key: [Decimal("2000"), Decimal("2000"), Decimal("2001"), Decimal("2001")]}


def test_engine_replaces_only_owned_fallback_history_and_promotes_native_cadence() -> None:
    provider = _provider_with(_hourly_source())
    provider.measured_granularity_seconds = 14_400
    key = token_ref_display(normalize_token_key(CHAIN, INDEX_TOKEN))
    indicators = BacktestIndicatorEngine()
    indicators.append_price(key, Decimal("999"))
    indicators.append_price(key, Decimal("998"))
    indicators.append_price("USDC", Decimal("1"))
    indicators.set_data_granularity(3600, 3600)

    backtester = PnLBacktester(data_provider=object(), fee_models={}, slippage_models={})
    backtester._active_indicator_engine = indicators
    config = PnLBacktestConfig(
        start_time=START,
        end_time=START + timedelta(hours=3),
        interval_seconds=3600,
        chain=CHAIN,
    )

    backtester._backfill_first_use_perp_history(provider, _state(START + timedelta(hours=2)), config)

    assert list(indicators._price_buffers[key]) == [Decimal("2000"), Decimal("2001")]
    assert list(indicators._price_buffers["USDC"]) == [Decimal("1")]
    assert indicators._data_granularity_seconds == 14_400
