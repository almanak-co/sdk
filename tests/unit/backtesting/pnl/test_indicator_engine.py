"""Unit tests for BacktestIndicatorEngine.

Tests cover:
- Initialization with default and custom indicators
- Price buffer management (append, get_buffer_size, reset)
- RSI population on MarketSnapshot
- MACD population on MarketSnapshot
- Bollinger Bands population on MarketSnapshot
- ATR population on MarketSnapshot
- Handling of insufficient data (graceful skip)
- Config-driven indicator parameters
- Unknown indicator warnings
"""

from __future__ import annotations

import math
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.backtesting.pnl.indicator_engine import (
    DEFAULT_INDICATORS,
    DEFAULT_MAX_HISTORY,
    BacktestIndicatorEngine,
)
from almanak.framework.market import ATRData, BollingerBandsData, MACDData, MarketSnapshot, RSIData

# =============================================================================
# Helpers
# =============================================================================


def _make_snapshot() -> MarketSnapshot:
    """Create a minimal MarketSnapshot for testing."""
    return MarketSnapshot(chain="arbitrum", wallet_address="0x" + "0" * 40)


def _generate_prices(base: float, count: int, volatility: float = 10.0) -> list[Decimal]:
    """Generate a list of semi-realistic prices for testing."""
    prices = []
    for i in range(count):
        price = base + volatility * math.sin(i * 0.5)
        prices.append(Decimal(str(round(price, 2))))
    return prices


def _create_engine_with_prices(
    token: str, prices: list[Decimal], indicators: set[str] | None = None
) -> BacktestIndicatorEngine:
    """Create an engine and append prices for a token."""
    engine = BacktestIndicatorEngine(required_indicators=indicators)
    for price in prices:
        engine.append_price(token, price)
    return engine


# =============================================================================
# Initialization Tests
# =============================================================================


class TestBacktestIndicatorEngineInit:
    """Tests for initialization and configuration."""

    def test_default_indicators(self) -> None:
        """Default should include rsi, macd, bollinger_bands, atr."""
        engine = BacktestIndicatorEngine()
        assert engine._required == DEFAULT_INDICATORS
        assert "rsi" in engine._required
        assert "macd" in engine._required
        assert "bollinger_bands" in engine._required
        assert "atr" in engine._required

    def test_custom_indicators(self) -> None:
        """Accept custom indicator set."""
        engine = BacktestIndicatorEngine(required_indicators={"rsi"})
        assert engine._required == frozenset({"rsi"})

    def test_default_max_history(self) -> None:
        """Default max history should be DEFAULT_MAX_HISTORY."""
        engine = BacktestIndicatorEngine()
        assert engine._max_history == DEFAULT_MAX_HISTORY

    def test_custom_max_history(self) -> None:
        """Accept custom max history."""
        engine = BacktestIndicatorEngine(max_history=50)
        assert engine._max_history == 50

    def test_unknown_indicator_warning(self) -> None:
        """Unknown indicators should log a warning."""
        with patch("almanak.framework.backtesting.pnl.indicator_engine.logger") as mock_logger:
            BacktestIndicatorEngine(required_indicators={"rsi", "vwap"})
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args[0]
            assert "vwap" in call_args[1]

    def test_empty_price_buffers_on_init(self) -> None:
        """Price buffers should be empty at initialization."""
        engine = BacktestIndicatorEngine()
        assert len(engine._price_buffers) == 0


# =============================================================================
# Price Buffer Tests
# =============================================================================


class TestPriceBuffer:
    """Tests for price buffer management."""

    def test_append_price_creates_buffer(self) -> None:
        """First price for a token should create a buffer."""
        engine = BacktestIndicatorEngine()
        engine.append_price("WETH", Decimal("3500.00"))
        assert "WETH" in engine._price_buffers
        assert engine.get_buffer_size("WETH") == 1

    def test_append_multiple_prices(self) -> None:
        """Multiple prices should accumulate in the buffer."""
        engine = BacktestIndicatorEngine()
        for i in range(10):
            engine.append_price("WETH", Decimal(str(3000 + i * 10)))
        assert engine.get_buffer_size("WETH") == 10

    def test_buffer_respects_max_history(self) -> None:
        """Buffer should not exceed max_history."""
        engine = BacktestIndicatorEngine(max_history=5)
        for i in range(10):
            engine.append_price("WETH", Decimal(str(3000 + i)))
        assert engine.get_buffer_size("WETH") == 5

    def test_multiple_tokens(self) -> None:
        """Each token should have its own buffer."""
        engine = BacktestIndicatorEngine()
        engine.append_price("WETH", Decimal("3500"))
        engine.append_price("USDC", Decimal("1.00"))
        assert engine.get_buffer_size("WETH") == 1
        assert engine.get_buffer_size("USDC") == 1

    def test_get_buffer_size_unknown_token(self) -> None:
        """Unknown token should return 0."""
        engine = BacktestIndicatorEngine()
        assert engine.get_buffer_size("UNKNOWN") == 0

    def test_reset_clears_buffers(self) -> None:
        """Reset should clear all price buffers."""
        engine = BacktestIndicatorEngine()
        engine.append_price("WETH", Decimal("3500"))
        engine.append_price("USDC", Decimal("1.00"))
        engine.reset()
        assert engine.get_buffer_size("WETH") == 0
        assert engine.get_buffer_size("USDC") == 0
        assert len(engine._price_buffers) == 0


# =============================================================================
# Snapshot Population Tests
# =============================================================================


class TestRSIPopulation:
    """Tests for RSI indicator population."""

    def test_rsi_populated_with_sufficient_data(self) -> None:
        """RSI should be set on snapshot when enough data is available."""
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"rsi"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        rsi_data = snapshot.rsi("WETH")
        assert isinstance(rsi_data, RSIData)
        assert Decimal("0") <= rsi_data.value <= Decimal("100")
        assert rsi_data.period == 14  # default period

    def test_rsi_skipped_with_insufficient_data(self) -> None:
        """RSI should not be set when not enough data is available."""
        prices = _generate_prices(3500.0, 5)  # Only 5 prices, need 14+1
        engine = _create_engine_with_prices("WETH", prices, {"rsi"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        # Accessing RSI without data should raise ValueError
        with pytest.raises(ValueError):
            snapshot.rsi("WETH")

    def test_rsi_custom_period(self) -> None:
        """RSI should respect custom period from config."""
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"rsi"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot, config={"rsi_period": 7})

        rsi_data = snapshot.rsi("WETH", period=7)
        assert rsi_data.period == 7

    def test_rsi_not_computed_when_not_required(self) -> None:
        """RSI should not be computed if not in required indicators."""
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"macd"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        with pytest.raises(ValueError):
            snapshot.rsi("WETH")


class TestMACDPopulation:
    """Tests for MACD indicator population."""

    def test_macd_populated_with_sufficient_data(self) -> None:
        """MACD should be set on snapshot when enough data is available."""
        prices = _generate_prices(3500.0, 50)  # Need 26 + 9 = 35 minimum
        engine = _create_engine_with_prices("WETH", prices, {"macd"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        macd_data = snapshot.macd("WETH")
        assert isinstance(macd_data, MACDData)
        assert macd_data.fast_period == 12
        assert macd_data.slow_period == 26
        assert macd_data.signal_period == 9

    def test_macd_skipped_with_insufficient_data(self) -> None:
        """MACD should not be set when not enough data is available."""
        prices = _generate_prices(3500.0, 10)
        engine = _create_engine_with_prices("WETH", prices, {"macd"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        with pytest.raises(ValueError):
            snapshot.macd("WETH")

    def test_macd_custom_periods(self) -> None:
        """MACD should respect custom periods from config."""
        prices = _generate_prices(3500.0, 50)
        engine = _create_engine_with_prices("WETH", prices, {"macd"})

        snapshot = _make_snapshot()
        config = {"macd_fast": 8, "macd_slow": 21, "macd_signal": 5}
        engine.populate_snapshot(snapshot, config=config)

        macd_data = snapshot.macd("WETH", fast_period=8, slow_period=21, signal_period=5)
        assert macd_data.fast_period == 8
        assert macd_data.slow_period == 21
        assert macd_data.signal_period == 5


class TestBollingerBandsPopulation:
    """Tests for Bollinger Bands indicator population."""

    def test_bollinger_populated_with_sufficient_data(self) -> None:
        """Bollinger Bands should be set on snapshot when enough data is available."""
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"bollinger_bands"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        bb_data = snapshot.bollinger_bands("WETH")
        assert isinstance(bb_data, BollingerBandsData)
        assert bb_data.period == 20
        assert bb_data.std_dev == 2.0
        # Upper band should be above middle, lower should be below
        assert bb_data.upper_band >= bb_data.middle_band
        assert bb_data.lower_band <= bb_data.middle_band

    def test_bollinger_skipped_with_insufficient_data(self) -> None:
        """Bollinger Bands should not be set when not enough data is available."""
        prices = _generate_prices(3500.0, 5)
        engine = _create_engine_with_prices("WETH", prices, {"bollinger_bands"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        with pytest.raises(ValueError):
            snapshot.bollinger_bands("WETH")

    def test_bollinger_custom_params(self) -> None:
        """Bollinger Bands should respect custom parameters from config."""
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"bollinger_bands"})

        snapshot = _make_snapshot()
        config = {"bb_period": 10, "bb_std_dev": 1.5}
        engine.populate_snapshot(snapshot, config=config)

        bb_data = snapshot.bollinger_bands("WETH", period=10, std_dev=1.5)
        assert bb_data.period == 10
        assert bb_data.std_dev == 1.5


class TestATRPopulation:
    """Tests for ATR indicator population."""

    def test_atr_populated_with_sufficient_data(self) -> None:
        """ATR should be set on snapshot when enough data is available."""
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"atr"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        atr_data = snapshot.atr("WETH")
        assert isinstance(atr_data, ATRData)
        assert atr_data.value > Decimal("0")
        assert atr_data.value_percent > Decimal("0")
        assert atr_data.period == 14  # default period

    def test_atr_skipped_with_insufficient_data(self) -> None:
        """ATR should not be set when not enough data is available."""
        prices = _generate_prices(3500.0, 5)  # Only 5 prices, need 14+1
        engine = _create_engine_with_prices("WETH", prices, {"atr"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        with pytest.raises(ValueError):
            snapshot.atr("WETH")

    def test_atr_custom_period(self) -> None:
        """ATR should respect custom period from config."""
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"atr"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot, config={"atr_period": 7})

        atr_data = snapshot.atr("WETH", period=7)
        assert atr_data.period == 7

    def test_atr_not_computed_when_not_required(self) -> None:
        """ATR should not be computed if not in required indicators."""
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"rsi"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        with pytest.raises(ValueError):
            snapshot.atr("WETH")

    def test_atr_value_percent_calculation(self) -> None:
        """ATR value_percent should be ATR/price * 100."""
        # Use constant-ish prices with known volatility
        prices = [Decimal("100"), Decimal("110"), Decimal("100"), Decimal("110")]
        # Add more prices to reach the 15 required (period 14 + 1)
        for i in range(20):
            prices.append(Decimal("100") if i % 2 == 0 else Decimal("110"))

        engine = _create_engine_with_prices("TEST", prices, {"atr"})
        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        atr_data = snapshot.atr("TEST")
        # ATR value_percent should be approximately ATR/current_price * 100
        expected_pct = float(atr_data.value) / float(prices[-1]) * 100
        assert float(atr_data.value_percent) == pytest.approx(expected_pct, rel=0.01)


class TestAllIndicators:
    """Tests for computing all indicators together."""

    def test_all_default_indicators(self) -> None:
        """All default indicators should populate when sufficient data exists."""
        prices = _generate_prices(3500.0, 50)
        engine = _create_engine_with_prices("WETH", prices)  # defaults

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        # All should be populated
        assert isinstance(snapshot.rsi("WETH"), RSIData)
        assert isinstance(snapshot.macd("WETH"), MACDData)
        assert isinstance(snapshot.bollinger_bands("WETH"), BollingerBandsData)
        assert isinstance(snapshot.atr("WETH"), ATRData)

    def test_multiple_tokens(self) -> None:
        """Indicators should be computed independently for each token."""
        engine = BacktestIndicatorEngine(required_indicators={"rsi"})

        # Add enough prices for both tokens
        for price in _generate_prices(3500.0, 30):
            engine.append_price("WETH", price)
        for price in _generate_prices(1.0, 30, volatility=0.01):
            engine.append_price("USDC", price)

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        weth_rsi = snapshot.rsi("WETH")
        usdc_rsi = snapshot.rsi("USDC")
        assert isinstance(weth_rsi, RSIData)
        assert isinstance(usdc_rsi, RSIData)

    def test_none_config_uses_defaults(self) -> None:
        """Passing None config should use default indicator parameters."""
        prices = _generate_prices(3500.0, 50)
        engine = _create_engine_with_prices("WETH", prices)

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot, config=None)

        rsi_data = snapshot.rsi("WETH")
        assert rsi_data.period == 14  # default RSI period

    def test_active_tokens_filter(self) -> None:
        """Only tokens in active_tokens should get indicators populated."""
        engine = BacktestIndicatorEngine(required_indicators={"rsi", "atr"})

        for price in _generate_prices(3500.0, 30):
            engine.append_price("WETH", price)
        for price in _generate_prices(1.0, 30, volatility=0.01):
            engine.append_price("USDC", price)

        snapshot = _make_snapshot()
        # Only populate WETH indicators
        engine.populate_snapshot(snapshot, active_tokens={"WETH"})

        # WETH should have indicators
        assert isinstance(snapshot.rsi("WETH"), RSIData)
        assert isinstance(snapshot.atr("WETH"), ATRData)

        # USDC should NOT have indicators (not in active_tokens)
        with pytest.raises(ValueError):
            snapshot.rsi("USDC")

    def test_address_key_populates_snapshot_indicators(self) -> None:
        """Address-keyed buffers should be readable through bare address queries."""
        addr = "0x5979d7b546e38e414f7e9822514be443a4800529"
        token = f"arbitrum:{addr}"
        engine = BacktestIndicatorEngine(required_indicators={"rsi", "bollinger_bands", "ema"})

        for price in _generate_prices(3500.0, 50):
            engine.append_price(token, price)

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot, config={"ema_period": 12}, active_tokens={token})

        assert isinstance(snapshot.rsi(addr), RSIData)
        assert isinstance(snapshot.bollinger_bands(addr), BollingerBandsData)
        assert snapshot.ema(addr, period=12).period == 12


# =============================================================================
# ATR Calculator from_prices Tests
# =============================================================================


class TestATRCalculatorFromPrices:
    """Tests for ATRCalculator.calculate_atr_from_prices static method."""

    def test_basic_calculation(self) -> None:
        """ATR from prices should return a positive value."""
        from almanak.framework.data.indicators.atr import ATRCalculator

        prices = _generate_prices(3500.0, 30)
        atr = ATRCalculator.calculate_atr_from_prices(prices, period=14)
        assert atr > 0

    def test_insufficient_data_raises(self) -> None:
        """Should raise InsufficientDataError with too few prices."""
        from almanak.framework.data.indicators.atr import ATRCalculator
        from almanak.framework.data.interfaces import InsufficientDataError

        prices = _generate_prices(3500.0, 5)
        with pytest.raises(InsufficientDataError):
            ATRCalculator.calculate_atr_from_prices(prices, period=14)

    def test_constant_prices_zero_atr(self) -> None:
        """Constant prices should produce zero ATR."""
        from almanak.framework.data.indicators.atr import ATRCalculator

        prices = [Decimal("100")] * 20
        atr = ATRCalculator.calculate_atr_from_prices(prices, period=14)
        assert atr == pytest.approx(0.0, abs=1e-10)

    def test_known_volatility(self) -> None:
        """Alternating prices should produce predictable ATR."""
        from almanak.framework.data.indicators.atr import ATRCalculator

        # Alternating between 100 and 110: TR = 10 every step
        prices = [Decimal("100") if i % 2 == 0 else Decimal("110") for i in range(20)]
        atr = ATRCalculator.calculate_atr_from_prices(prices, period=5)
        # All TRs are 10, so ATR should be approximately 10
        assert atr == pytest.approx(10.0, rel=0.01)

    def test_custom_period(self) -> None:
        """Different periods should produce different ATR values."""
        from almanak.framework.data.indicators.atr import ATRCalculator

        prices = _generate_prices(3500.0, 50)
        atr_7 = ATRCalculator.calculate_atr_from_prices(prices, period=7)
        atr_14 = ATRCalculator.calculate_atr_from_prices(prices, period=14)
        # Both should be positive but different
        assert atr_7 > 0
        assert atr_14 > 0


# ---------------------------------------------------------------------------
# Warm-up methods
# ---------------------------------------------------------------------------


class TestMinWarmupTicks:
    """Tests for min_warmup_ticks() method."""

    def test_defaults_rsi(self):
        engine = BacktestIndicatorEngine(required_indicators={"rsi"})
        assert engine.min_warmup_ticks() == 15  # default rsi_period=14, +1

    def test_defaults_macd(self):
        engine = BacktestIndicatorEngine(required_indicators={"macd"})
        assert engine.min_warmup_ticks() == 34  # 26 + 9 - 1

    def test_defaults_bollinger(self):
        engine = BacktestIndicatorEngine(required_indicators={"bollinger_bands"})
        assert engine.min_warmup_ticks() == 20  # default bb_period=20

    def test_defaults_atr(self):
        engine = BacktestIndicatorEngine(required_indicators={"atr"})
        assert engine.min_warmup_ticks() == 15  # default atr_period=14, +1

    def test_all_indicators_returns_max(self):
        engine = BacktestIndicatorEngine(required_indicators={"rsi", "macd", "bollinger_bands", "atr"})
        # MACD has the largest requirement (34)
        assert engine.min_warmup_ticks() == 34

    def test_custom_config(self):
        engine = BacktestIndicatorEngine(required_indicators={"rsi", "macd"})
        config = {"rsi_period": 20, "macd_slow": 50, "macd_signal": 12}
        # RSI: 20+1=21, MACD: 50+12-1=61
        assert engine.min_warmup_ticks(config) == 61

    def test_no_indicators(self):
        engine = BacktestIndicatorEngine(required_indicators=set())
        assert engine.min_warmup_ticks() == 0


class TestIsWarmingUp:
    """Tests for is_warming_up() method."""

    def test_warming_up_when_buffer_empty(self):
        engine = BacktestIndicatorEngine(required_indicators={"rsi"})
        assert engine.is_warming_up("ETH") is True

    def test_warming_up_with_insufficient_data(self):
        engine = BacktestIndicatorEngine(required_indicators={"rsi"})
        # Add 10 points (need 15 for RSI-14)
        for i in range(10):
            engine.append_price("ETH", Decimal(str(3000 + i)))
        assert engine.is_warming_up("ETH") is True

    def test_not_warming_up_with_sufficient_data(self):
        engine = BacktestIndicatorEngine(required_indicators={"rsi"})
        # Add 15 points (exactly enough for RSI-14)
        for i in range(15):
            engine.append_price("ETH", Decimal(str(3000 + i)))
        assert engine.is_warming_up("ETH") is False

    def test_not_warming_up_no_indicators(self):
        engine = BacktestIndicatorEngine(required_indicators=set())
        assert engine.is_warming_up("ETH") is False


# =============================================================================
# EMA population (VIB: address/EMA backtest support)
# =============================================================================


class TestEmaPopulation:
    """EMA must be computed for the periods a ta_swap-style strategy declares."""

    def test_ema_populated_for_fast_and_slow_periods(self) -> None:
        prices = _generate_prices(3500.0, 60)
        engine = _create_engine_with_prices("WETH", prices, {"ema"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot, config={"ema_fast_period": 21, "ema_slow_period": 55})

        fast = snapshot.ema("WETH", period=21)
        slow = snapshot.ema("WETH", period=55)
        assert fast.ma_type == "EMA" and fast.period == 21 and fast.value > 0
        assert slow.ma_type == "EMA" and slow.period == 55 and slow.value > 0

    def test_ema_in_default_indicator_set(self) -> None:
        assert "ema" in DEFAULT_INDICATORS

    def test_ema_skipped_for_period_with_insufficient_history(self) -> None:
        # 30 prices: EMA-21 resolves, EMA-55 does not.
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"ema"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot, config={"ema_fast_period": 21, "ema_slow_period": 55})

        assert snapshot.ema("WETH", period=21).period == 21
        with pytest.raises(ValueError):
            snapshot.ema("WETH", period=55)

    def test_ema_periods_from_config_dedupes_and_filters(self) -> None:
        periods = BacktestIndicatorEngine._ema_periods_from_config(
            {"ema_period": 9, "ema_fast_period": 9, "ema_slow_period": 21, "ema_periods": [21, 0, 50]}
        )
        assert periods == [9, 21, 50]


class TestEmaDefaultAndWarmup:
    """A bare `market.ema(token)` (snapshot default period=12) must resolve, and
    `min_warmup_ticks` must account for the largest EMA period."""

    def test_default_ema_12_when_no_period_configured(self) -> None:
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"ema"})

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot, config={})  # no ema_* keys

        ema = snapshot.ema("WETH")  # default period=12
        assert ema.ma_type == "EMA" and ema.period == 12 and ema.value > 0

    def test_ema_periods_default_to_twelve_when_unconfigured(self) -> None:
        assert BacktestIndicatorEngine._ema_periods_from_config({}) == [12]

    def test_min_warmup_ticks_includes_largest_ema_period(self) -> None:
        engine = BacktestIndicatorEngine(required_indicators={"ema"})
        # ema_slow_period dominates RSI/MACD/BB/ATR defaults.
        assert engine.min_warmup_ticks({"ema_fast_period": 21, "ema_slow_period": 55}) == 55

    def test_ema_only_strategy_warms_up_until_slow_period(self) -> None:
        engine = BacktestIndicatorEngine(required_indicators={"ema"})
        cfg = {"ema_fast_period": 21, "ema_slow_period": 55}
        for i in range(54):
            engine.append_price("ETH", Decimal(str(3000 + i)))
        assert engine.is_warming_up("ETH", cfg) is True  # 54 < 55
        engine.append_price("ETH", Decimal("3055"))
        assert engine.is_warming_up("ETH", cfg) is False  # 55 >= 55


class TestGranularityHonesty:
    """ALM-2957 backstop: indicators finer than the DATA's measured resolution
    refuse instead of serving values computed from flat upsampled ticks."""

    @staticmethod
    def _daily_under_hourly_engine() -> BacktestIndicatorEngine:
        # 10 daily prices upsampled onto an hourly tick grid — the exact
        # plane that pinned RSI at ~0/100 for months on staging.
        daily = [3000.0, 3050.0, 2990.0, 2940.0, 2970.0, 3010.0, 2950.0, 2900.0, 2930.0, 2960.0]
        hourly = [p for p in daily for _ in range(24)]
        engine = _create_engine_with_prices("WETH", hourly, {"rsi"})
        engine.set_data_granularity(86400, 3600)
        return engine

    def test_on_demand_finer_than_data_refuses(self) -> None:
        engine = self._daily_under_hourly_engine()
        rsi_provider, _ = engine.snapshot_providers(None, 3600)

        with pytest.raises(ValueError, match="resolution"):
            rsi_provider("WETH", period=14, timeframe="1h")
        with pytest.raises(ValueError, match="ALM-2957"):
            rsi_provider("WETH", period=14)  # default = tick timeframe

    def test_on_demand_at_data_resolution_serves(self) -> None:
        engine = self._daily_under_hourly_engine()
        rsi_provider, _ = engine.snapshot_providers(None, 3600)

        # "1d" resamples the buffer back to the REAL daily closes — served.
        rsi = rsi_provider("WETH", period=5, timeframe="1d")
        assert Decimal("0") <= rsi.value <= Decimal("100")

    def test_eager_population_skipped_when_degenerate(self) -> None:
        engine = self._daily_under_hourly_engine()
        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)

        with pytest.raises(ValueError):
            snapshot.rsi("WETH")

    def test_matching_granularity_is_unchanged(self) -> None:
        prices = _generate_prices(3500.0, 30)
        engine = _create_engine_with_prices("WETH", prices, {"rsi"})
        engine.set_data_granularity(3600, 3600)

        snapshot = _make_snapshot()
        engine.populate_snapshot(snapshot)
        assert snapshot.rsi("WETH").period == 14

        rsi_provider, _ = engine.snapshot_providers(None, 3600)
        rsi = rsi_provider("WETH", period=14, timeframe="1h")
        assert Decimal("0") <= rsi.value <= Decimal("100")

    def test_retention_scales_so_native_indicators_can_warm_up(self) -> None:
        # Review round (#3311, Codex): with daily data under hourly ticks the
        # default 200-tick buffer resamples to ~8 daily bars — the "request
        # 1d instead" fallback could never warm a realistic RSI(14). The
        # granularity handoff must scale retention by the coarseness ratio.
        engine = BacktestIndicatorEngine(required_indicators={"rsi"})
        engine.set_data_granularity(86400, 3600)

        # 20 days of daily prices upsampled onto the hourly grid (480 ticks —
        # far beyond the unscaled 200-tick cap).
        daily = [3000.0 + 13 * ((i * 7) % 11) for i in range(20)]
        for price in daily:
            for _ in range(24):
                engine.append_price("WETH", Decimal(str(price)))

        rsi_provider, _ = engine.snapshot_providers(None, 3600)
        rsi = rsi_provider("WETH", period=14, timeframe="1d")
        assert Decimal("0") <= rsi.value <= Decimal("100")

    def test_retention_scaling_is_idempotent(self) -> None:
        engine = BacktestIndicatorEngine(required_indicators={"rsi"})
        engine.set_data_granularity(86400, 3600)
        first = engine._max_history
        engine.set_data_granularity(86400, 3600)
        assert engine._max_history == first  # a second call must not compound
