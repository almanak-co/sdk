"""Unit tests for BacktestIndicatorEngine.

Tests cover:
- Initialization with default and custom indicators
- Price buffer management (append, get_buffer_size, reset)
- RSI population on MarketSnapshot
- MACD population on MarketSnapshot
- Bollinger Bands population on MarketSnapshot
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
from almanak.framework.strategies.intent_strategy import (
    BollingerBandsData,
    MACDData,
    MarketSnapshot,
    RSIData,
)


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
        """Default should include rsi, macd, bollinger_bands."""
        engine = BacktestIndicatorEngine()
        assert engine._required == DEFAULT_INDICATORS
        assert "rsi" in engine._required
        assert "macd" in engine._required
        assert "bollinger_bands" in engine._required

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
