"""Integration tests for perp backtest adapter functionality.

This module tests the PerpBacktestAdapter, focusing on:
- Funding accumulation over time
- Liquidation triggered by price moves
- Funding rate configuration
- Margin validation
- Position valuation with unrealized PnL
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.adapters.perp_adapter import (
    PerpBacktestAdapter,
    PerpBacktestConfig,
)
from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPosition,
)

# =============================================================================
# Mock Classes
# =============================================================================


@dataclass
class MockMarketState:
    """Mock market state for testing."""

    prices: dict[str, Decimal] = field(default_factory=dict)

    def get_price(self, token: str) -> Decimal | None:
        """Get price for a token."""
        if token not in self.prices:
            raise KeyError(f"Price not found for {token}")
        return self.prices.get(token)

    def get_prices(self, tokens: list[str]) -> dict[str, Decimal]:
        """Get prices for multiple tokens."""
        return {t: self.get_price(t) for t in tokens if t in self.prices}


def create_perp_long_position(
    token: str = "ETH",
    collateral_usd: Decimal = Decimal("10000"),
    leverage: Decimal = Decimal("5"),
    entry_price: Decimal = Decimal("2000"),
    entry_time: datetime | None = None,
    protocol: str = "gmx",
) -> SimulatedPosition:
    """Create a mock perp long position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    return SimulatedPosition.perp_long(
        token=token,
        collateral_usd=collateral_usd,
        leverage=leverage,
        entry_price=entry_price,
        entry_time=entry_time,
        protocol=protocol,
    )


def create_perp_short_position(
    token: str = "ETH",
    collateral_usd: Decimal = Decimal("10000"),
    leverage: Decimal = Decimal("5"),
    entry_price: Decimal = Decimal("2000"),
    entry_time: datetime | None = None,
    protocol: str = "gmx",
) -> SimulatedPosition:
    """Create a mock perp short position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    return SimulatedPosition.perp_short(
        token=token,
        collateral_usd=collateral_usd,
        leverage=leverage,
        entry_price=entry_price,
        entry_time=entry_time,
        protocol=protocol,
    )


# =============================================================================
# PerpBacktestConfig Tests
# =============================================================================


class TestPerpBacktestConfig:
    """Tests for PerpBacktestConfig."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = PerpBacktestConfig(strategy_type="perp")

        assert config.strategy_type == "perp"
        assert config.funding_application_frequency == "hourly"
        assert config.liquidation_model_enabled is True
        assert config.initial_margin_ratio == Decimal("0.1")
        assert config.maintenance_margin_ratio == Decimal("0.05")
        assert config.default_funding_rate == Decimal("0.0001")
        assert config.funding_rate_source == "fixed"
        assert config.liquidation_warning_threshold == Decimal("0.10")
        assert config.liquidation_critical_threshold == Decimal("0.05")
        assert config.liquidation_penalty == Decimal("0.05")
        assert config.protocol == "gmx"

    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="continuous",
            liquidation_model_enabled=False,
            initial_margin_ratio=Decimal("0.05"),
            maintenance_margin_ratio=Decimal("0.02"),
            default_funding_rate=Decimal("0.0002"),
            funding_rate_source="protocol",
            liquidation_penalty=Decimal("0.10"),
            protocol="hyperliquid",
        )

        assert config.funding_application_frequency == "continuous"
        assert config.liquidation_model_enabled is False
        assert config.initial_margin_ratio == Decimal("0.05")
        assert config.maintenance_margin_ratio == Decimal("0.02")
        assert config.default_funding_rate == Decimal("0.0002")
        assert config.funding_rate_source == "protocol"
        assert config.liquidation_penalty == Decimal("0.10")
        assert config.protocol == "hyperliquid"

    def test_invalid_strategy_type(self) -> None:
        """Test validation rejects non-perp strategy type."""
        with pytest.raises(ValueError, match="requires strategy_type='perp'"):
            PerpBacktestConfig(strategy_type="lp")

    def test_invalid_funding_frequency(self) -> None:
        """Test validation rejects invalid funding frequency."""
        with pytest.raises(ValueError, match="funding_application_frequency must be one of"):
            PerpBacktestConfig(strategy_type="perp", funding_application_frequency="daily")  # type: ignore[arg-type]

    def test_invalid_funding_rate_source(self) -> None:
        """Test validation rejects invalid funding rate source."""
        with pytest.raises(ValueError, match="funding_rate_source must be one of"):
            PerpBacktestConfig(strategy_type="perp", funding_rate_source="invalid")  # type: ignore[arg-type]

    def test_invalid_margin_ratios(self) -> None:
        """Test validation rejects invalid margin ratios."""
        # Zero initial margin
        with pytest.raises(ValueError, match="initial_margin_ratio must be > 0"):
            PerpBacktestConfig(strategy_type="perp", initial_margin_ratio=Decimal("0"))

        # Zero maintenance margin
        with pytest.raises(ValueError, match="maintenance_margin_ratio must be > 0"):
            PerpBacktestConfig(strategy_type="perp", maintenance_margin_ratio=Decimal("0"))

        # Maintenance > Initial
        with pytest.raises(ValueError, match="maintenance_margin_ratio .* cannot exceed"):
            PerpBacktestConfig(
                strategy_type="perp",
                initial_margin_ratio=Decimal("0.05"),
                maintenance_margin_ratio=Decimal("0.10"),
            )

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="8h",
            default_funding_rate=Decimal("0.0002"),
        )

        d = config.to_dict()

        assert d["strategy_type"] == "perp"
        assert d["funding_application_frequency"] == "8h"
        assert d["default_funding_rate"] == "0.0002"

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "strategy_type": "perp",
            "funding_application_frequency": "continuous",
            "default_funding_rate": "0.00015",
            "liquidation_penalty": "0.08",
        }

        config = PerpBacktestConfig.from_dict(data)

        assert config.strategy_type == "perp"
        assert config.funding_application_frequency == "continuous"
        assert config.default_funding_rate == Decimal("0.00015")
        assert config.liquidation_penalty == Decimal("0.08")

    def test_roundtrip_serialization(self) -> None:
        """Test config survives roundtrip serialization."""
        original = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="8h",
            initial_margin_ratio=Decimal("0.08"),
            liquidation_penalty=Decimal("0.07"),
        )

        restored = PerpBacktestConfig.from_dict(original.to_dict())

        assert restored.strategy_type == original.strategy_type
        assert restored.funding_application_frequency == original.funding_application_frequency
        assert restored.initial_margin_ratio == original.initial_margin_ratio
        assert restored.liquidation_penalty == original.liquidation_penalty


# =============================================================================
# Funding Accumulation Tests
# =============================================================================


class TestFundingAccumulationOverTime:
    """Tests for funding accumulation over multiple update_position calls."""

    def test_funding_accumulates_continuous_long(self) -> None:
        """Test funding accumulates for long position with continuous frequency.

        Scenario:
        - PERP_LONG position with $50,000 notional (5x on $10,000)
        - 0.01% hourly funding rate (default)
        - 24 hours of updates
        - Expected: ~$120 in funding paid
        """
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="continuous",
            default_funding_rate=Decimal("0.0001"),  # 0.01% per hour
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        # Simulate 24 hourly updates
        for _ in range(24):
            adapter.update_position(position, market, elapsed_seconds=3600)

        # Expected funding: $50,000 * 0.0001 * 24 = $120
        expected_funding_paid = Decimal("50000") * Decimal("0.0001") * Decimal("24")

        # Long pays funding (accumulated_funding is negative for paid funding)
        assert position.accumulated_funding < Decimal("0")
        assert abs(position.accumulated_funding) == pytest.approx(expected_funding_paid, rel=Decimal("0.01"))
        assert position.cumulative_funding_paid == pytest.approx(expected_funding_paid, rel=Decimal("0.01"))
        assert position.cumulative_funding_received == Decimal("0")

    def test_funding_accumulates_continuous_short(self) -> None:
        """Test funding accumulates for short position with continuous frequency.

        Scenario:
        - PERP_SHORT position with $50,000 notional
        - 0.01% hourly funding rate
        - 24 hours of updates
        - Expected: ~$120 in funding received
        """
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="continuous",
            default_funding_rate=Decimal("0.0001"),
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_short_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        # Simulate 24 hourly updates
        for _ in range(24):
            adapter.update_position(position, market, elapsed_seconds=3600)

        expected_funding_received = Decimal("50000") * Decimal("0.0001") * Decimal("24")

        # Short receives funding (accumulated_funding is positive)
        assert position.accumulated_funding > Decimal("0")
        assert position.accumulated_funding == pytest.approx(expected_funding_received, rel=Decimal("0.01"))
        assert position.cumulative_funding_received == pytest.approx(expected_funding_received, rel=Decimal("0.01"))
        assert position.cumulative_funding_paid == Decimal("0")

    def test_funding_accumulates_hourly_frequency(self) -> None:
        """Test funding accumulation with hourly frequency setting."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="hourly",
            default_funding_rate=Decimal("0.0001"),
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("20000"),
            leverage=Decimal("10"),  # $200,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        # Simulate 12 hours of updates (every 30 minutes)
        for _ in range(24):
            adapter.update_position(position, market, elapsed_seconds=1800)

        # Expected: $200,000 * 0.0001 * 12 hours = $240
        expected_funding = Decimal("200000") * Decimal("0.0001") * Decimal("12")

        assert position.cumulative_funding_paid == pytest.approx(expected_funding, rel=Decimal("0.01"))

    def test_funding_accumulates_8h_frequency(self) -> None:
        """Test funding accumulation with 8-hour frequency setting."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="8h",
            default_funding_rate=Decimal("0.0001"),
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_short_position(
            collateral_usd=Decimal("5000"),
            leverage=Decimal("4"),  # $20,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        # Simulate 24 hours in 4-hour increments
        for _ in range(6):
            adapter.update_position(position, market, elapsed_seconds=14400)

        # Expected: $20,000 * 0.0001 * 24 = $48
        expected_funding = Decimal("20000") * Decimal("0.0001") * Decimal("24")

        assert position.cumulative_funding_received == pytest.approx(expected_funding, rel=Decimal("0.01"))

    def test_funding_over_7_days(self) -> None:
        """Test funding accumulation over a 7-day period."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="continuous",
            default_funding_rate=Decimal("0.0001"),
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("50000"),
            leverage=Decimal("2"),  # $100,000 notional
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            token="BTC",
        )

        market = MockMarketState(prices={"BTC": Decimal("40000")})

        # Simulate 7 days with 4-hour intervals
        for _ in range(7 * 6):  # 42 updates
            adapter.update_position(position, market, elapsed_seconds=14400)  # 4 hours

        # Expected: $100,000 * 0.0001 * 168 hours = $1,680
        expected_funding = Decimal("100000") * Decimal("0.0001") * Decimal("168")

        assert position.cumulative_funding_paid == pytest.approx(expected_funding, rel=Decimal("0.01"))

    def test_funding_with_custom_rate(self) -> None:
        """Test funding accumulation with custom funding rate."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="continuous",
            default_funding_rate=Decimal("0.0002"),  # 0.02% per hour (higher rate)
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        # Simulate 12 hours
        for _ in range(12):
            adapter.update_position(position, market, elapsed_seconds=3600)

        # Expected: $50,000 * 0.0002 * 12 = $120
        expected_funding = Decimal("50000") * Decimal("0.0002") * Decimal("12")

        assert position.cumulative_funding_paid == pytest.approx(expected_funding, rel=Decimal("0.01"))

    def test_no_funding_for_non_perp_position(self) -> None:
        """Test that non-perp positions are not affected by funding."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = SimulatedPosition(
            position_type=PositionType.SPOT,
            protocol="spot",
            tokens=["ETH"],
            amounts={"ETH": Decimal("10")},
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        adapter.update_position(position, market, elapsed_seconds=86400)

        # No funding should be applied
        assert position.accumulated_funding == Decimal("0")
        assert position.cumulative_funding_paid == Decimal("0")
        assert position.cumulative_funding_received == Decimal("0")

    def test_no_funding_for_zero_elapsed(self) -> None:
        """Test that zero elapsed time results in no funding."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(entry_time=entry_time)

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        adapter.update_position(position, market, elapsed_seconds=0)

        assert position.accumulated_funding == Decimal("0")

    def test_strict_update_missing_token_price_raises(self) -> None:
        """Strict updates must not reuse entry price when market price is missing."""
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        config = PerpBacktestConfig(strategy_type="perp", strict_reproducibility=True)
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(entry_time=entry_time)
        market = MockMarketState(prices={})

        with pytest.raises(HistoricalDataUnavailableError, match="ETH"):
            adapter.update_position(position, market, elapsed_seconds=3600, timestamp=entry_time + timedelta(hours=1))

    def test_address_keyed_historical_funding_uses_provider_market_symbol(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        arbitrum_weth = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        token_key = ("arbitrum", arbitrum_weth)
        config = PerpBacktestConfig(
            strategy_type="perp",
            protocol="gmx",
            chain="arbitrum",
        )
        adapter = PerpBacktestAdapter(
            config,
            data_config=BacktestDataConfig(
                use_historical_funding=True,
                funding_fallback_rate=Decimal("0.0002"),
            ),
        )
        position = create_perp_long_position(
            token=token_key,
            collateral_usd=Decimal("1000"),
            leverage=Decimal("2"),
            entry_price=Decimal("2000"),
            protocol="gmx",
        )
        captured: dict[str, str] = {}

        monkeypatch.setattr(adapter, "_get_provider_for_protocol", lambda protocol, chain=None: object())

        def fake_fetch_historical_funding_rates(provider, lookup):
            captured["market"] = lookup.market
            return []

        monkeypatch.setattr(
            adapter,
            "_fetch_historical_funding_rates",
            fake_fetch_historical_funding_rates,
        )

        rate, confidence, source = adapter._get_historical_funding_rate_v2(
            position=position,
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            chain="arbitrum",
        )

        assert captured["market"] == "ETH-USD"
        assert rate == Decimal("0.0002")
        assert confidence == "low"
        assert source == "fallback:no_data"


# =============================================================================
# Liquidation Tests
# =============================================================================


class TestLiquidationTriggeredByPriceMove:
    """Tests for liquidation triggered by adverse price moves."""

    def test_long_liquidation_on_price_drop(self) -> None:
        """Test long position liquidated when price drops below liquidation price.

        Scenario:
        - PERP_LONG with 5x leverage
        - Entry price $2000
        - Liquidation price ~$1700 (with 5% maintenance margin)
        - Price drops to $1600 -> liquidation triggered
        """
        config = PerpBacktestConfig(
            strategy_type="perp",
            liquidation_model_enabled=True,
            maintenance_margin_ratio=Decimal("0.05"),
            liquidation_penalty=Decimal("0.05"),
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        # Verify liquidation price is set
        assert position.liquidation_price is not None
        liq_price = position.liquidation_price

        # Price drops below liquidation
        crash_price = liq_price - Decimal("100")
        crash_time = entry_time + timedelta(hours=24)

        event = adapter.check_and_simulate_liquidation(
            position=position,
            current_price=crash_price,
            timestamp=crash_time,
        )

        # Liquidation should have occurred
        assert event is not None
        assert position.is_liquidated is True
        assert event.position_id == position.position_id
        assert event.timestamp == crash_time
        assert event.price == crash_price
        assert event.loss_usd > Decimal("0")

    def test_short_liquidation_on_price_rise(self) -> None:
        """Test short position liquidated when price rises above liquidation price.

        Scenario:
        - PERP_SHORT with 5x leverage
        - Entry price $2000
        - Liquidation price ~$2300 (with 5% maintenance margin)
        - Price rises to $2400 -> liquidation triggered
        """
        config = PerpBacktestConfig(
            strategy_type="perp",
            liquidation_model_enabled=True,
            maintenance_margin_ratio=Decimal("0.05"),
            liquidation_penalty=Decimal("0.05"),
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_short_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        # Verify liquidation price is set
        assert position.liquidation_price is not None
        liq_price = position.liquidation_price

        # Price rises above liquidation
        pump_price = liq_price + Decimal("100")
        pump_time = entry_time + timedelta(hours=24)

        event = adapter.check_and_simulate_liquidation(
            position=position,
            current_price=pump_price,
            timestamp=pump_time,
        )

        # Liquidation should have occurred
        assert event is not None
        assert position.is_liquidated is True
        assert event.position_id == position.position_id
        assert event.price == pump_price
        assert event.loss_usd > Decimal("0")

    def test_no_liquidation_price_above_threshold_long(self) -> None:
        """Test long position NOT liquidated when price stays above liquidation."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            liquidation_model_enabled=True,
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        liq_price = position.liquidation_price
        assert liq_price is not None

        # Price drops but stays above liquidation
        safe_price = liq_price + Decimal("100")
        check_time = entry_time + timedelta(hours=24)

        event = adapter.check_and_simulate_liquidation(
            position=position,
            current_price=safe_price,
            timestamp=check_time,
        )

        assert event is None
        assert position.is_liquidated is False

    def test_no_liquidation_price_below_threshold_short(self) -> None:
        """Test short position NOT liquidated when price stays below liquidation."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            liquidation_model_enabled=True,
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_short_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        liq_price = position.liquidation_price
        assert liq_price is not None

        # Price rises but stays below liquidation
        safe_price = liq_price - Decimal("100")
        check_time = entry_time + timedelta(hours=24)

        event = adapter.check_and_simulate_liquidation(
            position=position,
            current_price=safe_price,
            timestamp=check_time,
        )

        assert event is None
        assert position.is_liquidated is False

    def test_liquidation_at_exact_liquidation_price(self) -> None:
        """Test liquidation triggers at exactly the liquidation price."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        liq_price = position.liquidation_price
        assert liq_price is not None

        event = adapter.check_and_simulate_liquidation(
            position=position,
            current_price=liq_price,
            timestamp=entry_time + timedelta(hours=1),
        )

        # Should be liquidated at exact price
        assert event is not None
        assert position.is_liquidated is True

    def test_liquidation_disabled(self) -> None:
        """Test liquidation does not occur when disabled in config."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            liquidation_model_enabled=False,
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        liq_price = position.liquidation_price
        assert liq_price is not None

        # Price crashes below liquidation
        crash_price = liq_price - Decimal("500")

        event = adapter.check_and_simulate_liquidation(
            position=position,
            current_price=crash_price,
            timestamp=entry_time + timedelta(hours=24),
        )

        # No liquidation because disabled
        assert event is None
        assert position.is_liquidated is False

    def test_already_liquidated_position_skipped(self) -> None:
        """Test that already liquidated positions are skipped."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(entry_time=entry_time)

        # Manually mark as liquidated
        position.is_liquidated = True

        event = adapter.check_and_simulate_liquidation(
            position=position,
            current_price=Decimal("1"),  # Any price
            timestamp=entry_time + timedelta(hours=1),
        )

        assert event is None

    def test_liquidation_penalty_applied(self) -> None:
        """Test that liquidation penalty is applied to remaining collateral."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            liquidation_model_enabled=True,
            liquidation_penalty=Decimal("0.10"),  # 10% penalty
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        liq_price = position.liquidation_price
        assert liq_price is not None

        # Trigger liquidation
        event = adapter.check_and_simulate_liquidation(
            position=position,
            current_price=liq_price - Decimal("100"),
            timestamp=entry_time + timedelta(hours=24),
        )

        assert event is not None
        assert position.is_liquidated is True

        # Collateral should be reduced due to penalty
        # Metadata should contain penalty info
        assert "liquidation_penalty" in position.metadata

    def test_liquidation_with_high_leverage(self) -> None:
        """Test liquidation with high leverage (10x)."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("5000"),
            leverage=Decimal("10"),  # 10x leverage - higher liquidation price
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        liq_price = position.liquidation_price
        assert liq_price is not None

        # High leverage means liquidation price is closer to entry
        # For 10x with 5% maintenance: liq = 2000 * (1 - 0.1 + 0.05) = 2000 * 0.95 = 1900
        assert liq_price > Decimal("1800")
        assert liq_price < Decimal("2000")

        # Price drops just below liquidation
        event = adapter.check_and_simulate_liquidation(
            position=position,
            current_price=liq_price - Decimal("50"),
            timestamp=entry_time + timedelta(hours=1),
        )

        assert event is not None
        assert position.is_liquidated is True

    def test_liquidation_event_contains_correct_data(self) -> None:
        """Test that LiquidationEvent contains all required data."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        liq_price = position.liquidation_price
        assert liq_price is not None

        crash_price = liq_price - Decimal("100")
        crash_time = entry_time + timedelta(days=1)

        event = adapter.check_and_simulate_liquidation(
            position=position,
            current_price=crash_price,
            timestamp=crash_time,
        )

        assert event is not None
        assert event.timestamp == crash_time
        assert event.position_id == position.position_id
        assert event.price == crash_price
        assert isinstance(event.loss_usd, Decimal)
        assert event.loss_usd > Decimal("0")


# =============================================================================
# Position Valuation Tests
# =============================================================================


class TestPositionValuation:
    """Tests for position valuation including unrealized PnL and funding."""

    def test_long_position_profit_on_price_increase(self) -> None:
        """Test long position value increases when price goes up."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        # Price increases 10%
        market = MockMarketState(prices={"ETH": Decimal("2200")})

        value = adapter.value_position(position, market)

        # Unrealized PnL = (2200 - 2000) / 2000 * 50000 = 0.1 * 50000 = $5,000
        # Total value = $10,000 collateral + $5,000 profit = $15,000
        expected_value = Decimal("10000") + Decimal("5000")
        assert value == pytest.approx(expected_value, rel=Decimal("0.01"))

    def test_long_position_loss_on_price_decrease(self) -> None:
        """Test long position value decreases when price goes down."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        # Price decreases 10%
        market = MockMarketState(prices={"ETH": Decimal("1800")})

        value = adapter.value_position(position, market)

        # Unrealized PnL = (1800 - 2000) / 2000 * 50000 = -0.1 * 50000 = -$5,000
        # Total value = $10,000 collateral - $5,000 loss = $5,000
        expected_value = Decimal("10000") - Decimal("5000")
        assert value == pytest.approx(expected_value, rel=Decimal("0.01"))

    def test_short_position_profit_on_price_decrease(self) -> None:
        """Test short position value increases when price goes down."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_short_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        # Price decreases 10%
        market = MockMarketState(prices={"ETH": Decimal("1800")})

        value = adapter.value_position(position, market)

        # Short profits when price falls
        # Unrealized PnL = (2000 - 1800) / 2000 * 50000 = 0.1 * 50000 = $5,000
        expected_value = Decimal("10000") + Decimal("5000")
        assert value == pytest.approx(expected_value, rel=Decimal("0.01"))

    def test_short_position_loss_on_price_increase(self) -> None:
        """Test short position value decreases when price goes up."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_short_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        # Price increases 10%
        market = MockMarketState(prices={"ETH": Decimal("2200")})

        value = adapter.value_position(position, market)

        # Short loses when price rises
        # Unrealized PnL = -(2200 - 2000) / 2000 * 50000 = -$5,000
        expected_value = Decimal("10000") - Decimal("5000")
        assert value == pytest.approx(expected_value, rel=Decimal("0.01"))

    def test_position_value_includes_accumulated_funding(self) -> None:
        """Test that position value includes accumulated funding."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="continuous",
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_short_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        # Apply 24 hours of funding
        for _ in range(24):
            adapter.update_position(position, market, elapsed_seconds=3600)

        # Get value - should include funding received
        value = adapter.value_position(position, market)

        # Value = collateral + unrealized PnL (0) + funding received
        # With constant price, unrealized PnL is 0
        # Funding received = $50,000 * 0.0001 * 24 = $120
        expected_value = Decimal("10000") + Decimal("120")
        assert value == pytest.approx(expected_value, rel=Decimal("0.01"))

    def test_strict_value_missing_token_price_raises(self) -> None:
        """Strict valuation must not reuse entry price when market price is missing."""
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        config = PerpBacktestConfig(strategy_type="perp", strict_reproducibility=True)
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(entry_time=entry_time)
        market = MockMarketState(prices={})

        with pytest.raises(HistoricalDataUnavailableError, match="ETH"):
            adapter.value_position(position, market)


# =============================================================================
# Should Rebalance Tests
# =============================================================================


class TestShouldRebalance:
    """Tests for should_rebalance method based on liquidation proximity."""

    def test_rebalance_when_approaching_liquidation(self) -> None:
        """Test rebalance suggested when approaching liquidation threshold."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            liquidation_warning_threshold=Decimal("0.10"),  # 10% threshold
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        liq_price = position.liquidation_price
        assert liq_price is not None

        # Price 8% above liquidation (within 10% threshold)
        warning_price = liq_price * Decimal("1.08")
        market = MockMarketState(prices={"ETH": warning_price})

        should_rebalance = adapter.should_rebalance(position, market)
        assert should_rebalance is True

    def test_no_rebalance_when_far_from_liquidation(self) -> None:
        """Test no rebalance when price is far from liquidation."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        # Price at entry (far from liquidation)
        market = MockMarketState(prices={"ETH": Decimal("2000")})

        should_rebalance = adapter.should_rebalance(position, market)
        assert should_rebalance is False

    def test_no_rebalance_for_non_perp_position(self) -> None:
        """Test no rebalance check for non-perp positions."""
        adapter = PerpBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = SimulatedPosition(
            position_type=PositionType.SPOT,
            protocol="spot",
            tokens=["ETH"],
            amounts={"ETH": Decimal("10")},
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        market = MockMarketState(prices={"ETH": Decimal("1")})

        should_rebalance = adapter.should_rebalance(position, market)
        assert should_rebalance is False


# =============================================================================
# Margin Validation Tests
# =============================================================================


class TestMarginValidation:
    """Tests for margin validation on position opening."""

    def test_margin_validation_pass(self) -> None:
        """Test margin validation passes with sufficient collateral."""
        adapter = PerpBacktestAdapter()

        is_valid, message = adapter.validate_margin(
            position_size=Decimal("50000"),  # $50,000 position
            collateral=Decimal("10000"),  # $10,000 collateral (20%)
        )

        # Default initial margin is 10%, so 20% should pass
        assert is_valid is True

    def test_margin_validation_fail_insufficient_collateral(self) -> None:
        """Test margin validation fails with insufficient collateral."""
        adapter = PerpBacktestAdapter()

        is_valid, message = adapter.validate_margin(
            position_size=Decimal("100000"),  # $100,000 position
            collateral=Decimal("5000"),  # $5,000 collateral (5%)
        )

        # Default initial margin is 10%, so 5% should fail
        assert is_valid is False
        assert "insufficient" in message.lower() or "margin" in message.lower()

    def test_liquidation_price_calculation(self) -> None:
        """Test liquidation price calculation method."""
        adapter = PerpBacktestAdapter()

        # Long position
        long_liq = adapter.get_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("5"),
            is_long=True,
        )

        # With 5% maintenance margin: liq = 2000 * (1 - 0.2 + 0.05) = 2000 * 0.85 = 1700
        assert long_liq == Decimal("1700")

        # Short position
        short_liq = adapter.get_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("5"),
            is_long=False,
        )

        # With 5% maintenance margin: liq = 2000 * (1 + 0.2 - 0.05) = 2000 * 1.15 = 2300
        assert short_liq == Decimal("2300")


# =============================================================================
# Integration Tests - Combined Scenarios
# =============================================================================


class TestIntegrationScenarios:
    """Integration tests combining multiple adapter features."""

    def test_funding_and_valuation_combined(self) -> None:
        """Test position value reflects both PnL and funding over time."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="continuous",
            default_funding_rate=Decimal("0.0001"),
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        # Price increases 5% over 24 hours
        market = MockMarketState(prices={"ETH": Decimal("2100")})

        for _ in range(24):
            adapter.update_position(position, market, elapsed_seconds=3600)

        value = adapter.value_position(position, market)

        # Unrealized PnL = (2100 - 2000) / 2000 * 50000 = 0.05 * 50000 = $2,500
        # Funding paid = 50000 * 0.0001 * 24 = $120
        # Total = 10000 + 2500 - 120 = $12,380
        expected_value = Decimal("10000") + Decimal("2500") - Decimal("120")
        assert value == pytest.approx(expected_value, rel=Decimal("0.02"))

    def test_price_crash_leading_to_liquidation(self) -> None:
        """Test scenario: price crashes over time leading to liquidation."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            liquidation_model_enabled=True,
            funding_application_frequency="continuous",
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("10"),  # High leverage
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        liq_price = position.liquidation_price
        assert liq_price is not None

        # Simulate gradual price decline crossing the venue-resolved
        # liquidation price (gmx 1% maintenance -> 1820 at 10x from 2000).
        prices = [Decimal("1950"), Decimal("1850"), Decimal("1800")]
        event = None

        for i, price in enumerate(prices):
            market = MockMarketState(prices={"ETH": price})
            adapter.update_position(position, market, elapsed_seconds=28800)  # 8 hours

            # Check for liquidation at each step
            if not position.is_liquidated:
                event = adapter.check_and_simulate_liquidation(
                    position=position,
                    current_price=price,
                    timestamp=entry_time + timedelta(hours=8 * (i + 1)),
                )

            if event is not None:
                break

        # Should have been liquidated at some point
        assert position.is_liquidated is True
        assert event is not None

    def test_adapter_serialization(self) -> None:
        """Test adapter configuration serialization."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="8h",
            liquidation_penalty=Decimal("0.08"),
            protocol="hyperliquid",
        )
        adapter = PerpBacktestAdapter(config)

        data = adapter.to_dict()

        assert data["adapter_name"] == "perp"
        assert data["config"]["funding_application_frequency"] == "8h"
        assert data["config"]["liquidation_penalty"] == "0.08"
        assert data["config"]["protocol"] == "hyperliquid"


# =============================================================================
# Historical Funding Rate Tests
# =============================================================================


class TestHistoricalFundingRateIntegration:
    """Tests for historical funding rate integration (US-053b, US-028)."""

    def test_historical_funding_rate_config(self) -> None:
        """Test that funding_rate_source='historical' enables historical funding lookup."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_rate_source="historical",
            protocol="gmx",
            chain="arbitrum",
        )
        adapter = PerpBacktestAdapter(config)

        # New providers are lazy-initialized, so check the use_historical_funding flag instead
        assert adapter._use_historical_funding() is True

        # When data_config is None and config.funding_rate_source=='historical',
        # the connector-declared providers are resolved lazily through the
        # generic cache. The legacy provider is not initialized in this case.
        assert adapter._provider_cache == {}
        assert adapter._provider_tried == set()

    def test_fixed_funding_rate_no_provider(self) -> None:
        """Test that funding_rate_source='fixed' does not initialize provider."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_rate_source="fixed",
        )
        adapter = PerpBacktestAdapter(config)

        # Provider should NOT be initialized
        assert adapter._funding_rate_provider is None

    def test_protocol_funding_rate_no_provider(self) -> None:
        """Test that funding_rate_source='protocol' does not initialize provider."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_rate_source="protocol",
        )
        adapter = PerpBacktestAdapter(config)

        # Provider should NOT be initialized
        assert adapter._funding_rate_provider is None

    def test_historical_rate_fallback_on_error(self) -> None:
        """Test that historical rate falls back to default on provider error."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        # Use BacktestDataConfig to enable historical funding with fallback rate
        data_config = BacktestDataConfig(
            use_historical_funding=True,
            funding_fallback_rate=Decimal("0.0002"),
        )
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_rate_source="historical",
            default_funding_rate=Decimal("0.0002"),
            protocol="gmx",
        )
        adapter = PerpBacktestAdapter(config, data_config=data_config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        adapter._provider_tried.add(("gmx_v2", "*"))
        adapter._provider_cache[("gmx_v2", "*")] = None
        rate, confidence, source = adapter._get_historical_funding_rate_v2(
            position=position,
            timestamp=entry_time,
        )

        # Should fall back to default rate
        assert rate == Decimal("0.0002")  # Uses data_config.funding_fallback_rate
        assert confidence == "low"
        assert "fallback" in source

    def test_historical_rate_fallback_no_timestamp(self) -> None:
        """Test that historical rate falls back to default when no timestamp."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_rate_source="historical",
            default_funding_rate=Decimal("0.0002"),
            protocol="gmx",
        )
        adapter = PerpBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        # Call without timestamp
        rate, source = adapter._get_historical_funding_rate(
            position=position,
            timestamp=None,
        )

        # Should fall back to default rate
        assert rate == Decimal("0.0001")  # DEFAULT_FUNDING_RATES["gmx"]
        assert source == "fallback:no_timestamp"

    def test_historical_rate_applied_to_position(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that historical rates are applied correctly to position with logging."""
        from unittest.mock import AsyncMock, MagicMock

        from almanak.framework.backtesting.config import BacktestDataConfig
        from almanak.framework.backtesting.pnl.types import DataConfidence, DataSourceInfo, FundingResult

        data_config = BacktestDataConfig(
            use_historical_funding=True,
            funding_fallback_rate=Decimal("0.0001"),
        )
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_rate_source="historical",
            funding_application_frequency="continuous",
            protocol="gmx",
        )
        adapter = PerpBacktestAdapter(config, data_config=data_config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        # Create mock funding result with higher rate
        mock_funding_result = FundingResult(
            rate=Decimal("0.0005"),  # 0.05% per hour (5x default)
            source_info=DataSourceInfo(
                source="gmx_api",
                confidence=DataConfidence.HIGH,
                timestamp=entry_time,
            ),
        )

        # Create a mock provider
        mock_provider = MagicMock()
        mock_provider.get_funding_rates = AsyncMock(return_value=[mock_funding_result])

        import logging

        adapter._seed_injected_provider("gmx_v2", mock_provider)
        with caplog.at_level(logging.DEBUG, logger="almanak.framework.backtesting.adapters.perp_adapter"):
            # Apply 1 hour of funding
            adapter.update_position(position, market, elapsed_seconds=3600, timestamp=entry_time)

            # With 0.05% rate and $50,000 notional for 1 hour:
            # Funding = 50000 * 0.0005 * 1 = $25
            expected_funding = Decimal("50000") * Decimal("0.0005") * Decimal("1")

            # Long pays funding (negative accumulated)
            assert position.accumulated_funding < Decimal("0")
            assert abs(position.accumulated_funding) == pytest.approx(expected_funding, rel=Decimal("0.01"))

            # Verify position tracks funding confidence
            assert position.funding_confidence == "high"
            assert position.funding_data_source is not None
            assert "historical" in position.funding_data_source

    def test_historical_rate_uses_latest_timestamp_not_provider_order(self) -> None:
        """Provider results may be unsorted; choose the newest timestamp explicitly."""
        from unittest.mock import AsyncMock, MagicMock

        from almanak.framework.backtesting.config import BacktestDataConfig
        from almanak.framework.backtesting.pnl.types import DataConfidence, DataSourceInfo, FundingResult

        data_config = BacktestDataConfig(use_historical_funding=True)
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_rate_source="historical",
            protocol="gmx",
        )
        adapter = PerpBacktestAdapter(config, data_config=data_config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(entry_time=entry_time, protocol="gmx")

        newest = FundingResult(
            rate=Decimal("0.0007"),
            source_info=DataSourceInfo(
                source="gmx_api_newest",
                confidence=DataConfidence.HIGH,
                timestamp=entry_time,
            ),
        )
        older = FundingResult(
            rate=Decimal("0.0001"),
            source_info=DataSourceInfo(
                source="gmx_api_older",
                confidence=DataConfidence.MEDIUM,
                timestamp=entry_time - timedelta(minutes=30),
            ),
        )
        mock_provider = MagicMock()
        mock_provider.get_funding_rates = AsyncMock(return_value=[newest, older])

        adapter._seed_injected_provider("gmx_v2", mock_provider)
        rate, confidence, source = adapter._get_historical_funding_rate_v2(
            position=position,
            timestamp=entry_time,
        )

        assert rate == Decimal("0.0007")
        assert confidence == "high"
        assert source == "historical:gmx_api_newest"

    def test_chain_config_in_serialization(self) -> None:
        """Test that chain field is properly serialized/deserialized."""
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_rate_source="historical",
            chain="avalanche",
            protocol="gmx",
        )

        data = config.to_dict()
        assert data["chain"] == "avalanche"

        restored = PerpBacktestConfig.from_dict(data)
        assert restored.chain == "avalanche"

    def test_hyperliquid_historical_rate(self) -> None:
        """Test historical rate lookup for Hyperliquid protocol."""
        from unittest.mock import AsyncMock, MagicMock

        from almanak.framework.backtesting.config import BacktestDataConfig
        from almanak.framework.backtesting.pnl.types import DataConfidence, DataSourceInfo, FundingResult

        data_config = BacktestDataConfig(
            use_historical_funding=True,
            funding_fallback_rate=Decimal("0.0001"),
        )
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_rate_source="historical",
            protocol="hyperliquid",
            chain="arbitrum",  # Chain doesn't matter for Hyperliquid
        )
        adapter = PerpBacktestAdapter(config, data_config=data_config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_perp_long_position(
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="hyperliquid",
        )

        mock_funding_result = FundingResult(
            rate=Decimal("0.00015"),  # 0.015% per hour
            source_info=DataSourceInfo(
                source="hyperliquid_api",
                confidence=DataConfidence.HIGH,
                timestamp=entry_time,
            ),
        )

        # Create a mock provider
        mock_provider = MagicMock()
        mock_provider.get_funding_rates = AsyncMock(return_value=[mock_funding_result])

        adapter._seed_injected_provider("hyperliquid", mock_provider)
        rate, confidence, source = adapter._get_historical_funding_rate_v2(
            position=position,
            timestamp=entry_time,
        )

        assert rate == Decimal("0.00015")
        assert confidence == "high"
        assert "historical:hyperliquid_api" in source

    @staticmethod
    def _legacy_adapter_with_provider(
        rate: Decimal | None = None,
        error: Exception | None = None,
        default_funding_rate: Decimal = Decimal("0.0001"),
    ) -> PerpBacktestAdapter:
        """Adapter wired to a mocked legacy FundingRateProvider.

        The provider mock either returns a gateway-sourced ``FundingRateData``
        with ``rate`` or raises ``error``.
        """
        from unittest.mock import AsyncMock, MagicMock

        from almanak.framework.backtesting.pnl.providers.funding_rates import FundingRateData

        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_rate_source="historical",
            default_funding_rate=default_funding_rate,
            protocol="gmx",
        )
        adapter = PerpBacktestAdapter(config)
        mock_provider = MagicMock()
        if error is not None:
            mock_provider.get_historical_funding_rate = AsyncMock(side_effect=error)
        else:
            assert rate is not None
            rate_data = FundingRateData(
                protocol="gmx",
                market="ETH-USD",
                timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                rate=rate,
                source="gateway",
            )
            mock_provider.get_historical_funding_rate = AsyncMock(return_value=rate_data)
        adapter._funding_rate_provider = mock_provider
        return adapter

    def test_legacy_historical_rate_success_no_event_loop(self) -> None:
        """Legacy method returns the provider's rate via the asyncio.run branch."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        adapter = self._legacy_adapter_with_provider(rate=Decimal("0.0003"))
        position = create_perp_long_position(entry_time=entry_time, protocol="gmx")

        rate, source = adapter._get_historical_funding_rate(position=position, timestamp=entry_time)

        assert rate == Decimal("0.0003")
        assert source == "historical:gateway"
        adapter._funding_rate_provider.get_historical_funding_rate.assert_awaited_once_with(
            protocol="gmx",
            market="ETH-USD",
            timestamp=entry_time,
        )

    @pytest.mark.asyncio
    async def test_legacy_historical_rate_success_inside_running_loop(self) -> None:
        """With a running event loop the query routes through the thread pool."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        adapter = self._legacy_adapter_with_provider(rate=Decimal("0.0004"))
        position = create_perp_long_position(entry_time=entry_time, protocol="gmx")

        rate, source = adapter._get_historical_funding_rate(position=position, timestamp=entry_time)

        assert rate == Decimal("0.0004")
        assert source == "historical:gateway"

    def test_legacy_historical_rate_error_falls_back_to_registry_default(self) -> None:
        """Provider failure on a registry-known protocol uses DEFAULT_FUNDING_RATE."""
        from almanak.framework.backtesting.pnl.providers.funding_rates import DEFAULT_FUNDING_RATE

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        adapter = self._legacy_adapter_with_provider(
            error=ValueError("gateway exploded"),
            default_funding_rate=Decimal("0.0009"),
        )
        position = create_perp_long_position(entry_time=entry_time, protocol="gmx")

        rate, source = adapter._get_historical_funding_rate(position=position, timestamp=entry_time)

        # "gmx" has a connector-owned funding venue, so the registry default
        # wins over config.default_funding_rate.
        assert rate == DEFAULT_FUNDING_RATE
        assert source == "fallback:error"

    def test_legacy_historical_rate_error_unknown_protocol_uses_config_default(self) -> None:
        """Provider failure on an unknown protocol uses config.default_funding_rate."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        adapter = self._legacy_adapter_with_provider(
            error=ValueError("gateway exploded"),
            default_funding_rate=Decimal("0.00042"),
        )
        position = create_perp_long_position(entry_time=entry_time, protocol="vertex")

        rate, source = adapter._get_historical_funding_rate(position=position, timestamp=entry_time)

        assert rate == Decimal("0.00042")
        assert source == "fallback:error"

    def test_legacy_historical_rate_no_timestamp_unknown_protocol_uses_config_default(self) -> None:
        """The early no-timestamp fallback also honors the registry split."""
        adapter = self._legacy_adapter_with_provider(
            rate=Decimal("0.0003"),
            default_funding_rate=Decimal("0.00021"),
        )
        position = create_perp_long_position(protocol="vertex")

        rate, source = adapter._get_historical_funding_rate(position=position, timestamp=None)

        assert rate == Decimal("0.00021")
        assert source == "fallback:no_timestamp"
        adapter._funding_rate_provider.get_historical_funding_rate.assert_not_awaited()


# =============================================================================
# execute_intent margin validation against the REAL SimulatedPortfolio
# =============================================================================
#
# Regression tests for the perp adapter referencing `portfolio.cash_balance`,
# an attribute that does not exist on SimulatedPortfolio (the real model only
# has `cash_usd`). The bug was masked because earlier tests only exercised
# execute_intent with hand-rolled mock portfolios that happened to define
# `cash_balance`.


class TestExecuteIntentUsesRealPortfolioCash:
    """execute_intent margin validation must work against SimulatedPortfolio."""

    @staticmethod
    def _market_state(price: str = "3000"):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        return MarketState(
            timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
            prices={
                "ETH": Decimal(price),
                "WETH": Decimal(price),
                "USDC": Decimal("1"),
            },
            chain="arbitrum",
        )

    @staticmethod
    def _open_intent(
        collateral_amount,
        size_usd: str = "5000",
        leverage: str = "5",
        collateral_token: str = "USDC",
    ):
        from almanak.framework.intents.vocabulary import PerpOpenIntent

        return PerpOpenIntent(
            market="ETH/USD",
            collateral_token=collateral_token,
            collateral_amount=collateral_amount,
            size_usd=Decimal(size_usd),
            leverage=Decimal(leverage),
            protocol="gmx_v2",
        )

    def test_open_with_sufficient_cash_passes_validation(self) -> None:
        """Margin validation reads cash_usd and lets a healthy open proceed."""
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        adapter = PerpBacktestAdapter(PerpBacktestConfig(strategy_type="perp"))
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        fill = adapter.execute_intent(self._open_intent(Decimal("1000")), portfolio, self._market_state())

        # None means validation passed and default execution proceeds.
        assert fill is None

    def test_open_with_insufficient_cash_returns_failed_fill(self) -> None:
        """Collateral above available cash must fail margin validation, not crash."""
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        adapter = PerpBacktestAdapter(PerpBacktestConfig(strategy_type="perp"))
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100"))

        fill = adapter.execute_intent(self._open_intent(Decimal("1000")), portfolio, self._market_state())

        assert fill is not None
        assert fill.success is False
        assert fill.metadata["validation_type"] == "margin"
        assert fill.tokens_out == {}
        assert fill.metadata["attempted_collateral_amount"] == "1000"
        assert fill.metadata["attempted_collateral_token"] == "USDC"

    def test_strict_open_missing_collateral_price_raises(self) -> None:
        """Strict perp open must not assume $1 for a missing collateral price."""
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError
        from almanak.framework.backtesting.pnl.data_provider import MarketState
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        adapter = PerpBacktestAdapter(PerpBacktestConfig(strategy_type="perp", strict_reproducibility=True))
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        market = MarketState(
            timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
            prices={"ETH": Decimal("3000"), "USDC": Decimal("1")},
            chain="arbitrum",
        )

        with pytest.raises(HistoricalDataUnavailableError, match="WETH"):
            adapter.execute_intent(
                self._open_intent(Decimal("1"), size_usd="5000", collateral_token="WETH"),
                portfolio,
                market,
            )

    def test_open_all_collateral_resolves_from_spendable_balance(self) -> None:
        """collateral_amount='all' sizes from the shared resolver (phase 5).

        Both lanes read the SAME resolution, so the old $1,000-vs-$50
        split-brain is unrepresentable; margin checks still gate the open.
        """
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        # "all" posts the full wallet as collateral, but the utilization gate
        # measures the position's initial-margin REQUIREMENT (size * ratio),
        # not the posted collateral — excess collateral is free equity. A
        # comfortable size ($5,000 at the 10% floor = 5% of the wallet)
        # validates under the DEFAULT risk policy and DEFERS to the generic
        # lane (returns None); the engine-level pin asserts the filled
        # position. (Counting posted collateral made "all" identically 100%
        # utilized and unfillable at any size — campaign-50 s36.)
        adapter = PerpBacktestAdapter(PerpBacktestConfig(strategy_type="perp"))
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        fill = adapter.execute_intent(self._open_intent("all"), portfolio, self._market_state())
        assert fill is None  # validated at $10,000 collateral, generic lane fills

        # RISK POLICY still decides: a size whose initial margin genuinely
        # needs >90% of the wallet ($95,000 * 10% = $9,500 on $10,000)
        # margin-rejects on the utilization cap, never the old
        # unsupported-sizing wall — sizing is resolved either way.
        rejected = adapter.execute_intent(
            self._open_intent("all", size_usd="950000", leverage="9.5"),
            SimulatedPortfolio(initial_capital_usd=Decimal("10000")),
            self._market_state(),
        )
        assert rejected is not None and rejected.success is False
        assert rejected.metadata["validation_type"] == "margin"
        assert "exceed max margin utilization" in rejected.metadata["failure_reason"]
        assert rejected.metadata["attempted_collateral_amount"] == "10000"


class TestFundingProviderChainRouting:
    """Funding providers are built and cached per (protocol, chain)."""

    def _adapter(self):
        from almanak.framework.backtesting.adapters.perp_adapter import PerpBacktestAdapter
        from almanak.framework.backtesting.config import BacktestDataConfig

        return PerpBacktestAdapter(data_config=BacktestDataConfig(use_historical_funding=True))

    def test_run_chain_threads_into_provider(self):
        adapter = self._adapter()
        provider = adapter._get_provider_for_protocol("gmx_v2", "avalanche")
        assert provider is not None
        assert provider._config.chain == "avalanche"

    def test_provider_cache_is_chain_keyed(self):
        adapter = self._adapter()
        arb = adapter._get_provider_for_protocol("gmx_v2", "arbitrum")
        avax = adapter._get_provider_for_protocol("gmx_v2", "avalanche")
        assert arb is not None and avax is not None
        assert arb is not avax
        assert adapter._get_provider_for_protocol("gmx_v2", "avalanche") is avax

    def test_chain_alias_canonicalizes_everywhere(self):
        """ "avax" and "avalanche" are the same chain: an aliased injection is
        accepted, an aliased lookup serves the canonical entry, and no
        alias-keyed duplicate is ever cached."""
        from unittest.mock import MagicMock

        adapter = self._adapter()
        injected = MagicMock(spec=[])
        injected.chain = "avalanche"
        adapter._seed_injected_provider("gmx_v2:avax", injected)

        assert adapter._provider_cache.get(("gmx_v2", "avalanche")) is injected
        assert ("gmx_v2", "avax") not in adapter._provider_cache
        assert adapter._get_provider_for_protocol("gmx_v2", "avax") is injected
        assert adapter._get_provider_for_protocol("gmx_v2", "avalanche") is injected

    def test_declared_chain_contract_enforced_on_injections(self):
        """A connector that declares funding chains never serves an undeclared
        one — not via explicit scope, not via public chain, not via the
        wildcard seam."""
        from unittest.mock import MagicMock

        # Explicit scope to an undeclared chain: rejected at seeding.
        adapter = self._adapter()
        chainless = MagicMock(spec=[])
        chainless.chain = None
        adapter._seed_injected_provider("gmx_v2:ethereum", chainless)
        assert ("gmx_v2", "ethereum") not in adapter._provider_cache

        # Public chain reporting an undeclared chain: rejected at seeding.
        adapter2 = self._adapter()
        eth_reporting = MagicMock(spec=[])
        eth_reporting.chain = "ethereum"
        adapter2._seed_injected_provider("gmx_v2", eth_reporting)
        assert ("gmx_v2", "ethereum") not in adapter2._provider_cache

        # Wildcard seam serves declared chains only.
        adapter3 = self._adapter()
        wildcard = MagicMock(spec=[])
        wildcard.chain = None
        adapter3._seed_injected_provider("gmx_v2", wildcard)
        assert adapter3._get_provider_for_protocol("gmx_v2", "ethereum") is None
        assert adapter3._get_provider_for_protocol("gmx_v2", "avalanche") is wildcard

    def test_chainless_factory_output_rejected_for_declared_protocols(self):
        """A chain-scoped factory returning chain=None must not be cached —
        such a provider can default internally to another chain with no way
        to detect it (round-7: silent cross-chain funding corruption)."""
        from unittest.mock import patch

        from almanak.connectors.gmx_v2.backtest_funding import GMXFundingProvider

        adapter = self._adapter()
        real = GMXFundingProvider.for_backtest

        def chainless_factory(config):
            provider = real(config)
            provider.chain = None
            return provider

        with patch.object(GMXFundingProvider, "for_backtest", staticmethod(chainless_factory)):
            provider = adapter._get_provider_for_protocol("gmx_v2", "avalanche")

        assert provider is None
        assert adapter._provider_cache.get(("gmx_v2", "avalanche")) is None

    def test_blank_explicit_scope_is_rejected_not_wildcard(self):
        """ "gmx_v2:" is a typo, not a wildcard — it must not silently broaden
        a chainless provider to every supported chain."""
        from unittest.mock import MagicMock

        adapter = self._adapter()
        injected = MagicMock(spec=[])
        injected.chain = None
        adapter._seed_injected_provider("gmx_v2:", injected)

        assert not any(key[0] == "gmx_v2" for key in adapter._provider_cache)
        assert adapter._get_provider_for_protocol("gmx_v2", "avalanche") is not injected

    def test_empty_declared_set_is_chain_agnostic(self):
        """Hyperliquid's empty declared-chain set is the explicit
        chain-agnostic contract: the wildcard serves any chain."""
        from unittest.mock import MagicMock

        adapter = self._adapter()
        injected = MagicMock(spec=[])
        injected.chain = None
        adapter._seed_injected_provider("hyperliquid", injected)

        assert adapter._get_provider_for_protocol("hyperliquid", "arbitrum") is injected

    def test_wrong_chain_construction_is_never_cached(self):
        """A factory that falls back to its default chain (GMX -> arbitrum for
        undeclared chains) must yield None, not another chain's history."""
        adapter = self._adapter()

        provider = adapter._get_provider_for_protocol("gmx_v2", "ethereum")

        assert provider is None
        assert adapter._provider_cache.get(("gmx_v2", "ethereum")) is None

    def test_exact_chain_entry_beats_injected_wildcard(self):
        from unittest.mock import MagicMock

        adapter = self._adapter()
        avax = adapter._get_provider_for_protocol("gmx_v2", "avalanche")
        wildcard = MagicMock(spec=[])
        adapter._provider_cache[("gmx_v2", "*")] = wildcard
        adapter._provider_tried.add(("gmx_v2", "*"))

        assert adapter._get_provider_for_protocol("gmx_v2", "avalanche") is avax

    def test_chain_scoped_injection_serves_its_chain_only(self):
        """A provider declaring a public chain scope is keyed to that chain."""
        from unittest.mock import MagicMock

        adapter = self._adapter()
        injected = MagicMock(spec=[])
        injected.chain = "arbitrum"
        adapter._seed_injected_provider("gmx_v2", injected)

        assert adapter._get_provider_for_protocol("gmx_v2", "arbitrum") is injected
        avax = adapter._get_provider_for_protocol("gmx_v2", "avalanche")
        assert avax is not injected

    def test_explicit_key_scopes_a_chainless_provider(self):
        """'protocol:chain' seeding explicitly scopes a chain-agnostic provider."""
        from unittest.mock import MagicMock

        adapter = self._adapter()
        injected = MagicMock(spec=[])
        injected.chain = None
        adapter._seed_injected_provider("gmx_v2:avalanche", injected)

        assert adapter._get_provider_for_protocol("gmx_v2", "avalanche") is injected
        arb = adapter._get_provider_for_protocol("gmx_v2", "arbitrum")
        assert arb is not injected

    def test_explicit_key_contradicting_provider_chain_is_rejected(self):
        """An explicit scope contradicting the provider's declared chain is
        refused — serving one chain's data under another chain's key silently
        corrupts every funding number downstream."""
        from unittest.mock import MagicMock

        adapter = self._adapter()
        injected = MagicMock(spec=[])
        injected.chain = "arbitrum"
        adapter._seed_injected_provider("gmx_v2:avalanche", injected)

        assert ("gmx_v2", "avalanche") not in adapter._provider_cache
        assert adapter._get_provider_for_protocol("gmx_v2", "avalanche") is not injected

    def test_mock_auto_attribute_does_not_leak_into_chain_key(self):
        """A bare MagicMock's auto-created .chain (not a str) seeds the wildcard."""
        from unittest.mock import MagicMock

        adapter = self._adapter()
        injected = MagicMock()  # .chain auto-creates a truthy Mock, NOT a str
        adapter._seed_injected_provider("gmx_v2", injected)

        assert adapter._provider_cache[("gmx_v2", "*")] is injected


class TestPrewarmHistory:
    """prewarm_history fills _funding_cache for the whole window (P5).

    Branch coverage for the post-open funding prewarm: happy path, wrapped-
    native market normalization, fallback-row skip, best-effort failure,
    and every early-return guard (CRAP-gate coverage for the cc=11 method).
    """

    @staticmethod
    def _adapter() -> PerpBacktestAdapter:
        return PerpBacktestAdapter(PerpBacktestConfig(strategy_type="perp"))

    @staticmethod
    def _intent(protocol: str = "gmx_v2", market: str = "ETH/USD"):
        from types import SimpleNamespace

        return SimpleNamespace(protocol=protocol, market=market)

    @staticmethod
    def _window() -> tuple[datetime, datetime]:
        return datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)

    @staticmethod
    def _funding_result(hour: int, source: str = "gateway"):
        from almanak.framework.backtesting.pnl.types import (
            DataConfidence,
            DataSourceInfo,
            FundingResult,
        )

        return FundingResult(
            rate=Decimal("0.0001"),
            source_info=DataSourceInfo(
                source=source,
                confidence=DataConfidence.HIGH,
                timestamp=datetime(2024, 1, 1, hour, 30, tzinfo=UTC),
            ),
        )

    class _FakeProvider:
        def __init__(self, rates):
            self._rates = rates
            self.calls: list[dict] = []

        async def get_funding_rates(self, *, market, start_date, end_date):
            self.calls.append({"market": market, "start": start_date, "end": end_date})
            if isinstance(self._rates, Exception):
                raise self._rates
            return self._rates

    @pytest.mark.asyncio
    async def test_measured_rates_fill_cache_with_hour_keys(self, monkeypatch):
        adapter = self._adapter()
        provider = self._FakeProvider([self._funding_result(0), self._funding_result(1)])
        monkeypatch.setattr(adapter, "_get_provider_for_protocol", lambda protocol, chain=None: provider)
        start, end = self._window()

        await adapter.prewarm_history(self._intent(), "arbitrum", start, end)

        key = ("gmx_v2", "ETH-USD", datetime(2024, 1, 1, 0, tzinfo=UTC))
        assert key in adapter._funding_cache
        rate, confidence, label = adapter._funding_cache[key]
        assert rate == Decimal("0.0001")
        assert confidence == "high"
        assert label == "historical:gateway"
        assert len([k for k in adapter._funding_cache if k[0] == "gmx_v2"]) == 2

    @pytest.mark.asyncio
    async def test_prewarm_key_parity_with_funding_lookup(self, monkeypatch):
        """The prewarm's cache key must equal the key _funding_lookup reads
        for the same underlying — both normalize through the same
        token_ref_provider_symbol call; a divergent key silently defeats the
        prewarm (gemini review pin, restated as the parity contract)."""
        adapter = self._adapter()
        provider = self._FakeProvider([self._funding_result(0)])
        monkeypatch.setattr(adapter, "_get_provider_for_protocol", lambda protocol, chain=None: provider)
        start, end = self._window()

        await adapter.prewarm_history(self._intent(market="ETH/USD"), "arbitrum", start, end)

        position = create_perp_long_position(token="ETH", protocol="gmx_v2")
        lookup = adapter._funding_lookup(position, datetime(2024, 1, 1, 0, 30, tzinfo=UTC), "arbitrum")

        assert provider.calls[0]["market"] == lookup.market  # fetched what the reader will ask for
        cache_key = ("gmx_v2", lookup.market, datetime(2024, 1, 1, 0, tzinfo=UTC))
        assert cache_key in adapter._funding_cache  # the per-tick read is a cache HIT

    @pytest.mark.asyncio
    async def test_fallback_rows_are_never_frozen_into_the_cache(self, monkeypatch):
        adapter = self._adapter()
        provider = self._FakeProvider([self._funding_result(0, source="fallback"), self._funding_result(1)])
        monkeypatch.setattr(adapter, "_get_provider_for_protocol", lambda protocol, chain=None: provider)
        start, end = self._window()

        await adapter.prewarm_history(self._intent(), "arbitrum", start, end)

        cached = [k for k in adapter._funding_cache if k[0] == "gmx_v2"]
        assert cached == [("gmx_v2", "ETH-USD", datetime(2024, 1, 1, 1, tzinfo=UTC))]

    @pytest.mark.asyncio
    async def test_provider_failure_is_best_effort(self, monkeypatch):
        adapter = self._adapter()
        provider = self._FakeProvider(RuntimeError("gateway down"))
        monkeypatch.setattr(adapter, "_get_provider_for_protocol", lambda protocol, chain=None: provider)
        start, end = self._window()

        await adapter.prewarm_history(self._intent(), "arbitrum", start, end)  # must not raise

        assert not [k for k in adapter._funding_cache if k[0] == "gmx_v2"]

    @pytest.mark.asyncio
    async def test_missing_protocol_or_market_returns_before_provider_lookup(self, monkeypatch):
        adapter = self._adapter()

        def _fail(protocol, chain=None):
            raise AssertionError("provider must not be resolved for unroutable intents")

        monkeypatch.setattr(adapter, "_get_provider_for_protocol", _fail)
        start, end = self._window()

        await adapter.prewarm_history(self._intent(protocol=""), "arbitrum", start, end)
        await adapter.prewarm_history(self._intent(market=None), "arbitrum", start, end)
        await adapter.prewarm_history(self._intent(market=""), "arbitrum", start, end)

    @pytest.mark.asyncio
    async def test_no_provider_is_a_noop(self, monkeypatch):
        adapter = self._adapter()
        monkeypatch.setattr(adapter, "_get_provider_for_protocol", lambda protocol, chain=None: None)
        start, end = self._window()

        await adapter.prewarm_history(self._intent(), "arbitrum", start, end)

        assert not [k for k in adapter._funding_cache if k[0] == "gmx_v2"]

    @pytest.mark.asyncio
    async def test_empty_rates_warm_nothing(self, monkeypatch):
        adapter = self._adapter()
        provider = self._FakeProvider(None)
        monkeypatch.setattr(adapter, "_get_provider_for_protocol", lambda protocol, chain=None: provider)
        start, end = self._window()

        await adapter.prewarm_history(self._intent(), "arbitrum", start, end)

        assert not [k for k in adapter._funding_cache if k[0] == "gmx_v2"]
