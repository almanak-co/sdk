"""Unit tests for historical APY integration in lending adapter.

This module tests the integration of the LendingAPYProvider with the
InterestCalculator and LendingBacktestAdapter, verifying:

- Historical APY is queried when interest_rate_source='historical'
- Fallback to default APY when historical data unavailable
- Logging of APY source (historical vs default)
- Correct APY values used in interest calculations

User Story: US-054b - Integrate historical APY into lending adapter
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.adapters.lending_adapter import (
    LendingBacktestAdapter,
    LendingBacktestConfig,
)
from almanak.framework.backtesting.pnl.calculators.interest import (
    InterestCalculator,
    InterestRateSource,
)
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedPosition,
)

# =============================================================================
# Mock Classes
# =============================================================================


@dataclass
class MockMarketState:
    """Mock market state for testing."""

    prices: dict[str, Decimal] = field(default_factory=dict)
    timestamp: datetime | None = None

    def get_price(self, token: str) -> Decimal | None:
        """Get price for a token."""
        if token not in self.prices:
            raise KeyError(f"Price not found for {token}")
        return self.prices.get(token)


@dataclass
class MockLendingAPYData:
    """Mock APY data returned by provider."""

    protocol: str
    market: str
    timestamp: datetime
    supply_apy: Decimal
    borrow_apy: Decimal
    supply_apy_pct: Decimal = Decimal("0")
    borrow_apy_pct: Decimal = Decimal("0")
    source: str = "mock_subgraph"

    def __post_init__(self) -> None:
        if self.supply_apy_pct == Decimal("0") and self.supply_apy != Decimal("0"):
            self.supply_apy_pct = self.supply_apy * Decimal("100")
        if self.borrow_apy_pct == Decimal("0") and self.borrow_apy != Decimal("0"):
            self.borrow_apy_pct = self.borrow_apy * Decimal("100")


def create_supply_position(
    token: str = "WETH",
    amount: Decimal = Decimal("10"),
    entry_price: Decimal = Decimal("2000"),
    entry_time: datetime | None = None,
    apy: Decimal | None = None,
    protocol: str = "aave_v3",
) -> SimulatedPosition:
    """Create a mock supply position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    return SimulatedPosition.supply(
        protocol=protocol,
        token=token,
        amount=amount,
        entry_price=entry_price,
        entry_time=entry_time,
        apy=apy,
    )


def create_borrow_position(
    token: str = "USDC",
    amount: Decimal = Decimal("10000"),
    entry_price: Decimal = Decimal("1"),
    entry_time: datetime | None = None,
    apy: Decimal | None = None,
    protocol: str = "aave_v3",
    health_factor: Decimal = Decimal("1.5"),
) -> SimulatedPosition:
    """Create a mock borrow position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    return SimulatedPosition.borrow(
        protocol=protocol,
        token=token,
        amount=amount,
        entry_price=entry_price,
        entry_time=entry_time,
        apy=apy,
        health_factor=health_factor,
    )


# =============================================================================
# InterestCalculator Historical APY Tests
# =============================================================================


class TestInterestCalculatorHistoricalAPY:
    """Tests for InterestCalculator historical APY integration."""

    def test_fixed_source_returns_default_apy(self) -> None:
        """Test that FIXED source returns default APY without provider."""
        calculator = InterestCalculator(
            interest_rate_source=InterestRateSource.FIXED,
            default_supply_apy=Decimal("0.04"),
            default_borrow_apy=Decimal("0.06"),
        )

        supply_apy = calculator.get_supply_apy_for_protocol("aave_v3")
        borrow_apy = calculator.get_borrow_apy_for_protocol("aave_v3")

        assert supply_apy == Decimal("0.03")  # Protocol-specific default
        assert borrow_apy == Decimal("0.05")  # Protocol-specific default

    def test_protocol_source_returns_protocol_apy(self) -> None:
        """Test that PROTOCOL source returns protocol-specific APY."""
        calculator = InterestCalculator(
            interest_rate_source=InterestRateSource.PROTOCOL,
        )

        # Known protocol rates
        assert calculator.get_supply_apy_for_protocol("aave_v3") == Decimal("0.03")
        assert calculator.get_supply_apy_for_protocol("compound_v3") == Decimal("0.025")
        assert calculator.get_borrow_apy_for_protocol("morpho") == Decimal("0.04")

    def test_historical_source_with_provider_unavailable(self) -> None:
        """Test fallback when historical source configured but provider unavailable."""
        # Patch the import to simulate provider not available
        with patch(
            "almanak.framework.backtesting.pnl.calculators.interest.InterestCalculator._init_apy_provider"
        ) as mock_init:
            mock_init.return_value = None

            calculator = InterestCalculator(
                interest_rate_source=InterestRateSource.HISTORICAL,
                chain="ethereum",
            )

            # Should fall back to default
            apy = calculator.get_historical_supply_apy_sync(
                protocol="aave_v3",
                market="USDC",
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            )

            # Should return protocol default
            assert apy == Decimal("0.03")

    @pytest.mark.asyncio
    async def test_historical_supply_apy_success(self) -> None:
        """Test successful historical supply APY fetch."""
        calculator = InterestCalculator(
            interest_rate_source=InterestRateSource.HISTORICAL,
            chain="ethereum",
        )

        # Mock the APY provider
        mock_apy_data = MockLendingAPYData(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            supply_apy=Decimal("0.045"),
            borrow_apy=Decimal("0.065"),
        )

        mock_provider = MagicMock()
        mock_provider.get_historical_apy = AsyncMock(return_value=mock_apy_data)
        calculator._apy_provider = mock_provider

        apy = await calculator.get_historical_supply_apy(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, tzinfo=UTC),
        )

        assert apy == Decimal("0.045")
        mock_provider.get_historical_apy.assert_called_once()

    @pytest.mark.asyncio
    async def test_historical_borrow_apy_success(self) -> None:
        """Test successful historical borrow APY fetch."""
        calculator = InterestCalculator(
            interest_rate_source=InterestRateSource.HISTORICAL,
            chain="arbitrum",
        )

        mock_apy_data = MockLendingAPYData(
            protocol="compound_v3",
            market="WETH",
            timestamp=datetime(2024, 1, 20, tzinfo=UTC),
            supply_apy=Decimal("0.025"),
            borrow_apy=Decimal("0.055"),
        )

        mock_provider = MagicMock()
        mock_provider.get_historical_apy = AsyncMock(return_value=mock_apy_data)
        calculator._apy_provider = mock_provider

        apy = await calculator.get_historical_borrow_apy(
            protocol="compound_v3",
            market="WETH",
            timestamp=datetime(2024, 1, 20, tzinfo=UTC),
        )

        assert apy == Decimal("0.055")

    @pytest.mark.asyncio
    async def test_historical_apy_fallback_on_error(self) -> None:
        """Test fallback to default when historical fetch fails."""
        calculator = InterestCalculator(
            interest_rate_source=InterestRateSource.HISTORICAL,
            chain="ethereum",
        )

        mock_provider = MagicMock()
        mock_provider.get_historical_apy = AsyncMock(side_effect=Exception("API error"))
        calculator._apy_provider = mock_provider

        # Should fall back to default
        apy = await calculator.get_historical_supply_apy(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, tzinfo=UTC),
        )

        assert apy == Decimal("0.03")  # Protocol default

    @pytest.mark.asyncio
    async def test_non_historical_source_skips_provider(self) -> None:
        """Test that non-HISTORICAL source skips provider even if available."""
        calculator = InterestCalculator(
            interest_rate_source=InterestRateSource.FIXED,
        )

        mock_provider = MagicMock()
        mock_provider.get_historical_apy = AsyncMock()
        calculator._apy_provider = mock_provider

        apy = await calculator.get_historical_supply_apy(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, tzinfo=UTC),
        )

        # Should return default without calling provider
        assert apy == Decimal("0.03")
        mock_provider.get_historical_apy.assert_not_called()

    def test_to_dict_includes_chain_and_provider_status(self) -> None:
        """Test serialization includes chain and provider availability."""
        calculator = InterestCalculator(
            interest_rate_source=InterestRateSource.HISTORICAL,
            chain="arbitrum",
        )

        d = calculator.to_dict()

        assert d["chain"] == "arbitrum"
        assert "apy_provider_available" in d


# =============================================================================
# LendingBacktestAdapter Historical APY Tests
# =============================================================================


class TestLendingAdapterHistoricalAPY:
    """Tests for LendingBacktestAdapter historical APY integration."""

    def test_adapter_with_historical_config(self) -> None:
        """Test adapter initialization with historical interest rate source."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="historical",
            protocol="aave_v3",
        )
        adapter = LendingBacktestAdapter(config)

        assert adapter.config.interest_rate_source == "historical"
        # InterestCalculator should be configured for historical
        assert adapter._interest_calculator.interest_rate_source == InterestRateSource.HISTORICAL

    def test_adapter_with_fixed_config(self) -> None:
        """Test adapter initialization with fixed interest rate source."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="fixed",
            default_supply_apy=Decimal("0.04"),
            default_borrow_apy=Decimal("0.06"),
        )
        adapter = LendingBacktestAdapter(config)

        assert adapter.config.interest_rate_source == "fixed"
        assert adapter._interest_calculator.interest_rate_source == InterestRateSource.FIXED

    def test_update_position_uses_historical_apy(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that update_position uses historical APY when configured."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="historical",
        )
        adapter = LendingBacktestAdapter(config)

        # Mock the sync APY fetch to return a specific rate
        with patch.object(
            adapter._interest_calculator,
            "get_historical_supply_apy_sync",
            return_value=Decimal("0.05"),
        ) as mock_fetch:
            entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
            position = create_supply_position(
                token="USDC",
                amount=Decimal("10000"),
                entry_price=Decimal("1"),
                entry_time=entry_time,
                apy=None,  # No entry APY, should use historical
            )

            market = MockMarketState(
                prices={"USDC": Decimal("1")},
                timestamp=datetime(2024, 1, 2, 0, 0, tzinfo=UTC),
            )

            adapter.update_position(position, market, elapsed_seconds=86400)

            # Should have called historical fetch
            mock_fetch.assert_called_once()
            call_args = mock_fetch.call_args
            assert call_args[1]["protocol"] == "aave_v3"
            assert call_args[1]["market"] == "USDC"

    def test_update_position_uses_position_apy_when_set(self) -> None:
        """Test that position's entry APY takes precedence over historical."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="historical",
        )
        adapter = LendingBacktestAdapter(config)

        # Mock historical fetch (should not be called)
        with patch.object(
            adapter._interest_calculator,
            "get_historical_supply_apy_sync",
            return_value=Decimal("0.05"),
        ) as mock_fetch:
            entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
            position = create_supply_position(
                token="WETH",
                amount=Decimal("10"),
                entry_price=Decimal("2000"),
                entry_time=entry_time,
                apy=Decimal("0.035"),  # Explicit entry APY
            )

            market = MockMarketState(
                prices={"WETH": Decimal("2000")},
                timestamp=datetime(2024, 1, 2, 0, 0, tzinfo=UTC),
            )

            adapter.update_position(position, market, elapsed_seconds=86400)

            # Historical fetch should NOT be called when position has APY
            mock_fetch.assert_not_called()

    def test_update_position_uses_fixed_when_configured(self) -> None:
        """Test that fixed APY is used when interest_rate_source='fixed'."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="fixed",
            default_supply_apy=Decimal("0.04"),
        )
        adapter = LendingBacktestAdapter(config)

        # Mock historical fetch (should not be called)
        with patch.object(
            adapter._interest_calculator,
            "get_historical_supply_apy_sync",
        ) as mock_fetch:
            entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
            position = create_supply_position(
                token="USDC",
                amount=Decimal("10000"),
                entry_price=Decimal("1"),
                entry_time=entry_time,
                apy=None,
            )

            market = MockMarketState(
                prices={"USDC": Decimal("1")},
                timestamp=datetime(2024, 1, 2, 0, 0, tzinfo=UTC),
            )

            adapter.update_position(position, market, elapsed_seconds=86400)

            # Should NOT call historical fetch
            mock_fetch.assert_not_called()

    def test_borrow_position_uses_historical_apy(self) -> None:
        """Test that borrow positions use historical APY."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="historical",
            health_factor_tracking_enabled=False,  # Disable HF for simpler test
        )
        adapter = LendingBacktestAdapter(config)

        with patch.object(
            adapter._interest_calculator,
            "get_historical_borrow_apy_sync",
            return_value=Decimal("0.07"),
        ) as mock_fetch:
            entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
            position = create_borrow_position(
                token="USDC",
                amount=Decimal("10000"),
                entry_price=Decimal("1"),
                entry_time=entry_time,
                apy=None,
            )

            market = MockMarketState(
                prices={"USDC": Decimal("1")},
                timestamp=datetime(2024, 1, 2, 0, 0, tzinfo=UTC),
            )

            adapter.update_position(position, market, elapsed_seconds=86400)

            mock_fetch.assert_called_once()
            call_args = mock_fetch.call_args
            assert call_args[1]["protocol"] == "aave_v3"
            assert call_args[1]["market"] == "USDC"

    def test_interest_accrual_with_historical_apy(self) -> None:
        """Test that interest is calculated correctly with historical APY."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="historical",
            interest_accrual_method="compound",
        )
        adapter = LendingBacktestAdapter(config)

        # Return 10% APY from historical provider
        with patch.object(
            adapter._interest_calculator,
            "get_historical_supply_apy_sync",
            return_value=Decimal("0.10"),  # 10% APY
        ):
            entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
            position = create_supply_position(
                token="USDC",
                amount=Decimal("10000"),  # $10,000
                entry_price=Decimal("1"),
                entry_time=entry_time,
                apy=None,
            )

            market = MockMarketState(
                prices={"USDC": Decimal("1")},
                timestamp=datetime(2024, 1, 2, 0, 0, tzinfo=UTC),
            )

            # 1 day = 86400 seconds
            adapter.update_position(position, market, elapsed_seconds=86400)

            # Expected daily interest at 10% APY: ~$2.74 per day
            # $10,000 * 0.10 / 365 = ~$2.74
            expected_daily = Decimal("10000") * Decimal("0.10") / Decimal("365")
            assert position.interest_accrued == pytest.approx(expected_daily, rel=Decimal("0.01"))


# =============================================================================
# Logging Tests
# =============================================================================


class TestAPYSourceLogging:
    """Tests for APY source logging."""

    def test_logs_historical_source(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that historical source is logged."""
        import logging

        # Enable logging for the lending adapter module
        caplog.set_level(logging.INFO, logger="almanak.framework.backtesting.adapters.lending_adapter")

        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="historical",
        )
        adapter = LendingBacktestAdapter(config)

        with patch.object(
            adapter._interest_calculator,
            "get_historical_supply_apy_sync",
            return_value=Decimal("0.05"),
        ):
            entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
            position = create_supply_position(
                token="USDC",
                amount=Decimal("100000"),  # Large amount to trigger info log
                entry_price=Decimal("1"),
                entry_time=entry_time,
                apy=None,
            )

            market = MockMarketState(
                prices={"USDC": Decimal("1")},
                timestamp=datetime(2024, 1, 2, 0, 0, tzinfo=UTC),
            )

            adapter.update_position(position, market, elapsed_seconds=86400)

            # Check that logging includes source
            log_messages = [r.message for r in caplog.records]
            # Check for either "source: historical" or "source: legacy_historical" to support
            # both new BacktestDataConfig approach and legacy InterestCalculator approach
            apy_log = [m for m in log_messages if "source: legacy_historical" in m or "source: historical" in m]
            assert len(apy_log) > 0, f"Expected 'source: *historical' in logs, got: {log_messages}"

    def test_logs_fixed_source(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that fixed source is logged."""
        import logging

        # Enable logging for the lending adapter module
        caplog.set_level(logging.INFO, logger="almanak.framework.backtesting.adapters.lending_adapter")

        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="fixed",
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="USDC",
            amount=Decimal("100000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=None,
        )

        market = MockMarketState(
            prices={"USDC": Decimal("1")},
            timestamp=datetime(2024, 1, 2, 0, 0, tzinfo=UTC),
        )

        adapter.update_position(position, market, elapsed_seconds=86400)

        log_messages = [r.message for r in caplog.records]
        apy_log = [m for m in log_messages if "source: fixed" in m]
        assert len(apy_log) > 0, f"Expected 'source: fixed' in logs, got: {log_messages}"

    def test_logs_position_entry_source(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that position_entry source is logged when APY is from position."""
        import logging

        # Enable logging for the lending adapter module
        caplog.set_level(logging.INFO, logger="almanak.framework.backtesting.adapters.lending_adapter")

        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="historical",
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="USDC",
            amount=Decimal("100000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.05"),  # Has entry APY
        )

        market = MockMarketState(
            prices={"USDC": Decimal("1")},
            timestamp=datetime(2024, 1, 2, 0, 0, tzinfo=UTC),
        )

        adapter.update_position(position, market, elapsed_seconds=86400)

        log_messages = [r.message for r in caplog.records]
        apy_log = [m for m in log_messages if "source: position_entry" in m]
        assert len(apy_log) > 0, f"Expected 'source: position_entry' in logs, got: {log_messages}"


# =============================================================================
# Config Serialization Tests
# =============================================================================


class TestConfigSerialization:
    """Tests for config serialization with interest_rate_source."""

    def test_config_to_dict_includes_rate_source(self) -> None:
        """Test that config serialization includes interest_rate_source."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="historical",
            protocol="compound_v3",
        )

        d = config.to_dict()

        assert d["interest_rate_source"] == "historical"
        assert d["protocol"] == "compound_v3"

    def test_config_from_dict_parses_rate_source(self) -> None:
        """Test that config deserialization handles interest_rate_source."""
        data = {
            "strategy_type": "lending",
            "interest_rate_source": "historical",
            "protocol": "aave_v3",
        }

        config = LendingBacktestConfig.from_dict(data)

        assert config.interest_rate_source == "historical"
        assert config.protocol == "aave_v3"

    def test_config_roundtrip(self) -> None:
        """Test config survives roundtrip serialization."""
        original = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="historical",
            default_supply_apy=Decimal("0.04"),
            default_borrow_apy=Decimal("0.06"),
            protocol="compound_v3",
        )

        restored = LendingBacktestConfig.from_dict(original.to_dict())

        assert restored.interest_rate_source == original.interest_rate_source
        assert restored.protocol == original.protocol
        assert restored.default_supply_apy == original.default_supply_apy
        assert restored.default_borrow_apy == original.default_borrow_apy
