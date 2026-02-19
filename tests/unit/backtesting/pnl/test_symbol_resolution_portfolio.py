"""Tests for symbol resolution in portfolio valuation.

This test suite validates that:
1. Portfolio valuation resolves addresses to symbols using token registry
2. require_symbol_mapping config enforces symbol resolution
3. Unresolved tokens are tracked in DataQualityReport
4. Warnings are logged for unknown tokens when require_symbol_mapping is False

Part of US-083b: Enforce symbol resolution in portfolio valuation (P0-AUDIT).
"""

import logging
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.backtesting.models import DataQualityReport
from almanak.framework.backtesting.paper.token_registry import (
    CHAIN_ID_ARBITRUM,
    CHAIN_ID_ETHEREUM,
)
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import DataQualityTracker
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPortfolio,
    SimulatedPosition,
)

# Test timestamp for positions
TEST_TIME = datetime(2024, 1, 1, 12, 0, 0)

# Known token addresses for testing
USDC_ETHEREUM = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH_ETHEREUM = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
UNKNOWN_TOKEN = "0x1234567890123456789012345678901234567890"


class TestDataQualityReportUnresolvedTokenCount:
    """Tests for unresolved_token_count field in DataQualityReport."""

    def test_data_quality_report_has_unresolved_token_count(self):
        """Test DataQualityReport has unresolved_token_count field with default 0."""
        report = DataQualityReport()
        assert hasattr(report, "unresolved_token_count")
        assert report.unresolved_token_count == 0

    def test_data_quality_report_unresolved_token_count_serialization(self):
        """Test unresolved_token_count is serialized to dict."""
        report = DataQualityReport(unresolved_token_count=5)
        data = report.to_dict()
        assert "unresolved_token_count" in data
        assert data["unresolved_token_count"] == 5

    def test_data_quality_report_unresolved_token_count_deserialization(self):
        """Test unresolved_token_count is deserialized from dict."""
        data = {"unresolved_token_count": 3}
        report = DataQualityReport.from_dict(data)
        assert report.unresolved_token_count == 3

    def test_data_quality_report_unresolved_token_count_default_on_missing(self):
        """Test unresolved_token_count defaults to 0 when missing from dict."""
        data = {}
        report = DataQualityReport.from_dict(data)
        assert report.unresolved_token_count == 0


class TestDataQualityTrackerUnresolvedTokens:
    """Tests for unresolved token tracking in DataQualityTracker."""

    def test_tracker_has_unresolved_token_count(self):
        """Test DataQualityTracker has unresolved_token_count field."""
        tracker = DataQualityTracker()
        assert hasattr(tracker, "unresolved_token_count")
        assert tracker.unresolved_token_count == 0

    def test_record_unresolved_token(self):
        """Test recording an unresolved token increments count."""
        tracker = DataQualityTracker()
        tracker.record_unresolved_token(UNKNOWN_TOKEN, CHAIN_ID_ETHEREUM)
        assert tracker.unresolved_token_count == 1

    def test_record_unresolved_token_unique(self):
        """Test same token is only counted once."""
        tracker = DataQualityTracker()
        tracker.record_unresolved_token(UNKNOWN_TOKEN, CHAIN_ID_ETHEREUM)
        tracker.record_unresolved_token(UNKNOWN_TOKEN, CHAIN_ID_ETHEREUM)
        tracker.record_unresolved_token(UNKNOWN_TOKEN, CHAIN_ID_ETHEREUM)
        assert tracker.unresolved_token_count == 1

    def test_record_unresolved_token_different_chains(self):
        """Test same token on different chains counted separately."""
        tracker = DataQualityTracker()
        tracker.record_unresolved_token(UNKNOWN_TOKEN, CHAIN_ID_ETHEREUM)
        tracker.record_unresolved_token(UNKNOWN_TOKEN, CHAIN_ID_ARBITRUM)
        assert tracker.unresolved_token_count == 2

    def test_to_data_quality_report_includes_unresolved_count(self):
        """Test to_data_quality_report includes unresolved_token_count."""
        tracker = DataQualityTracker()
        tracker.record_unresolved_token(UNKNOWN_TOKEN, CHAIN_ID_ETHEREUM)
        report = tracker.to_data_quality_report()
        assert report.unresolved_token_count == 1


class TestPortfolioSymbolResolution:
    """Tests for symbol resolution in SimulatedPortfolio valuation."""

    @pytest.fixture
    def market_state(self):
        """Create a market state with prices keyed by symbol."""
        return MarketState(
            timestamp=MagicMock(),
            prices={
                "USDC": Decimal("1.0"),
                "WETH": Decimal("3000.0"),
                "ETH": Decimal("3000.0"),
            },
        )

    @pytest.fixture
    def portfolio_with_symbol_tokens(self):
        """Create a portfolio with tokens keyed by symbols (no cash)."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {
            "USDC": Decimal("5000"),
            "WETH": Decimal("1.5"),
        }
        return portfolio

    @pytest.fixture
    def portfolio_with_address_tokens(self):
        """Create a portfolio with tokens keyed by addresses (no cash)."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {
            USDC_ETHEREUM: Decimal("5000"),
            WETH_ETHEREUM: Decimal("1.5"),
        }
        return portfolio

    def test_valuation_with_symbols_works(self, portfolio_with_symbol_tokens, market_state):
        """Test valuation works normally when tokens are already symbols."""
        value = portfolio_with_symbol_tokens.get_total_value_usd(market_state)
        # 5000 USDC * 1 + 1.5 WETH * 3000 = 5000 + 4500 = 9500
        assert value == Decimal("9500")

    def test_valuation_resolves_addresses_to_symbols(self, portfolio_with_address_tokens, market_state):
        """Test valuation resolves known addresses to symbols."""
        # Update market state to include prices by symbol (as would be populated)
        value = portfolio_with_address_tokens.get_total_value_usd(
            market_state,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        # After resolution: USDC_ETHEREUM -> USDC, WETH_ETHEREUM -> WETH
        # 5000 USDC * 1 + 1.5 WETH * 3000 = 5000 + 4500 = 9500
        assert value == Decimal("9500")

    def test_require_symbol_mapping_fails_on_unknown_token(self, market_state):
        """Test valuation fails with unknown token when require_symbol_mapping=True."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {
            UNKNOWN_TOKEN: Decimal("1000"),
        }

        with pytest.raises(ValueError, match="cannot be resolved to a symbol"):
            portfolio.get_total_value_usd(
                market_state,
                require_symbol_mapping=True,
                chain_id=CHAIN_ID_ETHEREUM,
            )

    def test_unknown_token_logs_warning_when_not_required(self, market_state, caplog):
        """Test unknown token logs warning when require_symbol_mapping=False."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {
            UNKNOWN_TOKEN: Decimal("1000"),
        }

        with caplog.at_level(logging.WARNING):
            portfolio.get_total_value_usd(
                market_state,
                require_symbol_mapping=False,
                chain_id=CHAIN_ID_ETHEREUM,
            )

        assert "Unknown token address" in caplog.text
        assert UNKNOWN_TOKEN in caplog.text

    def test_unresolved_token_tracked_in_data_tracker(self, market_state):
        """Test unresolved tokens are recorded in DataQualityTracker."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {
            UNKNOWN_TOKEN: Decimal("1000"),
        }
        tracker = DataQualityTracker()

        portfolio.get_total_value_usd(
            market_state,
            require_symbol_mapping=False,
            chain_id=CHAIN_ID_ETHEREUM,
            data_tracker=tracker,
        )

        assert tracker.unresolved_token_count == 1


class TestPositionValueSymbolResolution:
    """Tests for symbol resolution in _get_position_value method."""

    @pytest.fixture
    def market_state(self):
        """Create a market state with prices keyed by symbol."""
        return MarketState(
            timestamp=MagicMock(),
            prices={
                "USDC": Decimal("1.0"),
                "WETH": Decimal("3000.0"),
                "ETH": Decimal("3000.0"),
            },
        )

    def test_spot_position_resolves_address(self, market_state):
        """Test spot position resolves token address to symbol."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        position = SimulatedPosition(
            position_type=PositionType.SPOT,
            protocol="spot",
            tokens=[WETH_ETHEREUM],
            amounts={WETH_ETHEREUM: Decimal("2.0")},
            entry_price=Decimal("2800"),
            entry_time=TEST_TIME,
        )
        portfolio.positions = [position]

        value = portfolio.get_total_value_usd(
            market_state,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        # 2.0 WETH * 3000 = 6000
        assert value == Decimal("6000")

    def test_lp_position_resolves_addresses(self, market_state):
        """Test LP position resolves both token addresses to symbols."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        position = SimulatedPosition(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            tokens=[WETH_ETHEREUM, USDC_ETHEREUM],
            amounts={
                WETH_ETHEREUM: Decimal("1.0"),
                USDC_ETHEREUM: Decimal("3000"),
            },
            entry_price=Decimal("3000"),
            entry_time=TEST_TIME,
            fees_earned=Decimal("50"),
        )
        portfolio.positions = [position]

        value = portfolio.get_total_value_usd(
            market_state,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        # 1.0 WETH * 3000 + 3000 USDC * 1 + 50 fees = 3000 + 3000 + 50 = 6050
        assert value == Decimal("6050")

    def test_perp_position_resolves_address(self, market_state):
        """Test perp position resolves primary token address."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        position = SimulatedPosition(
            position_type=PositionType.PERP_LONG,
            protocol="gmx_v2",
            tokens=[WETH_ETHEREUM],
            amounts={WETH_ETHEREUM: Decimal("0")},
            entry_price=Decimal("2800"),
            entry_time=TEST_TIME,
            collateral_usd=Decimal("1000"),
            notional_usd=Decimal("5000"),
            accumulated_funding=Decimal("-10"),
        )
        portfolio.positions = [position]

        value = portfolio.get_total_value_usd(
            market_state,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        # PnL = ((3000 - 2800) / 2800) * 5000 = 357.14...
        # Total = 1000 + 357.14 - 10 = 1347.14...
        assert value > Decimal("1000")

    def test_lending_position_resolves_address(self, market_state):
        """Test lending position resolves primary token address."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        position = SimulatedPosition(
            position_type=PositionType.SUPPLY,
            protocol="aave_v3",
            tokens=[USDC_ETHEREUM],
            amounts={USDC_ETHEREUM: Decimal("5000")},
            entry_price=Decimal("1.0"),
            entry_time=TEST_TIME,
            interest_accrued=Decimal("25"),
        )
        portfolio.positions = [position]

        value = portfolio.get_total_value_usd(
            market_state,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        # 5000 USDC * 1 + 25 interest = 5025
        assert value == Decimal("5025")


class TestRequireSymbolMappingWithoutChainId:
    """Tests for require_symbol_mapping behavior without chain_id."""

    @pytest.fixture
    def market_state(self):
        """Create a market state."""
        return MarketState(
            timestamp=TEST_TIME,
            prices={"WETH": Decimal("3000.0")},
        )

    def test_require_symbol_mapping_without_chain_id_fails(self, market_state):
        """Test require_symbol_mapping=True without chain_id fails for addresses."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {WETH_ETHEREUM: Decimal("1.0")}

        with pytest.raises(ValueError, match="cannot be resolved without chain_id"):
            portfolio.get_total_value_usd(
                market_state,
                require_symbol_mapping=True,
                chain_id=None,
            )

    def test_symbol_tokens_work_without_chain_id(self, market_state):
        """Test symbol tokens work without chain_id."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {"WETH": Decimal("1.0")}

        value = portfolio.get_total_value_usd(
            market_state,
            require_symbol_mapping=True,
            chain_id=None,
        )
        assert value == Decimal("3000")
