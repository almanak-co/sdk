"""Integration tests for token identity consistency throughout backtest.

This test suite validates the P0-AUDIT requirement (US-083c) that token identity
is consistent from receipt parsing through portfolio valuation:

1. Receipt parsing outputs symbols (not addresses)
2. Portfolio tracks balances by symbol
3. Price lookup uses correct symbol
4. Final valuation is accurate for known tokens
5. Warning/failure for unknown tokens

These tests ensure that the token registry (US-083a) and symbol resolution (US-083b)
work together to prevent portfolio balance drift and mispricing.

Part of US-083c: [P0-AUDIT] Add token identity integration tests.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.backtesting.models import DataQualityReport
from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker
from almanak.framework.backtesting.paper.token_registry import (
    CHAIN_ID_ARBITRUM,
    CHAIN_ID_BASE,
    CHAIN_ID_ETHEREUM,
    get_token_info,
    get_token_symbol,
    is_token_known,
    resolve_to_canonical_symbol,
)
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import DataQualityTracker
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPortfolio,
    SimulatedPosition,
)

# =============================================================================
# Constants - Known Token Addresses
# =============================================================================

# Ethereum Mainnet
ETH_USDC_ADDRESS = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
ETH_WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
ETH_WBTC_ADDRESS = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

# Arbitrum
ARB_USDC_ADDRESS = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
ARB_WETH_ADDRESS = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
ARB_ARB_ADDRESS = "0x912ce59144191c1204e64559fe8253a0e49e6548"

# Base
BASE_USDC_ADDRESS = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
BASE_WETH_ADDRESS = "0x4200000000000000000000000000000000000006"

# Unknown token for testing
UNKNOWN_TOKEN_ADDRESS = "0x1234567890123456789012345678901234567890"

# Test timestamp
TEST_TIME = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


# =============================================================================
# Helper Classes
# =============================================================================


@dataclass
class MockMarketState:
    """Mock market state for testing."""

    prices: dict[str, Decimal] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    available_tokens: set[str] = field(default_factory=set)

    def get_price(self, token: str) -> Decimal | None:
        """Get price for a token by symbol."""
        return self.prices.get(token)


# =============================================================================
# Test Class 1: Receipt Parsing Outputs Symbols (Not Addresses)
# =============================================================================


class TestReceiptParsingOutputsSymbols:
    """Tests validating that receipt parsing outputs symbols, not addresses.

    Acceptance Criterion #1: Test receipt parsing outputs symbols (not addresses)
    """

    def test_known_token_resolved_to_symbol(self):
        """Test that known token address resolves to canonical symbol."""
        # Verify USDC on Ethereum resolves to "USDC"
        symbol = resolve_to_canonical_symbol(CHAIN_ID_ETHEREUM, ETH_USDC_ADDRESS)
        assert symbol == "USDC", f"Expected 'USDC', got '{symbol}'"

        # Verify WETH on Arbitrum resolves to "WETH"
        symbol = resolve_to_canonical_symbol(CHAIN_ID_ARBITRUM, ARB_WETH_ADDRESS)
        assert symbol == "WETH", f"Expected 'WETH', got '{symbol}'"

    def test_symbol_resolution_case_insensitive(self):
        """Test that symbol resolution is case-insensitive for addresses."""
        # Test lowercase
        symbol_lower = resolve_to_canonical_symbol(CHAIN_ID_ETHEREUM, ETH_USDC_ADDRESS.lower())
        # Test uppercase
        symbol_upper = resolve_to_canonical_symbol(CHAIN_ID_ETHEREUM, ETH_USDC_ADDRESS.upper())
        # Test mixed case (checksummed)
        symbol_checksum = resolve_to_canonical_symbol(CHAIN_ID_ETHEREUM, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

        assert symbol_lower == symbol_upper == symbol_checksum == "USDC"

    def test_unknown_token_returns_checksummed_address(self):
        """Test that unknown token returns checksummed address as symbol."""
        symbol = resolve_to_canonical_symbol(CHAIN_ID_ETHEREUM, UNKNOWN_TOKEN_ADDRESS)

        # Should be the address (checksummed format)
        assert symbol.lower() == UNKNOWN_TOKEN_ADDRESS.lower()
        # Should not be a human-readable symbol
        assert not symbol.isalpha()

    @pytest.mark.asyncio
    async def test_extract_token_flows_uses_symbols(self):
        """Test that _extract_token_flows returns symbol keys, not addresses."""
        from almanak.framework.backtesting.paper.engine import (
            PaperTrader as RealPaperTrader,
        )

        wallet = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

        # Create mock receipt with Transfer events
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        logs = [
            {
                "address": ETH_USDC_ADDRESS,
                "topics": [
                    transfer_topic,
                    "0x" + "0" * 24 + wallet[2:].lower(),  # from: wallet
                    "0x" + "0" * 24 + "def" * 13 + "d",  # to: DEX
                ],
                "data": "0x" + hex(1_000_000_000)[2:].zfill(64),  # 1000 USDC
            },
            {
                "address": ETH_WETH_ADDRESS,
                "topics": [
                    transfer_topic,
                    "0x" + "0" * 24 + "def" * 13 + "d",  # from: DEX
                    "0x" + "0" * 24 + wallet[2:].lower(),  # to: wallet
                ],
                "data": "0x" + hex(500_000_000_000_000_000)[2:].zfill(64),  # 0.5 WETH
            },
        ]

        receipt = MagicMock()
        receipt.to_dict.return_value = {
            "status": 1,
            "logs": logs,
            "block_number": 12345678,
            "gas_used": 150000,
        }

        # Create mock paper trader
        fork_manager = MagicMock()
        fork_manager.chain_id = CHAIN_ID_ETHEREUM
        fork_manager.is_running = True
        fork_manager.get_rpc_url.return_value = "http://localhost:8545"

        trader = MagicMock(spec=RealPaperTrader)
        trader.fork_manager = fork_manager
        trader._backtest_id = "test-token-identity"

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # Verify symbols are used as keys
        assert "WETH" in tokens_in, f"Expected 'WETH' in tokens_in: {tokens_in}"
        assert "USDC" in tokens_out, f"Expected 'USDC' in tokens_out: {tokens_out}"

        # Verify addresses are NOT used as keys
        assert ETH_WETH_ADDRESS not in tokens_in, f"Address should not be key: {tokens_in}"
        assert ETH_USDC_ADDRESS not in tokens_out, f"Address should not be key: {tokens_out}"


# =============================================================================
# Test Class 2: Portfolio Tracks Balances by Symbol
# =============================================================================


class TestPortfolioTracksBalancesBySymbol:
    """Tests validating that portfolio tracks balances by symbol.

    Acceptance Criterion #2: Test portfolio tracks balances by symbol
    """

    def test_portfolio_tracker_uses_symbols(self):
        """Test PaperPortfolioTracker uses symbols as balance keys."""
        tracker = PaperPortfolioTracker(
            strategy_id="symbol_test",
            chain="ethereum",
        )

        # Start with symbol-keyed balances
        initial_balances = {
            "USDC": Decimal("10000"),
            "WETH": Decimal("5"),
        }
        tracker.start_session(initial_balances)

        # Verify balances use symbols
        assert "USDC" in tracker.current_balances
        assert "WETH" in tracker.current_balances
        assert tracker.current_balances["USDC"] == Decimal("10000")
        assert tracker.current_balances["WETH"] == Decimal("5")

    def test_trade_recording_preserves_symbols(self):
        """Test that trade recording preserves symbol keys."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        tracker = PaperPortfolioTracker(
            strategy_id="trade_symbol_test",
            chain="ethereum",
        )

        initial_balances = {"USDC": Decimal("10000")}
        tracker.start_session(initial_balances)

        # Record a trade with symbol keys
        trade = PaperTrade(
            timestamp=TEST_TIME,
            block_number=12345678,
            intent={"type": "SWAP", "from_token": "USDC", "to_token": "WETH"},
            tx_hash="0x" + "a" * 64,
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"WETH": Decimal("0.5")},
            tokens_out={"USDC": Decimal("1000")},
            protocol="uniswap_v3",
        )
        tracker.record_trade(trade)

        # Verify balances use symbols after trade
        assert tracker.current_balances["USDC"] == Decimal("9000")
        assert tracker.current_balances["WETH"] == Decimal("0.5")

    def test_simulated_portfolio_tokens_by_symbol(self):
        """Test SimulatedPortfolio stores tokens by symbol."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        portfolio.tokens = {
            "USDC": Decimal("5000"),
            "WETH": Decimal("2"),
        }

        assert "USDC" in portfolio.tokens
        assert "WETH" in portfolio.tokens
        assert portfolio.tokens["USDC"] == Decimal("5000")


# =============================================================================
# Test Class 3: Price Lookup Uses Correct Symbol
# =============================================================================


class TestPriceLookupUsesCorrectSymbol:
    """Tests validating that price lookup uses the correct symbol.

    Acceptance Criterion #3: Test price lookup uses correct symbol
    """

    def test_market_state_prices_by_symbol(self):
        """Test that MarketState stores prices by symbol."""
        market_state = MarketState(
            timestamp=TEST_TIME,
            prices={
                "USDC": Decimal("1.0"),
                "WETH": Decimal("3000.0"),
                "WBTC": Decimal("60000.0"),
            },
        )

        # Prices should be retrievable by symbol
        assert market_state.prices.get("USDC") == Decimal("1.0")
        assert market_state.prices.get("WETH") == Decimal("3000.0")
        assert market_state.prices.get("WBTC") == Decimal("60000.0")

    def test_portfolio_valuation_uses_symbol_prices(self):
        """Test portfolio valuation uses symbol-keyed prices."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {
            "USDC": Decimal("5000"),
            "WETH": Decimal("2"),
        }

        market_state = MarketState(
            timestamp=TEST_TIME,
            prices={
                "USDC": Decimal("1.0"),
                "WETH": Decimal("3000.0"),
            },
        )

        value = portfolio.get_total_value_usd(market_state)
        # 5000 * 1 + 2 * 3000 = 11000
        assert value == Decimal("11000")

    def test_address_resolved_to_symbol_for_price_lookup(self):
        """Test that address tokens are resolved to symbols for price lookup."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {
            ETH_USDC_ADDRESS: Decimal("5000"),  # Using address as key
            ETH_WETH_ADDRESS: Decimal("2"),
        }

        # Prices keyed by symbol
        market_state = MarketState(
            timestamp=TEST_TIME,
            prices={
                "USDC": Decimal("1.0"),
                "WETH": Decimal("3000.0"),
            },
        )

        # With chain_id, addresses should be resolved to symbols
        value = portfolio.get_total_value_usd(
            market_state,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        # 5000 * 1 + 2 * 3000 = 11000
        assert value == Decimal("11000")


# =============================================================================
# Test Class 4: Final Valuation Accurate for Known Tokens
# =============================================================================


class TestFinalValuationAccurateForKnownTokens:
    """Tests validating that final valuation is accurate for known tokens.

    Acceptance Criterion #4: Test final valuation is accurate for known tokens
    """

    def test_multi_token_valuation_accurate(self):
        """Test valuation accuracy with multiple known tokens."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("1000")  # $1000 cash
        portfolio.tokens = {
            "USDC": Decimal("5000"),  # $5000
            "WETH": Decimal("2"),  # $6000 at $3000/ETH
            "WBTC": Decimal("0.1"),  # $6000 at $60000/BTC
        }

        market_state = MarketState(
            timestamp=TEST_TIME,
            prices={
                "USDC": Decimal("1.0"),
                "WETH": Decimal("3000.0"),
                "WBTC": Decimal("60000.0"),
            },
        )

        value = portfolio.get_total_value_usd(market_state)
        # 1000 + 5000 + 6000 + 6000 = 18000
        assert value == Decimal("18000")

    def test_lp_position_valuation_with_symbols(self):
        """Test LP position valuation uses correct symbol resolution."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.positions = [
            SimulatedPosition(
                position_type=PositionType.LP,
                protocol="uniswap_v3",
                tokens=["WETH", "USDC"],  # Using symbols
                amounts={
                    "WETH": Decimal("1.0"),
                    "USDC": Decimal("3000"),
                },
                entry_price=Decimal("3000"),
                entry_time=TEST_TIME,
                fees_earned=Decimal("100"),  # $100 in fees
            )
        ]

        market_state = MarketState(
            timestamp=TEST_TIME,
            prices={
                "USDC": Decimal("1.0"),
                "WETH": Decimal("3000.0"),
            },
        )

        value = portfolio.get_total_value_usd(market_state)
        # 1 WETH * 3000 + 3000 USDC * 1 + 100 fees = 6100
        assert value == Decimal("6100")

    def test_cross_chain_token_resolution(self):
        """Test token resolution works correctly across different chains."""
        # Same symbol (USDC) on different chains should resolve correctly
        eth_usdc_symbol = resolve_to_canonical_symbol(CHAIN_ID_ETHEREUM, ETH_USDC_ADDRESS)
        arb_usdc_symbol = resolve_to_canonical_symbol(CHAIN_ID_ARBITRUM, ARB_USDC_ADDRESS)
        base_usdc_symbol = resolve_to_canonical_symbol(CHAIN_ID_BASE, BASE_USDC_ADDRESS)

        assert eth_usdc_symbol == "USDC"
        assert arb_usdc_symbol == "USDC"
        assert base_usdc_symbol == "USDC"

    def test_pnl_calculation_consistent(self):
        """Test PnL calculation is consistent with symbol-based tracking."""
        tracker = PaperPortfolioTracker(
            strategy_id="pnl_consistency_test",
            chain="ethereum",
        )

        # Start with $10,000 equivalent
        initial_balances = {
            "USDC": Decimal("5000"),
            "WETH": Decimal("1.666666666666666667"),  # ~$5000 at $3000/ETH
        }
        tracker.start_session(initial_balances)

        # Calculate PnL at same prices
        current_prices = {
            "USDC": Decimal("1.0"),
            "WETH": Decimal("3000.0"),
        }
        pnl = tracker.get_pnl_usd(current_prices)

        # PnL should be ~$0 (no trades, same prices)
        assert abs(pnl) < Decimal("1")


# =============================================================================
# Test Class 5: Warning/Failure for Unknown Tokens
# =============================================================================


class TestUnknownTokenWarningFailure:
    """Tests validating warning/failure behavior for unknown tokens.

    Acceptance Criterion #5: Test warning/failure for unknown tokens
    """

    def test_unknown_token_not_in_registry(self):
        """Test that unknown token is not found in registry."""
        assert not is_token_known(CHAIN_ID_ETHEREUM, UNKNOWN_TOKEN_ADDRESS)
        assert get_token_symbol(CHAIN_ID_ETHEREUM, UNKNOWN_TOKEN_ADDRESS) is None
        assert get_token_info(CHAIN_ID_ETHEREUM, UNKNOWN_TOKEN_ADDRESS) is None

    def test_require_symbol_mapping_fails_on_unknown(self):
        """Test that require_symbol_mapping=True fails on unknown token."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {
            UNKNOWN_TOKEN_ADDRESS: Decimal("1000"),
        }

        market_state = MarketState(
            timestamp=TEST_TIME,
            prices={"USDC": Decimal("1.0")},  # Unknown token not in prices
        )

        with pytest.raises(ValueError, match="cannot be resolved"):
            portfolio.get_total_value_usd(
                market_state,
                require_symbol_mapping=True,
                chain_id=CHAIN_ID_ETHEREUM,
            )

    def test_unknown_token_logs_warning(self, caplog):
        """Test that unknown token logs warning when not required."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {
            UNKNOWN_TOKEN_ADDRESS: Decimal("1000"),
        }

        market_state = MarketState(
            timestamp=TEST_TIME,
            prices={},
        )

        with caplog.at_level(logging.WARNING):
            portfolio.get_total_value_usd(
                market_state,
                require_symbol_mapping=False,
                chain_id=CHAIN_ID_ETHEREUM,
            )

        # Should log warning about unknown token
        assert "Unknown token" in caplog.text or "unknown" in caplog.text.lower()

    def test_unresolved_token_tracked_in_data_quality(self):
        """Test that unresolved tokens are tracked in DataQualityTracker."""
        tracker = DataQualityTracker()

        # Record unresolved token
        tracker.record_unresolved_token(UNKNOWN_TOKEN_ADDRESS, CHAIN_ID_ETHEREUM)

        assert tracker.unresolved_token_count == 1

        # Convert to report
        report = tracker.to_data_quality_report()
        assert report.unresolved_token_count == 1

    def test_data_quality_report_serialization(self):
        """Test DataQualityReport serializes unresolved_token_count."""
        report = DataQualityReport(unresolved_token_count=3)

        # Serialize
        data = report.to_dict()
        assert "unresolved_token_count" in data
        assert data["unresolved_token_count"] == 3

        # Deserialize
        restored = DataQualityReport.from_dict(data)
        assert restored.unresolved_token_count == 3


# =============================================================================
# Test Class 6: End-to-End Integration
# =============================================================================


class TestEndToEndTokenIdentity:
    """End-to-end tests for token identity consistency throughout backtest flow."""

    def test_full_trade_cycle_preserves_identity(self):
        """Test complete trade cycle preserves token identity from intent to PnL."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        # 1. Initialize portfolio with symbol-based balances
        tracker = PaperPortfolioTracker(
            strategy_id="e2e_identity_test",
            chain="ethereum",
        )
        initial_balances = {"USDC": Decimal("10000")}
        tracker.start_session(initial_balances)

        # 2. Record trades using symbols
        trade1 = PaperTrade(
            timestamp=TEST_TIME,
            block_number=12345678,
            intent={"type": "SWAP", "from_token": "USDC", "to_token": "WETH"},
            tx_hash="0x" + "a" * 64,
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"WETH": Decimal("1.0")},
            tokens_out={"USDC": Decimal("3000")},
            protocol="uniswap_v3",
        )
        tracker.record_trade(trade1)

        # 3. Verify balances use symbols
        assert "USDC" in tracker.current_balances
        assert "WETH" in tracker.current_balances
        assert tracker.current_balances["USDC"] == Decimal("7000")
        assert tracker.current_balances["WETH"] == Decimal("1.0")

        # 4. Calculate PnL with symbol-based prices
        prices_at_profit = {
            "USDC": Decimal("1.0"),
            "WETH": Decimal("3500.0"),  # ETH went up
        }
        pnl = tracker.get_pnl_usd(prices_at_profit)

        # Initial: $10,000 USDC
        # Current: $7,000 USDC + 1 WETH * $3500 = $10,500
        # Gas: -$0.50
        # PnL: $10,500 - $10,000 - $0.50 = $499.50
        expected_pnl = Decimal("499.50")
        assert abs(pnl - expected_pnl) < Decimal("1")

    def test_token_registry_coverage(self):
        """Test token registry has sufficient coverage for major tokens."""
        # Check major tokens are in registry for each supported chain
        major_tokens = [
            (CHAIN_ID_ETHEREUM, "USDC", ETH_USDC_ADDRESS),
            (CHAIN_ID_ETHEREUM, "WETH", ETH_WETH_ADDRESS),
            (CHAIN_ID_ETHEREUM, "WBTC", ETH_WBTC_ADDRESS),
            (CHAIN_ID_ARBITRUM, "USDC", ARB_USDC_ADDRESS),
            (CHAIN_ID_ARBITRUM, "WETH", ARB_WETH_ADDRESS),
            (CHAIN_ID_ARBITRUM, "ARB", ARB_ARB_ADDRESS),
            (CHAIN_ID_BASE, "USDC", BASE_USDC_ADDRESS),
            (CHAIN_ID_BASE, "WETH", BASE_WETH_ADDRESS),
        ]

        for chain_id, expected_symbol, address in major_tokens:
            symbol = get_token_symbol(chain_id, address)
            assert symbol == expected_symbol, (
                f"Expected {expected_symbol} for {address} on chain {chain_id}, got {symbol}"
            )

    def test_deterministic_resolution(self):
        """Test that symbol resolution is deterministic (same input = same output)."""
        # Run resolution multiple times
        results = []
        for _ in range(10):
            symbol = resolve_to_canonical_symbol(CHAIN_ID_ETHEREUM, ETH_USDC_ADDRESS)
            results.append(symbol)

        # All results should be identical
        assert all(r == "USDC" for r in results)

    def test_mixed_address_and_symbol_portfolio(self):
        """Test portfolio handles mix of addresses and symbols."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        portfolio.cash_usd = Decimal("0")
        portfolio.tokens = {
            "USDC": Decimal("5000"),  # Symbol
            ETH_WETH_ADDRESS: Decimal("2"),  # Address
        }

        market_state = MarketState(
            timestamp=TEST_TIME,
            prices={
                "USDC": Decimal("1.0"),
                "WETH": Decimal("3000.0"),
            },
        )

        # Both should be valued correctly
        value = portfolio.get_total_value_usd(
            market_state,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        # 5000 * 1 + 2 * 3000 = 11000
        assert value == Decimal("11000")
