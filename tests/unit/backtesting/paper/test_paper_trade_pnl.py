"""Tests for PaperTrade PnL calculations.

This test suite validates that:
1. net_token_flow_usd correctly calculates token flow PnL
2. net_pnl_usd includes gas costs in the calculation
3. Edge cases (no prices, zero amounts) are handled correctly
4. Serialization includes computed PnL values

Part of US-085a: [P1-AUDIT] Implement actual Paper Trader net_token_flow_usd.
"""

from datetime import datetime
from decimal import Decimal

from almanak.framework.backtesting.paper.models import PaperTrade


class TestNetTokenFlowUSD:
    """Tests for net_token_flow_usd property."""

    def test_swap_profit_scenario(self):
        """Test PnL calculation for a profitable swap.

        Scenario: Buy 1 ETH for 2000 USDC when ETH is worth 2500 USD
        Expected: tokens_out (ETH) = $2500, tokens_in (USDC) = $2000
        Net flow = $2500 - $2000 = $500 profit
        """
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"USDC": Decimal("2000")},  # Paid 2000 USDC
            tokens_out={"ETH": Decimal("1")},  # Received 1 ETH
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("2500"),
            },
        )

        # Net token flow: received $2500 worth of ETH, paid $2000 USDC
        assert trade.net_token_flow_usd == Decimal("500")

    def test_swap_loss_scenario(self):
        """Test PnL calculation for a losing swap.

        Scenario: Buy 1 ETH for 2000 USDC when ETH is worth 1800 USD
        Expected: tokens_out (ETH) = $1800, tokens_in (USDC) = $2000
        Net flow = $1800 - $2000 = -$200 loss
        """
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"USDC": Decimal("2000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("1800"),
            },
        )

        assert trade.net_token_flow_usd == Decimal("-200")

    def test_neutral_swap_scenario(self):
        """Test PnL calculation when tokens have equal value.

        Scenario: Swap 2000 USDC for 1 ETH at exactly $2000 ETH price
        Expected: Net flow = $2000 - $2000 = $0
        """
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"USDC": Decimal("2000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("2000"),
            },
        )

        assert trade.net_token_flow_usd == Decimal("0")

    def test_empty_token_prices_returns_zero(self):
        """Test that empty token_prices_usd returns Decimal('0')."""
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"USDC": Decimal("2000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={},  # No prices
        )

        assert trade.net_token_flow_usd == Decimal("0")

    def test_missing_token_price_treated_as_zero(self):
        """Test that missing token price uses Decimal('0')."""
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"USDC": Decimal("2000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                # ETH price missing
            },
        )

        # tokens_out_usd = 1 * 0 = 0
        # tokens_in_usd = 2000 * 1 = 2000
        # Net = 0 - 2000 = -2000
        assert trade.net_token_flow_usd == Decimal("-2000")

    def test_multiple_tokens_in_and_out(self):
        """Test PnL with multiple tokens on both sides.

        Scenario: Complex swap with multiple inputs and outputs
        Input: 1000 USDC + 0.5 ETH (worth $1500 at $3000 ETH)
        Output: 1 WBTC (worth $60000)
        Net flow = $60000 - $1000 - $1500 = $57500
        """
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=200000,
            gas_cost_usd=Decimal("10.00"),
            tokens_in={
                "USDC": Decimal("1000"),
                "ETH": Decimal("0.5"),
            },
            tokens_out={"WBTC": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("3000"),
                "WBTC": Decimal("60000"),
            },
        )

        # tokens_out_usd = 60000
        # tokens_in_usd = 1000 + 1500 = 2500
        # Net = 60000 - 2500 = 57500
        assert trade.net_token_flow_usd == Decimal("57500")

    def test_case_insensitive_token_lookup(self):
        """Test that token price lookup is case-insensitive."""
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"usdc": Decimal("2000")},  # lowercase
            tokens_out={"eth": Decimal("1")},  # lowercase
            token_prices_usd={
                "USDC": Decimal("1"),  # uppercase
                "ETH": Decimal("2500"),  # uppercase
            },
        )

        # Should still work because lookup uses .upper()
        assert trade.net_token_flow_usd == Decimal("500")


class TestNetPnlUSD:
    """Tests for net_pnl_usd property (includes gas costs)."""

    def test_profitable_trade_after_gas(self):
        """Test net PnL is profit minus gas.

        Scenario: $500 token profit - $5 gas = $495 net profit
        """
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"USDC": Decimal("2000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("2500"),
            },
        )

        assert trade.net_token_flow_usd == Decimal("500")
        assert trade.net_pnl_usd == Decimal("495")  # 500 - 5

    def test_losing_trade_worse_after_gas(self):
        """Test net PnL adds gas to token loss.

        Scenario: -$200 token loss - $5 gas = -$205 net loss
        """
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"USDC": Decimal("2000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("1800"),
            },
        )

        assert trade.net_token_flow_usd == Decimal("-200")
        assert trade.net_pnl_usd == Decimal("-205")  # -200 - 5

    def test_neutral_trade_loses_gas(self):
        """Test neutral token flow still loses gas costs.

        Scenario: $0 token flow - $5 gas = -$5 net loss
        """
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"USDC": Decimal("2000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("2000"),
            },
        )

        assert trade.net_token_flow_usd == Decimal("0")
        assert trade.net_pnl_usd == Decimal("-5")  # 0 - 5

    def test_high_gas_can_flip_profit_to_loss(self):
        """Test that high gas can turn a small profit into a loss.

        Scenario: $100 token profit - $150 gas = -$50 net loss
        """
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=500000,
            gas_cost_usd=Decimal("150.00"),  # High gas
            tokens_in={"USDC": Decimal("1000")},
            tokens_out={"ETH": Decimal("0.4")},  # Worth $1100 at $2750
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("2750"),
            },
        )

        assert trade.net_token_flow_usd == Decimal("100")  # 1100 - 1000
        assert trade.net_pnl_usd == Decimal("-50")  # 100 - 150

    def test_zero_gas_equals_token_flow(self):
        """Test that zero gas makes net_pnl_usd equal net_token_flow_usd."""
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=0,
            gas_cost_usd=Decimal("0"),
            tokens_in={"USDC": Decimal("2000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("2500"),
            },
        )

        assert trade.net_token_flow_usd == trade.net_pnl_usd == Decimal("500")


class TestPaperTradeSerialization:
    """Tests for PaperTrade serialization with PnL fields."""

    def test_to_dict_includes_pnl_fields(self):
        """Test that to_dict() includes computed PnL values."""
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"USDC": Decimal("2000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("2500"),
            },
        )

        data = trade.to_dict()

        assert "net_token_flow_usd" in data
        assert data["net_token_flow_usd"] == "500"

        assert "net_pnl_usd" in data
        # gas_cost_usd is Decimal("5.00") so subtraction preserves 2 decimal places
        assert Decimal(data["net_pnl_usd"]) == Decimal("495")  # 500 - 5

    def test_round_trip_preserves_pnl_calculation(self):
        """Test that from_dict creates trade with correct PnL calculation."""
        original = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("5.00"),
            tokens_in={"USDC": Decimal("2000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("2500"),
            },
        )

        data = original.to_dict()
        restored = PaperTrade.from_dict(data)

        # Verify computed properties work correctly after round-trip
        assert restored.net_token_flow_usd == Decimal("500")
        assert restored.net_pnl_usd == Decimal("495")
        assert restored.net_token_flow_usd == original.net_token_flow_usd
        assert restored.net_pnl_usd == original.net_pnl_usd


class TestWinRateProfitFactorScenarios:
    """Test scenarios for win rate and profit factor calculation.

    These tests validate the data that feeds into win rate calculations
    done by the Paper Trader engine.
    """

    def test_winning_trade_has_positive_net_pnl(self):
        """A winning trade should have positive net_pnl_usd."""
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("10.00"),
            tokens_in={"USDC": Decimal("1000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("2000"),  # Worth 2000, paid 1000
            },
        )

        assert trade.net_pnl_usd > Decimal("0")

    def test_losing_trade_has_negative_net_pnl(self):
        """A losing trade should have negative net_pnl_usd."""
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("10.00"),
            tokens_in={"USDC": Decimal("1000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("500"),  # Worth 500, paid 1000
            },
        )

        assert trade.net_pnl_usd < Decimal("0")

    def test_break_even_before_gas_loses_after_gas(self):
        """Break-even on tokens still loses money due to gas."""
        trade = PaperTrade(
            timestamp=datetime.now(),
            block_number=12345,
            intent={"type": "SWAP"},
            tx_hash="0x123",
            gas_used=100000,
            gas_cost_usd=Decimal("10.00"),
            tokens_in={"USDC": Decimal("1000")},
            tokens_out={"ETH": Decimal("1")},
            token_prices_usd={
                "USDC": Decimal("1"),
                "ETH": Decimal("1000"),  # Worth 1000, paid 1000
            },
        )

        assert trade.net_token_flow_usd == Decimal("0")  # Break-even on tokens
        assert trade.net_pnl_usd == Decimal("-10")  # Lose gas cost
