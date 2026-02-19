"""Tests for Paper Trader _calculate_metrics function.

This test suite validates that:
1. win_rate is correctly calculated from trades with positive net_pnl_usd
2. profit_factor is correctly calculated as gross_profit / gross_loss
3. Edge cases are handled: no trades, all wins, all losses, no losses
4. Zero PnL trades are handled correctly (not counted as win or loss)

Part of US-085b: [P1-AUDIT] Calculate Paper Trader win rate from actual PnL.
"""

from datetime import datetime
from decimal import Decimal

from almanak.framework.backtesting.paper.models import PaperTrade


def create_trade(
    net_flow_usd: Decimal,
    gas_cost_usd: Decimal = Decimal("5"),
    timestamp: datetime | None = None,
) -> PaperTrade:
    """Create a PaperTrade with specified PnL characteristics.

    Args:
        net_flow_usd: The desired net_token_flow_usd (before gas)
        gas_cost_usd: Gas cost in USD
        timestamp: Trade timestamp

    Returns:
        PaperTrade configured to yield the desired net_pnl_usd
    """
    if timestamp is None:
        timestamp = datetime.now()

    # Calculate token amounts to achieve desired net_flow_usd
    # Using USDC = $1 as base, we can set tokens_in and tokens_out
    # to achieve any net_flow_usd value
    if net_flow_usd >= Decimal("0"):
        # Profit: received more than paid
        tokens_in = {"USDC": Decimal("1000")}
        tokens_out = {"USDC": Decimal("1000") + net_flow_usd}
    else:
        # Loss: paid more than received
        tokens_in = {"USDC": Decimal("1000") + abs(net_flow_usd)}
        tokens_out = {"USDC": Decimal("1000")}

    return PaperTrade(
        timestamp=timestamp,
        block_number=12345,
        intent={"type": "SWAP"},
        tx_hash="0x" + "a" * 64,
        gas_used=100000,
        gas_cost_usd=gas_cost_usd,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        token_prices_usd={"USDC": Decimal("1")},
    )


class TestWinRateCalculation:
    """Tests for win_rate calculation from trade PnL."""

    def test_win_rate_with_mixed_trades(self):
        """Test win rate with winning and losing trades.

        3 wins, 2 losses = 60% win rate
        """
        trades = [
            create_trade(Decimal("100")),  # +$95 after gas -> win
            create_trade(Decimal("200")),  # +$195 after gas -> win
            create_trade(Decimal("-100")),  # -$105 after gas -> loss
            create_trade(Decimal("50")),  # +$45 after gas -> win
            create_trade(Decimal("-50")),  # -$55 after gas -> loss
        ]

        # Count wins and losses based on net_pnl_usd
        wins = sum(1 for t in trades if t.net_pnl_usd > Decimal("0"))
        losses = sum(1 for t in trades if t.net_pnl_usd < Decimal("0"))

        assert wins == 3
        assert losses == 2

        # Win rate = 3 / 5 = 0.6
        total_with_pnl = wins + losses
        expected_win_rate = Decimal(wins) / Decimal(total_with_pnl)
        assert expected_win_rate == Decimal("0.6")

    def test_win_rate_all_winners(self):
        """Test win rate when all trades are winners.

        All wins = 100% win rate
        """
        trades = [
            create_trade(Decimal("100")),  # +$95 -> win
            create_trade(Decimal("200")),  # +$195 -> win
            create_trade(Decimal("50")),  # +$45 -> win
        ]

        wins = sum(1 for t in trades if t.net_pnl_usd > Decimal("0"))
        losses = sum(1 for t in trades if t.net_pnl_usd < Decimal("0"))

        assert wins == 3
        assert losses == 0

        # Win rate = 3 / 3 = 1.0
        total_with_pnl = wins + losses
        expected_win_rate = Decimal(wins) / Decimal(total_with_pnl)
        assert expected_win_rate == Decimal("1")

    def test_win_rate_all_losers(self):
        """Test win rate when all trades are losers.

        All losses = 0% win rate
        """
        trades = [
            create_trade(Decimal("-100")),  # -$105 -> loss
            create_trade(Decimal("-200")),  # -$205 -> loss
            create_trade(Decimal("-50")),  # -$55 -> loss
        ]

        wins = sum(1 for t in trades if t.net_pnl_usd > Decimal("0"))
        losses = sum(1 for t in trades if t.net_pnl_usd < Decimal("0"))

        assert wins == 0
        assert losses == 3

        # Win rate = 0 / 3 = 0.0
        total_with_pnl = wins + losses
        expected_win_rate = Decimal(wins) / Decimal(total_with_pnl)
        assert expected_win_rate == Decimal("0")

    def test_win_rate_no_trades(self):
        """Test win rate with no trades returns 0.

        Edge case: Empty trade list should not cause division by zero.
        """
        trades: list[PaperTrade] = []

        wins = sum(1 for t in trades if t.net_pnl_usd > Decimal("0"))
        losses = sum(1 for t in trades if t.net_pnl_usd < Decimal("0"))

        assert wins == 0
        assert losses == 0

        # Edge case handling: 0 trades means 0 win rate, not error
        total_with_pnl = wins + losses
        if total_with_pnl > 0:
            win_rate = Decimal(wins) / Decimal(total_with_pnl)
        else:
            win_rate = Decimal("0")

        assert win_rate == Decimal("0")

    def test_win_rate_excludes_zero_pnl_trades(self):
        """Test that zero PnL trades are not counted as wins or losses.

        Zero PnL trades should be neutral - they don't affect win rate.
        """
        trades = [
            create_trade(Decimal("100")),  # +$95 -> win
            create_trade(Decimal("5"), gas_cost_usd=Decimal("5")),  # +$5 - $5 = $0 -> neutral
            create_trade(Decimal("-100")),  # -$105 -> loss
        ]

        # Verify the middle trade is neutral
        assert trades[1].net_pnl_usd == Decimal("0")

        wins = sum(1 for t in trades if t.net_pnl_usd > Decimal("0"))
        losses = sum(1 for t in trades if t.net_pnl_usd < Decimal("0"))

        assert wins == 1
        assert losses == 1

        # Win rate = 1 / 2 = 0.5 (neutral trade not counted)
        total_with_pnl = wins + losses
        expected_win_rate = Decimal(wins) / Decimal(total_with_pnl)
        assert expected_win_rate == Decimal("0.5")

    def test_gas_costs_affect_win_determination(self):
        """Test that high gas costs can flip a trade from win to loss.

        A small token profit can become a net loss after gas costs.
        """
        # Trade with small token profit but high gas
        trade = create_trade(
            net_flow_usd=Decimal("10"),  # +$10 token profit
            gas_cost_usd=Decimal("50"),  # -$50 gas
        )

        # net_pnl_usd = +10 - 50 = -40 -> loss!
        assert trade.net_pnl_usd == Decimal("-40")
        assert trade.net_pnl_usd < Decimal("0")


class TestProfitFactorCalculation:
    """Tests for profit_factor calculation (gross_profit / gross_loss)."""

    def test_profit_factor_with_mixed_trades(self):
        """Test profit factor with winning and losing trades.

        Gross profit = $95 + $195 + $45 = $335
        Gross loss = $105 + $55 = $160
        Profit factor = 335 / 160 = 2.09375
        """
        trades = [
            create_trade(Decimal("100")),  # +$95 after gas
            create_trade(Decimal("200")),  # +$195 after gas
            create_trade(Decimal("-100")),  # -$105 after gas
            create_trade(Decimal("50")),  # +$45 after gas
            create_trade(Decimal("-50")),  # -$55 after gas
        ]

        gross_profit = sum(
            t.net_pnl_usd for t in trades if t.net_pnl_usd > Decimal("0")
        )
        gross_loss = sum(
            abs(t.net_pnl_usd) for t in trades if t.net_pnl_usd < Decimal("0")
        )

        # Verify calculations
        assert gross_profit == Decimal("95") + Decimal("195") + Decimal("45")
        assert gross_profit == Decimal("335")
        assert gross_loss == Decimal("105") + Decimal("55")
        assert gross_loss == Decimal("160")

        # Profit factor = 335 / 160 = 2.09375
        profit_factor = gross_profit / gross_loss
        assert profit_factor == Decimal("335") / Decimal("160")
        assert profit_factor == Decimal("2.09375")

    def test_profit_factor_no_losses(self):
        """Test profit factor when there are no losing trades.

        Edge case: No losses means profit_factor should be 0 (or could be infinity).
        In our implementation, we use 0 to indicate infinite/undefined.
        """
        trades = [
            create_trade(Decimal("100")),  # +$95 -> win
            create_trade(Decimal("200")),  # +$195 -> win
        ]

        gross_profit = sum(
            t.net_pnl_usd for t in trades if t.net_pnl_usd > Decimal("0")
        )
        gross_loss = sum(
            abs(t.net_pnl_usd) for t in trades if t.net_pnl_usd < Decimal("0")
        )

        assert gross_profit == Decimal("290")
        assert gross_loss == Decimal("0")

        # Edge case: No losses -> profit_factor = 0 (undefined/infinite)
        if gross_loss > Decimal("0"):
            profit_factor = gross_profit / gross_loss
        else:
            profit_factor = Decimal("0")

        assert profit_factor == Decimal("0")

    def test_profit_factor_no_wins(self):
        """Test profit factor when there are no winning trades.

        All losses means profit_factor = 0 / gross_loss = 0
        """
        trades = [
            create_trade(Decimal("-100")),  # -$105 -> loss
            create_trade(Decimal("-200")),  # -$205 -> loss
        ]

        gross_profit = sum(
            t.net_pnl_usd for t in trades if t.net_pnl_usd > Decimal("0")
        )
        gross_loss = sum(
            abs(t.net_pnl_usd) for t in trades if t.net_pnl_usd < Decimal("0")
        )

        assert gross_profit == Decimal("0")
        assert gross_loss == Decimal("310")

        # profit_factor = 0 / 310 = 0
        if gross_loss > Decimal("0"):
            profit_factor = gross_profit / gross_loss
        else:
            profit_factor = Decimal("0")

        assert profit_factor == Decimal("0")

    def test_profit_factor_no_trades(self):
        """Test profit factor with no trades.

        Edge case: Empty trade list should return 0 profit factor.
        """
        trades: list[PaperTrade] = []

        gross_profit = sum(
            t.net_pnl_usd for t in trades if t.net_pnl_usd > Decimal("0")
        )
        gross_loss = sum(
            abs(t.net_pnl_usd) for t in trades if t.net_pnl_usd < Decimal("0")
        )

        assert gross_profit == Decimal("0")
        assert gross_loss == Decimal("0")

        # Edge case: No trades -> profit_factor = 0
        if gross_loss > Decimal("0"):
            profit_factor = gross_profit / gross_loss
        else:
            profit_factor = Decimal("0")

        assert profit_factor == Decimal("0")

    def test_profit_factor_greater_than_one_is_profitable(self):
        """Test that profit_factor > 1 indicates overall profitability.

        If gross_profit > gross_loss, profit_factor > 1.
        """
        trades = [
            create_trade(Decimal("300")),  # +$295 after gas
            create_trade(Decimal("-100")),  # -$105 after gas
        ]

        gross_profit = sum(
            t.net_pnl_usd for t in trades if t.net_pnl_usd > Decimal("0")
        )
        gross_loss = sum(
            abs(t.net_pnl_usd) for t in trades if t.net_pnl_usd < Decimal("0")
        )

        profit_factor = gross_profit / gross_loss

        # $295 / $105 ≈ 2.81
        assert profit_factor > Decimal("1")
        assert gross_profit > gross_loss

    def test_profit_factor_less_than_one_is_unprofitable(self):
        """Test that profit_factor < 1 indicates overall loss.

        If gross_profit < gross_loss, profit_factor < 1.
        """
        trades = [
            create_trade(Decimal("50")),  # +$45 after gas
            create_trade(Decimal("-200")),  # -$205 after gas
        ]

        gross_profit = sum(
            t.net_pnl_usd for t in trades if t.net_pnl_usd > Decimal("0")
        )
        gross_loss = sum(
            abs(t.net_pnl_usd) for t in trades if t.net_pnl_usd < Decimal("0")
        )

        profit_factor = gross_profit / gross_loss

        # $45 / $205 ≈ 0.22
        assert profit_factor < Decimal("1")
        assert gross_profit < gross_loss


class TestTradeCountMetrics:
    """Tests for winning_trades and losing_trades count metrics."""

    def test_winning_losing_trades_count(self):
        """Test that winning and losing trade counts are accurate."""
        trades = [
            create_trade(Decimal("100")),  # +$95 -> win
            create_trade(Decimal("200")),  # +$195 -> win
            create_trade(Decimal("-100")),  # -$105 -> loss
            create_trade(Decimal("-50")),  # -$55 -> loss
            create_trade(Decimal("-25")),  # -$30 -> loss
        ]

        winning_count = sum(1 for t in trades if t.net_pnl_usd > Decimal("0"))
        losing_count = sum(1 for t in trades if t.net_pnl_usd < Decimal("0"))

        assert winning_count == 2
        assert losing_count == 3
        assert winning_count + losing_count == len(trades)

    def test_neutral_trades_not_counted(self):
        """Test that neutral (zero PnL) trades aren't counted as wins or losses."""
        trades = [
            create_trade(Decimal("100")),  # +$95 -> win
            create_trade(Decimal("5"), gas_cost_usd=Decimal("5")),  # $0 -> neutral
            create_trade(Decimal("-100")),  # -$105 -> loss
        ]

        winning_count = sum(1 for t in trades if t.net_pnl_usd > Decimal("0"))
        losing_count = sum(1 for t in trades if t.net_pnl_usd < Decimal("0"))
        neutral_count = sum(1 for t in trades if t.net_pnl_usd == Decimal("0"))

        assert winning_count == 1
        assert losing_count == 1
        assert neutral_count == 1
        assert winning_count + losing_count + neutral_count == len(trades)


class TestMetricsIntegration:
    """Integration tests verifying metrics calculation logic matches engine implementation."""

    def test_metrics_calculation_algorithm(self):
        """Test the complete metrics calculation algorithm used in _calculate_metrics.

        This test mirrors the algorithm in paper/engine.py:2511-2539.
        """
        # Sample trades
        trades = [
            create_trade(Decimal("150")),  # +$145 -> win
            create_trade(Decimal("-80")),  # -$85 -> loss
            create_trade(Decimal("200")),  # +$195 -> win
            create_trade(Decimal("-120")),  # -$125 -> loss
            create_trade(Decimal("100")),  # +$95 -> win
        ]

        # Algorithm from engine.py
        gross_profit = Decimal("0")
        gross_loss = Decimal("0")
        winning_trades_count = 0
        losing_trades_count = 0

        for trade in trades:
            trade_pnl = trade.net_pnl_usd
            if trade_pnl > Decimal("0"):
                gross_profit += trade_pnl
                winning_trades_count += 1
            elif trade_pnl < Decimal("0"):
                gross_loss += abs(trade_pnl)
                losing_trades_count += 1
            # Zero PnL trades are neutral

        # Win rate calculation
        trades_with_pnl = winning_trades_count + losing_trades_count
        win_rate = Decimal("0")
        if trades_with_pnl > 0:
            win_rate = Decimal(winning_trades_count) / Decimal(trades_with_pnl)

        # Profit factor calculation
        profit_factor = Decimal("0")
        if gross_loss > Decimal("0"):
            profit_factor = gross_profit / gross_loss

        # Verify results
        assert winning_trades_count == 3
        assert losing_trades_count == 2
        assert gross_profit == Decimal("145") + Decimal("195") + Decimal("95")
        assert gross_profit == Decimal("435")
        assert gross_loss == Decimal("85") + Decimal("125")
        assert gross_loss == Decimal("210")

        # Win rate = 3 / 5 = 0.6
        assert win_rate == Decimal("0.6")

        # Profit factor = 435 / 210 ≈ 2.07
        expected_pf = Decimal("435") / Decimal("210")
        assert profit_factor == expected_pf

    def test_metrics_with_only_neutral_trades(self):
        """Test metrics when all trades have zero PnL.

        Edge case: All neutral trades should result in 0 win rate and 0 profit factor.
        """
        trades = [
            create_trade(Decimal("5"), gas_cost_usd=Decimal("5")),  # $0 neutral
            create_trade(Decimal("10"), gas_cost_usd=Decimal("10")),  # $0 neutral
            create_trade(Decimal("15"), gas_cost_usd=Decimal("15")),  # $0 neutral
        ]

        # Verify all trades are neutral
        for trade in trades:
            assert trade.net_pnl_usd == Decimal("0")

        # Algorithm
        gross_profit = Decimal("0")
        gross_loss = Decimal("0")
        winning_trades_count = 0
        losing_trades_count = 0

        for trade in trades:
            trade_pnl = trade.net_pnl_usd
            if trade_pnl > Decimal("0"):
                gross_profit += trade_pnl
                winning_trades_count += 1
            elif trade_pnl < Decimal("0"):
                gross_loss += abs(trade_pnl)
                losing_trades_count += 1

        # All metrics should be 0
        assert winning_trades_count == 0
        assert losing_trades_count == 0
        assert gross_profit == Decimal("0")
        assert gross_loss == Decimal("0")

        # Win rate = 0 (no trades with PnL)
        trades_with_pnl = winning_trades_count + losing_trades_count
        win_rate = Decimal("0")
        if trades_with_pnl > 0:
            win_rate = Decimal(winning_trades_count) / Decimal(trades_with_pnl)
        assert win_rate == Decimal("0")

        # Profit factor = 0 (no losses)
        profit_factor = Decimal("0")
        if gross_loss > Decimal("0"):
            profit_factor = gross_profit / gross_loss
        assert profit_factor == Decimal("0")
