"""Unit tests for SimulatedPortfolio and related classes.

Tests cover:
- SimulatedPortfolio.apply_fill() updates positions correctly
- mark_to_market() calculates correct values for different position types
- get_metrics() produces valid BacktestMetrics
- Position management and token balance tracking
"""

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import (
    BacktestMetrics,
    EquityPoint,
    IntentType,
    TradeRecord,
)
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedFill,
    SimulatedPortfolio,
    SimulatedPosition,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def base_timestamp() -> datetime:
    """Base timestamp for tests."""
    return datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def market_state(base_timestamp: datetime) -> MarketState:
    """Market state with common token prices."""
    return MarketState(
        timestamp=base_timestamp,
        prices={
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
            "ARB": Decimal("1.50"),
            "BTC": Decimal("45000"),
        },
        chain="arbitrum",
    )


@pytest.fixture
def portfolio() -> SimulatedPortfolio:
    """Fresh portfolio with 10,000 USD initial capital."""
    return SimulatedPortfolio(initial_capital_usd=Decimal("10000"))


# =============================================================================
# SimulatedPosition Tests
# =============================================================================


class TestSimulatedPosition:
    """Tests for SimulatedPosition dataclass."""

    def test_spot_position_creation(self, base_timestamp: datetime) -> None:
        """Test creating a spot position via factory method."""
        position = SimulatedPosition.spot(
            token="WETH",
            amount=Decimal("1.5"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )

        assert position.position_type == PositionType.SPOT
        assert position.protocol == "spot"
        assert position.tokens == ["WETH"]
        assert position.amounts == {"WETH": Decimal("1.5")}
        assert position.entry_price == Decimal("3000")
        assert position.is_spot
        assert not position.is_lp
        assert not position.is_perp
        assert not position.is_lending
        assert position.primary_token == "WETH"
        assert position.total_amount == Decimal("1.5")

    def test_lp_position_creation(self, base_timestamp: datetime) -> None:
        """Test creating an LP position via factory method."""
        position = SimulatedPosition.lp(
            token0="WETH",
            token1="USDC",
            amount0=Decimal("0.5"),
            amount1=Decimal("1500"),
            liquidity=Decimal("1000"),
            tick_lower=-887272,
            tick_upper=887272,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )

        assert position.position_type == PositionType.LP
        assert position.protocol == "uniswap_v3"
        assert position.tokens == ["WETH", "USDC"]
        assert position.amounts == {"WETH": Decimal("0.5"), "USDC": Decimal("1500")}
        assert position.tick_lower == -887272
        assert position.tick_upper == 887272
        assert position.fee_tier == Decimal("0.003")
        assert position.is_lp
        assert not position.is_spot

    def test_perp_long_position_creation(self, base_timestamp: datetime) -> None:
        """Test creating a perp long position via factory method."""
        position = SimulatedPosition.perp_long(
            token="WETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )

        assert position.position_type == PositionType.PERP_LONG
        assert position.protocol == "gmx"
        assert position.collateral_usd == Decimal("1000")
        assert position.leverage == Decimal("5")
        assert position.notional_usd == Decimal("5000")
        assert position.is_perp
        assert position.is_long
        assert not position.is_short

    def test_perp_short_position_creation(self, base_timestamp: datetime) -> None:
        """Test creating a perp short position via factory method."""
        position = SimulatedPosition.perp_short(
            token="WETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("3"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )

        assert position.position_type == PositionType.PERP_SHORT
        assert position.notional_usd == Decimal("3000")
        assert position.is_perp
        assert position.is_short
        assert not position.is_long

    def test_supply_position_creation(self, base_timestamp: datetime) -> None:
        """Test creating a supply position via factory method."""
        position = SimulatedPosition.supply(
            token="WETH",
            amount=Decimal("2.0"),
            apy=Decimal("0.05"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )

        assert position.position_type == PositionType.SUPPLY
        assert position.protocol == "aave_v3"
        assert position.apy_at_entry == Decimal("0.05")
        assert position.is_lending
        assert position.is_long

    def test_borrow_position_creation(self, base_timestamp: datetime) -> None:
        """Test creating a borrow position via factory method."""
        position = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("5000"),
            apy=Decimal("0.08"),
            entry_price=Decimal("1"),
            entry_time=base_timestamp,
        )

        assert position.position_type == PositionType.BORROW
        assert position.apy_at_entry == Decimal("0.08")
        assert position.is_lending
        assert position.is_short

    def test_position_serialization(self, base_timestamp: datetime) -> None:
        """Test position to_dict and from_dict."""
        position = SimulatedPosition.spot(
            token="WETH",
            amount=Decimal("1.5"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )

        data = position.to_dict()
        restored = SimulatedPosition.from_dict(data)

        assert restored.position_type == position.position_type
        assert restored.tokens == position.tokens
        assert restored.amounts == position.amounts
        assert restored.entry_price == position.entry_price

    def test_position_id_generation(self, base_timestamp: datetime) -> None:
        """Test that position IDs are auto-generated."""
        position = SimulatedPosition.spot(
            token="WETH",
            amount=Decimal("1.0"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )

        assert position.position_id
        assert "SPOT" in position.position_id
        assert "spot" in position.position_id
        assert "WETH" in position.position_id


# =============================================================================
# SimulatedFill Tests
# =============================================================================


class TestSimulatedFill:
    """Tests for SimulatedFill dataclass."""

    def test_fill_creation(self, base_timestamp: datetime) -> None:
        """Test creating a simulated fill."""
        fill = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["USDC", "WETH"],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("1000"),
            fee_usd=Decimal("3"),
            slippage_usd=Decimal("1"),
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"WETH": Decimal("0.33")},
            tokens_out={"USDC": Decimal("1000")},
        )

        assert fill.intent_type == IntentType.SWAP
        assert fill.total_cost_usd == Decimal("4.50")
        assert fill.success

    def test_fill_to_trade_record(self, base_timestamp: datetime) -> None:
        """Test converting fill to trade record."""
        fill = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["USDC", "WETH"],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("1000"),
            fee_usd=Decimal("3"),
            slippage_usd=Decimal("1"),
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"WETH": Decimal("0.33")},
            tokens_out={"USDC": Decimal("1000")},
        )

        trade = fill.to_trade_record(pnl_usd=Decimal("10"))

        assert isinstance(trade, TradeRecord)
        assert trade.intent_type == IntentType.SWAP
        assert trade.pnl_usd == Decimal("10")
        assert trade.fee_usd == Decimal("3")
        assert trade.slippage_usd == Decimal("1")
        assert trade.gas_cost_usd == Decimal("0.50")


# =============================================================================
# SimulatedPortfolio.apply_fill Tests
# =============================================================================


class TestPortfolioApplyFill:
    """Tests for SimulatedPortfolio.apply_fill() method."""

    def test_apply_fill_swap_updates_tokens(self, portfolio: SimulatedPortfolio, base_timestamp: datetime) -> None:
        """Test that a swap fill correctly updates token balances."""
        # Start with USDC
        portfolio.tokens["USDC"] = Decimal("1000")
        portfolio.cash_usd = Decimal("9000")

        fill = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["USDC", "WETH"],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("1000"),
            fee_usd=Decimal("3"),
            slippage_usd=Decimal("1"),
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"WETH": Decimal("0.332")},
            tokens_out={"USDC": Decimal("1000")},
        )

        portfolio.apply_fill(fill)

        # USDC should be removed (went to swap)
        assert portfolio.tokens.get("USDC", Decimal("0")) == Decimal("0")
        # WETH should be added
        assert portfolio.tokens.get("WETH") == Decimal("0.332")
        # Gas cost should be deducted from cash
        assert portfolio.cash_usd == Decimal("9000") - Decimal("0.50")
        # Trade should be recorded
        assert len(portfolio.trades) == 1
        assert portfolio.trades[0].intent_type == IntentType.SWAP

    def test_apply_fill_stablecoins_convert_to_cash(
        self, portfolio: SimulatedPortfolio, base_timestamp: datetime
    ) -> None:
        """Test that stablecoins are automatically converted to cash."""
        fill = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["WETH", "USDC"],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("3000"),
            fee_usd=Decimal("9"),
            slippage_usd=Decimal("3"),
            gas_cost_usd=Decimal("1"),
            tokens_in={"USDC": Decimal("2987")},  # After fees
            tokens_out={"WETH": Decimal("1.0")},
        )

        initial_cash = portfolio.cash_usd
        portfolio.apply_fill(fill)

        # USDC should be converted to cash automatically
        assert "USDC" not in portfolio.tokens
        # Cash should increase by USDC received minus gas
        expected_cash = initial_cash + Decimal("2987") - Decimal("1")
        assert portfolio.cash_usd == expected_cash

    def test_apply_fill_opens_position(self, portfolio: SimulatedPortfolio, base_timestamp: datetime) -> None:
        """Test that fill with position_delta adds position."""
        position = SimulatedPosition.supply(
            token="WETH",
            amount=Decimal("1.0"),
            apy=Decimal("0.05"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )

        fill = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SUPPLY,
            protocol="aave_v3",
            tokens=["WETH"],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("3000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("1"),
            tokens_in={},
            tokens_out={"WETH": Decimal("1.0")},
            position_delta=position,
        )

        portfolio.tokens["WETH"] = Decimal("1.0")
        portfolio.apply_fill(fill)

        assert len(portfolio.positions) == 1
        assert portfolio.positions[0].position_type == PositionType.SUPPLY
        assert portfolio.positions[0].tokens == ["WETH"]

    def test_apply_fill_closes_position(self, portfolio: SimulatedPortfolio, base_timestamp: datetime) -> None:
        """Test that fill with position_close_id closes position."""
        # Add a position first
        position = SimulatedPosition.supply(
            token="WETH",
            amount=Decimal("1.0"),
            apy=Decimal("0.05"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )
        portfolio.positions.append(position)
        position_id = position.position_id

        # Create a close fill
        fill = SimulatedFill(
            timestamp=base_timestamp + timedelta(days=1),
            intent_type=IntentType.WITHDRAW,
            protocol="aave_v3",
            tokens=["WETH"],
            executed_price=Decimal("3100"),
            amount_usd=Decimal("3100"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("1"),
            tokens_in={"WETH": Decimal("1.0")},
            tokens_out={},
            position_close_id=position_id,
        )

        portfolio.apply_fill(fill)

        assert len(portfolio.positions) == 0
        assert len(portfolio._closed_positions) == 1


# =============================================================================
# SimulatedPortfolio.mark_to_market Tests
# =============================================================================


class TestPortfolioMarkToMarket:
    """Tests for SimulatedPortfolio.mark_to_market() method."""

    def test_mark_to_market_spot_positions(
        self,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        base_timestamp: datetime,
    ) -> None:
        """Test mark_to_market correctly values spot positions."""
        # Add a spot position
        position = SimulatedPosition.spot(
            token="WETH",
            amount=Decimal("2.0"),
            entry_price=Decimal("2800"),
            entry_time=base_timestamp - timedelta(days=1),
        )
        portfolio.positions.append(position)
        portfolio.cash_usd = Decimal("4000")

        value = portfolio.mark_to_market(market_state, market_state.timestamp)

        # Expected: cash (4000) + 2 WETH @ 3000 = 4000 + 6000 = 10000
        assert value == Decimal("10000")
        assert len(portfolio.equity_curve) == 1
        assert portfolio.equity_curve[0].value_usd == Decimal("10000")

    def test_mark_to_market_token_holdings(
        self,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        base_timestamp: datetime,
    ) -> None:
        """Test mark_to_market values direct token holdings."""
        portfolio.tokens["WETH"] = Decimal("1.5")
        portfolio.tokens["ARB"] = Decimal("100")
        portfolio.cash_usd = Decimal("5000")

        value = portfolio.mark_to_market(market_state, market_state.timestamp)

        # Expected: cash (5000) + 1.5 WETH @ 3000 + 100 ARB @ 1.50
        # = 5000 + 4500 + 150 = 9650
        assert value == Decimal("9650")

    def test_mark_to_market_perp_position_profit(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
        """Test mark_to_market values perp long position with profit."""
        # Create perp long at $3000, current price $3300 (10% up)
        position = SimulatedPosition.perp_long(
            token="WETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp - timedelta(hours=1),
        )
        portfolio.positions.append(position)
        portfolio.cash_usd = Decimal("9000")

        # Market state with higher price
        new_state = MarketState(
            timestamp=base_timestamp,
            prices={"WETH": Decimal("3300")},
        )

        value = portfolio.mark_to_market(new_state, new_state.timestamp)

        # Notional = 1000 * 5 = 5000
        # Price change = (3300 - 3000) / 3000 = 10%
        # Unrealized PnL = 10% * 5000 = 500
        # Position value = 1000 + 500 + funding (small) ≈ 1500
        # Total = 9000 + ~1500 = ~10500
        assert value > Decimal("10000")  # Profitable

    def test_mark_to_market_perp_position_loss(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
        """Test mark_to_market values perp long position with loss."""
        position = SimulatedPosition.perp_long(
            token="WETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp - timedelta(hours=1),
        )
        portfolio.positions.append(position)
        portfolio.cash_usd = Decimal("9000")

        # Market state with lower price
        new_state = MarketState(
            timestamp=base_timestamp,
            prices={"WETH": Decimal("2700")},
        )

        value = portfolio.mark_to_market(new_state, new_state.timestamp)

        # Price change = -10%, PnL = -500
        assert value < Decimal("10000")  # Loss

    def test_mark_to_market_lending_position_interest(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
        """Test mark_to_market accrues interest for lending positions."""
        position = SimulatedPosition.supply(
            token="WETH",
            amount=Decimal("1.0"),
            apy=Decimal("0.10"),  # 10% APY
            entry_price=Decimal("3000"),
            entry_time=base_timestamp - timedelta(days=365),  # 1 year ago
        )
        portfolio.positions.append(position)
        portfolio.cash_usd = Decimal("7000")

        market_state = MarketState(
            timestamp=base_timestamp,
            prices={"WETH": Decimal("3000")},
        )

        value = portfolio.mark_to_market(market_state, market_state.timestamp)

        # Principal = 1 * 3000 = 3000
        # Interest (1 year at 10%) = 3000 * 0.10 = 300
        # Position value = 3000 + 300 = 3300
        # Total = 7000 + 3300 = 10300
        assert value > Decimal("10000")
        assert position.interest_accrued > Decimal("0")

    def test_mark_to_market_borrow_position_interest(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
        """Test mark_to_market accrues interest for borrow positions (debt)."""
        position = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("1000"),
            apy=Decimal("0.08"),  # 8% APY
            entry_price=Decimal("1"),
            entry_time=base_timestamp - timedelta(days=365),  # 1 year ago
        )
        portfolio.positions.append(position)
        portfolio.cash_usd = Decimal("11000")  # Borrowed funds in cash

        market_state = MarketState(
            timestamp=base_timestamp,
            prices={"USDC": Decimal("1")},
        )

        value = portfolio.mark_to_market(market_state, market_state.timestamp)

        # Debt = 1000 + interest (80) = 1080
        # Position value = -1080
        # Total = 11000 - 1080 = 9920
        assert value < Decimal("11000")
        assert position.interest_accrued > Decimal("0")


# =============================================================================
# SimulatedPortfolio.get_metrics Tests
# =============================================================================


class TestPortfolioGetMetrics:
    """Tests for SimulatedPortfolio.get_metrics() method."""

    def test_get_metrics_empty_portfolio(self, portfolio: SimulatedPortfolio) -> None:
        """Test get_metrics with no equity curve."""
        metrics = portfolio.get_metrics()

        assert isinstance(metrics, BacktestMetrics)
        assert metrics.total_pnl_usd == Decimal("0")
        assert metrics.total_trades == 0

    def test_get_metrics_with_equity_curve(self, portfolio: SimulatedPortfolio, base_timestamp: datetime) -> None:
        """Test get_metrics calculates correct values from equity curve."""
        # Create an equity curve with 10% profit
        portfolio.equity_curve = [
            EquityPoint(timestamp=base_timestamp, value_usd=Decimal("10000")),
            EquityPoint(
                timestamp=base_timestamp + timedelta(days=1),
                value_usd=Decimal("10200"),
            ),
            EquityPoint(
                timestamp=base_timestamp + timedelta(days=2),
                value_usd=Decimal("10500"),
            ),
            EquityPoint(
                timestamp=base_timestamp + timedelta(days=3),
                value_usd=Decimal("11000"),
            ),
        ]

        metrics = portfolio.get_metrics()

        assert metrics.total_pnl_usd == Decimal("1000")
        assert metrics.total_return_pct == Decimal("0.1")  # 10%

    def test_get_metrics_with_trades(self, portfolio: SimulatedPortfolio, base_timestamp: datetime) -> None:
        """Test get_metrics calculates trade statistics."""
        # Add some trades
        portfolio.trades = [
            TradeRecord(
                timestamp=base_timestamp,
                intent_type=IntentType.SWAP,
                executed_price=Decimal("3000"),
                fee_usd=Decimal("3"),
                slippage_usd=Decimal("1"),
                gas_cost_usd=Decimal("0.5"),
                pnl_usd=Decimal("100"),
                success=True,
            ),
            TradeRecord(
                timestamp=base_timestamp + timedelta(days=1),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("3100"),
                fee_usd=Decimal("3"),
                slippage_usd=Decimal("1"),
                gas_cost_usd=Decimal("0.5"),
                pnl_usd=Decimal("-50"),
                success=True,
            ),
            TradeRecord(
                timestamp=base_timestamp + timedelta(days=2),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("3200"),
                fee_usd=Decimal("3"),
                slippage_usd=Decimal("1"),
                gas_cost_usd=Decimal("0.5"),
                pnl_usd=Decimal("75"),
                success=True,
            ),
        ]

        # Add equity curve
        portfolio.equity_curve = [
            EquityPoint(timestamp=base_timestamp, value_usd=Decimal("10000")),
            EquityPoint(
                timestamp=base_timestamp + timedelta(days=3),
                value_usd=Decimal("10125"),
            ),
        ]

        metrics = portfolio.get_metrics()

        assert metrics.total_trades == 3
        assert metrics.winning_trades == 2
        assert metrics.losing_trades == 1
        assert metrics.total_fees_usd == Decimal("9")
        assert metrics.total_slippage_usd == Decimal("3")
        assert metrics.total_gas_usd == Decimal("1.5")

    def test_get_metrics_max_drawdown(self, portfolio: SimulatedPortfolio, base_timestamp: datetime) -> None:
        """Test get_metrics calculates correct max drawdown."""
        # Create equity curve with a 10% drawdown
        portfolio.equity_curve = [
            EquityPoint(timestamp=base_timestamp, value_usd=Decimal("10000")),
            EquityPoint(
                timestamp=base_timestamp + timedelta(days=1),
                value_usd=Decimal("11000"),
            ),  # Peak
            EquityPoint(
                timestamp=base_timestamp + timedelta(days=2),
                value_usd=Decimal("9900"),
            ),  # 10% drawdown
            EquityPoint(
                timestamp=base_timestamp + timedelta(days=3),
                value_usd=Decimal("10500"),
            ),  # Recovery
        ]

        metrics = portfolio.get_metrics()

        # Max drawdown should be (11000 - 9900) / 11000 = 0.1 (10%)
        assert metrics.max_drawdown_pct == Decimal("0.1")

    def test_get_metrics_win_rate_and_profit_factor(
        self, portfolio: SimulatedPortfolio, base_timestamp: datetime
    ) -> None:
        """Test get_metrics calculates correct win rate and profit factor."""
        # 3 wins at $100 each, 2 losses at $50 each
        # Net PnL per trade after costs (~4.5 each)
        # Win: 100 - 4.5 = 95.5, Loss: -50 - 4.5 = -54.5
        portfolio.trades = []
        for i in range(3):
            portfolio.trades.append(
                TradeRecord(
                    timestamp=base_timestamp + timedelta(hours=i),
                    intent_type=IntentType.SWAP,
                    executed_price=Decimal("3000"),
                    fee_usd=Decimal("3"),
                    slippage_usd=Decimal("1"),
                    gas_cost_usd=Decimal("0.5"),
                    pnl_usd=Decimal("100"),
                    success=True,
                )
            )
        for i in range(2):
            portfolio.trades.append(
                TradeRecord(
                    timestamp=base_timestamp + timedelta(hours=3 + i),
                    intent_type=IntentType.SWAP,
                    executed_price=Decimal("2900"),
                    fee_usd=Decimal("3"),
                    slippage_usd=Decimal("1"),
                    gas_cost_usd=Decimal("0.5"),
                    pnl_usd=Decimal("-50"),
                    success=True,
                )
            )

        portfolio.equity_curve = [
            EquityPoint(timestamp=base_timestamp, value_usd=Decimal("10000")),
            EquityPoint(
                timestamp=base_timestamp + timedelta(days=1),
                value_usd=Decimal("10177.5"),
            ),
        ]

        metrics = portfolio.get_metrics()

        # Win rate = 3/5 = 0.6 (60%)
        assert metrics.win_rate == Decimal("0.6")
        assert metrics.winning_trades == 3
        assert metrics.losing_trades == 2


# =============================================================================
# SimulatedPortfolio Helper Method Tests
# =============================================================================


class TestPortfolioHelperMethods:
    """Tests for SimulatedPortfolio helper methods."""

    def test_get_total_value_usd(
        self,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
    ) -> None:
        """Test get_total_value_usd calculates correct total."""
        portfolio.cash_usd = Decimal("5000")
        portfolio.tokens["WETH"] = Decimal("1.0")
        portfolio.tokens["ARB"] = Decimal("200")

        value = portfolio.get_total_value_usd(market_state)

        # 5000 + 1*3000 + 200*1.5 = 5000 + 3000 + 300 = 8300
        assert value == Decimal("8300")

    def test_get_position(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
        """Test get_position finds position by ID."""
        position = SimulatedPosition.spot(
            token="WETH",
            amount=Decimal("1.0"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )
        portfolio.positions.append(position)

        found = portfolio.get_position(position.position_id)
        assert found is not None
        assert found.position_id == position.position_id

        not_found = portfolio.get_position("non-existent-id")
        assert not_found is None

    def test_get_positions_by_type(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
        """Test get_positions_by_type filters correctly."""
        spot_pos = SimulatedPosition.spot(
            token="WETH",
            amount=Decimal("1.0"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )
        supply_pos = SimulatedPosition.supply(
            token="WETH",
            amount=Decimal("2.0"),
            apy=Decimal("0.05"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )
        portfolio.positions.extend([spot_pos, supply_pos])

        spot_positions = portfolio.get_positions_by_type(PositionType.SPOT)
        assert len(spot_positions) == 1
        assert spot_positions[0].is_spot

        supply_positions = portfolio.get_positions_by_type(PositionType.SUPPLY)
        assert len(supply_positions) == 1
        assert supply_positions[0].is_lending

    def test_get_token_balance(self, portfolio: SimulatedPortfolio) -> None:
        """Test get_token_balance retrieves correct balance."""
        portfolio.tokens["WETH"] = Decimal("1.5")

        assert portfolio.get_token_balance("WETH") == Decimal("1.5")
        assert portfolio.get_token_balance("weth") == Decimal("1.5")
        assert portfolio.get_token_balance("ARB") == Decimal("0")

    def test_portfolio_serialization(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
        """Test portfolio to_dict and from_dict."""
        portfolio.tokens["WETH"] = Decimal("1.0")
        portfolio.equity_curve.append(EquityPoint(timestamp=base_timestamp, value_usd=Decimal("10000")))
        position = SimulatedPosition.spot(
            token="WETH",
            amount=Decimal("1.0"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )
        portfolio.positions.append(position)

        data = portfolio.to_dict()
        restored = SimulatedPortfolio.from_dict(data)

        assert restored.initial_capital_usd == portfolio.initial_capital_usd
        assert restored.cash_usd == portfolio.cash_usd
        assert restored.tokens == portfolio.tokens
        assert len(restored.positions) == 1
        assert len(restored.equity_curve) == 1


# =============================================================================
# Integration Tests
# =============================================================================


class TestPortfolioIntegration:
    """Integration tests for SimulatedPortfolio."""

    def test_full_trading_cycle(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
        """Test a complete trading cycle: swap, hold, swap back."""
        # Initial state: $10,000 cash
        assert portfolio.cash_usd == Decimal("10000")

        # Swap 1: Buy 1 WETH for $3000
        fill1 = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["USDC", "WETH"],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("3000"),
            fee_usd=Decimal("9"),
            slippage_usd=Decimal("3"),
            gas_cost_usd=Decimal("1"),
            tokens_in={"WETH": Decimal("0.996")},  # After fees/slippage
            tokens_out={"USDC": Decimal("3000")},
        )
        # Need to have USDC to trade
        portfolio.tokens["USDC"] = Decimal("3000")
        portfolio.cash_usd = Decimal("7000")  # Rest is in USDC
        portfolio.apply_fill(fill1)

        # After swap 1: have WETH, less cash
        assert portfolio.tokens.get("WETH") == Decimal("0.996")
        assert portfolio.cash_usd == Decimal("7000") - Decimal("1")  # Gas deducted

        # Mark to market with WETH at $3300 (10% up)
        market_state = MarketState(
            timestamp=base_timestamp + timedelta(days=7),
            prices={"WETH": Decimal("3300")},
        )
        value = portfolio.mark_to_market(market_state, market_state.timestamp)

        # Expected: 6999 + 0.996 * 3300 = 6999 + 3286.8 = 10285.8
        assert value > Decimal("10000")  # Made profit

        # Swap 2: Sell WETH back
        fill2 = SimulatedFill(
            timestamp=base_timestamp + timedelta(days=7),
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["WETH", "USDC"],
            executed_price=Decimal("3300"),
            amount_usd=Decimal("3286.80"),
            fee_usd=Decimal("9.86"),
            slippage_usd=Decimal("3.29"),
            gas_cost_usd=Decimal("1"),
            tokens_in={"USDC": Decimal("3272.65")},  # After fees/slippage
            tokens_out={"WETH": Decimal("0.996")},
        )
        portfolio.apply_fill(fill2)

        # After swap 2: WETH sold, USDC converted to cash
        assert portfolio.tokens.get("WETH", Decimal("0")) == Decimal("0")
        # Cash should have increased
        assert portfolio.cash_usd > Decimal("10000")

        # Metrics should show 2 trades
        metrics = portfolio.get_metrics()
        assert metrics.total_trades == 2
