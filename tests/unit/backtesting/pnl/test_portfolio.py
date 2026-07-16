"""Unit tests for SimulatedPortfolio and related classes.

Tests cover:
- SimulatedPortfolio.apply_fill() updates positions correctly
- mark_to_market() calculates correct values for different position types
- get_metrics() produces valid BacktestMetrics
- Position management and token balance tracking
"""

from datetime import UTC, datetime, timedelta
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

    def test_address_keyed_position_serialization(self, base_timestamp: datetime) -> None:
        """Address-keyed positions survive display-string serialization."""
        token_key = ("base", "0x4200000000000000000000000000000000000006")
        position = SimulatedPosition.spot(
            token=token_key,
            amount=Decimal("1.5"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )

        data = position.to_dict()
        restored = SimulatedPosition.from_dict(data)

        assert data["tokens"] == ["base:0x4200000000000000000000000000000000000006"]
        assert restored.tokens == [token_key]
        assert restored.amounts == {token_key: Decimal("1.5")}

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

    def test_rejected_fill_to_trade_record_carries_failure_reason(self, base_timestamp: datetime) -> None:
        """A rejected fill's failure_reason surfaces as TradeRecord.error."""
        fill = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["USDC", "WETH"],
            executed_price=Decimal("0"),
            amount_usd=Decimal("0"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out={},
            success=False,
            metadata={"failure_reason": "insufficient cash for fill: required 1, cash-like 0"},
        )

        trade = fill.to_trade_record()

        assert trade.success is False
        assert trade.error == "insufficient cash for fill: required 1, cash-like 0"

    def test_successful_fill_to_trade_record_has_no_error(self, base_timestamp: datetime) -> None:
        """Success clears error even when metadata carries stale reasons."""
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
            tokens_in={},
            tokens_out={},
            metadata={"failure_reason": "stale"},
        )

        assert fill.to_trade_record().error is None

    def test_fill_to_dict_keeps_none_optional_values(self, base_timestamp: datetime) -> None:
        """Optional measured fields remain None when they were not measured."""
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

        data = fill.to_dict()

        assert data["gas_price_gwei"] is None
        assert data["estimated_mev_cost_usd"] is None
        assert data["position_delta"] is None
        assert data["position_reduce_amounts"] == {}

    def test_fill_to_dict_preserves_measured_zero_values(self, base_timestamp: datetime) -> None:
        """Measured zero gas/MEV values serialize as zero, not missing."""
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
            executed_price=Decimal("0"),
            amount_usd=Decimal("3000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out={"WETH": Decimal("1.0")},
            position_delta=position,
            position_reduce_id="supply-1",
            position_reduce_amounts={"WETH": Decimal("0")},
            gas_price_gwei=Decimal("0"),
            estimated_mev_cost_usd=Decimal("0"),
        )

        data = fill.to_dict()

        assert data["executed_price"] == "0"
        assert data["gas_cost_usd"] == "0"
        assert data["gas_price_gwei"] == "0"
        assert data["estimated_mev_cost_usd"] == "0"
        assert data["position_delta"]["position_type"] == "SUPPLY"
        assert data["position_reduce_id"] == "supply-1"
        assert data["position_reduce_amounts"] == {"WETH": "0"}


# =============================================================================
# SimulatedPortfolio.apply_fill Tests
# =============================================================================


class TestPortfolioApplyFill:
    """Tests for SimulatedPortfolio.apply_fill() method."""

    BASE_USDC = ("base", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
    BASE_WETH = ("base", "0x4200000000000000000000000000000000000006")

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

    def test_apply_fill_zero_cash_need_accepted_with_negative_cash(
        self, portfolio: SimulatedPortfolio, base_timestamp: datetime
    ) -> None:
        """A fill funded entirely by held tokens must not be rejected by negative cash-like."""
        portfolio.tokens["WETH"] = Decimal("1.0")
        portfolio.cash_usd = Decimal("-0.001")

        fill = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["WETH", "USDC"],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("3000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={"USDC": Decimal("3000")},
            tokens_out={"WETH": Decimal("1.0")},
        )

        portfolio.apply_fill(fill)

        assert portfolio.trades[-1].success is True
        assert portfolio.tokens.get("WETH", Decimal("0")) == Decimal("0")

    def test_cash_like_available_sums_cash_and_stable_tokens(self, portfolio: SimulatedPortfolio) -> None:
        """cash_like_available() = cash_usd plus cash-equivalent stable token balances."""
        portfolio.cash_usd = Decimal("100")
        portfolio.tokens["USDC"] = Decimal("40")
        portfolio.tokens["WETH"] = Decimal("2")

        assert portfolio.cash_like_available() == Decimal("140")

    def test_apply_fill_stablecoins_convert_to_cash(
        self, portfolio: SimulatedPortfolio, base_timestamp: datetime
    ) -> None:
        """Test that stablecoins are automatically converted to cash."""
        # Fund the WETH being sold -- fills may only spend held balances
        portfolio.tokens["WETH"] = Decimal("1.0")
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

    def test_address_keyed_stable_spend_debits_cash(self, base_timestamp: datetime) -> None:
        """Address-keyed stablecoin outflows draw from cash at $1 face value."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")
        fill = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=[self.BASE_USDC, self.BASE_WETH],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("50"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={self.BASE_WETH: Decimal("0.01666666666666666666666666667")},
            tokens_out={self.BASE_USDC: Decimal("50")},
        )
        market = MarketState(
            timestamp=base_timestamp,
            chain="base",
            prices={self.BASE_USDC: Decimal("1"), self.BASE_WETH: Decimal("3000")},
        )

        applied = portfolio.apply_fill(fill, market_state=market)

        assert applied is True
        assert portfolio.cash_usd == Decimal("9950")
        assert portfolio.get_token_balance(self.BASE_WETH) == Decimal("0.01666666666666666666666666667")
        assert portfolio.get_total_value_usd(market) == Decimal("10000.00000000000000000000000")

    def test_address_keyed_stable_inflow_sweeps_to_cash(self, base_timestamp: datetime) -> None:
        """Address-keyed stablecoin inflows are swept out of token balances."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")
        portfolio.tokens[self.BASE_WETH] = Decimal("1")
        fill = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=[self.BASE_WETH, self.BASE_USDC],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("3000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("1"),
            tokens_in={self.BASE_USDC: Decimal("3000")},
            tokens_out={self.BASE_WETH: Decimal("1")},
        )

        applied = portfolio.apply_fill(fill)

        assert applied is True
        assert self.BASE_USDC not in portfolio.tokens
        assert self.BASE_WETH not in portfolio.tokens
        assert portfolio.cash_usd == Decimal("12999")

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

    def test_mark_to_market_prices_address_numeraire_without_symbol(
        self,
        base_timestamp: datetime,
    ) -> None:
        token_key = ("base", "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf")
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")
        portfolio._numeraire_token = token_key
        portfolio._numeraire_symbol = None
        market_state = MarketState(
            timestamp=base_timestamp,
            chain="base",
            prices={token_key: Decimal("50000")},
        )

        value = portfolio.mark_to_market(market_state, market_state.timestamp)

        assert value == Decimal("10000")
        assert portfolio.equity_curve[-1].numeraire_price_usd == Decimal("50000")

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

    def test_adapter_failure_falls_back_to_internal_position_mark(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
        """Adapter valuation errors fall back without flipping borrow debt positive."""

        class RaisingAdapter:
            def value_position(self, *_args, **_kwargs):
                raise RuntimeError("adapter unavailable")

        position = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("1000"),
            apy=Decimal("0.08"),
            entry_price=Decimal("1"),
            entry_time=base_timestamp - timedelta(days=365),
        )
        portfolio.positions.append(position)
        portfolio.cash_usd = Decimal("11000")
        market_state = MarketState(
            timestamp=base_timestamp,
            prices={"USDC": Decimal("1")},
        )

        value = portfolio.mark_to_market(market_state, market_state.timestamp, adapter=RaisingAdapter())

        assert value < Decimal("11000")
        assert value == portfolio.equity_curve[-1].value_usd
        assert position.interest_accrued > Decimal("0")

    def test_mark_lp_position_requires_token_pair(
        self,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        base_timestamp: datetime,
    ) -> None:
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
        position.tokens = ["WETH"]

        assert portfolio._mark_lp_position(position, market_state, base_timestamp) == Decimal("0")

    def test_mark_lp_position_non_strict_falls_back_for_missing_prices(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
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
            entry_time=base_timestamp - timedelta(days=1),
        )
        market_state = MarketState(timestamp=base_timestamp, prices={})

        value = portfolio._mark_lp_position(position, market_state, base_timestamp)

        assert value > Decimal("0")
        assert position.amounts["WETH"] > Decimal("0")
        assert position.amounts["USDC"] > Decimal("0")
        assert position.fees_earned > Decimal("0")
        assert position.last_updated == base_timestamp

    def test_mark_lp_position_strict_missing_token0_raises(
        self,
        base_timestamp: datetime,
    ) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), strict_reproducibility=True)
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
        market_state = MarketState(timestamp=base_timestamp, prices={"USDC": Decimal("1")})

        with pytest.raises(ValueError, match="Price unavailable for WETH"):
            portfolio._mark_lp_position(position, market_state, base_timestamp)


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
        # VIB-2915: `total_return_pct` is now stored as an actual percentage (10 for 10%), not a ratio.
        assert metrics.total_return_pct == Decimal("10")

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

    def test_net_pnl_equals_equity_curve_pnl_despite_execution_costs(
        self, portfolio: SimulatedPortfolio, base_timestamp: datetime
    ) -> None:
        """net_pnl_usd must equal the equity-curve PnL, not re-subtract costs.

        Execution costs are debited from the portfolio during execution (gas
        and venue fee/slippage from cash, SWAP fee/slippage netted into
        tokens_in), so the equity curve is already net of them. Subtracting
        the cost columns again from ``final - initial`` double-counts every
        cost. The cost columns remain the informational breakdown only —
        the contract `calculate_metrics` already pins.
        """
        portfolio.trades = [
            TradeRecord(
                timestamp=base_timestamp,
                intent_type=IntentType.SWAP,
                executed_price=Decimal("3000"),
                fee_usd=Decimal("30"),
                slippage_usd=Decimal("10"),
                gas_cost_usd=Decimal("5"),
                pnl_usd=Decimal("0"),
                success=True,
            ),
            TradeRecord(
                timestamp=base_timestamp + timedelta(days=1),
                intent_type=IntentType.LP_OPEN,
                executed_price=Decimal("3000"),
                fee_usd=Decimal("20"),
                slippage_usd=Decimal("8"),
                gas_cost_usd=Decimal("7"),
                pnl_usd=Decimal("0"),
                success=True,
            ),
        ]
        # The equity curve already reflects the $80 of costs above: the
        # portfolio ended at 10100, not 10180.
        portfolio.equity_curve = [
            EquityPoint(timestamp=base_timestamp, value_usd=Decimal("10000")),
            EquityPoint(
                timestamp=base_timestamp + timedelta(days=2),
                value_usd=Decimal("10100"),
            ),
        ]

        metrics = portfolio.get_metrics()

        assert metrics.total_pnl_usd == Decimal("100")
        assert metrics.net_pnl_usd == metrics.total_pnl_usd
        # The breakdown columns still report the costs without re-deducting them.
        assert metrics.total_fees_usd == Decimal("50")
        assert metrics.total_slippage_usd == Decimal("18")
        assert metrics.total_gas_usd == Decimal("12")

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


class TestPortfolioDataCoverageMetrics:
    """Tests for SimulatedPortfolio.calculate_data_coverage_metrics()."""

    def test_counts_open_closed_positions_confidence_and_unique_sources(
        self,
        portfolio: SimulatedPortfolio,
        base_timestamp: datetime,
    ) -> None:
        """Data coverage includes open/closed positions and normalizes unknown confidence."""
        lp_open = SimulatedPosition.lp(
            token0="WETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("3000"),
            liquidity=Decimal("1000"),
            tick_lower=-887272,
            tick_upper=887272,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )
        lp_open.fee_confidence = "high"
        lp_open.slippage_confidence = "medium"
        lp_open.metadata["data_source"] = "uniswap_v3_subgraph"

        lp_closed = SimulatedPosition.lp(
            token0="WETH",
            token1="USDC",
            amount0=Decimal("0.5"),
            amount1=Decimal("1500"),
            liquidity=Decimal("500"),
            tick_lower=-887272,
            tick_upper=887272,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
        )
        lp_closed.fee_confidence = "provider_specific"
        lp_closed.slippage_confidence = "provider_specific"
        lp_closed.metadata["data_source"] = "uniswap_v3_subgraph"

        perp_open = SimulatedPosition.perp_long(
            token="WETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("3"),
            entry_price=Decimal("3000"),
            entry_time=base_timestamp,
            protocol="gmx_v2",
        )
        perp_open.funding_confidence = "medium"
        perp_open.funding_data_source = "gmx_v2_reader"

        perp_closed = SimulatedPosition.perp_short(
            token="BTC",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("2"),
            entry_price=Decimal("45000"),
            entry_time=base_timestamp,
            protocol="gmx_v2",
        )
        perp_closed.funding_confidence = "provider_specific"
        perp_closed.funding_data_source = "gmx_v2_reader"

        supply_open = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("1000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=base_timestamp,
        )
        supply_open.apy_confidence = "high"
        supply_open.apy_data_source = "aave_v3_subgraph"

        borrow_closed = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("500"),
            apy=Decimal("0.08"),
            entry_price=Decimal("1"),
            entry_time=base_timestamp,
        )
        borrow_closed.apy_confidence = "provider_specific"
        borrow_closed.apy_data_source = "fallback:default_rate"

        portfolio.positions.extend([lp_open, perp_open, supply_open])
        portfolio._closed_positions.extend([lp_closed, perp_closed, borrow_closed])

        metrics = portfolio.calculate_data_coverage_metrics()

        assert metrics.lp_metrics.position_count == 2
        assert metrics.lp_metrics.fee_confidence_breakdown == {"high": 1, "medium": 0, "low": 1}
        assert metrics.lp_metrics.data_sources == ["uniswap_v3_subgraph"]
        assert metrics.slippage_metrics.calculation_count == 2
        assert metrics.slippage_metrics.slippage_confidence_breakdown == {"high": 0, "medium": 1, "low": 1}

        assert metrics.perp_metrics.position_count == 2
        assert metrics.perp_metrics.funding_confidence_breakdown == {"high": 0, "medium": 1, "low": 1}
        assert metrics.perp_metrics.data_sources == ["gmx_v2_reader"]

        assert metrics.lending_metrics.position_count == 2
        assert metrics.lending_metrics.apy_confidence_breakdown == {"high": 1, "medium": 0, "low": 1}
        assert metrics.lending_metrics.data_sources == ["aave_v3_subgraph", "fallback:default_rate"]

        assert metrics.total_data_points == 8
        assert metrics.high_confidence_data_points == 2
        assert metrics.data_coverage_pct == 25.0


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
        portfolio.equity_curve.append(
            EquityPoint(
                timestamp=base_timestamp,
                value_usd=Decimal("10000"),
                eth_price_usd=Decimal("3000"),
                spot_value_usd=Decimal("7000"),
                position_value_usd=Decimal("3000"),
                valuation_source="portfolio_valuer",
                numeraire_price_usd=Decimal("3000"),
            )
        )
        portfolio.trades.append(
            TradeRecord(
                timestamp=base_timestamp,
                intent_type=IntentType.SWAP,
                executed_price=Decimal("3000"),
                fee_usd=Decimal("1"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("4"),
                success=True,
                amount_usd=Decimal("3000"),
                protocol="uniswap_v3",
                tokens=["USDC", "WETH"],
                tx_hash="0xabc",
                error=None,
                metadata={"route": "direct"},
                actual_amount_in=Decimal("3000"),
                actual_amount_out=Decimal("1"),
                expected_amount_in=Decimal("3000"),
                expected_amount_out=Decimal("1.01"),
                il_loss_usd=Decimal("-5"),
                fees_earned_usd=Decimal("6"),
                net_lp_pnl_usd=Decimal("7"),
                gas_price_gwei=Decimal("0.1"),
                estimated_mev_cost_usd=Decimal("0.2"),
                delayed_at_end=True,
                position_id="pos-1",
            )
        )
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
        assert restored.equity_curve[0].eth_price_usd == Decimal("3000")
        assert restored.equity_curve[0].spot_value_usd == Decimal("7000")
        assert restored.equity_curve[0].position_value_usd == Decimal("3000")
        assert restored.equity_curve[0].valuation_source == "portfolio_valuer"
        assert restored.equity_curve[0].numeraire_price_usd == Decimal("3000")
        restored_trade = restored.trades[0]
        assert restored_trade.metadata == {"route": "direct"}
        assert restored_trade.actual_amount_in == Decimal("3000")
        assert restored_trade.actual_amount_out == Decimal("1")
        assert restored_trade.expected_amount_in == Decimal("3000")
        assert restored_trade.expected_amount_out == Decimal("1.01")
        assert restored_trade.il_loss_usd == Decimal("-5")
        assert restored_trade.fees_earned_usd == Decimal("6")
        assert restored_trade.net_lp_pnl_usd == Decimal("7")
        assert restored_trade.gas_price_gwei == Decimal("0.1")
        assert restored_trade.estimated_mev_cost_usd == Decimal("0.2")
        assert restored_trade.delayed_at_end is True
        assert restored_trade.position_id == "pos-1"

    def test_address_keyed_portfolio_serialization(
        self,
        base_timestamp: datetime,
    ) -> None:
        """Address-keyed portfolio state restores to priceable token keys."""
        usdc_key = ("base", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
        weth_key = ("base", "0x4200000000000000000000000000000000000006")
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")
        portfolio.tokens[usdc_key] = Decimal("12")
        portfolio.tokens[weth_key] = Decimal("1.5")
        portfolio._cost_basis[weth_key] = Decimal("3000")
        portfolio.positions.append(
            SimulatedPosition.spot(
                token=weth_key,
                amount=Decimal("1.5"),
                entry_price=Decimal("3000"),
                entry_time=base_timestamp,
            )
        )

        data = portfolio.to_dict()
        restored = SimulatedPortfolio.from_dict(data)

        assert data["tokens"] == {
            "base:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "12",
            "base:0x4200000000000000000000000000000000000006": "1.5",
        }
        assert restored.tokens == portfolio.tokens
        assert restored._cost_basis == portfolio._cost_basis
        assert restored.positions[0].tokens == [weth_key]
        assert restored.positions[0].amounts == {weth_key: Decimal("1.5")}
        assert restored._is_cash_equivalent(usdc_key) is True

    def test_address_keyed_restore_preserves_explicit_zero_cash(self, base_timestamp: datetime) -> None:
        """Restoring a fully deployed portfolio must not mint initial cash."""
        weth_key = ("base", "0x4200000000000000000000000000000000000006")
        data = {
            "initial_capital_usd": "10000",
            "cash_usd": "0",
            "chain": "base",
            "tokens": {"base:0x4200000000000000000000000000000000000006": "1.5"},
            "positions": [
                SimulatedPosition.spot(
                    token=weth_key,
                    amount=Decimal("1.5"),
                    entry_price=Decimal("3000"),
                    entry_time=base_timestamp,
                ).to_dict()
            ],
        }

        restored = SimulatedPortfolio.from_dict(data)

        assert restored.cash_usd == Decimal("0")
        assert restored.tokens == {weth_key: Decimal("1.5")}

    def test_annualized_return_for_wiped_out_portfolio_is_zero(self, base_timestamp: datetime) -> None:
        timestamps = [base_timestamp, base_timestamp + timedelta(days=30)]

        annualized = SimulatedPortfolio._annualized_return(Decimal("-1"), timestamps)

        assert annualized == Decimal("0")

    def test_strict_token_holdings_value_raises_for_missing_price(self, base_timestamp: datetime) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), strict_reproducibility=True)
        portfolio.tokens["MISSING"] = Decimal("1")
        market = MarketState(timestamp=base_timestamp, prices={})

        with pytest.raises(KeyError):
            portfolio._token_holdings_value(market)

    def test_acquired_swap_basis_uses_one_allocation_mode_when_any_price_missing(
        self,
        base_timestamp: datetime,
    ) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1000"), cash_usd=Decimal("1000"))
        fill = SimulatedFill(
            timestamp=base_timestamp,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["USDC", "AAA", "BBB"],
            executed_price=Decimal("1"),
            amount_usd=Decimal("100"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={"AAA": Decimal("1"), "BBB": Decimal("1")},
            tokens_out={"USDC": Decimal("100")},
        )
        market = MarketState(
            timestamp=base_timestamp,
            prices={"USDC": Decimal("1"), "AAA": Decimal("50")},
        )

        portfolio.apply_fill(fill, market_state=market)

        assert portfolio._cost_basis["AAA"] == Decimal("50")
        assert portfolio._cost_basis["BBB"] == Decimal("50")


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


class TestPriceSeriesSerialization:
    """SimulatedPortfolio.price_series checkpoint/resume round-trip."""

    def test_price_series_round_trips_through_to_dict(self) -> None:
        """price_series survives to_dict()/from_dict() exactly (keys + Decimals)."""
        from almanak.framework.backtesting.models import PricePoint

        ts0 = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        portfolio.price_series = [
            PricePoint(
                timestamp=ts0,
                prices={"arbitrum:0xweth": Decimal("2000"), "USDC": Decimal("1")},
            ),
            PricePoint(
                timestamp=ts0 + timedelta(hours=1),
                prices={"arbitrum:0xweth": Decimal("2100.50"), "USDC": Decimal("1")},
            ),
        ]

        restored = SimulatedPortfolio.from_dict(portfolio.to_dict())

        assert len(restored.price_series) == 2
        assert restored.price_series[0].timestamp == ts0
        assert restored.price_series[0].prices == {"arbitrum:0xweth": Decimal("2000"), "USDC": Decimal("1")}
        assert restored.price_series[1].prices["arbitrum:0xweth"] == Decimal("2100.50")

    def test_legacy_portfolio_dict_without_price_series_loads_empty(self) -> None:
        """Checkpoints written before price_series existed resume with an empty series."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        payload = portfolio.to_dict()
        payload.pop("price_series", None)

        restored = SimulatedPortfolio.from_dict(payload)

        assert restored.price_series == []
