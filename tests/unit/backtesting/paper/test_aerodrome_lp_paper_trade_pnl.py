"""Tests for paper trading PnL with Aerodrome LP intents on Base.

Validates that the paper trading pipeline correctly calculates PnL for
LP operations (LP_OPEN, LP_CLOSE) as opposed to simple swaps:

1. LP_OPEN: tokens_out for both assets, net_token_flow_usd is negative
2. LP_CLOSE: tokens_in for both assets plus fees, net_token_flow_usd positive
3. Full LP lifecycle PnL: open cost + close proceeds = net profit/loss
4. IL (impermanent loss) scenarios where close returns < open cost
5. Fee income scenarios where fees offset IL

First paper trading test for LP intent PnL calculations.
Kitchen Loop iteration 118, VIB-1695.
"""

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.backtesting.paper.models import PaperTrade


# =============================================================================
# LP_OPEN PnL tests
# =============================================================================


class TestLPOpenPnL:
    """Test PnL calculation for LP_OPEN intents (providing liquidity)."""

    def test_lp_open_has_negative_net_flow(self):
        """LP_OPEN sends both tokens to the pool - net flow should be negative."""
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20000000,
            intent={"type": "LP_OPEN", "protocol": "aerodrome"},
            tx_hash="0xopen1",
            gas_used=300000,
            gas_cost_usd=Decimal("0.15"),
            tokens_in={},  # No tokens received when opening LP
            tokens_out={
                "WETH": Decimal("0.5"),  # Sent 0.5 WETH
                "USDC": Decimal("1000"),  # Sent 1000 USDC
            },
            protocol="aerodrome",
            intent_type="LP_OPEN",
            token_prices_usd={
                "WETH": Decimal("2000"),
                "USDC": Decimal("1"),
            },
        )

        # Sent $1000 WETH + $1000 USDC = $2000 out, $0 in
        assert trade.net_token_flow_usd == Decimal("-2000")
        # Including gas: -$2000 - $0.15 = -$2000.15
        assert trade.net_pnl_usd == Decimal("-2000.15")

    def test_lp_open_small_position(self):
        """Small LP position for demo strategy testing."""
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20000000,
            intent={"type": "LP_OPEN"},
            tx_hash="0xopen_small",
            gas_used=250000,
            gas_cost_usd=Decimal("0.10"),
            tokens_in={},
            tokens_out={
                "WETH": Decimal("0.001"),
                "USDC": Decimal("2"),
            },
            protocol="aerodrome",
            intent_type="LP_OPEN",
            token_prices_usd={
                "WETH": Decimal("2000"),
                "USDC": Decimal("1"),
            },
        )

        # $2 WETH + $2 USDC = $4 out
        assert trade.net_token_flow_usd == Decimal("-4")

    def test_lp_open_stable_pair(self):
        """LP_OPEN for a stable pair (USDC/USDT) on Aerodrome."""
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20000000,
            intent={"type": "LP_OPEN", "stable": True},
            tx_hash="0xstable",
            gas_used=280000,
            gas_cost_usd=Decimal("0.12"),
            tokens_in={},
            tokens_out={
                "USDC": Decimal("500"),
                "USDT": Decimal("500"),
            },
            protocol="aerodrome",
            intent_type="LP_OPEN",
            token_prices_usd={
                "USDC": Decimal("1"),
                "USDT": Decimal("0.9999"),
            },
        )

        # $500 + $499.95 = $999.95 out
        expected = Decimal("-999.95")
        assert trade.net_token_flow_usd == expected


# =============================================================================
# LP_CLOSE PnL tests
# =============================================================================


class TestLPClosePnL:
    """Test PnL calculation for LP_CLOSE intents (removing liquidity)."""

    def test_lp_close_has_positive_net_flow(self):
        """LP_CLOSE receives both tokens back - net flow should be positive."""
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20001000,
            intent={"type": "LP_CLOSE", "protocol": "aerodrome"},
            tx_hash="0xclose1",
            gas_used=250000,
            gas_cost_usd=Decimal("0.12"),
            tokens_in={
                "WETH": Decimal("0.48"),  # Slightly less WETH (IL)
                "USDC": Decimal("1050"),  # More USDC (IL + fees)
            },
            tokens_out={},  # No tokens sent when closing LP
            protocol="aerodrome",
            intent_type="LP_CLOSE",
            token_prices_usd={
                "WETH": Decimal("2200"),  # Price moved up
                "USDC": Decimal("1"),
            },
        )

        # Received: 0.48 * $2200 + 1050 * $1 = $1056 + $1050 = $2106
        expected = Decimal("0.48") * Decimal("2200") + Decimal("1050")
        assert trade.net_token_flow_usd == expected
        assert trade.net_token_flow_usd > 0

    def test_lp_close_with_fee_income(self):
        """LP_CLOSE should include accumulated trading fees."""
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20002000,
            intent={"type": "LP_CLOSE"},
            tx_hash="0xclose_fees",
            gas_used=250000,
            gas_cost_usd=Decimal("0.12"),
            tokens_in={
                "WETH": Decimal("0.502"),  # Original + fees
                "USDC": Decimal("1005"),  # Original + fees
            },
            tokens_out={},
            protocol="aerodrome",
            intent_type="LP_CLOSE",
            token_prices_usd={
                "WETH": Decimal("2000"),  # Price unchanged
                "USDC": Decimal("1"),
            },
        )

        # Received: 0.502 * $2000 + $1005 = $1004 + $1005 = $2009
        expected = Decimal("0.502") * Decimal("2000") + Decimal("1005")
        assert trade.net_token_flow_usd == expected
        # Should be positive because of fee income
        assert trade.net_pnl_usd > 0


# =============================================================================
# Full LP lifecycle PnL tests
# =============================================================================


class TestLPLifecyclePnL:
    """Test end-to-end LP lifecycle: open -> accrue fees -> close."""

    def _make_open_trade(
        self,
        weth_amount: str = "0.5",
        usdc_amount: str = "1000",
        weth_price: str = "2000",
    ) -> PaperTrade:
        return PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20000000,
            intent={"type": "LP_OPEN"},
            tx_hash="0xopen",
            gas_used=300000,
            gas_cost_usd=Decimal("0.15"),
            tokens_in={},
            tokens_out={
                "WETH": Decimal(weth_amount),
                "USDC": Decimal(usdc_amount),
            },
            protocol="aerodrome",
            intent_type="LP_OPEN",
            token_prices_usd={
                "WETH": Decimal(weth_price),
                "USDC": Decimal("1"),
            },
        )

    def _make_close_trade(
        self,
        weth_amount: str = "0.5",
        usdc_amount: str = "1000",
        weth_price: str = "2000",
    ) -> PaperTrade:
        return PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20010000,
            intent={"type": "LP_CLOSE"},
            tx_hash="0xclose",
            gas_used=250000,
            gas_cost_usd=Decimal("0.12"),
            tokens_in={
                "WETH": Decimal(weth_amount),
                "USDC": Decimal(usdc_amount),
            },
            tokens_out={},
            protocol="aerodrome",
            intent_type="LP_CLOSE",
            token_prices_usd={
                "WETH": Decimal(weth_price),
                "USDC": Decimal("1"),
            },
        )

    def test_breakeven_lp_lifecycle(self):
        """Open and close at same price, same amounts -> only gas loss."""
        open_trade = self._make_open_trade()
        close_trade = self._make_close_trade()

        lifecycle_pnl = open_trade.net_pnl_usd + close_trade.net_pnl_usd

        # Should be negative by exactly the gas costs
        expected_gas = Decimal("0.15") + Decimal("0.12")
        assert lifecycle_pnl == -expected_gas

    def test_profitable_lp_with_fees(self):
        """LP earns fees that exceed gas costs -> net profit."""
        open_trade = self._make_open_trade(
            weth_amount="0.5", usdc_amount="1000", weth_price="2000"
        )
        # Close receives more due to fee accrual
        close_trade = self._make_close_trade(
            weth_amount="0.505", usdc_amount="1010", weth_price="2000"
        )

        lifecycle_pnl = open_trade.net_pnl_usd + close_trade.net_pnl_usd

        # Fee income: 0.005 * $2000 + $10 = $10 + $10 = $20
        # Gas costs: $0.15 + $0.12 = $0.27
        # Net: $20 - $0.27 = $19.73
        assert lifecycle_pnl > 0
        assert lifecycle_pnl == Decimal("19.73")

    def test_impermanent_loss_scenario(self):
        """ETH price increases -> IL reduces WETH amount returned."""
        open_trade = self._make_open_trade(
            weth_amount="0.5", usdc_amount="1000", weth_price="2000"
        )
        # Total open value: $1000 + $1000 = $2000

        # After price increase, IL reduces WETH:
        # If price doubles, AMM rebalances: less WETH, more USDC
        close_trade = self._make_close_trade(
            weth_amount="0.35",  # Less WETH (IL)
            usdc_amount="1400",  # More USDC (IL)
            weth_price="4000",  # ETH doubled
        )
        # Close value: 0.35 * $4000 + $1400 = $1400 + $1400 = $2800

        lifecycle_pnl = open_trade.net_pnl_usd + close_trade.net_pnl_usd

        # $2800 - $2000 - gas = $800 - $0.27 = $799.73
        # Note: without LP, HODL would have been 0.5*$4000 + $1000 = $3000
        # IL = $3000 - $2800 = $200
        assert lifecycle_pnl > 0  # Still profitable in USD terms
        assert lifecycle_pnl == Decimal("799.73")

    def test_loss_scenario_severe_il(self):
        """Severe price move + low fees -> net loss."""
        open_trade = self._make_open_trade(
            weth_amount="0.5", usdc_amount="1000", weth_price="2000"
        )
        # Total open: $2000

        # Severe crash: WETH drops 90%
        close_trade = self._make_close_trade(
            weth_amount="5.0",  # Much more WETH (IL from crash)
            usdc_amount="100",  # Much less USDC
            weth_price="200",  # ETH crashed 90%
        )
        # Close value: 5.0 * $200 + $100 = $1000 + $100 = $1100

        lifecycle_pnl = open_trade.net_pnl_usd + close_trade.net_pnl_usd

        # $1100 - $2000 - gas = -$900 - $0.27 = -$900.27
        assert lifecycle_pnl < 0
        assert lifecycle_pnl == Decimal("-900.27")

    def test_fee_income_offsets_il(self):
        """Trading fees offset impermanent loss."""
        open_trade = self._make_open_trade(
            weth_amount="0.5", usdc_amount="1000", weth_price="2000"
        )

        # Moderate price move, but good fee income
        close_trade = self._make_close_trade(
            weth_amount="0.45",  # Slight IL
            usdc_amount="1150",  # More USDC from IL + fees
            weth_price="2500",  # 25% price increase
        )
        # Close value: 0.45 * $2500 + $1150 = $1125 + $1150 = $2275
        # Open value: $2000
        # Net: $275 - gas

        lifecycle_pnl = open_trade.net_pnl_usd + close_trade.net_pnl_usd
        assert lifecycle_pnl == Decimal("274.73")


# =============================================================================
# Serialization tests
# =============================================================================


class TestLPTradeSerialization:
    """Test that LP trade PnL values are correctly serialized."""

    def test_lp_open_serialization_includes_pnl(self):
        """LP_OPEN trade serialization should include computed PnL fields."""
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20000000,
            intent={"type": "LP_OPEN"},
            tx_hash="0xopen",
            gas_used=300000,
            gas_cost_usd=Decimal("0.15"),
            tokens_in={},
            tokens_out={
                "WETH": Decimal("0.5"),
                "USDC": Decimal("1000"),
            },
            protocol="aerodrome",
            intent_type="LP_OPEN",
            token_prices_usd={
                "WETH": Decimal("2000"),
                "USDC": Decimal("1"),
            },
        )

        data = trade.to_dict()
        assert "net_token_flow_usd" in data
        assert Decimal(data["net_token_flow_usd"]) == Decimal("-2000")
        assert data["intent_type"] == "LP_OPEN"
        assert data["protocol"] == "aerodrome"

    def test_lp_close_serialization_includes_pnl(self):
        """LP_CLOSE trade serialization should include positive PnL."""
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20010000,
            intent={"type": "LP_CLOSE"},
            tx_hash="0xclose",
            gas_used=250000,
            gas_cost_usd=Decimal("0.12"),
            tokens_in={
                "WETH": Decimal("0.505"),
                "USDC": Decimal("1010"),
            },
            tokens_out={},
            protocol="aerodrome",
            intent_type="LP_CLOSE",
            token_prices_usd={
                "WETH": Decimal("2000"),
                "USDC": Decimal("1"),
            },
        )

        data = trade.to_dict()
        net_flow = Decimal(data["net_token_flow_usd"])
        # 0.505 * 2000 + 1010 = 1010 + 1010 = 2020
        assert net_flow == Decimal("2020")
        assert data["intent_type"] == "LP_CLOSE"


# =============================================================================
# Aerodrome-specific LP tests
# =============================================================================


class TestAerodromeLPSpecifics:
    """Test Aerodrome-specific LP behavior in paper trading."""

    def test_volatile_pool_lp_open(self):
        """Volatile pool (x*y=k) LP open - WETH/USDC."""
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20000000,
            intent={"type": "LP_OPEN", "stable": False, "pool": "WETH/USDC"},
            tx_hash="0xvolatile",
            gas_used=300000,
            gas_cost_usd=Decimal("0.15"),
            tokens_in={},
            tokens_out={
                "WETH": Decimal("0.001"),  # Demo size per config
                "USDC": Decimal("2"),
            },
            protocol="aerodrome",
            intent_type="LP_OPEN",
            token_prices_usd={
                "WETH": Decimal("2000"),
                "USDC": Decimal("1"),
            },
        )

        # $2 WETH + $2 USDC = $4 out (matches demo config amounts)
        assert trade.net_token_flow_usd == Decimal("-4")

    def test_gas_efficiency_base_l2(self):
        """Base L2 gas costs should be much lower than mainnet."""
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=20000000,
            intent={"type": "LP_OPEN"},
            tx_hash="0xgas",
            gas_used=300000,
            gas_cost_usd=Decimal("0.05"),  # ~$0.05 on Base
            tokens_in={},
            tokens_out={"WETH": Decimal("0.5"), "USDC": Decimal("1000")},
            protocol="aerodrome",
            intent_type="LP_OPEN",
            eth_price_usd=Decimal("2000"),
            token_prices_usd={"WETH": Decimal("2000"), "USDC": Decimal("1")},
        )

        # Gas on Base should be < $1
        assert trade.gas_cost_usd == Decimal("0.05")
        # Gas price in gwei: 0.05 * 1e9 / (300000 * 2000) = 0.083 -> truncates to 0
        gas_gwei = trade.gas_gwei
        assert gas_gwei == 0

    def test_multiple_trades_accumulate_pnl(self):
        """Verify PnL accumulation across multiple LP trades."""
        trades = [
            # Open position
            PaperTrade(
                timestamp=datetime.now(UTC),
                block_number=20000000,
                intent={"type": "LP_OPEN"},
                tx_hash="0x1",
                gas_used=300000,
                gas_cost_usd=Decimal("0.15"),
                tokens_in={},
                tokens_out={"WETH": Decimal("0.5"), "USDC": Decimal("1000")},
                protocol="aerodrome",
                intent_type="LP_OPEN",
                token_prices_usd={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            ),
            # Swap (rebalancing)
            PaperTrade(
                timestamp=datetime.now(UTC),
                block_number=20005000,
                intent={"type": "SWAP"},
                tx_hash="0x2",
                gas_used=150000,
                gas_cost_usd=Decimal("0.08"),
                tokens_in={"USDC": Decimal("100")},
                tokens_out={"WETH": Decimal("0.045")},
                protocol="aerodrome",
                intent_type="SWAP",
                token_prices_usd={"WETH": Decimal("2200"), "USDC": Decimal("1")},
            ),
            # Close position
            PaperTrade(
                timestamp=datetime.now(UTC),
                block_number=20010000,
                intent={"type": "LP_CLOSE"},
                tx_hash="0x3",
                gas_used=250000,
                gas_cost_usd=Decimal("0.12"),
                tokens_in={"WETH": Decimal("0.48"), "USDC": Decimal("1060")},
                tokens_out={},
                protocol="aerodrome",
                intent_type="LP_CLOSE",
                token_prices_usd={"WETH": Decimal("2200"), "USDC": Decimal("1")},
            ),
        ]

        total_pnl = sum(t.net_pnl_usd for t in trades)

        # Open: -$2000 - $0.15 = -$2000.15
        # Swap: $100 - 0.045*$2200 - $0.08 = $100 - $99 - $0.08 = $0.92
        # Close: 0.48*$2200 + $1060 - $0.12 = $1056 + $1060 - $0.12 = $2115.88
        # Total: -$2000.15 + $0.92 + $2115.88 = $116.65
        assert total_pnl == Decimal("116.65")

        # Track number of trades
        assert len(trades) == 3
        assert sum(1 for t in trades if t.intent_type == "LP_OPEN") == 1
        assert sum(1 for t in trades if t.intent_type == "LP_CLOSE") == 1
        assert sum(1 for t in trades if t.intent_type == "SWAP") == 1
