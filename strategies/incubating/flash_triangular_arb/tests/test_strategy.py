"""Tests for Flash Loan Triangular Arbitrage Strategy.

Tests cover:
- Configuration validation
- Token path generation
- Opportunity detection
- Flash loan intent creation
- State management
"""

import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import FlashLoanIntent, HoldIntent, SwapIntent
from strategies.flash_triangular_arb import (
    FlashTriangularArbConfig,
    FlashTriangularArbStrategy,
    TriangularArbState,
    TriangularOpportunity,
)
from strategies.flash_triangular_arb.strategy import SwapLeg

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def config() -> FlashTriangularArbConfig:
    """Create a test configuration."""
    return FlashTriangularArbConfig(
        strategy_id="test_flash_triangular_arb",
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
        tokens=["WETH", "USDC", "WBTC"],
        dexs=["uniswap_v3", "curve"],
        min_profit_bps=10,
        min_profit_usd=Decimal("10"),
        default_trade_size_usd=Decimal("10000"),
        max_hops=3,
        min_hops=3,
        max_slippage_bps=50,
        max_total_slippage_bps=150,
    )


@pytest.fixture
def strategy(config: FlashTriangularArbConfig) -> FlashTriangularArbStrategy:
    """Create a test strategy instance."""
    return FlashTriangularArbStrategy(config=config)


@pytest.fixture
def mock_price_service() -> MagicMock:
    """Create a mock price service."""
    service = MagicMock()
    return service


@pytest.fixture
def mock_flash_loan_selector() -> MagicMock:
    """Create a mock flash loan selector."""
    selector = MagicMock()
    return selector


@pytest.fixture
def mock_market_snapshot() -> MagicMock:
    """Create a mock market snapshot."""
    return MagicMock()


# =============================================================================
# Configuration Tests
# =============================================================================


class TestFlashTriangularArbConfig:
    """Tests for FlashTriangularArbConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = FlashTriangularArbConfig()
        assert config.chain == "ethereum"
        assert config.max_hops == 3
        assert config.min_hops == 3
        assert config.min_profit_bps == 10
        assert config.min_profit_usd == Decimal("10")
        assert config.max_slippage_bps == 50
        assert config.max_total_slippage_bps == 150

    def test_custom_values(self, config: FlashTriangularArbConfig) -> None:
        """Test custom configuration values."""
        assert config.strategy_id == "test_flash_triangular_arb"
        assert config.chain == "ethereum"
        assert config.tokens == ["WETH", "USDC", "WBTC"]
        assert config.dexs == ["uniswap_v3", "curve"]

    def test_to_dict(self, config: FlashTriangularArbConfig) -> None:
        """Test converting config to dictionary."""
        d = config.to_dict()
        assert d["strategy_id"] == "test_flash_triangular_arb"
        assert d["chain"] == "ethereum"
        assert d["max_hops"] == 3
        assert d["tokens"] == ["WETH", "USDC", "WBTC"]

    def test_from_dict(self) -> None:
        """Test creating config from dictionary."""
        data = {
            "strategy_id": "test",
            "chain": "arbitrum",
            "tokens": ["WETH", "USDC"],
            "min_profit_bps": 20,
            "max_hops": 4,
        }
        config = FlashTriangularArbConfig.from_dict(data)
        assert config.strategy_id == "test"
        assert config.chain == "arbitrum"
        assert config.tokens == ["WETH", "USDC"]
        assert config.min_profit_bps == 20
        assert config.max_hops == 4

    def test_is_profitable_true(self, config: FlashTriangularArbConfig) -> None:
        """Test profitability check - profitable case."""
        # Gross profit = $50, profit bps = 50
        assert config.is_profitable(
            gross_profit_usd=Decimal("50"),
            gross_profit_bps=50,
        )

    def test_is_profitable_false_low_bps(self, config: FlashTriangularArbConfig) -> None:
        """Test profitability check - fails on low bps."""
        # Profit bps = 5 < min 10
        assert not config.is_profitable(
            gross_profit_usd=Decimal("50"),
            gross_profit_bps=5,
        )

    def test_is_profitable_false_low_usd(self, config: FlashTriangularArbConfig) -> None:
        """Test profitability check - fails on low USD after gas."""
        # Net profit = $35 - $30 gas = $5 < min $10
        config.estimated_gas_cost_usd = Decimal("30")
        assert not config.is_profitable(
            gross_profit_usd=Decimal("35"),
            gross_profit_bps=50,
        )

    def test_calculate_min_output(self, config: FlashTriangularArbConfig) -> None:
        """Test minimum output calculation with slippage."""
        # 50 bps = 0.5% slippage
        min_out = config.calculate_min_output(Decimal("1000"))
        expected = Decimal("1000") * Decimal("9950") / Decimal("10000")
        assert min_out == expected

    def test_get_estimated_gas_per_hop(self, config: FlashTriangularArbConfig) -> None:
        """Test gas per hop estimation."""
        config.estimated_gas_cost_usd = Decimal("30")
        config.max_hops = 3
        gas_per_hop = config.get_estimated_gas_per_hop()
        assert gas_per_hop == Decimal("10")


# =============================================================================
# Strategy Initialization Tests
# =============================================================================


class TestStrategyInitialization:
    """Tests for strategy initialization."""

    def test_basic_initialization(self, config: FlashTriangularArbConfig) -> None:
        """Test basic strategy initialization."""
        strategy = FlashTriangularArbStrategy(config=config)
        assert strategy.config == config
        assert strategy._state == TriangularArbState.SCANNING
        assert strategy._current_opportunity is None
        assert strategy._paths_generated is False

    def test_initialization_with_mocks(
        self,
        config: FlashTriangularArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test initialization with mock services."""
        strategy = FlashTriangularArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )
        assert strategy._price_service == mock_price_service
        assert strategy._flash_loan_selector == mock_flash_loan_selector

    def test_strategy_name(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test strategy name is set correctly."""
        assert strategy.STRATEGY_NAME == "flash_triangular_arb"


# =============================================================================
# Token Path Generation Tests
# =============================================================================


class TestTokenPathGeneration:
    """Tests for token path generation."""

    def test_generate_triangular_paths(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test triangular path generation with 3 tokens."""
        strategy._generate_token_paths()
        paths = strategy.get_token_paths()

        # With 3 tokens, we have 3! = 6 permutations for triangular
        assert len(paths) == 6
        assert strategy._paths_generated is True

        # Each path should have 4 elements (A -> B -> C -> A)
        for path in paths:
            assert len(path) == 4
            assert path[0] == path[-1]  # Starts and ends with same token

    def test_generate_paths_more_tokens(self) -> None:
        """Test path generation with more tokens."""
        config = FlashTriangularArbConfig(
            strategy_id="test",
            chain="ethereum",
            wallet_address="0x1234",
            tokens=["WETH", "USDC", "WBTC", "DAI"],
            max_hops=3,
            min_hops=3,
            max_paths_to_evaluate=50,
        )
        strategy = FlashTriangularArbStrategy(config=config)
        strategy._generate_token_paths()
        paths = strategy.get_token_paths()

        # With 4 tokens and max_hops=3: 4 * 3 * 2 = 24 permutations
        assert len(paths) == 24

    def test_generate_paths_with_quadrilateral(self) -> None:
        """Test path generation including quadrilateral paths."""
        config = FlashTriangularArbConfig(
            strategy_id="test",
            chain="ethereum",
            wallet_address="0x1234",
            tokens=["WETH", "USDC", "WBTC", "DAI"],
            max_hops=4,
            min_hops=3,
            max_paths_to_evaluate=100,
        )
        strategy = FlashTriangularArbStrategy(config=config)
        strategy._generate_token_paths()
        paths = strategy.get_token_paths()

        # Should include both triangular (24) and quadrilateral (24) paths
        # Total = 48, capped at max_paths_to_evaluate
        assert len(paths) <= 100

        # Check we have both 4-hop and 5-hop paths
        triangular_count = sum(1 for p in paths if len(p) == 4)
        quadrilateral_count = sum(1 for p in paths if len(p) == 5)
        assert triangular_count > 0
        assert quadrilateral_count > 0

    def test_regenerate_paths(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test path regeneration."""
        strategy._generate_token_paths()
        initial_paths = strategy.get_token_paths()

        # Regenerate
        strategy.regenerate_paths()
        new_paths = strategy.get_token_paths()

        # Should have same paths
        assert len(initial_paths) == len(new_paths)


# =============================================================================
# State Management Tests
# =============================================================================


class TestStateManagement:
    """Tests for strategy state management."""

    def test_initial_state(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test initial state is SCANNING."""
        assert strategy.get_state() == TriangularArbState.SCANNING

    def test_cooldown_state(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test cooldown state after trade."""
        # Simulate recent trade
        strategy.config.last_trade_timestamp = int(time.time())
        strategy.config.trade_cooldown_seconds = 60

        strategy._update_state()
        assert strategy._state == TriangularArbState.COOLDOWN

    def test_can_trade_no_previous(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test can_trade returns True with no previous trades."""
        assert strategy._can_trade() is True

    def test_can_trade_cooldown_active(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test can_trade returns False during cooldown."""
        strategy.config.last_trade_timestamp = int(time.time())
        strategy.config.trade_cooldown_seconds = 60
        assert strategy._can_trade() is False

    def test_can_trade_cooldown_expired(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test can_trade returns True after cooldown expires."""
        strategy.config.last_trade_timestamp = int(time.time()) - 120
        strategy.config.trade_cooldown_seconds = 60
        assert strategy._can_trade() is True

    def test_cooldown_remaining(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test cooldown remaining calculation."""
        strategy.config.last_trade_timestamp = int(time.time()) - 30
        strategy.config.trade_cooldown_seconds = 60
        remaining = strategy._cooldown_remaining()
        assert 25 <= remaining <= 35  # Allow for timing variance

    def test_clear_state(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test clearing strategy state."""
        # Set some state
        strategy._state = TriangularArbState.COOLDOWN
        strategy.config.total_trades = 5
        strategy.config.total_profit_usd = Decimal("100")

        strategy.clear_state()

        assert strategy._state == TriangularArbState.SCANNING
        assert strategy._current_opportunity is None
        assert strategy.config.total_trades == 0
        assert strategy.config.total_profit_usd == Decimal("0")


# =============================================================================
# Opportunity Detection Tests
# =============================================================================


class TestOpportunityDetection:
    """Tests for opportunity detection."""

    def test_evaluate_path_short_path(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test that short paths are rejected."""
        result = strategy._evaluate_path(["WETH", "USDC"])
        assert result is None

    def test_evaluate_path_no_flash_loan(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test evaluation fails when flash loan unavailable."""
        with patch.object(strategy, "_get_flash_loan_info", return_value=None):
            result = strategy._evaluate_path(["WETH", "USDC", "WBTC", "WETH"])
            assert result is None

    def test_evaluate_path_no_quotes(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test evaluation fails when quotes unavailable."""
        # Mock flash loan success
        mock_flash_result = MagicMock()
        mock_flash_result.is_success = True
        mock_flash_result.fee_amount = Decimal("0")
        mock_flash_result.provider = "balancer"

        with patch.object(strategy, "_get_flash_loan_info", return_value=mock_flash_result):
            with patch.object(strategy, "_get_best_quote", return_value=None):
                result = strategy._evaluate_path(["WETH", "USDC", "WBTC", "WETH"])
                assert result is None

    def test_evaluate_path_profitable(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test successful path evaluation."""
        # Mock flash loan
        mock_flash_result = MagicMock()
        mock_flash_result.is_success = True
        mock_flash_result.fee_amount = Decimal("0")
        mock_flash_result.provider = "balancer"

        # Mock quotes that result in profit
        # WETH -> USDC: 10000 -> 25000
        # USDC -> WBTC: 25000 -> 0.6
        # WBTC -> WETH: 0.6 -> 10100 (1% profit)
        mock_quotes = [
            MagicMock(dex="uniswap_v3", amount_out=Decimal("25000"), price_impact_bps=10),
            MagicMock(dex="curve", amount_out=Decimal("0.6"), price_impact_bps=10),
            MagicMock(dex="uniswap_v3", amount_out=Decimal("10100"), price_impact_bps=10),
        ]

        call_count = [0]

        def mock_get_quote(from_token: str, to_token: str, amount: Decimal) -> MagicMock:
            result = mock_quotes[call_count[0]]
            call_count[0] += 1
            return result

        with patch.object(strategy, "_get_flash_loan_info", return_value=mock_flash_result):
            with patch.object(strategy, "_get_best_quote", side_effect=mock_get_quote):
                result = strategy._evaluate_path(["WETH", "USDC", "WBTC", "WETH"])

                assert result is not None
                assert result.path == ["WETH", "USDC", "WBTC", "WETH"]
                assert len(result.legs) == 3
                assert result.gross_profit == Decimal("100")  # 10100 - 10000
                assert result.flash_loan_provider == "balancer"

    def test_evaluate_path_price_impact_too_high(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test path rejected when cumulative price impact too high."""
        strategy.config.max_total_slippage_bps = 50

        # Mock flash loan
        mock_flash_result = MagicMock()
        mock_flash_result.is_success = True
        mock_flash_result.fee_amount = Decimal("0")
        mock_flash_result.provider = "balancer"

        # Mock quotes with high price impact
        mock_quotes = [
            MagicMock(dex="uniswap_v3", amount_out=Decimal("25000"), price_impact_bps=20),
            MagicMock(dex="curve", amount_out=Decimal("0.6"), price_impact_bps=20),
            MagicMock(dex="uniswap_v3", amount_out=Decimal("10100"), price_impact_bps=20),
        ]

        call_count = [0]

        def mock_get_quote(from_token: str, to_token: str, amount: Decimal) -> MagicMock:
            result = mock_quotes[call_count[0]]
            call_count[0] += 1
            return result

        with patch.object(strategy, "_get_flash_loan_info", return_value=mock_flash_result):
            with patch.object(strategy, "_get_best_quote", side_effect=mock_get_quote):
                result = strategy._evaluate_path(["WETH", "USDC", "WBTC", "WETH"])
                # Total impact = 60 bps > 50 max
                assert result is None

    def test_find_best_opportunity(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test finding best opportunity from multiple paths."""
        strategy._generate_token_paths()

        # Create mock opportunities with different profits
        opportunities = [
            MagicMock(net_profit_usd=Decimal("50")),
            MagicMock(net_profit_usd=Decimal("100")),  # Best
            MagicMock(net_profit_usd=Decimal("30")),
        ]

        call_count = [0]

        def mock_evaluate(path: list[str]) -> MagicMock | None:
            if call_count[0] < len(opportunities):
                result = opportunities[call_count[0]]
                call_count[0] += 1
                return result
            return None

        with patch.object(strategy, "_evaluate_path", side_effect=mock_evaluate):
            best = strategy._find_best_opportunity()
            assert best is not None
            assert best.net_profit_usd == Decimal("100")


# =============================================================================
# Intent Creation Tests
# =============================================================================


class TestIntentCreation:
    """Tests for intent creation."""

    def test_create_arbitrage_intent(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test creating flash loan arbitrage intent."""
        opportunity = TriangularOpportunity(
            path=["WETH", "USDC", "WBTC", "WETH"],
            legs=[
                SwapLeg(
                    from_token="WETH",
                    to_token="USDC",
                    dex="uniswap_v3",
                    amount_in=Decimal("10000"),
                    amount_out=Decimal("25000"),
                ),
                SwapLeg(
                    from_token="USDC",
                    to_token="WBTC",
                    dex="curve",
                    amount_in=Decimal("25000"),
                    amount_out=Decimal("0.6"),
                ),
                SwapLeg(
                    from_token="WBTC",
                    to_token="WETH",
                    dex="uniswap_v3",
                    amount_in=Decimal("0.6"),
                    amount_out=Decimal("10100"),
                ),
            ],
            flash_loan_token="WETH",
            flash_loan_amount=Decimal("10000"),
            flash_loan_provider="balancer",
            flash_loan_fee=Decimal("0"),
            gross_profit=Decimal("100"),
            gross_profit_bps=100,
            gross_profit_usd=Decimal("250"),  # 100 WETH * 2500
            net_profit_usd=Decimal("220"),
            total_price_impact_bps=30,
            timestamp=datetime.now(UTC),
        )

        intent = strategy._create_arbitrage_intent(opportunity)

        # Verify flash loan intent structure
        assert isinstance(intent, FlashLoanIntent)
        assert intent.token == "WETH"
        assert intent.amount == Decimal("10000")
        assert intent.provider == "balancer"

        # Verify callbacks
        assert len(intent.callback_intents) == 3

        # First swap should use exact amount
        first_swap = intent.callback_intents[0]
        assert isinstance(first_swap, SwapIntent)
        assert first_swap.from_token == "WETH"
        assert first_swap.to_token == "USDC"
        assert first_swap.amount == Decimal("10000")

        # Subsequent swaps use "all"
        second_swap = intent.callback_intents[1]
        assert isinstance(second_swap, SwapIntent)
        assert second_swap.from_token == "USDC"
        assert second_swap.to_token == "WBTC"
        assert second_swap.amount == "all"

        third_swap = intent.callback_intents[2]
        assert isinstance(third_swap, SwapIntent)
        assert third_swap.from_token == "WBTC"
        assert third_swap.to_token == "WETH"
        assert third_swap.amount == "all"

    def test_record_trade(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test trade recording."""
        opportunity = TriangularOpportunity(
            path=["WETH", "USDC", "WBTC", "WETH"],
            legs=[],
            flash_loan_token="WETH",
            flash_loan_amount=Decimal("10000"),
            flash_loan_provider="balancer",
            flash_loan_fee=Decimal("0"),
            gross_profit=Decimal("100"),
            gross_profit_bps=100,
            gross_profit_usd=Decimal("250"),
            net_profit_usd=Decimal("220"),
            total_price_impact_bps=30,
            timestamp=datetime.now(UTC),
        )

        strategy._current_opportunity = opportunity
        strategy._record_trade()

        assert strategy.config.total_trades == 1
        assert strategy.config.total_profit_usd == Decimal("220")
        assert strategy.config.last_trade_timestamp is not None
        assert strategy._current_opportunity is None
        assert strategy._state == TriangularArbState.COOLDOWN


# =============================================================================
# Decide Tests
# =============================================================================


class TestDecide:
    """Tests for decide method."""

    def test_decide_paused(
        self,
        strategy: FlashTriangularArbStrategy,
        mock_market_snapshot: MagicMock,
    ) -> None:
        """Test decide returns hold when paused."""
        strategy.config.pause_strategy = True

        result = strategy.decide(mock_market_snapshot)

        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "paused" in result.reason.lower()

    def test_decide_cooldown(
        self,
        strategy: FlashTriangularArbStrategy,
        mock_market_snapshot: MagicMock,
    ) -> None:
        """Test decide returns hold during cooldown."""
        strategy.config.last_trade_timestamp = int(time.time())
        strategy.config.trade_cooldown_seconds = 60

        result = strategy.decide(mock_market_snapshot)

        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "cooldown" in result.reason.lower()

    def test_decide_no_opportunity(
        self,
        strategy: FlashTriangularArbStrategy,
        mock_market_snapshot: MagicMock,
    ) -> None:
        """Test decide returns hold when no opportunity found."""
        with patch.object(strategy, "_find_best_opportunity", return_value=None):
            result = strategy.decide(mock_market_snapshot)

            assert isinstance(result, HoldIntent)
            assert result.reason is not None
            assert "no profitable" in result.reason.lower()

    def test_decide_opportunity_found(
        self,
        strategy: FlashTriangularArbStrategy,
        mock_market_snapshot: MagicMock,
    ) -> None:
        """Test decide returns flash loan intent when opportunity found."""
        opportunity = TriangularOpportunity(
            path=["WETH", "USDC", "WBTC", "WETH"],
            legs=[
                SwapLeg(
                    from_token="WETH",
                    to_token="USDC",
                    dex="uniswap_v3",
                    amount_in=Decimal("10000"),
                    amount_out=Decimal("25000"),
                ),
                SwapLeg(
                    from_token="USDC",
                    to_token="WBTC",
                    dex="curve",
                    amount_in=Decimal("25000"),
                    amount_out=Decimal("0.6"),
                ),
                SwapLeg(
                    from_token="WBTC",
                    to_token="WETH",
                    dex="uniswap_v3",
                    amount_in=Decimal("0.6"),
                    amount_out=Decimal("10100"),
                ),
            ],
            flash_loan_token="WETH",
            flash_loan_amount=Decimal("10000"),
            flash_loan_provider="balancer",
            flash_loan_fee=Decimal("0"),
            gross_profit=Decimal("100"),
            gross_profit_bps=100,
            gross_profit_usd=Decimal("250"),
            net_profit_usd=Decimal("220"),
            total_price_impact_bps=30,
            timestamp=datetime.now(UTC),
        )

        with patch.object(strategy, "_find_best_opportunity", return_value=opportunity):
            result = strategy.decide(mock_market_snapshot)

            assert isinstance(result, FlashLoanIntent)
            assert result.token == "WETH"


# =============================================================================
# Statistics Tests
# =============================================================================


class TestStatistics:
    """Tests for strategy statistics."""

    def test_get_stats(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test getting strategy statistics."""
        strategy._generate_token_paths()
        strategy.config.total_trades = 5
        strategy.config.total_profit_usd = Decimal("500")

        stats = strategy.get_stats()

        assert stats["state"] == "scanning"
        assert stats["total_trades"] == 5
        assert stats["total_profit_usd"] == "500"
        assert stats["paths_count"] == 6  # 3 tokens = 6 paths

    def test_scan_opportunities_manual(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test manual opportunity scanning."""
        opportunities = [
            MagicMock(net_profit_usd=Decimal("50")),
            MagicMock(net_profit_usd=Decimal("100")),
        ]

        call_count = [0]

        def mock_evaluate(path: list[str]) -> MagicMock | None:
            if call_count[0] < len(opportunities):
                result = opportunities[call_count[0]]
                call_count[0] += 1
                return result
            return None

        with patch.object(strategy, "_evaluate_path", side_effect=mock_evaluate):
            result = strategy.scan_opportunities()

            # Should be sorted by profit descending
            assert len(result) == 2
            assert result[0].net_profit_usd == Decimal("100")
            assert result[1].net_profit_usd == Decimal("50")


# =============================================================================
# SwapLeg Tests
# =============================================================================


class TestSwapLeg:
    """Tests for SwapLeg dataclass."""

    def test_swap_leg_creation(self) -> None:
        """Test creating a swap leg."""
        leg = SwapLeg(
            from_token="WETH",
            to_token="USDC",
            dex="uniswap_v3",
            amount_in=Decimal("1"),
            amount_out=Decimal("2500"),
            price_impact_bps=10,
        )

        assert leg.from_token == "WETH"
        assert leg.to_token == "USDC"
        assert leg.dex == "uniswap_v3"
        assert leg.amount_in == Decimal("1")
        assert leg.amount_out == Decimal("2500")
        assert leg.price_impact_bps == 10

    def test_swap_leg_to_dict(self) -> None:
        """Test swap leg to dict conversion."""
        leg = SwapLeg(
            from_token="WETH",
            to_token="USDC",
            dex="uniswap_v3",
            amount_in=Decimal("1"),
            amount_out=Decimal("2500"),
            price_impact_bps=10,
        )

        d = leg.to_dict()

        assert d["from_token"] == "WETH"
        assert d["to_token"] == "USDC"
        assert d["amount_in"] == "1"
        assert d["amount_out"] == "2500"


# =============================================================================
# TriangularOpportunity Tests
# =============================================================================


class TestTriangularOpportunity:
    """Tests for TriangularOpportunity dataclass."""

    def test_opportunity_path_str(self) -> None:
        """Test path string representation."""
        opportunity = TriangularOpportunity(
            path=["WETH", "USDC", "WBTC", "WETH"],
            legs=[],
            flash_loan_token="WETH",
            flash_loan_amount=Decimal("10000"),
            flash_loan_provider="balancer",
            flash_loan_fee=Decimal("0"),
            gross_profit=Decimal("100"),
            gross_profit_bps=100,
            gross_profit_usd=Decimal("250"),
            net_profit_usd=Decimal("220"),
            total_price_impact_bps=30,
            timestamp=datetime.now(UTC),
        )

        assert opportunity.path_str == "WETH -> USDC -> WBTC -> WETH"

    def test_opportunity_to_dict(self) -> None:
        """Test opportunity to dict conversion."""
        timestamp = datetime.now(UTC)
        opportunity = TriangularOpportunity(
            path=["WETH", "USDC", "WBTC", "WETH"],
            legs=[
                SwapLeg(
                    from_token="WETH",
                    to_token="USDC",
                    dex="uniswap_v3",
                    amount_in=Decimal("1"),
                    amount_out=Decimal("2500"),
                ),
            ],
            flash_loan_token="WETH",
            flash_loan_amount=Decimal("10000"),
            flash_loan_provider="balancer",
            flash_loan_fee=Decimal("0"),
            gross_profit=Decimal("100"),
            gross_profit_bps=100,
            gross_profit_usd=Decimal("250"),
            net_profit_usd=Decimal("220"),
            total_price_impact_bps=30,
            timestamp=timestamp,
        )

        d = opportunity.to_dict()

        assert d["path"] == ["WETH", "USDC", "WBTC", "WETH"]
        assert d["flash_loan_token"] == "WETH"
        assert d["flash_loan_provider"] == "balancer"
        assert d["gross_profit_bps"] == 100
        assert len(d["legs"]) == 1
        assert d["timestamp"] == timestamp.isoformat()


# =============================================================================
# USD Value Estimation Tests
# =============================================================================


class TestUSDValueEstimation:
    """Tests for USD value estimation."""

    def test_estimate_stablecoin(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test USD estimation for stablecoins."""
        value = strategy._estimate_usd_value(Decimal("1000"), "USDC")
        assert value == Decimal("1000")

        value = strategy._estimate_usd_value(Decimal("500"), "DAI")
        assert value == Decimal("500")

    def test_estimate_weth(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test USD estimation for WETH."""
        value = strategy._estimate_usd_value(Decimal("1"), "WETH")
        assert value == Decimal("2500")

    def test_estimate_wbtc(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test USD estimation for WBTC."""
        value = strategy._estimate_usd_value(Decimal("1"), "WBTC")
        assert value == Decimal("45000")

    def test_estimate_unknown_token(self, strategy: FlashTriangularArbStrategy) -> None:
        """Test USD estimation for unknown token defaults to 1:1."""
        value = strategy._estimate_usd_value(Decimal("100"), "UNKNOWN")
        assert value == Decimal("100")
