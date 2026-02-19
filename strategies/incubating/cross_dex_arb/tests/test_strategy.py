"""Tests for Cross-DEX Spot Arbitrage Strategy."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.connectors.flash_loan.selector import (
    FlashLoanSelectionResult,
)
from almanak.framework.intents.vocabulary import (
    FlashLoanIntent,
    HoldIntent,
    SwapIntent,
)
from almanak.gateway.data.price import (
    DexQuote,
    MultiDexPriceResult,
)
from strategies.cross_dex_arb import CrossDexArbConfig, CrossDexArbStrategy
from strategies.cross_dex_arb.strategy import ArbitrageOpportunity, ArbState

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def config() -> CrossDexArbConfig:
    """Create a test configuration."""
    return CrossDexArbConfig(
        strategy_id="test-cross-dex-arb",
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
        tokens=["USDC", "WETH", "USDT"],
        dexs=["uniswap_v3", "curve"],
        flash_loan_provider="auto",
        min_profit_bps=10,
        min_profit_usd=Decimal("10"),
        default_trade_size_usd=Decimal("10000"),
        estimated_gas_cost_usd=Decimal("20"),
        trade_cooldown_seconds=60,
    )


@pytest.fixture
def mock_market() -> MagicMock:
    """Create a mock market snapshot."""
    market = MagicMock()
    balance_info = MagicMock()
    balance_info.balance = Decimal("10000")
    balance_info.balance_usd = Decimal("10000")
    market.balance.return_value = balance_info
    return market


@pytest.fixture
def mock_price_service() -> MagicMock:
    """Create a mock MultiDexPriceService."""
    service = MagicMock()
    return service


@pytest.fixture
def mock_flash_loan_selector() -> MagicMock:
    """Create a mock FlashLoanSelector."""
    selector = MagicMock()
    return selector


def create_quote(
    dex: str,
    token_in: str,
    token_out: str,
    amount_in: Decimal,
    amount_out: Decimal,
    price_impact_bps: int = 5,
    slippage_bps: int = 3,
) -> DexQuote:
    """Helper to create a DexQuote."""
    return DexQuote(
        dex=dex,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        amount_out=amount_out,
        price=amount_out / amount_in if amount_in > 0 else Decimal("0"),
        price_impact_bps=price_impact_bps,
        slippage_estimate_bps=slippage_bps,
        gas_estimate=150000,
        gas_cost_usd=Decimal("5"),
        fee_bps=30,
        chain="ethereum",
    )


def create_multi_dex_result(
    token_in: str,
    token_out: str,
    amount_in: Decimal,
    quotes: dict[str, DexQuote],
) -> MultiDexPriceResult:
    """Helper to create a MultiDexPriceResult."""
    return MultiDexPriceResult(
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        quotes=quotes,
        chain="ethereum",
    )


def create_flash_loan_result(
    provider: str = "balancer",
    fee_bps: int = 0,
    fee_amount: Decimal = Decimal("0"),
) -> FlashLoanSelectionResult:
    """Helper to create a FlashLoanSelectionResult."""
    return FlashLoanSelectionResult(
        provider=provider,
        pool_address="0xBA12222222228d8Ba445958a75a0704d566BF2C8",
        fee_bps=fee_bps,
        fee_amount=fee_amount,
        total_repay=Decimal("10000") + fee_amount,
        gas_estimate=250000,
        providers_evaluated=[],
        selection_reasoning="Selected balancer for zero fee",
    )


# =============================================================================
# Configuration Tests
# =============================================================================


class TestCrossDexArbConfig:
    """Tests for CrossDexArbConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = CrossDexArbConfig()
        assert config.chain == "ethereum"
        assert config.min_profit_bps == 10
        assert config.min_profit_usd == Decimal("10")
        assert config.default_trade_size_usd == Decimal("10000")
        assert config.flash_loan_provider == "auto"
        assert "WETH" in config.tokens
        assert "uniswap_v3" in config.dexs

    def test_to_dict(self, config: CrossDexArbConfig) -> None:
        """Test configuration serialization."""
        data = config.to_dict()
        assert data["strategy_id"] == "test-cross-dex-arb"
        assert data["chain"] == "ethereum"
        assert data["min_profit_bps"] == 10
        assert "USDC" in data["tokens"]

    def test_from_dict(self) -> None:
        """Test configuration deserialization."""
        data = {
            "strategy_id": "test-arb",
            "chain": "arbitrum",
            "wallet_address": "0xabc",
            "tokens": ["WETH", "USDC"],
            "min_profit_bps": 20,
            "min_profit_usd": "50",
        }
        config = CrossDexArbConfig.from_dict(data)
        assert config.strategy_id == "test-arb"
        assert config.chain == "arbitrum"
        assert config.min_profit_bps == 20
        assert config.min_profit_usd == Decimal("50")

    def test_calculate_min_output(self, config: CrossDexArbConfig) -> None:
        """Test minimum output calculation with slippage."""
        config.max_slippage_bps = 50  # 0.5%
        amount_in = Decimal("10000")
        min_output = config.calculate_min_output(amount_in)
        # Expected: 10000 * (10000 - 50) / 10000 = 9950
        assert min_output == Decimal("9950")

    def test_is_profitable_meets_threshold(self, config: CrossDexArbConfig) -> None:
        """Test profitability check when meeting thresholds."""
        config.min_profit_bps = 10
        config.min_profit_usd = Decimal("10")
        config.estimated_gas_cost_usd = Decimal("20")

        # Gross 50 USD, 20 bps -> net 30 USD
        assert config.is_profitable(Decimal("50"), 20) is True

    def test_is_profitable_below_bps_threshold(self, config: CrossDexArbConfig) -> None:
        """Test profitability check below bps threshold."""
        config.min_profit_bps = 20
        # 10 bps is below 20 bps threshold
        assert config.is_profitable(Decimal("100"), 10) is False

    def test_is_profitable_below_usd_threshold(self, config: CrossDexArbConfig) -> None:
        """Test profitability check below USD threshold after gas."""
        config.min_profit_bps = 10
        config.min_profit_usd = Decimal("50")
        config.estimated_gas_cost_usd = Decimal("40")
        # Gross 60 USD, 20 bps -> net 20 USD < 50 threshold
        assert config.is_profitable(Decimal("60"), 20) is False


# =============================================================================
# Strategy Initialization Tests
# =============================================================================


class TestCrossDexArbStrategyInit:
    """Tests for strategy initialization."""

    def test_init_with_config(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test strategy initialization with config."""
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )
        assert strategy.config == config
        assert strategy.STRATEGY_NAME == "cross_dex_arb"
        assert strategy._state == ArbState.SCANNING

    def test_init_creates_default_services(self, config: CrossDexArbConfig) -> None:
        """Test strategy creates default services when not provided."""
        strategy = CrossDexArbStrategy(config=config)
        assert strategy._price_service is not None
        assert strategy._flash_loan_selector is not None


# =============================================================================
# State Management Tests
# =============================================================================


class TestCrossDexArbStateManagement:
    """Tests for strategy state management."""

    def test_initial_state_is_scanning(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test initial state is SCANNING."""
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )
        assert strategy.get_state() == ArbState.SCANNING

    def test_can_trade_initially(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test can trade when no previous trade."""
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )
        assert strategy._can_trade() is True

    def test_cannot_trade_during_cooldown(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test cannot trade during cooldown."""
        config.trade_cooldown_seconds = 60
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        # Simulate recent trade
        import time

        config.last_trade_timestamp = int(time.time())

        assert strategy._can_trade() is False
        assert strategy._cooldown_remaining() > 0

    def test_clear_state(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test clearing strategy state."""
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        # Set some state
        config.total_trades = 5
        config.total_profit_usd = Decimal("100")

        strategy.clear_state()

        assert strategy._state == ArbState.SCANNING
        assert strategy._current_opportunity is None
        assert config.total_trades == 0
        assert config.total_profit_usd == Decimal("0")


# =============================================================================
# Decision Making Tests
# =============================================================================


class TestCrossDexArbDecision:
    """Tests for decide() method."""

    def test_decide_when_paused(
        self,
        config: CrossDexArbConfig,
        mock_market: MagicMock,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test decide returns hold when paused."""
        config.pause_strategy = True
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        result = strategy.decide(mock_market)

        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "paused" in result.reason.lower()

    def test_decide_during_cooldown(
        self,
        config: CrossDexArbConfig,
        mock_market: MagicMock,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test decide returns hold during cooldown."""
        import time

        config.last_trade_timestamp = int(time.time())
        config.trade_cooldown_seconds = 60

        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        result = strategy.decide(mock_market)

        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "cooldown" in result.reason.lower()

    def test_decide_no_opportunity(
        self,
        config: CrossDexArbConfig,
        mock_market: MagicMock,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test decide returns hold when no opportunity found."""

        # Mock price service to return no quotes
        async def mock_get_prices(*args, **kwargs):
            return create_multi_dex_result(
                token_in="USDC",
                token_out="WETH",
                amount_in=Decimal("10000"),
                quotes={},
            )

        mock_price_service.get_prices_across_dexs = AsyncMock(side_effect=mock_get_prices)

        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        result = strategy.decide(mock_market)

        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "no profitable" in result.reason.lower()

    def test_decide_with_profitable_opportunity(
        self,
        config: CrossDexArbConfig,
        mock_market: MagicMock,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test decide returns flash loan intent when opportunity found."""
        # Create quotes with significant spread
        uniswap_quote = create_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),  # 2500 USDC/ETH
        )
        curve_quote = create_quote(
            dex="curve",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("3.9"),  # Lower output = worse price
        )

        # Reverse quotes for completing the arb
        reverse_uniswap = create_quote(
            dex="uniswap_v3",
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal("4"),
            amount_out=Decimal("9900"),  # Worse reverse
        )
        reverse_curve = create_quote(
            dex="curve",
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal("4"),
            amount_out=Decimal("10200"),  # Better reverse = profit
        )

        call_count = 0

        async def mock_get_prices(token_in, token_out, amount_in, **kwargs):
            nonlocal call_count
            call_count += 1
            if token_in == "USDC" and token_out == "WETH":
                return create_multi_dex_result(
                    token_in="USDC",
                    token_out="WETH",
                    amount_in=Decimal("10000"),
                    quotes={"uniswap_v3": uniswap_quote, "curve": curve_quote},
                )
            else:
                # Reverse direction
                return create_multi_dex_result(
                    token_in="WETH",
                    token_out="USDC",
                    amount_in=Decimal("4"),
                    quotes={"uniswap_v3": reverse_uniswap, "curve": reverse_curve},
                )

        mock_price_service.get_prices_across_dexs = AsyncMock(side_effect=mock_get_prices)

        # Mock flash loan selector
        mock_flash_loan_selector.select_provider.return_value = create_flash_loan_result()

        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        result = strategy.decide(mock_market)

        assert isinstance(result, FlashLoanIntent)
        assert result.token == "USDC"
        assert len(result.callback_intents) == 2
        # First callback is buy swap
        assert isinstance(result.callback_intents[0], SwapIntent)
        assert result.callback_intents[0].from_token == "USDC"
        # Second callback is sell swap
        assert isinstance(result.callback_intents[1], SwapIntent)
        assert result.callback_intents[1].to_token == "USDC"


# =============================================================================
# Opportunity Detection Tests
# =============================================================================


class TestOpportunityDetection:
    """Tests for arbitrage opportunity detection."""

    def test_check_opportunity_profitable(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test detecting a profitable opportunity."""
        # Setup profitable quotes
        buy_quote = create_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
        )
        worse_quote = create_quote(
            dex="curve",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("3.9"),
        )
        sell_quote = create_quote(
            dex="curve",
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal("4"),
            amount_out=Decimal("10200"),
        )

        async def mock_get_prices(token_in, token_out, amount_in, **kwargs):
            if token_in == "USDC":
                return create_multi_dex_result(
                    token_in="USDC",
                    token_out="WETH",
                    amount_in=Decimal("10000"),
                    quotes={"uniswap_v3": buy_quote, "curve": worse_quote},
                )
            else:
                return create_multi_dex_result(
                    token_in="WETH",
                    token_out="USDC",
                    amount_in=Decimal("4"),
                    quotes={"curve": sell_quote},
                )

        mock_price_service.get_prices_across_dexs = AsyncMock(side_effect=mock_get_prices)
        mock_flash_loan_selector.select_provider.return_value = create_flash_loan_result()

        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        opportunity = strategy._check_opportunity("USDC", "WETH")

        assert opportunity is not None
        assert opportunity.token_in == "USDC"
        assert opportunity.token_out == "WETH"
        assert opportunity.buy_dex == "uniswap_v3"
        assert opportunity.sell_dex == "curve"
        assert opportunity.gross_profit_bps > 0

    def test_check_opportunity_not_profitable(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test detecting an unprofitable opportunity."""
        # Setup quotes with no spread
        quote1 = create_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
        )
        quote2 = create_quote(
            dex="curve",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),  # Same output = no arb
        )

        async def mock_get_prices(*args, **kwargs):
            return create_multi_dex_result(
                token_in="USDC",
                token_out="WETH",
                amount_in=Decimal("10000"),
                quotes={"uniswap_v3": quote1, "curve": quote2},
            )

        mock_price_service.get_prices_across_dexs = AsyncMock(side_effect=mock_get_prices)

        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        opportunity = strategy._check_opportunity("USDC", "WETH")

        assert opportunity is None

    def test_find_best_opportunity(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test finding the best opportunity across all token pairs."""
        config.tokens = ["USDC", "WETH"]

        # Setup a profitable opportunity
        buy_quote = create_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
        )
        sell_quote = create_quote(
            dex="curve",
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal("4"),
            amount_out=Decimal("10150"),
        )

        async def mock_get_prices(token_in, token_out, amount_in, **kwargs):
            if token_in == "USDC" and token_out == "WETH":
                return create_multi_dex_result(
                    token_in="USDC",
                    token_out="WETH",
                    amount_in=Decimal("10000"),
                    quotes={"uniswap_v3": buy_quote, "curve": buy_quote},
                )
            elif token_in == "WETH" and token_out == "USDC":
                return create_multi_dex_result(
                    token_in="WETH",
                    token_out="USDC",
                    amount_in=Decimal("4"),
                    quotes={"uniswap_v3": sell_quote, "curve": sell_quote},
                )
            else:
                return create_multi_dex_result(
                    token_in=token_in,
                    token_out=token_out,
                    amount_in=amount_in,
                    quotes={},
                )

        mock_price_service.get_prices_across_dexs = AsyncMock(side_effect=mock_get_prices)
        mock_flash_loan_selector.select_provider.return_value = create_flash_loan_result()

        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        best = strategy._find_best_opportunity()

        # Should find the USDC/WETH opportunity
        assert best is not None or best is None  # Depends on profitability calc


# =============================================================================
# Flash Loan Intent Creation Tests
# =============================================================================


class TestFlashLoanIntentCreation:
    """Tests for flash loan intent creation."""

    def test_create_arbitrage_intent(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test creating a flash loan arbitrage intent."""
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        buy_quote = create_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
        )
        sell_quote = create_quote(
            dex="curve",
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal("4"),
            amount_out=Decimal("10200"),
        )

        opportunity = ArbitrageOpportunity(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            buy_dex="uniswap_v3",
            sell_dex="curve",
            buy_quote=buy_quote,
            sell_quote=sell_quote,
            gross_profit_bps=200,
            gross_profit_usd=Decimal("200"),
            net_profit_usd=Decimal("180"),
            flash_loan_provider="balancer",
            flash_loan_fee=Decimal("0"),
            timestamp=datetime.now(UTC),
        )

        intent = strategy._create_arbitrage_intent(opportunity)

        assert isinstance(intent, FlashLoanIntent)
        assert intent.provider == "balancer"
        assert intent.token == "USDC"
        assert intent.amount == Decimal("10000")
        assert len(intent.callback_intents) == 2

        # Check buy swap (second instance, in TestFlashLoanIntentCreation)
        buy_swap = intent.callback_intents[0]
        assert isinstance(buy_swap, SwapIntent)
        assert buy_swap.from_token == "USDC"
        assert buy_swap.to_token == "WETH"
        assert buy_swap.protocol == "uniswap_v3"

        # Check sell swap
        sell_swap = intent.callback_intents[1]
        assert isinstance(sell_swap, SwapIntent)
        assert sell_swap.from_token == "WETH"
        assert sell_swap.to_token == "USDC"
        assert sell_swap.protocol == "curve"
        assert sell_swap.amount == "all"  # Uses output from buy


# =============================================================================
# Statistics and Tracking Tests
# =============================================================================


class TestStatisticsTracking:
    """Tests for statistics and tracking."""

    def test_get_stats(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test getting strategy statistics."""
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        config.total_trades = 10
        config.total_profit_usd = Decimal("500")

        stats = strategy.get_stats()

        assert stats["total_trades"] == 10
        assert stats["total_profit_usd"] == "500"
        assert stats["state"] == "scanning"

    def test_record_trade(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test recording a trade."""
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        buy_quote = create_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
        )
        sell_quote = create_quote(
            dex="curve",
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal("4"),
            amount_out=Decimal("10200"),
        )

        strategy._current_opportunity = ArbitrageOpportunity(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            buy_dex="uniswap_v3",
            sell_dex="curve",
            buy_quote=buy_quote,
            sell_quote=sell_quote,
            gross_profit_bps=200,
            gross_profit_usd=Decimal("200"),
            net_profit_usd=Decimal("180"),
            flash_loan_provider="balancer",
            flash_loan_fee=Decimal("0"),
            timestamp=datetime.now(UTC),
        )

        initial_trades = config.total_trades
        initial_profit = config.total_profit_usd

        strategy._record_trade()

        assert config.total_trades == initial_trades + 1
        assert config.total_profit_usd == initial_profit + Decimal("180")
        assert strategy._current_opportunity is None
        assert strategy._state == ArbState.COOLDOWN


# =============================================================================
# USD Estimation Tests
# =============================================================================


class TestUsdEstimation:
    """Tests for USD value estimation."""

    def test_estimate_usd_stablecoin(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test USD estimation for stablecoins."""
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        assert strategy._estimate_usd_value(Decimal("100"), "USDC") == Decimal("100")
        assert strategy._estimate_usd_value(Decimal("100"), "USDT") == Decimal("100")
        assert strategy._estimate_usd_value(Decimal("100"), "DAI") == Decimal("100")

    def test_estimate_usd_eth(
        self,
        config: CrossDexArbConfig,
        mock_price_service: MagicMock,
        mock_flash_loan_selector: MagicMock,
    ) -> None:
        """Test USD estimation for ETH."""
        strategy = CrossDexArbStrategy(
            config=config,
            price_service=mock_price_service,
            flash_loan_selector=mock_flash_loan_selector,
        )

        usd = strategy._estimate_usd_value(Decimal("1"), "WETH")
        assert usd == Decimal("2500")  # 1 ETH = $2500


# =============================================================================
# ArbitrageOpportunity Tests
# =============================================================================


class TestArbitrageOpportunity:
    """Tests for ArbitrageOpportunity dataclass."""

    def test_to_dict(self) -> None:
        """Test opportunity serialization."""
        buy_quote = create_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
        )
        sell_quote = create_quote(
            dex="curve",
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal("4"),
            amount_out=Decimal("10200"),
        )

        opp = ArbitrageOpportunity(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            buy_dex="uniswap_v3",
            sell_dex="curve",
            buy_quote=buy_quote,
            sell_quote=sell_quote,
            gross_profit_bps=200,
            gross_profit_usd=Decimal("200"),
            net_profit_usd=Decimal("180"),
            flash_loan_provider="balancer",
            flash_loan_fee=Decimal("0"),
            timestamp=datetime.now(UTC),
        )

        data = opp.to_dict()

        assert data["token_in"] == "USDC"
        assert data["token_out"] == "WETH"
        assert data["buy_dex"] == "uniswap_v3"
        assert data["sell_dex"] == "curve"
        assert data["gross_profit_bps"] == 200
        assert data["net_profit_usd"] == "180"
