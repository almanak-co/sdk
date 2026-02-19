"""Tests for Lending Rate Arbitrage Strategy.

These tests verify the lending rate arbitrage strategy correctly:
1. Monitors rates across protocols
2. Identifies rebalance opportunities
3. Generates correct intent sequences
4. Respects threshold configurations
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.data.rates import BestRateResult, LendingRate, RateSide
from almanak.framework.intents.vocabulary import HoldIntent, IntentSequence, SupplyIntent, WithdrawIntent
from almanak.framework.strategies import MarketSnapshot, TokenBalance

from ..config import LendingRateArbConfig
from ..strategy import LendingRateArbStrategy, RebalanceOpportunity, TokenPosition


@pytest.fixture
def config() -> LendingRateArbConfig:
    """Create a test configuration."""
    return LendingRateArbConfig(
        strategy_id="test-lending-arb",
        chain="ethereum",
        wallet_address="0xabcdef1234567890abcdef1234567890abcdef12",
        tokens=["USDC", "USDT", "DAI"],
        protocols=["aave_v3", "morpho_blue", "compound_v3"],
        min_spread_bps=50,  # 0.5% minimum spread
        rebalance_threshold_usd=Decimal("100"),
    )


@pytest.fixture
def strategy(config: LendingRateArbConfig) -> LendingRateArbStrategy:
    """Create a test strategy instance."""
    return LendingRateArbStrategy(config=config)


@pytest.fixture
def market_snapshot(strategy: LendingRateArbStrategy) -> MarketSnapshot:
    """Create a market snapshot with test data."""
    market = strategy.create_market_snapshot()
    # Set up basic token balances
    market.set_balance(
        "USDC",
        TokenBalance(
            symbol="USDC",
            balance=Decimal("10000"),
            balance_usd=Decimal("10000"),
        ),
    )
    market.set_balance(
        "USDT",
        TokenBalance(
            symbol="USDT",
            balance=Decimal("5000"),
            balance_usd=Decimal("5000"),
        ),
    )
    return market


def create_mock_lending_rate(
    protocol: str,
    token: str,
    apy_percent: Decimal,
    side: str = "supply",
) -> LendingRate:
    """Create a mock LendingRate for testing."""
    return LendingRate(
        protocol=protocol,
        token=token,
        side=RateSide(side),
        apy_ray=apy_percent * Decimal("1e25"),  # Convert to ray as Decimal
        apy_percent=apy_percent,
        utilization_percent=Decimal("75"),
        timestamp=datetime.now(UTC),
        chain="ethereum",
        market_id=None,
    )


def create_mock_best_rate_result(
    token: str,
    rates: list[tuple[str, Decimal]],
    side: str = "supply",
) -> BestRateResult:
    """Create a mock BestRateResult for testing.

    Args:
        token: Token symbol
        rates: List of (protocol, apy_percent) tuples
        side: supply or borrow

    Returns:
        BestRateResult with best rate determined
    """
    all_rates = [create_mock_lending_rate(proto, token, apy, side) for proto, apy in rates]

    # For supply, best is highest APY
    best = None
    if all_rates:
        if side == "supply":
            best = max(all_rates, key=lambda r: r.apy_percent)
        else:
            best = min(all_rates, key=lambda r: r.apy_percent)

    return BestRateResult(
        token=token,
        side=RateSide(side),
        best_rate=best,
        all_rates=all_rates,
        timestamp=datetime.now(UTC),
    )


class TestLendingRateArbConfig:
    """Tests for LendingRateArbConfig."""

    def test_config_creation(self, config: LendingRateArbConfig) -> None:
        """Test config creation with defaults."""
        assert config.strategy_id == "test-lending-arb"
        assert config.chain == "ethereum"
        assert config.min_spread_bps == 50
        assert "USDC" in config.tokens
        assert "aave_v3" in config.protocols

    def test_config_to_dict(self, config: LendingRateArbConfig) -> None:
        """Test config serialization."""
        data = config.to_dict()
        assert data["strategy_id"] == "test-lending-arb"
        assert data["chain"] == "ethereum"
        assert data["min_spread_bps"] == 50
        assert "tokens" in data
        assert "protocols" in data

    def test_config_from_dict(self) -> None:
        """Test config deserialization."""
        data = {
            "strategy_id": "from-dict-test",
            "chain": "arbitrum",
            "wallet_address": "0x123",
            "tokens": ["USDC", "WETH"],
            "protocols": ["aave_v3", "compound_v3"],
            "min_spread_bps": 100,
            "rebalance_threshold_usd": "500",
        }
        config = LendingRateArbConfig.from_dict(data)
        assert config.strategy_id == "from-dict-test"
        assert config.chain == "arbitrum"
        assert config.min_spread_bps == 100
        assert config.rebalance_threshold_usd == Decimal("500")

    def test_min_spread_update(self, config: LendingRateArbConfig) -> None:
        """Test that min_spread_bps can be updated directly."""
        # min_spread_bps is an int, not a hot-reloadable Decimal field
        config.min_spread_bps = 100
        assert config.min_spread_bps == 100

    def test_hot_reload_rebalance_threshold(self, config: LendingRateArbConfig) -> None:
        """Test that rebalance_threshold_usd can be hot-reloaded."""
        result = config.update(rebalance_threshold_usd=Decimal("500"))
        assert result.success
        assert config.rebalance_threshold_usd == Decimal("500")


class TestLendingRateArbStrategy:
    """Tests for LendingRateArbStrategy."""

    def test_strategy_creation(self, strategy: LendingRateArbStrategy) -> None:
        """Test strategy initialization."""
        # STRATEGY_NAME comes from @almanak_strategy decorator (lowercase)
        assert strategy.STRATEGY_NAME == "lending_rate_arb"
        assert strategy.chain == "ethereum"
        assert strategy._positions == {}

    def test_strategy_metadata(self, strategy: LendingRateArbStrategy) -> None:
        """Test strategy metadata from decorator."""
        metadata = strategy.get_metadata()
        assert metadata is not None
        assert metadata.name == "lending_rate_arb"
        assert "lending" in metadata.tags
        assert "arbitrage" in metadata.tags
        assert "SUPPLY" in metadata.intent_types
        assert "WITHDRAW" in metadata.intent_types

    def test_hold_when_paused(self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot) -> None:
        """Test that strategy returns HOLD when paused."""
        strategy.config.pause_strategy = True
        intent = strategy.decide(market_snapshot)

        assert isinstance(intent, HoldIntent)
        assert intent.reason == "Strategy paused"


class TestOpportunityDetection:
    """Tests for rate opportunity detection."""

    def test_find_opportunity_with_spread(
        self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot
    ) -> None:
        """Test finding opportunity when spread exists."""
        # Set up position in compound_v3
        strategy._positions = {"USDC": {"compound_v3": Decimal("10000")}}

        # Mock rates: Morpho has best rate
        mock_result = create_mock_best_rate_result(
            "USDC",
            [
                ("aave_v3", Decimal("4.0")),
                ("morpho_blue", Decimal("5.5")),
                ("compound_v3", Decimal("3.5")),
            ],
        )

        with patch.object(strategy, "_get_best_rate", return_value=mock_result):
            opportunity = strategy._find_opportunity_for_token(market_snapshot, "USDC")

        assert opportunity is not None
        assert opportunity.token == "USDC"
        assert opportunity.from_protocol == "compound_v3"
        assert opportunity.to_protocol == "morpho_blue"
        assert opportunity.from_apy == Decimal("3.5")
        assert opportunity.to_apy == Decimal("5.5")
        assert opportunity.spread_bps == 200  # 2% = 200 bps

    def test_no_opportunity_when_in_best_protocol(
        self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot
    ) -> None:
        """Test no opportunity when already in best protocol."""
        # Set up position in best protocol
        strategy._positions = {"USDC": {"morpho_blue": Decimal("10000")}}

        mock_result = create_mock_best_rate_result(
            "USDC",
            [
                ("aave_v3", Decimal("4.0")),
                ("morpho_blue", Decimal("5.5")),
                ("compound_v3", Decimal("3.5")),
            ],
        )

        with patch.object(strategy, "_get_best_rate", return_value=mock_result):
            opportunity = strategy._find_opportunity_for_token(market_snapshot, "USDC")

        assert opportunity is None

    def test_new_position_opportunity_from_wallet(
        self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot
    ) -> None:
        """Test finding opportunity for idle wallet balance."""
        # No existing position
        strategy._positions = {}

        mock_result = create_mock_best_rate_result(
            "USDC",
            [
                ("aave_v3", Decimal("4.0")),
                ("morpho_blue", Decimal("5.5")),
                ("compound_v3", Decimal("3.5")),
            ],
        )

        with patch.object(strategy, "_get_best_rate", return_value=mock_result):
            opportunity = strategy._find_new_position_opportunity(market_snapshot, "USDC")

        assert opportunity is not None
        assert opportunity.from_protocol == "wallet"
        assert opportunity.to_protocol == "morpho_blue"
        assert opportunity.from_apy == Decimal("0")
        assert opportunity.to_apy == Decimal("5.5")
        assert opportunity.spread_bps == 550  # 5.5% = 550 bps


class TestRebalanceDecision:
    """Tests for rebalance decision making."""

    def test_hold_when_spread_below_threshold(
        self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot
    ) -> None:
        """Test HOLD when spread is below min_spread_bps."""
        # Use single-token config to simplify test
        strategy.config.tokens = ["USDC"]
        # Set up position
        strategy._positions = {"USDC": {"compound_v3": Decimal("10000")}}
        strategy.config.min_spread_bps = 100  # Require 1% spread

        # Mock rates with only 0.5% spread
        mock_result = create_mock_best_rate_result(
            "USDC",
            [
                ("aave_v3", Decimal("4.0")),
                ("morpho_blue", Decimal("4.5")),  # Only 0.5% better
                ("compound_v3", Decimal("4.0")),
            ],
        )

        with patch.object(strategy, "_get_best_rate", return_value=mock_result):
            intent = strategy.decide(market_snapshot)

        assert isinstance(intent, HoldIntent)
        assert intent.reason is not None
        assert "below threshold" in intent.reason

    def test_hold_when_amount_below_threshold(
        self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot
    ) -> None:
        """Test HOLD when position size is below rebalance threshold."""
        # Use single-token config to simplify test
        strategy.config.tokens = ["USDC"]
        # Set up small position
        strategy._positions = {"USDC": {"compound_v3": Decimal("50")}}  # Only $50
        strategy.config.rebalance_threshold_usd = Decimal("100")  # Require $100
        strategy.config.min_spread_bps = 10  # Low threshold

        mock_result = create_mock_best_rate_result(
            "USDC",
            [
                ("aave_v3", Decimal("4.0")),
                ("morpho_blue", Decimal("5.0")),
                ("compound_v3", Decimal("3.0")),
            ],
        )

        with patch.object(strategy, "_get_best_rate", return_value=mock_result):
            intent = strategy.decide(market_snapshot)

        assert isinstance(intent, HoldIntent)
        assert intent.reason is not None
        assert "below threshold" in intent.reason

    def test_rebalance_intent_sequence(self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot) -> None:
        """Test correct intent sequence generation for rebalance."""
        # Use single-token config to simplify test
        strategy.config.tokens = ["USDC"]
        # Set up position
        strategy._positions = {"USDC": {"compound_v3": Decimal("10000")}}
        strategy.config.min_spread_bps = 50
        strategy.config.rebalance_threshold_usd = Decimal("100")

        mock_result = create_mock_best_rate_result(
            "USDC",
            [
                ("aave_v3", Decimal("4.0")),
                ("morpho_blue", Decimal("5.5")),  # 2% better than compound
                ("compound_v3", Decimal("3.5")),
            ],
        )

        with patch.object(strategy, "_get_best_rate", return_value=mock_result):
            intent = strategy.decide(market_snapshot)

        assert isinstance(intent, IntentSequence)
        assert len(intent.intents) == 2

        # First should be withdraw
        withdraw = intent.intents[0]
        assert isinstance(withdraw, WithdrawIntent)
        assert withdraw.protocol == "compound_v3"
        assert withdraw.token == "USDC"
        assert withdraw.amount == Decimal("10000")

        # Second should be supply
        supply = intent.intents[1]
        assert isinstance(supply, SupplyIntent)
        assert supply.protocol == "morpho_blue"
        assert supply.token == "USDC"
        assert supply.amount == "all"  # Chained from withdraw
        assert supply.use_as_collateral is True  # Required for most protocols


class TestPositionTracking:
    """Tests for position tracking."""

    def test_update_position(self, strategy: LendingRateArbStrategy) -> None:
        """Test position update."""
        strategy.update_position("USDC", "aave_v3", Decimal("5000"))

        positions = strategy.get_positions()
        assert "USDC" in positions
        assert "aave_v3" in positions["USDC"]
        assert positions["USDC"]["aave_v3"] == Decimal("5000")

    def test_update_position_to_zero_removes(self, strategy: LendingRateArbStrategy) -> None:
        """Test that setting position to 0 removes it."""
        strategy.update_position("USDC", "aave_v3", Decimal("5000"))
        strategy.update_position("USDC", "aave_v3", Decimal("0"))

        positions = strategy.get_positions()
        assert "USDC" not in positions or "aave_v3" not in positions.get("USDC", {})

    def test_get_current_position(self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot) -> None:
        """Test getting current position for token."""
        strategy._positions = {"USDC": {"aave_v3": Decimal("5000"), "compound_v3": Decimal("3000")}}

        proto, amount = strategy._get_current_position(market_snapshot, "USDC")

        # Should return largest position
        assert proto == "aave_v3"
        assert amount == Decimal("5000")

    def test_get_current_position_from_config(
        self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot
    ) -> None:
        """Test that config positions take priority."""
        strategy.config.current_positions = {"USDC": {"morpho_blue": Decimal("8000")}}
        strategy._positions = {"USDC": {"aave_v3": Decimal("5000")}}

        proto, amount = strategy._get_current_position(market_snapshot, "USDC")

        # Config should take priority
        assert proto == "morpho_blue"
        assert amount == Decimal("8000")


class TestRatesSnapshot:
    """Tests for rates snapshot functionality."""

    def test_get_rates_snapshot(self, strategy: LendingRateArbStrategy) -> None:
        """Test getting rates snapshot across all tokens."""
        mock_results = {
            "USDC": create_mock_best_rate_result(
                "USDC",
                [
                    ("aave_v3", Decimal("4.0")),
                    ("morpho_blue", Decimal("5.5")),
                ],
            ),
            "USDT": create_mock_best_rate_result(
                "USDT",
                [
                    ("aave_v3", Decimal("3.5")),
                    ("compound_v3", Decimal("4.0")),
                ],
            ),
            "DAI": create_mock_best_rate_result(
                "DAI",
                [
                    ("aave_v3", Decimal("3.0")),
                ],
            ),
        }

        def mock_get_best_rate(token: str, protocols=None) -> BestRateResult:
            return mock_results.get(token, create_mock_best_rate_result(token, []))

        with patch.object(strategy, "_get_best_rate", side_effect=mock_get_best_rate):
            rates = strategy.get_rates_snapshot()

        assert "USDC" in rates
        assert "USDT" in rates
        assert rates["USDC"]["morpho_blue"] == Decimal("5.5")
        assert rates["USDT"]["compound_v3"] == Decimal("4.0")


class TestRebalanceOpportunity:
    """Tests for RebalanceOpportunity dataclass."""

    def test_spread_percent(self) -> None:
        """Test spread_percent property."""
        opp = RebalanceOpportunity(
            token="USDC",
            from_protocol="compound_v3",
            to_protocol="morpho_blue",
            from_apy=Decimal("3.5"),
            to_apy=Decimal("5.5"),
            spread_bps=200,
            amount=Decimal("10000"),
        )

        assert opp.spread_percent == Decimal("2")  # 200 bps = 2%


class TestTokenPosition:
    """Tests for TokenPosition dataclass."""

    def test_token_position_creation(self) -> None:
        """Test TokenPosition creation."""
        pos = TokenPosition(
            token="USDC",
            protocol="aave_v3",
            amount=Decimal("5000"),
            apy_percent=Decimal("4.5"),
        )

        assert pos.token == "USDC"
        assert pos.protocol == "aave_v3"
        assert pos.amount == Decimal("5000")
        assert pos.apy_percent == Decimal("4.5")
        assert pos.last_updated is not None


class TestIntentIntegration:
    """Tests for intent generation and serialization."""

    def test_intent_serialization(self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot) -> None:
        """Test that generated intents can be serialized."""
        # Use single-token config to simplify test
        strategy.config.tokens = ["USDC"]
        strategy._positions = {"USDC": {"compound_v3": Decimal("10000")}}

        mock_result = create_mock_best_rate_result(
            "USDC",
            [
                ("aave_v3", Decimal("4.0")),
                ("morpho_blue", Decimal("5.5")),
                ("compound_v3", Decimal("3.5")),
            ],
        )

        with patch.object(strategy, "_get_best_rate", return_value=mock_result):
            intent = strategy.decide(market_snapshot)

        # Should be able to serialize
        if intent and not isinstance(intent, HoldIntent):
            data = intent.serialize()
            assert "type" in data
            # IntentSequence serializes differently - check for intents field
            if isinstance(intent, IntentSequence):
                assert "intents" in data
            else:
                assert "intent_id" in data

    def test_hold_intent_serialization(self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot) -> None:
        """Test that HoldIntent can be serialized."""
        strategy.config.pause_strategy = True
        intent = strategy.decide(market_snapshot)

        assert isinstance(intent, HoldIntent)
        data = intent.serialize()
        assert data["type"] == "HOLD"
        assert "reason" in data


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_no_rates_available(self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot) -> None:
        """Test handling when no rates are available."""
        empty_result = BestRateResult(
            token="USDC",
            side=RateSide.SUPPLY,
            best_rate=None,
            all_rates=[],
            timestamp=datetime.now(UTC),
        )

        with patch.object(strategy, "_get_best_rate", return_value=empty_result):
            intent = strategy.decide(market_snapshot)

        assert isinstance(intent, HoldIntent)
        assert intent.reason is not None
        assert "No profitable" in intent.reason

    def test_rate_fetch_returns_none(self, strategy: LendingRateArbStrategy, market_snapshot: MarketSnapshot) -> None:
        """Test handling when rate fetch returns None."""
        with patch.object(strategy, "_get_best_rate", return_value=None):
            intent = strategy.decide(market_snapshot)

        # Should gracefully handle and return hold
        assert isinstance(intent, HoldIntent)

    def test_empty_tokens_list(self) -> None:
        """Test with empty tokens list."""
        config = LendingRateArbConfig(
            strategy_id="empty-test",
            chain="ethereum",
            wallet_address="0x123",
            tokens=[],
        )
        strategy = LendingRateArbStrategy(config=config)

        market = strategy.create_market_snapshot()
        intent = strategy.decide(market)

        assert isinstance(intent, HoldIntent)
