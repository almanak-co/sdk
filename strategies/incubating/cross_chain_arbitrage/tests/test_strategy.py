"""Tests for Cross-Chain Arbitrage Strategy.

Tests cover:
- Configuration fee calculations
- Profitability assessment
- Strategy state machine
- Opportunity detection
- Intent sequence generation
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.cross_chain_arbitrage import (
    ArbState,
    CrossChainArbConfig,
    CrossChainArbitrageStrategy,
    CrossChainOpportunity,
)


class TestCrossChainArbConfig:
    """Tests for CrossChainArbConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = CrossChainArbConfig()

        assert config.chains == ["arbitrum", "optimism", "base"]
        assert config.quote_token == "ETH"
        assert config.base_token == "USDC"
        assert config.min_spread_bps == 50
        assert config.min_spread_after_fees_bps == 10
        assert config.bridge_provider is None
        assert config.account_for_bridge_fees is True
        assert config.cooldown_seconds == 60

    def test_get_bridge_fee_bps_known_provider(self):
        """Test bridge fee retrieval for known providers."""
        config = CrossChainArbConfig()

        assert config.get_bridge_fee_bps("across") == 10
        assert config.get_bridge_fee_bps("stargate") == 15
        assert config.get_bridge_fee_bps("hop") == 20
        assert config.get_bridge_fee_bps("cbridge") == 25
        assert config.get_bridge_fee_bps("synapse") == 30

    def test_get_bridge_fee_bps_unknown_provider(self):
        """Test bridge fee returns default for unknown providers."""
        config = CrossChainArbConfig()

        assert config.get_bridge_fee_bps("unknown") == 50
        assert config.get_bridge_fee_bps(None) == 50

    def test_get_bridge_fee_bps_configured_provider(self):
        """Test bridge fee uses configured provider."""
        config = CrossChainArbConfig(bridge_provider="across")

        assert config.get_bridge_fee_bps() == 10

    def test_get_bridge_latency_seconds(self):
        """Test bridge latency retrieval."""
        config = CrossChainArbConfig()

        assert config.get_bridge_latency_seconds("across") == 120
        assert config.get_bridge_latency_seconds("stargate") == 600
        assert config.get_bridge_latency_seconds("unknown") == 900

    def test_calculate_total_fees_bps(self):
        """Test total fees calculation."""
        config = CrossChainArbConfig(
            max_slippage_swap=Decimal("0.003"),  # 30 bps
            max_slippage_bridge=Decimal("0.005"),  # 50 bps
        )

        # Total = bridge_fee + (swap_slippage * 2) + bridge_slippage
        # Across: 10 + 60 + 50 = 120 bps
        assert config.calculate_total_fees_bps("across") == 120

        # Default: 50 + 60 + 50 = 160 bps
        assert config.calculate_total_fees_bps("unknown") == 160

    def test_calculate_total_fees_bps_no_bridge_fees(self):
        """Test total fees when bridge fees are disabled."""
        config = CrossChainArbConfig(
            account_for_bridge_fees=False,
            max_slippage_swap=Decimal("0.003"),
            max_slippage_bridge=Decimal("0.005"),
        )

        # Total = 0 + 60 + 50 = 110 bps (no bridge fee)
        assert config.calculate_total_fees_bps("across") == 110

    def test_calculate_net_profit_bps(self):
        """Test net profit calculation."""
        config = CrossChainArbConfig(
            max_slippage_swap=Decimal("0.003"),
            max_slippage_bridge=Decimal("0.005"),
        )

        # Raw spread 200 bps - 120 bps fees = 80 bps net
        net = config.calculate_net_profit_bps(200, "across")
        assert net == 80

        # Raw spread 100 bps - 120 bps fees = -20 bps net (loss)
        net = config.calculate_net_profit_bps(100, "across")
        assert net == -20

    def test_is_profitable(self):
        """Test profitability check."""
        config = CrossChainArbConfig(
            min_spread_after_fees_bps=10,
            max_slippage_swap=Decimal("0.003"),
            max_slippage_bridge=Decimal("0.005"),
        )

        # 200 bps raw - 120 bps fees = 80 bps >= 10 bps threshold
        assert config.is_profitable(200, "across") is True

        # 130 bps raw - 120 bps fees = 10 bps >= 10 bps threshold
        assert config.is_profitable(130, "across") is True

        # 120 bps raw - 120 bps fees = 0 bps < 10 bps threshold
        assert config.is_profitable(120, "across") is False

    def test_estimate_profit_usd(self):
        """Test USD profit estimation."""
        config = CrossChainArbConfig(
            max_slippage_swap=Decimal("0.003"),
            max_slippage_bridge=Decimal("0.005"),
            estimated_swap_gas_usd=Decimal("5"),
            estimated_bridge_gas_usd=Decimal("10"),
        )

        # 200 bps on $10,000 = $200 gross
        # Fees: 120 bps = $120
        # Net from spread: $200 - $120 = $80
        # Gas: $5 * 2 + $10 = $20
        # Final: $80 - $20 = $60
        profit = config.estimate_profit_usd(200, Decimal("10000"), "across")

        # Net bps = 200 - 120 = 80
        # Profit from spread = 10000 * 80 / 10000 = $80
        # Minus gas = $80 - $20 = $60
        assert profit == Decimal("60")

    def test_to_dict_and_from_dict(self):
        """Test config serialization/deserialization."""
        original = CrossChainArbConfig(
            strategy_id="test-001",
            chains=["arbitrum", "base"],
            quote_token="WETH",
            min_spread_bps=75,
            bridge_provider="across",
        )

        data = original.to_dict()
        restored = CrossChainArbConfig.from_dict(data)

        assert restored.strategy_id == "test-001"
        assert restored.chains == ["arbitrum", "base"]
        assert restored.quote_token == "WETH"
        assert restored.min_spread_bps == 75
        assert restored.bridge_provider == "across"


class TestCrossChainOpportunity:
    """Tests for CrossChainOpportunity dataclass."""

    def test_opportunity_creation(self):
        """Test opportunity creation."""
        opportunity = CrossChainOpportunity(
            buy_chain="optimism",
            sell_chain="arbitrum",
            token="ETH",
            raw_spread_bps=150,
            net_profit_bps=30,
            estimated_profit_usd=Decimal("15"),
            bridge_provider="across",
            bridge_fee_bps=10,
            bridge_latency_seconds=120,
            buy_price=Decimal("3000"),
            sell_price=Decimal("3045"),
            timestamp=datetime.now(UTC),
        )

        assert opportunity.buy_chain == "optimism"
        assert opportunity.sell_chain == "arbitrum"
        assert opportunity.net_profit_bps == 30

    def test_opportunity_to_dict(self):
        """Test opportunity serialization."""
        timestamp = datetime(2025, 1, 15, 12, 0, 0)
        opportunity = CrossChainOpportunity(
            buy_chain="optimism",
            sell_chain="arbitrum",
            token="ETH",
            raw_spread_bps=150,
            net_profit_bps=30,
            estimated_profit_usd=Decimal("15"),
            bridge_provider="across",
            bridge_fee_bps=10,
            bridge_latency_seconds=120,
            buy_price=Decimal("3000"),
            sell_price=Decimal("3045"),
            timestamp=timestamp,
        )

        data = opportunity.to_dict()

        assert data["buy_chain"] == "optimism"
        assert data["sell_chain"] == "arbitrum"
        assert data["token"] == "ETH"
        assert data["raw_spread_bps"] == 150
        assert data["net_profit_bps"] == 30
        assert data["estimated_profit_usd"] == "15"
        assert data["timestamp"] == "2025-01-15T12:00:00"


class TestCrossChainArbitrageStrategy:
    """Tests for CrossChainArbitrageStrategy."""

    def create_mock_market(
        self,
        prices: dict[str, dict[str, Decimal]],
        price_diffs: dict[tuple[str, str, str], Decimal | None] = None,
    ) -> MagicMock:
        """Create a mock MultiChainMarketSnapshot.

        Args:
            prices: {chain: {token: price}}
            price_diffs: {(token, chain_a, chain_b): spread}
        """
        mock = MagicMock()

        def get_price(token: str, chain: str = None) -> Decimal:
            if chain and chain in prices:
                return prices[chain].get(token, Decimal("0"))
            return Decimal("0")

        def get_price_difference(token: str, chain_a: str = None, chain_b: str = None) -> Decimal | None:
            if price_diffs:
                key = (token, chain_a, chain_b)
                if key in price_diffs:
                    return price_diffs[key]
            # Calculate from prices
            if chain_a is None or chain_b is None:
                return None
            price_a = prices.get(chain_a, {}).get(token)
            price_b = prices.get(chain_b, {}).get(token)
            if price_a and price_b:
                return (price_a - price_b) / min(price_a, price_b)
            return None

        mock.price = MagicMock(side_effect=get_price)
        mock.price_difference = MagicMock(side_effect=get_price_difference)

        return mock

    def test_strategy_initialization(self):
        """Test strategy initialization."""
        config = CrossChainArbConfig(
            strategy_id="test-001",
            wallet_address="0x123",
        )

        strategy = CrossChainArbitrageStrategy(
            config=config,
            chain="arbitrum",
            wallet_address="0x123",
        )

        assert strategy.STRATEGY_NAME == "cross_chain_arbitrage"
        assert strategy.get_state() == ArbState.MONITORING

    def test_decide_when_paused(self):
        """Test strategy returns hold when paused."""
        config = CrossChainArbConfig(pause_strategy=True)
        strategy = CrossChainArbitrageStrategy(config=config)
        mock_market = self.create_mock_market({})

        result = strategy.decide(mock_market)

        assert hasattr(result, "reason")
        assert "paused" in result.reason.lower()

    def test_decide_in_cooldown(self):
        """Test strategy returns hold during cooldown."""
        config = CrossChainArbConfig(cooldown_seconds=60)
        strategy = CrossChainArbitrageStrategy(config=config)
        strategy._last_execution_time = float("inf")  # Force cooldown

        self.create_mock_market({})

        # Need to call _can_trade to properly test
        assert strategy._can_trade() is False

    def test_decide_no_opportunity(self):
        """Test strategy returns hold when no opportunity exists."""
        config = CrossChainArbConfig(
            chains=["arbitrum", "optimism"],
            min_spread_bps=100,  # 1% minimum
        )
        strategy = CrossChainArbitrageStrategy(config=config)

        # Same prices on both chains
        mock_market = self.create_mock_market(
            {
                "arbitrum": {"ETH": Decimal("3000")},
                "optimism": {"ETH": Decimal("3000")},
            }
        )

        result = strategy.decide(mock_market)

        assert hasattr(result, "reason")
        assert "no profitable" in result.reason.lower()

    def test_decide_finds_opportunity(self):
        """Test strategy finds and acts on opportunity."""
        config = CrossChainArbConfig(
            chains=["arbitrum", "optimism"],
            min_spread_bps=50,  # 0.5%
            min_spread_after_fees_bps=5,  # 0.05%
            max_slippage_swap=Decimal("0.001"),  # Reduce fees
            max_slippage_bridge=Decimal("0.001"),
            trade_amount_usd=Decimal("10000"),  # Larger trade for profitability
            bridge_provider="across",  # Lower fee bridge (10 bps vs 50)
            estimated_swap_gas_usd=Decimal("1"),  # Low gas estimate
            estimated_bridge_gas_usd=Decimal("2"),  # Low gas estimate
        )
        strategy = CrossChainArbitrageStrategy(config=config)

        # 2% price difference (200 bps)
        mock_market = self.create_mock_market(
            {
                "arbitrum": {"ETH": Decimal("3060")},
                "optimism": {"ETH": Decimal("3000")},
            }
        )

        result = strategy.decide(mock_market)

        # Should return an intent sequence, not a hold
        # Check it's not a hold intent
        assert not hasattr(result, "reason") or "no profitable" not in result.reason.lower()

    def test_find_best_opportunity(self):
        """Test finding the best opportunity across chains."""
        config = CrossChainArbConfig(
            chains=["arbitrum", "optimism", "base"],
            min_spread_bps=10,
            min_spread_after_fees_bps=5,
            max_slippage_swap=Decimal("0.001"),
            max_slippage_bridge=Decimal("0.001"),
            trade_amount_usd=Decimal("10000"),  # Larger trade for profitability
            bridge_provider="across",  # Lower fee bridge
            estimated_swap_gas_usd=Decimal("1"),  # Low gas estimate
            estimated_bridge_gas_usd=Decimal("2"),  # Low gas estimate
        )
        strategy = CrossChainArbitrageStrategy(config=config)

        # Different spreads on different chain pairs
        mock_market = self.create_mock_market(
            {
                "arbitrum": {"ETH": Decimal("3060")},  # Most expensive
                "optimism": {"ETH": Decimal("3000")},  # Cheapest
                "base": {"ETH": Decimal("3030")},  # Middle
            }
        )

        opportunity = strategy._find_best_opportunity(mock_market)

        # Best opportunity should be optimism -> arbitrum (2% spread)
        assert opportunity is not None
        assert opportunity.buy_chain == "optimism"
        assert opportunity.sell_chain == "arbitrum"

    def test_check_chain_pair_returns_none_for_low_spread(self):
        """Test that low spreads are rejected."""
        config = CrossChainArbConfig(
            min_spread_bps=100,  # 1% minimum
        )
        strategy = CrossChainArbitrageStrategy(config=config)

        # 0.1% spread (below threshold)
        mock_market = self.create_mock_market(
            {
                "arbitrum": {"ETH": Decimal("3003")},
                "optimism": {"ETH": Decimal("3000")},
            }
        )

        opportunity = strategy._check_chain_pair(mock_market, "arbitrum", "optimism", "ETH")

        assert opportunity is None

    def test_get_stats(self):
        """Test strategy statistics retrieval."""
        config = CrossChainArbConfig(
            chains=["arbitrum", "optimism"],
            quote_token="ETH",
            base_token="USDC",
        )
        strategy = CrossChainArbitrageStrategy(config=config)

        stats = strategy.get_stats()

        assert stats["state"] == "monitoring"
        assert stats["total_trades"] == 0
        assert stats["failed_trades"] == 0
        assert stats["total_profit_usd"] == "0"
        assert stats["chains"] == ["arbitrum", "optimism"]
        assert stats["quote_token"] == "ETH"
        assert stats["base_token"] == "USDC"

    def test_calculate_expected_fees(self):
        """Test fee calculation method."""
        config = CrossChainArbConfig(
            max_slippage_swap=Decimal("0.003"),  # 30 bps
            max_slippage_bridge=Decimal("0.005"),  # 50 bps
        )
        strategy = CrossChainArbitrageStrategy(config=config)

        fees = strategy.calculate_expected_fees("across")

        assert fees["bridge_fee_bps"] == 10
        assert fees["swap_slippage_bps"] == 60  # 30 * 2
        assert fees["bridge_slippage_bps"] == 50
        assert fees["total_fees_bps"] == 120

    def test_clear_state(self):
        """Test state clearing."""
        config = CrossChainArbConfig()
        strategy = CrossChainArbitrageStrategy(config=config)

        # Set some state
        strategy._state = ArbState.COOLDOWN
        strategy.config.total_trades = 5
        strategy.config.total_profit_usd = Decimal("100")

        strategy.clear_state()

        assert strategy.get_state() == ArbState.MONITORING
        assert strategy.config.total_trades == 0
        assert strategy.config.total_profit_usd == Decimal("0")

    def test_cooldown_mechanics(self):
        """Test cooldown timing."""
        config = CrossChainArbConfig(cooldown_seconds=60)
        strategy = CrossChainArbitrageStrategy(config=config)

        # No execution yet - can trade
        assert strategy._can_trade() is True
        assert strategy._cooldown_remaining() == 0

        # After execution - in cooldown
        import time

        strategy._last_execution_time = time.time()
        assert strategy._can_trade() is False
        assert strategy._cooldown_remaining() > 0


class TestIntegration:
    """Integration tests for cross-chain arbitrage."""

    def test_end_to_end_opportunity_detection(self):
        """Test full flow from detection to intent generation."""
        config = CrossChainArbConfig(
            chains=["arbitrum", "optimism"],
            quote_token="ETH",
            base_token="USDC",
            min_spread_bps=50,
            min_spread_after_fees_bps=1,
            max_slippage_swap=Decimal("0.001"),
            max_slippage_bridge=Decimal("0.001"),
            trade_amount_usd=Decimal("1000"),
            estimated_swap_gas_usd=Decimal("1"),
            estimated_bridge_gas_usd=Decimal("1"),
            bridge_provider="across",
        )
        strategy = CrossChainArbitrageStrategy(config=config)

        # Create market with 3% spread (well above threshold)
        mock_market = MagicMock()
        mock_market.price.side_effect = lambda token, chain=None: {
            ("ETH", "arbitrum"): Decimal("3090"),
            ("ETH", "optimism"): Decimal("3000"),
        }.get((token, chain), Decimal("0"))

        mock_market.price_difference.side_effect = lambda token, chain_a, chain_b: {
            ("ETH", "arbitrum", "optimism"): Decimal("0.03"),  # 3%
        }.get((token, chain_a, chain_b), None)

        strategy.decide(mock_market)

        # Should execute arbitrage
        # Verify strategy state changed
        assert strategy.config.total_trades == 1

    def test_bridge_provider_selection(self):
        """Test different bridge provider configurations."""
        # Test with Across (fast)
        config_across = CrossChainArbConfig(bridge_provider="across")
        assert config_across.get_bridge_fee_bps() == 10
        assert config_across.get_bridge_latency_seconds() == 120

        # Test with Stargate (higher fee, longer time)
        config_stargate = CrossChainArbConfig(bridge_provider="stargate")
        assert config_stargate.get_bridge_fee_bps() == 15
        assert config_stargate.get_bridge_latency_seconds() == 600

        # Test auto-select (uses default)
        config_auto = CrossChainArbConfig(bridge_provider=None)
        assert config_auto.get_bridge_fee_bps() == 50  # Default


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
