"""Tests for MEV (Maximal Extractable Value) simulation module.

Tests cover:
- MEVSimulatorConfig validation and serialization
- MEVSimulationResult serialization
- MEVSimulator sandwich probability calculation
- Token vulnerability classification
- Size vulnerability calculation
- Extraction rate calculation
- Inclusion delay calculation
- Additional slippage application
- Disabled MEV simulation (non-vulnerable intent types)
- Seeded random for reproducibility
- Convenience functions
"""

import logging
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.mev_simulator import (
    HIGH_VOLATILITY_TOKENS,
    LST_TOKENS,
    STABLECOIN_TOKENS,
    MEVSimulationResult,
    MEVSimulator,
    MEVSimulatorConfig,
    get_token_vulnerability,
    simulate_mev_cost,
)

# =============================================================================
# MEVSimulatorConfig Tests
# =============================================================================


class TestMEVSimulatorConfigDefaults:
    """Tests for default configuration values."""

    def test_default_base_sandwich_probability(self) -> None:
        """Default base sandwich probability should be 5%."""
        config = MEVSimulatorConfig()
        assert config.base_sandwich_probability == Decimal("0.05")

    def test_default_max_sandwich_probability(self) -> None:
        """Default max sandwich probability should be 80%."""
        config = MEVSimulatorConfig()
        assert config.max_sandwich_probability == Decimal("0.80")

    def test_default_max_mev_extraction_rate(self) -> None:
        """Default max MEV extraction rate should be 2%."""
        config = MEVSimulatorConfig()
        assert config.max_mev_extraction_rate == Decimal("0.02")

    def test_default_min_trade_size(self) -> None:
        """Default min trade size for MEV should be $1,000."""
        config = MEVSimulatorConfig()
        assert config.min_trade_size_for_mev == Decimal("1000")

    def test_default_large_trade_threshold(self) -> None:
        """Default large trade threshold should be $100,000."""
        config = MEVSimulatorConfig()
        assert config.large_trade_threshold == Decimal("100000")

    def test_default_inclusion_delays(self) -> None:
        """Default inclusion delays should be 1 and 5 blocks."""
        config = MEVSimulatorConfig()
        assert config.base_inclusion_delay == 1
        assert config.max_inclusion_delay == 5

    def test_default_gas_thresholds(self) -> None:
        """Default gas thresholds should be 15 and 50 gwei."""
        config = MEVSimulatorConfig()
        assert config.low_gas_threshold_gwei == Decimal("15")
        assert config.high_gas_threshold_gwei == Decimal("50")

    def test_default_random_seed_none(self) -> None:
        """Default random seed should be None."""
        config = MEVSimulatorConfig()
        assert config.random_seed is None


class TestMEVSimulatorConfigValidation:
    """Tests for configuration validation."""

    def test_invalid_base_probability_negative(self) -> None:
        """Negative base probability should raise ValueError."""
        with pytest.raises(ValueError, match="base_sandwich_probability must be between 0 and 1"):
            MEVSimulatorConfig(base_sandwich_probability=Decimal("-0.1"))

    def test_invalid_base_probability_above_one(self) -> None:
        """Base probability above 1 should raise ValueError."""
        with pytest.raises(ValueError, match="base_sandwich_probability must be between 0 and 1"):
            MEVSimulatorConfig(base_sandwich_probability=Decimal("1.5"))

    def test_invalid_max_probability_negative(self) -> None:
        """Negative max probability should raise ValueError."""
        with pytest.raises(ValueError, match="max_sandwich_probability must be between 0 and 1"):
            MEVSimulatorConfig(max_sandwich_probability=Decimal("-0.1"))

    def test_invalid_base_greater_than_max(self) -> None:
        """Base probability greater than max should raise ValueError."""
        with pytest.raises(ValueError, match="base_sandwich_probability must be <= max_sandwich_probability"):
            MEVSimulatorConfig(
                base_sandwich_probability=Decimal("0.9"),
                max_sandwich_probability=Decimal("0.5"),
            )

    def test_invalid_extraction_rate_negative(self) -> None:
        """Negative extraction rate should raise ValueError."""
        with pytest.raises(ValueError, match="max_mev_extraction_rate must be between 0 and 0.5"):
            MEVSimulatorConfig(max_mev_extraction_rate=Decimal("-0.01"))

    def test_invalid_extraction_rate_above_half(self) -> None:
        """Extraction rate above 50% should raise ValueError."""
        with pytest.raises(ValueError, match="max_mev_extraction_rate must be between 0 and 0.5"):
            MEVSimulatorConfig(max_mev_extraction_rate=Decimal("0.6"))

    def test_invalid_min_trade_size_negative(self) -> None:
        """Negative min trade size should raise ValueError."""
        with pytest.raises(ValueError, match="min_trade_size_for_mev must be non-negative"):
            MEVSimulatorConfig(min_trade_size_for_mev=Decimal("-100"))

    def test_invalid_large_threshold_below_min(self) -> None:
        """Large threshold below min should raise ValueError."""
        with pytest.raises(ValueError, match="large_trade_threshold must be >= min_trade_size_for_mev"):
            MEVSimulatorConfig(
                min_trade_size_for_mev=Decimal("10000"),
                large_trade_threshold=Decimal("5000"),
            )

    def test_invalid_base_delay_negative(self) -> None:
        """Negative base delay should raise ValueError."""
        with pytest.raises(ValueError, match="base_inclusion_delay must be non-negative"):
            MEVSimulatorConfig(base_inclusion_delay=-1)

    def test_invalid_max_delay_below_base(self) -> None:
        """Max delay below base should raise ValueError."""
        with pytest.raises(ValueError, match="max_inclusion_delay must be >= base_inclusion_delay"):
            MEVSimulatorConfig(
                base_inclusion_delay=3,
                max_inclusion_delay=2,
            )

    def test_invalid_low_gas_negative(self) -> None:
        """Negative low gas threshold should raise ValueError."""
        with pytest.raises(ValueError, match="low_gas_threshold_gwei must be non-negative"):
            MEVSimulatorConfig(low_gas_threshold_gwei=Decimal("-5"))

    def test_invalid_high_gas_below_low(self) -> None:
        """High gas below low should raise ValueError."""
        with pytest.raises(ValueError, match="high_gas_threshold_gwei must be >= low_gas_threshold_gwei"):
            MEVSimulatorConfig(
                low_gas_threshold_gwei=Decimal("100"),
                high_gas_threshold_gwei=Decimal("50"),
            )


class TestMEVSimulatorConfigSerialization:
    """Tests for config serialization and deserialization."""

    def test_to_dict_roundtrip(self) -> None:
        """Config should survive to_dict/from_dict roundtrip."""
        config = MEVSimulatorConfig(
            base_sandwich_probability=Decimal("0.1"),
            max_sandwich_probability=Decimal("0.7"),
            max_mev_extraction_rate=Decimal("0.03"),
            min_trade_size_for_mev=Decimal("2000"),
            large_trade_threshold=Decimal("50000"),
            base_inclusion_delay=2,
            max_inclusion_delay=8,
            high_gas_threshold_gwei=Decimal("75"),
            low_gas_threshold_gwei=Decimal("20"),
            random_seed=42,
        )

        data = config.to_dict()
        restored = MEVSimulatorConfig.from_dict(data)

        assert restored.base_sandwich_probability == config.base_sandwich_probability
        assert restored.max_sandwich_probability == config.max_sandwich_probability
        assert restored.max_mev_extraction_rate == config.max_mev_extraction_rate
        assert restored.min_trade_size_for_mev == config.min_trade_size_for_mev
        assert restored.large_trade_threshold == config.large_trade_threshold
        assert restored.base_inclusion_delay == config.base_inclusion_delay
        assert restored.max_inclusion_delay == config.max_inclusion_delay
        assert restored.high_gas_threshold_gwei == config.high_gas_threshold_gwei
        assert restored.low_gas_threshold_gwei == config.low_gas_threshold_gwei
        assert restored.random_seed == config.random_seed

    def test_from_dict_with_defaults(self) -> None:
        """from_dict should use defaults for missing fields."""
        restored = MEVSimulatorConfig.from_dict({})
        default = MEVSimulatorConfig()

        assert restored.base_sandwich_probability == default.base_sandwich_probability
        assert restored.max_sandwich_probability == default.max_sandwich_probability
        assert restored.random_seed is None


# =============================================================================
# MEVSimulationResult Tests
# =============================================================================


class TestMEVSimulationResultSerialization:
    """Tests for result serialization."""

    def test_to_dict_includes_all_fields(self) -> None:
        """to_dict should include all fields."""
        result = MEVSimulationResult(
            is_sandwiched=True,
            mev_cost_usd=Decimal("100.50"),
            additional_slippage_pct=Decimal("0.005"),
            inclusion_delay_blocks=3,
            sandwich_probability=Decimal("0.45"),
            token_vulnerability_factor=Decimal("0.8"),
            size_vulnerability_factor=Decimal("0.6"),
            details={"token_in": "WETH", "token_out": "USDC"},
        )

        data = result.to_dict()

        assert data["is_sandwiched"] is True
        assert data["mev_cost_usd"] == "100.50"
        assert data["additional_slippage_pct"] == "0.005"
        assert data["additional_slippage_bps"] == 50.0  # 0.005 * 10000
        assert data["inclusion_delay_blocks"] == 3
        assert data["sandwich_probability"] == "0.45"
        assert data["sandwich_probability_pct"] == "45.00%"
        assert data["token_vulnerability_factor"] == "0.8"
        assert data["size_vulnerability_factor"] == "0.6"
        assert data["details"]["token_in"] == "WETH"

    def test_to_dict_not_sandwiched(self) -> None:
        """to_dict should work for non-sandwiched trades."""
        result = MEVSimulationResult(
            is_sandwiched=False,
            mev_cost_usd=Decimal("0"),
            additional_slippage_pct=Decimal("0"),
            inclusion_delay_blocks=1,
            sandwich_probability=Decimal("0.10"),
            token_vulnerability_factor=Decimal("0.3"),
            size_vulnerability_factor=Decimal("0.2"),
        )

        data = result.to_dict()

        assert data["is_sandwiched"] is False
        assert data["mev_cost_usd"] == "0"
        assert data["additional_slippage_bps"] == 0.0


# =============================================================================
# Token Vulnerability Tests
# =============================================================================


class TestTokenVulnerability:
    """Tests for token vulnerability classification."""

    def test_both_stablecoins_low_vulnerability(self) -> None:
        """Stablecoin-to-stablecoin swaps should have low vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_token_vulnerability("USDC", "USDT")
        assert vulnerability == Decimal("0.1")

    def test_stablecoin_to_high_volatility(self) -> None:
        """Stablecoin to high volatility should have 0.7 vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_token_vulnerability("USDC", "WETH")
        assert vulnerability == Decimal("0.7")

    def test_high_volatility_to_stablecoin(self) -> None:
        """High volatility to stablecoin should have 0.7 vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_token_vulnerability("WBTC", "DAI")
        assert vulnerability == Decimal("0.7")

    def test_stablecoin_to_lst(self) -> None:
        """Stablecoin to LST should have 0.4 vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_token_vulnerability("USDC", "STETH")
        assert vulnerability == Decimal("0.4")

    def test_stablecoin_to_unknown(self) -> None:
        """Stablecoin to unknown token should have 0.5 vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_token_vulnerability("USDC", "UNKNOWN_TOKEN")
        assert vulnerability == Decimal("0.5")

    def test_both_lst_tokens(self) -> None:
        """LST to LST swaps should have 0.3 vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_token_vulnerability("STETH", "RETH")
        assert vulnerability == Decimal("0.3")

    def test_lst_to_volatile(self) -> None:
        """LST to volatile should have 0.5 vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_token_vulnerability("STETH", "WETH")
        assert vulnerability == Decimal("0.5")

    def test_both_high_volatility(self) -> None:
        """Both high volatility should have max 1.0 vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_token_vulnerability("WETH", "WBTC")
        assert vulnerability == Decimal("1.0")

    def test_one_high_volatility(self) -> None:
        """One high volatility token should have 0.8 vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_token_vulnerability("WETH", "UNKNOWN")
        assert vulnerability == Decimal("0.8")

    def test_unknown_tokens(self) -> None:
        """Unknown tokens should have 0.6 vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_token_vulnerability("TOKEN_A", "TOKEN_B")
        assert vulnerability == Decimal("0.6")

    def test_case_insensitive(self) -> None:
        """Token symbols should be case insensitive."""
        simulator = MEVSimulator()
        # Note: case normalization happens in simulate_mev_cost, not _calculate_token_vulnerability
        # So we test through simulate_mev_cost
        result1 = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("10000"),
            token_in="usdc",
            token_out="weth",
        )
        result2 = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("10000"),
            token_in="USDC",
            token_out="WETH",
        )
        assert result1.token_vulnerability_factor == result2.token_vulnerability_factor


class TestTokenSets:
    """Tests for token classification sets."""

    def test_high_volatility_tokens_contains_expected(self) -> None:
        """HIGH_VOLATILITY_TOKENS should contain expected tokens."""
        assert "WETH" in HIGH_VOLATILITY_TOKENS
        assert "ETH" in HIGH_VOLATILITY_TOKENS
        assert "WBTC" in HIGH_VOLATILITY_TOKENS
        assert "BTC" in HIGH_VOLATILITY_TOKENS
        assert "UNI" in HIGH_VOLATILITY_TOKENS
        assert "AAVE" in HIGH_VOLATILITY_TOKENS
        assert "GMX" in HIGH_VOLATILITY_TOKENS
        assert "ARB" in HIGH_VOLATILITY_TOKENS

    def test_stablecoins_contains_expected(self) -> None:
        """STABLECOIN_TOKENS should contain expected tokens."""
        assert "USDC" in STABLECOIN_TOKENS
        assert "USDT" in STABLECOIN_TOKENS
        assert "DAI" in STABLECOIN_TOKENS
        assert "FRAX" in STABLECOIN_TOKENS
        assert "GHO" in STABLECOIN_TOKENS

    def test_lst_tokens_contains_expected(self) -> None:
        """LST_TOKENS should contain expected tokens."""
        assert "STETH" in LST_TOKENS
        assert "WSTETH" in LST_TOKENS
        assert "RETH" in LST_TOKENS
        assert "CBETH" in LST_TOKENS


# =============================================================================
# Size Vulnerability Tests
# =============================================================================


class TestSizeVulnerability:
    """Tests for size-based vulnerability calculation."""

    def test_below_threshold_zero_vulnerability(self) -> None:
        """Trades below threshold should have zero vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_size_vulnerability(Decimal("500"))
        assert vulnerability == Decimal("0")

    def test_at_threshold_minimal_vulnerability(self) -> None:
        """Trades at threshold should have minimal vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_size_vulnerability(Decimal("1000"))
        assert vulnerability == Decimal("0")  # At min threshold

    def test_above_threshold_nonzero_vulnerability(self) -> None:
        """Trades above threshold should have nonzero vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_size_vulnerability(Decimal("10000"))
        assert vulnerability > Decimal("0")
        assert vulnerability < Decimal("1")

    def test_large_trade_max_vulnerability(self) -> None:
        """Trades at or above large threshold should have max vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_size_vulnerability(Decimal("100000"))
        assert vulnerability == Decimal("1.0")

    def test_above_large_threshold_still_max(self) -> None:
        """Trades above large threshold should still have max vulnerability."""
        simulator = MEVSimulator()
        vulnerability = simulator._calculate_size_vulnerability(Decimal("500000"))
        assert vulnerability == Decimal("1.0")

    def test_sqrt_scaling(self) -> None:
        """Size vulnerability should use square root scaling."""
        simulator = MEVSimulator()
        # At 50% of range, sqrt should give ~0.707
        midpoint = (Decimal("100000") + Decimal("1000")) / 2  # ~50500
        vulnerability = simulator._calculate_size_vulnerability(midpoint)
        # sqrt(0.5) ≈ 0.707
        assert Decimal("0.6") < vulnerability < Decimal("0.8")


# =============================================================================
# Sandwich Probability Tests
# =============================================================================


class TestSandwichProbability:
    """Tests for sandwich attack probability calculation."""

    def test_low_vulnerability_gives_low_probability(self) -> None:
        """Low vulnerability should give probability near base."""
        simulator = MEVSimulator()
        probability = simulator._calculate_sandwich_probability(
            token_vulnerability=Decimal("0.1"),
            size_vulnerability=Decimal("0.1"),
        )
        # Combined vulnerability = 0.3*0.1 + 0.7*0.1 = 0.1
        # Probability = 0.05 + 0.75*0.1 = 0.125
        assert Decimal("0.05") <= probability <= Decimal("0.20")

    def test_high_vulnerability_gives_high_probability(self) -> None:
        """High vulnerability should give probability near max."""
        simulator = MEVSimulator()
        probability = simulator._calculate_sandwich_probability(
            token_vulnerability=Decimal("1.0"),
            size_vulnerability=Decimal("1.0"),
        )
        # Combined = 0.3*1.0 + 0.7*1.0 = 1.0
        # Probability = 0.05 + 0.75*1.0 = 0.80
        assert probability == Decimal("0.80")

    def test_size_has_higher_weight(self) -> None:
        """Size vulnerability should have higher weight than token."""
        simulator = MEVSimulator()
        # High token, low size
        prob1 = simulator._calculate_sandwich_probability(
            token_vulnerability=Decimal("1.0"),
            size_vulnerability=Decimal("0.0"),
        )
        # Low token, high size
        prob2 = simulator._calculate_sandwich_probability(
            token_vulnerability=Decimal("0.0"),
            size_vulnerability=Decimal("1.0"),
        )
        # Size has 0.7 weight, token has 0.3 weight
        assert prob2 > prob1

    def test_probability_never_exceeds_max(self) -> None:
        """Probability should never exceed max_sandwich_probability."""
        config = MEVSimulatorConfig(
            base_sandwich_probability=Decimal("0.5"),
            max_sandwich_probability=Decimal("0.6"),
        )
        simulator = MEVSimulator(config=config)
        probability = simulator._calculate_sandwich_probability(
            token_vulnerability=Decimal("1.0"),
            size_vulnerability=Decimal("1.0"),
        )
        assert probability <= Decimal("0.6")


# =============================================================================
# Extraction Rate Tests
# =============================================================================


class TestExtractionRate:
    """Tests for MEV extraction rate calculation."""

    def test_zero_token_vulnerability_low_extraction(self) -> None:
        """Zero token vulnerability should give low extraction."""
        config = MEVSimulatorConfig(random_seed=42)
        simulator = MEVSimulator(config=config)
        extraction = simulator._calculate_extraction_rate(
            token_vulnerability=Decimal("0"),
            size_vulnerability=Decimal("1.0"),
        )
        assert extraction == Decimal("0")

    def test_max_vulnerability_bounded_by_max_rate(self) -> None:
        """Max vulnerability should be bounded by max extraction rate."""
        config = MEVSimulatorConfig(random_seed=42)
        simulator = MEVSimulator(config=config)
        extraction = simulator._calculate_extraction_rate(
            token_vulnerability=Decimal("1.0"),
            size_vulnerability=Decimal("1.0"),
        )
        assert extraction <= Decimal("0.02")

    def test_extraction_varies_with_randomness(self) -> None:
        """Extraction rate should vary with random factor."""
        # With different seeds, we should get different rates
        extractions = []
        for seed in [1, 2, 3, 4, 5]:
            config = MEVSimulatorConfig(random_seed=seed)
            simulator = MEVSimulator(config=config)
            extraction = simulator._calculate_extraction_rate(
                token_vulnerability=Decimal("0.8"),
                size_vulnerability=Decimal("0.8"),
            )
            extractions.append(extraction)
        # Not all extractions should be identical
        assert len(set(extractions)) > 1


# =============================================================================
# Inclusion Delay Tests
# =============================================================================


class TestInclusionDelay:
    """Tests for transaction inclusion delay calculation."""

    def test_none_gas_returns_base_delay(self) -> None:
        """None gas price should return base delay."""
        simulator = MEVSimulator()
        delay = simulator._calculate_inclusion_delay(None)
        assert delay == 1

    def test_high_gas_returns_base_delay(self) -> None:
        """High gas price should return base delay."""
        simulator = MEVSimulator()
        delay = simulator._calculate_inclusion_delay(Decimal("100"))
        assert delay == 1

    def test_low_gas_returns_max_delay(self) -> None:
        """Low gas price should return max delay."""
        simulator = MEVSimulator()
        delay = simulator._calculate_inclusion_delay(Decimal("5"))
        assert delay == 5

    def test_mid_gas_returns_interpolated_delay(self) -> None:
        """Mid-range gas should return interpolated delay."""
        simulator = MEVSimulator()
        # Midpoint of 15-50 is 32.5
        delay = simulator._calculate_inclusion_delay(Decimal("32.5"))
        assert 1 < delay < 5

    def test_at_low_threshold_returns_max(self) -> None:
        """Gas at low threshold should return max delay."""
        simulator = MEVSimulator()
        delay = simulator._calculate_inclusion_delay(Decimal("15"))
        assert delay == 5

    def test_at_high_threshold_returns_base(self) -> None:
        """Gas at high threshold should return base delay."""
        simulator = MEVSimulator()
        delay = simulator._calculate_inclusion_delay(Decimal("50"))
        assert delay == 1


# =============================================================================
# MEV Simulation Tests
# =============================================================================


class TestMEVSimulation:
    """Tests for full MEV simulation."""

    def test_small_trade_not_sandwiched(self) -> None:
        """Trades below threshold should not be sandwiched."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("500"),
            token_in="WETH",
            token_out="USDC",
        )
        assert result.is_sandwiched is False
        assert result.mev_cost_usd == Decimal("0")
        assert result.sandwich_probability == Decimal("0")
        assert "below MEV threshold" in result.details.get("reason", "")

    def test_large_trade_vulnerability_factors(self) -> None:
        """Large trades should have proper vulnerability factors."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("100000"),
            token_in="WETH",
            token_out="USDC",
        )
        assert result.size_vulnerability_factor == Decimal("1.0")
        assert result.token_vulnerability_factor == Decimal("0.7")
        assert result.sandwich_probability > Decimal("0")

    def test_seeded_simulation_reproducible(self) -> None:
        """Seeded simulations should be reproducible."""
        config = MEVSimulatorConfig(random_seed=42)
        simulator1 = MEVSimulator(config=config)
        result1 = simulator1.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
        )

        simulator2 = MEVSimulator(config=config)
        result2 = simulator2.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
        )

        assert result1.is_sandwiched == result2.is_sandwiched
        assert result1.mev_cost_usd == result2.mev_cost_usd

    def test_sandwiched_trade_has_cost(self) -> None:
        """Sandwiched trade should have MEV cost and slippage."""
        # Use a seed that we know produces a sandwich
        config = MEVSimulatorConfig(
            random_seed=42,
            base_sandwich_probability=Decimal("0.99"),  # Nearly certain
            max_sandwich_probability=Decimal("0.99"),   # Must be >= base
        )
        simulator = MEVSimulator(config=config)
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
        )

        if result.is_sandwiched:
            assert result.mev_cost_usd > Decimal("0")
            assert result.additional_slippage_pct > Decimal("0")
            assert result.additional_slippage_pct <= Decimal("0.02")  # Max extraction rate

    def test_gas_affects_delay(self) -> None:
        """Gas price should affect inclusion delay."""
        simulator = MEVSimulator()

        result_high_gas = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
            gas_price_gwei=Decimal("100"),
        )

        result_low_gas = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
            gas_price_gwei=Decimal("5"),
        )

        assert result_high_gas.inclusion_delay_blocks < result_low_gas.inclusion_delay_blocks


class TestDisabledMEVSimulation:
    """Tests for MEV simulation with non-vulnerable intent types."""

    def test_hold_intent_not_vulnerable(self) -> None:
        """Hold intents should not be MEV vulnerable."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("100000"),
            token_in="WETH",
            token_out="USDC",
            intent_type=IntentType.HOLD,
        )
        assert result.is_sandwiched is False
        assert result.mev_cost_usd == Decimal("0")
        assert "not MEV-vulnerable" in result.details.get("reason", "")

    def test_supply_intent_not_vulnerable(self) -> None:
        """Supply intents should not be MEV vulnerable."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("100000"),
            token_in="USDC",
            intent_type=IntentType.SUPPLY,
        )
        assert result.is_sandwiched is False
        assert "not MEV-vulnerable" in result.details.get("reason", "")

    def test_borrow_intent_not_vulnerable(self) -> None:
        """Borrow intents should not be MEV vulnerable."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_out="USDC",
            intent_type=IntentType.BORROW,
        )
        assert result.is_sandwiched is False

    def test_repay_intent_not_vulnerable(self) -> None:
        """Repay intents should not be MEV vulnerable."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="USDC",
            intent_type=IntentType.REPAY,
        )
        assert result.is_sandwiched is False

    def test_withdraw_intent_not_vulnerable(self) -> None:
        """Withdraw intents should not be MEV vulnerable."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_out="USDC",
            intent_type=IntentType.WITHDRAW,
        )
        assert result.is_sandwiched is False

    def test_swap_intent_is_vulnerable(self) -> None:
        """Swap intents should be MEV vulnerable."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
            intent_type=IntentType.SWAP,
        )
        # Swap should have positive sandwich probability
        assert result.sandwich_probability > Decimal("0")

    def test_lp_open_intent_is_vulnerable(self) -> None:
        """LP open intents should be MEV vulnerable."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
            intent_type=IntentType.LP_OPEN,
        )
        assert result.sandwich_probability > Decimal("0")

    def test_lp_close_intent_is_vulnerable(self) -> None:
        """LP close intents should be MEV vulnerable."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
            intent_type=IntentType.LP_CLOSE,
        )
        assert result.sandwich_probability > Decimal("0")

    def test_perp_open_intent_not_vulnerable(self) -> None:
        """Perp open intents should not be MEV vulnerable (they're on-chain but not AMM-based)."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("100000"),
            intent_type=IntentType.PERP_OPEN,
        )
        assert result.is_sandwiched is False

    def test_perp_close_intent_not_vulnerable(self) -> None:
        """Perp close intents should not be MEV vulnerable."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("100000"),
            intent_type=IntentType.PERP_CLOSE,
        )
        assert result.is_sandwiched is False


# =============================================================================
# MEVSimulator Serialization Tests
# =============================================================================


class TestMEVSimulatorSerialization:
    """Tests for MEVSimulator serialization."""

    def test_to_dict_from_dict_roundtrip(self) -> None:
        """Simulator should survive to_dict/from_dict roundtrip."""
        config = MEVSimulatorConfig(
            base_sandwich_probability=Decimal("0.1"),
            max_mev_extraction_rate=Decimal("0.03"),
            random_seed=123,
        )
        simulator = MEVSimulator(config=config)

        data = simulator.to_dict()
        restored = MEVSimulator.from_dict(data)

        assert restored.config.base_sandwich_probability == Decimal("0.1")
        assert restored.config.max_mev_extraction_rate == Decimal("0.03")
        assert restored.config.random_seed == 123


# =============================================================================
# Convenience Function Tests
# =============================================================================


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_simulate_mev_cost_default_config(self) -> None:
        """simulate_mev_cost should work with default config."""
        result = simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
        )
        assert result is not None
        assert result.sandwich_probability >= Decimal("0")

    def test_simulate_mev_cost_custom_config(self) -> None:
        """simulate_mev_cost should accept custom config."""
        config = MEVSimulatorConfig(
            base_sandwich_probability=Decimal("0.70"),  # High base probability
            max_sandwich_probability=Decimal("0.95"),   # Must be >= base
            random_seed=42,
        )
        result = simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
            config=config,
        )
        # With 70% base probability, should have high probability
        assert result.sandwich_probability > Decimal("0.5")

    def test_get_token_vulnerability_low(self) -> None:
        """get_token_vulnerability should return 'low' for stablecoin pairs."""
        vulnerability = get_token_vulnerability("USDC", "USDT")
        assert vulnerability == "low"

    def test_get_token_vulnerability_medium(self) -> None:
        """get_token_vulnerability should return 'medium' for LST pairs."""
        vulnerability = get_token_vulnerability("STETH", "RETH")
        assert vulnerability == "medium"

    def test_get_token_vulnerability_high(self) -> None:
        """get_token_vulnerability should return 'high' for stable-volatile."""
        vulnerability = get_token_vulnerability("USDC", "WETH")
        assert vulnerability == "high"

    def test_get_token_vulnerability_very_high(self) -> None:
        """get_token_vulnerability should return 'very_high' for volatile pairs."""
        vulnerability = get_token_vulnerability("WETH", "WBTC")
        assert vulnerability == "very_high"

    def test_get_token_vulnerability_case_insensitive(self) -> None:
        """get_token_vulnerability should be case insensitive."""
        v1 = get_token_vulnerability("usdc", "weth")
        v2 = get_token_vulnerability("USDC", "WETH")
        assert v1 == v2


# =============================================================================
# Logging Tests
# =============================================================================


class TestMEVSimulatorLogging:
    """Tests for MEV simulator logging behavior."""

    def test_sandwiched_trade_logs_debug(self, caplog: pytest.LogCaptureFixture) -> None:
        """Sandwiched trades should log debug messages."""
        config = MEVSimulatorConfig(
            random_seed=42,
            base_sandwich_probability=Decimal("0.99"),
            max_sandwich_probability=Decimal("0.99"),  # Must be >= base
        )
        simulator = MEVSimulator(config=config)

        with caplog.at_level(logging.DEBUG, logger="almanak.framework.backtesting.pnl.mev_simulator"):
            # Run multiple times to ensure we get a sandwich
            for _ in range(20):
                result = simulator.simulate_mev_cost(
                    trade_amount_usd=Decimal("50000"),
                    token_in="WETH",
                    token_out="USDC",
                )
                if result.is_sandwiched:
                    break

        # Check if debug message was logged for sandwiched trade
        if result.is_sandwiched:
            assert any("MEV simulation" in record.message for record in caplog.records)


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestMEVSimulatorEdgeCases:
    """Tests for edge cases in MEV simulation."""

    def test_zero_trade_amount(self) -> None:
        """Zero trade amount should not be sandwiched."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("0"),
            token_in="WETH",
            token_out="USDC",
        )
        assert result.is_sandwiched is False

    def test_empty_token_symbols(self) -> None:
        """Empty token symbols should work with default vulnerability."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="",
            token_out="",
        )
        # Should get medium-high vulnerability for unknown tokens
        assert result.token_vulnerability_factor == Decimal("0.6")

    def test_exactly_at_min_threshold(self) -> None:
        """Trade exactly at min threshold should work."""
        # Use a high random seed value that gives random value > sandwich probability
        config = MEVSimulatorConfig(random_seed=999)
        simulator = MEVSimulator(config=config)
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("1000"),
            token_in="WETH",
            token_out="USDC",
        )
        # At min threshold, size vulnerability is 0 (not above threshold)
        assert result.size_vulnerability_factor == Decimal("0")
        # Sandwich probability is still > 0 due to base probability and token vulnerability
        # Whether sandwiched depends on random draw
        assert result.sandwich_probability > Decimal("0")

    def test_extremely_large_trade(self) -> None:
        """Extremely large trades should max out vulnerability."""
        simulator = MEVSimulator()
        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("10000000"),  # $10M
            token_in="WETH",
            token_out="USDC",
        )
        assert result.size_vulnerability_factor == Decimal("1.0")

    def test_custom_gas_thresholds(self) -> None:
        """Custom gas thresholds should work correctly."""
        config = MEVSimulatorConfig(
            low_gas_threshold_gwei=Decimal("5"),
            high_gas_threshold_gwei=Decimal("20"),
        )
        simulator = MEVSimulator(config=config)

        delay_low = simulator._calculate_inclusion_delay(Decimal("5"))
        delay_high = simulator._calculate_inclusion_delay(Decimal("20"))

        assert delay_low == 5
        assert delay_high == 1

    def test_custom_trade_size_thresholds(self) -> None:
        """Custom trade size thresholds should work correctly."""
        config = MEVSimulatorConfig(
            min_trade_size_for_mev=Decimal("5000"),
            large_trade_threshold=Decimal("50000"),
        )
        simulator = MEVSimulator(config=config)

        # Below new threshold
        result_small = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("4000"),
            token_in="WETH",
            token_out="USDC",
        )
        assert result_small.is_sandwiched is False

        # At new large threshold
        result_large = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
        )
        assert result_large.size_vulnerability_factor == Decimal("1.0")
