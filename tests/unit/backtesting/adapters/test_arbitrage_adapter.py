"""Tests for Arbitrage backtest adapter functionality.

This module tests the ArbitrageBacktestAdapter, focusing on:
- Cumulative slippage calculation (multiplicative and additive models)
- Multi-hop execution simulation
- MEV impact simulation
- Configuration validation
- Execution step tracking
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.adapters.arbitrage_adapter import (
    ArbitrageBacktestAdapter,
    ArbitrageBacktestConfig,
    ArbitrageExecutionResult,
    CumulativeSlippageModel,
    ExecutionStep,
)
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPosition,
)

# =============================================================================
# Mock Classes
# =============================================================================


@dataclass
class MockMarketState:
    """Mock market state for testing."""

    prices: dict[str, Decimal] = field(default_factory=dict)

    def get_price(self, token: str) -> Decimal:
        """Get price for a token.

        Raises:
            KeyError: If token not found in prices.
        """
        if token not in self.prices:
            raise KeyError(f"Price not found for {token}")
        return self.prices[token]


@dataclass
class MockSwapIntent:
    """Mock swap intent for testing."""

    from_token: str
    to_token: str
    amount: Decimal | str = Decimal("1000")
    amount_usd: Decimal | None = None
    max_slippage: Decimal = Decimal("0.05")  # 5%
    protocol: str | None = None
    metadata: dict | None = None


def create_spot_position(
    token: str = "USDC",
    amount: Decimal = Decimal("10000"),
    entry_price: Decimal = Decimal("1"),
) -> SimulatedPosition:
    """Create a mock spot position for testing."""
    return SimulatedPosition(
        position_type=PositionType.SPOT,
        protocol="spot",
        tokens=[token],
        amounts={token: amount},
        entry_price=entry_price,
        entry_time=datetime.now(),
    )


# =============================================================================
# ArbitrageBacktestConfig Tests
# =============================================================================


class TestArbitrageBacktestConfig:
    """Tests for ArbitrageBacktestConfig."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = ArbitrageBacktestConfig(strategy_type="arbitrage")

        assert config.strategy_type == "arbitrage"
        assert config.mev_simulation_enabled is False
        assert config.cumulative_slippage_model == "multiplicative"
        assert config.execution_delay_seconds == 1.0
        assert config.max_hops == 5
        assert config.base_slippage_per_hop_pct == Decimal("0.001")
        assert config.base_fee_per_hop_pct == Decimal("0.003")
        assert config.mev_random_seed is None

    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            mev_simulation_enabled=True,
            cumulative_slippage_model="additive",
            execution_delay_seconds=0.5,
            max_hops=3,
            base_slippage_per_hop_pct=Decimal("0.002"),
            base_fee_per_hop_pct=Decimal("0.005"),
            mev_random_seed=42,
        )

        assert config.mev_simulation_enabled is True
        assert config.cumulative_slippage_model == "additive"
        assert config.execution_delay_seconds == 0.5
        assert config.max_hops == 3
        assert config.base_slippage_per_hop_pct == Decimal("0.002")
        assert config.base_fee_per_hop_pct == Decimal("0.005")
        assert config.mev_random_seed == 42

    def test_invalid_strategy_type(self) -> None:
        """Test validation rejects non-arbitrage strategy type."""
        with pytest.raises(ValueError, match="requires strategy_type='arbitrage'"):
            ArbitrageBacktestConfig(strategy_type="lp")

    def test_invalid_slippage_model(self) -> None:
        """Test validation rejects invalid cumulative slippage model."""
        with pytest.raises(ValueError, match="cumulative_slippage_model must be one of"):
            ArbitrageBacktestConfig(
                strategy_type="arbitrage",
                cumulative_slippage_model="invalid",  # type: ignore[arg-type]
            )

    def test_negative_execution_delay(self) -> None:
        """Test validation rejects negative execution delay."""
        with pytest.raises(ValueError, match="execution_delay_seconds must be non-negative"):
            ArbitrageBacktestConfig(
                strategy_type="arbitrage",
                execution_delay_seconds=-1.0,
            )

    def test_invalid_max_hops(self) -> None:
        """Test validation rejects max_hops < 1."""
        with pytest.raises(ValueError, match="max_hops must be at least 1"):
            ArbitrageBacktestConfig(
                strategy_type="arbitrage",
                max_hops=0,
            )

    def test_invalid_slippage_range(self) -> None:
        """Test validation rejects slippage outside 0-1 range."""
        with pytest.raises(ValueError, match="base_slippage_per_hop_pct must be between 0 and 1"):
            ArbitrageBacktestConfig(
                strategy_type="arbitrage",
                base_slippage_per_hop_pct=Decimal("1.5"),
            )

    def test_invalid_fee_range(self) -> None:
        """Test validation rejects fee outside 0-1 range."""
        with pytest.raises(ValueError, match="base_fee_per_hop_pct must be between 0 and 1"):
            ArbitrageBacktestConfig(
                strategy_type="arbitrage",
                base_fee_per_hop_pct=Decimal("-0.1"),
            )

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            mev_simulation_enabled=True,
            cumulative_slippage_model="additive",
            max_hops=4,
        )

        d = config.to_dict()

        assert d["strategy_type"] == "arbitrage"
        assert d["mev_simulation_enabled"] is True
        assert d["cumulative_slippage_model"] == "additive"
        assert d["max_hops"] == 4

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "strategy_type": "arbitrage",
            "mev_simulation_enabled": True,
            "cumulative_slippage_model": "additive",
            "execution_delay_seconds": 0.25,
            "max_hops": 3,
            "base_slippage_per_hop_pct": "0.002",
            "base_fee_per_hop_pct": "0.004",
        }

        config = ArbitrageBacktestConfig.from_dict(data)

        assert config.strategy_type == "arbitrage"
        assert config.mev_simulation_enabled is True
        assert config.cumulative_slippage_model == "additive"
        assert config.execution_delay_seconds == 0.25
        assert config.max_hops == 3
        assert config.base_slippage_per_hop_pct == Decimal("0.002")
        assert config.base_fee_per_hop_pct == Decimal("0.004")

    def test_roundtrip_serialization(self) -> None:
        """Test config survives roundtrip serialization."""
        original = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            mev_simulation_enabled=True,
            cumulative_slippage_model="additive",
            execution_delay_seconds=0.75,
            max_hops=4,
            base_slippage_per_hop_pct=Decimal("0.0015"),
        )

        restored = ArbitrageBacktestConfig.from_dict(original.to_dict())

        assert restored.strategy_type == original.strategy_type
        assert restored.mev_simulation_enabled == original.mev_simulation_enabled
        assert restored.cumulative_slippage_model == original.cumulative_slippage_model
        assert restored.execution_delay_seconds == original.execution_delay_seconds
        assert restored.max_hops == original.max_hops
        assert restored.base_slippage_per_hop_pct == original.base_slippage_per_hop_pct


# =============================================================================
# ExecutionStep Tests
# =============================================================================


class TestExecutionStep:
    """Tests for ExecutionStep dataclass."""

    def test_basic_step(self) -> None:
        """Test basic execution step creation."""
        step = ExecutionStep(
            step_number=1,
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4.9"),
            slippage_pct=Decimal("0.003"),
            fee_pct=Decimal("0.003"),
        )

        assert step.step_number == 1
        assert step.token_in == "USDC"
        assert step.token_out == "WETH"
        assert step.amount_in == Decimal("10000")
        assert step.amount_out == Decimal("4.9")
        assert step.slippage_pct == Decimal("0.003")
        assert step.fee_pct == Decimal("0.003")
        assert step.mev_cost_usd == Decimal("0")

    def test_step_with_mev(self) -> None:
        """Test execution step with MEV cost."""
        step = ExecutionStep(
            step_number=1,
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100000"),
            amount_out=Decimal("49"),
            slippage_pct=Decimal("0.01"),
            fee_pct=Decimal("0.003"),
            mev_cost_usd=Decimal("50"),
            protocol="uniswap_v3",
        )

        assert step.mev_cost_usd == Decimal("50")
        assert step.protocol == "uniswap_v3"

    def test_step_to_dict(self) -> None:
        """Test step serialization."""
        step = ExecutionStep(
            step_number=2,
            token_in="WETH",
            token_out="ARB",
            amount_in=Decimal("4.9"),
            amount_out=Decimal("4800"),
            slippage_pct=Decimal("0.005"),
            fee_pct=Decimal("0.003"),
            mev_cost_usd=Decimal("10"),
            execution_delay_seconds=0.5,
        )

        d = step.to_dict()

        assert d["step_number"] == 2
        assert d["token_in"] == "WETH"
        assert d["token_out"] == "ARB"
        assert d["amount_in"] == "4.9"
        assert d["amount_out"] == "4800"
        assert d["slippage_pct"] == "0.005"
        assert d["slippage_bps"] == 50.0
        assert d["fee_pct"] == "0.003"
        assert d["fee_bps"] == 30.0
        assert d["mev_cost_usd"] == "10"
        assert d["execution_delay_seconds"] == 0.5

    def test_step_from_dict(self) -> None:
        """Test step deserialization."""
        data = {
            "step_number": 3,
            "token_in": "ARB",
            "token_out": "USDC",
            "amount_in": "4800",
            "amount_out": "10100",
            "slippage_pct": "0.004",
            "fee_pct": "0.003",
            "mev_cost_usd": "5",
            "execution_delay_seconds": 1.0,
            "protocol": "sushiswap",
        }

        step = ExecutionStep.from_dict(data)

        assert step.step_number == 3
        assert step.token_in == "ARB"
        assert step.token_out == "USDC"
        assert step.amount_in == Decimal("4800")
        assert step.amount_out == Decimal("10100")
        assert step.slippage_pct == Decimal("0.004")
        assert step.fee_pct == Decimal("0.003")
        assert step.mev_cost_usd == Decimal("5")
        assert step.protocol == "sushiswap"


# =============================================================================
# Cumulative Slippage Model Tests
# =============================================================================


class TestCumulativeSlippageModels:
    """Tests for cumulative slippage calculation models."""

    def test_multiplicative_model_single_hop(self) -> None:
        """Test multiplicative model with single hop."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="multiplicative",
            base_slippage_per_hop_pct=Decimal("0.01"),  # 1%
            base_fee_per_hop_pct=Decimal("0"),  # No fees for cleaner test
        )
        adapter = ArbitrageBacktestAdapter(config)

        hops = [("USDC", "WETH", Decimal("0.01"))]  # 1% slippage
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # For single hop, multiplicative = additive = 1%
        assert result.total_slippage_pct == Decimal("0.01")
        assert result.num_hops == 1
        assert result.execution_model == CumulativeSlippageModel.MULTIPLICATIVE

    def test_multiplicative_model_multi_hop(self) -> None:
        """Test multiplicative model with multiple hops.

        Multiplicative: final = initial * (1-s1) * (1-s2) * (1-s3)
        For 3 hops at 1% each:
        final = 10000 * 0.99 * 0.99 * 0.99 = 10000 * 0.970299 = 9702.99
        total_slippage = 1 - 0.970299 = 0.029701 (2.9701%)
        """
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="multiplicative",
            base_fee_per_hop_pct=Decimal("0"),
            execution_delay_seconds=0,
        )
        adapter = ArbitrageBacktestAdapter(config)

        hops = [
            ("USDC", "WETH", Decimal("0.01")),   # 1%
            ("WETH", "ARB", Decimal("0.01")),    # 1%
            ("ARB", "USDC", Decimal("0.01")),    # 1%
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # Multiplicative: 1 - (0.99)^3 = 1 - 0.970299 = 0.029701
        expected_slippage = Decimal("1") - (Decimal("0.99") ** 3)
        assert abs(result.total_slippage_pct - expected_slippage) < Decimal("0.00001")
        assert result.num_hops == 3

    def test_additive_model_single_hop(self) -> None:
        """Test additive model with single hop."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="additive",
            base_fee_per_hop_pct=Decimal("0"),
        )
        adapter = ArbitrageBacktestAdapter(config)

        hops = [("USDC", "WETH", Decimal("0.01"))]  # 1%
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # For single hop, additive = multiplicative = 1%
        assert result.total_slippage_pct == Decimal("0.01")
        assert result.execution_model == CumulativeSlippageModel.ADDITIVE

    def test_additive_model_multi_hop(self) -> None:
        """Test additive model with multiple hops.

        Additive: total_slippage = s1 + s2 + s3
        For 3 hops at 1% each: 1% + 1% + 1% = 3%
        """
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="additive",
            base_fee_per_hop_pct=Decimal("0"),
            execution_delay_seconds=0,
        )
        adapter = ArbitrageBacktestAdapter(config)

        hops = [
            ("USDC", "WETH", Decimal("0.01")),   # 1%
            ("WETH", "ARB", Decimal("0.01")),    # 1%
            ("ARB", "USDC", Decimal("0.01")),    # 1%
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # Additive: 0.01 + 0.01 + 0.01 = 0.03 (3%)
        assert result.total_slippage_pct == Decimal("0.03")

    def test_multiplicative_less_than_additive(self) -> None:
        """Test that multiplicative slippage is always less than additive for multiple hops."""
        # Create two adapters with different models
        config_mult = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="multiplicative",
            base_fee_per_hop_pct=Decimal("0"),
        )
        config_add = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="additive",
            base_fee_per_hop_pct=Decimal("0"),
        )
        adapter_mult = ArbitrageBacktestAdapter(config_mult)
        adapter_add = ArbitrageBacktestAdapter(config_add)

        # Test with varying slippages
        hops = [
            ("USDC", "WETH", Decimal("0.02")),   # 2%
            ("WETH", "ARB", Decimal("0.015")),   # 1.5%
            ("ARB", "USDC", Decimal("0.01")),    # 1%
        ]

        result_mult = adapter_mult.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )
        result_add = adapter_add.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # Multiplicative should be less than additive
        assert result_mult.total_slippage_pct < result_add.total_slippage_pct
        # Additive = 0.02 + 0.015 + 0.01 = 0.045 (4.5%)
        assert result_add.total_slippage_pct == Decimal("0.045")

    def test_varying_slippage_per_hop(self) -> None:
        """Test cumulative slippage with different slippage per hop."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="multiplicative",
            base_fee_per_hop_pct=Decimal("0"),
        )
        adapter = ArbitrageBacktestAdapter(config)

        hops = [
            ("USDC", "WETH", Decimal("0.005")),  # 0.5% - stablecoin swap
            ("WETH", "ARB", Decimal("0.02")),    # 2% - more volatile pair
            ("ARB", "USDC", Decimal("0.008")),   # 0.8%
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # Multiplicative: 1 - (0.995 * 0.98 * 0.992)
        expected_retention = Decimal("0.995") * Decimal("0.98") * Decimal("0.992")
        expected_slippage = Decimal("1") - expected_retention
        assert abs(result.total_slippage_pct - expected_slippage) < Decimal("0.00001")


# =============================================================================
# Execution Step Tracking Tests
# =============================================================================


class TestExecutionStepTracking:
    """Tests for execution step tracking."""

    def test_steps_recorded_for_each_hop(self) -> None:
        """Test that each hop creates an execution step."""
        adapter = ArbitrageBacktestAdapter()

        hops = [
            ("USDC", "WETH", Decimal("0.003")),
            ("WETH", "ARB", Decimal("0.005")),
            ("ARB", "USDC", Decimal("0.004")),
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        assert len(result.steps) == 3
        assert result.steps[0].step_number == 1
        assert result.steps[1].step_number == 2
        assert result.steps[2].step_number == 3

    def test_step_tokens_match_hops(self) -> None:
        """Test that step tokens match the hop tokens."""
        adapter = ArbitrageBacktestAdapter()

        hops = [
            ("USDC", "WETH", Decimal("0.003")),
            ("WETH", "ARB", Decimal("0.005")),
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        assert result.steps[0].token_in == "USDC"
        assert result.steps[0].token_out == "WETH"
        assert result.steps[1].token_in == "WETH"
        assert result.steps[1].token_out == "ARB"

    def test_step_amounts_chain_correctly(self) -> None:
        """Test that step output amounts chain to next step input."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            base_fee_per_hop_pct=Decimal("0"),  # No fees for cleaner test
        )
        adapter = ArbitrageBacktestAdapter(config)

        hops = [
            ("USDC", "WETH", Decimal("0.01")),
            ("WETH", "ARB", Decimal("0.01")),
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # First step: input 10000, output = 10000 * 0.99 = 9900
        assert result.steps[0].amount_in == Decimal("10000")
        assert result.steps[0].amount_out == Decimal("9900")

        # Second step: input = previous output = 9900
        assert result.steps[1].amount_in == result.steps[0].amount_out

    def test_execution_delay_accumulated(self) -> None:
        """Test that execution delay is accumulated across steps."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            execution_delay_seconds=0.5,
        )
        adapter = ArbitrageBacktestAdapter(config)

        hops = [
            ("USDC", "WETH", Decimal("0.003")),
            ("WETH", "ARB", Decimal("0.005")),
            ("ARB", "USDC", Decimal("0.004")),
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # 3 hops at 0.5s each = 1.5s total
        assert result.total_execution_delay_seconds == 1.5


# =============================================================================
# Integration Tests - Multi-Hop Swap Validation
# =============================================================================


class TestMultiHopSwapIntegration:
    """Integration tests validating multi-hop swap slippage."""

    def test_triangular_arbitrage_path(self) -> None:
        """Test slippage calculation for typical triangular arbitrage.

        Path: USDC -> WETH -> ARB -> USDC
        This is a common arbitrage pattern where the goal is to end up
        with more USDC than started with.
        """
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="multiplicative",
            base_slippage_per_hop_pct=Decimal("0.003"),  # 0.3%
            base_fee_per_hop_pct=Decimal("0.003"),       # 0.3%
        )
        adapter = ArbitrageBacktestAdapter(config)

        hops = [
            ("USDC", "WETH", Decimal("0.003")),  # 0.3%
            ("WETH", "ARB", Decimal("0.005")),   # 0.5%
            ("ARB", "USDC", Decimal("0.004")),   # 0.4%
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
            initial_amount_usd=Decimal("10000"),
        )

        # Verify the execution result structure
        assert result.num_hops == 3
        assert result.initial_amount == Decimal("10000")
        assert result.final_amount < result.initial_amount  # Lost due to slippage/fees

        # Verify total slippage is calculated correctly
        # Multiplicative: 1 - (0.997 * 0.995 * 0.996) = ~1.2%
        expected_retention = Decimal("0.997") * Decimal("0.995") * Decimal("0.996")
        expected_slippage = Decimal("1") - expected_retention
        assert abs(result.total_slippage_pct - expected_slippage) < Decimal("0.0001")

        # Verify total fees
        # 3 hops * 0.3% = 0.9%
        assert result.total_fees_pct == Decimal("0.009")

        # Verify PnL is negative (lost to slippage/fees)
        assert result.profit_loss_pct < 0
        assert not result.is_profitable

    def test_two_hop_arbitrage_profitable(self) -> None:
        """Test 2-hop arbitrage that could be profitable.

        In real arbitrage, the price discrepancy would create profit.
        Here we verify slippage calculation is correct.
        """
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="multiplicative",
            base_fee_per_hop_pct=Decimal("0.001"),  # 0.1% low fees
        )
        adapter = ArbitrageBacktestAdapter(config)

        # Low slippage path
        hops = [
            ("USDC", "WETH", Decimal("0.001")),  # 0.1%
            ("WETH", "USDC", Decimal("0.001")),  # 0.1%
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("100000"),
            initial_amount_usd=Decimal("100000"),
        )

        # Total slippage: 1 - (0.999 * 0.999) = ~0.2%
        expected_slippage = Decimal("1") - (Decimal("0.999") ** 2)
        assert abs(result.total_slippage_pct - expected_slippage) < Decimal("0.00001")

        # Total fees: 0.1% * 2 = 0.2%
        assert result.total_fees_pct == Decimal("0.002")

    def test_high_slippage_multi_hop(self) -> None:
        """Test multi-hop with high slippage (illiquid pools)."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="multiplicative",
            base_fee_per_hop_pct=Decimal("0.01"),  # 1% high fee
        )
        adapter = ArbitrageBacktestAdapter(config)

        hops = [
            ("USDC", "RARE_TOKEN", Decimal("0.05")),   # 5% slippage (illiquid)
            ("RARE_TOKEN", "WETH", Decimal("0.05")),   # 5% slippage
            ("WETH", "USDC", Decimal("0.003")),        # 0.3% (liquid)
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # High slippage should result in significant losses
        # Multiplicative: 1 - (0.95 * 0.95 * 0.997) = ~10%
        expected_retention = Decimal("0.95") * Decimal("0.95") * Decimal("0.997")
        expected_slippage = Decimal("1") - expected_retention
        assert abs(result.total_slippage_pct - expected_slippage) < Decimal("0.0001")

        # Verify high total costs
        assert result.total_slippage_pct > Decimal("0.09")  # > 9%
        assert result.profit_loss_pct < Decimal("-0.10")    # > 10% loss

    def test_flash_loan_arbitrage_path(self) -> None:
        """Test flash loan style arbitrage (large amount, tight margins).

        Flash loans typically involve large amounts with very tight margins
        where slippage is critical.
        """
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="multiplicative",
            base_slippage_per_hop_pct=Decimal("0.0001"),  # 0.01% (very liquid)
            base_fee_per_hop_pct=Decimal("0.0003"),       # 0.03%
            execution_delay_seconds=0.1,  # Fast execution
        )
        adapter = ArbitrageBacktestAdapter(config)

        hops = [
            ("USDC", "WETH", Decimal("0.0001")),
            ("WETH", "DAI", Decimal("0.0001")),
            ("DAI", "USDC", Decimal("0.0001")),
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("1000000"),  # $1M flash loan
            initial_amount_usd=Decimal("1000000"),
        )

        # Very low slippage due to high liquidity
        assert result.total_slippage_pct < Decimal("0.001")  # < 0.1%
        # Total fees: 0.03% * 3 = 0.09%
        assert result.total_fees_pct == Decimal("0.0009")
        # Fast execution
        assert result.total_execution_delay_seconds == pytest.approx(0.3)

    def test_cross_dex_arbitrage(self) -> None:
        """Test cross-DEX arbitrage simulation."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="additive",  # Use additive for conservative estimate
        )
        adapter = ArbitrageBacktestAdapter(config)

        # Buy on Uniswap, sell on Sushiswap
        hops = [
            ("USDC", "WETH", Decimal("0.003")),  # Uniswap
            ("WETH", "USDC", Decimal("0.004")),  # Sushiswap
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("50000"),
        )

        # Additive slippage: 0.3% + 0.4% = 0.7%
        assert result.total_slippage_pct == Decimal("0.007")
        assert result.execution_model == CumulativeSlippageModel.ADDITIVE


# =============================================================================
# Max Hops and Edge Cases Tests
# =============================================================================


class TestMaxHopsAndEdgeCases:
    """Tests for max hops enforcement and edge cases."""

    def test_max_hops_enforced(self) -> None:
        """Test that max_hops limit is enforced."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            max_hops=3,
        )
        adapter = ArbitrageBacktestAdapter(config)

        # Try 5 hops when max is 3
        hops = [
            ("A", "B", Decimal("0.01")),
            ("B", "C", Decimal("0.01")),
            ("C", "D", Decimal("0.01")),
            ("D", "E", Decimal("0.01")),
            ("E", "A", Decimal("0.01")),
        ]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # Should only process 3 hops
        assert result.num_hops == 3
        assert len(result.steps) == 3

    def test_empty_hops_list(self) -> None:
        """Test handling of empty hops list."""
        adapter = ArbitrageBacktestAdapter()

        result = adapter.calculate_cumulative_slippage(
            hops=[],
            initial_amount=Decimal("10000"),
        )

        assert result.num_hops == 0
        assert result.total_slippage_pct == Decimal("0")
        assert result.final_amount == Decimal("10000")

    def test_zero_slippage_hop(self) -> None:
        """Test handling of zero slippage (uses base_slippage_per_hop_pct)."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            base_slippage_per_hop_pct=Decimal("0.002"),
        )
        adapter = ArbitrageBacktestAdapter(config)

        # Provide 0 slippage - should use base
        hops = [("USDC", "WETH", Decimal("0"))]
        result = adapter.calculate_cumulative_slippage(
            hops=hops,
            initial_amount=Decimal("10000"),
        )

        # Should use base slippage of 0.2%
        assert result.steps[0].slippage_pct == Decimal("0.002")

    def test_execution_history_tracked(self) -> None:
        """Test that execution history is tracked across multiple calls."""
        adapter = ArbitrageBacktestAdapter()

        # Execute multiple arbitrages
        for _ in range(3):
            adapter.calculate_cumulative_slippage(
                hops=[("USDC", "WETH", Decimal("0.003"))],
                initial_amount=Decimal("1000"),
            )

        assert len(adapter.execution_history) == 3

    def test_clear_execution_history(self) -> None:
        """Test clearing execution history."""
        adapter = ArbitrageBacktestAdapter()

        # Execute some arbitrages
        adapter.calculate_cumulative_slippage(
            hops=[("USDC", "WETH", Decimal("0.003"))],
            initial_amount=Decimal("1000"),
        )
        assert len(adapter.execution_history) == 1

        # Clear history
        adapter.clear_execution_history()
        assert len(adapter.execution_history) == 0

    def test_execution_stats(self) -> None:
        """Test execution statistics calculation."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            base_fee_per_hop_pct=Decimal("0"),
        )
        adapter = ArbitrageBacktestAdapter(config)

        # Execute a few arbitrages
        adapter.calculate_cumulative_slippage(
            hops=[("USDC", "WETH", Decimal("0.01"))],
            initial_amount=Decimal("1000"),
        )
        adapter.calculate_cumulative_slippage(
            hops=[("A", "B", Decimal("0.02")), ("B", "C", Decimal("0.02"))],
            initial_amount=Decimal("1000"),
        )

        stats = adapter.get_execution_stats()

        assert stats["total_executions"] == 2
        assert stats["total_hops"] == 3  # 1 + 2
        assert stats["avg_hops_per_execution"] == 1.5

    def test_execution_stats_empty(self) -> None:
        """Test execution stats when no executions."""
        adapter = ArbitrageBacktestAdapter()

        stats = adapter.get_execution_stats()

        assert stats["total_executions"] == 0
        assert stats["total_hops"] == 0


# =============================================================================
# Adapter Properties Tests
# =============================================================================


class TestAdapterProperties:
    """Tests for adapter properties and methods."""

    def test_adapter_name(self) -> None:
        """Test adapter name property."""
        adapter = ArbitrageBacktestAdapter()
        assert adapter.adapter_name == "arbitrage"

    def test_config_property(self) -> None:
        """Test config property returns configuration."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            max_hops=4,
        )
        adapter = ArbitrageBacktestAdapter(config)

        assert adapter.config.max_hops == 4

    def test_default_config_when_none(self) -> None:
        """Test that default config is created when None provided."""
        adapter = ArbitrageBacktestAdapter()

        assert adapter.config is not None
        assert adapter.config.strategy_type == "arbitrage"

    def test_to_dict(self) -> None:
        """Test adapter serialization."""
        adapter = ArbitrageBacktestAdapter()
        adapter.calculate_cumulative_slippage(
            hops=[("USDC", "WETH", Decimal("0.003"))],
            initial_amount=Decimal("1000"),
        )

        d = adapter.to_dict()

        assert d["adapter_name"] == "arbitrage"
        assert "config" in d
        assert "execution_stats" in d
        assert d["execution_stats"]["total_executions"] == 1

    def test_update_position_updates_timestamp(self) -> None:
        """Test that update_position updates last_updated timestamp."""
        adapter = ArbitrageBacktestAdapter()
        position = create_spot_position()
        market = MockMarketState(prices={"USDC": Decimal("1")})

        # Store original (may be None)
        adapter.update_position(position, market, elapsed_seconds=3600)

        # last_updated should now be set
        assert position.last_updated is not None

    def test_value_position_spot(self) -> None:
        """Test valuation of spot position."""
        adapter = ArbitrageBacktestAdapter()
        position = create_spot_position(token="ETH", amount=Decimal("5"))
        market = MockMarketState(prices={"ETH": Decimal("2000")})

        value = adapter.value_position(position, market)

        # 5 ETH * $2000 = $10000
        assert value == Decimal("10000")

    def test_value_position_missing_price(self) -> None:
        """Test valuation falls back to entry price when market price unavailable."""
        adapter = ArbitrageBacktestAdapter()
        position = create_spot_position(
            token="RARE",
            amount=Decimal("100"),
            entry_price=Decimal("50"),
        )
        market = MockMarketState(prices={})  # No prices

        value = adapter.value_position(position, market)

        # Uses entry price: 100 * $50 = $5000
        assert value == Decimal("5000")

    def test_should_rebalance_always_false(self) -> None:
        """Test that arbitrage positions never need rebalancing."""
        adapter = ArbitrageBacktestAdapter()
        position = create_spot_position()
        market = MockMarketState(prices={"USDC": Decimal("1")})

        result = adapter.should_rebalance(position, market)

        # Arbitrage positions don't rebalance
        assert result is False


# =============================================================================
# CumulativeSlippageModel Enum Tests
# =============================================================================


class TestCumulativeSlippageModelEnum:
    """Tests for CumulativeSlippageModel enum."""

    def test_multiplicative_value(self) -> None:
        """Test multiplicative enum value."""
        assert CumulativeSlippageModel.MULTIPLICATIVE.value == "multiplicative"

    def test_additive_value(self) -> None:
        """Test additive enum value."""
        assert CumulativeSlippageModel.ADDITIVE.value == "additive"

    def test_enum_from_string(self) -> None:
        """Test creating enum from string value."""
        model = CumulativeSlippageModel("multiplicative")
        assert model == CumulativeSlippageModel.MULTIPLICATIVE

    def test_enum_in_result(self) -> None:
        """Test enum is correctly set in execution result."""
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            cumulative_slippage_model="additive",
        )
        adapter = ArbitrageBacktestAdapter(config)

        result = adapter.calculate_cumulative_slippage(
            hops=[("A", "B", Decimal("0.01"))],
            initial_amount=Decimal("1000"),
        )

        assert result.execution_model == CumulativeSlippageModel.ADDITIVE


# =============================================================================
# ArbitrageExecutionResult Tests
# =============================================================================


class TestArbitrageExecutionResult:
    """Tests for ArbitrageExecutionResult dataclass."""

    def test_num_hops_property(self) -> None:
        """Test num_hops property."""
        result = ArbitrageExecutionResult(
            steps=[
                ExecutionStep(1, "A", "B", Decimal("1000"), Decimal("990"), Decimal("0.01"), Decimal("0")),
                ExecutionStep(2, "B", "C", Decimal("990"), Decimal("980"), Decimal("0.01"), Decimal("0")),
            ],
            total_slippage_pct=Decimal("0.02"),
            total_fees_pct=Decimal("0"),
            total_mev_cost_usd=Decimal("0"),
            total_execution_delay_seconds=2.0,
            initial_amount=Decimal("1000"),
            final_amount=Decimal("980"),
            profit_loss_pct=Decimal("-0.02"),
            execution_model=CumulativeSlippageModel.MULTIPLICATIVE,
        )

        assert result.num_hops == 2

    def test_is_profitable_positive(self) -> None:
        """Test is_profitable with positive PnL."""
        result = ArbitrageExecutionResult(
            steps=[],
            total_slippage_pct=Decimal("0"),
            total_fees_pct=Decimal("0"),
            total_mev_cost_usd=Decimal("0"),
            total_execution_delay_seconds=0,
            initial_amount=Decimal("1000"),
            final_amount=Decimal("1100"),
            profit_loss_pct=Decimal("0.10"),
            execution_model=CumulativeSlippageModel.MULTIPLICATIVE,
        )

        assert result.is_profitable is True

    def test_is_profitable_negative(self) -> None:
        """Test is_profitable with negative PnL."""
        result = ArbitrageExecutionResult(
            steps=[],
            total_slippage_pct=Decimal("0.05"),
            total_fees_pct=Decimal("0.01"),
            total_mev_cost_usd=Decimal("10"),
            total_execution_delay_seconds=3.0,
            initial_amount=Decimal("1000"),
            final_amount=Decimal("950"),
            profit_loss_pct=Decimal("-0.05"),
            execution_model=CumulativeSlippageModel.MULTIPLICATIVE,
        )

        assert result.is_profitable is False

    def test_to_dict(self) -> None:
        """Test result serialization."""
        step = ExecutionStep(1, "USDC", "WETH", Decimal("10000"), Decimal("4.9"), Decimal("0.003"), Decimal("0.003"))
        result = ArbitrageExecutionResult(
            steps=[step],
            total_slippage_pct=Decimal("0.003"),
            total_fees_pct=Decimal("0.003"),
            total_mev_cost_usd=Decimal("5"),
            total_execution_delay_seconds=1.0,
            initial_amount=Decimal("10000"),
            final_amount=Decimal("9940"),
            profit_loss_pct=Decimal("-0.006"),
            execution_model=CumulativeSlippageModel.MULTIPLICATIVE,
        )

        d = result.to_dict()

        assert d["num_hops"] == 1
        assert d["total_slippage_pct"] == "0.003"
        assert d["total_slippage_bps"] == pytest.approx(30.0)
        assert d["is_profitable"] is False
        assert d["execution_model"] == "multiplicative"
        assert len(d["steps"]) == 1
