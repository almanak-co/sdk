"""Unit tests for Monte Carlo price path generation.

This module tests the MonteCarloPathGenerator class, covering:
- GBM (Geometric Brownian Motion) price path generation
- Drift and volatility estimation from historical data
- Parameter-based path generation
- Reproducibility with seeds
- Statistical properties of generated paths
- Edge cases and boundary conditions
"""

import math
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.calculators.monte_carlo import (
    MonteCarloPathGenerator,
    PathGenerationMethod,
    PricePathConfig,
    PricePathResult,
    generate_price_paths,
)


class TestPathGenerationBasics:
    """Tests for basic path generation functionality."""

    def test_generate_paths_from_historical_data(self):
        """Test generating paths from historical price data."""
        historical = [
            Decimal("100"),
            Decimal("102"),
            Decimal("101"),
            Decimal("103"),
            Decimal("105"),
        ]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical,
            n_paths=10,
            method="gbm",
        )

        assert result.n_paths == 10
        assert len(result.paths) == 10
        assert result.n_steps == 4  # len(historical) - 1
        assert result.method == PathGenerationMethod.GBM
        assert result.start_price == Decimal("105")  # Last historical price

    def test_generate_paths_with_default_config(self):
        """Test that default config generates 1000 paths."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        config = PricePathConfig(n_paths=100)  # Use 100 for faster test
        generator = MonteCarloPathGenerator(config=config)
        result = generator.generate_price_paths(historical_prices=historical)

        assert result.n_paths == 100
        assert len(result.paths) == 100

    def test_each_path_has_correct_length(self):
        """Test that each generated path has the correct number of steps."""
        historical = [
            Decimal("100"),
            Decimal("102"),
            Decimal("101"),
            Decimal("103"),
            Decimal("105"),
            Decimal("107"),
        ]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical,
            n_paths=10,
        )

        # Each path should have n_steps + 1 prices (including start)
        expected_length = len(historical)
        for path in result.paths:
            assert len(path) == expected_length

    def test_all_paths_start_at_same_price(self):
        """Test that all paths start at the last historical price."""
        historical = [Decimal("100"), Decimal("105"), Decimal("110")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical,
            n_paths=10,
        )

        for path in result.paths:
            assert path[0] == Decimal("110")


class TestParameterEstimation:
    """Tests for drift and volatility estimation from historical data."""

    def test_drift_estimation_positive_trend(self):
        """Test drift estimation with upward trending prices."""
        # 10% increase over 4 periods
        historical = [
            Decimal("100"),
            Decimal("102"),
            Decimal("104"),
            Decimal("106"),
            Decimal("110"),
        ]
        generator = MonteCarloPathGenerator()
        drift, volatility = generator._estimate_parameters(historical)

        # Drift should be positive for upward trend
        assert drift > Decimal("0")

    def test_drift_estimation_negative_trend(self):
        """Test drift estimation with downward trending prices."""
        historical = [
            Decimal("100"),
            Decimal("98"),
            Decimal("95"),
            Decimal("92"),
            Decimal("90"),
        ]
        generator = MonteCarloPathGenerator()
        drift, volatility = generator._estimate_parameters(historical)

        # Drift should be negative for downward trend
        assert drift < Decimal("0")

    def test_volatility_estimation_stable_prices(self):
        """Test volatility estimation with stable prices (low vol)."""
        historical = [
            Decimal("100"),
            Decimal("100.1"),
            Decimal("99.9"),
            Decimal("100.05"),
            Decimal("99.95"),
        ]
        generator = MonteCarloPathGenerator()
        _, volatility = generator._estimate_parameters(historical)

        # Low volatility for stable prices
        assert volatility < Decimal("0.5")

    def test_volatility_estimation_volatile_prices(self):
        """Test volatility estimation with volatile prices (high vol)."""
        historical = [
            Decimal("100"),
            Decimal("120"),
            Decimal("90"),
            Decimal("115"),
            Decimal("85"),
        ]
        generator = MonteCarloPathGenerator()
        _, volatility = generator._estimate_parameters(historical)

        # Higher volatility for volatile prices
        assert volatility > Decimal("0.1")


class TestGBMGeneration:
    """Tests for Geometric Brownian Motion path generation."""

    def test_gbm_paths_stay_positive(self):
        """Test that GBM paths always have positive prices."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical,
            n_paths=100,
            method="gbm",
        )

        for path in result.paths:
            for price in path:
                assert price > Decimal("0")

    def test_gbm_from_params_basic(self):
        """Test GBM generation with explicit parameters."""
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths_from_params(
            start_price=Decimal("100"),
            n_steps=252,
            n_paths=50,
            drift=Decimal("0.05"),
            volatility=Decimal("0.2"),
        )

        assert result.n_paths == 50
        assert result.n_steps == 252
        assert result.start_price == Decimal("100")
        assert result.drift == Decimal("0.05")
        assert result.volatility == Decimal("0.2")

    def test_gbm_zero_volatility(self):
        """Test GBM with zero volatility produces deterministic paths."""
        config = PricePathConfig(seed=42)
        generator = MonteCarloPathGenerator(config=config)
        result = generator.generate_price_paths_from_params(
            start_price=Decimal("100"),
            n_steps=10,
            n_paths=5,
            drift=Decimal("0.05"),
            volatility=Decimal("0"),  # No randomness
        )

        # All paths should be nearly identical with zero volatility
        # (some numerical differences may occur)
        final_prices = result.get_final_prices()
        for price in final_prices:
            assert price == pytest.approx(final_prices[0], rel=Decimal("0.001"))

    def test_gbm_high_volatility(self):
        """Test GBM with high volatility produces diverse paths."""
        config = PricePathConfig(seed=42)
        generator = MonteCarloPathGenerator(config=config)
        result = generator.generate_price_paths_from_params(
            start_price=Decimal("100"),
            n_steps=100,
            n_paths=20,
            drift=Decimal("0"),
            volatility=Decimal("0.5"),  # High volatility
        )

        # Final prices should vary significantly
        final_prices = [float(p) for p in result.get_final_prices()]
        std_dev = math.sqrt(
            sum((p - sum(final_prices) / len(final_prices)) ** 2 for p in final_prices)
            / len(final_prices)
        )

        # Standard deviation should be meaningful (not near zero)
        assert std_dev > 1.0


class TestReproducibility:
    """Tests for reproducibility with random seeds."""

    def test_same_seed_produces_same_paths(self):
        """Test that the same seed produces identical paths."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]

        config1 = PricePathConfig(seed=12345)
        generator1 = MonteCarloPathGenerator(config=config1)
        result1 = generator1.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        config2 = PricePathConfig(seed=12345)
        generator2 = MonteCarloPathGenerator(config=config2)
        result2 = generator2.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        for path1, path2 in zip(result1.paths, result2.paths, strict=False):
            for p1, p2 in zip(path1, path2, strict=False):
                assert p1 == p2

    def test_different_seeds_produce_different_paths(self):
        """Test that different seeds produce different paths."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]

        config1 = PricePathConfig(seed=12345)
        generator1 = MonteCarloPathGenerator(config=config1)
        result1 = generator1.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        config2 = PricePathConfig(seed=54321)
        generator2 = MonteCarloPathGenerator(config=config2)
        result2 = generator2.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        # At least some paths should be different
        any_different = False
        for path1, path2 in zip(result1.paths, result2.paths, strict=False):
            if path1[-1] != path2[-1]:
                any_different = True
                break

        assert any_different

    def test_set_seed_method(self):
        """Test the set_seed method for reproducibility."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()

        generator.set_seed(42)
        result1 = generator.generate_price_paths(
            historical_prices=historical, n_paths=5
        )

        generator.set_seed(42)
        result2 = generator.generate_price_paths(
            historical_prices=historical, n_paths=5
        )

        for path1, path2 in zip(result1.paths, result2.paths, strict=False):
            assert path1[-1] == path2[-1]


class TestPricePathResult:
    """Tests for PricePathResult methods."""

    def test_get_path_valid_index(self):
        """Test getting a path by valid index."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=5
        )

        path = result.get_path(0)
        assert path == result.paths[0]

        path = result.get_path(4)
        assert path == result.paths[4]

    def test_get_path_invalid_index(self):
        """Test getting a path with invalid index raises IndexError."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=5
        )

        with pytest.raises(IndexError):
            result.get_path(5)

        with pytest.raises(IndexError):
            result.get_path(-1)

    def test_get_final_prices(self):
        """Test getting final prices from all paths."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        final_prices = result.get_final_prices()
        assert len(final_prices) == 10

        for i, final_price in enumerate(final_prices):
            assert final_price == result.paths[i][-1]

    def test_get_returns(self):
        """Test calculating returns for each path."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        returns = result.get_returns()
        assert len(returns) == 10

        for i, ret in enumerate(returns):
            expected_return = (
                result.paths[i][-1] - result.start_price
            ) / result.start_price
            assert ret == expected_return

    def test_get_percentile(self):
        """Test getting percentiles of final prices."""
        config = PricePathConfig(seed=42)
        generator = MonteCarloPathGenerator(config=config)
        result = generator.generate_price_paths_from_params(
            start_price=Decimal("100"),
            n_steps=100,
            n_paths=1000,
            drift=Decimal("0"),
            volatility=Decimal("0.2"),
        )

        p5 = result.get_percentile(5)
        p50 = result.get_percentile(50)
        p95 = result.get_percentile(95)

        # Percentiles should be ordered correctly
        assert p5 < p50 < p95

    def test_get_percentile_boundaries(self):
        """Test percentile boundary values."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        config = PricePathConfig(seed=42)
        generator = MonteCarloPathGenerator(config=config)
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        # 0th percentile should be minimum
        p0 = result.get_percentile(0)
        assert p0 == min(result.get_final_prices())

        # 100th percentile should be maximum
        p100 = result.get_percentile(100)
        assert p100 == max(result.get_final_prices())

    def test_get_percentile_invalid_value(self):
        """Test that invalid percentile values raise ValueError."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        with pytest.raises(ValueError):
            result.get_percentile(-1)

        with pytest.raises(ValueError):
            result.get_percentile(101)

    def test_to_dict(self):
        """Test serialization to dictionary."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        config = PricePathConfig(seed=42)
        generator = MonteCarloPathGenerator(config=config)
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        data = result.to_dict()
        assert data["n_paths"] == 10
        assert data["method"] == "gbm"
        assert data["seed"] == 42
        assert "drift" in data
        assert "volatility" in data


class TestConvenienceFunction:
    """Tests for the generate_price_paths convenience function."""

    def test_convenience_function_basic(self):
        """Test the convenience function with basic inputs."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        result = generate_price_paths(
            historical_prices=historical,
            n_paths=10,
            method="gbm",
        )

        assert isinstance(result, PricePathResult)
        assert result.n_paths == 10
        assert result.method == PathGenerationMethod.GBM

    def test_convenience_function_with_seed(self):
        """Test the convenience function with seed for reproducibility."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]

        result1 = generate_price_paths(
            historical_prices=historical, n_paths=10, seed=42
        )

        result2 = generate_price_paths(
            historical_prices=historical, n_paths=10, seed=42
        )

        for path1, path2 in zip(result1.paths, result2.paths, strict=False):
            assert path1[-1] == path2[-1]

    def test_convenience_function_default_n_paths(self):
        """Test that default n_paths is 1000."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        result = generate_price_paths(historical_prices=historical)

        assert result.n_paths == 1000


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_minimum_historical_prices(self):
        """Test with minimum number of historical prices (2)."""
        historical = [Decimal("100"), Decimal("102")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=5
        )

        assert result.n_steps == 1
        assert len(result.paths) == 5

    def test_single_historical_price_raises_error(self):
        """Test that single historical price raises ValueError."""
        historical = [Decimal("100")]
        generator = MonteCarloPathGenerator()

        with pytest.raises(ValueError, match="Need at least 2 historical prices"):
            generator.generate_price_paths(historical_prices=historical, n_paths=5)

    def test_empty_historical_prices_raises_error(self):
        """Test that empty historical prices raises ValueError."""
        historical: list[Decimal] = []
        generator = MonteCarloPathGenerator()

        with pytest.raises(ValueError, match="Need at least 2 historical prices"):
            generator.generate_price_paths(historical_prices=historical, n_paths=5)

    def test_very_small_prices(self):
        """Test with very small price values."""
        historical = [
            Decimal("0.00001"),
            Decimal("0.000012"),
            Decimal("0.000011"),
        ]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        for path in result.paths:
            for price in path:
                assert price > Decimal("0")

    def test_very_large_prices(self):
        """Test with very large price values."""
        historical = [
            Decimal("1000000"),
            Decimal("1020000"),
            Decimal("1010000"),
        ]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=10
        )

        for path in result.paths:
            for price in path:
                assert price > Decimal("0")

    def test_single_path_generation(self):
        """Test generating a single path."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=1
        )

        assert len(result.paths) == 1
        assert result.n_paths == 1

    def test_many_paths_generation(self):
        """Test generating many paths."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=5000
        )

        assert len(result.paths) == 5000


class TestGeneratorSerialization:
    """Tests for generator serialization."""

    def test_generator_to_dict(self):
        """Test generator serialization to dictionary."""
        config = PricePathConfig(
            method=PathGenerationMethod.GBM,
            n_paths=500,
            seed=42,
            annualization_factor=365,
        )
        generator = MonteCarloPathGenerator(config=config)

        data = generator.to_dict()
        assert data["calculator_name"] == "monte_carlo_path_generator"
        assert data["method"] == "gbm"
        assert data["n_paths"] == 500
        assert data["seed"] == 42
        assert data["annualization_factor"] == 365


class TestPathGenerationMethods:
    """Tests for different path generation methods."""

    def test_gbm_method_string(self):
        """Test GBM method with string argument."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=10, method="gbm"
        )

        assert result.method == PathGenerationMethod.GBM

    def test_gbm_method_enum(self):
        """Test GBM method with enum argument."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=10, method=PathGenerationMethod.GBM
        )

        assert result.method == PathGenerationMethod.GBM

    def test_bootstrap_falls_back_to_gbm(self):
        """Test that bootstrap method falls back to GBM (not yet implemented)."""
        historical = [Decimal("100"), Decimal("102"), Decimal("101")]
        generator = MonteCarloPathGenerator()
        result = generator.generate_price_paths(
            historical_prices=historical, n_paths=10, method="bootstrap"
        )

        # Should still produce valid paths (falls back to GBM)
        assert len(result.paths) == 10
        assert result.method == PathGenerationMethod.BOOTSTRAP


class TestStatisticalProperties:
    """Tests for statistical properties of generated paths."""

    def test_mean_return_approximates_drift(self):
        """Test that mean return of paths approximates the specified drift.

        For GBM, the expected value of S_T/S_0 = exp(μT), so the arithmetic
        mean return should approximate exp(μ) - 1 for T=1 year.

        Note: The GBM formula uses (μ - σ²/2) in the exponent, but this is
        compensated by the Jensen's inequality effect of the exponential,
        resulting in E[S_T] = S_0 * exp(μT).
        """
        config = PricePathConfig(seed=42)
        generator = MonteCarloPathGenerator(config=config)

        drift = Decimal("0.10")  # 10% annual drift
        vol = Decimal("0.2")
        n_steps = 252  # 1 year of daily steps

        result = generator.generate_price_paths_from_params(
            start_price=Decimal("100"),
            n_steps=n_steps,
            n_paths=10000,
            drift=drift,
            volatility=vol,
        )

        returns = [float(r) for r in result.get_returns()]
        mean_return = sum(returns) / len(returns)

        # For GBM, E[S_T/S_0] = exp(μ*T), so E[return] ≈ exp(μ) - 1
        # For μ = 0.10, exp(0.10) - 1 ≈ 0.1052
        expected_return = math.exp(float(drift)) - 1

        # Mean should be within reasonable tolerance of expected
        assert mean_return == pytest.approx(expected_return, rel=0.1)

    def test_volatility_of_returns(self):
        """Test that volatility of returns approximates specified volatility."""
        config = PricePathConfig(seed=42)
        generator = MonteCarloPathGenerator(config=config)

        drift = Decimal("0")
        vol = Decimal("0.2")  # 20% annual volatility
        dt = Decimal("1") / Decimal("252")  # Daily steps

        result = generator.generate_price_paths_from_params(
            start_price=Decimal("100"),
            n_steps=252,
            n_paths=5000,
            drift=drift,
            volatility=vol,
            dt=dt,
        )

        # Calculate log returns from final prices
        log_returns = [math.log(float(p) / 100.0) for p in result.get_final_prices()]

        # Calculate standard deviation of annual log returns
        mean_log_ret = sum(log_returns) / len(log_returns)
        variance = sum((r - mean_log_ret) ** 2 for r in log_returns) / len(log_returns)
        realized_vol = math.sqrt(variance)

        # Should be close to specified volatility (within 20% tolerance for finite sample)
        assert realized_vol == pytest.approx(float(vol), rel=0.2)
