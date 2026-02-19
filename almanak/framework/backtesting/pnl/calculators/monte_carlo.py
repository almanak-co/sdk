"""Monte Carlo price path generator for backtesting.

This module provides tools for generating simulated price paths using various
stochastic models. Monte Carlo simulation enables robust statistical analysis
of strategy performance across many possible market scenarios.

Key Concepts:
    - GBM (Geometric Brownian Motion): Standard model for asset prices
    - Drift (μ): Expected return per unit time
    - Volatility (σ): Standard deviation of returns per unit time
    - Log returns: ln(S_t / S_{t-1}), used for statistical estimation

How GBM Works:
    The GBM model assumes that asset prices follow:
        dS = μS dt + σS dW

    In discrete form for simulation:
        S_{t+dt} = S_t * exp((μ - σ²/2) dt + σ * sqrt(dt) * Z)

    Where Z ~ N(0,1) is a standard normal random variable.

Example:
    from almanak.framework.backtesting.pnl.calculators.monte_carlo import (
        MonteCarloPathGenerator,
        PricePathConfig,
        generate_price_paths,
    )

    # Using historical prices to estimate parameters
    historical = [Decimal("100"), Decimal("102"), Decimal("99"), ...]
    generator = MonteCarloPathGenerator()
    paths = generator.generate_price_paths(
        historical_prices=historical,
        n_paths=1000,
        method="gbm",
    )
    # paths.paths contains 1000 simulated price trajectories

    # Using direct parameter specification
    paths = generator.generate_price_paths_from_params(
        start_price=Decimal("100"),
        n_steps=252,
        n_paths=1000,
        drift=Decimal("0.05"),  # 5% annualized drift
        volatility=Decimal("0.2"),  # 20% annualized volatility
        dt=Decimal("1") / Decimal("252"),  # Daily steps
    )

References:
    - Black-Scholes Model: https://en.wikipedia.org/wiki/Black%E2%80%93Scholes_model
    - Geometric Brownian Motion: https://en.wikipedia.org/wiki/Geometric_Brownian_motion
"""

import math
import random
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any


class PathGenerationMethod(StrEnum):
    """Method for generating price paths.

    Attributes:
        GBM: Geometric Brownian Motion - standard model for asset prices
        BOOTSTRAP: Bootstrap resampling from historical returns
        JUMP_DIFFUSION: GBM with random jumps (Merton model) - future extension
    """

    GBM = "gbm"
    BOOTSTRAP = "bootstrap"
    JUMP_DIFFUSION = "jump_diffusion"


@dataclass
class PricePathConfig:
    """Configuration for price path generation.

    Attributes:
        method: The stochastic model to use (default: GBM)
        n_paths: Number of paths to generate (default: 1000)
        seed: Random seed for reproducibility (None for random)
        annualization_factor: Trading days per year for drift/vol conversion (default: 252)
    """

    method: PathGenerationMethod = PathGenerationMethod.GBM
    n_paths: int = 1000
    seed: int | None = None
    annualization_factor: int = 252


@dataclass
class PricePathResult:
    """Result of price path generation.

    Attributes:
        paths: List of price paths, each path is a list of Decimal prices
        n_paths: Number of paths generated
        n_steps: Number of time steps per path
        method: Method used for generation
        drift: Annualized drift (μ) used/estimated
        volatility: Annualized volatility (σ) used/estimated
        start_price: Starting price for all paths
        dt: Time step size (fraction of year)
        seed: Random seed used (if any)
    """

    paths: list[list[Decimal]]
    n_paths: int
    n_steps: int
    method: PathGenerationMethod
    drift: Decimal
    volatility: Decimal
    start_price: Decimal
    dt: Decimal
    seed: int | None = None

    def get_path(self, index: int) -> list[Decimal]:
        """Get a specific path by index."""
        if index < 0 or index >= len(self.paths):
            raise IndexError(f"Path index {index} out of range [0, {len(self.paths)})")
        return self.paths[index]

    def get_final_prices(self) -> list[Decimal]:
        """Get the final price from each path."""
        return [path[-1] for path in self.paths]

    def get_returns(self) -> list[Decimal]:
        """Get the total return for each path."""
        return [(path[-1] - self.start_price) / self.start_price for path in self.paths]

    def get_percentile(self, percentile: float) -> Decimal:
        """Get the percentile of final prices.

        Args:
            percentile: Percentile value (0-100)

        Returns:
            The price at the given percentile
        """
        if percentile < 0 or percentile > 100:
            raise ValueError("Percentile must be between 0 and 100")

        final_prices = sorted(self.get_final_prices())
        idx = int((percentile / 100) * (len(final_prices) - 1))
        return final_prices[idx]

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "n_paths": self.n_paths,
            "n_steps": self.n_steps,
            "method": self.method.value,
            "drift": str(self.drift),
            "volatility": str(self.volatility),
            "start_price": str(self.start_price),
            "dt": str(self.dt),
            "seed": self.seed,
            # Note: paths omitted for size - use get_path() or serialize separately
            "paths_count": len(self.paths),
        }


@dataclass
class MonteCarloPathGenerator:
    """Generator for Monte Carlo price paths.

    This class provides the primary API for generating simulated price paths
    for Monte Carlo backtesting. It can estimate drift and volatility from
    historical data or use user-specified parameters.

    GBM Formulas:
        Log return: r_t = ln(S_t / S_{t-1})
        Drift estimate: μ = (1/n) * Σ r_t * annualization_factor
        Volatility estimate: σ = std(r_t) * sqrt(annualization_factor)

        Path simulation:
        S_{t+dt} = S_t * exp((μ - σ²/2) dt + σ * sqrt(dt) * Z)

    Attributes:
        config: Configuration for path generation
        _rng: Random number generator instance

    Example:
        generator = MonteCarloPathGenerator()

        # From historical data
        paths = generator.generate_price_paths(
            historical_prices=[Decimal("100"), Decimal("102"), ...],
            n_paths=1000,
            method="gbm",
        )

        # From parameters
        paths = generator.generate_price_paths_from_params(
            start_price=Decimal("100"),
            n_steps=252,
            n_paths=1000,
            drift=Decimal("0.05"),
            volatility=Decimal("0.2"),
        )
    """

    config: PricePathConfig = field(default_factory=PricePathConfig)
    _rng: random.Random = field(default_factory=random.Random, repr=False)

    def __post_init__(self) -> None:
        """Initialize random number generator with seed if provided."""
        if self.config.seed is not None:
            self._rng.seed(self.config.seed)

    def generate_price_paths(
        self,
        historical_prices: list[Decimal],
        n_paths: int | None = None,
        method: str | PathGenerationMethod | None = None,
    ) -> PricePathResult:
        """Generate price paths using parameters estimated from historical data.

        This is the main entry point for generating price paths. It estimates
        drift and volatility from the historical price series and uses them
        to simulate new paths.

        Args:
            historical_prices: List of historical prices (oldest first)
            n_paths: Number of paths to generate (default: from config)
            method: Generation method (default: from config)

        Returns:
            PricePathResult with generated paths and metadata

        Raises:
            ValueError: If historical_prices has fewer than 2 elements

        Example:
            historical = [Decimal("100"), Decimal("102"), Decimal("99"), ...]
            result = generator.generate_price_paths(historical, n_paths=1000)
            print(f"Generated {result.n_paths} paths with vol={result.volatility}")
        """
        if len(historical_prices) < 2:
            raise ValueError("Need at least 2 historical prices to estimate parameters")

        # Parse method
        if method is None:
            parsed_method = self.config.method
        elif isinstance(method, str):
            parsed_method = PathGenerationMethod(method.lower())
        else:
            parsed_method = method

        # Use config defaults if not specified
        paths_count = n_paths if n_paths is not None else self.config.n_paths

        # Estimate drift and volatility from historical data
        drift, volatility = self._estimate_parameters(historical_prices)

        # Generate paths
        start_price = historical_prices[-1]  # Start from most recent price
        n_steps = len(historical_prices) - 1  # Same length as historical

        # Calculate dt assuming daily data by default
        dt = Decimal("1") / Decimal(str(self.config.annualization_factor))

        return self._generate_paths(
            start_price=start_price,
            n_steps=n_steps,
            n_paths=paths_count,
            drift=drift,
            volatility=volatility,
            dt=dt,
            method=parsed_method,
        )

    def generate_price_paths_from_params(
        self,
        start_price: Decimal,
        n_steps: int,
        n_paths: int | None = None,
        drift: Decimal = Decimal("0"),
        volatility: Decimal = Decimal("0.2"),
        dt: Decimal | None = None,
        method: str | PathGenerationMethod | None = None,
    ) -> PricePathResult:
        """Generate price paths using explicitly specified parameters.

        Use this method when you want direct control over the drift and
        volatility parameters rather than estimating them from data.

        Args:
            start_price: Starting price for all paths
            n_steps: Number of time steps per path
            n_paths: Number of paths to generate (default: from config)
            drift: Annualized drift (μ), e.g., 0.05 for 5% annual return
            volatility: Annualized volatility (σ), e.g., 0.2 for 20% annual vol
            dt: Time step size as fraction of year (default: 1/252 for daily)
            method: Generation method (default: from config)

        Returns:
            PricePathResult with generated paths and metadata

        Example:
            result = generator.generate_price_paths_from_params(
                start_price=Decimal("100"),
                n_steps=252,  # One year of daily data
                n_paths=1000,
                drift=Decimal("0.05"),
                volatility=Decimal("0.2"),
            )
        """
        # Parse method
        if method is None:
            parsed_method = self.config.method
        elif isinstance(method, str):
            parsed_method = PathGenerationMethod(method.lower())
        else:
            parsed_method = method

        # Use config defaults if not specified
        paths_count = n_paths if n_paths is not None else self.config.n_paths
        time_step = dt if dt is not None else Decimal("1") / Decimal(str(self.config.annualization_factor))

        return self._generate_paths(
            start_price=start_price,
            n_steps=n_steps,
            n_paths=paths_count,
            drift=drift,
            volatility=volatility,
            dt=time_step,
            method=parsed_method,
        )

    def _estimate_parameters(self, historical_prices: list[Decimal]) -> tuple[Decimal, Decimal]:
        """Estimate drift and volatility from historical prices.

        Uses log returns to estimate annualized drift and volatility.

        Formulas:
            Log return: r_t = ln(S_t / S_{t-1})
            Daily drift: μ_daily = mean(r_t)
            Daily volatility: σ_daily = std(r_t)
            Annualized drift: μ = μ_daily * annualization_factor
            Annualized volatility: σ = σ_daily * sqrt(annualization_factor)

        Args:
            historical_prices: List of historical prices (oldest first)

        Returns:
            Tuple of (annualized_drift, annualized_volatility)
        """
        # Calculate log returns
        log_returns: list[float] = []
        for i in range(1, len(historical_prices)):
            if historical_prices[i - 1] > Decimal("0"):
                ratio = float(historical_prices[i] / historical_prices[i - 1])
                if ratio > 0:
                    log_returns.append(math.log(ratio))

        if len(log_returns) == 0:
            return Decimal("0"), Decimal("0.2")  # Default volatility if no valid returns

        # Calculate mean and standard deviation
        mean_return = sum(log_returns) / len(log_returns)

        if len(log_returns) > 1:
            variance = sum((r - mean_return) ** 2 for r in log_returns) / (len(log_returns) - 1)
            std_return = math.sqrt(variance)
        else:
            std_return = 0.0

        # Annualize
        annualization = self.config.annualization_factor
        drift = Decimal(str(mean_return * annualization))
        volatility = Decimal(str(std_return * math.sqrt(annualization)))

        # Ensure non-negative volatility and reasonable bounds
        volatility = max(volatility, Decimal("0.001"))  # Minimum 0.1% vol

        return drift, volatility

    def _generate_paths(
        self,
        start_price: Decimal,
        n_steps: int,
        n_paths: int,
        drift: Decimal,
        volatility: Decimal,
        dt: Decimal,
        method: PathGenerationMethod,
    ) -> PricePathResult:
        """Generate paths using the specified method.

        Args:
            start_price: Starting price
            n_steps: Number of time steps
            n_paths: Number of paths
            drift: Annualized drift
            volatility: Annualized volatility
            dt: Time step size
            method: Generation method

        Returns:
            PricePathResult with generated paths
        """
        if method == PathGenerationMethod.GBM:
            paths = self._generate_gbm_paths(start_price, n_steps, n_paths, drift, volatility, dt)
        elif method == PathGenerationMethod.BOOTSTRAP:
            # Bootstrap not implemented in this story - fall back to GBM
            paths = self._generate_gbm_paths(start_price, n_steps, n_paths, drift, volatility, dt)
        elif method == PathGenerationMethod.JUMP_DIFFUSION:
            # Jump diffusion not implemented in this story - fall back to GBM
            paths = self._generate_gbm_paths(start_price, n_steps, n_paths, drift, volatility, dt)
        else:
            paths = self._generate_gbm_paths(start_price, n_steps, n_paths, drift, volatility, dt)

        return PricePathResult(
            paths=paths,
            n_paths=n_paths,
            n_steps=n_steps,
            method=method,
            drift=drift,
            volatility=volatility,
            start_price=start_price,
            dt=dt,
            seed=self.config.seed,
        )

    def _generate_gbm_paths(
        self,
        start_price: Decimal,
        n_steps: int,
        n_paths: int,
        drift: Decimal,
        volatility: Decimal,
        dt: Decimal,
    ) -> list[list[Decimal]]:
        """Generate paths using Geometric Brownian Motion.

        GBM Formula (discrete):
            S_{t+dt} = S_t * exp((μ - σ²/2) * dt + σ * sqrt(dt) * Z)

        Where:
            μ = drift (annualized)
            σ = volatility (annualized)
            dt = time step (fraction of year)
            Z ~ N(0,1) = standard normal random variable

        Args:
            start_price: Starting price
            n_steps: Number of time steps
            n_paths: Number of paths
            drift: Annualized drift (μ)
            volatility: Annualized volatility (σ)
            dt: Time step size (fraction of year)

        Returns:
            List of paths, each path is a list of Decimal prices
        """
        paths: list[list[Decimal]] = []

        # Pre-compute constants for efficiency
        dt_float = float(dt)
        drift_float = float(drift)
        vol_float = float(volatility)

        # Drift adjustment: (μ - σ²/2) * dt
        drift_term = (drift_float - 0.5 * vol_float * vol_float) * dt_float

        # Diffusion scaling: σ * sqrt(dt)
        diffusion_scale = vol_float * math.sqrt(dt_float)

        start_price_float = float(start_price)

        for _ in range(n_paths):
            path: list[Decimal] = [start_price]
            current_price = start_price_float

            for _ in range(n_steps):
                # Generate standard normal random variable
                z = self._rng.gauss(0, 1)

                # Calculate log return
                log_return = drift_term + diffusion_scale * z

                # Update price
                current_price = current_price * math.exp(log_return)

                # Ensure price stays positive (numerical safety)
                current_price = max(current_price, 1e-10)

                path.append(Decimal(str(current_price)))

            paths.append(path)

        return paths

    def set_seed(self, seed: int) -> None:
        """Set the random seed for reproducibility.

        Args:
            seed: Random seed value
        """
        self.config.seed = seed
        self._rng.seed(seed)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "calculator_name": "monte_carlo_path_generator",
            "method": self.config.method.value,
            "n_paths": self.config.n_paths,
            "seed": self.config.seed,
            "annualization_factor": self.config.annualization_factor,
        }


def generate_price_paths(
    historical_prices: list[Decimal],
    n_paths: int = 1000,
    method: str = "gbm",
    seed: int | None = None,
) -> PricePathResult:
    """Convenience function to generate price paths.

    This is the main entry point for simple use cases. For more control,
    use MonteCarloPathGenerator directly.

    Args:
        historical_prices: List of historical prices (oldest first)
        n_paths: Number of paths to generate (default: 1000)
        method: Generation method - "gbm", "bootstrap", or "jump_diffusion" (default: "gbm")
        seed: Random seed for reproducibility (default: None)

    Returns:
        PricePathResult with generated paths and metadata

    Example:
        from almanak.framework.backtesting.pnl.calculators.monte_carlo import (
            generate_price_paths,
        )

        historical = [Decimal("100"), Decimal("102"), Decimal("99"), ...]
        result = generate_price_paths(historical, n_paths=1000, method="gbm")

        # Access paths
        path_0 = result.get_path(0)
        final_prices = result.get_final_prices()

        # Get statistics
        median_price = result.get_percentile(50)
        p5_price = result.get_percentile(5)
        p95_price = result.get_percentile(95)
    """
    config = PricePathConfig(
        method=PathGenerationMethod(method.lower()),
        n_paths=n_paths,
        seed=seed,
    )
    generator = MonteCarloPathGenerator(config=config)
    return generator.generate_price_paths(
        historical_prices=historical_prices,
        n_paths=n_paths,
        method=method,
    )


__all__ = [
    "MonteCarloPathGenerator",
    "PathGenerationMethod",
    "PricePathConfig",
    "PricePathResult",
    "generate_price_paths",
]
