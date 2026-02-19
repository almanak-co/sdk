"""Configuration for backtesting data providers.

This module defines the BacktestDataConfig dataclass for controlling backtesting
data provider behavior including strict mode, cache settings, and fallback values.

Key Components:
    - BacktestDataConfig: Configuration for historical data providers

Examples:
    Basic configuration with defaults:

        from almanak.framework.backtesting.config import BacktestDataConfig

        config = BacktestDataConfig()

    Configuration with strict mode (fails if historical data unavailable):

        config = BacktestDataConfig(
            strict_historical_mode=True,
            use_historical_volume=True,
            use_historical_funding=True,
            use_historical_apy=True,
        )

    Configuration with custom fallbacks:

        from decimal import Decimal

        config = BacktestDataConfig(
            volume_fallback_multiplier=Decimal("5"),
            funding_fallback_rate=Decimal("0.0002"),
            supply_apy_fallback=Decimal("0.02"),
            borrow_apy_fallback=Decimal("0.04"),
        )

    Configuration with caching enabled:

        config = BacktestDataConfig(
            enable_persistent_cache=True,
            cache_directory="/path/to/cache",
        )
"""

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Literal


@dataclass
class BacktestDataConfig:
    """Configuration for backtesting data providers.

    Controls backtesting data provider behavior including which historical data
    sources to use, strict mode enforcement, cache settings, rate limiting,
    and fallback values when historical data is unavailable.

    Attributes:
        price_provider: Price data source to use. Options:
            - 'auto': Use fallback chain (Chainlink -> TWAP -> CoinGecko)
            - 'coingecko': Use CoinGecko API only
            - 'chainlink': Use Chainlink on-chain oracles only
            - 'twap': Use Uniswap V3 TWAP only

        use_historical_volume: Whether to fetch historical pool volume from
            subgraphs for LP fee calculations.

        use_historical_funding: Whether to fetch historical funding rates
            from perp protocol APIs for accurate funding P&L.

        use_historical_apy: Whether to fetch historical supply/borrow APY
            from lending protocol subgraphs for interest calculations.

        use_historical_liquidity: Whether to fetch historical liquidity depth
            from subgraphs for slippage modeling.

        strict_historical_mode: When True, raises HistoricalDataUnavailableError
            if historical data is unavailable instead of using fallbacks.
            When False (default), uses fallback values with warnings.

        volume_fallback_multiplier: Multiplier applied to current volume when
            historical volume is unavailable. Used to estimate daily volume
            for LP fee calculations.

        funding_fallback_rate: Hourly funding rate to use when historical
            funding data is unavailable (e.g., 0.0001 = 0.01%/hr).

        supply_apy_fallback: Annual supply APY to use when historical APY
            is unavailable (e.g., 0.03 = 3% APY).

        borrow_apy_fallback: Annual borrow APY to use when historical APY
            is unavailable (e.g., 0.05 = 5% APY).

        coingecko_rate_limit_per_minute: Rate limit for CoinGecko API calls.
            Free tier is 10-30/min, Pro tier is 500/min.

        subgraph_rate_limit_per_minute: Rate limit for The Graph subgraph
            queries. Default 100/min is conservative for free tier.

        enable_persistent_cache: Whether to persist fetched data to disk
            for reuse across backtest runs.

        cache_directory: Directory path for persistent cache storage.
            If None, uses system temp directory.

    Example:
        config = BacktestDataConfig(
            price_provider="auto",
            use_historical_volume=True,
            strict_historical_mode=False,
        )
    """

    # Price provider configuration
    price_provider: Literal["auto", "coingecko", "chainlink", "twap"] = "auto"

    # Historical data source toggles
    use_historical_volume: bool = True
    use_historical_funding: bool = True
    use_historical_apy: bool = True
    use_historical_liquidity: bool = True

    # Strict mode - fail instead of fallback when historical data unavailable
    strict_historical_mode: bool = False

    # Fallback values when historical data is unavailable
    volume_fallback_multiplier: Decimal = Decimal("10")
    funding_fallback_rate: Decimal = Decimal("0.0001")  # 0.01%/hr
    supply_apy_fallback: Decimal = Decimal("0.03")  # 3% APY
    borrow_apy_fallback: Decimal = Decimal("0.05")  # 5% APY
    gas_fallback_gwei: Decimal = Decimal("20")  # 20 gwei default for Ethereum

    # Rate limiting configuration
    coingecko_rate_limit_per_minute: int = 10
    subgraph_rate_limit_per_minute: int = 100

    # Cache configuration
    enable_persistent_cache: bool = False
    cache_directory: str | None = None

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Validate price provider
        valid_providers = ("auto", "coingecko", "chainlink", "twap")
        if self.price_provider not in valid_providers:
            raise ValueError(f"price_provider must be one of {valid_providers}, got '{self.price_provider}'")

        # Validate fallback values are non-negative
        if self.volume_fallback_multiplier < Decimal("0"):
            raise ValueError("volume_fallback_multiplier cannot be negative")
        if self.funding_fallback_rate < Decimal("0"):
            raise ValueError("funding_fallback_rate cannot be negative")
        if self.supply_apy_fallback < Decimal("0"):
            raise ValueError("supply_apy_fallback cannot be negative")
        if self.borrow_apy_fallback < Decimal("0"):
            raise ValueError("borrow_apy_fallback cannot be negative")
        if self.gas_fallback_gwei < Decimal("0"):
            raise ValueError("gas_fallback_gwei cannot be negative")

        # Validate rate limits are positive
        if self.coingecko_rate_limit_per_minute <= 0:
            raise ValueError("coingecko_rate_limit_per_minute must be positive")
        if self.subgraph_rate_limit_per_minute <= 0:
            raise ValueError("subgraph_rate_limit_per_minute must be positive")

        # Validate cache directory exists if specified
        if self.cache_directory is not None:
            cache_path = Path(self.cache_directory)
            if not cache_path.exists():
                # Create the cache directory if it doesn't exist
                cache_path.mkdir(parents=True, exist_ok=True)

    def get_cache_path(self) -> Path | None:
        """Get the cache directory path.

        Returns:
            Path object for cache directory, or None if caching disabled.
        """
        if not self.enable_persistent_cache:
            return None
        if self.cache_directory:
            return Path(self.cache_directory)
        # Use system temp directory as default
        import tempfile

        return Path(tempfile.gettempdir()) / "almanak_backtest_cache"


__all__ = ["BacktestDataConfig"]
