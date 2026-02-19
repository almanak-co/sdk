"""Configuration for PnL backtesting.

This module defines the configuration dataclass for PnL backtests,
which controls simulation parameters like time range, initial capital,
fee/slippage models, gas costs, and inclusion delay.

Key Components:
    - PnLBacktestConfig: Main configuration dataclass for backtests

Examples:
    Basic configuration with minimal settings:

        from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
        from datetime import datetime
        from decimal import Decimal

        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
        )

    Custom configuration with fee/slippage models:

        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            initial_capital_usd=Decimal("100000"),
            fee_model="uniswap_v3",
            slippage_model="liquidity_aware",
            include_gas_costs=True,
            gas_price_gwei=Decimal("30"),
            inclusion_delay_blocks=2,
        )

    Institutional mode for production-grade backtests:

        # Institutional mode automatically enforces:
        # - strict_reproducibility=True (audit trails)
        # - allow_degraded_data=False (data quality)
        # - allow_hardcoded_fallback=False (accurate valuations)
        # - require_symbol_mapping=True (clear token identification)
        # - min_data_coverage >= 98% (data coverage guarantee)
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            initial_capital_usd=Decimal("1000000"),
            institutional_mode=True,  # Enable strict requirements
            random_seed=42,  # For reproducibility
        )

        # Check compliance after backtest
        result = await backtester.backtest(strategy, config)
        if not result.institutional_compliance:
            print(f"Compliance issues: {result.compliance_violations}")
"""

import hashlib
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any


@dataclass
class PnLBacktestConfig:
    """Configuration for a PnL backtest simulation.

    Controls all parameters of the backtest including time range, initial
    capital, fee/slippage models, gas costs, and execution delay simulation.

    Attributes:
        start_time: Start of the backtest period (inclusive)
        end_time: End of the backtest period (inclusive)
        interval_seconds: Time between simulation ticks in seconds (default: 3600 = 1 hour)
        initial_capital_usd: Starting capital in USD
        fee_model: Fee model to use - 'realistic', 'zero', or protocol-specific
            (e.g., 'uniswap_v3', 'aave_v3', 'gmx')
        slippage_model: Slippage model to use - 'realistic', 'zero', or protocol-specific
            (e.g., 'liquidity_aware', 'constant')
        include_gas_costs: Whether to include gas costs in PnL calculations
        gas_price_gwei: Gas price to use for cost calculations (default: 30 gwei)
        inclusion_delay_blocks: Number of blocks to delay intent execution to simulate
            realistic trade timing (default: 1). When > 0, intents are queued and
            executed in the next iteration(s) rather than immediately.
        chain: Blockchain to simulate execution on (default: 'arbitrum')
        tokens: List of tokens to track prices for (default: ['WETH', 'USDC'])
        benchmark_token: Token to use for benchmark comparisons (default: 'WETH')
        risk_free_rate: Annual risk-free rate for Sharpe ratio calculation (default: 0.05)
        trading_days_per_year: Number of trading days for annualization (default: 365)
        initial_margin_ratio: Initial margin ratio for opening perp positions (default: 0.1 = 10%)
        maintenance_margin_ratio: Maintenance margin ratio for liquidation (default: 0.05 = 5%)
        mev_simulation_enabled: Enable MEV cost simulation for realistic execution costs (default: False)
        auto_correct_positions: Enable auto-correction of tracked positions when discrepancies are detected
        reconciliation_alert_threshold_pct: Threshold percentage for triggering reconciliation alerts (default: 5%)

    Example:
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            initial_capital_usd=Decimal("10000"),
        )
        print(f"Duration: {config.duration_days:.1f} days")
        print(f"Estimated ticks: {config.estimated_ticks}")
    """

    # Time range configuration
    start_time: datetime
    end_time: datetime
    interval_seconds: int = 3600  # 1 hour default

    # Capital configuration
    initial_capital_usd: Decimal = Decimal("10000")

    # Fee and slippage model configuration
    fee_model: str = "realistic"
    slippage_model: str = "realistic"

    # Gas cost configuration
    include_gas_costs: bool = True
    gas_price_gwei: Decimal = Decimal("30")

    # Execution simulation
    inclusion_delay_blocks: int = 1

    # Chain and token configuration
    chain: str = "arbitrum"
    tokens: list[str] = field(default_factory=lambda: ["WETH", "USDC"])

    # Metrics configuration
    benchmark_token: str = "WETH"
    risk_free_rate: Decimal = Decimal("0.05")  # 5% annual
    trading_days_per_year: int = 365  # Crypto markets trade 24/7

    # Perp margin configuration
    initial_margin_ratio: Decimal = Decimal("0.1")
    """Initial margin ratio required to open a position (default: 0.1 = 10%).
    This is the minimum margin/position_value ratio required to open a new perp position.
    """
    maintenance_margin_ratio: Decimal = Decimal("0.05")
    """Maintenance margin ratio for liquidation threshold (default: 0.05 = 5%).
    When margin/position_value falls below this, the position may be liquidated.
    """

    # MEV simulation configuration
    mev_simulation_enabled: bool = False
    """Enable MEV (Maximal Extractable Value) cost simulation (default: False).
    When enabled, simulates sandwich attack probability and additional slippage
    based on trade size and token characteristics. Adds estimated MEV costs to
    trade records and total MEV cost to backtest metrics.
    """

    # Position reconciliation configuration
    auto_correct_positions: bool = False
    """Enable automatic position correction when discrepancies are detected (default: False).
    When enabled, the reconciliation process will update tracked positions to match
    actual on-chain state when discrepancies exceed the alert threshold. Corrected
    positions will have auto_corrected=True in their ReconciliationEvent.
    """
    reconciliation_alert_threshold_pct: Decimal = Decimal("0.05")
    """Threshold percentage for triggering reconciliation alerts (default: 5%).
    When discrepancy_pct exceeds this threshold, an alert is emitted. Set to 0 to
    alert on any discrepancy, or higher values to only alert on significant drift.
    """

    # Reproducibility configuration
    random_seed: int | None = None
    """Random seed for reproducibility (default: None = no seed).
    When set, any randomness in the backtest (e.g., Monte Carlo simulations,
    random sampling) will use this seed for reproducibility. The seed is
    recorded in the config for re-running identical backtests.
    """

    strict_reproducibility: bool = False
    """Enforce strict reproducibility mode (default: False).

    When enabled, the backtest will raise errors instead of using fallbacks
    that could produce non-deterministic results:

    - Raises ValueError if simulation timestamp is missing (instead of using datetime.now())
    - Raises ValueError if required historical data is unavailable
    - Requires all price sources to provide historical data, not just current prices

    Use this mode when you need byte-identical results across multiple runs
    with the same configuration and random_seed. When disabled, the backtester
    will use reasonable defaults and log warnings instead of failing.
    """

    # Data quality configuration
    staleness_threshold_seconds: int = 3600
    """Threshold in seconds for marking price data as stale (default: 3600 = 1 hour).

    Price data older than this threshold relative to the simulation timestamp
    will be counted as stale in the data quality report. This helps identify
    backtests that may be using outdated price information.

    Set to 0 to disable staleness tracking.
    """

    # Institutional mode configuration
    institutional_mode: bool = False
    """Enable institutional-grade enforcement mode (default: False).

    When enabled, applies stricter data quality requirements suitable for
    institutional trading operations:

    - Fails backtest if data coverage is below min_data_coverage threshold
    - Disables hardcoded price fallbacks (allow_hardcoded_fallback=False)
    - Requires historical price data from verified sources
    - Enforces strict reproducibility (strict_reproducibility=True)

    This mode is designed for production-grade backtests where data quality
    and reproducibility are critical. Use for institutional trading strategies
    or when accurate PnL calculations are required for compliance/reporting.
    """

    min_data_coverage: Decimal = Decimal("0.98")
    """Minimum data coverage ratio required in institutional mode (default: 0.98 = 98%).

    When institutional_mode is enabled, the backtest will fail if the actual
    data coverage ratio (successful price lookups / total lookups) falls
    below this threshold.

    When institutional_mode is disabled, this threshold is only used for
    warnings in the data quality report, not enforcement.

    Valid range: 0.0 to 1.0 (0% to 100%)
    """

    allow_hardcoded_fallback: bool = False
    """Allow hardcoded price fallbacks when price data is unavailable (default: False).

    When disabled (default): The backtester will raise an error if it cannot find
    price data, ensuring that all valuations use actual market prices. This is
    the institutional-grade setting for production backtests.

    When enabled: The backtester may use hardcoded fallback prices for tokens
    when historical price data is unavailable. This can mask data quality issues
    and should only be used for development/testing where price accuracy is not
    critical.

    Note: This is automatically set to False when institutional_mode=True
    in __post_init__, as institutional-grade backtests should never use
    arbitrary hardcoded prices.

    Environment variable: Set ALMANAK_ALLOW_HARDCODED_PRICES=1 to override
    for testing scenarios where you need relaxed defaults.
    """

    allow_degraded_data: bool = True
    """Allow backtests to proceed with degraded or incomplete data (default: True).

    When enabled, the backtester will continue execution even when:
    - Some price data is missing or interpolated
    - Data sources return stale information
    - Historical data has gaps

    When disabled, the backtester will fail fast if data quality issues are
    detected, ensuring only high-quality data is used for analysis.

    Note: This is automatically set to False when institutional_mode=True
    in __post_init__, as institutional-grade backtests require complete data.
    """

    require_symbol_mapping: bool = False
    """Require all token addresses to be resolved to symbols (default: False).

    When enabled, the backtester will fail if any token address cannot be
    resolved to a human-readable symbol. This ensures all trade records and
    reports use consistent, recognizable token names.

    When disabled, unresolved token addresses are used as-is (checksummed),
    which may make reports harder to read and audit.

    Note: This is automatically set to True when institutional_mode=True
    in __post_init__, as institutional-grade backtests require clear symbol
    identification for compliance and reporting purposes.
    """

    # Historical gas pricing configuration
    use_historical_gas_prices: bool = False
    """Use historical gas prices for accurate gas cost simulation (default: False).

    When enabled, the backtester will attempt to fetch historical ETH prices
    at each simulation timestamp to calculate gas costs more accurately.
    This provides realistic gas cost estimates that reflect market conditions
    at the time of simulated trades.

    When disabled, gas costs use the current ETH price or gas_eth_price_override
    if specified. This is faster but less accurate for historical backtests.

    Note: Requires a data provider that supports historical price lookups.
    """

    gas_eth_price_override: Decimal | None = None
    """Override ETH price for gas cost calculations (default: None = use market price).

    When set, this value is used as the ETH price for all gas cost calculations,
    ignoring both historical and current market prices. This is useful for:

    - Testing with a fixed ETH price for reproducibility
    - Stress testing with extreme ETH price scenarios
    - Backtests where gas cost accuracy is not critical

    When None, gas costs use:
    1. Historical ETH price (if use_historical_gas_prices=True)
    2. Current ETH price from data provider
    3. Default fallback ($3000) with warning if unavailable

    Value should be in USD (e.g., Decimal("3000") for $3000 per ETH).
    """

    use_historical_gas_gwei: bool = False
    """Use historical gas prices (gwei) from gas price provider (default: False).

    When enabled and a gas_provider is attached to the PnLBacktester, the engine
    will fetch historical gas prices at each simulation timestamp instead of using
    the static gas_price_gwei value. This provides more realistic gas cost
    estimates that reflect network congestion at historical timestamps.

    Priority order for gas price (gwei):
    1. Historical gas price from gas_provider (if use_historical_gas_gwei=True)
    2. MarketState.gas_price_gwei (if populated by data provider)
    3. config.gas_price_gwei (static default: 30 gwei)

    When disabled, gas costs use the static gas_price_gwei for all trades,
    which is faster but may not reflect actual network conditions.

    Note: Requires a GasPriceProvider (e.g., EtherscanGasPriceProvider) to be
    passed to the PnLBacktester. If enabled without a provider, falls back to
    MarketState.gas_price_gwei or config.gas_price_gwei with a warning.
    """

    track_gas_prices: bool = False
    """Track detailed gas price records for each trade (default: False).

    When enabled, the backtester records a GasPriceRecord for each trade,
    capturing the gas price in gwei, source, and USD cost. These records
    are stored in BacktestResult.gas_prices_used for detailed analysis.

    This is useful for:
    - Analyzing gas price volatility impact on strategy performance
    - Understanding gas cost breakdown by source (historical vs config)
    - Auditing gas costs in institutional-grade backtests

    When disabled, only summary statistics (gas_price_summary) are populated
    from the TradeRecord.gas_price_gwei values, reducing result size.

    Note: Gas price summary statistics are always calculated regardless of
    this setting, since TradeRecord already contains gas_price_gwei.
    """

    # Preflight validation configuration
    preflight_validation: bool = True
    """Enable preflight validation before running backtest (default: True).

    When enabled, the backtester performs validation checks before starting
    the simulation to ensure data requirements can be met:

    - Checks price data availability for all tokens in config
    - Verifies data provider capabilities match requirements
    - Tests archive node accessibility if historical TWAP/Chainlink needed
    - Reports estimated data coverage and potential gaps

    Results are returned in a PreflightReport with pass/fail and details.
    This helps identify data issues early, before spending time on a backtest
    that would fail or produce inaccurate results.

    When disabled, the backtest proceeds without validation, which is faster
    but may encounter data issues during simulation.
    """

    fail_on_preflight_error: bool = True
    """Fail fast if preflight validation fails (default: True).

    When enabled (True): If preflight validation detects critical issues
    (e.g., missing price data, insufficient data coverage), the backtester
    raises PreflightValidationError with an actionable error message that
    includes:
    - What failed (specific checks that did not pass)
    - Why it failed (the underlying cause)
    - How to fix it (recommendations for resolution)

    When disabled (False): The backtester logs warnings about preflight
    issues but continues in degraded mode. This is useful for exploratory
    backtests where you want to see partial results even with data gaps.

    The preflight_passed field in BacktestResult indicates whether preflight
    validation passed, regardless of this setting.

    Note: This setting only applies when preflight_validation=True.
    """

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Time validation
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")

        # Capital validation
        if self.initial_capital_usd <= Decimal("0"):
            raise ValueError("initial_capital_usd must be positive")

        # Gas configuration validation
        if self.gas_price_gwei < Decimal("0"):
            raise ValueError("gas_price_gwei cannot be negative")

        # Inclusion delay validation
        if self.inclusion_delay_blocks < 0:
            raise ValueError("inclusion_delay_blocks cannot be negative")

        # Ensure tokens is not empty
        if not self.tokens:
            raise ValueError("tokens list cannot be empty")

        # Margin ratio validation
        if self.initial_margin_ratio <= Decimal("0") or self.initial_margin_ratio > Decimal("1"):
            raise ValueError("initial_margin_ratio must be between 0 and 1 (exclusive/inclusive)")
        if self.maintenance_margin_ratio <= Decimal("0") or self.maintenance_margin_ratio > Decimal("1"):
            raise ValueError("maintenance_margin_ratio must be between 0 and 1 (exclusive/inclusive)")
        if self.maintenance_margin_ratio > self.initial_margin_ratio:
            raise ValueError("maintenance_margin_ratio must be <= initial_margin_ratio")

        # Reconciliation configuration validation
        if self.reconciliation_alert_threshold_pct < Decimal("0"):
            raise ValueError("reconciliation_alert_threshold_pct cannot be negative")

        # Data quality configuration validation
        if self.staleness_threshold_seconds < 0:
            raise ValueError("staleness_threshold_seconds cannot be negative")

        # Institutional mode configuration validation
        if self.min_data_coverage < Decimal("0") or self.min_data_coverage > Decimal("1"):
            raise ValueError("min_data_coverage must be between 0 and 1")

        # Institutional mode enforcement: set strict defaults
        # When institutional_mode=True, automatically enforce stricter settings
        # for regulated/compliance-critical deployments
        if self.institutional_mode:
            # Enforce strict reproducibility for audit trails
            object.__setattr__(self, "strict_reproducibility", True)
            # Disable degraded data to ensure data quality
            object.__setattr__(self, "allow_degraded_data", False)
            # Disable hardcoded fallback prices for accurate valuations
            object.__setattr__(self, "allow_hardcoded_fallback", False)
            # Require symbol mapping for clear audit trails
            object.__setattr__(self, "require_symbol_mapping", True)
            # Ensure minimum data coverage is at least 98%
            if self.min_data_coverage < Decimal("0.98"):
                object.__setattr__(self, "min_data_coverage", Decimal("0.98"))

    @property
    def duration_seconds(self) -> int:
        """Get the total backtest duration in seconds."""
        delta = self.end_time - self.start_time
        return int(delta.total_seconds())

    @property
    def duration_days(self) -> float:
        """Get the total backtest duration in days."""
        return self.duration_seconds / (24 * 3600)

    @property
    def duration_hours(self) -> float:
        """Get the total backtest duration in hours."""
        return self.duration_seconds / 3600

    @property
    def estimated_ticks(self) -> int:
        """Get the estimated number of simulation ticks."""
        return self.duration_seconds // self.interval_seconds

    @property
    def interval_hours(self) -> float:
        """Get the interval between ticks in hours."""
        return self.interval_seconds / 3600

    def get_gas_cost_usd(self, gas_used: int, eth_price_usd: Decimal) -> Decimal:
        """Calculate gas cost in USD for a given amount of gas used.

        Args:
            gas_used: Amount of gas consumed by the transaction
            eth_price_usd: Current ETH price in USD

        Returns:
            Gas cost in USD
        """
        # Gas cost = gas_used * gas_price (in gwei) * ETH price / 1e9
        gas_cost_eth = Decimal(gas_used) * self.gas_price_gwei / Decimal("1000000000")
        return gas_cost_eth * eth_price_usd

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "interval_seconds": self.interval_seconds,
            "initial_capital_usd": str(self.initial_capital_usd),
            "fee_model": self.fee_model,
            "slippage_model": self.slippage_model,
            "include_gas_costs": self.include_gas_costs,
            "gas_price_gwei": str(self.gas_price_gwei),
            "inclusion_delay_blocks": self.inclusion_delay_blocks,
            "chain": self.chain,
            "tokens": self.tokens,
            "benchmark_token": self.benchmark_token,
            "risk_free_rate": str(self.risk_free_rate),
            "trading_days_per_year": self.trading_days_per_year,
            "initial_margin_ratio": str(self.initial_margin_ratio),
            "maintenance_margin_ratio": str(self.maintenance_margin_ratio),
            "mev_simulation_enabled": self.mev_simulation_enabled,
            "auto_correct_positions": self.auto_correct_positions,
            "reconciliation_alert_threshold_pct": str(self.reconciliation_alert_threshold_pct),
            "random_seed": self.random_seed,
            "strict_reproducibility": self.strict_reproducibility,
            "staleness_threshold_seconds": self.staleness_threshold_seconds,
            "institutional_mode": self.institutional_mode,
            "min_data_coverage": str(self.min_data_coverage),
            "allow_hardcoded_fallback": self.allow_hardcoded_fallback,
            "allow_degraded_data": self.allow_degraded_data,
            "require_symbol_mapping": self.require_symbol_mapping,
            "use_historical_gas_prices": self.use_historical_gas_prices,
            "gas_eth_price_override": str(self.gas_eth_price_override)
            if self.gas_eth_price_override is not None
            else None,
            "use_historical_gas_gwei": self.use_historical_gas_gwei,
            "preflight_validation": self.preflight_validation,
            "fail_on_preflight_error": self.fail_on_preflight_error,
            # Computed properties
            "duration_seconds": self.duration_seconds,
            "duration_days": self.duration_days,
            "estimated_ticks": self.estimated_ticks,
        }

    def to_dict_with_metadata(
        self,
        data_provider_info: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Serialize to dictionary with full metadata for reproducibility.

        This method extends to_dict() to include additional metadata needed
        to reproduce a backtest exactly, such as:
        - Data provider versions and timestamps
        - SDK/framework versions
        - Run timestamp

        Args:
            data_provider_info: Optional dict containing data provider information:
                - name: Provider name (e.g., "coingecko", "chainlink")
                - version: Provider version if available
                - data_fetched_at: ISO timestamp when data was fetched
                - cache_hit_rate: Optional cache hit rate percentage
                - additional provider-specific metadata

        Returns:
            Dictionary with full config and metadata for reproducibility
        """
        config_dict = self.to_dict()

        # Add metadata section
        config_dict["_metadata"] = {
            "config_created_at": datetime.now(UTC).isoformat(),
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "sdk_version": self._get_sdk_version(),
        }

        # Add data provider info if provided
        if data_provider_info:
            config_dict["_metadata"]["data_provider"] = data_provider_info

        return config_dict

    def calculate_config_hash(self) -> str:
        """Calculate a deterministic hash of the configuration for verification.

        The hash is calculated from all configuration parameters that affect
        backtest results, excluding runtime metadata like timestamps. This
        enables verification that a backtest was run with identical config.

        The hash uses SHA-256 and includes:
        - Time range (start_time, end_time, interval_seconds)
        - Capital settings (initial_capital_usd)
        - Model settings (fee_model, slippage_model)
        - Gas settings (include_gas_costs, gas_price_gwei)
        - Execution settings (inclusion_delay_blocks)
        - Chain and token settings
        - Metrics settings (benchmark_token, risk_free_rate, etc.)
        - Margin settings
        - Other simulation parameters

        Returns:
            64-character hex string (SHA-256 hash)

        Example:
            >>> config = PnLBacktestConfig(...)
            >>> hash1 = config.calculate_config_hash()
            >>> # Same config produces same hash
            >>> config2 = PnLBacktestConfig(...)  # identical params
            >>> hash2 = config2.calculate_config_hash()
            >>> assert hash1 == hash2
        """
        # Build a dictionary of hashable config values
        # Use to_dict() but exclude computed properties and metadata
        hash_dict = {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "interval_seconds": self.interval_seconds,
            "initial_capital_usd": str(self.initial_capital_usd),
            "fee_model": self.fee_model,
            "slippage_model": self.slippage_model,
            "include_gas_costs": self.include_gas_costs,
            "gas_price_gwei": str(self.gas_price_gwei),
            "inclusion_delay_blocks": self.inclusion_delay_blocks,
            "chain": self.chain,
            "tokens": sorted(self.tokens),  # Sort for consistency
            "benchmark_token": self.benchmark_token,
            "risk_free_rate": str(self.risk_free_rate),
            "trading_days_per_year": self.trading_days_per_year,
            "initial_margin_ratio": str(self.initial_margin_ratio),
            "maintenance_margin_ratio": str(self.maintenance_margin_ratio),
            "mev_simulation_enabled": self.mev_simulation_enabled,
            "auto_correct_positions": self.auto_correct_positions,
            "reconciliation_alert_threshold_pct": str(self.reconciliation_alert_threshold_pct),
            "random_seed": self.random_seed,
            "strict_reproducibility": self.strict_reproducibility,
            "staleness_threshold_seconds": self.staleness_threshold_seconds,
            "institutional_mode": self.institutional_mode,
            "min_data_coverage": str(self.min_data_coverage),
            "allow_hardcoded_fallback": self.allow_hardcoded_fallback,
            "allow_degraded_data": self.allow_degraded_data,
            "require_symbol_mapping": self.require_symbol_mapping,
            "use_historical_gas_prices": self.use_historical_gas_prices,
            "gas_eth_price_override": str(self.gas_eth_price_override)
            if self.gas_eth_price_override is not None
            else None,
            "use_historical_gas_gwei": self.use_historical_gas_gwei,
            "preflight_validation": self.preflight_validation,
            "fail_on_preflight_error": self.fail_on_preflight_error,
        }

        # Create a deterministic JSON string (sorted keys, no extra whitespace)
        json_str = json.dumps(hash_dict, sort_keys=True, separators=(",", ":"))

        # Calculate SHA-256 hash
        return hashlib.sha256(json_str.encode("utf-8")).hexdigest()

    @staticmethod
    def _get_sdk_version() -> str:
        """Get the almanak SDK version.

        Returns:
            SDK version string or "unknown" if not available
        """
        try:
            from importlib.metadata import version

            return version("almanak")
        except Exception:
            return "unknown"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PnLBacktestConfig":
        """Deserialize from dictionary.

        Args:
            data: Dictionary containing config fields

        Returns:
            PnLBacktestConfig instance
        """
        # Parse datetime fields
        start_time = (
            datetime.fromisoformat(data["start_time"]) if isinstance(data["start_time"], str) else data["start_time"]
        )
        end_time = datetime.fromisoformat(data["end_time"]) if isinstance(data["end_time"], str) else data["end_time"]

        # Parse Decimal fields
        initial_capital = (
            Decimal(data["initial_capital_usd"])
            if isinstance(data["initial_capital_usd"], str)
            else data["initial_capital_usd"]
        )
        gas_price = (
            Decimal(data.get("gas_price_gwei", "30"))
            if isinstance(data.get("gas_price_gwei"), str)
            else data.get("gas_price_gwei", Decimal("30"))
        )
        risk_free_rate = (
            Decimal(data.get("risk_free_rate", "0.05"))
            if isinstance(data.get("risk_free_rate"), str)
            else data.get("risk_free_rate", Decimal("0.05"))
        )
        initial_margin_ratio = (
            Decimal(data.get("initial_margin_ratio", "0.1"))
            if isinstance(data.get("initial_margin_ratio"), str)
            else data.get("initial_margin_ratio", Decimal("0.1"))
        )
        maintenance_margin_ratio = (
            Decimal(data.get("maintenance_margin_ratio", "0.05"))
            if isinstance(data.get("maintenance_margin_ratio"), str)
            else data.get("maintenance_margin_ratio", Decimal("0.05"))
        )
        reconciliation_alert_threshold_pct = (
            Decimal(data.get("reconciliation_alert_threshold_pct", "0.05"))
            if isinstance(data.get("reconciliation_alert_threshold_pct"), str)
            else data.get("reconciliation_alert_threshold_pct", Decimal("0.05"))
        )
        min_data_coverage = (
            Decimal(data.get("min_data_coverage", "0.98"))
            if isinstance(data.get("min_data_coverage"), str)
            else data.get("min_data_coverage", Decimal("0.98"))
        )
        gas_eth_price_override_raw = data.get("gas_eth_price_override")
        gas_eth_price_override = (
            Decimal(gas_eth_price_override_raw)
            if gas_eth_price_override_raw is not None and isinstance(gas_eth_price_override_raw, str)
            else gas_eth_price_override_raw
        )

        return cls(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=data.get("interval_seconds", 3600),
            initial_capital_usd=initial_capital,
            fee_model=data.get("fee_model", "realistic"),
            slippage_model=data.get("slippage_model", "realistic"),
            include_gas_costs=data.get("include_gas_costs", True),
            gas_price_gwei=gas_price,
            inclusion_delay_blocks=data.get("inclusion_delay_blocks", 1),
            chain=data.get("chain", "arbitrum"),
            tokens=data.get("tokens", ["WETH", "USDC"]),
            benchmark_token=data.get("benchmark_token", "WETH"),
            risk_free_rate=risk_free_rate,
            trading_days_per_year=data.get("trading_days_per_year", 365),
            initial_margin_ratio=initial_margin_ratio,
            maintenance_margin_ratio=maintenance_margin_ratio,
            mev_simulation_enabled=data.get("mev_simulation_enabled", False),
            auto_correct_positions=data.get("auto_correct_positions", False),
            reconciliation_alert_threshold_pct=reconciliation_alert_threshold_pct,
            random_seed=data.get("random_seed"),
            strict_reproducibility=data.get("strict_reproducibility", False),
            staleness_threshold_seconds=data.get("staleness_threshold_seconds", 3600),
            institutional_mode=data.get("institutional_mode", False),
            min_data_coverage=min_data_coverage,
            allow_hardcoded_fallback=data.get("allow_hardcoded_fallback", False),
            allow_degraded_data=data.get("allow_degraded_data", True),
            require_symbol_mapping=data.get("require_symbol_mapping", False),
            use_historical_gas_prices=data.get("use_historical_gas_prices", False),
            gas_eth_price_override=gas_eth_price_override,
            use_historical_gas_gwei=data.get("use_historical_gas_gwei", False),
            preflight_validation=data.get("preflight_validation", True),
            fail_on_preflight_error=data.get("fail_on_preflight_error", True),
        )

    def __repr__(self) -> str:
        """Return a human-readable representation."""
        return (
            f"PnLBacktestConfig("
            f"start={self.start_time.date()}, "
            f"end={self.end_time.date()}, "
            f"capital=${self.initial_capital_usd:,.0f}, "
            f"interval={self.interval_hours:.1f}h, "
            f"ticks={self.estimated_ticks})"
        )


__all__ = ["PnLBacktestConfig"]
