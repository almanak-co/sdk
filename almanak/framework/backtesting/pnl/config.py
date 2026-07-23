"""Configuration for PnL backtesting.

This module defines the configuration dataclass for PnL backtests,
which controls simulation parameters like time range, token funding,
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
            token_funding=[
                {
                    "symbol": "USDC",
                    "address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                    "chain": "arbitrum",
                    "amount": "10000",
                    "amount_type": "usd",
                }
            ],
        )

    Custom configuration with fee/slippage models:

        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            token_funding=[...],
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
            token_funding=[...],
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

from almanak.core.chains import DEFAULT_CHAIN, LEGACY_SERIALIZED_CHAIN, ChainRegistry


def default_gas_price_gwei_for_chain(chain: str) -> Decimal:
    """Resolve the chain-aware default gas price in gwei for a chain.

    VIB-5088: the old flat ``Decimal("30")`` default was wrong by two or
    more orders of magnitude on L2s (a $10k/84-trade Arbitrum run simulated
    $1,486.72 of gas). Values come from the chain registry --
    ``ChainDescriptor.gas.fallback_base_fee_gwei`` +
    ``fallback_priority_fee_gwei`` -- the same sourced constants that back
    ``DEFAULT_GAS_PRICES`` in ``pnl/providers/gas.py`` (e.g. Arbitrum
    0.1 + 0.0, Base/Optimism 0.001 + 0.001, Ethereum 0.16 + 0.05 after the
    2026-07 post-blob retune).

    Chains without registered fallback fees use the ethereum descriptor's
    values -- the documented conservative default, mirroring the legacy
    ``DEFAULT_GAS_PRICES.get(chain, DEFAULT_GAS_PRICES["ethereum"])``
    consumer shape. No numbers are invented here: every value traces to a
    ``GasProfile`` in ``almanak/core/chains/``.

    Args:
        chain: Chain identifier (e.g. "arbitrum", "base", "ethereum").

    Returns:
        Default gas price in gwei (base fee + priority fee) as Decimal.
    """
    descriptor = ChainRegistry.try_resolve(chain)
    gas = descriptor.gas if descriptor is not None else None
    if gas is None or gas.fallback_base_fee_gwei is None or gas.fallback_priority_fee_gwei is None:
        # Registry-owned policy (no chain literal here per the VIB-4851
        # coupling rule): unknown chains assume the most expensive case.
        gas = ChainRegistry.conservative_gas_fallback()
    return Decimal(str(gas.fallback_base_fee_gwei)) + Decimal(str(gas.fallback_priority_fee_gwei))


def _token_funding_for_hash(token_funding: list[Any] | None) -> list[Any] | None:
    """Return a stable token_funding representation for config hashing."""
    if not token_funding:
        return token_funding

    normalized: list[Any] = []
    for entry in token_funding:
        model_dump = getattr(entry, "model_dump", None)
        normalized_entry = model_dump(mode="json") if callable(model_dump) else entry
        normalized.append(_stable_json_value(normalized_entry))
    return sorted(normalized, key=lambda entry: json.dumps(entry, sort_keys=True))


def _stable_json_value(value: Any) -> Any:
    """Convert config fragments into deterministic JSON-safe values."""
    return json.loads(json.dumps(value, sort_keys=True, default=str))


def _tokens_for_hash(tokens: list[Any]) -> list[Any]:
    """Return a stable, JSON-safe token list for config hashing."""
    normalized = [_stable_json_value(token) for token in tokens]
    return sorted(normalized, key=lambda token: json.dumps(token, sort_keys=True))


def _tokens_for_serialization(tokens: list[Any]) -> list[Any]:
    """Return a JSON-safe token list while preserving user-provided order."""
    return [_stable_json_value(token) for token in tokens]


@dataclass
class PnLBacktestConfig:
    """Configuration for a PnL backtest simulation.

    Controls all parameters of the backtest including time range, token
    funding, fee/slippage models, gas costs, and execution delay simulation.

    Attributes:
        start_time: Start of the backtest period (inclusive)
        end_time: End of the backtest period (inclusive)
        interval_seconds: Time between simulation ticks in seconds (default: 3600 = 1 hour)
        token_funding: Strategy funding basket used to seed the starting wallet
        fee_model: Fee model to use - 'realistic', 'zero', or protocol-specific
            (e.g., 'uniswap_v3', 'aave_v3', 'gmx')
        slippage_model: Slippage model to use - 'realistic', 'zero', or protocol-specific
            (e.g., 'liquidity_aware', 'constant')
        include_gas_costs: Whether to include gas costs in PnL calculations
        gas_price_gwei: Gas price to use for cost calculations (default: None =
            chain-aware default from the chain registry, e.g. 0.1 gwei on
            Arbitrum, 0.21 gwei on Ethereum -- VIB-5088)
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
            token_funding=[...],
        )
        print(f"Duration: {config.duration_days:.1f} days")
        print(f"Estimated ticks: {config.estimated_ticks}")
    """

    # Time range configuration
    start_time: datetime
    end_time: datetime
    interval_seconds: int = 3600  # 1 hour default

    # Startup wallet configuration. Historical PnL backtests seed the wallet
    # exclusively from token_funding.
    token_funding: list[dict[str, Any]] | None = None

    # Fee and slippage model configuration
    fee_model: str = "realistic"
    slippage_model: str = "realistic"

    # Gas cost configuration
    include_gas_costs: bool = True
    gas_funding_usd: Decimal | None = None
    """Operational gas tank funded at run start, in USD (ALM-2958 structural).

    Live, gas is native ETH paid by the agent EOA — platform-funded, never
    strategy capital. The tank models that wallet: every fill's gas draws
    from it, never from strategy cash, and when a finite tank cannot cover a
    fill's gas the fill is REJECTED ("gas tank exhausted") exactly as the
    live wallet would fail. ``None`` (the default) is an unlimited tank:
    gas is metered but never binds, so existing configs are unaffected.
    The phase-5 behavior replay revisits the default.
    """
    gas_price_gwei: Decimal | None = None
    """Static gas price in gwei for cost simulation (default: None = chain-aware).

    ``None`` means "not set by the user": ``__post_init__`` resolves it via
    :func:`default_gas_price_gwei_for_chain` from the chain registry and
    flips ``gas_price_gwei_is_default`` to True (VIB-5088 -- the old flat
    30 gwei default overstated L2 gas costs by ~100x or more). After
    construction this field is always a ``Decimal``.
    """

    gas_price_gwei_is_default: bool = field(init=False, default=False)
    """Provenance marker: True when ``gas_price_gwei`` was not user-set.

    Set by ``__post_init__`` when the chain-aware registry default was
    used. The engine consumes this to label the gas price source as
    ``chain_default`` (still a fabrication for compliance purposes, just a
    plausible one) and, in institutional mode, to refuse to fabricate gas
    costs entirely.
    """

    # Execution simulation
    inclusion_delay_blocks: int = 1

    # Chain and token configuration
    chain: str = DEFAULT_CHAIN
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
    3. No fallback -- the engine raises ValueError if no ETH/WETH price is
       available (the legacy $3000 fabrication was removed; see
       tests/unit/backtesting/pnl/test_gas_eth_price_fallback.py)

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
    3. config.gas_price_gwei (chain-aware default from the chain registry, e.g.
       ~0.1 gwei on Arbitrum, ~0.21 gwei on Ethereum -- VIB-5088)

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
        self._validate_time_config()
        self._validate_token_funding_config()
        self._resolve_and_validate_gas_price()
        self._validate_execution_config()
        self._validate_tokens_config()
        self._validate_margin_ratios()
        self._validate_reconciliation_config()
        self._validate_data_quality_config()
        self._apply_institutional_mode_defaults()

    def _validate_time_config(self) -> None:
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")

    def _validate_token_funding_config(self) -> None:
        if self.token_funding is not None and not isinstance(self.token_funding, list):
            raise ValueError("token_funding must be a list")

    def _validate_execution_config(self) -> None:
        if self.inclusion_delay_blocks < 0:
            raise ValueError("inclusion_delay_blocks cannot be negative")

    def _validate_tokens_config(self) -> None:
        if not self.tokens:
            raise ValueError("tokens list cannot be empty")

    def _validate_margin_ratios(self) -> None:
        if self.initial_margin_ratio <= Decimal("0") or self.initial_margin_ratio > Decimal("1"):
            raise ValueError("initial_margin_ratio must be between 0 and 1 (exclusive/inclusive)")
        if self.maintenance_margin_ratio <= Decimal("0") or self.maintenance_margin_ratio > Decimal("1"):
            raise ValueError("maintenance_margin_ratio must be between 0 and 1 (exclusive/inclusive)")
        if self.maintenance_margin_ratio > self.initial_margin_ratio:
            raise ValueError("maintenance_margin_ratio must be <= initial_margin_ratio")

    def _validate_reconciliation_config(self) -> None:
        if self.reconciliation_alert_threshold_pct < Decimal("0"):
            raise ValueError("reconciliation_alert_threshold_pct cannot be negative")

    def _validate_data_quality_config(self) -> None:
        if self.gas_funding_usd is not None and self.gas_funding_usd < 0:
            raise ValueError(f"gas_funding_usd cannot be negative: {self.gas_funding_usd}")
        if self.staleness_threshold_seconds < 0:
            raise ValueError("staleness_threshold_seconds cannot be negative")

        if self.min_data_coverage < Decimal("0") or self.min_data_coverage > Decimal("1"):
            raise ValueError("min_data_coverage must be between 0 and 1")

    def _apply_institutional_mode_defaults(self) -> None:
        if not self.institutional_mode:
            return

        object.__setattr__(self, "strict_reproducibility", True)
        object.__setattr__(self, "allow_degraded_data", False)
        object.__setattr__(self, "allow_hardcoded_fallback", False)
        object.__setattr__(self, "require_symbol_mapping", True)
        if self.min_data_coverage < Decimal("0.98"):
            object.__setattr__(self, "min_data_coverage", Decimal("0.98"))

    def _resolve_and_validate_gas_price(self) -> None:
        """Resolve the chain-aware gas default and validate the result.

        VIB-5088: ``gas_price_gwei is None`` means "not set by the user" --
        resolve it from the chain registry (no silent flat 30 gwei) and mark
        the provenance so the engine can label the source ``chain_default``
        and institutional mode can refuse to fabricate.
        """
        # Direct assignment (the dataclass is not frozen) keeps mypy's
        # None-narrowing intact for the validation below. Decimal-ness is the
        # caller's contract (the field is typed Decimal | None and the CLI /
        # from_dict boundaries already coerce) -- a defensive isinstance(...,
        # Decimal) branch here would re-introduce the VIB-4062 caller
        # bifurcation that tests/contracts/test_no_bifurcation.py forbids.
        if self.gas_price_gwei is None:
            self.gas_price_gwei = default_gas_price_gwei_for_chain(self.chain)
            self.gas_price_gwei_is_default = True

        if self.gas_price_gwei < Decimal("0"):
            raise ValueError("gas_price_gwei cannot be negative")

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
        # Gas cost = gas_used * gas_price (in gwei) * ETH price / 1e9.
        # gas_price_gwei is always a Decimal after __post_init__ (None is
        # resolved to the chain-aware default there).
        assert self.gas_price_gwei is not None  # resolved in __post_init__
        gas_cost_eth = Decimal(gas_used) * self.gas_price_gwei / Decimal("1000000000")
        return gas_cost_eth * eth_price_usd

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "interval_seconds": self.interval_seconds,
            "token_funding": _token_funding_for_hash(self.token_funding),
            "fee_model": self.fee_model,
            "slippage_model": self.slippage_model,
            "include_gas_costs": self.include_gas_costs,
            "gas_price_gwei": str(self.gas_price_gwei),
            "gas_funding_usd": str(self.gas_funding_usd) if self.gas_funding_usd is not None else None,
            "gas_price_gwei_is_default": self.gas_price_gwei_is_default,
            "inclusion_delay_blocks": self.inclusion_delay_blocks,
            "chain": self.chain,
            "tokens": _tokens_for_serialization(self.tokens),
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
        - Startup funding settings (token_funding)
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
            "token_funding": _token_funding_for_hash(self.token_funding),
            "fee_model": self.fee_model,
            "slippage_model": self.slippage_model,
            "include_gas_costs": self.include_gas_costs,
            # The RESOLVED gas price is hashed, not the provenance flag:
            # the simulation math depends only on the value, so an explicit
            # gas_price_gwei equal to the chain default hashes identically
            # (and pre-VIB-5088 hashes of explicitly-set configs are stable).
            "gas_price_gwei": str(self.gas_price_gwei),
            "gas_funding_usd": str(self.gas_funding_usd) if self.gas_funding_usd is not None else None,
            "inclusion_delay_blocks": self.inclusion_delay_blocks,
            "chain": self.chain,
            "tokens": _tokens_for_hash(self.tokens),
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

        if "initial_capital_usd" in data:
            raise ValueError("initial_capital_usd is no longer a valid PnL backtest config field; use token_funding")
        # VIB-5088: a missing gas_price_gwei deserializes as None so the
        # chain-aware default resolves in __post_init__. A present value is
        # preserved byte-for-byte (legacy artifacts always wrote the key, so
        # pre-VIB-5088 results keep their original -- possibly flat-30 --
        # behaviour); the provenance flag is restored after construction.
        raw_gas_price = data.get("gas_price_gwei")
        gas_price = Decimal(raw_gas_price) if isinstance(raw_gas_price, str) else raw_gas_price
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
        gas_funding_usd_raw = data.get("gas_funding_usd")
        gas_funding_usd = (
            Decimal(gas_funding_usd_raw)
            if gas_funding_usd_raw is not None and isinstance(gas_funding_usd_raw, str)
            else gas_funding_usd_raw
        )
        gas_eth_price_override_raw = data.get("gas_eth_price_override")
        gas_eth_price_override = (
            Decimal(gas_eth_price_override_raw)
            if gas_eth_price_override_raw is not None and isinstance(gas_eth_price_override_raw, str)
            else gas_eth_price_override_raw
        )

        config = cls(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=data.get("interval_seconds", 3600),
            token_funding=data.get("token_funding"),
            fee_model=data.get("fee_model", "realistic"),
            slippage_model=data.get("slippage_model", "realistic"),
            include_gas_costs=data.get("include_gas_costs", True),
            gas_funding_usd=gas_funding_usd,
            gas_price_gwei=gas_price,
            inclusion_delay_blocks=data.get("inclusion_delay_blocks", 1),
            chain=data.get("chain", LEGACY_SERIALIZED_CHAIN),
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
        # Restore gas-price provenance: when the artifact recorded the value
        # as a chain-aware default, keep the exact serialized value (set
        # above) but mark it as default-sourced for audit/compliance lanes.
        if raw_gas_price is not None and data.get("gas_price_gwei_is_default", False):
            config.gas_price_gwei_is_default = True
        return config

    def __repr__(self) -> str:
        """Return a human-readable representation."""
        return (
            f"PnLBacktestConfig("
            f"start={self.start_time.date()}, "
            f"end={self.end_time.date()}, "
            f"funded_tokens={len(self.token_funding or [])}, "
            f"interval={self.interval_hours:.1f}h, "
            f"ticks={self.estimated_ticks})"
        )


__all__ = ["PnLBacktestConfig", "default_gas_price_gwei_for_chain"]
