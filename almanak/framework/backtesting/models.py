"""Shared data models for backtesting engines.

This module provides core data models used by both the PnL Backtester
(historical simulation) and Paper Trader (real-time fork execution).

Models:
    - BacktestMetrics: Performance metrics calculated from backtest results
    - TradeRecord: Record of a single trade execution with fees and slippage
    - BacktestResult: Complete results from a backtest run
    - EquityPoint: Single point on the equity curve
    - DataQualityReport: Data quality metrics for institutional compliance
    - FeeAccrualResult: LP fee calculation with confidence tracking

The BacktestResult model includes institutional compliance tracking:
    - institutional_compliance: Boolean indicating if the run meets standards
    - compliance_violations: List of specific compliance failures
    - data_quality: DataQualityReport with coverage and source breakdown
    - data_source_capabilities: Map of provider names to their capabilities

Example:
    result = await backtester.backtest(strategy, config)

    # Check institutional compliance
    if result.institutional_compliance:
        print("Backtest meets institutional standards")
    else:
        print(f"Violations: {result.compliance_violations}")

    # Access data quality metrics
    if result.data_quality:
        print(f"Coverage: {result.data_quality.coverage_ratio:.1%}")
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
        MonteCarloSimulationResult,
    )
    from almanak.framework.backtesting.pnl.data_provider import HistoricalDataCapability
    from almanak.framework.backtesting.pnl.walk_forward import WalkForwardResult


class BacktestEngine(StrEnum):
    """Type of backtesting engine used."""

    PNL = "pnl"  # Historical PnL simulation
    PAPER = "paper"  # Real-time paper trading on fork


class StrategyType(StrEnum):
    """Type of trading strategy for accuracy estimation.

    Used in conjunction with data quality tier to estimate expected
    backtest accuracy. Each strategy type has different accuracy
    characteristics based on complexity of execution simulation.
    """

    LP = "lp"  # Liquidity Provider strategies (Uniswap, Aave supply)
    PERP = "perp"  # Perpetual trading strategies (GMX, Hyperliquid)
    LENDING = "lending"  # Lending/borrowing strategies (Aave, Compound)
    ARBITRAGE = "arbitrage"  # Arbitrage strategies (cross-DEX, MEV)
    SPOT = "spot"  # Spot trading strategies (simple swaps)
    SWAP = "swap"  # Swap strategies (alias for spot, simple token swaps)
    YIELD = "yield"  # Yield farming strategies (similar to lending)
    MULTI_PROTOCOL = "multi_protocol"  # Multi-protocol strategies (combined)
    UNKNOWN = "unknown"  # Strategy type could not be determined


# Accuracy matrix: (strategy_type, data_quality_tier) -> (min_accuracy, max_accuracy, primary_error)
# Values derived from ACCURACY_LIMITATIONS.md and golden test tolerances
ACCURACY_MATRIX: dict[tuple[str, str], tuple[float, float, str]] = {
    # LP strategies
    ("lp", "full"): (0.90, 0.95, "Fee estimation and volume data accuracy"),
    ("lp", "pre_cache"): (0.85, 0.93, "Cache coverage gaps cause interpolation"),
    ("lp", "current_only"): (0.50, 0.70, "Historical prices unavailable"),
    # Perp strategies
    ("perp", "full"): (0.92, 0.97, "Funding rate averaging over 8h periods"),
    ("perp", "pre_cache"): (0.88, 0.95, "Funding rate cache completeness"),
    ("perp", "current_only"): (0.60, 0.75, "Historical funding rates unavailable"),
    # Lending strategies
    ("lending", "full"): (0.97, 0.99, "Interest compounding frequency"),
    ("lending", "pre_cache"): (0.95, 0.98, "APY cache completeness"),
    ("lending", "current_only"): (0.80, 0.90, "Historical APY unavailable"),
    # Arbitrage strategies
    ("arbitrage", "full"): (0.70, 0.85, "MEV competition not simulatable"),
    ("arbitrage", "pre_cache"): (0.60, 0.80, "Price lag between sources"),
    ("arbitrage", "current_only"): (0.20, 0.40, "All arbitrage signals invalid"),
    # Spot strategies
    ("spot", "full"): (0.93, 0.97, "Slippage estimation"),
    ("spot", "pre_cache"): (0.90, 0.95, "Price cache completeness"),
    ("spot", "current_only"): (0.65, 0.80, "Historical prices unavailable"),
    # Swap strategies (same as spot - simple token swaps)
    ("swap", "full"): (0.93, 0.97, "Slippage estimation"),
    ("swap", "pre_cache"): (0.90, 0.95, "Price cache completeness"),
    ("swap", "current_only"): (0.65, 0.80, "Historical prices unavailable"),
    # Yield strategies (similar to lending - yield farming)
    ("yield", "full"): (0.95, 0.98, "APY estimation and compounding"),
    ("yield", "pre_cache"): (0.90, 0.95, "Yield rate cache completeness"),
    ("yield", "current_only"): (0.70, 0.85, "Historical yield rates unavailable"),
    # Multi-protocol strategies (conservative - combines multiple protocol risks)
    ("multi_protocol", "full"): (0.85, 0.92, "Combined protocol simulation complexity"),
    ("multi_protocol", "pre_cache"): (0.80, 0.88, "Cross-protocol data consistency"),
    ("multi_protocol", "current_only"): (0.55, 0.70, "Multi-source historical data gaps"),
    # Unknown strategy type - conservative estimates
    ("unknown", "full"): (0.80, 0.90, "Strategy type not determined"),
    ("unknown", "pre_cache"): (0.70, 0.85, "Strategy type not determined"),
    ("unknown", "current_only"): (0.40, 0.60, "Strategy type not determined"),
}


@dataclass
class AccuracyEstimate:
    """Estimated accuracy of a backtest based on strategy type and data quality.

    Provides a quick reference for users to understand expected accuracy
    based on the strategy type detected and the data quality tier used.
    The estimate is derived from the ACCURACY_MATRIX which is based on
    documented accuracy limitations and golden test tolerances.

    Attributes:
        strategy_type: Detected strategy type (lp, perp, lending, etc.)
        data_quality_tier: Best data quality tier used (full, pre_cache, current_only)
        min_accuracy_pct: Minimum expected accuracy percentage (0-100)
        max_accuracy_pct: Maximum expected accuracy percentage (0-100)
        primary_error_source: Main source of error for this combination
        confidence_interval: Human-readable confidence interval string
        warnings: Any warnings about the estimate (e.g., low confidence)
    """

    strategy_type: str
    data_quality_tier: str
    min_accuracy_pct: float
    max_accuracy_pct: float
    primary_error_source: str
    confidence_interval: str = ""
    warnings: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Generate confidence interval string if not provided."""
        if not self.confidence_interval:
            self.confidence_interval = f"{self.min_accuracy_pct:.0f}-{self.max_accuracy_pct:.0f}%"

    @classmethod
    def from_config(
        cls,
        strategy_type: str | StrategyType,
        data_quality_tier: str,
        detected_confidence: str = "high",
    ) -> "AccuracyEstimate":
        """Create accuracy estimate from strategy type and data quality tier.

        Args:
            strategy_type: Strategy type (string or enum)
            data_quality_tier: Data quality tier (full, pre_cache, current_only)
            detected_confidence: Confidence in strategy type detection (high, medium, low)

        Returns:
            AccuracyEstimate with values from ACCURACY_MATRIX
        """
        # Normalize inputs
        st = strategy_type.value if isinstance(strategy_type, StrategyType) else strategy_type.lower()
        dq = data_quality_tier.lower()

        # Handle enum values for data quality
        if dq not in ("full", "pre_cache", "current_only"):
            dq = "current_only"  # Conservative default

        # Look up in matrix, fall back to unknown
        key = (st, dq)
        if key not in ACCURACY_MATRIX:
            key = ("unknown", dq)

        min_acc, max_acc, error_source = ACCURACY_MATRIX[key]

        # Adjust for detection confidence
        warnings: list[str] = []
        if detected_confidence == "low":
            # Widen range by 5% on each end
            min_acc = max(0.0, min_acc - 0.05)
            max_acc = min(1.0, max_acc + 0.05)
            warnings.append("Low confidence in strategy type detection - accuracy range widened")
        elif detected_confidence == "medium":
            # Widen range by 2% on each end
            min_acc = max(0.0, min_acc - 0.02)
            max_acc = min(1.0, max_acc + 0.02)
            warnings.append("Medium confidence in strategy type detection")

        return cls(
            strategy_type=st,
            data_quality_tier=dq,
            min_accuracy_pct=min_acc * 100,
            max_accuracy_pct=max_acc * 100,
            primary_error_source=error_source,
            warnings=warnings,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "strategy_type": self.strategy_type,
            "data_quality_tier": self.data_quality_tier,
            "min_accuracy_pct": self.min_accuracy_pct,
            "max_accuracy_pct": self.max_accuracy_pct,
            "primary_error_source": self.primary_error_source,
            "confidence_interval": self.confidence_interval,
            "warnings": self.warnings,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AccuracyEstimate":
        """Deserialize from dictionary."""
        return cls(
            strategy_type=data["strategy_type"],
            data_quality_tier=data["data_quality_tier"],
            min_accuracy_pct=data["min_accuracy_pct"],
            max_accuracy_pct=data["max_accuracy_pct"],
            primary_error_source=data["primary_error_source"],
            confidence_interval=data.get("confidence_interval", ""),
            warnings=data.get("warnings", []),
        )


class ParameterSource(StrEnum):
    """Source of a configuration parameter value.

    Used for audit trail tracking to understand where each parameter value
    came from during a backtest run. This is critical for institutional
    compliance and reproducibility analysis.
    """

    DEFAULT = "default"  # Used dataclass/class default value
    CONFIG_FILE = "config_file"  # Loaded from configuration file (JSON, YAML)
    ENV_VAR = "env_var"  # Loaded from environment variable
    EXPLICIT = "explicit"  # Explicitly passed as parameter
    ASSET_SPECIFIC = "asset_specific"  # Asset-specific override (e.g., per-token liquidation threshold)
    PROTOCOL_DEFAULT = "protocol_default"  # Protocol-specific default (e.g., Aave V3 defaults)
    GLOBAL_DEFAULT = "global_default"  # Global fallback default
    HISTORICAL = "historical"  # Derived from historical data (e.g., fetched APY)
    FIXED = "fixed"  # Fixed/hardcoded value in config
    PROVIDER = "provider"  # From data provider (e.g., live funding rate)


@dataclass
class ParameterSourceRecord:
    """Record of a parameter's value and its source.

    Tracks not just where the value came from, but also the raw value
    and any fallback chain that was used.

    Attributes:
        parameter_name: Name of the parameter (e.g., "initial_margin_ratio")
        value: The actual value used (as string for serialization)
        source: How the value was determined
        category: Category of parameter (config, liquidation, apy_funding)
        fallback_chain: List of sources tried before this one succeeded
        timestamp: When this value was resolved (for time-varying values)
    """

    parameter_name: str
    value: str
    source: ParameterSource
    category: Literal["config", "liquidation", "apy_funding", "gas", "other"] = "other"
    fallback_chain: list[str] = field(default_factory=list)
    timestamp: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "parameter_name": self.parameter_name,
            "value": self.value,
            "source": self.source.value,
            "category": self.category,
            "fallback_chain": self.fallback_chain,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ParameterSourceRecord":
        """Deserialize from dictionary."""
        return cls(
            parameter_name=data["parameter_name"],
            value=data["value"],
            source=ParameterSource(data["source"]),
            category=data.get("category", "other"),
            fallback_chain=data.get("fallback_chain", []),
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else None,
        )


@dataclass
class ParameterSourceTracker:
    """Tracks the source of all configuration parameters for audit purposes.

    This class aggregates all parameter source records and provides
    summary statistics for the institutional compliance report.

    Attributes:
        records: List of all parameter source records
        config_sources: Summary of config parameter sources (param_name -> source)
        liquidation_sources: Summary of liquidation threshold sources
        apy_funding_sources: Summary of APY/funding rate sources
    """

    records: list[ParameterSourceRecord] = field(default_factory=list)

    def record_parameter(
        self,
        name: str,
        value: Any,
        source: ParameterSource,
        category: Literal["config", "liquidation", "apy_funding", "gas", "other"] = "other",
        fallback_chain: list[str] | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        """Record a parameter's source.

        Args:
            name: Parameter name
            value: The value used (will be converted to string)
            source: Source of the value
            category: Category of parameter
            fallback_chain: Optional list of sources tried before success
            timestamp: Optional timestamp for time-varying values
        """
        self.records.append(
            ParameterSourceRecord(
                parameter_name=name,
                value=str(value),
                source=source,
                category=category,
                fallback_chain=fallback_chain or [],
                timestamp=timestamp,
            )
        )

    @property
    def config_sources(self) -> dict[str, str]:
        """Get summary of config parameter sources."""
        return {r.parameter_name: r.source.value for r in self.records if r.category == "config"}

    @property
    def liquidation_sources(self) -> dict[str, str]:
        """Get summary of liquidation threshold sources."""
        return {r.parameter_name: r.source.value for r in self.records if r.category == "liquidation"}

    @property
    def apy_funding_sources(self) -> dict[str, str]:
        """Get summary of APY/funding rate sources."""
        return {r.parameter_name: r.source.value for r in self.records if r.category == "apy_funding"}

    @property
    def gas_sources(self) -> dict[str, str]:
        """Get summary of gas-related parameter sources."""
        return {r.parameter_name: r.source.value for r in self.records if r.category == "gas"}

    def get_by_category(self, category: str) -> list[ParameterSourceRecord]:
        """Get all records for a specific category."""
        return [r for r in self.records if r.category == category]

    def get_sources_summary(self) -> dict[str, int]:
        """Get count of parameters by source type."""
        summary: dict[str, int] = {}
        for record in self.records:
            source = record.source.value
            summary[source] = summary.get(source, 0) + 1
        return summary

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "records": [r.to_dict() for r in self.records],
            "config_sources": self.config_sources,
            "liquidation_sources": self.liquidation_sources,
            "apy_funding_sources": self.apy_funding_sources,
            "gas_sources": self.gas_sources,
            "sources_summary": self.get_sources_summary(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ParameterSourceTracker":
        """Deserialize from dictionary."""
        records = [ParameterSourceRecord.from_dict(r) for r in data.get("records", [])]
        return cls(records=records)


@dataclass
class LiquidationEvent:
    """Record of a liquidation event during backtesting.

    This model captures details when a position is liquidated due to
    insufficient margin or collateral.

    Attributes:
        timestamp: When the liquidation occurred
        position_id: ID of the position that was liquidated
        price: Market price at time of liquidation
        loss_usd: Total loss from the liquidation in USD
    """

    timestamp: datetime
    position_id: str
    price: Decimal
    loss_usd: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "position_id": self.position_id,
            "price": str(self.price),
            "loss_usd": str(self.loss_usd),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LiquidationEvent":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized LiquidationEvent data

        Returns:
            LiquidationEvent instance
        """
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            position_id=data["position_id"],
            price=Decimal(data["price"]),
            loss_usd=Decimal(data["loss_usd"]),
        )


@dataclass
class LendingLiquidationEvent:
    """Record of a lending liquidation event during backtesting.

    This model captures details when a lending position is liquidated due to
    health factor falling below 1.0 (undercollateralized).

    Attributes:
        timestamp: When the liquidation occurred
        position_id: ID of the position that was liquidated
        health_factor: Health factor at time of liquidation (< 1.0)
        collateral_seized: Amount of collateral seized by liquidator in USD
        debt_repaid: Amount of debt repaid by liquidator in USD
        penalty: Liquidation penalty applied as decimal (e.g., 0.05 = 5%)
    """

    timestamp: datetime
    position_id: str
    health_factor: Decimal
    collateral_seized: Decimal
    debt_repaid: Decimal
    penalty: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "position_id": self.position_id,
            "health_factor": str(self.health_factor),
            "collateral_seized": str(self.collateral_seized),
            "debt_repaid": str(self.debt_repaid),
            "penalty": str(self.penalty),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LendingLiquidationEvent":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized LendingLiquidationEvent data

        Returns:
            LendingLiquidationEvent instance
        """
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            position_id=data["position_id"],
            health_factor=Decimal(data["health_factor"]),
            collateral_seized=Decimal(data["collateral_seized"]),
            debt_repaid=Decimal(data["debt_repaid"]),
            penalty=Decimal(data["penalty"]),
        )


@dataclass
class ReconciliationEvent:
    """Record of a position reconciliation event during backtesting.

    This model captures details when a discrepancy is detected between
    the tracked position state and the actual on-chain state.

    Attributes:
        timestamp: When the reconciliation occurred
        position_id: ID of the position being reconciled
        expected: Expected value (from tracked state)
        actual: Actual value (from on-chain query)
        discrepancy: Absolute difference between expected and actual
        discrepancy_pct: Discrepancy as a percentage of expected value
        field_name: Name of the field being reconciled (e.g., "amount", "value_usd")
        auto_corrected: Whether the position was auto-corrected
    """

    timestamp: datetime
    position_id: str
    expected: Decimal
    actual: Decimal
    discrepancy: Decimal
    discrepancy_pct: Decimal = Decimal("0")
    field_name: str = "amount"
    auto_corrected: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "position_id": self.position_id,
            "expected": str(self.expected),
            "actual": str(self.actual),
            "discrepancy": str(self.discrepancy),
            "discrepancy_pct": str(self.discrepancy_pct),
            "field_name": self.field_name,
            "auto_corrected": self.auto_corrected,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReconciliationEvent":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized ReconciliationEvent data

        Returns:
            ReconciliationEvent instance
        """
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            position_id=data["position_id"],
            expected=Decimal(data["expected"]),
            actual=Decimal(data["actual"]),
            discrepancy=Decimal(data["discrepancy"]),
            discrepancy_pct=Decimal(data.get("discrepancy_pct", "0")),
            field_name=data.get("field_name", "amount"),
            auto_corrected=data.get("auto_corrected", False),
        )


@dataclass
class GasPriceRecord:
    """Record of gas price at a specific point in time during a backtest.

    This dataclass captures the gas price used for a trade execution, including
    the source of the gas price data for transparency and audit purposes.

    Attributes:
        timestamp: When this gas price was used
        gwei: Gas price in gwei
        source: Data source for the gas price. Valid values:
            - "historical": From historical gas provider (EtherscanGasPriceProvider)
            - "market_state": From MarketState.gas_price_gwei at simulation time
            - "config": Static value from config.gas_price_gwei (default fallback)
        usd_cost: Gas cost in USD for the associated trade
        eth_price_usd: ETH price in USD used for gas cost calculation (optional)
    """

    timestamp: datetime
    gwei: Decimal
    source: str
    usd_cost: Decimal = Decimal("0")
    eth_price_usd: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "gwei": str(self.gwei),
            "source": self.source,
            "usd_cost": str(self.usd_cost),
            "eth_price_usd": str(self.eth_price_usd) if self.eth_price_usd is not None else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GasPriceRecord":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized GasPriceRecord data

        Returns:
            GasPriceRecord instance
        """
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            gwei=Decimal(data["gwei"]),
            source=data["source"],
            usd_cost=Decimal(data.get("usd_cost", "0")),
            eth_price_usd=Decimal(data["eth_price_usd"]) if data.get("eth_price_usd") else None,
        )


@dataclass
class GasPriceSummary:
    """Summary statistics for gas prices used during a backtest.

    Provides aggregate statistics on gas prices for cost analysis and
    understanding gas price volatility impact on strategy performance.

    Attributes:
        min_gwei: Minimum gas price in gwei observed during backtest
        max_gwei: Maximum gas price in gwei observed during backtest
        mean_gwei: Mean gas price in gwei across all trades
        std_gwei: Standard deviation of gas prices in gwei
        source_breakdown: Count of gas price records by source type
            (e.g., {"historical": 80, "config": 20})
        total_records: Total number of gas price records
    """

    min_gwei: Decimal = Decimal("0")
    max_gwei: Decimal = Decimal("0")
    mean_gwei: Decimal = Decimal("0")
    std_gwei: Decimal = Decimal("0")
    source_breakdown: dict[str, int] = field(default_factory=dict)
    total_records: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "min_gwei": str(self.min_gwei),
            "max_gwei": str(self.max_gwei),
            "mean_gwei": str(self.mean_gwei),
            "std_gwei": str(self.std_gwei),
            "source_breakdown": self.source_breakdown,
            "total_records": self.total_records,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GasPriceSummary":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized GasPriceSummary data

        Returns:
            GasPriceSummary instance
        """
        return cls(
            min_gwei=Decimal(data.get("min_gwei", "0")),
            max_gwei=Decimal(data.get("max_gwei", "0")),
            mean_gwei=Decimal(data.get("mean_gwei", "0")),
            std_gwei=Decimal(data.get("std_gwei", "0")),
            source_breakdown=data.get("source_breakdown", {}),
            total_records=data.get("total_records", 0),
        )

    @classmethod
    def from_records(cls, records: list["GasPriceRecord"]) -> "GasPriceSummary":
        """Create summary from a list of gas price records.

        Args:
            records: List of GasPriceRecord instances

        Returns:
            GasPriceSummary with calculated statistics
        """
        if not records:
            return cls()

        gwei_values = [r.gwei for r in records]
        source_counts: dict[str, int] = {}
        for r in records:
            source_counts[r.source] = source_counts.get(r.source, 0) + 1

        # Calculate statistics
        min_gwei = min(gwei_values)
        max_gwei = max(gwei_values)
        mean_gwei = sum(gwei_values, Decimal("0")) / Decimal(len(gwei_values))

        # Calculate standard deviation
        if len(gwei_values) > 1:
            variance = sum((g - mean_gwei) ** 2 for g in gwei_values) / Decimal(len(gwei_values))
            # Use approximation for square root with Decimal
            std_gwei = variance.sqrt() if hasattr(variance, "sqrt") else Decimal(str(float(variance) ** 0.5))
        else:
            std_gwei = Decimal("0")

        return cls(
            min_gwei=min_gwei,
            max_gwei=max_gwei,
            mean_gwei=mean_gwei,
            std_gwei=std_gwei,
            source_breakdown=source_counts,
            total_records=len(records),
        )


@dataclass
class CrisisMetrics:
    """Crisis-specific metrics for comparing crisis vs normal period performance.

    This dataclass captures metrics specifically relevant for analyzing strategy
    performance during crisis periods, including drawdown analysis, recovery time,
    and comparison to normal market conditions.

    Attributes:
        scenario_name: Name of the crisis scenario (e.g., 'black_thursday')
        scenario_start: Start date of the crisis period
        scenario_end: End date of the crisis period
        scenario_duration_days: Duration of the crisis in days
        max_drawdown_pct: Maximum drawdown during the crisis period
        drawdown_start: Timestamp when drawdown began (peak)
        drawdown_trough: Timestamp when drawdown reached lowest point
        days_to_trough: Number of days from peak to trough
        recovery_time_days: Days to recover from trough to previous peak (None if not recovered)
        recovery_pct: Percentage recovered from trough (100% = full recovery)
        total_return_pct: Total return during the crisis period
        volatility: Annualized volatility during crisis
        sharpe_ratio: Sharpe ratio during crisis
        total_trades: Number of trades during crisis
        winning_trades: Number of winning trades
        losing_trades: Number of losing trades
        win_rate: Percentage of winning trades
        total_costs_usd: Total execution costs (fees + slippage + gas + MEV)
        normal_period_comparison: Comparison metrics vs normal periods (if available)
    """

    scenario_name: str
    scenario_start: datetime
    scenario_end: datetime
    scenario_duration_days: int
    max_drawdown_pct: Decimal = Decimal("0")
    drawdown_start: datetime | None = None
    drawdown_trough: datetime | None = None
    days_to_trough: int = 0
    recovery_time_days: int | None = None
    recovery_pct: Decimal = Decimal("0")
    total_return_pct: Decimal = Decimal("0")
    volatility: Decimal = Decimal("0")
    sharpe_ratio: Decimal = Decimal("0")
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: Decimal = Decimal("0")
    total_costs_usd: Decimal = Decimal("0")
    normal_period_comparison: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "scenario_name": self.scenario_name,
            "scenario_start": self.scenario_start.isoformat(),
            "scenario_end": self.scenario_end.isoformat(),
            "scenario_duration_days": self.scenario_duration_days,
            "max_drawdown_pct": str(self.max_drawdown_pct),
            "drawdown_start": self.drawdown_start.isoformat() if self.drawdown_start else None,
            "drawdown_trough": self.drawdown_trough.isoformat() if self.drawdown_trough else None,
            "days_to_trough": self.days_to_trough,
            "recovery_time_days": self.recovery_time_days,
            "recovery_pct": str(self.recovery_pct),
            "total_return_pct": str(self.total_return_pct),
            "volatility": str(self.volatility),
            "sharpe_ratio": str(self.sharpe_ratio),
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": str(self.win_rate),
            "total_costs_usd": str(self.total_costs_usd),
            "normal_period_comparison": self.normal_period_comparison,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CrisisMetrics":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized CrisisMetrics data

        Returns:
            CrisisMetrics instance
        """
        return cls(
            scenario_name=data["scenario_name"],
            scenario_start=datetime.fromisoformat(data["scenario_start"]),
            scenario_end=datetime.fromisoformat(data["scenario_end"]),
            scenario_duration_days=data["scenario_duration_days"],
            max_drawdown_pct=Decimal(data.get("max_drawdown_pct", "0")),
            drawdown_start=datetime.fromisoformat(data["drawdown_start"]) if data.get("drawdown_start") else None,
            drawdown_trough=datetime.fromisoformat(data["drawdown_trough"]) if data.get("drawdown_trough") else None,
            days_to_trough=data.get("days_to_trough", 0),
            recovery_time_days=data.get("recovery_time_days"),
            recovery_pct=Decimal(data.get("recovery_pct", "0")),
            total_return_pct=Decimal(data.get("total_return_pct", "0")),
            volatility=Decimal(data.get("volatility", "0")),
            sharpe_ratio=Decimal(data.get("sharpe_ratio", "0")),
            total_trades=data.get("total_trades", 0),
            winning_trades=data.get("winning_trades", 0),
            losing_trades=data.get("losing_trades", 0),
            win_rate=Decimal(data.get("win_rate", "0")),
            total_costs_usd=Decimal(data.get("total_costs_usd", "0")),
            normal_period_comparison=data.get("normal_period_comparison", {}),
        )

    def summary(self) -> str:
        """Generate a human-readable summary of crisis metrics.

        Returns:
            Formatted string with key crisis metrics
        """
        lines = [
            f"Crisis: {self.scenario_name}",
            f"Period: {self.scenario_start.strftime('%Y-%m-%d')} to {self.scenario_end.strftime('%Y-%m-%d')} ({self.scenario_duration_days} days)",
            "",
            "Drawdown Analysis:",
            f"  Max Drawdown: {self.max_drawdown_pct * 100:.2f}%",
            f"  Days to Trough: {self.days_to_trough}",
            f"  Recovery: {self.recovery_pct * 100:.1f}%"
            + (f" in {self.recovery_time_days} days" if self.recovery_time_days else " (not recovered)"),
            "",
            "Performance:",
            f"  Total Return: {self.total_return_pct * 100:.2f}%",
            f"  Volatility: {self.volatility * 100:.2f}%",
            f"  Sharpe Ratio: {self.sharpe_ratio:.3f}",
            "",
            "Trading:",
            f"  Total Trades: {self.total_trades}",
            f"  Win Rate: {self.win_rate * 100:.1f}%",
            f"  Total Costs: ${self.total_costs_usd:,.2f}",
        ]

        if self.normal_period_comparison:
            lines.extend(
                [
                    "",
                    "vs Normal Period:",
                    f"  Return Diff: {Decimal(self.normal_period_comparison.get('return_diff_pct', '0')) * 100:+.2f}%",
                    f"  Volatility Ratio: {Decimal(self.normal_period_comparison.get('volatility_ratio', '1')):.2f}x",
                    f"  Drawdown Ratio: {Decimal(self.normal_period_comparison.get('drawdown_ratio', '1')):.2f}x",
                ]
            )

        return "\n".join(lines)


@dataclass
class DataQualityReport:
    """Data quality metrics for backtest execution.

    This dataclass captures data quality information about the price and market
    data used during a backtest run. It helps identify potential issues with
    data accuracy that may affect backtest reliability.

    Attributes:
        coverage_ratio: Percentage of timestamps with valid prices (0.0-1.0).
            A value of 1.0 means all requested prices were available.
            Lower values indicate missing data that required fallback or interpolation.
        source_breakdown: Count of price lookups by provider (e.g., {"coingecko": 100, "chainlink": 50}).
            Shows which data sources were used and their relative contribution.
        stale_data_count: Number of prices that were older than the staleness threshold.
            High values indicate the backtest may have used outdated price data.
        interpolation_count: Number of data points that were interpolated or estimated.
            Interpolated data may be less accurate than direct observations.
        unresolved_token_count: Number of token addresses that could not be resolved to symbols.
            When require_symbol_mapping=True, unresolved tokens cause valuation to fail.
            High values indicate unknown tokens that may affect price lookup accuracy.
        gas_price_source_counts: Count of gas price lookups by source type.
            Tracks where ETH prices for gas calculations came from:
            - "override": User-provided gas_eth_price_override value
            - "historical": Historical ETH price from market state at simulation timestamp
            - "market": Current market ETH price from market state
            Example: {"override": 10, "historical": 90, "market": 0}
        missing_price_count: Number of unique tokens that had missing prices during valuation.
            These are tokens for which no price could be found from any provider.
            High values indicate potential data gaps that may affect valuation accuracy.
        missing_price_tokens: List of unique tokens (as chain_id:token strings) with missing prices.
            Format: ["1:unknown_token", "42161:weird_token"]
            Useful for identifying which specific tokens need price data support.
    """

    coverage_ratio: Decimal = Decimal("1.0")
    source_breakdown: dict[str, int] = field(default_factory=dict)
    stale_data_count: int = 0
    interpolation_count: int = 0
    unresolved_token_count: int = 0
    gas_price_source_counts: dict[str, int] = field(default_factory=dict)
    missing_price_count: int = 0
    missing_price_tokens: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "coverage_ratio": str(self.coverage_ratio),
            "source_breakdown": self.source_breakdown,
            "stale_data_count": self.stale_data_count,
            "interpolation_count": self.interpolation_count,
            "unresolved_token_count": self.unresolved_token_count,
            "gas_price_source_counts": self.gas_price_source_counts,
            "missing_price_count": self.missing_price_count,
            "missing_price_tokens": self.missing_price_tokens,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataQualityReport":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized DataQualityReport data

        Returns:
            DataQualityReport instance
        """
        return cls(
            coverage_ratio=Decimal(data.get("coverage_ratio", "1.0")),
            source_breakdown=data.get("source_breakdown", {}),
            stale_data_count=data.get("stale_data_count", 0),
            interpolation_count=data.get("interpolation_count", 0),
            unresolved_token_count=data.get("unresolved_token_count", 0),
            gas_price_source_counts=data.get("gas_price_source_counts", {}),
            missing_price_count=data.get("missing_price_count", 0),
            missing_price_tokens=data.get("missing_price_tokens", []),
        )


@dataclass
class PreflightCheckResult:
    """Result of a single preflight check.

    Captures the result of one specific preflight validation check,
    such as token price availability or archive node accessibility.

    Attributes:
        check_name: Name of the check (e.g., "token_price_availability")
        passed: Whether the check passed
        message: Human-readable description of the result
        details: Optional detailed information about the check result
        severity: Importance level if check failed ("error", "warning", "info")
    """

    check_name: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    severity: Literal["error", "warning", "info"] = "error"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "message": self.message,
            "details": self.details,
            "severity": self.severity,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PreflightCheckResult":
        """Deserialize from dictionary."""
        return cls(
            check_name=data["check_name"],
            passed=data["passed"],
            message=data["message"],
            details=data.get("details", {}),
            severity=data.get("severity", "error"),
        )


@dataclass
class PreflightReport:
    """Report from preflight validation before running a backtest.

    This dataclass captures the results of preflight validation checks
    performed before running a backtest. It helps identify data issues
    early, before spending time on a backtest that might fail.

    Preflight validation checks include:
    - Token price availability for all configured tokens
    - Data provider capability matching requirements (FULL vs CURRENT_ONLY)
    - Archive node accessibility if historical TWAP/Chainlink needed
    - Estimated data coverage based on provider capabilities

    Attributes:
        passed: Overall pass/fail status. True only if all critical checks pass.
        checks: List of individual check results with pass/fail and details.
        estimated_coverage: Estimated data coverage ratio (0.0-1.0) based on
            provider capabilities and token support. 1.0 means all data should
            be available; lower values indicate potential gaps.
        tokens_available: List of tokens confirmed to have price data available.
        tokens_unavailable: List of tokens that may have missing price data.
        provider_capabilities: Dict mapping provider names to their capability
            (FULL, CURRENT_ONLY, PRE_CACHE).
        archive_node_accessible: Whether archive node was confirmed accessible
            for historical queries (None if not tested).
        recommendations: List of actionable recommendations to fix issues.
        validation_time_seconds: Time taken to run preflight validation.
    """

    passed: bool
    checks: list[PreflightCheckResult] = field(default_factory=list)
    estimated_coverage: Decimal = Decimal("1.0")
    tokens_available: list[str] = field(default_factory=list)
    tokens_unavailable: list[str] = field(default_factory=list)
    provider_capabilities: dict[str, str] = field(default_factory=dict)
    archive_node_accessible: bool | None = None
    recommendations: list[str] = field(default_factory=list)
    validation_time_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "passed": self.passed,
            "checks": [c.to_dict() for c in self.checks],
            "estimated_coverage": str(self.estimated_coverage),
            "tokens_available": self.tokens_available,
            "tokens_unavailable": self.tokens_unavailable,
            "provider_capabilities": self.provider_capabilities,
            "archive_node_accessible": self.archive_node_accessible,
            "recommendations": self.recommendations,
            "validation_time_seconds": self.validation_time_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PreflightReport":
        """Deserialize from dictionary."""
        checks = [PreflightCheckResult.from_dict(c) for c in data.get("checks", [])]
        return cls(
            passed=data["passed"],
            checks=checks,
            estimated_coverage=Decimal(data.get("estimated_coverage", "1.0")),
            tokens_available=data.get("tokens_available", []),
            tokens_unavailable=data.get("tokens_unavailable", []),
            provider_capabilities=data.get("provider_capabilities", {}),
            archive_node_accessible=data.get("archive_node_accessible"),
            recommendations=data.get("recommendations", []),
            validation_time_seconds=data.get("validation_time_seconds", 0.0),
        )

    @property
    def failed_checks(self) -> list[PreflightCheckResult]:
        """Return list of checks that failed."""
        return [c for c in self.checks if not c.passed]

    @property
    def error_count(self) -> int:
        """Count of failed checks with severity 'error'."""
        return len([c for c in self.checks if not c.passed and c.severity == "error"])

    @property
    def warning_count(self) -> int:
        """Count of failed checks with severity 'warning'."""
        return len([c for c in self.checks if not c.passed and c.severity == "warning"])

    def summary(self) -> str:
        """Generate a human-readable summary of the preflight report."""
        status = "PASSED" if self.passed else "FAILED"
        lines = [
            f"Preflight Validation: {status}",
            f"Estimated Coverage: {self.estimated_coverage:.1%}",
            f"Tokens Available: {len(self.tokens_available)}/{len(self.tokens_available) + len(self.tokens_unavailable)}",
        ]

        if self.failed_checks:
            lines.append(f"Failed Checks: {len(self.failed_checks)}")
            for check in self.failed_checks:
                lines.append(f"  - [{check.severity.upper()}] {check.check_name}: {check.message}")

        if self.recommendations:
            lines.append("Recommendations:")
            for rec in self.recommendations:
                lines.append(f"  - {rec}")

        return "\n".join(lines)


@dataclass
class FeeAccrualResult:
    """Result of LP fee accrual calculation with confidence tracking.

    This dataclass captures the result of fee accrual calculations for LP positions,
    including confidence indicators that reflect the reliability of the fee estimate.

    Fee confidence levels:
        - high: Fee calculated using actual historical volume data from subgraph.
            Most accurate, based on real on-chain trading activity.
        - medium: Fee calculated using interpolated or estimated data.
            Reasonable accuracy, but not based on exact historical values.
        - low: Fee calculated using multiplier heuristic (position_value * multiplier).
            Least accurate, used when no historical data is available.

    Slippage confidence levels (same scale):
        - high: Slippage calculated using historical liquidity depth from subgraph.
        - medium: Slippage calculated using TWAP or estimated liquidity.
        - low: Slippage calculated using constant product fallback.

    Attributes:
        fees_usd: Total fees accrued in USD for this period.
        fee_confidence: Confidence level of the fee calculation ('high', 'medium', 'low').
            High=subgraph data, Medium=interpolated, Low=multiplier heuristic.
        data_source: Description of the data source used for the calculation.
            Examples: "subgraph:uniswap_v3", "interpolated:daily_average", "multiplier:10x".
        fees_token0: Fees attributed to token0 (in token units).
        fees_token1: Fees attributed to token1 (in token units).
        volume_usd: Trading volume used for fee calculation (if available).
        pool_address: Pool address the fees were calculated for.
        timestamp: Timestamp of the fee accrual calculation.
        slippage_confidence: Confidence level of slippage calculation ('high', 'medium', 'low').
            High=historical liquidity, Medium=TWAP, Low=constant product fallback.
        slippage_pct: Calculated slippage as percentage (e.g., 0.01 = 1%).
        liquidity_usd: Liquidity depth used for slippage calculation.
    """

    fees_usd: Decimal
    fee_confidence: Literal["high", "medium", "low"]
    data_source: str
    fees_token0: Decimal = Decimal("0")
    fees_token1: Decimal = Decimal("0")
    volume_usd: Decimal | None = None
    pool_address: str | None = None
    timestamp: datetime | None = None
    slippage_confidence: Literal["high", "medium", "low"] | None = None
    slippage_pct: Decimal | None = None
    liquidity_usd: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "fees_usd": str(self.fees_usd),
            "fee_confidence": self.fee_confidence,
            "data_source": self.data_source,
            "fees_token0": str(self.fees_token0),
            "fees_token1": str(self.fees_token1),
            "volume_usd": str(self.volume_usd) if self.volume_usd is not None else None,
            "pool_address": self.pool_address,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "slippage_confidence": self.slippage_confidence,
            "slippage_pct": str(self.slippage_pct) if self.slippage_pct is not None else None,
            "liquidity_usd": str(self.liquidity_usd) if self.liquidity_usd is not None else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FeeAccrualResult":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized FeeAccrualResult data

        Returns:
            FeeAccrualResult instance
        """
        return cls(
            fees_usd=Decimal(data["fees_usd"]),
            fee_confidence=data["fee_confidence"],
            data_source=data["data_source"],
            fees_token0=Decimal(data.get("fees_token0", "0")),
            fees_token1=Decimal(data.get("fees_token1", "0")),
            volume_usd=Decimal(data["volume_usd"]) if data.get("volume_usd") else None,
            pool_address=data.get("pool_address"),
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else None,
            slippage_confidence=data.get("slippage_confidence"),
            slippage_pct=Decimal(data["slippage_pct"]) if data.get("slippage_pct") else None,
            liquidity_usd=Decimal(data["liquidity_usd"]) if data.get("liquidity_usd") else None,
        )


@dataclass
class LPMetrics:
    """LP-specific data quality metrics for backtest results.

    Tracks the confidence levels and data sources used for LP fee calculations
    across all LP positions during the backtest.

    Attributes:
        position_count: Number of LP positions tracked
        fee_confidence_breakdown: Count of positions by fee confidence level
        data_sources: List of unique data sources used
        high_confidence_pct: Percentage of positions with HIGH confidence fee data
    """

    position_count: int = 0
    fee_confidence_breakdown: dict[str, int] = field(default_factory=lambda: {"high": 0, "medium": 0, "low": 0})
    data_sources: list[str] = field(default_factory=list)

    @property
    def high_confidence_pct(self) -> float:
        """Calculate percentage of HIGH confidence fee calculations."""
        if self.position_count == 0:
            return 0.0
        return (self.fee_confidence_breakdown.get("high", 0) / self.position_count) * 100

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "position_count": self.position_count,
            "fee_confidence_breakdown": self.fee_confidence_breakdown,
            "data_sources": self.data_sources,
            "high_confidence_pct": self.high_confidence_pct,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LPMetrics":
        """Deserialize from dictionary."""
        return cls(
            position_count=data.get("position_count", 0),
            fee_confidence_breakdown=data.get("fee_confidence_breakdown", {"high": 0, "medium": 0, "low": 0}),
            data_sources=data.get("data_sources", []),
        )


@dataclass
class PerpMetrics:
    """Perp-specific data quality metrics for backtest results.

    Tracks the confidence levels and data sources used for funding rate calculations
    across all perp positions during the backtest.

    Attributes:
        position_count: Number of perp positions tracked
        funding_confidence_breakdown: Count of positions by funding confidence level
        data_sources: List of unique data sources used
        high_confidence_pct: Percentage of positions with HIGH confidence funding data
    """

    position_count: int = 0
    funding_confidence_breakdown: dict[str, int] = field(default_factory=lambda: {"high": 0, "medium": 0, "low": 0})
    data_sources: list[str] = field(default_factory=list)

    @property
    def high_confidence_pct(self) -> float:
        """Calculate percentage of HIGH confidence funding calculations."""
        if self.position_count == 0:
            return 0.0
        return (self.funding_confidence_breakdown.get("high", 0) / self.position_count) * 100

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "position_count": self.position_count,
            "funding_confidence_breakdown": self.funding_confidence_breakdown,
            "data_sources": self.data_sources,
            "high_confidence_pct": self.high_confidence_pct,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PerpMetrics":
        """Deserialize from dictionary."""
        return cls(
            position_count=data.get("position_count", 0),
            funding_confidence_breakdown=data.get("funding_confidence_breakdown", {"high": 0, "medium": 0, "low": 0}),
            data_sources=data.get("data_sources", []),
        )


@dataclass
class LendingMetrics:
    """Lending-specific data quality metrics for backtest results.

    Tracks the confidence levels and data sources used for APY calculations
    across all lending positions during the backtest.

    Attributes:
        position_count: Number of lending positions tracked
        apy_confidence_breakdown: Count of positions by APY confidence level
        data_sources: List of unique data sources used
        high_confidence_pct: Percentage of positions with HIGH confidence APY data
    """

    position_count: int = 0
    apy_confidence_breakdown: dict[str, int] = field(default_factory=lambda: {"high": 0, "medium": 0, "low": 0})
    data_sources: list[str] = field(default_factory=list)

    @property
    def high_confidence_pct(self) -> float:
        """Calculate percentage of HIGH confidence APY calculations."""
        if self.position_count == 0:
            return 0.0
        return (self.apy_confidence_breakdown.get("high", 0) / self.position_count) * 100

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "position_count": self.position_count,
            "apy_confidence_breakdown": self.apy_confidence_breakdown,
            "data_sources": self.data_sources,
            "high_confidence_pct": self.high_confidence_pct,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LendingMetrics":
        """Deserialize from dictionary."""
        return cls(
            position_count=data.get("position_count", 0),
            apy_confidence_breakdown=data.get("apy_confidence_breakdown", {"high": 0, "medium": 0, "low": 0}),
            data_sources=data.get("data_sources", []),
        )


@dataclass
class SlippageMetrics:
    """Slippage-specific data quality metrics for backtest results.

    Tracks the confidence levels used for slippage calculations across all
    trades during the backtest.

    Attributes:
        calculation_count: Number of slippage calculations performed
        slippage_confidence_breakdown: Count of calculations by confidence level
        high_confidence_pct: Percentage of calculations with HIGH confidence
    """

    calculation_count: int = 0
    slippage_confidence_breakdown: dict[str, int] = field(default_factory=lambda: {"high": 0, "medium": 0, "low": 0})

    @property
    def high_confidence_pct(self) -> float:
        """Calculate percentage of HIGH confidence slippage calculations."""
        if self.calculation_count == 0:
            return 0.0
        return (self.slippage_confidence_breakdown.get("high", 0) / self.calculation_count) * 100

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "calculation_count": self.calculation_count,
            "slippage_confidence_breakdown": self.slippage_confidence_breakdown,
            "high_confidence_pct": self.high_confidence_pct,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SlippageMetrics":
        """Deserialize from dictionary."""
        return cls(
            calculation_count=data.get("calculation_count", 0),
            slippage_confidence_breakdown=data.get("slippage_confidence_breakdown", {"high": 0, "medium": 0, "low": 0}),
        )


@dataclass
class DataCoverageMetrics:
    """Aggregated data coverage metrics for backtest results.

    Combines metrics from all position types (LP, Perp, Lending) and slippage
    calculations to provide an overall view of data quality in the backtest.

    Attributes:
        lp_metrics: LP-specific data quality metrics
        perp_metrics: Perp-specific data quality metrics
        lending_metrics: Lending-specific data quality metrics
        slippage_metrics: Slippage-specific data quality metrics
        data_coverage_pct: Overall percentage of data points from HIGH confidence sources
        total_data_points: Total number of data points across all categories
        high_confidence_data_points: Number of HIGH confidence data points
    """

    lp_metrics: LPMetrics = field(default_factory=LPMetrics)
    perp_metrics: PerpMetrics = field(default_factory=PerpMetrics)
    lending_metrics: LendingMetrics = field(default_factory=LendingMetrics)
    slippage_metrics: SlippageMetrics = field(default_factory=SlippageMetrics)

    @property
    def total_data_points(self) -> int:
        """Calculate total number of data points across all categories."""
        return (
            self.lp_metrics.position_count
            + self.perp_metrics.position_count
            + self.lending_metrics.position_count
            + self.slippage_metrics.calculation_count
        )

    @property
    def high_confidence_data_points(self) -> int:
        """Calculate number of HIGH confidence data points."""
        return (
            self.lp_metrics.fee_confidence_breakdown.get("high", 0)
            + self.perp_metrics.funding_confidence_breakdown.get("high", 0)
            + self.lending_metrics.apy_confidence_breakdown.get("high", 0)
            + self.slippage_metrics.slippage_confidence_breakdown.get("high", 0)
        )

    @property
    def data_coverage_pct(self) -> float:
        """Calculate overall data coverage as percentage of HIGH confidence data points."""
        if self.total_data_points == 0:
            return 100.0  # No data points = no coverage issues (vacuously true)
        return (self.high_confidence_data_points / self.total_data_points) * 100

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "lp_metrics": self.lp_metrics.to_dict(),
            "perp_metrics": self.perp_metrics.to_dict(),
            "lending_metrics": self.lending_metrics.to_dict(),
            "slippage_metrics": self.slippage_metrics.to_dict(),
            "data_coverage_pct": self.data_coverage_pct,
            "total_data_points": self.total_data_points,
            "high_confidence_data_points": self.high_confidence_data_points,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataCoverageMetrics":
        """Deserialize from dictionary."""
        return cls(
            lp_metrics=LPMetrics.from_dict(data.get("lp_metrics", {})),
            perp_metrics=PerpMetrics.from_dict(data.get("perp_metrics", {})),
            lending_metrics=LendingMetrics.from_dict(data.get("lending_metrics", {})),
            slippage_metrics=SlippageMetrics.from_dict(data.get("slippage_metrics", {})),
        )


class IntentType(StrEnum):
    """Types of intents that can be executed during backtesting."""

    SWAP = "SWAP"
    LP_OPEN = "LP_OPEN"
    LP_CLOSE = "LP_CLOSE"
    BORROW = "BORROW"
    REPAY = "REPAY"
    SUPPLY = "SUPPLY"
    WITHDRAW = "WITHDRAW"
    PERP_OPEN = "PERP_OPEN"
    PERP_CLOSE = "PERP_CLOSE"
    BRIDGE = "BRIDGE"
    HOLD = "HOLD"
    UNKNOWN = "UNKNOWN"


@dataclass
class EquityPoint:
    """A single point on the equity curve.

    Attributes:
        timestamp: When this value was recorded
        value_usd: Portfolio value in USD at this timestamp
    """

    timestamp: datetime
    value_usd: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "value_usd": str(self.value_usd),
        }


@dataclass
class TradeRecord:
    """Record of a single trade executed during backtest.

    This model captures all details of trade execution including
    fees, slippage, and gas costs for accurate PnL calculation.

    Attributes:
        timestamp: When the trade was executed
        intent_type: Type of intent that was executed (SWAP, LP_OPEN, etc.)
        executed_price: Actual execution price (for swaps/perps)
        fee_usd: Protocol/exchange fee in USD
        slippage_usd: Slippage cost in USD (difference from expected price)
        gas_cost_usd: Gas cost in USD
        pnl_usd: Realized PnL from this trade
        success: Whether the trade succeeded
        amount_usd: Notional amount of the trade in USD
        protocol: Protocol used (uniswap_v3, aave_v3, gmx, etc.)
        tokens: Tokens involved in the trade
        tx_hash: Transaction hash (if available)
        error: Error message if trade failed
        metadata: Additional trade-specific metadata
        il_loss_usd: Impermanent loss in USD (for LP positions, negative = loss)
        fees_earned_usd: Trading fees earned in USD (for LP positions)
        net_lp_pnl_usd: Net LP PnL = (Current Value + Fees) - Initial Value - IL
        gas_price_gwei: Gas price in gwei used for this trade (for gas cost analysis)
        estimated_mev_cost_usd: Estimated MEV (sandwich attack) cost in USD (None if MEV simulation disabled)
        delayed_at_end: Whether this trade was executed at simulation end from pending intents queue
    """

    timestamp: datetime
    intent_type: IntentType
    executed_price: Decimal
    fee_usd: Decimal
    slippage_usd: Decimal
    gas_cost_usd: Decimal
    pnl_usd: Decimal
    success: bool
    amount_usd: Decimal = Decimal("0")
    protocol: str = ""
    tokens: list[str] = field(default_factory=list)
    tx_hash: str | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    actual_amount_in: Decimal | None = None
    actual_amount_out: Decimal | None = None
    expected_amount_in: Decimal | None = None
    expected_amount_out: Decimal | None = None
    il_loss_usd: Decimal | None = None
    fees_earned_usd: Decimal | None = None
    net_lp_pnl_usd: Decimal | None = None
    gas_price_gwei: Decimal | None = None
    estimated_mev_cost_usd: Decimal | None = None
    delayed_at_end: bool = False

    @property
    def net_pnl_usd(self) -> Decimal:
        """Get net PnL after fees, slippage, and gas."""
        return self.pnl_usd - self.fee_usd - self.slippage_usd - self.gas_cost_usd

    @property
    def total_cost_usd(self) -> Decimal:
        """Get total execution cost (fees + slippage + gas)."""
        return self.fee_usd + self.slippage_usd + self.gas_cost_usd

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "intent_type": self.intent_type.value,
            "executed_price": str(self.executed_price),
            "fee_usd": str(self.fee_usd),
            "slippage_usd": str(self.slippage_usd),
            "gas_cost_usd": str(self.gas_cost_usd),
            "pnl_usd": str(self.pnl_usd),
            "net_pnl_usd": str(self.net_pnl_usd),
            "success": self.success,
            "amount_usd": str(self.amount_usd),
            "protocol": self.protocol,
            "tokens": self.tokens,
            "tx_hash": self.tx_hash,
            "error": self.error,
            "metadata": self.metadata,
            "actual_amount_in": str(self.actual_amount_in) if self.actual_amount_in is not None else None,
            "actual_amount_out": str(self.actual_amount_out) if self.actual_amount_out is not None else None,
            "expected_amount_in": str(self.expected_amount_in) if self.expected_amount_in is not None else None,
            "expected_amount_out": str(self.expected_amount_out) if self.expected_amount_out is not None else None,
            "il_loss_usd": str(self.il_loss_usd) if self.il_loss_usd is not None else None,
            "fees_earned_usd": str(self.fees_earned_usd) if self.fees_earned_usd is not None else None,
            "net_lp_pnl_usd": str(self.net_lp_pnl_usd) if self.net_lp_pnl_usd is not None else None,
            "gas_price_gwei": str(self.gas_price_gwei) if self.gas_price_gwei is not None else None,
            "estimated_mev_cost_usd": str(self.estimated_mev_cost_usd)
            if self.estimated_mev_cost_usd is not None
            else None,
            "delayed_at_end": self.delayed_at_end,
        }


@dataclass
class BacktestMetrics:
    """Performance metrics calculated from backtest results.

    All financial values are in USD. Ratios are decimal (0.1 = 10%).

    Attributes:
        total_pnl_usd: Total PnL before execution costs
        net_pnl_usd: Net PnL after all execution costs
        sharpe_ratio: Risk-adjusted return (annualized, assuming 0 risk-free rate)
        max_drawdown_pct: Maximum peak-to-trough decline as decimal (0.1 = 10%)
        win_rate: Percentage of profitable trades as decimal (0.6 = 60%)
        total_trades: Total number of trades executed
        profit_factor: Ratio of gross profit to gross loss
        total_return_pct: Total return as decimal (0.15 = 15% return)
        annualized_return_pct: Annualized return as decimal
        total_fees_usd: Total protocol fees paid
        total_slippage_usd: Total slippage incurred
        total_gas_usd: Total gas costs
        winning_trades: Number of profitable trades
        losing_trades: Number of losing trades
        avg_trade_pnl_usd: Average PnL per trade
        largest_win_usd: Largest single winning trade
        largest_loss_usd: Largest single losing trade
        avg_win_usd: Average winning trade PnL
        avg_loss_usd: Average losing trade PnL
        volatility: Annualized volatility of returns as decimal
        sortino_ratio: Downside risk-adjusted return
        calmar_ratio: Return / max drawdown
        total_fees_earned_usd: Total fees earned from LP positions in USD
        fees_by_pool: Dict mapping pool identifier to fees earned in USD
        total_funding_paid: Total funding payments made from perp positions in USD
        total_funding_received: Total funding payments received by perp positions in USD
        liquidations_count: Number of liquidation events that occurred
        liquidation_losses_usd: Total losses from liquidations in USD
        max_margin_utilization: Maximum margin utilization ratio observed during backtest (0-1)
        total_interest_earned: Total interest earned from lending supply positions in USD
        total_interest_paid: Total interest paid on borrow positions in USD
        min_health_factor: Minimum health factor observed for lending positions during backtest (lower = more risk)
        health_factor_warnings: Number of times health factor dropped below warning threshold
        avg_gas_price_gwei: Average gas price in gwei across all trades (for cost analysis)
        max_gas_price_gwei: Maximum gas price in gwei observed during backtest (for peak cost analysis)
        total_gas_cost_usd: Total gas costs in USD (same as total_gas_usd, kept for API consistency)
        total_mev_cost_usd: Total estimated MEV (sandwich attack) costs in USD across all trades
        total_leverage: Total portfolio leverage ratio (sum of all position notionals / equity)
        max_net_delta: Maximum net delta exposure observed per asset (token symbol -> max delta)
        correlation_risk: Portfolio correlation risk score (0-1, higher = more correlated positions)
        liquidation_cascade_risk: Risk of cascading liquidations across protocols (0-1, higher = more risk)
        information_ratio: Information ratio measuring risk-adjusted excess return vs benchmark (None if not calculated)
        beta: Portfolio beta measuring sensitivity to benchmark movements (None if not calculated)
        alpha: Jensen's alpha measuring excess return beyond what beta would predict (None if not calculated)
        benchmark_return: Total return of the benchmark over the backtest period as decimal (None if not calculated)
        pnl_by_protocol: PnL breakdown by protocol (e.g., {"uniswap_v3": Decimal("100"), "aave_v3": Decimal("-50")})
        pnl_by_intent_type: PnL breakdown by intent type (e.g., {"SWAP": Decimal("75"), "LP_OPEN": Decimal("25")})
        pnl_by_asset: PnL breakdown by asset (e.g., {"ETH": Decimal("80"), "USDC": Decimal("20")})
        realized_pnl: Total realized PnL from closed positions in USD
        unrealized_pnl: Total unrealized PnL from open positions in USD
    """

    total_pnl_usd: Decimal = Decimal("0")
    net_pnl_usd: Decimal = Decimal("0")
    sharpe_ratio: Decimal = Decimal("0")
    max_drawdown_pct: Decimal = Decimal("0")
    win_rate: Decimal = Decimal("0")
    total_trades: int = 0
    profit_factor: Decimal = Decimal("0")
    total_return_pct: Decimal = Decimal("0")
    annualized_return_pct: Decimal = Decimal("0")
    total_fees_usd: Decimal = Decimal("0")
    total_slippage_usd: Decimal = Decimal("0")
    total_gas_usd: Decimal = Decimal("0")
    winning_trades: int = 0
    losing_trades: int = 0
    avg_trade_pnl_usd: Decimal = Decimal("0")
    largest_win_usd: Decimal = Decimal("0")
    largest_loss_usd: Decimal = Decimal("0")
    avg_win_usd: Decimal = Decimal("0")
    avg_loss_usd: Decimal = Decimal("0")
    volatility: Decimal = Decimal("0")
    sortino_ratio: Decimal = Decimal("0")
    calmar_ratio: Decimal = Decimal("0")
    total_fees_earned_usd: Decimal = Decimal("0")
    fees_by_pool: dict[str, Decimal] = field(default_factory=dict)
    lp_fee_confidence_breakdown: dict[str, int] = field(default_factory=dict)
    """Count of LP positions by fee confidence level.

    Example: {"high": 2, "medium": 1, "low": 0}
    - high: Fees calculated using actual historical volume data from subgraph
    - medium: Fees calculated using interpolated or estimated data
    - low: Fees calculated using multiplier heuristic
    """
    total_funding_paid: Decimal = Decimal("0")
    total_funding_received: Decimal = Decimal("0")
    liquidations_count: int = 0
    liquidation_losses_usd: Decimal = Decimal("0")
    max_margin_utilization: Decimal = Decimal("0")
    total_interest_earned: Decimal = Decimal("0")
    total_interest_paid: Decimal = Decimal("0")
    min_health_factor: Decimal = Decimal("999")
    health_factor_warnings: int = 0
    avg_gas_price_gwei: Decimal = Decimal("0")
    max_gas_price_gwei: Decimal = Decimal("0")
    total_gas_cost_usd: Decimal = Decimal("0")
    total_mev_cost_usd: Decimal = Decimal("0")
    total_leverage: Decimal = Decimal("0")
    max_net_delta: dict[str, Decimal] = field(default_factory=dict)
    correlation_risk: Decimal | None = None
    liquidation_cascade_risk: Decimal = Decimal("0")
    information_ratio: Decimal | None = None
    beta: Decimal | None = None
    alpha: Decimal | None = None
    benchmark_return: Decimal | None = None
    pnl_by_protocol: dict[str, Decimal] = field(default_factory=dict)
    pnl_by_intent_type: dict[str, Decimal] = field(default_factory=dict)
    pnl_by_asset: dict[str, Decimal] = field(default_factory=dict)
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")

    @property
    def total_execution_cost_usd(self) -> Decimal:
        """Get total execution costs (fees + slippage + gas)."""
        return self.total_fees_usd + self.total_slippage_usd + self.total_gas_usd

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "total_pnl_usd": str(self.total_pnl_usd),
            "net_pnl_usd": str(self.net_pnl_usd),
            "sharpe_ratio": str(self.sharpe_ratio),
            "max_drawdown_pct": str(self.max_drawdown_pct),
            "win_rate": str(self.win_rate),
            "total_trades": self.total_trades,
            "profit_factor": str(self.profit_factor),
            "total_return_pct": str(self.total_return_pct),
            "annualized_return_pct": str(self.annualized_return_pct),
            "total_fees_usd": str(self.total_fees_usd),
            "total_slippage_usd": str(self.total_slippage_usd),
            "total_gas_usd": str(self.total_gas_usd),
            "total_execution_cost_usd": str(self.total_execution_cost_usd),
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "avg_trade_pnl_usd": str(self.avg_trade_pnl_usd),
            "largest_win_usd": str(self.largest_win_usd),
            "largest_loss_usd": str(self.largest_loss_usd),
            "avg_win_usd": str(self.avg_win_usd),
            "avg_loss_usd": str(self.avg_loss_usd),
            "volatility": str(self.volatility),
            "sortino_ratio": str(self.sortino_ratio),
            "calmar_ratio": str(self.calmar_ratio),
            "total_fees_earned_usd": str(self.total_fees_earned_usd),
            "fees_by_pool": {k: str(v) for k, v in self.fees_by_pool.items()},
            "lp_fee_confidence_breakdown": self.lp_fee_confidence_breakdown,
            "total_funding_paid": str(self.total_funding_paid),
            "total_funding_received": str(self.total_funding_received),
            "liquidations_count": self.liquidations_count,
            "liquidation_losses_usd": str(self.liquidation_losses_usd),
            "max_margin_utilization": str(self.max_margin_utilization),
            "total_interest_earned": str(self.total_interest_earned),
            "total_interest_paid": str(self.total_interest_paid),
            "min_health_factor": str(self.min_health_factor),
            "health_factor_warnings": self.health_factor_warnings,
            "avg_gas_price_gwei": str(self.avg_gas_price_gwei),
            "max_gas_price_gwei": str(self.max_gas_price_gwei),
            "total_gas_cost_usd": str(self.total_gas_cost_usd),
            "total_mev_cost_usd": str(self.total_mev_cost_usd),
            "total_leverage": str(self.total_leverage),
            "max_net_delta": {k: str(v) for k, v in self.max_net_delta.items()},
            "correlation_risk": str(self.correlation_risk) if self.correlation_risk is not None else None,
            "liquidation_cascade_risk": str(self.liquidation_cascade_risk),
            "information_ratio": str(self.information_ratio) if self.information_ratio is not None else None,
            "beta": str(self.beta) if self.beta is not None else None,
            "alpha": str(self.alpha) if self.alpha is not None else None,
            "benchmark_return": str(self.benchmark_return) if self.benchmark_return is not None else None,
            "pnl_by_protocol": {k: str(v) for k, v in self.pnl_by_protocol.items()},
            "pnl_by_intent_type": {k: str(v) for k, v in self.pnl_by_intent_type.items()},
            "pnl_by_asset": {k: str(v) for k, v in self.pnl_by_asset.items()},
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
        }


@dataclass
class AggregatedPortfolioView:
    """Aggregated portfolio view with snapshots for tick-by-tick analysis.

    This dataclass holds the complete portfolio state history across all ticks,
    enabling detailed analysis of portfolio evolution and risk over time.

    Attributes:
        snapshots: List of portfolio snapshots at each tick (serialized dicts)
        final_risk_score: Final unified risk score at end of backtest
        max_risk_score: Maximum risk score observed during backtest
        avg_risk_score: Average risk score across all ticks
        risk_score_history: List of risk scores at each snapshot timestamp
    """

    snapshots: list[dict[str, Any]] = field(default_factory=list)
    final_risk_score: Decimal = Decimal("0")
    max_risk_score: Decimal = Decimal("0")
    avg_risk_score: Decimal = Decimal("0")
    risk_score_history: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "snapshots": self.snapshots,
            "final_risk_score": str(self.final_risk_score),
            "max_risk_score": str(self.max_risk_score),
            "avg_risk_score": str(self.avg_risk_score),
            "risk_score_history": self.risk_score_history,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AggregatedPortfolioView":
        """Deserialize from dictionary."""
        return cls(
            snapshots=data.get("snapshots", []),
            final_risk_score=Decimal(data.get("final_risk_score", "0")),
            max_risk_score=Decimal(data.get("max_risk_score", "0")),
            avg_risk_score=Decimal(data.get("avg_risk_score", "0")),
            risk_score_history=data.get("risk_score_history", []),
        )

    def add_snapshot(self, snapshot_dict: dict[str, Any]) -> None:
        """Add a portfolio snapshot and update risk score tracking.

        Args:
            snapshot_dict: Serialized PortfolioSnapshot from PortfolioAggregator.create_snapshot()
        """
        self.snapshots.append(snapshot_dict)

        # Extract and track risk score
        risk_score_data = snapshot_dict.get("risk_score")
        if risk_score_data:
            score = Decimal(risk_score_data.get("score", "0"))
            timestamp = snapshot_dict.get("timestamp", "")

            self.risk_score_history.append(
                {
                    "timestamp": timestamp,
                    "score": str(score),
                }
            )

            # Update max
            if score > self.max_risk_score:
                self.max_risk_score = score

            # Update final (always the latest)
            self.final_risk_score = score

            # Update average
            if self.risk_score_history:
                total = sum(Decimal(h["score"]) for h in self.risk_score_history)
                self.avg_risk_score = total / Decimal(len(self.risk_score_history))


@dataclass
class BacktestResult:
    """Complete results from a backtest run.

    This model is used by both the PnL Backtester and Paper Trader
    to provide consistent result formatting and analysis.

    Attributes:
        engine: Which backtesting engine was used (pnl or paper)
        strategy_id: Identifier of the strategy being tested
        start_time: When the backtest started (simulation time)
        end_time: When the backtest ended (simulation time)
        metrics: Calculated performance metrics
        trades: List of all trade records
        equity_curve: Portfolio value over time
        initial_capital_usd: Starting capital in USD
        final_capital_usd: Ending capital in USD
        chain: Target blockchain (arbitrum, base, etc.)
        run_started_at: When the backtest run actually started (wall time)
        run_ended_at: When the backtest run actually completed (wall time)
        run_duration_seconds: Wall clock duration of the backtest run
        config: Configuration used for the backtest
        error: Error message if backtest failed
        lending_liquidations: List of lending liquidation events that occurred
        aggregated_portfolio_view: Tick-by-tick portfolio state snapshots with risk scores
        reconciliation_events: List of position reconciliation events (discrepancies detected)
        walk_forward_results: Results from walk-forward optimization (if run with --walk-forward)
        monte_carlo_results: Results from Monte Carlo simulation (if run with --monte-carlo).
            Contains return confidence intervals, drawdown probabilities, and path statistics.
        crisis_results: Crisis-specific metrics when backtest was run during a crisis scenario.
            Contains drawdown analysis, recovery time, and comparison to normal period performance.
        errors: List of error records as dictionaries with timestamps and context for debugging
            and analysis. Each error dict contains: timestamp, error_type, error_message,
            classification (with error_type, category, is_recoverable, is_fatal, is_non_critical,
            suggested_action), context, and handled action.
        backtest_id: Unique correlation ID (UUID) for this backtest run. Used for structured
            logging and tracing across all log messages generated during this backtest.
        phase_timings: List of phase timing records showing how long each backtest phase took.
            Each record contains: phase_name, start_time, end_time, duration_seconds, error.
            Useful for performance analysis and identifying bottlenecks.
        config_hash: SHA-256 hash of the configuration used for this backtest. Enables
            verification that a backtest was run with identical configuration. Calculated
            from all parameters that affect backtest results, excluding runtime metadata.
        execution_delayed_at_end: Count of pending intents executed at simulation end.
            These were queued due to inclusion_delay_blocks > 0 and executed with the
            last market state when the simulation completed.
        data_source_capabilities: Dictionary mapping data provider names to their
            HistoricalDataCapability enum values. Shows which providers were used and
            their ability to provide accurate historical data (FULL, CURRENT_ONLY, PRE_CACHE).
            Useful for understanding potential data quality limitations in the backtest.
        data_source_warnings: List of warning messages about data source limitations.
            Generated when providers with CURRENT_ONLY or PRE_CACHE capability are used,
            as these may affect backtest accuracy.
        data_quality: Data quality metrics for the backtest run. Includes coverage ratio,
            source breakdown, stale data count, and interpolation count. Useful for
            understanding data reliability and identifying potential accuracy issues.
        institutional_compliance: Whether the backtest run meets institutional standards.
            Set to False when any strict reproducibility, data quality, or compliance
            check fails. Use compliance_violations to see which checks failed.
        compliance_violations: List of compliance violations that caused institutional_compliance
            to be set to False. Each entry describes a specific compliance failure such as
            "CURRENT_ONLY data provider used", "Symbol mapping failed for 0x...",
            "Data coverage below minimum threshold (95% < 98%)".
        fallback_usage: Dictionary tracking count of each fallback type used during the backtest.
            Keys include: "hardcoded_price", "default_gas_price", "default_usd_amount".
            Empty dict means no fallbacks were used, which is the desired state for
            institutional-grade backtests.
        preflight_report: Preflight validation report from checks run before the backtest.
            Contains pass/fail status, individual check results, estimated data coverage,
            and recommendations for fixing any issues. None if preflight validation was disabled.
        preflight_passed: Whether preflight validation passed (True) or failed (False).
            Defaults to True if preflight validation was disabled. This is a convenience
            field for quick checks - for full details, inspect preflight_report.
        parameter_sources: Tracks the source of all configuration parameters for audit purposes.
            Contains detailed records of where each configuration value came from (default,
            config_file, env_var, explicit) for config parameters, (asset_specific,
            protocol_default, global_default) for liquidation thresholds, and (historical,
            fixed, provider) for APY/funding rates. Critical for institutional compliance.
        accuracy_estimate: Estimated accuracy of this backtest based on strategy type and data
            quality tier. Provides expected accuracy range (e.g., "90-95%") and primary error
            source. Derived from ACCURACY_MATRIX based on documented accuracy limitations.
    """

    engine: BacktestEngine
    strategy_id: str
    start_time: datetime
    end_time: datetime
    metrics: BacktestMetrics
    trades: list[TradeRecord] = field(default_factory=list)
    equity_curve: list[EquityPoint] = field(default_factory=list)
    initial_capital_usd: Decimal = Decimal("10000")
    final_capital_usd: Decimal = Decimal("10000")
    chain: str = "arbitrum"
    run_started_at: datetime | None = None
    run_ended_at: datetime | None = None
    run_duration_seconds: float = 0.0
    config: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    lending_liquidations: list[LendingLiquidationEvent] = field(default_factory=list)
    aggregated_portfolio_view: AggregatedPortfolioView | None = None
    reconciliation_events: list[ReconciliationEvent] = field(default_factory=list)
    walk_forward_results: "WalkForwardResult | None" = None
    monte_carlo_results: "MonteCarloSimulationResult | None" = None
    crisis_results: CrisisMetrics | None = None
    errors: list[dict[str, Any]] = field(default_factory=list)
    backtest_id: str | None = None
    phase_timings: list[dict[str, Any]] = field(default_factory=list)
    config_hash: str | None = None
    execution_delayed_at_end: int = 0
    data_source_capabilities: dict[str, "HistoricalDataCapability"] = field(default_factory=dict)
    data_source_warnings: list[str] = field(default_factory=list)
    data_quality: DataQualityReport | None = None
    institutional_compliance: bool = True
    compliance_violations: list[str] = field(default_factory=list)
    fallback_usage: dict[str, int] = field(default_factory=dict)
    preflight_report: PreflightReport | None = None
    preflight_passed: bool = True
    gas_prices_used: list[GasPriceRecord] = field(default_factory=list)
    """Optional detailed gas price records for each trade during the backtest.

    When track_gas_prices=True in config, this list contains a GasPriceRecord
    for each trade showing the gas price used, its source, and USD cost.
    Useful for detailed gas cost analysis but may increase result size.
    """
    gas_price_summary: GasPriceSummary | None = None
    """Summary statistics for gas prices used during the backtest.

    Contains min, max, mean, std of gas prices in gwei plus source breakdown.
    Always populated when trades occurred, regardless of track_gas_prices setting.
    """
    parameter_sources: ParameterSourceTracker | None = None
    """Tracks the source of all configuration parameters for audit purposes.

    Contains detailed records of where each configuration value came from:
    - Config parameters: default, config_file, env_var, explicit
    - Liquidation thresholds: asset_specific, protocol_default, global_default
    - APY/funding rates: historical, fixed, provider

    This information is critical for institutional compliance and audit trails.
    When institutional_mode=True, this is always populated. The tracker provides
    summary dicts (config_sources, liquidation_sources, apy_funding_sources)
    for quick inspection and a full list of ParameterSourceRecord objects
    for detailed analysis.
    """
    accuracy_estimate: AccuracyEstimate | None = None
    """Estimated accuracy of this backtest based on strategy type and data quality.

    Provides a quick reference showing expected accuracy range (e.g., "90-95%")
    based on the detected strategy type (LP, perp, lending, arbitrage, spot)
    and the data quality tier used (FULL, PRE_CACHE, CURRENT_ONLY).

    The estimate is derived from the ACCURACY_MATRIX which is based on
    documented accuracy limitations and golden test tolerances. See
    docs/ACCURACY_LIMITATIONS.md for the full accuracy matrix and methodology.

    Example usage:
        if result.accuracy_estimate:
            print(f"Expected accuracy: {result.accuracy_estimate.confidence_interval}")
            print(f"Primary error source: {result.accuracy_estimate.primary_error_source}")
    """
    data_coverage_metrics: DataCoverageMetrics | None = None
    """Data coverage metrics tracking confidence levels across all position types.

    Provides detailed breakdown of data quality for LP, Perp, Lending, and Slippage
    calculations. Includes confidence level breakdowns (high/medium/low) and
    data sources used for each position type.

    The data_coverage_pct property gives overall percentage of HIGH confidence
    data points across all categories.

    Example usage:
        if result.data_coverage_metrics:
            print(f"Data coverage: {result.data_coverage_metrics.data_coverage_pct:.1f}%")
            print(f"LP HIGH: {result.data_coverage_metrics.lp_metrics.high_confidence_pct:.1f}%")
    """

    @property
    def success(self) -> bool:
        """Check if backtest completed successfully."""
        return self.error is None

    @property
    def simulation_duration_days(self) -> float:
        """Get the simulated duration in days."""
        delta = self.end_time - self.start_time
        return delta.total_seconds() / (24 * 3600)

    @property
    def total_return_pct(self) -> Decimal:
        """Get total return as a percentage."""
        if self.initial_capital_usd == 0:
            return Decimal("0")
        return (self.final_capital_usd - self.initial_capital_usd) / self.initial_capital_usd

    @property
    def used_any_fallback(self) -> bool:
        """Check if any fallbacks were used during the backtest.

        Returns True if the fallback_usage dict has any non-zero counts.
        When this is True, the backtest may have reduced accuracy due to
        using fallback values instead of real market data.
        """
        return any(count > 0 for count in self.fallback_usage.values())

    def add_error(self, error_dict: dict[str, Any]) -> None:
        """Add an error record and log it with timestamp and context.

        This method is used to track errors that occurred during the backtest,
        along with their timestamps, classification, and handling.

        Args:
            error_dict: Serialized error record from ErrorRecord.to_dict() or
                equivalent dict with keys: timestamp, error_type, error_message,
                classification, context, handled
        """
        import logging

        logger = logging.getLogger(__name__)

        self.errors.append(error_dict)

        # Log the error with timestamp and context
        timestamp = error_dict.get("timestamp", "unknown")
        error_type = error_dict.get("error_type", "Unknown")
        error_message = error_dict.get("error_message", "No message")
        context = error_dict.get("context", "")
        handled = error_dict.get("handled", "")
        classification = error_dict.get("classification", {})

        context_str = f"[{context}] " if context else ""
        handled_str = f" (handled: {handled})" if handled else ""

        if classification.get("is_fatal"):
            logger.error(f"Backtest error at {timestamp}: {context_str}{error_type} - {error_message}{handled_str}")
        elif classification.get("is_recoverable"):
            logger.warning(
                f"Recoverable error at {timestamp}: {context_str}{error_type} - {error_message}{handled_str}"
            )
        else:
            logger.info(f"Non-critical error at {timestamp}: {context_str}{error_type} - {error_message}{handled_str}")

    def summary(self) -> str:
        """Generate a human-readable summary of backtest results.

        Returns:
            Multi-line string with formatted backtest results
        """
        lines = [
            "=" * 70,
            f"BACKTEST RESULTS - {self.engine.value.upper()} ENGINE",
            "=" * 70,
            "",
            "CONFIGURATION",
            "-" * 70,
            f"Strategy:           {self.strategy_id}",
            f"Chain:              {self.chain}",
            f"Period:             {self.start_time.strftime('%Y-%m-%d')} to {self.end_time.strftime('%Y-%m-%d')}",
            f"Duration:           {self.simulation_duration_days:.1f} days",
            f"Initial Capital:    ${self.initial_capital_usd:,.2f}",
            "",
            "PERFORMANCE",
            "-" * 70,
            f"Final Capital:      ${self.final_capital_usd:,.2f}",
            f"Net PnL:            ${self.metrics.net_pnl_usd:,.2f}",
            f"Total Return:       {self.metrics.total_return_pct * 100:.2f}%",
            f"Annualized Return:  {self.metrics.annualized_return_pct * 100:.2f}%",
            f"Sharpe Ratio:       {self.metrics.sharpe_ratio:.3f}",
            f"Sortino Ratio:      {self.metrics.sortino_ratio:.3f}",
            f"Max Drawdown:       {self.metrics.max_drawdown_pct * 100:.2f}%",
            f"Calmar Ratio:       {self.metrics.calmar_ratio:.3f}",
            "",
            "TRADING STATISTICS",
            "-" * 70,
            f"Total Trades:       {self.metrics.total_trades}",
            f"Winning Trades:     {self.metrics.winning_trades}",
            f"Losing Trades:      {self.metrics.losing_trades}",
            f"Win Rate:           {self.metrics.win_rate * 100:.1f}%",
            f"Profit Factor:      {self.metrics.profit_factor:.2f}",
            f"Avg Trade PnL:      ${self.metrics.avg_trade_pnl_usd:,.2f}",
            f"Largest Win:        ${self.metrics.largest_win_usd:,.2f}",
            f"Largest Loss:       ${self.metrics.largest_loss_usd:,.2f}",
            "",
            "EXECUTION COSTS",
            "-" * 70,
            f"Total Fees:         ${self.metrics.total_fees_usd:,.2f}",
            f"Total Slippage:     ${self.metrics.total_slippage_usd:,.2f}",
            f"Total Gas:          ${self.metrics.total_gas_usd:,.2f}",
            f"Total Costs:        ${self.metrics.total_execution_cost_usd:,.2f}",
            "",
        ]

        if self.run_duration_seconds > 0:
            lines.extend(
                [
                    "RUN INFO",
                    "-" * 70,
                    f"Run Duration:       {self.run_duration_seconds:.2f}s",
                ]
            )

        if self.error:
            lines.extend(
                [
                    "",
                    "ERROR",
                    "-" * 70,
                    f"{self.error}",
                ]
            )

        if self.errors:
            # Count errors by category
            fatal_count = sum(1 for e in self.errors if e.get("classification", {}).get("is_fatal"))
            recoverable_count = sum(1 for e in self.errors if e.get("classification", {}).get("is_recoverable"))
            non_critical_count = sum(1 for e in self.errors if e.get("classification", {}).get("is_non_critical"))

            lines.extend(
                [
                    "",
                    "ERROR SUMMARY",
                    "-" * 70,
                    f"Total Errors:       {len(self.errors)}",
                    f"Fatal Errors:       {fatal_count}",
                    f"Recoverable Errors: {recoverable_count}",
                    f"Non-Critical Errors: {non_critical_count}",
                ]
            )

        # Institutional compliance section
        lines.extend(
            [
                "",
                "INSTITUTIONAL COMPLIANCE",
                "-" * 70,
                f"Compliant:          {'YES' if self.institutional_compliance else 'NO'}",
                f"Preflight Passed:   {'YES' if self.preflight_passed else 'NO'}",
                f"Used Fallbacks:     {'YES' if self.used_any_fallback else 'NO'}",
            ]
        )

        if self.compliance_violations:
            lines.append(f"Violations ({len(self.compliance_violations)}):")
            for violation in self.compliance_violations[:5]:  # Show first 5
                lines.append(f"  - {violation[:60]}...")
            if len(self.compliance_violations) > 5:
                lines.append(f"  ... and {len(self.compliance_violations) - 5} more")

        # Parameter source tracking section
        if self.parameter_sources:
            sources_summary = self.parameter_sources.get_sources_summary()
            lines.extend(
                [
                    "",
                    "PARAMETER SOURCES (Audit Trail)",
                    "-" * 70,
                    f"Total Tracked:      {len(self.parameter_sources.records)}",
                    f"Config Params:      {len(self.parameter_sources.config_sources)} tracked",
                    f"Liquidation Params: {len(self.parameter_sources.liquidation_sources)} tracked",
                    f"APY/Funding Params: {len(self.parameter_sources.apy_funding_sources)} tracked",
                ]
            )
            if sources_summary:
                lines.append("By Source Type:")
                for source, count in sorted(sources_summary.items()):
                    lines.append(f"  - {source}: {count}")

        # Data coverage metrics section (only show when protocol data points exist)
        if self.data_coverage_metrics and self.data_coverage_metrics.total_data_points > 0:
            dcm = self.data_coverage_metrics
            lines.extend(
                [
                    "",
                    "DATA COVERAGE METRICS",
                    "-" * 70,
                    f"Ticks Processed:    {len(self.equity_curve)}",
                    f"Overall Coverage:   {dcm.data_coverage_pct:.1f}% HIGH confidence",
                    f"Protocol Data Pts:  {dcm.total_data_points}",
                    f"HIGH Confidence:    {dcm.high_confidence_data_points}",
                ]
            )
            if dcm.lp_metrics.position_count > 0:
                lines.append(
                    f"  LP Positions:     {dcm.lp_metrics.position_count} "
                    f"(HIGH: {dcm.lp_metrics.high_confidence_pct:.1f}%)"
                )
            if dcm.perp_metrics.position_count > 0:
                lines.append(
                    f"  Perp Positions:   {dcm.perp_metrics.position_count} "
                    f"(HIGH: {dcm.perp_metrics.high_confidence_pct:.1f}%)"
                )
            if dcm.lending_metrics.position_count > 0:
                lines.append(
                    f"  Lending Positions: {dcm.lending_metrics.position_count} "
                    f"(HIGH: {dcm.lending_metrics.high_confidence_pct:.1f}%)"
                )
            if dcm.slippage_metrics.calculation_count > 0:
                lines.append(
                    f"  Slippage Calcs:   {dcm.slippage_metrics.calculation_count} "
                    f"(HIGH: {dcm.slippage_metrics.high_confidence_pct:.1f}%)"
                )

        lines.append("=" * 70)

        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "engine": self.engine.value,
            "strategy_id": self.strategy_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "metrics": self.metrics.to_dict(),
            "trades": [t.to_dict() for t in self.trades],
            "equity_curve": [p.to_dict() for p in self.equity_curve],
            "initial_capital_usd": str(self.initial_capital_usd),
            "final_capital_usd": str(self.final_capital_usd),
            "chain": self.chain,
            "run_started_at": self.run_started_at.isoformat() if self.run_started_at else None,
            "run_ended_at": self.run_ended_at.isoformat() if self.run_ended_at else None,
            "run_duration_seconds": self.run_duration_seconds,
            "config": self.config,
            "error": self.error,
            "success": self.success,
            "simulation_duration_days": self.simulation_duration_days,
            "total_return_pct": str(self.total_return_pct),
            "lending_liquidations": [e.to_dict() for e in self.lending_liquidations],
            "aggregated_portfolio_view": self.aggregated_portfolio_view.to_dict()
            if self.aggregated_portfolio_view
            else None,
            "reconciliation_events": [e.to_dict() for e in self.reconciliation_events],
            "walk_forward_results": self.walk_forward_results.to_dict() if self.walk_forward_results else None,
            "monte_carlo_results": self.monte_carlo_results.to_dict() if self.monte_carlo_results else None,
            "crisis_results": self.crisis_results.to_dict() if self.crisis_results else None,
            "errors": self.errors,
            "backtest_id": self.backtest_id,
            "phase_timings": self.phase_timings,
            "config_hash": self.config_hash,
            "execution_delayed_at_end": self.execution_delayed_at_end,
            "data_source_capabilities": {k: v.value for k, v in self.data_source_capabilities.items()},
            "data_source_warnings": self.data_source_warnings,
            "data_quality": self.data_quality.to_dict() if self.data_quality else None,
            "institutional_compliance": self.institutional_compliance,
            "compliance_violations": self.compliance_violations,
            "fallback_usage": self.fallback_usage,
            "used_any_fallback": self.used_any_fallback,
            "preflight_report": self.preflight_report.to_dict() if self.preflight_report else None,
            "preflight_passed": self.preflight_passed,
            "gas_prices_used": [r.to_dict() for r in self.gas_prices_used],
            "gas_price_summary": self.gas_price_summary.to_dict() if self.gas_price_summary else None,
            "parameter_sources": self.parameter_sources.to_dict() if self.parameter_sources else None,
            "accuracy_estimate": self.accuracy_estimate.to_dict() if self.accuracy_estimate else None,
            "data_coverage_metrics": self.data_coverage_metrics.to_dict() if self.data_coverage_metrics else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BacktestResult":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized BacktestResult data

        Returns:
            BacktestResult instance
        """
        # Parse metrics
        metrics_data = data.get("metrics", {})
        metrics = BacktestMetrics(
            total_pnl_usd=Decimal(metrics_data.get("total_pnl_usd", "0")),
            net_pnl_usd=Decimal(metrics_data.get("net_pnl_usd", "0")),
            sharpe_ratio=Decimal(metrics_data.get("sharpe_ratio", "0")),
            max_drawdown_pct=Decimal(metrics_data.get("max_drawdown_pct", "0")),
            win_rate=Decimal(metrics_data.get("win_rate", "0")),
            total_trades=metrics_data.get("total_trades", 0),
            profit_factor=Decimal(metrics_data.get("profit_factor", "0")),
            total_return_pct=Decimal(metrics_data.get("total_return_pct", "0")),
            annualized_return_pct=Decimal(metrics_data.get("annualized_return_pct", "0")),
            total_fees_usd=Decimal(metrics_data.get("total_fees_usd", "0")),
            total_slippage_usd=Decimal(metrics_data.get("total_slippage_usd", "0")),
            total_gas_usd=Decimal(metrics_data.get("total_gas_usd", "0")),
            winning_trades=metrics_data.get("winning_trades", 0),
            losing_trades=metrics_data.get("losing_trades", 0),
            avg_trade_pnl_usd=Decimal(metrics_data.get("avg_trade_pnl_usd", "0")),
            largest_win_usd=Decimal(metrics_data.get("largest_win_usd", "0")),
            largest_loss_usd=Decimal(metrics_data.get("largest_loss_usd", "0")),
            avg_win_usd=Decimal(metrics_data.get("avg_win_usd", "0")),
            avg_loss_usd=Decimal(metrics_data.get("avg_loss_usd", "0")),
            volatility=Decimal(metrics_data.get("volatility", "0")),
            sortino_ratio=Decimal(metrics_data.get("sortino_ratio", "0")),
            calmar_ratio=Decimal(metrics_data.get("calmar_ratio", "0")),
            total_fees_earned_usd=Decimal(metrics_data.get("total_fees_earned_usd", "0")),
            fees_by_pool={k: Decimal(v) for k, v in metrics_data.get("fees_by_pool", {}).items()},
            lp_fee_confidence_breakdown=metrics_data.get("lp_fee_confidence_breakdown", {}),
            total_funding_paid=Decimal(metrics_data.get("total_funding_paid", "0")),
            total_funding_received=Decimal(metrics_data.get("total_funding_received", "0")),
            liquidations_count=metrics_data.get("liquidations_count", 0),
            liquidation_losses_usd=Decimal(metrics_data.get("liquidation_losses_usd", "0")),
            max_margin_utilization=Decimal(metrics_data.get("max_margin_utilization", "0")),
            total_interest_earned=Decimal(metrics_data.get("total_interest_earned", "0")),
            total_interest_paid=Decimal(metrics_data.get("total_interest_paid", "0")),
            min_health_factor=Decimal(metrics_data.get("min_health_factor", "999")),
            health_factor_warnings=metrics_data.get("health_factor_warnings", 0),
            avg_gas_price_gwei=Decimal(metrics_data.get("avg_gas_price_gwei", "0")),
            max_gas_price_gwei=Decimal(metrics_data.get("max_gas_price_gwei", "0")),
            total_gas_cost_usd=Decimal(metrics_data.get("total_gas_cost_usd", "0")),
            total_mev_cost_usd=Decimal(metrics_data.get("total_mev_cost_usd", "0")),
            total_leverage=Decimal(metrics_data.get("total_leverage", "0")),
            max_net_delta={k: Decimal(v) for k, v in metrics_data.get("max_net_delta", {}).items()},
            correlation_risk=Decimal(metrics_data["correlation_risk"])
            if metrics_data.get("correlation_risk") is not None
            else None,
            liquidation_cascade_risk=Decimal(metrics_data.get("liquidation_cascade_risk", "0")),
            information_ratio=Decimal(metrics_data["information_ratio"])
            if metrics_data.get("information_ratio") is not None
            else None,
            beta=Decimal(metrics_data["beta"]) if metrics_data.get("beta") is not None else None,
            alpha=Decimal(metrics_data["alpha"]) if metrics_data.get("alpha") is not None else None,
            benchmark_return=Decimal(metrics_data["benchmark_return"])
            if metrics_data.get("benchmark_return") is not None
            else None,
            pnl_by_protocol={k: Decimal(v) for k, v in metrics_data.get("pnl_by_protocol", {}).items()},
            pnl_by_intent_type={k: Decimal(v) for k, v in metrics_data.get("pnl_by_intent_type", {}).items()},
            pnl_by_asset={k: Decimal(v) for k, v in metrics_data.get("pnl_by_asset", {}).items()},
            realized_pnl=Decimal(metrics_data.get("realized_pnl", "0")),
            unrealized_pnl=Decimal(metrics_data.get("unrealized_pnl", "0")),
        )

        # Parse trades
        trades = []
        for t_data in data.get("trades", []):
            trades.append(
                TradeRecord(
                    timestamp=datetime.fromisoformat(t_data["timestamp"]),
                    intent_type=IntentType(t_data["intent_type"]),
                    executed_price=Decimal(t_data["executed_price"]),
                    fee_usd=Decimal(t_data["fee_usd"]),
                    slippage_usd=Decimal(t_data["slippage_usd"]),
                    gas_cost_usd=Decimal(t_data["gas_cost_usd"]),
                    pnl_usd=Decimal(t_data["pnl_usd"]),
                    success=t_data["success"],
                    amount_usd=Decimal(t_data.get("amount_usd", "0")),
                    protocol=t_data.get("protocol", ""),
                    tokens=t_data.get("tokens", []),
                    tx_hash=t_data.get("tx_hash"),
                    error=t_data.get("error"),
                    metadata=t_data.get("metadata", {}),
                    actual_amount_in=Decimal(t_data["actual_amount_in"])
                    if t_data.get("actual_amount_in") is not None
                    else None,
                    actual_amount_out=Decimal(t_data["actual_amount_out"])
                    if t_data.get("actual_amount_out") is not None
                    else None,
                    expected_amount_in=Decimal(t_data["expected_amount_in"])
                    if t_data.get("expected_amount_in") is not None
                    else None,
                    expected_amount_out=Decimal(t_data["expected_amount_out"])
                    if t_data.get("expected_amount_out") is not None
                    else None,
                    il_loss_usd=Decimal(t_data["il_loss_usd"]) if t_data.get("il_loss_usd") is not None else None,
                    fees_earned_usd=Decimal(t_data["fees_earned_usd"])
                    if t_data.get("fees_earned_usd") is not None
                    else None,
                    net_lp_pnl_usd=Decimal(t_data["net_lp_pnl_usd"])
                    if t_data.get("net_lp_pnl_usd") is not None
                    else None,
                    gas_price_gwei=Decimal(t_data["gas_price_gwei"])
                    if t_data.get("gas_price_gwei") is not None
                    else None,
                    estimated_mev_cost_usd=Decimal(t_data["estimated_mev_cost_usd"])
                    if t_data.get("estimated_mev_cost_usd") is not None
                    else None,
                    delayed_at_end=t_data.get("delayed_at_end", False),
                )
            )

        # Parse equity curve
        equity_curve = []
        for e_data in data.get("equity_curve", []):
            equity_curve.append(
                EquityPoint(
                    timestamp=datetime.fromisoformat(e_data["timestamp"]),
                    value_usd=Decimal(e_data["value_usd"]),
                )
            )

        # Parse lending liquidations
        lending_liquidations = []
        for ll_data in data.get("lending_liquidations", []):
            lending_liquidations.append(LendingLiquidationEvent.from_dict(ll_data))

        # Parse aggregated portfolio view
        aggregated_portfolio_view = None
        if data.get("aggregated_portfolio_view"):
            aggregated_portfolio_view = AggregatedPortfolioView.from_dict(data["aggregated_portfolio_view"])

        # Parse reconciliation events
        reconciliation_events = []
        for re_data in data.get("reconciliation_events", []):
            reconciliation_events.append(ReconciliationEvent.from_dict(re_data))

        # Parse walk-forward results (import here to avoid circular import)
        walk_forward_results = None
        if data.get("walk_forward_results"):
            from almanak.framework.backtesting.pnl.walk_forward import WalkForwardResult

            walk_forward_results = WalkForwardResult.from_dict(data["walk_forward_results"])

        # Parse Monte Carlo results (import here to avoid circular import)
        monte_carlo_results = None
        if data.get("monte_carlo_results"):
            from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
                MonteCarloSimulationResult,
            )

            monte_carlo_results = MonteCarloSimulationResult.from_dict(data["monte_carlo_results"])

        # Parse crisis results
        crisis_results = None
        if data.get("crisis_results"):
            crisis_results = CrisisMetrics.from_dict(data["crisis_results"])

        # Parse data source capabilities (import here to avoid circular import)
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataCapability

        data_source_capabilities: dict[str, HistoricalDataCapability] = {}
        if data.get("data_source_capabilities"):
            for k, v in data["data_source_capabilities"].items():
                data_source_capabilities[k] = HistoricalDataCapability(v)

        # Parse data quality report
        data_quality = None
        if data.get("data_quality"):
            data_quality = DataQualityReport.from_dict(data["data_quality"])

        # Parse preflight report
        preflight_report = None
        if data.get("preflight_report"):
            preflight_report = PreflightReport.from_dict(data["preflight_report"])

        return cls(
            engine=BacktestEngine(data["engine"]),
            strategy_id=data["strategy_id"],
            start_time=datetime.fromisoformat(data["start_time"]),
            end_time=datetime.fromisoformat(data["end_time"]),
            metrics=metrics,
            trades=trades,
            equity_curve=equity_curve,
            initial_capital_usd=Decimal(data.get("initial_capital_usd", "10000")),
            final_capital_usd=Decimal(data.get("final_capital_usd", "10000")),
            chain=data.get("chain", "arbitrum"),
            run_started_at=datetime.fromisoformat(data["run_started_at"]) if data.get("run_started_at") else None,
            run_ended_at=datetime.fromisoformat(data["run_ended_at"]) if data.get("run_ended_at") else None,
            run_duration_seconds=data.get("run_duration_seconds", 0.0),
            config=data.get("config", {}),
            error=data.get("error"),
            lending_liquidations=lending_liquidations,
            aggregated_portfolio_view=aggregated_portfolio_view,
            reconciliation_events=reconciliation_events,
            walk_forward_results=walk_forward_results,
            monte_carlo_results=monte_carlo_results,
            crisis_results=crisis_results,
            errors=data.get("errors", []),
            backtest_id=data.get("backtest_id"),
            phase_timings=data.get("phase_timings", []),
            config_hash=data.get("config_hash"),
            execution_delayed_at_end=data.get("execution_delayed_at_end", 0),
            data_source_capabilities=data_source_capabilities,
            data_source_warnings=data.get("data_source_warnings", []),
            data_quality=data_quality,
            institutional_compliance=data.get("institutional_compliance", True),
            compliance_violations=data.get("compliance_violations", []),
            fallback_usage=data.get("fallback_usage", {}),
            preflight_report=preflight_report,
            preflight_passed=data.get("preflight_passed", True),
            gas_prices_used=[GasPriceRecord.from_dict(r) for r in data.get("gas_prices_used", [])],
            gas_price_summary=GasPriceSummary.from_dict(data["gas_price_summary"])
            if data.get("gas_price_summary")
            else None,
            parameter_sources=ParameterSourceTracker.from_dict(data["parameter_sources"])
            if data.get("parameter_sources")
            else None,
            accuracy_estimate=AccuracyEstimate.from_dict(data["accuracy_estimate"])
            if data.get("accuracy_estimate")
            else None,
            data_coverage_metrics=DataCoverageMetrics.from_dict(data["data_coverage_metrics"])
            if data.get("data_coverage_metrics")
            else None,
        )


__all__ = [
    "BacktestEngine",
    "StrategyType",
    "ACCURACY_MATRIX",
    "AccuracyEstimate",
    "IntentType",
    "LiquidationEvent",
    "LendingLiquidationEvent",
    "ReconciliationEvent",
    "GasPriceRecord",
    "GasPriceSummary",
    "CrisisMetrics",
    "DataQualityReport",
    "PreflightCheckResult",
    "PreflightReport",
    "FeeAccrualResult",
    "LPMetrics",
    "PerpMetrics",
    "LendingMetrics",
    "SlippageMetrics",
    "DataCoverageMetrics",
    "EquityPoint",
    "TradeRecord",
    "BacktestMetrics",
    "AggregatedPortfolioView",
    "BacktestResult",
    "ParameterSource",
    "ParameterSourceRecord",
    "ParameterSourceTracker",
]
