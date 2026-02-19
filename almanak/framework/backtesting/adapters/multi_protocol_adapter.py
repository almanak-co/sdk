"""Multi-protocol backtest adapter for cross-protocol strategies.

This module provides the backtest adapter for strategies that span multiple protocols,
handling unified position tracking, risk aggregation, and coordinated execution. It manages:

- Aggregation of positions across LP, perp, lending, and other protocol types
- Unified liquidation risk calculation across all protocol positions
- Net exposure tracking across protocols
- Coordinated execution when order matters

Key Features:
    - Sub-adapter management for protocol-specific logic
    - Unified risk scoring across position types
    - Position reconciliation across protocols
    - Configurable execution coordination

Example:
    from almanak.framework.backtesting.adapters.multi_protocol_adapter import (
        MultiProtocolBacktestAdapter,
        MultiProtocolBacktestConfig,
    )

    # Create config for multi-protocol backtesting
    config = MultiProtocolBacktestConfig(
        strategy_type="multi_protocol",
        reconcile_positions=True,
        unified_liquidation_model="conservative",
        protocol_configs={
            "lp": {"fee_tracking_enabled": True},
            "lending": {"health_factor_tracking_enabled": True},
        },
    )

    # Get adapter instance
    adapter = MultiProtocolBacktestAdapter(config)

    # Use in backtesting
    fill = adapter.execute_intent(intent, portfolio, market_state)
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

from almanak.framework.backtesting.adapters.base import (
    AdapterRegistry,
    StrategyBacktestAdapter,
    StrategyBacktestConfig,
    get_adapter,
    register_adapter,
)

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.data_provider import MarketState
    from almanak.framework.backtesting.pnl.portfolio import (
        SimulatedFill,
        SimulatedPortfolio,
        SimulatedPosition,
    )
    from almanak.framework.intents.vocabulary import Intent

from almanak.framework.backtesting.pnl.portfolio_aggregator import (
    PortfolioAggregator,
    UnifiedRiskScore,
)

logger = logging.getLogger(__name__)


class UnifiedLiquidationModel(StrEnum):
    """Model for calculating unified liquidation risk across protocols.

    Attributes:
        CONSERVATIVE: Uses the most conservative (highest) risk score from any
            single position. Liquidation threshold is triggered when any position
            approaches liquidation. Best for risk-averse strategies.
        WEIGHTED: Weights risk by position size across protocols. Larger positions
            contribute more to the overall risk score. Balanced approach.
        AGGREGATE: Aggregates all collateral and debt across protocols to calculate
            a unified health factor. Most accurate for cross-collateralized strategies.
    """

    CONSERVATIVE = "conservative"
    WEIGHTED = "weighted"
    AGGREGATE = "aggregate"


class ExecutionPriority(StrEnum):
    """Priority levels for coordinated execution order.

    When executing multiple intents across protocols, certain operations must
    happen before others to ensure safety and correctness. This enum defines
    the priority levels used to determine execution order.

    Attributes:
        COLLATERAL_FIRST: Operations that add collateral (supply, deposits) should
            execute first to ensure sufficient backing before opening leveraged positions.
        CLOSE_POSITIONS: Operations that close positions should execute before opening
            new ones to free up collateral and reduce risk.
        OPEN_POSITIONS: Operations that open new positions or increase exposure.
        WITHDRAWALS: Withdrawals should execute last after positions are closed.
    """

    COLLATERAL_FIRST = "collateral_first"
    CLOSE_POSITIONS = "close_positions"
    OPEN_POSITIONS = "open_positions"
    WITHDRAWALS = "withdrawals"


@dataclass
class CoordinatedExecution:
    """Result of coordinating multiple intent executions.

    Attributes:
        executions: List of (intent, fill, priority, delay) tuples in execution order
        total_delay_seconds: Total execution delay across all intents
        coordination_strategy: Description of coordination strategy used
        execution_order: List of intent types in execution order
        success: Whether all executions succeeded
        partial_success: Whether some but not all executions succeeded
        failed_intents: List of intent types that failed
    """

    executions: list[tuple[Any, Any, ExecutionPriority, float]]
    total_delay_seconds: float
    coordination_strategy: str
    execution_order: list[str]
    success: bool
    partial_success: bool
    failed_intents: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "total_delay_seconds": self.total_delay_seconds,
            "coordination_strategy": self.coordination_strategy,
            "execution_order": self.execution_order,
            "num_executions": len(self.executions),
            "success": self.success,
            "partial_success": self.partial_success,
            "failed_intents": self.failed_intents,
        }


@dataclass
class ProtocolExposure:
    """Exposure summary for a single protocol.

    Attributes:
        protocol_type: Type of protocol (e.g., "lp", "perp", "lending")
        position_count: Number of positions for this protocol
        total_value_usd: Total value of positions in USD
        net_exposure_usd: Net exposure (positive = long, negative = short)
        risk_score: Protocol-specific risk score (0-1, higher = more risk)
        liquidation_risk: Whether any position is at liquidation risk
    """

    protocol_type: str
    position_count: int
    total_value_usd: Decimal
    net_exposure_usd: Decimal
    risk_score: Decimal
    liquidation_risk: bool

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "protocol_type": self.protocol_type,
            "position_count": self.position_count,
            "total_value_usd": str(self.total_value_usd),
            "net_exposure_usd": str(self.net_exposure_usd),
            "risk_score": str(self.risk_score),
            "liquidation_risk": self.liquidation_risk,
        }


@dataclass
class AggregatedRiskResult:
    """Result of aggregated risk calculation across protocols.

    Attributes:
        unified_risk_score: Combined risk score (0-1, higher = more risk)
        unified_health_factor: Combined health factor (> 1 is safe)
        protocol_exposures: Breakdown by protocol type
        total_collateral_usd: Total collateral value across protocols
        total_debt_usd: Total debt value across protocols
        net_exposure_usd: Net exposure across all protocols
        at_liquidation_risk: Whether unified position is at liquidation risk
        risk_model: Which liquidation model was used
    """

    unified_risk_score: Decimal
    unified_health_factor: Decimal
    protocol_exposures: list[ProtocolExposure]
    total_collateral_usd: Decimal
    total_debt_usd: Decimal
    net_exposure_usd: Decimal
    at_liquidation_risk: bool
    risk_model: UnifiedLiquidationModel

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "unified_risk_score": str(self.unified_risk_score),
            "unified_health_factor": str(self.unified_health_factor),
            "protocol_exposures": [exp.to_dict() for exp in self.protocol_exposures],
            "total_collateral_usd": str(self.total_collateral_usd),
            "total_debt_usd": str(self.total_debt_usd),
            "net_exposure_usd": str(self.net_exposure_usd),
            "at_liquidation_risk": self.at_liquidation_risk,
            "risk_model": self.risk_model.value,
        }


@dataclass
class MultiProtocolBacktestConfig(StrategyBacktestConfig):
    """Configuration for multi-protocol-specific backtesting.

    This config extends the base StrategyBacktestConfig with multi-protocol-specific
    options for controlling position reconciliation, unified liquidation modeling,
    and per-protocol configuration.

    Attributes:
        strategy_type: Must be "multi_protocol" for multi-protocol adapter (inherited)
        fee_tracking_enabled: Whether to track protocol fees (inherited)
        position_tracking_enabled: Whether to track positions in detail (inherited)
        reconcile_on_tick: Whether to reconcile position state each tick (inherited)
        extra_params: Additional parameters (inherited)
        reconcile_positions: Whether to reconcile positions across sub-adapters
            each tick. When True, validates that position state is consistent
            across all protocol adapters. Default True.
        unified_liquidation_model: How to calculate unified liquidation risk:
            - "conservative": Use highest risk from any single position
            - "weighted": Weight risk by position size
            - "aggregate": Aggregate all collateral/debt for unified HF
            Default "conservative".
        protocol_configs: Per-protocol configuration overrides. Keys are protocol
            types (e.g., "lp", "perp", "lending"), values are config dicts that
            will be passed to the respective adapter configs.
        liquidation_warning_threshold: Unified health factor below which to warn.
            Default 1.3.
        liquidation_critical_threshold: Unified health factor below which to
            emit critical warnings. Default 1.1.
        execution_coordination_enabled: Whether to coordinate execution order
            across protocols when order matters. Default True.
        max_execution_delay_seconds: Maximum delay between coordinated executions.
            Default 5.0 seconds.

    Example:
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            reconcile_positions=True,
            unified_liquidation_model="weighted",
            protocol_configs={
                "lp": {"fee_tracking_enabled": True},
                "lending": {
                    "interest_accrual_method": "compound",
                    "health_factor_tracking_enabled": True,
                },
                "perp": {"funding_enabled": True},
            },
            liquidation_warning_threshold=Decimal("1.4"),
        )
    """

    reconcile_positions: bool = True
    """Whether to reconcile positions across sub-adapters each tick."""

    unified_liquidation_model: Literal["conservative", "weighted", "aggregate"] = "conservative"
    """How to calculate unified liquidation risk."""

    protocol_configs: dict[str, dict[str, Any]] = field(default_factory=dict)
    """Per-protocol configuration overrides."""

    liquidation_warning_threshold: Decimal = Decimal("1.3")
    """Unified health factor below which to warn."""

    liquidation_critical_threshold: Decimal = Decimal("1.1")
    """Unified health factor below which to emit critical warnings."""

    execution_coordination_enabled: bool = True
    """Whether to coordinate execution order across protocols."""

    max_execution_delay_seconds: float = 5.0
    """Maximum delay between coordinated executions."""

    def __post_init__(self) -> None:
        """Validate multi-protocol-specific configuration.

        Raises:
            ValueError: If strategy_type is not "multi_protocol" or invalid parameters.
        """
        # Call parent validation
        super().__post_init__()

        # Validate strategy_type for multi_protocol
        if self.strategy_type.lower() != "multi_protocol":
            msg = f"MultiProtocolBacktestConfig requires strategy_type='multi_protocol', got '{self.strategy_type}'"
            raise ValueError(msg)

        # Validate unified_liquidation_model
        valid_models = {"conservative", "weighted", "aggregate"}
        if self.unified_liquidation_model not in valid_models:
            msg = f"unified_liquidation_model must be one of {valid_models}, got '{self.unified_liquidation_model}'"
            raise ValueError(msg)

        # Validate threshold values
        if self.liquidation_warning_threshold <= Decimal("1"):
            msg = f"liquidation_warning_threshold must be > 1, got {self.liquidation_warning_threshold}"
            raise ValueError(msg)
        if self.liquidation_critical_threshold <= Decimal("1"):
            msg = f"liquidation_critical_threshold must be > 1, got {self.liquidation_critical_threshold}"
            raise ValueError(msg)
        if self.liquidation_critical_threshold >= self.liquidation_warning_threshold:
            msg = (
                f"liquidation_critical_threshold ({self.liquidation_critical_threshold}) "
                f"must be < liquidation_warning_threshold ({self.liquidation_warning_threshold})"
            )
            raise ValueError(msg)

        # Validate max_execution_delay
        if self.max_execution_delay_seconds < 0:
            msg = "max_execution_delay_seconds must be non-negative"
            raise ValueError(msg)

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to a dictionary.

        Returns:
            Dictionary representation of the configuration.
        """
        base = super().to_dict()
        base.update(
            {
                "reconcile_positions": self.reconcile_positions,
                "unified_liquidation_model": self.unified_liquidation_model,
                "protocol_configs": dict(self.protocol_configs),
                "liquidation_warning_threshold": str(self.liquidation_warning_threshold),
                "liquidation_critical_threshold": str(self.liquidation_critical_threshold),
                "execution_coordination_enabled": self.execution_coordination_enabled,
                "max_execution_delay_seconds": self.max_execution_delay_seconds,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MultiProtocolBacktestConfig":
        """Create configuration from a dictionary.

        Args:
            data: Dictionary with configuration values.

        Returns:
            New MultiProtocolBacktestConfig instance.
        """
        return cls(
            strategy_type=data.get("strategy_type", "multi_protocol"),
            fee_tracking_enabled=data.get("fee_tracking_enabled", True),
            position_tracking_enabled=data.get("position_tracking_enabled", True),
            reconcile_on_tick=data.get("reconcile_on_tick", False),
            extra_params=data.get("extra_params", {}),
            strict_reproducibility=data.get("strict_reproducibility", False),
            reconcile_positions=data.get("reconcile_positions", True),
            unified_liquidation_model=data.get("unified_liquidation_model", "conservative"),
            protocol_configs=data.get("protocol_configs", {}),
            liquidation_warning_threshold=Decimal(str(data.get("liquidation_warning_threshold", "1.3"))),
            liquidation_critical_threshold=Decimal(str(data.get("liquidation_critical_threshold", "1.1"))),
            execution_coordination_enabled=data.get("execution_coordination_enabled", True),
            max_execution_delay_seconds=data.get("max_execution_delay_seconds", 5.0),
        )


@register_adapter(
    "multi_protocol",
    description="Adapter for cross-protocol strategies with unified risk tracking",
    aliases=["multiprotocol", "cross_protocol", "multi"],
)
class MultiProtocolBacktestAdapter(StrategyBacktestAdapter):
    """Backtest adapter for multi-protocol strategies.

    This adapter handles the simulation of strategies that span multiple protocols
    during backtesting. It provides:

    - Unified position tracking across protocol types
    - Aggregated risk calculation (conservative, weighted, or aggregate models)
    - Coordinated execution when order matters
    - Position reconciliation across sub-adapters

    The adapter delegates protocol-specific logic to sub-adapters while managing
    cross-protocol concerns like unified risk and execution coordination.

    Attributes:
        config: Multi-protocol-specific configuration (optional)

    Example:
        # With config
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            reconcile_positions=True,
            unified_liquidation_model="weighted",
            protocol_configs={
                "lp": {"fee_tracking_enabled": True},
                "lending": {"health_factor_tracking_enabled": True},
            },
        )
        adapter = MultiProtocolBacktestAdapter(config)

        # Without config (uses defaults)
        adapter = MultiProtocolBacktestAdapter()

        # Calculate unified risk across all positions
        risk_result = adapter.calculate_unified_risk(portfolio, market_state)
    """

    def __init__(self, config: MultiProtocolBacktestConfig | None = None) -> None:
        """Initialize the multi-protocol backtest adapter.

        Args:
            config: Multi-protocol-specific configuration. If None, uses default
                MultiProtocolBacktestConfig with strategy_type="multi_protocol".
        """
        self._config = config or MultiProtocolBacktestConfig(strategy_type="multi_protocol")
        self._sub_adapters: dict[str, StrategyBacktestAdapter] = {}
        self._risk_history: list[AggregatedRiskResult] = []
        self._portfolio_aggregator: PortfolioAggregator = PortfolioAggregator()
        self._unified_risk_scores: list[UnifiedRiskScore] = []
        self._initialize_sub_adapters()

    def _initialize_sub_adapters(self) -> None:
        """Initialize sub-adapters for each registered protocol type."""
        # Get all registered adapter types
        registered_types = AdapterRegistry.list_strategy_types()

        for adapter_type in registered_types:
            # Skip multi_protocol to avoid recursion
            if adapter_type == "multi_protocol":
                continue

            # Get adapter instance
            adapter = get_adapter(adapter_type)
            if adapter:
                self._sub_adapters[adapter_type] = adapter
                logger.debug("Initialized sub-adapter for protocol type: %s", adapter_type)

    @property
    def adapter_name(self) -> str:
        """Return the unique name of this adapter.

        Returns:
            Strategy type identifier "multi_protocol"
        """
        return "multi_protocol"

    @property
    def config(self) -> MultiProtocolBacktestConfig:
        """Get the adapter configuration.

        Returns:
            Multi-protocol backtest configuration
        """
        return self._config

    @property
    def sub_adapters(self) -> dict[str, StrategyBacktestAdapter]:
        """Get the sub-adapters for each protocol type.

        Returns:
            Dictionary mapping protocol types to their adapters
        """
        return self._sub_adapters

    @property
    def risk_history(self) -> list[AggregatedRiskResult]:
        """Get the risk calculation history.

        Returns:
            List of AggregatedRiskResult from previous calculations
        """
        return self._risk_history

    @property
    def portfolio_aggregator(self) -> PortfolioAggregator:
        """Get the portfolio aggregator for unified position tracking.

        The portfolio aggregator provides comprehensive position tracking and
        risk calculation across all protocols.

        Returns:
            PortfolioAggregator instance
        """
        return self._portfolio_aggregator

    @property
    def unified_risk_scores(self) -> list[UnifiedRiskScore]:
        """Get the history of unified risk score calculations.

        Returns:
            List of UnifiedRiskScore from previous calculations
        """
        return self._unified_risk_scores

    def get_sub_adapter(self, protocol_type: str) -> StrategyBacktestAdapter | None:
        """Get the sub-adapter for a specific protocol type.

        Args:
            protocol_type: Protocol type (e.g., "lp", "perp", "lending")

        Returns:
            Sub-adapter instance or None if not found
        """
        return self._sub_adapters.get(protocol_type.lower())

    def _detect_intent_protocol_type(self, intent: "Intent") -> str | None:
        """Detect the protocol type for an intent.

        Args:
            intent: The intent to analyze

        Returns:
            Protocol type string (e.g., "lp", "perp", "lending") or None
        """
        intent_class_name = intent.__class__.__name__.upper()

        # Map intent classes to protocol types
        intent_type_map = {
            "LP": "lp",
            "PERP": "perp",
            "BORROW": "lending",
            "REPAY": "lending",
            "SUPPLY": "lending",
            "WITHDRAW": "lending",
            "SWAP": "arbitrage",  # Default to arbitrage for swaps in multi-protocol
        }

        for key, protocol_type in intent_type_map.items():
            if key in intent_class_name:
                return protocol_type

        return None

    def execute_intent(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Simulate execution of an intent using the appropriate sub-adapter.

        This method detects the protocol type from the intent and delegates
        execution to the appropriate sub-adapter. If no sub-adapter handles
        the intent, returns None for default execution.

        Args:
            intent: The intent to execute
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            SimulatedFill describing the execution result, or None for default
            execution logic.
        """
        # Detect protocol type from intent
        protocol_type = self._detect_intent_protocol_type(intent)

        if protocol_type and protocol_type in self._sub_adapters:
            sub_adapter = self._sub_adapters[protocol_type]
            logger.debug(
                "Delegating %s intent to %s sub-adapter",
                intent.__class__.__name__,
                protocol_type,
            )
            return sub_adapter.execute_intent(intent, portfolio, market_state)

        # No matching sub-adapter, return None for default execution
        logger.debug(
            "No sub-adapter for intent %s, using default execution",
            intent.__class__.__name__,
        )
        return None

    def update_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        elapsed_seconds: float,
        timestamp: datetime | None = None,
    ) -> None:
        """Update position state using the appropriate sub-adapter.

        Delegates to the sub-adapter based on position type, then performs
        position reconciliation if enabled.

        Args:
            position: The position to update (modified in-place)
            market_state: Current market prices and data
            elapsed_seconds: Time elapsed since last update in seconds
            timestamp: Simulation timestamp for deterministic updates. If None,
                uses market_state.timestamp for reproducible backtests.
        """
        from almanak.framework.backtesting.pnl.portfolio import PositionType

        # Map position types to protocol adapter types
        position_type_map = {
            PositionType.LP: "lp",
            PositionType.PERP_LONG: "perp",
            PositionType.PERP_SHORT: "perp",
            PositionType.SUPPLY: "lending",
            PositionType.BORROW: "lending",
            PositionType.SPOT: None,  # Spot doesn't need special handling
        }

        protocol_type = position_type_map.get(position.position_type)

        if protocol_type and protocol_type in self._sub_adapters:
            sub_adapter = self._sub_adapters[protocol_type]
            sub_adapter.update_position(position, market_state, elapsed_seconds, timestamp)
        else:
            # Default update: just update timestamp using simulation time
            # Prefer explicit timestamp > market_state.timestamp > datetime.now() (with warning)
            if timestamp is not None:
                update_time = timestamp
            elif hasattr(market_state, "timestamp") and market_state.timestamp is not None:
                update_time = market_state.timestamp
            else:
                if self._config.strict_reproducibility:
                    msg = (
                        f"No simulation timestamp available for position {position.position_id}. "
                        "In strict reproducibility mode, timestamp must be provided. "
                        "Either pass timestamp parameter or ensure market_state.timestamp is set."
                    )
                    raise ValueError(msg)
                logger.warning(
                    "No simulation timestamp available for position %s, "
                    "falling back to datetime.now(). This breaks backtest reproducibility.",
                    position.position_id,
                )
                update_time = datetime.now()
            position.last_updated = update_time

    def value_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Calculate position value using the appropriate sub-adapter.

        Args:
            position: The position to value
            market_state: Current market prices and data
            timestamp: Simulation timestamp for deterministic valuation. If None,
                uses market_state.timestamp. Passed to sub-adapters for consistency.

        Returns:
            Total position value in USD as a Decimal
        """
        from almanak.framework.backtesting.pnl.portfolio import PositionType

        # Map position types to protocol adapter types
        position_type_map = {
            PositionType.LP: "lp",
            PositionType.PERP_LONG: "perp",
            PositionType.PERP_SHORT: "perp",
            PositionType.SUPPLY: "lending",
            PositionType.BORROW: "lending",
            PositionType.SPOT: None,
        }

        protocol_type = position_type_map.get(position.position_type)

        if protocol_type and protocol_type in self._sub_adapters:
            sub_adapter = self._sub_adapters[protocol_type]
            return sub_adapter.value_position(position, market_state, timestamp)

        # Default valuation: simple token amount * price
        total_value = Decimal("0")
        for token, amount in position.amounts.items():
            try:
                price = market_state.get_price(token)
                if price and price > 0:
                    total_value += amount * price
            except KeyError:
                if position.entry_price and position.entry_price > 0:
                    total_value += amount * position.entry_price

        return total_value

    def should_rebalance(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> bool:
        """Determine if position should be rebalanced using sub-adapter.

        Args:
            position: The position to check
            market_state: Current market prices and data

        Returns:
            True if position should be rebalanced
        """
        from almanak.framework.backtesting.pnl.portfolio import PositionType

        position_type_map = {
            PositionType.LP: "lp",
            PositionType.PERP_LONG: "perp",
            PositionType.PERP_SHORT: "perp",
            PositionType.SUPPLY: "lending",
            PositionType.BORROW: "lending",
            PositionType.SPOT: None,
        }

        protocol_type = position_type_map.get(position.position_type)

        if protocol_type and protocol_type in self._sub_adapters:
            sub_adapter = self._sub_adapters[protocol_type]
            return sub_adapter.should_rebalance(position, market_state)

        return False

    def calculate_unified_risk(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> AggregatedRiskResult:
        """Calculate unified risk across all protocol positions.

        This method aggregates positions by protocol type and calculates a
        unified risk score based on the configured liquidation model.

        Args:
            portfolio: Portfolio with all positions
            market_state: Current market state

        Returns:
            AggregatedRiskResult with unified risk metrics
        """
        from almanak.framework.backtesting.pnl.portfolio import PositionType

        # Group positions by protocol type
        protocol_positions: dict[str, list[Any]] = {}
        for position in portfolio.positions:
            position_type_map = {
                PositionType.LP: "lp",
                PositionType.PERP_LONG: "perp",
                PositionType.PERP_SHORT: "perp",
                PositionType.SUPPLY: "lending",
                PositionType.BORROW: "lending",
                PositionType.SPOT: "spot",
            }
            protocol_type = position_type_map.get(position.position_type, "other")
            if protocol_type not in protocol_positions:
                protocol_positions[protocol_type] = []
            protocol_positions[protocol_type].append(position)

        # Calculate exposure for each protocol
        protocol_exposures: list[ProtocolExposure] = []
        total_collateral = Decimal("0")
        total_debt = Decimal("0")
        total_value = Decimal("0")
        max_risk_score = Decimal("0")
        weighted_risk_sum = Decimal("0")
        any_liquidation_risk = False

        for protocol_type, positions in protocol_positions.items():
            protocol_value = Decimal("0")
            protocol_collateral = Decimal("0")
            protocol_debt = Decimal("0")
            protocol_risk = Decimal("0")
            liquidation_risk = False

            for position in positions:
                value = self.value_position(position, market_state)
                protocol_value += value

                # Track collateral vs debt
                if position.position_type == PositionType.BORROW:
                    protocol_debt += value
                    total_debt += value
                else:
                    protocol_collateral += value
                    total_collateral += value

                # Check for liquidation risk
                health_factor = getattr(position, "health_factor", None)
                if health_factor is not None:
                    if health_factor < Decimal("1.1"):
                        liquidation_risk = True
                        any_liquidation_risk = True
                        protocol_risk = max(
                            protocol_risk,
                            Decimal("1") - (health_factor / Decimal("2")),
                        )

                # Check perp liquidation
                if hasattr(position, "liquidation_price") and position.liquidation_price:
                    try:
                        current_price = market_state.get_price(position.primary_token)
                        if current_price:
                            distance = abs(current_price - position.liquidation_price) / current_price
                            if distance < Decimal("0.1"):  # Within 10%
                                liquidation_risk = True
                                any_liquidation_risk = True
                                protocol_risk = max(protocol_risk, Decimal("1") - distance)
                    except KeyError:
                        pass

            net_exposure = protocol_collateral - protocol_debt
            total_value += protocol_value

            exposure = ProtocolExposure(
                protocol_type=protocol_type,
                position_count=len(positions),
                total_value_usd=protocol_value,
                net_exposure_usd=net_exposure,
                risk_score=protocol_risk,
                liquidation_risk=liquidation_risk,
            )
            protocol_exposures.append(exposure)

            # Track for unified risk calculation
            max_risk_score = max(max_risk_score, protocol_risk)
            if protocol_value > 0:
                weighted_risk_sum += protocol_risk * protocol_value

        # Calculate unified metrics based on model
        model = UnifiedLiquidationModel(self._config.unified_liquidation_model)

        if model == UnifiedLiquidationModel.CONSERVATIVE:
            unified_risk_score = max_risk_score
        elif model == UnifiedLiquidationModel.WEIGHTED:
            if total_value > 0:
                unified_risk_score = weighted_risk_sum / total_value
            else:
                unified_risk_score = Decimal("0")
        else:  # AGGREGATE
            if total_collateral > 0 and total_debt > 0:
                unified_risk_score = total_debt / total_collateral
            else:
                unified_risk_score = Decimal("0")

        # Calculate unified health factor
        if total_debt > 0:
            unified_health_factor = total_collateral / total_debt
        else:
            unified_health_factor = Decimal("999")  # No debt = very healthy

        # Check against thresholds
        at_liquidation_risk = unified_health_factor < Decimal("1.0") or any_liquidation_risk

        if unified_health_factor < self._config.liquidation_critical_threshold:
            logger.warning(
                "CRITICAL: Unified health factor %.2f is below critical threshold %.2f",
                float(unified_health_factor),
                float(self._config.liquidation_critical_threshold),
            )
        elif unified_health_factor < self._config.liquidation_warning_threshold:
            logger.warning(
                "WARNING: Unified health factor %.2f is below warning threshold %.2f",
                float(unified_health_factor),
                float(self._config.liquidation_warning_threshold),
            )

        result = AggregatedRiskResult(
            unified_risk_score=unified_risk_score,
            unified_health_factor=unified_health_factor,
            protocol_exposures=protocol_exposures,
            total_collateral_usd=total_collateral,
            total_debt_usd=total_debt,
            net_exposure_usd=total_collateral - total_debt,
            at_liquidation_risk=at_liquidation_risk,
            risk_model=model,
        )

        # Store in history
        self._risk_history.append(result)

        logger.debug(
            "Unified risk: score=%.4f, HF=%.2f, collateral=$%.2f, debt=$%.2f, model=%s",
            float(unified_risk_score),
            float(unified_health_factor),
            float(total_collateral),
            float(total_debt),
            model.value,
        )

        return result

    def aggregate_positions(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> dict[str, list["SimulatedPosition"]]:
        """Aggregate positions by protocol type.

        Args:
            portfolio: Portfolio with all positions
            market_state: Current market state

        Returns:
            Dictionary mapping protocol types to their positions
        """
        from almanak.framework.backtesting.pnl.portfolio import PositionType

        result: dict[str, list[Any]] = {}

        for position in portfolio.positions:
            position_type_map = {
                PositionType.LP: "lp",
                PositionType.PERP_LONG: "perp",
                PositionType.PERP_SHORT: "perp",
                PositionType.SUPPLY: "lending",
                PositionType.BORROW: "lending",
                PositionType.SPOT: "spot",
            }
            protocol_type = position_type_map.get(position.position_type, "other")

            if protocol_type not in result:
                result[protocol_type] = []
            result[protocol_type].append(position)

        return result

    def get_net_exposure(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
        token: str | None = None,
    ) -> Decimal:
        """Calculate net exposure across all protocols.

        Args:
            portfolio: Portfolio with all positions
            market_state: Current market state
            token: Optional token to filter exposure (None = all tokens)

        Returns:
            Net exposure in USD (positive = long, negative = short)
        """
        from almanak.framework.backtesting.pnl.portfolio import PositionType

        net_exposure = Decimal("0")

        for position in portfolio.positions:
            # Filter by token if specified
            if token and position.primary_token != token:
                continue

            value = self.value_position(position, market_state)

            # Adjust sign based on position type
            if position.position_type in {PositionType.PERP_SHORT, PositionType.BORROW}:
                net_exposure -= value
            else:
                net_exposure += value

        return net_exposure

    def sync_positions_to_aggregator(
        self,
        portfolio: "SimulatedPortfolio",
    ) -> None:
        """Sync portfolio positions to the internal PortfolioAggregator.

        This method updates the internal PortfolioAggregator with the current
        positions from the portfolio. Should be called before risk calculations
        to ensure the aggregator has the latest position state.

        Args:
            portfolio: Portfolio with all positions to sync
        """
        # Clear existing positions in aggregator
        self._portfolio_aggregator.clear()

        # Add all positions from portfolio
        for position in portfolio.positions:
            try:
                self._portfolio_aggregator.add_position(position)
            except ValueError:
                # Position already exists (shouldn't happen after clear, but be safe)
                self._portfolio_aggregator.update_position(position)

        logger.debug(
            "Synced %d positions to aggregator across %d protocols",
            self._portfolio_aggregator.position_count,
            len(self._portfolio_aggregator.protocols),
        )

    def calculate_unified_risk_score(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
        sync_positions: bool = True,
    ) -> UnifiedRiskScore:
        """Calculate unified risk score using PortfolioAggregator.

        This method provides a comprehensive risk assessment across all protocol
        positions, combining health factors, leverage, and liquidation proximity.

        The risk score combines multiple factors:
        - Health factor risk: Lending positions with low health factor
        - Leverage risk: Perp positions with high leverage
        - Liquidation proximity: Positions near liquidation price
        - Concentration risk: High collateral utilization

        Args:
            portfolio: Portfolio with all positions
            market_state: Current market state for price data
            sync_positions: Whether to sync positions to aggregator first (default True)

        Returns:
            UnifiedRiskScore with overall score and individual risk components
        """
        if sync_positions:
            self.sync_positions_to_aggregator(portfolio)

        # Build prices dict from market state
        prices: dict[str, Decimal] = {}
        for position in portfolio.positions:
            for token in position.tokens:
                try:
                    price = market_state.get_price(token)
                    if price:
                        prices[token] = price
                except KeyError:
                    pass

        # Use PortfolioAggregator's unified risk score calculation
        risk_score = self._portfolio_aggregator.calculate_unified_risk_score(
            prices=prices,
            health_factor_warning_threshold=self._config.liquidation_warning_threshold,
            leverage_warning_threshold=Decimal("5"),  # Standard warning threshold
            liquidation_proximity_threshold=Decimal("0.1"),  # 10% from liquidation
        )

        # Store in history
        self._unified_risk_scores.append(risk_score)

        # Log warnings based on thresholds
        if risk_score.score >= Decimal("0.8"):
            logger.warning(
                "CRITICAL: Unified risk score %.2f is at critical level",
                float(risk_score.score),
            )
        elif risk_score.score >= Decimal("0.6"):
            logger.warning(
                "WARNING: Unified risk score %.2f is at high level",
                float(risk_score.score),
            )

        logger.debug(
            "Unified risk score: %.4f, min_hf=%s, max_leverage=%.2fx, positions_at_risk=%d",
            float(risk_score.score),
            f"{float(risk_score.min_health_factor):.2f}" if risk_score.min_health_factor else "N/A",
            float(risk_score.max_leverage),
            risk_score.positions_at_risk,
        )

        return risk_score

    def get_net_exposure_by_asset(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
        sync_positions: bool = True,
    ) -> dict[str, Decimal]:
        """Calculate net exposure for all assets across protocols.

        This method tracks directional exposure (long/short) for each asset
        across all protocol positions.

        Args:
            portfolio: Portfolio with all positions
            market_state: Current market state for price data
            sync_positions: Whether to sync positions to aggregator first (default True)

        Returns:
            Dict mapping asset symbol to net exposure in asset units
        """
        if sync_positions:
            self.sync_positions_to_aggregator(portfolio)

        # Build prices dict from market state
        prices: dict[str, Decimal] = {}
        for position in portfolio.positions:
            for token in position.tokens:
                try:
                    price = market_state.get_price(token)
                    if price:
                        prices[token] = price
                except KeyError:
                    pass

        return self._portfolio_aggregator.calculate_all_net_exposures(prices)

    def get_net_exposure_usd_by_asset(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
        sync_positions: bool = True,
    ) -> dict[str, Decimal]:
        """Calculate net exposure in USD for all assets across protocols.

        This method tracks directional exposure (long/short) for each asset
        across all protocol positions, converted to USD.

        Args:
            portfolio: Portfolio with all positions
            market_state: Current market state for price data
            sync_positions: Whether to sync positions to aggregator first (default True)

        Returns:
            Dict mapping asset symbol to net exposure in USD
        """
        exposures = self.get_net_exposure_by_asset(portfolio, market_state, sync_positions=sync_positions)

        # Build prices dict from market state
        prices: dict[str, Decimal] = {}
        for position in portfolio.positions:
            for token in position.tokens:
                try:
                    price = market_state.get_price(token)
                    if price:
                        prices[token] = price
                except KeyError:
                    pass

        # Convert to USD
        usd_exposures: dict[str, Decimal] = {}
        for asset, exposure in exposures.items():
            price = prices.get(asset, Decimal("0"))
            usd_exposures[asset] = exposure * price

        return usd_exposures

    def get_total_leverage(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
        sync_positions: bool = True,
    ) -> Decimal:
        """Calculate total portfolio leverage ratio across all protocols.

        Total leverage is the ratio of total notional exposure to total equity,
        aggregated across all leveraged position types.

        Args:
            portfolio: Portfolio with all positions
            market_state: Current market state for price data
            sync_positions: Whether to sync positions to aggregator first (default True)

        Returns:
            Total leverage ratio as Decimal
        """
        if sync_positions:
            self.sync_positions_to_aggregator(portfolio)

        # Build prices dict from market state
        prices: dict[str, Decimal] = {}
        for position in portfolio.positions:
            for token in position.tokens:
                try:
                    price = market_state.get_price(token)
                    if price:
                        prices[token] = price
                except KeyError:
                    pass

        return self._portfolio_aggregator.calculate_total_leverage(prices)

    def get_collateral_utilization(
        self,
        portfolio: "SimulatedPortfolio",
        sync_positions: bool = True,
    ) -> Decimal:
        """Calculate overall collateral utilization ratio across protocols.

        Collateral utilization measures how much of the available collateral
        is being used to back leveraged positions.

        Args:
            portfolio: Portfolio with all positions
            sync_positions: Whether to sync positions to aggregator first (default True)

        Returns:
            Collateral utilization as a decimal (e.g., 0.75 = 75% utilized)
        """
        if sync_positions:
            self.sync_positions_to_aggregator(portfolio)

        return self._portfolio_aggregator.calculate_collateral_utilization()

    def get_leverage_by_protocol(
        self,
        portfolio: "SimulatedPortfolio",
        sync_positions: bool = True,
    ) -> dict[str, Decimal]:
        """Calculate effective leverage per protocol.

        Args:
            portfolio: Portfolio with all positions
            sync_positions: Whether to sync positions to aggregator first (default True)

        Returns:
            Dict mapping protocol name to effective leverage ratio
        """
        if sync_positions:
            self.sync_positions_to_aggregator(portfolio)

        return self._portfolio_aggregator.calculate_leverage_by_protocol()

    def get_unified_risk_stats(self) -> dict[str, Any]:
        """Get summary statistics for unified risk score calculations.

        Returns:
            Dictionary with risk statistics from PortfolioAggregator
        """
        if not self._unified_risk_scores:
            return {
                "total_calculations": 0,
                "avg_risk_score": "0",
                "min_risk_score": "0",
                "max_risk_score": "0",
                "min_health_factor": None,
                "max_leverage": "0",
                "positions_at_risk_max": 0,
            }

        risk_scores = [r.score for r in self._unified_risk_scores]
        health_factors = [r.min_health_factor for r in self._unified_risk_scores if r.min_health_factor is not None]
        leverages = [r.max_leverage for r in self._unified_risk_scores]
        positions_at_risk = [r.positions_at_risk for r in self._unified_risk_scores]

        return {
            "total_calculations": len(self._unified_risk_scores),
            "avg_risk_score": str(sum(risk_scores) / len(risk_scores)),
            "min_risk_score": str(min(risk_scores)),
            "max_risk_score": str(max(risk_scores)),
            "min_health_factor": str(min(health_factors)) if health_factors else None,
            "max_leverage": str(max(leverages)),
            "positions_at_risk_max": max(positions_at_risk),
        }

    def clear_risk_history(self) -> None:
        """Clear the risk calculation history."""
        self._risk_history.clear()
        self._unified_risk_scores.clear()

    def _get_intent_priority(self, intent: "Intent") -> ExecutionPriority:
        """Determine the execution priority for an intent.

        This method assigns a priority level to an intent based on its type,
        which is used to determine execution order in coordinated execution.

        Priority order (highest to lowest):
        1. COLLATERAL_FIRST: Supply operations that add collateral
        2. CLOSE_POSITIONS: Close/repay operations that reduce exposure
        3. OPEN_POSITIONS: Open positions, swaps, borrows that increase exposure
        4. WITHDRAWALS: Withdraw operations (should be last)

        Args:
            intent: The intent to get priority for

        Returns:
            ExecutionPriority for the intent
        """
        intent_class_name = intent.__class__.__name__.upper()

        # Supply operations add collateral - execute first
        if "SUPPLY" in intent_class_name:
            return ExecutionPriority.COLLATERAL_FIRST

        # Close/repay operations reduce exposure - execute second
        if any(kw in intent_class_name for kw in ["CLOSE", "REPAY", "UNSTAKE"]):
            return ExecutionPriority.CLOSE_POSITIONS

        # Withdraw operations - execute last
        if "WITHDRAW" in intent_class_name:
            return ExecutionPriority.WITHDRAWALS

        # Everything else (open, swap, borrow) - middle priority
        return ExecutionPriority.OPEN_POSITIONS

    def _calculate_execution_delay(
        self,
        intent: "Intent",
        position_in_sequence: int,
        total_intents: int,
    ) -> float:
        """Calculate the execution delay for an intent in a sequence.

        The delay is based on position in the sequence and the configured
        max_execution_delay_seconds. Each subsequent intent gets a proportionally
        larger delay.

        Args:
            intent: The intent being executed
            position_in_sequence: Position (0-indexed) in the execution sequence
            total_intents: Total number of intents in the sequence

        Returns:
            Delay in seconds before this intent should execute
        """
        if not self._config.execution_coordination_enabled:
            return 0.0

        if total_intents <= 1:
            return 0.0

        # Distribute delays evenly across the max_execution_delay
        delay_per_step = self._config.max_execution_delay_seconds / (total_intents - 1)
        return delay_per_step * position_in_sequence

    def execute_coordinated_intents(
        self,
        intents: list["Intent"],
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> CoordinatedExecution:
        """Execute multiple intents with coordinated ordering and delays.

        This method coordinates the execution of multiple intents, ensuring
        that they execute in an order that maintains portfolio safety:

        1. Supply collateral first (to back subsequent borrows)
        2. Close existing positions (to free collateral)
        3. Open new positions/swaps/borrows
        4. Withdrawals last

        Each execution includes a configurable delay between steps to simulate
        realistic multi-transaction scenarios.

        Args:
            intents: List of intents to execute
            portfolio: Current portfolio state
            market_state: Current market state

        Returns:
            CoordinatedExecution with all execution results and metadata
        """
        if not intents:
            return CoordinatedExecution(
                executions=[],
                total_delay_seconds=0.0,
                coordination_strategy="empty",
                execution_order=[],
                success=True,
                partial_success=False,
                failed_intents=[],
            )

        # If coordination is disabled, execute in original order
        if not self._config.execution_coordination_enabled:
            return self._execute_intents_sequential(intents, portfolio, market_state)

        # Sort intents by priority
        priority_order = [
            ExecutionPriority.COLLATERAL_FIRST,
            ExecutionPriority.CLOSE_POSITIONS,
            ExecutionPriority.OPEN_POSITIONS,
            ExecutionPriority.WITHDRAWALS,
        ]

        # Group intents by priority
        intent_by_priority: dict[ExecutionPriority, list[Any]] = {p: [] for p in priority_order}
        for intent in intents:
            priority = self._get_intent_priority(intent)
            intent_by_priority[priority].append(intent)

        # Build sorted list by priority
        sorted_intents: list[tuple[Any, ExecutionPriority]] = []
        for priority in priority_order:
            for intent in intent_by_priority[priority]:
                sorted_intents.append((intent, priority))

        # Execute in order with delays
        executions: list[tuple[Any, Any, ExecutionPriority, float]] = []
        total_delay = 0.0
        execution_order: list[str] = []
        failed_intents: list[str] = []
        all_success = True
        any_success = False

        for idx, (intent, priority) in enumerate(sorted_intents):
            # Calculate delay for this execution
            delay = self._calculate_execution_delay(intent, idx, len(sorted_intents))
            total_delay += delay

            # Execute the intent
            fill = self.execute_intent(intent, portfolio, market_state)

            intent_type = intent.__class__.__name__
            execution_order.append(intent_type)

            if fill is None:
                # No adapter handled it, mark as None fill (default execution)
                executions.append((intent, None, priority, delay))
                any_success = True
            elif fill.success:
                executions.append((intent, fill, priority, delay))
                any_success = True
            else:
                executions.append((intent, fill, priority, delay))
                failed_intents.append(intent_type)
                all_success = False

            logger.debug(
                "Coordinated execution %d/%d: %s (priority=%s, delay=%.2fs, success=%s)",
                idx + 1,
                len(sorted_intents),
                intent_type,
                priority.value,
                delay,
                fill.success if fill else "delegated",
            )

        return CoordinatedExecution(
            executions=executions,
            total_delay_seconds=total_delay,
            coordination_strategy="priority_ordered",
            execution_order=execution_order,
            success=all_success,
            partial_success=any_success and not all_success,
            failed_intents=failed_intents,
        )

    def _execute_intents_sequential(
        self,
        intents: list["Intent"],
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> CoordinatedExecution:
        """Execute intents in sequential order without reordering.

        Used when execution_coordination_enabled is False.

        Args:
            intents: List of intents to execute
            portfolio: Current portfolio state
            market_state: Current market state

        Returns:
            CoordinatedExecution with results
        """
        executions: list[tuple[Any, Any, ExecutionPriority, float]] = []
        execution_order: list[str] = []
        failed_intents: list[str] = []
        all_success = True
        any_success = False

        for intent in intents:
            priority = self._get_intent_priority(intent)
            fill = self.execute_intent(intent, portfolio, market_state)

            intent_type = intent.__class__.__name__
            execution_order.append(intent_type)

            if fill is None:
                executions.append((intent, None, priority, 0.0))
                any_success = True
            elif fill.success:
                executions.append((intent, fill, priority, 0.0))
                any_success = True
            else:
                executions.append((intent, fill, priority, 0.0))
                failed_intents.append(intent_type)
                all_success = False

        return CoordinatedExecution(
            executions=executions,
            total_delay_seconds=0.0,
            coordination_strategy="sequential",
            execution_order=execution_order,
            success=all_success,
            partial_success=any_success and not all_success,
            failed_intents=failed_intents,
        )

    def execute_intent_sequence(
        self,
        sequence: Any,  # IntentSequence
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> CoordinatedExecution:
        """Execute an IntentSequence with coordinated ordering.

        IntentSequence represents dependent operations that must execute
        in a specific order. This method respects that order while also
        applying execution coordination rules and delays.

        Args:
            sequence: IntentSequence to execute
            portfolio: Current portfolio state
            market_state: Current market state

        Returns:
            CoordinatedExecution with results
        """
        # Extract intents from sequence
        intents = list(sequence.intents) if hasattr(sequence, "intents") else list(sequence)

        # Execute with coordination
        return self.execute_coordinated_intents(intents, portfolio, market_state)

    def get_risk_stats(self) -> dict[str, Any]:
        """Get summary statistics for risk calculations.

        Returns:
            Dictionary with risk statistics
        """
        if not self._risk_history:
            return {
                "total_calculations": 0,
                "avg_risk_score": "0",
                "min_health_factor": "999",
                "max_risk_score": "0",
                "liquidation_risk_count": 0,
            }

        risk_scores = [r.unified_risk_score for r in self._risk_history]
        health_factors = [r.unified_health_factor for r in self._risk_history]
        liquidation_risk_count = sum(1 for r in self._risk_history if r.at_liquidation_risk)

        return {
            "total_calculations": len(self._risk_history),
            "avg_risk_score": str(sum(risk_scores) / len(risk_scores)),
            "min_health_factor": str(min(health_factors)),
            "max_risk_score": str(max(risk_scores)),
            "liquidation_risk_count": liquidation_risk_count,
            "liquidation_risk_pct": f"{liquidation_risk_count / len(self._risk_history) * 100:.1f}%",
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize the adapter configuration to a dictionary.

        Returns:
            Dictionary with adapter configuration
        """
        return {
            "adapter_name": self.adapter_name,
            "config": self._config.to_dict(),
            "sub_adapters": list(self._sub_adapters.keys()),
            "risk_stats": self.get_risk_stats(),
            "unified_risk_stats": self.get_unified_risk_stats(),
            "portfolio_aggregator_stats": {
                "position_count": self._portfolio_aggregator.position_count,
                "protocols": self._portfolio_aggregator.protocols,
                "protocol_counts": self._portfolio_aggregator.get_protocol_counts(),
                "type_counts": {pt.value: count for pt, count in self._portfolio_aggregator.get_type_counts().items()},
            },
            "execution_coordination": {
                "enabled": self._config.execution_coordination_enabled,
                "max_delay_seconds": self._config.max_execution_delay_seconds,
            },
        }


__all__ = [
    "AggregatedRiskResult",
    "CoordinatedExecution",
    "ExecutionPriority",
    "MultiProtocolBacktestAdapter",
    "MultiProtocolBacktestConfig",
    "ProtocolExposure",
    "UnifiedLiquidationModel",
]
