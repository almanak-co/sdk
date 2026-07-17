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
    get_adapter_with_config,
    register_adapter,
)
from almanak.framework.backtesting.pnl.data_provider import TokenRef, token_ref_display
from almanak.framework.backtesting.pnl.position_models import PositionType

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig
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


# Saturation cap for unified risk scores. The score is defined on [0, 1]; the
# AGGREGATE branch returns this when total_debt > 0 with zero collateral
# (a debt-only portfolio is at maximum liquidation risk, not zero risk).
UNIFIED_RISK_SCORE_MAX: Decimal = Decimal("1")


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


_POSITION_ADAPTER_BY_TYPE: dict[PositionType, str | None] = {
    PositionType.LP: "lp",
    PositionType.PERP_LONG: "perp",
    PositionType.PERP_SHORT: "perp",
    PositionType.SUPPLY: "lending",
    PositionType.BORROW: "lending",
    PositionType.SPOT: None,
}

_EXECUTION_PRIORITY_ORDER: tuple[ExecutionPriority, ...] = (
    ExecutionPriority.COLLATERAL_FIRST,
    ExecutionPriority.CLOSE_POSITIONS,
    ExecutionPriority.OPEN_POSITIONS,
    ExecutionPriority.WITHDRAWALS,
)


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

    swap_lane: Literal["arbitrage", "generic"] = "arbitrage"
    """Which lane executes plain SWAP intents: "arbitrage" (the historical
    multi-protocol behavior) sends them to the arbitrage sub-adapter;
    "generic" leaves them to the engine's generic lane — the router default."""

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

        # Validate swap_lane (from_dict can carry arbitrary strings)
        if self.swap_lane not in {"arbitrage", "generic"}:
            msg = f"swap_lane must be one of {{'arbitrage', 'generic'}}, got '{self.swap_lane}'"
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
                "swap_lane": self.swap_lane,
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
            swap_lane=data.get("swap_lane", "arbitrage"),
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

    def __init__(
        self,
        config: MultiProtocolBacktestConfig | None = None,
        data_config: "BacktestDataConfig | None" = None,
    ) -> None:
        """Initialize the multi-protocol backtest adapter.

        Args:
            config: Multi-protocol-specific configuration. If None, uses default
                MultiProtocolBacktestConfig with strategy_type="multi_protocol".
            data_config: Engine BacktestDataConfig, threaded through to every
                sub-adapter so their historical-data providers honor the
                caller's settings (ALM-2930: dropping it left the LP sub-adapter
                on LPBacktestConfig defaults, turning a missing pool-volume
                source into a fatal error for every multi-protocol strategy).
        """
        self._config = config or MultiProtocolBacktestConfig(strategy_type="multi_protocol")
        self._data_config = data_config
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
            adapter = get_adapter_with_config(adapter_type, data_config=self._data_config)
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
        }
        if self._config.swap_lane == "arbitrage":
            intent_type_map["SWAP"] = "arbitrage"

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

    def _sub_adapter_for_position(self, position: "SimulatedPosition") -> StrategyBacktestAdapter | None:
        protocol_type = _POSITION_ADAPTER_BY_TYPE.get(position.position_type)
        if protocol_type is None:
            return None
        return self._sub_adapters.get(protocol_type)

    async def prewarm_history(
        self,
        intent: "Intent",
        chain: str,
        start_time: datetime,
        end_time: datetime,
    ) -> None:
        """Route the engine's post-open prewarm hook to the owning sub-adapter.

        LP_OPEN → lp sub-adapter (volume/liquidity); PERP_OPEN → perp
        sub-adapter (funding). Sub-adapters without the hook are skipped —
        prewarm is best-effort everywhere.
        """
        intent_type = str(getattr(getattr(intent, "intent_type", None), "value", "")).upper()
        sub_type = {"LP_OPEN": "lp", "PERP_OPEN": "perp"}.get(intent_type)
        if sub_type is None:
            return
        prewarm = getattr(self._sub_adapters.get(sub_type), "prewarm_history", None)
        if prewarm is not None:
            await prewarm(intent, chain=chain, start_time=start_time, end_time=end_time)

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
        sub_adapter = self._sub_adapter_for_position(position)
        if sub_adapter is not None:
            sub_adapter.update_position(position, market_state, elapsed_seconds, timestamp)
            return

        position.last_updated = self._default_update_time(position, market_state, timestamp)

    def _default_update_time(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime | None,
    ) -> datetime:
        if timestamp is not None:
            return timestamp

        market_timestamp = getattr(market_state, "timestamp", None)
        if market_timestamp is not None:
            return market_timestamp

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
        return datetime.now()

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
        sub_adapter = self._sub_adapter_for_position(position)
        if sub_adapter is not None:
            return sub_adapter.value_position(position, market_state, timestamp)

        return self._default_position_value(position, market_state)

    def _default_position_value(self, position: "SimulatedPosition", market_state: "MarketState") -> Decimal:
        total_value = Decimal("0")
        for token, amount in position.amounts.items():
            total_value += self._default_token_value(position, market_state, token, amount)
        return total_value

    def _default_token_value(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        token: TokenRef,
        amount: Decimal,
    ) -> Decimal:
        price = self._market_price_or_none(market_state, token)
        if price is not None:
            return amount * price if price > Decimal("0") else Decimal("0")
        if position.entry_price and position.entry_price > Decimal("0"):
            return amount * position.entry_price
        return Decimal("0")

    @staticmethod
    def _market_price_or_none(market_state: "MarketState", token: TokenRef) -> Decimal | None:
        try:
            return market_state.get_price(token)
        except KeyError:
            return None

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
        protocol_positions = self._group_positions_by_protocol(portfolio)

        protocol_exposures: list[ProtocolExposure] = []
        total_collateral = Decimal("0")
        total_debt = Decimal("0")
        total_value = Decimal("0")
        max_risk_score = Decimal("0")
        weighted_risk_sum = Decimal("0")
        any_liquidation_risk = False

        for protocol_type, positions in protocol_positions.items():
            exposure, totals = self._evaluate_protocol_exposure(protocol_type, positions, market_state)
            protocol_exposures.append(exposure)

            total_collateral += totals["collateral"]
            total_debt += totals["debt"]
            total_value += exposure.total_value_usd
            max_risk_score = max(max_risk_score, exposure.risk_score)
            if exposure.total_value_usd > 0:
                weighted_risk_sum += exposure.risk_score * exposure.total_value_usd
            if exposure.liquidation_risk:
                any_liquidation_risk = True

        model = UnifiedLiquidationModel(self._config.unified_liquidation_model)
        unified_risk_score = self._compute_unified_risk_score(
            model,
            max_risk_score=max_risk_score,
            weighted_risk_sum=weighted_risk_sum,
            total_value=total_value,
            total_collateral=total_collateral,
            total_debt=total_debt,
        )

        # No debt = very healthy
        unified_health_factor = total_collateral / total_debt if total_debt > 0 else Decimal("999")

        at_liquidation_risk = unified_health_factor < Decimal("1.0") or any_liquidation_risk
        self._log_health_factor_warnings(unified_health_factor)

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

    @staticmethod
    def _group_positions_by_protocol(portfolio: "SimulatedPortfolio") -> dict[str, list[Any]]:
        """Group portfolio positions by their protocol type."""
        from almanak.framework.backtesting.pnl.portfolio import PositionType

        position_type_map = {
            PositionType.LP: "lp",
            PositionType.PERP_LONG: "perp",
            PositionType.PERP_SHORT: "perp",
            PositionType.SUPPLY: "lending",
            PositionType.BORROW: "lending",
            PositionType.SPOT: "spot",
        }
        protocol_positions: dict[str, list[Any]] = {}
        for position in portfolio.positions:
            protocol_type = position_type_map.get(position.position_type, "other")
            protocol_positions.setdefault(protocol_type, []).append(position)
        return protocol_positions

    def _evaluate_protocol_exposure(
        self,
        protocol_type: str,
        positions: list[Any],
        market_state: "MarketState",
    ) -> tuple[ProtocolExposure, dict[str, Decimal]]:
        """Aggregate per-position risk into a single ProtocolExposure plus collateral/debt totals."""
        from almanak.framework.backtesting.pnl.portfolio import PositionType

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
            else:
                protocol_collateral += value

            position_risk, position_at_risk = self._evaluate_position_risk(position, market_state)
            if position_at_risk:
                liquidation_risk = True
            if position_risk > protocol_risk:
                protocol_risk = position_risk

        exposure = ProtocolExposure(
            protocol_type=protocol_type,
            position_count=len(positions),
            total_value_usd=protocol_value,
            net_exposure_usd=protocol_collateral - protocol_debt,
            risk_score=protocol_risk,
            liquidation_risk=liquidation_risk,
        )
        return exposure, {"collateral": protocol_collateral, "debt": protocol_debt}

    @staticmethod
    def _evaluate_position_risk(
        position: Any,
        market_state: "MarketState",
    ) -> tuple[Decimal, bool]:
        """Return the worst-case (risk_score, at_risk_flag) for a single position."""
        risk_score = Decimal("0")
        at_risk = False

        # Lending health-factor branch
        health_factor = getattr(position, "health_factor", None)
        if health_factor is not None and health_factor < Decimal("1.1"):
            at_risk = True
            risk_score = max(risk_score, Decimal("1") - (health_factor / Decimal("2")))

        # Perp liquidation-price branch
        liquidation_price = getattr(position, "liquidation_price", None)
        if liquidation_price:
            try:
                current_price = market_state.get_price(position.primary_token)
            except KeyError:
                current_price = None
            if current_price:
                distance = abs(current_price - liquidation_price) / current_price
                if distance < Decimal("0.1"):  # Within 10%
                    at_risk = True
                    risk_score = max(risk_score, Decimal("1") - distance)

        return risk_score, at_risk

    @staticmethod
    def _compute_unified_risk_score(
        model: UnifiedLiquidationModel,
        *,
        max_risk_score: Decimal,
        weighted_risk_sum: Decimal,
        total_value: Decimal,
        total_collateral: Decimal,
        total_debt: Decimal,
    ) -> Decimal:
        """Apply the configured liquidation model to derive the unified risk score."""
        if model == UnifiedLiquidationModel.CONSERVATIVE:
            return max_risk_score
        if model == UnifiedLiquidationModel.WEIGHTED:
            return weighted_risk_sum / total_value if total_value > 0 else Decimal("0")
        # AGGREGATE
        if total_collateral > 0 and total_debt > 0:
            return total_debt / total_collateral
        # Debt-only portfolio (no collateral) is at max liquidation risk;
        # returning 0 here would silently understate it.
        if total_debt > 0:
            return UNIFIED_RISK_SCORE_MAX
        return Decimal("0")

    def _log_health_factor_warnings(self, unified_health_factor: Decimal) -> None:
        """Emit critical/warning logs when the unified HF crosses configured thresholds."""
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

        prices = self._portfolio_prices(portfolio, market_state)

        # Use PortfolioAggregator's unified risk score calculation
        risk_score = self._portfolio_aggregator.calculate_unified_risk_score(
            prices=prices,
            health_factor_warning_threshold=self._config.liquidation_warning_threshold,
            leverage_warning_threshold=Decimal("5"),  # Standard warning threshold
            liquidation_proximity_threshold=Decimal("0.1"),  # 10% from liquidation
        )

        self._record_unified_risk_score(risk_score)
        return risk_score

    def _portfolio_prices(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> dict[str, Decimal]:
        prices: dict[str, Decimal] = {}
        for position in portfolio.positions:
            self._add_position_prices(prices, position, market_state)
        return prices

    def _add_position_prices(
        self,
        prices: dict[str, Decimal],
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> None:
        for token in position.tokens:
            price = self._market_price_or_none(market_state, token)
            if price is not None:
                prices[token_ref_display(token)] = price

    def _record_unified_risk_score(self, risk_score: UnifiedRiskScore) -> None:
        self._unified_risk_scores.append(risk_score)
        self._log_unified_risk_warning(risk_score)

        logger.debug(
            "Unified risk score: %.4f, min_hf=%s, max_leverage=%.2fx, positions_at_risk=%d",
            float(risk_score.score),
            f"{float(risk_score.min_health_factor):.2f}" if risk_score.min_health_factor else "N/A",
            float(risk_score.max_leverage),
            risk_score.positions_at_risk,
        )

    @staticmethod
    def _log_unified_risk_warning(risk_score: UnifiedRiskScore) -> None:
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
                        prices[token_ref_display(token)] = price
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
                        prices[token_ref_display(token)] = price
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
                        prices[token_ref_display(token)] = price
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
            return self._empty_coordinated_execution()

        # If coordination is disabled, execute in original order
        if not self._config.execution_coordination_enabled:
            return self._execute_intents_sequential(intents, portfolio, market_state)

        return self._execute_prioritized_plan(
            self._priority_ordered_intents(intents),
            portfolio,
            market_state,
            coordination_strategy="priority_ordered",
            apply_delays=True,
        )

    @staticmethod
    def _empty_coordinated_execution() -> CoordinatedExecution:
        return CoordinatedExecution(
            executions=[],
            total_delay_seconds=0.0,
            coordination_strategy="empty",
            execution_order=[],
            success=True,
            partial_success=False,
            failed_intents=[],
        )

    def _priority_ordered_intents(self, intents: list["Intent"]) -> list[tuple[Any, ExecutionPriority]]:
        intent_by_priority = self._group_intents_by_priority(intents)
        return [(intent, priority) for priority in _EXECUTION_PRIORITY_ORDER for intent in intent_by_priority[priority]]

    def _group_intents_by_priority(
        self,
        intents: list["Intent"],
    ) -> dict[ExecutionPriority, list[Any]]:
        intent_by_priority: dict[ExecutionPriority, list[Any]] = {p: [] for p in _EXECUTION_PRIORITY_ORDER}
        for intent in intents:
            priority = self._get_intent_priority(intent)
            intent_by_priority[priority].append(intent)
        return intent_by_priority

    def _execute_prioritized_plan(
        self,
        prioritized_intents: list[tuple[Any, ExecutionPriority]],
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
        *,
        coordination_strategy: str,
        apply_delays: bool,
    ) -> CoordinatedExecution:
        executions: list[tuple[Any, Any, ExecutionPriority, float]] = []
        total_delay = 0.0
        execution_order: list[str] = []
        failed_intents: list[str] = []
        all_success = True
        any_success = False

        for idx, (intent, priority) in enumerate(prioritized_intents):
            delay = self._planned_execution_delay(intent, idx, len(prioritized_intents), apply_delays)
            total_delay += delay
            fill = self.execute_intent(intent, portfolio, market_state)
            intent_type = intent.__class__.__name__

            executions.append((intent, fill, priority, delay))
            execution_order.append(intent_type)

            if self._execution_succeeded(fill):
                any_success = True
            else:
                failed_intents.append(intent_type)
                all_success = False

            self._log_plan_execution(idx, len(prioritized_intents), intent_type, priority, delay, fill)

        return CoordinatedExecution(
            executions=executions,
            total_delay_seconds=total_delay,
            coordination_strategy=coordination_strategy,
            execution_order=execution_order,
            success=all_success,
            partial_success=any_success and not all_success,
            failed_intents=failed_intents,
        )

    def _planned_execution_delay(
        self,
        intent: "Intent",
        index: int,
        total_intents: int,
        apply_delays: bool,
    ) -> float:
        if not apply_delays:
            return 0.0
        return self._calculate_execution_delay(intent, index, total_intents)

    @staticmethod
    def _execution_succeeded(fill: Any) -> bool:
        return fill is None or bool(getattr(fill, "success", False))

    @staticmethod
    def _log_plan_execution(
        index: int,
        total_intents: int,
        intent_type: str,
        priority: ExecutionPriority,
        delay: float,
        fill: Any,
    ) -> None:
        logger.debug(
            "Coordinated execution %d/%d: %s (priority=%s, delay=%.2fs, success=%s)",
            index + 1,
            total_intents,
            intent_type,
            priority.value,
            delay,
            fill.success if fill else "delegated",
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
        return self._execute_prioritized_plan(
            [(intent, self._get_intent_priority(intent)) for intent in intents],
            portfolio,
            market_state,
            coordination_strategy="sequential",
            apply_delays=False,
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
    "UNIFIED_RISK_SCORE_MAX",
    "AggregatedRiskResult",
    "CoordinatedExecution",
    "ExecutionPriority",
    "MultiProtocolBacktestAdapter",
    "MultiProtocolBacktestConfig",
    "ProtocolExposure",
    "UnifiedLiquidationModel",
]
