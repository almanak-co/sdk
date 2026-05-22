"""Canary Deployment System for safe strategy version testing.

This module provides canary deployment functionality that allows:
- Deploying new strategy versions with limited capital allocation
- Running canary and stable versions in parallel
- Monitoring and comparing performance between versions
- Automatic promotion if canary outperforms stable
- Automatic rollback if canary underperforms

Usage:
    from almanak.framework.deployment.canary import CanaryDeployment, CanaryConfig

    # Configure canary deployment
    config = CanaryConfig(
        canary_percent=10,
        observation_period_minutes=60,
        auto_promote=True,
        auto_rollback=True,
    )

    # Create deployment manager
    canary = CanaryDeployment(
        deployment_id="my_strategy",
        stable_version_id="v_stable",
        canary_version_id="v_canary",
        config=config,
    )

    # Start canary
    result = await canary.deploy_canary(capital_usd=Decimal("100000"))

    # Check status
    comparison = canary.compare_performance()

    # Manual promotion or rollback
    await canary.promote_canary()
    # or
    await canary.rollback_canary()
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from ..models.strategy_version import PerformanceMetrics

logger = logging.getLogger(__name__)


class CanaryStatus(StrEnum):
    """Status of a canary deployment."""

    # Canary not yet started
    PENDING = "PENDING"

    # Canary is running and being monitored
    RUNNING = "RUNNING"

    # Observation period completed, awaiting decision
    OBSERVATION_COMPLETE = "OBSERVATION_COMPLETE"

    # Canary was promoted to stable
    PROMOTED = "PROMOTED"

    # Canary was rolled back
    ROLLED_BACK = "ROLLED_BACK"

    # Canary was manually cancelled
    CANCELLED = "CANCELLED"

    # Canary failed due to critical error
    FAILED = "FAILED"


class CanaryDecision(StrEnum):
    """Decision to make about a canary deployment."""

    # Continue observing
    CONTINUE = "CONTINUE"

    # Promote canary to stable
    PROMOTE = "PROMOTE"

    # Rollback to stable
    ROLLBACK = "ROLLBACK"

    # Requires manual review
    MANUAL_REVIEW = "MANUAL_REVIEW"


class CanaryEventType(StrEnum):
    """Types of canary-specific events."""

    CANARY_STARTED = "CANARY_STARTED"
    CANARY_OBSERVATION_COMPLETE = "CANARY_OBSERVATION_COMPLETE"
    CANARY_PROMOTED = "CANARY_PROMOTED"
    CANARY_ROLLED_BACK = "CANARY_ROLLED_BACK"
    CANARY_CANCELLED = "CANARY_CANCELLED"
    CANARY_FAILED = "CANARY_FAILED"
    CANARY_METRICS_UPDATED = "CANARY_METRICS_UPDATED"


@dataclass
class PromotionCriteria:
    """Criteria for automatic canary promotion.

    Defines the thresholds that determine whether a canary
    should be automatically promoted, rolled back, or requires
    manual review.

    Attributes:
        min_pnl_ratio: Minimum ratio of canary PnL to stable PnL (e.g., 0.9 = 90%)
        max_drawdown_ratio: Maximum ratio of canary drawdown to stable drawdown (e.g., 1.2 = 120%)
        min_sharpe_ratio: Minimum ratio of canary Sharpe to stable Sharpe (e.g., 0.8 = 80%)
        min_win_rate_ratio: Minimum ratio of canary win rate to stable win rate
        min_trades: Minimum number of trades required for evaluation
        max_error_rate: Maximum allowed error rate (errors / trades)
        require_positive_pnl: Whether canary must have positive PnL to promote
    """

    min_pnl_ratio: Decimal = Decimal("0.9")
    max_drawdown_ratio: Decimal = Decimal("1.2")
    min_sharpe_ratio: Decimal = Decimal("0.8")
    min_win_rate_ratio: Decimal = Decimal("0.8")
    min_trades: int = 5
    max_error_rate: Decimal = Decimal("0.1")
    require_positive_pnl: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "min_pnl_ratio": str(self.min_pnl_ratio),
            "max_drawdown_ratio": str(self.max_drawdown_ratio),
            "min_sharpe_ratio": str(self.min_sharpe_ratio),
            "min_win_rate_ratio": str(self.min_win_rate_ratio),
            "min_trades": self.min_trades,
            "max_error_rate": str(self.max_error_rate),
            "require_positive_pnl": self.require_positive_pnl,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PromotionCriteria":
        """Create from dictionary."""
        return cls(
            min_pnl_ratio=Decimal(data.get("min_pnl_ratio", "0.9")),
            max_drawdown_ratio=Decimal(data.get("max_drawdown_ratio", "1.2")),
            min_sharpe_ratio=Decimal(data.get("min_sharpe_ratio", "0.8")),
            min_win_rate_ratio=Decimal(data.get("min_win_rate_ratio", "0.8")),
            min_trades=data.get("min_trades", 5),
            max_error_rate=Decimal(data.get("max_error_rate", "0.1")),
            require_positive_pnl=data.get("require_positive_pnl", False),
        )


@dataclass
class CanaryConfig:
    """Configuration for a canary deployment.

    Attributes:
        canary_percent: Percentage of total capital allocated to canary (1-50)
        observation_period_minutes: How long to observe before deciding (min 5)
        auto_promote: Automatically promote if criteria met
        auto_rollback: Automatically rollback if criteria not met
        promotion_criteria: Criteria for promotion decisions
        check_interval_seconds: How often to check metrics
        emit_events: Whether to emit timeline events
    """

    canary_percent: int = 10
    observation_period_minutes: int = 60
    auto_promote: bool = True
    auto_rollback: bool = True
    promotion_criteria: PromotionCriteria = field(default_factory=PromotionCriteria)
    check_interval_seconds: int = 60
    emit_events: bool = True

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not 1 <= self.canary_percent <= 50:
            raise ValueError(f"canary_percent must be between 1 and 50, got {self.canary_percent}")
        if self.observation_period_minutes < 5:
            raise ValueError(f"observation_period_minutes must be at least 5, got {self.observation_period_minutes}")
        if self.check_interval_seconds < 10:
            raise ValueError(f"check_interval_seconds must be at least 10, got {self.check_interval_seconds}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "canary_percent": self.canary_percent,
            "observation_period_minutes": self.observation_period_minutes,
            "auto_promote": self.auto_promote,
            "auto_rollback": self.auto_rollback,
            "promotion_criteria": self.promotion_criteria.to_dict(),
            "check_interval_seconds": self.check_interval_seconds,
            "emit_events": self.emit_events,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CanaryConfig":
        """Create from dictionary."""
        criteria = PromotionCriteria.from_dict(data.get("promotion_criteria", {}))
        return cls(
            canary_percent=data.get("canary_percent", 10),
            observation_period_minutes=data.get("observation_period_minutes", 60),
            auto_promote=data.get("auto_promote", True),
            auto_rollback=data.get("auto_rollback", True),
            promotion_criteria=criteria,
            check_interval_seconds=data.get("check_interval_seconds", 60),
            emit_events=data.get("emit_events", True),
        )


@dataclass
class CanaryMetrics:
    """Performance metrics tracked during canary deployment.

    Extends PerformanceMetrics with canary-specific tracking.

    Attributes:
        version_id: The version these metrics belong to
        capital_allocated_usd: Capital allocated to this version
        metrics: The underlying performance metrics
        error_count: Number of errors encountered
        trade_count: Number of trades executed
        is_canary: Whether this is the canary version
        measurement_start: When metrics collection started
    """

    version_id: str
    capital_allocated_usd: Decimal
    metrics: PerformanceMetrics
    error_count: int = 0
    trade_count: int = 0
    is_canary: bool = False
    measurement_start: datetime | None = None

    def __post_init__(self) -> None:
        """Set default values after initialization."""
        if self.measurement_start is None:
            self.measurement_start = datetime.now(UTC)

    @property
    def error_rate(self) -> Decimal:
        """Calculate the error rate."""
        if self.trade_count == 0:
            return Decimal("0")
        return Decimal(str(self.error_count)) / Decimal(str(self.trade_count))

    @property
    def duration_seconds(self) -> int:
        """Get the duration of metrics collection."""
        if not self.measurement_start:
            return 0
        return int((datetime.now(UTC) - self.measurement_start).total_seconds())

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "version_id": self.version_id,
            "capital_allocated_usd": str(self.capital_allocated_usd),
            "metrics": self.metrics.to_dict(),
            "error_count": self.error_count,
            "trade_count": self.trade_count,
            "is_canary": self.is_canary,
            "measurement_start": self.measurement_start.isoformat() if self.measurement_start else None,
            "error_rate": str(self.error_rate),
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CanaryMetrics":
        """Create from dictionary."""
        return cls(
            version_id=data["version_id"],
            capital_allocated_usd=Decimal(data["capital_allocated_usd"]),
            metrics=PerformanceMetrics.from_dict(data["metrics"]),
            error_count=data.get("error_count", 0),
            trade_count=data.get("trade_count", 0),
            is_canary=data.get("is_canary", False),
            measurement_start=datetime.fromisoformat(data["measurement_start"])
            if data.get("measurement_start")
            else None,
        )


@dataclass
class CanaryComparison:
    """Comparison results between canary and stable versions.

    Provides detailed comparison of performance metrics to
    support promotion decisions.

    Attributes:
        canary_metrics: Metrics for the canary version
        stable_metrics: Metrics for the stable version
        pnl_ratio: Canary PnL / Stable PnL ratio
        drawdown_ratio: Canary drawdown / Stable drawdown ratio
        sharpe_ratio: Canary Sharpe / Stable Sharpe ratio
        win_rate_ratio: Canary win rate / Stable win rate ratio
        decision: Recommended decision based on comparison
        decision_reasons: List of reasons supporting the decision
    """

    canary_metrics: CanaryMetrics
    stable_metrics: CanaryMetrics
    pnl_ratio: Decimal | None = None
    drawdown_ratio: Decimal | None = None
    sharpe_ratio: Decimal | None = None
    win_rate_ratio: Decimal | None = None
    decision: CanaryDecision = CanaryDecision.CONTINUE
    decision_reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "canary_metrics": self.canary_metrics.to_dict(),
            "stable_metrics": self.stable_metrics.to_dict(),
            "pnl_ratio": str(self.pnl_ratio) if self.pnl_ratio is not None else None,
            "drawdown_ratio": str(self.drawdown_ratio) if self.drawdown_ratio is not None else None,
            "sharpe_ratio": str(self.sharpe_ratio) if self.sharpe_ratio is not None else None,
            "win_rate_ratio": str(self.win_rate_ratio) if self.win_rate_ratio is not None else None,
            "decision": self.decision.value,
            "decision_reasons": self.decision_reasons,
        }


@dataclass
class CanaryState:
    """Current state of a canary deployment.

    Tracks the full state of an ongoing or completed canary deployment.

    Attributes:
        deployment_id: Unique identifier for this deployment
        deployment_id: Strategy being deployed
        stable_version_id: ID of the stable version
        canary_version_id: ID of the canary version
        status: Current deployment status
        config: Deployment configuration
        started_at: When the deployment started
        ended_at: When the deployment ended (if complete)
        canary_metrics: Performance metrics for canary
        stable_metrics: Performance metrics for stable
        total_capital_usd: Total capital across both versions
        decision_history: History of decisions made
    """

    deployment_id: str
    stable_version_id: str
    canary_version_id: str
    status: CanaryStatus = CanaryStatus.PENDING
    config: CanaryConfig = field(default_factory=CanaryConfig)
    started_at: datetime | None = None
    ended_at: datetime | None = None
    canary_metrics: CanaryMetrics | None = None
    stable_metrics: CanaryMetrics | None = None
    total_capital_usd: Decimal = Decimal("0")
    decision_history: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Generate deployment ID if not provided."""
        if not self.deployment_id:
            ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
            self.deployment_id = f"canary_{self.deployment_id}_{ts}"

    @property
    def canary_capital_usd(self) -> Decimal:
        """Calculate capital allocated to canary."""
        return self.total_capital_usd * Decimal(str(self.config.canary_percent)) / Decimal("100")

    @property
    def stable_capital_usd(self) -> Decimal:
        """Calculate capital allocated to stable."""
        return self.total_capital_usd - self.canary_capital_usd

    @property
    def observation_deadline(self) -> datetime | None:
        """Calculate when observation period ends."""
        if not self.started_at:
            return None
        return self.started_at + timedelta(minutes=self.config.observation_period_minutes)

    @property
    def observation_remaining_seconds(self) -> int:
        """Calculate seconds remaining in observation period."""
        if not self.observation_deadline:
            return 0
        remaining = (self.observation_deadline - datetime.now(UTC)).total_seconds()
        return max(0, int(remaining))

    @property
    def observation_complete(self) -> bool:
        """Check if observation period is complete."""
        return self.observation_remaining_seconds == 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "deployment_id": self.deployment_id,
            "stable_version_id": self.stable_version_id,
            "canary_version_id": self.canary_version_id,
            "status": self.status.value,
            "config": self.config.to_dict(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "canary_metrics": self.canary_metrics.to_dict() if self.canary_metrics else None,
            "stable_metrics": self.stable_metrics.to_dict() if self.stable_metrics else None,
            "total_capital_usd": str(self.total_capital_usd),
            "decision_history": self.decision_history,
            "canary_capital_usd": str(self.canary_capital_usd),
            "stable_capital_usd": str(self.stable_capital_usd),
            "observation_deadline": self.observation_deadline.isoformat() if self.observation_deadline else None,
            "observation_remaining_seconds": self.observation_remaining_seconds,
            "observation_complete": self.observation_complete,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CanaryState":
        """Create from dictionary."""
        canary_metrics = None
        if data.get("canary_metrics"):
            canary_metrics = CanaryMetrics.from_dict(data["canary_metrics"])

        stable_metrics = None
        if data.get("stable_metrics"):
            stable_metrics = CanaryMetrics.from_dict(data["stable_metrics"])

        return cls(
            deployment_id=data["deployment_id"],
            stable_version_id=data["stable_version_id"],
            canary_version_id=data["canary_version_id"],
            status=CanaryStatus(data["status"]),
            config=CanaryConfig.from_dict(data.get("config", {})),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            ended_at=datetime.fromisoformat(data["ended_at"]) if data.get("ended_at") else None,
            canary_metrics=canary_metrics,
            stable_metrics=stable_metrics,
            total_capital_usd=Decimal(data.get("total_capital_usd", "0")),
            decision_history=data.get("decision_history", []),
        )


@dataclass
class CanaryResult:
    """Result of a canary decision or action.

    Attributes:
        success: Whether the action succeeded
        decision: The decision made
        comparison: Performance comparison (if available)
        error: Error message if failed
        message: Human-readable status message
    """

    success: bool
    decision: CanaryDecision = CanaryDecision.CONTINUE
    comparison: CanaryComparison | None = None
    error: str | None = None
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "decision": self.decision.value,
            "comparison": self.comparison.to_dict() if self.comparison else None,
            "error": self.error,
            "message": self.message,
        }


@dataclass
class DeployCanaryResult:
    """Result of initiating a canary deployment.

    Attributes:
        success: Whether deployment started successfully
        deployment_id: Unique ID of the deployment
        state: Current canary state
        error: Error message if failed
    """

    success: bool
    deployment_id: str = ""
    state: CanaryState | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "deployment_id": self.deployment_id,
            "state": self.state.to_dict() if self.state else None,
            "error": self.error,
        }


# Type aliases for callbacks
CanaryCallback = Callable[[CanaryState], None]
MetricsProvider = Callable[[str], PerformanceMetrics]


class CanaryDeployment:
    """Manages canary deployments for safe strategy version testing.

    This class handles the full lifecycle of a canary deployment:
    1. Deploy canary with limited capital allocation
    2. Run both versions in parallel
    3. Monitor and compare performance
    4. Auto-promote or auto-rollback based on criteria

    Attributes:
        deployment_id: ID of the strategy being deployed
        stable_version_id: ID of the current stable version
        canary_version_id: ID of the new canary version
        state: Current deployment state
        config: Deployment configuration
    """

    def __init__(
        self,
        deployment_id: str,
        stable_version_id: str,
        canary_version_id: str,
        config: CanaryConfig | None = None,
        on_promote: CanaryCallback | None = None,
        on_rollback: CanaryCallback | None = None,
        on_state_change: CanaryCallback | None = None,
        metrics_provider: MetricsProvider | None = None,
        chain: str = "unknown",
    ) -> None:
        """Initialize the canary deployment manager.

        Args:
            deployment_id: ID of the strategy being deployed
            stable_version_id: ID of the current stable version
            canary_version_id: ID of the new canary version
            config: Deployment configuration (uses defaults if not provided)
            on_promote: Callback when canary is promoted
            on_rollback: Callback when canary is rolled back
            on_state_change: Callback on any state change
            metrics_provider: Function to fetch metrics for a version
            chain: Blockchain network for event emission
        """
        self.deployment_id = deployment_id
        self.stable_version_id = stable_version_id
        self.canary_version_id = canary_version_id
        self.config = config or CanaryConfig()
        self._chain = chain

        # Callbacks
        self._on_promote = on_promote
        self._on_rollback = on_rollback
        self._on_state_change = on_state_change
        self._metrics_provider = metrics_provider

        # Initialize state
        self.state = CanaryState(
            deployment_id=deployment_id,
            stable_version_id=stable_version_id,
            canary_version_id=canary_version_id,
            config=self.config,
        )

        # Monitoring task handle
        self._monitoring_task: asyncio.Task[None] | None = None

        logger.info(
            f"CanaryDeployment initialized for strategy {deployment_id}: "
            f"stable={stable_version_id}, canary={canary_version_id}"
        )

    async def deploy_canary(
        self,
        capital_usd: Decimal,
    ) -> DeployCanaryResult:
        """Start the canary deployment.

        Allocates capital between canary and stable versions and
        begins the observation period.

        Args:
            capital_usd: Total capital to allocate across both versions

        Returns:
            DeployCanaryResult with deployment details
        """
        if self.state.status != CanaryStatus.PENDING:
            return DeployCanaryResult(
                success=False,
                error=f"Cannot deploy canary in status {self.state.status.value}",
            )

        if capital_usd <= Decimal("0"):
            return DeployCanaryResult(
                success=False,
                error="Capital must be positive",
            )

        # Update state
        self.state.total_capital_usd = capital_usd
        self.state.started_at = datetime.now(UTC)
        self.state.status = CanaryStatus.RUNNING

        # Initialize metrics for both versions
        self.state.canary_metrics = CanaryMetrics(
            version_id=self.canary_version_id,
            capital_allocated_usd=self.state.canary_capital_usd,
            metrics=PerformanceMetrics(),
            is_canary=True,
            measurement_start=datetime.now(UTC),
        )

        self.state.stable_metrics = CanaryMetrics(
            version_id=self.stable_version_id,
            capital_allocated_usd=self.state.stable_capital_usd,
            metrics=PerformanceMetrics(),
            is_canary=False,
            measurement_start=datetime.now(UTC),
        )

        # Emit deployment started event
        self._emit_event(
            CanaryEventType.CANARY_STARTED,
            f"Canary deployment started: {self.config.canary_percent}% allocation, "
            f"{self.config.observation_period_minutes}min observation period",
            {
                "canary_percent": self.config.canary_percent,
                "canary_capital_usd": str(self.state.canary_capital_usd),
                "stable_capital_usd": str(self.state.stable_capital_usd),
                "observation_period_minutes": self.config.observation_period_minutes,
            },
        )

        # Start background monitoring
        self._monitoring_task = asyncio.create_task(self._monitor_loop())

        logger.info(
            f"Canary deployment {self.state.deployment_id} started: "
            f"canary={self.state.canary_capital_usd} USD, "
            f"stable={self.state.stable_capital_usd} USD"
        )

        # Notify state change
        self._notify_state_change()

        return DeployCanaryResult(
            success=True,
            deployment_id=self.state.deployment_id,
            state=self.state,
        )

    async def _monitor_loop(self) -> None:
        """Background loop to monitor canary performance.

        Runs until observation period completes or deployment ends.
        """
        while self.state.status == CanaryStatus.RUNNING:
            try:
                # Update metrics from provider if available
                if self._metrics_provider:
                    await self._update_metrics()

                # Check if observation period is complete
                if self.state.observation_complete:
                    await self._handle_observation_complete()
                    break

                # Sleep until next check
                await asyncio.sleep(self.config.check_interval_seconds)

            except asyncio.CancelledError:
                logger.info(f"Monitoring loop cancelled for {self.state.deployment_id}")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.config.check_interval_seconds)

    async def _update_metrics(self) -> None:
        """Update metrics from the metrics provider."""
        if not self._metrics_provider:
            return

        try:
            # Update canary metrics
            if self.state.canary_metrics:
                canary_perf = self._metrics_provider(self.canary_version_id)
                self.state.canary_metrics.metrics = canary_perf
                self.state.canary_metrics.trade_count = canary_perf.total_trades

            # Update stable metrics
            if self.state.stable_metrics:
                stable_perf = self._metrics_provider(self.stable_version_id)
                self.state.stable_metrics.metrics = stable_perf
                self.state.stable_metrics.trade_count = stable_perf.total_trades

            # Emit metrics updated event
            self._emit_event(
                CanaryEventType.CANARY_METRICS_UPDATED,
                "Canary metrics updated",
                {
                    "canary_pnl_usd": str(self.state.canary_metrics.metrics.net_pnl_usd)
                    if self.state.canary_metrics
                    else "0",
                    "stable_pnl_usd": str(self.state.stable_metrics.metrics.net_pnl_usd)
                    if self.state.stable_metrics
                    else "0",
                },
            )

        except Exception as e:
            logger.error(f"Error updating metrics: {e}")

    async def _handle_observation_complete(self) -> None:
        """Handle completion of the observation period."""
        self.state.status = CanaryStatus.OBSERVATION_COMPLETE

        # Compare performance
        comparison = self.compare_performance()

        # Emit observation complete event
        self._emit_event(
            CanaryEventType.CANARY_OBSERVATION_COMPLETE,
            f"Observation period complete. Decision: {comparison.decision.value}",
            {
                "decision": comparison.decision.value,
                "decision_reasons": comparison.decision_reasons,
            },
        )

        # Record decision in history
        self.state.decision_history.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "decision": comparison.decision.value,
                "reasons": comparison.decision_reasons,
                "comparison": comparison.to_dict(),
            }
        )

        # Auto-promote or auto-rollback based on config and decision
        if comparison.decision == CanaryDecision.PROMOTE and self.config.auto_promote:
            await self.promote_canary()
        elif comparison.decision == CanaryDecision.ROLLBACK and self.config.auto_rollback:
            await self.rollback_canary()
        else:
            logger.info(
                f"Canary {self.state.deployment_id} requires manual review: decision={comparison.decision.value}"
            )

        # Notify state change
        self._notify_state_change()

    def compare_performance(self) -> CanaryComparison:
        """Compare canary performance against stable version.

        Evaluates performance metrics against promotion criteria
        and returns a decision recommendation.

        The function is a thin orchestrator over four pure helpers:
        ``_compare_insufficient_metrics`` (short-circuit when either side
        is missing), ``_compute_metric_ratios`` (produce the four ratios
        with the documented division-by-zero sentinels),
        ``_decide_promotion`` (evaluate the decision cascade), and
        ``CanaryComparison`` assembly.

        Returns:
            CanaryComparison with metrics and decision
        """
        missing = self._compare_insufficient_metrics()
        if missing is not None:
            return missing

        # Narrowing for type checkers - ``_compare_insufficient_metrics``
        # returning ``None`` guarantees both sides are populated.
        assert self.state.canary_metrics is not None
        assert self.state.stable_metrics is not None
        canary = self.state.canary_metrics
        stable = self.state.stable_metrics
        criteria = self.config.promotion_criteria

        pnl_ratio, drawdown_ratio, sharpe_ratio, win_rate_ratio = self._compute_metric_ratios(canary, stable)

        decision, reasons = self._decide_promotion(
            canary=canary,
            criteria=criteria,
            pnl_ratio=pnl_ratio,
            drawdown_ratio=drawdown_ratio,
            sharpe_ratio=sharpe_ratio,
            win_rate_ratio=win_rate_ratio,
        )

        return CanaryComparison(
            canary_metrics=canary,
            stable_metrics=stable,
            pnl_ratio=pnl_ratio,
            drawdown_ratio=drawdown_ratio,
            sharpe_ratio=sharpe_ratio,
            win_rate_ratio=win_rate_ratio,
            decision=decision,
            decision_reasons=reasons,
        )

    def _compare_insufficient_metrics(self) -> CanaryComparison | None:
        """Return an insufficient-data ``CanaryComparison`` if metrics are
        missing on either side, otherwise ``None``.

        Preserves the prior behaviour of synthesising a placeholder
        ``CanaryMetrics`` for whichever side is missing so callers always
        receive a populated ``canary_metrics`` + ``stable_metrics`` pair.
        """
        if self.state.canary_metrics and self.state.stable_metrics:
            return None

        canary_metrics = self.state.canary_metrics or CanaryMetrics(
            version_id=self.canary_version_id,
            capital_allocated_usd=Decimal("0"),
            metrics=PerformanceMetrics(),
            is_canary=True,
        )
        stable_metrics = self.state.stable_metrics or CanaryMetrics(
            version_id=self.stable_version_id,
            capital_allocated_usd=Decimal("0"),
            metrics=PerformanceMetrics(),
            is_canary=False,
        )
        return CanaryComparison(
            canary_metrics=canary_metrics,
            stable_metrics=stable_metrics,
            decision=CanaryDecision.CONTINUE,
            decision_reasons=["Insufficient metrics data"],
        )

    @staticmethod
    def _compute_metric_ratios(
        canary: CanaryMetrics, stable: CanaryMetrics
    ) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
        """Compute the four comparison ratios (pnl, drawdown, sharpe,
        win_rate).

        Division-by-zero is encoded via sentinel values that preserve
        the pre-refactor semantics:
        - ``pnl_ratio`` = ``+999`` when stable=0 and canary>0;
          ``-999`` when stable=0 and canary<0; ``None`` when both are 0.
        - ``drawdown_ratio`` = ``+999`` when stable=0 and canary>0;
          ``None`` when both are 0.
        - ``sharpe_ratio`` / ``win_rate_ratio`` = ``None`` whenever the
          stable side is zero/None or the canary side is falsy.
        """
        return (
            CanaryDeployment._compare_pnl(canary, stable),
            CanaryDeployment._compare_drawdown(canary, stable),
            CanaryDeployment._compare_sharpe(canary, stable),
            CanaryDeployment._compare_win_rate(canary, stable),
        )

    @staticmethod
    def _compare_pnl(canary: CanaryMetrics, stable: CanaryMetrics) -> Decimal | None:
        """PnL ratio with divide-by-zero sentinels. See ``_compute_metric_ratios``."""
        if stable.metrics.net_pnl_usd != Decimal("0"):
            return canary.metrics.net_pnl_usd / stable.metrics.net_pnl_usd
        if canary.metrics.net_pnl_usd > Decimal("0"):
            return Decimal("999")  # Canary positive, stable zero
        if canary.metrics.net_pnl_usd < Decimal("0"):
            return Decimal("-999")  # Canary negative, stable zero
        return None

    @staticmethod
    def _compare_drawdown(canary: CanaryMetrics, stable: CanaryMetrics) -> Decimal | None:
        """Drawdown ratio (lower is better) with divide-by-zero sentinel."""
        if stable.metrics.max_drawdown > Decimal("0"):
            return canary.metrics.max_drawdown / stable.metrics.max_drawdown
        if canary.metrics.max_drawdown > Decimal("0"):
            return Decimal("999")  # Canary has drawdown, stable zero
        return None

    @staticmethod
    def _compare_sharpe(canary: CanaryMetrics, stable: CanaryMetrics) -> Decimal | None:
        """Sharpe ratio, or ``None`` when either side is missing/zero."""
        stable_sharpe = stable.metrics.sharpe_ratio
        canary_sharpe = canary.metrics.sharpe_ratio
        if stable_sharpe and stable_sharpe != Decimal("0") and canary_sharpe:
            return canary_sharpe / stable_sharpe
        return None

    @staticmethod
    def _compare_win_rate(canary: CanaryMetrics, stable: CanaryMetrics) -> Decimal | None:
        """Win-rate ratio, or ``None`` when either side is missing/zero."""
        stable_wr = stable.metrics.win_rate
        canary_wr = canary.metrics.win_rate
        if stable_wr and stable_wr > Decimal("0") and canary_wr:
            return canary_wr / stable_wr
        return None

    @staticmethod
    def _decide_promotion(
        *,
        canary: CanaryMetrics,
        criteria: PromotionCriteria,
        pnl_ratio: Decimal | None,
        drawdown_ratio: Decimal | None,
        sharpe_ratio: Decimal | None,
        win_rate_ratio: Decimal | None,
    ) -> tuple[CanaryDecision, list[str]]:
        """Evaluate the promotion decision cascade.

        Cascade order (preserved from pre-refactor):
          0. any Decimal-NaN ratio                   -> MANUAL_REVIEW (new guard)
          1. ``trade_count < min_trades``            -> CONTINUE
          2. ``error_rate > max_error_rate``         -> ROLLBACK
          3. ``require_positive_pnl`` and pnl <= 0   -> ROLLBACK
          4. ``pnl_ratio < min_pnl_ratio``           -> ROLLBACK
          5. ``drawdown_ratio > max_drawdown_ratio`` -> ROLLBACK
          6. ``sharpe_ratio < min_sharpe_ratio``     -> MANUAL_REVIEW (soft)
          7. ``win_rate_ratio < min_win_rate_ratio`` -> MANUAL_REVIEW (soft)
          8. otherwise                               -> PROMOTE

        Short-circuits on the first matching gate. Returns the decision
        and a single-element ``reasons`` list explaining that gate.
        """
        # Gate 0: Decimal NaN short-circuit. NaN is non-orderable and
        # raises ``decimal.InvalidOperation`` on ``<`` / ``>`` under the
        # default context. ``PerformanceMetrics.from_dict`` can surface
        # NaN when external data is corrupt; surface as MANUAL_REVIEW
        # rather than crashing the promotion gate.
        if any(
            ratio is not None and ratio.is_nan() for ratio in (pnl_ratio, drawdown_ratio, sharpe_ratio, win_rate_ratio)
        ):
            return CanaryDecision.MANUAL_REVIEW, ["Non-comparable metric ratio (NaN)"]

        # Gate 1: sample size.
        if canary.trade_count < criteria.min_trades:
            return CanaryDecision.CONTINUE, [
                f"Insufficient trades: {canary.trade_count} < {criteria.min_trades}",
            ]

        # Gates 2-5: hard fails -> ROLLBACK.
        hard = CanaryDeployment._apply_hard_gates(
            canary=canary,
            criteria=criteria,
            pnl_ratio=pnl_ratio,
            drawdown_ratio=drawdown_ratio,
        )
        if hard is not None:
            return CanaryDecision.ROLLBACK, [hard]

        # Gates 6-7: soft fails -> MANUAL_REVIEW.
        soft = CanaryDeployment._apply_soft_gates(
            criteria=criteria,
            sharpe_ratio=sharpe_ratio,
            win_rate_ratio=win_rate_ratio,
        )
        if soft is not None:
            return CanaryDecision.MANUAL_REVIEW, [soft]

        # Gate 8: all criteria met.
        return CanaryDecision.PROMOTE, ["All promotion criteria met"]

    @staticmethod
    def _apply_hard_gates(
        *,
        canary: CanaryMetrics,
        criteria: PromotionCriteria,
        pnl_ratio: Decimal | None,
        drawdown_ratio: Decimal | None,
    ) -> str | None:
        """Return the first hard-gate failure reason, or ``None`` if all
        hard gates pass. Caller maps any failure to ROLLBACK."""
        if canary.error_rate > criteria.max_error_rate:
            return f"Error rate too high: {canary.error_rate} > {criteria.max_error_rate}"
        if criteria.require_positive_pnl and canary.metrics.net_pnl_usd <= Decimal("0"):
            return f"Canary PnL not positive: {canary.metrics.net_pnl_usd}"
        if pnl_ratio is not None and pnl_ratio < criteria.min_pnl_ratio:
            return f"PnL ratio too low: {pnl_ratio:.4f} < {criteria.min_pnl_ratio}"
        if drawdown_ratio is not None and drawdown_ratio > criteria.max_drawdown_ratio:
            return f"Drawdown ratio too high: {drawdown_ratio:.4f} > {criteria.max_drawdown_ratio}"
        return None

    @staticmethod
    def _apply_soft_gates(
        *,
        criteria: PromotionCriteria,
        sharpe_ratio: Decimal | None,
        win_rate_ratio: Decimal | None,
    ) -> str | None:
        """Return the first soft-gate failure reason, or ``None`` if all
        soft gates pass. Caller maps any failure to MANUAL_REVIEW."""
        if sharpe_ratio is not None and sharpe_ratio < criteria.min_sharpe_ratio:
            return f"Sharpe ratio too low: {sharpe_ratio:.4f} < {criteria.min_sharpe_ratio}"
        if win_rate_ratio is not None and win_rate_ratio < criteria.min_win_rate_ratio:
            return f"Win rate ratio too low: {win_rate_ratio:.4f} < {criteria.min_win_rate_ratio}"
        return None

    async def promote_canary(self) -> CanaryResult:
        """Promote the canary version to stable.

        Makes the canary version the new stable version and
        reallocates all capital to it.

        Returns:
            CanaryResult indicating success or failure
        """
        if self.state.status not in (CanaryStatus.RUNNING, CanaryStatus.OBSERVATION_COMPLETE):
            return CanaryResult(
                success=False,
                error=f"Cannot promote canary in status {self.state.status.value}",
            )

        # Stop monitoring
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        # Update state
        self.state.status = CanaryStatus.PROMOTED
        self.state.ended_at = datetime.now(UTC)

        # Emit promotion event
        self._emit_event(
            CanaryEventType.CANARY_PROMOTED,
            f"Canary version {self.canary_version_id} promoted to stable",
            {
                "new_stable_version": self.canary_version_id,
                "previous_stable_version": self.stable_version_id,
            },
        )

        # Record decision in history
        self.state.decision_history.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "action": "PROMOTED",
                "canary_version": self.canary_version_id,
            }
        )

        logger.info(f"Canary {self.state.deployment_id} promoted: {self.canary_version_id} is now stable")

        # Call promotion callback
        if self._on_promote:
            try:
                self._on_promote(self.state)
            except Exception as e:
                logger.error(f"Promotion callback failed: {e}")

        # Notify state change
        self._notify_state_change()

        return CanaryResult(
            success=True,
            decision=CanaryDecision.PROMOTE,
            message=f"Canary version {self.canary_version_id} promoted to stable",
        )

    async def rollback_canary(self) -> CanaryResult:
        """Rollback the canary deployment.

        Stops the canary version and reallocates all capital
        back to the stable version.

        Returns:
            CanaryResult indicating success or failure
        """
        if self.state.status not in (CanaryStatus.RUNNING, CanaryStatus.OBSERVATION_COMPLETE):
            return CanaryResult(
                success=False,
                error=f"Cannot rollback canary in status {self.state.status.value}",
            )

        # Stop monitoring
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        # Update state
        self.state.status = CanaryStatus.ROLLED_BACK
        self.state.ended_at = datetime.now(UTC)

        # Emit rollback event
        self._emit_event(
            CanaryEventType.CANARY_ROLLED_BACK,
            f"Canary version {self.canary_version_id} rolled back",
            {
                "rolled_back_version": self.canary_version_id,
                "stable_version": self.stable_version_id,
            },
        )

        # Record decision in history
        self.state.decision_history.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "action": "ROLLED_BACK",
                "canary_version": self.canary_version_id,
            }
        )

        logger.info(
            f"Canary {self.state.deployment_id} rolled back: returning to stable version {self.stable_version_id}"
        )

        # Call rollback callback
        if self._on_rollback:
            try:
                self._on_rollback(self.state)
            except Exception as e:
                logger.error(f"Rollback callback failed: {e}")

        # Notify state change
        self._notify_state_change()

        return CanaryResult(
            success=True,
            decision=CanaryDecision.ROLLBACK,
            message=f"Canary version {self.canary_version_id} rolled back, "
            f"stable version {self.stable_version_id} retained",
        )

    async def cancel_deployment(self) -> CanaryResult:
        """Cancel the canary deployment without promoting or rolling back.

        Returns:
            CanaryResult indicating success or failure
        """
        if self.state.status not in (CanaryStatus.PENDING, CanaryStatus.RUNNING, CanaryStatus.OBSERVATION_COMPLETE):
            return CanaryResult(
                success=False,
                error=f"Cannot cancel deployment in status {self.state.status.value}",
            )

        # Stop monitoring
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        # Update state
        self.state.status = CanaryStatus.CANCELLED
        self.state.ended_at = datetime.now(UTC)

        # Emit cancellation event
        self._emit_event(
            CanaryEventType.CANARY_CANCELLED,
            "Canary deployment cancelled",
            {
                "cancelled_at": datetime.now(UTC).isoformat(),
            },
        )

        logger.info(f"Canary deployment {self.state.deployment_id} cancelled")

        # Notify state change
        self._notify_state_change()

        return CanaryResult(
            success=True,
            decision=CanaryDecision.ROLLBACK,
            message="Canary deployment cancelled",
        )

    def update_canary_metrics(
        self,
        pnl_usd: Decimal | None = None,
        trades: int | None = None,
        errors: int | None = None,
        drawdown: Decimal | None = None,
    ) -> None:
        """Manually update canary metrics.

        Used when metrics_provider is not available.

        Args:
            pnl_usd: Net PnL in USD
            trades: Number of trades
            errors: Number of errors
            drawdown: Max drawdown
        """
        if not self.state.canary_metrics:
            return

        if pnl_usd is not None:
            self.state.canary_metrics.metrics.net_pnl_usd = pnl_usd
        if trades is not None:
            self.state.canary_metrics.trade_count = trades
            self.state.canary_metrics.metrics.total_trades = trades
        if errors is not None:
            self.state.canary_metrics.error_count = errors
        if drawdown is not None:
            self.state.canary_metrics.metrics.max_drawdown = drawdown

    def update_stable_metrics(
        self,
        pnl_usd: Decimal | None = None,
        trades: int | None = None,
        errors: int | None = None,
        drawdown: Decimal | None = None,
    ) -> None:
        """Manually update stable metrics.

        Used when metrics_provider is not available.

        Args:
            pnl_usd: Net PnL in USD
            trades: Number of trades
            errors: Number of errors
            drawdown: Max drawdown
        """
        if not self.state.stable_metrics:
            return

        if pnl_usd is not None:
            self.state.stable_metrics.metrics.net_pnl_usd = pnl_usd
        if trades is not None:
            self.state.stable_metrics.trade_count = trades
            self.state.stable_metrics.metrics.total_trades = trades
        if errors is not None:
            self.state.stable_metrics.error_count = errors
        if drawdown is not None:
            self.state.stable_metrics.metrics.max_drawdown = drawdown

    def _emit_event(
        self,
        event_type: CanaryEventType,
        description: str,
        details: dict[str, Any],
    ) -> None:
        """Emit a canary-related timeline event.

        Args:
            event_type: Type of canary event
            description: Human-readable description
            details: Additional event details
        """
        if not self.config.emit_events:
            return

        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.CUSTOM,
            description=description,
            deployment_id=self.deployment_id,
            chain=self._chain,
            details={
                "canary_event_type": event_type.value,
                "deployment_id": self.state.deployment_id,
                **details,
            },
        )

        add_event(event)
        logger.debug(f"Canary event emitted: {event_type.value} - {description}")

    def _notify_state_change(self) -> None:
        """Notify listeners of a state change."""
        if self._on_state_change:
            try:
                self._on_state_change(self.state)
            except Exception as e:
                logger.error(f"State change callback failed: {e}")

    def get_status(self) -> dict[str, Any]:
        """Get the current status summary.

        Returns:
            Dictionary with deployment status
        """
        comparison = None
        if self.state.canary_metrics and self.state.stable_metrics:
            comparison = self.compare_performance()

        return {
            "deployment_id": self.state.deployment_id,
            "status": self.state.status.value,
            "observation_remaining_seconds": self.state.observation_remaining_seconds,
            "canary_metrics": self.state.canary_metrics.to_dict() if self.state.canary_metrics else None,
            "stable_metrics": self.state.stable_metrics.to_dict() if self.state.stable_metrics else None,
            "comparison": comparison.to_dict() if comparison else None,
        }

    def to_dict(self) -> dict[str, Any]:
        """Export the deployment state for persistence.

        Returns:
            Dictionary containing full deployment state
        """
        return {
            "deployment_id": self.deployment_id,
            "stable_version_id": self.stable_version_id,
            "canary_version_id": self.canary_version_id,
            "config": self.config.to_dict(),
            "state": self.state.to_dict(),
            "chain": self._chain,
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        on_promote: CanaryCallback | None = None,
        on_rollback: CanaryCallback | None = None,
        on_state_change: CanaryCallback | None = None,
        metrics_provider: MetricsProvider | None = None,
    ) -> "CanaryDeployment":
        """Restore a deployment from persisted state.

        Args:
            data: Dictionary with deployment data
            on_promote: Optional promotion callback
            on_rollback: Optional rollback callback
            on_state_change: Optional state change callback
            metrics_provider: Optional metrics provider function

        Returns:
            CanaryDeployment instance with restored state
        """
        deployment = cls(
            deployment_id=data["deployment_id"],
            stable_version_id=data["stable_version_id"],
            canary_version_id=data["canary_version_id"],
            config=CanaryConfig.from_dict(data.get("config", {})),
            on_promote=on_promote,
            on_rollback=on_rollback,
            on_state_change=on_state_change,
            metrics_provider=metrics_provider,
            chain=data.get("chain", "unknown"),
        )

        # Restore state
        if data.get("state"):
            deployment.state = CanaryState.from_dict(data["state"])

        return deployment


__all__ = [
    "CanaryDeployment",
    "CanaryConfig",
    "CanaryState",
    "CanaryStatus",
    "CanaryMetrics",
    "CanaryDecision",
    "CanaryResult",
    "DeployCanaryResult",
    "CanaryEventType",
    "CanaryComparison",
    "PromotionCriteria",
    "CanaryCallback",
    "MetricsProvider",
]
