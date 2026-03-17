"""Pydantic request/response models for the BacktestService API."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ---------------------------------------------------------------------------
# Shared models
# ---------------------------------------------------------------------------


class ProgressInfo(BaseModel):
    """Structured progress for any long-running job."""

    percent: float = Field(0.0, ge=0.0, le=100.0)
    current_step: str = ""
    eta_seconds: int | None = None


class StrategySpec(BaseModel):
    """Opaque strategy specification from any caller.

    The SDK translates this into internal backtest config.
    Edge (or any other caller) adapts its own types to this schema.
    """

    protocol: str = Field(..., description="e.g. 'uniswap_v3', 'aave_v3'")
    chain: str = Field(..., description="e.g. 'arbitrum', 'ethereum'")
    action: str = Field(..., description="e.g. 'swap', 'provide_liquidity'")
    parameters: dict[str, Any] = Field(default_factory=dict)


class TimeframeSpec(BaseModel):
    """Time range for a backtest."""

    start: date
    end: date


# ---------------------------------------------------------------------------
# Backtest models
# ---------------------------------------------------------------------------


class JobStatus(StrEnum):
    """Status of a backtest job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


class BacktestRequest(BaseModel):
    """Request to submit a backtest job.

    Two modes of specifying a strategy:
    1. ``strategy_spec`` — declarative spec (protocol + action + params).
       SDK builds a single-action strategy from this.
    2. ``strategy_name`` — name of a registered SDK strategy (e.g.
       "demo_uniswap_rsi", "aerodrome_mean_reversion_lp"). SDK loads the
       full strategy class with its decide() logic.

    At least one of ``strategy_spec`` or ``strategy_name`` must be provided.
    If both are given, ``strategy_name`` takes precedence.
    """

    strategy_spec: StrategySpec | None = None
    strategy_name: str | None = Field(None, description="Name of a registered SDK strategy")
    timeframe: TimeframeSpec
    chain: str | None = Field(None, description="Override chain (required when using strategy_name)")
    tokens: list[str] | None = Field(None, description="Tokens to track (required when using strategy_name)")
    initial_capital_usd: Decimal | None = Field(None, description="Override initial capital")
    mode: Literal["full", "quick"] = "full"

    @model_validator(mode="after")
    def _require_strategy(self) -> BacktestRequest:
        if not self.strategy_name and not self.strategy_spec:
            raise ValueError("Either strategy_name or strategy_spec must be provided")
        return self


class BacktestMetricsResponse(BaseModel):
    """Full backtest metrics aligned with the SDK BacktestMetrics dataclass.

    All Decimal values are serialized as strings for JSON safety.
    Field names match BacktestMetrics.to_dict() for consistency.
    Extra fields from to_dict() that aren't modeled here are silently ignored.
    """

    model_config = ConfigDict(extra="ignore")

    # --- Core PnL ---
    total_pnl_usd: str = "0"
    net_pnl_usd: str = "0"
    realized_pnl: str = "0"
    unrealized_pnl: str = "0"

    # --- Returns ---
    total_return_pct: str = "0"
    annualized_return_pct: str = "0"
    benchmark_return: str | None = None

    # --- Risk metrics ---
    sharpe_ratio: str = "0"
    sortino_ratio: str = "0"
    calmar_ratio: str = "0"
    volatility: str = "0"
    max_drawdown_pct: str = "0"
    information_ratio: str | None = None
    beta: str | None = None
    alpha: str | None = None

    # --- Trade statistics ---
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: str = "0"
    profit_factor: str = "0"
    avg_trade_pnl_usd: str = "0"
    largest_win_usd: str = "0"
    largest_loss_usd: str = "0"
    avg_win_usd: str = "0"
    avg_loss_usd: str = "0"

    # --- Execution costs ---
    total_fees_usd: str = "0"
    total_slippage_usd: str = "0"
    total_gas_usd: str = "0"
    total_execution_cost_usd: str = "0"
    avg_gas_price_gwei: str = "0"
    max_gas_price_gwei: str = "0"
    total_mev_cost_usd: str = "0"

    # --- LP metrics ---
    total_fees_earned_usd: str = "0"
    fees_by_pool: dict[str, str] = Field(default_factory=dict)

    # --- Perp metrics ---
    total_funding_paid: str = "0"
    total_funding_received: str = "0"
    liquidations_count: int = 0
    liquidation_losses_usd: str = "0"
    max_margin_utilization: str = "0"

    # --- Lending metrics ---
    total_interest_earned: str = "0"
    total_interest_paid: str = "0"
    min_health_factor: str = "999"
    health_factor_warnings: int = 0

    # --- Portfolio risk ---
    total_leverage: str = "0"
    max_net_delta: dict[str, str] = Field(default_factory=dict)
    correlation_risk: str | None = None
    liquidation_cascade_risk: str = "0"

    # --- Breakdowns ---
    pnl_by_protocol: dict[str, str] = Field(default_factory=dict)
    pnl_by_intent_type: dict[str, str] = Field(default_factory=dict)
    pnl_by_asset: dict[str, str] = Field(default_factory=dict)


class BacktestResultResponse(BaseModel):
    """Serialized backtest result for HTTP responses."""

    metrics: BacktestMetricsResponse
    equity_curve: list[dict[str, Any]] = Field(default_factory=list)
    trades: list[dict[str, Any]] = Field(default_factory=list)
    duration_seconds: float = 0.0


class BacktestJobResponse(BaseModel):
    """Response for a backtest job (submit or poll)."""

    job_id: str
    status: JobStatus
    progress: ProgressInfo = Field(default_factory=ProgressInfo)  # type: ignore[arg-type]
    result: BacktestResultResponse | None = None
    error: str | None = None
    created_at: datetime
    completed_at: datetime | None = None


class QuickBacktestRequest(BaseModel):
    """Request for a synchronous quick eligibility check."""

    strategy_spec: StrategySpec | None = None
    strategy_name: str | None = None
    timeframe: TimeframeSpec | None = None  # defaults to last 7 days
    chain: str | None = None
    tokens: list[str] | None = None
    initial_capital_usd: Decimal | None = None

    @model_validator(mode="after")
    def _require_strategy(self) -> QuickBacktestRequest:
        if not self.strategy_name and not self.strategy_spec:
            raise ValueError("Either strategy_name or strategy_spec must be provided")
        return self


class QuickBacktestResponse(BaseModel):
    """Response from a quick eligibility check."""

    eligible: bool
    metrics: BacktestMetricsResponse
    duration_seconds: float


class StrategyListResponse(BaseModel):
    """Response listing all available strategies."""

    strategies: list[str]
    count: int


# ---------------------------------------------------------------------------
# Paper trading models
# ---------------------------------------------------------------------------


class PaperTradeRequest(BaseModel):
    """Request to start a paper trading session."""

    strategy_spec: StrategySpec
    chain: str
    duration_hours: float | None = None  # None = indefinite
    initial_capital_usd: Decimal = Decimal("10000")
    tick_interval_seconds: int = 60


class PaperTradeSessionStatus(StrEnum):
    """Status of a paper trading session."""

    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class PaperTradeLiveMetrics(BaseModel):
    """Live metrics from a running paper trading session."""

    pnl_usd: str = "0"
    total_trades: int = 0
    gas_cost_usd: str = "0"


class PaperTradeSessionResponse(BaseModel):
    """Response for a paper trading session."""

    session_id: str
    status: PaperTradeSessionStatus
    progress: ProgressInfo = Field(default_factory=ProgressInfo)  # type: ignore[arg-type]
    metrics: PaperTradeLiveMetrics = Field(default_factory=PaperTradeLiveMetrics)
    result: BacktestResultResponse | None = None
    created_at: datetime
    stopped_at: datetime | None = None


# ---------------------------------------------------------------------------
# Fee model models
# ---------------------------------------------------------------------------


class FeeModelSummary(BaseModel):
    """Summary of a fee model for listing."""

    protocol: str
    model_name: str
    supported_chains: list[str] = Field(default_factory=list)


class FeeModelDetail(BaseModel):
    """Detailed fee model information."""

    protocol: str
    model_name: str
    fee_tiers: list[float] = Field(default_factory=list)
    default_fee: float | None = None
    slippage_model: str = "default"
    supported_intent_types: list[str] = Field(default_factory=list)
    supported_chains: list[str] = Field(default_factory=list)
    gas_estimates: dict[str, int] = Field(default_factory=dict)
    raw_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Protocol-specific configuration from the fee model's to_dict()",
    )


class FeeModelListResponse(BaseModel):
    """Response listing all fee models."""

    protocols: list[FeeModelSummary]


# ---------------------------------------------------------------------------
# Health models
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    """Health check response with resource reporting."""

    status: str  # "ok", "degraded"
    version: str
    active_backtest_jobs: int = 0
    active_paper_sessions: int = 0
    uptime_seconds: float = 0.0
    peak_memory_mb: float = 0.0
    cpu_percent: float | None = None
