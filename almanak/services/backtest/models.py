"""Pydantic request/response models for the BacktestService API."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    model_validator,
)
from pydantic.functional_validators import BeforeValidator

from almanak.framework.backtesting.models import decimal_str
from almanak.framework.models.base import validate_decimal_safe


def _decimal_verbatim(value: Decimal) -> str:
    """Preserve the Decimal's existing scale at the JSON boundary."""
    return str(value)


# Both aliases remain ``Decimal`` in Python mode.  Their serializers run only
# for JSON output, keeping stringification at the HTTP boundary while retaining
# the service's established wire formats: normalized metrics and scale-preserving
# trade/equity values.
JsonDecimal = Annotated[
    Decimal,
    BeforeValidator(validate_decimal_safe),
    PlainSerializer(_decimal_verbatim, return_type=str, when_used="json"),
]
JsonNormalizedDecimal = Annotated[
    Decimal,
    BeforeValidator(validate_decimal_safe),
    PlainSerializer(decimal_str, return_type=str, when_used="json"),
]

# ---------------------------------------------------------------------------
# Shared models
# ---------------------------------------------------------------------------


class ProgressInfo(BaseModel):
    """Structured progress for any long-running job."""

    percent: float = Field(0.0, ge=0.0, le=100.0)
    current_step: str = ""
    eta_seconds: int | None = None


class BacktestAction(StrEnum):
    """Closed StrategySpec action vocabulary owned by the HTTP service.

    These values deliberately preserve the existing lowercase API contract.
    They are service-level templates rather than a second intent vocabulary;
    the runner maps each action to canonical ``IntentType`` members.
    """

    SWAP = "swap"
    PROVIDE_LIQUIDITY = "provide_liquidity"
    LEND = "lend"
    SUPPLY = "supply"
    BORROW = "borrow"

    @classmethod
    def _missing_(cls, value: object) -> BacktestAction | None:
        """Keep the historical case-insensitive input behavior."""
        if not isinstance(value, str):
            return None
        normalized = value.strip().lower()
        return next((action for action in cls if action.value == normalized), None)


class StrategySpec(BaseModel):
    """Opaque strategy specification from any caller.

    The SDK translates this into internal backtest config.
    Edge (or any other caller) adapts its own types to this schema.
    """

    protocol: str = Field(..., description="e.g. 'uniswap_v3', 'aave_v3'")
    chain: str = Field(..., description="e.g. 'arbitrum', 'ethereum'")
    action: BacktestAction = Field(..., description="Backtest action template")
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

    model_config = ConfigDict(extra="forbid")

    strategy_spec: StrategySpec | None = None
    strategy_name: str | None = Field(None, description="Name of a registered SDK strategy")
    timeframe: TimeframeSpec
    chain: str | None = Field(None, description="Override chain (required when using strategy_name)")
    tokens: list[str] | None = Field(None, description="Tokens to track (required when using strategy_name)")
    token_funding: list[dict[str, Any]] | None = Field(None, description="Starting wallet token funding")
    mode: Literal["full", "quick"] = "full"

    @model_validator(mode="after")
    def _require_strategy(self) -> BacktestRequest:
        if not self.strategy_name and not self.strategy_spec:
            raise ValueError("Either strategy_name or strategy_spec must be provided")
        return self


class BacktestMetricsResponse(BaseModel):
    """Full backtest metrics aligned with the SDK BacktestMetrics dataclass.

    Financial values remain ``Decimal`` through service construction and are
    normalized to the existing JSON string representation only during JSON
    serialization. Field names match ``BacktestMetrics.to_dict()`` for
    compatibility. Extra engine metrics that are not part of this API response
    remain ignored.
    """

    model_config = ConfigDict(extra="ignore", from_attributes=True, allow_inf_nan=True)

    # --- Core PnL ---
    total_pnl_usd: JsonNormalizedDecimal = Decimal("0")
    net_pnl_usd: JsonNormalizedDecimal = Decimal("0")
    realized_pnl: JsonNormalizedDecimal = Decimal("0")
    unrealized_pnl: JsonNormalizedDecimal = Decimal("0")

    # --- Returns ---
    total_return_pct: JsonNormalizedDecimal = Decimal("0")
    annualized_return_pct: JsonNormalizedDecimal = Decimal("0")
    benchmark_return: JsonNormalizedDecimal | None = None

    # --- Risk metrics ---
    sharpe_ratio: JsonNormalizedDecimal = Decimal("0")
    sortino_ratio: JsonNormalizedDecimal = Decimal("0")
    calmar_ratio: JsonNormalizedDecimal = Decimal("0")
    volatility: JsonNormalizedDecimal = Decimal("0")
    max_drawdown_pct: JsonNormalizedDecimal = Decimal("0")
    information_ratio: JsonNormalizedDecimal | None = None
    beta: JsonNormalizedDecimal | None = None
    alpha: JsonNormalizedDecimal | None = None

    # --- Trade statistics ---
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: JsonNormalizedDecimal = Decimal("0")
    profit_factor: JsonNormalizedDecimal = Decimal("0")
    avg_trade_pnl_usd: JsonNormalizedDecimal = Decimal("0")
    largest_win_usd: JsonNormalizedDecimal = Decimal("0")
    largest_loss_usd: JsonNormalizedDecimal = Decimal("0")
    avg_win_usd: JsonNormalizedDecimal = Decimal("0")
    avg_loss_usd: JsonNormalizedDecimal = Decimal("0")

    # --- Execution costs ---
    total_fees_usd: JsonNormalizedDecimal = Decimal("0")
    total_slippage_usd: JsonNormalizedDecimal = Decimal("0")
    total_gas_usd: JsonNormalizedDecimal = Decimal("0")
    total_execution_cost_usd: JsonNormalizedDecimal = Decimal("0")
    avg_gas_price_gwei: JsonNormalizedDecimal = Decimal("0")
    max_gas_price_gwei: JsonNormalizedDecimal = Decimal("0")
    total_mev_cost_usd: JsonNormalizedDecimal = Decimal("0")

    # --- LP metrics ---
    total_fees_earned_usd: JsonNormalizedDecimal = Decimal("0")
    fees_by_pool: dict[str, JsonNormalizedDecimal] = Field(default_factory=dict)

    # --- Perp metrics ---
    total_funding_paid: JsonNormalizedDecimal = Decimal("0")
    total_funding_received: JsonNormalizedDecimal = Decimal("0")
    liquidations_count: int = 0
    liquidation_losses_usd: JsonNormalizedDecimal = Decimal("0")
    max_margin_utilization: JsonNormalizedDecimal = Decimal("0")

    # --- Lending metrics ---
    total_interest_earned: JsonNormalizedDecimal = Decimal("0")
    total_interest_paid: JsonNormalizedDecimal = Decimal("0")
    min_health_factor: JsonNormalizedDecimal = Decimal("999")
    health_factor_warnings: int = 0

    # --- Portfolio risk ---
    total_leverage: JsonNormalizedDecimal = Decimal("0")
    max_net_delta: dict[str, JsonNormalizedDecimal] = Field(default_factory=dict)
    max_net_delta_display_labels: dict[str, str] = Field(default_factory=dict)
    correlation_risk: JsonNormalizedDecimal | None = None
    liquidation_cascade_risk: JsonNormalizedDecimal = Decimal("0")

    # --- Breakdowns ---
    pnl_by_protocol: dict[str, JsonNormalizedDecimal] = Field(default_factory=dict)
    pnl_by_intent_type: dict[str, JsonNormalizedDecimal] = Field(default_factory=dict)
    pnl_by_asset: dict[str, JsonNormalizedDecimal] = Field(default_factory=dict)
    pnl_by_asset_display_labels: dict[str, str] = Field(default_factory=dict)


class BacktestEquityPointResponse(BaseModel):
    """One USD HTTP equity point with Decimal values retained in Python mode."""

    timestamp: str
    value_usd: JsonDecimal


class BacktestNumeraireEquityPointResponse(BacktestEquityPointResponse):
    """Equity point whose measured numeraire projection is present."""

    numeraire_price_usd: JsonNormalizedDecimal
    value_numeraire: JsonNormalizedDecimal


class BacktestTradeResponse(BaseModel):
    """One HTTP trade row with the established scale-preserving wire format."""

    timestamp: str
    intent_type: str
    amount_usd: JsonDecimal
    fee_usd: JsonDecimal
    slippage_usd: JsonDecimal
    pnl_usd: JsonDecimal | None
    status: Literal["filled", "rejected"]
    rejection_reason: str | None


class BacktestResultResponse(BaseModel):
    """Typed backtest result serialized only by the HTTP response boundary."""

    metrics: BacktestMetricsResponse
    equity_curve: list[BacktestNumeraireEquityPointResponse | BacktestEquityPointResponse] = Field(default_factory=list)
    trades: list[BacktestTradeResponse] = Field(default_factory=list)
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

    model_config = ConfigDict(extra="forbid")

    strategy_spec: StrategySpec | None = None
    strategy_name: str | None = None
    timeframe: TimeframeSpec | None = None  # defaults to last 7 days
    chain: str | None = None
    tokens: list[str] | None = None
    token_funding: list[dict[str, Any]] | None = None

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

    pnl_usd: JsonDecimal = Decimal("0")
    total_trades: int = 0
    gas_cost_usd: JsonDecimal = Decimal("0")


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
