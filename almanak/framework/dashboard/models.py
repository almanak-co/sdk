"""Data models for the Almanak Operator Dashboard.

Contains all dataclasses and enums used across the dashboard.
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class StrategyStatus(StrEnum):
    """Status of a strategy."""

    RUNNING = "RUNNING"
    STUCK = "STUCK"
    PAUSED = "PAUSED"
    ERROR = "ERROR"
    INACTIVE = "INACTIVE"
    STALE = "STALE"
    ARCHIVED = "ARCHIVED"


class TimelineEventType(StrEnum):
    """Type of timeline event."""

    TRADE = "TRADE"
    SWAP = "SWAP"
    REBALANCE = "REBALANCE"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    LP_OPEN = "LP_OPEN"
    LP_CLOSE = "LP_CLOSE"
    BORROW = "BORROW"
    REPAY = "REPAY"
    ALERT = "ALERT"
    ERROR = "ERROR"
    CONFIG_UPDATE = "CONFIG_UPDATE"
    STATE_CHANGE = "STATE_CHANGE"
    TRANSACTION_SUBMITTED = "TRANSACTION_SUBMITTED"
    TRANSACTION_CONFIRMED = "TRANSACTION_CONFIRMED"
    TRANSACTION_FAILED = "TRANSACTION_FAILED"
    TRANSACTION_REVERTED = "TRANSACTION_REVERTED"
    STRATEGY_STARTED = "STRATEGY_STARTED"
    STRATEGY_PAUSED = "STRATEGY_PAUSED"
    STRATEGY_RESUMED = "STRATEGY_RESUMED"
    STRATEGY_STOPPED = "STRATEGY_STOPPED"
    OPERATOR_ACTION_EXECUTED = "OPERATOR_ACTION_EXECUTED"
    RISK_GUARD_TRIGGERED = "RISK_GUARD_TRIGGERED"
    CIRCUIT_BREAKER_TRIGGERED = "CIRCUIT_BREAKER_TRIGGERED"
    # Multi-chain events
    BRIDGE_INITIATED = "BRIDGE_INITIATED"
    BRIDGE_COMPLETED = "BRIDGE_COMPLETED"
    BRIDGE_FAILED = "BRIDGE_FAILED"


class ChainHealthStatus(StrEnum):
    """Health status of a chain RPC connection."""

    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"


class AvailableAction(StrEnum):
    """Actions that can be taken on a strategy."""

    BUMP_GAS = "BUMP_GAS"
    CANCEL_TX = "CANCEL_TX"
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    EMERGENCY_UNWIND = "EMERGENCY_UNWIND"


class StuckReason(StrEnum):
    """Reason why a strategy is stuck."""

    GAS_PRICE_BLOCKED = "GAS_PRICE_BLOCKED"
    NONCE_CONFLICT = "NONCE_CONFLICT"
    TRANSACTION_REVERTED = "TRANSACTION_REVERTED"
    NOT_INCLUDED_TIMEOUT = "NOT_INCLUDED_TIMEOUT"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    INSUFFICIENT_GAS = "INSUFFICIENT_GAS"
    ALLOWANCE_MISSING = "ALLOWANCE_MISSING"
    SLIPPAGE_EXCEEDED = "SLIPPAGE_EXCEEDED"
    POOL_LIQUIDITY_LOW = "POOL_LIQUIDITY_LOW"
    ORACLE_STALE = "ORACLE_STALE"
    PROTOCOL_PAUSED = "PROTOCOL_PAUSED"
    RPC_FAILURE = "RPC_FAILURE"
    RECEIPT_PARSE_FAILED = "RECEIPT_PARSE_FAILED"
    STATE_CONFLICT = "STATE_CONFLICT"
    RISK_GUARD_BLOCKED = "RISK_GUARD_BLOCKED"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"
    UNKNOWN = "UNKNOWN"


class Severity(StrEnum):
    """Severity level of an issue."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class TimelineEvent:
    """A single event in the strategy timeline."""

    timestamp: datetime
    event_type: TimelineEventType
    description: str
    tx_hash: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    # Multi-chain support
    chain: str | None = None
    # For bridge events - links source and destination events
    linked_event_id: str | None = None
    # For bridge events - destination chain
    destination_chain: str | None = None


@dataclass
class ChainHealth:
    """Health information for a single chain."""

    chain: str
    status: ChainHealthStatus
    rpc_latency_ms: int | None = None
    gas_price_gwei: Decimal | None = None
    block_number: int | None = None
    last_updated: datetime | None = None
    error_message: str | None = None


@dataclass
class ChainPosition:
    """Position summary for a single chain."""

    chain: str
    token_balances: list["TokenBalance"] = field(default_factory=list)
    lp_positions: list["LPPosition"] = field(default_factory=list)
    total_value_usd: Decimal = Decimal("0")
    health_factor: Decimal | None = None
    leverage: Decimal | None = None


@dataclass
class BridgeTransfer:
    """Represents a bridge transfer between chains."""

    transfer_id: str
    token: str
    amount: Decimal
    from_chain: str
    to_chain: str
    initiated_at: datetime
    completed_at: datetime | None = None
    status: str = "IN_FLIGHT"  # IN_FLIGHT, COMPLETED, FAILED
    fee_usd: Decimal = Decimal("0")
    source_tx_hash: str | None = None
    destination_tx_hash: str | None = None
    bridge_protocol: str | None = None  # e.g., "Across", "Stargate"


@dataclass
class TokenBalance:
    """Token balance information."""

    symbol: str
    balance: Decimal
    value_usd: Decimal


@dataclass
class LPPosition:
    """LP position information."""

    pool: str
    token0: str
    token1: str
    liquidity_usd: Decimal
    range_lower: Decimal
    range_upper: Decimal
    current_price: Decimal
    in_range: bool


@dataclass
class PositionSummary:
    """Summary of strategy position."""

    token_balances: list[TokenBalance] = field(default_factory=list)
    lp_positions: list[LPPosition] = field(default_factory=list)
    total_lp_value_usd: Decimal = Decimal("0")
    health_factor: Decimal | None = None
    leverage: Decimal | None = None


@dataclass
class PnLDataPoint:
    """A single PnL data point for charting."""

    timestamp: datetime
    value_usd: Decimal
    pnl_usd: Decimal


@dataclass
class SuggestedAction:
    """A suggested action with description and params."""

    action: AvailableAction
    description: str
    priority: int = 1
    params: dict[str, Any] = field(default_factory=dict)
    is_recommended: bool = False


@dataclass
class AutoRemediation:
    """Auto-remediation configuration."""

    enabled: bool
    action: AvailableAction
    trigger_after_seconds: int
    scheduled_at: datetime | None = None


@dataclass
class OperatorCard:
    """Operator card for displaying issues requiring attention."""

    strategy_id: str
    timestamp: datetime
    reason: StuckReason
    severity: Severity
    context: dict[str, Any]
    position_at_risk_usd: Decimal
    risk_description: str
    suggested_actions: list[SuggestedAction]
    available_actions: list[AvailableAction]
    auto_remediation: AutoRemediation | None = None
    # Multi-chain support - which chain triggered the alert
    alert_chain: str | None = None


@dataclass
class Strategy:
    """Strategy data for dashboard display."""

    id: str
    name: str
    status: StrategyStatus
    pnl_24h_usd: Decimal
    total_value_usd: Decimal
    chain: str  # Primary chain (for backward compat) or comma-separated list
    protocol: str  # Primary protocol or comma-separated for multi-chain
    last_action_at: datetime | None = None
    attention_required: bool = False
    attention_reason: str | None = None
    position: PositionSummary | None = None
    timeline_events: list[TimelineEvent] = field(default_factory=list)
    pnl_history: list[PnLDataPoint] = field(default_factory=list)
    operator_card: OperatorCard | None = None
    # Multi-chain support
    is_multi_chain: bool = False
    chains: list[str] = field(default_factory=list)
    protocols_by_chain: dict[str, list[str]] = field(default_factory=dict)
    positions_by_chain: dict[str, ChainPosition] = field(default_factory=dict)
    chain_health: dict[str, ChainHealth] = field(default_factory=dict)
    bridge_transfers: list[BridgeTransfer] = field(default_factory=list)
    bridge_fees_usd: Decimal = Decimal("0")  # Total bridge fees for P&L
    pnl_by_chain: dict[str, Decimal] = field(default_factory=dict)  # Per-chain P&L
    # Configuration path for config editor
    config_path: str | None = None
    # Value confidence indicator (HIGH, ESTIMATED, STALE, UNAVAILABLE)
    value_confidence: str | None = None


@dataclass
class ConfigHistoryEntry:
    """A single entry in config change history."""

    timestamp: datetime
    changed_by: str
    changes: dict[str, dict[str, str]]  # field -> {old, new}
    version: int


@dataclass
class StrategyConfig:
    """Strategy configuration for the config editor."""

    strategy_id: str
    strategy_name: str
    # Trading parameters
    max_slippage: Decimal
    trade_size_usd: Decimal
    rebalance_threshold: Decimal
    # Risk parameters
    min_health_factor: Decimal
    max_leverage: Decimal
    daily_loss_limit_usd: Decimal
    # Metadata
    last_updated: datetime | None = None
    update_count: int = 0
    config_history: list[ConfigHistoryEntry] = field(default_factory=list)
