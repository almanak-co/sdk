"""Data models for the Strategy Teardown System.

Defines the core types used throughout the teardown process:
- TeardownMode: Internal execution modes (SOFT/HARD)
- PositionType: Types of positions in close priority order
- PositionInfo: Individual position details
- TeardownPositionSummary: Complete position summary for teardown
- TeardownPreview: What user sees before confirming
- TeardownResult: Outcome of a teardown operation
- TeardownState: Persisted state for resumability
- EscalationLevel: Slippage escalation levels
- ApprovalRequest: Request for human approval at escalation points
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class TeardownMode(StrEnum):
    """Internal execution mode (not exposed directly to users).

    User-facing names:
    - SOFT: "Graceful Shutdown"
    - HARD: "Safe Emergency Exit"
    """

    SOFT = "SOFT"  # Graceful: 15-30 minutes, minimize costs
    HARD = "HARD"  # Emergency: 1-3 minutes, prioritize speed


class TeardownPhase(StrEnum):
    """Phases of the teardown pipeline.

    Three-phase pipeline:
    - Phase 1: Position Closure (required)
    - Phase 2: Token Consolidation (optional, ON by default)
    - Phase 3: Chain Consolidation (optional, OFF by default)
    """

    CANCEL_WINDOW = "cancel_window"
    POSITION_CLOSURE = "position_closure"  # Phase 1: Required
    TOKEN_CONSOLIDATION = "token_consolidation"  # Phase 2: Optional
    CHAIN_CONSOLIDATION = "chain_consolidation"  # Phase 3: Optional


class TeardownAssetPolicy(StrEnum):
    """How to handle final asset allocation after closing positions.

    Determines what happens in Phase 2 (Token Consolidation):
    - TARGET_TOKEN: Swap all tokens to a target (default: USDC)
    - ENTRY_TOKEN: Swap back to original entry assets
    - KEEP_OUTPUTS: No terminal swaps, keep natural exit tokens

    Emergency mode automatically overrides to KEEP_OUTPUTS for safety.
    """

    TARGET_TOKEN = "target_token"  # Swap all to target (default: USDC)
    ENTRY_TOKEN = "entry_token"  # Return to original entry asset
    KEEP_OUTPUTS = "keep_outputs"  # No terminal swaps, keep native tokens


class PositionType(StrEnum):
    """Position types in close order priority.

    Critical for safety - always close in this order:
    1. PERP - Close perpetuals first (highest liquidation risk)
    2. BORROW - Repay borrows (frees collateral)
    3. SUPPLY - Withdraw collateral
    4. LP - Close LP positions
    5. STAKE - Unstake staked tokens
    6. PREDICTION - Close prediction market positions
    7. CEX - Withdraw from CEX
    8. TOKEN - Swap to target token last

    Note: STAKE, PREDICTION, CEX added for portfolio tracking.
    They have lower priority than core DeFi positions.
    """

    PERP = "PERP"
    BORROW = "BORROW"
    SUPPLY = "SUPPLY"
    LP = "LP"
    STAKE = "STAKE"  # Staked tokens (Lido, Ethena)
    PREDICTION = "PREDICTION"  # Prediction market positions (Polymarket)
    CEX = "CEX"  # CEX holdings (Kraken)
    TOKEN = "TOKEN"

    @property
    def priority(self) -> int:
        """Return close order priority (lower = close first)."""
        priorities = {
            PositionType.PERP: 1,
            PositionType.BORROW: 2,
            PositionType.SUPPLY: 3,
            PositionType.LP: 4,
            PositionType.STAKE: 5,
            PositionType.PREDICTION: 6,
            PositionType.CEX: 7,
            PositionType.TOKEN: 8,
        }
        return priorities[self]


@dataclass
class PositionInfo:
    """A single position to be closed during teardown."""

    position_type: PositionType
    position_id: str
    chain: str
    protocol: str
    value_usd: Decimal

    # Risk info
    liquidation_risk: bool = False
    health_factor: Decimal | None = None

    # Protocol-specific details
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate and normalize fields."""
        if isinstance(self.value_usd, int | float | str):
            self.value_usd = Decimal(str(self.value_usd))
        if self.health_factor is not None and isinstance(self.health_factor, int | float | str):
            self.health_factor = Decimal(str(self.health_factor))


def calculate_max_acceptable_loss(position_value_usd: Decimal) -> Decimal:
    """Position-size-aware loss cap.

    Larger positions get tighter relative caps because the absolute
    dollar amounts are more significant.

    Args:
        position_value_usd: Total value of the position in USD

    Returns:
        Maximum acceptable loss as a decimal percentage (e.g., 0.03 for 3%)
    """
    if position_value_usd < Decimal("50_000"):
        return Decimal("0.03")  # 3% for small positions (<$50K)
    elif position_value_usd < Decimal("200_000"):
        return Decimal("0.025")  # 2.5% for medium positions ($50K-$200K)
    elif position_value_usd < Decimal("500_000"):
        return Decimal("0.02")  # 2% for medium-large positions ($200K-$500K)
    elif position_value_usd < Decimal("2_000_000"):
        return Decimal("0.015")  # 1.5% for large positions ($500K-$2M)
    else:
        return Decimal("0.01")  # 1% for whale positions (>$2M)


@dataclass
class TeardownPositionSummary:
    """Complete summary of positions for teardown."""

    strategy_id: str
    timestamp: datetime
    positions: list[PositionInfo] = field(default_factory=list)

    # Computed fields (can be set explicitly or calculated)
    total_value_usd: Decimal = Decimal("0")
    has_liquidation_risk: bool = False

    def __post_init__(self) -> None:
        """Calculate derived fields if not set."""
        if isinstance(self.total_value_usd, int | float | str):
            self.total_value_usd = Decimal(str(self.total_value_usd))

        # Auto-calculate if positions exist but total not set
        if self.positions and self.total_value_usd == Decimal("0"):
            self.total_value_usd = sum((p.value_usd for p in self.positions), Decimal("0"))

        if self.positions and not self.has_liquidation_risk:
            self.has_liquidation_risk = any(p.liquidation_risk for p in self.positions)

    @property
    def max_loss_percent(self) -> Decimal:
        """Get position-aware max loss percentage."""
        return calculate_max_acceptable_loss(self.total_value_usd)

    @property
    def max_loss_usd(self) -> Decimal:
        """Get maximum acceptable loss in USD."""
        return self.total_value_usd * self.max_loss_percent

    @property
    def protected_minimum_usd(self) -> Decimal:
        """Get the protected minimum value (what user is guaranteed to keep)."""
        return self.total_value_usd - self.max_loss_usd

    @property
    def chains_involved(self) -> set[str]:
        """Get all chains with positions."""
        return {p.chain for p in self.positions}

    def positions_by_chain(self, chain: str) -> list[PositionInfo]:
        """Get positions for a specific chain."""
        return [p for p in self.positions if p.chain == chain]

    def positions_by_type(self, ptype: PositionType) -> list[PositionInfo]:
        """Get positions of a specific type."""
        return [p for p in self.positions if p.position_type == ptype]

    def positions_sorted_by_priority(self) -> list[PositionInfo]:
        """Get positions sorted by close order priority."""
        return sorted(self.positions, key=lambda p: p.position_type.priority)


@dataclass
class TeardownPreview:
    """What the user sees before confirming teardown.

    This is the key UX element - it shows protections clearly.
    """

    strategy_id: str
    strategy_name: str
    mode: str  # "graceful" or "emergency" (user-facing)

    # Position info
    positions: list[dict[str, Any]]  # Simplified for API response
    current_value_usd: Decimal

    # Protection info (the key UX element)
    protected_minimum_usd: Decimal
    max_loss_percent: Decimal
    max_loss_usd: Decimal

    # Estimates
    estimated_return_min_usd: Decimal
    estimated_return_max_usd: Decimal
    estimated_duration_minutes: int

    # Steps (human readable)
    steps: list[str]  # ["Close perp position", "Swap to USDC", ...]

    # Warnings
    warnings: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Convert numeric fields to Decimal."""
        for attr in [
            "current_value_usd",
            "protected_minimum_usd",
            "max_loss_percent",
            "max_loss_usd",
            "estimated_return_min_usd",
            "estimated_return_max_usd",
        ]:
            value = getattr(self, attr)
            if isinstance(value, int | float | str):
                setattr(self, attr, Decimal(str(value)))


@dataclass
class TeardownResult:
    """Result of a completed teardown operation."""

    success: bool
    strategy_id: str
    mode: str

    # Timing
    started_at: datetime
    completed_at: datetime | None
    duration_seconds: float

    # Execution stats
    intents_total: int
    intents_succeeded: int
    intents_failed: int

    # Financial summary
    starting_value_usd: Decimal
    final_value_usd: Decimal
    total_costs_usd: Decimal  # gas + slippage

    # Final state
    final_balances: dict[str, Decimal]  # {token: amount}

    # If failed
    error: str | None = None
    recovery_options: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Convert numeric fields to Decimal."""
        for attr in ["starting_value_usd", "final_value_usd", "total_costs_usd"]:
            value = getattr(self, attr)
            if isinstance(value, int | float | str):
                setattr(self, attr, Decimal(str(value)))

    @property
    def cost_percent(self) -> Decimal:
        """Get total costs as percentage of starting value."""
        if self.starting_value_usd == 0:
            return Decimal("0")
        return (self.total_costs_usd / self.starting_value_usd) * 100

    @property
    def all_succeeded(self) -> bool:
        """Check if all intents succeeded."""
        return self.intents_succeeded == self.intents_total


class TeardownStatus(StrEnum):
    """Status of a teardown operation."""

    PENDING = "pending"  # Created but not started
    CANCEL_WINDOW = "cancel_window"  # In 10-second cancel window
    EXECUTING = "executing"  # Actively executing intents
    PAUSED = "paused"  # Paused for approval or error
    COMPLETED = "completed"  # Successfully completed
    FAILED = "failed"  # Failed with error
    CANCELLED = "cancelled"  # Cancelled by user


@dataclass
class TeardownState:
    """Persisted state for resumable teardowns.

    This state survives system restarts, allowing interrupted
    teardowns to resume from the last checkpoint.
    """

    teardown_id: str
    strategy_id: str
    mode: TeardownMode
    status: TeardownStatus

    # Progress
    total_intents: int
    completed_intents: int
    current_intent_index: int

    # Timing
    started_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None

    # For resumption
    pending_intents_json: str = ""  # Serialized intents
    intent_results: list[dict[str, Any]] = field(default_factory=list)

    # Cancel window
    cancel_window_until: datetime | None = None

    # Configuration snapshot (for resumption with same settings)
    config_json: str = ""

    @property
    def progress_percent(self) -> int:
        """Get completion percentage."""
        if self.total_intents == 0:
            return 0
        return int((self.completed_intents / self.total_intents) * 100)

    @property
    def is_in_cancel_window(self) -> bool:
        """Check if still in cancel window."""
        if self.cancel_window_until is None:
            return False
        return datetime.now(UTC) < self.cancel_window_until

    @property
    def is_resumable(self) -> bool:
        """Check if this teardown can be resumed."""
        return self.status in (TeardownStatus.EXECUTING, TeardownStatus.PAUSED)


class EscalationLevel(StrEnum):
    """Slippage escalation levels.

    Each level represents an increasing slippage tolerance.
    Levels 1-2 auto-approve, levels 3+ require human approval.
    """

    LEVEL_1 = "level_1"  # 2% slippage, auto-approve, 3 retries
    LEVEL_2 = "level_2"  # 3% slippage, auto-approve, 2 retries
    LEVEL_3 = "level_3"  # 5% slippage, needs approval, 1 retry
    LEVEL_4 = "level_4"  # 8% slippage, needs explicit approval, 1 retry
    LEVEL_5 = "level_5"  # >8% slippage, manual intervention required


@dataclass
class EscalationConfig:
    """Configuration for a single escalation level."""

    level: EscalationLevel
    slippage: Decimal
    auto_approve: bool
    retries: int

    @classmethod
    def default_levels(cls) -> list["EscalationConfig"]:
        """Get default escalation level configurations."""
        return [
            cls(
                level=EscalationLevel.LEVEL_1,
                slippage=Decimal("0.02"),
                auto_approve=True,
                retries=3,
            ),
            cls(
                level=EscalationLevel.LEVEL_2,
                slippage=Decimal("0.03"),
                auto_approve=True,
                retries=2,
            ),
            cls(
                level=EscalationLevel.LEVEL_3,
                slippage=Decimal("0.05"),
                auto_approve=False,
                retries=1,
            ),
            cls(
                level=EscalationLevel.LEVEL_4,
                slippage=Decimal("0.08"),
                auto_approve=False,
                retries=1,
            ),
        ]


@dataclass
class ApprovalRequest:
    """Request for human approval at an escalation point.

    Sent when slippage would exceed auto-approved levels.
    """

    teardown_id: str
    strategy_id: str
    current_level: EscalationLevel
    current_slippage: Decimal
    estimated_loss_usd: Decimal
    position_value_usd: Decimal

    # Context for user decision
    reason: str  # Why approval is needed
    options: list[str]  # Available actions

    # Timestamps
    requested_at: datetime
    expires_at: datetime | None = None  # Auto-cancel if not responded

    def __post_init__(self) -> None:
        """Convert numeric fields to Decimal."""
        for attr in ["current_slippage", "estimated_loss_usd", "position_value_usd"]:
            value = getattr(self, attr)
            if isinstance(value, int | float | str):
                setattr(self, attr, Decimal(str(value)))

    @property
    def loss_percent(self) -> Decimal:
        """Get estimated loss as percentage."""
        if self.position_value_usd == 0:
            return Decimal("0")
        return (self.estimated_loss_usd / self.position_value_usd) * 100


@dataclass
class ApprovalResponse:
    """Response to an approval request."""

    approved: bool
    teardown_id: str
    approved_slippage: Decimal | None = None  # New max slippage if approved
    action: str = "continue"  # "continue", "wait_and_retry", "cancel"

    def __post_init__(self) -> None:
        """Convert numeric fields."""
        if self.approved_slippage is not None and isinstance(self.approved_slippage, int | float | str):
            self.approved_slippage = Decimal(str(self.approved_slippage))


@dataclass
class TeardownRequest:
    """State-based teardown request stored in SQLite/PostgreSQL.

    This is the signal mechanism for triggering teardowns from multiple sources:
    - CLI command
    - Config hot-reload
    - Dashboard UI
    - Risk guards (auto-protect)

    The strategy checks for this request each iteration via _check_teardown_request()
    and initiates teardown if found.
    """

    strategy_id: str
    mode: TeardownMode
    asset_policy: TeardownAssetPolicy = TeardownAssetPolicy.TARGET_TOKEN
    target_token: str = "USDC"

    # Request metadata
    reason: str | None = None
    requested_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    requested_by: str = "dashboard"  # "cli", "config", "dashboard", "risk_guard"

    # Processing state
    status: TeardownStatus = TeardownStatus.PENDING
    acknowledged_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    # Phase tracking
    current_phase: TeardownPhase | None = None

    # Progress tracking
    positions_total: int = 0
    positions_closed: int = 0
    positions_failed: int = 0

    # Cancel window
    cancel_requested: bool = False
    cancel_deadline: datetime | None = None

    @property
    def is_active(self) -> bool:
        """Check if this request is still active (not completed/cancelled)."""
        return self.status not in (
            TeardownStatus.COMPLETED,
            TeardownStatus.CANCELLED,
            TeardownStatus.FAILED,
        )

    @property
    def can_cancel(self) -> bool:
        """Check if this request can be cancelled."""
        if not self.is_active:
            return False
        if self.cancel_deadline is None:
            return True
        return datetime.now(UTC) < self.cancel_deadline

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for storage."""
        return {
            "strategy_id": self.strategy_id,
            "mode": self.mode.value,
            "asset_policy": self.asset_policy.value,
            "target_token": self.target_token,
            "reason": self.reason,
            "requested_at": self.requested_at.isoformat(),
            "requested_by": self.requested_by,
            "status": self.status.value,
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "current_phase": self.current_phase.value if self.current_phase else None,
            "positions_total": self.positions_total,
            "positions_closed": self.positions_closed,
            "positions_failed": self.positions_failed,
            "cancel_requested": self.cancel_requested,
            "cancel_deadline": self.cancel_deadline.isoformat() if self.cancel_deadline else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TeardownRequest":
        """Deserialize from dictionary."""
        return cls(
            strategy_id=data["strategy_id"],
            mode=TeardownMode(data["mode"]),
            asset_policy=TeardownAssetPolicy(data.get("asset_policy", "target_token")),
            target_token=data.get("target_token", "USDC"),
            reason=data.get("reason"),
            requested_at=datetime.fromisoformat(data["requested_at"]),
            requested_by=data.get("requested_by", "dashboard"),
            status=TeardownStatus(data["status"]),
            acknowledged_at=datetime.fromisoformat(data["acknowledged_at"]) if data.get("acknowledged_at") else None,
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            current_phase=TeardownPhase(data["current_phase"]) if data.get("current_phase") else None,
            positions_total=data.get("positions_total", 0),
            positions_closed=data.get("positions_closed", 0),
            positions_failed=data.get("positions_failed", 0),
            cancel_requested=data.get("cancel_requested", False),
            cancel_deadline=datetime.fromisoformat(data["cancel_deadline"]) if data.get("cancel_deadline") else None,
        )


@dataclass
class TeardownProfile:
    """Strategy-specific teardown metadata for UX.

    Strategies can provide this profile to help the UI display
    better information about what the teardown will look like.
    """

    natural_exit_assets: list[str] = field(default_factory=list)  # e.g., ["WETH", "USDC"]
    original_entry_assets: list[str] = field(default_factory=list)  # e.g., ["USDC"]
    recommended_target: str = "USDC"
    conversion_complexity: str = "low"  # "low", "medium", "high"
    estimated_steps: int = 3
    chains_involved: list[str] = field(default_factory=list)
    has_perp_positions: bool = False
    has_lending_positions: bool = False
    has_lp_positions: bool = False
