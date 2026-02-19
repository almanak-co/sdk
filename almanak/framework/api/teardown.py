"""Teardown API Endpoints for safe strategy closure.

This module provides FastAPI endpoints for the Strategy Teardown System,
enabling operators to safely close all positions with position-aware loss caps,
escalating slippage, and full safety guarantees.

Endpoints:
- GET  /{strategy_id}/close/preview - Preview what closing will do
- POST /{strategy_id}/close - Start closing the strategy
- GET  /{strategy_id}/close/status - Get current status
- POST /{strategy_id}/close/cancel - Cancel an in-progress close
- POST /{strategy_id}/close/approve-escalation - Approve higher slippage
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional, Protocol

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ..teardown import (
    PositionInfo,
    PositionType,
    TeardownManager,
    TeardownMode,
    TeardownPositionSummary,
    TeardownRequest,
    TeardownStatus,
    calculate_max_acceptable_loss,
    get_teardown_state_manager,
)
from .actions import emit_audit_event, verify_api_key
from .timeline import TimelineEvent, TimelineEventType, add_event

if TYPE_CHECKING:
    from ..strategies import IntentStrategy

logger = logging.getLogger(__name__)


# =============================================================================
# Strategy Registry Protocol
# =============================================================================


class StrategyRegistryProtocol(Protocol):
    """Protocol for strategy registries.

    Implementations provide access to running strategy instances,
    enabling the teardown API to query real position data instead
    of mock data.
    """

    def get_strategy(self, strategy_id: str) -> Optional["IntentStrategy"]:
        """Get a strategy by ID.

        Args:
            strategy_id: The strategy identifier

        Returns:
            IntentStrategy instance if found, None otherwise
        """
        ...

    def list_strategies(self) -> list[str]:
        """List all registered strategy IDs.

        Returns:
            List of strategy IDs
        """
        ...


class InMemoryStrategyRegistry:
    """In-memory strategy registry for testing and simple deployments.

    For production, use a distributed registry backed by database/cache.
    """

    def __init__(self) -> None:
        self._strategies: dict[str, IntentStrategy] = {}

    def register(self, strategy: "IntentStrategy") -> None:
        """Register a strategy instance."""
        self._strategies[strategy.strategy_id] = strategy
        logger.info(f"Registered strategy: {strategy.strategy_id}")

    def unregister(self, strategy_id: str) -> None:
        """Unregister a strategy by ID."""
        if strategy_id in self._strategies:
            del self._strategies[strategy_id]
            logger.info(f"Unregistered strategy: {strategy_id}")

    def get_strategy(self, strategy_id: str) -> Optional["IntentStrategy"]:
        """Get a strategy by ID."""
        return self._strategies.get(strategy_id)

    def list_strategies(self) -> list[str]:
        """List all registered strategy IDs."""
        return list(self._strategies.keys())


# Global strategy registry - set via configure_strategy_registry()
_strategy_registry: StrategyRegistryProtocol | None = None


def configure_strategy_registry(registry: StrategyRegistryProtocol) -> None:
    """Configure the global strategy registry.

    Call this during application startup to wire the teardown API
    to real strategy instances.

    Args:
        registry: Registry implementation providing strategy access
    """
    global _strategy_registry
    _strategy_registry = registry
    logger.info("Strategy registry configured for teardown API")


# =============================================================================
# Request/Response Models
# =============================================================================


class CloseRequest(BaseModel):
    """Request to start closing a strategy."""

    mode: str = Field(
        default="graceful",
        pattern="^(graceful|emergency)$",
        description="Teardown mode: 'graceful' (15-30 min) or 'emergency' (1-3 min)",
    )


class ClosePreviewResponse(BaseModel):
    """Preview of what closing will do."""

    strategy_id: str
    strategy_name: str
    mode: str

    # Position info
    current_value_usd: float
    positions: list[dict[str, Any]]

    # Protection (the key info)
    protected_minimum_usd: float
    max_loss_percent: float
    max_loss_usd: float

    # Estimates
    estimated_return_min_usd: float
    estimated_return_max_usd: float
    estimated_duration_minutes: int

    # Steps
    steps: list[str]

    # Warnings
    warnings: list[str]

    # Safety summary
    safety_info: dict[str, Any]


class CloseStartedResponse(BaseModel):
    """Response after starting close."""

    teardown_id: str
    strategy_id: str
    mode: str
    status: str  # "cancel_window", "executing", etc.

    # For cancel window
    cancel_until: str | None  # ISO timestamp
    cancel_seconds_remaining: int | None

    # WebSocket URL for real-time updates
    websocket_url: str | None


class CloseStatusResponse(BaseModel):
    """Current status of close operation."""

    teardown_id: str
    strategy_id: str
    status: str  # "cancel_window", "executing", "paused", "completed", "failed"

    # Progress
    percent_complete: int
    recovered_usd: float

    # Steps
    steps: list[dict[str, Any]]

    # If paused for approval
    approval_needed: dict[str, Any] | None

    # If completed
    result: dict[str, Any] | None


class CancelResponse(BaseModel):
    """Response from cancel request."""

    success: bool
    message: str
    strategy_id: str
    was_in_cancel_window: bool


class EscalationApprovalRequest(BaseModel):
    """Request to approve higher slippage."""

    action: str = Field(
        pattern="^(approve|wait_and_retry|cancel)$",
        description="Action to take: 'approve', 'wait_and_retry', or 'cancel'",
    )
    approved_slippage: float | None = Field(
        default=None,
        description="New approved slippage percentage (e.g., 0.05 for 5%)",
    )


class ApprovalResponseModel(BaseModel):
    """Response from approval request."""

    success: bool
    message: str
    teardown_id: str
    new_status: str


# =============================================================================
# In-Memory State (for demo - would be database in production)
# =============================================================================


class TeardownState:
    """Tracks active teardowns."""

    def __init__(self):
        self.active_teardowns: dict[str, dict[str, Any]] = {}
        self.manager = TeardownManager()

    def get_teardown(self, strategy_id: str) -> dict[str, Any] | None:
        return self.active_teardowns.get(strategy_id)

    def set_teardown(self, strategy_id: str, state: dict[str, Any]) -> None:
        self.active_teardowns[strategy_id] = state

    def remove_teardown(self, strategy_id: str) -> None:
        if strategy_id in self.active_teardowns:
            del self.active_teardowns[strategy_id]


_teardown_state = TeardownState()


def _get_strategy_data(strategy_id: str) -> dict[str, Any]:
    """Get strategy data from registry or raise 404.

    Uses the configured strategy registry to look up real strategy
    instances and their position data. Raises HTTPException if no
    registry is configured or strategy is not found.

    Args:
        strategy_id: The strategy to look up

    Returns:
        Dictionary with strategy metadata and positions

    Raises:
        HTTPException: 404 if strategy not found, 503 if no registry configured
    """
    if _strategy_registry is None:
        logger.error(
            "Teardown API called but no strategy registry configured. "
            "Call configure_strategy_registry() during application startup."
        )
        raise HTTPException(
            status_code=503,
            detail="Strategy registry not configured. Cannot query strategy data.",
        )

    strategy = _strategy_registry.get_strategy(strategy_id)
    if strategy is None:
        available = _strategy_registry.list_strategies()
        raise HTTPException(
            status_code=404,
            detail=f"Strategy {strategy_id} not found. Available: {available}",
        )

    # Check if strategy supports teardown
    if not strategy.supports_teardown():
        raise HTTPException(
            status_code=400,
            detail=f"Strategy {strategy_id} does not support teardown. "
            f"Implement supports_teardown(), get_open_positions(), and generate_teardown_intents().",
        )

    # Get real position data from the strategy
    try:
        position_summary = strategy.get_open_positions()
    except Exception as e:
        logger.exception(f"Failed to get positions from strategy {strategy_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to query positions from strategy: {e}",
        ) from e

    # Convert to the dict format expected by the API
    positions = []
    health_factor = None
    for pos in position_summary.positions:
        pos_dict = {
            "type": pos.position_type.value,
            "position_id": pos.position_id,
            "chain": pos.chain,
            "protocol": pos.protocol,
            "value_usd": float(pos.value_usd),
            "liquidation_risk": pos.liquidation_risk,
            "details": pos.details,
        }
        if pos.health_factor is not None:
            pos_dict["health_factor"] = float(pos.health_factor)
            # Track minimum health factor across all positions
            if health_factor is None or pos.health_factor < health_factor:
                health_factor = pos.health_factor
        positions.append(pos_dict)

    return {
        "strategy_id": strategy_id,
        "name": getattr(strategy, "name", strategy_id),
        "chain": getattr(strategy, "chain", "unknown"),
        "protocol": getattr(strategy, "protocol", "unknown"),
        "positions": positions,
        "total_value_usd": float(position_summary.total_value_usd),
        "health_factor": float(health_factor) if health_factor else None,
    }


def _build_position_summary(strategy: dict[str, Any]) -> TeardownPositionSummary:
    """Build a TeardownPositionSummary from strategy data."""
    positions = []
    for pos in strategy.get("positions", []):
        positions.append(
            PositionInfo(
                position_type=PositionType(pos["type"]),
                position_id=pos["position_id"],
                chain=pos["chain"],
                protocol=pos["protocol"],
                value_usd=Decimal(str(pos["value_usd"])),
                liquidation_risk=pos.get("liquidation_risk", False),
                health_factor=Decimal(str(pos["health_factor"])) if pos.get("health_factor") else None,
                details=pos.get("details", {}),
            )
        )

    return TeardownPositionSummary(
        strategy_id=strategy["strategy_id"],
        timestamp=datetime.now(UTC),
        positions=positions,
    )


def _generate_steps(positions: list[dict], mode: str) -> list[str]:
    """Generate human-readable steps for teardown."""
    steps = []
    position_types = [p["type"] for p in positions]

    if "PERP" in position_types:
        steps.append("Close perpetual position(s)")
    if "BORROW" in position_types:
        steps.append("Repay borrowed amounts")
    if "SUPPLY" in position_types:
        steps.append("Withdraw supplied collateral")
    if "LP" in position_types:
        steps.append("Close LP position(s) and collect fees")
    steps.append("Swap all tokens to USDC")

    return steps


def _generate_warnings(strategy: dict, mode: str) -> list[str]:
    """Generate warnings based on strategy state."""
    warnings = []
    total_value = strategy.get("total_value_usd", 0)
    health_factor = strategy.get("health_factor")

    if health_factor and health_factor < 1.5:
        warnings.append(f"Low health factor ({health_factor}). Position may be at liquidation risk.")

    if mode == "emergency" and not health_factor:
        warnings.append(
            "Emergency mode selected but no immediate liquidation risk detected. "
            "Consider graceful mode for lower costs."
        )

    if total_value > 100000:
        warnings.append("Large position value. Extra care will be taken to minimize slippage.")

    return warnings


# =============================================================================
# API Router
# =============================================================================

router = APIRouter(prefix="/api/strategies", tags=["teardown"])


@router.get("/{strategy_id}/close/preview")
async def preview_close(
    strategy_id: str,
    mode: str = "graceful",
    api_key: str = Depends(verify_api_key),
) -> ClosePreviewResponse:
    """Preview what closing will do, without executing.

    Shows the operator exactly what will happen, what protections are in place,
    and what they can expect to receive.

    Args:
        strategy_id: The strategy to preview closing
        mode: "graceful" or "emergency"
        api_key: Authenticated API key

    Returns:
        ClosePreviewResponse with all details for user confirmation
    """
    if mode not in ("graceful", "emergency"):
        raise HTTPException(status_code=400, detail="Mode must be 'graceful' or 'emergency'")

    strategy = _get_strategy_data(strategy_id)
    summary = _build_position_summary(strategy)

    # Calculate protection values
    total_value = float(summary.total_value_usd)
    max_loss_pct = float(calculate_max_acceptable_loss(summary.total_value_usd))
    max_loss_usd = total_value * max_loss_pct
    protected_min = total_value - max_loss_usd

    # Estimate returns based on mode
    if mode == "graceful":
        min_cost_pct = 0.003  # 0.3%
        max_cost_pct = max_loss_pct * 0.5
        duration = max(15, len(strategy.get("positions", [])) * 5)
    else:
        min_cost_pct = 0.01  # 1%
        max_cost_pct = max_loss_pct
        duration = max(1, len(strategy.get("positions", [])))

    est_min = total_value * (1 - max_cost_pct)
    est_max = total_value * (1 - min_cost_pct)

    steps = _generate_steps(strategy.get("positions", []), mode)
    warnings = _generate_warnings(strategy, mode)

    return ClosePreviewResponse(
        strategy_id=strategy_id,
        strategy_name=strategy["name"],
        mode=mode,
        current_value_usd=total_value,
        positions=strategy.get("positions", []),
        protected_minimum_usd=protected_min,
        max_loss_percent=max_loss_pct * 100,
        max_loss_usd=max_loss_usd,
        estimated_return_min_usd=est_min,
        estimated_return_max_usd=est_max,
        estimated_duration_minutes=duration,
        steps=steps,
        warnings=warnings,
        safety_info={
            "position_aware_cap": True,
            "mev_protection": True,
            "cancel_window_seconds": 10,
            "simulation_required": True,
            "atomic_bundling": True,
            "post_verification": True,
        },
    )


@router.post("/{strategy_id}/close")
async def start_close(
    strategy_id: str,
    request: CloseRequest,
    api_key: str = Depends(verify_api_key),
) -> CloseStartedResponse:
    """Start closing the strategy.

    Initiates the teardown process with a 10-second cancel window.
    Returns immediately with status and cancel deadline.

    Args:
        strategy_id: The strategy to close
        request: Close request with mode selection
        api_key: Authenticated API key

    Returns:
        CloseStartedResponse with teardown ID and cancel window info
    """
    strategy = _get_strategy_data(strategy_id)

    # Check if already tearing down
    existing = _teardown_state.get_teardown(strategy_id)
    if existing and existing["status"] not in ("completed", "failed", "cancelled"):
        raise HTTPException(
            status_code=400,
            detail=f"Strategy {strategy_id} already has an active teardown (status: {existing['status']})",
        )

    # Generate teardown ID
    import uuid

    teardown_id = f"td_{uuid.uuid4().hex[:12]}"

    # Calculate cancel window end
    cancel_until = datetime.now(UTC)
    from datetime import timedelta

    cancel_until = cancel_until + timedelta(seconds=10)

    # Store teardown state
    teardown_state = {
        "teardown_id": teardown_id,
        "strategy_id": strategy_id,
        "mode": request.mode,
        "status": "cancel_window",
        "started_at": datetime.now(UTC).isoformat(),
        "cancel_until": cancel_until.isoformat(),
        "percent_complete": 0,
        "recovered_usd": 0,
        "steps": [],
    }
    _teardown_state.set_teardown(strategy_id, teardown_state)

    # Persist teardown request in shared teardown state so StrategyRunner can pick it up.
    try:
        teardown_manager = get_teardown_state_manager()
        internal_mode = TeardownMode.SOFT if request.mode == "graceful" else TeardownMode.HARD
        persisted_request = TeardownRequest(
            strategy_id=strategy_id,
            mode=internal_mode,
            reason=f"Dashboard requested {request.mode} teardown",
            requested_by="dashboard_api",
            status=TeardownStatus.CANCEL_WINDOW,
            cancel_deadline=cancel_until,
        )
        teardown_manager.create_request(persisted_request)
    except Exception as e:
        logger.exception(f"Failed to persist teardown request for {strategy_id}")
        _teardown_state.remove_teardown(strategy_id)
        raise HTTPException(
            status_code=503,
            detail=f"Failed to persist teardown request for strategy {strategy_id}",
        ) from e

    # Emit audit event
    emit_audit_event(
        strategy_id=strategy_id,
        action="TEARDOWN_STARTED",
        details={
            "teardown_id": teardown_id,
            "mode": request.mode,
            "total_value_usd": strategy.get("total_value_usd", 0),
        },
        api_key=api_key,
    )

    # Emit timeline event
    event = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=TimelineEventType.OPERATOR_ACTION_EXECUTED,
        description=f"Teardown initiated: {request.mode} mode",
        strategy_id=strategy_id,
        chain=strategy.get("chain", "unknown"),
        details={
            "teardown_id": teardown_id,
            "mode": request.mode,
            "cancel_window_seconds": 10,
        },
    )
    add_event(event)

    return CloseStartedResponse(
        teardown_id=teardown_id,
        strategy_id=strategy_id,
        mode=request.mode,
        status="cancel_window",
        cancel_until=cancel_until.isoformat(),
        cancel_seconds_remaining=10,
        websocket_url=f"/ws/teardown/{teardown_id}",
    )


@router.get("/{strategy_id}/close/status")
async def close_status(
    strategy_id: str,
    api_key: str = Depends(verify_api_key),
) -> CloseStatusResponse:
    """Get current status of close operation.

    Returns progress, current step, and any approval requests.

    Args:
        strategy_id: The strategy being closed
        api_key: Authenticated API key

    Returns:
        CloseStatusResponse with current teardown status
    """
    teardown = _teardown_state.get_teardown(strategy_id)
    if not teardown:
        raise HTTPException(
            status_code=404,
            detail=f"No active teardown for strategy {strategy_id}",
        )

    # Check if cancel window has expired
    if teardown["status"] == "cancel_window":
        cancel_until = datetime.fromisoformat(teardown["cancel_until"].replace("Z", "+00:00"))
        if datetime.now(UTC) >= cancel_until:
            teardown["status"] = "executing"
            _teardown_state.set_teardown(strategy_id, teardown)
            try:
                teardown_manager = get_teardown_state_manager()
                teardown_manager.mark_started(strategy_id)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Failed to mark persisted teardown as started for {strategy_id}: {e}")

    return CloseStatusResponse(
        teardown_id=teardown["teardown_id"],
        strategy_id=strategy_id,
        status=teardown["status"],
        percent_complete=teardown.get("percent_complete", 0),
        recovered_usd=teardown.get("recovered_usd", 0),
        steps=teardown.get("steps", []),
        approval_needed=teardown.get("approval_needed"),
        result=teardown.get("result"),
    )


@router.post("/{strategy_id}/close/cancel")
async def cancel_close(
    strategy_id: str,
    api_key: str = Depends(verify_api_key),
) -> CancelResponse:
    """Cancel an in-progress close.

    - Graceful mode: Cancellable anytime before completion
    - Emergency mode: Only within 10-second cancel window

    Args:
        strategy_id: The strategy to cancel teardown for
        api_key: Authenticated API key

    Returns:
        CancelResponse with success status
    """
    teardown = _teardown_state.get_teardown(strategy_id)
    if not teardown:
        raise HTTPException(
            status_code=404,
            detail=f"No active teardown for strategy {strategy_id}",
        )

    status = teardown["status"]

    # Check if cancellable
    if status in ("completed", "failed", "cancelled"):
        raise HTTPException(
            status_code=400,
            detail=f"Teardown already {status} - cannot cancel",
        )

    # For emergency mode, check cancel window
    was_in_window = False
    if teardown["mode"] == "emergency":
        if status == "cancel_window":
            was_in_window = True
        else:
            raise HTTPException(
                status_code=400,
                detail="Cancel window has expired for emergency teardown. Cannot cancel.",
            )

    # Cancel the teardown
    teardown["status"] = "cancelled"
    _teardown_state.set_teardown(strategy_id, teardown)
    try:
        teardown_manager = get_teardown_state_manager()
        teardown_manager.mark_cancelled(strategy_id)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to mark persisted teardown as cancelled for {strategy_id}: {e}")

    # Emit audit event
    emit_audit_event(
        strategy_id=strategy_id,
        action="TEARDOWN_CANCELLED",
        details={
            "teardown_id": teardown["teardown_id"],
            "was_in_cancel_window": was_in_window,
        },
        api_key=api_key,
    )

    return CancelResponse(
        success=True,
        message=f"Teardown cancelled for strategy {strategy_id}",
        strategy_id=strategy_id,
        was_in_cancel_window=was_in_window,
    )


@router.post("/{strategy_id}/close/approve-escalation")
async def approve_escalation(
    strategy_id: str,
    request: EscalationApprovalRequest,
    api_key: str = Depends(verify_api_key),
) -> ApprovalResponseModel:
    """Approve higher slippage when system pauses for protection.

    Called when market conditions require slippage above auto-approved levels.
    Operator can approve, wait and retry, or cancel.

    Args:
        strategy_id: The strategy being torn down
        request: Approval request with action
        api_key: Authenticated API key

    Returns:
        ApprovalResponseModel with result
    """
    teardown = _teardown_state.get_teardown(strategy_id)
    if not teardown:
        raise HTTPException(
            status_code=404,
            detail=f"No active teardown for strategy {strategy_id}",
        )

    if teardown["status"] != "paused":
        raise HTTPException(
            status_code=400,
            detail=f"Teardown is not paused (status: {teardown['status']})",
        )

    if not teardown.get("approval_needed"):
        raise HTTPException(
            status_code=400,
            detail="No approval request pending",
        )

    # Handle the action
    if request.action == "approve":
        teardown["status"] = "executing"
        teardown["approval_needed"] = None
        if request.approved_slippage:
            teardown["approved_slippage"] = request.approved_slippage
        message = "Slippage approved. Continuing teardown."

    elif request.action == "wait_and_retry":
        teardown["status"] = "waiting_retry"
        message = "Waiting for better market conditions. Will retry in 5 minutes."

    else:  # cancel
        teardown["status"] = "cancelled"
        teardown["approval_needed"] = None
        message = "Teardown cancelled by operator."

    _teardown_state.set_teardown(strategy_id, teardown)

    # Emit audit event
    emit_audit_event(
        strategy_id=strategy_id,
        action="TEARDOWN_ESCALATION_RESPONSE",
        details={
            "teardown_id": teardown["teardown_id"],
            "action": request.action,
            "approved_slippage": request.approved_slippage,
        },
        api_key=api_key,
    )

    return ApprovalResponseModel(
        success=True,
        message=message,
        teardown_id=teardown["teardown_id"],
        new_status=teardown["status"],
    )
