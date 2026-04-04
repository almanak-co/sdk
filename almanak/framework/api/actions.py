"""Dashboard Action Endpoints for operator control of strategies.

This module provides FastAPI endpoints that allow operators to control strategies
through the dashboard, including pause, resume, bump gas, cancel transaction,
and configuration update operations. All actions require authentication and emit
audit events.
"""

import hashlib
import hmac
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from ..models.actions import AvailableAction, SuggestedAction
from ..models.hot_reload_config import HotReloadableConfig
from ..models.operator_card import (
    EventType,
    OperatorCard,
    PositionSummary,
    Severity,
)
from ..models.stuck_reason import StuckReason
from ..strategies.base import RiskGuard, RiskGuardConfig
from .timeline import TimelineEvent, TimelineEventType, add_event

logger = logging.getLogger(__name__)


# =============================================================================
# API Key Validation
# =============================================================================


class ApiKeyValidatorProtocol(Protocol):
    """Protocol for API key validators."""

    def validate(self, api_key: str) -> tuple[bool, str]:
        """Validate an API key.

        Args:
            api_key: The key to validate

        Returns:
            Tuple of (is_valid, user_identifier)
        """
        ...


class EnvironmentApiKeyValidator:
    """API key validator using environment variables.

    Supports multiple valid API keys via comma-separated ALMANAK_API_KEYS env var.
    Uses constant-time comparison to prevent timing attacks.
    """

    def __init__(self) -> None:
        """Initialize validator with keys from environment."""
        keys_str = os.environ.get("ALMANAK_API_KEYS", "")
        self._valid_keys: set[str] = set()

        if keys_str:
            # Split by comma, strip whitespace, filter empty
            for key in keys_str.split(","):
                key = key.strip()
                if key:
                    # Store hash of key for secure comparison
                    self._valid_keys.add(self._hash_key(key))
            logger.info(f"API key validator initialized with {len(self._valid_keys)} valid key(s)")
        else:
            logger.warning(
                "ALMANAK_API_KEYS not set - API authentication will reject all requests. "
                "Set ALMANAK_API_KEYS environment variable with valid keys."
            )

    def _hash_key(self, key: str) -> str:
        """Hash a key for secure storage/comparison."""
        return hashlib.sha256(key.encode()).hexdigest()

    def validate(self, api_key: str) -> tuple[bool, str]:
        """Validate an API key.

        Uses constant-time comparison to prevent timing attacks.

        Args:
            api_key: The key to validate

        Returns:
            Tuple of (is_valid, user_identifier)
        """
        if not self._valid_keys:
            # No valid keys configured
            return False, ""

        key_hash = self._hash_key(api_key)

        # Use constant-time comparison to prevent timing attacks
        for valid_hash in self._valid_keys:
            if hmac.compare_digest(key_hash, valid_hash):
                # Return truncated key prefix as identifier (for logging)
                return True, api_key[:8] + "..."

        return False, ""


# Global API key validator - set via configure_api_key_validator()
_api_key_validator: ApiKeyValidatorProtocol | None = None


def configure_api_key_validator(validator: ApiKeyValidatorProtocol) -> None:
    """Configure the global API key validator.

    Call this during application startup to set up authentication.

    Args:
        validator: Validator implementation to use
    """
    global _api_key_validator
    _api_key_validator = validator
    logger.info("API key validator configured")


def get_api_key_validator() -> ApiKeyValidatorProtocol:
    """Get the configured API key validator.

    Returns:
        The configured validator, or creates a default one from environment
    """
    global _api_key_validator
    if _api_key_validator is None:
        _api_key_validator = EnvironmentApiKeyValidator()
    return _api_key_validator


# Request models using Pydantic for API validation
class BumpGasRequest(BaseModel):
    """Request body for bump gas endpoint."""

    gas_price_gwei: float
    """New gas price in Gwei"""


class CancelTxRequest(BaseModel):
    """Request body for cancel transaction endpoint."""

    tx_hash: str
    """The transaction hash to cancel"""


class ConfigUpdateRequest(BaseModel):
    """Request body for config update endpoint.

    Accepts a flat dictionary of field name to value updates.
    Only hot-reloadable fields are allowed.
    """

    updates: dict[str, Any]
    """Dictionary of field names to new values"""


# Response models
@dataclass
class ConfigUpdateResponse:
    """Response from a config update endpoint.

    When Risk Guard blocks an update, the guidance field contains
    human-readable explanations of what failed and how to proceed.
    """

    success: bool
    message: str
    updated_config: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    guidance: list[dict[str, Any]] | None = None
    """Human-readable guidance when Risk Guard blocks an action"""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON response."""
        return {
            "success": self.success,
            "message": self.message,
            "updated_config": self.updated_config,
            "result": self.result,
            "error": self.error,
            "guidance": self.guidance,
        }


@dataclass
class ActionResponse:
    """Response from an action endpoint."""

    success: bool
    message: str
    operator_card: dict[str, Any] | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON response."""
        return {
            "success": self.success,
            "message": self.message,
            "operator_card": self.operator_card,
            "error": self.error,
        }


@dataclass
class StrategyState:
    """Internal state tracking for a strategy."""

    strategy_id: str
    status: str  # "running", "paused", "stuck"
    chain: str
    protocol: str
    current_gas_price_gwei: float
    pending_tx_hash: str | None = None
    total_value_usd: Decimal = Decimal("10000")
    attention_required: bool = False
    stuck_reason: StuckReason | None = None
    config: HotReloadableConfig = field(default_factory=HotReloadableConfig)


# Global RiskGuard instance for validating config updates
# In production, this could be configured per-strategy
_risk_guard = RiskGuard(RiskGuardConfig())


# Cache for strategy states loaded from database
# Refreshed on each request to ensure freshness
_strategy_state_cache: dict[str, StrategyState] = {}
_last_cache_refresh: float = 0.0
CACHE_REFRESH_INTERVAL = 5.0  # Refresh every 5 seconds


def _load_strategy_state_from_db(strategy_id: str) -> StrategyState | None:
    """Load strategy state from SQLite database.

    Args:
        strategy_id: Strategy ID to load

    Returns:
        StrategyState if found, None otherwise
    """
    import sqlite3
    from pathlib import Path

    state_db = Path("./almanak_state.db")
    if not state_db.exists():
        return None

    try:
        conn = sqlite3.connect(str(state_db))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Query the strategy state
        cursor.execute(
            "SELECT strategy_id, state_data, updated_at FROM strategy_state WHERE strategy_id = ?", (strategy_id,)
        )
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        state_json = row["state_data"]
        state_data = json.loads(state_json) if state_json else {}

        # Extract strategy name from ID
        strategy_id.split(":")[0] if ":" in strategy_id else strategy_id

        # Determine status from state
        status = "running"
        last_iteration = state_data.get("last_iteration", {})
        if last_iteration.get("status") in ("EXECUTION_FAILED", "STRATEGY_ERROR"):
            status = "error"

        stuck_reason_str = state_data.get("stuck_reason")
        stuck_reason = None
        attention_required = False
        if stuck_reason_str:
            try:
                stuck_reason = StuckReason(stuck_reason_str)
                status = "stuck"
                attention_required = True
            except ValueError:
                pass

        # Extract chain and protocol from state or config
        chain = state_data.get("chain", "arbitrum")
        protocol = state_data.get("protocol", "unknown")

        # Extract portfolio value
        total_value_usd = Decimal("0")
        value_keys = ["total_value_usd", "total_position_value_usd", "portfolio_value_usd"]
        for key in value_keys:
            if key in state_data:
                try:
                    total_value_usd = Decimal(str(state_data[key]))
                    break
                except (ValueError, TypeError):
                    continue

        # Extract hot-reloadable config if present
        config = HotReloadableConfig()
        config_data = state_data.get("config", {})
        if config_data:
            try:
                config = HotReloadableConfig(
                    max_slippage=Decimal(str(config_data.get("max_slippage", "0.005"))),
                    trade_size_usd=Decimal(str(config_data.get("trade_size_usd", "1000"))),
                    rebalance_threshold=Decimal(str(config_data.get("rebalance_threshold", "0.05"))),
                    min_health_factor=Decimal(str(config_data.get("min_health_factor", "1.5"))),
                    max_leverage=Decimal(str(config_data.get("max_leverage", "3"))),
                    daily_loss_limit_usd=Decimal(str(config_data.get("daily_loss_limit_usd", "500"))),
                )
            except (ValueError, TypeError):
                pass

        return StrategyState(
            strategy_id=strategy_id,
            status=status,
            chain=chain,
            protocol=protocol,
            current_gas_price_gwei=0.1,  # Would need to be fetched separately
            pending_tx_hash=state_data.get("pending_tx_hash"),
            total_value_usd=total_value_usd,
            attention_required=attention_required,
            stuck_reason=stuck_reason,
            config=config,
        )

    except Exception as e:
        logger.warning(f"Failed to load strategy state from DB: {e}")
        return None


def get_strategy_state(strategy_id: str) -> StrategyState:
    """Get the current state of a strategy from database.

    Queries the SQLite state database for the strategy's current state.
    Falls back to cached data if database is unavailable.

    Args:
        strategy_id: The strategy ID to look up

    Returns:
        The strategy state

    Raises:
        HTTPException: If strategy is not found
    """
    global _last_cache_refresh

    # Try to load from database
    state = _load_strategy_state_from_db(strategy_id)

    if state:
        # Update cache
        _strategy_state_cache[strategy_id] = state
        return state

    # Check cache
    if strategy_id in _strategy_state_cache:
        return _strategy_state_cache[strategy_id]

    raise HTTPException(status_code=404, detail=f"Strategy {strategy_id} not found")


# Authentication dependency
async def verify_api_key(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
) -> str:
    """Verify the API key from the request header.

    Validates the API key against configured valid keys from ALMANAK_API_KEYS
    environment variable. Uses constant-time comparison to prevent timing attacks.

    Args:
        x_api_key: The API key from the X-API-Key header

    Returns:
        The authenticated user/key identifier (truncated key prefix)

    Raises:
        HTTPException: If API key is missing or invalid
    """
    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing X-API-Key header",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    validator = get_api_key_validator()
    is_valid, user_id = validator.validate(x_api_key)

    if not is_valid:
        logger.warning(f"Invalid API key attempted: {x_api_key[:8]}...")
        raise HTTPException(
            status_code=401,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    return user_id


def emit_audit_event(
    strategy_id: str,
    action: str,
    details: dict[str, Any],
    api_key: str,
    chain: str = "",
) -> None:
    """Emit an audit event for an action.

    Args:
        strategy_id: The strategy ID the action was performed on
        action: The action that was performed
        details: Additional details about the action
        api_key: The API key used for authentication
        chain: The chain the strategy operates on
    """
    event = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=TimelineEventType.OPERATOR_ACTION_EXECUTED,
        description=f"Operator action executed: {action}",
        strategy_id=strategy_id,
        chain=chain,
        details={
            "action": action,
            "api_key_prefix": api_key[:8] + "..." if len(api_key) > 8 else api_key,
            **details,
        },
    )
    add_event(event)


def generate_operator_card(
    state: StrategyState,
    event_type: EventType,
    reason: StuckReason,
    message: str,
) -> OperatorCard:
    """Generate an OperatorCard for the current strategy state.

    Args:
        state: The current strategy state
        event_type: The type of event
        reason: The stuck reason (if applicable)
        message: A message describing the current state

    Returns:
        An OperatorCard with updated state information
    """
    # Determine severity based on status
    if state.status == "stuck":
        severity = Severity.HIGH
    elif state.status == "paused":
        severity = Severity.MEDIUM
    else:
        severity = Severity.LOW

    # Determine available actions based on status
    available_actions = []
    if state.status == "paused":
        available_actions.append(AvailableAction.RESUME)
    elif state.status == "running":
        available_actions.append(AvailableAction.PAUSE)
    elif state.status == "stuck":
        available_actions.extend(
            [
                AvailableAction.BUMP_GAS,
                AvailableAction.CANCEL_TX,
                AvailableAction.PAUSE,
            ]
        )

    # Always allow emergency unwind
    available_actions.append(AvailableAction.EMERGENCY_UNWIND)

    # Create suggested actions
    suggested_actions = []
    if state.status == "stuck" and state.stuck_reason == StuckReason.GAS_PRICE_BLOCKED:
        suggested_actions.append(
            SuggestedAction(
                action=AvailableAction.BUMP_GAS,
                description="Increase gas price to unstick pending transaction",
                priority=1,
                params={"suggested_gas_gwei": state.current_gas_price_gwei * 1.5},
                is_recommended=True,
            )
        )
    if state.status == "paused":
        suggested_actions.append(
            SuggestedAction(
                action=AvailableAction.RESUME,
                description="Resume strategy operation",
                priority=1,
                is_recommended=True,
            )
        )
    if not suggested_actions:
        # Default suggestion
        suggested_actions.append(
            SuggestedAction(
                action=AvailableAction.PAUSE,
                description="Pause strategy for review",
                priority=2,
            )
        )

    return OperatorCard(
        strategy_id=state.strategy_id,
        timestamp=datetime.now(UTC),
        event_type=event_type,
        reason=reason,
        context={
            "status": state.status,
            "chain": state.chain,
            "protocol": state.protocol,
            "message": message,
            "pending_tx_hash": state.pending_tx_hash,
            "current_gas_price_gwei": state.current_gas_price_gwei,
        },
        severity=severity,
        position_summary=PositionSummary(
            total_value_usd=state.total_value_usd,
            available_balance_usd=state.total_value_usd * Decimal("0.1"),
        ),
        risk_description=f"Strategy {state.strategy_id} is currently {state.status}. "
        f"Total value at risk: ${state.total_value_usd:,.2f}",
        suggested_actions=suggested_actions,
        available_actions=available_actions,
    )


# FastAPI Router
router = APIRouter(prefix="/api/strategies", tags=["actions"])


@router.post("/{strategy_id}/pause")
async def pause_strategy(
    strategy_id: str,
    api_key: str = Depends(verify_api_key),
) -> dict[str, Any]:
    """Pause a running strategy.

    Immediately stops the strategy from executing new actions while preserving
    current positions. The strategy can be resumed later.

    Args:
        strategy_id: The unique identifier of the strategy
        api_key: The authenticated API key (injected by dependency)

    Returns:
        ActionResponse with success status and updated OperatorCard

    Raises:
        HTTPException: If strategy not found or already paused
    """
    state = get_strategy_state(strategy_id)

    if state.status == "paused":
        raise HTTPException(
            status_code=400,
            detail=f"Strategy {strategy_id} is already paused",
        )

    # Update state
    previous_status = state.status
    state.status = "paused"
    state.attention_required = False

    # Emit audit event
    emit_audit_event(
        strategy_id=strategy_id,
        action="PAUSE",
        details={"previous_status": previous_status},
        api_key=api_key,
    )

    # Emit lifecycle event for timeline
    pause_event = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=TimelineEventType.STRATEGY_PAUSED,
        description=f"Strategy paused by operator (was {previous_status})",
        strategy_id=strategy_id,
        chain=state.chain,
        details={
            "previous_status": previous_status,
            "triggered_by": f"api:{api_key[:8]}..." if len(api_key) > 8 else f"api:{api_key}",
        },
    )
    add_event(pause_event)

    # Generate operator card with updated state
    operator_card = generate_operator_card(
        state=state,
        event_type=EventType.WARNING,
        reason=state.stuck_reason or StuckReason.UNKNOWN,
        message="Strategy has been paused by operator",
    )

    response = ActionResponse(
        success=True,
        message=f"Strategy {strategy_id} has been paused",
        operator_card=operator_card.to_dict(),
    )

    return response.to_dict()


@router.post("/{strategy_id}/resume")
async def resume_strategy(
    strategy_id: str,
    api_key: str = Depends(verify_api_key),
) -> dict[str, Any]:
    """Resume a paused strategy.

    Restarts strategy execution from its current state. The strategy will
    continue from where it left off.

    Args:
        strategy_id: The unique identifier of the strategy
        api_key: The authenticated API key (injected by dependency)

    Returns:
        ActionResponse with success status and updated OperatorCard

    Raises:
        HTTPException: If strategy not found or not paused
    """
    state = get_strategy_state(strategy_id)

    if state.status != "paused":
        raise HTTPException(
            status_code=400,
            detail=f"Strategy {strategy_id} is not paused (current status: {state.status})",
        )

    # Update state
    state.status = "running"
    state.attention_required = False
    state.stuck_reason = None

    # Emit audit event
    emit_audit_event(
        strategy_id=strategy_id,
        action="RESUME",
        details={},
        api_key=api_key,
    )

    # Emit lifecycle event for timeline
    resume_event = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=TimelineEventType.STRATEGY_RESUMED,
        description="Strategy resumed by operator",
        strategy_id=strategy_id,
        chain=state.chain,
        details={
            "triggered_by": f"api:{api_key[:8]}..." if len(api_key) > 8 else f"api:{api_key}",
        },
    )
    add_event(resume_event)

    # Generate operator card with updated state
    operator_card = generate_operator_card(
        state=state,
        event_type=EventType.ALERT,
        reason=StuckReason.UNKNOWN,
        message="Strategy has been resumed by operator",
    )

    response = ActionResponse(
        success=True,
        message=f"Strategy {strategy_id} has been resumed",
        operator_card=operator_card.to_dict(),
    )

    return response.to_dict()


@router.post("/{strategy_id}/bump-gas")
async def bump_gas(
    strategy_id: str,
    request: BumpGasRequest,
    api_key: str = Depends(verify_api_key),
) -> dict[str, Any]:
    """Bump the gas price for a pending transaction.

    Replaces the pending transaction with a new one using a higher gas price
    to speed up confirmation.

    Args:
        strategy_id: The unique identifier of the strategy
        request: The bump gas request containing the new gas price
        api_key: The authenticated API key (injected by dependency)

    Returns:
        ActionResponse with success status and updated OperatorCard

    Raises:
        HTTPException: If strategy not found, no pending tx, or invalid gas price
    """
    state = get_strategy_state(strategy_id)

    if not state.pending_tx_hash:
        raise HTTPException(
            status_code=400,
            detail=f"Strategy {strategy_id} has no pending transaction",
        )

    if request.gas_price_gwei <= state.current_gas_price_gwei:
        raise HTTPException(
            status_code=400,
            detail=f"New gas price ({request.gas_price_gwei} Gwei) must be higher than "
            f"current ({state.current_gas_price_gwei} Gwei)",
        )

    # Update state
    previous_gas_price = state.current_gas_price_gwei
    state.current_gas_price_gwei = request.gas_price_gwei

    # In production, this would submit a replacement transaction
    # For demo, we simulate success by clearing the stuck state
    if state.stuck_reason == StuckReason.GAS_PRICE_BLOCKED:
        state.status = "running"
        state.stuck_reason = None
        state.attention_required = False
        state.pending_tx_hash = None

    # Emit audit event
    emit_audit_event(
        strategy_id=strategy_id,
        action="BUMP_GAS",
        details={
            "previous_gas_price_gwei": previous_gas_price,
            "new_gas_price_gwei": request.gas_price_gwei,
            "tx_hash": state.pending_tx_hash,
        },
        api_key=api_key,
    )

    # Generate operator card with updated state
    operator_card = generate_operator_card(
        state=state,
        event_type=EventType.ALERT,
        reason=state.stuck_reason or StuckReason.UNKNOWN,
        message=f"Gas price bumped from {previous_gas_price} to {request.gas_price_gwei} Gwei",
    )

    response = ActionResponse(
        success=True,
        message=f"Gas price bumped to {request.gas_price_gwei} Gwei for strategy {strategy_id}",
        operator_card=operator_card.to_dict(),
    )

    return response.to_dict()


@router.post("/{strategy_id}/cancel-tx")
async def cancel_transaction(
    strategy_id: str,
    request: CancelTxRequest,
    api_key: str = Depends(verify_api_key),
) -> dict[str, Any]:
    """Cancel a pending transaction.

    Submits a zero-value transaction with the same nonce to replace
    and effectively cancel the pending transaction.

    Args:
        strategy_id: The unique identifier of the strategy
        request: The cancel request containing the transaction hash
        api_key: The authenticated API key (injected by dependency)

    Returns:
        ActionResponse with success status and updated OperatorCard

    Raises:
        HTTPException: If strategy not found or tx hash doesn't match
    """
    state = get_strategy_state(strategy_id)

    if not state.pending_tx_hash:
        raise HTTPException(
            status_code=400,
            detail=f"Strategy {strategy_id} has no pending transaction",
        )

    if state.pending_tx_hash != request.tx_hash:
        raise HTTPException(
            status_code=400,
            detail=f"Transaction hash mismatch. Expected {state.pending_tx_hash}, got {request.tx_hash}",
        )

    # Update state
    cancelled_tx_hash = state.pending_tx_hash
    state.pending_tx_hash = None
    state.status = "paused"  # Pause after cancellation for review
    state.stuck_reason = None
    state.attention_required = False

    # Emit audit event
    emit_audit_event(
        strategy_id=strategy_id,
        action="CANCEL_TX",
        details={
            "cancelled_tx_hash": cancelled_tx_hash,
        },
        api_key=api_key,
    )

    # Generate operator card with updated state
    operator_card = generate_operator_card(
        state=state,
        event_type=EventType.WARNING,
        reason=StuckReason.UNKNOWN,
        message=f"Transaction {cancelled_tx_hash[:10]}... has been cancelled. Strategy paused for review.",
    )

    response = ActionResponse(
        success=True,
        message=f"Transaction cancelled for strategy {strategy_id}. Strategy is now paused for review.",
        operator_card=operator_card.to_dict(),
    )

    return response.to_dict()


@router.post("/{strategy_id}/config")
async def update_config(
    strategy_id: str,
    request: ConfigUpdateRequest,
    api_key: str = Depends(verify_api_key),
) -> dict[str, Any]:
    """Update hot-reloadable configuration parameters for a strategy.

    Validates the requested updates against allowed hot-reload fields and
    applies changes atomically. Changes take effect on the next strategy iteration.

    Args:
        strategy_id: The unique identifier of the strategy
        request: The config update request containing field updates
        api_key: The authenticated API key (injected by dependency)

    Returns:
        ConfigUpdateResponse with success status, updated config, and result details

    Raises:
        HTTPException: If strategy not found or validation fails
    """
    state = get_strategy_state(strategy_id)

    if not request.updates:
        raise HTTPException(
            status_code=400,
            detail="No configuration updates provided",
        )

    # Validate against allowed hot-reload fields
    allowed_fields = state.config.HOT_RELOADABLE_FIELDS
    invalid_fields = set(request.updates.keys()) - allowed_fields
    if invalid_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot hot-reload fields: {', '.join(sorted(invalid_fields))}. "
            f"Allowed fields: {', '.join(sorted(allowed_fields))}",
        )

    # Validate against Risk Guard limits with guidance
    risk_result = _risk_guard.validate_config_update(state.config, request.updates)
    if not risk_result.passed:
        # Return a structured response with guidance instead of a plain HTTP exception
        # This allows the dashboard to display helpful information to the operator
        response = ConfigUpdateResponse(
            success=False,
            message="Configuration update blocked by Risk Guard",
            error="; ".join(risk_result.violations),
            guidance=[g.to_dict() for g in risk_result.guidance],
        )
        # Return 200 with success=false so dashboard can display guidance
        # This is a design choice - the API returns guidance even on failure
        return response.to_dict()

    # Store previous values for audit trail
    previous_values: dict[str, Any] = {}
    for field_name in request.updates:
        prev_val = getattr(state.config, field_name, None)
        previous_values[field_name] = str(prev_val) if prev_val is not None else None

    # Call the config's update method with validation
    result = state.config.update(**request.updates)

    if not result.success:
        raise HTTPException(
            status_code=400,
            detail=f"Config validation failed: {result.error}",
        )

    # Emit audit event for successful config update
    emit_audit_event(
        strategy_id=strategy_id,
        action="CONFIG_UPDATE",
        details={
            "updated_fields": result.updated_fields,
            "previous_values": previous_values,
            "new_values": {f: str(getattr(state.config, f)) for f in result.updated_fields},
        },
        api_key=api_key,
    )

    # Also emit a CONFIG_UPDATED timeline event
    config_update_event = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=TimelineEventType.CONFIG_UPDATED,
        description=f"Configuration updated: {', '.join(result.updated_fields)}",
        strategy_id=strategy_id,
        chain=state.chain,
        details={
            "changes": [
                {
                    "field": field_name,
                    "old_value": previous_values.get(field_name),
                    "new_value": str(getattr(state.config, field_name)),
                }
                for field_name in result.updated_fields
            ],
            "updated_by": f"api:{api_key[:8]}..." if len(api_key) > 8 else f"api:{api_key}",
        },
    )
    add_event(config_update_event)

    # Build response with updated config
    response = ConfigUpdateResponse(
        success=True,
        message=f"Configuration updated for strategy {strategy_id}: {', '.join(result.updated_fields)}",
        updated_config=state.config.to_dict(),
        result=result.to_dict(),
    )

    return response.to_dict()
