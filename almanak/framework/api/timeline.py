"""Event Timeline API for strategy event history.

This module provides FastAPI endpoints for retrieving chronological views of
strategy events, supporting pagination and filtering.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query

logger = logging.getLogger(__name__)

# Path to the events cache file (shared between runner and dashboard)
EVENTS_CACHE_FILE = Path(__file__).parent.parent.parent / ".dashboard_events.json"


class TimelineEventType(StrEnum):
    """Types of events that can appear in the timeline."""

    # Transaction events
    TRANSACTION_SUBMITTED = "TRANSACTION_SUBMITTED"
    TRANSACTION_CONFIRMED = "TRANSACTION_CONFIRMED"
    TRANSACTION_FAILED = "TRANSACTION_FAILED"
    TRANSACTION_REVERTED = "TRANSACTION_REVERTED"

    # Position events
    POSITION_OPENED = "POSITION_OPENED"
    POSITION_CLOSED = "POSITION_CLOSED"
    POSITION_MODIFIED = "POSITION_MODIFIED"
    REBALANCE_EXECUTED = "REBALANCE_EXECUTED"

    # LP-specific events
    LP_OPEN = "LP_OPEN"
    LP_CLOSE = "LP_CLOSE"

    # Trade events
    SWAP = "SWAP"
    TRADE = "TRADE"

    # Strategy state events
    STATE_CHANGE = "STATE_CHANGE"
    STRATEGY_STARTED = "STRATEGY_STARTED"
    STRATEGY_PAUSED = "STRATEGY_PAUSED"
    STRATEGY_RESUMED = "STRATEGY_RESUMED"
    STRATEGY_STOPPED = "STRATEGY_STOPPED"
    STRATEGY_STUCK = "STRATEGY_STUCK"
    STRATEGY_RECOVERED = "STRATEGY_RECOVERED"

    # Risk events
    RISK_GUARD_TRIGGERED = "RISK_GUARD_TRIGGERED"
    CIRCUIT_BREAKER_TRIGGERED = "CIRCUIT_BREAKER_TRIGGERED"
    HEALTH_FACTOR_LOW = "HEALTH_FACTOR_LOW"

    # Config events
    CONFIG_UPDATED = "CONFIG_UPDATED"

    # Alerting events
    ALERT_SENT = "ALERT_SENT"
    ALERT_ACKNOWLEDGED = "ALERT_ACKNOWLEDGED"

    # Auto-remediation events
    AUTO_REMEDIATION_STARTED = "AUTO_REMEDIATION_STARTED"
    AUTO_REMEDIATION_SUCCESS = "AUTO_REMEDIATION_SUCCESS"
    AUTO_REMEDIATION_FAILED = "AUTO_REMEDIATION_FAILED"

    # Operator actions
    OPERATOR_ACTION_EXECUTED = "OPERATOR_ACTION_EXECUTED"

    # Error events
    ERROR = "ERROR"
    ERROR_OCCURRED = "ERROR_OCCURRED"

    # Copy trading events
    LEADER_SIGNAL_DETECTED = "LEADER_SIGNAL_DETECTED"
    LEADER_SIGNAL_SKIPPED = "LEADER_SIGNAL_SKIPPED"
    COPY_INTENT_CREATED = "COPY_INTENT_CREATED"
    COPY_EXECUTION_RESULT = "COPY_EXECUTION_RESULT"
    COPY_DECISION_MADE = "COPY_DECISION_MADE"
    COPY_POLICY_BLOCKED = "COPY_POLICY_BLOCKED"
    COPY_EXECUTION_QUALITY = "COPY_EXECUTION_QUALITY"
    COPY_CIRCUIT_BREAKER = "COPY_CIRCUIT_BREAKER"

    # Generic event for custom events
    CUSTOM = "CUSTOM"


# Block explorer URL templates by chain
BLOCK_EXPLORER_URLS: dict[str, str] = {
    "ethereum": "https://etherscan.io/tx/{tx_hash}",
    "arbitrum": "https://arbiscan.io/tx/{tx_hash}",
    "optimism": "https://optimistic.etherscan.io/tx/{tx_hash}",
    "polygon": "https://polygonscan.com/tx/{tx_hash}",
    "base": "https://basescan.org/tx/{tx_hash}",
    "avalanche": "https://snowtrace.io/tx/{tx_hash}",
    "bsc": "https://bscscan.com/tx/{tx_hash}",
}


def get_block_explorer_url(chain: str, tx_hash: str) -> str | None:
    """Get the block explorer URL for a transaction.

    Args:
        chain: The blockchain network name (lowercase)
        tx_hash: The transaction hash

    Returns:
        The block explorer URL for the transaction, or None if chain is unknown
    """
    template = BLOCK_EXPLORER_URLS.get(chain.lower())
    if template:
        return template.format(tx_hash=tx_hash)
    return None


@dataclass
class TimelineEvent:
    """An event in the strategy timeline.

    Represents a single event with all relevant context for display
    in a chronological timeline view.
    """

    timestamp: datetime
    event_type: TimelineEventType
    description: str
    tx_hash: str | None = None

    # Additional context
    strategy_id: str = ""
    chain: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    # Computed field for block explorer link
    block_explorer_url: str | None = None

    def __post_init__(self) -> None:
        """Compute block explorer URL after initialization."""
        if self.tx_hash and self.chain and not self.block_explorer_url:
            self.block_explorer_url = get_block_explorer_url(self.chain, self.tx_hash)

    def to_dict(self) -> dict[str, Any]:
        """Convert the timeline event to a dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type.value,
            "description": self.description,
            "tx_hash": self.tx_hash,
            "strategy_id": self.strategy_id,
            "chain": self.chain,
            "details": self.details,
            "block_explorer_url": self.block_explorer_url,
        }


@dataclass
class TimelineResponse:
    """Response object for timeline queries.

    Includes pagination metadata and the list of events.
    """

    events: list[TimelineEvent]
    total_count: int
    offset: int
    limit: int
    has_more: bool

    def to_dict(self) -> dict[str, Any]:
        """Convert the response to a dictionary for serialization."""
        return {
            "events": [e.to_dict() for e in self.events],
            "total_count": self.total_count,
            "offset": self.offset,
            "limit": self.limit,
            "has_more": self.has_more,
        }


# In-memory event storage
# Events are also persisted to file for cross-process sharing with dashboard
_event_store: dict[str, list[TimelineEvent]] = {}

# Gateway client for dual-write persistence (registered by StrategyRunner at startup)
_gateway_client: Any = None


def set_event_gateway_client(client: Any) -> None:
    """Register a gateway gRPC client for persistent event storage.

    Called by StrategyRunner at startup to enable dual-write of timeline events
    to both local file and gateway's persistent SQLite store.

    Args:
        client: A gateway gRPC client with an `observe` stub.
    """
    global _gateway_client
    _gateway_client = client


def get_event_gateway_client() -> Any:
    """Get the registered gateway client (for testing)."""
    return _gateway_client


def _load_events_from_file() -> None:
    """Load events from cache file into memory on startup."""
    global _event_store
    if not EVENTS_CACHE_FILE.exists():
        return

    try:
        with open(EVENTS_CACHE_FILE) as f:
            cached_data = json.load(f)

        # Handle both formats: dict keyed by strategy_id, or flat list
        if isinstance(cached_data, list):
            # Old format: flat list of events
            events_by_strategy: dict[str, list] = {}
            for event_data in cached_data:
                sid = event_data.get("strategy_id", "unknown")
                if sid not in events_by_strategy:
                    events_by_strategy[sid] = []
                events_by_strategy[sid].append(event_data)
            cached_data = events_by_strategy

        for strategy_id, events_data in cached_data.items():
            if strategy_id not in _event_store:
                _event_store[strategy_id] = []
            for event_data in events_data:
                # Map event_type - handle both old lowercase and new uppercase formats
                event_type_str = event_data.get("event_type", "CUSTOM").upper()
                # Map old format types to new enum values
                type_mapping = {
                    "TRADE": "TRANSACTION_CONFIRMED",
                    "DEPOSIT": "POSITION_MODIFIED",
                    "WITHDRAW": "POSITION_MODIFIED",
                    "REBALANCE": "REBALANCE_EXECUTED",
                    "STATE_CHANGE": "STRATEGY_STARTED",
                }
                event_type_str = type_mapping.get(event_type_str, event_type_str)

                try:
                    event_type = TimelineEventType(event_type_str)
                except ValueError:
                    event_type = TimelineEventType.CUSTOM

                event = TimelineEvent(
                    timestamp=datetime.fromisoformat(event_data["timestamp"]),
                    event_type=event_type,
                    description=event_data.get("description", ""),
                    tx_hash=event_data.get("tx_hash"),
                    strategy_id=event_data.get("strategy_id", strategy_id),
                    chain=event_data.get("chain", ""),
                    details=event_data.get("details") or event_data.get("metadata", {}),
                )
                # Avoid duplicates
                if not any(
                    e.timestamp == event.timestamp and e.description == event.description
                    for e in _event_store[strategy_id]
                ):
                    _event_store[strategy_id].append(event)

            # Sort by timestamp descending
            _event_store[strategy_id].sort(key=lambda e: e.timestamp, reverse=True)

        total_events = sum(len(e) for e in _event_store.values())
        if total_events > 0:
            logger.info(f"Loaded {total_events} events from {EVENTS_CACHE_FILE}")
    except Exception as e:
        logger.warning(f"Failed to load events from file: {e}")


# Load existing events on module import
_load_events_from_file()


def add_event(event: TimelineEvent) -> None:
    """Add an event to the timeline store, persist to file, and send to gateway.

    Dual-write: events go to both local .dashboard_events.json (fallback) and
    the gateway's persistent SQLite store (if a gateway client is registered).
    All 26+ callers get gateway persistence for free with zero call-site changes.

    Args:
        event: The timeline event to store
    """
    if event.strategy_id not in _event_store:
        _event_store[event.strategy_id] = []
    _event_store[event.strategy_id].append(event)
    # Keep events sorted by timestamp descending
    _event_store[event.strategy_id].sort(key=lambda e: e.timestamp, reverse=True)

    # Persist to file for cross-process sharing (dashboard) — only in local mode
    if _gateway_client is None:
        _persist_events_to_file()

    # Write to gateway for persistent storage (non-fatal)
    if _gateway_client is not None:
        try:
            from almanak.gateway.proto import gateway_pb2

            request = gateway_pb2.RecordTimelineEventRequest(
                strategy_id=event.strategy_id,
                event_type=event.event_type.value if hasattr(event.event_type, "value") else str(event.event_type),
                description=event.description,
                tx_hash=event.tx_hash or "",
                chain=event.chain or "",
                details_json=json.dumps(event.details) if event.details else "",
                timestamp=int(event.timestamp.timestamp()),
            )
            _gateway_client.observe.RecordTimelineEvent(request, timeout=2.0)
        except Exception as e:
            logger.debug(f"Failed to send event to gateway (non-fatal): {e}")


def _persist_events_to_file() -> None:
    """Persist all events to the cache file for dashboard access."""
    try:
        # Convert events to serializable format
        cache_data = {}
        for strategy_id, events in _event_store.items():
            cache_data[strategy_id] = [e.to_dict() for e in events]

        # Write atomically using temp file
        temp_file = EVENTS_CACHE_FILE.with_suffix(".tmp")
        with open(temp_file, "w") as f:
            json.dump(cache_data, f, indent=2, default=str)
        temp_file.replace(EVENTS_CACHE_FILE)

        logger.debug(f"Persisted {sum(len(e) for e in cache_data.values())} events to {EVENTS_CACHE_FILE}")
    except Exception as e:
        logger.warning(f"Failed to persist events to file: {e}")


def get_events(
    strategy_id: str,
    event_type: TimelineEventType | None = None,
    offset: int = 0,
    limit: int = 50,
) -> TimelineResponse:
    """Get timeline events for a strategy.

    Args:
        strategy_id: The strategy ID to get events for
        event_type: Optional filter by event type
        offset: Number of events to skip (for pagination)
        limit: Maximum number of events to return

    Returns:
        TimelineResponse with paginated events
    """
    events = _event_store.get(strategy_id, [])

    # Filter by event type if specified
    if event_type is not None:
        events = [e for e in events if e.event_type == event_type]

    # Calculate total before pagination
    total_count = len(events)

    # Apply pagination
    paginated_events = events[offset : offset + limit]

    # Check if there are more events
    has_more = offset + len(paginated_events) < total_count

    return TimelineResponse(
        events=paginated_events,
        total_count=total_count,
        offset=offset,
        limit=limit,
        has_more=has_more,
    )


def clear_events(strategy_id: str | None = None) -> None:
    """Clear events from the store and file.

    Args:
        strategy_id: If provided, only clear events for this strategy.
                    If None, clear all events.
    """
    if strategy_id:
        _event_store.pop(strategy_id, None)
    else:
        _event_store.clear()

    # Persist the cleared state
    _persist_events_to_file()


# FastAPI Router
router = APIRouter(prefix="/api/strategies", tags=["timeline"])


@router.get("/{strategy_id}/timeline")
def get_strategy_timeline(
    strategy_id: str,
    event_type: TimelineEventType | None = Query(None, description="Filter by event type"),
    offset: int = Query(0, ge=0, description="Number of events to skip"),
    limit: int = Query(50, ge=1, le=100, description="Maximum events to return"),
) -> dict[str, Any]:
    """Get the event timeline for a strategy.

    Returns a paginated list of events in chronological order (most recent first),
    with optional filtering by event type.

    Args:
        strategy_id: The unique identifier of the strategy
        event_type: Optional event type to filter by
        offset: Number of events to skip for pagination
        limit: Maximum number of events to return (1-100)

    Returns:
        TimelineResponse with events and pagination metadata
    """
    response = get_events(
        strategy_id=strategy_id,
        event_type=event_type,
        offset=offset,
        limit=limit,
    )
    return response.to_dict()
