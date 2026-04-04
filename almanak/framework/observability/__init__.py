"""Observability module for structured forensic events and correlation tracking.

Provides:
- StrategyPhase enum for strategy lifecycle phases
- ForensicEvent dataclass for structured event logging
- cycle_id context management for correlation across phases
"""

from almanak.framework.observability.context import get_cycle_id, set_cycle_id
from almanak.framework.observability.events import ForensicEvent, StrategyPhase

__all__ = [
    "ForensicEvent",
    "StrategyPhase",
    "get_cycle_id",
    "set_cycle_id",
]
