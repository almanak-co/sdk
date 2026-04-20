"""Observability module for structured forensic events, correlation tracking, and transaction ledger.

Provides:
- StrategyPhase enum for strategy lifecycle phases
- ForensicEvent dataclass for structured event logging
- cycle_id context management for correlation across phases
- LedgerEntry / build_ledger_entry for structured trade records
"""

from almanak.framework.observability.context import get_cycle_id, set_cycle_id
from almanak.framework.observability.events import ForensicEvent, StrategyPhase
from almanak.framework.observability.ledger import LedgerEntry, build_ledger_entry

__all__ = [
    "ForensicEvent",
    "LedgerEntry",
    "StrategyPhase",
    "build_ledger_entry",
    "get_cycle_id",
    "set_cycle_id",
]
