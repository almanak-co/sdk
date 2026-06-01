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
from almanak.framework.observability.metrics import (
    ACCOUNTING_RAW_WEI_SUSPECTED_TOTAL,
    FRAMEWORK_REGISTRY,
    V4_LP_PARSER_DROPS_TOTAL,
    V4LPDropReason,
    record_raw_wei_suspected,
    record_v4_lp_parser_drop,
)

__all__ = [
    "ACCOUNTING_RAW_WEI_SUSPECTED_TOTAL",
    "FRAMEWORK_REGISTRY",
    "ForensicEvent",
    "LedgerEntry",
    "StrategyPhase",
    "V4_LP_PARSER_DROPS_TOTAL",
    "V4LPDropReason",
    "build_ledger_entry",
    "get_cycle_id",
    "record_raw_wei_suspected",
    "record_v4_lp_parser_drop",
    "set_cycle_id",
]
