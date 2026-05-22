"""Data loader for strategy-class-aware accounting reports.

Pulls all accounting-related rows from SQLite for a given deployment_id
and returns a unified AccountingData bundle. Also detects the strategy class
from event types present.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)

from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
    PendleAccountingEvent,
    PendleEventType,
)


class StrategyClass(StrEnum):
    UNKNOWN = "unknown"
    SWAP = "swap"
    LP = "lp"
    LENDING = "lending"
    PENDLE = "pendle"


_LENDING_TYPES: frozenset[str] = frozenset(e.value for e in LendingEventType)
_PENDLE_TYPES: frozenset[str] = frozenset(e.value for e in PendleEventType)


@dataclass
class AccountingData:
    """All persisted accounting data for a single deployment."""

    deployment_id: str
    metrics: Any  # PortfolioMetrics | None
    ledger_entries: list[Any]  # list[LedgerEntry]
    position_events: list[dict]
    snapshot: Any  # PortfolioSnapshot | None

    lending_events: list[LendingAccountingEvent] = field(default_factory=list)
    pendle_events: list[PendleAccountingEvent] = field(default_factory=list)
    # Raw dicts for events where confidence == UNAVAILABLE
    unavailable_records: list[dict] = field(default_factory=list)
    # Count of rows that failed payload deserialization (schema mismatch, etc.)
    parse_errors: int = 0

    strategy_classes: frozenset[StrategyClass] = field(default_factory=frozenset)

    @property
    def has_lending(self) -> bool:
        return StrategyClass.LENDING in self.strategy_classes

    @property
    def has_pendle(self) -> bool:
        return StrategyClass.PENDLE in self.strategy_classes

    @property
    def has_lp(self) -> bool:
        return StrategyClass.LP in self.strategy_classes


def _parse_identity(row: dict) -> AccountingIdentity:
    return AccountingIdentity(
        id=row["id"],
        deployment_id=row["deployment_id"],
        cycle_id=row["cycle_id"],
        execution_mode=row.get("execution_mode", ""),
        timestamp=datetime.fromisoformat(row["timestamp"]),
        chain=row["chain"],
        protocol=row["protocol"],
        wallet_address=row.get("wallet_address", ""),
        tx_hash=row.get("tx_hash") or "",
        ledger_entry_id=row.get("ledger_entry_id") or "",
    )


def _deserialize_events(
    raw_events: list[dict],
) -> tuple[list[LendingAccountingEvent], list[PendleAccountingEvent], list[dict], int]:
    lending: list[LendingAccountingEvent] = []
    pendle: list[PendleAccountingEvent] = []
    unavailable: list[dict] = []
    parse_errors = 0

    for row in raw_events:
        event_type = row.get("event_type", "")
        confidence = row.get("confidence", "")
        payload = row.get("payload_json", "{}")

        if confidence == AccountingConfidence.UNAVAILABLE:
            unavailable.append(row)
            continue  # UNAVAILABLE rows are incomplete — don't also materialize into typed lists

        try:
            identity = _parse_identity(row)
            if event_type in _LENDING_TYPES:
                lending.append(LendingAccountingEvent.from_payload_json(identity, payload))
            elif event_type in _PENDLE_TYPES:
                pendle.append(PendleAccountingEvent.from_payload_json(identity, payload))
        except Exception as exc:
            logger.debug("Failed to deserialize accounting row id=%s event_type=%s: %s", row.get("id"), event_type, exc)
            parse_errors += 1

    return lending, pendle, unavailable, parse_errors


def _detect_strategy_classes(
    lending_events: list[LendingAccountingEvent],
    pendle_events: list[PendleAccountingEvent],
    position_events: list[dict],
    ledger_entries: list[Any],
    unavailable_records: list[dict] | None = None,
) -> frozenset[StrategyClass]:
    classes: set[StrategyClass] = set()

    if lending_events:
        classes.add(StrategyClass.LENDING)
    if pendle_events:
        classes.add(StrategyClass.PENDLE)

    # Also check raw event_type markers from UNAVAILABLE/malformed rows —
    # these carry the protocol signal even when payload deserialization fails.
    for row in unavailable_records or []:
        et = (row.get("event_type") or "").upper()
        if et in _LENDING_TYPES:
            classes.add(StrategyClass.LENDING)
        elif et in _PENDLE_TYPES:
            classes.add(StrategyClass.PENDLE)

    for ev in position_events:
        if (ev.get("position_type") or "").upper() == "LP":
            classes.add(StrategyClass.LP)
            break

    if not classes and any((getattr(e, "intent_type", "") or "").upper() == "SWAP" for e in ledger_entries):
        classes.add(StrategyClass.SWAP)

    if not classes:
        classes.add(StrategyClass.UNKNOWN)

    return frozenset(classes)


async def load_accounting_data(
    db_path: str,
    deployment_id: str,
    ledger_limit: int = 10000,
    position_limit: int = 10000,
    accounting_limit: int = 5000,
) -> AccountingData:
    """Load all persisted accounting rows and return a unified AccountingData."""
    from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    await store.initialize()
    try:
        metrics = await store.get_portfolio_metrics(deployment_id)
        ledger_entries = await store.get_ledger_entries(deployment_id, limit=ledger_limit)
        position_events = await store.get_position_events(deployment_id, limit=position_limit)
        snapshot = await store.get_latest_snapshot(deployment_id)
        raw_accounting = await store.get_accounting_events(deployment_id, limit=accounting_limit)
    finally:
        await store.close()

    lending_events, pendle_events, unavailable, parse_errors = _deserialize_events(raw_accounting or [])

    strategy_classes = _detect_strategy_classes(
        lending_events, pendle_events, position_events or [], ledger_entries or [], unavailable
    )

    return AccountingData(
        deployment_id=deployment_id,
        metrics=metrics,
        ledger_entries=ledger_entries or [],
        position_events=position_events or [],
        snapshot=snapshot,
        lending_events=lending_events,
        pendle_events=pendle_events,
        unavailable_records=unavailable,
        parse_errors=parse_errors,
        strategy_classes=strategy_classes,
    )
