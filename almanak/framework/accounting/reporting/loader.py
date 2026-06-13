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
)


class StrategyClass(StrEnum):
    UNKNOWN = "unknown"
    SWAP = "swap"
    LP = "lp"
    LENDING = "lending"


_LENDING_TYPES: frozenset[str] = frozenset(e.value for e in LendingEventType)

# VIB-5084: how many recent Track-C rows to pull for the live-HF lookup. The
# latest portfolio snapshot's lending legs sit at the head of the ``captured_at
# DESC`` order, so a small bounded window suffices; generous enough to cover a
# multi-leg / multi-protocol deployment's most recent snapshot.
_TRACK_C_LOOKBACK = 256


@dataclass
class AccountingData:
    """All persisted accounting data for a single deployment."""

    deployment_id: str
    metrics: Any  # PortfolioMetrics | None
    ledger_entries: list[Any]  # list[LedgerEntry]
    position_events: list[dict]
    snapshot: Any  # PortfolioSnapshot | None

    lending_events: list[LendingAccountingEvent] = field(default_factory=list)
    connector_events: dict[str, list[Any]] = field(default_factory=dict)
    # Raw dicts for events where confidence == UNAVAILABLE
    unavailable_records: list[dict] = field(default_factory=list)
    # Count of rows that failed payload deserialization (schema mismatch, etc.)
    parse_errors: int = 0

    strategy_classes: frozenset[StrategyClass | str] = field(default_factory=frozenset)

    # VIB-4907 / F4: recent portfolio snapshots ordered oldest-first within
    # the loaded window.  ``snapshot`` (above) is always the latest of these
    # when populated, kept as a separate field for backward compat with
    # consumers that only need the head.  The window is consumed by
    # :func:`detect_stale_post_teardown_snapshot` to suppress misleading
    # headline PnL on the SWAP-class fallback pattern.  Empty list when no
    # snapshots exist.
    recent_snapshots: list[Any] = field(default_factory=list)

    # VIB-5084: recent Track-C ``position_state_snapshots`` rows (raw dicts,
    # newest-first). Carries the live per-iteration ``health_factor`` /
    # ``supply_apy_pct`` the lending report prefers over the frozen
    # event-derived values. Empty list when Track-C is absent (the report falls
    # back to the event-derived HF with an as-of timestamp).
    position_state_snapshots: list[dict] = field(default_factory=list)

    @property
    def has_lending(self) -> bool:
        return self.has_strategy_class(StrategyClass.LENDING)

    def has_strategy_class(self, strategy_class: StrategyClass | str) -> bool:
        """Return whether this data bundle includes a framework or connector class label."""
        return strategy_class in self.strategy_classes

    @property
    def has_lp(self) -> bool:
        return self.has_strategy_class(StrategyClass.LP)


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
) -> tuple[list[LendingAccountingEvent], dict[str, list[Any]], list[dict], int]:
    from almanak.connectors._strategy_accounting_report_registry import ACCOUNTING_REPORT_REGISTRY

    lending: list[LendingAccountingEvent] = []
    connector_events: dict[str, list[Any]] = {}
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
                continue
            connector_event = ACCOUNTING_REPORT_REGISTRY.deserialize_event(event_type, identity, payload)
            if connector_event is not None:
                key, event = connector_event
                connector_events.setdefault(key, []).append(event)
        except Exception as exc:
            logger.debug("Failed to deserialize accounting row id=%s event_type=%s: %s", row.get("id"), event_type, exc)
            parse_errors += 1

    return lending, connector_events, unavailable, parse_errors


def _strategy_class_label(value: str) -> StrategyClass | str:
    try:
        return StrategyClass(value)
    except ValueError:
        return value


def _detect_strategy_classes(
    lending_events: list[LendingAccountingEvent],
    position_events: list[dict],
    ledger_entries: list[Any],
    unavailable_records: list[dict] | None = None,
    connector_events: dict[str, list[Any]] | None = None,
) -> frozenset[StrategyClass | str]:
    from almanak.connectors._strategy_accounting_report_registry import ACCOUNTING_REPORT_REGISTRY

    classes: set[StrategyClass | str] = set()

    if lending_events:
        classes.add(StrategyClass.LENDING)
    for key, events in (connector_events or {}).items():
        if not events:
            continue
        connector = ACCOUNTING_REPORT_REGISTRY.get(key)
        classes.add(_strategy_class_label(connector.strategy_class if connector is not None else key))

    # Also check raw event_type markers from UNAVAILABLE/malformed rows —
    # these carry the protocol signal even when payload deserialization fails.
    for row in unavailable_records or []:
        et = (row.get("event_type") or "").upper()
        if et in _LENDING_TYPES:
            classes.add(StrategyClass.LENDING)
            continue
        connector_class = ACCOUNTING_REPORT_REGISTRY.strategy_class_for_event_type(et)
        if connector_class is not None:
            classes.add(_strategy_class_label(connector_class))

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
    snapshot_window: int = 2,
) -> AccountingData:
    """Load all persisted accounting rows and return a unified AccountingData.

    ``snapshot_window`` controls how many of the most-recent
    ``portfolio_snapshots`` rows to materialise into
    ``AccountingData.recent_snapshots`` (oldest-first).  Defaults to ``2``
    so the F4 / VIB-4907 SWAP-class fallback detector has the pre / post
    pair it needs.  The latest is also returned via ``snapshot`` for
    backward compatibility.
    """
    from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

    if snapshot_window < 1:
        snapshot_window = 1

    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    await store.initialize()
    try:
        metrics = await store.get_portfolio_metrics(deployment_id)
        ledger_entries = await store.get_ledger_entries(deployment_id, limit=ledger_limit)
        position_events = await store.get_position_events(deployment_id, limit=position_limit)
        recent_snapshots = await store.get_recent_snapshots(deployment_id, limit=snapshot_window)
        raw_accounting = await store.get_accounting_events(deployment_id, limit=accounting_limit)
        # VIB-5084: latest Track-C rows (newest-first) for the live HF / APY the
        # lending report prefers over frozen event values. Bounded lookback —
        # the latest snapshot's lending legs sit at the head of the DESC order.
        position_state_snapshots = await store.get_position_state_snapshots(
            deployment_id=deployment_id, limit=_TRACK_C_LOOKBACK
        )
    finally:
        await store.close()

    # The latest snapshot is the tail of the oldest-first window.  Preserves
    # the original ``snapshot`` contract while making the prior snapshot
    # available alongside.
    snapshot = recent_snapshots[-1] if recent_snapshots else None

    lending_events, connector_events, unavailable, parse_errors = _deserialize_events(raw_accounting or [])

    strategy_classes = _detect_strategy_classes(
        lending_events,
        position_events or [],
        ledger_entries or [],
        unavailable,
        connector_events=connector_events,
    )

    return AccountingData(
        deployment_id=deployment_id,
        metrics=metrics,
        ledger_entries=ledger_entries or [],
        position_events=position_events or [],
        snapshot=snapshot,
        lending_events=lending_events,
        connector_events=connector_events,
        unavailable_records=unavailable,
        parse_errors=parse_errors,
        strategy_classes=strategy_classes,
        recent_snapshots=recent_snapshots or [],
        position_state_snapshots=position_state_snapshots or [],
    )
