"""Per-tick decision telemetry for PnL backtest runs.

Backtest counterpart of the live runner's ``iteration_summary`` record
(``almanak/framework/runner/runner_state.py::emit_iteration_summary``): every
``decide()`` outcome lands here as one structured, JSON-safe record, so a run
can explain *why* it held or traded without being re-run. Before this plane
existed, a strategy that returned ``Intent.hold(reason=...)`` on every tick
produced a "successful" flat run with the reasons discarded — zero log lines,
zero artifact evidence (the silent 100%-hold genre; see staging backtest
``643d3686`` / ALM-3123).

Design constraints:

- **Purely observational.** Recording never touches simulation state and never
  raises into the tick loop (defensive extraction; a strategy's exotic intent
  object cannot break the run).
- **Deterministic.** Records carry the tick index and *simulated* timestamp
  only — no wall clock, no randomness — so identical runs produce identical
  telemetry (bit-identity sweeps stay clean).
- **Exactly one record per tick.** The engine-side warm-up / decide-error
  branches record first (with their cause); the loop's unconditional
  follow-up call is then a no-op for that tick (first write wins).
- **Bounded aggregate.** Hold reasons are grouped by a digit-normalized
  template (``"cooldown 1799s remaining"`` and ``"cooldown 1798s remaining"``
  are one group) and the group table is capped, so free-text reasons with
  embedded numbers cannot bloat the summary.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any

__all__ = ["DecisionLog", "DECISION_SUMMARY_SCHEMA_VERSION"]

DECISION_SUMMARY_SCHEMA_VERSION = 1

# Distinct hold-reason groups kept in the aggregate; the tail collapses into a
# single "(other)" row so a pathological reason generator cannot bloat
# result.json. 100 >> anything observed in real runs (typical runs have <5).
_MAX_REASON_GROUPS = 100

_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")

# Synthetic reason codes for engine-side holds (no HoldIntent exists there).
# ``source`` on the record already distinguishes engine from strategy holds;
# the codes make the aggregate rows self-describing.
_ENGINE_HOLD_CODES = {
    "warm_up": "ENGINE_INDICATOR_WARM_UP",
    "decide_error": "ENGINE_DECIDE_ERROR",
}


def _json_safe(value: Any, _depth: int = 0) -> Any:
    """Best-effort conversion of an intent payload into JSON-serializable types."""
    if _depth > 6:  # defensive: exotic self-referential payloads
        return str(value)
    if value is None or isinstance(value, bool | int | float | str):
        return value
    # NOTE: deliberately no explicit Decimal branch — the terminal
    # ``str(value)`` fallback renders Decimals identically, and an explicit
    # Decimal type-check here would trip the VIB-4062 caller-bifurcation
    # contract gate (tests/contracts/test_no_bifurcation.py greps source text).
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(k): _json_safe(v, _depth + 1) for k, v in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [_json_safe(v, _depth + 1) for v in value]
    return str(value)


def _reason_template(reason: str) -> str:
    """Digit-normalized grouping key for free-text hold reasons."""
    return _NUMBER_RE.sub("N", reason)


@dataclass
class _ReasonGroup:
    source: str
    reason_code: str | None
    reason_template: str
    example: str
    ticks: int
    first_tick: int
    last_tick: int


@dataclass
class DecisionLog:
    """Collects one decision record per simulation tick; aggregates at the end."""

    _events: list[dict[str, Any]] = field(default_factory=list)
    _groups: dict[tuple[str, str, str], _ReasonGroup] = field(default_factory=dict)
    _intent_types: dict[str, int] = field(default_factory=dict)
    _hold_ticks: int = 0
    _intent_ticks: int = 0
    _last_tick_recorded: int = 0

    def record(
        self,
        *,
        tick: int,
        timestamp: datetime,
        intent: Any,
        source: str,
        detail: str | None = None,
    ) -> None:
        """Record the decision for ``tick`` (first write wins; never raises).

        Args:
            tick: 1-based tick index.
            timestamp: The tick's *simulated* timestamp.
            intent: The extracted intent — an intent object, a ``HoldIntent``,
                or ``None`` (engine-side hold / strategy returned nothing).
            source: ``"strategy"`` for ``decide()`` outcomes, ``"warm_up"`` /
                ``"decide_error"`` for the engine's exception branches.
            detail: Engine-side cause (the exception text) when ``intent`` is
                ``None`` because ``decide()`` raised.
        """
        if tick <= self._last_tick_recorded:
            return
        try:
            self._events.append(self._build_event(tick, timestamp, intent, source, detail))
            self._last_tick_recorded = tick
        except Exception:  # noqa: BLE001 — telemetry must never break the tick loop
            # Exactly-once still holds on extraction failure: append a minimal
            # marker event so summary tick counts match the event stream (an
            # advanced marker without an event would under-report events and
            # silently drop the tick from the sidecar).
            try:
                self._events.append(
                    {
                        "event": "decision",
                        "tick": tick,
                        "timestamp": timestamp.isoformat(),
                        "source": source,
                        "decision": None,
                        "extraction_error": True,
                    }
                )
                self._hold_ticks += 1
                self._count_reason(source, "TELEMETRY_EXTRACTION_ERROR", "(intent extraction failed)", tick)
            except Exception:  # noqa: BLE001 — last-resort: never raise into the loop
                pass
            self._last_tick_recorded = tick

    def _build_event(
        self,
        tick: int,
        timestamp: datetime,
        intent: Any,
        source: str,
        detail: str | None,
    ) -> dict[str, Any]:
        # Tolerate both intent shapes the engine accepts: enum intent_type
        # (canonical intents — read .value) and plain-string intent_type
        # (duck-typed strategy intents).
        raw_type = getattr(intent, "intent_type", None)
        intent_type = getattr(raw_type, "value", raw_type)
        intent_type = str(intent_type) if intent_type is not None else None
        is_hold = intent is None or intent_type == "HOLD"
        event: dict[str, Any] = {
            "event": "decision",
            "tick": tick,
            "timestamp": timestamp.isoformat(),
            "source": source,
            "decision": intent_type,
        }
        if is_hold:
            reason = getattr(intent, "reason", None) if intent is not None else detail
            reason_code = getattr(intent, "reason_code", None) if intent is not None else None
            if reason_code is None and source in _ENGINE_HOLD_CODES:
                reason_code = _ENGINE_HOLD_CODES[source]
            event["hold_reason"] = reason
            event["hold_reason_code"] = reason_code
            self._hold_ticks += 1
            self._count_reason(source, reason_code, reason, tick)
        else:
            self._intent_ticks += 1
            self._intent_types[intent_type or "UNKNOWN"] = self._intent_types.get(intent_type or "UNKNOWN", 0) + 1
            serialize = getattr(intent, "serialize", None)
            if callable(serialize):
                try:
                    event["intents"] = [_json_safe(serialize())]
                except Exception:  # noqa: BLE001 — a broken serialize() must not lose the record
                    event["intents"] = [str(intent)]
            else:
                event["intents"] = [str(intent)]
        return event

    def _count_reason(self, source: str, reason_code: str | None, reason: str | None, tick: int) -> None:
        reason_text = reason if reason else "(no reason given)"
        key = (source, reason_code or "", _reason_template(reason_text))
        group = self._groups.get(key)
        if group is not None:
            group.ticks += 1
            group.last_tick = tick
            return
        if len(self._groups) >= _MAX_REASON_GROUPS:
            key = ("", "", "(other)")
            overflow = self._groups.get(key)
            if overflow is not None:
                overflow.ticks += 1
                overflow.last_tick = tick
                return
            self._groups[key] = _ReasonGroup("", None, "(other)", "(other)", 1, tick, tick)
            return
        self._groups[key] = _ReasonGroup(
            source=source,
            reason_code=reason_code,
            reason_template=_reason_template(reason_text),
            example=reason_text,
            ticks=1,
            first_tick=tick,
            last_tick=tick,
        )

    def events(self) -> list[dict[str, Any]]:
        """All per-tick decision records, in tick order."""
        return list(self._events)

    def summary(self, *, trades: list[Any] | None = None) -> dict[str, Any]:
        """Deterministic aggregate for ``result.json`` and the run-end log.

        Args:
            trades: The run's ``TradeRecord`` list; folded in as execution
                counts so one block answers "decided what / filled what".
        """
        reasons = sorted(
            self._groups.values(),
            key=lambda g: (-g.ticks, g.first_tick, g.reason_template),
        )
        trade_rows = trades or []
        fills = sum(1 for t in trade_rows if getattr(t, "success", False))
        rejected = sum(1 for t in trade_rows if not getattr(t, "success", True))
        by_intent_type: dict[str, dict[str, int]] = {}
        rejection_groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for trade in trade_rows:
            raw_type = getattr(trade, "intent_type", "UNKNOWN")
            intent_type = str(getattr(raw_type, "value", raw_type)).upper()
            counts = by_intent_type.setdefault(intent_type, {"fills": 0, "rejected": 0})
            if getattr(trade, "success", False):
                counts["fills"] += 1
                continue
            counts["rejected"] += 1
            metadata = getattr(trade, "metadata", None) or {}
            reason = str(getattr(trade, "error", None) or metadata.get("failure_reason") or "fill rejected")
            code = str(metadata.get("rejection_code") or "UNCLASSIFIED")
            protocol = str(getattr(trade, "protocol", "") or "unknown").lower()
            key = (intent_type, protocol, code, _reason_template(reason))
            group = rejection_groups.get(key)
            if group is None:
                if len(rejection_groups) >= _MAX_REASON_GROUPS:
                    key = ("OTHER", "other", "OTHER", "(other)")
                    group = rejection_groups.get(key)
                if group is None:
                    timestamp = getattr(trade, "timestamp", None)
                    serialized_ts = timestamp.isoformat() if isinstance(timestamp, datetime) else None
                    group = {
                        "intent_type": key[0],
                        "protocol": key[1],
                        "rejection_code": key[2],
                        "reason_template": key[3],
                        "example": reason,
                        "count": 0,
                        "first_timestamp": serialized_ts,
                        "last_timestamp": serialized_ts,
                        "intent_id": metadata.get("intent_id"),
                        "position_id": getattr(trade, "position_id", None),
                        "retryable": metadata.get("retryable"),
                    }
                    rejection_groups[key] = group
            group["count"] += 1
            timestamp = getattr(trade, "timestamp", None)
            if isinstance(timestamp, datetime):
                group["last_timestamp"] = timestamp.isoformat()
        sorted_rejections = sorted(
            rejection_groups.values(),
            key=lambda group: (-group["count"], group["intent_type"], group["reason_template"]),
        )
        return {
            "schema_version": DECISION_SUMMARY_SCHEMA_VERSION,
            "ticks": self._last_tick_recorded,
            "intent_ticks": self._intent_ticks,
            "hold_ticks": self._hold_ticks,
            "intent_types": dict(sorted(self._intent_types.items())),
            "hold_reasons": [
                {
                    "source": g.source,
                    "reason_code": g.reason_code,
                    "reason_template": g.reason_template,
                    "example": g.example,
                    "ticks": g.ticks,
                    "first_tick": g.first_tick,
                    "last_tick": g.last_tick,
                }
                for g in reasons
            ],
            "executions": {"fills": fills, "rejected": rejected},
            "execution_by_intent_type": dict(sorted(by_intent_type.items())),
            "rejections": sorted_rejections,
        }
