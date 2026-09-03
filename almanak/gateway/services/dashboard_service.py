"""DashboardService implementation - provides data for operator dashboards.

This service exposes strategy data for dashboards via gRPC. All filesystem
and database access happens here in the gateway; dashboard containers only
receive the formatted data.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import grpc

if TYPE_CHECKING:
    from almanak.framework.dashboard.quant_aggregations import DrawdownState
    from almanak.framework.observability.ledger import LedgerQuantStats
    from almanak.framework.portfolio.models import PortfolioSnapshot
    from almanak.framework.state.state_manager import StateManager

from almanak.core.chains import LEGACY_SERIALIZED_CHAIN
from almanak.core.lifecycle import LifecycleValueError, parse_lifecycle_command, require_enqueueable_command
from almanak.framework.portfolio.models import serialize_value_confidence
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.registry import get_instance_registry
from almanak.gateway.services._dashboard_helpers import (
    build_chain_health,
    build_position_proto,
    build_registry_strategy_info,
    build_state_only_strategy_info,
    build_strategy_summary_kwargs,
    enrich_strategy_info,
    lookup_strategy_source,
)
from almanak.gateway.timeline.store import get_timeline_store
from almanak.gateway.validation import ValidationError, validate_deployment_id
from almanak.integrations._base.gateway.portfolio_chain import PortfolioProviderChain, build_portfolio_chain

logger = logging.getLogger(__name__)


STRATEGY_CATEGORIES = ["demo", "production", "incubating", "poster_child", "tests"]
PORTFOLIO_STALE_THRESHOLD_SECONDS = 300

# Bound the oldest-first anchor walk without restoring full ledger materialization.
_QUANT_ANCHOR_BATCH_LIMIT = 64
_QUANT_ANCHOR_BATCH_MAX = 4096
_QUANT_ANCHOR_SCAN_ROW_CAP = 100_000

# Coalesce sibling quant RPCs within a render; zero disables sequential caching.
_QUANT_INPUTS_CACHE_TTL_SECONDS = 5.0

# Refresh the O(history) maximum every 30 seconds; fold new rows between scans.
_LIFETIME_DRAWDOWN_TTL_SECONDS = 30.0

# Retry failed full scans sooner, but not on every render.
_LIFETIME_DRAWDOWN_RETRY_SECONDS = 5.0


@dataclasses.dataclass(frozen=True)
class _LifetimeDrawdownCheckpoint:
    """In-memory resumable lifetime-drawdown state per deployment (VIB-5134).

    ``state`` is the running-peak drawdown fold; ``cursor`` is the ``(timestamp,
    id)`` of the last snapshot folded so the next render fetches only newer rows;
    ``full_scan_at`` is the ``time.monotonic()`` of the last expensive full-history
    scan (gated by :data:`_LIFETIME_DRAWDOWN_TTL_SECONDS`); ``truncated`` records
    whether that scan hit ``scan_cap`` (lifetime understated — surfaced as a
    WARNING, same contract as VIB-5118). The checkpoint is ephemeral: a gateway
    restart drops it and the next call reseeds via a full scan (the persisted fold
    is VIB-5059 Phase 3).
    """

    state: DrawdownState
    cursor: tuple[datetime, int] | None
    full_scan_at: float
    truncated: bool


# Lexicographic descending keys put the financial ledger truth before timeline duplicates.
_ACTIVITY_FEED_KEY_LEDGER_PRIORITY = "1"
_ACTIVITY_FEED_KEY_TIMELINE_PRIORITY = "0"


class _QuantPositionSummary:
    """Position summary shim parsed off a snapshot's ``positions_json``.

    Feeds the primary-risk gauge in ``compute_pnl_summary`` (LP range /
    lending HF / perp leverage). Extracted from ``_load_quant_inputs``
    verbatim (VIB-5059 — behaviour unchanged; the inline class pushed the
    loader over the complexity budget).
    """

    def __init__(self, snap: Any) -> None:
        self.lp_positions: list[Any] = []
        self.health_factor = None
        self.leverage = None
        # SQLite returns JSON text; hosted JSONB may already be a list, dict, or envelope.
        pjson = getattr(snap, "positions_json", None) or "[]"
        if isinstance(pjson, str):
            try:
                positions = json.loads(pjson)
            except (json.JSONDecodeError, TypeError):
                positions = []
        else:
            positions = pjson
        if isinstance(positions, dict):
            positions = positions.get("positions", [])
        if isinstance(positions, list):
            for p in positions:
                if not isinstance(p, dict):
                    continue
                ptype = (p.get("position_type") or "").upper()
                if ptype == "LP":
                    lp = type("LP", (), {})()
                    # Preserve unknown rather than rendering an unmeasured range as false.
                    raw = p.get("in_range")
                    lp.in_range = None if raw is None else bool(raw)
                    self.lp_positions.append(lp)
                elif ptype == "LENDING":
                    hf = p.get("health_factor")
                    if hf is not None:
                        try:
                            self.health_factor = Decimal(str(hf))
                        except Exception:
                            pass
                elif ptype == "PERP":
                    lev = p.get("leverage")
                    if lev is not None:
                        try:
                            self.leverage = Decimal(str(lev))
                        except Exception:
                            pass


def _folded_inventory_tokens(swap_inventory_stamp: Any) -> frozenset[str]:
    """Case-folded ``"<chain>:<token>"`` keys the writer folded into a leg (VIB-6362).

    Reads ``snapshot_metadata["swap_inventory"]["folded_into_positions"]``.
    Data-shape gate, version-tolerant across writer/reader rollouts: a snapshot
    from a pre-VIB-6362 writer carries no key and yields the empty set, which
    restores the previous behaviour exactly. Non-string members are dropped
    rather than coerced — a malformed stamp must not widen the exclusion and
    silently delete a legitimate inventory term.

    **Known rollout limitation (VIB-6366).** Tolerance is one-way. Gateway
    ahead of the writer is safe: no stamp, empty set, pre-VIB-6362 behaviour.
    Writer ahead of the gateway double-counts folded tokens' mark-to-market for
    the lag window, because an old gateway does not read this key at all — no
    reader-side rule can close that, so it is a deployment-ordering constraint,
    not a code one.
    """
    if not isinstance(swap_inventory_stamp, dict):
        return frozenset()
    folded = swap_inventory_stamp.get("folded_into_positions")
    if not isinstance(folded, list):
        return frozenset()
    return frozenset(t.casefold() for t in folded if isinstance(t, str) and t)


def _index_trade_tape_accounting_events(
    accounting_events: Sequence[Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Index accounting events by ``ledger_entry_id`` and ``cycle_id`` for fast trade-tape join."""
    events_by_ledger: dict[str, dict[str, Any]] = {}
    events_by_cycle: dict[str, list[dict[str, Any]]] = {}
    for ev in accounting_events:
        if not isinstance(ev, dict):
            continue
        le = ev.get("ledger_entry_id")
        if le:
            events_by_ledger[le] = ev
        cy = ev.get("cycle_id")
        if cy:
            events_by_cycle.setdefault(cy, []).append(ev)
    return events_by_ledger, events_by_cycle


def _index_trade_tape_position_events(position_events: Sequence[Any]) -> dict[str, dict[str, Any]]:
    """Index position events by ``ledger_entry_id`` for fast trade-tape join."""
    pos_by_ledger: dict[str, dict[str, Any]] = {}
    for pe in position_events:
        if not isinstance(pe, dict):
            continue
        le = pe.get("ledger_entry_id")
        if le:
            pos_by_ledger[le] = pe
    return pos_by_ledger


def _resolve_trade_tape_row_event(
    entry_id: str,
    cycle_id: str,
    events_by_ledger: dict[str, dict[str, Any]],
    events_by_cycle: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    """Pick the accounting event for a ledger row, with a safe cycle-level fallback.

    The cycle-level fallback only fires when the cycle has *exactly one*
    accounting event. A teardown cycle deliberately writes one event per
    intent (LP_CLOSE, REPAY, swap-back …), and grabbing ``cyc_events[0]``
    could attach another intent's payload, confidence, position key, and
    version stamps to this row. The trade tape is an audit surface — wrong
    joins are worse than empty cells. (Codex audit on PR #2014.)
    """
    row_event = events_by_ledger.get(entry_id)
    if row_event is not None:
        return row_event
    cyc_events = events_by_cycle.get(cycle_id, [])
    if len(cyc_events) == 1:
        return cyc_events[0]
    return None


def _parse_trade_tape_payload_versions(payload_raw: str) -> tuple[str, int, int, int]:
    """Extract ``(unavailable_reason, schema_v, formula_v, matching_v)`` from an accounting payload.

    Non-integer version stamps (e.g. ``"v1"`` from older/corrupt rows) are coerced to 0
    rather than allowed to bubble out as ``ValueError`` — a single malformed payload must
    not turn ``GetTradeTape`` into an INTERNAL gRPC error.
    """
    if not payload_raw:
        return "", 0, 0, 0
    try:
        parsed = json.loads(payload_raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return "", 0, 0, 0
    if not isinstance(parsed, dict):
        return "", 0, 0, 0

    def _safe_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    raw_unavailable_reason = parsed.get("unavailable_reason")
    unavailable_reason = raw_unavailable_reason if isinstance(raw_unavailable_reason, str) else ""
    schema_v = _safe_int(parsed.get("schema_version"))
    formula_v = _safe_int(parsed.get("formula_version"))
    matching_v = _safe_int(parsed.get("matching_policy_version"))
    return unavailable_reason, schema_v, formula_v, matching_v


def _build_trade_tape_position_fields(pe: dict[str, Any] | None) -> tuple[str, str, str]:
    """Return ``(position_event_json, position_id, position_event_type)`` for a row."""
    if pe is None:
        return "", "", ""
    pe_clean = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in pe.items()}
    try:
        position_event_json = json.dumps(pe_clean, default=str)
    except (TypeError, ValueError):
        position_event_json = ""
    position_id = str(pe.get("position_id") or "")
    position_event_type = str(pe.get("event_type") or "")
    return position_event_json, position_id, position_event_type


def _trade_tape_entry_str(entry: Any, name: str) -> str:
    """Read a string-valued ledger attribute, coercing missing/None to empty string."""
    return getattr(entry, name, "") or ""


def _coerce_trade_tape_proto_string(value: Any) -> str:
    """Coerce an accounting-event field to a proto-safe ``str``.

    Strings pass through; ``None`` / missing collapses to ``""``; anything else
    (e.g. JSONB returned as ``dict`` / ``list``, numeric, bool) is JSON-serialised
    so the value survives across the proto boundary instead of crashing the
    string-field assignment in ``_build_trade_tape_row``.
    """
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    try:
        return json.dumps(value, default=str)
    except (TypeError, ValueError):
        return ""


def _build_trade_tape_row(
    entry: Any,
    row_event: dict[str, Any] | None,
    pe: dict[str, Any] | None,
) -> gateway_pb2.TradeTapeRow:
    """Assemble a single ``TradeTapeRow`` proto from a ledger entry + joined events."""
    ts = getattr(entry, "timestamp", None)
    ts_unix = int(ts.timestamp()) if ts else 0

    # Treat persisted event payloads as untrusted at the protobuf boundary.
    payload_raw = _coerce_trade_tape_proto_string(row_event.get("payload_json") if row_event else None)
    confidence = _coerce_trade_tape_proto_string(row_event.get("confidence") if row_event else None)
    event_type = _coerce_trade_tape_proto_string(row_event.get("event_type") if row_event else None)
    position_key = _coerce_trade_tape_proto_string(row_event.get("position_key") if row_event else None)
    unavailable_reason, schema_v, formula_v, matching_v = _parse_trade_tape_payload_versions(payload_raw)
    position_event_json, position_id, position_event_type = _build_trade_tape_position_fields(pe)

    return gateway_pb2.TradeTapeRow(
        id=getattr(entry, "id", ""),
        cycle_id=getattr(entry, "cycle_id", ""),
        timestamp=ts_unix,
        intent_type=getattr(entry, "intent_type", ""),
        token_in=_trade_tape_entry_str(entry, "token_in"),
        amount_in=_trade_tape_entry_str(entry, "amount_in"),
        token_out=_trade_tape_entry_str(entry, "token_out"),
        amount_out=_trade_tape_entry_str(entry, "amount_out"),
        effective_price=_trade_tape_entry_str(entry, "effective_price"),
        slippage_bps=getattr(entry, "slippage_bps", None) or 0.0,
        gas_used=getattr(entry, "gas_used", 0) or 0,
        gas_usd=_trade_tape_entry_str(entry, "gas_usd"),
        tx_hash=_trade_tape_entry_str(entry, "tx_hash"),
        chain=_trade_tape_entry_str(entry, "chain"),
        protocol=_trade_tape_entry_str(entry, "protocol"),
        success=bool(getattr(entry, "success", True)),
        error=_trade_tape_entry_str(entry, "error"),
        amount_in_usd="",
        amount_out_usd="",
        extracted_data_json=_trade_tape_entry_str(entry, "extracted_data_json"),
        price_inputs_json=_trade_tape_entry_str(entry, "price_inputs_json"),
        pre_state_json=_trade_tape_entry_str(entry, "pre_state_json"),
        post_state_json=_trade_tape_entry_str(entry, "post_state_json"),
        accounting_payload_json=payload_raw,
        accounting_event_type=event_type,
        position_key=position_key,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
        schema_version=schema_v,
        formula_version=formula_v,
        matching_policy_version=matching_v,
        position_event_json=position_event_json,
        position_id=position_id,
        position_event_type=position_event_type,
        row_wallet_delta_usd="",
        row_component_usd="",
        row_residual_usd="",
    )


def _to_timeline_feed_item(event: Any, resolved_id: str) -> tuple[gateway_pb2.ActivityFeedItem, str]:
    """Build an ``ActivityFeedItem`` proto for a timeline event + its cursor key."""
    ts_unix = int(event.timestamp.timestamp()) if event.timestamp else 0
    return (
        gateway_pb2.ActivityFeedItem(
            kind=gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT,
            timestamp=ts_unix,
            deployment_id=resolved_id,
            cycle_id=event.cycle_id or "",
            timeline_event=gateway_pb2.TimelineEventInfo(
                timestamp=ts_unix,
                event_type=event.event_type,
                description=event.description,
                tx_hash=event.tx_hash or "",
                details_json=json.dumps(event.details) if event.details else "",
                chain=event.chain or "",
                cycle_id=event.cycle_id or "",
                phase=event.phase or "",
                related_ledger_entry_id=event.related_ledger_entry_id or "",
            ),
        ),
        f"{_ACTIVITY_FEED_KEY_TIMELINE_PRIORITY}:T:{event.event_id}",
    )


def _to_ledger_feed_item(entry: Any) -> tuple[gateway_pb2.ActivityFeedItem, str]:
    """Build an ``ActivityFeedItem`` proto for a ledger entry + its cursor key."""
    ts_unix = int(entry.timestamp.timestamp()) if entry.timestamp else 0
    return (
        gateway_pb2.ActivityFeedItem(
            kind=gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY,
            timestamp=ts_unix,
            deployment_id=entry.deployment_id,
            cycle_id=entry.cycle_id,
            ledger_entry=gateway_pb2.LedgerEntryInfo(
                id=entry.id,
                cycle_id=entry.cycle_id,
                deployment_id=entry.deployment_id,
                timestamp=ts_unix,
                intent_type=entry.intent_type,
                token_in=entry.token_in,
                amount_in=entry.amount_in,
                token_out=entry.token_out,
                amount_out=entry.amount_out,
                effective_price=entry.effective_price,
                slippage_bps=entry.slippage_bps or 0.0,
                gas_used=entry.gas_used,
                gas_usd=entry.gas_usd,
                tx_hash=entry.tx_hash,
                chain=entry.chain,
                protocol=entry.protocol,
                success=entry.success,
                error=entry.error,
            ),
        ),
        f"{_ACTIVITY_FEED_KEY_LEDGER_PRIORITY}:L:{entry.id}",
    )


_PAPER_INACTIVE_FILE_STATUSES = frozenset({"stopped", "stopped_clean", "error", "completed"})

_PAPER_STALE_THRESHOLD_SECONDS = 300

# Downsampling always preserves the latest equity point.
_PAPER_EQUITY_CURVE_MAX_POINTS = 200


def _load_paper_state_file(state_file: Path) -> dict | None:
    """Parse a paper-trader state file, returning ``None`` on read/parse errors.

    ``Path.read_text()`` defaults to UTF-8 and raises ``UnicodeDecodeError``
    (a subclass of ``ValueError`` but NOT of ``OSError``) on a non-UTF-8 file.
    Without that branch, one corrupt-encoding ``.state.json`` would abort
    ``_discover_paper_sessions`` for the whole dashboard request.
    """
    try:
        data = json.loads(state_file.read_text())
    except (json.JSONDecodeError, OSError, UnicodeDecodeError) as e:
        logger.debug(f"Failed to read paper state file {state_file}: {e}")
        return None
    if not isinstance(data, dict):
        logger.debug(f"Paper state file {state_file} is not a JSON object, skipping")
        return None
    return data


def _parse_iso_utc(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp, normalising naive values to UTC."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _is_pid_alive(pid: int) -> bool:
    """Return True if a signal-0 probe to ``pid`` succeeds."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _determine_paper_session_status(data: dict) -> str:
    """Classify a paper session as PAPER_TRADING or INACTIVE.

    A session is INACTIVE when its persisted file_status is terminal, or its
    PID is dead AND its last_save is missing/unparseable/older than the stale
    threshold. Otherwise it's reported as PAPER_TRADING.
    """
    raw_status = data.get("status", "unknown")
    status = raw_status if isinstance(raw_status, str) else "unknown"
    if status in _PAPER_INACTIVE_FILE_STATUSES:
        return "INACTIVE"

    pid = data.get("pid")
    if not (isinstance(pid, int) and pid > 0):
        return "PAPER_TRADING"
    if _is_pid_alive(pid):
        return "PAPER_TRADING"

    last_save = data.get("last_save")
    if not last_save:
        return "INACTIVE"
    last_dt = _parse_iso_utc(last_save)
    if last_dt is None:
        return "INACTIVE"
    age = (datetime.now(UTC) - last_dt).total_seconds()
    return "INACTIVE" if age > _PAPER_STALE_THRESHOLD_SECONDS else "PAPER_TRADING"


def _compute_paper_equity_pnl(equity_curve: list) -> tuple[Decimal, Decimal, Decimal]:
    """Return (initial_value, current_value, simulated_pnl) from an equity curve.

    Returns zeroes when the curve is empty or its endpoints can't be parsed as
    Decimals — PnL without a basis is reported as 0 rather than guessed.
    """
    if not equity_curve:
        return Decimal("0"), Decimal("0"), Decimal("0")
    try:
        initial = Decimal(str(equity_curve[0].get("value", "0")))
        current = Decimal(str(equity_curve[-1].get("value", "0")))
    except (IndexError, AttributeError, ValueError, ArithmeticError):
        return Decimal("0"), Decimal("0"), Decimal("0")
    return initial, current, current - initial


def _sum_paper_gas_cost(trades: list) -> Decimal:
    """Sum gas_cost_usd over trades, skipping malformed entries with a debug log."""
    total = Decimal("0")
    for trade in trades:
        try:
            total += Decimal(str(trade.get("gas_cost_usd", "0")))
        except (ValueError, TypeError, ArithmeticError) as e:
            logger.debug("Skipping malformed gas_cost_usd in trade %s: %s", trade, e)
    return total


def _compute_paper_trades_per_hour(session_start: str, success_count: int) -> Decimal:
    """Compute trades/hour from session_start to now; 0 when not derivable."""
    if not session_start or success_count <= 0:
        return Decimal("0")
    start_dt = _parse_iso_utc(session_start)
    if start_dt is None:
        return Decimal("0")
    hours = Decimal(str((datetime.now(UTC) - start_dt).total_seconds())) / Decimal("3600")
    if hours <= 0:
        return Decimal("0")
    return Decimal(success_count) / hours


def _build_paper_error_breakdown(persisted: Any, errors: list) -> dict:
    """Prefer persisted error_breakdown; otherwise reconstruct from errors list."""
    if isinstance(persisted, dict):
        return persisted
    breakdown: dict = {}
    for error in errors:
        if isinstance(error, dict):
            raw_etype = error.get("error_type", "unknown")
            etype = raw_etype if isinstance(raw_etype, str) and raw_etype else "unknown"
            breakdown[etype] = breakdown.get(etype, 0) + 1
    return breakdown


def _downsample_equity_curve(points: list, max_points: int = _PAPER_EQUITY_CURVE_MAX_POINTS) -> list:
    """Downsample an equity curve to at most ``max_points`` (last point preserved)."""
    if max_points <= 0:
        return []
    if len(points) <= max_points:
        return points
    if max_points == 1:
        return [points[-1]]
    step = len(points) / (max_points - 1)
    return [points[int(i * step)] for i in range(max_points - 1)] + [points[-1]]


def _compute_paper_last_action_ts(last_save: Any) -> int:
    """Return last_save as a unix timestamp, or 0 when missing/unparseable."""
    last_dt = _parse_iso_utc(last_save if isinstance(last_save, str) else None)
    return int(last_dt.timestamp()) if last_dt else 0


def _safe_int(value: Any, default: int = 0) -> int:
    """Coerce ``value`` to a non-negative ``int`` with a fallback default.

    Persisted JSON fields (``tick_count``, ``ticks_with_*``) can arrive as
    strings, ``None``, or other unexpected types from corrupt ``.state.json``
    files. ``int(None)`` raises ``TypeError`` and ``int("garbage")`` raises
    ``ValueError`` — either would abort the entire paper-session discovery.
    """
    try:
        coerced = int(value if value is not None else default)
    except (TypeError, ValueError):
        return default
    return max(0, coerced)


def _build_paper_metrics(
    *,
    data: dict,
    trades: list,
    errors: list,
    equity_curve: list,
    simulated_pnl: Decimal,
) -> dict:
    """Build the ``paper_metrics`` dict embedded as JSON in the session row."""
    tick_count = _safe_int(data.get("tick_count"))
    success_count = len(trades)
    error_count = len(errors)
    last_trade_at = trades[-1].get("timestamp", "") if trades else ""
    session_start = data.get("session_start", "")

    return {
        "tick_count": tick_count,
        "success_count": success_count,
        "hold_count": max(0, tick_count - success_count - error_count),
        "error_count": error_count,
        "simulated_pnl_usd": str(simulated_pnl),
        "total_gas_cost_usd": str(_sum_paper_gas_cost(trades)),
        "last_trade_at": last_trade_at,
        "session_start": session_start,
        "trades_per_hour": str(_compute_paper_trades_per_hour(session_start, success_count)),
        "equity_curve": _downsample_equity_curve(equity_curve),
        "error_breakdown": _build_paper_error_breakdown(data.get("error_breakdown"), errors),
        "ticks_with_fork": _safe_int(data.get("ticks_with_fork")),
        "ticks_with_indicators": _safe_int(data.get("ticks_with_indicators")),
        "ticks_with_action": _safe_int(data.get("ticks_with_action")),
        "anvil_result": data.get("anvil_result"),
    }


def cost_stack_to_proto(cs: Any) -> gateway_pb2.CostStackInfo:
    """Serialise a ``CostStack`` onto the wire (VIB-5942 / VIB-4984 / VIB-6283).

    Module-level and public on purpose: this is the only place the dashboard's
    money crosses the gateway boundary, and while it lived inline inside
    ``GetCostStack`` nothing could test it. That gap is not hypothetical — a
    revision of this function gated the ``fees_earned_usd`` VALUE on the
    all-or-nothing ``fees_earned_measured`` meter, silently deleting real
    measured fee income from the Strategy PnL headline, and the whole unit
    suite plus a 17-mutation harness stayed green because no test and no
    mutation anchor could reach the expression.

    Two orthogonal signals, deliberately not folded together:

    * the ``""`` sentinel  -> the bucket is INAPPLICABLE (no contributing event
      exists at all). The client reads ``None`` and renders "—". Never "$0.00":
      a fabricated zero here contradicted ~$88 of fees genuinely owed on-chain
      on a real fork.
    * ``*_measured``        -> every applicable event supplied its term. False
      with a value present means "this sum is REAL but partial" — show it and
      caption it. Dropping it is what cost the headline its money.
    """
    return gateway_pb2.CostStackInfo(
        cost_gas_usd=str(cs.gas_usd),
        cost_protocol_fees_usd=(str(cs.protocol_fees_usd) if cs.protocol_fees_measured else ""),
        cost_slippage_usd=(str(cs.slippage_usd) if cs.slippage_measured else ""),
        fees_earned_usd=(str(cs.fees_earned_usd) if cs.fees_earned_any_measured else ""),
        interest_paid_usd=str(cs.interest_paid_usd),
        interest_earned_usd=str(cs.interest_earned_usd),
        funding_paid_usd=str(cs.funding_paid_usd),
        funding_earned_usd=str(cs.funding_earned_usd),
        realized_pnl_usd=str(cs.realized_pnl_usd),
        il_usd=(str(cs.il_usd) if cs.il_any_measured else ""),
        # Coverage qualifies a present value; "" alone means not applicable.
        fees_earned_partial=(cs.fees_earned_applicable and not cs.fees_earned_measured),
        il_partial=(cs.il_applicable and not cs.il_measured),
        inventory_unrealized_usd=("" if cs.inventory_unrealized_usd is None else str(cs.inventory_unrealized_usd)),
        # Partial coverage must not erase measured settlement values.
        cost_venue_execution_fee_usd=(str(cs.venue_execution_fee_usd) if cs.venue_execution_fee_any_measured else ""),
        venue_execution_fee_partial=(cs.venue_execution_fee_applicable and not cs.venue_execution_fee_measured),
        cost_perp_settlement_fee_usd=(str(cs.perp_settlement_fee_usd) if cs.perp_settlement_fee_any_measured else ""),
        perp_settlement_fee_partial=(cs.perp_settlement_fee_applicable and not cs.perp_settlement_fee_measured),
    )


class DashboardServiceServicer(gateway_pb2_grpc.DashboardServiceServicer):
    """Implements DashboardService gRPC interface.

    Provides dashboard data access for operator dashboards:
    - ListStrategies: Discover and list available strategies
    - GetStrategyDetails: Get strategy status, position, timeline
    - GetTimeline: Get strategy timeline events
    - GetStrategyConfig: Get strategy configuration
    - GetStrategyState: Get current strategy state
    - ExecuteAction: Enqueue the current lifecycle STOP command
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize DashboardService.

        Args:
            settings: Gateway settings with configuration.
        """
        self.settings = settings
        self._state_manager: StateManager | None = None
        self._initialized = False
        self._strategies_root: Path | None = None
        self._cached_positions: dict[str, list[gateway_pb2.StrategyPosition]] = {}
        self._quant_inputs_cache: dict[str, tuple[float, Any]] = {}
        self._quant_inputs_locks: dict[str, asyncio.Lock] = {}
        # Keep the full-history drawdown scan off cost and audit RPCs.
        self._lifetime_dd_ckpt: dict[str, _LifetimeDrawdownCheckpoint] = {}
        self._lifetime_dd_locks: dict[str, asyncio.Lock] = {}
        self._portfolio_chain: PortfolioProviderChain | None = None

        # Wired after construction to avoid a circular service import.
        self.position_servicer: Any = None
        self._reconciliation_report_cache: Any = None
        self._preview_token_store: Any = None
        # PositionService.Reconcile has no concurrency guard of its own.
        self._registry_refresh_locks: dict[str, Any] = {}

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of dependencies."""
        if self._initialized:
            return

        possible_roots = [
            Path(__file__).parent.parent.parent.parent / "strategies",
            Path.cwd() / "strategies",
            Path(__file__).parent.parent.parent.parent.parent / "strategies",
        ]

        for root in possible_roots:
            if root.exists():
                self._strategies_root = root
                break

        if self._strategies_root is None:
            logger.warning("Strategies directory not found")
            self._strategies_root = Path.cwd() / "strategies"

        try:
            from almanak.framework.state.state_manager import (
                StateManager,
                StateManagerConfig,
                WarmBackendType,
            )

            if self.settings.database_url:
                backend_type = WarmBackendType.POSTGRESQL
                config = StateManagerConfig(
                    warm_backend=backend_type,
                    database_url=self.settings.database_url,
                )
            else:
                backend_type = WarmBackendType.SQLITE
                config = StateManagerConfig(warm_backend=backend_type)

            self._state_manager = StateManager(config)
            await self._state_manager.initialize()
            logger.info(f"DashboardService: StateManager initialized with {backend_type.name}")
        except Exception as e:
            logger.warning(f"DashboardService: Failed to initialize StateManager: {e}")
            self._state_manager = None

        try:
            self._portfolio_chain = build_portfolio_chain(
                portfolio_providers_csv=self.settings.portfolio_providers,
                portfolio_api_key=self.settings.portfolio_api_key,
                portfolio_api_provider=self.settings.portfolio_api_provider,
                portfolio_api_cache_ttl=self.settings.portfolio_api_cache_ttl,
                settings=self.settings,
            )
        except Exception as e:
            logger.warning(f"DashboardService: Failed to initialize portfolio providers: {e}")
            self._portfolio_chain = None

        self._initialized = True
        logger.info(f"DashboardService initialized (strategies_root={self._strategies_root})")

    def _discover_strategies_from_filesystem(self) -> list[dict]:
        """Discover strategies from the strategies/ directory.

        Returns:
            List of strategy info dicts from config.json files
        """
        strategies: list[dict] = []

        if self._strategies_root is None or not self._strategies_root.exists():
            return strategies

        for category in STRATEGY_CATEGORIES:
            category_dir = self._strategies_root / category
            if not category_dir.exists():
                continue

            for strategy_dir in category_dir.iterdir():
                if not strategy_dir.is_dir():
                    continue

                config_file = strategy_dir / "config.json"
                if not config_file.exists():
                    continue

                try:
                    config = json.loads(config_file.read_text())
                    deployment_id = config.get("deployment_id", strategy_dir.name)
                    strategy_name = config.get("strategy_name", strategy_dir.name)

                    display_name = strategy_name.replace("_", " ").title()
                    if category != "demo":
                        display_name += f" ({category.title()})"

                    raw_chain = config.get("chain")
                    chain = raw_chain if isinstance(raw_chain, str) and raw_chain else LEGACY_SERIALIZED_CHAIN
                    protocol = self._derive_protocol_from_config(config, deployment_id)

                    strategies.append(
                        {
                            "deployment_id": deployment_id,
                            "name": display_name,
                            "status": "PAUSED",
                            "chain": chain,
                            "protocol": protocol,
                            "total_value_usd": "0",
                            "pnl_24h_usd": "0",
                            "last_action_at": 0,
                            "attention_required": False,
                            "attention_reason": "",
                            "is_multi_chain": "," in str(chain),
                            "chains": [c.strip() for c in str(chain).split(",")],
                            "config_path": str(config_file),
                            "category": category,
                            "consecutive_errors": 0,
                            "last_iteration_at": 0,
                            "pnl_since_deploy_usd": "",
                        }
                    )
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Failed to load strategy config from {config_file}: {e}")
                    continue

        return strategies

    def _discover_paper_sessions(self) -> list[dict]:
        """Discover paper trading sessions from ~/.almanak/paper/.

        Reads state files produced by the BackgroundPaperTrader to surface
        paper sessions alongside live strategies in the dashboard.

        Returns:
            List of strategy info dicts for paper sessions.
        """
        paper_dir = Path.home() / ".almanak" / "paper"
        if not paper_dir.exists():
            return []

        sessions: list[dict] = []
        for state_file in paper_dir.glob("*.state.json"):
            data = _load_paper_state_file(state_file)
            if data is None:
                continue
            sessions.append(self._build_paper_session(state_file, data))
        return sessions

    def _build_paper_session(self, state_file: Path, data: dict) -> dict:
        """Assemble a single paper-session info dict from a parsed state file."""
        raw_deployment_id = data.get("deployment_id")
        deployment_id = (
            raw_deployment_id
            if isinstance(raw_deployment_id, str) and raw_deployment_id
            else state_file.stem.replace(".state", "")
        )
        raw_config = data.get("config")
        config: dict = raw_config if isinstance(raw_config, dict) else {}

        status = _determine_paper_session_status(data)
        # Paper state is untrusted input; keep malformed values out of protobuf fields.
        raw_chain = config.get("chain")
        chain = raw_chain if isinstance(raw_chain, str) and raw_chain else LEGACY_SERIALIZED_CHAIN
        raw_protocol = config.get("protocol")
        protocol = (
            raw_protocol
            if isinstance(raw_protocol, str) and raw_protocol
            else self._derive_protocol_from_config(config, deployment_id)
        )

        raw_trades = data.get("trades")
        trades: list = [t for t in raw_trades if isinstance(t, dict)] if isinstance(raw_trades, list) else []
        raw_errors = data.get("errors")
        errors: list = [e for e in raw_errors if isinstance(e, dict)] if isinstance(raw_errors, list) else []
        raw_equity_curve = data.get("equity_curve")
        equity_curve: list = (
            [p for p in raw_equity_curve if isinstance(p, dict)] if isinstance(raw_equity_curve, list) else []
        )

        # Equity endpoints include open-position mark-to-market; trade deltas do not.
        _, current_value, simulated_pnl = _compute_paper_equity_pnl(equity_curve)

        paper_metrics = _build_paper_metrics(
            data=data,
            trades=trades,
            errors=errors,
            equity_curve=equity_curve,
            simulated_pnl=simulated_pnl,
        )

        return {
            "deployment_id": f"paper:{deployment_id}",
            "name": deployment_id.replace("_", " ").title() + " (Paper)",
            "status": status,
            "chain": chain,
            "protocol": protocol,
            "total_value_usd": str(current_value) if current_value else "0",
            # Paper PnL is isolated in paper_metrics_json from live 24-hour totals.
            "pnl_24h_usd": "0",
            "last_action_at": _compute_paper_last_action_ts(data.get("last_save")),
            "attention_required": status == "INACTIVE",
            "attention_reason": "Paper session inactive" if status == "INACTIVE" else "",
            "is_multi_chain": "," in str(chain),
            "chains": [c.strip() for c in str(chain).split(",")],
            "execution_mode": "paper",
            "paper_metrics_json": json.dumps(paper_metrics),
        }

    def _derive_protocol_from_config(self, config: dict, _deployment_id: str) -> str:
        """Derive protocol string from config.

        Honours an explicit ``config["protocol"]`` string only. The previous
        implementation also substring-sniffed the caller-supplied
        ``_deployment_id`` against a hard-coded ladder of 14 protocol
        keywords ("uniswap", "aave", "gmx", …) — a user-controlled string
        used as a routing key is a real bug: any deployment id containing
        one of those substrings would be misclassified on the dashboard,
        and the ladder bypasses the connector-self-containment program's
        canonical ``ProtocolName`` registry (VIB-4810).

        ``_deployment_id`` is intentionally ignored (underscore-prefixed
        so static analysis doesn't flag the unused argument). When no
        explicit ``config["protocol"]`` is present, return ``"Unknown"``
        and let the connector registry (Phase 3) carry the protocol
        identity.
        """
        explicit = config.get("protocol")
        if isinstance(explicit, str) and explicit:
            return explicit

        return "Unknown"

    async def _get_strategy_state_data(self, deployment_id: str) -> dict | None:
        """Get strategy state from StateManager.

        Per blueprint 29 §4 the gateway filters the caller-supplied
        ``deployment_id`` directly — there is no identity translation and
        no fallback key. A zero-row read means the deployment genuinely has
        no state.

        Args:
            deployment_id: The canonical deployment_id to look up.

        Returns:
            State dict or None if not found
        """
        if self._state_manager is None:
            return None

        try:
            state = await self._state_manager.load_state(deployment_id)
            if state is not None:
                return state.state
        except Exception as e:
            logger.debug(f"Failed to load state for {deployment_id}: {e}")

        return None

    async def _get_portfolio_value_and_pnl(
        self,
        deployment_id: str,
    ) -> tuple[str, str]:
        """Get portfolio total value and PnL.

        Two-level read path (simplified from the former 6-level cascade):
        1. PortfolioMetrics (framework-owned, populated by PortfolioValuer)
        2. Fresh latest snapshot (grace period for newly-started strategies)

        If neither source has data, returns ("0", "0") — explicitly meaning
        "no data yet" rather than masking a write-side bug with stale or
        external fallbacks.

        Returns:
            Tuple of (total_value_usd, pnl_usd) as strings.
        """
        # A metrics row with an unmeasured NAV is not a measured zero or an authoritative value.
        if self._state_manager is not None:
            try:
                metrics = await self._state_manager.get_portfolio_metrics(deployment_id)
                if metrics is not None and metrics.total_value_usd is not None:
                    pnl_24h = await self._compute_pnl_24h(deployment_id, metrics.total_value_usd)
                    return str(metrics.total_value_usd), str(pnl_24h)
            except Exception:
                logger.debug("Failed to get portfolio metrics for %s", deployment_id, exc_info=True)

        # Fresh snapshots bridge startup before PortfolioMetrics exists.
        latest_snapshot = await self._get_latest_snapshot(deployment_id)
        if latest_snapshot is not None and self._snapshot_is_fresh(latest_snapshot):
            return str(latest_snapshot.total_value_usd), "0"

        logger.info(
            "No portfolio data available for %s — neither metrics nor a fresh snapshot exist. "
            "The dashboard will show $0 until the strategy's PortfolioValuer writes data.",
            deployment_id,
        )
        return "0", "0"

    async def _compute_pnl_24h(self, deployment_id: str, current_value: Decimal) -> Decimal:
        """Compute PnL over a 24-hour window using snapshot history.

        Falls back to lifetime PnL if strategy has been running < 24h.

        Note: Both paths report PnL net of gas — in the 24h path, gas is
        implicitly captured because total_value_usd on snapshots already
        reflects the lower wallet balance after gas expenditure. The fallback
        path uses the same implicit approach: current_value already accounts
        for gas spent. Neither path adjusts for capital flows (deposits/
        withdrawals), which are rare for SDK strategies.
        """
        if self._state_manager is None or current_value <= 0:
            return Decimal("0")

        try:
            target_time = datetime.now(UTC) - timedelta(hours=24)
            snapshot_24h = await self._state_manager.get_snapshot_at(deployment_id, target_time)

            if snapshot_24h is not None and snapshot_24h.total_value_usd > 0:
                return current_value - snapshot_24h.total_value_usd

            # Current wallet value already includes gas expenditure.
            metrics = await self._state_manager.get_portfolio_metrics(deployment_id)
            if metrics is not None and metrics.initial_value_usd > 0:
                return current_value - metrics.initial_value_usd

        except Exception:
            logger.debug("Failed to compute PnL 24h for %s", deployment_id, exc_info=True)

        return Decimal("0")

    # crap-allowlist: VIB-4722 only renamed identity plumbing inside existing dashboard history logic.
    @staticmethod
    def _unix_to_dt(ts: int) -> datetime | None:
        """Map a unix-seconds window bound to a UTC datetime (``<= 0`` → open bound).

        Out-of-range epoch values (year > 9999, platform bounds) raise
        ``ValueError`` so the caller maps them to ``INVALID_ARGUMENT`` rather than
        crashing the RPC — same contract as ``GetTradeTape``'s cursor parsing.
        """
        if ts <= 0:
            return None
        try:
            return datetime.fromtimestamp(ts, tz=UTC)
        except (OverflowError, OSError, ValueError) as e:
            raise ValueError(f"timestamp out of range: {ts}") from e

    async def _build_pnl_history(
        self,
        deployment_id: str,
        *,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        max_points: int = 0,
    ) -> list:
        """Build the PnL/NAV time series for chart rendering.

        ``max_points > 0`` selects **windowed mode** (VIB-5059 Phase 2):
        server-side decimation of an arbitrary ``[from_dt, to_dt]`` window down to
        the point budget, preserving spikes — and **loud** on backend failure (the
        operator explicitly asked to go back in time, so an empty-but-OK series
        would be a lie). ``max_points <= 0`` keeps the legacy recent-window path,
        which degrades gracefully for the default dashboard render. The mode split
        keeps every existing caller byte-for-byte unchanged.
        """
        if max_points > 0:
            return await self._build_pnl_history_windowed(deployment_id, from_dt, to_dt, max_points)
        return await self._build_pnl_history_recent(deployment_id)

    async def _build_pnl_history_recent(self, deployment_id: str) -> list:
        """Legacy recent-window PnL series (oldest-first, up to 168 snapshots).

        VIB-5026: previously used ``get_snapshots_since(now-7d, limit=168)``,
        which — once a deployment had >168 snapshots — returned the OLDEST 168
        rows, freezing the chart on the strategy's first ~14h. ``get_recent_snapshots``
        returns the latest window. Backend errors degrade to an empty series (the
        default render has other panels); the windowed path is the loud one.
        """
        from almanak.framework.valuation.net_debt import net_debt_from_snapshot, wallet_nav_usd
        from almanak.gateway.proto import gateway_pb2

        pnl_points: list[gateway_pb2.PnLDataPoint] = []
        if self._state_manager is None:
            return pnl_points

        try:
            snapshots = await self._state_manager.get_recent_snapshots(deployment_id, limit=168)
            if not snapshots:
                return pnl_points
            metrics = await self._state_manager.get_portfolio_metrics(deployment_id)
            initial_value = metrics.initial_value_usd if metrics else Decimal("0")
            for snap in snapshots:
                ts = snap.timestamp
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)
                # Missing or non-finite NAV components are unmeasured, not zero.
                if snap.total_value_usd is None or snap.available_cash_usd is None:
                    continue
                if not snap.total_value_usd.is_finite() or not snap.available_cash_usd.is_finite():
                    continue
                # Chart, headline, and drawdown share wallet NAV: total - debt + idle cash.
                _count, debt_mark, _debt_cost, _net_cost = net_debt_from_snapshot(snap)
                net_value = wallet_nav_usd(snap.total_value_usd, debt_mark, snap.available_cash_usd)
                pnl = net_value - initial_value if initial_value > 0 else Decimal("0")
                pnl_points.append(
                    gateway_pb2.PnLDataPoint(
                        timestamp=int(ts.timestamp()),
                        value_usd=str(net_value),
                        pnl_usd=str(pnl),
                    )
                )
        except Exception:
            logger.debug("Failed to build PnL history for %s", deployment_id, exc_info=True)

        return pnl_points

    async def _build_pnl_history_windowed(
        self,
        deployment_id: str,
        from_dt: datetime | None,
        to_dt: datetime | None,
        max_points: int,
    ) -> list:
        """Windowed, decimated NAV series (VIB-5059 Phase 2) — raises on backend error.

        Fetches the projected NAV samples in ``[from_dt, to_dt]`` (newest scan-cap
        slice + a ``truncated`` flag), drops Empty≠Zero unmeasured samples loudly
        (never coerced to ``$0``), decimates to ``max_points`` preserving per-bucket
        min/max/last, and stamps the chart endpoints from the window anchors. Any
        backend failure propagates so :meth:`GetStrategyDetails` maps it to a non-OK
        status rather than returning an OK response with empty history.
        """
        from almanak.framework.dashboard.chart_window import (
            NavPoint,
            clamp_max_points,
            decimate_nav,
        )
        from almanak.gateway.metrics import DASHBOARD_NAV_HISTORY_TRUNCATED
        from almanak.gateway.proto import gateway_pb2

        if self._state_manager is None:
            # Explicit historical requests fail loudly rather than claiming empty history.
            raise RuntimeError("StateManager unavailable for windowed PnL history")

        rows, truncated = await self._state_manager.get_snapshots_in_window(deployment_id, from_dt, to_dt)

        from almanak.framework.valuation.net_debt import net_debt_from_positions_json, wallet_nav_usd

        points: list[NavPoint] = []
        dropped = 0
        for ts, value_text, cash_text, _confidence, positions_text in rows:
            # Explicit checks preserve measured "0" while dropping unmeasured components.
            if value_text is None or value_text == "" or cash_text is None or cash_text == "":
                dropped += 1
                continue
            try:
                value = Decimal(value_text)
                cash = Decimal(cash_text)
            except (InvalidOperation, ValueError, TypeError):
                dropped += 1
                continue
            if not value.is_finite() or not cash.is_finite():
                dropped += 1
                continue
            _count, debt_mark, _debt_cost = net_debt_from_positions_json(positions_text)
            value = wallet_nav_usd(value, debt_mark, cash)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            points.append(NavPoint(ts, value))

        if truncated:
            DASHBOARD_NAV_HISTORY_TRUNCATED.labels(deployment_id=deployment_id).inc()
            logger.warning(
                "NAV history truncated at scan cap for %s (window=[%s, %s]); returning newest in-window slice",
                deployment_id,
                from_dt,
                to_dt,
            )
        if dropped:
            logger.warning(
                "NAV history dropped %d unmeasured (Empty!=Zero) sample(s) for %s; excluded, not coerced to 0",
                dropped,
                deployment_id,
            )

        decimated = decimate_nav(points, clamp_max_points(max_points))

        metrics = await self._state_manager.get_portfolio_metrics(deployment_id)
        initial_value = metrics.initial_value_usd if metrics else Decimal("0")
        return [
            gateway_pb2.PnLDataPoint(
                timestamp=int(point.timestamp.timestamp()),
                value_usd=str(point.value),
                pnl_usd=str(point.value - initial_value if initial_value > 0 else Decimal("0")),
            )
            for point in decimated
        ]

    async def _get_latest_snapshot(self, deployment_id: str) -> PortfolioSnapshot | None:
        """Get the most recent portfolio snapshot for staleness checks."""
        if self._state_manager is None:
            return None
        try:
            return await self._state_manager.get_latest_snapshot(deployment_id)
        except Exception:
            logger.debug("Failed to get latest snapshot for %s", deployment_id, exc_info=True)
            return None

    async def _get_first_snapshot(self, deployment_id: str) -> PortfolioSnapshot | None:
        """Get the earliest portfolio snapshot for lifetime reconciliation."""
        if self._state_manager is None:
            return None
        try:
            return await self._state_manager.get_first_snapshot(deployment_id)
        except Exception:
            logger.debug("Failed to get first snapshot for %s", deployment_id, exc_info=True)
            return None

    @staticmethod
    def _snapshot_for_inventory_revaluation(snapshot: PortfolioSnapshot | None) -> dict[str, Any] | None:
        """Serialize a typed snapshot into the persisted-column shape G6 reads.

        ``compute_inventory_revaluation`` deliberately consumes the same JSON
        columns the Accountant Test reads from SQLite/Postgres rows. Hosted
        dashboard callers receive typed ``PortfolioSnapshot`` instances from the
        StateManager facade, so convert only the persisted wallet/price payloads
        needed for that read-only calculation. Empty lists/maps stay explicit
        empty JSON, not fabricated zero values.
        """
        if snapshot is None:
            return None
        wallet_balances = []
        for balance in getattr(snapshot, "wallet_balances", []) or []:
            price_usd = getattr(balance, "price_usd", None)
            wallet_balances.append(
                {
                    "symbol": str(getattr(balance, "symbol", "") or ""),
                    "balance": str(getattr(balance, "balance", "") or ""),
                    "value_usd": str(getattr(balance, "value_usd", "") or ""),
                    "address": str(getattr(balance, "address", "") or ""),
                    "price_usd": None if price_usd is None else str(price_usd),
                }
            )
        return {"wallet_balances_json": json.dumps(wallet_balances)}

    async def _get_reconciliation_endpoint_snapshots(
        self,
        deployment_id: str,
        recent_snapshots: list[PortfolioSnapshot],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Return lifetime initial/final snapshots for G6 inventory revaluation."""
        first = await self._get_first_snapshot(deployment_id)
        latest = recent_snapshots[-1] if recent_snapshots else await self._get_latest_snapshot(deployment_id)
        return (
            self._snapshot_for_inventory_revaluation(first),
            self._snapshot_for_inventory_revaluation(latest),
        )

    @staticmethod
    def _snapshot_is_fresh(
        snapshot: PortfolioSnapshot | None,
        stale_threshold_seconds: int = PORTFOLIO_STALE_THRESHOLD_SECONDS,
    ) -> bool:
        """Return True when a snapshot is recent enough to trust directly."""
        if snapshot is None:
            return False
        timestamp = snapshot.timestamp
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        age = (datetime.now(UTC) - timestamp).total_seconds()
        return age <= stale_threshold_seconds

    async def _get_portfolio_metrics(self, deployment_id: str) -> Decimal | None:
        """Return pnl_after_gas for a strategy, or None if unavailable."""
        if self._state_manager is None:
            return None
        try:
            metrics = await self._state_manager.get_portfolio_metrics(deployment_id)
            if metrics is None:
                return None
            return metrics.pnl_after_gas
        except Exception:
            return None

    def _compute_effective_status(self, instance: Any, stale_threshold_seconds: int = 300) -> str:
        """Compute effective status for a registered instance.

        If an instance reports RUNNING but hasn't heartbeated within the threshold,
        its effective status is STALE (likely crashed).

        Args:
            instance: A StrategyInstance from the registry.
            stale_threshold_seconds: Seconds without heartbeat before marking STALE.

        Returns:
            Effective status string.
        """
        if instance.status == "RUNNING" and instance.last_heartbeat_at is not None:
            heartbeat = instance.last_heartbeat_at
            if heartbeat.tzinfo is None:
                heartbeat = heartbeat.replace(tzinfo=UTC)
            age = (datetime.now(UTC) - heartbeat).total_seconds()
            if age > stale_threshold_seconds:
                return "STALE"
        return instance.status

    _SOURCE_FILTERS = frozenset({"REGISTRY", "AVAILABLE", "ALL"})
    _STATUS_FILTERS = frozenset(
        {"RUNNING", "PAUSED", "ERROR", "STUCK", "STALE", "INACTIVE", "ARCHIVED", "PAPER_TRADING"}
    )
    _VALID_FILTERS = _SOURCE_FILTERS | _STATUS_FILTERS

    @staticmethod
    def _canonical_template_id(deployment_id: str) -> str:
        """Extract canonical template ID from a strategy instance ID.

        Instance IDs use the format ``"template_name:uuid_suffix"`` for
        continuous runs, or plain ``"template_name"`` for ``--once`` runs.
        This returns the part before the first colon.
        """
        return deployment_id.split(":")[0]

    async def ListStrategies(
        self,
        request: gateway_pb2.ListStrategiesRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.ListStrategiesResponse:
        """List strategies with summary info.

        Uses ``status_filter`` to control data source:

        Source modes:
        - ``REGISTRY`` (default): Only instances from the instance registry
          (executed/running strategies). Used by the Command Center page.
        - ``AVAILABLE``: Only templates from filesystem discovery, excluding
          templates that already have a non-archived instance in the registry.
          Used by the Strategy Library page.
        - ``ALL``: Registry instances combined with filesystem templates
          (deduplicated). Useful for API consumers that want both.

        Status modes (applied on top of registry results):
        - ``RUNNING``, ``PAUSED``, ``ERROR``, ``STUCK``, ``STALE``,
          ``INACTIVE``, ``ARCHIVED``: Filter registry instances by status.

        Args:
            request: List request with optional filters
            context: gRPC context

        Returns:
            ListStrategiesResponse with strategy summaries
        """
        await self._ensure_initialized()

        status_filter = request.status_filter.upper() if request.status_filter else "REGISTRY"
        chain_filter = request.chain_filter.lower() if request.chain_filter else ""

        if status_filter not in self._VALID_FILTERS:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Unknown status_filter '{request.status_filter}'. "
                f"Valid values: {', '.join(sorted(self._VALID_FILTERS))}",
            )
            return gateway_pb2.ListStrategiesResponse()

        strategies: list[dict] = []

        include_registry = status_filter != "AVAILABLE"
        registry_template_ids: set[str] = set()

        if include_registry or status_filter in ("AVAILABLE", "ALL"):
            try:
                registry = get_instance_registry()
                registered = registry.list_all(
                    include_archived=(status_filter == "ARCHIVED"),
                )

                for inst in registered:
                    # Hosted deployment IDs may differ from the filesystem template identity.
                    template_key = inst.strategy_name or self._canonical_template_id(inst.deployment_id)
                    registry_template_ids.add(template_key)

                    if include_registry:
                        effective_status = self._compute_effective_status(inst)
                        strategy_info = build_registry_strategy_info(inst, effective_status)

                        state = await self._get_strategy_state_data(inst.deployment_id)
                        total_value, pnl = await self._get_portfolio_value_and_pnl(
                            inst.deployment_id,
                        )
                        pnl_metrics = await self._get_portfolio_metrics(inst.deployment_id)
                        enrich_strategy_info(
                            strategy_info,
                            state=state,
                            total_value=total_value,
                            pnl=pnl,
                            pnl_metrics=pnl_metrics,
                            preserve_status_precedence=False,
                        )

                        strategies.append(strategy_info)

            except Exception as e:
                logger.debug(f"Failed to get instances from registry: {e}")

        if status_filter in ("AVAILABLE", "ALL"):
            for fs_strategy in self._discover_strategies_from_filesystem():
                template_id = self._canonical_template_id(fs_strategy["deployment_id"])
                if template_id in registry_template_ids:
                    continue
                strategies.append(fs_strategy)

        if status_filter not in ("AVAILABLE",):
            for paper_session in self._discover_paper_sessions():
                strategies.append(paper_session)

        # Filter only after merging registry and paper sources.
        if status_filter in self._STATUS_FILTERS:
            strategies = [s for s in strategies if s["status"] == status_filter]

        filtered = []
        for s in strategies:
            if chain_filter and chain_filter not in s["chain"].lower():
                continue
            filtered.append(s)

        summaries = [gateway_pb2.StrategySummary(**build_strategy_summary_kwargs(s)) for s in filtered]

        return gateway_pb2.ListStrategiesResponse(
            strategies=summaries,
            total_count=len(summaries),
        )

    async def GetStrategyDetails(
        self,
        request: gateway_pb2.GetStrategyDetailsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.StrategyDetails:
        """Get detailed information about a specific strategy.

        Args:
            request: Details request with deployment_id
            context: gRPC context

        Returns:
            StrategyDetails with summary, position, timeline, etc.
        """
        await self._ensure_initialized()

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.StrategyDetails()

        # The validated deployment ID is the storage and registry identity; never translate it here.
        strategy_info = lookup_strategy_source(
            deployment_id=deployment_id,
            registry_getter=get_instance_registry,
            compute_effective_status=self._compute_effective_status,
            discover_filesystem=self._discover_strategies_from_filesystem,
            discover_paper_sessions=self._discover_paper_sessions,
        )

        state = await self._get_strategy_state_data(deployment_id)
        try:
            latest_snap = await self._get_latest_snapshot(deployment_id)
        except Exception:
            logger.debug("Failed to get snapshot balances for %s", deployment_id, exc_info=True)
            latest_snap = None

        if strategy_info is None:
            # Hosted dashboard pods reconstruct remote strategies from shared state; unknown IDs still fail.
            strategy_info = build_state_only_strategy_info(deployment_id, state, latest_snap)

        if strategy_info is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Strategy not found: {deployment_id}")
            return gateway_pb2.StrategyDetails()

        total_value, pnl = await self._get_portfolio_value_and_pnl(deployment_id)
        pnl_metrics = await self._get_portfolio_metrics(deployment_id)
        enrich_strategy_info(
            strategy_info,
            state=state,
            total_value=total_value,
            pnl=pnl,
            pnl_metrics=pnl_metrics,
            preserve_status_precedence=True,
        )

        summary = gateway_pb2.StrategySummary(**build_strategy_summary_kwargs(strategy_info))

        # Snapshot valuation wins over the state-dict fallback.
        position = build_position_proto(
            state=state,
            cached_positions=self._cached_positions.get(deployment_id),
            snapshot=latest_snap,
        )

        timeline = []
        if request.include_timeline:
            limit = request.timeline_limit if request.timeline_limit > 0 else 20
            timeline_response = await self.GetTimeline(
                gateway_pb2.GetTimelineRequest(deployment_id=deployment_id, limit=limit),
                context,
            )
            timeline = list(timeline_response.events)

        pnl_history = []
        if request.include_pnl_history:
            if request.max_points > 0:
                from almanak.framework.dashboard.chart_window import validate_window

                try:
                    validate_window(request.from_ts, request.to_ts)
                    from_dt = self._unix_to_dt(request.from_ts)
                    to_dt = self._unix_to_dt(request.to_ts)
                except ValueError as e:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(str(e))
                    return gateway_pb2.StrategyDetails()
                try:
                    pnl_history = await self._build_pnl_history(
                        deployment_id, from_dt=from_dt, to_dt=to_dt, max_points=request.max_points
                    )
                except Exception:
                    logger.exception("Windowed PnL history failed for %s", deployment_id)
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    context.set_details("Failed to load windowed PnL history")
                    return gateway_pb2.StrategyDetails()
            else:
                pnl_history = await self._build_pnl_history(deployment_id)

        # Strings are sequences but never valid chain collections.
        raw_chains = strategy_info.get("chains")
        if isinstance(raw_chains, Sequence) and not isinstance(raw_chains, str | bytes):
            chains: list[str] = [str(c) for c in raw_chains]
        else:
            if raw_chains is not None:
                logger.warning(
                    "Unexpected chains type %s for strategy %s; coercing to empty list",
                    type(raw_chains).__name__,
                    strategy_info.get("deployment_id", "<unknown>"),
                )
            chains = []
        chain_health = build_chain_health(chains)

        return gateway_pb2.StrategyDetails(
            summary=summary,
            position=position,
            timeline=timeline,
            pnl_history=pnl_history,
            chain_health=chain_health,
        )

    # crap-allowlist: pre-existing RPC. PR2 (VIB-4041) only added a one-line
    # ``related_ledger_entry_id`` field to the per-event proto construction;
    # CC=26 was already over-threshold against unit-test coverage. The full
    # surface is exercised by integration tests in
    # ``tests/gateway/test_timeline_store.py`` + the round-trip test in
    # ``tests/gateway/test_timeline_related_ledger.py``.
    async def GetTimeline(
        self,
        request: gateway_pb2.GetTimelineRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetTimelineResponse:
        """Get timeline events for a strategy.

        Args:
            request: Timeline request with deployment_id, limit, filters
            context: gRPC context

        Returns:
            GetTimelineResponse with timeline events
        """
        await self._ensure_initialized()

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetTimelineResponse()

        limit = request.limit if request.limit > 0 else 50
        event_type_filter = request.event_type_filter if request.event_type_filter else None
        since = datetime.fromtimestamp(request.since_timestamp, tz=UTC) if request.since_timestamp > 0 else None

        events = []

        try:
            store = get_timeline_store()
            timeline_events = store.get_events(
                deployment_id=deployment_id,
                limit=limit,
                event_type=event_type_filter,
                since=since,
            )

            for event in timeline_events:
                events.append(
                    gateway_pb2.TimelineEventInfo(
                        timestamp=int(event.timestamp.timestamp()) if event.timestamp else 0,
                        event_type=event.event_type,
                        description=event.description,
                        tx_hash=event.tx_hash or "",
                        details_json=json.dumps(event.details) if event.details else "",
                        chain=event.chain or "",
                        cycle_id=event.cycle_id or "",
                        phase=event.phase or "",
                        related_ledger_entry_id=event.related_ledger_entry_id or "",
                    )
                )
        except Exception as e:
            logger.debug(f"Failed to get events from TimelineStore: {e}")

        # Local cache and state history remain fallbacks when the timeline store is empty.
        if not events:
            cache_file = self._strategies_root.parent / ".dashboard_events.json" if self._strategies_root else None
            if cache_file and cache_file.exists():
                try:
                    cached_data = json.loads(cache_file.read_text())
                    strategy_events = cached_data.get(deployment_id, [])

                    for event_data in strategy_events[:limit]:
                        events.append(
                            gateway_pb2.TimelineEventInfo(
                                timestamp=int(datetime.fromisoformat(event_data.get("timestamp", "")).timestamp())
                                if event_data.get("timestamp")
                                else 0,
                                event_type=event_data.get("event_type", "UNKNOWN"),
                                description=event_data.get("description", ""),
                                tx_hash=event_data.get("tx_hash", ""),
                                details_json=json.dumps(event_data.get("details", {})),
                                chain=event_data.get("chain", ""),
                                cycle_id=event_data.get("cycle_id", ""),
                                phase=event_data.get("phase", ""),
                                related_ledger_entry_id=event_data.get("related_ledger_entry_id", ""),
                            )
                        )
                except Exception as e:
                    logger.debug(f"Failed to load timeline events from cache: {e}")

        state = await self._get_strategy_state_data(deployment_id)
        if state and "execution_history" in state:
            for exec_record in state.get("execution_history", [])[:limit]:
                if isinstance(exec_record, dict):
                    events.append(
                        gateway_pb2.TimelineEventInfo(
                            timestamp=int(datetime.fromisoformat(exec_record.get("timestamp", "")).timestamp())
                            if exec_record.get("timestamp")
                            else 0,
                            event_type=exec_record.get("event_type", "EXECUTION"),
                            description=exec_record.get("description", "Execution completed"),
                            tx_hash=exec_record.get("tx_hash", ""),
                            details_json=json.dumps(exec_record.get("details", {})),
                            chain=exec_record.get("chain", ""),
                        )
                    )

        events.sort(key=lambda e: e.timestamp, reverse=True)
        events = events[:limit]

        return gateway_pb2.GetTimelineResponse(
            events=events,
            has_more=len(events) >= limit,
        )

    async def GetStrategyConfig(
        self,
        request: gateway_pb2.GetStrategyConfigRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.StrategyConfigResponse:
        """Get strategy configuration.

        Args:
            request: Config request with deployment_id
            context: gRPC context

        Returns:
            StrategyConfigResponse with config JSON
        """
        await self._ensure_initialized()

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.StrategyConfigResponse()

        # Local gateways read files; hosted gateways fall back to registration-time config.
        if self._strategies_root is not None:
            for category in STRATEGY_CATEGORIES:
                config_file = self._strategies_root / category / deployment_id / "config.json"
                if config_file.exists():
                    try:
                        config = json.loads(config_file.read_text())
                        return gateway_pb2.StrategyConfigResponse(
                            deployment_id=deployment_id,
                            strategy_name=config.get("strategy_name", deployment_id),
                            config_json=json.dumps(config),
                            last_updated=int(config_file.stat().st_mtime),
                        )
                    except Exception as e:
                        logger.error(f"Failed to read config file for {deployment_id}: {e}")
                        context.set_code(grpc.StatusCode.INTERNAL)
                        context.set_details("Failed to read strategy config")
                        return gateway_pb2.StrategyConfigResponse()

        try:
            registry = get_instance_registry()
            inst = registry.get(deployment_id)
        except Exception as e:
            logger.error(f"Failed to get config from registry for {deployment_id}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to read strategy config")
            return gateway_pb2.StrategyConfigResponse()

        if inst is not None and inst.config_json:
            try:
                config = json.loads(inst.config_json)
            except json.JSONDecodeError:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Stored config is invalid JSON")
                return gateway_pb2.StrategyConfigResponse()
            return gateway_pb2.StrategyConfigResponse(
                deployment_id=deployment_id,
                strategy_name=config.get("strategy_name", inst.strategy_name),
                config_json=inst.config_json,
                last_updated=int(inst.updated_at.timestamp()) if inst.updated_at else 0,
            )

        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Config not found for strategy: {deployment_id}")
        return gateway_pb2.StrategyConfigResponse()

    async def GetStrategyState(
        self,
        request: gateway_pb2.GetStrategyStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.StrategyStateResponse:
        """Get current strategy state.

        Args:
            request: State request with deployment_id and optional field filter
            context: gRPC context

        Returns:
            StrategyStateResponse with state JSON
        """
        await self._ensure_initialized()

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.StrategyStateResponse()

        state = await self._get_strategy_state_data(deployment_id)
        if state is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"State not found for strategy: {deployment_id}")
            return gateway_pb2.StrategyStateResponse()

        if request.fields:
            filtered_state = {k: v for k, v in state.items() if k in request.fields}
        else:
            filtered_state = state

        version = 0
        updated_at = 0
        if self._state_manager:
            try:
                state_obj = await self._state_manager.load_state(deployment_id)
                if state_obj:
                    version = state_obj.version
                    if state_obj.created_at:
                        updated_at = int(state_obj.created_at.timestamp())
            except Exception:
                pass

        return gateway_pb2.StrategyStateResponse(
            deployment_id=deployment_id,
            state_json=json.dumps(filtered_state),
            version=version,
            updated_at=updated_at,
        )

    async def ExecuteAction(
        self,
        request: gateway_pb2.ExecuteActionRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.ExecuteActionResponse:
        """Execute a supported operator lifecycle action.

        Args:
            request: Action request with deployment_id, action, reason
            context: gRPC context

        Returns:
            ExecuteActionResponse with success status
        """
        await self._ensure_initialized()

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.ExecuteActionResponse(success=False, error=str(e))

        action = request.action.upper()
        reason = request.reason

        if not reason:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Reason is required for audit")
            return gateway_pb2.ExecuteActionResponse(success=False, error="Reason is required")

        action_id = str(uuid4())

        logger.info(f"Dashboard action: {action} on {deployment_id}, reason: {reason}, action_id: {action_id}")

        # Lifecycle commands are queued for the runner; dashboard RPCs never mutate state flags.
        try:
            command = require_enqueueable_command(parse_lifecycle_command(action))
        except LifecycleValueError:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(f"Action not implemented: {action}")
            return gateway_pb2.ExecuteActionResponse(
                success=False,
                error=f"Action not implemented: {action}",
                action_id=action_id,
            )

        try:
            from almanak.gateway.lifecycle import get_lifecycle_store

            store = get_lifecycle_store()
            store.write_command(
                deployment_id=deployment_id,
                command=command,
                issued_by=f"dashboard:{reason}",
            )
            logger.info(f"Issued {command.value} command to {deployment_id} via lifecycle store: {reason}")
            return gateway_pb2.ExecuteActionResponse(
                success=True,
                action_id=action_id,
            )
        except Exception as e:
            logger.error(f"Failed to issue {command.value} command to {deployment_id}: {e}")
            return gateway_pb2.ExecuteActionResponse(
                success=False,
                error=str(e),
                action_id=action_id,
            )

    # crap-allowlist: VIB-4722 only unified deployment identity fields in the existing registry RPC.
    async def RegisterStrategyInstance(
        self,
        request: gateway_pb2.RegisterInstanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.RegisterInstanceResponse:
        """Register a strategy instance in the persistent registry."""
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            return gateway_pb2.RegisterInstanceResponse(success=False, error=str(e))

        try:
            from almanak.gateway.registry.store import StrategyInstance

            registry = get_instance_registry()
            now = datetime.now(UTC)

            existing = registry.get(deployment_id)
            chains_str = ",".join(request.chains) if request.chains else request.chain
            chain_wallets_str = ""
            if request.chain_wallets:
                chain_wallets_str = json.dumps(dict(request.chain_wallets))

            # Derive from the caller identity, not a hosted-normalized deployment ID.
            protocol = request.protocol
            if not protocol:
                config = {}
                if request.config_json:
                    try:
                        config = json.loads(request.config_json)
                    except (json.JSONDecodeError, TypeError):
                        pass
                derivation_key = request.strategy_name or request.deployment_id or deployment_id
                protocol = self._derive_protocol_from_config(config, derivation_key)
                if protocol == "Unknown":
                    protocol = ""

            instance = StrategyInstance(
                deployment_id=deployment_id,
                strategy_name=request.strategy_name or deployment_id,
                template_name=request.template_name,
                chain=request.chain,
                protocol=protocol,
                wallet_address=request.wallet_address,
                config_json=request.config_json,
                chains=chains_str,
                chain_wallets=chain_wallets_str,
                status="RUNNING",
                archived=existing.archived if existing else False,
                created_at=existing.created_at if existing else now,
                updated_at=now,
                last_heartbeat_at=now,
                version=request.version,
            )

            registry.register(instance)

            return gateway_pb2.RegisterInstanceResponse(
                success=True,
                already_existed=existing is not None,
            )
        except Exception as e:
            logger.error(f"Failed to register instance {request.deployment_id}: {e}")
            return gateway_pb2.RegisterInstanceResponse(success=False, error=str(e))

    async def UpdateStrategyInstanceStatus(
        self,
        request: gateway_pb2.UpdateInstanceStatusRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.UpdateInstanceStatusResponse:
        """Update strategy instance status or send heartbeat."""
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            return gateway_pb2.UpdateInstanceStatusResponse(success=False, error=str(e))

        try:
            registry = get_instance_registry()

            if request.heartbeat_only:
                success = registry.heartbeat(deployment_id)
            else:
                success = registry.update_status(deployment_id, request.status, request.reason)

            if not success:
                return gateway_pb2.UpdateInstanceStatusResponse(
                    success=False,
                    error=f"Instance not found: {deployment_id}",
                )

            if request.positions:
                self._cached_positions[deployment_id] = list(request.positions)
            else:
                self._cached_positions.pop(deployment_id, None)

            return gateway_pb2.UpdateInstanceStatusResponse(success=True)
        except Exception as e:
            logger.error(f"Failed to update instance status {request.deployment_id}: {e}")
            return gateway_pb2.UpdateInstanceStatusResponse(success=False, error=str(e))

    async def ArchiveStrategyInstance(
        self,
        request: gateway_pb2.ArchiveInstanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.ArchiveInstanceResponse:
        """Archive a strategy instance (hidden from dashboard, data retained)."""
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            return gateway_pb2.ArchiveInstanceResponse(success=False, error=str(e))

        try:
            registry = get_instance_registry()
            success = registry.archive(deployment_id)
            if not success:
                return gateway_pb2.ArchiveInstanceResponse(
                    success=False,
                    error=f"Instance not found: {deployment_id}",
                )

            self._cached_positions.pop(deployment_id, None)
            logger.info(f"Archived instance {deployment_id}: {request.reason}")
            return gateway_pb2.ArchiveInstanceResponse(success=True)
        except Exception as e:
            logger.error(f"Failed to archive instance {request.deployment_id}: {e}")
            return gateway_pb2.ArchiveInstanceResponse(success=False, error=str(e))

    # crap-allowlist: VIB-4722 only unified deployment identity fields in the existing purge RPC.
    async def PurgeStrategyInstance(
        self,
        request: gateway_pb2.PurgeInstanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PurgeInstanceResponse:
        """Purge a strategy instance and all its events (permanent delete)."""
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            return gateway_pb2.PurgeInstanceResponse(success=False, error=str(e))

        if not request.reason:
            return gateway_pb2.PurgeInstanceResponse(
                success=False,
                error="Reason is required for audit when purging",
            )

        try:
            registry = get_instance_registry()

            success = registry.purge_with_events(deployment_id)
            if not success:
                return gateway_pb2.PurgeInstanceResponse(
                    success=False,
                    error=f"Instance not found: {deployment_id}",
                )

            try:
                store = get_timeline_store()
                store.clear_events(deployment_id)
            except Exception as e:
                logger.debug(f"Failed to clear timeline cache for {deployment_id} (non-fatal): {e}")

            self._cached_positions.pop(deployment_id, None)
            logger.info(f"Purged instance {deployment_id}: {request.reason}")
            return gateway_pb2.PurgeInstanceResponse(success=True)
        except Exception as e:
            logger.error(f"Failed to purge instance {request.deployment_id}: {e}")
            return gateway_pb2.PurgeInstanceResponse(success=False, error=str(e))

    # crap-allowlist: VIB-4722 only unified deployment identity fields in the existing ledger RPC.
    async def GetTransactionLedger(
        self,
        request: gateway_pb2.GetTransactionLedgerRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetTransactionLedgerResponse:
        """Get structured trade records from the transaction ledger."""
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning(f"Invalid deployment_id in GetTransactionLedger: {e}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetTransactionLedgerResponse()

        await self._ensure_initialized()

        since = None
        if request.since_timestamp > 0:
            since = datetime.fromtimestamp(request.since_timestamp, tz=UTC)

        intent_type = request.intent_type_filter or None
        limit = request.limit if request.limit > 0 else 100

        entries = []
        if self._state_manager is not None:
            try:
                entries = await self._state_manager.get_ledger_entries(
                    deployment_id, since=since, intent_type=intent_type, limit=limit + 1
                )
            except Exception:
                logger.debug("Failed to query transaction ledger for %s", deployment_id, exc_info=True)

        has_more = len(entries) > limit
        if has_more:
            entries = entries[:limit]

        proto_entries = []
        for entry in entries:
            proto_entries.append(
                gateway_pb2.LedgerEntryInfo(
                    id=entry.id,
                    cycle_id=entry.cycle_id,
                    deployment_id=entry.deployment_id,
                    timestamp=int(entry.timestamp.timestamp()),
                    intent_type=entry.intent_type,
                    token_in=entry.token_in,
                    amount_in=entry.amount_in,
                    token_out=entry.token_out,
                    amount_out=entry.amount_out,
                    effective_price=entry.effective_price,
                    slippage_bps=entry.slippage_bps or 0.0,
                    gas_used=entry.gas_used,
                    gas_usd=entry.gas_usd,
                    tx_hash=entry.tx_hash,
                    chain=entry.chain,
                    protocol=entry.protocol,
                    success=entry.success,
                    error=entry.error,
                )
            )

        return gateway_pb2.GetTransactionLedgerResponse(
            entries=proto_entries,
            has_more=has_more,
        )

    async def _first_action_wallet_value(self, deployment_id: str) -> Decimal | None:
        """Wallet USD value at the strategy's first action (VIB-3914 anchor).

        VIB-5059: replaces the bulk full-width ledger fetch for the one
        consumer that genuinely needs row contents — the first-action
        "Deployed" anchor. Walks oldest-first, LIMIT-bounded batches of the
        ONLY rows that can anchor (both ``pre_state_json`` and
        ``price_inputs_json`` present — filtered in SQL) and stops at the
        first row whose pre-state values to a positive total, exactly like
        the legacy in-memory walk. Normally resolves on the first batch;
        the scan cap bounds the pathological all-candidates-valueless case.

        Returns ``None`` when no candidate anchors (the caller falls back to
        ``portfolio_metrics``, the legacy behavior).
        """
        from almanak.framework.dashboard.quant_aggregations import (
            _wallet_value_at_first_action,
        )

        if self._state_manager is None:
            return None
        offset = 0
        # Geometric batches bound hosted OFFSET rescans while keeping the common fetch small.
        limit = _QUANT_ANCHOR_BATCH_LIMIT
        while offset < _QUANT_ANCHOR_SCAN_ROW_CAP:
            rows = await self._state_manager.get_ledger_anchor_candidates(deployment_id, limit=limit, offset=offset)
            if not rows:
                return None
            value = _wallet_value_at_first_action(rows)
            if value is not None:
                return value
            if len(rows) < limit:
                return None
            offset += len(rows)
            limit = min(limit * 2, _QUANT_ANCHOR_BATCH_MAX)
        logger.warning(
            "First-action anchor walk hit scan cap (%d rows) for strategy=%s without a valued anchor; "
            "falling back to portfolio_metrics.",
            _QUANT_ANCHOR_SCAN_ROW_CAP,
            deployment_id,
        )
        return None

    async def _load_quant_inputs(
        self, deployment_id: str
    ) -> tuple[Any, list[Any], LedgerQuantStats, list[dict[str, Any]], Any]:
        """Load the on-disk inputs every quant aggregation needs.

        Shared data path for ``GetPnLSummary`` / ``GetCostStack`` /
        ``GetAuditPosture``. Returns ``(portfolio_metrics, snapshots,
        ledger_stats, accounting_events, position_summary)``.

        VIB-5059 Phase 1 (SQL half): the ledger element is a
        ``LedgerQuantStats`` — the store computes the LTD COUNT/SUM
        aggregates SQL-side (O(1) rows, no JSON-blob columns) and the
        bounded anchor walk supplies the first-action "Deployed" value, so
        the load no longer materialises up to 100k full-width rows per
        render. Because the SQL aggregates see EVERY row, lifetime totals
        on >100k-row deployments are now exact where the legacy Python
        path silently truncated to the newest 100k.

        Hosted Postgres uses async backends with no sync API, so all I/O
        here is awaited (VIB-3933). Failures collapse to empty inputs
        rather than raising — the compute_* layer is built to handle
        missing data and surface ``UNAVAILABLE`` confidence.
        """
        from almanak.framework.observability.ledger import LedgerQuantStats

        portfolio_metrics: Any = None
        snapshots: list[Any] = []
        ledger_stats = LedgerQuantStats()
        accounting_events: list[dict[str, Any]] = []
        position_summary: Any = None

        if self._state_manager is not None:
            try:
                portfolio_metrics = await self._state_manager.get_portfolio_metrics(deployment_id)
            except Exception:
                logger.debug("get_portfolio_metrics failed for %s", deployment_id, exc_info=True)
            try:
                # The store returns the latest window oldest-first; snapshots[-1] is current.
                snapshots = await self._state_manager.get_recent_snapshots(deployment_id, limit=168)
            except Exception:
                logger.debug("get_recent_snapshots failed for %s", deployment_id, exc_info=True)
            try:
                ledger_stats = await self._state_manager.get_ledger_quant_stats(deployment_id)
            except Exception:
                logger.debug("get_ledger_quant_stats failed for %s", deployment_id, exc_info=True)
            try:
                anchor = await self._first_action_wallet_value(deployment_id)
                if anchor is not None:
                    ledger_stats = dataclasses.replace(ledger_stats, first_action_wallet_value_usd=anchor)
            except Exception:
                logger.debug("first-action anchor walk failed for %s", deployment_id, exc_info=True)
            try:
                accounting_events = await self._state_manager.get_accounting_events_for_dashboard(
                    deployment_id=deployment_id
                )
            except Exception:
                logger.debug("get_accounting_events_for_dashboard failed for %s", deployment_id, exc_info=True)

        latest_snapshot = await self._get_latest_snapshot(deployment_id)
        if latest_snapshot is not None:
            position_summary = _QuantPositionSummary(latest_snapshot)

        return portfolio_metrics, snapshots, ledger_stats, accounting_events, position_summary

    async def _get_quant_inputs(
        self, deployment_id: str
    ) -> tuple[Any, list[Any], LedgerQuantStats, list[dict[str, Any]], Any]:
        """TTL-cached, single-flight wrapper around :meth:`_load_quant_inputs`.

        VIB-5059 Phase 1: a single dashboard render fans out to
        ``GetPnLSummary`` / ``GetCostStack`` / ``GetAuditPosture``, which all
        need the same inputs — previously three independent full loads per
        render. A per-deployment entry expires after
        ``_QUANT_INPUTS_CACHE_TTL_SECONDS``; concurrent callers for the same
        deployment coalesce behind one lock so only the first runs the load.

        The returned objects are SHARED across RPCs — consumers (the
        ``compute_*`` aggregations) treat them as read-only and must keep
        doing so. A TTL of 0 disables caching for sequential calls (the
        kill-switch semantic pinned by tests).
        """
        if not hasattr(self, "_quant_inputs_cache"):
            self._quant_inputs_cache = {}
            self._quant_inputs_locks = {}
        ttl = getattr(self, "_quant_inputs_ttl_seconds", _QUANT_INPUTS_CACHE_TTL_SECONDS)

        cached = self._quant_inputs_cache.get(deployment_id)
        if cached is not None and time.monotonic() - cached[0] < ttl:
            return cached[1]

        lock = self._quant_inputs_locks.setdefault(deployment_id, asyncio.Lock())
        async with lock:
            cached = self._quant_inputs_cache.get(deployment_id)
            if cached is not None and time.monotonic() - cached[0] < ttl:
                return cached[1]
            inputs = await self._load_quant_inputs(deployment_id)
            now = time.monotonic()
            self._quant_inputs_cache = {
                key: value for key, value in self._quant_inputs_cache.items() if now - value[0] < ttl
            }
            self._quant_inputs_cache[deployment_id] = (now, inputs)
            # Retain held locks so waiters cannot race a replacement lock.
            self._quant_inputs_locks = {
                key: existing
                for key, existing in self._quant_inputs_locks.items()
                if key in self._quant_inputs_cache or existing.locked()
            }
            return inputs

    async def _get_lifetime_drawdown(self, deployment_id: str) -> tuple[Decimal, Decimal] | None:
        """Lifetime ``(max_drawdown_pct, current_drawdown_pct)`` over FULL history (VIB-5118/5134).

        Single-flight per deployment, mirroring :meth:`_get_quant_inputs`. **Deliberately
        separate from the shared quant-input load**: only ``GetPnLSummary`` surfaces
        drawdown, and the full-history ``get_nav_series`` scan would otherwise make
        ``GetCostStack`` / ``GetAuditPosture`` pay for a scan they discard (Codex review
        of PR #2801).

        VIB-5134 splits the cost in two:

        - **max-drawdown** rides the expensive full-history scan, refreshed only every
          :data:`_LIFETIME_DRAWDOWN_TTL_SECONDS` (slow-moving → ~6× fewer scans).
        - **current-drawdown** stays live every render: between full scans the
          checkpoint is advanced by a cheap incremental "fetch since cursor" fold
          (:meth:`_fold_lifetime_drawdown`), so a new high-water mark that lands after
          the last scan is folded into the running peak *before* current-drawdown is
          computed (a plain "latest NAV vs. cached peak" would understate it).

        Returns ``None`` (→ ``compute_pnl_summary`` degrades to the recent-window
        drawdown) when there is no series or the backend read fails — never raises.
        """
        if not hasattr(self, "_lifetime_dd_ckpt"):
            self._lifetime_dd_ckpt = {}
            self._lifetime_dd_locks = {}
        if self._state_manager is None:
            return None
        full_ttl = getattr(self, "_lifetime_dd_ttl_seconds", _LIFETIME_DRAWDOWN_TTL_SECONDS)

        lock = self._lifetime_dd_locks.setdefault(deployment_id, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            existing = self._lifetime_dd_ckpt.get(deployment_id)
            if existing is None or now - existing.full_scan_at >= full_ttl:
                ckpt = await self._full_scan_lifetime_drawdown(deployment_id, existing)
            else:
                ckpt = await self._fold_lifetime_drawdown(deployment_id, existing)
            self._lifetime_dd_ckpt[deployment_id] = ckpt

            # Awaited scans stamp completion time, so prune against a fresh clock reading.
            now = time.monotonic()
            self._lifetime_dd_ckpt = {
                key: value
                for key, value in self._lifetime_dd_ckpt.items()
                if now - value.full_scan_at < full_ttl or key == deployment_id
            }
            self._lifetime_dd_locks = {
                key: existing
                for key, existing in self._lifetime_dd_locks.items()
                if key in self._lifetime_dd_ckpt or existing.locked()
            }

            if ckpt.state.running_peak is None:
                return None
            return ckpt.state.as_pcts()

    async def _full_scan_lifetime_drawdown(
        self, deployment_id: str, prior: _LifetimeDrawdownCheckpoint | None
    ) -> _LifetimeDrawdownCheckpoint:
        """Re-seed the lifetime-drawdown checkpoint from a full-history NAV scan (VIB-5134).

        The expensive O(history) read. ``full_scan_at`` is stamped **after** the awaited
        scan (a slow scan must not leave the checkpoint already-expired and re-trigger a
        full scan on the very next render — CodeRabbit).

        On backend FAILURE the prior good lifetime is **preserved** rather than blanked:
        a transient blip must not strand the tile on the recent-window fallback for the
        full slow TTL. The checkpoint is stamped to retry within
        ``_LIFETIME_DRAWDOWN_RETRY_SECONDS`` (bounded, not every render). With no prior
        good state the checkpoint is empty (``running_peak=None`` → the accessor returns
        ``None`` and the summary degrades to the recent window).
        """
        from almanak.framework.dashboard.quant_aggregations import DrawdownState, fold_nav_text

        assert self._state_manager is not None
        try:
            nav_rows, truncated = await self._state_manager.get_nav_series(deployment_id)
        except Exception:
            logger.debug("get_nav_series (full scan) failed for %s", deployment_id, exc_info=True)
            full_ttl = getattr(self, "_lifetime_dd_ttl_seconds", _LIFETIME_DRAWDOWN_TTL_SECONDS)
            retry_at = time.monotonic() - max(0.0, full_ttl - _LIFETIME_DRAWDOWN_RETRY_SECONDS)
            if prior is not None and prior.state.running_peak is not None:
                # Preserve last-known-good state across transient backend failures.
                return _LifetimeDrawdownCheckpoint(
                    state=prior.state, cursor=prior.cursor, full_scan_at=retry_at, truncated=prior.truncated
                )
            return _LifetimeDrawdownCheckpoint(
                state=DrawdownState(), cursor=None, full_scan_at=retry_at, truncated=False
            )

        state = DrawdownState()
        cursor: tuple[datetime, int] | None = None
        if nav_rows:
            state = fold_nav_text(state, nav_rows)
            # Cursor fields precede the trailing positions JSON projection.
            last_ts, _total, _cash, last_id = nav_rows[-1][:4]
            cursor = (last_ts, last_id)
            if truncated:
                logger.warning(
                    "NAV series truncated at scan_cap for %s — lifetime drawdown "
                    "computed over the newest window, not full history",
                    deployment_id,
                )
        return _LifetimeDrawdownCheckpoint(
            state=state, cursor=cursor, full_scan_at=time.monotonic(), truncated=truncated
        )

    async def _fold_lifetime_drawdown(
        self, deployment_id: str, ckpt: _LifetimeDrawdownCheckpoint
    ) -> _LifetimeDrawdownCheckpoint:
        """Advance the checkpoint by snapshots newer than its cursor (VIB-5134).

        Keeps current-drawdown live between full scans without re-scanning history:
        the running-peak fold is resumable, so folding only the new tail is identical
        to a full recompute. ``full_scan_at`` / ``truncated`` are preserved (this is
        NOT a full scan). On fetch failure (or no cursor yet) the checkpoint is
        returned unchanged — the last good state, never a degrade-to-None.
        """
        from almanak.framework.dashboard.quant_aggregations import fold_nav_text

        if ckpt.cursor is None:
            return ckpt
        assert self._state_manager is not None
        try:
            # A truncated incremental read advances the cursor and catches up next render.
            new_rows, _truncated = await self._state_manager.get_nav_series(deployment_id, since=ckpt.cursor)
        except Exception:
            logger.debug("get_nav_series (incremental) failed for %s", deployment_id, exc_info=True)
            return ckpt
        if not new_rows:
            return ckpt
        state = fold_nav_text(ckpt.state, new_rows)
        last_ts, _total, _cash, last_id = new_rows[-1][:4]
        return _LifetimeDrawdownCheckpoint(
            state=state,
            cursor=(last_ts, last_id),
            full_scan_at=ckpt.full_scan_at,
            truncated=ckpt.truncated,
        )

    @staticmethod
    def _perp_summaries_from_snapshot(
        snapshot: Any,
    ) -> tuple[list[gateway_pb2.PerpPositionSummary], str]:
        """Build the snapshot-derived perp story (VIB-5942 / ALM-2977).

        Returns ``(perp_position_summaries, positions_as_of)`` from the LATEST
        snapshot's PERP positions (plural). Every field honours Empty≠Zero: a
        details value that is absent / ``None`` / empty goes on the wire as ``""``
        (UNMEASURED → the client renders "— unmeasured"), never a fabricated ``0``.
        ``is_long`` is only set when the snapshot actually measured it; otherwise
        the direction string stays ``""`` and the optional bool is absent.
        """
        if snapshot is None:
            return [], ""
        positions = getattr(snapshot, "positions", None) or []

        def _measured(details: dict[str, Any], key: str) -> str:
            v = details.get(key)
            return "" if v is None or v == "" else str(v)

        summaries: list[gateway_pb2.PerpPositionSummary] = []
        for pos in positions:
            ptype = getattr(pos, "position_type", "")
            ptype_str = ptype.value if hasattr(ptype, "value") else str(ptype)
            if ptype_str != "PERP":
                continue
            # Malformed persisted details degrade to unmeasured fields, never an RPC failure.
            raw_details = getattr(pos, "details", None)
            details = raw_details if isinstance(raw_details, dict) else {}

            entry = gateway_pb2.PerpPositionSummary(
                market=_measured(details, "market"),
                entry_price_usd=_measured(details, "entry_price_usd"),
                mark_price_usd=_measured(details, "mark_price_usd"),
                leverage=_measured(details, "leverage"),
                notional_usd=_measured(details, "size_usd"),
                collateral_usd=_measured(details, "collateral_value_usd"),
                unrealized_pnl_usd=_measured(details, "unrealized_pnl_usd"),
                protocol=str(getattr(pos, "protocol", "") or ""),
                chain=str(getattr(pos, "chain", "") or ""),
            )
            # Unknown direction remains empty; never default a money position to long.
            is_long = details.get("is_long")
            if isinstance(is_long, bool):
                entry.is_long = is_long
                entry.direction = "LONG" if is_long else "SHORT"
            else:
                side = str(details.get("side") or "").upper()
                if side in ("LONG", "SHORT"):
                    entry.direction = side
            summaries.append(entry)

        as_of = ""
        ts = getattr(snapshot, "timestamp", None)
        if ts is not None:
            if hasattr(ts, "isoformat"):
                # Persisted naive timestamps follow the UTC convention used by NAV builders.
                if getattr(ts, "tzinfo", None) is None:
                    ts = ts.replace(tzinfo=UTC)
                as_of = ts.isoformat()
            else:
                as_of = str(ts)
        return summaries, as_of

    async def GetPnLSummary(
        self,
        request: gateway_pb2.GetPnLSummaryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PnLSummary:
        """5-second-eyeball card: wallet money trail + cash + primary-risk gauge.

        VIB-3969: focused replacement for the PnL-shaped slice of
        ``GetQuantHeader``. Clients calling this never pay the cost of
        computing G6 reconciliation or the 21-cell Accountant Test
        posture.
        """
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning("Invalid deployment_id in GetPnLSummary: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.PnLSummary()

        await self._ensure_initialized()

        from almanak.framework.dashboard.quant_aggregations import compute_pnl_summary

        (
            portfolio_metrics,
            snapshots,
            ledger_stats,
            accounting_events,
            position_summary,
        ) = await self._get_quant_inputs(deployment_id)

        lifetime_drawdown = await self._get_lifetime_drawdown(deployment_id)

        pnl = compute_pnl_summary(
            portfolio_metrics=portfolio_metrics,
            snapshots=snapshots,
            ledger_entries=ledger_stats,
            accounting_events=accounting_events,
            position_summary=position_summary,
            lifetime_drawdown=lifetime_drawdown,
        )

        perp_positions, positions_as_of = self._perp_summaries_from_snapshot(snapshots[-1] if snapshots else None)

        return gateway_pb2.PnLSummary(
            deployed_usd=str(pnl.deployed_usd),
            nav_usd=str(pnl.nav_usd),
            perp_positions=perp_positions,
            positions_as_of=positions_as_of,
            # Coverage travels beside the money it qualifies.
            cost_basis_partial=bool(pnl.cost_basis_partial),
            # Empty strings encode unmeasured optional money values on the wire.
            lifetime_pnl_usd=("" if pnl.lifetime_pnl_usd is None else str(pnl.lifetime_pnl_usd)),
            lifetime_pnl_pct=("" if pnl.lifetime_pnl_pct is None else f"{pnl.lifetime_pnl_pct:.2f}"),
            net_apr_pct=("" if pnl.net_apr_pct is None else f"{pnl.net_apr_pct:.2f}"),
            max_drawdown_pct=f"{pnl.max_drawdown_pct:.2f}",
            current_drawdown_pct=f"{pnl.current_drawdown_pct:.2f}",
            value_confidence=serialize_value_confidence(pnl.value_confidence),
            age_days=pnl.age_days,
            # Fractional days are the annualization denominator.
            age_days_exact=str(pnl.age_days_exact),
            deployed_capital_usd=str(pnl.deployed_capital_usd),
            available_cash_usd=str(pnl.available_cash_usd),
            open_position_count=pnl.open_position_count,
            primary_risk_kind=pnl.primary_risk_kind,
            primary_risk_label=pnl.primary_risk_label,
            primary_risk_value=pnl.primary_risk_value,
            primary_risk_color=pnl.primary_risk_color,
        )

    async def GetCostStack(
        self,
        request: gateway_pb2.GetCostStackRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.CostStackInfo:
        """Life-to-date Gas / Fees / Slip / Earn decomposition.

        VIB-3969: focused replacement for the cost-stack slice of
        ``GetQuantHeader``. Reads the SQL-side ledger aggregates +
        accounting_events; cost is proportional to accounting-event count
        (the ledger side is O(1) since VIB-5059 Phase 1).
        """
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning("Invalid deployment_id in GetCostStack: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.CostStackInfo()

        await self._ensure_initialized()

        from almanak.framework.dashboard.quant_aggregations import (
            compute_cost_stack,
            compute_inventory_unrealized,
        )

        _, snapshots, ledger_stats, accounting_events, _ = await self._get_quant_inputs(deployment_id)

        cs = compute_cost_stack(ledger_stats, accounting_events)

        # Use persisted marks only. The same cached snapshot must drive every quant tile.
        # An applied inventory stamp means the mark is already in position NAV.
        latest_snapshot = snapshots[-1] if snapshots else None
        latest_token_prices = getattr(latest_snapshot, "token_prices", None) or {}
        snapshot_metadata = getattr(latest_snapshot, "snapshot_metadata", None)
        swap_inventory_stamp = snapshot_metadata.get("swap_inventory") if isinstance(snapshot_metadata, dict) else None
        if isinstance(swap_inventory_stamp, dict) and swap_inventory_stamp.get("status") == "applied":
            cs.inventory_unrealized_usd = None
        else:
            # Exclude only token legs already carrying swap-lot basis; cash inventory remains additive.
            cs.inventory_unrealized_usd = compute_inventory_unrealized(
                accounting_events,
                deployment_id,
                latest_token_prices,
                exclude_tokens=_folded_inventory_tokens(swap_inventory_stamp),
            )

        return cost_stack_to_proto(cs)

    async def GetAuditPosture(
        self,
        request: gateway_pb2.GetAuditPostureRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.AuditPosture:
        """Reconciliation (G6) + audit-trail completeness + Accountant Test posture.

        VIB-3969: focused replacement for the audit slice of
        ``GetQuantHeader``. Server-computed only — clients must NOT
        reconstruct G6 client-side, or the math will drift between the
        dashboard and the accountant test harness.
        """
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning("Invalid deployment_id in GetAuditPosture: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.AuditPosture()

        await self._ensure_initialized()

        from almanak.framework.dashboard.quant_aggregations import (
            _detect_primitive,
            compute_audit_trail,
            compute_cost_stack,
            compute_pnl_summary,
            compute_reconciliation,
            evaluate_posture,
        )

        (
            portfolio_metrics,
            snapshots,
            ledger_stats,
            accounting_events,
            position_summary,
        ) = await self._get_quant_inputs(deployment_id)

        # Reconciliation shares PnL anchors but must not trigger the unused full-history scan.
        pnl = compute_pnl_summary(
            portfolio_metrics=portfolio_metrics,
            snapshots=snapshots,
            ledger_entries=ledger_stats,
            accounting_events=accounting_events,
            position_summary=position_summary,
        )
        cost_stack = compute_cost_stack(ledger_stats, accounting_events)
        audit_trail = compute_audit_trail(ledger_stats, accounting_events)
        snapshot_initial, snapshot_final = await self._get_reconciliation_endpoint_snapshots(
            deployment_id,
            snapshots,
        )
        reconciliation = compute_reconciliation(
            initial_value_usd=pnl.deployed_usd,
            nav_usd=pnl.nav_usd,
            cost_stack=cost_stack,
            accounting_events=accounting_events,
            snapshot_initial=snapshot_initial,
            snapshot_final=snapshot_final,
            deployment_id=deployment_id,
        )
        primitive = _detect_primitive(accounting_events)
        posture = evaluate_posture(
            primitive=primitive,
            ledger_entries=ledger_stats,
            accounting_events=accounting_events,
            snapshots=snapshots,
            audit=audit_trail,
            reconciliation=reconciliation,
            portfolio_metrics=portfolio_metrics,
        )

        return gateway_pb2.AuditPosture(
            g6_status=("PASS" if reconciliation.passed else "FAIL") if reconciliation.has_data else "NA",
            g6_wallet_pnl_usd=str(reconciliation.wallet_pnl_usd),
            g6_component_pnl_usd=str(reconciliation.component_pnl_usd),
            g6_gap_usd=str(reconciliation.gap_usd),
            g6_epsilon_usd=str(reconciliation.epsilon_usd),
            g6_sum_swap=str(reconciliation.sum_swap),
            g6_sum_lp=str(reconciliation.sum_lp),
            g6_sum_perp=str(reconciliation.sum_perp),
            g6_sum_fees=str(reconciliation.sum_fees),
            g6_sum_funding=str(reconciliation.sum_funding),
            g6_sum_interest=str(reconciliation.sum_interest),
            g6_sum_gas=str(reconciliation.sum_gas),
            ledger_total=audit_trail.ledger_total,
            ledger_with_price_inputs=audit_trail.ledger_with_price_inputs,
            ledger_with_pre_post_state=audit_trail.ledger_with_pre_post_state,
            ledger_with_gas_usd=audit_trail.ledger_with_gas_usd,
            events_total=audit_trail.events_total,
            events_with_versions=audit_trail.events_with_versions,
            primitive=posture.primitive,
            cells_passed=posture.cells_passed,
            cells_failed=posture.cells_failed,
            cells_xfail=posture.cells_xfail,
            cells_total=posture.cells_total,
            failing_cells=posture.failing,
            xfail_cells=posture.xfail,
        )

    async def _collect_trade_tape_sources(
        self,
        deployment_id: str,
        limit: int,
        before_ts: datetime | None,
        since_ts: datetime | None = None,
    ) -> tuple[list[Any], list[dict[str, Any]], list[Any]]:
        """Fetch ledger / accounting / position rows for a trade tape.

        ``since_ts`` (VIB-5059 P2) bounds the ledger fetch below — markers for the
        same window as the chart — following the ledger's existing ``since``
        semantics. ``None`` leaves the lower bound open (today's behavior).

        Failure semantics differ across sources:
          - **Ledger** is the primary source. ``GetTradeTapeResponse`` has no
            error field, so a swallowed backend failure here is indistinguishable
            from a genuine empty history. We let the exception propagate so the
            caller can map it to a non-OK gRPC status. A missing ``StateManager``
            (initialization failed) is treated the same way — the ledger backend
            is unavailable and callers must be told, not lied to with empty rows.
          - **Accounting / position** are optional enrichment. Per-source failures
            are logged at DEBUG and degrade gracefully to empty lists — the trade
            tape still renders the ledger rows without joined event payloads.
        """
        accounting_events: list[dict[str, Any]] = []
        position_events: list[Any] = []
        if self._state_manager is None:
            raise RuntimeError("StateManager unavailable")

        # Ledger is authoritative and over-fetched by one for has_more.
        ledger_entries = await self._state_manager.get_ledger_entries(
            deployment_id, since=since_ts, intent_type=None, limit=limit + 1, before=before_ts
        )
        try:
            accounting_events = await self._state_manager.get_accounting_events_for_dashboard(
                deployment_id=deployment_id
            )
        except Exception:
            logger.debug("get_accounting_events_for_dashboard failed for %s", deployment_id, exc_info=True)
        try:
            position_events = await self._state_manager.get_position_events_for_dashboard(deployment_id=deployment_id)
        except Exception:
            logger.debug("get_position_events_for_dashboard failed for %s", deployment_id, exc_info=True)
        return ledger_entries, accounting_events, position_events

    async def GetTradeTape(
        self,
        request: gateway_pb2.GetTradeTapeRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetTradeTapeResponse:
        """One row per intent (cycle_id) with receipt-parsed payloads.

        Joins ledger × accounting_events × position_events on
        ``ledger_entry_id`` and ``cycle_id`` so the dashboard can render
        a Quant-grade trade tape without re-reading the chain.
        """
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning("Invalid deployment_id in GetTradeTape: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetTradeTapeResponse()

        await self._ensure_initialized()

        limit = request.limit if request.limit > 0 else 50
        # Untrusted epoch values are caller errors, not internal gateway failures.
        try:
            before_ts = (
                datetime.fromtimestamp(request.before_timestamp, tz=UTC) if request.before_timestamp > 0 else None
            )
            from_dt = self._unix_to_dt(request.from_ts)
        except (OverflowError, OSError, ValueError) as e:
            logger.warning(
                "Invalid timestamp in GetTradeTape (before=%r, from=%r): %s",
                request.before_timestamp,
                request.from_ts,
                e,
            )
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                "before_timestamp / from_ts out of range "
                "(must be a Unix epoch second within the supported datetime range)"
            )
            return gateway_pb2.GetTradeTapeResponse()

        if from_dt is not None and before_ts is not None and from_dt >= before_ts:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("inverted window: from_ts must be strictly before before_timestamp")
            return gateway_pb2.GetTradeTapeResponse()

        # The ledger's exclusive lower bound is nudged one storage-resolution unit to match the chart.
        since_ts = (from_dt - timedelta(microseconds=1)) if from_dt is not None else None

        try:
            ledger_entries, accounting_events, position_events = await self._collect_trade_tape_sources(
                deployment_id, limit, before_ts, since_ts
            )
        except Exception:
            # Do not misreport an unavailable ledger as empty or leak backend details.
            logger.exception("get_ledger_entries failed for %s", deployment_id)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Failed to load trade tape from ledger backend")
            return gateway_pb2.GetTradeTapeResponse()
        events_by_ledger, events_by_cycle = _index_trade_tape_accounting_events(accounting_events)
        pos_by_ledger = _index_trade_tape_position_events(position_events)

        rows: list[gateway_pb2.TradeTapeRow] = []
        for entry in ledger_entries:
            ts = getattr(entry, "timestamp", None)
            if before_ts is not None and ts and ts >= before_ts:
                continue
            entry_id = getattr(entry, "id", "")
            cycle_id = getattr(entry, "cycle_id", "")
            row_event = _resolve_trade_tape_row_event(entry_id, cycle_id, events_by_ledger, events_by_cycle)
            rows.append(_build_trade_tape_row(entry, row_event, pos_by_ledger.get(entry_id)))
            if len(rows) >= limit:
                break

        has_more = len(ledger_entries) > len(rows)
        return gateway_pb2.GetTradeTapeResponse(rows=rows, has_more=has_more)

    _ACTIVITY_FEED_LIMIT_DEFAULT = 50
    _ACTIVITY_FEED_LIMIT_MAX = 200
    # Bound backend fan-out when cursor filtering or dedup leaves a page short.
    _ACTIVITY_FEED_OVER_FETCH_FACTOR = 3
    _ACTIVITY_FEED_MAX_BACKFILL_ATTEMPTS = 3

    async def GetActivityFeed(
        self,
        request: gateway_pb2.GetActivityFeedRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetActivityFeedResponse:
        """Return a chronologically merged feed of timeline events + ledger rows.

        The merge rule (PRD-TimelineEvents §6.1, §9):
          * Both streams are loaded with the same ``before_timestamp`` cursor
            pushed DOWN into each backend (so a paginated caller can never
            receive a "newest N rows that don't match the cursor" empty page
            when activity is dense).
          * Items sort by timestamp DESC, ties broken by ``(kind, item_id)``.
          * A TIMELINE_EVENT whose ``related_ledger_entry_id`` references a
            LEDGER_ENTRY in the same response window is dropped — the ledger
            row IS the financial truth.
          * The first ``limit`` items are returned. The cursor returned is
            composite: ``next_before_timestamp`` + ``next_before_id`` so
            multiple items at the same timestamp paginate deterministically
            (a tied item is never returned twice nor skipped).
        """
        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning(f"Invalid deployment_id in GetActivityFeed: {e}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetActivityFeedResponse()

        await self._ensure_initialized()
        resolved_id = deployment_id

        limit = min(
            request.limit if request.limit > 0 else self._ACTIVITY_FEED_LIMIT_DEFAULT,
            self._ACTIVITY_FEED_LIMIT_MAX,
        )
        before_ts = request.before_timestamp if request.before_timestamp > 0 else None
        # Treat out-of-range client cursors as INVALID_ARGUMENT, not an internal failure.
        try:
            before_dt = datetime.fromtimestamp(before_ts, tz=UTC) if before_ts is not None else None
        except (OverflowError, OSError, ValueError) as exc:
            logger.warning(f"Invalid GetActivityFeed before_timestamp {before_ts!r}: {exc}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                "before_timestamp out of range (must be a valid Unix epoch second within supported datetime range)"
            )
            return gateway_pb2.GetActivityFeedResponse()
        before_id = request.before_id or ""

        cursor_error = self._validate_activity_feed_cursor(before_ts, before_id)
        if cursor_error is not None:
            logger.warning(f"Invalid GetActivityFeed cursor: {cursor_error}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(cursor_error)
            return gateway_pb2.GetActivityFeedResponse()

        store_before = self._compute_store_before(before_dt, before_id)

        page_pairs, has_more, backfill_truncated = await self._gather_activity_feed_page(
            resolved_id=resolved_id,
            limit=limit,
            event_type_filter=request.event_type_filter or None,
            intent_type_filter=request.intent_type_filter or None,
            initial_store_before=store_before,
            initial_before_ts=before_ts,
            initial_before_id=before_id,
        )
        next_ts, next_id = (page_pairs[-1][0].timestamp, page_pairs[-1][1]) if (has_more and page_pairs) else (0, "")

        return gateway_pb2.GetActivityFeedResponse(
            items=[item for (item, _key) in page_pairs],
            has_more=has_more,
            next_before_timestamp=next_ts,
            next_before_id=next_id,
            backfill_truncated=backfill_truncated,
        )

    async def _gather_activity_feed_page(
        self,
        *,
        resolved_id: str,
        limit: int,
        event_type_filter: str | None,
        intent_type_filter: str | None,
        initial_store_before: datetime | None,
        initial_before_ts: int | None,
        initial_before_id: str,
    ) -> tuple[list[tuple[gateway_pb2.ActivityFeedItem, str]], bool, bool]:
        """Backfill loop: fetch enough from each stream to fill a page after
        merge + boundary-filter + incremental-dedup.

        Each iteration:
          1. Over-fetches ``limit * OVER_FETCH_FACTOR + 1`` from each non-exhausted stream
             (using its independent cursor that strictly excludes already-fetched rows).
          2. Applies the user's boundary filter (idempotent — items already
             strictly before the cursor pass through unchanged).
          3. Sorts the cumulative buffer and runs the page-incremental dedup.
          4. Returns immediately if the page is full or both streams have
             exhausted.

        Bounded by ``_ACTIVITY_FEED_MAX_BACKFILL_ATTEMPTS`` so a saturated
        tie-second cannot fan out unbounded backend round-trips.

        Returns ``(page_pairs, has_more, backfill_truncated)``:
          * ``backfill_truncated`` is True only when MAX_ATTEMPTS was hit
            WITHOUT filling the page AND at least one stream still has more
            rows (a saturated tie-second). Renderers surface this so
            operators can distinguish "end of feed" from "tail of a tie
            second was dropped" (CodeRabbit on PR #2117).
        """
        timeline_cursor_dt = initial_store_before
        ledger_cursor_dt = initial_store_before
        timeline_exhausted = False
        ledger_exhausted = False

        over_fetch = limit * self._ACTIVITY_FEED_OVER_FETCH_FACTOR + 1
        cumulative_items: list[tuple[gateway_pb2.ActivityFeedItem, str]] = []
        page_pairs: list[tuple[gateway_pb2.ActivityFeedItem, str]] = []
        has_more = False
        attempts_used = 0

        for _attempt in range(self._ACTIVITY_FEED_MAX_BACKFILL_ATTEMPTS):
            attempts_used += 1
            new_timeline_events: list[Any] = []
            new_ledger_entries: list[Any] = []

            if not timeline_exhausted:
                new_timeline_events = self._load_timeline_for_feed(
                    resolved_id, over_fetch, event_type_filter, timeline_cursor_dt
                )
                if len(new_timeline_events) < over_fetch:
                    timeline_exhausted = True

            if not ledger_exhausted:
                new_ledger_entries = await self._load_ledger_for_feed(
                    resolved_id, over_fetch, intent_type_filter, ledger_cursor_dt
                )
                if len(new_ledger_entries) < over_fetch:
                    ledger_exhausted = True

            if not new_timeline_events and not new_ledger_entries:
                break

            new_items = self._build_feed_items(resolved_id, new_timeline_events, new_ledger_entries)
            new_items = self._apply_boundary_filter(new_items, initial_before_ts, initial_before_id)
            cumulative_items.extend(new_items)

            # Stable sorts make timestamp primary and composite key the tie-breaker.
            cumulative_items.sort(key=lambda pair: pair[1], reverse=True)
            cumulative_items.sort(key=lambda pair: pair[0].timestamp, reverse=True)
            page_pairs, has_more = self._select_page_with_incremental_dedup(cumulative_items, limit)

            if len(page_pairs) >= limit:
                return page_pairs, has_more, False
            if timeline_exhausted and ledger_exhausted:
                return page_pairs, has_more, False

            # Each store uses a strict-before cursor, so advance to its oldest fetched item.
            timeline_cursor_dt = self._advance_stream_cursor(new_timeline_events, timeline_cursor_dt)
            ledger_cursor_dt = self._advance_stream_cursor(new_ledger_entries, ledger_cursor_dt)

        backfill_truncated = (
            attempts_used >= self._ACTIVITY_FEED_MAX_BACKFILL_ATTEMPTS
            and not (timeline_exhausted and ledger_exhausted)
            and len(page_pairs) < limit
        )
        if backfill_truncated:
            logger.warning(
                "GetActivityFeed: backfill loop hit MAX_ATTEMPTS without "
                "filling page (limit=%d, page=%d, timeline_exhausted=%s, "
                "ledger_exhausted=%s). Tie-second saturation may be present.",
                limit,
                len(page_pairs),
                timeline_exhausted,
                ledger_exhausted,
            )
        return page_pairs, has_more, backfill_truncated

    @staticmethod
    def _advance_stream_cursor(
        items: list[Any],
        fallback: datetime | None,
    ) -> datetime | None:
        """Return the per-stream ``before`` cursor for the next backfill attempt.

        The store filters with strict ``<``: setting cursor to the oldest
        item's timestamp yields strict ``<`` against all returned items. The
        over-fetch (``limit * OVER_FETCH_FACTOR + 1``) is sized so that this
        rarely loses ties at the boundary in normal traffic. When no items
        were returned, fall back to the prior cursor.

        Documented degradation (CodeRabbit on PR #2117): the cursor is
        timestamp-only, not composite ``(timestamp, item_id)``. When a
        single second has more than ``OVER_FETCH_FACTOR * limit + 1`` items
        of the same stream, the unfetched tail of that second is
        unreachable from this stream's pagination — the gateway logs
        ``backfill loop hit MAX_ATTEMPTS without filling page`` so an
        operator can spot the saturation case. Realistically, our
        producers emit a few rows per minute per strategy and our default
        ``MAX = 200`` admits 601 over-fetched items per attempt, so the
        bound is unreachable in normal operation. The proto contract for
        ``GetActivityFeed`` documents this explicitly so clients are not
        promised pagination guarantees the gateway cannot meet under
        adversarial load.
        """
        if not items:
            return fallback
        timestamps: list[datetime] = [
            ts for ts in (getattr(item, "timestamp", None) for item in items) if isinstance(ts, datetime)
        ]
        if not timestamps:
            return fallback
        return min(timestamps)

    @staticmethod
    def _validate_activity_feed_cursor(before_ts: int | None, before_id: str) -> str | None:
        """Reject malformed composite cursors (CodeRabbit review).

        The cursor is gateway-emitted in the form ``"<priority>:<kind>:<id>"``
        where priority ∈ {"0","1"} and kind ∈ {"T","L"}. A caller passing
        ``before_id`` without ``before_timestamp``, or an out-of-shape string,
        is bypassing the contract — return INVALID_ARGUMENT instead of
        silently corrupting pagination.

        Returns an error message string when the cursor is malformed, ``None``
        when it's valid (or absent).
        """
        if not before_id:
            return None
        if before_ts is None:
            return "before_id requires before_timestamp"
        parts = before_id.split(":", 2)
        if len(parts) != 3:
            return f"before_id must be '<priority>:<kind>:<id>'; got {before_id!r}"
        priority, kind, item_id = parts
        if priority not in (
            _ACTIVITY_FEED_KEY_LEDGER_PRIORITY,
            _ACTIVITY_FEED_KEY_TIMELINE_PRIORITY,
        ):
            return f"before_id priority must be '0' or '1'; got {priority!r}"
        if kind not in ("L", "T"):
            return f"before_id kind must be 'L' or 'T'; got {kind!r}"
        if not item_id:
            return "before_id item_id may not be empty"
        expected_kind = "L" if priority == _ACTIVITY_FEED_KEY_LEDGER_PRIORITY else "T"
        if kind != expected_kind:
            return f"before_id kind {kind!r} inconsistent with priority {priority!r}"
        return None

    @staticmethod
    def _compute_store_before(before_dt: datetime | None, before_id: str) -> datetime | None:
        """Translate the proto cursor to the strict-``<`` filter the stores expect.

        The proto cursor is integer-second; items can carry sub-second
        timestamps. Without a tie-breaker, a strict ``< before_dt`` push-down
        is correct. With a tie-breaker we must include items at exactly
        ``before_ts`` so the post-filter can pick the ones with composite
        keys lex-less-than ``before_id``. Bumping by 1s achieves that under
        the stores' strict-``<`` semantics.

        CodeRabbit: at the extreme upper edge of ``datetime`` range
        (``before_dt`` near year 9999 inclusive), ``+1s`` overflows
        ``datetime.fromtimestamp`` — fall back to ``before_dt`` unchanged
        and rely on the post-filter to handle the boundary tie. Out-of-range
        ``before_dt`` itself is already rejected upstream at the RPC.
        """
        if before_dt is None or not before_id:
            return before_dt
        try:
            return datetime.fromtimestamp(int(before_dt.timestamp()) + 1, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return before_dt

    @staticmethod
    def _load_timeline_for_feed(
        resolved_id: str,
        limit_plus_one: int,
        event_type_filter: str | None,
        store_before: datetime | None,
    ) -> list[Any]:
        try:
            return list(
                get_timeline_store().get_events(
                    deployment_id=resolved_id,
                    limit=limit_plus_one,
                    event_type=event_type_filter,
                    before=store_before,
                )
            )
        except Exception:
            logger.debug("GetActivityFeed: failed to load timeline events", exc_info=True)
            return []

    async def _load_ledger_for_feed(
        self,
        resolved_id: str,
        limit_plus_one: int,
        intent_type_filter: str | None,
        store_before: datetime | None,
    ) -> list[Any]:
        if self._state_manager is None:
            return []
        try:
            return await self._state_manager.get_ledger_entries(
                resolved_id,
                since=None,
                intent_type=intent_type_filter,
                limit=limit_plus_one,
                before=store_before,
            )
        except TypeError:
            return await self._load_ledger_fallback_no_before(
                resolved_id, limit_plus_one, intent_type_filter, store_before
            )
        except Exception:
            logger.debug("GetActivityFeed: failed to load ledger entries", exc_info=True)
            return []

    async def _load_ledger_fallback_no_before(
        self,
        resolved_id: str,
        limit_plus_one: int,
        intent_type_filter: str | None,
        store_before: datetime | None,
    ) -> list[Any]:
        if self._state_manager is None:
            return []
        try:
            entries = await self._state_manager.get_ledger_entries(
                resolved_id,
                since=None,
                intent_type=intent_type_filter,
                limit=limit_plus_one,
            )
            if store_before is not None:
                entries = [e for e in entries if e.timestamp < store_before]
            return entries
        except Exception:
            logger.debug("GetActivityFeed: ledger fallback failed", exc_info=True)
            return []

    @staticmethod
    def _build_feed_items(
        resolved_id: str,
        timeline_events: list[Any],
        ledger_entries: list[Any],
    ) -> list[tuple[gateway_pb2.ActivityFeedItem, str]]:
        """Compose ``ActivityFeedItem`` protos from both streams.

        Composite tie-breaker key carried in the cursor:
        ``"T:<event_id>"`` for timeline events, ``"L:<ledger_id>"`` for
        ledger rows. Dedup of timeline events against their referenced
        ledger row is intentionally NOT done here — it must happen during
        page-incremental selection so a ledger row that falls in the
        over-fetch tail can't suppress a timeline event whose ledger
        counterpart will only appear on a later page (CodeRabbit review).
        """
        items: list[tuple[gateway_pb2.ActivityFeedItem, str]] = []
        for event in timeline_events:
            items.append(_to_timeline_feed_item(event, resolved_id))
        for entry in ledger_entries:
            items.append(_to_ledger_feed_item(entry))
        return items

    @staticmethod
    def _select_page_with_incremental_dedup(
        sorted_items: list[tuple[gateway_pb2.ActivityFeedItem, str]],
        limit: int,
    ) -> tuple[list[tuple[gateway_pb2.ActivityFeedItem, str]], bool]:
        """Sort-order-resilient page selection with ledger-wins dedup
        (CodeRabbit on PR #2117).

        The contract from PRD-TimelineEvents §6.1 is "drop the timeline
        duplicate when its ledger counterpart is on the page." The earlier
        single-pass walk had a sort-order coupling: it only dropped a
        timeline event if the ledger row had ALREADY been emitted earlier in
        the walk. When the producer emits the timeline event after the
        ledger write, the timeline's ``datetime.now()`` can be a tick newer
        than the ledger's — so the timeline sorted FIRST in DESC order. The
        timeline got added; then the ledger got added; both ended up on the
        page.

        Fix: track a ``pending_timeline_refs`` map of timelines whose
        ``related_ledger_entry_id`` may still be encountered later in the
        walk. When the matching ledger arrives, POP the pending timeline and
        APPEND the ledger at its natural sort position (the ledger is older,
        so it sorts later — appending preserves DESC order).

        Strict page boundary: walking stops as soon as the page reaches
        ``limit``. Items past the page boundary do NOT trigger dedup —
        otherwise a ledger row in the over-fetch tail could promote itself
        onto the page and drop a newer timeline that should have stayed
        (the old ``test_ledger_in_over_fetch_tail_does_not_suppress_timeline``
        contract). The dedup contract is "if BOTH the ledger and the timeline
        would land in the page-sized window, the ledger wins"; if only the
        timeline would land, the timeline survives.
        """
        page: list[tuple[gateway_pb2.ActivityFeedItem, str]] = []
        seen_ledger_ids: set[str] = set()
        # One ledger row may replace multiple timeline events that reference it.
        pending_timeline_refs: dict[str, list[int]] = {}
        has_more = False

        def _drop_pending(lid: str) -> None:
            """Pop every page row whose timeline event referenced ``lid`` and
            keep ``pending_timeline_refs`` indices consistent with the
            shifted page list."""
            indices = pending_timeline_refs.pop(lid, None)
            if not indices:
                return
            # Descending pops preserve lower indices.
            removed_sorted = sorted(indices, reverse=True)
            for ridx in removed_sorted:
                page.pop(ridx)
            for ref, refs in list(pending_timeline_refs.items()):
                pending_timeline_refs[ref] = [i - sum(1 for r in removed_sorted if r < i) for i in refs]

        for item, key in sorted_items:
            if len(page) >= limit:
                # A ledger outside the page cannot evict a newer in-page timeline event.
                has_more = True
                break

            if item.kind == gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY:
                lid = item.ledger_entry.id
                if lid in seen_ledger_ids:
                    continue
                _drop_pending(lid)
                seen_ledger_ids.add(lid)
                page.append((item, key))
                continue

            ref = item.timeline_event.related_ledger_entry_id
            if ref and ref in seen_ledger_ids:
                continue
            idx = len(page)
            page.append((item, key))
            if ref:
                pending_timeline_refs.setdefault(ref, []).append(idx)

        return page, has_more

    @staticmethod
    def _apply_boundary_filter(
        items: list[tuple[gateway_pb2.ActivityFeedItem, str]],
        before_ts: int | None,
        before_id: str,
    ) -> list[tuple[gateway_pb2.ActivityFeedItem, str]]:
        """Second-precision boundary filter at the cursor.

        Items carry sub-second timestamps but the proto cursor is integer
        seconds. ``item.timestamp`` is already int-seconds (set during item
        construction), so comparisons are second-precise:
          * Without ``before_id``: strict ``< before_ts``.
          * With ``before_id`` (tie-breaker): strict ``< before_ts`` OR
            (``== before_ts`` AND composite key lex < cursor).
        """
        if before_ts is None:
            return items
        if before_id:
            return [
                (item, key)
                for (item, key) in items
                if item.timestamp < before_ts or (item.timestamp == before_ts and key < before_id)
            ]
        return [(item, key) for (item, key) in items if item.timestamp < before_ts]

    async def _build_snapshot_position_index(
        self,
        deployment_id: str,
    ) -> tuple[dict[str, dict[str, Any]], int]:
        """Return ``(position_id → snapshot dict, snapshot_taken_at_unix)``.

        Empty dict + 0 on any failure — caller treats both as "no snapshot
        valuation available" and renders POSITION_SOURCE_SNAPSHOT confidence
        accordingly. Extracted from GetPositions to keep its complexity below
        the project's CC=15 gate (VIB-4493 Phase 1 follow-up).
        """
        snapshot_by_id: dict[str, dict[str, Any]] = {}
        assert self._state_manager is not None
        try:
            latest = await self._state_manager.get_latest_snapshot(deployment_id)
        except Exception as e:
            logger.warning("get_latest_snapshot failed in GetPositions: %s", e)
            return snapshot_by_id, 0
        if latest is None:
            return snapshot_by_id, 0
        snapshot_taken_at_unix = int(latest.timestamp.timestamp()) if latest.timestamp else 0
        for pos in latest.positions or []:
            if hasattr(pos, "to_dict"):
                pos_dict = pos.to_dict()
            elif isinstance(pos, dict):
                pos_dict = dict(pos)
            else:
                try:
                    pos_dict = dict(vars(pos))
                except TypeError:
                    logger.debug(
                        "Skipping snapshot position without mappable fields: %r",
                        type(pos).__name__,
                    )
                    continue
            pos_id = pos_dict.get("position_id") or pos_dict.get("handle") or pos_dict.get("symbol", "")
            if pos_id:
                snapshot_by_id[str(pos_id)] = pos_dict
        return snapshot_by_id, snapshot_taken_at_unix

    async def _collect_cutover_derivations(
        self,
        *,
        deployment_id: str,
        accounting_categories: set[str],
        registry_rows: list[dict[str, Any]],
        now_unix: int,
    ) -> dict[str, Any]:
        """Build the per-category cutover derivations map for GetPositions.

        Returns category → ``CutoverDerivation``. Extracted from GetPositions
        to keep its complexity below the project's CC=15 gate; the per-category
        try/except + cutover lookup + max() reduction collapses to a single
        await + comprehension at the call site.
        """
        from almanak.gateway.services._dashboard_phase1 import (
            CutoverDerivation,
            cutover_lookup_key,
            derive_cutover_state,
        )

        assert self._state_manager is not None
        cutover_by_category: dict[str, CutoverDerivation] = {}
        for category in accounting_categories:
            if not category:
                continue
            primitive, cutover_key = cutover_lookup_key(category)
            try:
                ms_row = await self._state_manager.get_migration_state(
                    deployment_id=deployment_id,
                    primitive=primitive,
                    cutover_key=cutover_key,
                )
            except Exception as e:
                logger.warning(
                    "get_migration_state failed for (%s, %s) in GetPositions: %s",
                    primitive,
                    cutover_key,
                    e,
                )
                ms_row = None
            last_reconciled_block = max(
                (
                    int(row.get("last_reconciled_at_block") or 0)
                    for row in registry_rows
                    if row.get("accounting_category") == category
                ),
                default=0,
            )
            state = derive_cutover_state(
                ms_row,
                last_reconciled_unix_seconds=0,
                now_unix_seconds=now_unix,
            )
            cutover_by_category[category] = CutoverDerivation(
                state=state,
                migration_state_row=ms_row,
                last_reconciled_at_block=last_reconciled_block,
                last_reconciled_unix_seconds=0,
            )
        return cutover_by_category

    async def GetPositions(
        self,
        request: gateway_pb2.GetPositionsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetPositionsResponse:
        """Registry-authoritative identity + snapshot-authoritative valuation.

        Identity from `position_registry` (via `get_position_registry_open_rows`).
        Valuation from latest portfolio_snapshot (via `get_latest_snapshot`,
        positions matched by `position_id` / `handle`). Cutover state per
        accounting_category derived from `migration_state` (via
        `get_migration_state`).

        Phase 1 v1 constraints (documented in design doc v5 and Phase 1A audit):
          - Returns OPEN positions only. Closed/reorg_invalidated rows require
            a new state-manager method (deferred to Phase 1+1).
          - `cutover_key` is currently always `'lp'` for LP categories until
            VIB-4202/4209/4501 add typed cutover keys.
          - REGISTRY_AUTHORITATIVE promotion (multi-snapshot stability gate)
            is not asserted in v1; handler reports BACKFILL_COMPLETE for
            complete+fresh, leaves the multi-snapshot stability to a future
            aggregation layer.
        """
        from almanak.gateway.services._dashboard_phase1 import (
            build_authoritative_positions as _build_authoritative_positions,
        )
        from almanak.gateway.services._dashboard_phase1 import (
            build_cutover_state_entry,
        )

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning("Invalid deployment_id in GetPositions: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetPositionsResponse()

        await self._ensure_initialized()

        if self._state_manager is None:
            return gateway_pb2.GetPositionsResponse()

        chain_filter = (request.chain or "").strip() or None
        primitive_filter = (request.primitive or "").strip() or None
        accounting_category_filter = (request.accounting_category or "").strip() or None

        try:
            registry_rows = await self._state_manager.get_position_registry_open_rows(
                deployment_id,
                chain=chain_filter,
                primitive=primitive_filter,
                accounting_category=accounting_category_filter,
            )
        except Exception as e:
            logger.warning("get_position_registry_open_rows failed in GetPositions: %s", e)
            registry_rows = []

        # Snapshot position_id matches the registry handle used by the backfill writer.
        snapshot_by_id, snapshot_taken_at_unix = await self._build_snapshot_position_index(deployment_id)

        now_unix = int(datetime.now(tz=UTC).timestamp())
        accounting_categories = {row.get("accounting_category", "") for row in registry_rows}
        if accounting_category_filter:
            accounting_categories.add(accounting_category_filter)
        cutover_by_category = await self._collect_cutover_derivations(
            deployment_id=deployment_id,
            accounting_categories=accounting_categories,
            registry_rows=registry_rows,
            now_unix=now_unix,
        )

        positions = _build_authoritative_positions(
            registry_rows=registry_rows,
            cutover_by_category=cutover_by_category,
            snapshot_by_id=snapshot_by_id,
            snapshot_taken_at_unix=snapshot_taken_at_unix,
        )

        # Always return cutover state so renderers can label each category.
        cutover_entries = [
            build_cutover_state_entry(accounting_category=category, derivation=derivation)
            for category, derivation in cutover_by_category.items()
        ]

        if request.status != gateway_pb2.POSITION_STATUS_UNSPECIFIED:
            positions = [p for p in positions if p.status == request.status]

        return gateway_pb2.GetPositionsResponse(
            positions=positions,
            cutover_states=cutover_entries,
        )

    async def GetPositionRangeHistory(
        self,
        request: gateway_pb2.GetPositionRangeHistoryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetPositionRangeHistoryResponse:
        """Per-position range / fee / balance history.

        Source-routes by primitive:
          - LP, PERP   → `position_events` via `get_position_history(...)`
          - LENDING    → `accounting_events` (Phase 1 v1: returns stub,
                         pending VIB-4501 for the state-manager method)
          - SWAP / PREDICTION → empty + RANGE_HISTORY_NA_STUB

        Lookup primary key per v5 design: `(deployment_id, chain,
        accounting_category, handle | physical_identity_hash)`. v1 honors the
        physical_identity_hash when supplied; falls back to handle (display
        key) otherwise. `position_id` from the wire is treated as
        equivalent to handle.
        """
        from almanak.gateway.services._dashboard_phase1 import (
            LENDING_RANGE_HISTORY_V1_STUB,
            RANGE_HISTORY_NA_STUB,
            build_range_history_entry_from_position_event,
            filter_events_by_time_window,
            infer_primitive_from_accounting_category,
        )

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning("Invalid deployment_id in GetPositionRangeHistory: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetPositionRangeHistoryResponse()

        if not request.chain:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("chain is required")
            return gateway_pb2.GetPositionRangeHistoryResponse()
        if not request.accounting_category:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("accounting_category is required")
            return gateway_pb2.GetPositionRangeHistoryResponse()
        if not request.handle and not request.physical_identity_hash:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("either handle or physical_identity_hash is required")
            return gateway_pb2.GetPositionRangeHistoryResponse()

        await self._ensure_initialized()

        if self._state_manager is None:
            return gateway_pb2.GetPositionRangeHistoryResponse(stub_message="state manager unavailable")

        primitive = infer_primitive_from_accounting_category(request.accounting_category)

        # Unsupported history sources return an explicit renderer-facing stub.
        if primitive == "lending":
            return gateway_pb2.GetPositionRangeHistoryResponse(stub_message=LENDING_RANGE_HISTORY_V1_STUB)
        if primitive == "swap":
            return gateway_pb2.GetPositionRangeHistoryResponse(stub_message=RANGE_HISTORY_NA_STUB)

        # Handles can collide across chains and categories, so resolve then post-filter.
        position_id = await self._resolve_position_history_key(
            deployment_id=deployment_id,
            chain=request.chain,
            accounting_category=request.accounting_category,
            handle=request.handle,
            physical_identity_hash=request.physical_identity_hash,
        )
        if not position_id:
            return gateway_pb2.GetPositionRangeHistoryResponse(
                stub_message=(
                    "no registry row matched the supplied identifier on this "
                    f"chain / accounting_category ({request.chain} / "
                    f"{request.accounting_category})"
                ),
            )

        try:
            events = await self._state_manager.get_position_history(
                deployment_id,
                position_id,
            )
        except Exception as e:
            logger.warning("get_position_history failed in GetPositionRangeHistory: %s", e)
            events = []

        # Legacy unstamped events remain eligible after registry-scoped identity resolution.
        def _matches(e: dict, key: str, expected: str) -> bool:
            if not expected:
                return True
            value = str(e.get(key) or "")
            return value == "" or value == expected

        events = [
            e
            for e in events
            if _matches(e, "chain", request.chain) and _matches(e, "accounting_category", request.accounting_category)
        ]

        filtered_events = filter_events_by_time_window(
            events,
            from_unix_seconds=request.from_unix_seconds,
            to_unix_seconds=request.to_unix_seconds,
        )
        entries = [build_range_history_entry_from_position_event(e) for e in filtered_events]

        return gateway_pb2.GetPositionRangeHistoryResponse(
            entries=entries,
            stub_message="" if entries else "no events found for this position in the requested window",
        )

    async def _resolve_position_history_key(
        self,
        *,
        deployment_id: str,
        chain: str,
        accounting_category: str,
        handle: str,
        physical_identity_hash: str,
    ) -> str:
        """Translate the wire identifier to the ``position_events.position_id`` value.

        Behaviour:
          * ``physical_identity_hash`` wins when both are supplied (it's
            the stable primary key). We look it up in the position_registry
            filtered by (deployment_id, chain, accounting_category) and
            return the matching row's ``handle`` — which IS the
            ``position_id`` used in position_events (writer convention,
            cutover.py:69 / backfill.py:861).
          * If only ``handle`` is supplied: return it verbatim.
          * Empty string return = "no row matched"; caller surfaces a
            stub message rather than running an empty query.

        State manager unavailability (`None`) returns the raw handle so
        the degraded path still works.
        """
        if not physical_identity_hash:
            return handle
        if self._state_manager is None:
            return handle
        try:
            rows = await self._state_manager.get_position_registry_open_rows(
                deployment_id,
                chain=chain or None,
                accounting_category=accounting_category or None,
            )
        except Exception as e:
            logger.warning(
                "get_position_registry_open_rows failed in _resolve_position_history_key: %s",
                e,
            )
            return handle
        for row in rows:
            if row.get("physical_identity_hash") == physical_identity_hash:
                resolved = row.get("handle") or row.get("physical_identity_hash") or ""
                return str(resolved)
        return ""

    async def _resolve_chain_and_wallet(self, deployment_id: str) -> tuple[str, str]:
        """Resolve (chain, wallet_address) for a reconciliation call.

        v1 sources both from the latest portfolio_snapshot. Multi-chain
        strategies get reconciled against their primary chain only — v1
        limit, documented. Returns ("", "") when neither can be resolved
        (caller surfaces this as FAILED_PRECONDITION).
        """
        if self._state_manager is None:
            return ("", "")
        try:
            snap = await self._state_manager.get_latest_snapshot(deployment_id)
        except Exception as e:
            logger.warning("get_latest_snapshot failed in _resolve_chain_and_wallet: %s", e)
            return ("", "")
        if snap is None:
            return ("", "")
        chain = getattr(snap, "chain", "") or ""
        wallet = ""
        # Multi-chain snapshots may place the wallet on their first balance.
        wallet = getattr(snap, "wallet_address", "") or ""
        if not wallet and getattr(snap, "wallet_balances", None):
            try:
                first = snap.wallet_balances[0]
                wallet = getattr(first, "wallet_address", "") or ""
            except (IndexError, AttributeError):
                wallet = ""
        return (chain, wallet)

    async def _invoke_reconcile(
        self,
        *,
        deployment_id: str,
        chain: str,
        wallet_address: str,
        apply: bool,
        operator_note: str = "",
        trigger: str = "dashboard",
    ) -> gateway_pb2.ReconcileResponse | None:
        """Call PositionService.Reconcile in-process.

        Returns None when position_servicer is unwired (degraded mode —
        unit-test friendly, surfaced as empty findings to the caller).
        Wired by GatewayServer._register_services after construction.
        """
        if self.position_servicer is None:
            logger.warning("position_servicer unwired — Reconcile RPC unavailable in this DashboardService instance")
            return None
        req = gateway_pb2.ReconcileRequest(
            deployment_id=deployment_id,
            chain=chain,
            wallet_address=wallet_address,
            primitives=["lp"],
            apply=apply,
            operator_note=operator_note,
            trigger=trigger,
        )
        import grpc as _grpc

        class _PassthroughContext:
            """Minimal ServicerContext-like — captures set_code / set_details
            without aborting (in-process callers handle errors via response shape)."""

            def __init__(self) -> None:
                self.code: _grpc.StatusCode | None = None
                self.details: str = ""

            def set_code(self, code: _grpc.StatusCode) -> None:
                self.code = code

            def set_details(self, details: str) -> None:
                self.details = details

            def set_trailing_metadata(self, _metadata: Any) -> None:
                pass

            async def abort(self, _code: Any, _details: str) -> None:
                raise _grpc.RpcError(f"Reconcile aborted: {_details}")

        ctx = _PassthroughContext()
        return await self.position_servicer.Reconcile(req, ctx)

    async def GetReconciliationReport(
        self,
        request: gateway_pb2.GetReconciliationReportRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetReconciliationReportResponse:
        """Three-way diff across ledger / snapshots / registry.

        LP-only in v1 — non-LP primitives surface PrimitiveCoverageStub
        cards (see _dashboard_phase1.PER_PRIMITIVE_STUBS for the catalogue).
        5-second gateway-side cache per (deployment_id) per v5 design.
        """
        from almanak.gateway.services._dashboard_phase1 import (
            ReconciliationReportCache,
            reconcile_response_to_report,
        )

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning("Invalid deployment_id in GetReconciliationReport: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetReconciliationReportResponse()

        await self._ensure_initialized()
        now_unix = int(datetime.now(tz=UTC).timestamp())

        if self._reconciliation_report_cache is None:
            self._reconciliation_report_cache = ReconciliationReportCache(ttl_seconds=5)

        cached = self._reconciliation_report_cache.get(deployment_id, now_unix_seconds=now_unix)
        if cached is not None:
            return cached

        chain, wallet = await self._resolve_chain_and_wallet(deployment_id)
        if not chain or not wallet:
            return gateway_pb2.GetReconciliationReportResponse(
                as_of=datetime.fromtimestamp(now_unix, tz=UTC).isoformat(),
            )

        reconcile_response = await self._invoke_reconcile(
            deployment_id=deployment_id,
            chain=chain,
            wallet_address=wallet,
            apply=False,
            trigger="dashboard",
        )
        if reconcile_response is None:
            return gateway_pb2.GetReconciliationReportResponse(
                as_of=datetime.fromtimestamp(now_unix, tz=UTC).isoformat(),
            )

        report = reconcile_response_to_report(
            reconcile_response=reconcile_response,
            now_unix_seconds=now_unix,
        )
        self._reconciliation_report_cache.put(deployment_id, report, now_unix_seconds=now_unix)
        return report

    async def _require_operator_authorization(self, context: grpc.aio.ServicerContext) -> bool:
        """Server-side gate for the three mutation RPCs (Codex review fix).

        The single-token gateway interceptor (`almanak/gateway/auth.py`)
        treats every caller equally — any client holding
        `ALMANAK_GATEWAY_AUTH_TOKEN` can in principle invoke
        `PreviewReconcile` / `ApplyReconcile` / `RefreshRegistryFromChain`.
        The client-side two-tier split (`DashboardServiceClient` vs
        `OperatorDashboardServiceClient`) is only a typing convention.

        This helper adds an opt-in second factor for hosted multi-tenant
        deployments:

          * If env `ALMANAK_GATEWAY_OPERATOR_TOKEN` is unset → return
            True. Single-user / local deployments keep current behaviour.
          * If env is set → require the matching value in the request's
            `x-operator-token` metadata header. Mismatch / missing →
            abort with PERMISSION_DENIED.

        A proper RBAC system (per-RPC role check, per-strategy scope,
        signed bearer tokens) is the next ticket. This is the
        minimum-viable defense-in-depth that closes Codex's HIGH
        finding without forcing every existing deployment to rotate
        tokens. Reads via the central ``GatewaySettings.operator_token``
        (env ``ALMANAK_GATEWAY_OPERATOR_TOKEN``) — not ``os.environ``
        directly — to respect the project's config-boundary lint.
        """
        required = (self.settings.operator_token or "").strip()
        if not required:
            # An unset second factor intentionally preserves local and single-user behavior.
            return True

        metadata = dict(context.invocation_metadata() or [])
        provided = metadata.get("x-operator-token", "")
        if isinstance(provided, bytes):
            provided = provided.decode("utf-8", errors="ignore")
        if provided != required:
            logger.warning("operator-only RPC rejected: x-operator-token missing or wrong")
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                "operator-only RPC requires the x-operator-token metadata header",
            )
            return False
        return True

    async def PreviewReconcile(
        self,
        request: gateway_pb2.PreviewReconcileRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PreviewReconcileResponse:
        """Dry-run reconciliation. Returns a preview_token + diff buckets.

        Token bound to a (registry_count, registry_max_block, ledger_max_id,
        source_block_number) fingerprint. ApplyReconcile recomputes the
        fingerprint and rejects with STATE_DRIFT if it has changed.

        Operator-only. v1 token TTL: 5 minutes.
        """
        if not await self._require_operator_authorization(context):
            return gateway_pb2.PreviewReconcileResponse()
        from almanak.gateway.services._dashboard_phase1 import (
            PreviewTokenStore,
            build_per_primitive_stubs,
            compute_state_fingerprint,
        )

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning("Invalid deployment_id in PreviewReconcile: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.PreviewReconcileResponse()

        await self._ensure_initialized()
        now_unix = int(datetime.now(tz=UTC).timestamp())

        if self._preview_token_store is None:
            self._preview_token_store = PreviewTokenStore(default_ttl_seconds=300)
        self._preview_token_store.gc_expired(now_unix_seconds=now_unix)

        chain, wallet = await self._resolve_chain_and_wallet(deployment_id)
        if not chain or not wallet:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("could not resolve chain/wallet from latest snapshot — strategy may not be initialized")
            return gateway_pb2.PreviewReconcileResponse()

        reconcile_response = await self._invoke_reconcile(
            deployment_id=deployment_id,
            chain=chain,
            wallet_address=wallet,
            apply=False,
            trigger="dashboard",
        )
        if reconcile_response is None:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("position_servicer unwired on this gateway instance")
            return gateway_pb2.PreviewReconcileResponse()

        registry_rows: list[dict[str, Any]] = []
        ledger_max_id = ""
        if self._state_manager is not None:
            try:
                registry_rows = await self._state_manager.get_position_registry_open_rows(deployment_id)
            except Exception as e:
                logger.warning("get_position_registry_open_rows failed in PreviewReconcile: %s", e)
                registry_rows = []
            try:
                latest_entries = await self._state_manager.get_ledger_entries(
                    deployment_id=deployment_id,
                    limit=1,
                )
                if latest_entries:
                    ledger_max_id = str(getattr(latest_entries[0], "id", "") or "")
            except Exception as e:
                logger.warning("get_ledger_entries failed in PreviewReconcile: %s", e)

        fingerprint = compute_state_fingerprint(
            registry_rows=registry_rows,
            ledger_max_id=ledger_max_id,
            source_block_number=reconcile_response.source_block_number,
        )
        token, expires_at = self._preview_token_store.issue(
            deployment_id=deployment_id,
            fingerprint=fingerprint,
            reconcile_response=reconcile_response,
            now_unix_seconds=now_unix,
        )

        primitive_stubs = build_per_primitive_stubs(
            {err.primitive for err in reconcile_response.primitive_errors if err.code == "PARSER_UNSUPPORTED"}
        )

        return gateway_pb2.PreviewReconcileResponse(
            preview_token=token,
            matched=list(reconcile_response.matched),
            phantom_missing=list(reconcile_response.phantom_missing),
            stranded=list(reconcile_response.stranded),
            primitive_stubs=primitive_stubs,
            reconciliation_id=reconcile_response.reconciliation_id,
            source_block_number=reconcile_response.source_block_number,
            expires_at_unix_seconds=expires_at,
        )

    async def ApplyReconcile(
        self,
        request: gateway_pb2.ApplyReconcileRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.ApplyReconcileResponse:
        """Apply a previously-issued PreviewReconcile.

        Validates token → fingerprint match → calls Reconcile(apply=true).
        Returns SUCCESS / PARTIAL_SUCCESS / STATE_DRIFT / EXPIRED / NOT_FOUND
        per the ApplyReconcileResponse.result string contract.

        Operator-only.
        """
        if not await self._require_operator_authorization(context):
            return gateway_pb2.ApplyReconcileResponse(
                result="PERMISSION_DENIED",
                detail="operator-only RPC",
            )

        from almanak.gateway.services._dashboard_phase1 import (
            PreviewTokenStore,
            categorize_apply_result,
            compute_state_fingerprint,
        )

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning("Invalid deployment_id in ApplyReconcile: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.ApplyReconcileResponse(result="INVALID_ARGUMENT", detail=str(e))

        if not request.preview_token:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("preview_token is required")
            return gateway_pb2.ApplyReconcileResponse(result="INVALID_ARGUMENT", detail="preview_token is required")

        await self._ensure_initialized()
        now_unix = int(datetime.now(tz=UTC).timestamp())

        if self._preview_token_store is None:
            self._preview_token_store = PreviewTokenStore(default_ttl_seconds=300)

        status, entry = self._preview_token_store.consume(
            token=request.preview_token,
            deployment_id=deployment_id,
            now_unix_seconds=now_unix,
        )
        if status != "OK" or entry is None:
            return gateway_pb2.ApplyReconcileResponse(
                result=status,
                detail={
                    "NOT_FOUND": "preview_token unrecognized — likely expired or gateway restarted; re-issue PreviewReconcile",
                    "EXPIRED": "preview_token TTL elapsed; re-issue PreviewReconcile",
                    "WRONG_STRATEGY": "preview_token does not belong to this strategy",
                }.get(status, "unknown token state"),
            )

        chain, wallet = await self._resolve_chain_and_wallet(deployment_id)
        if not chain or not wallet:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("could not resolve chain/wallet from latest snapshot")
            return gateway_pb2.ApplyReconcileResponse(
                result="STATE_DRIFT",
                detail="chain/wallet resolution failed between preview and apply",
            )

        # Sample chain first, then registry and ledger; reject drift before any mutation.
        # Reconcile reports drift that occurs in the remaining compare-to-apply race window.
        dry_run_response = await self._invoke_reconcile(
            deployment_id=deployment_id,
            chain=chain,
            wallet_address=wallet,
            apply=False,
            operator_note=f"ApplyReconcile drift check via preview_token={request.preview_token[:16]}...",
            trigger="dashboard",
        )
        if dry_run_response is None:
            return gateway_pb2.ApplyReconcileResponse(
                result="STATE_DRIFT",
                detail="position_servicer unwired on this gateway instance",
            )

        registry_rows: list[dict[str, Any]] = []
        ledger_max_id = ""
        if self._state_manager is not None:
            try:
                registry_rows = await self._state_manager.get_position_registry_open_rows(deployment_id)
            except Exception as e:
                logger.warning("get_position_registry_open_rows failed in ApplyReconcile fingerprint: %s", e)
            try:
                latest_entries = await self._state_manager.get_ledger_entries(
                    deployment_id=deployment_id,
                    limit=1,
                )
                if latest_entries:
                    ledger_max_id = str(getattr(latest_entries[0], "id", "") or "")
            except Exception as e:
                logger.warning("get_ledger_entries failed in ApplyReconcile fingerprint: %s", e)

        current_fingerprint = compute_state_fingerprint(
            registry_rows=registry_rows,
            ledger_max_id=ledger_max_id,
            source_block_number=dry_run_response.source_block_number,
        )
        if not entry.fingerprint.equals(current_fingerprint):
            return gateway_pb2.ApplyReconcileResponse(
                result="STATE_DRIFT",
                detail=(
                    "state advanced between preview and apply (registry / ledger / "
                    "chain head moved); no changes applied. Re-issue PreviewReconcile."
                ),
                reconciliation_id=dry_run_response.reconciliation_id,
            )

        reconcile_response = await self._invoke_reconcile(
            deployment_id=deployment_id,
            chain=chain,
            wallet_address=wallet,
            apply=True,
            operator_note=f"ApplyReconcile via preview_token={request.preview_token[:16]}...",
            trigger="dashboard",
        )
        if reconcile_response is None:
            return gateway_pb2.ApplyReconcileResponse(
                result="STATE_DRIFT",
                detail="position_servicer unwired on this gateway instance",
            )

        result_code, detail = categorize_apply_result(
            reconcile_response=reconcile_response,
            fingerprint_matched=True,
        )

        return gateway_pb2.ApplyReconcileResponse(
            result=result_code,
            detail=detail,
            rebuilt=list(reconcile_response.rebuilt),
            primitive_errors=list(reconcile_response.primitive_errors),
            reconciliation_id=reconcile_response.reconciliation_id,
        )

    async def RefreshRegistryFromChain(
        self,
        request: gateway_pb2.RefreshRegistryFromChainRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.RefreshRegistryFromChainResponse:
        """Force fresh on-chain reads for every position in position_registry.

        Per v5 design + A2 audit: PositionService.Reconcile has NO
        concurrency guard, so the per-strategy in-flight lock lives here.
        Implementation is a thin wrapper around Reconcile(apply=true) —
        the same engine that PreviewReconcile/ApplyReconcile use, but
        invoked directly without a preview_token (operator explicitly
        wants a fresh chain read + register-divergence-as-events pass).

        Rate-limited: one in-flight per strategy. Concurrent calls return
        RATE_LIMITED with the in-flight reconciliation_id in detail.

        Operator-only.
        """
        if not await self._require_operator_authorization(context):
            return gateway_pb2.RefreshRegistryFromChainResponse(
                result="PERMISSION_DENIED",
                detail="operator-only RPC",
            )

        import asyncio as _asyncio

        try:
            deployment_id = validate_deployment_id(request.deployment_id)
        except ValidationError as e:
            logger.warning("Invalid deployment_id in RefreshRegistryFromChain: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.RefreshRegistryFromChainResponse(result="INVALID_ARGUMENT", detail=str(e))

        await self._ensure_initialized()

        lock = self._registry_refresh_locks.get(deployment_id)
        if lock is None:
            lock = _asyncio.Lock()
            self._registry_refresh_locks[deployment_id] = lock

        if lock.locked():
            return gateway_pb2.RefreshRegistryFromChainResponse(
                result="RATE_LIMITED",
                detail="another RefreshRegistryFromChain is in flight for this strategy",
            )

        async with lock:
            chain, wallet = await self._resolve_chain_and_wallet(deployment_id)
            if not chain or not wallet:
                return gateway_pb2.RefreshRegistryFromChainResponse(
                    result="FAILED",
                    detail="could not resolve chain/wallet from latest snapshot",
                )

            reconcile_response = await self._invoke_reconcile(
                deployment_id=deployment_id,
                chain=chain,
                wallet_address=wallet,
                apply=True,
                operator_note="RefreshRegistryFromChain (explicit operator action)",
                trigger="dashboard",
            )
            if reconcile_response is None:
                return gateway_pb2.RefreshRegistryFromChainResponse(
                    result="FAILED",
                    detail="position_servicer unwired on this gateway instance",
                )

            positions_refreshed = int(reconcile_response.matched_count) + int(reconcile_response.rebuilt_count)
            events_emitted = int(reconcile_response.rebuilt_count)

            return gateway_pb2.RefreshRegistryFromChainResponse(
                result="SUCCESS",
                detail=f"refreshed {positions_refreshed} positions, emitted {events_emitted} events",
                positions_refreshed=positions_refreshed,
                events_emitted=events_emitted,
                source_block_number=reconcile_response.source_block_number,
                reconciliation_id=reconcile_response.reconciliation_id,
            )
