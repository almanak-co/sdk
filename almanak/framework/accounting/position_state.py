"""Per-iteration position-state materializer (AttemptNo17 §3 D4, Track C).

The Layer-1/3/5 writers handle EVENT-DRIVEN money flow (LP open/close, swap,
supply/borrow, perp open/close) — they fire when a transaction lands.

But several Accountant Test cells are CONTINUOUSLY-ACCRUED, not event-driven:
- L1 (net carry over hold period) needs interest accrued every block.
- L2 (HF / LTV trajectory) needs HF read every iteration.
- L5 (APR snapshot) needs the active rate at each iteration.
- P2 (cumulative funding during hold) needs funding accrual per iteration.
- P4 (liquidation buffer over time) needs the protocol's liq price.

Reading these only at REPAY/CLOSE gives the integral; reading them per
snapshot gives the curve. Without per-snapshot reads, those cells are
unanswerable.

The materializer runs alongside `_capture_portfolio_snapshot()` and writes
a typed row per open position into `position_state_snapshots`.

## Hosted-mode short-circuit (Codex Finding 2)

VIB-3763's schema-contract check refuses to start the gateway when the live
backend lacks a column the SDK needs. Relying on that check as a deployment
strategy would brick every hosted strategy the moment we merge — so this
module returns ``None`` in hosted mode (until Track B's metrics-database
migration deploys the new table).

Local-mode runs get the full materialization. Hosted-mode runs get an
explicit no-op + a paging-grade gauge so operators can see continuous
fields are unavailable. Once Track B deploys, a one-line follow-up removes
the short-circuit and hosted runs flip the same cells from UNAVAILABLE to
HIGH.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from almanak.framework.deployment.mode import is_hosted

logger = logging.getLogger(__name__)


PositionType = Literal["LP", "LENDING", "PERP"]
ConfidenceLiteral = Literal["HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"]


@dataclass(frozen=True)
class PositionStateRow:
    """One row per open position per iteration in `position_state_snapshots`."""

    snapshot_id: int | None  # FK → portfolio_snapshots.id; set after snapshot row insert
    strategy_id: str
    deployment_id: str
    cycle_id: str
    timestamp: datetime
    position_id: str
    position_type: PositionType

    # ─── LP ──────────────────────────────────────────────────────────────
    current_tick: int | None = None
    in_range: bool | None = None
    liquidity: int | None = None
    sqrt_price_x96: int | None = None

    # ─── Lending ─────────────────────────────────────────────────────────
    supply_balance: Decimal | None = None
    borrow_balance: Decimal | None = None
    health_factor: Decimal | None = None
    supply_apy_pct: Decimal | None = None
    borrow_apy_pct: Decimal | None = None
    interest_accrued_since_last: Decimal | None = None

    # ─── Perp ────────────────────────────────────────────────────────────
    mark_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    funding_accrued_since_last: Decimal | None = None
    liquidation_price: Decimal | None = None
    margin_utilisation_pct: Decimal | None = None

    # ─── Common ──────────────────────────────────────────────────────────
    value_confidence: ConfidenceLiteral = "ESTIMATED"
    delta_vs_protocol_pct: Decimal | None = None  # G14 reconciliation cell

    # Versioning
    schema_version: int = 1
    formula_version: int = 1
    matching_policy_version: int = 1


def materialise_position_state(
    *,
    position: Any,
    market: Any,
    prices: dict[str, Any] | None,
    strategy_id: str,
    deployment_id: str,
    cycle_id: str,
    timestamp: datetime,
) -> PositionStateRow | None:
    """Read protocol state for a single open position into a typed row.

    Returns ``None`` in hosted mode (Codex Finding 2). Local mode reads
    the protocol-specific time-varying fields off ``market`` /
    ``MarketSnapshot`` (no live chain calls — those happened at snapshot
    time and are cached on ``market``).

    The caller (``_capture_portfolio_snapshot``) is responsible for batching
    rows and writing them in the same transaction as the snapshot row, so
    the time series can never have gaps that look like "no change" but are
    actually crashes.
    """
    if is_hosted():
        # Hosted strategies do not get continuous-accrual tracking until
        # the metrics-database migration for `position_state_snapshots`
        # lands (Track B / VIB-3844). The gateway logs the no-op at boot
        # and the `accounting_continuous_fields_unavailable` gauge fires
        # in dashboards. Wire the gauge here so the docstring claim
        # ("the gauge fires in dashboards") is actually true — without
        # this call, the gauge stayed at zero and operators couldn't
        # distinguish "no positions" from "positions exist but
        # continuous-accrual is short-circuited".
        primitive = _classify_position(position) or "unknown"
        report_continuous_fields_unavailable(strategy_id, primitive.lower())
        return None

    pos_type = _classify_position(position)
    if pos_type is None:
        return None

    pos_id = str(getattr(position, "position_id", "") or getattr(position, "id", ""))
    if not pos_id:
        return None

    common = {
        "snapshot_id": None,  # caller sets after parent insert
        "strategy_id": strategy_id,
        "deployment_id": deployment_id,
        "cycle_id": cycle_id,
        "timestamp": timestamp,
        "position_id": pos_id,
        "position_type": pos_type,
    }

    if pos_type == "LP":
        return _materialise_lp(position, market, prices, common)
    if pos_type == "LENDING":
        return _materialise_lending(position, market, prices, common)
    if pos_type == "PERP":
        return _materialise_perp(position, market, prices, common)
    return None


def _classify_position(position: Any) -> PositionType | None:
    """Classify a PositionInfo / equivalent into the materializer's enum."""
    pt = getattr(position, "position_type", None) or getattr(position, "type", None) or ""
    pt = str(pt).upper()
    if pt in ("LP", "UNI_V3", "UNISWAP_V3", "AERODROME", "AERODROME_LP", "TRADERJOE_LP"):
        return "LP"
    if pt in ("LENDING", "AAVE_V3", "AAVE", "MORPHO", "COMPOUND_V3", "COMPOUND"):
        return "LENDING"
    if pt in ("PERP", "GMX", "GMX_V2", "DRIFT", "HYPERLIQUID"):
        return "PERP"
    return None


def _dec(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _materialise_lp(
    position: Any, market: Any, prices: dict[str, Any] | None, common: dict[str, Any]
) -> PositionStateRow:
    details = getattr(position, "details", None) or {}
    if not isinstance(details, dict):
        details = {}
    return PositionStateRow(
        **common,
        current_tick=_int(details.get("current_tick") or details.get("tick_current")),
        in_range=details.get("in_range"),
        liquidity=_int(details.get("liquidity")),
        sqrt_price_x96=_int(details.get("sqrt_price_x96")),
        value_confidence=_confidence_from(details, prices),
    )


def _materialise_lending(
    position: Any, market: Any, prices: dict[str, Any] | None, common: dict[str, Any]
) -> PositionStateRow:
    details = getattr(position, "details", None) or {}
    if not isinstance(details, dict):
        details = {}
    return PositionStateRow(
        **common,
        supply_balance=_dec(details.get("supply_balance")),
        borrow_balance=_dec(details.get("borrow_balance")),
        health_factor=_dec(details.get("health_factor") or details.get("hf")),
        supply_apy_pct=_dec(details.get("supply_apy_pct") or details.get("supply_apy")),
        borrow_apy_pct=_dec(details.get("borrow_apy_pct") or details.get("borrow_apy")),
        interest_accrued_since_last=_dec(details.get("interest_accrued_since_last")),
        value_confidence=_confidence_from(details, prices),
    )


def _materialise_perp(
    position: Any, market: Any, prices: dict[str, Any] | None, common: dict[str, Any]
) -> PositionStateRow:
    details = getattr(position, "details", None) or {}
    if not isinstance(details, dict):
        details = {}
    return PositionStateRow(
        **common,
        mark_price=_dec(details.get("mark_price")),
        unrealized_pnl=_dec(details.get("unrealized_pnl")),
        funding_accrued_since_last=_dec(details.get("funding_accrued_since_last")),
        liquidation_price=_dec(details.get("liquidation_price")),
        margin_utilisation_pct=_dec(details.get("margin_utilisation_pct")),
        value_confidence=_confidence_from(details, prices),
    )


def _int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _confidence_from(details: dict[str, Any], prices: dict[str, Any] | None) -> ConfidenceLiteral:
    """Infer confidence from how the details were populated.

    HIGH: protocol read landed cleanly + every priced asset is HIGH.
    ESTIMATED: protocol read had to fall back, OR any priced asset is
        ``ESTIMATED`` / has unrecognised confidence.
    STALE: any priced asset is explicitly ``STALE``.
    UNAVAILABLE: protocol read failed entirely, OR every priced asset is
        ``UNAVAILABLE``.

    The previous implementation collapsed any non-empty ``prices`` dict to
    HIGH — but ``prices`` is the per-asset confidence map (the same shape
    ``PriceSnapshot.confidence()`` consumes) — so a row priced from a
    STALE oracle would have been stamped HIGH on the snapshot, and the
    G14 / Track-C reconciliation would silently accept it. Walk the
    underlying confidences and propagate the worst.
    """
    if details.get("read_failed") or not details:
        return "UNAVAILABLE"
    explicit = details.get("value_confidence")
    if explicit in ("HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"):
        return explicit
    if not prices:
        return "ESTIMATED"

    # Fold the per-asset confidences. ``prices`` is shaped per AttemptNo17
    # §1.2 G12 — ``{symbol: {"confidence": HIGH|ESTIMATED|STALE|UNAVAILABLE,
    # ...}}``. Tolerate flat ``{symbol: price}`` shapes (legacy callers) by
    # treating them as ``ESTIMATED``.
    seen: set[str] = set()
    for entry in prices.values():
        if isinstance(entry, dict):
            c = entry.get("confidence")
            if c in ("HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"):
                seen.add(c)
            else:
                seen.add("ESTIMATED")
        else:
            seen.add("ESTIMATED")
    if not seen:
        return "ESTIMATED"
    if seen == {"UNAVAILABLE"}:
        return "UNAVAILABLE"
    if "STALE" in seen:
        return "STALE"
    if "ESTIMATED" in seen or "UNAVAILABLE" in seen:
        return "ESTIMATED"
    return "HIGH"


# ─── No-op gauge for hosted mode (Codex Finding 2) ───────────────────────


@dataclass
class _GaugeState:
    """Module-level state for the hosted no-op gauge.

    The metric `accounting_continuous_fields_unavailable{strategy_id, primitive}`
    is `1` whenever the materializer is short-circuited and `0` otherwise.
    Hosted dashboards page when ANY leveraged strategy's gauge is `>0`.
    """

    fired: dict[tuple[str, str], int] = field(default_factory=dict)


_gauge = _GaugeState()


def report_continuous_fields_unavailable(strategy_id: str, primitive: str) -> None:
    """Set the gauge to 1 for (strategy, primitive). Idempotent per process.

    Called once per startup per (strategy, primitive). The gateway's metrics
    publisher polls this state. Wiring the actual prom_client / OpenTelemetry
    publish lives in the gateway side; this module just owns the state map
    so framework code has a single import surface.
    """
    key = (strategy_id, primitive)
    if _gauge.fired.get(key) == 1:
        return
    _gauge.fired[key] = 1
    logger.info(
        "accounting_continuous_fields_unavailable strategy_id=%s primitive=%s — "
        "hosted-mode position-state materializer is no-op pending Track B "
        "(metrics-db migration). Continuous-accrual cells will report "
        "confidence=UNAVAILABLE on the Accountant Test for this strategy.",
        strategy_id,
        primitive,
    )


def report_continuous_fields_writing(strategy_id: str, primitive: str) -> None:
    """Set the gauge to 0 — local-mode runs OR post-Track-B hosted runs."""
    _gauge.fired[(strategy_id, primitive)] = 0


def get_gauge_state() -> dict[tuple[str, str], int]:
    """Read the gauge map (gateway uses this for dashboard publish)."""
    return dict(_gauge.fired)
