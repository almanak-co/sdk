"""Fill-vs-submission reconciliation for Hyperliquid CoreWriter orders (VIB-5597).

CoreWriter settlement is **asynchronous**: a ``sendRawAction`` EVM tx returning
status 1 proves the order was *submitted*, not that it *filled*. An IOC order can
partial-fill or be rejected outright (insufficient margin, sub-``$10`` notional,
no liquidity) while the EVM tx still succeeds. Trusting submission-success would
have a strategy manage — and try to close — a position that may not exist.

This module is the **fill-confirmation seam** the reference strategies use to
drive position state from the *observed* fill rather than from submission
success. It mirrors the gmx_v2 settlement-observer pattern (``orders_read.py`` +
``teardown_reads.py`` + ``perps_read.py``): the submit receipt only proves the
order was created; the true state "appears" on a later on-chain / API read.

Two independent signals, combined by :func:`reconcile_fill`:

1. **``orderStatus`` by ``cloid``** — the HyperCore Info API
   (``api.hyperliquid.xyz/info``, ``type=orderStatus``) resolves a specific
   submission to ``filled`` / ``open`` (resting) / ``canceled`` / ``rejected`` /
   ``triggered`` etc. Our CoreWriter orders carry a **deterministic uint128
   ``cloid``** derived from the intent id (``compiler.HyperliquidCompiler._cloid``
   = ``keccak(intent_id)[:16]``), so a submission is positively addressable. This
   is API-sourced, so the egress lives **gateway-side** (see
   ``gateway/provider.py``); this module only builds the request body and parses
   the response — both pure, no sockets (gateway-boundary rule).

2. **The ``0x0800`` position precompile read** — the settlement observer already
   wired in ``perps_read.py``. Once HyperCore has filled the order, the position
   becomes visible on this read; a still-unfilled / rejected submission leaves the
   position unchanged. This signal needs no new gateway capability (it reuses the
   ``eth_call`` the compiler / perps-read already do) but, for an *open*, cannot
   by itself distinguish "our order filled" from "a pre-existing position" — so it
   is a **confirmatory** signal, strongest when combined with ``orderStatus`` or a
   before/after size delta.

**Empty ≠ Zero is the safety spine.** When neither signal could be measured, the
outcome is :attr:`FillStatus.UNMEASURED` — an unconfirmed submission is *never*
assumed filled and *never* fabricated flat. A strategy that cannot confirm a fill
must treat the position as **pending/unconfirmed** (unmeasured), not as open.

Everything here is **pure**: request-body builders + response/position decoders.
No provider is opened, no key is touched.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from .sdk import Position

logger = logging.getLogger(__name__)

# HyperCore Info API request type for a single order's status.
_ORDER_STATUS_REQUEST_TYPE = "orderStatus"

# ``status`` string values the Info API returns for an order. Grouped by the
# fill verdict they imply. Verified against the Hyperliquid API docs; the
# parser is tolerant of unknown values (→ UNKNOWN, treated as unmeasured).
_FILLED_STATUSES = frozenset({"filled"})
_RESTING_STATUSES = frozenset({"open", "resting"})  # accepted onto the book, not (yet) filled
_REJECTED_STATUSES = frozenset(
    {
        "rejected",
        "canceled",
        "cancelled",
        "marginCanceled",
        "reduceOnlyCanceled",
        "badAloPxCanceled",
        "expired",
        "vaultWithdrawalCanceled",
        "openInterestCapCanceled",
        "selfTradeCanceled",
        "oracleCanceled",
        "insufficientMarginCanceled",
    }
)
# IOC orders never rest, so an unrecognised-but-terminal state is safest read as
# "not a confirmed fill" rather than assumed filled.


class FillStatus(StrEnum):
    """The reconciled verdict for a submitted CoreWriter order.

    Empty ≠ Zero: :attr:`UNMEASURED` is the fail-safe verdict when no signal
    could be read — it is NOT a fill and NOT a rejection, so the caller must
    keep the position *unconfirmed* (pending) rather than assume either.
    """

    FILLED = "filled"  # fully filled — safe to commit the position as OPEN
    PARTIALLY_FILLED = "partially_filled"  # some size filled; a position exists but is smaller than requested
    RESTING = "resting"  # accepted, not filled (not expected for IOC; treat as not-yet-filled)
    REJECTED = "rejected"  # rejected / canceled with no fill — NO position resulted
    UNMEASURED = "unmeasured"  # could not be measured — never assume filled or flat

    @property
    def is_confirmed_fill(self) -> bool:
        """True only when a position provably resulted from the submission."""
        return self in (FillStatus.FILLED, FillStatus.PARTIALLY_FILLED)

    @property
    def is_confirmed_reject(self) -> bool:
        """True only when the submission provably produced NO position."""
        return self is FillStatus.REJECTED


@dataclass(frozen=True)
class FillOutcome:
    """The reconciled fill verdict plus the evidence it was derived from.

    Attributes:
        status: The reconciled :class:`FillStatus`.
        filled_size: Base-asset size the API reported filled, when known
            (``None`` = unmeasured; ``0`` would be a measured zero fill, but a
            zero fill is reported as REJECTED, not FILLED with size 0).
        avg_fill_price: Average fill price the API reported, when known
            (``None`` = unmeasured; NEVER a fabricated ``0``).
        source: Which signal(s) produced the verdict, for observability
            ("order_status", "position_read", "order_status+position_read").
        detail: Human-readable note (raw status string, error, etc.).
    """

    status: FillStatus
    filled_size: Any | None = None  # Decimal-or-None; Any keeps this module import-light
    avg_fill_price: Any | None = None
    source: str = ""
    detail: str = ""
    residual: dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Signal 1 — orderStatus by cloid (Info API; egress lives gateway-side)
# =============================================================================


def build_order_status_request(wallet_address: str, cloid: int) -> dict[str, Any]:
    """Build the HyperCore Info-API ``orderStatus`` request body for a ``cloid``.

    The Info API addresses an order by ``oid`` (numeric) OR ``cloid`` (the
    128-bit client order id, hex ``0x``-padded to 32 nibbles). Our orders carry
    a deterministic ``cloid`` (``compiler._cloid``), so this is the positive
    submission→status handle. Pure: returns the JSON body only; the POST is done
    gateway-side (``gateway/provider.py``), never from strategy/framework code.
    """
    if not isinstance(cloid, int) or isinstance(cloid, bool) or cloid <= 0:
        raise ValueError(f"cloid must be a positive int, got {cloid!r}")
    if not isinstance(wallet_address, str) or not wallet_address.startswith("0x") or len(wallet_address) != 42:
        raise ValueError(f"wallet_address must be a 0x EVM address, got {wallet_address!r}")
    return {
        "type": _ORDER_STATUS_REQUEST_TYPE,
        "user": wallet_address,
        # HyperCore expects the cloid as a 0x-prefixed 16-byte (32 nibble) hex.
        "oid": cloid_to_hex(cloid),
    }


def cloid_to_hex(cloid: int) -> str:
    """Render a uint128 ``cloid`` as the API's 0x-prefixed 32-nibble hex."""
    if not isinstance(cloid, int) or isinstance(cloid, bool) or cloid < 0 or cloid >= 2**128:
        raise ValueError(f"cloid out of uint128 range: {cloid!r}")
    return "0x" + format(cloid, "032x")


def parse_order_status_response(response: Any) -> FillOutcome:
    """Parse a HyperCore ``orderStatus`` API response into a :class:`FillOutcome`.

    Expected shape (Hyperliquid Info API):
        ``{"status": "order", "order": {"status": "filled", "order": {...}}}``
    or ``{"status": "unknownOid"}`` when the order id is not known to the API.

    Empty ≠ Zero: a missing / malformed / unknown response yields
    :attr:`FillStatus.UNMEASURED` — never a fabricated fill or reject. A
    positively-``filled`` status yields FILLED; a terminal cancel/reject with no
    fill yields REJECTED; a resting order yields RESTING (not expected for IOC).
    """
    if not isinstance(response, dict):
        return FillOutcome(FillStatus.UNMEASURED, source="order_status", detail="response not a dict (unmeasured)")

    # ``unknownOid`` means the API has no record — for a *just-submitted* order
    # this is genuinely ambiguous (propagation lag vs never-accepted), so it is
    # unmeasured, not a reject.
    top = str(response.get("status") or "").strip()
    if top == "unknownOid":
        return FillOutcome(FillStatus.UNMEASURED, source="order_status", detail="unknownOid (unmeasured)")

    order_wrap = response.get("order")
    if not isinstance(order_wrap, dict):
        return FillOutcome(
            FillStatus.UNMEASURED, source="order_status", detail=f"no order payload (top status={top!r})"
        )

    inner_status = str(order_wrap.get("status") or "").strip()
    order_inner = order_wrap.get("order")
    order_obj = order_inner if isinstance(order_inner, dict) else {}

    filled_size = _maybe_decimal(order_obj.get("filledSz"))
    avg_price = _maybe_decimal(order_obj.get("avgPx"))

    if inner_status in _FILLED_STATUSES:
        return FillOutcome(
            FillStatus.FILLED,
            filled_size=filled_size,
            avg_fill_price=avg_price,
            source="order_status",
            detail=f"status={inner_status}",
        )
    if inner_status in _RESTING_STATUSES:
        # A resting order has, by definition, not filled yet. For an IOC market
        # order this is unusual, but the honest verdict is "not a confirmed fill".
        if filled_size is not None and filled_size > 0:
            return FillOutcome(
                FillStatus.PARTIALLY_FILLED,
                filled_size=filled_size,
                avg_fill_price=avg_price,
                source="order_status",
                detail=f"status={inner_status} with partial fill",
            )
        return FillOutcome(FillStatus.RESTING, source="order_status", detail=f"status={inner_status}")
    if inner_status in _REJECTED_STATUSES:
        # A cancel/reject that nonetheless shows a partial fill still produced a
        # (smaller) position — report it honestly as partially filled, not reject.
        if filled_size is not None and filled_size > 0:
            return FillOutcome(
                FillStatus.PARTIALLY_FILLED,
                filled_size=filled_size,
                avg_fill_price=avg_price,
                source="order_status",
                detail=f"status={inner_status} but partial fill observed",
            )
        return FillOutcome(FillStatus.REJECTED, source="order_status", detail=f"status={inner_status}")

    # Unrecognised inner status — do not guess a fill; unmeasured is fail-safe.
    return FillOutcome(
        FillStatus.UNMEASURED, source="order_status", detail=f"unrecognised status={inner_status!r} (unmeasured)"
    )


# =============================================================================
# Signal 2 — position read (the 0x0800 settlement observer)
# =============================================================================


def confirm_open_from_position(
    position: Position | None,
    *,
    expected_is_long: bool,
    prior_position: Position | None = None,
) -> FillOutcome:
    """Confirm a PERP_OPEN fill from the observed ``0x0800`` position.

    ``position`` is the decoded position AFTER the submit (the settlement
    observer). ``prior_position`` is the position BEFORE the submit, when the
    caller captured it: the size DELTA is the unambiguous "our order filled"
    signal (a fresh open on top of an existing position is invisible to a bare
    "is there a position?" check, so the delta is what makes this exact).

    Verdicts (Empty ≠ Zero throughout):
      * ``position is None`` → UNMEASURED (the read failed; never "no fill").
      * No prior baseline: an open position on the expected side → FILLED
        (confirmatory — cannot exclude a pre-existing position without a
        baseline, but the position IS present on the expected side); a flat /
        opposite-side read → UNMEASURED (an IOC that did not visibly move the
        book, but we lack a baseline to call it a reject).
      * With a prior baseline: a size increase on the expected side → FILLED;
        no change → REJECTED (the order provably did not open anything); a
        decrease/flip is anomalous → UNMEASURED.
    """
    if position is None:
        return FillOutcome(FillStatus.UNMEASURED, source="position_read", detail="position read unmeasured")

    now_open = position.is_open
    now_long = position.is_long if now_open else None

    if prior_position is not None:
        prior_signed = prior_position.szi if prior_position.is_open else 0
        delta = position.szi - prior_signed
        # Increase in the expected direction (long → more positive; short → more negative).
        moved_right_way = (delta > 0) if expected_is_long else (delta < 0)
        if now_open and now_long == expected_is_long and moved_right_way:
            return FillOutcome(
                FillStatus.FILLED,
                filled_size=abs(delta),
                source="position_read",
                detail=f"position size moved by {delta} (baseline {prior_signed})",
            )
        if delta == 0:
            return FillOutcome(
                FillStatus.REJECTED,
                source="position_read",
                detail="position unchanged vs baseline (no fill)",
            )
        # A move against the expected direction, or a side flip: anomalous — do
        # not claim a fill; unmeasured is fail-safe.
        return FillOutcome(
            FillStatus.UNMEASURED,
            source="position_read",
            detail=f"anomalous size delta {delta} (baseline {prior_signed}, expected_long={expected_is_long})",
        )

    # No baseline: confirmatory only.
    if now_open and now_long == expected_is_long:
        return FillOutcome(
            FillStatus.FILLED,
            filled_size=abs(position.szi),
            source="position_read",
            detail="position present on expected side (no baseline; confirmatory)",
        )
    return FillOutcome(
        FillStatus.UNMEASURED,
        source="position_read",
        detail="no position on expected side and no baseline — cannot call reject",
    )


def confirm_close_from_position(position: Position | None, *, was_full_close: bool) -> FillOutcome:
    """Confirm a PERP_CLOSE fill from the observed ``0x0800`` position.

    A reduce-only close is confirmed FILLED when the position is flat (full
    close) or measurably smaller. Empty ≠ Zero: a failed read is UNMEASURED,
    never "closed".
    """
    if position is None:
        return FillOutcome(FillStatus.UNMEASURED, source="position_read", detail="position read unmeasured")
    if not position.is_open:
        return FillOutcome(FillStatus.FILLED, filled_size=0, source="position_read", detail="position flat")
    if was_full_close:
        # Still open after a full-close submission: the IOC did not (fully) fill.
        return FillOutcome(
            FillStatus.RESTING,
            source="position_read",
            detail="position still open after full-close submission (unfilled)",
        )
    # Partial close: a still-open (smaller) position is expected — treat as a
    # (partial) fill only if the caller supplied evidence of a reduction; here,
    # without a baseline, we can only say the position still exists (unmeasured
    # reduction). Fail-safe to UNMEASURED so a partial close is not assumed done.
    return FillOutcome(
        FillStatus.UNMEASURED,
        source="position_read",
        detail="position still open after partial-close submission (reduction unmeasured)",
    )


# =============================================================================
# Reconciliation — combine the two signals
# =============================================================================


def reconcile_fill(
    order_status: FillOutcome | None,
    position_signal: FillOutcome | None,
) -> FillOutcome:
    """Combine the two fill signals into a single verdict.

    Precedence:
      * A **confirmed reject** from ``orderStatus`` (terminal cancel/reject, no
        fill) is authoritative — no position resulted.
      * A **confirmed fill** from either signal → FILLED / PARTIALLY_FILLED.
        ``orderStatus`` (which carries fill size/price) wins the economics; the
        position read corroborates.
      * Otherwise → UNMEASURED. Empty ≠ Zero: if neither signal positively
        confirms a fill OR a reject, the submission stays unconfirmed.

    Either argument may be ``None`` (that signal was not read).
    """
    os_out = order_status
    pos_out = position_signal

    # 1) A positively-measured reject from orderStatus is authoritative UNLESS the
    #    position read shows a fill (partial fill despite a cancel is possible).
    if os_out is not None and os_out.status.is_confirmed_reject:
        if pos_out is not None and pos_out.status.is_confirmed_fill:
            return _merge_fill(pos_out, os_out, source="order_status+position_read")
        return FillOutcome(
            FillStatus.REJECTED,
            source=_join_sources(os_out, pos_out),
            detail=f"orderStatus reject: {os_out.detail}",
        )

    # 2) A confirmed fill from orderStatus (authoritative economics).
    if os_out is not None and os_out.status.is_confirmed_fill:
        return _merge_fill(os_out, pos_out, source=_join_sources(os_out, pos_out))

    # 3) No orderStatus fill/reject — fall back to the position read.
    if pos_out is not None and pos_out.status.is_confirmed_fill:
        return _merge_fill(pos_out, os_out, source=_join_sources(os_out, pos_out))
    if pos_out is not None and pos_out.status.is_confirmed_reject:
        return FillOutcome(
            FillStatus.REJECTED,
            source=_join_sources(os_out, pos_out),
            detail=f"position read reject: {pos_out.detail}",
        )

    # 4) Neither confirmed anything — unmeasured (fail-safe).
    resting = (os_out is not None and os_out.status is FillStatus.RESTING) or (
        pos_out is not None and pos_out.status is FillStatus.RESTING
    )
    status = FillStatus.RESTING if resting else FillStatus.UNMEASURED
    details = "; ".join(o.detail for o in (os_out, pos_out) if o is not None and o.detail) or "no signal measured"
    return FillOutcome(status, source=_join_sources(os_out, pos_out), detail=details)


def _merge_fill(primary: FillOutcome, secondary: FillOutcome | None, *, source: str) -> FillOutcome:
    """Combine two fill outcomes, preferring ``primary``'s economics."""
    filled = primary.filled_size
    avg = primary.avg_fill_price
    if filled is None and secondary is not None:
        filled = secondary.filled_size
    if avg is None and secondary is not None:
        avg = secondary.avg_fill_price
    # PARTIALLY_FILLED is "stickier" than FILLED — if either signal says partial,
    # the combined verdict is partial (the position is smaller than requested).
    status = FillStatus.FILLED
    for o in (primary, secondary):
        if o is not None and o.status is FillStatus.PARTIALLY_FILLED:
            status = FillStatus.PARTIALLY_FILLED
            break
    detail = "; ".join(o.detail for o in (primary, secondary) if o is not None and o.detail)
    return FillOutcome(status, filled_size=filled, avg_fill_price=avg, source=source, detail=detail)


def _join_sources(*outcomes: FillOutcome | None) -> str:
    parts = [o.source for o in outcomes if o is not None and o.source]
    seen: list[str] = []
    for p in parts:
        for token in p.split("+"):
            if token and token not in seen:
                seen.append(token)
    return "+".join(seen)


def _maybe_decimal(value: Any) -> Any | None:
    """Coerce a numeric-ish API field to Decimal; ``None`` on missing/garbage.

    Empty ≠ Zero: a missing field is ``None`` (unmeasured), NEVER ``Decimal(0)``.
    """
    if value is None or value == "":
        return None
    from decimal import Decimal, InvalidOperation

    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


__all__ = [
    "FillOutcome",
    "FillStatus",
    "build_order_status_request",
    "cloid_to_hex",
    "confirm_close_from_position",
    "confirm_open_from_position",
    "parse_order_status_response",
    "reconcile_fill",
]
