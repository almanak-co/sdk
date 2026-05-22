"""Decimal-unit soft-fail guards for accounting payload writes (VIB-4780 / W1-5).

Observability helpers that detect suspiciously large integer-looking values in
LP attribution and transaction ledger payloads.  These values indicate that a
raw-wei amount was persisted instead of a human-form decimal amount.

Hard rule for Wave 1: **soft-fail only**.  No exceptions raised.  No payload
mutation.  No schema change.  The guard logs a warning + returns a count of
suspicious fields so callers can emit an application metric.  Wave 3 (W3-1)
will flip this to a hard reject once legacy DBs have been audited.

Threshold rationale
-------------------
``_RAW_WEI_THRESHOLD = 10 ** 12``

A real human-form WETH amount tops out around 10^9 (≈ $2B at $2000/WETH).
A real USDC 6dp amount stays well below 10^9 too (max supply ≈ $50B / 10^6 dp
= 5 × 10^7).  Raw-wei WETH starts at 10^16–10^18 for any realistic position.
10^12 gives a comfortable margin on both sides.

Fields monitored
----------------
``fees_token0``, ``fees_token1``, ``amount0_in``, ``amount0_out``,
``amount1_in``, ``amount1_out``, ``amount_in``, ``amount_out``.

These are the fields that have been observed carrying raw-wei values in
production LP attribution and transaction ledger payloads (Appendix B LP-2,
LP-3 of the May 22 audit).
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)

# Fields that may carry raw-wei values in LP attribution or ledger payloads.
_GUARDED_FIELDS: frozenset[str] = frozenset(
    {
        "fees_token0",
        "fees_token1",
        "amount0_in",
        "amount0_out",
        "amount1_in",
        "amount1_out",
        "amount_in",
        "amount_out",
    }
)

# Magnitudes at or above this threshold are flagged as suspiciously raw-wei.
_RAW_WEI_THRESHOLD = Decimal("1e12")


def _to_decimal(value: Any) -> Decimal | None:
    """Try to parse *value* as a Decimal.  Returns ``None`` on failure."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _check_decimal_unit_soft_fail(
    payload: dict[str, Any],
    *,
    event_id: str,
    event_type: str,
) -> int:
    """Log a warning for each field whose magnitude looks raw-wei.

    Returns the count of suspicious fields detected.

    **Soft-fail / observability only.**  Does NOT raise.  Does NOT mutate
    *payload*.

    Parameters
    ----------
    payload:
        The dict that is about to be persisted (e.g. ``attribution_json`` dict
        or a ``LedgerEntry``-shaped dict).  Only the keys that appear in
        ``_GUARDED_FIELDS`` are examined.
    event_id:
        Stable identifier for the event being written (position event id,
        ledger entry id, etc.).  Used in the warning message for triage.
    event_type:
        The intent / event type string (e.g. ``"LP_CLOSE"``, ``"LP_OPEN"``).
        Used in the warning message.
    """
    suspicious_count = 0
    for field in _GUARDED_FIELDS:
        raw = payload.get(field)
        if raw is None:
            continue
        val = _to_decimal(raw)
        if val is None:
            continue
        magnitude = abs(val)
        if magnitude >= _RAW_WEI_THRESHOLD:
            suspicious_count += 1
            # Format the magnitude via Decimal directly — ``float(Decimal)``
            # would raise ``OverflowError`` for huge values, which violates
            # the soft-fail contract of this guard.
            logger.warning(
                "decimal_unit_guard: suspiciously large value in payload field "
                "(event_id=%s event_type=%s field=%s value_magnitude=%s) — "
                "possible raw-wei amount persisted instead of human-form decimal. "
                "Wave 3 (W3-1) will hard-reject this. VIB-4780.",
                event_id,
                event_type,
                field,
                f"{magnitude:.2E}",
            )
    return suspicious_count
