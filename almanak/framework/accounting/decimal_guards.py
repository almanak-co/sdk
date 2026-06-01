"""Decimal-unit soft-fail guards for accounting payload writes (VIB-4780 / W1-5).

Observability helpers that detect suspiciously large integer-looking values in
LP attribution and transaction ledger payloads.  These values indicate that a
raw-wei amount was persisted instead of a human-form decimal amount.

Hard rule for Wave 1: **soft-fail only**.  No exceptions raised.  No payload
mutation.  No schema change.  The guard logs a WARNING + increments a
Prometheus counter so the on-call can see the pattern in production.  Wave 3
(W3-1) will flip this to a hard reject once legacy DBs have been audited.

Heuristic (the rule)
--------------------
Two complementary rules combined; **either** firing is enough to flag.

1. **Decimals-aware rule** (preferred — runs when ``token_decimals_map`` is
   supplied for the field's token).  Field-class-aware so legitimate "100
   USDC fees" doesn't false-positive while raw-wei "148" still trips.

   For ``fees_*`` fields the rule is aggressive (real LP fees are sub-cent
   in any production-realistic position; an integer-shaped fee >= 10 is
   almost certainly raw-wei):

   * Value is integer-shaped — no decimal point, no exponent.
   * Integer magnitude ``>= 10``.
   * Hypothetical raw-wei interpretation ``value / 10**decimals`` lands in
     ``[1e-12, 1e9]`` (plausible token-amount range).

   For ``amount_*`` fields the rule is conservative — a swap could legitimately
   move 100k USDC.  An integer-shaped ``amount_in/out`` only trips when:

   * Value is integer-shaped.
   * The hypothetical raw-wei interpretation lands in the plausible range AND
     the integer magnitude is ``>= 10 ** (decimals - 1)`` — i.e. large enough
     to require a divider-by-10^k to be a "normal" amount.  For USDC (6dp)
     that means integer ``>= 10^5 = 100000``; for WETH (18dp) integer
     ``>= 10^17``.  Below those, the rule defers to the magnitude fallback.

   This rule catches the canonical Appendix B LP-2 / LP-3 bug fixtures:

   * WETH 18-dp ``fees_token0 = "75817134186"`` → ``7.58e-8 WETH`` → FLAG.
   * USDC 6-dp ``fees_token1 = "148"`` → ``0.000148 USDC`` → FLAG (fees rule).
   * WETH 18-dp ledger ``amount_in = "701279299182337"`` → 7e14 < 10^17 so the
     amount-class decimals-aware rule does NOT fire; the **magnitude rule
     fallback** (>= 10^12) catches it.  Both rules combined still flag the
     canonical bug.
   * USDC 6-dp ledger ``amount_out = "1585552"`` → ``1.585552 USDC`` →
     1.585M > 100k threshold → FLAG.

   And it leaves legitimate human-form Decimal strings alone:

   * ``"0.000148"``  — has a decimal point → not integer-shaped → SAFE.
   * ``"4.50"``      — has a decimal point → SAFE.
   * ``"0"``         — magnitude < 10 → SAFE (Empty ≠ Zero).
   * ``"100"`` USDC ``amount_in`` — 100 < 10^5 = 100000 (USDC integer floor) → SAFE.
   * ``"10000"`` USDC ``amount_in`` — legit 10k USDC swap, 10000 < 100000 → SAFE.
   * ``"100"`` USDC ``fees_token1`` — would trip the fees-class rule.  In a
     fee field a "100 USDC" value is itself almost certainly a bug — real LP
     fees are sub-cent.  Documented trade-off (see PR body W1-5).

2. **Magnitude fallback rule** (runs when the decimals-aware rule didn't fire
   OR decimals are unknown).  Fires when ``abs(value) >= 10 ** 12``.  Catches
   raw-wei WETH ledger amounts (~7e14) at call sites that don't plumb
   decimals AND extends the decimals-aware rule's coverage upward.

Fields monitored
----------------
``fees_token0``, ``fees_token1``, ``amount0_in``, ``amount0_out``,
``amount1_in``, ``amount1_out``, ``amount_in``, ``amount_out``.

These are the fields observed carrying raw-wei values in production LP
attribution and transaction ledger payloads (Appendix B LP-2, LP-3 of the
May 22 audit).  Each guarded field declares which side (``token0`` /
``token1`` / single-token) it represents so the right decimals lookup is
performed when ``token_decimals_map`` is supplied.

Conservatism — flag fewer; W3 will tighten
------------------------------------------
The rule deliberately does NOT flag integer-typed legitimate large values
(e.g. ``Decimal("100")`` for a 100-USDC fee on a six-figure position).  In
typed accounting payloads such values exist but cannot be distinguished from
a small-magnitude raw-wei without protocol-level context; W3-1 will add the
protocol-aware shape check.  For W1-5 we accept the false-negative cost
because the soft-fail signal is for on-call triage, not a hard gate.
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)

# Fields that may carry raw-wei values in LP attribution or ledger payloads,
# with the token side they reference.  ``token0`` / ``token1`` are LP-style;
# ``in`` / ``out`` are ledger-style; ``"in"`` resolves to the trade's source
# token, ``"out"`` to the destination.
_GUARDED_FIELDS: dict[str, str] = {
    "fees_token0": "token0",
    "fees_token1": "token1",
    "amount0_in": "token0",
    "amount0_out": "token0",
    "amount1_in": "token1",
    "amount1_out": "token1",
    "amount_in": "in",
    "amount_out": "out",
}

# Fallback magnitude threshold (applies when decimals are unknown).
_RAW_WEI_THRESHOLD = Decimal("1e12")

# Decimals-aware rule bounds — the raw-wei interpretation must land here
# to count as plausible-wei.  Below 1e-12 the value is so dust-tiny that
# it's more likely a real micro-fee; above 1e9 it exceeds any real token
# amount and the fallback magnitude rule will catch it.
_PLAUSIBLE_WEI_LOWER = Decimal("1e-12")
_PLAUSIBLE_WEI_UPPER = Decimal("1e9")

# Minimum integer magnitude for the decimals-aware rule.  Below this, the
# value is so small that ``Decimal("0")`` / single-digit counts cannot be
# distinguished from raw-wei dust; we leave them alone (Empty ≠ Zero).
_DECIMALS_RULE_MIN_INTEGER = Decimal("10")


def _to_decimal(value: Any) -> Decimal | None:
    """Try to parse *value* as a Decimal.  Returns ``None`` on failure."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _is_integer_shaped(value: Any) -> bool:
    """True if the value's string form has no decimal point and no exponent.

    Catches the raw-wei tell: producer emitted ``"148"`` instead of
    ``"0.000148"``.  We check the *string* shape, not just ``Decimal``
    equality, because ``Decimal("100")`` and ``Decimal("100.0")``
    serialise differently (``"100"`` vs ``"100.0"``) and only the former
    has the integer-shape footprint of a raw-wei write.
    """
    if value is None:
        return False
    s = str(value).strip()
    if not s:
        return False
    return "." not in s and "e" not in s and "E" not in s


def _decimals_aware_suspect(
    value: Decimal,
    raw_str: str,
    decimals: int,
    *,
    field_class: str,
) -> tuple[bool, Decimal | None]:
    """Return ``(is_suspect, hypothetical_human_form)``.

    Field-class-aware:

    * ``"fees"`` (i.e. ``fees_token0`` / ``fees_token1``): aggressive rule.
      Real LP fees are sub-cent in any production-realistic position; an
      integer-shaped fee >= 10 with a plausible wei interpretation is
      almost certainly raw-wei.
    * ``"amount"`` (``amount_in/out`` / ``amount0_*`` / ``amount1_*``):
      conservative rule.  A SWAP legitimately moves 100 USDC; only flag
      when the integer magnitude is large enough to require a wei-scale
      divider to be a "normal" amount (i.e. ``>= 10 ** (decimals - 2)``).

    Returns ``(True, human_form)`` when suspect.
    """
    if not _is_integer_shaped(raw_str):
        return False, None
    magnitude = abs(value)
    if magnitude < _DECIMALS_RULE_MIN_INTEGER:
        return False, None
    if decimals < 0:
        return False, None
    scale = Decimal(10) ** decimals
    human = magnitude / scale
    # Plausibility window — raw-wei interpretation must look like a real
    # token amount, otherwise the magnitude rule (>= 10^12) is a better
    # signal anyway.
    if not (_PLAUSIBLE_WEI_LOWER <= human <= _PLAUSIBLE_WEI_UPPER):
        return False, None
    if field_class == "fees":
        # Aggressive — every integer-shaped fee >= 10 is suspect.
        return True, human
    if field_class == "amount":
        # Conservative — require magnitude >= 10^(decimals - 1) to flag.
        # For USDC (6dp): integer >= 100000 (== legit human swap of 100k USDC
        # at human scale still safe; the canonical bug 1585552 still trips
        # because 1.585M > 100k threshold).
        # For WETH (18dp): integer >= 10^17 (≈ 0.1 WETH at human scale).
        # Below this threshold a legit "10000 USDC" amount is indistinguishable
        # from a "0.01 USDC" raw-wei without protocol-level context — we accept
        # the false-negative cost in W1-5 and let W3-1 add protocol-aware
        # shape checks.  Canonical raw-wei WETH amounts (e.g. 7e14) fall
        # below this threshold but are caught by the magnitude rule fallback
        # (>= 10^12) downstream.
        threshold = Decimal(10) ** max(decimals - 1, 0)
        if magnitude >= threshold:
            return True, human
        return False, None
    # Unknown field class — fail closed, do not flag.
    return False, None


def _classify_field(field: str) -> str:
    """Return the field-class key (``"fees"`` / ``"amount"``)."""
    if field.startswith("fees_"):
        return "fees"
    return "amount"


def _check_decimal_unit_soft_fail(
    payload: dict[str, Any],
    *,
    event_id: str,
    event_type: str,
    chain: str | None = None,
    token_decimals_map: dict[str, int] | None = None,
    token_symbols_map: dict[str, str] | None = None,
) -> int:
    """Log a warning + emit a metric for each field whose magnitude looks raw-wei.

    Returns the count of suspicious fields detected.

    **Soft-fail / observability only.**  Does NOT raise.  Does NOT mutate
    *payload*.  Does NOT block the caller's write.

    Parameters
    ----------
    payload:
        The dict that is about to be persisted (e.g. ``attribution_json``
        dict or a ``LedgerEntry``-shaped dict).  Only the keys that appear
        in ``_GUARDED_FIELDS`` are examined.
    event_id:
        Stable identifier for the event being written (position event id,
        ledger entry id, etc.).  Used in the warning message for triage.
    event_type:
        The intent / event type string (e.g. ``"LP_CLOSE"``, ``"LP_OPEN"``,
        ``"SWAP"``).  Used in the warning message and as a metric label.
    chain:
        Chain name (e.g. ``"arbitrum"``).  Used as a metric label when
        supplied; defaults to ``"unknown"`` in the metric otherwise.  Does
        NOT affect the heuristic.
    token_decimals_map:
        Optional mapping of side identifier (``"token0"`` / ``"token1"`` /
        ``"in"`` / ``"out"``) to that side's token decimals.  When supplied,
        the decimals-aware rule runs for the matching fields; absent
        entries fall back to the magnitude rule.
    token_symbols_map:
        Optional mapping of the same side identifiers to token symbols
        (e.g. ``{"token0": "WETH", "token1": "USDC"}``).  Used in the
        WARNING message and as a metric label so the on-call can correlate
        the alert to a token.  Absent entries default to the empty string.

    The metric ``accounting_raw_wei_suspected_total`` is incremented per
    suspect field with labels ``{chain, field, event_type, token_symbol}``.
    """
    suspicious_count = 0
    chain_label = (chain or "unknown").lower() or "unknown"
    decimals_map = token_decimals_map or {}
    symbols_map = token_symbols_map or {}

    for field, side in _GUARDED_FIELDS.items():
        # Wrap the entire per-field body in try/except so any unforeseen
        # exception (overflow, comparison error against an unexpected type,
        # exotic Decimal subclass, etc.) falls through harmlessly per the
        # soft-fail contract.  We log at DEBUG for observability without
        # raising or affecting the suspicious count.
        try:
            raw = payload.get(field)
            if raw is None:
                continue
            val = _to_decimal(raw)
            # Reject non-finite values (NaN / Infinity).  These can raise on
            # ordered comparisons / exponent math and would violate the
            # "never raise" contract if they reached _decimals_aware_suspect
            # or the magnitude comparison below.
            if val is None or not val.is_finite():
                continue

            symbol = symbols_map.get(side, "") or ""
            # Treat decimals as valid only if it is a non-negative int.
            # Anything else (None, float, str, negative) falls through to
            # the magnitude rule rather than risking Decimal(10) ** decimals
            # raising on a bogus operand.
            decimals_raw = decimals_map.get(side)
            decimals: int | None = (
                decimals_raw
                if isinstance(decimals_raw, int) and not isinstance(decimals_raw, bool) and decimals_raw >= 0
                else None
            )

            suspect, human_form = (False, None)
            rule = ""
            field_class = _classify_field(field)
            if decimals is not None:
                # Defence-in-depth: even with validated decimals, wrap the
                # decimals-aware check so a future refactor that introduces
                # a raise inside _decimals_aware_suspect cannot break the
                # soft-fail contract.  On any exception we fall through to
                # the magnitude rule.
                try:
                    suspect, human_form = _decimals_aware_suspect(
                        val,
                        str(raw),
                        decimals,
                        field_class=field_class,
                    )
                except Exception:  # pragma: no cover - decimals path must never raise
                    logger.debug(
                        "decimal_unit_guard: decimals-aware check failed (non-fatal)",
                        exc_info=True,
                    )
                    suspect, human_form = False, None
                if suspect:
                    rule = f"decimals_aware_{field_class}"

            # Fallback magnitude rule — always runs when the decimals-aware
            # rule didn't fire (either no decimals supplied, or the value did
            # not match the integer-shape / plausibility window).  This keeps
            # the pre-decimals-aware behaviour intact for call sites that
            # don't yet plumb token decimals.
            if not suspect:
                magnitude = abs(val)
                if magnitude >= _RAW_WEI_THRESHOLD:
                    suspect = True
                    rule = "magnitude"
                    # Best-effort human-form interpretation when decimals known.
                    if decimals is not None:
                        human_form = magnitude / (Decimal(10) ** decimals)

            if not suspect:
                continue

            suspicious_count += 1
            # Format the magnitude via Decimal directly — ``float(Decimal)``
            # would raise ``OverflowError`` for huge values, which violates
            # the soft-fail contract of this guard.
            magnitude_str = f"{abs(val):.2E}"
            human_form_str = f"~={human_form:.4E}" if human_form is not None else "(decimals unknown)"
            decimals_str = str(decimals) if decimals is not None else "unknown"
            logger.warning(
                "decimal_unit_guard: suspiciously large value in payload field "
                "(event_id=%s event_type=%s chain=%s field=%s token=%s decimals=%s "
                "value_magnitude=%s human_form_if_raw_wei=%s rule=%s) — "
                "possible raw-wei amount persisted instead of human-form decimal. "
                "Wave 3 (W3-1) will hard-reject this. VIB-4780.",
                event_id,
                event_type,
                chain_label,
                field,
                symbol,
                decimals_str,
                magnitude_str,
                human_form_str,
                rule,
            )
            # Emit Prometheus counter for the dashboard.  Lazy import so this
            # module remains importable when prometheus_client is unavailable
            # (e.g. minimal test environments) — the soft-fail contract means
            # we never let the metric path break the write.
            try:
                from almanak.framework.observability.metrics import (
                    record_raw_wei_suspected,
                )

                record_raw_wei_suspected(
                    chain=chain_label,
                    field=field,
                    event_type=event_type or "unknown",
                    token_symbol=symbol or "unknown",
                )
            except Exception:  # pragma: no cover - metric path must never raise
                logger.debug(
                    "decimal_unit_guard: metric emission failed (non-fatal)",
                    exc_info=True,
                )
        except Exception:  # pragma: no cover - outer guard must never raise
            # Defence-in-depth: any exception from a single field's
            # inspection (e.g., exotic Decimal subclass behaviour, library
            # bug) must not break the loop for other fields or propagate.
            logger.debug(
                "decimal_unit_guard: per-field check failed (non-fatal); event_id=%s field=%s",
                event_id,
                field,
                exc_info=True,
            )
            continue

    return suspicious_count
