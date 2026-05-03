"""Tolerant ``price_inputs_json`` parser for category handlers (VIB-3885).

The ledger's canonical wire shape (per AttemptNo17 §1.2 G12, written by
``framework/observability/ledger.py:529-544``) is::

    {symbol: {"price_usd": "<str-numeric>",
              "oracle_source": "<str>",
              "fetched_at": "<iso-ts>",
              "confidence": "<str>"}}

Pre-AttemptNo17 callers (and a handful of unit-test fixtures) still pass a
flat shape::

    {symbol: "<str-numeric>"}

Before this helper, three category handlers (`swap_handler`, `lp_handler`,
`lending_handler`) called ``json.loads`` directly and then did
``oracle.get(symbol)`` expecting the flat shape. Multiplying a token amount
by the nested dict raised in the ``Decimal(str(price)) * amount`` step,
fell into the bare ``Exception`` branches, silently returned ``None``, and
stamped ``unavailable_reason`` on every USD field downstream. That cascade
produced the May 2 dashboard miscount (G6 FAIL, NAV 343% cash, etc.).

The fix is a single tolerant parser shared by all handlers: it always
returns a flat ``dict[str, Decimal]`` with upper-cased symbol keys, no
matter which shape the ledger wrote. The dashboard-side reader at
``pages/trade_tape.py:402`` is independent — it tolerates both shapes for
its own rendering — and is intentionally untouched.
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)


def parse_price_inputs(price_inputs_json: str | None) -> dict[str, Decimal]:
    """Parse ``price_inputs_json`` into a flat ``{SYMBOL: Decimal}`` dict.

    Tolerates both the canonical nested shape and the legacy flat shape.
    Symbols are upper-cased so callers can match case-insensitively without
    re-implementing the lookup ladder. Returns an empty dict on any kind of
    parse failure (empty string, malformed JSON, non-dict root) — the
    fail-closed contract the handlers already rely on.

    Entries with a missing / non-numeric / non-finite price are dropped
    from the returned dict. Callers that need to distinguish "symbol
    absent from the row" from "symbol present but unpriceable" should
    inspect the raw JSON separately; the diagnostic strings emitted by
    the LP handler use ``_safe_decimal`` against the raw mapping returned
    by :func:`load_raw_price_inputs` for that purpose.
    """
    raw = load_raw_price_inputs(price_inputs_json)
    if not raw:
        return {}

    result: dict[str, Decimal] = {}
    for sym, val in raw.items():
        if not isinstance(sym, str) or not sym:
            continue
        price = _coerce_price(val)
        if price is None:
            continue
        result[sym.upper()] = price
    return result


def load_raw_price_inputs(price_inputs_json: str | None) -> dict[str, Any]:
    """Decode the JSON column into a raw ``{symbol: value}`` mapping.

    Preserves the on-disk shape (nested dict OR flat scalar). Used by
    handlers whose ``unavailable_reason`` diagnostics need to distinguish
    "symbol absent" from "symbol present but the price entry was
    malformed/non-numeric" — both are recoverable signals for the
    operator triaging a NULL ``cost_basis_usd`` column.
    """
    if not price_inputs_json:
        return {}
    try:
        d = json.loads(price_inputs_json)
    except (json.JSONDecodeError, TypeError):
        return {}
    return d if isinstance(d, dict) else {}


def _coerce_price(value: Any) -> Decimal | None:
    """Pull a numeric price out of either shape; return None on failure.

    - Nested shape: ``{"price_usd": "1.0001", ...}`` → ``Decimal("1.0001")``.
    - Flat shape:   ``"1.0001"`` (str) or ``1.0001`` (int/float) → ``Decimal``.
    - Anything else (None, empty, NaN, Infinity, malformed) → ``None``.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        # Canonical nested shape per AttemptNo17 §1.2 G12.
        candidate = value.get("price_usd")
        if candidate is None:
            # Defensive: a few legacy callers wrote {"price": ...} instead.
            candidate = value.get("price")
        if candidate is None:
            return None
        return _to_finite_decimal(candidate)
    return _to_finite_decimal(value)


def _to_finite_decimal(value: Any) -> Decimal | None:
    """Coerce to a finite ``Decimal`` (rejects NaN / Infinity / non-numeric)."""
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return d if d.is_finite() else None
