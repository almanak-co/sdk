"""Pendle PT buy accounting event builder (VIB-3422).

Wired into strategy_runner after every successful SWAP intent that results
in PT tokens being received (PT buy).

Records the locked fixed yield at entry:
  pt_price       = sy_amount_in / pt_amount_out   (PT per SY)
  days_to_mat    = (maturity_ts - now).days
  implied_apr    = (1 - pt_price) / pt_price * (365 / days_to_mat) * 10000

Maturity is derived from the PT token symbol (e.g. "PT-wstETH-25JUN2026")
using _parse_pt_maturity(). This avoids any outbound API call at accounting time.
If the PT symbol format is unrecognised, maturity = None and implied_apr = None;
confidence is set to ESTIMATED but all amount fields are still populated.

Edge cases:
  - days_to_maturity = 0 (at/past maturity): implied_apr_bps = None
  - implied APR > 500_000 bps (50 000 %): capped at 500_000 to avoid int overflow
    from strategies that buy PT with 1 day to maturity.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.framework.accounting.ids import make_accounting_event_id

logger = logging.getLogger(__name__)

_PENDLE_SWAP_PROTOCOL = "pendle"
_APR_BPS_CAP = 500_000  # 50 000 % — sentinel for near-maturity buys

_MONTH_MAP: dict[str, int] = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def _parse_pt_maturity(pt_symbol: str) -> datetime | None:
    """Parse the maturity date embedded in a Pendle PT symbol.

    Accepts formats like:
      PT-wstETH-25JUN2026   → datetime(2026, 6, 25, UTC)
      PT-sUSDe-29MAY2025    → datetime(2025, 5, 29, UTC)
      PT-SUSDAI-15OCT2026   → datetime(2026, 10, 15, UTC)

    Returns None when the symbol doesn't follow the pattern.
    """
    m = re.search(r"[-_](\d{1,2})([A-Z]{3})(\d{4})(?:$|[-_])", pt_symbol.upper())
    if not m:
        return None
    day_s, month_abbr, year_s = m.group(1), m.group(2), m.group(3)
    month = _MONTH_MAP.get(month_abbr)
    if month is None:
        return None
    try:
        return datetime(int(year_s), month, int(day_s), tzinfo=UTC)
    except ValueError:
        return None


def compute_implied_apr_bps(pt_price: Decimal, days_to_maturity: int) -> int | None:
    """Compute implied APR in basis-points from PT price and days to maturity.

    Formula: (1 - pt_price) / pt_price * (365 / days_to_maturity) * 10_000

    Returns None when days_to_maturity <= 0 (at/past maturity).
    Caps the result at _APR_BPS_CAP (500 000 bps) for near-maturity buys.
    """
    if days_to_maturity <= 0:
        return None
    try:
        discount = (Decimal("1") - pt_price) / pt_price
        annualised = discount * (Decimal("365") / Decimal(str(days_to_maturity)))
        bps = int((annualised * Decimal("10000")).to_integral_value())
        return min(bps, _APR_BPS_CAP)
    except (InvalidOperation, ZeroDivisionError):
        return None


def _is_pt_buy(intent: Any, result: Any) -> bool:
    """Return True when the intent is a Pendle PT buy.

    Detects via to_token symbol starting with "PT-" (the Pendle naming convention
    like "PT-wstETH-25JUN2026"). The dash is required to avoid false positives from
    tokens like PTC (Pesetacoin). The secondary check requires "PT-" in token_out
    to avoid matching APT, OPT, or other tokens that contain "PT".
    """
    to_token = str(getattr(intent, "to_token", "") or "")
    if to_token.upper().startswith("PT-"):
        return True
    # Secondary check from enriched swap_amounts: require "PT-" prefix, not just "PT"
    extracted = getattr(result, "extracted_data", None) or {}
    swap_amounts = extracted.get("swap_amounts")
    if swap_amounts:
        token_out = str(getattr(swap_amounts, "token_out", "") or "")
        if token_out.upper().startswith("PT-"):
            return True
    return False


def build_pendle_pt_buy_accounting_event(
    *,
    intent: Any,
    result: Any,
    deployment_id: str,
    strategy_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    ledger_entry_id: str | None = None,
) -> Any | None:
    """Build a PendleAccountingEvent(PT_BUY) for a completed Pendle PT swap.

    Returns None when:
    - The intent is not a SWAP for Pendle protocol
    - The result is not a PT buy (to_token is SY, YT, or other)

    Fields populated:
    - pt_price           from receipt (sy_in / pt_out)
    - sy_amount          SY tokens paid (raw int as Decimal)
    - pt_amount          PT tokens received (raw int as Decimal)
    - maturity_timestamp parsed from PT symbol (None if symbol format unknown)
    - days_to_maturity   computed from maturity_timestamp (None if unavailable)
    - implied_apr_bps    locked yield at entry (None if maturity unavailable)
    """
    from almanak.framework.accounting.models import (
        AccountingConfidence,
        AccountingIdentity,
        PendleAccountingEvent,
        PendleEventType,
    )

    # Guard: SWAP intent for Pendle only
    intent_type_str = ""
    it = getattr(intent, "intent_type", None)
    if it is not None:
        intent_type_str = it.value if hasattr(it, "value") else str(it)
    if intent_type_str != "SWAP":
        return None

    protocol = (getattr(intent, "protocol", "") or "").lower()
    if _PENDLE_SWAP_PROTOCOL not in protocol:
        return None

    if not _is_pt_buy(intent, result):
        return None

    now = datetime.now(UTC)
    tx_hash = getattr(result, "tx_hash", None) or ""
    market_address = (getattr(intent, "pool", None) or "").lower()

    # ── Amounts from swap_amounts ─────────────────────────────────────────────
    extracted = getattr(result, "extracted_data", None) or {}
    swap_amounts = extracted.get("swap_amounts")
    sy_amount_raw: int | None = None
    pt_amount_raw: int | None = None
    if swap_amounts:
        sy_amount_raw = getattr(swap_amounts, "amount_in", None)
        pt_amount_raw = getattr(swap_amounts, "amount_out", None)

    sy_amount = Decimal(str(sy_amount_raw)) if sy_amount_raw is not None else None
    pt_amount = Decimal(str(pt_amount_raw)) if pt_amount_raw is not None else None

    # ── Resolve PT token symbol ───────────────────────────────────────────────
    # Prefer intent.to_token; fall back to swap_amounts.token_out when _is_pt_buy
    # fired via the secondary swap_amounts path and intent.to_token is not a PT symbol.
    intent_to_token = str(getattr(intent, "to_token", "") or "")
    if intent_to_token.upper().startswith("PT-"):
        pt_token_sym = intent_to_token
    else:
        token_out = str(getattr(swap_amounts, "token_out", "") or "") if swap_amounts else ""
        pt_token_sym = token_out if token_out.upper().startswith("PT-") else intent_to_token

    # ── pt_price ─────────────────────────────────────────────────────────────
    pt_price: Decimal | None = None
    if sy_amount and pt_amount and pt_amount > 0:
        try:
            pt_price = sy_amount / pt_amount
        except (InvalidOperation, ZeroDivisionError):
            pass

    # ── Maturity from PT symbol ───────────────────────────────────────────────
    maturity_ts = _parse_pt_maturity(pt_token_sym)
    days_to_maturity: int | None = None
    if maturity_ts is not None:
        # Calendar-day distance avoids partial-day truncation from timedelta.days.
        # A position with <24h remaining is still live; we want days_to_maturity >= 1
        # until the calendar date crosses the maturity date.
        days_to_maturity = (maturity_ts.date() - now.date()).days

    # ── Implied APR ───────────────────────────────────────────────────────────
    implied_apr_bps: int | None = None
    if pt_price is not None and days_to_maturity is not None:
        implied_apr_bps = compute_implied_apr_bps(pt_price, days_to_maturity)

    # ── Confidence ───────────────────────────────────────────────────────────
    has_core_fields = pt_price is not None and pt_amount is not None
    has_apr = implied_apr_bps is not None
    if has_core_fields and has_apr:
        confidence = AccountingConfidence.HIGH
        unavailable_reason = ""
    elif has_core_fields and maturity_ts is not None:
        # Maturity was parsed but days_to_maturity <= 0: position already matured.
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "PT matured — days_to_maturity <= 0, implied APR not applicable"
    elif has_core_fields:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "maturity not parsed from PT symbol (implied APR unavailable)"
    else:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "PT buy amounts unavailable from receipt"

    position_key = f"pendle_pt:{chain.lower()}:{wallet_address.lower()}:{market_address}"
    _id_seed = tx_hash or ledger_entry_id or position_key

    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, "PT_BUY", _id_seed, position_key),
        deployment_id=deployment_id,
        strategy_id=strategy_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        timestamp=now,
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id or "",
    )

    return PendleAccountingEvent(
        identity=identity,
        event_type=PendleEventType.PT_BUY,
        position_key=position_key,
        market_id=market_address,
        pt_token=pt_token_sym,
        maturity_timestamp=maturity_ts,
        pt_amount=pt_amount,
        sy_amount=sy_amount,
        pt_price=pt_price,
        implied_apr_bps=implied_apr_bps,
        days_to_maturity=days_to_maturity,
        realized_yield_usd=None,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )
