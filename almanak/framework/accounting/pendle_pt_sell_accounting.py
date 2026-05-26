"""Pendle PT pre-maturity sell accounting event builder (VIB-3492).

Wired into strategy_runner after every successful SWAP intent where
``intent.from_token`` is a known PT address or PT-prefixed symbol.

Records the early exit from a PT position:
  pt_amount  — PT tokens sold (raw int from swap receipt: amount_in)
  sy_amount  — SY / underlying tokens received (raw int from swap receipt: amount_out)
  pt_price   — sy_amount / pt_amount (< 1 before maturity)

Also reduces the FIFO lot in FIFOBasisStore (same instance as PT_BUY) via
match_pt_redeem so that a subsequent PT_REDEEM correctly matches only the
remaining lot.

Detection:
  A SWAP intent is a PT sell when ``intent.from_token`` starts with "PT-" or
  matches a known PT address in PT_TOKEN_INFO.  The secondary check from
  swap_amounts.token_in follows the same "PT-" prefix convention.

Unlike PT_BUY, a PT sell does NOT record a new lot — it *consumes* an existing
one.  If no FIFO lot exists (e.g. PT was transferred in), match_pt_redeem returns
unmatched_amount > 0 and confidence is set to ESTIMATED.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

from almanak.framework.accounting.ids import make_accounting_event_id

logger = logging.getLogger(__name__)

_PENDLE_SWAP_PROTOCOL = "pendle"


def _is_pt_sell(intent: Any, result: Any) -> bool:
    """Return True when the SWAP intent is selling PT tokens.

    Detects:
    1. intent.from_token starts with "PT-" (canonical PT symbol prefix).
    2. intent.from_token matches a known PT address in PT_TOKEN_INFO.
    3. Fallback: swap_amounts.token_in starts with "PT-" (enriched receipt path).

    The "PT-" prefix check requires a dash to avoid false positives (APT, OPT …).
    """
    from_token = str(getattr(intent, "from_token", "") or "")
    if from_token.upper().startswith("PT-"):
        return True

    # Check against registered PT addresses
    if from_token.startswith("0x") or from_token.startswith("0X"):
        try:
            from almanak.connectors.pendle.sdk import PT_TOKEN_INFO

            chain = str(getattr(intent, "chain", "") or "")
            if not chain:
                # Try all chains
                for chain_pts in PT_TOKEN_INFO.values():
                    for _sym, (addr, _dec) in chain_pts.items():
                        if addr.lower() == from_token.lower():
                            return True
            else:
                chain_pts = PT_TOKEN_INFO.get(chain.lower(), {})
                for _sym, (addr, _dec) in chain_pts.items():
                    if addr.lower() == from_token.lower():
                        return True
        except Exception:
            logger.debug("PT_TOKEN_INFO lookup failed for from_token=%s", from_token, exc_info=True)

    # Fallback: check swap_amounts.token_in
    extracted = getattr(result, "extracted_data", None) or {}
    swap_amounts = extracted.get("swap_amounts")
    if swap_amounts:
        token_in = str(getattr(swap_amounts, "token_in", "") or "")
        if token_in.upper().startswith("PT-"):
            return True

    return False


def _resolve_pt_token_sym(intent: Any, result: Any) -> str:
    """Return the canonical PT token symbol for the sell.

    Prefers intent.from_token when it looks like a PT symbol.
    Falls back to a PT_TOKEN_INFO address lookup so the symbol matches what
    the PT_BUY lot was stored under, ensuring FIFO matching consistency.
    Then tries swap_amounts.token_in from the enriched receipt.
    Returns "PT" as last resort.
    """
    from_token = str(getattr(intent, "from_token", "") or "")
    if from_token.upper().startswith("PT-"):
        return from_token

    # Resolve address → symbol so lot key matches the PT_BUY recording convention
    if from_token.startswith("0x") or from_token.startswith("0X"):
        try:
            from almanak.connectors.pendle.sdk import PT_TOKEN_INFO

            chain = str(getattr(intent, "chain", "") or "").lower()
            if chain:
                for sym, (addr, _) in PT_TOKEN_INFO.get(chain, {}).items():
                    if addr.lower() == from_token.lower():
                        return sym
            else:
                for chain_pts in PT_TOKEN_INFO.values():
                    for sym, (addr, _) in chain_pts.items():
                        if addr.lower() == from_token.lower():
                            return sym
        except Exception:
            pass

    extracted = getattr(result, "extracted_data", None) or {}
    swap_amounts = extracted.get("swap_amounts")
    if swap_amounts:
        token_in = str(getattr(swap_amounts, "token_in", "") or "")
        if token_in.upper().startswith("PT-"):
            return token_in

    return from_token or "PT"


def build_pendle_pt_sell_accounting_event(
    *,
    intent: Any,
    result: Any,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    basis_store: FIFOBasisStore,
    ledger_entry_id: str | None = None,
) -> Any | None:
    """Build a PendleAccountingEvent(PT_SELL) for a completed Pendle PT pre-maturity sale.

    Returns None when:
    - The intent is not a SWAP for Pendle protocol.
    - The from_token is not a PT token (i.e. this is a PT_BUY, not a PT_SELL).

    Also calls ``basis_store.match_pt_redeem()`` to reduce the FIFO lot so
    subsequent PT_REDEEM events only match the remaining PT quantity.

    Fields populated:
    - pt_amount   PT sold (raw int as Decimal; stored as raw for FIFO replay parity)
    - sy_amount   SY / tokens received (raw int as Decimal)
    - pt_price    sy_amount / pt_amount (None when amounts unavailable)
    - confidence  ESTIMATED (lot-unmatched case) or HIGH (fully matched)
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

    if not _is_pt_sell(intent, result):
        return None

    now = datetime.now(UTC)
    tx_hash = getattr(result, "tx_hash", None) or ""
    market_address = (getattr(intent, "pool", None) or "").lower()
    if not market_address:
        logger.debug("PT_SELL: missing pool/market_address on intent; skipping event")
        return None

    # ── Amounts from swap_amounts (raw ints) ──────────────────────────────────
    # PT_SELL: from_token = PT (amount_in = pt sold), to_token = SY (amount_out = sy received)
    extracted = getattr(result, "extracted_data", None) or {}
    swap_amounts = extracted.get("swap_amounts")
    pt_amount_raw: int | None = None
    sy_amount_raw: int | None = None
    if swap_amounts:
        pt_amount_raw = getattr(swap_amounts, "amount_in", None)
        sy_amount_raw = getattr(swap_amounts, "amount_out", None)

    # Store as raw Decimal (same convention as PT_BUY) so FIFO replay in
    # reconstruct_from_events divides by 1e18.
    pt_amount = Decimal(str(pt_amount_raw)) if pt_amount_raw is not None else None
    sy_amount = Decimal(str(sy_amount_raw)) if sy_amount_raw is not None else None

    # ── pt_price ─────────────────────────────────────────────────────────────
    pt_price: Decimal | None = None
    if sy_amount is not None and pt_amount is not None and pt_amount > 0:
        try:
            pt_price = sy_amount / pt_amount
        except (InvalidOperation, ZeroDivisionError):
            pass

    # ── Resolve PT token symbol ───────────────────────────────────────────────
    pt_token_sym = _resolve_pt_token_sym(intent, result)

    # ── FIFO lot reduction ────────────────────────────────────────────────────
    # Reduce remaining_pt in the existing lot so PT_REDEEM only matches what's left.
    position_key = f"pendle_pt:{chain.lower()}:{wallet_address.lower()}:{market_address}"
    _PT_DECIMALS_ASSUMED = 18  # Consistent with PT_BUY recording assumption
    _scale = Decimal(10**_PT_DECIMALS_ASSUMED)
    pt_human = pt_amount / _scale if pt_amount is not None else None
    sy_human = sy_amount / _scale if sy_amount is not None else None

    unmatched_amount = Decimal("0")
    if pt_human is not None and sy_human is not None and pt_human > 0:
        match_result = basis_store.match_pt_redeem(
            deployment_id=deployment_id,
            position_key=position_key,
            pt_token=pt_token_sym,
            pt_redeemed=pt_human,
            sy_received=sy_human,
        )
        unmatched_amount = match_result.unmatched_amount
    elif pt_amount is not None:
        # Amounts present but sy_human is None — treat as unmatched (no sy basis)
        unmatched_amount = pt_human or Decimal("0")

    # ── Confidence ───────────────────────────────────────────────────────────
    has_amounts = pt_amount is not None and sy_amount is not None
    fully_matched = unmatched_amount == Decimal("0") and has_amounts

    if fully_matched and pt_price is not None:
        confidence = AccountingConfidence.HIGH
        unavailable_reason = ""
    elif has_amounts and not fully_matched:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = (
            f"PT_BUY lots partially/not matched (unmatched={unmatched_amount}); "
            "realized cost-basis attribution incomplete"
        )
    elif not has_amounts:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "PT/SY amounts unavailable from receipt"
    else:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "partial data"

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, "PT_SELL", _id_seed, position_key),
        deployment_id=deployment_id,
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
        event_type=PendleEventType.PT_SELL,
        position_key=position_key,
        market_id=market_address,
        pt_token=pt_token_sym,
        maturity_timestamp=None,  # sold before maturity; maturity not tracked here
        pt_amount=pt_amount,
        sy_amount=sy_amount,
        pt_price=pt_price,
        implied_apr_bps=None,  # realized APR on early sell not computed here
        days_to_maturity=None,
        realized_yield_usd=None,  # actual P&L computed at reporting time from basis lots
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )
