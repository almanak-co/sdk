"""Pendle PT maturity settlement accounting event builder (VIB-3423).

Wired into strategy_runner after every successful WITHDRAW intent on Pendle
that produces a RedeemPY event (PT redemption at maturity).

Records the realized fixed yield:
  original_sy_paid  — from FIFO lot recorded at PT_BUY time (VIB-3422)
  sy_received       — from RedeemPY receipt (net_sy_redeemed)
  realized_yield    — sy_received_human − original_sy_paid_human
  realized_yield_usd — realized_yield * sy_price_usd (if price_oracle available)
  yield_apr_bps     — realized_yield / original_sy_paid / hold_days * 365 * 10_000

Lot matching uses FIFOBasisStore (same instance as lending):
  - position_key = pendle_pt:{chain}:{wallet}:{market_address}
  - token = "SY"
  - The PT_BUY hook (VIB-3422) records principal_amount = sy_human at purchase time

SY decimals assumption: 18 (consistent with PT_BUY recording; most Pendle SY tokens
are 18-decimal wrappers). ESTIMATED confidence records this assumption.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

logger = logging.getLogger(__name__)

_PENDLE_WITHDRAW_PROTOCOL = "pendle"
_SY_DECIMALS_ASSUMED = 18  # See module docstring — 18d is correct for most SY tokens


def _derive_pendle_pt_position_key(chain: str, wallet: str, market_address: str) -> str:
    """Canonical PT position key matching the one written at PT_BUY time."""
    return f"pendle_pt:{chain.lower()}:{wallet.lower()}:{market_address.lower()}"


def _is_pendle_redeem(intent: Any, result: Any) -> bool:
    """Return True when the result contains a RedeemPY event."""
    extracted = getattr(result, "extracted_data", None) or {}
    redemption = extracted.get("redemption_amounts")
    if isinstance(redemption, dict) and redemption.get("sy_received") is not None:
        return True
    return False


def build_pendle_pt_redeem_accounting_event(
    *,
    intent: Any,
    result: Any,
    deployment_id: str,
    strategy_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    basis_store: FIFOBasisStore,
    price_oracle: dict | None = None,
    ledger_entry_id: str | None = None,
) -> Any | None:
    """Build a PendleAccountingEvent(PT_REDEEM) for a completed Pendle PT redemption.

    Returns None when:
    - The intent is not a WITHDRAW for Pendle protocol
    - The result has no RedeemPY event (not a PT redemption)
    - The result has no redemption_amounts in extracted_data

    Fields populated:
    - sy_amount          SY received (human-decimal, 18d assumed)
    - pt_amount          PT redeemed (py_redeemed, raw converted to human 18d)
    - realized_yield_usd sy_received_usd - original_sy_paid_usd (from FIFO lots)
    - confidence         ESTIMATED (18-decimal assumption on SY)
    """
    from almanak.framework.accounting.models import (
        AccountingConfidence,
        AccountingIdentity,
        PendleAccountingEvent,
        PendleEventType,
    )

    # Guard: WITHDRAW intent for Pendle
    intent_type_str = ""
    it = getattr(intent, "intent_type", None)
    if it is not None:
        intent_type_str = it.value if hasattr(it, "value") else str(it)
    if intent_type_str != "WITHDRAW":
        return None

    protocol = (getattr(intent, "protocol", "") or "").lower()
    if _PENDLE_WITHDRAW_PROTOCOL not in protocol:
        return None

    extracted = getattr(result, "extracted_data", None) or {}
    redemption = extracted.get("redemption_amounts")
    if not isinstance(redemption, dict):
        return None

    sy_received_raw = redemption.get("sy_received")
    py_redeemed_raw = redemption.get("py_redeemed")
    if sy_received_raw is None:
        return None

    now = datetime.now(UTC)
    tx_hash = getattr(result, "tx_hash", None) or ""
    market_address = (getattr(intent, "pool", None) or "").lower()

    # ── Human-decimal amounts (18-decimal assumption) ─────────────────────────
    _scale = Decimal(10**_SY_DECIMALS_ASSUMED)
    try:
        sy_received_human = Decimal(str(sy_received_raw)) / _scale
        py_redeemed_human = Decimal(str(py_redeemed_raw)) / _scale if py_redeemed_raw is not None else None
    except (InvalidOperation, TypeError):
        return None

    # ── FIFO lot matching ─────────────────────────────────────────────────────
    position_key = _derive_pendle_pt_position_key(chain, wallet_address, market_address)
    pt_token_sym = str(getattr(intent, "from_token", "") or "PT")
    match_result = basis_store.match_pt_redeem(
        deployment_id=deployment_id,
        position_key=position_key,
        pt_token=pt_token_sym,
        pt_redeemed=py_redeemed_human if py_redeemed_human is not None else sy_received_human,
        sy_received=sy_received_human,
    )

    interest_human = match_result.interest_or_yield  # sy_received - original_sy_cost
    has_lots = match_result.unmatched_amount == Decimal("0")

    # ── USD conversion ─────────────────────────────────────────────────────────
    # Only convert when fully matched: unmatched PT has zero cost basis so
    # interest_or_yield would equal all of sy_received, overstating realized yield.
    realized_yield_usd: Decimal | None = None
    if has_lots and price_oracle is not None:
        sy_price = price_oracle.get("SY") or price_oracle.get("sy")
        if sy_price is not None and interest_human is not None:
            try:
                realized_yield_usd = Decimal(str(sy_price)) * interest_human
            except (InvalidOperation, TypeError):
                pass

    # ── Yield APR computation ─────────────────────────────────────────────────
    # yield_apr_bps = (interest / principal) / (hold_days / 365) * 10_000
    # hold_days comes from earliest_lot_timestamp returned by match_pt_redeem.
    yield_apr_bps: int | None = None
    if (
        has_lots
        and match_result.earliest_lot_timestamp is not None
        and interest_human is not None
        and match_result.repaid_principal > 0
    ):
        hold_days = (now.date() - match_result.earliest_lot_timestamp.date()).days
        if hold_days > 0:
            try:
                apr = (
                    interest_human
                    / match_result.repaid_principal
                    / (Decimal(str(hold_days)) / Decimal("365"))
                    * Decimal("10000")
                )
                yield_apr_bps = int(apr.to_integral_value())
            except (InvalidOperation, ZeroDivisionError):
                pass

    # ── Confidence ───────────────────────────────────────────────────────────
    if realized_yield_usd is not None and has_lots:
        confidence = AccountingConfidence.ESTIMATED  # 18d SY assumption
        unavailable_reason = "SY decimals assumed 18; USD price from oracle"
    elif has_lots:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "realized_yield_usd unavailable (no SY price in oracle)"
    else:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = (
            f"PT_BUY lots not fully matched (unmatched={match_result.unmatched_amount}); "
            "realized_yield_usd omitted — cost basis incomplete"
        )

    pt_token = str(getattr(intent, "from_token", "") or "")

    identity = AccountingIdentity(
        id=f"pendle_pt_{deployment_id}_{cycle_id}_PT_REDEEM_{tx_hash[-8:] if tx_hash else 'unknown'}",
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
        event_type=PendleEventType.PT_REDEEM,
        position_key=position_key,
        market_id=market_address,
        pt_token=pt_token,
        maturity_timestamp=None,  # at redemption the position is closed; maturity is now
        pt_amount=py_redeemed_human,
        sy_amount=sy_received_human,
        pt_price=None,  # not meaningful at redemption (PT=1:1 SY at maturity)
        implied_apr_bps=yield_apr_bps,  # realized APR over hold period (None when lot timestamp unavailable)
        days_to_maturity=0,
        realized_yield_usd=realized_yield_usd,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )
