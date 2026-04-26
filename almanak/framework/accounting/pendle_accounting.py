"""Pendle LP accounting event builder (VIB-3421).

Wired into strategy_runner after every successful LP_OPEN / LP_CLOSE for
Pendle markets.  Produces a PendleAccountingEvent(LP_OPEN|LP_CLOSE) which
is persisted to the local accounting_events store via AccountingWriter.

Amount reporting:
  SY / PT amounts are stored as raw on-chain integers in the sy_amount /
  pt_amount fields until a decimal-aware Pendle market resolver is added
  (VIB-3422 + VIB-3423 scope).  confidence is always ESTIMATED until
  VIB-3422 adds human-decimal conversion and pt_token resolution.
  The position_event pipeline (via LPOpenData / LPCloseData) carries
  the amounts through the 7-phase builder and pnl_attributor for cost
  basis and IL attribution — that path is independent of USD conversion.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

# Fixed namespace for deterministic AccountingIdentity.id (uuid5 → same inputs → same UUID).
_ACCOUNTING_EVENT_NAMESPACE = uuid.UUID("5c4da812-3b0f-4e47-9a32-1b8c6d0f2e5a")

logger = logging.getLogger(__name__)

_PENDLE_LP_INTENT_TYPES = frozenset({"LP_OPEN", "LP_CLOSE"})


def _derive_pendle_position_key(chain: str, wallet: str, market_address: str) -> str:
    """Canonical position key for a Pendle LP position."""
    return f"pendle_lp:{chain.lower()}:{wallet.lower()}:{market_address.lower()}"


def _intent_type_value(intent: Any) -> str:
    it = getattr(intent, "intent_type", None)
    if it is None:
        return ""
    return it.value if hasattr(it, "value") else str(it)


def _get_market_address(intent: Any) -> str:
    """Extract the Pendle market address from the intent pool field.

    LP_OPEN pool format is "TOKEN/0xmarket_address"; LP_CLOSE is bare "0xmarket_address".
    Parses out the address portion in both cases.  Returns empty string when the
    market address cannot be resolved — callers should guard against empty values.
    """
    pool = getattr(intent, "pool", None)
    if not pool:
        return ""
    pool_str = str(pool).strip()
    if "/" in pool_str:
        pool_str = pool_str.split("/", 1)[1].strip()
    return pool_str.lower() if pool_str.startswith("0x") else ""


def build_pendle_lp_accounting_event(
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
    """Build a PendleAccountingEvent for a completed LP_OPEN or LP_CLOSE intent.

    Returns None for non-Pendle-LP intents or if the intent type cannot be mapped.

    Raw amounts (SY, PT, LP tokens) are stored from the extracted data.
    USD conversion is deferred until a proper price oracle path for Pendle
    tokens is added (VIB-3422).
    """
    from almanak.framework.accounting.models import (
        AccountingConfidence,
        AccountingIdentity,
        PendleAccountingEvent,
        PendleEventType,
    )

    intent_type_str = _intent_type_value(intent)
    if intent_type_str not in _PENDLE_LP_INTENT_TYPES:
        return None

    # Only handle Pendle protocol
    protocol = (getattr(intent, "protocol", "") or "").lower()
    if "pendle" not in protocol:
        return None

    event_type = PendleEventType.LP_OPEN if intent_type_str == "LP_OPEN" else PendleEventType.LP_CLOSE

    now = datetime.now(UTC)
    tx_hash = getattr(result, "tx_hash", None) or ""
    if not tx_hash:
        for tr in getattr(result, "transaction_results", None) or []:
            h = getattr(tr, "tx_hash", None)
            if h:
                tx_hash = h
                break

    market_address = _get_market_address(intent)
    if not market_address:
        logger.debug("Pendle LP accounting: intent.pool missing, skipping event")
        return None
    position_key = _derive_pendle_position_key(chain, wallet_address, market_address)

    # Extract raw amounts from the position pipeline data
    extracted = getattr(result, "extracted_data", None) or {}
    sy_amount_raw: int | None = None
    pt_amount_raw: int | None = None

    if intent_type_str == "LP_OPEN":
        lp_open = extracted.get("lp_open_data")
        if lp_open:
            sy_amount_raw = getattr(lp_open, "amount0", None)  # net_sy_used
            pt_amount_raw = getattr(lp_open, "amount1", None)  # net_pt_used
    else:
        lp_close = extracted.get("lp_close_data")
        if lp_close:
            sy_amount_raw = getattr(lp_close, "amount0_collected", None)  # net_sy_out
            pt_amount_raw = getattr(lp_close, "amount1_collected", None)  # net_pt_out

    # Human-decimal amounts: all Pendle SY/PT tokens use 18 decimals.
    _SCALE = Decimal(10**18)
    sy_amount = Decimal(str(sy_amount_raw)) / _SCALE if sy_amount_raw is not None else None
    pt_amount = Decimal(str(pt_amount_raw)) / _SCALE if pt_amount_raw is not None else None

    # ESTIMATED: 18-decimal assumption is correct for all current Pendle SY/PT
    # tokens but is not dynamically verified. pt_token and USD price are absent.
    confidence = AccountingConfidence.ESTIMATED
    unavailable_reason = "SY/PT scaled by assumed 18-decimal precision; pt_token and USD price absent"

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=str(
            uuid.uuid5(
                _ACCOUNTING_EVENT_NAMESPACE,
                f"pendle_lp:{deployment_id}:{cycle_id}:{intent_type_str}:{_id_seed}",
            )
        ),
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
        event_type=event_type,
        position_key=position_key,
        market_id=market_address,
        pt_token="",  # resolved when Pendle market reader is added (VIB-3422)
        maturity_timestamp=None,
        pt_amount=pt_amount,
        sy_amount=sy_amount,
        pt_price=None,
        implied_apr_bps=None,
        days_to_maturity=None,
        realized_yield_usd=None,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )
