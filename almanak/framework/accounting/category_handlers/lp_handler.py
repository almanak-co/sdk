"""LP category handler for AccountingProcessor (VIB-3470).

Ports logic from lp_accounting.py to work from ledger_row / outbox_row dicts
rather than live intent / result objects.  No live chain calls.

Pendle LP is handled by pendle_handler.py; this handler skips any intent whose
protocol contains "pendle".
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.lp_accounting import LPAccountingEvent, compute_lp_cost_basis
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, LPEventType

logger = logging.getLogger(__name__)

_LP_OPEN_CLOSE = frozenset({"LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"})

_INTENT_TO_EVENT_TYPE: dict[str, LPEventType] = {
    "LP_OPEN": LPEventType.LP_OPEN,
    "LP_CLOSE": LPEventType.LP_CLOSE,
    "LP_COLLECT_FEES": LPEventType.LP_COLLECT_FEES,
}


def _parse_price_oracle(price_inputs_json: str) -> dict | None:
    if not price_inputs_json:
        return None
    try:
        d = json.loads(price_inputs_json)
        return d if isinstance(d, dict) else None
    except (json.JSONDecodeError, TypeError):
        return None


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else None
    except Exception:  # noqa: BLE001
        return None


def _pool_address_from_position_key(position_key: str) -> str:
    """Extract the pool address (last ':' segment) from a position key.

    e.g. "lp:aerodrome:base:0xwallet:0xpooladdr" → "0xpooladdr"
    """
    if not position_key:
        return ""
    return position_key.rsplit(":", 1)[-1]


def _to_human_from_raw(raw: Any, decimals: int) -> Decimal | None:
    """Convert a raw integer amount (possibly stored as string) to human-decimal."""
    if raw is None:
        return None
    try:
        scale = Decimal(10**decimals)
        return Decimal(str(int(raw))) / scale
    except Exception:  # noqa: BLE001
        return None


def _resolve_lp_amounts(
    extracted: dict[str, Any],
    intent_type_str: str,
    token0: str,
    token1: str,
    chain: str,
    amount_in_str: str,
    amount_out_str: str,
) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, bool]:
    """Return (amount0, amount1, fees0, fees1, assumed_decimals).

    Priority:
      1. LPOpenData / LPCloseData typed objects from extracted_data_json
         — decimals resolved via token_resolver (HIGH confidence if available)
      2. amount_in / amount_out strings from ledger row (already human-decimal)
         — no scaling needed; decimals are considered known (HIGH confidence)
      3. All None (can't determine amounts)

    Typed objects carry raw int amounts; we need token decimals to scale them.
    If the token resolver fails we fall back to the amount_in/amount_out fields.
    """
    amount0: Decimal | None = None
    amount1: Decimal | None = None
    fees0: Decimal | None = None
    fees1: Decimal | None = None
    assumed_decimals = False

    # ── Try typed extracted_data objects first ───────────────────────────────
    lp_open_data = extracted.get("lp_open_data")
    lp_close_data = extracted.get("lp_close_data")

    # Resolve decimals from token_resolver so we can scale raw ints.
    dec0: int | None = None
    dec1: int | None = None
    if (lp_open_data is not None or lp_close_data is not None) and (token0 or token1):
        try:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            resolver = get_token_resolver()
            if token0:
                ti0 = resolver.resolve(token0, chain=chain)
                dec0 = ti0.decimals if ti0 is not None else None
            if token1:
                ti1 = resolver.resolve(token1, chain=chain)
                dec1 = ti1.decimals if ti1 is not None else None
        except Exception:  # noqa: BLE001
            logger.debug("LP handler: token resolver failed for %s/%s on %s", token0, token1, chain)

    if lp_open_data is not None and intent_type_str == "LP_OPEN":
        raw0 = getattr(lp_open_data, "amount0", None)
        raw1 = getattr(lp_open_data, "amount1", None)
        if dec0 is not None:
            amount0 = _to_human_from_raw(raw0, dec0)
        if dec1 is not None:
            amount1 = _to_human_from_raw(raw1, dec1)
        if dec0 is None or dec1 is None:
            assumed_decimals = True
        return amount0, amount1, None, None, assumed_decimals

    if lp_close_data is not None and intent_type_str in ("LP_CLOSE", "LP_COLLECT_FEES"):
        raw0 = getattr(lp_close_data, "amount0_collected", None)
        raw1 = getattr(lp_close_data, "amount1_collected", None)
        raw_fees0 = getattr(lp_close_data, "fees0", None)
        raw_fees1 = getattr(lp_close_data, "fees1", None)
        if dec0 is not None:
            amount0 = _to_human_from_raw(raw0, dec0)
            fees0 = _to_human_from_raw(raw_fees0, dec0)
        if dec1 is not None:
            amount1 = _to_human_from_raw(raw1, dec1)
            fees1 = _to_human_from_raw(raw_fees1, dec1)
        if dec0 is None or dec1 is None:
            assumed_decimals = True
        return amount0, amount1, fees0, fees1, assumed_decimals

    # ── Fallback: use human-decimal strings from ledger row ──────────────────
    # These are already in user-facing units — no scaling.
    amount0 = _safe_decimal(amount_in_str) if amount_in_str else None
    amount1 = _safe_decimal(amount_out_str) if amount_out_str else None
    # assumed_decimals stays False here because no scaling was done.
    return amount0, amount1, None, None, False


def handle_lp(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
) -> LPAccountingEvent | None:
    """Build an LPAccountingEvent from an outbox + ledger row pair.

    Returns None for:
    - Non-LP intent types
    - Pendle LP intents (handled by pendle_handler.py)
    - Intents where both position_key and market_id are absent (cannot identify pool)

    All inputs come from the dicts — no live chain calls.
    """
    from almanak.framework.observability.ledger import deserialize_extracted_data

    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    if intent_type_str not in _LP_OPEN_CLOSE:
        return None

    protocol = (ledger_row.get("protocol") or "").lower()
    if "pendle" in protocol:
        return None

    event_type = _INTENT_TO_EVENT_TYPE.get(intent_type_str)
    if event_type is None:
        return None

    # ── Identity fields ──────────────────────────────────────────────────────
    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    strategy_id = ledger_row.get("strategy_id") or outbox_row.get("strategy_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""
    position_key = outbox_row.get("position_key") or ""

    # ── Timestamp ────────────────────────────────────────────────────────────
    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        timestamp = datetime.now(UTC)

    # ── Pool address ─────────────────────────────────────────────────────────
    # Parse from the last segment of position_key; fall back to market_id.
    pool_address = _pool_address_from_position_key(position_key) or outbox_row.get("market_id") or ""

    if not pool_address:
        logger.warning(
            "LP handler: cannot resolve pool address from position_key=%r or market_id=%r; dropping event",
            position_key,
            outbox_row.get("market_id"),
        )
        return None

    # ── Tokens ───────────────────────────────────────────────────────────────
    token0 = (ledger_row.get("token_in") or "").upper()
    token1 = (ledger_row.get("token_out") or "").upper()

    # ── Extracted data ───────────────────────────────────────────────────────
    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")

    amount0, amount1, fees0, fees1, assumed_decimals = _resolve_lp_amounts(
        extracted=extracted,
        intent_type_str=intent_type_str,
        token0=token0,
        token1=token1,
        chain=chain,
        amount_in_str=ledger_row.get("amount_in") or "",
        amount_out_str=ledger_row.get("amount_out") or "",
    )

    # ── USD pricing (VIB-3756) ───────────────────────────────────────────────
    # The handler used to hard-code ``cost_basis_usd=None`` which downstream
    # dashboards (QA harness deployed_usd column, position-PnL reporter)
    # render as "$0.00". That made an LP_OPEN that *did* mint an NFT and fire
    # accounting events look like a $0 deposit.
    #
    # ``price_inputs_json`` is captured at execution time (VIB-3480 audit-grade
    # replay) and contains uppercase token-symbol → USD-price entries. Reusing
    # the live-builder's ``compute_lp_cost_basis`` keeps the same fail-closed
    # contract as ``swap_handler.py``: any non-None amount whose price is
    # missing returns None for the whole sum (NOT 0). Decimals-assumed events
    # also bypass pricing because amounts can be off by 1e12 for 6-decimal
    # tokens — pricing them would print confidently wrong USD numbers.
    price_oracle = _parse_price_oracle(ledger_row.get("price_inputs_json") or "")
    cost_basis_usd: Decimal | None = None
    pricing_unavailable_reason = ""
    if not assumed_decimals:
        cost_basis_usd = compute_lp_cost_basis(amount0, amount1, token0, token1, price_oracle)
        if cost_basis_usd is None:
            # Distinguish "no price oracle attached" from "price-oracle present but
            # one of token0/token1 was missing a quote". Operators triaging a
            # $None deployed_usd column need this disambiguation.
            if not price_oracle:
                pricing_unavailable_reason = (
                    f"{intent_type_str} cost_basis_usd unavailable: no price_inputs_json on ledger row"
                )
            else:
                missing: list[str] = []
                invalid: list[str] = []
                # ``token0`` / ``token1`` are typed as ``str`` upstream but a
                # malformed ledger row could carry ``None``. ``(t or "")`` keeps
                # the diagnostic alive without raising AttributeError.
                token_pairs = (
                    (amount0, (token0 or "").upper()),
                    (amount1, (token1 or "").upper()),
                )
                for amt, sym in token_pairs:
                    if amt is None:
                        continue
                    raw = price_oracle.get(sym)
                    if raw is None:
                        missing.append(sym or "?")
                        continue
                    if _safe_decimal(raw) is None:
                        invalid.append(sym or "?")
                if missing:
                    pricing_unavailable_reason = (
                        f"{intent_type_str} cost_basis_usd unavailable: missing prices in "
                        f"price_inputs_json: {', '.join(missing)}"
                    )
                elif invalid:
                    # ``price_inputs_json`` carried a key for the token but its
                    # value was non-numeric / NaN / Infinity. Surface this as
                    # distinct from "missing" so operators can tell whether the
                    # producer side dropped a price entirely or wrote a bad one.
                    pricing_unavailable_reason = (
                        f"{intent_type_str} cost_basis_usd unavailable: invalid prices in "
                        f"price_inputs_json: {', '.join(invalid)}"
                    )
                else:
                    # Defensive: covers the "both legs are None amounts" case
                    # where compute_lp_cost_basis returns None but neither the
                    # missing nor invalid bucket fired. Without this, operators
                    # see cost_basis_usd=None with no explanation.
                    pricing_unavailable_reason = (
                        f"{intent_type_str} cost_basis_usd unavailable: no resolvable amount legs"
                    )

    if assumed_decimals:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "token decimals assumed; LP amounts are estimated"
    elif cost_basis_usd is None and pricing_unavailable_reason:
        # Decimals are known but pricing is missing — still HIGH confidence on
        # the unit amounts (those are correct), but flag the dollar field.
        confidence = AccountingConfidence.HIGH
        unavailable_reason = pricing_unavailable_reason
    else:
        confidence = AccountingConfidence.HIGH
        unavailable_reason = ""

    # ── Identity / ID ────────────────────────────────────────────────────────
    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, intent_type_str, _id_seed, position_key),
        deployment_id=deployment_id,
        strategy_id=strategy_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        timestamp=timestamp,
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id,
    )

    return LPAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=position_key,
        pool_address=pool_address,
        token0=token0,
        token1=token1,
        amount0=amount0,
        amount1=amount1,
        lp_token_amount=None,
        cost_basis_usd=cost_basis_usd,
        realized_pnl_usd=None,
        fees0_collected=fees0,
        fees1_collected=fees1,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )
