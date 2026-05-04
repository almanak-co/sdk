"""Generic LP accounting event builder for non-Pendle LP strategies (VIB-3515).

Covers: Aerodrome, Uniswap V3/V4, Curve, Velodrome, TraderJoe V2,
        PancakeSwap V3, SushiSwap V3, and any future LP connectors.
Pendle LP is handled by pendle_accounting.py; this builder skips Pendle.

Amounts are stored in human-decimal form using token0/token1 decimals from
the intent where available.  confidence is ESTIMATED when decimals must be
assumed (fallback to 18).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, LPEventType

logger = logging.getLogger(__name__)

_LP_INTENT_TYPES = frozenset({"LP_OPEN", "LP_CLOSE"})


class LPAccountingEvent:
    """Duck-typed LP accounting event consumed by AccountingWriter and both backends."""

    schema_version: int = 1

    def __init__(
        self,
        identity: AccountingIdentity,
        event_type: LPEventType,
        position_key: str,
        pool_address: str,
        token0: str,
        token1: str,
        amount0: Decimal | None,
        amount1: Decimal | None,
        lp_token_amount: Decimal | None,
        cost_basis_usd: Decimal | None,
        realized_pnl_usd: Decimal | None,
        fees0_collected: Decimal | None,
        fees1_collected: Decimal | None,
        confidence: AccountingConfidence,
        unavailable_reason: str = "",
        # VIB-3933 — fees expressed in USD (sum of fees0×price0 +
        # fees1×price1 at execution-block prices). Persisted separately
        # from ``realized_pnl_usd`` so the G6 reconciliation and the
        # dashboard cost stack can attribute fee income without
        # double-counting against realized PnL. ``realized_pnl_usd``
        # MUST be net-of-fees on this event for the G6 contract to
        # hold (see lp_handler computation).
        fees_total_usd: Decimal | None = None,
        # VIB-3893: position-range metadata propagated from receipt-parser
        # ``lp_open_data`` (and slot0 fallback). Populated on LP_OPEN; left
        # ``None`` on LP_CLOSE / LP_COLLECT_FEES where the bracket is the
        # one stamped at OPEN time and lives on ``position_events``.
        tick_lower: int | None = None,
        tick_upper: int | None = None,
        liquidity: int | None = None,
        current_tick: int | None = None,
        in_range: bool | None = None,
    ) -> None:
        self.identity = identity
        self.event_type = event_type.value
        self.position_key = position_key
        self.pool_address = pool_address
        self.token0 = token0
        self.token1 = token1
        self.amount0 = amount0
        self.amount1 = amount1
        self.lp_token_amount = lp_token_amount
        self.cost_basis_usd = cost_basis_usd
        self.realized_pnl_usd = realized_pnl_usd
        self.fees0_collected = fees0_collected
        self.fees1_collected = fees1_collected
        self.fees_total_usd = fees_total_usd
        self.confidence = confidence
        self.unavailable_reason = unavailable_reason
        self.tick_lower = tick_lower
        self.tick_upper = tick_upper
        self.liquidity = liquidity
        self.current_tick = current_tick
        self.in_range = in_range

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                return str(v)
            return v

        return json.dumps(
            {
                "event_type": self.event_type,
                "position_key": self.position_key,
                "pool_address": self.pool_address,
                "token0": self.token0,
                "token1": self.token1,
                "amount0": _enc(self.amount0),
                "amount1": _enc(self.amount1),
                "lp_token_amount": _enc(self.lp_token_amount),
                "cost_basis_usd": _enc(self.cost_basis_usd),
                "realized_pnl_usd": _enc(self.realized_pnl_usd),
                "fees0_collected": _enc(self.fees0_collected),
                "fees1_collected": _enc(self.fees1_collected),
                # VIB-3933 — net USD value of LP fees collected, populated
                # on LP_CLOSE / LP_COLLECT_FEES from token-level fees0/1
                # priced at execution-block oracle prices. Dashboard's
                # ``fees_earned_usd`` bucket and G6 ``sum_fees`` read this
                # field; ``realized_pnl_usd`` on the same event is net of
                # this amount so the two contribute additively (no
                # double-count).
                "fees_total_usd": _enc(self.fees_total_usd),
                "confidence": str(self.confidence),
                # VIB-3938 — write JSON null when the in-memory field is the
                # empty string ("no reason because confidence is HIGH"). Per
                # CLAUDE.md "Empty ≠ zero": "" in payload JSON is the parser-
                # didn't-emit signal and false-positives the 4b CONF invariant
                # query (``IS NOT NULL`` matches ""). Real reasons (ESTIMATED
                # / MISSING events) still serialize as themselves; only the
                # absence-signal collapses to null.
                "unavailable_reason": self.unavailable_reason or None,
                # VIB-3893 — position-range metadata. Pre-fix every LP_OPEN
                # accounting_event omitted these even though receipt-parser
                # populated them on ``lp_open_data``; downstream Trade Tape
                # rendered "in_range UNKNOWN" on every production LP open.
                "tick_lower": self.tick_lower,
                "tick_upper": self.tick_upper,
                "liquidity": self.liquidity,
                "current_tick": self.current_tick,
                "in_range": self.in_range,
                "schema_version": self.schema_version,
            }
        )


def _intent_type_str(intent: Any) -> str:
    it = getattr(intent, "intent_type", None)
    if it is None:
        return ""
    return it.value if hasattr(it, "value") else str(it)


def _get_pool_address(intent: Any) -> str:
    """Extract pool address or stable identifier from LP intent.

    Handles several pool field formats used across protocols:
    - "0xaddr"              → bare pool address
    - "TOKEN/0xaddr"        → Pendle-style (Pendle is excluded upstream, but handles gracefully)
    - "TOKEN0/TOKEN1/0xaddr" → last segment is the pool address
    - "TOKEN0/TOKEN1/stable" → stable/volatile type string — returned as-is for stable pool
      position_key disambiguation

    The position_key uses this value to distinguish positions, so for symbolic
    forms like "USDC/DAI/stable" the result is "usdc/dai/stable" which is still
    a stable, unique identifier.
    """
    pool = getattr(intent, "pool", None) or ""
    pool_str = str(pool).strip()
    if not pool_str:
        return ""
    # If no slash, treat as bare address/identifier
    if "/" not in pool_str:
        return pool_str.lower()
    # Check the last segment — if it starts with "0x" it's the pool address.
    last = pool_str.rsplit("/", 1)[1].strip()
    if last.lower().startswith("0x"):
        return last.lower()
    # Last segment is a pool type ("stable", "volatile") or similar label.
    # Return the full lowercased string as a stable position key component.
    return pool_str.lower()


def _to_human(raw: int | None, decimals: int) -> Decimal | None:
    if raw is None:
        return None
    scale = Decimal(10**decimals)
    return Decimal(str(raw)) / scale


def compute_lp_cost_basis(
    amount0: Decimal | None,
    amount1: Decimal | None,
    token0: str,
    token1: str,
    price_oracle: dict[str, Any] | None,
) -> Decimal | None:
    """Compute LP entry cost basis as amount0*price0 + amount1*price1.

    Returns None when price_oracle is unavailable, any non-None amount lacks a price,
    or both amounts are None (no legs contributed — not a concrete zero basis).
    price_oracle keys are uppercase token symbols (e.g. "WETH", "USDC").

    Public canonical implementation — also imported by
    ``framework.accounting.category_handlers.lp_handler``. The leading-underscore
    alias ``_compute_cost_basis`` is preserved for one release as an internal
    back-compat shim and may be removed in a future cleanup.
    """
    if not price_oracle:
        return None
    total = Decimal(0)
    has_any = False
    # ``token0`` / ``token1`` are typed as ``str`` upstream but a malformed
    # ledger row could carry ``None``. Guard with ``(t or "")`` to keep the
    # function fail-closed (returns None) instead of raising AttributeError.
    for amt, sym in ((amount0, (token0 or "").upper()), (amount1, (token1 or "").upper())):
        if amt is None:
            continue
        price = price_oracle.get(sym)
        if price is None:
            return None
        try:
            decimal_price = Decimal(str(price))
        except Exception:  # noqa: BLE001
            return None
        # Reject non-finite prices (NaN / Infinity) — they would propagate
        # through arithmetic into a NaN total and silently corrupt accounting.
        if not decimal_price.is_finite():
            return None
        try:
            total += amt * decimal_price
            has_any = True
        except Exception:  # noqa: BLE001
            return None
    if has_any and not total.is_finite():
        return None
    return total if has_any else None


# Back-compat alias — preserved so an in-flight or in-review caller that imported
# the leading-underscore symbol does not break. Prefer ``compute_lp_cost_basis``.
_compute_cost_basis = compute_lp_cost_basis


def build_lp_accounting_event(
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
    price_oracle: dict[str, Any] | None = None,
) -> LPAccountingEvent | None:
    """Build an LPAccountingEvent for a completed LP_OPEN or LP_CLOSE intent.

    Returns None for:
    - Non-LP intents
    - Pendle LP intents (handled by pendle_accounting.py)
    - Intents where the pool address cannot be resolved

    Amounts are sourced from result.lp_open_data / result.lp_close_data when
    available, with token decimals from the intent (fallback: 18 → ESTIMATED).
    cost_basis_usd is computed from price_oracle when provided.
    """
    intent_type_str = _intent_type_str(intent)
    if intent_type_str not in _LP_INTENT_TYPES:
        return None

    # Skip Pendle: it has its own builder with Pendle-specific market data.
    protocol = (getattr(intent, "protocol", "") or "").lower()
    if "pendle" in protocol:
        return None

    pool_address = _get_pool_address(intent)
    if not pool_address:
        logger.warning("LP accounting skipped: cannot resolve pool address from intent (protocol=%s)", protocol)
        return None

    event_type = LPEventType.LP_OPEN if intent_type_str == "LP_OPEN" else LPEventType.LP_CLOSE
    now = datetime.now(UTC)

    tx_hash = getattr(result, "tx_hash", None) or ""
    if not tx_hash:
        for tr in getattr(result, "transaction_results", None) or []:
            h = getattr(tr, "tx_hash", None)
            if h:
                tx_hash = h
                break

    token0 = str(getattr(intent, "token0", None) or getattr(intent, "token_a", None) or "")
    token1 = str(getattr(intent, "token1", None) or getattr(intent, "token_b", None) or "")
    # LP intents store tokens in the pool string (e.g. "WETH/USDC/3000", "USDC/DAI/stable").
    # Bare token0/token1 attributes are not set on LP intents, so parse from pool string.
    if not token0 or not token1:
        pool_str = (getattr(intent, "pool", "") or "").strip()
        if "/" in pool_str:
            parts = [p.strip() for p in pool_str.split("/") if p.strip()]
            normalized = [
                p.split("(")[0].split(" ")[0].strip()
                for p in parts
                if not p.strip().isdigit() and not p.strip().lower().startswith("0x")
            ]
            if not token0 and normalized:
                token0 = normalized[0].upper()
            if not token1 and len(normalized) > 1:
                token1 = normalized[1].upper()

    # Prefer explicit decimal fields; fall back to 18 with ESTIMATED confidence.
    # Use `is None` checks — `or` would treat decimals=0 as missing (valid for some tokens).
    dec0_raw = getattr(intent, "token0_decimals", None)
    if dec0_raw is None:
        dec0_raw = getattr(intent, "token_a_decimals", None)
    dec1_raw = getattr(intent, "token1_decimals", None)
    if dec1_raw is None:
        dec1_raw = getattr(intent, "token_b_decimals", None)
    assumed_decimals = dec0_raw is None or dec1_raw is None
    dec0 = int(dec0_raw) if dec0_raw is not None else 18
    dec1 = int(dec1_raw) if dec1_raw is not None else 18

    amount0: Decimal | None = None
    amount1: Decimal | None = None
    lp_token_amount: Decimal | None = None
    fees0_collected: Decimal | None = None
    fees1_collected: Decimal | None = None

    if intent_type_str == "LP_OPEN":
        lp_data = getattr(result, "lp_open_data", None)
        if lp_data is not None:
            amount0 = _to_human(getattr(lp_data, "amount0", None), dec0)
            amount1 = _to_human(getattr(lp_data, "amount1", None), dec1)
        else:
            # Fall back to extracted_data dict (older receipt parsers)
            extracted = getattr(result, "extracted_data", None) or {}
            amount0 = _to_human(extracted.get("amount0"), dec0)
            amount1 = _to_human(extracted.get("amount1"), dec1)
    else:
        lp_data = getattr(result, "lp_close_data", None)
        if lp_data is not None:
            amount0 = _to_human(getattr(lp_data, "amount0_collected", None), dec0)
            amount1 = _to_human(getattr(lp_data, "amount1_collected", None), dec1)
            fees0_collected = _to_human(getattr(lp_data, "fees0", None), dec0)
            fees1_collected = _to_human(getattr(lp_data, "fees1", None), dec1)

    confidence = AccountingConfidence.ESTIMATED if assumed_decimals else AccountingConfidence.HIGH
    unavailable_reason = ""
    if assumed_decimals:
        unavailable_reason = "token decimals assumed 18; LP amounts are estimated"

    position_key = f"lp:{protocol}:{chain.lower()}:{wallet_address.lower()}:{pool_address}"

    _id_seed = tx_hash or ledger_entry_id or str(uuid4())
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, event_type.value, _id_seed, position_key),
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

    # Skip cost basis when decimals were assumed: amounts may be off by 1e12 for 6-decimal tokens.
    cost_basis_usd = _compute_cost_basis(
        amount0, amount1, token0, token1, price_oracle if not assumed_decimals else None
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
        lp_token_amount=lp_token_amount,
        cost_basis_usd=cost_basis_usd,
        realized_pnl_usd=None,
        fees0_collected=fees0_collected,
        fees1_collected=fees1_collected,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )
