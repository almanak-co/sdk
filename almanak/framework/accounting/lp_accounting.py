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

    # NOTE: this per-event attribute is a fallback only — the
    # ``writer.augment_accounting_payload`` chokepoint UNCONDITIONALLY
    # overwrites ``schema_version`` with the GLOBAL
    # ``payload_schemas.SCHEMA_VERSION`` at write time, so the persisted value
    # is always the global one. VIB-4275 adds an additive ``position_id``
    # payload field; we deliberately do NOT bump the global ``SCHEMA_VERSION``
    # because it is shared across every primitive and a bump would restamp
    # Lending / Perp / Vault / etc. rows too — violating the per-primitive
    # isolation the matching-policy-version map exists to provide. The additive
    # field is tolerant (readers default a missing ``position_id`` to ``None``),
    # and the behaviour change (co-pool close→open matching) is signalled by the
    # per-primitive ``matching_policy_version`` bump for ``Primitive.LP`` instead.
    schema_version: int = 1
    # VIB-4166 (T6) — see ``almanak.framework.accounting.payload_schemas`` module
    # docstring for the bump policy. Class attribute so the augment chokepoint
    # has a sane fallback when writers don't override it; the chokepoint
    # overwrites with the canonical per-primitive value at write time.
    primitive_version: int = 1

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
        # VIB-4319 — impermanent-loss diagnostic stamped on LP_CLOSE /
        # LP_COLLECT_FEES events. ``il_usd = V_lp_at_close − V_hodl_at_close``
        # where ``V_lp`` is the USD value of the principal recovered at
        # close-time prices (this event's freshly-computed
        # ``cost_basis_usd``) and ``V_hodl`` is the USD value of the
        # entry amounts re-priced at the same close-time oracle. Negative
        # = LP lost vs HODL. Persisted ``None`` when the prior OPEN
        # payload is missing or when the close-time oracle lacks a
        # non-zero leg's price (Empty ≠ Zero — never substitute 0 for
        # "unmeasured"). LP_OPEN leaves both fields ``None`` because IL is
        # only defined at unwind. Diagnostic only — NOT added to
        # ``realized_pnl_usd``; ``cost_basis_usd`` already reflects the
        # post-IL on-chain outcome.
        il_usd: Decimal | None = None,
        hodl_value_usd: Decimal | None = None,
        # VIB-4473 — V4 lot-matching anchor (keccak of
        # owner ‖ tickLower ‖ tickUpper ‖ salt per V4 ``Position.calculatePositionKey``).
        # V3 callers leave this None and lot-matching falls back to
        # ``position_token_id``; V4 receipt parser populates it (T05).
        position_hash: str | None = None,
        # VIB-4275 — per-position discriminator for co-pool attribution.
        # On LP_OPEN this is the minted NFT token id (``LPOpenData.position_id``)
        # written into the open payload so a later LP_CLOSE / LP_COLLECT_FEES on
        # the SAME pool-level ``position_key`` can resolve THIS leg's open rather
        # than the most-recent open by timestamp. ``None`` for connectors that
        # have no per-position id (fungible-LP venues — Curve / Aerodrome
        # classic — where one fungible balance per pool means there is no
        # co-leg to disambiguate). String form so it is JSON-stable across the
        # V3 integer-tokenId and any future address/handle discriminators.
        position_id: str | None = None,
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
        self.il_usd = il_usd
        self.hodl_value_usd = hodl_value_usd
        self.position_hash = position_hash
        self.position_id = position_id

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                return str(v)
            return v

        return json.dumps(
            {
                "event_type": self.event_type,
                # VIB-4426 — protocol MUST be on the payload (not only on
                # ``identity``) so the augment chokepoint's
                # ``primitive_for(event_type, protocol)`` override can refine
                # ``Primitive.LP`` to ``Primitive.LP_V4`` for Uniswap V4 rows.
                # Without this key the V4 per-primitive version stream is
                # silently dead code (CodeRabbit Major on PR #2335).
                "protocol": self.identity.protocol,
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
                # VIB-4319 — impermanent-loss diagnostic (V_lp − V_hodl) and
                # the V_hodl reference value used to compute it, both at
                # close-time oracle prices. Populated on LP_CLOSE /
                # LP_COLLECT_FEES when the prior OPEN payload and a
                # close-time price for every non-zero entry leg are
                # available; ``None`` otherwise (Empty ≠ Zero — the
                # Accountant Test LP4 cell PASSes on any non-null value
                # and a fabricated zero would lie about an unmeasured
                # quantity).
                "il_usd": _enc(self.il_usd),
                "hodl_value_usd": _enc(self.hodl_value_usd),
                # VIB-4473 — V4 lot-matching anchor. Always emitted (None for
                # V3, populated for V4) so downstream JSON consumers see a
                # stable key shape across protocols.
                "position_hash": self.position_hash,
                # VIB-4275 — per-position discriminator. On LP_OPEN this is the
                # minted NFT token id; the close-side resolver
                # (``AccountingProcessor._lookup_prior_lp_open``) filters the
                # same-``position_key`` candidate opens by this field so a
                # co-pool close attributes to its OWN open. Always emitted
                # (None for fungible-LP venues) for a stable key shape.
                "position_id": self.position_id,
                "schema_version": self.schema_version,
                "primitive_version": self.primitive_version,
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


def _v4_align_tokens_to_currency_order(
    lp_data: Any,
    chain: str,
    token0: str,
    token1: str,
    dec0: int,
    dec1: int,
    assumed_decimals: bool,
) -> tuple[str, str, int, int, bool]:
    """VIB-4426 P1 #4 — re-pair (token, decimals) by canonical PoolKey address order.

    The V4 receipt parser emits ``amount0`` / ``amount1`` in PoolKey-sorted
    order (``int(currency0, 16) < int(currency1, 16)``). The user's intent
    may carry the pool string in the OPPOSITE order
    (``"USDC/WETH/3000"`` when canonical is WETH<USDC). Pre-fix
    ``amount0`` (the WETH amount in raw units) got scaled with the user's
    ``token0_decimals`` (USDC's 6 decimals) and labelled as USDC — silent
    misattribution and wrong cost basis.

    This helper resolves the canonical currency addresses (when populated
    by the V4 receipt parser) into symbols + decimals via the token
    resolver and returns ``(token0, token1, dec0, dec1, assumed_decimals)``
    aligned to PoolKey order. If the resolver fails or the currency
    addresses aren't present (V3 callers), returns the inputs unchanged.

    Args:
        lp_data: ``LPOpenData`` or ``LPCloseData`` carrying optional
            ``currency0`` / ``currency1`` addresses.
        chain: Chain name forwarded to the token resolver.
        token0, token1, dec0, dec1, assumed_decimals: Current values
            derived from the intent, returned unchanged on the V3 / no-data
            path.
    """
    c0 = getattr(lp_data, "currency0", None)
    c1 = getattr(lp_data, "currency1", None)
    if not c0 or not c1:
        # V3 or single-sided V4 open — no canonical address pair available.
        return token0, token1, dec0, dec1, assumed_decimals
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        ti0 = resolver.resolve(c0, chain=chain, log_errors=False)
        ti1 = resolver.resolve(c1, chain=chain, log_errors=False)
    except Exception:  # noqa: BLE001
        logger.warning(
            "V4 LP accounting: token resolver failed for currency pair (%s, %s) on %s; "
            "falling back to user-intent token order — amounts may be misattributed",
            c0,
            c1,
            chain,
        )
        return token0, token1, dec0, dec1, assumed_decimals

    if ti0 is None or ti1 is None:
        logger.warning(
            "V4 LP accounting: token resolver returned None for (%s, %s) on %s; "
            "falling back to user-intent token order",
            c0,
            c1,
            chain,
        )
        return token0, token1, dec0, dec1, assumed_decimals

    aligned_token0 = (ti0.symbol or c0).upper()
    aligned_token1 = (ti1.symbol or c1).upper()
    aligned_dec0 = int(ti0.decimals) if ti0.decimals is not None else dec0
    aligned_dec1 = int(ti1.decimals) if ti1.decimals is not None else dec1
    aligned_assumed = ti0.decimals is None or ti1.decimals is None
    return aligned_token0, aligned_token1, aligned_dec0, aligned_dec1, aligned_assumed


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


# crap-allowlist: VIB-4426 — build_lp_accounting_event is the canonical LP event
# constructor. The high cc reflects the breadth of the LP payload (V3 + V4
# branches, LP_OPEN vs LP_CLOSE, optional fees / IL / HODL value / position_hash
# fields, fallback paths for older receipt parsers). Decomposing would shred
# legibility for marginal cc gain — the function is already grouped by intent
# direction (LP_OPEN vs LP_CLOSE) and field family. Coverage stays > 85%.
# Refactor will be considered as part of a future accounting-writer rework
# epic, NOT inside the VIB-4426 PR-1 scope.
def build_lp_accounting_event(  # noqa: C901
    *,
    intent: Any,
    result: Any,
    deployment_id: str,
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
    # VIB-4473 — V4 lot-matching anchor read from ``lp_open_data`` on
    # LP_OPEN. V3 parsers leave it None and the field is forwarded as-is
    # so the payload key is stable. LP_CLOSE leaves it None: the close
    # leg matches against the prior OPEN payload by position_key, not by
    # re-reading the hash off the burn receipt.
    position_hash: str | None = None

    if intent_type_str == "LP_OPEN":
        lp_data = getattr(result, "lp_open_data", None)
        if lp_data is not None:
            # VIB-4426 P1 #4 — for V4 (currency0/currency1 populated),
            # re-resolve (token0, token1, dec0, dec1) by canonical PoolKey
            # address order so amount0 (in PoolKey order) is paired with
            # the correct symbol/decimals. Otherwise a user pool string in
            # the opposite order (e.g. "USDC/WETH" when canonical is WETH<USDC)
            # silently mis-scales and mis-prices.
            token0, token1, dec0, dec1, assumed_decimals = _v4_align_tokens_to_currency_order(
                lp_data, chain, token0, token1, dec0, dec1, assumed_decimals
            )
            amount0 = _to_human(getattr(lp_data, "amount0", None), dec0)
            amount1 = _to_human(getattr(lp_data, "amount1", None), dec1)
            position_hash = getattr(lp_data, "position_hash", None)
        else:
            # Fall back to extracted_data dict (older receipt parsers)
            extracted = getattr(result, "extracted_data", None) or {}
            amount0 = _to_human(extracted.get("amount0"), dec0)
            amount1 = _to_human(extracted.get("amount1"), dec1)
    else:
        lp_data = getattr(result, "lp_close_data", None)
        if lp_data is not None:
            # VIB-4426 P1 #4 — same alignment as LP_OPEN (see above).
            token0, token1, dec0, dec1, assumed_decimals = _v4_align_tokens_to_currency_order(
                lp_data, chain, token0, token1, dec0, dec1, assumed_decimals
            )
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
        position_hash=position_hash,
    )
