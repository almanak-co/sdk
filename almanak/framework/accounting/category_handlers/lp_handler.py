"""LP category handler for AccountingProcessor (VIB-3470).

Ports logic from lp_accounting.py to work from ledger_row / outbox_row dicts
rather than live intent / result objects.  No live chain calls.

Pendle LP is handled by pendle_handler.py; this handler skips any intent whose
protocol contains "pendle".
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.category_handlers._price_helpers import (
    load_raw_price_inputs,
    parse_price_inputs,
)
from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.lp_accounting import LPAccountingEvent, compute_lp_cost_basis
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, LPEventType

logger = logging.getLogger(__name__)

_LP_OPEN_CLOSE = frozenset({"LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"})
_LP_CLOSE_LIKE = frozenset({"LP_CLOSE", "LP_COLLECT_FEES"})

_INTENT_TO_EVENT_TYPE: dict[str, LPEventType] = {
    "LP_OPEN": LPEventType.LP_OPEN,
    "LP_CLOSE": LPEventType.LP_CLOSE,
    "LP_COLLECT_FEES": LPEventType.LP_COLLECT_FEES,
}


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else None
    except Exception:  # noqa: BLE001
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _pool_address_from_position_key(position_key: str) -> str:
    """Extract the pool address (last ':' segment) from a position key.

    e.g. "lp:aerodrome:base:0xwallet:0xpooladdr" → "0xpooladdr"
    """
    if not position_key:
        return ""
    return position_key.rsplit(":", 1)[-1]


def _tokens_from_position_key(position_key: str) -> tuple[str, str]:
    """Extract (token0, token1) symbols from a Uniswap-V3-style position key.

    The Uniswap V3 / V4 / PancakeSwap-V3 position-key tail is
    ``"<token0>/<token1>/<fee_tier>"`` (e.g. ``weth/usdc/500``). LP_CLOSE
    ledger rows do not populate ``token_in`` / ``token_out`` because a close
    returns BOTH tokens — there is no swap-style in/out direction. Without
    token symbols the handler cannot resolve decimals and the entire
    LP_CLOSE payload (amounts, fees, cost basis, realized PnL) collapses
    to NULL with an "assumed decimals" downgrade.

    Returns ("", "") for non-V3-style keys (aerodrome, pancakeswap-v2,
    sushiswap-v2 — last segment is an address, not a slash-separated
    descriptor) so the handler's existing token_in/token_out path remains
    authoritative for those venues.
    """
    if not position_key:
        return "", ""
    tail = position_key.rsplit(":", 1)[-1]
    parts = tail.split("/")
    if len(parts) < 2:
        return "", ""
    return parts[0].upper(), parts[1].upper()


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

    if lp_close_data is not None and intent_type_str in _LP_CLOSE_LIKE:
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


def _parse_lp_timestamp(raw_ts: Any) -> datetime:
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        return datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        return datetime.now(UTC)


def _resolve_lp_pool_address(outbox_row: dict[str, Any], position_key: str) -> str | None:
    """Resolve the pool address from the position_key tail or fall back to market_id."""
    pool_address = _pool_address_from_position_key(position_key) or outbox_row.get("market_id") or ""
    if not pool_address:
        logger.warning(
            "LP handler: cannot resolve pool address from position_key=%r or market_id=%r; dropping event",
            position_key,
            outbox_row.get("market_id"),
        )
        return None
    return pool_address


def _resolve_lp_tokens(ledger_row: dict[str, Any], position_key: str) -> tuple[str, str]:
    """Resolve (token0, token1) symbols, falling back to position-key descriptor for V3-style closes."""
    # LP_OPEN ledger rows carry token_in/token_out from the swap-style intent
    # compilation. LP_CLOSE rows leave both empty because a close returns BOTH
    # tokens — there is no swap-style direction. When empty, fall back to the
    # Uniswap-V3-style position-key descriptor ``<token0>/<token1>/<fee_tier>``
    # so the decimal lookup can still happen and the LP_CLOSE payload doesn't
    # collapse to NULLs with an "assumed decimals" downgrade.
    token0 = (ledger_row.get("token_in") or "").upper()
    token1 = (ledger_row.get("token_out") or "").upper()
    if not token0 or not token1:
        pk_t0, pk_t1 = _tokens_from_position_key(position_key)
        if pk_t0 and pk_t1:
            token0 = token0 or pk_t0
            token1 = token1 or pk_t1
    return token0, token1


def _diagnose_pricing_failure(
    intent_type_str: str,
    amount0: Decimal | None,
    amount1: Decimal | None,
    token0: str,
    token1: str,
    raw_price_inputs: dict[str, Any],
    price_oracle: dict[str, Decimal],
) -> str:
    """Produce a human-readable reason explaining why cost_basis_usd is None."""
    if not raw_price_inputs:
        return f"{intent_type_str} cost_basis_usd unavailable: no price_inputs_json on ledger row"

    missing: list[str] = []
    invalid: list[str] = []
    # ``token0`` / ``token1`` are typed as ``str`` upstream but a malformed
    # ledger row could carry ``None``. ``(t or "")`` keeps the diagnostic
    # alive without raising AttributeError.
    token_pairs = (
        (amount0, (token0 or "").upper()),
        (amount1, (token1 or "").upper()),
    )
    # Look in the *raw* on-disk mapping so we can distinguish "symbol absent"
    # (missing) from "symbol present but value non-numeric / nested-without-
    # price_usd" (invalid). The parsed ``price_oracle`` already filtered both
    # out, so it cannot tell us which case fired.
    raw_keys = {k.upper() for k in raw_price_inputs if isinstance(k, str)}
    for amt, sym in token_pairs:
        if amt is None:
            continue
        if sym not in raw_keys:
            missing.append(sym or "?")
            continue
        if sym not in price_oracle:
            invalid.append(sym or "?")

    if missing:
        return (
            f"{intent_type_str} cost_basis_usd unavailable: missing prices in price_inputs_json: {', '.join(missing)}"
        )
    if invalid:
        # ``price_inputs_json`` carried a key for the token but its value was
        # non-numeric / NaN / Infinity / a nested object missing ``price_usd``.
        # Surface this as distinct from "missing" so operators can tell whether
        # the producer side dropped a price entirely or wrote a bad one.
        return (
            f"{intent_type_str} cost_basis_usd unavailable: invalid prices in price_inputs_json: {', '.join(invalid)}"
        )
    # Defensive: covers the "both legs are None amounts" case where
    # compute_lp_cost_basis returns None but neither the missing nor invalid
    # bucket fired. Without this, operators see cost_basis_usd=None with no
    # explanation.
    return f"{intent_type_str} cost_basis_usd unavailable: no resolvable amount legs"


def _compute_lp_pricing(
    amount0: Decimal | None,
    amount1: Decimal | None,
    token0: str,
    token1: str,
    ledger_row: dict[str, Any],
    intent_type_str: str,
    assumed_decimals: bool,
) -> tuple[Decimal | None, str, dict[str, Decimal]]:
    """USD pricing of an LP event (VIB-3756 + VIB-3885).

    Returns ``(cost_basis_usd, pricing_unavailable_reason, price_oracle)``. The
    parsed price oracle is returned alongside so the caller can re-use it for
    fees pricing without re-parsing ``price_inputs_json``.

    The handler used to hard-code ``cost_basis_usd=None`` which downstream
    dashboards (QA harness deployed_usd column, position-PnL reporter)
    render as "$0.00". That made an LP_OPEN that *did* mint an NFT and fire
    accounting events look like a $0 deposit.

    ``price_inputs_json`` is captured at execution time (VIB-3480 audit-grade
    replay). Per AttemptNo17 §1.2 G12 the canonical shape is
    ``{symbol: {price_usd, oracle_source, fetched_at, confidence}}``;
    legacy / fixture rows still carry the flat ``{symbol: price}`` shape.
    ``parse_price_inputs`` is the tolerant reader (VIB-3885) — both shapes
    come back as a flat ``{SYMBOL: Decimal}`` dict so ``compute_lp_cost_basis``
    keeps the same fail-closed contract as ``swap_handler.py``: any non-None
    amount whose price is missing returns None for the whole sum (NOT 0).
    Decimals-assumed events also bypass pricing because amounts can be off
    by 1e12 for 6-decimal tokens — pricing them would print confidently
    wrong USD numbers.
    """
    raw_price_inputs = load_raw_price_inputs(ledger_row.get("price_inputs_json"))
    price_oracle = parse_price_inputs(ledger_row.get("price_inputs_json"))
    if assumed_decimals:
        return None, "", price_oracle

    cost_basis_usd = compute_lp_cost_basis(amount0, amount1, token0, token1, price_oracle)
    if cost_basis_usd is not None:
        return cost_basis_usd, "", price_oracle

    # Distinguish "no price oracle attached" from "price-oracle present but one
    # of token0/token1 was missing a quote". Operators triaging a $None
    # deployed_usd column need this disambiguation.
    reason = _diagnose_pricing_failure(
        intent_type_str, amount0, amount1, token0, token1, raw_price_inputs, price_oracle
    )
    return None, reason, price_oracle


def _determine_lp_confidence(
    assumed_decimals: bool,
    cost_basis_usd: Decimal | None,
    pricing_unavailable_reason: str,
) -> tuple[AccountingConfidence, str]:
    if assumed_decimals:
        return AccountingConfidence.ESTIMATED, "token decimals assumed; LP amounts are estimated"
    if cost_basis_usd is None and pricing_unavailable_reason:
        # VIB-3886: pricing is missing, so the USD field is incomplete —
        # confidence MUST degrade to ESTIMATED. Pre-VIB-3886 the LP handler
        # stamped HIGH+unavailable_reason simultaneously, which the
        # downstream Accountant Test treated as "USD field is fine" while
        # the operator-facing dashboard rendered the missing dollars. The
        # SWAP handler always degraded in this scenario; the LP path now
        # matches.
        return AccountingConfidence.ESTIMATED, pricing_unavailable_reason
    return AccountingConfidence.HIGH, ""


def _resolve_lp_position_metadata(
    intent_type_str: str,
    extracted: Any,
    prior_open_payload: dict[str, Any] | None,
) -> tuple[int | None, int | None, int | None, int | None, bool | None]:
    """Return (tick_lower, tick_upper, liquidity, current_tick, in_range) for an LP event.

    VIB-3893: receipt-parser stamps tick_lower/tick_upper/liquidity/current_tick on
    the ``lp_open_data`` typed object inside ``extracted_data_json``. The
    runner's slot0 fallback fills current_tick when the receipt didn't
    carry a Swap event. Thread the bracket through to the accounting
    payload so the dashboard's Trade Tape can render in-range without a
    second on-chain call. ``in_range`` is derived here using the
    half-open Uniswap convention ``tick_lower <= current_tick <
    tick_upper`` — same definition as ``position_events.in_range`` so
    the two surfaces never disagree (VIB-3887 contract).
    """
    tick_lower_v: int | None = None
    tick_upper_v: int | None = None
    liquidity_v: int | None = None
    current_tick_v: int | None = None
    in_range_v: bool | None = None

    if intent_type_str == "LP_OPEN":
        lp_open = extracted.get("lp_open_data") if isinstance(extracted, dict) else None
        if lp_open is not None:
            tick_lower_v = _as_int(getattr(lp_open, "tick_lower", None))
            tick_upper_v = _as_int(getattr(lp_open, "tick_upper", None))
            liquidity_v = _as_int(getattr(lp_open, "liquidity", None))
            current_tick_v = _as_int(getattr(lp_open, "current_tick", None))
            if tick_lower_v is not None and tick_upper_v is not None and current_tick_v is not None:
                in_range_v = tick_lower_v <= current_tick_v < tick_upper_v
        return tick_lower_v, tick_upper_v, liquidity_v, current_tick_v, in_range_v

    # LP_CLOSE / LP_COLLECT_FEES branch.
    # The close receipt carries the burned-liquidity total (Burn events)
    # but no tick range — that lives on the prior OPEN. Stamp the
    # liquidity removed on the CLOSE event so a Quant reading the trade
    # tape can verify the principal was fully unwound (liquidity ==
    # opening liquidity ⇒ full close).
    lp_close = extracted.get("lp_close_data") if isinstance(extracted, dict) else None
    if lp_close is not None:
        liquidity_v = _as_int(getattr(lp_close, "liquidity_removed", None))
        # VIB-3940 — current_tick at close-block. Sourced from a Swap
        # event in the close receipt when present, with a slot0() RPC
        # fallback in the runner. Without this the LP_CLOSE accounting
        # event inherited current_tick=None / in_range=None and
        # violated lane symmetry vs. LP_OPEN.
        current_tick_v = _as_int(getattr(lp_close, "current_tick", None))
    # Backfill tick range from the prior OPEN — a CLOSE receipt does
    # not re-emit the position bracket, but the bracket is immutable
    # over the position's lifetime. Without this the trade tape
    # cannot answer "was the position in-range at close?".
    if prior_open_payload:
        if tick_lower_v is None:
            tick_lower_v = _as_int(prior_open_payload.get("tick_lower"))
        if tick_upper_v is None:
            tick_upper_v = _as_int(prior_open_payload.get("tick_upper"))
    # VIB-3940 — derive in_range using the same half-open Uniswap
    # convention as the LP_OPEN branch so the two surfaces never
    # disagree. Requires both the backfilled bracket AND a non-null
    # current_tick (from the close receipt or slot0 fallback).
    if tick_lower_v is not None and tick_upper_v is not None and current_tick_v is not None:
        in_range_v = tick_lower_v <= current_tick_v < tick_upper_v
    return tick_lower_v, tick_upper_v, liquidity_v, current_tick_v, in_range_v


def _compute_lp_realized_pnl_and_fees(
    intent_type_str: str,
    fees0: Decimal | None,
    fees1: Decimal | None,
    token0: str,
    token1: str,
    price_oracle: dict[str, Decimal],
    prior_open_payload: dict[str, Any] | None,
    cost_basis_usd: Decimal | None,
) -> tuple[Decimal | None, Decimal | None]:
    """Realized PnL + fees_total_usd on LP_CLOSE / LP_COLLECT_FEES (G6 contract VIB-3933).

    G6 reconciliation contract (VIB-3933, Codex audit on PR #2014):
        realized_pnl_usd = received_value_usd − cost_basis_at_open_usd
        fees_total_usd   = USD-priced fees0_collected + fees1_collected
    i.e. realized PnL is **net of fees** on the LP_CLOSE event, and fees
    are persisted separately on ``fees_total_usd``. The dashboard cost
    stack adds ``realized_pnl_usd`` and ``fees_total_usd`` independently
    (lines lp_handler dashboard ``compute_cost_stack`` LP_CLOSE branch);
    if realized PnL were gross-of-fees here, G6's ``sum_lp + sum_fees``
    would double-count fee income. ``cost_basis_usd`` on this event is
    the freshly-computed "USD value of amount0/1 returned at close-time
    prices" — the close handler re-uses that variable name. We do not
    fabricate a PnL number when any input is missing — None means "the
    dashboard should render '—', not '$0.00'".
    """
    if intent_type_str not in _LP_CLOSE_LIKE:
        return None, None

    # Compute fees in USD even when there is no prior OPEN — the fee
    # bucket is a function of fees0/1 + close-time prices, not of the
    # open-basis context. realized_pnl_usd still requires the prior
    # OPEN below.
    fees_total_usd = compute_lp_cost_basis(fees0, fees1, token0, token1, price_oracle)
    realized_pnl_usd: Decimal | None = None
    if prior_open_payload and cost_basis_usd is not None:
        open_basis = _safe_decimal(prior_open_payload.get("cost_basis_usd"))
        if open_basis is not None:
            realized_pnl_usd = cost_basis_usd - open_basis
    return realized_pnl_usd, fees_total_usd


def handle_lp(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
    prior_open_payload: dict[str, Any] | None = None,
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

    position_key = outbox_row.get("position_key") or ""
    pool_address = _resolve_lp_pool_address(outbox_row, position_key)
    if pool_address is None:
        return None

    timestamp = _parse_lp_timestamp(ledger_row.get("timestamp"))
    token0, token1 = _resolve_lp_tokens(ledger_row, position_key)
    chain = ledger_row.get("chain") or ""

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

    cost_basis_usd, pricing_unavailable_reason, price_oracle = _compute_lp_pricing(
        amount0=amount0,
        amount1=amount1,
        token0=token0,
        token1=token1,
        ledger_row=ledger_row,
        intent_type_str=intent_type_str,
        assumed_decimals=assumed_decimals,
    )

    confidence, unavailable_reason = _determine_lp_confidence(
        assumed_decimals, cost_basis_usd, pricing_unavailable_reason
    )

    # ── Identity ─────────────────────────────────────────────────────────────
    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    strategy_id = ledger_row.get("strategy_id") or outbox_row.get("strategy_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""

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

    tick_lower_v, tick_upper_v, liquidity_v, current_tick_v, in_range_v = _resolve_lp_position_metadata(
        intent_type_str, extracted, prior_open_payload
    )

    realized_pnl_usd, fees_total_usd = _compute_lp_realized_pnl_and_fees(
        intent_type_str=intent_type_str,
        fees0=fees0,
        fees1=fees1,
        token0=token0,
        token1=token1,
        price_oracle=price_oracle,
        prior_open_payload=prior_open_payload,
        cost_basis_usd=cost_basis_usd,
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
        realized_pnl_usd=realized_pnl_usd,
        fees0_collected=fees0,
        fees1_collected=fees1,
        fees_total_usd=fees_total_usd,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
        tick_lower=tick_lower_v,
        tick_upper=tick_upper_v,
        liquidity=liquidity_v,
        current_tick=current_tick_v,
        in_range=in_range_v,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Registry adapter (VIB-4163, T3)
# ──────────────────────────────────────────────────────────────────────────────

from almanak.framework.accounting.category_handlers import HandlerContext, register
from almanak.framework.primitives.types import AccountingCategory


@register(AccountingCategory.LP)
def _dispatch_lp(ctx: HandlerContext) -> LPAccountingEvent | None:
    """Adapter that resolves prior_open_payload before calling ``handle_lp``.

    The legacy ladder did this resolution inside ``AccountingProcessor._dispatch``;
    pulling it here keeps the LP-specific pre-work co-located with the handler
    and lets the dispatcher stay generic over every registered category.
    """
    intent_type = (ctx.ledger_row.get("intent_type") or "").upper()
    prior_open: dict[str, Any] | None = None
    if intent_type in {"LP_CLOSE", "LP_COLLECT_FEES"}:
        prior_open = ctx.prior_open_lookup(ctx.outbox_row.get("position_key") or "")
    return handle_lp(ctx.outbox_row, ctx.ledger_row, prior_open_payload=prior_open)
