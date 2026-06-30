"""LP category handler for AccountingProcessor (VIB-3470).

Ports logic from lp_accounting.py to work from ledger_row / outbox_row dicts
rather than live intent / result objects.  No live chain calls.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.accounting.category_handlers._price_helpers import (
    load_raw_price_inputs,
    parse_price_inputs,
)
from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.lp_accounting import LPAccountingEvent, compute_lp_cost_basis
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, LPEventType

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

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

    # ── Declared money legs win for LP_OPEN (VIB-3587) ───────────────────────
    # When a connector DECLARES its LP_OPEN money legs (the US-009 contract,
    # e.g. Curve single-sided / multi-coin deposits), the ledger row's
    # ``amount_in`` / ``amount_out`` are already the human amounts ALIGNED to the
    # declared ``token_in`` / ``token_out`` (= token0 / token1 here). The legacy
    # ``lp_open_data.amount0`` / ``amount1`` are positional over the pool's FIRST
    # TWO coins, so for a single-sided / non-leading deposit the token and the
    # raw amount come from DIFFERENT coins — pairing them mis-attributes the
    # amount (Curve: token0=USDC but amount0=DAI's raw 0). Preferring the aligned
    # ledger strings keeps (token0, amount0) a single coin's fact. An unfunded
    # coin's slot stays ``""`` → ``None`` (Empty ≠ Zero — absent, not a measured
    # zero). Mirrors the ledger dispatcher's prefer-declared-legs inversion.
    if intent_type_str == "LP_OPEN" and extracted.get("primitive_money_legs") is not None:
        amount0 = _safe_decimal(amount_in_str) if amount_in_str else None
        amount1 = _safe_decimal(amount_out_str) if amount_out_str else None
        return amount0, amount1, None, None, False

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


def _clean_pool_address_candidate(value: Any) -> str:
    """Accept 20-byte EVM addresses and 32-byte V4 PoolId hashes; reject V3 fee tiers.

    Heuristic:

    - Empty / whitespace → ``""``.
    - No ``/`` → require a structured 0x-prefixed lowercase-hex value of
      one of two specific lengths (VIB-4471):

      * ``^0x[0-9a-f]{40}$`` — 20-byte EVM address (V3 / Aerodrome /
        classic AMM pool contract).
      * ``^0x[0-9a-f]{64}$`` — 32-byte V4 ``pool_id`` hash
        (VIB-4426; ``keccak256(abi.encode(PoolKey))`` carried in
        ``topics[1]`` of V4 ``Swap`` / ``ModifyLiquidity`` events).

      Anything else (arbitrary identifiers, mixed-case hex, wrong
      length, non-hex characters) returns ``""``. This is the
      defense-in-depth point caught by Codex P2 on the VIB-4426 design
      review: the pre-VIB-4471 implementation returned the input
      unchanged for any non-slash string, which let arbitrary
      identifiers slip through into ``payload.pool_address`` and
      surface as downstream null-key bugs.
    - Has ``/`` AND the last segment is purely numeric → V3 fee-tier
      descriptor (``weth/usdc/500``). Reject (VIB-4274 / VIB-4396).
    - Otherwise the slash-bearing value is a canonical Solidly-style
      descriptor (``TOKEN0/TOKEN1/stable|volatile``) — the only stable
      position identifier classic Aerodrome surfaces. Accept (Codex P1
      on PR #2289).
    """
    text = str(value or "").strip()
    if not text:
        return ""
    if "/" not in text:
        # VIB-4471: accept only 20-byte EVM addresses (V3 / Aerodrome /
        # classic AMM) or 32-byte V4 pool_id hashes (VIB-4426). The shape
        # is self-describing — no protocol context argument required.
        if not text.startswith("0x"):
            return ""
        body = text[2:]
        if len(body) not in (40, 64):
            return ""
        if not all(c in "0123456789abcdef" for c in body):
            return ""
        return text
    last_segment = text.rsplit("/", 1)[-1].strip()
    if not last_segment:
        return ""
    if last_segment.isdigit():
        return ""
    return text


def _typed_pool_address(typed: Any) -> str:
    """Return ``typed.pool_address`` whether ``typed`` is an object or dict.

    ``deserialize_extracted_data`` returns the dataclass when
    reconstruction succeeds and falls back to a dict (with ``_type``
    re-added) on failure (``ledger.py:_reconstruct_dataclass``). Both
    paths must be honoured here or we silently drop the chain-extracted
    ``pool_address`` (gemini HIGH on PR #2289).
    """
    if typed is None:
        return ""
    if isinstance(typed, dict):
        return str(typed.get("pool_address") or "")
    return str(getattr(typed, "pool_address", "") or "")


def _resolve_lp_pool_address(
    *,
    outbox_row: dict[str, Any],
    position_key: str,
    extracted: dict[str, Any] | None = None,
    prior_open_payload: dict[str, Any] | None = None,
) -> str | None:
    """Resolve the on-chain pool address for an LP accounting event.

    Priority order (chain-data first, descriptor last):

    1. **Receipt extraction (current event).** The receipt parser stamps
       ``lp_open_data.pool_address`` (VIB-3893) and
       ``lp_close_data.pool_address`` (VIB-3940) on the on-chain Mint/Burn
       emitter, which IS the canonical V3 pool address. Sourced from chain
       data — most reliable.
    2. **Prior LP_OPEN payload (LP_CLOSE / LP_COLLECT_FEES).** When the
       close-side receipt didn't re-emit the pool address, reuse the
       OPEN-side ``payload.pool_address``. If that historical row was
       written under the pre-VIB-4396 regime (descriptor leaked into
       ``pool_address``), fall through to the position_reference's
       ``semantic_grouping_key`` (= ``"chain:0xpool"``) for the canonical
       address.
    3. **Position-key tail.** V2-family / classic-AMM keys end in either
       a bare pool address (``lp:aerodrome:base:<wallet>:0xpool``) or in a
       canonical Solidly-style descriptor (``TOKEN0/TOKEN1/stable`` or
       ``TOKEN0/TOKEN1/volatile``) — the latter is the only stable
       position identifier available for classic Aerodrome (no NPM, no
       on-chain pool address surfaced at the receipt layer). V3-style
       keys end in a numeric fee-tier descriptor (e.g. ``weth/usdc/500``)
       — VIB-4274 rejects those tails outright.
       ``_clean_pool_address_candidate`` permits the Solidly descriptor
       shape and rejects V3 fee-tier descriptors.
    4. **outbox_row.market_id.** Same ``_clean_pool_address_candidate``
       rules: bare address or Solidly descriptor accepted; V3 fee-tier
       descriptor rejected (the live VIB-4396 trigger).

    Returns ``None`` only when every source is empty / descriptor-shaped —
    the caller drops the event with a warning.
    """
    # 1) Receipt-extracted pool_address — chain data, most reliable.
    extracted_map = extracted or {}
    for key in ("lp_open_data", "lp_close_data"):
        candidate = _clean_pool_address_candidate(_typed_pool_address(extracted_map.get(key)))
        if candidate:
            return candidate

    # 2) Prior LP_OPEN payload fallback (LP_CLOSE / LP_COLLECT_FEES).
    if prior_open_payload:
        candidate = _clean_pool_address_candidate(prior_open_payload.get("pool_address"))
        if candidate:
            return candidate
        # Legacy OPEN payloads stamped the descriptor — recover the
        # canonical address from semantic_grouping_key (``"chain:0xpool"``
        # today, but ``rsplit(":", 1)[-1]`` is robust to additional
        # ``:``-delimited prefixes the writer may add later — gemini medium
        # on PR #2289).
        pos_ref = prior_open_payload.get("position_reference") or {}
        sgk = str(pos_ref.get("semantic_grouping_key") or "").strip()
        if sgk and ":" in sgk:
            candidate = _clean_pool_address_candidate(sgk.rsplit(":", 1)[-1])
            if candidate:
                return candidate

    # 3) Position-key tail — bare address (Aerodrome v2 / V2 family) or
    # Solidly-style descriptor (classic Aerodrome). V3 numeric fee tiers
    # are rejected by ``_clean_pool_address_candidate``.
    candidate = _clean_pool_address_candidate(_pool_address_from_position_key(position_key))
    if candidate:
        return candidate

    # 4) outbox_row.market_id, subject to the same
    # _clean_pool_address_candidate rules.
    candidate = _clean_pool_address_candidate(outbox_row.get("market_id"))
    if candidate:
        return candidate

    logger.warning(
        "LP handler: cannot resolve pool address from receipt, prior payload, "
        "position_key=%r, or market_id=%r; dropping event",
        position_key,
        outbox_row.get("market_id"),
    )
    return None


def _v4_realign_token_pair(
    lp_data: Any,
    chain: str,
    token0: str,
    token1: str,
) -> tuple[str, str]:
    """VIB-4636 (sibling) — return ``(token0, token1)`` re-paired by canonical V4 PoolKey order.

    The V4 receipt parser emits ``amount0`` / ``amount1`` in PoolKey-sorted
    order (``int(currency0, 16) < int(currency1, 16)``). The user's intent
    may carry the pair in the OPPOSITE order (``"USDC/WETH"`` when canonical
    is ``WETH<USDC``); without alignment, the WETH-shaped raw amount0 gets
    scaled with USDC's 6 decimals and labelled USDC — silent misattribution
    that corrupts cost basis. Mirrors
    ``almanak.framework.accounting.lp_accounting._v4_align_tokens_to_currency_order``
    (which is part of the unused ``build_lp_accounting_event`` ladder); kept
    inline here so the production handler is the single source of truth.

    Returns the inputs unchanged when ``currency0`` / ``currency1`` are
    absent (V3 callers, single-sided V4 opens that did not surface both
    legs, or token-resolver failures — fail-open so a missing resolver
    cannot block the write).
    """
    # ``deserialize_extracted_data`` falls back to a plain dict when typed
    # reconstruction fails — read both shapes so the realign is not silently
    # disabled (which would re-introduce the misattribution this fixes).
    if isinstance(lp_data, dict):
        c0 = lp_data.get("currency0")
        c1 = lp_data.get("currency1")
    else:
        c0 = getattr(lp_data, "currency0", None)
        c1 = getattr(lp_data, "currency1", None)
    if not c0 or not c1:
        return token0, token1
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        ti0 = resolver.resolve(c0, chain=chain, log_errors=False)
        ti1 = resolver.resolve(c1, chain=chain, log_errors=False)
    except Exception:  # noqa: BLE001 — fail-open per docstring contract
        logger.warning(
            "V4 LP accounting: token resolver failed for currency pair (%s, %s) on %s; "
            "falling back to user-intent token order — amounts may be misattributed",
            c0,
            c1,
            chain,
        )
        return token0, token1
    if ti0 is None or ti1 is None:
        logger.warning(
            "V4 LP accounting: token resolver returned None for currency pair (%s, %s) on %s; "
            "falling back to user-intent token order",
            c0,
            c1,
            chain,
        )
        return token0, token1
    return (ti0.symbol or c0).upper(), (ti1.symbol or c1).upper()


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
    unmeasured: list[str] = []
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
            # VIB-5131 — an UNMEASURED leg (None amount) is now the dominant
            # reason ``compute_lp_cost_basis`` returns None (it fails closed on
            # the first None leg). Report it explicitly rather than letting it
            # fall through to the misleading "no resolvable amount legs"
            # catch-all (which reads as "nothing was deposited" when in fact one
            # leg IS resolvable and only the other is unmeasured — e.g. a native
            # V4 leg whose stamp read failed).
            unmeasured.append(sym or "?")
            continue
        # VIB-5124 — consistent with ``compute_lp_cost_basis`` (a stronger,
        # unconditional skip here): a measured-zero leg contributes $0 and never
        # needs a price, so it can never be the leg that voided the basis and
        # must not be reported as a missing/invalid price (Empty≠Zero). The
        # consumer skips a zero leg only when its price is *also* missing; this
        # diagnostic only runs when the basis is already None, so skipping every
        # zero leg here can never wrongly clear a real failure — it just avoids
        # falsely blaming e.g. "missing SUSDAI" for the unfunded leg of a
        # single-sided LP_OPEN.
        if amt == 0:
            continue
        if sym not in raw_keys:
            missing.append(sym or "?")
            continue
        if sym not in price_oracle:
            invalid.append(sym or "?")

    if unmeasured:
        # VIB-5131 — an unmeasured (None) leg fails the basis closed regardless of
        # prices, so report it first. "Empty≠Zero" — the leg amount was never
        # measured (e.g. a native-ETH leg the parser left None and the runner
        # stamp could not fill), NOT a measured zero.
        return (
            f"{intent_type_str} cost_basis_usd unavailable: unmeasured amount leg(s) "
            f"(Empty≠Zero — amount was None): {', '.join(unmeasured)}"
        )
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
    # Defensive: covers the residual case where compute_lp_cost_basis returns
    # None but neither the unmeasured, missing, nor invalid bucket fired. Without
    # this, operators see cost_basis_usd=None with no explanation.
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


def _resolve_lp_open_discriminator(intent_type_str: str, extracted: Any) -> str | None:
    """Return the per-position discriminator to stamp on an LP_OPEN payload (VIB-4275).

    The minted NFT token id (``LPOpenData.position_id``) is the per-leg
    discriminator that lets a later LP_CLOSE / LP_COLLECT_FEES on the SAME
    pool-level ``position_key`` resolve THIS leg's open — instead of the
    most-recent open by timestamp (the co-pool attribution bug). Only LP_OPEN
    carries this: the token id is minted by the open, and the close-side
    resolver reads it back off the open payload.

    Returns the discriminator as a string (JSON-stable across the V3 integer
    tokenId and any future address/handle form), or ``None`` when the connector
    has no per-position id — e.g. fungible-LP venues (Curve / Aerodrome
    classic) where one balance per pool means there is no co-leg to
    disambiguate. ``None`` is faithful per Empty ≠ Zero: it means "no
    discriminator", which the resolver treats as "single-open legacy only".
    """
    if intent_type_str != "LP_OPEN":
        return None
    lp_open = extracted.get("lp_open_data") if isinstance(extracted, dict) else None
    if lp_open is None:
        return None
    raw = getattr(lp_open, "position_id", None)
    # ``LPOpenData.position_id`` is typed ``int`` but defend against the empty /
    # zero-id case: a real minted NFT id is a positive integer. An id of 0 or an
    # empty string is "no usable discriminator" — stamp None so the resolver
    # falls back to single-open legacy rather than matching on a degenerate id.
    if raw is None or raw == "" or raw == 0:
        return None
    return str(raw)


def _resolve_lp_close_discriminator(ledger_row: dict[str, Any]) -> str | None:
    """Return the closing leg's per-position discriminator from the ledger row (VIB-4275).

    The runner stamps the close intent's ``position_id`` onto
    ``LPCloseData.position_id`` (carried in ``transaction_ledger.extracted_data_json``)
    at ledger-build time. Read it back so the close-side resolver can attribute
    a co-pool close to its OWN prior open.

    Returns the discriminator as a stripped string, or ``None`` when absent
    (fungible-LP venue, legacy row written before VIB-4275, or a connector that
    never carries a per-position id). ``None`` is faithful per Empty ≠ Zero:
    the resolver then resolves ONLY the unambiguous single-open case and never
    guesses a sibling/latest open.
    """
    from almanak.framework.observability.ledger import deserialize_extracted_data

    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")
    lp_close = extracted.get("lp_close_data") if isinstance(extracted, dict) else None
    if lp_close is None:
        return None
    raw = getattr(lp_close, "position_id", None)
    # Mirror _resolve_lp_open_discriminator: a degenerate 0 / "0" is "no
    # discriminator" (Empty != Zero) — a real minted NFT id is a positive
    # integer. Filter it on BOTH sides so a close never tries to match against
    # an open whose id-0 was already discarded (gemini review on PR #2459).
    if raw is None or raw == "" or raw == 0 or str(raw).strip() == "0":
        return None
    disc = str(raw).strip()
    return disc or None


def _compute_lp_impermanent_loss(
    intent_type_str: str,
    price_oracle: dict[str, Decimal],
    prior_open_payload: dict[str, Any] | None,
    cost_basis_usd: Decimal | None,
    fees_total_usd: Decimal | None,
) -> tuple[Decimal | None, Decimal | None]:
    """Compute ``(il_usd, hodl_value_usd)`` for ``LP_CLOSE``.

    VIB-4319 — restores LP4 emission. The Accountant Test LP4 cell PASSes
    when any LP_OPEN/LP_CLOSE payload carries a non-null ``il_usd``; pre-fix
    the LP close handler never wrote the field even though the
    :class:`LPCloseEventPayload` schema (``payload_schemas.py:319``) and the
    frozen ``tests/fixtures/accounting/lp/expected_baseline.sqlite``
    expected it. Restoring emission moves LP4 from XFAIL back to PASS.

    Scope — ``LP_CLOSE`` only, NEVER ``LP_COLLECT_FEES`` (Codex review on
    PR #2259, 2026-05-13). A fee-collect operation leaves the principal
    on-chain — ``amount*_collected`` carries fees only (zero principal),
    so ``cost_basis_usd ≈ fees_total_usd`` and the principal-only V_lp
    collapses to zero against a full-position V_hodl. The resulting
    ``il_usd = -V_hodl`` would write a large bogus negative IL into
    accounting on every fee collection even though no IL has crystallised.
    IL is realised when principal is unwound; ``LP_COLLECT_FEES`` does not
    unwind. Future LP_COLLECT_FEES-with-full-close shapes would need their
    own valuation path, not this one.

    Formula (mirrors ``pnl_attributor.compute_impermanent_loss`` for the
    legacy ``position_events`` rail at ``framework/observability/
    pnl_attributor.py:300``)::

        V_hodl    = amount0_open × price0_close + amount1_open × price1_close
        V_lp      = principal_only at close-time prices
                  = cost_basis_usd − fees_total_usd
        il_usd    = V_lp − V_hodl   (negative ⇒ LP lost vs HODL)

    Critical fee-exclusion (Codex review on PR #2259, 2026-05-13)
    ------------------------------------------------------------
    ``cost_basis_usd`` on this event is built from
    ``LPCloseData.amount0_collected`` / ``amount1_collected`` which
    ``almanak/framework/execution/extracted_data.py:153`` defines as
    *principal + fees*. Using ``cost_basis_usd`` directly as V_lp would
    count every dollar of fee income as positive IL — an LP that opened
    100 USDC and closed 100 principal + 10 fees at $1 would emit
    ``il_usd = +10`` against a true IL of 0. To recover the principal-
    only V_lp the handler subtracts ``fees_total_usd`` (computed against
    the same close-time ``price_oracle`` by
    ``_compute_lp_realized_pnl_and_fees``) BEFORE the IL subtraction.
    This keeps fee income exclusively on the ``fees_total_usd`` /
    ``realized_pnl_usd`` surfaces — never silently aliased into IL.

    Fail-closed contracts (CLAUDE.md "Empty ≠ Zero")
    ------------------------------------------------
    Returns ``(None, None)``:

      - Outside ``LP_CLOSE`` (LP_OPEN has no IL; ``LP_COLLECT_FEES``
        leaves principal on-chain — see scope note above).
      - No prior OPEN payload (cannot recover entry amounts).
      - Either entry amount is ``None`` ⇒ V_hodl is not computable.
        Post-VIB-3587 :class:`LPOpenEventPayload` widens ``amount0`` /
        ``amount1`` to ``Decimal | None`` (Empty ≠ Zero), so ``None``
        covers BOTH a parse failure that dropped a two-sided leg AND a
        SINGLE-SIDED LP_OPEN whose unfunded coin is ABSENT (``None``, NOT
        a fabricated ``Decimal("0")``). Either way IL is undefined — a
        single-sided position has no two-sided HODL anchor to diff
        against (see the inline note on the ``amount0_open is None`` guard
        below).
      - Close-time oracle lacks a price for any non-zero entry leg
        (V_hodl unmeasurable).

    Returns ``(None, hodl_value_usd)`` — IL itself is unmeasurable but
    the HODL anchor is fully measurable so operators triaging "why is
    il_usd null?" can still see V_hodl:

      - ``cost_basis_usd`` is ``None`` (principal recovered at close was
        unpriced — typically due to ``assumed_decimals``).
      - ``fees_total_usd`` is ``None`` (parser emitted no measurable
        fees in USD on at least one leg). Without a separable fee
        amount we CANNOT extract the principal portion from
        ``cost_basis_usd`` — the alternative of "assume fees were zero"
        violates Empty ≠ Zero and is exactly the bug Codex flagged.

    Returns ``(Decimal("0"), …)`` only when V_lp == V_hodl exactly —
    that is a measured zero IL, not a fabricated one.

    ``price_oracle`` keys are upper-case symbols (per
    ``parse_price_inputs`` contract). ``token0`` / ``token1`` are pulled
    from the prior OPEN payload so they match the entry token symbols
    regardless of any re-ordering done at the close handler's
    ``_resolve_lp_tokens`` step.
    """
    if intent_type_str != "LP_CLOSE":
        return None, None
    if not prior_open_payload:
        return None, None

    amount0_open = _safe_decimal(prior_open_payload.get("amount0"))
    amount1_open = _safe_decimal(prior_open_payload.get("amount1"))
    # Either leg ``None`` ⇒ V_hodl is not computable, so fail closed (return
    # both ``None``). Two distinct cases both land here and both are correct to
    # skip IL on:
    #   * a parse failure that dropped a two-sided leg, and
    #   * a SINGLE-SIDED LP_OPEN whose unfunded coin is ABSENT (``None``) — the
    #     VIB-3587 Empty ≠ Zero contract (the unfunded coin is NOT a measured
    #     ``Decimal("0")``). A single-sided position has no two-sided HODL anchor
    #     to diff against, so IL is genuinely undefined; computing a partial
    #     V_hodl against a full ``cost_basis_usd`` would emit a misleading
    #     ``il_usd`` (gemini review on PR #2259, 2026-05-13).
    if amount0_open is None or amount1_open is None:
        return None, None

    token0_open = (prior_open_payload.get("token0") or "").upper()
    token1_open = (prior_open_payload.get("token1") or "").upper()

    price0 = price_oracle.get(token0_open) if token0_open else None
    price1 = price_oracle.get(token1_open) if token1_open else None

    # Fail-closed per CLAUDE.md "Empty ≠ Zero": any non-zero entry leg
    # missing a close-time price ⇒ V_hodl is unmeasurable, return both
    # ``None``. Single-sided positions (one entry amount == 0) tolerate
    # the missing price on the zero leg because its contribution is
    # mathematically zero.
    if amount0_open is not None and amount0_open != 0 and price0 is None:
        return None, None
    if amount1_open is not None and amount1_open != 0 and price1 is None:
        return None, None

    hodl = Decimal("0")
    if amount0_open is not None and price0 is not None:
        hodl += amount0_open * price0
    if amount1_open is not None and price1 is not None:
        hodl += amount1_open * price1

    if not hodl.is_finite():
        return None, None

    if cost_basis_usd is None:
        # HODL anchor is still useful even without V_lp; IL itself is
        # unmeasurable.
        return None, hodl

    # Codex review fix — fail closed on unmeasured fees rather than
    # silently substitute zero. Without ``fees_total_usd`` we cannot back
    # the fee portion out of ``cost_basis_usd`` (which is
    # principal + fees per ``LPCloseData.amount*_collected`` semantics)
    # to recover the principal-only V_lp, so any il_usd we emit would
    # double-count fees as IL gain. ``fees_total_usd == Decimal("0")`` is
    # the legitimate "measured zero fees" case (parser emitted
    # ``LPCloseData.fees0/fees1 = 0`` from a real observation and
    # multiplying by any oracle yields exactly zero) and proceeds as
    # ``V_lp == cost_basis_usd``. Post-VIB-4470, parsers emit ``None``
    # instead of fabricating a zero, so this path collapses to the "fees
    # not separated from principal" case (e.g. V4, Fluid, Aerodrome V1).
    if fees_total_usd is None:
        return None, hodl

    v_lp_principal_only = cost_basis_usd - fees_total_usd
    il_usd = v_lp_principal_only - hodl
    if not il_usd.is_finite():
        return None, hodl
    return il_usd, hodl


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


def _coin_decimals(symbol: str, chain: str) -> int | None:
    """Resolve a token's on-chain decimals from the STATIC registry, or ``None``.

    Scales an N-coin Curve leg's raw amount to a human amount. ``skip_gateway=True``
    forces static-registry-only resolution: framework/accounting code MUST NOT make
    a gateway/network call (CLAUDE.md §Gateway boundary), and a deterministic
    accounting valuation must not depend on a live on-chain lookup. A symbol the
    static registry can't resolve raises / returns ``None`` here ⇒ the caller fails
    closed on that leg per Empty ≠ Zero rather than assuming 18 (the static
    registry already carries every coin of the supported USD-stable Curve pools —
    USDC/USDT=6, DAI=18, FRAX/crvUSD=18, USDbC/axlUSDC=6, …).
    """
    if not symbol:
        return None
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        info = get_token_resolver().resolve(symbol, chain=chain, skip_gateway=True)
        return info.decimals if info is not None else None
    except Exception:  # noqa: BLE001 — accounting path: degrade, never raise
        logger.debug("LP handler: static decimals resolve failed for %s on %s", symbol, chain)
        return None


def _is_usd_stable_pool(coin_symbols: list[str]) -> bool:
    """True when EVERY coin in the pool is a recognized ~$1 USD stablecoin.

    VIB-5429 — single source of truth for "is this a USD-stable pool": imports
    the canonical ``_USD_STABLE_SYMBOLS`` frozenset READ-ONLY from the valuation
    layer (``curve_lp_position_reader``), which already owns it for the NAV
    repricer. Do NOT fork/duplicate the list — that drifts. The import is lazy so
    it pays no module-load cost and cannot form an import cycle
    (``curve_lp_position_reader`` imports nothing from accounting at module
    scope). vib542728 is restructuring that file and may relocate the frozenset —
    if this import target moves, it is a rename to reconcile at merge.

    A pool with ANY non-stable coin (tricrypto WBTC/WETH, a metapool's base-LP
    token) is NOT a USD-numeraire pool: its principal legs stay UNAVAILABLE
    (``None``) so G6 correctly FAILs for it — that valuation is the NAV repricer's
    scope, never pegged to $1 here to force a green cell.
    """
    if not coin_symbols:
        return False
    try:
        from almanak.framework.valuation.curve_lp_position_reader import _USD_STABLE_SYMBOLS
    except Exception:  # noqa: BLE001 — degrade to "not stable" (fail closed), never raise
        logger.debug("LP handler: could not import _USD_STABLE_SYMBOLS; treating pool as non-stable")
        return False
    return all((c or "").upper() in _USD_STABLE_SYMBOLS for c in coin_symbols)


def _value_curve_legs_usd(
    legs: list[tuple[str, Decimal | None]],
    price_oracle: dict[str, Decimal],
    is_usd_stable: bool,
) -> tuple[Decimal | None, bool]:
    """Sum ``(symbol, human_amount)`` legs to USD. Returns ``(usd, used_peg)``.

    VIB-5429 — the shared pricing primitive for both Curve fee and principal
    legs. Empty ≠ Zero (CLAUDE.md §Accounting):

      * ANY leg whose amount is UNMEASURED (``None``) ⇒ ``(None, False)``
        (whole-hook unmeasured — never fold an unmeasured leg in as zero).
      * A measured-zero leg contributes exactly ``$0`` and needs no price.
      * A NON-ZERO leg with an oracle price ⇒ ``amount × price``.
      * A NON-ZERO leg with NO oracle price: priced at the ``$1`` USD-stable peg
        ONLY when ``is_usd_stable`` (and ``used_peg`` becomes ``True`` so the
        caller can stamp provenance); otherwise ⇒ ``(None, False)`` (fail closed —
        a non-stable coin is not assumed to be $1).

    Returns ``(None, False)`` when there are no legs at all (nothing measured).
    """
    total = Decimal(0)
    has_any = False
    used_peg = False
    for symbol, amount in legs:
        if amount is None:
            return None, False  # unmeasured leg ⇒ whole-hook None
        has_any = True
        if amount == 0:
            continue  # measured-zero leg: $0, no price needed
        price = price_oracle.get((symbol or "").upper())
        if price is None:
            if is_usd_stable:
                price = Decimal("1")  # USD-stable peg (provenance-stamped by caller)
                used_peg = True
            else:
                return None, False  # non-stable, unpriceable ⇒ fail closed
        total += amount * price
    return (total if has_any else None), used_peg


def _curve_legs(
    raw_amounts: list[Any],
    coin_symbols: list[str],
    chain: str,
) -> list[tuple[str, Decimal | None]] | None:
    """Build one ``(symbol, human_amount)`` leg per pool coin, scaled from wei.

    VIB-5429 — iterates ``coin_symbols`` (the authoritative per-coin universe),
    NOT ``raw_amounts``: every pool coin MUST be accounted for. A coin with no
    corresponding measured amount (index beyond ``raw_amounts``, or a ``None``
    slot) is UNMEASURED ⇒ ``(symbol, None)`` so the pricing primitive fails the
    whole hook (Empty ≠ Zero — a missing close leg is not a fabricated zero). The
    N-coin ``all_amounts`` / ``all_fees`` carriers stamp a measured ``0`` for an
    unfunded coin, which scales to ``Decimal(0)`` here (a measured zero, valued at
    $0). Returns ``None`` only when a present leg's token decimals cannot be
    resolved (cannot scale ⇒ fail closed).
    """
    legs: list[tuple[str, Decimal | None]] = []
    for i, symbol in enumerate(coin_symbols):
        raw = raw_amounts[i] if i < len(raw_amounts) else None
        coerced = _as_int(raw)
        if coerced is None:
            legs.append((symbol, None))  # unmeasured coin ⇒ propagate
            continue
        if coerced == 0:
            # A measured-zero leg scales to ``Decimal(0)`` for ANY decimals, so it
            # needs no decimals resolution — short-circuit BEFORE ``_coin_decimals``.
            # Otherwise a balanced removal's zero leg whose symbol the static
            # registry can't resolve would fail-close the WHOLE Curve valuation
            # (e.g. fees_total_usd → None) over a coin that contributes exactly $0.
            legs.append((symbol, Decimal(0)))
            continue
        decimals = _coin_decimals(symbol, chain)
        if decimals is None:
            return None  # NON-ZERO leg we cannot scale ⇒ fail closed
        legs.append((symbol, Decimal(coerced) / (Decimal(10) ** decimals)))
    return legs


def _curve_close_fees_usd(
    lp_close_data: Any,
    chain: str,
    price_oracle: dict[str, Decimal],
) -> Decimal | None:
    """USD-price a Curve LP_CLOSE's per-coin protocol/imbalance fees (VIB-5429).

    A Curve fungible close returns ALL N pool coins, so the ledger row carries no
    swap-style ``token_in`` / ``token_out`` and the position_key has no token
    descriptor — the generic 2-leg path leaves ``fees_total_usd`` NULL (the
    Accountant Test G6 ``Σ_lp_fees_null_count`` gap this ticket closes). The
    parser stamps the pool-coin-ordered symbols on ``LPCloseData.coin_symbols``
    (same index order as ``all_fees``); we price each fee leg via the shared
    primitive. A balanced proportional ``remove_liquidity`` charges no imbalance
    fee ⇒ every leg is a measured zero ⇒ ``Decimal(0)`` (no price needed).

    Returns ``None`` (handler keeps its legacy 2-leg result) when the parser
    stamped no ``coin_symbols`` (non-Curve / unknown pool) or a leg is unmeasured.
    """
    coin_symbols = getattr(lp_close_data, "coin_symbols", None)
    if not coin_symbols:
        return None
    legs = _curve_legs(lp_close_data.all_fees, coin_symbols, chain)
    if legs is None:
        return None
    fees_usd, _used_peg = _value_curve_legs_usd(legs, price_oracle, _is_usd_stable_pool(coin_symbols))
    return fees_usd


def _curve_lp_principal_usd(
    lp_data: Any,
    intent_type_str: str,
    chain: str,
    price_oracle: dict[str, Decimal],
) -> tuple[Decimal | None, bool]:
    """USD-value a Curve LP event's per-coin PRINCIPAL legs (VIB-5429).

    The realized-PnL / cost-basis sibling of :func:`_curve_close_fees_usd`. The
    generic 2-leg path leaves ``cost_basis_usd`` NULL for a fungible Curve event
    (no token0/token1 on the ledger row) → the LP_CLOSE ``realized_pnl_usd`` is
    NULL → Accountant Test G6 ``Σ_lp_usd_null_count``. Values every coin leg the
    parser measured against the price oracle, with a ``$1`` USD-stable peg for
    unpriced legs ONLY when the pool's coins are ALL recognized USD-stables
    (``_is_usd_stable_pool``) — a non-stable/crypto/metapool pool stays
    UNAVAILABLE (``None``), correctly leaving G6 FAIL for it (the NAV repricer's
    scope).

    Per-coin amount source is ``lp_data.all_amounts`` (pool-coin order, same
    order as ``coin_symbols``) for both events: ``LPOpenData`` (deposits) and
    ``LPCloseData`` (proceeds) each expose the N-coin vector, so a single-sided
    deposit's unfunded coins surface as measured zeros rather than being dropped.

    ``intent_type_str`` is accepted for call-site symmetry / future per-direction
    handling; the valuation itself is direction-agnostic.

    Returns ``(usd, used_peg)``; ``(None, False)`` when no ``coin_symbols`` are
    stamped or any coin leg is unmeasured / unscalable (Empty ≠ Zero).
    """
    del intent_type_str  # accepted for symmetry; valuation is direction-agnostic
    coin_symbols = getattr(lp_data, "coin_symbols", None)
    if not coin_symbols:
        return None, False
    all_amounts = getattr(lp_data, "all_amounts", None)
    if all_amounts is None:
        return None, False
    legs = _curve_legs(all_amounts, coin_symbols, chain)
    if legs is None:
        return None, False
    return _value_curve_legs_usd(legs, price_oracle, _is_usd_stable_pool(coin_symbols))


def _resolve_curve_lp_basis_and_confidence(
    *,
    lp_data: Any,
    intent_type_str: str,
    chain: str,
    price_oracle: dict[str, Decimal],
    cost_basis_usd: Decimal | None,
    assumed_decimals: bool,
    pricing_unavailable_reason: str,
) -> tuple[Decimal | None, AccountingConfidence, str]:
    """Override ``(cost_basis_usd, confidence, unavailable_reason)`` for a Curve LP event (VIB-5429).

    The generic 2-leg path leaves ``cost_basis_usd`` NULL for a fungible Curve LP
    event (no token0/token1 on the ledger row) → LP_CLOSE ``realized_pnl_usd`` is
    NULL → Accountant Test G6 ``Σ_lp_usd_null_count``. When the parser stamped
    pool-coin symbols, value the per-coin principal legs ($1-peg for unpriced legs
    of an all-USD-stable pool only) and override the basis. The Curve valuation
    resolved per-coin decimals + prices itself, so its provenance is authoritative
    and supersedes the legacy 2-token "assumed decimals" confidence: per blueprint
    27 §7.10 a $1 USD-stable-peg basis is an explicit approximation, stamped
    ESTIMATED + a self-describing reason, never re-marked HIGH; an all-oracle-priced
    basis (incl. measured-zero legs) is HIGH.

    No-op (returns the inputs + the legacy ``_determine_lp_confidence`` result) for
    non-Curve events / when the parser stamped no ``coin_symbols`` or the valuation
    is unmeasurable (Empty ≠ Zero — the legacy NULL basis is preserved). Extracted
    from ``handle_lp`` to keep it within the CRAP budget.
    """
    curve_basis_applied = False
    curve_basis_used_peg = False
    if lp_data is not None and getattr(lp_data, "coin_symbols", None):
        curve_basis_usd, curve_basis_used_peg = _curve_lp_principal_usd(lp_data, intent_type_str, chain, price_oracle)
        if curve_basis_usd is not None:
            cost_basis_usd = curve_basis_usd
            curve_basis_applied = True

    confidence, unavailable_reason = _determine_lp_confidence(
        assumed_decimals, cost_basis_usd, pricing_unavailable_reason
    )

    if curve_basis_applied:
        if curve_basis_used_peg:
            confidence = AccountingConfidence.ESTIMATED
            unavailable_reason = "usd_stable_peg: unpriced USD-stable Curve coin(s) valued at $1 (VIB-5429)"
        else:
            confidence = AccountingConfidence.HIGH
            unavailable_reason = ""
    return cost_basis_usd, confidence, unavailable_reason


def _override_curve_close_fees(
    intent_type_str: str,
    lp_data: Any,
    chain: str,
    price_oracle: dict[str, Decimal],
    fees_total_usd: Decimal | None,
) -> Decimal | None:
    """Return the Curve N-coin close fee USD when measurable, else ``fees_total_usd`` (VIB-5429).

    A Curve fungible close has no token0/token1 on the ledger row, so the generic
    2-leg path leaves ``fees_total_usd`` NULL (G6 ``Σ_lp_fees_null_count``). When
    the parser stamped pool-coin symbols on the close data, price every fee leg
    from them instead. Strictly additive: a measured result (incl. a balanced
    removal's ``Decimal(0)``) wins; ``None`` (unmeasured / unpriced leg, or a
    non-close intent) keeps the legacy value — never overwrites measured with NULL.
    """
    if intent_type_str not in _LP_CLOSE_LIKE or lp_data is None:
        return fees_total_usd
    curve_fees_usd = _curve_close_fees_usd(lp_data, chain, price_oracle)
    return curve_fees_usd if curve_fees_usd is not None else fees_total_usd


def _value_weighted_leg_basis(
    active_legs: list[tuple[str, Decimal]],
    total_val_usd: Decimal | None,
    price_oracle: dict[str, Decimal],
) -> list[Decimal | None]:
    """Split ``total_val_usd`` across legs by close-time USD value (VIB-4264).

    Returns a ``cost_usd`` per leg, keyed BY INDEX (not by token symbol) so the
    degenerate ``token0 == token1`` case keeps two independent slots. Replaces
    the prior equal-split which over-based the smaller-value leg and inflated
    the closing SWAP's ``realized_pnl_usd_matched`` (mainnet repro −$0.2507 vs
    true ≈ −$0.02).

    Empty ≠ Zero (CLAUDE.md), Option (a) — whole-hook None: the weight is a
    RATIO across legs, so ``total_val_usd is None`` OR ANY leg with a missing /
    non-finite price ⇒ ``None`` for ALL legs (the denominator Σ leg_value is
    unmeasurable; assigning a concrete basis to the priced leg would fabricate
    it). The LAST leg takes the residual so Σ(result) == ``total_val_usd``
    EXACTLY — no basis created/destroyed to Decimal rounding (same residual
    technique as ``swap_handler._split_proceeds``).
    """
    n = len(active_legs)
    if total_val_usd is None:
        return [None] * n

    leg_value_usd: list[Decimal] = []
    for leg_token, leg_amount in active_legs:
        price = price_oracle.get((leg_token or "").upper())
        if price is None or not price.is_finite():
            return [None] * n  # whole-hook None: unmeasurable cross-leg ratio
        leg_value_usd.append(leg_amount * price)

    total_value = sum(leg_value_usd, Decimal("0"))
    if total_value <= 0:
        # Degenerate (leg values net to ≤ 0, e.g. a zero-priced leg): fall back
        # to an equal split, last leg taking the residual so Σ stays exact —
        # basis neither dropped nor fabricated, same invariant as the weighted
        # path below.
        even = total_val_usd / Decimal(n)
        fallback: list[Decimal | None] = [even] * (n - 1)
        fallback.append(total_val_usd - even * Decimal(n - 1))
        return fallback

    per_leg_basis: list[Decimal | None] = [None] * n
    running = Decimal("0")
    last = n - 1
    for i in range(n):
        if i < last:
            share = total_val_usd * (leg_value_usd[i] / total_value)
            per_leg_basis[i] = share
            running += share
        else:
            # Last leg absorbs the residual ⇒ Σ == total_val_usd exactly.
            per_leg_basis[i] = total_val_usd - running
    return per_leg_basis


# crap-allowlist: VIB-4262 — _apply_lp_wallet_basis_hooks branches per
# (intent_type × token leg × skip-condition) which is the irreducible shape
# of LP semantics: LP_OPEN drains both tokens, LP_CLOSE / LP_COLLECT_FEES
# record an active_legs loop with per-leg amount=principal+fees and
# per-leg cost_basis split (gemini-code-assist 2026-05-11). cc=31 is per-
# leg-branch cost; decomposing into 4-5 micro-helpers would add naming
# overhead without architectural value (no shared abstraction emerges from
# the per-leg branches). Anti-regression coverage: 6 tests in
# tests/unit/framework/accounting/test_lp_perp_vault_handlers.py
# (TestHandleLpWalletBasisHooks; 100% line coverage).
def _apply_lp_wallet_basis_hooks(
    *,
    basis_store: FIFOBasisStore | None,
    intent_type_str: str,
    deployment_id: str,
    cycle_id: str,
    chain: str,
    wallet_address: str,
    token0: str,
    token1: str,
    amount0: Decimal | None,
    amount1: Decimal | None,
    fees0: Decimal | None,
    fees1: Decimal | None,
    cost_basis_usd: Decimal | None,
    fees_total_usd: Decimal | None,
    price_oracle: dict[str, Decimal],  # NEW (VIB-4264)
    timestamp: datetime,
    tx_hash: str,
    ledger_entry_id: str,
) -> None:
    """Mirror LP token flow into the chain+wallet basis pool (VIB-4262).

    LP_OPEN deposits token0 + token1 into the pool — drains wallet inventory.
    LP_CLOSE / LP_COLLECT_FEES return token0 + token1 (+ accumulated fees) —
    credits wallet inventory.

    This is the LP analogue of the lending handler's VIB-3964 hooks
    (BORROW credits, REPAY drains, SUPPLY drains, WITHDRAW credits). Without
    these, an LP_OPEN/LP_CLOSE round-trip leaves the wallet's pre-LP token-out
    lots intact, and a follow-up SWAP that disposes the LP-returned tokens
    can't compute ``realized_pnl_usd`` against fresh LP-CLOSE basis.

    Skip silently if any of the following hold (Empty ≠ zero per CLAUDE.md):
      - basis_store is None (paper / dry-run mode);
      - chain or wallet_address are empty (swap_wallet_key cannot resolve);
      - both token0 and token1 are empty (no leg can be hooked);
      - on LP_OPEN: both amounts are None (nothing to dispose);
      - on LP_CLOSE: both amounts are None AND no fees (nothing to record).

    Per-leg granularity: each (token, amount) pair is independently hooked, so
    a partial pricing failure on one leg does not silently drop the other.
    """
    if basis_store is None:
        return
    chain_norm = (chain or "").lower().strip()
    wallet_norm = (wallet_address or "").lower().strip()
    if not chain_norm or not wallet_norm:
        return
    swap_wallet_key = f"swap:{chain_norm}:{wallet_norm}"
    _seed = tx_hash or ledger_entry_id

    if intent_type_str == "LP_OPEN":
        # LP_OPEN moves token0 + token1 OUT of the wallet into the LP NFT.
        # Mirror as a wallet-basis disposal so the lots minted by prior SWAP /
        # BORROW / WITHDRAW are drained correctly.
        if token0 and amount0 is not None and amount0 > 0:
            basis_store.match_swap_disposal(
                deployment_id=deployment_id,
                position_key=swap_wallet_key,
                token=token0,
                amount=amount0,
            )
        if token1 and amount1 is not None and amount1 > 0:
            basis_store.match_swap_disposal(
                deployment_id=deployment_id,
                position_key=swap_wallet_key,
                token=token1,
                amount=amount1,
            )
        return

    # LP_CLOSE / LP_COLLECT_FEES return amount + accumulated fees per token.
    # Mirror as wallet-basis acquisition so a follow-up SWAP that disposes
    # the returned tokens has a basis lot to match against.
    #
    # Cost-basis distribution (gemini-code-assist 2026-05-11; VIB-4264):
    #
    # 1. Per-leg amount = principal + accumulated fees. LP_COLLECT_FEES has
    #    amount0/amount1 == 0 by design; without summing fees the hook would
    #    skip fee-only events entirely.
    # 2. Total returned-USD = cost_basis_usd (principal MTM) + fees_total_usd.
    #    Fees collected to the wallet have economic value and SHOULD anchor
    #    the cost lot — without them, a follow-up SWAP that disposes the
    #    fee portion mis-computes realized PnL.
    # 3. Active legs only. Single-sided exits (one token amount==0) get the
    #    full per-leg basis.
    # 4. VIB-4264 — VALUE-WEIGHTED distribution. The whole-position
    #    ``total_val_usd`` is split across legs in proportion to each leg's
    #    USD value (leg_amount × close-time price), NOT equally by leg count.
    #    The prior equal split over-based the smaller-value leg: a 100 USDC +
    #    0.01 WETH close (USDC:1, WETH:2000 ⇒ $100 vs $20, total $120) stamped
    #    $60 on EACH leg, over-basing WETH by +$40. That over-based lot
    #    re-enters the swap FIFO pool and inflates the closing SWAP's
    #    ``realized_pnl_usd_matched`` (= matched_proceeds − cost_basis_consumed
    #    in ``swap_handler.py``). Mainnet repro: −$0.2507 vs true ≈ −$0.02.
    #    The split keeps the Σ-invariant EXACTLY — the LAST leg takes the
    #    residual (``total_val_usd − Σ(previous)``) so no basis is created or
    #    destroyed to Decimal rounding, mirroring ``swap_handler._split_proceeds``.
    #    Empty ≠ Zero (CLAUDE.md), Option (a) — whole-hook None: the weight is
    #    a RATIO across legs, so a single missing/non-finite leg price makes the
    #    denominator (Σ leg_value) unmeasurable for EVERY leg. Assigning a
    #    concrete basis to the priced leg would fabricate that denominator, so
    #    ALL legs fall back to ``cost_usd = None`` (lots still recorded; neither
    #    leg dropped). Fail-closed (swap emits realized_pnl = None) beats
    #    fail-wrong.
    if intent_type_str in {"LP_CLOSE", "LP_COLLECT_FEES"}:
        t0_total = (amount0 if amount0 is not None else Decimal("0")) + (fees0 if fees0 is not None else Decimal("0"))
        t1_total = (amount1 if amount1 is not None else Decimal("0")) + (fees1 if fees1 is not None else Decimal("0"))
        active_legs: list[tuple[str, Decimal]] = []
        if token0 and t0_total > 0:
            active_legs.append((token0, t0_total))
        if token1 and t1_total > 0:
            active_legs.append((token1, t1_total))
        if not active_legs:
            return

        # Empty ≠ zero (CLAUDE.md): only seed `cost_usd` when ALL economic
        # components contributing to the lots are measured. If principal
        # amounts are present but `cost_basis_usd is None`, OR fee amounts
        # are present but `fees_total_usd is None`, leave `per_leg_basis = None`
        # rather than substitute zeros and fabricate basis for the unmeasured
        # component. CodeRabbit 2026-05-11 catch — without this guard, a
        # close/collect with one fee leg unpriced would assign a concrete
        # `per_leg_basis` to lots that include the fee amount and skew the
        # next SWAP's `realized_pnl_usd`.
        principal_present = any(a is not None and a > 0 for a in (amount0, amount1))
        fees_present = any(f is not None and f > 0 for f in (fees0, fees1))
        total_val_usd: Decimal | None = None
        if (not principal_present or cost_basis_usd is not None) and (not fees_present or fees_total_usd is not None):
            total_val_usd = Decimal("0")
            if cost_basis_usd is not None:
                total_val_usd += cost_basis_usd
            if fees_total_usd is not None:
                total_val_usd += fees_total_usd
        # VIB-4264: value-weight ``total_val_usd`` across legs by close-time
        # USD value (see ``_value_weighted_leg_basis``). Keyed BY INDEX.
        per_leg_basis = _value_weighted_leg_basis(active_legs, total_val_usd, price_oracle)

        for idx, (leg_token, leg_amount) in enumerate(active_legs):
            basis_store.record_swap_acquisition(
                deployment_id=deployment_id,
                position_key=swap_wallet_key,
                token=leg_token,
                amount=leg_amount,
                cost_usd=per_leg_basis[idx],
                timestamp=timestamp,
                lot_id=(
                    make_accounting_event_id(deployment_id, cycle_id, "LP_CLOSE_WALLET_LOT", _seed, leg_token)
                    if _seed
                    else ""
                ),
                source=intent_type_str,
            )


def handle_lp(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
    prior_open_payload: dict[str, Any] | None = None,
    basis_store: FIFOBasisStore | None = None,
) -> LPAccountingEvent | None:
    """Build an LPAccountingEvent from an outbox + ledger row pair.

    Returns None for:
    - Non-LP intent types
    - Intents where both position_key and market_id are absent (cannot identify pool)

    All inputs come from the dicts — no live chain calls.

    VIB-4262 (basis_store): when provided, LP_OPEN/LP_CLOSE/LP_COLLECT_FEES
    mirror their on-chain wallet flow into the chain+wallet FIFO pool used by
    SWAP / lending realized-PnL math. See :func:`_apply_lp_wallet_basis_hooks`.
    """
    from almanak.framework.observability.ledger import deserialize_extracted_data

    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    if intent_type_str not in _LP_OPEN_CLOSE:
        return None

    event_type = _INTENT_TO_EVENT_TYPE.get(intent_type_str)
    if event_type is None:
        return None

    position_key = outbox_row.get("position_key") or ""
    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")
    pool_address = _resolve_lp_pool_address(
        outbox_row=outbox_row,
        position_key=position_key,
        extracted=extracted,
        prior_open_payload=prior_open_payload,
    )
    if pool_address is None:
        return None

    timestamp = _parse_lp_timestamp(ledger_row.get("timestamp"))
    token0, token1 = _resolve_lp_tokens(ledger_row, position_key)
    chain = ledger_row.get("chain") or ""
    protocol = (ledger_row.get("protocol") or "").lower()

    # VIB-4636 (sibling alignment fix) — V4 LP amounts ship in canonical
    # PoolKey order (``currency0 < currency1``) regardless of the order the
    # user wrote into the intent / ledger row. Re-pair ``(token0, token1)``
    # by canonical currency addresses BEFORE decimals are resolved, so
    # ``_resolve_lp_amounts`` scales each raw amount with the matching
    # decimals instead of silently mis-scaling. Capability-gated, not
    # protocol-name-gated: ``_v4_realign_token_pair`` no-ops unless the typed
    # LP data carries ``currency0/currency1`` (the V4 PoolKey signal), so V3
    # and fungible-LP callers fall through unchanged.
    lp_field = "lp_open_data" if intent_type_str == "LP_OPEN" else "lp_close_data"
    lp_data = extracted.get(lp_field) if isinstance(extracted, dict) else None
    token0, token1 = _v4_realign_token_pair(lp_data, chain, token0, token1)

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

    # VIB-5429 — override cost_basis + confidence with the N-coin Curve valuation
    # (no-op for non-Curve / no coin_symbols). Must run BEFORE realized_pnl so it
    # sees the measured basis. Extracted to keep handle_lp within the CRAP budget.
    cost_basis_usd, confidence, unavailable_reason = _resolve_curve_lp_basis_and_confidence(
        lp_data=lp_data,
        intent_type_str=intent_type_str,
        chain=chain,
        price_oracle=price_oracle,
        cost_basis_usd=cost_basis_usd,
        assumed_decimals=assumed_decimals,
        pricing_unavailable_reason=pricing_unavailable_reason,
    )

    # ── Identity ─────────────────────────────────────────────────────────────
    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, intent_type_str, _id_seed, position_key),
        deployment_id=deployment_id,
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

    # VIB-4275 — stamp the per-position discriminator (the NFT token id) on EVERY
    # LP event so the audit row is joinable to position_events by position_id:
    #   • LP_OPEN: the minted token id read off LPOpenData — lets a later co-pool
    #     close resolve THIS leg's open rather than the most-recent by timestamp.
    #   • LP_CLOSE / LP_COLLECT_FEES: the closing intent's position_id (carried in
    #     extracted_data_json) — the SAME id used to resolve the prior open, now
    #     also persisted on the close row instead of being inferable only from the
    #     resolved tick range. The intent always knows which NFT it is closing.
    # None for fungible-LP venues (Curve / Aerodrome classic) with no per-leg id.
    if intent_type_str == "LP_OPEN":
        position_id_v = _resolve_lp_open_discriminator(intent_type_str, extracted)
    else:
        position_id_v = _resolve_lp_close_discriminator(ledger_row)

    # VIB-4473 / VIB-4636 — V4 lot-matching anchor read from
    # ``extracted["lp_open_data"]`` on LP_OPEN. V3 parsers leave the field
    # ``None`` and it forwards as-is so the payload key shape is stable
    # across protocols. LP_CLOSE / LP_COLLECT_FEES leave it ``None``: the
    # close leg matches against the prior OPEN payload by ``position_key``,
    # not by re-reading the hash off the burn receipt.
    position_hash_v: str | None = None
    if intent_type_str == "LP_OPEN":
        lp_open_extracted = extracted.get("lp_open_data") if isinstance(extracted, dict) else None
        if isinstance(lp_open_extracted, dict):
            # dict fallback from deserialize_extracted_data — getattr would
            # silently lose the anchor, so read the key directly.
            position_hash_v = lp_open_extracted.get("position_hash")
        elif lp_open_extracted is not None:
            position_hash_v = getattr(lp_open_extracted, "position_hash", None)

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

    # VIB-5429 — override fees_total_usd with the N-coin Curve fee valuation on a
    # close (no-op otherwise / when no coin_symbols). Extracted to keep handle_lp
    # within the CRAP budget; strictly additive (never overwrites measured w/ NULL).
    fees_total_usd = _override_curve_close_fees(intent_type_str, lp_data, chain, price_oracle, fees_total_usd)

    # VIB-4319 — IL diagnostic on ``LP_CLOSE`` ONLY (Codex review on
    # PR #2259: ``LP_COLLECT_FEES`` leaves principal on-chain so the IL
    # math collapses to a bogus negative — see
    # ``_compute_lp_impermanent_loss`` scope note). Same fail-closed
    # contract as ``_compute_lp_realized_pnl_and_fees``: missing prior
    # OPEN, either entry leg ``None`` (data integrity), missing close-time
    # price on a non-zero entry leg, OR missing ``fees_total_usd``
    # (cannot separate principal from fees in ``cost_basis_usd``) ⇒
    # ``il_usd = None`` (Empty ≠ Zero). ``hodl_value_usd`` is reported
    # independently so the dashboard can render V_hodl even when V_lp is
    # unpriced. ``fees_total_usd`` MUST be threaded in because
    # ``cost_basis_usd`` is built from ``amount*_collected`` which carries
    # principal + fees per ``LPCloseData`` semantics — see
    # ``_compute_lp_impermanent_loss`` docstring for the fee-exclusion
    # math.
    il_usd, hodl_value_usd = _compute_lp_impermanent_loss(
        intent_type_str=intent_type_str,
        price_oracle=price_oracle,
        prior_open_payload=prior_open_payload,
        cost_basis_usd=cost_basis_usd,
        fees_total_usd=fees_total_usd,
    )

    # VIB-4262: mirror LP token flow into the chain+wallet basis pool so a
    # follow-up SWAP that disposes the LP-returned tokens can compute
    # realized_pnl_usd. LP_OPEN drains, LP_CLOSE / LP_COLLECT_FEES record.
    _apply_lp_wallet_basis_hooks(
        basis_store=basis_store,
        intent_type_str=intent_type_str,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        chain=chain,
        wallet_address=wallet_address,
        token0=token0,
        token1=token1,
        amount0=amount0,
        amount1=amount1,
        fees0=fees0,
        fees1=fees1,
        cost_basis_usd=cost_basis_usd,
        fees_total_usd=fees_total_usd,
        price_oracle=price_oracle,
        timestamp=timestamp,
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
        il_usd=il_usd,
        hodl_value_usd=hodl_value_usd,
        position_id=position_id_v,
        position_hash=position_hash_v,
        # VIB-5429 — forward the N-coin pool-coin identity (Curve) onto the event
        # so a proportional close's measured USD basis self-documents WHICH coins
        # back it, even though token0/token1 are empty (no 2-token direction).
        coin_symbols=getattr(lp_data, "coin_symbols", None),
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

    VIB-4262: forwards ``ctx.basis_store`` so LP_OPEN / LP_CLOSE /
    LP_COLLECT_FEES can mirror their on-chain wallet flow into the chain+wallet
    FIFO pool. See :func:`_apply_lp_wallet_basis_hooks`.
    """
    intent_type = (ctx.ledger_row.get("intent_type") or "").upper()
    prior_open: dict[str, Any] | None = None
    if intent_type in {"LP_CLOSE", "LP_COLLECT_FEES"}:
        # VIB-4275 — extract the closing leg's per-position discriminator (the
        # NFT token id the runner stamped onto ``lp_close_data`` from the close
        # intent) and thread it into the resolver so a co-pool close attributes
        # to its OWN prior open. None ⇒ fungible-LP venue / legacy row; the
        # resolver then only resolves the unambiguous single-open case (it never
        # guesses a sibling/latest open).
        discriminator = _resolve_lp_close_discriminator(ctx.ledger_row)
        prior_open = ctx.prior_open_lookup(ctx.outbox_row.get("position_key") or "", discriminator)
    return handle_lp(
        ctx.outbox_row,
        ctx.ledger_row,
        prior_open_payload=prior_open,
        basis_store=ctx.basis_store,
    )
