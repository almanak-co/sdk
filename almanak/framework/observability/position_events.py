"""Position lifecycle events for LP and perps tracking.

Immutable-ID positions (LP NFTs, perp positions) have a lifecycle:
OPEN -> SNAPSHOT* -> CLOSE.  Each state change is recorded as a
PositionEvent with raw observables (amounts, prices, fees).

Fungible positions (lending supply/borrow, staking) are tracked via
enriched snapshot deltas (Phase 1d), not lifecycle events.

Phase 5i — helper extraction layout
-----------------------------------
``build_position_event_from_intent`` is composed from small phase helpers.
The phase ordering is LOAD-BEARING and must not change:

    α  _seed_event          : intent-type dispatch + seed (position_id,
                              tx details, protocol, chain, ledger link)
    γ  _apply_lp_open       : lp_open_data enrichment (pair tokens,
                              liquidity, ticks, deposit amounts)
    δ  _apply_lp_close      : lp_close_data enrichment (received amounts,
                              fee coalescing)
    δ- _apply_collect_fees  : VIB-3494 COLLECT_FEES-specific enrichment.
                              Reads fee amounts from lp_close_data when
                              the intent type is LP_COLLECT_FEES. MUST run
                              after δ so the collect-only data path doesn't
                              conflict with a close that already populated
                              the same slots.
    ε  _apply_swap_fallback : swap_amounts fills ONLY empty token/amount
                              slots — MUST run AFTER γ so an LP_OPEN with
                              a co-occurring swap leg keeps its real pair
                              identities (token0/token1) instead of being
                              clobbered by the swap's token_in/token_out.
    ζ  _apply_perp          : perp_data enrichment. Overrides
                              ``position_id`` when ``perp.position_id`` is
                              truthy; a mismatch against an already-seeded
                              ``event.position_id`` is logged as a WARNING
                              (fix #1709 — perp still wins, but silently
                              no longer).
    η  _apply_protocol_fees : VIB-3205 protocol fee USD capture. Empty
                              string ("unknown") is DISTINCT from "0"
                              ("measured zero") — preserve that invariant.
                              VIB-3495: explicit ProtocolFees with
                              unavailable_reason also leaves the field as
                              "" (known-unknown, not mis-reported as zero).
    θ                         final guard: no position_id → drop the event.

Constraint (critical): γ → δ → δ- → ε → ζ → η ordering. Re-ordering ε
before γ silently regresses the invariant called out above.
"""

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


class PositionEventType(StrEnum):
    """Types of position lifecycle events."""

    OPEN = "OPEN"
    CLOSE = "CLOSE"
    COLLECT_FEES = "COLLECT_FEES"
    SNAPSHOT = "SNAPSHOT"


class PositionType(StrEnum):
    """Types of tracked positions (immutable-ID only)."""

    LP = "LP"
    PERP = "PERP"


# Intent types that map to position events (LP + perps only).
# Fungible positions (SUPPLY, BORROW, STAKE, etc.) are excluded.
INTENT_TO_EVENT_TYPE: dict[str, PositionEventType] = {
    "LP_OPEN": PositionEventType.OPEN,
    "LP_CLOSE": PositionEventType.CLOSE,
    "LP_COLLECT_FEES": PositionEventType.COLLECT_FEES,
    "PERP_OPEN": PositionEventType.OPEN,
    "PERP_CLOSE": PositionEventType.CLOSE,
}

INTENT_TO_POSITION_TYPE: dict[str, PositionType] = {
    "LP_OPEN": PositionType.LP,
    "LP_CLOSE": PositionType.LP,
    "LP_COLLECT_FEES": PositionType.LP,
    "PERP_OPEN": PositionType.PERP,
    "PERP_CLOSE": PositionType.PERP,
}


@dataclass
class PositionEvent:
    """A single position lifecycle event.

    Attributes:
        id: Unique event identifier (UUID).
        deployment_id: Strategy deployment that owns this position.
        position_id: Immutable position identifier (e.g. NFT tokenId).
        position_type: LP or PERP.
        event_type: OPEN, CLOSE, COLLECT_FEES, or SNAPSHOT.
        timestamp: When the event occurred.
        protocol: Protocol used (e.g. uniswap_v3, gmx_v2).
        chain: Chain where the position lives.

        # Token amounts (raw observables)
        token0: First token symbol or address.
        token1: Second token symbol or address.
        amount0: Amount of token0 (human-readable decimal).
        amount1: Amount of token1 (human-readable decimal).
        value_usd: Total USD value at event time.

        # LP-specific
        tick_lower: Lower tick boundary.
        tick_upper: Upper tick boundary.
        liquidity: Liquidity amount.
        in_range: Whether position is in range.
        fees_token0: Uncollected fees in token0.
        fees_token1: Uncollected fees in token1.

        # Perp-specific
        leverage: Position leverage.
        entry_price: Entry price.
        mark_price: Current mark price.
        unrealized_pnl: Unrealized PnL in USD.
        is_long: Long or short.

        # Execution details
        tx_hash: Transaction hash (for trade events).
        gas_usd: Gas cost in USD.
        ledger_entry_id: FK to transaction_ledger.

        # Versioned attribution (Phase 2 PnLAttributor)
        attribution_json: Derived PnL breakdown (versioned, recomputable).
        attribution_version: Version of the attribution algorithm.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    deployment_id: str = ""
    cycle_id: str = ""  # Phase 4: correlation to iteration (VIB-2835)
    execution_mode: str = ""  # Phase 4: "live", "paper", "dry_run" (VIB-2837)
    position_id: str = ""
    position_type: str = ""
    event_type: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    protocol: str = ""
    chain: str = ""

    # Token amounts
    token0: str = ""
    token1: str = ""
    amount0: str = ""
    amount1: str = ""
    value_usd: str = ""

    # LP-specific
    tick_lower: int | None = None
    tick_upper: int | None = None
    liquidity: str = ""
    in_range: bool | None = None
    fees_token0: str = ""
    fees_token1: str = ""

    # Perp-specific
    leverage: str = ""
    entry_price: str = ""
    mark_price: str = ""
    unrealized_pnl: str = ""
    is_long: bool | None = None

    # Execution details
    tx_hash: str = ""
    gas_usd: str = ""
    ledger_entry_id: str = ""

    # Protocol fees (VIB-3205): USD cost captured by the protocol on this tx.
    # Sourced from ``result.extracted_data["protocol_fees"].total_usd`` (the
    # ProtocolFees dataclass shipped by VIB-3204). A parser that does not yet
    # emit ``protocol_fees`` leaves this empty string — attribution must treat
    # empty as "unknown", distinct from a measured zero ("0").
    protocol_fees_usd: str = ""

    # Attribution
    attribution_json: str = "{}"
    attribution_version: int = 0

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d


@dataclass(frozen=True)
class IntentEventContext:
    """Immutable bag of inputs shared across phase helpers (Phase 5i).

    Bundles the raw intent/result, pre-fetched ``extracted_data`` dict, and
    the static wiring fields (deployment_id, chain, ledger_entry_id,
    price_oracle) so each ``_apply_*`` helper has one parameter instead of
    seven.
    """

    intent: Any
    result: Any
    extracted: dict[str, Any]
    deployment_id: str
    chain: str
    ledger_entry_id: str
    price_oracle: dict | None = None


def _seed_event(ctx: IntentEventContext) -> PositionEvent | None:
    """Phase α + β — intent-type dispatch and seed the PositionEvent.

    Returns ``None`` when the intent type is not a position-producing
    lifecycle intent (SWAP / SUPPLY / BORROW / ...), matching the original
    early-exit on line 174 of the pre-refactor implementation.
    """
    intent = ctx.intent
    intent_type = ""
    if hasattr(intent, "intent_type"):
        it = intent.intent_type
        intent_type = it.value if hasattr(it, "value") else str(it)

    event_type = INTENT_TO_EVENT_TYPE.get(intent_type)
    if event_type is None:
        return None

    position_type = INTENT_TO_POSITION_TYPE.get(intent_type, PositionType.LP)
    protocol = getattr(intent, "protocol", "") or ""

    # Position id: result.position_id takes precedence over intent.position_id.
    position_id = ""
    result = ctx.result
    if result and hasattr(result, "position_id") and result.position_id:
        position_id = str(result.position_id)
    elif hasattr(intent, "position_id") and intent.position_id:
        position_id = str(intent.position_id)

    # Tx details from the result envelope (first transaction only).
    # Gas USD precedence mirrors the ledger writer's
    # ``observability.ledger._extract_tx_and_gas``:
    #   1. honour a pre-computed ``result.gas_cost_usd`` if set (legacy
    #      enrichers like the prediction-handler path);
    #   2. otherwise compute from ``result.total_gas_cost_wei × native_usd``
    #      via ``accounting.gas_pricing.compute_gas_usd`` — closes the gap
    #      where ``position_events.gas_usd`` was empty even when the ledger
    #      had real numbers, because the orchestrator only populates
    #      ``total_gas_cost_wei``, not ``gas_cost_usd``.
    tx_hash = ""
    gas_usd = ""
    if result:
        if hasattr(result, "transaction_results") and result.transaction_results:
            tx_hash = result.transaction_results[0].tx_hash or ""
        gas_cost_legacy = getattr(result, "gas_cost_usd", None)
        if gas_cost_legacy is not None:
            gas_usd = str(gas_cost_legacy)
        else:
            from almanak.framework.accounting.gas_pricing import compute_gas_usd

            gas_cost_wei = getattr(result, "total_gas_cost_wei", None)
            computed = compute_gas_usd(
                gas_cost_wei=gas_cost_wei,
                chain=ctx.chain,
                price_oracle=ctx.price_oracle,
            )
            if computed is not None:
                gas_usd = str(computed)

    return PositionEvent(
        deployment_id=ctx.deployment_id,
        position_id=position_id,
        position_type=position_type.value,
        event_type=event_type.value,
        protocol=protocol,
        chain=ctx.chain,
        tx_hash=tx_hash,
        gas_usd=gas_usd,
        ledger_entry_id=ctx.ledger_entry_id,
    )


def _apply_lp_open(event: PositionEvent, ctx: IntentEventContext) -> None:
    """Phase γ — enrich with lp_open_data.

    Populates position_id (override), liquidity, ticks, deposit amounts, and
    the LP pair tokens. Tokens prefer intent.token0/token1, falling back to
    intent.from_token/to_token when the LP intent carries the pair as the
    two swap sides.

    VIB-3887: when ``lp_open_data`` carries ``current_tick``, derive
    ``in_range`` directly from the bracket. The current_tick is sourced
    from the gateway-side receipt parser (which has authority to call
    ``slot0().tick`` after the mint receipt) — framework code consumes
    it here, never populates it via direct RPC. When ``current_tick``
    is None (gateway hasn't been updated, or the protocol has no range
    semantic) ``in_range`` stays None — readers degrade gracefully.
    """
    lp_open = ctx.extracted.get("lp_open_data")
    if not (lp_open and hasattr(lp_open, "position_id")):
        return

    # Only override when non-zero: protocols without an NFT tokenId (e.g. Pendle)
    # set position_id=0 so that extract_position_id() (which returns the canonical
    # hex market address) remains authoritative via _seed_event.
    if lp_open.position_id:
        event.position_id = str(lp_open.position_id)
    event.liquidity = str(getattr(lp_open, "liquidity", "") or "")
    event.tick_lower = getattr(lp_open, "tick_lower", None)
    event.tick_upper = getattr(lp_open, "tick_upper", None)
    # VIB-3887 — in_range derivation from gateway-supplied current_tick.
    current_tick = getattr(lp_open, "current_tick", None)
    if current_tick is not None and event.tick_lower is not None and event.tick_upper is not None:
        # Uniswap V3 / TraderJoe / aerodrome convention: position is in
        # range when tick_lower <= current_tick < tick_upper. Equality on
        # the upper bound is exclusive.
        event.in_range = event.tick_lower <= current_tick < event.tick_upper
    # VIB-3205 audit fix (Codex P1, pr-auditor Blocker #1): populate
    # amount0/amount1 + token0/token1 from the extracted LP open data.
    # Without these, `compute_impermanent_loss` short-circuits to None
    # because the entry-state builder reads amount0/amount1 off the
    # PositionEvent. Previously this block only copied position_id /
    # liquidity / ticks, leaving the IL pipeline as dead code in
    # production.
    amount0 = getattr(lp_open, "amount0", None)
    amount1 = getattr(lp_open, "amount1", None)
    if amount0 is not None:
        event.amount0 = str(amount0)
    if amount1 is not None:
        event.amount1 = str(amount1)
    # Token addresses: LPOpenData doesn't carry them directly. Try intent attrs,
    # then parse from the pool string (e.g. "WETH/USDC/3000", "USDC/DAI/stable").
    intent = ctx.intent
    t0 = getattr(intent, "token0", None) or getattr(intent, "from_token", None)
    t1 = getattr(intent, "token1", None) or getattr(intent, "to_token", None)
    if not t0 or not t1:
        pool_str = (getattr(intent, "pool", "") or "").strip()
        if "/" in pool_str:
            parts = [p.strip() for p in pool_str.split("/") if p.strip()]
            normalized = [
                p.split("(")[0].split(" ")[0].strip()
                for p in parts
                if not p.strip().isdigit() and not p.strip().lower().startswith("0x")
            ]
            if not t0 and normalized:
                t0 = normalized[0].upper()
            if not t1 and len(normalized) > 1:
                t1 = normalized[1].upper()
    if t0:
        event.token0 = str(t0)
    if t1:
        event.token1 = str(t1)


def _apply_lp_close(event: PositionEvent, ctx: IntentEventContext) -> None:
    """Phase δ — enrich with lp_close_data.

    Reads received amounts and coalesces the parser-variant fee attribute
    names (fees0/fees1 canonical, fees_token0/fees_token1 legacy, fee0/fee1
    older aliases) for both token sides.

    Fix #1710: an lp_open that already wrote amount0/amount1 (Phase γ) is
    never clobbered. If an extracted payload somehow carries BOTH
    lp_open_data and lp_close_data on the same intent — lifecycle-wise
    this shouldn't happen — Phase δ's received amounts are only written
    into slots that Phase γ left empty, and the anomaly is logged. The
    fee fields are independent data (not populated by lp_open) so they
    are written unconditionally.
    """
    lp_close = ctx.extracted.get("lp_close_data")
    if not lp_close:
        return

    # CR #1751 (CodeRabbit): do NOT coerce with `or ""` — an explicit
    # measured zero ("0" / 0) is a legitimate value that must reach
    # persistence. Truthiness coercion would drop it. Use `is not None`
    # instead so only genuinely missing values fall through.
    # Accept both naming conventions: amount0_received (legacy) and
    # amount0_collected (LPCloseData standard used by Uniswap V3, Pendle, etc.)
    amount0_received = getattr(lp_close, "amount0_received", None)
    if amount0_received is None:
        amount0_received = getattr(lp_close, "amount0_collected", None)
    amount1_received = getattr(lp_close, "amount1_received", None)
    if amount1_received is None:
        amount1_received = getattr(lp_close, "amount1_collected", None)

    # Mutual-exclusivity check — log whenever BOTH payloads coexist on the
    # same intent, regardless of whether lp_open already wrote amount0/
    # amount1. CR #1751 round 2 (CodeRabbit): keying this off event.amount0/
    # amount1 hid the collision whenever lp_open_data was present but
    # carried missing / None amounts (payload corruption, parser regression,
    # genuinely zero-deposit edge cases). The collision itself is the
    # operator-visible anomaly; the preservation logic below handles value
    # writes independently.
    lp_open_present = ctx.extracted.get("lp_open_data") is not None
    if lp_open_present:
        logger.warning(
            "Both lp_open_data and lp_close_data present on the same intent "
            "(deployment=%s protocol=%s position_id=%s); preserving existing "
            "amount slots and only filling empty ones. See issue #1710.",
            ctx.deployment_id,
            event.protocol,
            event.position_id,
        )

    if not event.amount0 and amount0_received is not None:
        event.amount0 = str(amount0_received)
    if not event.amount1 and amount1_received is not None:
        event.amount1 = str(amount1_received)
    # Attribute name priority: fees0/fees1 (LPCloseData canonical, e.g. Curve),
    # fees_token0/fees_token1 (legacy), fee0/fee1 (older aliases).
    # ``is not None`` guard preserves measured-zero (fees0=0 is meaningful).
    for fee_attr in ("fees0", "fees_token0", "fee0"):
        fee = getattr(lp_close, fee_attr, None)
        if fee is not None:
            event.fees_token0 = str(fee)
            break
    for fee_attr in ("fees1", "fees_token1", "fee1"):
        fee = getattr(lp_close, fee_attr, None)
        if fee is not None:
            event.fees_token1 = str(fee)
            break


def _apply_swap_fallback(event: PositionEvent, ctx: IntentEventContext) -> None:
    """Phase ε — fill EMPTY token/amount slots from swap_amounts.

    CRITICAL invariant: this helper reads the current event.token0/token1/
    amount0/amount1 values and only writes to slots that are still empty.
    That's what prevents a SWAP leg that co-occurs with an LP_OPEN (e.g.
    single-asset provisioning that swaps half into the other side) from
    clobbering the real LP pair identities with (token_in, token_out).

    This is the reason the phase ordering γ → ε is load-bearing: ε needs
    γ's populated slots to know what to skip.
    """
    swap = ctx.extracted.get("swap_amounts")
    if not swap:
        return

    if not event.token0:
        event.token0 = getattr(swap, "token_in", "") or ""
    if not event.token1:
        event.token1 = getattr(swap, "token_out", "") or ""
    if not event.amount0:
        event.amount0 = str(getattr(swap, "amount_in_decimal", "") or "")
    if not event.amount1:
        event.amount1 = str(getattr(swap, "amount_out_decimal", "") or "")


def _apply_perp(event: PositionEvent, ctx: IntentEventContext) -> None:
    """Phase ζ — enrich with perp_data.

    Copies leverage, entry/mark price, unrealized PnL and direction.
    For CLOSE events, also writes ``funding_fee_usd`` into the event's
    ``attribution_json`` sidecar when available (VIB-3497). This lets
    ``attribute_perp()`` incorporate the funding cost into ``funding_pnl_usd``
    and ``net_pnl_usd`` without needing a new DB column.

    Position-id precedence (fix #1709): a ``perp.position_id`` that
    disagrees with the already-seeded ``event.position_id`` is now logged
    as a WARNING before the perp value is written. Silent override was
    the old (buggy) behaviour — it meant PnL attribution could key off a
    different position than the LP close / accounting write with no
    signal to the operator. The perp extractor still wins on mismatch
    (the parser is typically the most authoritative source for perp NFT
    ids), but the mismatch itself is no longer invisible.
    """
    perp = ctx.extracted.get("perp_data")

    # VIB-3497: ``funding_fee_usd`` arrives as a top-level extracted_data
    # key for PERP_CLOSE (from the ResultEnricher PERP_CLOSE spec), not
    # inside a ``perp_data`` struct. Read it separately so it works even
    # when ``perp_data`` is absent (the common case for GMX V2 where each
    # field is extracted individually, not wrapped in a PerpData object).
    raw_funding = ctx.extracted.get("funding_fee_usd")
    if raw_funding is None and perp is not None:
        raw_funding = getattr(perp, "funding_fee_usd", None)

    # Persist funding_fee_usd in attribution_json sidecar so
    # run_attribution_on_close / attribute_perp can read it without a DB
    # schema change. Only write when a value (including measured zero) is
    # present — None means "unknown" and must not be silently promoted to 0.
    if raw_funding is not None and event.event_type == "CLOSE":
        try:
            existing = json.loads(event.attribution_json or "{}")
            if not isinstance(existing, dict):
                existing = {}
            existing["funding_fee_usd"] = str(raw_funding)
            event.attribution_json = json.dumps(existing)
        except Exception:  # noqa: BLE001
            logger.debug("Failed to stamp funding_fee_usd into attribution_json", exc_info=True)

    if not perp:
        return

    event.leverage = str(getattr(perp, "leverage", "") or "")
    event.entry_price = str(getattr(perp, "entry_price", "") or "")
    event.mark_price = str(getattr(perp, "mark_price", "") or "")
    event.unrealized_pnl = str(getattr(perp, "unrealized_pnl", "") or "")
    event.is_long = getattr(perp, "is_long", None)
    if hasattr(perp, "position_id") and perp.position_id:
        new_pid = str(perp.position_id)
        if event.position_id and event.position_id != new_pid:
            logger.warning(
                "perp.position_id=%s differs from already-set event.position_id=%s "
                "(deployment=%s protocol=%s); perp wins. See issue #1709.",
                new_pid,
                event.position_id,
                ctx.deployment_id,
                event.protocol,
            )
        event.position_id = new_pid


def _apply_collect_fees(event: PositionEvent, ctx: IntentEventContext) -> None:
    """Phase δ-alt — enrich COLLECT_FEES events with fee amounts.

    VIB-3494: LP_COLLECT_FEES intents produce COLLECT_FEES position events.
    Fee amounts are read from ``lp_close_data`` (the same data class used by
    LP_CLOSE — a fee-collect receipt uses the same Collect/Burn events as a
    close). The field priority is:

        fees_token0 / fee0 on lp_close_data  →  event.fees_token0
        fees_token1 / fee1 on lp_close_data  →  event.fees_token1
        amount0_collected / amount0_received →  event.amount0 (total collected)
        amount1_collected / amount1_received →  event.amount1

    For protocols where fee collection is always bundled with the close (no
    standalone collect intent is possible), this phase is still called but
    amount0/amount1 will already be populated by ``_apply_lp_close``, so the
    collect amounts won't double-write. The fee-specific fields (fees_token0/
    fees_token1) are populated unconditionally when present.

    Note: time-weighted fee APY is computed post-hoc by
    ``compute_fee_apy()`` in ``pnl_attributor.py``, which queries all
    COLLECT_FEES events for a position and divides total fees_usd by the
    hold duration and principal.
    """
    if event.event_type != "COLLECT_FEES":
        return

    lp_close = ctx.extracted.get("lp_close_data")
    if not lp_close:
        return

    # Received amounts (principal + fees in a collect-only TX)
    amount0_received = getattr(lp_close, "amount0_received", None)
    if amount0_received is None:
        amount0_received = getattr(lp_close, "amount0_collected", None)
    amount1_received = getattr(lp_close, "amount1_received", None)
    if amount1_received is None:
        amount1_received = getattr(lp_close, "amount1_collected", None)

    if not event.amount0 and amount0_received is not None:
        event.amount0 = str(amount0_received)
    if not event.amount1 and amount1_received is not None:
        event.amount1 = str(amount1_received)

    # Fee-specific fields (may be zero when protocol doesn't separate them).
    # Attribute name priority matches LPCloseData (fees0/fees1), legacy
    # parser names (fees_token0/fees_token1), and older aliases (fee0/fee1).
    # ``is not None`` guard preserves measured-zero (fees0=0 is meaningful).
    for fee_attr in ("fees0", "fees_token0", "fee0"):
        fee = getattr(lp_close, fee_attr, None)
        if fee is not None:
            event.fees_token0 = str(fee)
            break
    for fee_attr in ("fees1", "fees_token1", "fee1"):
        fee = getattr(lp_close, fee_attr, None)
        if fee is not None:
            event.fees_token1 = str(fee)
            break


def _apply_protocol_fees(event: PositionEvent, ctx: IntentEventContext) -> None:
    """Phase η — VIB-3205 protocol fee capture.

    Preserves the empty-vs-zero distinction: a parser that does not emit
    ``protocol_fees`` leaves the field as "" (unknown); a parser that
    measures and reports a zero fee sets it to "0" (measured zero). The
    two are semantically different to downstream PnL attribution.

    VIB-3495: a parser that emits ``ProtocolFees(unavailable_reason=...)``
    signals "I checked but the on-chain data does not carry the fee amount".
    This is distinct from returning ``None`` (parser not implemented).
    Both leave the field as "" (unknown) so attribution emits fee_pnl=None,
    but the explicit ProtocolFees form is testable and self-documenting.
    """
    protocol_fees = ctx.extracted.get("protocol_fees")
    if protocol_fees is None or not hasattr(protocol_fees, "total_usd"):
        return
    # VIB-3495: explicit "known-unknown" — fee exists but receipt data is
    # insufficient to measure it. Leave protocol_fees_usd as "" (unknown).
    if getattr(protocol_fees, "is_unavailable", False):
        return
    total_usd = getattr(protocol_fees, "total_usd", None)
    if total_usd is not None:
        event.protocol_fees_usd = str(total_usd)


def build_position_event_from_intent(
    *,
    deployment_id: str,
    intent: Any,
    result: Any,
    ledger_entry_id: str = "",
    chain: str = "",
    price_oracle: dict | None = None,
    recent_open_events: dict | None = None,
) -> PositionEvent | None:
    """Build a PositionEvent from an intent and execution result.

    Returns None if the intent type doesn't produce position events
    (e.g., SWAP, SUPPLY, BORROW).

    Sequences the phase helpers α → γ → δ → ε → ζ → η → θ. Ordering is
    load-bearing (see module docstring).

    ``price_oracle`` (VIB-3883): mapping ``{SYMBOL: price}`` (Decimal /
    str / float — coerced internally) used to populate
    ``PositionEvent.value_usd`` on LP_OPEN events. Without this,
    ``portfolio_snapshots.deployed_capital_usd`` reads zero even with
    an open LP position because ``portfolio_valuer._enrich_lp_pnl``
    derives ``cost_basis_usd`` from the OPEN event's ``value_usd``
    column. Callers that don't have a price oracle in scope omit it —
    the field stays empty and downstream readers degrade as they
    already do.
    """
    extracted = getattr(result, "extracted_data", {}) if result else {}
    ctx = IntentEventContext(
        intent=intent,
        result=result,
        extracted=extracted or {},
        deployment_id=deployment_id,
        chain=chain,
        ledger_entry_id=ledger_entry_id,
        price_oracle=price_oracle,
    )

    # α + β — dispatch + seed.
    event = _seed_event(ctx)
    if event is None:
        return None

    # Short-circuit: without extracted_data we can't enrich. Only emit the
    # bare event if it already has a joinable position_id.
    if not extracted:
        return event if event.position_id else None

    # γ → δ → δ-alt → ε → ζ → η (ordering load-bearing).
    _apply_lp_open(event, ctx)
    _apply_lp_close(event, ctx)
    _apply_collect_fees(event, ctx)  # VIB-3494: COLLECT_FEES enrichment
    _apply_swap_fallback(event, ctx)
    _apply_perp(event, ctx)
    _apply_protocol_fees(event, ctx)

    # ι — VIB-3883: populate value_usd for LP_OPEN so deployed_capital_usd
    # on portfolio_snapshots reflects the deployed position size. Must run
    # AFTER _apply_lp_open populates amount0/amount1.
    if price_oracle:
        _apply_lp_open_value_usd(event, price_oracle, chain=chain)

    # κ — VIB-3919: LP_CLOSE column symmetry. The CLOSE event's
    # tick_lower/tick_upper/liquidity/in_range come from the matching
    # OPEN event (the bracket is immutable across the position
    # lifecycle); value_usd at CLOSE = sum of received amounts × prices.
    # Pre-fix the CLOSE row landed with all six columns empty even when
    # the OPEN had populated them, breaking dashboard symmetry and the
    # G5 ship gate. The runner threads ``recent_open_events`` (an
    # in-memory cache keyed by ``(position_id, position_type)`` populated
    # on every save_position_event success) so we can hydrate without
    # a state-manager round-trip.
    if event.event_type == "CLOSE" and event.position_type == "LP":
        _apply_lp_close_columns(event, ctx, recent_open_events, price_oracle)

    # θ — final guard: drop events that never acquired a position_id.
    return event if event.position_id else None


def _apply_lp_close_columns(
    event: PositionEvent,
    ctx: IntentEventContext,
    recent_open_events: dict | None,
    price_oracle: dict | None,
) -> None:
    """VIB-3919 — backfill the immutable LP_CLOSE columns from the prior
    OPEN event + close-time pricing.

    Carries forward ``tick_lower``, ``tick_upper``, ``liquidity`` from
    the runner's ``recent_open_events`` cache. Sets ``in_range = False``
    on CLOSE (the position is being burned; "in-range" semantics no
    longer apply in any meaningful way — False > None for ledger
    completeness). Computes ``value_usd`` from received amounts ×
    prices when available.
    """
    pos_id = event.position_id or ""
    if pos_id and recent_open_events:
        cached = recent_open_events.get((pos_id, "LP"))
        if cached is not None:
            # Bracket is immutable; carry it forward verbatim.
            tl = cached.get("tick_lower")
            tu = cached.get("tick_upper")
            liq = cached.get("liquidity")
            if event.tick_lower is None and isinstance(tl, int):
                event.tick_lower = tl
            if event.tick_upper is None and isinstance(tu, int):
                event.tick_upper = tu
            if not event.liquidity and liq is not None:
                event.liquidity = str(liq)
    # in_range is unambiguously False post-close (NFT burned / liquidity
    # withdrawn). The dashboard reads ``in_range=None`` as "unknown" and
    # ``False`` as "out of range". Either is honest; False is more
    # informative for the closed lifecycle stage.
    if event.in_range is None:
        event.in_range = False
    # Compute value_usd from received amounts when prices available.
    if not event.value_usd and price_oracle:
        _apply_lp_close_value_usd(event, price_oracle, chain=ctx.chain)


def _apply_lp_close_value_usd(event: PositionEvent, price_oracle: dict, chain: str = "") -> None:
    """VIB-3919 — value_usd at CLOSE = received amount0 × price0 +
    received amount1 × price1.

    Mirrors ``_apply_lp_open_value_usd`` but reads the CLOSE-time
    received amounts (already populated by ``_apply_lp_close``) instead
    of the OPEN-time deposit amounts. Fails closed: if either price is
    missing or decimals can't be resolved, ``value_usd`` stays "".
    """
    if event.event_type != "CLOSE" or event.position_type != "LP":
        return
    if event.value_usd:
        return
    amount0_str = event.amount0
    amount1_str = event.amount1
    token0 = (event.token0 or "").upper()
    token1 = (event.token1 or "").upper()
    if not (amount0_str and amount1_str and token0 and token1):
        return
    try:
        from decimal import Decimal as _D

        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        ti0 = resolver.resolve(token0, chain=chain)
        ti1 = resolver.resolve(token1, chain=chain)
        if ti0 is None or ti1 is None:
            return
        a0 = _D(str(amount0_str)) / _D(10**ti0.decimals)
        a1 = _D(str(amount1_str)) / _D(10**ti1.decimals)

        # Tolerant price lookup (matches _apply_lp_open_value_usd).
        def _price(sym: str) -> _D | None:
            entry = price_oracle.get(sym) or price_oracle.get(sym.upper())
            if entry is None:
                return None
            if isinstance(entry, dict):
                p = entry.get("price_usd")
                return _D(str(p)) if p is not None else None
            try:
                return _D(str(entry))
            except Exception:  # noqa: BLE001
                return None

        p0, p1 = _price(token0), _price(token1)
        if p0 is None or p1 is None:
            return
        event.value_usd = str(a0 * p0 + a1 * p1)
    except Exception:  # noqa: BLE001 — best-effort enrichment
        logger.debug("LP_CLOSE value_usd compute failed", exc_info=True)


def _apply_lp_open_value_usd(event: PositionEvent, price_oracle: dict, chain: str = "") -> None:
    """Phase ι (VIB-3883) — compute ``value_usd`` for LP_OPEN events.

    Reads ``amount0/1`` + ``token0/1`` off the event, scales the raw
    on-chain integer amounts to human-readable units using the token
    resolver, then multiplies each leg by the corresponding USD price.
    Fails closed (leaves ``value_usd=""``) when either leg is unpriceable
    OR token decimals can't be resolved — matches the fail-closed contract
    used by ``compute_lp_cost_basis``.

    Decimals scaling is critical (the bug pre-fix): ``_apply_lp_open``
    writes ``amount0`` as the raw int from ``LPOpenData.amount0`` (e.g.
    ``891556839636852`` for WETH 18-dec). Multiplying that integer by
    the USD price directly produces ``$2e18`` of nonsense. We scale by
    ``10 ** decimals`` to recover the human-readable amount before
    pricing.

    Only fires for LP_OPEN where amount0/1 are populated and prices
    cover both legs. Other event types are unaffected.
    """
    if event.event_type != "OPEN" or event.position_type != "LP":
        return
    if event.value_usd:
        return  # already set by something upstream — don't overwrite
    amount0_str = event.amount0
    amount1_str = event.amount1
    token0 = (event.token0 or "").upper()
    token1 = (event.token1 or "").upper()
    if not (amount0_str and amount1_str and token0 and token1):
        return

    def _price(sym: str) -> Decimal | None:
        # Tolerant of both nested ({price_usd: ...}) and flat shapes —
        # mirrors the VIB-3885 helper for category handlers.
        raw = price_oracle.get(sym) or price_oracle.get(sym.lower())
        if raw is None:
            return None
        if isinstance(raw, dict):
            raw = raw.get("price_usd") or raw.get("price")
            if raw is None:
                return None
        try:
            d = Decimal(str(raw))
        except (ArithmeticError, ValueError, TypeError):
            return None
        return d if d.is_finite() else None

    p0 = _price(token0)
    p1 = _price(token1)
    if p0 is None or p1 is None:
        return

    # Resolve token decimals to scale raw on-chain integers. Without this
    # ``Decimal("891556839636852") * Decimal("2301.69")`` writes 2e18 —
    # the H2 production bug.
    chain_lc = (chain or "").lower()
    dec0 = _resolve_token_decimals(token0, chain_lc)
    dec1 = _resolve_token_decimals(token1, chain_lc)
    if dec0 is None or dec1 is None:
        # Decimals unknown — fail closed rather than emit a wildly
        # mis-scaled USD. lp_handler.py uses the same fail-closed
        # contract on the cost_basis_usd path.
        return

    try:
        a0_human = _scale_to_human(amount0_str, dec0)
        a1_human = _scale_to_human(amount1_str, dec1)
    except (ArithmeticError, ValueError):
        return
    if a0_human is None or a1_human is None:
        return

    total = a0_human * p0 + a1_human * p1
    if total.is_finite() and total > Decimal("0"):
        event.value_usd = str(total)


def _resolve_token_decimals(symbol: str, chain: str) -> int | None:
    """Best-effort token-decimals lookup; returns None on any failure.

    Returns ``None`` (not a default like 18) so the caller can fail-closed
    on unknown tokens rather than silently emit a 1e12-off USD value.
    """
    if not symbol or not chain:
        return None
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        info = resolver.resolve(symbol, chain=chain)
        return info.decimals if info is not None else None
    except Exception:
        return None


def _scale_to_human(raw_str: str, decimals: int) -> Decimal | None:
    """Convert a raw on-chain integer string to a human-readable Decimal.

    Tolerant of an already-human input (e.g. ``"0.000891"``): if the
    string parses to a Decimal that's already non-integer, we return it
    unchanged. Pure integers get divided by ``10 ** decimals``.
    """
    try:
        d = Decimal(str(raw_str))
    except (ArithmeticError, ValueError, TypeError):
        return None
    if not d.is_finite():
        return None
    if d == d.to_integral_value():
        # Pure integer → assume raw on-chain units; scale down.
        scale = Decimal(10) ** decimals
        return d / scale
    # Already a fractional Decimal → assume human-readable, return as-is.
    return d
