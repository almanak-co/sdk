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
    θ                         final guard: no position_id → drop the event.

Constraint (critical): γ → δ → ε → ζ → η ordering. Re-ordering ε before
γ silently regresses the invariant called out above.
"""

import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
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
    the static wiring fields (deployment_id, chain, ledger_entry_id) so each
    ``_apply_*`` helper has one parameter instead of six.
    """

    intent: Any
    result: Any
    extracted: dict[str, Any]
    deployment_id: str
    chain: str
    ledger_entry_id: str


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
    tx_hash = ""
    gas_usd = ""
    if result:
        if hasattr(result, "transaction_results") and result.transaction_results:
            tx_hash = result.transaction_results[0].tx_hash or ""
        gas_cost = getattr(result, "gas_cost_usd", None)
        if gas_cost is not None:
            gas_usd = str(gas_cost)

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
    # Token addresses: LPOpenData doesn't carry them directly, so fall
    # back to the intent's from_token / to_token (LP intents expose these
    # as the two sides of the pair) before the swap-based fallback below.
    intent = ctx.intent
    t0 = getattr(intent, "token0", None) or getattr(intent, "from_token", None)
    t1 = getattr(intent, "token1", None) or getattr(intent, "to_token", None)
    if t0:
        event.token0 = str(t0)
    if t1:
        event.token1 = str(t1)


def _apply_lp_close(event: PositionEvent, ctx: IntentEventContext) -> None:
    """Phase δ — enrich with lp_close_data.

    Reads received amounts and coalesces the parser-variant fee attribute
    names (fees_token0 preferred, fee0 fallback) for both token sides.

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
    for fee_attr in ("fees_token0", "fee0"):
        fee = getattr(lp_close, fee_attr, None)
        if fee is not None:
            event.fees_token0 = str(fee)
            break
    for fee_attr in ("fees_token1", "fee1"):
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


def _apply_protocol_fees(event: PositionEvent, ctx: IntentEventContext) -> None:
    """Phase η — VIB-3205 protocol fee capture.

    Preserves the empty-vs-zero distinction: a parser that does not emit
    ``protocol_fees`` leaves the field as "" (unknown); a parser that
    measures and reports a zero fee sets it to "0" (measured zero). The
    two are semantically different to downstream PnL attribution.
    """
    protocol_fees = ctx.extracted.get("protocol_fees")
    if protocol_fees is None or not hasattr(protocol_fees, "total_usd"):
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
) -> PositionEvent | None:
    """Build a PositionEvent from an intent and execution result.

    Returns None if the intent type doesn't produce position events
    (e.g., SWAP, SUPPLY, BORROW).

    Sequences the phase helpers α → γ → δ → ε → ζ → η → θ. Ordering is
    load-bearing (see module docstring).
    """
    extracted = getattr(result, "extracted_data", {}) if result else {}
    ctx = IntentEventContext(
        intent=intent,
        result=result,
        extracted=extracted or {},
        deployment_id=deployment_id,
        chain=chain,
        ledger_entry_id=ledger_entry_id,
    )

    # α + β — dispatch + seed.
    event = _seed_event(ctx)
    if event is None:
        return None

    # Short-circuit: without extracted_data we can't enrich. Only emit the
    # bare event if it already has a joinable position_id.
    if not extracted:
        return event if event.position_id else None

    # γ → δ → ε → ζ → η (ordering load-bearing).
    _apply_lp_open(event, ctx)
    _apply_lp_close(event, ctx)
    _apply_swap_fallback(event, ctx)
    _apply_perp(event, ctx)
    _apply_protocol_fees(event, ctx)

    # θ — final guard: drop events that never acquired a position_id.
    return event if event.position_id else None
