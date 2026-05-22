"""Position lifecycle events for LP, perps, and lending.

Immutable-ID positions (LP NFTs, perp positions) have a lifecycle:
OPEN -> SNAPSHOT* -> CLOSE.  Each state change is recorded as a
PositionEvent with raw observables (amounts, prices, fees).

VIB-4085 — fungible lending positions (Aave V3 supply/borrow, Morpho
markets) also produce PositionEvents with lifecycle states OPEN /
INCREASE / DECREASE / CLOSE keyed on a non-NFT
``position_id = "lending:<chain>:<protocol>:<wallet>:<asset>"``. The
runner's ``_recent_open_events`` cache decides OPEN vs INCREASE on a
new SUPPLY/BORROW; the ledger row's ``post_state`` decides DECREASE vs
CLOSE on a REPAY/WITHDRAW (collateral or debt value <= dust threshold
=> CLOSE). This mirrors the data already captured in Layer 5
``accounting_events`` so the dashboard can render the lifecycle without
re-deriving it.

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
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from almanak.framework.primitives.taxonomy import (  # noqa: F401 — taxonomy delegation lock
    UnknownIntentTypeError,
    record_for,
)

logger = logging.getLogger(__name__)


# Lazy import to avoid circular-dependency issues at module load time.
# _check_decimal_unit_soft_fail is called only inside build_position_event_from_intent.
def _decimal_unit_soft_fail(event: "PositionEvent") -> None:  # noqa: F821
    """Run the W1-5 decimal-unit soft-fail guard over the LP fee fields."""
    from almanak.framework.accounting.decimal_guards import _check_decimal_unit_soft_fail

    payload = {
        "fees_token0": event.fees_token0,
        "fees_token1": event.fees_token1,
    }
    _check_decimal_unit_soft_fail(
        payload,
        event_id=event.id,
        event_type=event.event_type,
    )


class PositionEventType(StrEnum):
    """Types of position lifecycle events."""

    OPEN = "OPEN"
    CLOSE = "CLOSE"
    COLLECT_FEES = "COLLECT_FEES"
    SNAPSHOT = "SNAPSHOT"
    # VIB-4085 — lending lifecycle is non-monotonic (a loop adds collateral
    # and debt repeatedly before unwinding), so OPEN/CLOSE alone don't tell
    # the dashboard whether the leg is being grown or shrunk. INCREASE /
    # DECREASE record additive / subtractive actions on an already-open leg.
    INCREASE = "INCREASE"
    DECREASE = "DECREASE"


class PositionType(StrEnum):
    """Types of tracked positions."""

    LP = "LP"
    PERP = "PERP"
    # VIB-4085 — fungible lending legs. Both share the same FIFO-keyed
    # ``position_id`` shape (`lending:<chain>:<protocol>:<wallet>:<asset>`)
    # but are tracked as separate position types so the dashboard can
    # render the collateral leg and the debt leg side-by-side without
    # joining on intent_type.
    LENDING_COLLATERAL = "LENDING_COLLATERAL"
    LENDING_DEBT = "LENDING_DEBT"


# Intent types that map to position events.
# VIB-4085 — lending intents (SUPPLY/BORROW/REPAY/WITHDRAW) now produce
# events as well; the static dispatch below maps them to OPEN / CLOSE
# defaults that ``_apply_lending`` refines into INCREASE / DECREASE
# based on lifecycle state read from the ledger row's ``post_state``.
INTENT_TO_EVENT_TYPE: dict[str, PositionEventType] = {
    "LP_OPEN": PositionEventType.OPEN,
    "LP_CLOSE": PositionEventType.CLOSE,
    "LP_COLLECT_FEES": PositionEventType.COLLECT_FEES,
    "PERP_OPEN": PositionEventType.OPEN,
    "PERP_CLOSE": PositionEventType.CLOSE,
    # Lending — defaults; ``_apply_lending`` refines based on lifecycle.
    "SUPPLY": PositionEventType.OPEN,  # → INCREASE on cache hit
    "BORROW": PositionEventType.OPEN,  # → INCREASE on cache hit
    "REPAY": PositionEventType.CLOSE,  # → DECREASE when debt_value_after > dust
    "WITHDRAW": PositionEventType.CLOSE,  # → DECREASE when collateral_value_after > dust
    "DELEVERAGE": PositionEventType.CLOSE,  # mirrors REPAY refinement
}

# VIB-4162 (T2): the legacy ``INTENT_TO_POSITION_TYPE`` dict is gone.
# Position-type resolution delegates to :func:`_resolve_position_type`,
# which is a strict wrapper around
# :func:`almanak.framework.primitives.taxonomy.record_for`. The previous
# implementation silently fell back to ``PositionType.LP`` on an unknown
# intent string — the canonical class-of-bug T2 exists to fix.


def _resolve_position_type(intent_type: str) -> PositionType:
    """Strict lookup — raises ``UnknownIntentTypeError`` if no taxonomy row.

    Used by :func:`_seed_event` AFTER ``INTENT_TO_EVENT_TYPE.get`` has
    confirmed the intent is position-producing, so a missing taxonomy row
    is a genuine inconsistency that must surface.
    """
    record = record_for(intent_type)  # raises UnknownIntentTypeError on miss
    pk = record.position_type
    if pk is None:
        raise UnknownIntentTypeError(intent_type)
    return PositionType(pk.value)


# VIB-4085 — dust threshold for lending CLOSE detection. A leg with
# remaining value <= this threshold is treated as fully closed. Aave V3
# accrues sub-cent residuals from interest indices; treating exact-zero
# as the only close signal would fragment the lifecycle.
LENDING_CLOSE_DUST_USD = "0.01"


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
    # VIB-4085 — lending lifecycle decisions read post-state (collateral,
    # debt, HF, LTV, APR) to refine OPEN→INCREASE / CLOSE→DECREASE. The
    # runner computes ``post_state`` already (it's persisted to
    # ``transaction_ledger.post_state_json``); threading it through the
    # context lets the position_event seeder reuse the same data without
    # round-tripping back through the gateway.
    post_state: dict[str, Any] | None = None
    # VIB-4493 — pre_state is needed for CLOSE event value_usd: post-state
    # leg value is 0 by definition when refined to CLOSE (that's WHY it's
    # CLOSE), so stamping post-state would write ``0E-8`` and the dashboard
    # cannot tell "how much was closed". pre_state's leg value IS the
    # closed amount. Same dict shape / resolver as ``post_state``.
    pre_state: dict[str, Any] | None = None
    # VIB-4085 — wallet address scopes the lending position_id so two
    # strategies on different wallets don't collide on the same chain +
    # protocol + asset.
    wallet_address: str = ""
    # VIB-4085 — the runner's in-memory recent-open cache (populated by
    # ``_update_recent_open_events_cache`` on every successful save) is
    # the authority on whether a SUPPLY/BORROW is the FIRST action on a
    # position (→ OPEN) or a subsequent action (→ INCREASE). Pre-fix
    # there was no signal at all and lending events weren't emitted.
    recent_open_events: dict | None = None


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

    # VIB-4162: strict resolution — raises UnknownIntentTypeError if the
    # intent passed the INTENT_TO_EVENT_TYPE gate but the taxonomy has
    # no record. The pre-T2 silent-LP fallback at this site is the
    # canonical class-of-bug T2 fixes (see module commit history).
    position_type = _resolve_position_type(intent_type)
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


# ──────────────────────────────────────────────────────────────────────────
# VIB-4085 — lending lifecycle helpers
# ──────────────────────────────────────────────────────────────────────────


def lending_position_id(*, chain: str, protocol: str, wallet: str, asset: str) -> str:
    """Canonical lending position_id shape — must match
    ``LendingAccountingEvent.position_key`` so Layer 3 (position_events)
    and Layer 5 (accounting_events) are joinable on a single column.

    All segments are lower-cased; an empty wallet (e.g. dry_run with no
    signer) becomes ``unknown`` rather than producing a malformed key
    like ``lending:arbitrum:aave_v3::usdc``.
    """
    chain_n = (chain or "unknown").strip().lower() or "unknown"
    proto_n = (protocol or "unknown").strip().lower() or "unknown"
    wallet_n = (wallet or "unknown").strip().lower() or "unknown"
    asset_n = (asset or "unknown").strip().lower() or "unknown"
    return f"lending:{chain_n}:{proto_n}:{wallet_n}:{asset_n}"


def _lending_amount(intent: Any, extracted: dict[str, Any], intent_type: str) -> str:
    """Extract the principal token amount for a lending intent.

    Tries the receipt-parser-extracted field first (authoritative —
    reflects what actually moved on-chain), falling back to the intent's
    declared amount. Returns "" when neither is available; downstream
    readers treat "" as unknown distinct from "0" (measured zero).
    """
    field_map = {
        "SUPPLY": ("supply_amount",),
        "BORROW": ("borrow_amount",),
        "REPAY": ("repay_amount", "repaid_amount"),
        "WITHDRAW": ("withdraw_amount", "withdrawn_amount"),
        "DELEVERAGE": ("repay_amount", "repaid_amount"),
    }
    for key in field_map.get(intent_type, ()):
        v = extracted.get(key)
        # SupplyAmounts / BorrowAmounts dataclasses expose ``.amount`` or are
        # the raw int themselves; tolerate both.
        if v is not None:
            inner = getattr(v, "amount", None)
            if inner is not None:
                return str(inner)
            if isinstance(v, int | str):
                return str(v)
    declared = getattr(intent, "amount", None)
    return str(declared) if declared is not None else ""


_LENDING_FLAT_KEYS = (
    "collateral_value_usd",
    "debt_value_usd",
    "collateral_usd",
    "debt_usd",
)


def _resolve_lending_post_state(post_state: dict | None) -> dict[str, Any]:
    """Some capture pipelines wrap lending post-state under a protocol
    key (``post_state["aave_v3"]["collateral_value_usd"]``); others write
    the fields flat. Return a dict normalised to the canonical keys
    (``collateral_value_usd``, ``debt_value_usd``, ``liquidation_threshold``),
    falling back to ``{}`` so callers can use ``.get`` unconditionally.

    Connectors emit either canonical names (``collateral_value_usd`` etc.)
    or compact aliases (``collateral_usd``, ``debt_usd``,
    ``liquidation_threshold_bps``). The aliases are preserved verbatim
    on the returned dict alongside the canonical keys so the projection
    is non-destructive and round-trippable.
    """
    if not isinstance(post_state, dict):
        return {}

    # Start from the root-level fields; promoting nested protocol-keyed values
    # must NOT drop sibling root keys like ``health_factor`` / APR / liquidation
    # metadata that the connector may emit at the outer scope.
    out: dict[str, Any] = dict(post_state)
    if not any(k in post_state for k in _LENDING_FLAT_KEYS):
        for v in post_state.values():
            if isinstance(v, dict) and any(k in v for k in _LENDING_FLAT_KEYS):
                # Merge nested into root, preferring nested for overlapping keys
                # (the wrapping protocol dict is the more specific source).
                for k, val in v.items():
                    out.setdefault(k, val)
                # Promote nested overrides for the canonical lending keys we
                # branch on below — root-level proxies (if any) should not
                # win over the protocol-scoped value.
                for k in _LENDING_FLAT_KEYS:
                    if k in v:
                        out[k] = v[k]
                break

    if "collateral_value_usd" not in out and "collateral_usd" in out:
        out["collateral_value_usd"] = out["collateral_usd"]
    if "debt_value_usd" not in out and "debt_usd" in out:
        out["debt_value_usd"] = out["debt_usd"]
    if "liquidation_threshold" not in out and "liquidation_threshold_bps" in out:
        bps = out["liquidation_threshold_bps"]
        try:
            out["liquidation_threshold"] = str(Decimal(str(bps)) / Decimal(10000))
        except (InvalidOperation, ValueError, TypeError):
            pass
    return out


def _refine_lending_event_type(
    event: PositionEvent,
    intent_type: str,
    leg_value: Any,
    cache: dict,
) -> None:
    """OPEN→INCREASE / CLOSE→DECREASE refinement keyed on cache + leg_value."""
    if intent_type in ("SUPPLY", "BORROW"):
        cache_key = (event.position_id, str(event.position_type))
        if cache_key in cache:
            event.event_type = PositionEventType.INCREASE.value
        return
    if intent_type not in ("REPAY", "WITHDRAW", "DELEVERAGE"):
        return
    if leg_value is None:
        event.event_type = PositionEventType.DECREASE.value
        logger.debug(
            "lending lifecycle: post-state missing for %s on %s; "
            "defaulting to DECREASE (would have been CLOSE if leg_value <= dust)",
            intent_type,
            event.position_id,
        )
        return
    # NaN/Infinity round-trip cleanly through Decimal(str(...)) but break the
    # ``<= dust`` comparison: NaN raises InvalidOperation, +/-Infinity returns
    # False. Either misroute would silently misclassify the lifecycle event,
    # so reject non-finite values the same way we handle a missing leg_value.
    try:
        value_d = Decimal(str(leg_value))
        if not value_d.is_finite():
            raise InvalidOperation(f"non-finite leg_value: {leg_value!r}")
    except (InvalidOperation, ValueError, TypeError) as exc:
        event.event_type = PositionEventType.DECREASE.value
        logger.debug(
            "lending lifecycle: unparseable leg_value=%r for %s on %s (%s); "
            "defaulting to DECREASE (would have been CLOSE if value <= dust)",
            leg_value,
            intent_type,
            event.position_id,
            exc,
        )
        return
    dust = Decimal(LENDING_CLOSE_DUST_USD)
    event.event_type = PositionEventType.CLOSE.value if value_d <= dust else PositionEventType.DECREASE.value


def _build_lending_attribution(event: PositionEvent, post: dict, asset: str, intent_type: str) -> None:
    """v1 lending attribution. Fully derivable from the ledger row's
    post_state — no FIFO replay required. Schema-version-stamped so a
    future v2 producer (e.g. a dedicated lending PnL attributor that
    splits principal vs interest the way the FIFO basis store does for
    swaps) is distinguishable from this seed-time payload."""
    if not post:
        return
    attribution = {
        "version": 1,
        "schema": "lending_v1",
        "position_type": str(event.position_type),
        "collateral_value_after_usd": _stringify_or_none(post.get("collateral_value_usd")),
        "debt_value_after_usd": _stringify_or_none(post.get("debt_value_usd")),
        "health_factor_after": _stringify_or_none(post.get("health_factor")),
        "liquidation_threshold": _stringify_or_none(post.get("liquidation_threshold")),
        "supply_apr_bps": post.get("supply_apr_bps"),
        "borrow_apr_bps": post.get("borrow_apr_bps"),
        "asset": asset or None,
        "intent_type": intent_type or None,
    }
    try:
        event.attribution_json = json.dumps(attribution, default=str)
    except (TypeError, ValueError):  # noqa: BLE001 — defensive; payload is small + flat
        logger.warning(
            "Failed to serialise lending attribution for %s; leaving attribution_json empty",
            event.position_id,
        )


def _apply_lending(event: PositionEvent, ctx: IntentEventContext) -> None:
    """Phase φ — lending lifecycle enrichment (VIB-4085).

    Refines the static OPEN/CLOSE event_type from
    ``INTENT_TO_EVENT_TYPE`` into one of OPEN / INCREASE / DECREASE /
    CLOSE based on:

    * For SUPPLY / BORROW: the runner's ``recent_open_events`` cache.
      Cache hit on ``(position_id, position_type)`` → INCREASE; miss →
      OPEN. Across process restarts a SUPPLY may incorrectly emit a
      second OPEN, but the dashboard reads max-timestamp-OPEN-without-
      a-following-CLOSE so lifecycle still resolves correctly.
    * For REPAY / WITHDRAW: the leg's own post-state value
      (``collateral_value_usd`` for LENDING_COLLATERAL,
      ``debt_value_usd`` for LENDING_DEBT). Below
      ``LENDING_CLOSE_DUST_USD`` ⇒ CLOSE; above ⇒ DECREASE.

    Populates ``position_id`` (canonical join key with Layer 5
    ``accounting_events.position_key``), ``token0`` = asset symbol,
    ``amount0`` = principal in token-smallest-unit, ``value_usd`` =
    post-state value of THIS leg, and ``attribution_json`` (lending v1)
    when post_state is present. No-op for non-lending events.
    """
    if event.position_type not in (PositionType.LENDING_COLLATERAL, PositionType.LENDING_DEBT):
        return

    intent = ctx.intent
    intent_type_raw = ""
    if hasattr(intent, "intent_type"):
        it = intent.intent_type
        intent_type_raw = it.value if hasattr(it, "value") else str(it)
    intent_type = (intent_type_raw or "").upper()

    # Resolution order is position-type-aware because lending intents have
    # asymmetric field names: BorrowIntent / RepayIntent identify the debt
    # leg via ``borrow_token``; SupplyIntent / WithdrawIntent identify the
    # collateral leg via ``token``. A naive single-field resolver would
    # populate LENDING_DEBT with the (collateral) ``token`` if the intent
    # carried both — semantically wrong for the debt-leg event.
    if event.position_type == PositionType.LENDING_DEBT:
        asset = (
            getattr(intent, "borrow_token", None)
            or getattr(intent, "amount_token", None)
            or getattr(intent, "token", None)
            or getattr(intent, "asset", None)
            or ""
        )
    else:
        asset = (
            getattr(intent, "amount_token", None)
            or getattr(intent, "token", None)
            or getattr(intent, "collateral_token", None)
            or getattr(intent, "token_in", None)
            or getattr(intent, "asset", None)
            or ""
        )
    asset = str(asset or "").upper()

    event.position_id = lending_position_id(
        chain=ctx.chain,
        protocol=event.protocol or getattr(intent, "protocol", "") or "",
        wallet=ctx.wallet_address,
        asset=asset,
    )
    if asset and not event.token0:
        event.token0 = asset

    amount = _lending_amount(intent, ctx.extracted, intent_type)
    if amount and not event.amount0:
        event.amount0 = amount

    post = _resolve_lending_post_state(ctx.post_state)
    leg_value = (
        post.get("collateral_value_usd")
        if event.position_type == PositionType.LENDING_COLLATERAL
        else post.get("debt_value_usd")
    )
    if leg_value is not None and not event.value_usd:
        event.value_usd = str(leg_value)

    _refine_lending_event_type(event, intent_type, leg_value, ctx.recent_open_events or {})

    action_delta = _compute_lending_action_delta(
        pre_state=ctx.pre_state,
        leg_value=leg_value,
        position_type=event.position_type,
    )
    if action_delta is not None:
        event.value_usd = action_delta

    _build_lending_attribution(event, post, asset, intent_type)


def _compute_lending_action_delta(
    *,
    pre_state: dict | None,
    leg_value: Any,
    position_type: str,
) -> str | None:
    """Return ``abs(pre - post)`` as a decimal string, or None to keep the
    post-state stamp.

    VIB-4493 / VIB-4529 — every lending event should stamp the **action
    delta** into ``value_usd``, not the post-state remaining balance.
    Post-state semantics break in three ways for an operator scanning
    the Position Lifecycle table:

      * CLOSE    — post is 0 by definition (that's WHY it's CLOSE), so
                   every row reads ``0E-8`` and you can't tell the close
                   size at a glance.
      * DECREASE — post is the remaining balance after the partial
                   WITHDRAW, NOT the amount withdrawn. Reader has to diff
                   against the prior row to find the action size.
      * OPEN / INCREASE — post conflates the pre-existing balance with
                   this action's contribution. Surfaces as inflated
                   opening values when the wallet already had a position
                   from a previous run on the same shared Anvil fork.

    LP_OPEN / LP_CLOSE already write action-related values; this aligns
    lending with that contract. The unified ``abs(pre - post)`` formula
    collapses to ``pre`` for CLOSE (post=0) and to ``post - pre`` /
    ``pre - post`` for INCREASE / DECREASE / OPEN — i.e. the size of
    what the action moved on-chain.

    Opt-in via ``pre_state``: callers that don't pass it (legacy paper
    / dry-run, third-party harnesses, fixtures) get ``None`` back and
    keep the pre-fix post-state semantics. Tests pin both paths.
    """
    if pre_state is None or leg_value is None:
        return None
    pre = _resolve_lending_post_state(pre_state)
    pre_leg_value = (
        pre.get("collateral_value_usd")
        if position_type == PositionType.LENDING_COLLATERAL
        else pre.get("debt_value_usd")
    )
    if pre_leg_value is None:
        return None
    try:
        delta = abs(Decimal(str(pre_leg_value)) - Decimal(str(leg_value)))
    except (InvalidOperation, ValueError, TypeError):
        # Non-finite or unparseable pre/post → keep post-state stamp.
        return None
    if not delta.is_finite():
        return None
    return str(delta)


def _stringify_or_none(v: Any) -> str | None:
    """Coerce numerics / Decimals to strings for stable JSON; pass None
    through unchanged. ``""`` becomes None — empty string in lending
    payloads means "unmeasured" and should not survive into JSON as a
    string that downstream readers can't distinguish from "0"."""
    if v is None:
        return None
    if isinstance(v, str) and v == "":
        return None
    return str(v)


def build_position_event_from_intent(
    *,
    deployment_id: str,
    intent: Any,
    result: Any,
    ledger_entry_id: str = "",
    chain: str = "",
    price_oracle: dict | None = None,
    recent_open_events: dict | None = None,
    post_state: dict | None = None,
    pre_state: dict | None = None,
    wallet_address: str = "",
) -> PositionEvent | None:
    """Build a PositionEvent from an intent and execution result.

    Returns None if the intent type doesn't produce position events
    (e.g., SWAP, ENSURE_BALANCE, BRIDGE).

    Sequences the phase helpers α → γ → δ → ε → ζ → η → φ → θ. Ordering
    is load-bearing (see module docstring).

    ``price_oracle`` (VIB-3883): mapping ``{SYMBOL: price}`` (Decimal /
    str / float — coerced internally) used to populate
    ``PositionEvent.value_usd`` on LP_OPEN events. Without this,
    ``portfolio_snapshots.deployed_capital_usd`` reads zero even with
    an open LP position because ``portfolio_valuer._enrich_lp_pnl``
    derives ``cost_basis_usd`` from the OPEN event's ``value_usd``
    column. Callers that don't have a price oracle in scope omit it —
    the field stays empty and downstream readers degrade as they
    already do.

    ``post_state`` / ``wallet_address`` (VIB-4085): drives lending
    lifecycle refinement. ``post_state`` is the dict the runner
    serialises into ``transaction_ledger.post_state_json``; passing it
    in lets ``_apply_lending`` decide CLOSE vs DECREASE without a
    state-manager round-trip. ``wallet_address`` scopes the lending
    ``position_id`` so two strategies on different wallets don't
    collide on the same chain + protocol + asset.
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
        post_state=post_state,
        pre_state=pre_state,
        wallet_address=wallet_address,
        recent_open_events=recent_open_events,
    )

    # α + β — dispatch + seed.
    event = _seed_event(ctx)
    if event is None:
        return None

    # Short-circuit: without extracted_data AND no post_state we can't
    # enrich. Lending events specifically need post_state, not extracted,
    # so don't short-circuit purely on missing extracted_data when this
    # is a lending intent.
    is_lending = event.position_type in (PositionType.LENDING_COLLATERAL, PositionType.LENDING_DEBT)
    if not extracted and not is_lending:
        return event if event.position_id else None

    # γ → δ → δ-alt → ε → ζ → η → φ (ordering load-bearing).
    _apply_lp_open(event, ctx)
    _apply_lp_close(event, ctx)
    _apply_collect_fees(event, ctx)  # VIB-3494: COLLECT_FEES enrichment
    _apply_swap_fallback(event, ctx)
    _apply_perp(event, ctx)
    _apply_protocol_fees(event, ctx)
    _apply_lending(event, ctx)  # VIB-4085: lending lifecycle refinement

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
    if not event.position_id:
        return None

    # ν — W1-5 decimal-unit soft-fail guard (VIB-4780).  Runs after all
    # enrichment phases so fees_token0/1 are fully populated.  Soft-fail
    # only: logs a WARNING, never raises.
    _decimal_unit_soft_fail(event)

    return event


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
            # VIB-4086 — pair tokens are also immutable across the
            # position lifecycle. Carry them forward so
            # ``_apply_lp_close_value_usd`` below can resolve decimals
            # and look up close-time prices, and so the CLOSE row's
            # token columns are populated for dashboard / Accountant Test
            # reads. Pre-fix the close row landed with token0='' /
            # token1='' even though the OPEN had them.
            t0 = cached.get("token0")
            t1 = cached.get("token1")
            if not event.token0 and t0:
                event.token0 = str(t0)
            if not event.token1 and t1:
                event.token1 = str(t1)
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
