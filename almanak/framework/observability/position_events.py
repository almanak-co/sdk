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


# VIB-5036: the W1-5 decimal-unit soft-fail guard is intentionally NOT wired
# over position_events.  Its sibling on ``transaction_ledger``
# (``build_ledger_entry``) guards a HUMAN-units column and remains in place.
# But ``position_events`` ``amount0`` / ``amount1`` / ``fees_token0`` /
# ``fees_token1`` are RAW-by-contract (smallest unit): NAV valuation
# (``portfolio_valuer`` ``amount0_wei``), post-restart hydration
# (``_run_loop_helpers`` "amount0/amount1 (wei)"), and the attribution lane
# (``pnl_attributor`` "raw token-denominated") all read them as raw and scale
# at point-of-use.  Running the human-form guard here therefore produced a
# guaranteed FALSE WARNING on every LP fee write (the original field report on
# deployment a9e54a85), eroding the guard's signal.  The fix for that report
# is the writer-side ``transaction_ledger`` normalization (LP_OPEN amount_in)
# plus scaling the raw columns at their genuine consumers (IL, display) — not
# flagging the raw-by-contract columns as suspect.


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
    # Pendle PT (VIB-52xx) — kept in sync with ``primitives.PositionKind`` so
    # the taxonomy can declare ``position_type=PENDLE_PT`` on the PT rows. A PT
    # buy seeds OPEN, a sell/redeem seeds CLOSE on the same ``position_id``.
    PENDLE_PT = "PENDLE_PT"


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


def _redeem_pt_symbol_from_legs(extracted: Any) -> str:
    """Return the canonical PT symbol from a redeem's DECLARED INPUT money leg.

    A PT redeem (WITHDRAW) carries the canonical maturity-bearing PT symbol on
    neither ``intent.from_token`` nor ``intent.to_token`` — the compiler names
    the underlying out token and the YT address, not the PT. The Pendle parser's
    ``extract_primitive_money_legs`` (G-PT / VIB-4988 part 2) instead declares the
    redeem's INPUT leg as the PT symbol, surfaced on
    ``extracted_data["primitive_money_legs"]``. This reads that INPUT leg's token
    so the CLOSE position_id is keyed on the SAME maturity-bearing PT symbol the
    accounting ``_pt_context`` reads off the ledger ``token_in`` column — keeping
    OPEN(buy) and CLOSE(redeem) on one byte-identical key.

    Returns ``""`` (never a fabricated symbol; Empty != Zero) when no declared
    legs are present, the value is not a ``PrimitiveMoneyLegs``, or no INPUT leg
    carries a ``PT-`` token — the caller then treats the withdraw as non-PT.
    """
    if not isinstance(extracted, dict):
        return ""
    legs = extracted.get("primitive_money_legs")
    if legs is None:
        return ""
    # Deferred import: connector value types must never load at module import
    # (framework → connector boundary; mirrors the ledger dispatcher's resolver).
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

    if not isinstance(legs, PrimitiveMoneyLegs):
        return ""
    for leg in legs.input_legs:
        token = (leg.token or "").strip()
        if token.upper().startswith("PT-"):
            return token
    return ""


def _pendle_pt_event(
    intent: Any,
    intent_type: str,
    chain: str,
    wallet: str,
    *,
    redeem_pt_symbol: str = "",
) -> tuple[PositionEventType, str] | None:
    """Resolve a Pendle PT lifecycle action to ``(event_type, position_id)``.

    Pendle PT trades do not flow through the static ``INTENT_TO_EVENT_TYPE``
    map: a PT buy/sell arrives as ``SWAP`` (absent from the map → no event) and
    a redeem arrives as ``WITHDRAW`` (present, but would mis-resolve to a
    *lending* leg via ``_resolve_position_type``). This helper intercepts the
    Pendle-PT shape so it seeds a ``PENDLE_PT`` OPEN→CLOSE lifecycle on a
    market-derived ``position_id`` that is byte-identical to the accounting
    treatment's ``pendle_pt:<chain>:<wallet>:<market>`` key
    (``connectors/pendle/accounting_spec.py:_position_key``) — so OPEN (buy) and
    CLOSE (sell/redeem) collapse onto one renderable position.

    A redeem's PT symbol comes from the connector-DECLARED INPUT money leg
    (``redeem_pt_symbol``, G-PT / VIB-4988 part 2), NOT from
    ``intent.from_token`` / ``to_token`` — the redeem intent carries the
    underlying out token and the YT address on those, never the PT. When no PT
    symbol is resolvable for a WITHDRAW (a non-PT Pendle withdraw, e.g. a YT
    redeem), this returns ``None`` so the generic path runs.

    Returns ``None`` for every non-Pendle-PT intent — including non-PT Pendle
    swaps (YT / SY ↔ underlying) — so the generic path is unchanged.
    """
    protocol = (getattr(intent, "protocol", "") or "").lower()
    # tech-debt VIB-5292: this framework-side protocol-name gate is the position-
    # events analogue of the accounting AccountingTreatmentRegistry de-coupling —
    # tracked for migration to a connector-owned position-event classifier
    # (blueprint 22). Baselined in the chain/protocol coupling ratchet meanwhile.
    if "pendle" not in protocol:
        return None
    from_token = (getattr(intent, "from_token", "") or "").strip()
    to_token = (getattr(intent, "to_token", "") or "").strip()
    from_u = from_token.upper()
    to_u = to_token.upper()
    if intent_type == "SWAP":
        if to_u.startswith("PT-"):
            event_type = PositionEventType.OPEN  # buy PT
            pt_symbol = to_token
        elif from_u.startswith("PT-"):
            event_type = PositionEventType.CLOSE  # sell PT
            pt_symbol = from_token
        else:
            return None  # YT / SY swap — not a PT position action
    elif intent_type == "WITHDRAW":
        # A PT redeem's symbol lives on the DECLARED INPUT leg (the redeem intent
        # itself carries the underlying out token / YT address, never the PT). No
        # resolvable PT symbol → a non-PT Pendle withdraw (YT redeem) → generic
        # path. This guards the prior bug where a non-PT withdraw produced a bogus
        # ``pendle_pt:...:<underlying>`` id, and the bug where a real redeem (no
        # from/to PT token) produced an empty position_id and seeded no CLOSE.
        pt_symbol = redeem_pt_symbol.strip()
        if not pt_symbol.upper().startswith("PT-"):
            return None
        event_type = PositionEventType.CLOSE  # redeem at/after maturity
    else:
        return None
    # Identity is the normalized maturity-bearing PT symbol (e.g.
    # "pt-wsteth-25jun2026") — the ONLY identifier present in BOTH the intent
    # (here) and the persisted ledger row (the accounting treatment's
    # ``_pt_context`` reads ``token_in``/``token_out``). A Pendle ``SwapIntent``
    # carries no pool/market, and the resolved market address is never persisted
    # on the ledger row, so a market-derived key is empty in practice. Keying on
    # the PT symbol makes this ``position_id`` byte-identical to the
    # ``accounting_events`` ``position_key``
    # (connectors/pendle/accounting_spec.py:_pt_context) — the surface the
    # dashboard joins on — and the maturity-bearing symbol uniquely identifies
    # the PT (one symbol ⇔ one market ⇔ one maturity). Upstream (the G-PT0
    # receipt parser + the demo config) guarantees the canonical symbol is used
    # on every leg so OPEN (buy) and CLOSE (sell/redeem) collapse onto one key.
    pt_key = pt_symbol.strip().lower()
    position_id = f"pendle_pt:{chain.lower()}:{wallet.lower()}:{pt_key}" if pt_key else ""
    return event_type, position_id


def _resolve_event_and_position_type(
    ctx: IntentEventContext, intent_type: str
) -> tuple[PositionEventType, PositionType, str] | None:
    """Resolve ``(event_type, position_type, pendle_position_id_override)`` for an intent.

    The Pendle-PT interception runs before the generic gate because a Pendle
    SWAP/WITHDRAW would otherwise either drop (SWAP absent from the map) or
    mis-resolve to a lending leg (WITHDRAW). A redeem's PT symbol comes from the
    connector-declared INPUT money leg (G-PT / VIB-4988 part 2). When the intent
    is not a Pendle PT action it falls to the generic ``INTENT_TO_EVENT_TYPE``
    map; an intent type absent from that map yields ``None`` (not a
    position-producing lifecycle intent).
    """
    intent = ctx.intent
    pendle_pt = _pendle_pt_event(
        intent,
        intent_type,
        ctx.chain,
        ctx.wallet_address,
        redeem_pt_symbol=_redeem_pt_symbol_from_legs(ctx.extracted),
    )
    if pendle_pt is not None:
        pt_event_type, pendle_position_id_override = pendle_pt
        return pt_event_type, PositionType.PENDLE_PT, pendle_position_id_override

    event_type = INTENT_TO_EVENT_TYPE.get(intent_type)
    if event_type is None:
        return None
    # VIB-4162: strict resolution — raises UnknownIntentTypeError if the intent
    # passed the INTENT_TO_EVENT_TYPE gate but the taxonomy has no record. The
    # pre-T2 silent-LP fallback at this site is the canonical class-of-bug T2
    # fixes (see module commit history).
    return event_type, _resolve_position_type(intent_type), ""


def _tx_and_gas_details(ctx: IntentEventContext, result: Any) -> tuple[str, str]:
    """Resolve ``(tx_hash, gas_usd)`` from the result envelope (first tx only).

    Gas USD precedence mirrors the ledger writer's
    ``observability.ledger._extract_tx_and_gas``:
      1. honour a pre-computed ``result.gas_cost_usd`` if set (legacy enrichers
         like the prediction-handler path);
      2. otherwise compute from ``result.total_gas_cost_wei × native_usd`` via
         ``accounting.gas_pricing.compute_gas_usd`` — closes the gap where
         ``position_events.gas_usd`` was empty even when the ledger had real
         numbers, because the orchestrator only populates ``total_gas_cost_wei``,
         not ``gas_cost_usd``.
    """
    tx_hash = ""
    gas_usd = ""
    if not result:
        return tx_hash, gas_usd
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
    return tx_hash, gas_usd


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

    resolved = _resolve_event_and_position_type(ctx, intent_type)
    if resolved is None:
        return None
    event_type, position_type, pendle_position_id_override = resolved
    protocol = getattr(intent, "protocol", "") or ""

    # Position id: a Pendle-PT market-derived key (continuity across buy→redeem)
    # takes precedence; otherwise result.position_id over intent.position_id.
    position_id = pendle_position_id_override
    result = ctx.result
    if position_id:
        pass
    elif result and hasattr(result, "position_id") and result.position_id:
        position_id = str(result.position_id)
    elif hasattr(intent, "position_id") and intent.position_id:
        position_id = str(intent.position_id)

    tx_hash, gas_usd = _tx_and_gas_details(ctx, result)

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


def _pair_tokens_from_intent(intent: Any) -> tuple[str | None, str | None]:
    """Resolve the LP pair ``(token0, token1)`` symbols from an LP intent.

    Prefers explicit ``token0`` / ``token1`` (or the ``from_token`` /
    ``to_token`` aliases an LP open may carry), falling back to parsing the
    ``pool`` descriptor — e.g. ``"WETH/USDC/3000"`` (Uniswap V3 fee tier),
    ``"WAVAX/USDC/20"`` (TraderJoe V2 bin step), ``"USDC/DAI/stable"``
    (Solidly). Numeric and ``0x``-address segments are dropped so only the
    two token symbols survive; a ``pool`` that is itself a bare address
    yields ``(None, None)`` (the descriptor carries no symbols to recover).

    Shared by the OPEN (Phase γ) and CLOSE (Phase κ) enrichers. The CLOSE
    use makes the close event self-describing — its token columns no longer
    depend solely on the in-process ``recent_open_events`` carry-forward,
    which misses when the CLOSE's ``position_id`` differs from the OPEN leg's
    (e.g. TraderJoe V2 fungible-LP closes under a synthetic id) or on a
    cross-process teardown / resume where the cache is empty (VIB-5195).

    Invariant: the ``pool`` descriptor's first two segments are token0 / token1
    in that order, and a given connector emits the SAME order on OPEN and CLOSE
    (the CLOSE fallback fires only on a cache miss, so a connector that reversed
    the order between OPEN and CLOSE would swap the per-leg token columns — the
    value_usd magnitude is order-invariant, ``a0·p0 + a1·p1``, so the total is
    unaffected). The pair is parsed POSITIONALLY (not filter-then-index) and is
    accepted only when BOTH leading segments are symbols; a mixed / partial
    descriptor (an address in either leg) is rejected whole so a trailing symbol
    can never be misattributed onto the wrong slot.
    """
    t0 = getattr(intent, "token0", None) or getattr(intent, "from_token", None)
    t1 = getattr(intent, "token1", None) or getattr(intent, "to_token", None)
    if not t0 or not t1:
        pool_str = (getattr(intent, "pool", "") or "").strip()
        if "/" in pool_str:
            parts = [p.strip() for p in pool_str.split("/") if p.strip()]
            if len(parts) >= 2:
                seg0 = parts[0].split("(")[0].split(" ")[0].strip()
                seg1 = parts[1].split("(")[0].split(" ")[0].strip()

                def _is_symbol(seg: str) -> bool:
                    return bool(seg) and not seg.isdigit() and not seg.lower().startswith("0x")

                if _is_symbol(seg0) and _is_symbol(seg1):
                    if not t0:
                        t0 = seg0.upper()
                    if not t1:
                        t1 = seg1.upper()
    return (str(t0) if t0 else None, str(t1) if t1 else None)


def _pair_tokens_from_declared_legs(extracted: Any) -> tuple[str | None, str | None]:
    """Read the LP pair ``(token0, token1)`` from a connector-DECLARED
    ``PrimitiveMoneyLegs`` (VIB-5221 / US-011), when present.

    The typed money-leg contract (blueprint 27 §6.6) supersedes the #2894
    ``_pair_tokens_from_intent`` threading: a migrated connector declares its
    LP_CLOSE proceeds as two OUTPUT legs in ``token0`` / ``token1`` order on
    ``extracted_data["primitive_money_legs"]`` (TraderJoe V2 builds them from the
    on-chain WithdrawnFromBins legs — chain truth, independent of the intent and
    the ``position_id``). Reading the pair from the contract makes the close
    event's token columns a property of what actually moved on-chain rather than
    of the intent's pool descriptor.

    Returns ``(None, None)`` when no declared legs are present (a non-migrated
    connector) or a leg's identity is unknown (``""``), so the caller falls back
    to ``_pair_tokens_from_intent``. A leg whose token identity is ``""`` (Empty ≠
    Zero) yields ``None`` for that slot — never a fabricated symbol.
    """
    if not isinstance(extracted, dict):
        return (None, None)
    legs = extracted.get("primitive_money_legs")
    if legs is None:
        return (None, None)
    # Deferred import: connector value types must never load at module import
    # (framework → connector boundary; mirrors the ledger dispatcher's resolver).
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

    if not isinstance(legs, PrimitiveMoneyLegs):
        return (None, None)
    outputs = legs.output_legs
    t0 = outputs[0].token if len(outputs) >= 1 and outputs[0].token else None
    t1 = outputs[1].token if len(outputs) >= 2 and outputs[1].token else None
    return (t0, t1)


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
    # Token symbols: LPOpenData doesn't carry them directly. Resolve from the
    # intent attrs / pool descriptor (e.g. "WETH/USDC/3000", "USDC/DAI/stable").
    t0, t1 = _pair_tokens_from_intent(ctx.intent)
    if t0:
        event.token0 = t0
    if t1:
        event.token1 = t1


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

    _stamp_lp_close_fee_taxonomy(event, lp_close)


def _stamp_lp_close_fee_taxonomy(event: PositionEvent, lp_close: Any) -> None:
    """VIB-4848 (T8) — stamp ``fee_separation_method`` / ``fee_confidence``
    onto ``event.attribution_json``.

    Mirrors the ``funding_fee_usd`` sidecar pattern from ``_apply_perp``.
    Pulls values from the ``LPCloseData`` post-init inference when the
    parser did not set explicit ones; ``"UNKNOWN"`` survives only for
    hand-built fixtures that bypass the dataclass.

    Gated to ``event_type == "CLOSE"`` — ``_apply_lp_close`` Phase δ also
    runs for ``LP_COLLECT_FEES`` intents (which materialise the same
    Burn / Collect events), but those produce ``COLLECT_FEES`` position
    events for which the close-only fee-separation taxonomy is not
    meaningful and would leak CLOSE semantics into downstream
    attribution.
    """
    if event.event_type != "CLOSE":
        return
    method = getattr(lp_close, "fee_separation_method", None)
    confidence = getattr(lp_close, "fee_confidence", None)
    if not (method or confidence):
        return
    try:
        existing = json.loads(event.attribution_json or "{}")
        if not isinstance(existing, dict):
            existing = {}
        if method:
            existing["fee_separation_method"] = str(method)
        if confidence:
            existing["fee_confidence"] = str(confidence)
        event.attribution_json = json.dumps(existing)
    except Exception:  # noqa: BLE001
        logger.debug(
            "Failed to stamp fee_separation_method/fee_confidence into attribution_json",
            exc_info=True,
        )


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


def lending_position_id(*, chain: str, protocol: str, wallet: str, asset: str, market_id: str | None = None) -> str:
    """Canonical lending position_id shape — must match
    ``LendingAccountingEvent.position_key`` so Layer 3 (position_events)
    and Layer 5 (accounting_events) are joinable on a single column.

    Shape (VIB-4981): ``lending:{chain}:{protocol}:{wallet}[:{market_id}]:{asset}``.
    ``market_id`` is inserted BETWEEN wallet and asset, and ONLY when truthy —
    matching ``lending_accounting._derive_position_key`` byte-for-byte for both
    the market-scoped (isolated lending, e.g. Morpho Blue) and the
    non-market-scoped (Aave-style) cases. Without it the
    ``AccountingProcessor._backfill_lending_position_pnl`` join on
    ``position_key == position_id`` silently missed every Morpho close (win
    rate stuck 0/0). The blueprint canonical form is
    ``lending:{chain}:{protocol}:{wallet}:{market}:{asset}``
    (docs/internal/blueprints/27-accounting.md §10).

    All segments are lower-cased; an empty wallet (e.g. dry_run with no
    signer) becomes ``unknown`` rather than producing a malformed key
    like ``lending:arbitrum:aave_v3::usdc``.

    ``market_id`` is normalised with ``str(market_id).lower()`` (no strip / no
    ``unknown`` default) so it stays byte-identical to L5
    ``_derive_position_key`` for every ``str`` market id (the only shape any
    Morpho producer emits today) while defensively coercing a future non-``str``
    ``details``-sourced value rather than raising — matching the house style of
    the sibling deriver ``portfolio_valuer._try_derive_lending_position_key``.
    A falsy ``market_id`` (``None`` / ``""``) inserts NO segment — Empty ≠ Zero —
    so Aave-style keys are unchanged (zero regression).
    """
    chain_n = (chain or "unknown").strip().lower() or "unknown"
    proto_n = (protocol or "unknown").strip().lower() or "unknown"
    wallet_n = (wallet or "unknown").strip().lower() or "unknown"
    asset_n = (asset or "unknown").strip().lower() or "unknown"
    if market_id:
        return f"lending:{chain_n}:{proto_n}:{wallet_n}:{str(market_id).lower()}:{asset_n}"
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


def lending_realized_net_pnl_usd(
    intent_type: str,
    interest_delta_usd: Decimal | str | float | int | None,
) -> Decimal | None:
    """Signed realized PnL for one lending action from its interest delta.

    VIB-4977 — the lending CLOSE attribution lane omitted ``net_pnl_usd``,
    so ``almanak strat pnl`` scored every leveraged-lending close as
    *unattributed* (win rate ``0/0``). The realized PnL is the FIFO
    interest split computed by the Layer-5 lending handler
    (``interest_delta_usd``):

    * REPAY / DELEVERAGE → ``-interest`` (debt-side interest paid is a cost)
    * WITHDRAW           → ``+interest`` (supply-side yield received is a gain)

    This is the correct sign convention. It matches
    ``accounting/position_pnl.py:compute_position_pnl`` for REPAY / WITHDRAW
    on every base. Cross-surface consistency for **DELEVERAGE** depends on
    VIB-4974 (PR #2584): on the pre-VIB-4974 base, ``compute_position_pnl``
    gates only ``("REPAY", "WITHDRAW")`` and drops DELEVERAGE interest, so
    that surface and this helper agree on DELEVERAGE only once VIB-4974
    widens the gate (and fixes the symmetric ``portfolio_valuer`` BORROW-side
    DELEVERAGE filter). This helper's DELEVERAGE→``-interest`` sign is the
    target both surfaces converge on — do not narrow it.

    Returns ``None`` (Empty ≠ Zero) when:

    * ``interest_delta_usd`` is unmeasured (``None`` — e.g. no matching FIFO
      borrow/supply lots), so the close stays unattributed rather than
      scoring as a fabricated break-even win, OR
    * the intent type does not carry a realized-interest leg (SUPPLY /
      BORROW open the position; only the debt-reducing / supply-unlocking
      legs realize interest).

    ``Decimal("0")`` interest is a *measured* zero (e.g. a same-block
    open→close) and yields ``Decimal("0")`` net PnL — a scored break-even
    close, distinct from the unattributed ``None`` case above.
    """
    if interest_delta_usd is None:
        return None
    it = (intent_type or "").upper()
    if it not in ("REPAY", "DELEVERAGE", "WITHDRAW"):
        return None
    try:
        interest = Decimal(str(interest_delta_usd))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not interest.is_finite():
        return None
    # Debt-side interest paid is a cost; supply-side yield received is a gain.
    return -interest if it in ("REPAY", "DELEVERAGE") else interest


def _build_lending_attribution(
    event: PositionEvent,
    post: dict,
    asset: str,
    intent_type: str,
    net_pnl_usd: Decimal | None = None,
) -> None:
    """v1 lending attribution. The after-state fields (collateral / debt /
    HF / APR) are fully derivable from the ledger row's post_state — no
    FIFO replay required.

    ``net_pnl_usd`` (VIB-4977) is the signed realized PnL for this action,
    sourced from the Layer-5 FIFO interest split (``interest_delta_usd``)
    which is NOT available at seed time. The production seed caller
    (``_apply_lending``) therefore always passes ``net_pnl_usd=None`` — this
    parameter exists for back-fill / seed-time parity and is currently
    written ONLY by the AccountingProcessor drain
    (``AccountingProcessor._backfill_lending_position_pnl``), which has the
    interest split in hand. The seed path itself never stamps realized PnL.
    ``None`` ⇒ the key is omitted (Empty ≠ Zero — an unattributed close,
    distinct from a measured ``"0"`` break-even). When present,
    ``almanak strat pnl`` scores the close via ``_pnl_from_attribution``.

    Schema-version-stamped (``lending_v1``) so a future v2 producer is
    distinguishable from this payload. Adding an OPTIONAL ``net_pnl_usd``
    key is additive and does NOT bump the schema version — readers that
    don't know the key ignore it, and ``_pnl_from_attribution`` keys off
    its presence, not the schema version.
    """
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
    if net_pnl_usd is not None:
        attribution["net_pnl_usd"] = str(net_pnl_usd)
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

    # VIB-4981 — isolated lending (Morpho Blue & friends) scopes positions by
    # market_id. L5 (_derive_position_key) inserts it between wallet and asset;
    # the L3 key must do the same or the (position_key == position_id) join in
    # AccountingProcessor._backfill_lending_position_pnl silently misses every
    # market-scoped close. ``market_id`` is the canonical intent field on every
    # lending intent (BorrowIntent/SupplyIntent/RepayIntent/WithdrawIntent —
    # lending_intents.py); it is the SAME source L5 reads via _intent_market_id.
    # Absent (Aave-style) ⇒ None ⇒ no extra segment ⇒ key unchanged.
    market_id = getattr(intent, "market_id", None)

    # VIB-5030 — canonicalize lending-scoped protocol aliases (the platform
    # spec's ``"fluid_lending"`` → ``fluid``) before deriving the L3 join key.
    # ``event.protocol`` carries the RAW intent string (``_seed_event``), and
    # this writer does NOT flow through ``lending_accounting``, so without
    # this call the L3 ``position_id`` would diverge from the L5
    # ``position_key`` (canonicalized inside ``_derive_position_key``) and
    # the ``_backfill_lending_position_pnl`` join would silently miss
    # (VIB-4981 class). Deferred import: registry dispatch must not run at
    # module import (same idiom as the lending_accounting consumers).
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    raw_protocol = event.protocol or getattr(intent, "protocol", "") or ""
    event.position_id = lending_position_id(
        chain=ctx.chain,
        protocol=LendingReadRegistry.normalize_protocol(raw_protocol) or str(raw_protocol),
        wallet=ctx.wallet_address,
        asset=asset,
        market_id=market_id,
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

    # VIB-5036: the W1-5 decimal-unit soft-fail guard is deliberately NOT run
    # here — position_events amount/fee columns are raw-by-contract, so the
    # human-form guard only produced false warnings (see the module note above
    # the removed ``_decimal_unit_soft_fail`` helper). The guard stays active
    # on the human-units ``transaction_ledger`` via ``build_ledger_entry``.
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
    # VIB-5195 — self-describing fallback when the cache carry-forward above
    # leaves a token slot empty. The cache (and its durable-store sibling,
    # VIB-4839) is keyed by ``position_id``, so it misses whenever the CLOSE's
    # ``position_id`` differs from the OPEN leg's — the TraderJoe V2 fungible-LP
    # demos close under a synthetic id (``traderjoe_*_lp_0``) while the OPEN
    # cached under the pool descriptor — or on a cross-process teardown / resume
    # where the cache is empty. The LP_CLOSE intent carries the pair via its
    # ``pool`` descriptor ("TOKEN_X/TOKEN_Y/<bin_step|fee>"), so resolve the
    # symbols from it. Without this the close lands with token0=''/token1='' and
    # ``_apply_lp_close_value_usd`` below fails closed (missing_tokens_or_amounts
    # have_token0=False), leaving the close-leg USD value unattributed (Empty ≠
    # Zero — unmeasured, not a fabricated zero). Fills empty slots only, so the
    # cache stays authoritative for every path that already resolves it.
    if not event.token0 or not event.token1:
        # VIB-5221 — prefer the connector-DECLARED PrimitiveMoneyLegs (the typed
        # contract) over the #2894 intent-pool-descriptor threading. For a
        # migrated connector (TraderJoe V2) the close pair comes from the OUTPUT
        # legs built off the on-chain withdrawal; ``_pair_tokens_from_intent``
        # stays the fallback for not-yet-migrated connectors and for any slot the
        # contract leaves unknown. Fills empty slots only — the cache carry-
        # forward above stays authoritative for every path that already resolved.
        t0, t1 = _pair_tokens_from_declared_legs(ctx.extracted)
        if not t0 or not t1:
            it0, it1 = _pair_tokens_from_intent(ctx.intent)
            t0 = t0 or it0
            t1 = t1 or it1
        if not event.token0 and t0:
            event.token0 = t0
        if not event.token1 and t1:
            event.token1 = t1
    # in_range is unambiguously False post-close (NFT burned / liquidity
    # withdrawn). The dashboard reads ``in_range=None`` as "unknown" and
    # ``False`` as "out of range". Either is honest; False is more
    # informative for the closed lifecycle stage.
    if event.in_range is None:
        event.in_range = False
    # Compute value_usd from received amounts when prices available.
    if not event.value_usd and price_oracle:
        _apply_lp_close_value_usd(event, price_oracle, chain=ctx.chain)


@dataclass(frozen=True)
class LpCloseValueResult:
    """Outcome of :func:`compute_lp_close_value_usd`.

    ``value_usd`` is the empty string on every fail-closed path (Empty ≠
    Zero — never a fabricated ``"0"``). When ``value_usd`` is non-empty the
    decimals + prices used to compute it are returned so the caller can
    stamp dependent enrichments (e.g. ``fees_total_usd``) without re-running
    the resolver / oracle lookup. ``skip_reason`` is ``None`` on success and
    one of the documented sentinels on a fail-closed path.
    """

    value_usd: str = ""
    decimals0: int | None = None
    decimals1: int | None = None
    price0: Decimal | None = None
    price1: Decimal | None = None
    skip_reason: str | None = None


def compute_lp_close_value_usd(
    token0: str,
    token1: str,
    amount0: str | None,
    amount1: str | None,
    price_oracle: dict,
    chain: str = "",
    *,
    position_id: str = "",
) -> LpCloseValueResult:
    """VIB-3919 / VIB-4896 — pure ``value_usd`` math for an LP_CLOSE.

    ``value_usd`` at CLOSE = received amount0 × price0 + received amount1 ×
    price1. Mirrors ``_apply_lp_open_value_usd`` but reads the CLOSE-time
    received amounts. **Fails closed**: if a token is missing, an amount is
    missing, decimals can't be resolved, or a price is unavailable, the
    returned ``value_usd`` stays ``""`` — never fabricated to ``0`` (Empty ≠
    Zero per CLAUDE.md §Accounting).

    Extracted from ``_apply_lp_close_value_usd`` (VIB-4896) so the offline
    repair CLI (``almanak strat repair-teardown-lp-close``) and the runner's
    iteration / teardown lanes share one implementation of the decimal/price
    math. Behaviour is preserved verbatim — same upper-casing, same tolerant
    ``price_usd``/``price`` + lower-case fallback price lookup, same
    structured WARNs (each carrying ``position_id``/``chain``) on every
    early-exit branch so a silent empty value_usd never hides in production
    again (the May-22 ``lp_triple`` rerun bug).
    """
    token0 = (token0 or "").upper()
    token1 = (token1 or "").upper()
    amount0_str = amount0
    amount1_str = amount1
    if not (amount0_str and amount1_str and token0 and token1):
        logger.warning(
            "lp_close_value_usd.skipped reason=missing_tokens_or_amounts "
            "position_id=%s chain=%s have_token0=%s have_token1=%s "
            "have_amount0=%s have_amount1=%s",
            position_id,
            chain,
            bool(token0),
            bool(token1),
            bool(amount0_str),
            bool(amount1_str),
        )
        return LpCloseValueResult(skip_reason="missing_tokens_or_amounts")
    try:
        from decimal import Decimal as _D

        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        # ``resolver.resolve`` raises ``TokenNotFoundError`` (etc.) on miss —
        # it never returns ``None``.  Catch the specific resolver-failure
        # shape here so the operator gets the precise
        # ``token_decimals_unresolved`` reason rather than the generic
        # ``arithmetic_or_resolver_error`` tail at the bottom of this
        # function. (Gemini code-assist on PR #2490 caught the original
        # ``is None`` check as dead code.)
        try:
            ti0 = resolver.resolve(token0, chain=chain, log_errors=False)
            ti1 = resolver.resolve(token1, chain=chain, log_errors=False)
        except Exception as resolver_err:  # noqa: BLE001 — fail-closed: empty stays empty
            logger.warning(
                "lp_close_value_usd.skipped reason=token_decimals_unresolved "
                "position_id=%s chain=%s token0=%s token1=%s err=%s",
                position_id,
                chain,
                token0,
                token1,
                resolver_err,
            )
            return LpCloseValueResult(skip_reason="token_decimals_unresolved")
        a0 = _D(str(amount0_str)) / _D(10**ti0.decimals)
        a1 = _D(str(amount1_str)) / _D(10**ti1.decimals)

        # Tolerant price lookup — mirrors VIB-3885 helper used by category
        # handlers and ``_apply_lp_open_value_usd`` at L1478. ``sym`` is
        # already upper-cased above, so the OPEN path uses ``sym.lower()`` as
        # the second fallback for oracles that key on lowercase symbols.
        # Likewise accepts both ``price_usd`` and ``price`` keys on nested
        # entries. (CodeRabbit Major on PR #2490 — the prior ``sym.upper()``
        # fallback was dead and the single ``price_usd`` key diverged from
        # the OPEN helper.)
        def _price(sym: str) -> _D | None:
            entry = price_oracle.get(sym) or price_oracle.get(sym.lower())
            if entry is None:
                return None
            if isinstance(entry, dict):
                p = entry.get("price_usd") or entry.get("price")
                if p is None:
                    return None
                try:
                    return _D(str(p))
                except Exception:  # noqa: BLE001
                    return None
            try:
                d = _D(str(entry))
            except Exception:  # noqa: BLE001
                return None
            return d if d.is_finite() else None

        p0, p1 = _price(token0), _price(token1)
        if p0 is None or p1 is None:
            logger.warning(
                "lp_close_value_usd.skipped reason=price_oracle_miss "
                "position_id=%s chain=%s token0=%s token1=%s have_price0=%s have_price1=%s",
                position_id,
                chain,
                token0,
                token1,
                p0 is not None,
                p1 is not None,
            )
            return LpCloseValueResult(skip_reason="price_unavailable")
        return LpCloseValueResult(
            value_usd=str(a0 * p0 + a1 * p1),
            decimals0=ti0.decimals,
            decimals1=ti1.decimals,
            price0=p0,
            price1=p1,
        )
    except Exception:  # noqa: BLE001 — best-effort enrichment
        # VIB-4839 — escalate from debug to warning. A swallowed exception
        # here is observationally indistinguishable from the silent missing-
        # token early-return that hid the lp_triple bug, so it must surface.
        logger.warning(
            "lp_close_value_usd.skipped reason=arithmetic_or_resolver_error position_id=%s chain=%s",
            position_id,
            chain,
            exc_info=True,
        )
        return LpCloseValueResult(skip_reason="arithmetic_or_resolver_error")


def _apply_lp_close_value_usd(event: PositionEvent, price_oracle: dict, chain: str = "") -> None:
    """VIB-3919 — set ``event.value_usd`` from the close-time received amounts.

    Thin wrapper around :func:`compute_lp_close_value_usd` (VIB-4896): reads
    the CLOSE-time amounts off the event, delegates the decimal/price math,
    and — on success — writes ``value_usd`` and stamps ``fees_total_usd``.
    Fails closed exactly as before: ``value_usd`` stays "" on any missing
    input (Empty ≠ Zero per CLAUDE.md §Accounting).
    """
    if event.event_type != "CLOSE" or event.position_type != "LP":
        return
    if event.value_usd:
        return
    result = compute_lp_close_value_usd(
        event.token0 or "",
        event.token1 or "",
        event.amount0,
        event.amount1,
        price_oracle,
        chain=chain,
        position_id=event.position_id,
    )
    if not result.value_usd:
        return
    event.value_usd = result.value_usd
    # decimals + prices are always populated alongside a non-empty value_usd.
    if (
        result.decimals0 is not None
        and result.decimals1 is not None
        and result.price0 is not None
        and result.price1 is not None
    ):
        _stamp_lp_close_fees_total_usd(event, result.decimals0, result.decimals1, result.price0, result.price1)


def _load_attribution_dict(event: PositionEvent) -> dict:
    """Return ``event.attribution_json`` as a dict, tolerating malformed JSON.

    Centralised so callers do not have to repeat the try / non-dict
    coalesce — the result is a dict that can be mutated and re-serialised
    via ``json.dumps``. Used by the VIB-4848 close-event enrichers.
    """
    try:
        existing = json.loads(event.attribution_json or "{}")
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(existing, dict):
        return {}
    return existing


def _fees_unmeasured(raw: Any) -> bool:
    """Empty ≠ Zero guard for ``PositionEvent.fees_token0`` / ``fees_token1``.

    ``PositionEvent`` columns default to the empty string ``""`` (the
    column default survives the SQLite round-trip when no parser populated
    the field).  Both ``None`` and ``""`` signal "unmeasured" and must be
    skipped before they reach ``Decimal(str(...))`` and crash with
    ``InvalidOperation``.  An explicit ``"0"`` is measured zero and is
    *not* unmeasured.
    """
    return raw is None or raw == ""


def _stamp_lp_close_fees_total_usd(
    event: PositionEvent,
    decimals0: int,
    decimals1: int,
    price0: Decimal,
    price1: Decimal,
) -> None:
    """VIB-4848 (T9) — stamp ``fees_total_usd`` onto attribution_json.

    Only SEPARATE/EXACT closes (UniV3 / PancakeSwap V3 / SushiSwap V3 /
    Aerodrome Slipstream) get a non-None ``fees_total_usd`` stamp:
    BUNDLED closes (UniV4 / Fluid / Aerodrome V1) have no measured fee
    separation, so emitting a value here would silently substitute a
    fabricated zero for an unmeasured observation (Empty ≠ Zero). The
    downstream ``attribute_lp`` uses this stamp to recover the
    principal-only V_lp before computing IL.
    """
    existing = _load_attribution_dict(event)
    method = str(existing.get("fee_separation_method") or "").upper()
    if method != "SEPARATE":
        return
    if _fees_unmeasured(event.fees_token0) or _fees_unmeasured(event.fees_token1):
        return
    try:
        f0 = Decimal(str(event.fees_token0)) / Decimal(10**decimals0)
        f1 = Decimal(str(event.fees_token1)) / Decimal(10**decimals1)
    except (InvalidOperation, ValueError):
        logger.debug(
            "LP_CLOSE fees_total_usd compute failed (decimal parse)",
            exc_info=True,
        )
        return
    existing["fees_total_usd"] = str(f0 * price0 + f1 * price1)
    event.attribution_json = json.dumps(existing)


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
