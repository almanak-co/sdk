"""Position lifecycle events for LP and perps tracking.

Immutable-ID positions (LP NFTs, perp positions) have a lifecycle:
OPEN -> SNAPSHOT* -> CLOSE.  Each state change is recorded as a
PositionEvent with raw observables (amounts, prices, fees).

Fungible positions (lending supply/borrow, staking) are tracked via
enriched snapshot deltas (Phase 1d), not lifecycle events.
"""

import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class PositionEventType(str, Enum):
    """Types of position lifecycle events."""

    OPEN = "OPEN"
    CLOSE = "CLOSE"
    COLLECT_FEES = "COLLECT_FEES"
    SNAPSHOT = "SNAPSHOT"


class PositionType(str, Enum):
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

    # Attribution
    attribution_json: str = "{}"
    attribution_version: int = 0

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d


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
    """
    intent_type = ""
    if hasattr(intent, "intent_type"):
        it = intent.intent_type
        intent_type = it.value if hasattr(it, "value") else str(it)

    event_type = INTENT_TO_EVENT_TYPE.get(intent_type)
    if event_type is None:
        return None

    position_type = INTENT_TO_POSITION_TYPE.get(intent_type, PositionType.LP)
    protocol = getattr(intent, "protocol", "") or ""

    # Extract position_id from result
    position_id = ""
    if result and hasattr(result, "position_id") and result.position_id:
        position_id = str(result.position_id)
    elif hasattr(intent, "position_id") and intent.position_id:
        position_id = str(intent.position_id)

    # Extract tx details
    tx_hash = ""
    gas_usd = ""
    if result:
        if hasattr(result, "transaction_results") and result.transaction_results:
            tx_hash = result.transaction_results[0].tx_hash or ""
        gas_cost = getattr(result, "gas_cost_usd", None)
        if gas_cost is not None:
            gas_usd = str(gas_cost)

    event = PositionEvent(
        deployment_id=deployment_id,
        position_id=position_id,
        position_type=position_type.value,
        event_type=event_type.value,
        protocol=protocol,
        chain=chain,
        tx_hash=tx_hash,
        gas_usd=gas_usd,
        ledger_entry_id=ledger_entry_id,
    )

    # Populate from extracted_data
    extracted = getattr(result, "extracted_data", {}) if result else {}
    if not extracted:
        # Don't emit lifecycle events without a position_id — they can't be
        # joined reliably downstream.
        return event if event.position_id else None

    # LP data
    lp_open = extracted.get("lp_open_data")
    lp_close = extracted.get("lp_close_data")

    if lp_open and hasattr(lp_open, "position_id"):
        event.position_id = str(lp_open.position_id)
        event.liquidity = str(getattr(lp_open, "liquidity", "") or "")
        event.tick_lower = getattr(lp_open, "tick_lower", None)
        event.tick_upper = getattr(lp_open, "tick_upper", None)

    if lp_close:
        event.amount0 = str(getattr(lp_close, "amount0_received", "") or "")
        event.amount1 = str(getattr(lp_close, "amount1_received", "") or "")
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

    # Swap amounts (for value estimation)
    swap = extracted.get("swap_amounts")
    if swap:
        event.token0 = getattr(swap, "token_in", "") or ""
        event.token1 = getattr(swap, "token_out", "") or ""
        if not event.amount0:
            event.amount0 = str(getattr(swap, "amount_in_decimal", "") or "")
        if not event.amount1:
            event.amount1 = str(getattr(swap, "amount_out_decimal", "") or "")

    # Perp data
    perp = extracted.get("perp_data")
    if perp:
        event.leverage = str(getattr(perp, "leverage", "") or "")
        event.entry_price = str(getattr(perp, "entry_price", "") or "")
        event.mark_price = str(getattr(perp, "mark_price", "") or "")
        event.unrealized_pnl = str(getattr(perp, "unrealized_pnl", "") or "")
        event.is_long = getattr(perp, "is_long", None)
        if hasattr(perp, "position_id") and perp.position_id:
            event.position_id = str(perp.position_id)

    # Final guard: don't emit lifecycle events without a position_id
    return event if event.position_id else None
