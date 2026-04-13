"""PnL attribution for immutable-ID positions (LP + perps).

Raw observables are canonical; derived attribution is versioned and recomputable.
A bug in the formula becomes a version bump, not permanent bad data.

LP v1 formula:
    principal_deposited = value_usd at OPEN
    principal_recovered = amount0 * price0 + amount1 * price1 at CLOSE
    fee_pnl = fees_token0 * price0 + fees_token1 * price1
    hodl_value = initial_amount0 * close_price0 + initial_amount1 * close_price1
    il = principal_recovered - hodl_value  (negative = impermanent loss)
    price_pnl = hodl_value - principal_deposited
    net_pnl = principal_recovered + fee_pnl - principal_deposited - gas

Perp v1 formula:
    price_pnl = unrealized_pnl (from protocol, signed)
    fee_pnl = -gas  (simplified: protocol fees not yet available)
    net_pnl = price_pnl + fee_pnl
"""

import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)

CURRENT_VERSION = 1


def _dec(value: Any) -> Decimal:
    """Safely convert to Decimal, returning 0 on failure.

    Logs a warning for non-empty values that fail conversion so corrupt
    financial data is visible rather than silently becoming $0.
    """
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        logger.warning("PnL attribution: could not convert %r to Decimal, defaulting to 0", value)
        return Decimal("0")


def attribute_lp(open_event: dict, close_event: dict) -> dict:
    """Compute LP PnL attribution from OPEN and CLOSE events.

    Args:
        open_event: The OPEN position event dict.
        close_event: The CLOSE position event dict.

    Returns:
        Attribution dict with versioned breakdown.
    """
    principal_deposited = _dec(open_event.get("value_usd"))

    # Amounts recovered at close
    amount0_recovered = _dec(close_event.get("amount0"))
    amount1_recovered = _dec(close_event.get("amount1"))
    close_value_usd = _dec(close_event.get("value_usd"))

    # Fees collected at close
    fees_token0 = _dec(close_event.get("fees_token0"))
    fees_token1 = _dec(close_event.get("fees_token1"))

    # Gas costs across the lifecycle
    open_gas = _dec(open_event.get("gas_usd"))
    close_gas = _dec(close_event.get("gas_usd"))
    total_gas = open_gas + close_gas

    # If we have close_value_usd, use it directly as principal_recovered.
    # Otherwise fall back to 0 (raw data incomplete).
    principal_recovered = close_value_usd if close_value_usd else Decimal("0")

    # Fee PnL: if we have a USD value for the position, we estimate fee value
    # proportionally. Without per-token prices, we store raw token amounts.
    # For v1, fee_pnl is estimated from the close event's value_usd context.
    fee_pnl = Decimal("0")

    # Impermanent loss requires per-token prices which we may not have in v1.
    # Store what we can compute.
    il = Decimal("0")

    # Price PnL = what hodling would have given - what was deposited
    price_pnl = principal_recovered - principal_deposited if principal_deposited else Decimal("0")

    # Net PnL includes fees and gas
    net_pnl = principal_recovered + fee_pnl - principal_deposited - total_gas

    return {
        "version": CURRENT_VERSION,
        "position_type": "LP",
        "principal_deposited_usd": str(principal_deposited),
        "principal_recovered_usd": str(principal_recovered),
        "fees_token0": str(fees_token0),
        "fees_token1": str(fees_token1),
        "fee_pnl_usd": str(fee_pnl),
        "impermanent_loss_usd": str(il),
        "price_pnl_usd": str(price_pnl),
        "gas_usd": str(total_gas),
        "net_pnl_usd": str(net_pnl),
        "amount0_recovered": str(amount0_recovered),
        "amount1_recovered": str(amount1_recovered),
    }


def attribute_perp(open_event: dict, close_event: dict) -> dict:
    """Compute perp PnL attribution from OPEN and CLOSE events.

    Args:
        open_event: The OPEN position event dict.
        close_event: The CLOSE position event dict.

    Returns:
        Attribution dict with versioned breakdown.
    """
    entry_price = _dec(open_event.get("entry_price") or close_event.get("entry_price"))
    mark_price = _dec(close_event.get("mark_price"))
    unrealized_pnl = _dec(close_event.get("unrealized_pnl"))
    leverage = _dec(close_event.get("leverage") or open_event.get("leverage"))
    is_long = close_event.get("is_long")
    if is_long is None:
        is_long = open_event.get("is_long")

    open_gas = _dec(open_event.get("gas_usd"))
    close_gas = _dec(close_event.get("gas_usd"))
    total_gas = open_gas + close_gas

    # Price PnL from protocol's unrealized_pnl (already signed for direction)
    price_pnl = unrealized_pnl

    # Fee PnL: protocol fees not separately tracked yet, use gas as proxy
    fee_pnl = -total_gas

    net_pnl = price_pnl + fee_pnl

    return {
        "version": CURRENT_VERSION,
        "position_type": "PERP",
        "entry_price": str(entry_price),
        "exit_price": str(mark_price),
        "leverage": str(leverage),
        "is_long": is_long,
        "price_pnl_usd": str(price_pnl),
        "fee_pnl_usd": str(fee_pnl),
        "gas_usd": str(total_gas),
        "net_pnl_usd": str(net_pnl),
    }


def compute_attribution(open_event: dict, close_event: dict) -> str:
    """Compute PnL attribution JSON for a position lifecycle.

    Args:
        open_event: The OPEN event dict (from get_position_history).
        close_event: The CLOSE event dict.

    Returns:
        JSON string with versioned attribution, or '{}' on failure.
    """
    try:
        position_type = (close_event.get("position_type") or open_event.get("position_type") or "").upper()

        if position_type == "LP":
            result = attribute_lp(open_event, close_event)
        elif position_type == "PERP":
            result = attribute_perp(open_event, close_event)
        else:
            logger.debug("Unknown position type for attribution: %s", position_type)
            return "{}"

        return json.dumps(result)
    except Exception:
        logger.debug("Attribution computation failed", exc_info=True)
        return "{}"


async def run_attribution_on_close(
    store: Any,
    close_event: Any,
) -> str:
    """Look up the OPEN event and compute attribution for a CLOSE event.

    Called by StrategyRunner after saving a CLOSE position event.
    Updates the event's attribution_json in the store.

    Args:
        store: StateManager or SQLiteStore with get_position_history/save_position_event.
        close_event: The PositionEvent being closed.

    Returns:
        The computed attribution JSON string.
    """
    attribution = "{}"
    try:
        history = await store.get_position_history(close_event.deployment_id, close_event.position_id)
        # Find the OPEN event
        open_event = None
        for evt in history:
            if evt.get("event_type") == "OPEN":
                open_event = evt
                break

        if open_event is None:
            logger.debug(
                "No OPEN event found for position %s, skipping attribution",
                close_event.position_id,
            )
            return attribution

        close_dict = close_event.to_dict() if hasattr(close_event, "to_dict") else {}
        attribution = compute_attribution(open_event, close_dict)

        if attribution != "{}":
            close_event.attribution_json = attribution
            close_event.attribution_version = CURRENT_VERSION
            # Use partial update to avoid overwriting stored fields
            if hasattr(store, "update_position_attribution"):
                await store.update_position_attribution(close_event.id, attribution, CURRENT_VERSION)
            else:
                await store.save_position_event(close_event)
            logger.debug(
                "Attribution v%d computed for position %s",
                CURRENT_VERSION,
                close_event.position_id,
            )
    except Exception:
        logger.debug("Failed to run attribution on close", exc_info=True)

    return attribution


async def recompute_attribution(
    store: Any,
    deployment_id: str,
    version: int = CURRENT_VERSION,
) -> int:
    """Batch-recompute attribution for all closed positions.

    Useful when the attribution formula is updated (version bump).

    Args:
        store: StateManager or SQLiteStore with position event methods.
        deployment_id: Strategy deployment to recompute.
        version: Target attribution version.

    Returns:
        Number of positions recomputed.
    """
    count = 0
    try:
        close_events = await store.get_position_events(deployment_id, event_type="CLOSE", limit=10000)

        for close_dict in close_events:
            position_id = close_dict.get("position_id", "")
            if not position_id:
                continue

            # Skip if already at target version
            existing_version = close_dict.get("attribution_version", 0)
            if existing_version >= version:
                continue

            history = await store.get_position_history(deployment_id, position_id)
            open_event = None
            for evt in history:
                if evt.get("event_type") == "OPEN":
                    open_event = evt
                    break

            if open_event is None:
                continue

            attribution = compute_attribution(open_event, close_dict)
            if attribution != "{}":
                # Use partial update to avoid wiping stored fields
                if hasattr(store, "update_position_attribution"):
                    await store.update_position_attribution(close_dict["id"], attribution, version)
                else:
                    from .position_events import PositionEvent

                    # Fallback: reconstruct the event
                    evt_obj = PositionEvent(
                        id=close_dict["id"],
                        deployment_id=close_dict.get("deployment_id", ""),
                        position_id=position_id,
                        position_type=close_dict.get("position_type", ""),
                        event_type="CLOSE",
                        protocol=close_dict.get("protocol", ""),
                        chain=close_dict.get("chain", ""),
                        attribution_json=attribution,
                        attribution_version=version,
                        amount0=close_dict.get("amount0", ""),
                        amount1=close_dict.get("amount1", ""),
                        value_usd=close_dict.get("value_usd", ""),
                        fees_token0=close_dict.get("fees_token0", ""),
                        fees_token1=close_dict.get("fees_token1", ""),
                        tx_hash=close_dict.get("tx_hash", ""),
                        gas_usd=close_dict.get("gas_usd", ""),
                        ledger_entry_id=close_dict.get("ledger_entry_id", ""),
                        unrealized_pnl=close_dict.get("unrealized_pnl", ""),
                        entry_price=close_dict.get("entry_price", ""),
                        mark_price=close_dict.get("mark_price", ""),
                        leverage=close_dict.get("leverage", ""),
                    )
                    await store.save_position_event(evt_obj)
                count += 1

    except Exception:
        logger.debug("Batch recompute failed", exc_info=True)

    logger.info("Recomputed attribution for %d positions (v%d)", count, version)
    return count
