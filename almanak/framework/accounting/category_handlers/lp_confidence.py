"""Retain the confidence of the price inputs used by LP valuation."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from almanak.framework.accounting.category_handlers._price_helpers import parse_price_inputs
from almanak.framework.accounting.lp_accounting import LPAccountingEvent
from almanak.framework.accounting.models import AccountingConfidence
from almanak.framework.accounting.price_snapshot import PriceSnapshot
from almanak.framework.market.price_store import lookup_price

_RANK = {
    AccountingConfidence.HIGH: 0,
    AccountingConfidence.ESTIMATED: 1,
    AccountingConfidence.STALE: 2,
    AccountingConfidence.UNAVAILABLE: 3,
}


def _leg_amount(value: Any) -> Decimal | None:
    try:
        amount = Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError):
        return None
    return amount if amount.is_finite() else None


def _price_legs(event: LPAccountingEvent, lp_data: Any) -> list[tuple[str, str | None, Any]]:
    symbols = getattr(lp_data, "coin_symbols", None)
    if symbols:
        addresses = getattr(lp_data, "coin_addresses", None) or [None] * len(symbols)
        vectors = []
        if event.cost_basis_usd is not None:
            vectors.append(getattr(lp_data, "all_amounts", None) or [])
        if event.fees_total_usd is not None:
            vectors.append(getattr(lp_data, "all_fees", None) or [])
        return [
            (symbol, addresses[index] if index < len(addresses) else None, amount)
            for vector in vectors
            for index, (symbol, amount) in enumerate(zip(symbols, vector, strict=False))
        ]
    legs: list[tuple[str, str | None, Any]] = []
    if event.cost_basis_usd is not None:
        legs.extend([(event.token0, None, event.amount0), (event.token1, None, event.amount1)])
    if event.fees_total_usd is not None:
        legs.extend([(event.token0, None, event.fees0_collected), (event.token1, None, event.fees1_collected)])
    return legs


def retain_lp_price_confidence(
    event: LPAccountingEvent,
    price_inputs_json: str | None,
    lp_data: Any,
    prior_open_payload: dict[str, Any] | None,
) -> LPAccountingEvent:
    """Degrade the event using exactly the oracle records selected by valuation.

    Measured-zero legs need no oracle. A synthetic peg retains the valuation's
    existing ESTIMATED confidence; unrelated prices never degrade this event.
    """
    oracle = parse_price_inputs(price_inputs_json)
    snapshot = PriceSnapshot.from_json(price_inputs_json or "")
    legs = _price_legs(event, lp_data)
    if event.hodl_value_usd is not None and prior_open_payload:
        legs.extend(
            (prior_open_payload.get(f"token{i}", ""), None, prior_open_payload.get(f"amount{i}")) for i in (0, 1)
        )
    observations: dict[str, AccountingConfidence] = {}
    for token, address, amount in legs:
        parsed_amount = _leg_amount(amount)
        if parsed_amount is None:
            observations[f"{token} unmeasured amount"] = AccountingConfidence.UNAVAILABLE
            continue
        if parsed_amount == 0:
            continue
        found = lookup_price(oracle, token=token, address=address, chain=event.identity.chain, quote="USD")
        if found is not None:
            key = str(found.key)
            observations[key] = AccountingConfidence(snapshot.confidence(key))
    if event.realized_pnl_usd is not None and prior_open_payload:
        try:
            observations["entry basis"] = AccountingConfidence(prior_open_payload.get("confidence", ""))
        except (ValueError, TypeError):
            observations["entry basis"] = AccountingConfidence.UNAVAILABLE
    if not observations:
        return event
    event.confidence = max((event.confidence, *observations.values()), key=_RANK.__getitem__)
    degraded = [f"{key}={value.value}" for key, value in observations.items() if value != AccountingConfidence.HIGH]
    if degraded:
        reason = "price provenance degraded: " + ", ".join(degraded)
        event.unavailable_reason = "; ".join(filter(None, (event.unavailable_reason, reason)))
    return event
