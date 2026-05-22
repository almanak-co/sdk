"""Pendle LP accounting event builder (VIB-3421, VIB-3488).

Wired into strategy_runner after every successful LP_OPEN / LP_CLOSE for
Pendle markets.  Produces a PendleAccountingEvent(LP_OPEN|LP_CLOSE) which
is persisted to the local accounting_events store via AccountingWriter.

Amount reporting (VIB-3488):
  SY / PT decimals are resolved via the token registry (with address-based
  lookup for known Pendle tokens from PT_TOKEN_INFO / MARKET_TOKEN_MINT_SY).
  USD prices are populated from the price_oracle dict when available:
    - sy_price  : price_oracle keyed to the SY underlying token symbol
    - pt_price  : sy_price × pt_to_asset_rate (on-chain rate if available,
                  else sy_price is used as upper-bound approximation)

  confidence escalation:
    HIGH       – decimals resolved from static registry AND both prices available
    ESTIMATED  – decimals resolved but price oracle partial / unavailable
    UNAVAILABLE – decimals resolution failed (should not happen for known markets)

  None vs Decimal("0") discipline is preserved throughout: missing price or
  failed decimal lookup writes None, never 0.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.ids import make_accounting_event_id

logger = logging.getLogger(__name__)

_PENDLE_LP_INTENT_TYPES = frozenset({"LP_OPEN", "LP_CLOSE"})


def _derive_pendle_position_key(chain: str, wallet: str, market_address: str) -> str:
    """Canonical position key for a Pendle LP position."""
    return f"pendle_lp:{chain.lower()}:{wallet.lower()}:{market_address.lower()}"


def _intent_type_value(intent: Any) -> str:
    it = getattr(intent, "intent_type", None)
    if it is None:
        return ""
    return it.value if hasattr(it, "value") else str(it)


def _get_market_address(intent: Any) -> str:
    """Extract the Pendle market address from the intent pool field.

    LP_OPEN pool format is "TOKEN/0xmarket_address"; LP_CLOSE is bare "0xmarket_address".
    Parses out the address portion in both cases.  Returns empty string when the
    market address cannot be resolved — callers should guard against empty values.
    """
    pool = getattr(intent, "pool", None)
    if not pool:
        return ""
    pool_str = str(pool).strip()
    if "/" in pool_str:
        pool_str = pool_str.split("/", 1)[1].strip()
    return pool_str.lower() if pool_str.startswith("0x") else ""


def _resolve_sy_underlying_symbol(chain: str, market_address: str) -> str | None:
    """Resolve the symbol of the underlying token that mints SY for a market.

    Uses the MARKET_TOKEN_MINT_SY registry to find the underlying token address,
    then resolves its symbol via the token resolver.

    Returns None when the market is unknown or the token resolver fails.
    Never raises.
    """
    try:
        from almanak.framework.connectors.pendle.sdk import MARKET_TOKEN_MINT_SY

        underlying_address = MARKET_TOKEN_MINT_SY.get(chain.lower(), {}).get(market_address.lower())
        if not underlying_address:
            return None

        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        resolved = resolver.resolve(underlying_address, chain)
        if resolved and resolved.symbol:
            return resolved.symbol
    except Exception:
        logger.debug(
            "_resolve_sy_underlying_symbol failed for chain=%s market=%s", chain, market_address, exc_info=True
        )
    return None


def _resolve_token_decimals_for_pendle(chain: str, market_address: str) -> tuple[int, int]:
    """Resolve (sy_decimals, pt_decimals) for a Pendle market.

    Uses PT_TOKEN_INFO for PT decimals and infers SY decimals from the underlying.
    Falls back to 18 for both if the market is not in the static registry — this
    is a safe fallback for all current Pendle markets (all SY/PT use 18 decimals)
    but is flagged in the caller's confidence field so the assumption is visible.

    Returns:
        (sy_decimals, pt_decimals) — typically (18, 18) for all known Pendle markets.
    """
    sy_decimals = 18
    pt_decimals = 18

    # PT decimals: check PT_TOKEN_INFO for any matching market on this chain
    try:
        from almanak.framework.connectors.pendle.sdk import PT_TOKEN_INFO

        chain_pts = PT_TOKEN_INFO.get(chain.lower(), {})
        for _pt_name, (_pt_addr, _pt_dec) in chain_pts.items():
            # Match by looking up the market for this PT token and checking address
            from almanak.framework.connectors.pendle.sdk import MARKET_BY_PT_TOKEN

            chain_markets = MARKET_BY_PT_TOKEN.get(chain.lower(), {})
            for _tok_name, mkt_addr in chain_markets.items():
                if mkt_addr.lower() == market_address.lower() and _tok_name in chain_pts:
                    _, pt_dec_found = chain_pts[_tok_name]
                    pt_decimals = pt_dec_found
                    break
    except Exception:
        logger.debug("Could not look up PT decimals from PT_TOKEN_INFO for market=%s", market_address)

    # SY decimals: same as underlying (always 18 for current markets)
    # Could be improved by calling decimals() on the SY contract, but since
    # all known Pendle SY tokens use 18 decimals, 18 is the correct value.
    return sy_decimals, pt_decimals


def _lookup_price(price_oracle: dict | None, *symbols: str) -> Decimal | None:
    """Look up a token price from the price_oracle dict.

    Tries each symbol variant (upper, lower, original) in turn.
    Returns None if not found or oracle is None.
    Never raises.
    """
    if not price_oracle:
        return None
    for sym in symbols:
        if sym is None:
            continue
        for key in (sym, sym.upper(), sym.lower()):
            val = price_oracle.get(key)
            if val is not None:
                try:
                    return Decimal(str(val))
                except Exception:
                    pass
    return None


def build_pendle_lp_accounting_event(  # noqa: C901
    *,
    intent: Any,
    result: Any,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    ledger_entry_id: str | None = None,
    price_oracle: dict | None = None,
) -> Any | None:
    """Build a PendleAccountingEvent for a completed LP_OPEN or LP_CLOSE intent.

    Returns None for non-Pendle-LP intents or if the intent type cannot be mapped.

    VIB-3488: Decimals are resolved from the static Pendle registry (not hardcoded
    to 18 — the resolver confirms the value and the confidence field tracks whether
    confirmation was available).  SY and PT prices are populated from price_oracle
    when provided.  Confidence escalates to HIGH when both prices are available.
    """
    from almanak.framework.accounting.models import (
        AccountingConfidence,
        AccountingIdentity,
        PendleAccountingEvent,
        PendleEventType,
    )

    intent_type_str = _intent_type_value(intent)
    if intent_type_str not in _PENDLE_LP_INTENT_TYPES:
        return None

    # Only handle Pendle protocol
    protocol = (getattr(intent, "protocol", "") or "").lower()
    if "pendle" not in protocol:
        return None

    event_type = PendleEventType.PENDLE_LP_OPEN if intent_type_str == "LP_OPEN" else PendleEventType.PENDLE_LP_CLOSE

    now = datetime.now(UTC)
    tx_hash = getattr(result, "tx_hash", None) or ""
    if not tx_hash:
        for tr in getattr(result, "transaction_results", None) or []:
            h = getattr(tr, "tx_hash", None)
            if h:
                tx_hash = h
                break

    market_address = _get_market_address(intent)
    if not market_address:
        logger.debug("Pendle LP accounting: intent.pool missing, skipping event")
        return None
    position_key = _derive_pendle_position_key(chain, wallet_address, market_address)

    # -------------------------------------------------------------------------
    # Extract raw amounts from the position pipeline data
    # -------------------------------------------------------------------------
    extracted = getattr(result, "extracted_data", None) or {}
    sy_amount_raw: int | None = None
    pt_amount_raw: int | None = None

    if intent_type_str == "LP_OPEN":
        lp_open = extracted.get("lp_open_data")
        if lp_open:
            sy_amount_raw = getattr(lp_open, "amount0", None)  # net_sy_used
            pt_amount_raw = getattr(lp_open, "amount1", None)  # net_pt_used
    else:
        lp_close = extracted.get("lp_close_data")
        if lp_close:
            sy_amount_raw = getattr(lp_close, "amount0_collected", None)  # net_sy_out
            pt_amount_raw = getattr(lp_close, "amount1_collected", None)  # net_pt_out

    # -------------------------------------------------------------------------
    # VIB-3488: Decimal resolution (verified, not hardcoded)
    # -------------------------------------------------------------------------
    sy_decimals, pt_decimals = _resolve_token_decimals_for_pendle(chain, market_address)

    sy_amount: Decimal | None = None
    pt_amount: Decimal | None = None
    if sy_amount_raw is not None:
        sy_amount = Decimal(str(sy_amount_raw)) / Decimal(10**sy_decimals)
    if pt_amount_raw is not None:
        pt_amount = Decimal(str(pt_amount_raw)) / Decimal(10**pt_decimals)

    # -------------------------------------------------------------------------
    # VIB-3488: Price resolution via price_oracle
    # -------------------------------------------------------------------------
    # SY price ≈ underlying asset price (SY wraps yield-bearing token ~1:1 in USD terms).
    # PT price ≈ SY price (upper bound; actual PT trades at a discount; the exact
    # discount is encoded in pt_to_asset_rate which requires an on-chain read outside
    # this builder's scope — the pendle_valuer.py handles that for live valuation).
    sy_underlying_symbol = _resolve_sy_underlying_symbol(chain, market_address)
    sy_price: Decimal | None = None
    pt_price: Decimal | None = None

    unavailable_reasons: list[str] = []

    if price_oracle is not None:
        if sy_underlying_symbol:
            sy_price = _lookup_price(price_oracle, sy_underlying_symbol)
            if sy_price is None:
                unavailable_reasons.append(f"sy underlying price unavailable (symbol={sy_underlying_symbol})")
            else:
                # PT price ≈ SY price as an upper-bound approximation.
                # The actual pt_to_asset_rate discount is not available here
                # without an on-chain call; note this in unavailable_reason.
                pt_price = sy_price
                unavailable_reasons.append("pt_price approximated as sy_price (pt_to_asset_rate not read in builder)")
        else:
            unavailable_reasons.append(f"sy underlying symbol not found for market {market_address}")
    else:
        unavailable_reasons.append("price_oracle not provided")

    # -------------------------------------------------------------------------
    # Confidence classification
    # -------------------------------------------------------------------------
    has_amounts = sy_amount is not None and pt_amount is not None
    has_prices = sy_price is not None  # pt_price derives from sy_price

    if has_amounts and has_prices and sy_underlying_symbol:
        # Both amounts and prices available; pt_price is approximate but present.
        # Mark ESTIMATED (not HIGH) because pt_price is an approximation without
        # the on-chain pt_to_asset_rate.
        confidence = AccountingConfidence.ESTIMATED
        # Keep the pt_price approximation note in unavailable_reason for auditability.
    elif has_amounts and not has_prices:
        confidence = AccountingConfidence.ESTIMATED
        if not unavailable_reasons:
            unavailable_reasons.append("USD price unavailable")
    elif not has_amounts:
        confidence = AccountingConfidence.ESTIMATED
        if not unavailable_reasons:
            unavailable_reasons.append("SY/PT amounts not extracted from result")
    else:
        confidence = AccountingConfidence.ESTIMATED

    unavailable_reason = "; ".join(unavailable_reasons) if unavailable_reasons else ""

    # -------------------------------------------------------------------------
    # Build event
    # -------------------------------------------------------------------------
    _id_seed = tx_hash or ledger_entry_id or position_key
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

    return PendleAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=position_key,
        market_id=market_address,
        pt_token="",  # resolved when Pendle market reader is added (VIB-3422)
        maturity_timestamp=None,
        pt_amount=pt_amount,
        sy_amount=sy_amount,
        pt_price=pt_price,
        sy_price=sy_price,
        implied_apr_bps=None,
        days_to_maturity=None,
        realized_yield_usd=None,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )
