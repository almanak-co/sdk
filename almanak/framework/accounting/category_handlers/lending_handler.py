"""Lending category handler for AccountingProcessor.

Reads all inputs from the ledger row (extracted_data_json, price_inputs_json,
post_state_json) — no live chain calls.  Ports the math from lending_accounting.py.

Post-state fields (collateral, debt, health factor) are populated from
post_state_json. The runner captures lending protocol state via
``capture_lending_post_state()`` and serialises it into the ledger row at write
time (VIB-3474). When the read fails (gateway error, unsupported protocol,
non-lending intent), post_state stays empty and confidence falls back to
ESTIMATED with an unavailable_reason — never fabricated.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.accounting.category_handlers._price_helpers import parse_price_inputs
from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.lending_accounting import (
    _amount_to_usd,
    _derive_position_key,
    _ray_to_bps,
)
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
)

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

logger = logging.getLogger(__name__)

_INTENT_TO_EVENT_TYPE: dict[str, LendingEventType] = {
    "SUPPLY": LendingEventType.SUPPLY,
    "BORROW": LendingEventType.BORROW,
    "REPAY": LendingEventType.REPAY,
    "DELEVERAGE": LendingEventType.DELEVERAGE,
    "WITHDRAW": LendingEventType.WITHDRAW,
}


def _parse_post_state(post_state_json: str) -> dict[str, Any] | None:
    """Parse post_state_json into a dict. Returns None when empty or invalid."""
    if not post_state_json:
        return None
    try:
        d = json.loads(post_state_json)
        return d if isinstance(d, dict) else None
    except (json.JSONDecodeError, TypeError):
        return None


def handle_lending(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
    basis_store: FIFOBasisStore,
) -> LendingAccountingEvent | None:
    """Build a LendingAccountingEvent from an outbox + ledger row pair.

    All inputs are read from the ledger row fields — no live chain calls.
    Returns None for non-lending intent types.

    The outbox_row provides: wallet_address, position_key (pre-computed by runner).
    The ledger_row provides: all other fields.

    FIFO lot management:
      - BORROW: record_borrow on basis_store with deterministic lot_id
      - REPAY: match_repay on basis_store
      - Others: principal_delta_usd from price_oracle

    Called from AccountingProcessor.drain_one after idempotency check.
    """
    from almanak.framework.observability.ledger import deserialize_extracted_data

    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    event_type = _INTENT_TO_EVENT_TYPE.get(intent_type_str)
    if event_type is None:
        return None

    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    strategy_id = ledger_row.get("strategy_id") or outbox_row.get("strategy_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    protocol = ledger_row.get("protocol") or ""
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""
    position_key = outbox_row.get("position_key") or ""

    # Timestamp from ledger row; fall back to now() only as last resort.
    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        timestamp = datetime.now(UTC)

    # Deserialize extracted_data and price_oracle from JSON fields. The
    # tolerant ``parse_price_inputs`` (VIB-3885) returns a flat
    # ``{SYMBOL: Decimal}`` dict regardless of whether the ledger wrote the
    # canonical nested shape or the legacy flat shape.
    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")
    price_oracle = parse_price_inputs(ledger_row.get("price_inputs_json"))
    post_state = _parse_post_state(ledger_row.get("post_state_json") or "")

    # Resolve asset: extracted_data first, then ledger row token_in as fallback.
    # Normal enriched lending results store the amount in borrow_amount/supply_amount
    # but debt_token may not be in extracted_data — token_in on the ledger row is the
    # reliable fallback (it's the borrowed/supplied asset symbol for lending intents).
    asset = _extract_asset(extracted)
    if asset == "UNKNOWN":
        asset = (ledger_row.get("token_in") or "").upper() or "UNKNOWN"

    # If position_key wasn't stored in the outbox row, derive it using market_id from the
    # outbox row so per-market protocols (Morpho Blue) produce distinct FIFO keys.
    if not position_key:
        market_id_fallback = outbox_row.get("market_id") or None
        position_key = _derive_position_key(protocol, chain, wallet_address, market_id_fallback, asset)

    # ── Token amount from extracted_data ────────────────────────────────────────
    amount_human = _extract_amount_human(extracted, intent_type_str, chain, asset)

    # ── APRs ────────────────────────────────────────────────────────────────────
    supply_apr_bps = _ray_to_bps(extracted.get("supply_rate"))
    borrow_apr_bps = _ray_to_bps(extracted.get("borrow_rate"))

    # ── Gas ─────────────────────────────────────────────────────────────────────
    gas_usd: Decimal | None = None
    gas_usd_raw = ledger_row.get("gas_usd")
    if gas_usd_raw:
        try:
            gas_usd = Decimal(str(gas_usd_raw))
        except Exception:
            pass

    # ── FIFO lot matching ────────────────────────────────────────────────────────
    principal_delta_usd: Decimal | None = None
    interest_delta_usd: Decimal | None = None

    if amount_human is not None and basis_store is not None:
        if intent_type_str == "BORROW":
            principal_delta_usd = _amount_to_usd(amount_human, price_oracle, asset)
            _borrow_id_seed = tx_hash or ledger_entry_id or position_key
            basis_store.record_borrow(
                deployment_id=deployment_id,
                position_key=position_key,
                token=asset,
                principal_amount=amount_human,
                principal_usd=principal_delta_usd,
                timestamp=timestamp,
                lot_id=make_accounting_event_id(deployment_id, cycle_id, "BORROW_LOT", _borrow_id_seed, position_key),
                source_ledger_entry_id=ledger_entry_id,
            )
            interest_delta_usd = None

        elif intent_type_str in ("REPAY", "DELEVERAGE"):
            match_result = basis_store.match_repay(
                deployment_id=deployment_id,
                position_key=position_key,
                token=asset,
                repay_amount=amount_human,
            )
            principal_delta_usd = _amount_to_usd(match_result.repaid_principal, price_oracle, asset)
            interest_delta_usd = (
                None
                if match_result.unmatched_amount > 0
                else _amount_to_usd(match_result.interest_or_yield, price_oracle, asset)
            )

        elif intent_type_str in ("SUPPLY", "WITHDRAW"):
            principal_delta_usd = _amount_to_usd(amount_human, price_oracle, asset)

    # ── Post-state from post_state_json (VIB-3474: populated by the runner) ─────
    collateral_after: Decimal | None = None
    debt_after: Decimal | None = None
    net_equity_after: Decimal | None = None
    hf_after: Decimal | None = None
    liquidation_threshold: Decimal | None = None

    if post_state:
        try:
            collateral_after = (
                Decimal(str(post_state["collateral_usd"])) if post_state.get("collateral_usd") is not None else None
            )
            debt_after = Decimal(str(post_state["debt_usd"])) if post_state.get("debt_usd") is not None else None
            if collateral_after is not None and debt_after is not None:
                net_equity_after = collateral_after - debt_after
            hf_raw = post_state.get("health_factor")
            hf_after = Decimal(str(hf_raw)) if hf_raw is not None else None
            lt_bps = post_state.get("liquidation_threshold_bps")
            liquidation_threshold = Decimal(lt_bps) / Decimal("10000") if lt_bps is not None else None
        except Exception:
            logger.debug("Failed to parse post_state_json fields", exc_info=True)

    has_post_state = collateral_after is not None or hf_after is not None
    confidence = AccountingConfidence.HIGH if has_post_state else AccountingConfidence.ESTIMATED
    unavailable_reason = (
        "" if has_post_state else "post_state_json missing or invalid (gateway read unavailable for this row)"
    )

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

    return LendingAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=position_key,
        market_id=outbox_row.get("market_id") or "",
        asset=asset,
        collateral_value_before_usd=None,
        collateral_value_after_usd=collateral_after,
        debt_value_before_usd=None,
        debt_value_after_usd=debt_after,
        net_equity_before_usd=None,
        net_equity_after_usd=net_equity_after,
        health_factor_before=None,
        health_factor_after=hf_after,
        liquidation_threshold=liquidation_threshold,
        lltv=None,
        supply_apr_bps=supply_apr_bps,
        borrow_apr_bps=borrow_apr_bps,
        principal_delta_usd=principal_delta_usd,
        interest_delta_usd=interest_delta_usd,
        gas_usd=gas_usd,
        amount_token=amount_human,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────


def _extract_asset(extracted: dict[str, Any]) -> str:
    """Extract the primary asset from deserialized extracted_data.

    Checks typed dataclass objects first (BorrowData, SupplyData), then
    falls back to raw string fields.
    """
    for key in ("borrow_data", "supply_data"):
        obj = extracted.get(key)
        if obj is not None:
            token = getattr(obj, "token", None) or (obj.get("token") if isinstance(obj, dict) else None)
            if token:
                return str(token)
    for key in ("borrow_token", "supply_token", "token", "asset"):
        v = extracted.get(key)
        if v:
            return str(v)
    return "UNKNOWN"


def _extract_amount_human(
    extracted: dict[str, Any],
    intent_type_str: str,
    chain: str,
    asset: str = "UNKNOWN",
) -> Decimal | None:
    """Extract human-decimal token amount from deserialized extracted_data.

    Priority: raw int from amount fields → token resolver for decimal scaling.
    ``asset`` should be pre-resolved by the caller via _extract_asset + ledger fallback.
    """
    _AMOUNT_KEY_BY_INTENT: dict[str, str] = {
        "BORROW": "borrow_amount",
        "SUPPLY": "supply_amount",
        "REPAY": "repay_amount",
        "DELEVERAGE": "repay_amount",
        "WITHDRAW": "withdraw_amount",
    }
    raw_amount: int | None = None
    primary_key = _AMOUNT_KEY_BY_INTENT.get(intent_type_str)
    if primary_key is not None:
        v = extracted.get(primary_key)
        if v is not None:
            try:
                raw_amount = int(v)
            except (TypeError, ValueError):
                pass

    if raw_amount is None:
        return None

    if not asset or asset == "UNKNOWN":
        logger.debug("_extract_amount_human: asset unknown, cannot scale raw amount")
        return None

    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        token_info = resolver.resolve(asset, chain=chain)
        if token_info is None:
            logger.debug("_extract_amount_human: resolver returned None for %s on %s", asset, chain)
            return None
        return Decimal(str(raw_amount)) / Decimal(10**token_info.decimals)
    except Exception:
        logger.debug("token decimal resolution failed for %s on %s", asset, chain)
        return None
