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
from dataclasses import dataclass
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
from almanak.framework.models.run_mode import RunMode

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


def _parse_state_json(state_json: str) -> dict[str, Any] | None:
    """Parse a pre_state_json or post_state_json blob into a dict.

    Both columns share the same schema (produced by
    :func:`almanak.framework.accounting.lending_accounting.lending_state_to_dict`),
    so a single parser serves both lanes. Returns ``None`` when empty or
    invalid — callers fall through to leaving the corresponding event
    fields ``None`` (Empty ≠ zero per CLAUDE.md).
    """
    if not state_json:
        return None
    try:
        d = json.loads(state_json)
        return d if isinstance(d, dict) else None
    except (json.JSONDecodeError, TypeError):
        return None


@dataclass
class _FifoContext:
    """Inputs shared by every per-intent FIFO branch (§12.1)."""

    deployment_id: str
    cycle_id: str
    position_key: str
    asset: str
    amount_human: Decimal
    price_oracle: dict[str, Any]
    timestamp: datetime
    tx_hash: str
    ledger_entry_id: str
    swap_wallet_key: str


@dataclass
class _FifoDeltas:
    """Principal/interest split for one lending action (§10.11)."""

    principal_delta_usd: Decimal | None = None
    interest_delta_usd: Decimal | None = None


@dataclass
class _LendingSnapshots:
    """Pre/post position snapshots; None means unmeasured (§10.10)."""

    collateral_before: Decimal | None = None
    debt_before: Decimal | None = None
    net_equity_before: Decimal | None = None
    hf_before: Decimal | None = None
    collateral_after: Decimal | None = None
    debt_after: Decimal | None = None
    net_equity_after: Decimal | None = None
    hf_after: Decimal | None = None
    liquidation_threshold: Decimal | None = None


def _resolve_timestamp(raw_ts: Any) -> datetime:
    """Ledger timestamp with now() fallback (§10.8 identity header)."""
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        return datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        return datetime.now(UTC)


def _parse_gas_usd(ledger_row: dict[str, Any]) -> Decimal | None:
    """Gas cost in USD; None when absent or unparseable (§10.10)."""
    gas_usd_raw = ledger_row.get("gas_usd")
    if gas_usd_raw:
        try:
            return Decimal(str(gas_usd_raw))
        except Exception:
            pass
    return None


def _resolve_asset_with_fallback(extracted: dict[str, Any], ledger_row: dict[str, Any]) -> str:
    """Primary asset from extracted_data, else ledger token_in (§10.11)."""
    asset = _extract_asset(extracted)
    if asset == "UNKNOWN":
        asset = (ledger_row.get("token_in") or "").upper() or "UNKNOWN"
    return asset


def _resolve_position_key_with_fallback(
    outbox_row: dict[str, Any],
    protocol: str,
    chain: str,
    wallet_address: str,
    asset: str,
    position_key: str,
) -> str:
    """Outbox position_key, else derived per-market key (§10.11)."""
    if not position_key:
        market_id_fallback = outbox_row.get("market_id") or None
        position_key = _derive_position_key(protocol, chain, wallet_address, market_id_fallback, asset)
    return position_key


def _swap_wallet_key_for(chain: str, wallet_address: str) -> str:
    """Shared SWAP/lending wallet-basis pool key (§12.1, VIB-3964)."""
    chain_norm = (chain or "").lower().strip()
    wallet_norm = (wallet_address or "").lower().strip()
    return f"swap:{chain_norm}:{wallet_norm}" if chain_norm and wallet_norm else ""


def _fifo_borrow(ctx: _FifoContext, basis_store: FIFOBasisStore) -> _FifoDeltas:
    """BORROW: record debt lot + credit wallet-basis pool (§12.1)."""
    principal_delta_usd = _amount_to_usd(ctx.amount_human, ctx.price_oracle, ctx.asset)
    borrow_id_seed = ctx.tx_hash or ctx.ledger_entry_id or ctx.position_key
    basis_store.record_borrow(
        deployment_id=ctx.deployment_id,
        position_key=ctx.position_key,
        token=ctx.asset,
        principal_amount=ctx.amount_human,
        principal_usd=principal_delta_usd,
        timestamp=ctx.timestamp,
        lot_id=make_accounting_event_id(
            ctx.deployment_id, ctx.cycle_id, "BORROW_LOT", borrow_id_seed, ctx.position_key
        ),
        source_ledger_entry_id=ctx.ledger_entry_id,
    )
    if ctx.swap_wallet_key:
        basis_store.record_swap_acquisition(
            deployment_id=ctx.deployment_id,
            position_key=ctx.swap_wallet_key,
            token=ctx.asset,
            amount=ctx.amount_human,
            cost_usd=principal_delta_usd,
            timestamp=ctx.timestamp,
            lot_id=make_accounting_event_id(
                ctx.deployment_id, ctx.cycle_id, "BORROW_WALLET_LOT", borrow_id_seed, ctx.asset
            ),
            source="BORROW",
        )
    return _FifoDeltas(principal_delta_usd=principal_delta_usd, interest_delta_usd=None)


def _fifo_repay(ctx: _FifoContext, basis_store: FIFOBasisStore) -> _FifoDeltas:
    """REPAY/DELEVERAGE: FIFO-match debt lots, drain wallet pool (§12.1)."""
    match_result = basis_store.match_repay(
        deployment_id=ctx.deployment_id,
        position_key=ctx.position_key,
        token=ctx.asset,
        repay_amount=ctx.amount_human,
    )
    principal_delta_usd = _amount_to_usd(match_result.repaid_principal, ctx.price_oracle, ctx.asset)
    interest_delta_usd = (
        None
        if match_result.unmatched_amount > 0
        else _amount_to_usd(match_result.interest_or_yield, ctx.price_oracle, ctx.asset)
    )
    if ctx.swap_wallet_key:
        basis_store.match_swap_disposal(
            deployment_id=ctx.deployment_id,
            position_key=ctx.swap_wallet_key,
            token=ctx.asset,
            amount=ctx.amount_human,
        )
    return _FifoDeltas(principal_delta_usd=principal_delta_usd, interest_delta_usd=interest_delta_usd)


def _fifo_supply(ctx: _FifoContext, basis_store: FIFOBasisStore) -> _FifoDeltas:
    """SUPPLY: drain wallet pool + record supply principal lot (§12.1)."""
    principal_delta_usd = _amount_to_usd(ctx.amount_human, ctx.price_oracle, ctx.asset)
    if ctx.swap_wallet_key:
        basis_store.match_swap_disposal(
            deployment_id=ctx.deployment_id,
            position_key=ctx.swap_wallet_key,
            token=ctx.asset,
            amount=ctx.amount_human,
        )
    supply_position_key = f"supply:{ctx.position_key}"
    supply_id_seed = ctx.tx_hash or ctx.ledger_entry_id or ctx.position_key
    basis_store.record_borrow(
        deployment_id=ctx.deployment_id,
        position_key=supply_position_key,
        token=ctx.asset,
        principal_amount=ctx.amount_human,
        principal_usd=principal_delta_usd,
        timestamp=ctx.timestamp,
        lot_id=make_accounting_event_id(
            ctx.deployment_id, ctx.cycle_id, "SUPPLY_LOT", supply_id_seed, supply_position_key
        ),
        source_ledger_entry_id=ctx.ledger_entry_id,
    )
    return _FifoDeltas(principal_delta_usd=principal_delta_usd, interest_delta_usd=None)


def _fifo_withdraw(ctx: _FifoContext, basis_store: FIFOBasisStore) -> _FifoDeltas:
    """WITHDRAW: credit wallet pool + FIFO-split principal vs interest (§12.1)."""
    withdraw_total_usd = _amount_to_usd(ctx.amount_human, ctx.price_oracle, ctx.asset)
    if ctx.swap_wallet_key:
        withdraw_id_seed = ctx.tx_hash or ctx.ledger_entry_id or ctx.position_key
        basis_store.record_swap_acquisition(
            deployment_id=ctx.deployment_id,
            position_key=ctx.swap_wallet_key,
            token=ctx.asset,
            amount=ctx.amount_human,
            cost_usd=withdraw_total_usd,
            timestamp=ctx.timestamp,
            lot_id=make_accounting_event_id(
                ctx.deployment_id, ctx.cycle_id, "WITHDRAW_WALLET_LOT", withdraw_id_seed, ctx.asset
            ),
            source="WITHDRAW",
        )
    supply_position_key = f"supply:{ctx.position_key}"
    supply_match = basis_store.match_repay(
        deployment_id=ctx.deployment_id,
        position_key=supply_position_key,
        token=ctx.asset,
        repay_amount=ctx.amount_human,
    )
    return _split_withdraw_deltas(ctx, supply_match, withdraw_total_usd)


def _split_withdraw_deltas(ctx: _FifoContext, supply_match: Any, withdraw_total_usd: Decimal | None) -> _FifoDeltas:
    """Trust the FIFO interest split only when plausibly bounded (§10.10).

    The matcher reports ``unmatched=0`` whenever it consumed at least one
    lot, even when tracked supply covers only part of the withdraw. Past a
    100%-of-principal implied yield the residual is almost certainly
    untracked supply, so leave interest unmeasured instead of fabricating it.
    """
    if supply_match.unmatched_amount > 0:
        return _FifoDeltas(principal_delta_usd=withdraw_total_usd, interest_delta_usd=None)
    if (
        supply_match.repaid_principal >= ctx.amount_human
        or supply_match.interest_or_yield <= supply_match.repaid_principal
    ):
        return _FifoDeltas(
            principal_delta_usd=_amount_to_usd(supply_match.repaid_principal, ctx.price_oracle, ctx.asset),
            interest_delta_usd=_amount_to_usd(supply_match.interest_or_yield, ctx.price_oracle, ctx.asset),
        )
    return _FifoDeltas(principal_delta_usd=withdraw_total_usd, interest_delta_usd=None)


def _apply_fifo_lots(
    intent_type_str: str,
    ctx: _FifoContext | None,
    basis_store: FIFOBasisStore | None,
) -> _FifoDeltas:
    """Dispatch one per-intent FIFO branch (§12.1); no-op when unmeasurable."""
    if ctx is None or basis_store is None:
        return _FifoDeltas()
    if intent_type_str == "BORROW":
        return _fifo_borrow(ctx, basis_store)
    if intent_type_str in ("REPAY", "DELEVERAGE"):
        return _fifo_repay(ctx, basis_store)
    if intent_type_str == "SUPPLY":
        return _fifo_supply(ctx, basis_store)
    if intent_type_str == "WITHDRAW":
        return _fifo_withdraw(ctx, basis_store)
    return _FifoDeltas()


def _snapshot_from_state(
    state: dict[str, Any] | None, *, log_msg: str
) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    """Collateral/debt/net-equity/health-factor quad; None when unmeasured (§10.10).

    Single-try shape mirrors the pre-refactor inline blocks: fields parsed
    before a failing field keep their values, later fields stay None.
    """
    collateral: Decimal | None = None
    debt: Decimal | None = None
    net_equity: Decimal | None = None
    hf: Decimal | None = None
    if not state:
        return collateral, debt, net_equity, hf
    try:
        collateral = Decimal(str(state["collateral_usd"])) if state.get("collateral_usd") is not None else None
        debt = Decimal(str(state["debt_usd"])) if state.get("debt_usd") is not None else None
        if collateral is not None and debt is not None:
            net_equity = collateral - debt
        hf_raw = state.get("health_factor")
        hf = Decimal(str(hf_raw)) if hf_raw is not None else None
    except Exception:
        logger.debug(log_msg, exc_info=True)
    return collateral, debt, net_equity, hf


def _extract_snapshots(pre_state: dict[str, Any] | None, post_state: dict[str, Any] | None) -> _LendingSnapshots:
    """Pre/post snapshots with lane symmetry; failures stay unmeasured (§10.10)."""
    snapshots = _LendingSnapshots()
    snapshots.collateral_before, snapshots.debt_before, snapshots.net_equity_before, snapshots.hf_before = (
        _snapshot_from_state(pre_state, log_msg="Failed to parse pre_state_json fields")
    )
    snapshots.collateral_after, snapshots.debt_after, snapshots.net_equity_after, snapshots.hf_after = (
        _snapshot_from_state(post_state, log_msg="Failed to parse post_state_json fields")
    )
    if post_state:
        try:
            lt_bps = post_state.get("liquidation_threshold_bps")
            snapshots.liquidation_threshold = Decimal(lt_bps) / Decimal("10000") if lt_bps is not None else None
        except Exception:
            logger.debug("Failed to parse post_state_json fields", exc_info=True)
    return snapshots


def _confidence_for_post_state(snapshots: _LendingSnapshots) -> tuple[AccountingConfidence, str]:
    """HIGH when post-state measured, else ESTIMATED with reason (§10.9)."""
    has_post_state = snapshots.collateral_after is not None or snapshots.hf_after is not None
    if has_post_state:
        return AccountingConfidence.HIGH, ""
    return (
        AccountingConfidence.ESTIMATED,
        "post_state_json missing or invalid (gateway read unavailable for this row)",
    )


@dataclass
class _RowIds:
    """Persisted identity columns for one lending row (§10.8)."""

    deployment_id: str
    cycle_id: str
    execution_mode: Any
    chain: str
    protocol: str
    tx_hash: str
    ledger_entry_id: str
    wallet_address: str
    position_key: str


def _resolve_row_ids(ledger_row: dict[str, Any], outbox_row: dict[str, Any]) -> _RowIds:
    """Ledger-first identity with outbox fallback (§10.8).

    Parses (and validates) the persisted execution mode before any FIFO
    mutation, so a malformed legacy row fails without consuming lots.
    """
    return _RowIds(
        deployment_id=ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or "",
        cycle_id=ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or "",
        execution_mode=RunMode.parse_optional(ledger_row.get("execution_mode")),
        chain=ledger_row.get("chain") or "",
        protocol=ledger_row.get("protocol") or "",
        tx_hash=ledger_row.get("tx_hash") or "",
        ledger_entry_id=ledger_row.get("id") or "",
        wallet_address=outbox_row.get("wallet_address") or "",
        position_key=outbox_row.get("position_key") or "",
    )


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

    # Validate persisted identity before any FIFO mutation.  A malformed
    # legacy row must fail without consuming or recording basis lots; the
    # outbox processor will mark that row failed and can safely retry it.
    ids = _resolve_row_ids(ledger_row, outbox_row)
    deployment_id = ids.deployment_id
    cycle_id = ids.cycle_id
    execution_mode = ids.execution_mode
    chain = ids.chain
    protocol = ids.protocol
    tx_hash = ids.tx_hash
    ledger_entry_id = ids.ledger_entry_id
    wallet_address = ids.wallet_address
    position_key = ids.position_key

    # Timestamp from ledger row; fall back to now() only as last resort.
    timestamp = _resolve_timestamp(ledger_row.get("timestamp"))

    # Deserialize extracted_data and price_oracle from JSON fields. The
    # tolerant ``parse_price_inputs`` (VIB-3885) returns a flat
    # ``{SYMBOL: Decimal}`` dict regardless of whether the ledger wrote the
    # canonical nested shape or the legacy flat shape.
    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")
    price_oracle = parse_price_inputs(ledger_row.get("price_inputs_json"))
    pre_state = _parse_state_json(ledger_row.get("pre_state_json") or "")
    post_state = _parse_state_json(ledger_row.get("post_state_json") or "")

    # Resolve asset: extracted_data first, then ledger row token_in as fallback.
    # Normal enriched lending results store the amount in borrow_amount/supply_amount
    # but debt_token may not be in extracted_data — token_in on the ledger row is the
    # reliable fallback (it's the borrowed/supplied asset symbol for lending intents).
    asset = _resolve_asset_with_fallback(extracted, ledger_row)

    # If position_key wasn't stored in the outbox row, derive it using market_id from the
    # outbox row so per-market protocols (Morpho Blue) produce distinct FIFO keys.
    position_key = _resolve_position_key_with_fallback(outbox_row, protocol, chain, wallet_address, asset, position_key)

    # ── Token amount from extracted_data ────────────────────────────────────────
    amount_human = _extract_amount_human(extracted, intent_type_str, chain, asset)

    # ── APRs ────────────────────────────────────────────────────────────────────
    supply_apr_bps = _ray_to_bps(extracted.get("supply_rate"))
    borrow_apr_bps = _ray_to_bps(extracted.get("borrow_rate"))

    # ── Gas ─────────────────────────────────────────────────────────────────────
    gas_usd = _parse_gas_usd(ledger_row)

    # ── FIFO lot matching ────────────────────────────────────────────────────────
    # A single chain+wallet wallet-basis pool is shared across the SWAP
    # handler and the lending handler — BORROW / WITHDRAW credit it,
    # SUPPLY / REPAY drain it. Mirroring on-chain wallet flow into the FIFO
    # store is what lets a SWAP that disposes a borrowed (or withdrawn) token
    # report a non-null ``realized_pnl_usd``.
    fifo_ctx: _FifoContext | None = None
    if amount_human is not None:
        fifo_ctx = _FifoContext(
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            position_key=position_key,
            asset=asset,
            amount_human=amount_human,
            price_oracle=price_oracle,
            timestamp=timestamp,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            swap_wallet_key=_swap_wallet_key_for(chain, wallet_address),
        )
    deltas = _apply_fifo_lots(intent_type_str, fifo_ctx, basis_store)
    principal_delta_usd = deltas.principal_delta_usd
    interest_delta_usd = deltas.interest_delta_usd

    # ── Pre/post-state (populated by the runner; failures stay unmeasured) ─────
    snapshots = _extract_snapshots(pre_state, post_state)

    confidence, unavailable_reason = _confidence_for_post_state(snapshots)

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

    return LendingAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=position_key,
        market_id=outbox_row.get("market_id") or "",
        asset=asset,
        collateral_value_before_usd=snapshots.collateral_before,
        collateral_value_after_usd=snapshots.collateral_after,
        debt_value_before_usd=snapshots.debt_before,
        debt_value_after_usd=snapshots.debt_after,
        net_equity_before_usd=snapshots.net_equity_before,
        net_equity_after_usd=snapshots.net_equity_after,
        health_factor_before=snapshots.hf_before,
        health_factor_after=snapshots.hf_after,
        liquidation_threshold=snapshots.liquidation_threshold,
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
    # MorphoMay15 §6.2 (F2): per-intent fallback to the collateral-side key
    # when the primary loan-side key is absent. Morpho Blue isolated-market
    # SUPPLY intents emit ``SupplyCollateral`` (not ``Supply``); the enricher
    # surfaces the amount as ``supply_collateral_amount`` via the morpho_blue
    # overlay in ``ResultEnricher.EXTRACTION_SPECS_BY_PROTOCOL``. Without this
    # fallback the live writer silently produces ``amount_token=None``.
    # VIB-4635 wires the symmetric WITHDRAW leg: collateral withdrawals emit
    # ``WithdrawCollateral`` (not the loan-side ``Withdraw``), so the
    # ``withdraw_amount`` key is absent; the enricher now surfaces the amount
    # as ``withdraw_collateral_amount`` (via the parser's
    # ``extract_withdraw_collateral_amount``). Without this fallback a Morpho
    # collateral WITHDRAW recorded ``amount_token=None`` even though the amount
    # is known exactly on-chain (Empty ≠ Zero ≠ None).
    _COLLATERAL_FALLBACK_BY_INTENT: dict[str, str] = {
        "SUPPLY": "supply_collateral_amount",
        "WITHDRAW": "withdraw_collateral_amount",
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
        fallback_key = _COLLATERAL_FALLBACK_BY_INTENT.get(intent_type_str)
        if fallback_key is not None:
            v = extracted.get(fallback_key)
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


# ──────────────────────────────────────────────────────────────────────────────
# Registry adapter (VIB-4163, T3)
# ──────────────────────────────────────────────────────────────────────────────

from almanak.framework.accounting.category_handlers import HandlerContext, register
from almanak.framework.primitives.types import AccountingCategory


@register(AccountingCategory.LENDING)
def _dispatch_lending(ctx: HandlerContext) -> LendingAccountingEvent | None:
    """Adapter that converts ``HandlerContext`` to the legacy ``handle_lending`` signature.

    Kept thin so the legacy public function stays usable by existing tests
    (``test_lending_accounting.py`` etc.) without signature changes.
    """
    return handle_lending(ctx.outbox_row, ctx.ledger_row, ctx.basis_store)
