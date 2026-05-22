"""Vault category handler for AccountingProcessor (VIB-3472).

Ports logic from vault_accounting.py to work from ledger_row / outbox_row dicts
rather than live intent / result objects.  No live chain calls.

Fields sourced from ledger row; confidence is ESTIMATED because shares, share
price, and yield require a vault receipt parser not yet wired in.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, VaultEventType
from almanak.framework.accounting.vault_accounting import VaultAccountingEvent

logger = logging.getLogger(__name__)

_VAULT_TYPES = frozenset({"VAULT_DEPOSIT", "VAULT_WITHDRAW", "VAULT_REDEEM", "VAULT_HARVEST", "VAULT_REALLOCATE"})

_INTENT_TO_EVENT_TYPE: dict[str, VaultEventType] = {
    "VAULT_DEPOSIT": VaultEventType.VAULT_DEPOSIT,
    "VAULT_WITHDRAW": VaultEventType.VAULT_WITHDRAW,
    # VAULT_REDEEM maps to VAULT_WITHDRAW (matching old builder behaviour)
    "VAULT_REDEEM": VaultEventType.VAULT_WITHDRAW,
    "VAULT_HARVEST": VaultEventType.VAULT_HARVEST,
    # VAULT_REALLOCATE is classified as vault but has no dedicated VaultEventType;
    # map to VAULT_SNAPSHOT as the closest neutral type.
    "VAULT_REALLOCATE": VaultEventType.VAULT_SNAPSHOT,
}


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else None
    except Exception:  # noqa: BLE001
        return None


def _vault_address_from_position_key(position_key: str) -> str:
    """Extract the vault address (last ':' segment) from a position key.

    e.g. "vault:metamorpho:arbitrum:0xwallet:0xvaultaddr" → "0xvaultaddr"
    """
    if not position_key:
        return ""
    return position_key.rsplit(":", 1)[-1]


def handle_vault(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
) -> VaultAccountingEvent | None:
    """Build a VaultAccountingEvent from an outbox + ledger row pair.

    Returns None for non-vault intent types.

    All inputs come from the dicts — no live chain calls.
    Confidence is ESTIMATED because shares, share_price, and yield require a vault
    receipt parser not yet wired in.
    """
    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    if intent_type_str not in _VAULT_TYPES:
        return None

    event_type = _INTENT_TO_EVENT_TYPE.get(intent_type_str)
    if event_type is None:
        return None

    # ── Identity fields ──────────────────────────────────────────────────────
    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    protocol = (ledger_row.get("protocol") or "").lower()
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""
    position_key = outbox_row.get("position_key") or ""

    # ── Timestamp ────────────────────────────────────────────────────────────
    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        timestamp = datetime.now(UTC)

    # ── Vault address ────────────────────────────────────────────────────────
    # market_id from outbox is the canonical source (set at execution time by runner).
    # Fall back to last segment of position_key.
    vault_address = outbox_row.get("market_id") or _vault_address_from_position_key(position_key) or ""

    # ── Token / amount ───────────────────────────────────────────────────────
    asset_token = (ledger_row.get("token_in") or "").upper()

    # amount_in is already in human-decimal units in the ledger row.
    # Treat "all" or empty string as None (position-closing "redeem everything" case).
    amount_in_raw = ledger_row.get("amount_in") or ""
    assets_amount: Decimal | None = None
    if amount_in_raw and str(amount_in_raw).lower() not in ("all", ""):
        assets_amount = _safe_decimal(amount_in_raw)

    # ── Identity / ID ────────────────────────────────────────────────────────
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

    return VaultAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=position_key,
        vault_address=vault_address,
        asset_token=asset_token,
        assets_amount=assets_amount,
        shares_amount=None,
        share_price=None,
        cost_basis_usd=None,
        yield_usd=None,
        confidence=AccountingConfidence.ESTIMATED,
        unavailable_reason="shares, share_price, and yield require vault receipt parser (pending)",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Registry adapter (VIB-4163, T3)
# ──────────────────────────────────────────────────────────────────────────────

from almanak.framework.accounting.category_handlers import HandlerContext, register
from almanak.framework.primitives.types import AccountingCategory


@register(AccountingCategory.VAULT)
def _dispatch_vault(ctx: HandlerContext) -> VaultAccountingEvent | None:
    return handle_vault(ctx.outbox_row, ctx.ledger_row)
