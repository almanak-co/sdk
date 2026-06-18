"""Vault accounting event builder for ERC-4626 / MetaMorpho vault strategies (VIB-3517).

Covers VAULT_DEPOSIT and VAULT_REDEEM (MetaMorpho, yearn, ERC-4626 templates).
Fields are sourced from the intent (vault_address, amount, asset_token).
Share price and yield are not yet extractable from receipt data; confidence
is ESTIMATED until a vault receipt parser provides structured output.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.measured import encode_money_payload
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, VaultEventType

logger = logging.getLogger(__name__)

_VAULT_INTENT_TYPES = frozenset({"VAULT_DEPOSIT", "VAULT_REDEEM"})


class VaultAccountingEvent:
    """Duck-typed vault accounting event consumed by AccountingWriter and both backends."""

    schema_version: int = 1
    # VIB-4166 (T6) — see ``almanak.framework.accounting.payload_schemas`` module
    # docstring for the bump policy. Class attribute so the augment chokepoint
    # has a sane fallback when writers don't override it; the chokepoint
    # overwrites with the canonical per-primitive value at write time.
    primitive_version: int = 1

    def __init__(
        self,
        identity: AccountingIdentity,
        event_type: VaultEventType,
        position_key: str,
        vault_address: str,
        asset_token: str,
        assets_amount: Decimal | None,
        shares_amount: Decimal | None,
        share_price: Decimal | None,
        cost_basis_usd: Decimal | None,
        yield_usd: Decimal | None,
        confidence: AccountingConfidence,
        unavailable_reason: str = "",
    ) -> None:
        self.identity = identity
        self.event_type = event_type.value
        self.position_key = position_key
        self.vault_address = vault_address
        self.asset_token = asset_token
        self.assets_amount = assets_amount
        self.shares_amount = shares_amount
        self.share_price = share_price
        self.cost_basis_usd = cost_basis_usd
        self.yield_usd = yield_usd
        self.confidence = confidence
        self.unavailable_reason = unavailable_reason

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                # VIB-5213 (US-007): money crosses the serialization seam as a
                # MeasuredMoney. Byte-identical to ``str(v)`` for finite Decimals.
                return encode_money_payload(v)
            return v

        return json.dumps(
            {
                "event_type": self.event_type,
                "position_key": self.position_key,
                "vault_address": self.vault_address,
                "asset_token": self.asset_token,
                "assets_amount": _enc(self.assets_amount),
                "shares_amount": _enc(self.shares_amount),
                "share_price": _enc(self.share_price),
                "cost_basis_usd": _enc(self.cost_basis_usd),
                "yield_usd": _enc(self.yield_usd),
                "confidence": str(self.confidence),
                "unavailable_reason": self.unavailable_reason,
                "schema_version": self.schema_version,
                "primitive_version": self.primitive_version,
            }
        )


def _intent_type_str(intent: Any) -> str:
    it = getattr(intent, "intent_type", None)
    if it is None:
        return ""
    return it.value if hasattr(it, "value") else str(it)


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else None
    except Exception:  # noqa: BLE001
        return None


def build_vault_accounting_event(
    *,
    intent: Any,
    result: Any,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    ledger_entry_id: str | None = None,
) -> VaultAccountingEvent | None:
    """Build a VaultAccountingEvent for a completed VAULT_DEPOSIT or VAULT_REDEEM intent.

    Returns None for non-vault intents.

    assets_amount is taken from the intent's amount field (before execution).
    shares_amount, share_price, and yield are not yet extracted from receipts;
    they are left as None (confidence ESTIMATED) until a vault receipt parser is added.
    """
    intent_type_str = _intent_type_str(intent)
    if intent_type_str not in _VAULT_INTENT_TYPES:
        return None

    event_type = VaultEventType.VAULT_DEPOSIT if intent_type_str == "VAULT_DEPOSIT" else VaultEventType.VAULT_WITHDRAW
    protocol = (getattr(intent, "protocol", "") or "").lower()
    now = datetime.now(UTC)

    tx_hash = getattr(result, "tx_hash", None) or ""
    if not tx_hash:
        for tr in getattr(result, "transaction_results", None) or []:
            h = getattr(tr, "tx_hash", None)
            if h:
                tx_hash = h
                break

    vault_address = (getattr(intent, "vault_address", None) or "").lower()
    asset_token = str(getattr(intent, "asset_token", None) or getattr(intent, "token", None) or "")

    # Amount in human-decimal (the intent amount is already in user-facing units).
    amount_raw = getattr(intent, "amount", None)
    assets_amount: Decimal | None = None
    if isinstance(amount_raw, Decimal):
        assets_amount = amount_raw if amount_raw.is_finite() else None
    elif amount_raw is not None and str(amount_raw) not in ("all", ""):
        assets_amount = _safe_decimal(amount_raw)

    position_key = f"vault:{protocol}:{chain.lower()}:{wallet_address.lower()}:{vault_address}"

    _id_seed = tx_hash or ledger_entry_id or str(uuid4())
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
