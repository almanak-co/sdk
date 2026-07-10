"""Typed accounting event for vault SETTLEMENT (Lagoon operator side) — VIB-5666.

A vault SETTLEMENT is the strategy — which *is* a Lagoon ERC-7540 vault — running
``settleDeposit`` / ``settleRedeem`` to issue shares against pending depositor
capital or burn redeem shares and return assets. Historically these txs landed
on-chain and produced **zero** rows in ``transaction_ledger`` /
``accounting_events`` because :mod:`almanak.framework.vault.lifecycle` called
``ExecutionOrchestrator.execute`` directly, bypassing the commit/accounting
pipeline (the identical "pre-``decide()`` therefore unaccounted" hole that bit
teardown, but firing every settlement interval).

Design rules (mirrors :class:`~almanak.framework.accounting.vault_accounting.VaultAccountingEvent`):

* **Capital event, NOT a return.** Depositor inflows/outflows are principal, not
  strategy PnL. This model deliberately carries **no** ``principal_delta_usd`` /
  ``realized_pnl_usd`` / ``cost_basis_usd`` field, so no position- or
  portfolio-level PnL fold (``accounting.position_pnl.compute_position_pnl`` and
  friends, which match specific event-type strings) can read a deposit as profit
  or a redemption as loss. The exact receipt-measured deltas
  (``assets_delta`` / ``shares_delta``) are recorded for downstream capital-flow
  accounting (``portfolio_metrics.deposits_usd`` / ``withdrawals_usd``) without
  contaminating returns.
* **Empty ≠ Zero.** ``None`` = unmeasured (receipt leg absent / parser did not
  emit); ``Decimal("0")`` = measured zero. Never substitute.
* **Version stamps.** ``schema_version`` + ``primitive_version`` on the payload;
  the writer's augment chokepoint overwrites ``primitive_version`` with the
  canonical ``Primitive.SETTLEMENT`` value at write time.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.measured import encode_money_payload
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, SettlementEventType


class SettlementAccountingEvent:
    """Duck-typed settlement accounting event consumed by AccountingWriter and both backends."""

    schema_version: int = 1
    # VIB-4166 (T6) — the augment chokepoint overwrites this with the canonical
    # per-primitive value (``PRIMITIVE_VERSIONS[Primitive.SETTLEMENT]``) at write
    # time; the class attribute is the sane fallback.
    primitive_version: int = 1

    def __init__(
        self,
        identity: AccountingIdentity,
        event_type: SettlementEventType,
        position_key: str,
        vault_address: str,
        asset_token: str,
        assets_delta: Decimal | None,
        shares_delta: Decimal | None,
        new_total_assets: Decimal | None,
        fee_shares: Decimal | None,
        assets_usd: Decimal | None,
        epoch_id: int | None,
        confidence: AccountingConfidence,
        unavailable_reason: str = "",
    ) -> None:
        self.identity = identity
        self.event_type = event_type.value
        self.position_key = position_key
        self.vault_address = vault_address
        self.asset_token = asset_token
        # assets_delta / shares_delta are SIGNED-BY-EVENT-TYPE magnitudes in human
        # units: for SETTLE_DEPOSIT they are assets received / shares minted; for
        # SETTLE_REDEEM they are assets withdrawn / shares burned. The event_type
        # carries the direction — the magnitudes stay positive so a reader never
        # has to know the sign convention to sum gross flow.
        self.assets_delta = assets_delta
        self.shares_delta = shares_delta
        self.new_total_assets = new_total_assets
        self.fee_shares = fee_shares
        self.assets_usd = assets_usd
        self.epoch_id = epoch_id
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
                "assets_delta": _enc(self.assets_delta),
                "shares_delta": _enc(self.shares_delta),
                "new_total_assets": _enc(self.new_total_assets),
                "fee_shares": _enc(self.fee_shares),
                "assets_usd": _enc(self.assets_usd),
                "epoch_id": self.epoch_id,
                "confidence": str(self.confidence),
                "unavailable_reason": self.unavailable_reason,
                "schema_version": self.schema_version,
                "primitive_version": self.primitive_version,
            }
        )
