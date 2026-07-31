"""Typed accounting event for keeper-settled perp fills — VIB-3872 (WI-3).

Phase-2 of the two-phase perp write model (design §3 D2). Phase-1 is the
submission-time ``PERP_OPEN`` / ``PERP_CLOSE`` event (unchanged, honestly
ESTIMATED with intent-known fields). Phase-2 is this append-only
``PERP_SETTLEMENT`` event: once the keeper fills / cancels / freezes OUR
submitted order, the runner reconciler correlates the keeper receipt (WI-2) and
books the measured fill economics — entry/exit price, fees, funding, realized
PnL — keyed back to the submission ledger row + venue position key.

Design rules (mirrors :class:`~almanak.framework.accounting.settlement_accounting.SettlementAccountingEvent`):

* **Append, never mutate.** ``accounting_events`` is an append-only audit
  surface; the deterministic event id (``make_accounting_event_id``) is the
  idempotency contract, so a reconciler restart re-books exactly the same row.
* **No keeper-tx ledger row.** The keeper pays gas, not us — a
  ``transaction_ledger`` row for the keeper tx would poison ``gas_usd`` and the
  capital-flow classifier (the VIB-5952 class). The event is written DIRECTLY
  through ``AccountingWriter``; its ``identity.ledger_entry_id`` points at OUR
  submission ledger row.
* **Empty ≠ Zero.** ``None`` = the keeper events did not yield the field;
  ``Decimal("0")`` = measured zero. ``settlement_state`` is the measured venue
  outcome; every non-EXECUTED state carries an ``unavailable_reason``.
* **Version stamps.** ``schema_version`` + ``primitive_version`` on the payload;
  the writer's augment chokepoint overwrites ``primitive_version`` with the
  canonical ``Primitive.PERP`` value at write time.
* **Pure build.** This module does NO chain reads — the reconciler materializes
  the verdict (WI-2) BEFORE calling the builder; the build is a pure function of
  the verdict (the ``observations.py`` invariant).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.measured import encode_money_payload
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, PerpEventType
from almanak.framework.models.run_mode import RunMode


class PerpSettlementAccountingEvent:
    """Duck-typed perp-settlement accounting event consumed by AccountingWriter."""

    schema_version: int = 1
    # The augment chokepoint overwrites this with PRIMITIVE_VERSIONS[Primitive.PERP]
    # at write time; the class attribute is the sane fallback.
    primitive_version: int = 2

    def __init__(
        self,
        identity: AccountingIdentity,
        *,
        protocol: str,
        position_key: str,
        submission_ledger_entry_id: str,
        order_key: str,
        settlement_state: str,
        keeper_tx_hash: str | None,
        is_open: bool | None,
        is_long: bool | None,
        market: str | None,
        collateral_token: str | None,
        entry_price: Decimal | None,
        exit_price: Decimal | None,
        size_delta_usd: Decimal | None,
        collateral_delta_amount: Decimal | None,
        price_impact_usd: Decimal | None,
        realized_pnl_usd: Decimal | None,
        position_fee_usd: Decimal | None,
        funding_fee_usd: Decimal | None,
        borrowing_fee_usd: Decimal | None,
        block_number: int | None,
        unavailable_reason: str | None,
        confidence: AccountingConfidence,
    ) -> None:
        self.identity = identity
        self.event_type = PerpEventType.PERP_SETTLEMENT.value
        self.confidence = confidence
        self.position_key = position_key
        self.protocol = protocol
        self.submission_ledger_entry_id = submission_ledger_entry_id
        self.order_key = order_key
        self.settlement_state = settlement_state
        self.keeper_tx_hash = keeper_tx_hash
        self.is_open = is_open
        self.is_long = is_long
        self.market = market
        self.collateral_token = collateral_token
        self.entry_price = entry_price
        self.exit_price = exit_price
        self.size_delta_usd = size_delta_usd
        self.collateral_delta_amount = collateral_delta_amount
        self.price_impact_usd = price_impact_usd
        self.realized_pnl_usd = realized_pnl_usd
        self.position_fee_usd = position_fee_usd
        self.funding_fee_usd = funding_fee_usd
        self.borrowing_fee_usd = borrowing_fee_usd
        self.block_number = block_number
        self.unavailable_reason = unavailable_reason

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                return encode_money_payload(v)
            return v

        return json.dumps(
            {
                "event_type": self.event_type,
                "protocol": self.protocol,
                "position_key": self.position_key,
                "submission_ledger_entry_id": self.submission_ledger_entry_id,
                "order_key": self.order_key,
                "settlement_state": self.settlement_state,
                "keeper_tx_hash": self.keeper_tx_hash,
                "is_open": self.is_open,
                "is_long": self.is_long,
                "market": self.market,
                "collateral_token": self.collateral_token,
                "entry_price": _enc(self.entry_price),
                "exit_price": _enc(self.exit_price),
                "size_delta_usd": _enc(self.size_delta_usd),
                "collateral_delta_amount": _enc(self.collateral_delta_amount),
                "price_impact_usd": _enc(self.price_impact_usd),
                "realized_pnl_usd": _enc(self.realized_pnl_usd),
                "position_fee_usd": _enc(self.position_fee_usd),
                "funding_fee_usd": _enc(self.funding_fee_usd),
                "borrowing_fee_usd": _enc(self.borrowing_fee_usd),
                "block_number": self.block_number,
                "unavailable_reason": self.unavailable_reason,
                "confidence": str(self.confidence),
                "schema_version": self.schema_version,
                "primitive_version": self.primitive_version,
            }
        )


def _dec(value: Any) -> Decimal | None:
    """Coerce a verdict fill field to ``Decimal | None`` (Empty ≠ Zero preserved)."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001 — unparseable ⇒ unmeasured, never a fabricated zero
        return None


def build_perp_settlement_event(
    *,
    verdict: Any,
    submission_ledger_entry_id: str,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    protocol: str,
    wallet_address: str,
    is_open: bool | None,
    timestamp: datetime | None = None,
) -> PerpSettlementAccountingEvent:
    """Build the typed ``PERP_SETTLEMENT`` event from a WI-2 verdict — a pure function.

    ``verdict`` is a ``PerpSettlementVerdict`` (WI-2, frozen). Its ``fill_data`` is
    a ``PerpFillData`` (or ``None`` for the non-EXECUTED states); fields are read
    by ``getattr`` so this module holds no connector import. The event id is
    deterministic and keyed ONLY on STABLE settlement identity —
    ``(deployment_id, "PERP_SETTLEMENT", keeper_tx_hash or order_key, position_key)``,
    NOT the per-tick ``cycle_id`` — so a re-book on a later tick / restart mints the
    SAME id (restart-idempotent append-only backstop). ``cycle_id`` is carried as
    identity metadata only.
    """
    state = str(getattr(verdict.state, "value", verdict.state))
    fill = getattr(verdict, "fill_data", None)
    order_key = str(getattr(verdict, "order_key", "") or "")
    keeper_tx_hash = getattr(verdict, "keeper_tx_hash", None)

    def _f(name: str) -> Any:
        return getattr(fill, name, None) if fill is not None else None

    position_key = str(_f("position_key") or "")
    # A non-EXECUTED terminal state carries no measured fill economics; keep the
    # reason auditable (payload validator requires it) even when the verdict's own
    # reason is None (e.g. a measured CANCELLED/FROZEN outcome).
    reason = getattr(verdict, "unavailable_reason", None)
    if state != "EXECUTED" and not (reason or "").strip():
        reason = f"perp order settlement state={state}: no measured fill economics"

    identity = AccountingIdentity(
        # Deterministic id keyed ONLY on STABLE settlement identity — NOT cycle_id.
        # The reconciler mints a fresh cycle_id every tick, so including it would give
        # the same settlement a DIFFERENT id on a re-book (later tick / restart) and
        # defeat the append-only idempotency backstop. cycle_id stays as identity
        # METADATA below. ``make_accounting_event_id`` requires a cycle_id positional,
        # so a fixed "" is passed in that slot: the stable inputs
        # (deployment_id, "PERP_SETTLEMENT", keeper_tx or order_key, position_key)
        # fully determine the id. (Design §3 D2; CodeRabbit PR #3446.)
        id=make_accounting_event_id(
            deployment_id, "", PerpEventType.PERP_SETTLEMENT.value, keeper_tx_hash or order_key, position_key
        ),
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=RunMode.parse_optional(execution_mode),
        timestamp=timestamp or datetime.now(UTC),
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        tx_hash=str(keeper_tx_hash or ""),
        ledger_entry_id=submission_ledger_entry_id,
    )

    block_number = _f("block_number")
    return PerpSettlementAccountingEvent(
        identity,
        protocol=protocol,
        position_key=position_key,
        submission_ledger_entry_id=submission_ledger_entry_id,
        order_key=order_key,
        settlement_state=state,
        keeper_tx_hash=str(keeper_tx_hash) if keeper_tx_hash else None,
        is_open=is_open if isinstance(is_open, bool) else _f("is_open"),
        is_long=_f("is_long") if isinstance(_f("is_long"), bool) else None,
        market=str(_f("market")) if _f("market") else None,
        collateral_token=str(_f("collateral_token")) if _f("collateral_token") else None,
        entry_price=_dec(_f("entry_price")),
        exit_price=_dec(_f("exit_price")),
        size_delta_usd=_dec(_f("size_delta_usd")),
        collateral_delta_amount=_dec(_f("collateral_delta_amount")),
        price_impact_usd=_dec(_f("price_impact_usd")),
        realized_pnl_usd=_dec(_f("realized_pnl_usd")),
        position_fee_usd=_dec(_f("position_fee_usd")),
        funding_fee_usd=_dec(_f("funding_fee_usd")),
        borrowing_fee_usd=_dec(_f("borrowing_fee_usd")),
        block_number=int(block_number)
        if isinstance(block_number, int) and not isinstance(block_number, bool)
        else None,
        unavailable_reason=reason,
        # EXECUTED with measured fill → HIGH; a non-EXECUTED terminal outcome carries
        # no measured fill economics → UNAVAILABLE + reason (Empty ≠ Zero; the
        # inherited confidence-exclusivity rule forbids HIGH alongside a reason).
        confidence=AccountingConfidence.HIGH if state == "EXECUTED" else AccountingConfidence.UNAVAILABLE,
    )


__all__ = ["PerpSettlementAccountingEvent", "build_perp_settlement_event"]
