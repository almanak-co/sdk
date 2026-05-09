"""Transfer handler stub tests (VIB-4163, T3).

Covers UAT card steps:

- D1.S3 — TransferAccountingEvent round-trip + StrEnum coercion + registry binding.
- D3.F5 — augment chokepoint mode-aware behaviour for unmapped event_type.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from almanak.framework.accounting.category_handlers import HANDLERS
from almanak.framework.accounting.category_handlers.transfer_handler import handle_transfer
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    TransferAccountingEvent,
    TransferEventType,
    TransferSettlementStatus,
)
from almanak.framework.accounting.writer import augment_accounting_payload
from almanak.framework.primitives.types import AccountingCategory
from almanak.framework.state.exceptions import AccountingPersistenceError


# ─── D1.S3 — round-trip + StrEnum coercion + registry binding ────────────────


def _identity() -> AccountingIdentity:
    return AccountingIdentity(
        id="evt-id",
        deployment_id="dep",
        strategy_id="strat",
        cycle_id="cyc",
        execution_mode="live",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        chain="arbitrum",
        protocol="across",
        wallet_address="0xwallet",
        tx_hash="0xtx",
        ledger_entry_id="led",
    )


def _event(**overrides) -> TransferAccountingEvent:
    fields: dict = {
        "identity": _identity(),
        "event_type": TransferEventType.TRANSFER,
        "asset": "USDC",
        "amount": Decimal("100"),
        "amount_usd": Decimal("100"),
        "source_chain": "arbitrum",
        "destination_chain": "ethereum",
        "settlement_status": TransferSettlementStatus.PENDING,
    }
    fields.update(overrides)
    return TransferAccountingEvent(**fields)


def test_settlement_status_strenum_members() -> None:
    """``TransferSettlementStatus`` must have exactly the documented members."""
    assert {s.name for s in TransferSettlementStatus} == {"PENDING", "SETTLED", "FAILED"}
    assert TransferSettlementStatus.PENDING.value == "pending"
    assert TransferSettlementStatus.SETTLED.value == "settled"
    assert TransferSettlementStatus.FAILED.value == "failed"


def test_transfer_event_round_trips_through_payload_json() -> None:
    event = _event()
    payload = event.to_payload_json()
    rebuilt = TransferAccountingEvent.from_payload_json(_identity(), payload)
    assert rebuilt.event_type == TransferEventType.TRANSFER
    assert rebuilt.settlement_status == TransferSettlementStatus.PENDING
    assert rebuilt.asset == "USDC"
    assert rebuilt.amount == Decimal("100")
    assert rebuilt.source_chain == "arbitrum"
    assert rebuilt.destination_chain == "ethereum"


def test_transfer_event_rejects_invalid_settlement_status() -> None:
    """Constructing the event with a string outside the enum raises at coercion."""
    with pytest.raises(ValueError):
        TransferAccountingEvent(
            identity=_identity(),
            event_type=TransferEventType.TRANSFER,
            asset="USDC",
            amount=Decimal("1"),
            amount_usd=None,
            source_chain="arbitrum",
            destination_chain="ethereum",
            settlement_status=TransferSettlementStatus("garbage"),  # raises ValueError
        )


def test_transfer_event_post_init_coerces_raw_string_settlement_status() -> None:
    """Raw strings (not wrapped in the enum constructor) must also raise.

    CodeRabbit Major finding on PR #2194: without ``__post_init__``,
    ``TransferAccountingEvent(..., settlement_status="garbage")`` silently
    stores the string and only crashes at serialization. The dataclass's
    ``__post_init__`` coerces via ``TransferSettlementStatus(...)`` so invalid
    values raise immediately at construction.
    """
    with pytest.raises(ValueError):
        TransferAccountingEvent(
            identity=_identity(),
            event_type=TransferEventType.TRANSFER,
            asset="USDC",
            amount=Decimal("1"),
            amount_usd=None,
            source_chain="arbitrum",
            destination_chain="ethereum",
            settlement_status="garbage",  # type: ignore[arg-type]  # raw string, NOT enum
        )


def test_transfer_event_post_init_coerces_valid_string_settlement_status() -> None:
    """Valid raw strings ARE coerced into enum members by __post_init__.

    The complementary case to the rejection test above: passing
    ``settlement_status="pending"`` (the StrEnum's value, but as a raw str)
    should succeed and the field should be the enum member after construction.
    """
    event = TransferAccountingEvent(
        identity=_identity(),
        event_type=TransferEventType.TRANSFER,
        asset="USDC",
        amount=Decimal("1"),
        amount_usd=None,
        source_chain="arbitrum",
        destination_chain="ethereum",
        settlement_status="pending",  # type: ignore[arg-type]  # raw string coerced by __post_init__
    )
    assert event.settlement_status is TransferSettlementStatus.PENDING


def test_handler_emits_pending_for_default_ledger_row() -> None:
    """Default-shaped BRIDGE ledger row produces PENDING settlement_status.

    Also asserts ``position_key`` flows from the outbox into the event so the
    state-backend ``getattr(event, "position_key", "")`` writes a non-empty
    value to ``accounting_events.position_key`` (per audit finding #2 round 1).
    """
    outbox = {
        "id": "ob",
        "deployment_id": "dep",
        "wallet_address": "0xwallet",
        "position_key": "transfer:arbitrum→ethereum:USDC",
    }
    ledger = {
        "id": "led",
        "deployment_id": "dep",
        "strategy_id": "strat",
        "cycle_id": "cyc",
        "execution_mode": "live",
        "chain": "arbitrum",
        "destination_chain": "ethereum",
        "protocol": "across",
        "intent_type": "BRIDGE",
        "tx_hash": "0xtx",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "token_in": "USDC",
        "amount_in": "100",
        "gas_usd": "0.5",
    }
    event = handle_transfer(outbox, ledger)
    assert event is not None
    assert event.settlement_status == TransferSettlementStatus.PENDING
    assert event.asset == "USDC"
    assert event.amount == Decimal("100")
    assert event.source_chain == "arbitrum"
    assert event.destination_chain == "ethereum"
    assert event.confidence == AccountingConfidence.ESTIMATED
    assert event.position_key == "transfer:arbitrum→ethereum:USDC"


def test_handler_honours_settlement_status_override() -> None:
    outbox = {"id": "ob", "wallet_address": "0xwallet"}
    ledger = {
        "id": "led",
        "intent_type": "BRIDGE",
        "chain": "arbitrum",
        "token_in": "USDC",
        "amount_in": "10",
        "settlement_status": "settled",
        "tx_hash": "0xtx",
        "timestamp": "2026-01-01T00:00:00+00:00",
    }
    event = handle_transfer(outbox, ledger)
    assert event is not None
    assert event.settlement_status == TransferSettlementStatus.SETTLED
    assert event.confidence == AccountingConfidence.HIGH


def test_handler_falls_back_to_pending_on_invalid_settlement_status(caplog: pytest.LogCaptureFixture) -> None:
    outbox = {"id": "ob", "wallet_address": "0xwallet"}
    ledger = {
        "id": "led",
        "intent_type": "BRIDGE",
        "chain": "arbitrum",
        "token_in": "USDC",
        "amount_in": "10",
        "settlement_status": "garbage",
        "tx_hash": "0xtx",
        "timestamp": "2026-01-01T00:00:00+00:00",
    }
    with caplog.at_level(logging.WARNING):
        event = handle_transfer(outbox, ledger)
    assert event is not None
    assert event.settlement_status == TransferSettlementStatus.PENDING
    assert any("garbage" in r.message for r in caplog.records)


def test_registry_binds_transfer_to_handle_transfer() -> None:
    fn = HANDLERS.get(AccountingCategory.TRANSFER)
    assert fn is not None
    assert fn.__module__.endswith("transfer_handler")


# ─── D3.F5 — Augment chokepoint mode-aware contract for TRANSFER ─────────────


def test_writer_augment_chokepoint_live_raises_for_transfer_until_t4() -> None:
    """Live writes of a TRANSFER event_type raise until T4 adds the TAXONOMY row.

    This is **expected** behaviour in T3: T4 (VIB-4164) extends
    ``primitives.taxonomy.TAXONOMY`` with a row for ``TRANSFER``. Until then, the
    writer's augment chokepoint resolves primitive via ``record_for(event_type)``
    and raises ``AccountingPersistenceError`` (whose ``__cause__`` is
    ``UnknownIntentTypeError``) in live mode.

    A future PR that "fixes" the live-mode raise by stamping a default version
    would silently mask the unknown-event_type and FAIL this test.
    """
    payload = _event().to_payload_json()
    with pytest.raises(AccountingPersistenceError) as exc_info:
        augment_accounting_payload(payload, is_live=True)
    # Cause is UnknownIntentTypeError (per writer.py augment chokepoint).
    cause = exc_info.value.__cause__
    assert cause is not None, "AccountingPersistenceError must wrap a cause for TRANSFER"
    assert "TRANSFER" in str(exc_info.value).upper() or "TRANSFER" in str(cause).upper()


def test_writer_augment_chokepoint_paper_logs_for_transfer_until_t4(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Paper / dry-run writes of TRANSFER fall back to UTILITY's version + ERROR log."""
    payload = _event().to_payload_json()
    with caplog.at_level(logging.ERROR):
        augmented = augment_accounting_payload(payload, is_live=False)
    # Augment returned the augmented payload (does NOT raise).
    assert isinstance(augmented, str) and augmented
    # ERROR log mentions TRANSFER and the unknown-taxonomy condition.
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert error_records, "expected at least one ERROR log line"
    combined = "\n".join(r.message for r in error_records).upper()
    assert "TRANSFER" in combined
