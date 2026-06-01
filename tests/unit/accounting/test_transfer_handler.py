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


# ─── D1.S3 — round-trip + StrEnum coercion + registry binding ────────────────


def _identity() -> AccountingIdentity:
    return AccountingIdentity(
        id="evt-id",
        deployment_id="strat",
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
        "deployment_id": "strat",
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


# ─── D1.S4 / D3.F5 — Augment chokepoint contract for TRANSFER (after T4) ─────
#
# T3 carried two tests asserting the *broken* state (`live raises`,
# `paper logs ERROR + falls back to UTILITY`). T4 (VIB-4164) wires the
# TAXONOMY row + whitelist so both modes succeed cleanly. The T3 tests have
# been **deleted** rather than weakened to ``pytest.skip`` so a future
# regression cannot silently re-broken behaviour. The two positive tests
# below cover the post-T4 happy paths (D1.S4) and the non-degraded paper
# path (D3.F5).


def _bridge_matching_policy_version() -> int:
    """Helper — read the version the post-T4 augment step is expected to stamp."""
    from almanak.framework.accounting.payload_schemas import MATCHING_POLICY_VERSIONS
    from almanak.framework.primitives.types import Primitive

    return MATCHING_POLICY_VERSIONS[Primitive.BRIDGE]


def test_writer_augment_chokepoint_live_succeeds_after_t4() -> None:
    """D1.S4 — Live writes of a TRANSFER event succeed and stamp the BRIDGE matching version.

    Pre-T4 this raised ``AccountingPersistenceError`` because TAXONOMY had no
    row for the event_type ``"TRANSFER"`` and ``record_for("TRANSFER")`` failed
    with ``UnknownIntentTypeError``. T4 added the row keyed on ``"TRANSFER"``
    pointing at ``Primitive.BRIDGE`` so the writer chokepoint stamps
    ``MATCHING_POLICY_VERSIONS[Primitive.BRIDGE]``.
    """
    import json

    from almanak.framework.accounting.payload_schemas import (
        FORMULA_VERSION,
        SCHEMA_VERSION,
    )

    payload = _event().to_payload_json()
    augmented = augment_accounting_payload(payload, is_live=True)
    assert isinstance(augmented, str) and augmented

    d = json.loads(augmented)
    assert d["event_type"] == "TRANSFER"
    assert d["matching_policy_version"] == _bridge_matching_policy_version()
    assert d["formula_version"] == FORMULA_VERSION
    assert d["schema_version"] == SCHEMA_VERSION


def test_writer_augment_chokepoint_paper_succeeds_after_t4(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """D3.F5 — Paper / dry-run writes of TRANSFER complete cleanly with no degraded-mode ERROR log.

    Pre-T4 the chokepoint logged ERROR and fell back to
    ``MATCHING_POLICY_VERSIONS[Primitive.UTILITY]``. T4 makes this a clean
    path. The test asserts ZERO ERROR records mentioning TRANSFER or
    "no taxonomy row" — a future regression that re-introduces the fallback
    fails this test.
    """
    import json

    payload = _event().to_payload_json()
    with caplog.at_level(logging.ERROR):
        augmented = augment_accounting_payload(payload, is_live=False)
    assert isinstance(augmented, str) and augmented

    d = json.loads(augmented)
    assert d["matching_policy_version"] == _bridge_matching_policy_version()

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    suspicious = [
        r
        for r in error_records
        if "TRANSFER" in r.message.upper() or "NO TAXONOMY ROW" in r.message.upper()
    ]
    assert not suspicious, (
        f"Paper-mode TRANSFER must not log ERROR after T4; got: "
        f"{[r.message for r in suspicious]!r}"
    )


@pytest.mark.parametrize(
    "raw_status",
    ["pending", "settled", "failed"],
)
def test_settlement_status_variants_survive_augmentation(raw_status: str) -> None:
    """D2.V2 — every legal settlement_status round-trips through to_payload_json
    → augment_accounting_payload(is_live=True) → from_payload_json with the value preserved.
    """
    import json

    event = _event(settlement_status=TransferSettlementStatus(raw_status))
    payload = event.to_payload_json()
    augmented = augment_accounting_payload(payload, is_live=True)

    d = json.loads(augmented)
    assert d["settlement_status"] == raw_status

    rebuilt = TransferAccountingEvent.from_payload_json(_identity(), augmented)
    assert rebuilt.settlement_status == TransferSettlementStatus(raw_status)


def test_end_to_end_fake_bridge_produces_transfer_event() -> None:
    """D1.S5 — fake BRIDGE intent → registry dispatcher → TransferAccountingEvent
    → writer chokepoint live success.

    Walks the full production-shaped flow without touching the gateway:

    1. Build a synthetic ledger row with ``intent_type="BRIDGE"``,
       ``protocol="across"``, ``chain="arbitrum"``,
       ``destination_chain="ethereum"``, plus identity fields.
    2. Call ``classify(intent_type, protocol, token_out)`` and assert
       ``AccountingCategory.TRANSFER``.
    3. Look up ``HANDLERS[AccountingCategory.TRANSFER]`` and invoke it via a
       ``HandlerContext``.
    4. Assert the returned ``TransferAccountingEvent`` has
       ``settlement_status=PENDING``, ``confidence=ESTIMATED``,
       and the right source/destination chain values.
    5. Round-trip the payload through ``augment_accounting_payload(is_live=True)``
       → ``from_payload_json`` and assert the augmented dict still says
       ``event_type="TRANSFER"`` and carries the BRIDGE matching version.
    """
    import json

    from almanak.framework.accounting.basis import FIFOBasisStore
    from almanak.framework.accounting.category_handlers import (
        HANDLERS,
        HandlerContext,
    )
    from almanak.framework.primitives.taxonomy import classify

    outbox = {
        "id": "ob",
        "deployment_id": "dep",
        "wallet_address": "0xwallet",
        "position_key": "transfer:arbitrum→ethereum:USDC",
    }
    ledger = {
        "id": "led",
        "deployment_id": "dep",
        "deployment_id": "strat",
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

    # Step 2 — classifier.
    category = classify(ledger["intent_type"], ledger["protocol"], ledger.get("token_out", ""))
    assert category == AccountingCategory.TRANSFER

    # Step 3 — registry dispatch.
    handler = HANDLERS.get(category)
    assert handler is not None

    ctx = HandlerContext(
        outbox_row=outbox,
        ledger_row=ledger,
        basis_store=FIFOBasisStore(),
        prior_open_lookup=lambda _key, _disc=None: None,
    )
    event = handler(ctx)

    # Step 4 — event subject fields.
    assert isinstance(event, TransferAccountingEvent)
    assert event.settlement_status == TransferSettlementStatus.PENDING
    assert event.confidence == AccountingConfidence.ESTIMATED
    assert event.source_chain == "arbitrum"
    assert event.destination_chain == "ethereum"
    assert event.asset == "USDC"
    assert event.amount == Decimal("100")
    assert event.position_key == "transfer:arbitrum→ethereum:USDC"

    # Step 5 — augment chokepoint live success + round-trip.
    augmented = augment_accounting_payload(event.to_payload_json(), is_live=True)
    d = json.loads(augmented)
    assert d["event_type"] == "TRANSFER"
    assert d["matching_policy_version"] == _bridge_matching_policy_version()

    rebuilt = TransferAccountingEvent.from_payload_json(_identity(), augmented)
    assert rebuilt.settlement_status == TransferSettlementStatus.PENDING
    assert rebuilt.source_chain == "arbitrum"
    assert rebuilt.destination_chain == "ethereum"
