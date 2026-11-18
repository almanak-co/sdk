"""Mixed wire decoding preserves native records and refuses unknown protocol data."""

import json

import pytest
from pydantic import ValidationError

from almanak.connectors.composed.codec import codec
from almanak.connectors.composed.execution import preflight
from almanak.connectors.composed.inventory import cash_evidence
from almanak.connectors.uniswap_v3.state import DecisionRecord as ExchangeDecision
from almanak.strategy_api import Memory
from almanak.triggers import DecisionSchedule
from tests.aave_v3_helpers import recorded_v3 as recorded_v3
from tests.test_uniswap_v3_validation import state_for


def test_native_receipt_records_roundtrip_through_composed_codec(recorded_v3):
    binding = codec()
    outcome = recorded_v3.outcomes[0]
    records = {
        "snapshot": outcome.before,
        "decision_record": outcome.decision,
        "outcome": outcome,
    }
    for name, record in records.items():
        assert record is not None
        model = getattr(binding, name)
        restored = model.model_validate_json(record.model_dump_json())
        assert type(restored) is type(record)
        assert restored == record
        constructed = model(**{f: getattr(record, f) for f in type(record).model_fields})
        assert constructed == record


def test_plan_and_pending_keep_native_operation_type(recorded_v3):
    binding = codec()
    outcome = recorded_v3.outcomes[0]
    plan = binding.plan(
        attempt_id="wire-component-test",
        decision=outcome.decision,
        operation=outcome.operation,
        before=outcome.before,
        call=outcome.call,
        execution=outcome.execution,
        nonce=outcome.nonce,
        gas_estimate=1,
        gas=1,
        gas_price=1,
    )
    pending = binding.pending(
        **{f: getattr(plan, f) for f in type(plan).model_fields},
        raw_transaction="0x12",
        tx_hash="0x" + "12" * 32,
    )
    restored = binding.pending.model_validate_json(pending.model_dump_json())
    assert restored == pending
    assert type(restored.operation) is type(outcome.operation)
    assert binding.plan.model_validate_json(plan.model_dump_json()) == plan


def test_decision_retains_both_protocol_observations(recorded_v3):
    _, exchange, intent = state_for()
    lending = recorded_v3.outcomes[0].before
    binding = codec()
    record = binding.decision_record(
        trigger=DecisionSchedule().event(exchange.effective_at),
        identity="12" * 16,
        intent=intent,
        observations=(lending, exchange),
        memory=Memory(),
    )
    restored = binding.decision_record.model_validate_json(record.model_dump_json())
    assert isinstance(restored, ExchangeDecision)
    assert restored == record
    assert type(restored.observations[0]) is type(lending)
    assert type(restored.observations[1]) is type(exchange)
    assert binding.snapshot.model_validate_json(exchange.model_dump_json()) == exchange


def test_unknown_protocol_and_extra_fields_are_refused():
    _, exchange, _ = state_for()
    wire = json.loads(exchange.model_dump_json())
    wire["unexpected"] = "cannot bypass the native schema"
    with pytest.raises(ValidationError):
        codec().snapshot.model_validate_json(json.dumps(wire))
    with pytest.raises(ValidationError):
        codec().decision_record(intent={"kind": "unreviewed_protocol"})


@pytest.mark.parametrize("changed", ["wallet_balance", "position"])
def test_preflight_refuses_conflicting_shared_wallet_evidence(recorded_v3, changed):
    before = recorded_v3.outcomes[0].before
    value = (
        before.wallet_balance + 1
        if changed == "wallet_balance"
        else before.position.model_copy(
            update={
                "reserve": before.position.reserve.model_copy(
                    update={
                        "block": before.block.model_copy(update={"number": before.block.number + 1})
                    }
                )
            }
        )
    )
    conflicting = before.model_copy(update={changed: value})
    with pytest.raises(ValueError, match="disagree"):
        preflight(recorded_v3.config, before.target.account, (before, conflicting))
    assert len(cash_evidence((before, before))) == 1
