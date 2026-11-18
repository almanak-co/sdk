"""Typed lending requests retain connector identity at every JSON boundary."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

from almanak.caip import AssetType
from almanak.connectors.aave_v4.models import Decision, Intent, Memory, SupplyTarget
from almanak.connectors.aave_v4.operations import (
    AaveOperation,
    ApproveOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.aave_v4.permission_adapter import AavePermissions
from almanak.connectors.aave_v4.state import Checkpoint
from almanak.gateway.service import Request as Envelope
from almanak.lending_intents import (
    LendingIntent,
    LendingTarget,
    SupplyIntent,
    WithdrawIntent,
)
from almanak.models import AssetAmount, Hash32
from tests.lending_captures import read_capture

ROOT = Path(__file__).parents[1]
Request = Envelope[Intent, Checkpoint, AavePermissions.manifest_type]


class SyntheticTarget(LendingTarget):
    """A second target shape, without claiming another connector implementation."""

    market: Hash32
    underlying: AssetType

    @property
    def asset(self) -> AssetType:
        return self.underlying


@pytest.fixture
def target(recorded):
    return recorded.config.target(recorded.config.allocations[0].name)


@pytest.mark.parametrize("intent_type", [SupplyIntent, WithdrawIntent])
def test_intents_reuse_concrete_connector_targets(target, intent_type):
    other = SyntheticTarget(
        allocation=target.allocation,
        account=target.account,
        fund=target.fund,
        market="0x" + "12" * 32,
        underlying=target.asset,
    )
    amount = (
        {"amount": AssetAmount(asset=other.asset, amount=7)} if intent_type is SupplyIntent else {}
    )
    intent = intent_type[SyntheticTarget](target=other, **amount)
    adapter = TypeAdapter(LendingIntent[SyntheticTarget])
    restored = adapter.validate_json(adapter.dump_json(intent))
    assert restored == intent
    assert type(restored.target) is SyntheticTarget
    assert restored.target.market == other.market
    assert "market" in json.loads(adapter.dump_json(intent))["target"]
    with pytest.raises(ValidationError):
        TypeAdapter(Intent).validate_json(intent.model_dump_json())


def test_target_asset_cannot_change_fund_chain(target):
    with pytest.raises(ValidationError, match="fund chain"):
        SyntheticTarget(
            allocation=target.allocation,
            account=target.account,
            fund=target.fund,
            market="0x" + "12" * 32,
            underlying=AssetType("eip155:1/slip44:60"),
        )


@pytest.mark.parametrize("intent_type", [SupplyIntent, WithdrawIntent])
def test_unparameterized_intents_refuse_target_erasure(target, intent_type):
    amount = (
        {"amount": AssetAmount(asset=target.asset, amount=7)} if intent_type is SupplyIntent else {}
    )
    with pytest.raises(ValidationError, match="concrete target type"):
        intent_type(target=target, **amount)


@pytest.mark.parametrize("intent_type", [SupplyIntent, WithdrawIntent])
def test_base_target_specialization_refuses_identity_erasure(target, intent_type):
    amount = (
        {"amount": AssetAmount(asset=target.asset, amount=7)} if intent_type is SupplyIntent else {}
    )
    with pytest.raises(ValidationError, match="concrete target type"):
        intent_type[LendingTarget](target=target, **amount)


@pytest.mark.parametrize("kind", ["supply", "withdraw"])
def test_intent_round_trips_through_decision_and_gateway(recorded, target, kind):
    intent = (
        recorded.config.supply(target.allocation, 7)
        if kind == "supply"
        else recorded.config.withdraw(target.allocation)
    )
    expected = SupplyIntent[SupplyTarget] if kind == "supply" else WithdrawIntent[SupplyTarget]
    decision = Decision(memory=Memory(), intent=intent)
    decoded = Decision.model_validate_json(decision.model_dump_json())
    request = Request.model_validate_json(Request(intent=intent).model_dump_json())
    assert type(decoded.intent) is expected
    assert type(request.intent) is expected
    assert decoded.intent == request.intent == intent


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"kind": "borrow"},
        {"kind": "approve"},
        {"kind": "supply"},
        {"kind": "supply", "amount": None},
        {"kind": "withdraw", "amount": 1},
        {"kind": "withdraw", "recipient": "unbound"},
    ],
)
def test_malformed_intents_are_refused_at_gateway_boundary(target, payload):
    wire = {"intent": {"target": target.model_dump(mode="json"), **payload}}
    with pytest.raises(ValidationError):
        Request.model_validate_json(json.dumps(wire))


@pytest.mark.parametrize(
    ("operation_type", "quantity"),
    [(ApproveOperation, 7), (SupplyOperation, 7), (WithdrawOperation, None), (RevokeOperation, 0)],
)
def test_operations_preserve_valid_wire_payloads(target, operation_type, quantity):
    amount = None if quantity is None else AssetAmount(asset=target.asset, amount=quantity)
    operation = operation_type(target=target, amount=amount)
    adapter = TypeAdapter(AaveOperation)
    restored = adapter.validate_json(operation.model_dump_json())
    assert type(restored) is operation_type
    assert restored == operation
    assert restored.account == target.account
    assert restored.allocation == target.allocation


@pytest.mark.parametrize(
    ("kind", "quantity"),
    [
        ("approve", None),
        ("approve", 0),
        ("approve", 2**256),
        ("supply", None),
        ("supply", 0),
        ("supply", 2**256),
        ("withdraw", 0),
        ("withdraw", 7),
        ("revoke", None),
        ("revoke", 7),
    ],
)
def test_operation_variants_refuse_invalid_quantities(target, kind, quantity):
    amount = None if quantity is None else AssetAmount(asset=target.asset, amount=quantity)
    with pytest.raises(ValidationError):
        TypeAdapter(AaveOperation).validate_python(
            {"target": target, "kind": kind, "amount": amount}
        )


@pytest.mark.parametrize("kind", ["approve", "supply", "revoke"])
def test_operations_refuse_wrong_asset(target, kind):
    quantity = 0 if kind == "revoke" else 1
    with pytest.raises(ValidationError, match="reserve asset"):
        TypeAdapter(AaveOperation).validate_python(
            {
                "target": target,
                "kind": kind,
                "amount": AssetAmount(
                    asset=AssetType(f"{target.asset.chain}/slip44:60"), amount=quantity
                ),
            }
        )


def test_existing_checkpoint_keeps_exact_json_meaning():
    # The capture's retired supply `amount` is read as `capital` (blueprint 27); every
    # other value must keep its exact JSON meaning.
    raw = json.loads(read_capture(ROOT / "tests/fixtures/aave-v4-interval-lifecycle.json"))
    state = Checkpoint.model_validate_json(json.dumps(raw))
    assert state.model_dump(mode="json", exclude_unset=True) == raw
    assert isinstance(state.outcomes[0].operation, ApproveOperation)
    assert isinstance(state.outcomes[1].operation, SupplyOperation)
    assert isinstance(state.outcomes[2].operation, WithdrawOperation)


def test_type_checker_narrows_supply_payload_and_target(tmp_path):
    source = tmp_path / "intent_types.py"
    source.write_text(
        """
from typing import assert_never, assert_type
from almanak.connectors.aave_v4.models import Intent, LendingConfig, SupplyTarget
from almanak.lending_intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.models import AssetAmount

def inspect(intent: Intent, config: LendingConfig) -> None:
    assert_type(config.supply("position", 1), SupplyIntent[SupplyTarget])
    assert_type(config.withdraw("position"), WithdrawIntent[SupplyTarget])
    if isinstance(intent, (SupplyIntent, BorrowIntent)):
        assert_type(intent.amount, AssetAmount)
        assert_type(intent.target, SupplyTarget)
    elif isinstance(intent, (WithdrawIntent, RepayIntent)):
        assert_type(intent.amount, None)
        assert_type(intent.target, SupplyTarget)
    else:
        assert_never(intent)
"""
    )
    result = subprocess.run(
        [sys.executable, "-m", "mypy", "--strict", str(source)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
