"""Captured deployed settlements exercise durable inventory and allowance accounting."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter

from almanak.connectors.uniswap_v3.observations import Snapshot
from almanak.connectors.uniswap_v3.operations import Close, ProtocolOperation
from almanak.connectors.uniswap_v3.receipts import Effects
from almanak.connectors.uniswap_v3.reconciliation import (
    allowances_after,
    owned_after,
    settlement,
    validate_balances,
)
from almanak.gateway.ethereum import DataError

CAPTURE = json.loads(
    gzip.decompress(
        (Path(__file__).with_name("fixtures") / "uniswap-v3-operations.json.gz").read_bytes()
    )
)


def decoded(step):
    return (
        TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"])),
        Snapshot.model_validate_json(json.dumps(step["before"])),
        Snapshot.model_validate_json(json.dumps(step["after"])),
        Effects.model_validate_json(json.dumps(step["effects"])),
    )


@pytest.mark.parametrize("row", CAPTURE, ids=lambda r: r["origin"]["chain"]["chain_id"])
def test_captured_all_operations_preserve_inventory_and_authority(row):
    for step in row["steps"]:
        operation, before, after, effects = decoded(step)
        pending = SimpleNamespace(operation=operation, before=before)
        owned = owned_after(pending, effects, after.position, True)
        assert owned == after.owned
        assert allowances_after(before, operation, effects, True) == after.allowances
        gas = before.native_balance.amount - after.native_balance.amount
        assert validate_balances(pending, after, effects, gas, True) == ()
        assert settlement(operation, effects, True)[1] == ()
        assert owned_after(pending, effects, None, False) == before.owned
        assert allowances_after(before, operation, effects, False) == before.allowances


def test_rounding_shortfall_remains_actual_cash_and_missing_fee_split():
    operation, before, after, effects = decoded(
        next(s for s in CAPTURE[0]["steps"] if s["operation"]["kind"] == "close")
    )
    assert isinstance(operation, Close)
    short = effects.model_copy(update={"delta0": operation.minimum0.amount - 1})
    fees, issues = settlement(operation, short, True)
    assert fees[0] is None
    assert len(issues) == 2
    owned = owned_after(SimpleNamespace(operation=operation, before=before), short, None, True)
    assert owned.inventory0.amount == before.owned.inventory0.amount + short.delta0
    assert owned.position is None


def test_external_movements_are_visible_without_losing_protocol_effects():
    operation, before, after, effects = decoded(
        next(s for s in CAPTURE[0]["steps"] if s["operation"]["kind"] == "swap")
    )
    changed = after.model_copy(
        update={"wallet0": after.wallet0.model_copy(update={"amount": after.wallet0.amount + 1})}
    )
    pending = SimpleNamespace(operation=operation, before=before)
    gas = before.native_balance.amount - after.native_balance.amount
    assert "unexplained external" in validate_balances(pending, changed, effects, gas, True)[0]
    changed = after.model_copy(update={"allowances": before.allowances})
    with pytest.raises(DataError, match="allowance"):
        validate_balances(pending, changed, effects, gas, True)
