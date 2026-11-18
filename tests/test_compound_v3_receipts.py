"""Comet receipt attribution and ownership reconcile all retained direct transactions."""

import copy
from types import SimpleNamespace

import pytest

from almanak.connectors.compound_v3.execution import validate_simulation
from almanak.connectors.compound_v3.operations import ApproveOperation, RevokeOperation
from almanak.connectors.compound_v3.receipts import allowance_after, movement, validate_effects
from almanak.gateway.ethereum import DataError
from tests.test_compound_v3_operations import operation, read
from tests.test_compound_v3_projection import snapshot_input


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_every_direct_receipt_reconciles_event_amount_principal_collateral_and_allowance(network):
    raw, run = read(network), read(network + "-operations")
    allowances = {}
    for step in run["steps"]:
        op = operation(raw, step, allowances)
        identity = step["account"], op.target.asset
        previous = allowances.get(identity, 0)
        before = snapshot_input(network, step, op, previous)
        assets = movement(step["receipt"], op)
        next_allowance = allowance_after(op, previous, assets)
        changed = {**step, "before": step["after"]}
        after = snapshot_input(network, changed, op, next_allowance)
        principal, collateral = validate_effects(
            SimpleNamespace(before=before, operation=op), after, assets, True
        )
        expected_before, expected_after = (
            step["before"]["accounts"][step["account"]],
            step["after"]["accounts"][step["account"]],
        )
        assert principal == (
            expected_after["user"][0] - expected_before["user"][0]
            if op.target.reserve.is_base
            else 0
        )
        assert collateral == (
            0
            if op.target.reserve.is_base
            else expected_after["collateral"] - expected_before["collateral"]
        )
        validate_simulation(op, step["simulation"], before)
        if isinstance(op, (ApproveOperation, RevokeOperation)):
            assert assets == 0
        else:
            field = "cash" if op.target.reserve.is_base else "weth"
            assert assets == abs(expected_after[field] - expected_before[field])
        allowances[identity] = next_allowance
    assert all(value == 0 for value in allowances.values())


@pytest.mark.parametrize("fault", ["missing", "duplicate", "fund", "quantity", "asset"])
def test_receipt_refuses_missing_duplicated_or_misattributed_protocol_events(fault):
    raw, run = read("base"), read("base-operations")
    step = next(s for s in run["steps"] if s["kind"] == "collateral_supply")
    op = operation(raw, step, {})
    receipt = copy.deepcopy(step["receipt"])
    log = next(
        log for log in receipt["logs"] if log["address"].lower() == op.target.reserve.market.comet
    )
    if fault == "missing":
        receipt["logs"].remove(log)
    elif fault == "duplicate":
        receipt["logs"].append(copy.deepcopy(log))
    elif fault == "quantity":
        log["data"] = "0x"
    else:
        log["topics"][1 if fault == "fund" else 3] = "0x" + "00" * 12 + "12" * 20
    with pytest.raises(DataError):
        movement(receipt, op)


def test_failed_receipt_preserves_positions_and_allowance():
    raw, run = read("base"), read("base-operations")
    step = next(s for s in run["steps"] if s["kind"] == "collateral_supply")
    op = operation(raw, step, {})
    before = snapshot_input("base", step, op, op.amount.amount)
    pending = SimpleNamespace(before=before, operation=op)
    assert validate_effects(pending, before, 0, False) == (0, 0)
    with pytest.raises(DataError, match="non-economic"):
        validate_effects(pending, before, 1, False)
    changed = before.model_copy(update={"allowance": 0})
    with pytest.raises(DataError, match="reconcile"):
        validate_effects(pending, changed, 0, False)
