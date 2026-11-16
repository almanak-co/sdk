"""Collateral changes carry independent economic bounds and protocol-native request amounts."""

import gzip
import json
from pathlib import Path

import pytest
from eth_abi import decode

from almanak.connectors.gmx_v2 import abi, authority, planning
from almanak.connectors.gmx_v2.candidate_permissions import ORDER_PARAMS
from almanak.connectors.gmx_v2.intents import DecreasePosition, IncreasePosition
from almanak.connectors.gmx_v2.state import Checkpoint
from almanak.models import AssetAmount
from tests import gmx_collateral_path, gmx_position_path
from tests.gmx_evidence import accepted, holding
from tests.gmx_retired import current_json


def withdrawal(before, amount):
    return DecreasePosition(
        target=before.target,
        collateral_withdrawal=AssetAmount(asset=before.target.collateral_asset, amount=amount),
        size_delta_usd_e30=0,
    )


def test_collateral_withdrawal_is_bounded_by_the_retained_position_only():
    state = holding()
    before = state.outcomes[-1].after
    intent = withdrawal(before, 10)
    # Scope-only authority: no author field bounds a withdrawal; ownership does.
    authority.admit(state.capabilities, intent)
    allocation = state.config.allocations[0]
    held = before.owned.position.collateral.amount
    with pytest.raises(ValueError, match="retained position ownership"):
        planning.economic_order(allocation, withdrawal(before, held + 1), before)
    operation = planning.economic_order(allocation, intent, before)
    assert operation.order_type == 4 and operation.size_delta_usd_e30 == 0
    assert operation.collateral_delta.amount == 10
    assert abi.input_amount(operation) == allocation.execution_fee.amount
    calls = decode(("bytes[]",), abi.create_data(operation)[4:])[0]
    numbers = decode((ORDER_PARAMS,), calls[1][4:])[0][1]
    assert numbers[0] == 0 and numbers[1] == 10


def test_collateral_addition_requires_owned_position_and_preserves_zero_notional():
    state = holding()
    before = state.outcomes[-1].after
    allocation = state.config.allocations[0]
    intent = IncreasePosition(
        target=before.target,
        collateral=AssetAmount(asset=before.target.collateral_asset, amount=10),
        size_delta_usd_e30=0,
    )
    authority.admit(state.capabilities, intent)
    with pytest.raises(ValueError, match="requires a retained position"):
        planning.economic_order(allocation, intent, accepted().outcomes[-1].after)
    operation = planning.economic_order(allocation, intent, before)
    assert operation.order_type == 2 and operation.size_delta_usd_e30 == 0
    assert abi.input_amount(operation) == allocation.execution_fee.amount + 10
    calls = decode(("bytes[]",), abi.create_data(operation)[4:])[0]
    numbers = decode((ORDER_PARAMS,), calls[1][4:])[0][1]
    assert numbers[0] == 0 and numbers[1] == 0


def test_collateral_withdrawal_cannot_exceed_owned_aggregate_balance():
    state = holding()
    before = state.outcomes[-1].after
    with pytest.raises(ValueError, match="exceeds retained position ownership"):
        planning.decrease_amounts(
            withdrawal(before, before.owned.position.collateral.amount + 1), before
        )


def preflight():
    path = (
        Path(__file__).parents[1]
        / "docs/internal/reports/evidence/2026-09-09-gmx-collateral-paths"
        / "preflight-avalanche-short-safe-runtime/checkpoint.json.gz"
    )
    return Checkpoint.model_validate_json(current_json(gzip.decompress(path.read_bytes())))


def test_actual_collateral_proof_assertions_refuse_notional_or_receipt_drift():
    state = preflight()
    gmx_collateral_path.assert_collateral_changes(state)
    rows = list(state.outcomes)
    index = [i for i, o in enumerate(rows) if o.kind == "gmx_request_outcome"][2]
    row = rows[index]
    changed = row.after.position.model_copy(
        update={"size_usd_e30": row.after.position.size_usd_e30 + 1}
    )
    rows[index] = row.model_copy(
        update={"after": row.after.model_copy(update={"position": changed})}
    )
    with pytest.raises(AssertionError):
        gmx_collateral_path.assert_collateral_changes(
            state.model_copy(update={"outcomes": tuple(rows)})
        )


def test_fixture_timestamp_precedes_next_authentic_report_without_expiry_fallback():
    proof = json.loads(
        gzip.decompress((gmx_position_path.ROOT / "avalanche-signed-reports.json.gz").read_bytes())
    )
    assert gmx_collateral_path.next_fixture_timestamp(proof, 1788828043) == 1788828353
    assert gmx_collateral_path.next_fixture_timestamp(proof, 1788828358) == 1788829182
    with pytest.raises(StopIteration):
        gmx_collateral_path.next_fixture_timestamp(proof, 2**64)
